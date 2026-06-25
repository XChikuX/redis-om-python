# type: ignore
"""Tests for HOTKEYS observability (Redis 8.6+).

HOTKEYS is a server-side top-K hot-key sampler. These tests skip
gracefully on Redis < 8.6.
"""

import pytest

from aredis_om import get_redis_connection
from aredis_om.hotkeys import (
    HotKeysSnapshot,
    has_hotkeys,
    hotkeys_get,
    hotkeys_reset,
    hotkeys_snapshot,
    hotkeys_start,
    hotkeys_stop,
)

from .conftest import py_test_mark_asyncio


def _has_command(db, command):
    async def _check():
        try:
            info = await db.execute_command("COMMAND", "INFO", command)
        except Exception:
            return False
        if not info:
            return False
        name = command.lower()
        if isinstance(info, dict):
            return name in {k.lower() for k in info.keys()}
        for entry in info:
            if entry is None:
                continue
            if isinstance(entry, (list, tuple)) and entry:
                if str(entry[0]).lower() == name:
                    return True
        return False

    return _check


@pytest.fixture
def db():
    return get_redis_connection()


class TestCapability:
    @py_test_mark_asyncio
    async def test_has_hotkeys(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        assert await has_hotkeys(db) is True


class TestStartStop:
    @py_test_mark_asyncio
    async def test_start_with_duration(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        await hotkeys_start(db, metrics=["CPU"], duration_seconds=1)
        # Server auto-stops after duration; explicit RESET cleans up.
        import asyncio

        await asyncio.sleep(2)
        snap = await hotkeys_get(db)
        assert isinstance(snap, HotKeysSnapshot)
        await hotkeys_reset(db)

    @py_test_mark_asyncio
    async def test_start_invalid_metric_raises(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        with pytest.raises(ValueError, match="CPU.*NET"):
            await hotkeys_start(db, metrics=["INVALID"], duration_seconds=1)

    @py_test_mark_asyncio
    async def test_start_empty_metrics_raises(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        with pytest.raises(ValueError, match="at least one metric"):
            await hotkeys_start(db, metrics=[], duration_seconds=1)

    @py_test_mark_asyncio
    async def test_stop_and_reset(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        # Start with duration=0 (run until stopped).
        await hotkeys_start(db, metrics=["CPU"], duration_seconds=1)
        import asyncio

        await asyncio.sleep(2)  # let it auto-stop
        assert await hotkeys_stop(db) is True
        assert await hotkeys_reset(db) is True


class TestGet:
    @py_test_mark_asyncio
    async def test_get_after_collection(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        # Start a 1s sampling window.
        await hotkeys_start(
            db, metrics=["CPU", "NET"], top_k=5, duration_seconds=1
        )
        # Generate some load.
        for i in range(10):
            await db.set(f"hotkeys:test:{i}", "x" * 100)
        await db.get("hotkeys:test:0")
        import asyncio

        await asyncio.sleep(2)
        snap = await hotkeys_get(db)
        await hotkeys_reset(db)

        assert isinstance(snap, HotKeysSnapshot)
        assert snap.sample_ratio == 1
        assert snap.duration_ms > 0
        # The CPU section should have detected our keys.
        assert isinstance(snap.top_by_cpu, list)
        assert isinstance(snap.top_by_net, list)

    @py_test_mark_asyncio
    async def test_snapshot_fields_populated(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        await hotkeys_start(db, metrics=["CPU"], duration_seconds=1)
        import asyncio

        await asyncio.sleep(2)
        snap = await hotkeys_get(db)
        await hotkeys_reset(db)

        # CPU section present, NET section empty.
        assert isinstance(snap.top_by_cpu, list)
        assert snap.top_by_net == []

    @py_test_mark_asyncio
    async def test_raw_dict_contains_metadata(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        await hotkeys_start(db, metrics=["CPU"], duration_seconds=1)
        import asyncio

        await asyncio.sleep(2)
        snap = await hotkeys_get(db)
        await hotkeys_reset(db)

        assert "sample-ratio" in snap.raw
        assert "collection-duration-ms" in snap.raw


class TestSnapshotHelper:
    @py_test_mark_asyncio
    async def test_snapshot_helper_runs_cycle(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        # Generate load in a background task during sampling.
        import asyncio

        async def gen_load():
            for _ in range(20):
                try:
                    await db.set("hotkeys:helper:key", "x" * 200)
                except Exception:
                    pass
                await asyncio.sleep(0.05)

        task = asyncio.create_task(gen_load())
        snap = await hotkeys_snapshot(
            db, metrics=["CPU", "NET"], duration_seconds=1
        )
        await task

        assert isinstance(snap, HotKeysSnapshot)
        assert snap.duration_ms > 0

    @py_test_mark_asyncio
    async def test_snapshot_helper_rejects_zero_duration(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        with pytest.raises(ValueError, match="duration_seconds"):
            await hotkeys_snapshot(db, duration_seconds=0)


class TestTopKBounded:
    @py_test_mark_asyncio
    async def test_topk_count_limit(self, db):
        if not await _has_command(db, "hotkeys")():
            pytest.skip("HOTKEYS requires Redis 8.6+")
        # top_k=2 should cap result lists at 2 entries.
        await hotkeys_start(
            db, metrics=["CPU"], top_k=2, duration_seconds=1
        )
        import asyncio

        await asyncio.sleep(2)
        snap = await hotkeys_get(db)
        await hotkeys_reset(db)

        assert len(snap.top_by_cpu) <= 2
