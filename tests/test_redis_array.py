# type: ignore
"""Tests for the RedisArray (AR* commands) helper.

These tests require Redis 8.8+ with the preview Arrays data type.
They are skipped automatically when ``ARSET`` is not available.
"""

import pytest

from aredis_om import RedisArray, get_redis_connection

from .conftest import py_test_mark_asyncio


def _skip_if_no_arrays(db):
    """Decorator-style skip: call inside the test body."""
    try:
        import asyncio
    except ImportError:
        pytest.skip("asyncio unavailable")

    async def _check():
        try:
            info = await db.execute_command("COMMAND", "INFO", "arset")
            if not info or not all(info):
                pytest.skip("Redis Arrays not available (need Redis 8.8+)")
        except Exception:
            pytest.skip("Redis Arrays not available")

    return _check()


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def arr(db, key_prefix):
    a = RedisArray(db, f"{key_prefix}:arr")
    yield a


class TestRedisArrayIndexed:
    @py_test_mark_asyncio
    async def test_set_and_get(self, arr, db):
        await _skip_if_no_arrays(db)
        created = await arr.set(0, "a", "b", "c")
        assert created == 3
        assert await arr.get(0) == "a"
        assert await arr.get(1) == "b"
        assert await arr.get(2) == "c"
        assert await arr.get(999) is None

    @py_test_mark_asyncio
    async def test_mset_and_mget(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "x", 5: "y", 100: "z"})
        vals = await arr.mget(0, 5, 100, 999)
        assert vals == ["x", "y", "z", None]

    @py_test_mark_asyncio
    async def test_get_range(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "a", 1: "b", 3: "d"})
        vals = await arr.get_range(0, 3)
        assert vals == ["a", "b", None, "d"]


class TestRedisArrayScan:
    @py_test_mark_asyncio
    async def test_scan_skips_gaps(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "a", 1: "b", 3: "d"})
        pairs = await arr.scan(0, 3)
        assert pairs == [(0, "a"), (1, "b"), (3, "d")]


class TestRedisArrayInsert:
    @py_test_mark_asyncio
    async def test_sequential_insert(self, arr, db):
        await _skip_if_no_arrays(db)
        idx0 = await arr.insert("event1")
        idx1 = await arr.insert("event2")
        assert idx0 == 0
        assert idx1 == 1
        assert await arr.next_index() == 2

    @py_test_mark_asyncio
    async def test_seek_and_insert(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.insert("a")
        await arr.seek(10)
        idx = await arr.insert("b")
        assert idx == 10


class TestRedisArrayRing:
    @py_test_mark_asyncio
    async def test_ring_wraps(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.ring(3, "v0", "v1", "v2")
        await arr.ring(3, "v3")  # wraps to index 0
        assert await arr.get(0) == "v3"

    @py_test_mark_asyncio
    async def test_last_items(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.ring(3, "v0", "v1", "v2", "v3")
        items = await arr.last_items(2)
        assert items == ["v2", "v3"]


class TestRedisArrayIntrospection:
    @py_test_mark_asyncio
    async def test_length_and_count(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.set(0, "a")
        await arr.set(1000000, "b")
        assert await arr.length() == 1000001
        assert await arr.count() == 2


class TestRedisArrayAggregate:
    @py_test_mark_asyncio
    async def test_sum(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "10", 1: "20", 2: "30"})
        result = await arr.aggregate(0, 2, "SUM")
        assert int(result) == 60

    @py_test_mark_asyncio
    async def test_max(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "10", 1: "20", 2: "30"})
        result = await arr.aggregate(0, 2, "MAX")
        assert int(result) == 30

    @py_test_mark_asyncio
    async def test_match_count(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "10", 1: "20", 2: "30"})
        result = await arr.aggregate(0, 2, "MATCH", value="10")
        assert int(result) == 1


class TestRedisArrayGrep:
    @py_test_mark_asyncio
    async def test_match_search(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset(
            {0: "boot: ok", 1: "warn: disk", 2: "ERROR: cpu", 4: "error: net"}
        )
        indices = await arr.grep(0, 4, [("MATCH", "error")], nocase=True)
        assert set(indices) == {2, 4}


class TestRedisArrayDelete:
    @py_test_mark_asyncio
    async def test_delete_at(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "a", 1: "b", 2: "c"})
        deleted = await arr.delete_at(1)
        assert deleted == 1
        assert await arr.get(1) is None
        assert await arr.get(0) == "a"

    @py_test_mark_asyncio
    async def test_delete_range(self, arr, db):
        await _skip_if_no_arrays(db)
        await arr.mset({0: "a", 1: "b", 2: "c"})
        deleted = await arr.delete_range(0, 1)
        assert deleted == 2
        assert await arr.get(0) is None
        assert await arr.get(2) == "c"
