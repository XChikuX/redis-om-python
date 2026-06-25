# type: ignore
"""Tests for HashModel per-field expiration (HEXPIRE family) and the
Redis 8.0 HGETEX / HSETEX / HGETDEL commands.

Requires Redis 7.4+ for HEXPIRE-family commands and Redis 8.0+ for
HGETEX/HSETEX/HGETDEL.  Tests skip gracefully when the commands are
unavailable.
"""

import abc
import time

import pytest
import pytest_asyncio

from aredis_om import HashModel, Migrator

from .conftest import py_test_mark_asyncio


def _has_hexpire(db):
    async def _check():
        try:
            info = await db.execute_command("COMMAND", "INFO", "hexpire")
            return bool(info and all(info))
        except Exception:
            return False

    return _check


def _has_hgetdel(db):
    async def _check():
        try:
            info = await db.execute_command("COMMAND", "INFO", "hgetdel")
            return bool(info and all(info))
        except Exception:
            return False

    return _check


@pytest_asyncio.fixture
async def model(key_prefix, redis):
    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Doc(BaseHashModel):
        title: str
        body: str = ""
        views: int = 0

    await Migrator(conn=redis).run()
    return Doc


@pytest_asyncio.fixture
async def optional_model(key_prefix, redis):
    """A model with all-optional fields, so deleting/expiring one field
    doesn't break validation on reload."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Doc(BaseHashModel):
        title: str = ""
        body: str = ""
        views: int = 0

    await Migrator(conn=redis).run()
    return Doc


class TestHashFieldTTL:
    """HEXPIRE / HTTL / HPERSIST / HEXPIREAT / HEXPIRETIME family."""

    @py_test_mark_asyncio
    async def test_set_field_ttl(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIRE requires Redis 7.4+")
        doc = await model(title="hello", body="world").save()
        result = await doc.set_field_ttl("title", 60)
        assert result == 1

    @py_test_mark_asyncio
    async def test_set_field_ttl_px(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HPEXPIRE requires Redis 7.4+")
        doc = await model(title="hello").save()
        result = await doc.set_field_ttl("title", 60000, px=True)
        assert result == 1

    @py_test_mark_asyncio
    async def test_get_field_ttl(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HTTL requires Redis 7.4+")
        doc = await model(title="hello").save()
        await doc.set_field_ttl("title", 100)
        ttl = await doc.get_field_ttl("title")
        assert 0 < ttl <= 100

    @py_test_mark_asyncio
    async def test_get_field_ttl_no_expiry(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HTTL requires Redis 7.4+")
        doc = await model(title="hello").save()
        ttl = await doc.get_field_ttl("title")
        assert ttl == -1  # no expiry

    @py_test_mark_asyncio
    async def test_get_field_ttl_missing_field(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HTTL requires Redis 7.4+")
        doc = await model(title="hello").save()
        ttl = await doc.get_field_ttl("nonexistent")
        assert ttl == -2  # field doesn't exist

    @py_test_mark_asyncio
    async def test_persist_field(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HPERSIST requires Redis 7.4+")
        doc = await model(title="hello").save()
        await doc.set_field_ttl("title", 60)
        result = await doc.persist_field("title")
        assert result == 1
        ttl = await doc.get_field_ttl("title")
        assert ttl == -1

    @py_test_mark_asyncio
    async def test_set_field_ttl_at(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIREAT requires Redis 7.4+")
        doc = await model(title="hello").save()
        future_ts = int(time.time()) + 120
        result = await doc.set_field_ttl_at("title", future_ts)
        assert result == 1
        expire_time = await doc.get_field_expire_time("title")
        assert expire_time == future_ts

    @py_test_mark_asyncio
    async def test_get_field_expire_time_no_expiry(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIRETIME requires Redis 7.4+")
        doc = await model(title="hello").save()
        t = await doc.get_field_expire_time("title")
        assert t == -1

    @py_test_mark_asyncio
    async def test_expire_multiple_fields(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIRE requires Redis 7.4+")
        doc = await model(title="hello", body="world").save()
        results = await doc.expire_fields(60, "title", "body")
        assert results == [1, 1]

    @py_test_mark_asyncio
    async def test_expire_multiple_fields_one_missing(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIRE requires Redis 7.4+")
        doc = await model(title="hello").save()
        results = await doc.expire_fields(60, "title", "ghost")
        assert results[0] == 1
        assert results[1] == -2  # field doesn't exist

    @py_test_mark_asyncio
    async def test_field_expires_independently(self, optional_model, redis):
        """Per-field TTL must not affect the key TTL or other fields."""
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIRE requires Redis 7.4+")
        doc = await optional_model(title="hello", body="world").save()
        await doc.set_field_ttl("title", 1)
        # body should have no expiry
        assert await doc.get_field_ttl("body") == -1
        # Wait for title to expire.
        await _sleep_past_expiry()
        reloaded = await optional_model.get(doc.pk)
        # body survives; title is gone (empty after empty-string-to-None).
        assert reloaded.body == "world"

    @py_test_mark_asyncio
    async def test_set_field_ttl_missing_field(self, model, redis):
        if not await _has_hexpire(redis)():
            pytest.skip("HEXPIRE requires Redis 7.4+")
        doc = await model(title="hello").save()
        result = await doc.set_field_ttl("ghost", 60)
        assert result == -2  # field doesn't exist


class TestHashGetEx:
    """HGETEX — get a field and set its expiry atomically (Redis 8.0+)."""

    @py_test_mark_asyncio
    async def test_get_and_set_field_expiry(self, model, redis):
        if not await _has_hgetdel(redis)():
            pytest.skip("HGETEX requires Redis 8.0+")
        doc = await model(title="hello", body="world").save()
        val = await doc.get_and_set_field_expiry("title", 60)
        assert val == "hello"
        ttl = await doc.get_field_ttl("title")
        assert 0 < ttl <= 60

    @py_test_mark_asyncio
    async def test_get_and_set_field_expiry_missing(self, model, redis):
        if not await _has_hgetdel(redis)():
            pytest.skip("HGETEX requires Redis 8.0+")
        doc = await model(title="hello").save()
        val = await doc.get_and_set_field_expiry("ghost", 60)
        assert val is None


class TestHashSetEx:
    """HSETEX — set fields with a shared expiry (Redis 8.0+)."""

    @py_test_mark_asyncio
    async def test_set_fields_with_expiry(self, model, redis):
        if not await _has_hgetdel(redis)():
            pytest.skip("HSETEX requires Redis 8.0+")
        doc = await model(title="hello").save()
        n = await doc.set_fields_with_expiry(60, body="new body", views="5")
        assert n >= 1
        ttl = await doc.get_field_ttl("body")
        assert 0 < ttl <= 60


class TestHashGetDel:
    """HGETDEL — get and delete a field atomically (Redis 8.0+)."""

    @py_test_mark_asyncio
    async def test_get_and_delete_field(self, optional_model, redis):
        if not await _has_hgetdel(redis)():
            pytest.skip("HGETDEL requires Redis 8.0+")
        doc = await optional_model(title="hello", body="world").save()
        val = await doc.get_and_delete_field("title")
        assert val == "hello"
        # body survives
        reloaded = await optional_model.get(doc.pk)
        assert reloaded.body == "world"

    @py_test_mark_asyncio
    async def test_get_and_delete_field_missing(self, model, redis):
        if not await _has_hgetdel(redis)():
            pytest.skip("HGETDEL requires Redis 8.0+")
        doc = await model(title="hello").save()
        val = await doc.get_and_delete_field("ghost")
        assert val is None


async def _sleep_past_expiry():
    """Sleep long enough for a 1-second field TTL to elapse."""
    import asyncio

    await asyncio.sleep(1.2)
