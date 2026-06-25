# type: ignore
"""Tests for AtomicString (SET IFEQ, DELEX, DIGEST) and MSETEX.

These commands require Redis 8.4+. Tests skip gracefully when the
commands are unavailable.
"""

import pytest

from aredis_om import AtomicString, get_redis_connection, msetex

from .conftest import py_test_mark_asyncio


def _has_command(db, command):
    """Probe the server for a command's availability."""

    async def _check():
        try:
            info = await db.execute_command("COMMAND", "INFO", command)
            return bool(info and all(info))
        except Exception:
            return False

    return _check


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def atom(db, key_prefix):
    return AtomicString(db, f"{key_prefix}:lock")


class TestCompareAndSet:
    @py_test_mark_asyncio
    async def test_cas_success(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("SET IFEQ requires Redis 8.4+")
        await atom.set("idle")
        got = await atom.compare_and_set(expected="idle", new="held")
        assert got is True
        assert await atom.get() == "held"

    @py_test_mark_asyncio
    async def test_cas_mismatch(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("SET IFEQ requires Redis 8.4+")
        await atom.set("held")
        got = await atom.compare_and_set(expected="idle", new="stolen")
        assert got is False
        assert await atom.get() == "held"

    @py_test_mark_asyncio
    async def test_cas_with_expire(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("SET IFEQ requires Redis 8.4+")
        await atom.set("idle")
        got = await atom.compare_and_set(expected="idle", new="held", expire=60)
        assert got is True
        ttl = await atom._db.ttl(atom.key)
        assert 0 < ttl <= 60


class TestSetIfNotEqual:
    @py_test_mark_asyncio
    async def test_ifne_different(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("SET IFNE requires Redis 8.4+")
        await atom.set("a")
        got = await atom.set_if_not_equal(not_equal="b", new="c")
        assert got is True
        assert await atom.get() == "c"

    @py_test_mark_asyncio
    async def test_ifne_same(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("SET IFNE requires Redis 8.4+")
        await atom.set("a")
        got = await atom.set_if_not_equal(not_equal="a", new="c")
        assert got is False
        assert await atom.get() == "a"


class TestCompareAndDelete:
    @py_test_mark_asyncio
    async def test_cad_success(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("DELEX requires Redis 8.4+")
        await atom.set("held")
        got = await atom.compare_and_delete(expected="held")
        assert got is True
        assert await atom.get() is None

    @py_test_mark_asyncio
    async def test_cad_mismatch(self, atom, db):
        if not await _has_command(db, "delex")():
            pytest.skip("DELEX requires Redis 8.4+")
        await atom.set("held")
        got = await atom.compare_and_delete(expected="stale")
        assert got is False
        assert await atom.get() == "held"


class TestDigest:
    @py_test_mark_asyncio
    async def test_digest_present(self, atom, db):
        if not await _has_command(db, "digest")():
            pytest.skip("DIGEST requires Redis 8.4+")
        await atom.set("abcdef")
        d = await atom.digest()
        assert isinstance(d, str)
        assert len(d) > 0

    @py_test_mark_asyncio
    async def test_digest_stable(self, atom, db):
        if not await _has_command(db, "digest")():
            pytest.skip("DIGEST requires Redis 8.4+")
        await atom.set("hello")
        d1 = await atom.digest()
        d2 = await atom.digest()
        assert d1 == d2

    @py_test_mark_asyncio
    async def test_digest_different_values(self, atom, db):
        if not await _has_command(db, "digest")():
            pytest.skip("DIGEST requires Redis 8.4+")
        await atom.set("aaa")
        d1 = await atom.digest()
        await atom.set("bbb")
        d2 = await atom.digest()
        assert d1 != d2


class TestMSetEx:
    @py_test_mark_asyncio
    async def test_msetex_basic(self, db, key_prefix):
        if not await _has_command(db, "msetex")():
            pytest.skip("MSETEX requires Redis 8.4+")
        k1 = f"{key_prefix}:a"
        k2 = f"{key_prefix}:b"
        n = await msetex(db, {k1: "v1", k2: "v2"})
        assert n == 1
        assert await db.get(k1) == "v1"
        assert await db.get(k2) == "v2"

    @py_test_mark_asyncio
    async def test_msetex_with_expire(self, db, key_prefix):
        if not await _has_command(db, "msetex")():
            pytest.skip("MSETEX requires Redis 8.4+")
        k1 = f"{key_prefix}:a"
        k2 = f"{key_prefix}:b"
        n = await msetex(db, {k1: "v1", k2: "v2"}, expire=60)
        assert n == 1
        assert 0 < await db.ttl(k1) <= 60
        assert 0 < await db.ttl(k2) <= 60

    @py_test_mark_asyncio
    async def test_msetex_nx(self, db, key_prefix):
        if not await _has_command(db, "msetex")():
            pytest.skip("MSETEX requires Redis 8.4+")
        k1 = f"{key_prefix}:a"
        k2 = f"{key_prefix}:b"
        await db.set(k1, "existing")
        # NX should fail because k1 already exists.
        n = await msetex(db, {k1: "NEW", k2: "v2"}, nx=True)
        assert n == 0
        assert await db.get(k1) == "existing"

    @py_test_mark_asyncio
    async def test_msetex_xx(self, db, key_prefix):
        if not await _has_command(db, "msetex")():
            pytest.skip("MSETEX requires Redis 8.4+")
        k1 = f"{key_prefix}:a"
        await db.set(k1, "existing")
        # XX should succeed because k1 exists (k2 doesn't, but MSETEX is
        # all-or-nothing on the condition — it sets when keys exist).
        n = await msetex(db, {k1: "updated"}, xx=True)
        assert n == 1
        assert await db.get(k1) == "updated"

    @py_test_mark_asyncio
    async def test_msetex_empty_mapping(self, db):
        n = await msetex(db, {})
        assert n == 0

    @py_test_mark_asyncio
    async def test_msetex_keepttl(self, db, key_prefix):
        if not await _has_command(db, "msetex")():
            pytest.skip("MSETEX requires Redis 8.4+")
        k1 = f"{key_prefix}:a"
        await db.set(k1, "v1", ex=100)
        n = await msetex(db, {k1: "v2"}, keepttl=True)
        assert n == 1
        assert await db.get(k1) == "v2"
        # TTL should be preserved (~100, not reset)
        ttl = await db.ttl(k1)
        assert 0 < ttl <= 100

    @py_test_mark_asyncio
    async def test_msetex_nx_xx_conflict(self, db, key_prefix):
        with pytest.raises(ValueError):
            await msetex(db, {"k": "v"}, nx=True, xx=True)

    @py_test_mark_asyncio
    async def test_msetex_expire_conflict(self, db, key_prefix):
        with pytest.raises(ValueError):
            await msetex(db, {"k": "v"}, expire=60, expire_ms=60000)
