# type: ignore
"""Unit tests for AtomicString — comparison logic, validation, and arg building.

Covers ``compare_and_set``/``set_if_not_equal`` truthy-coercion logic,
``msetex`` argument building and input validation, and ``clear_atomic_string_cache``.

End-to-end coverage lives in ``tests/test_atomic_string.py``.
"""

import pytest

from aredis_om.model.atomic_string import (
    AtomicString,
    _capability_cache,
    clear_atomic_string_cache,
    msetex,
)


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


class _FakeDb:
    def __init__(self, *, return_value=None, side_effect=None):
        self.calls = []
        self._return_value = return_value
        self._side_effect = side_effect

    async def execute_command(self, *args):
        self.calls.append(args)
        if self._side_effect is not None:
            raise self._side_effect
        return self._return_value

    async def get(self, key):
        self.calls.append(("get", key))
        return self._return_value

    async def set(self, key, value, **kwargs):
        self.calls.append(("set", key, value, kwargs))
        return self._return_value

    async def delete(self, key):
        self.calls.append(("delete", key))
        return self._return_value


# ── AtomicString wrappers ────────────────────────────────────────────────


class TestCompareAndSet:
    @py_test_mark_asyncio
    async def test_basic(self):
        db = _FakeDb(return_value="OK")
        s = AtomicString(db, "k")
        assert await s.compare_and_set("idle", "held") is True
        assert db.calls == [("SET", "k", "held", "IFEQ", "idle")]

    @py_test_mark_asyncio
    async def test_with_expire(self):
        db = _FakeDb(return_value="OK")
        s = AtomicString(db, "k")
        await s.compare_and_set("idle", "held", expire=60)
        assert db.calls == [("SET", "k", "held", "IFEQ", "idle", "EX", 60)]

    @py_test_mark_asyncio
    async def test_failure_returns_false(self):
        # On condition mismatch the server returns None (nil). The wrapper
        # normalises that to ``False``.
        db = _FakeDb(return_value=None)
        s = AtomicString(db, "k")
        assert await s.compare_and_set("expected", "new") is False

    @py_test_mark_asyncio
    async def test_failure_returns_false_when_server_returns_false(self):
        # Redis-py may decode ``nil`` as ``False`` rather than ``None``.
        db = _FakeDb(return_value=False)
        s = AtomicString(db, "k")
        assert await s.compare_and_set("expected", "new") is False


class TestSetIfNotEqual:
    @py_test_mark_asyncio
    async def test_basic(self):
        db = _FakeDb(return_value="OK")
        s = AtomicString(db, "k")
        assert await s.set_if_not_equal("idle", "held") is True
        assert db.calls == [("SET", "k", "held", "IFNE", "idle")]

    @py_test_mark_asyncio
    async def test_with_expire(self):
        db = _FakeDb(return_value="OK")
        s = AtomicString(db, "k")
        await s.set_if_not_equal("idle", "held", expire=30)
        assert db.calls[-1] == (
            "SET",
            "k",
            "held",
            "IFNE",
            "idle",
            "EX",
            30,
        )

    @py_test_mark_asyncio
    async def test_condition_match_returns_false(self):
        db = _FakeDb(return_value=None)
        s = AtomicString(db, "k")
        assert await s.set_if_not_equal("held", "new") is False


class TestCompareAndDelete:
    @py_test_mark_asyncio
    async def test_basic(self):
        db = _FakeDb(return_value=1)
        s = AtomicString(db, "k")
        assert await s.compare_and_delete("expected") is True
        assert db.calls == [("DELEX", "k", "IFEQ", "expected")]

    @py_test_mark_asyncio
    async def test_no_match_returns_false(self):
        db = _FakeDb(return_value=0)
        s = AtomicString(db, "k")
        assert await s.compare_and_delete("expected") is False

    @py_test_mark_asyncio
    async def test_bool_coercion(self):
        # Boolean reply (rare, but possible from a custom server) is also
        # coerced.
        db = _FakeDb(return_value=True)
        s = AtomicString(db, "k")
        assert await s.compare_and_delete("expected") is True


class TestDigest:
    @py_test_mark_asyncio
    async def test_returns_hex(self):
        db = _FakeDb(return_value="abc123def456")
        s = AtomicString(db, "k")
        assert await s.digest() == "abc123def456"

    @py_test_mark_asyncio
    async def test_unset_returns_none(self):
        # When the key doesn't exist the server returns ``None``; the helper
        # returns ``None`` as well (rather than empty string or 0).
        db = _FakeDb(return_value=None)
        s = AtomicString(db, "k")
        assert await s.digest() is None

    @py_test_mark_asyncio
    async def test_falsy_value_returns_none(self):
        # A digest of an empty/falsy value returns ``None`` to signal
        # "key not set", which avoids leaking a possibly-misleading hash.
        db = _FakeDb(return_value=0)
        s = AtomicString(db, "k")
        assert await s.digest() is None


# ── convenience accessors ────────────────────────────────────────────────


class TestConvenienceAccessors:
    @py_test_mark_asyncio
    async def test_get(self):
        db = _FakeDb(return_value="hello")
        s = AtomicString(db, "k")
        assert await s.get() == "hello"
        assert db.calls == [("get", "k")]

    @py_test_mark_asyncio
    async def test_get_unset_returns_none(self):
        db = _FakeDb(return_value=None)
        s = AtomicString(db, "k")
        assert await s.get() is None

    @py_test_mark_asyncio
    async def test_set_without_expire(self):
        db = _FakeDb(return_value=True)
        s = AtomicString(db, "k")
        assert await s.set("hello") is True
        assert db.calls == [("set", "k", "hello", {})]

    @py_test_mark_asyncio
    async def test_set_with_expire(self):
        db = _FakeDb(return_value=True)
        s = AtomicString(db, "k")
        assert await s.set("hello", expire=10) is True
        call = db.calls[-1]
        assert call[0] == "set"
        assert call[1] == "k"
        assert call[2] == "hello"
        assert call[3].get("ex") == 10

    @py_test_mark_asyncio
    async def test_delete_returns_true(self):
        db = _FakeDb(return_value=1)
        s = AtomicString(db, "k")
        assert await s.delete() is True

    @py_test_mark_asyncio
    async def test_delete_returns_false_when_missing(self):
        db = _FakeDb(return_value=0)
        s = AtomicString(db, "k")
        assert await s.delete() is False

    def test_key_property(self):
        s = AtomicString(_FakeDb(), "my-key")
        assert s.key == "my-key"


# ── msetex ────────────────────────────────────────────────────────────────


class TestMsetex:
    @py_test_mark_asyncio
    async def test_basic(self):
        db = _FakeDb(return_value=1)
        n = await msetex(db, {"k1": "v1", "k2": "v2"})
        assert n == 1
        # Layout: MSETEX numkeys k1 v1 k2 v2 (no flags).
        assert db.calls[0] == (
            "MSETEX",
            2,
            "k1",
            "v1",
            "k2",
            "v2",
        )

    @py_test_mark_asyncio
    async def test_nx_flag(self):
        db = _FakeDb(return_value=1)
        await msetex(db, {"k1": "v1"}, nx=True)
        assert db.calls[0][-1] == "NX"

    @py_test_mark_asyncio
    async def test_xx_flag(self):
        db = _FakeDb(return_value=1)
        await msetex(db, {"k1": "v1"}, xx=True)
        assert db.calls[0][-1] == "XX"

    @py_test_mark_asyncio
    async def test_expire_seconds(self):
        db = _FakeDb(return_value=1)
        await msetex(db, {"k1": "v1"}, expire=60)
        # The arg layout is ... "k1", "v1", "EX", 60.
        assert db.calls[0][-2:] == ("EX", 60)

    @py_test_mark_asyncio
    async def test_expire_ms(self):
        db = _FakeDb(return_value=1)
        await msetex(db, {"k1": "v1"}, expire_ms=1500)
        assert db.calls[0][-2:] == ("PX", 1500)

    @py_test_mark_asyncio
    async def test_keepttl(self):
        db = _FakeDb(return_value=1)
        await msetex(db, {"k1": "v1"}, keepttl=True)
        assert "KEEPTTL" in db.calls[0]

    @py_test_mark_asyncio
    async def test_empty_mapping_no_op(self):
        db = _FakeDb(return_value=42)
        # Empty mapping is a no-op; returns 0 without contacting Redis.
        n = await msetex(db, {})
        assert n == 0
        assert db.calls == []

    def test_nx_and_xx_mutually_exclusive(self):
        # Validation happens before the Redis call; the helper raises
        # without sending any command.
        with pytest.raises(ValueError, match="nx and xx are mutually"):
            # We pass a non-async db; the validator should fire first.
            import asyncio

            async def go():
                await msetex(_FakeDb(), {"k": "v"}, nx=True, xx=True)

            asyncio.run(go())

    def test_expire_and_expire_ms_mutually_exclusive(self):
        with pytest.raises(ValueError, match="expire and expire_ms are mutually"):
            import asyncio

            async def go():
                await msetex(
                    _FakeDb(),
                    {"k": "v"},
                    expire=10,
                    expire_ms=1000,
                )

            asyncio.run(go())

    @py_test_mark_asyncio
    async def test_returns_int(self):
        db = _FakeDb(return_value="1")
        n = await msetex(db, {"k": "v"})
        assert n == 1
        assert isinstance(n, int)


# ── clear_atomic_string_cache ─────────────────────────────────────────────


class TestClearCache:
    def test_clears_cache(self):
        _capability_cache[id({})] = {"SET"}
        _capability_cache[id([])] = {"GET"}
        assert _capability_cache
        clear_atomic_string_cache()
        assert _capability_cache == {}
