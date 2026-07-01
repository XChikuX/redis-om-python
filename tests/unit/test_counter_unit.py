# type: ignore
"""Unit tests for AtomicCounter — pure-Python arg builders and fallback paths.

These tests exercise ``_build_increx_args``, the legacy ``_incr_fallback``
two-round-trip implementation, ``value()`` int/float coercion, and the
clear-cache helper. They use a fake ``db`` client so no live Redis is
required.

End-to-end coverage lives in ``tests/test_atomic_counter.py``.
"""

import pytest

from aredis_om.model.counter import (
    AtomicCounter,
    _increx_cache,
    clear_increx_cache,
)


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


# ── fake client ──────────────────────────────────────────────────────────


class _FakeDb:
    def __init__(self):
        self.calls = []

    async def execute_command(self, *args):
        self.calls.append(args)
        # Default INCREX reply: (new_value, actual_increment).
        return [5, 1]

    # Override below in _IncrexAvailableDb / _IncrexUnavailableDb.

    async def incrby(self, key, amount):
        self.calls.append(("incrby", key, amount))
        return amount

    async def incrbyfloat(self, key, amount):
        self.calls.append(("incrbyfloat", key, amount))
        return amount

    async def set(self, key, value):
        self.calls.append(("set", key, value))
        return True

    async def get(self, key):
        self.calls.append(("get", key))
        return None

    async def expire(self, key, ttl):
        self.calls.append(("expire", key, ttl))
        return True

    async def persist(self, key):
        self.calls.append(("persist", key))
        return True

    async def delete(self, key):
        self.calls.append(("delete", key))
        return 1

    async def ttl(self, key):
        self.calls.append(("ttl", key))
        return -1


# ── _build_increx_args: pure function, no Redis required ────────────────


class TestBuildIncrexArgs:
    def test_default(self):
        out = AtomicCounter._build_increx_args("k", 1, None, None, False, False)
        assert out == ["INCREX", "k"]

    def test_int_non_one_uses_BYINT(self):
        out = AtomicCounter._build_increx_args("k", 5, None, None, False, False)
        assert out == ["INCREX", "k", "BYINT", "5"]

    def test_int_one_omitted(self):
        # amount=1 is the default; BYINT is never added because it would be
        # redundant. The arg list stays at the minimum.
        out = AtomicCounter._build_increx_args("k", 1, None, None, False, False)
        assert "BYINT" not in out
        assert "BYFLOAT" not in out

    def test_float_uses_BYFLOAT_with_string_form(self):
        out = AtomicCounter._build_increx_args("k", 0.5, None, None, False, False)
        assert out == ["INCREX", "k", "BYFLOAT", "0.5"]

    def test_lower_bound_only(self):
        out = AtomicCounter._build_increx_args("k", 1, None, (0, None), False, False)
        assert out == ["INCREX", "k", "LBOUND", "0"]

    def test_upper_bound_only(self):
        out = AtomicCounter._build_increx_args("k", 1, None, (None, 100), False, False)
        assert out == ["INCREX", "k", "UBOUND", "100"]

    def test_both_bounds(self):
        out = AtomicCounter._build_increx_args("k", 1, None, (0, 100), False, False)
        assert out == ["INCREX", "k", "LBOUND", "0", "UBOUND", "100"]

    def test_saturate_flag(self):
        out = AtomicCounter._build_increx_args("k", 1, None, None, True, False)
        assert out == ["INCREX", "k", "SATURATE"]

    def test_expire(self):
        out = AtomicCounter._build_increx_args("k", 1, 60, None, False, False)
        assert out == ["INCREX", "k", "EX", "60"]

    def test_enx_requires_expire(self):
        # ENX without EX is silently dropped — the server requires both.
        out = AtomicCounter._build_increx_args("k", 1, None, None, False, True)
        assert "ENX" not in out
        assert out == ["INCREX", "k"]

    def test_enx_with_expire(self):
        out = AtomicCounter._build_increx_args("k", 1, 60, None, False, True)
        assert out == ["INCREX", "k", "EX", "60", "ENX"]

    def test_all_options_combined(self):
        out = AtomicCounter._build_increx_args("k", 0.5, 60, (0, 100), True, True)
        assert out == [
            "INCREX",
            "k",
            "BYFLOAT",
            "0.5",
            "LBOUND",
            "0",
            "UBOUND",
            "100",
            "SATURATE",
            "EX",
            "60",
            "ENX",
        ]

    def test_negative_int_amount(self):
        out = AtomicCounter._build_increx_args("k", -3, None, None, False, False)
        # amount=-3 != 1, so BYINT "-3" is emitted.
        assert out == ["INCREX", "k", "BYINT", "-3"]

    def test_negative_float_amount(self):
        out = AtomicCounter._build_increx_args("k", -0.25, None, None, False, False)
        assert out == ["INCREX", "k", "BYFLOAT", "-0.25"]


# ── value() int/float coercion ────────────────────────────────────────────


class TestValue:
    @py_test_mark_asyncio
    async def test_value_unset_returns_none(self):
        db = _FakeDb()

        async def fake_get(key):
            return None

        db.get = fake_get  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        assert await c.value() is None

    @py_test_mark_asyncio
    async def test_value_int_string(self):
        db = _FakeDb()

        async def fake_get(key):
            return "42"

        db.get = fake_get  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        assert await c.value() == 42

    @py_test_mark_asyncio
    async def test_value_int_bytes(self):
        db = _FakeDb()

        async def fake_get(key):
            return b"7"

        db.get = fake_get  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        # Even raw bytes (decode_responses=False) coerce via int().
        assert await c.value() == 7

    @py_test_mark_asyncio
    async def test_value_float_fallback(self):
        # When ``int()`` raises ValueError, falls through to ``float()``.
        db = _FakeDb()

        async def fake_get(key):
            return "1.5"

        db.get = fake_get  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        assert await c.value() == 1.5

    @py_test_mark_asyncio
    async def test_value_float_native(self):
        # If the server returns a string that looks like a float, int()
        # raises ValueError so we fall through to float().
        db = _FakeDb()

        async def fake_get(key):
            return "3.14"

        db.get = fake_get  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        assert await c.value() == 3.14


# ── simple pass-through commands ──────────────────────────────────────────


class TestResetPersistDeleteTtl:
    @py_test_mark_asyncio
    async def test_reset_sets_zero(self):
        db = _FakeDb()
        c = AtomicCounter(db, "k")
        await c.reset()
        assert db.calls[-1] == ("set", "k", 0)

    @py_test_mark_asyncio
    async def test_persist(self):
        db = _FakeDb()
        c = AtomicCounter(db, "k")
        await c.persist()
        assert db.calls[-1] == ("persist", "k")

    @py_test_mark_asyncio
    async def test_delete(self):
        db = _FakeDb()
        c = AtomicCounter(db, "k")
        await c.delete()
        assert db.calls[-1] == ("delete", "k")

    @py_test_mark_asyncio
    async def test_ttl(self):
        db = _FakeDb()
        c = AtomicCounter(db, "k")
        out = await c.ttl()
        # The default ``_FakeDb.ttl`` returns -1.
        assert out == -1
        assert db.calls[-1] == ("ttl", "k")

    def test_key_property(self):
        db = _FakeDb()
        c = AtomicCounter(db, "my-key")
        assert c.key == "my-key"


# ── clear_increx_cache ────────────────────────────────────────────────────


class TestClearCache:
    def test_clears_cache(self):
        # Seed the cache and verify the helper clears it.
        _increx_cache[id({})] = True
        _increx_cache[id([])] = False
        assert len(_increx_cache) >= 2
        clear_increx_cache()
        assert _increx_cache == {}


# ── incr() fallback path: bypass INCREX entirely ─────────────────────────


class _IncrexUnavailableDb(_FakeDb):
    """A fake client where INCREX is reported as unavailable.

    The first ``COMMAND INFO increx`` call raises ``RuntimeError`` — the
    standard "unknown command" reply we expect on < Redis 8.8.
    """

    def __init__(self):
        super().__init__()
        self.command_info_calls = 0

    async def execute_command(self, *args):
        self.command_info_calls += 1
        if args[:2] == ("COMMAND", "INFO"):
            raise RuntimeError("unknown command")
        return [5, 1]


class TestIncrFallback:
    @py_test_mark_asyncio
    async def test_int_fallback_uses_incrby(self):
        clear_increx_cache()
        db = _IncrexUnavailableDb()
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=1)
        assert new_val == 1
        assert actual == 1
        # Ensure the fallback used incrby rather than INCREX.
        assert ("incrby", "k", 1) in db.calls

    @py_test_mark_asyncio
    async def test_float_fallback_uses_incrbyfloat(self):
        clear_increx_cache()
        db = _IncrexUnavailableDb()

        async def fake_incrbyfloat(key, amount):
            db.calls.append(("incrbyfloat", key, amount))
            return amount

        db.incrbyfloat = fake_incrbyfloat  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=0.5)
        assert new_val == 0.5
        assert actual == 0.5
        assert ("incrbyfloat", "k", 0.5) in db.calls  # type: ignore[operator]

    @py_test_mark_asyncio
    async def test_fallback_saturate_lower(self):
        # When amount pushes new_val below lower AND saturate=True, the
        # fallback clamps via ``set`` rather than rolling back.
        clear_increx_cache()
        db = _IncrexUnavailableDb()

        async def fake_incrby(key, amount):
            db.calls.append(("incrby", key, amount))
            return -20  # below the lower bound (-10)

        db.incrby = fake_incrby  # type: ignore[assignment]

        # Patch set to record value.
        sets = []

        async def fake_set(key, value):
            sets.append((key, value))
            return True

        db.set = fake_set  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=1, bounds=(-10, 100), saturate=True)
        # Lower-bound saturation: value clamped to -10.
        assert new_val == -10
        assert actual == 1
        assert ("k", -10) in sets
        # Make sure we never rolled back via incrby(-amount).
        assert not any(call[0] == "incrby" and call[2] == -1 for call in db.calls)

    @py_test_mark_asyncio
    async def test_fallback_saturate_upper(self):
        clear_increx_cache()
        db = _IncrexUnavailableDb()

        async def fake_incrby(key, amount):
            db.calls.append(("incrby", key, amount))
            return 200

        db.incrby = fake_incrby  # type: ignore[assignment]

        async def fake_set(key, value):
            db.calls.append(("set", "k", value))
            return True

        db.set = fake_set  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        new_val, _ = await c.incr(amount=1, bounds=(0, 100), saturate=True)
        assert new_val == 100

    @py_test_mark_asyncio
    async def test_fallback_rollback_lower(self):
        # Without saturate, an out-of-bounds increment is rolled back.
        clear_increx_cache()
        db = _IncrexUnavailableDb()

        async def fake_incrby(key, amount):
            db.calls.append(("incrby", key, amount))
            return -1

        db.incrby = fake_incrby  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=1, bounds=(0, 100), saturate=False)
        assert new_val == -1
        assert actual == 0
        # The negative rollback call (-amount) was issued.
        assert any(call[0] == "incrby" and call[2] == -1 for call in db.calls)

    @py_test_mark_asyncio
    async def test_fallback_rollback_upper(self):
        clear_increx_cache()
        db = _IncrexUnavailableDb()

        async def fake_incrby(key, amount):
            db.calls.append(("incrby", key, amount))
            return 200

        db.incrby = fake_incrby  # type: ignore[assignment]
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=1, bounds=(0, 100), saturate=False)
        assert new_val == 200
        assert actual == 0
        assert any(call[0] == "incrby" and call[2] == -1 for call in db.calls)

    @py_test_mark_asyncio
    async def test_fallback_expire(self):
        clear_increx_cache()
        db = _IncrexUnavailableDb()
        c = AtomicCounter(db, "k")
        await c.incr(amount=1, expire=30)
        assert ("expire", "k", 30) in db.calls

    @py_test_mark_asyncio
    async def test_fallback_no_expire_skips_set(self):
        clear_increx_cache()
        db = _IncrexUnavailableDb()
        c = AtomicCounter(db, "k")
        await c.incr(amount=1)
        assert not any(call[0] == "expire" for call in db.calls)


# ── incr() with INCREX available ─────────────────────────────────────────


class _IncrexAvailableDb(_FakeDb):
    """A fake client where INCREX is reported as available.

    Subsequent ``execute_command`` calls return a configurable INCREX reply.
    """

    def __init__(self, increx_reply):
        super().__init__()
        self._increx_reply = increx_reply
        # First call: the COMMAND INFO probe must succeed.
        self._probed = False

    async def execute_command(self, *args):
        self.calls.append(args)
        if args[:2] == ["COMMAND", "INFO"] and not self._probed:
            self._probed = True
            return [["INCREX", -4, 1, 1, 1]]  # non-empty list of args
        return self._increx_reply


class TestIncrViaIncrex:
    @py_test_mark_asyncio
    async def test_int_increx_returns_int_tuple(self):
        clear_increx_cache()
        db = _IncrexAvailableDb(increx_reply=[10, 1])
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=1)
        assert new_val == 10
        assert actual == 1
        # The INCREX command (not the fallback) was used.
        assert any(call and call[0] == "INCREX" for call in db.calls)

    @py_test_mark_asyncio
    async def test_float_increx_returns_float_tuple(self):
        clear_increx_cache()
        db = _IncrexAvailableDb(increx_reply=[0.5, 0.5])
        c = AtomicCounter(db, "k")
        new_val, actual = await c.incr(amount=0.5)
        assert new_val == 0.5
        # Match by float equality.
        assert abs(actual - 0.5) < 1e-9

    @py_test_mark_asyncio
    async def test_increx_caches_availability(self):
        clear_increx_cache()
        # Force the probe to fail so the negative result gets cached.
        db = _IncrexUnavailableDb()
        c = AtomicCounter(db, "k")
        await c.incr(amount=1)
        # Cache should now hold an entry; another incr call should not
        # re-probe COMMAND INFO.
        calls_after_first = db.command_info_calls
        await c.incr(amount=1)
        assert db.command_info_calls == calls_after_first
