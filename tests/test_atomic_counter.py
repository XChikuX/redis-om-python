# type: ignore
"""Tests for the AtomicCounter (INCREX) helper."""

import pytest

from aredis_om import AtomicCounter, get_redis_connection
from aredis_om.model.counter import clear_increx_cache

from .conftest import py_test_mark_asyncio


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def counter(db, key_prefix):
    c = AtomicCounter(db, f"{key_prefix}:counter")
    yield c
    clear_increx_cache()


class TestAtomicCounterBasic:
    @py_test_mark_asyncio
    async def test_incr_default(self, counter):
        new_val, applied = await counter.incr()
        assert new_val == 1
        assert applied == 1

    @py_test_mark_asyncio
    async def test_incr_by_amount(self, counter):
        new_val, applied = await counter.incr(amount=5)
        assert new_val == 5
        assert applied == 5

    @py_test_mark_asyncio
    async def test_incr_float(self, counter):
        new_val, applied = await counter.incr(amount=1.5)
        assert new_val == pytest.approx(1.5)
        assert applied == pytest.approx(1.5)

    @py_test_mark_asyncio
    async def test_value_unset(self, counter):
        assert await counter.value() is None

    @py_test_mark_asyncio
    async def test_value_after_incr(self, counter):
        await counter.incr(amount=10)
        assert await counter.value() == 10

    @py_test_mark_asyncio
    async def test_reset(self, counter):
        await counter.incr(amount=42)
        await counter.reset()
        assert await counter.value() == 0

    @py_test_mark_asyncio
    async def test_delete(self, counter):
        await counter.incr()
        await counter.delete()
        assert await counter.value() is None


class TestAtomicCounterExpire:
    @py_test_mark_asyncio
    async def test_incr_with_expire(self, counter):
        await counter.incr(amount=1, expire=100)
        ttl = await counter.ttl()
        assert 0 < ttl <= 100

    @py_test_mark_asyncio
    async def test_incr_no_expire(self, counter):
        await counter.incr()
        ttl = await counter.ttl()
        assert ttl == -1  # no TTL


class TestAtomicCounterBounds:
    @py_test_mark_asyncio
    async def test_upper_bound_skip(self, counter):
        await counter.incr(amount=99)
        new_val, applied = await counter.incr(amount=5, bounds=(None, 100))
        # 99 + 5 = 104 > 100, so operation skipped
        assert new_val == 99
        assert applied == 0

    @py_test_mark_asyncio
    async def test_upper_bound_saturate(self, counter):
        await counter.incr(amount=99)
        new_val, applied = await counter.incr(
            amount=5, bounds=(None, 100), saturate=True
        )
        # Saturate caps at bound; INCREX reports the actual delta applied
        assert new_val == 100
        assert applied > 0

    @py_test_mark_asyncio
    async def test_lower_bound_skip(self, counter):
        await counter.incr(amount=5)
        new_val, applied = await counter.incr(amount=-10, bounds=(0, None))
        # 5 - 10 = -5 < 0, so operation skipped
        assert new_val == 5
        assert applied == 0

    @py_test_mark_asyncio
    async def test_within_bounds(self, counter):
        new_val, applied = await counter.incr(amount=50, bounds=(0, 100))
        assert new_val == 50
        assert applied == 50


class TestAtomicCounterRateLimit:
    """Simulate a rate-limiting pattern using INCREX."""

    @py_test_mark_asyncio
    async def test_rate_limit_window(self, counter):
        # Allow 3 requests per window
        results = []
        for _ in range(5):
            _, applied = await counter.incr(
                amount=1, bounds=(0, 3), expire=60, enx=True
            )
            results.append(applied)

        # First 3 succeed (applied=1), last 2 rejected (applied=0)
        assert results == [1, 1, 1, 0, 0]
