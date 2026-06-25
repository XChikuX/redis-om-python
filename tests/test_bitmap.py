# type: ignore
"""Tests for BitmapOps (BITOP DIFF, DIFF1, ANDOR, ONE — Redis 8.2+).

Skips gracefully on Redis < 8.2.
"""

import pytest

from aredis_om import get_redis_connection
from aredis_om.model.bitmap import BitmapOps, has_bitmap_ops

from .conftest import py_test_mark_asyncio


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def ops(db, key_prefix):
    return BitmapOps(db)


async def _seed(db, key: str, bits: list[int]):
    """SETBIT each position in ``bits`` to 1."""
    for i in bits:
        await db.setbit(key, i, 1)


async def _read(db, key: str, n: int = 8) -> list[int]:
    """Read ``n`` bits from ``key`` as a list."""
    if await db.exists(key) == 0:
        return [0] * n
    return [await db.getbit(key, i) for i in range(n)]


class TestCapability:
    @py_test_mark_asyncio
    async def test_has_bitmap_ops(self, db):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")


class TestDiff:
    @py_test_mark_asyncio
    async def test_diff_basic(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 3])  # 1001
        await _seed(db, b, [1, 2])  # 0110
        n = await ops.diff(out, a, b)
        assert n == 1  # dest size in bytes
        # dest = a AND NOT b = 1001 AND 1001 = 1001
        assert await _read(db, out) == [1, 0, 0, 1, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_diff_empty_first(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 1, 2, 3])
        await _seed(db, b, [])  # empty
        n = await ops.diff(out, a, b)
        assert await _read(db, out) == [1, 1, 1, 1, 0, 0, 0, 0]
        assert n == 1


class TestDiff1:
    @py_test_mark_asyncio
    async def test_diff1_basic(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 3])  # 1001
        await _seed(db, b, [1, 2])  # 0110
        n = await ops.diff1(out, a, b)
        assert n == 1
        # dest = b AND NOT a = 0110 AND 0110 = 0110
        assert await _read(db, out) == [0, 1, 1, 0, 0, 0, 0, 0]


class TestAndor:
    @py_test_mark_asyncio
    async def test_andor_basic(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, c, out = (
            f"{key_prefix}:a",
            f"{key_prefix}:b",
            f"{key_prefix}:c",
            f"{key_prefix}:d",
        )
        # a=1001, b=0110, c=0011
        await _seed(db, a, [0, 3])
        await _seed(db, b, [1, 2])
        await _seed(db, c, [2, 3])
        n = await ops.andor(out, a, b, c)
        assert n == 1
        # dest = a AND (b OR c) = 1001 AND (0110 OR 0011) = 1001 AND 0111 = 0001
        assert await _read(db, out) == [0, 0, 0, 1, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_andor_requires_two_keys(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, out = f"{key_prefix}:a", f"{key_prefix}:d"
        with pytest.raises(ValueError, match="two source keys"):
            await ops.andor(out, a)


class TestOne:
    @py_test_mark_asyncio
    async def test_one_three_keys(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, c, out = (
            f"{key_prefix}:a",
            f"{key_prefix}:b",
            f"{key_prefix}:c",
            f"{key_prefix}:d",
        )
        await _seed(db, a, [0, 3])
        await _seed(db, b, [1, 2])
        await _seed(db, c, [2, 3])
        n = await ops.one(out, a, b, c)
        assert n == 1
        # bit 0: only a → 1
        # bit 1: only b → 1
        # bit 2: b+c → 0
        # bit 3: a+c → 0
        assert await _read(db, out) == [1, 1, 0, 0, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_one_two_keys_equals_xor(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 3])
        await _seed(db, b, [1, 2])
        n = await ops.one(out, a, b)
        assert n == 1
        # For 2 keys: ONE == XOR = 1001 XOR 0110 = 1111
        assert await _read(db, out) == [1, 1, 1, 1, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_one_requires_two_keys(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, out = f"{key_prefix}:a", f"{key_prefix}:d"
        with pytest.raises(ValueError, match="two source keys"):
            await ops.one(out, a)


class TestLegacyPassthroughs:
    @py_test_mark_asyncio
    async def test_and(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 1, 3])
        await _seed(db, b, [0, 2, 3])
        await ops.and_(out, a, b)
        assert await _read(db, out) == [1, 0, 0, 1, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_or(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 3])
        await _seed(db, b, [1, 2])
        await ops.or_(out, a, b)
        assert await _read(db, out) == [1, 1, 1, 1, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_xor(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, b, out = f"{key_prefix}:a", f"{key_prefix}:b", f"{key_prefix}:d"
        await _seed(db, a, [0, 3])
        await _seed(db, b, [1, 2])
        await ops.xor(out, a, b)
        assert await _read(db, out) == [1, 1, 1, 1, 0, 0, 0, 0]

    @py_test_mark_asyncio
    async def test_not(self, ops, db, key_prefix):
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        a, out = f"{key_prefix}:a", f"{key_prefix}:d"
        await _seed(db, a, [0, 1])
        await ops.not_(out, a)
        # First byte: 11000000 (bits 0,1 cleared, rest set)
        bits = await _read(db, out)
        assert bits[0] == 0
        assert bits[1] == 0
        assert bits[2] == 1
        assert bits[7] == 1


class TestEndToEnd:
    @py_test_mark_asyncio
    async def test_segment_overlap_workflow(self, ops, db, key_prefix):
        """Realistic use case: user segment bitmaps + targeting.

        ``new_users`` = signed up in last 7 days
        ``premium_users`` = paying customers
        ``target`` = new users who are not yet premium (free trial
        candidates).
        """
        if not await has_bitmap_ops(db):
            pytest.skip("BITOP DIFF requires Redis 8.2+")
        new, prem, target = (
            f"{key_prefix}:new",
            f"{key_prefix}:premium",
            f"{key_prefix}:trial_target",
        )
        # Bit i represents user_id i.
        await _seed(db, new, [0, 1, 2, 3, 4, 5])
        await _seed(db, prem, [0, 2, 5])  # 0, 2, 5 already converted.
        n = await ops.diff(target, new, prem)
        assert n == 1
        # Trial candidates: 1, 3, 4.
        assert await _read(db, target) == [0, 1, 0, 1, 1, 0, 0, 0]
