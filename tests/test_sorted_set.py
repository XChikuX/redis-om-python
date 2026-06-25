# type: ignore
"""Tests for SortedSetOps (AGGREGATE COUNT — Redis 8.8+).

Skips gracefully on Redis < 8.8.
"""

import pytest
import pytest_asyncio

from aredis_om import get_redis_connection
from aredis_om.model.sorted_set import SortedSetOps, has_aggregate_count

from .conftest import py_test_mark_asyncio


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def ops(db):
    return SortedSetOps(db)


@pytest_asyncio.fixture
async def sources(db, key_prefix):
    """Seed three sorted sets:
    z1 = {a:1, b:2, c:3, d:4}
    z2 = {b:2, c:3, e:5}
    z3 = {c:3, d:4, f:6}
    """
    z1, z2, z3 = f"{key_prefix}:z1", f"{key_prefix}:z2", f"{key_prefix}:z3"
    for key, members in [
        (z1, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]),
        (z2, [("b", 2), ("c", 3), ("e", 5)]),
        (z3, [("c", 3), ("d", 4), ("f", 6)]),
    ]:
        await db.delete(key)
        if members:
            await db.zadd(key, dict(members))
    return z1, z2, z3


class TestCapability:
    @py_test_mark_asyncio
    async def test_has_aggregate_count(self, db):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        assert await has_aggregate_count(db) is True


class TestZunionstoreCount:
    @py_test_mark_asyncio
    async def test_writes_counts(self, ops, db, sources, key_prefix):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2, z3 = sources
        dest = f"{key_prefix}:out"
        n = await ops.zunionstore_count(dest, z1, z2, z3)
        # Total distinct elements across all 3 sets: a,b,c,d,e,f = 6
        assert n == 6
        # a only in z1 → count 1
        assert await db.zscore(dest, "a") == 1
        # b in z1, z2 → count 2
        assert await db.zscore(dest, "b") == 2
        # c in z1, z2, z3 → count 3
        assert await db.zscore(dest, "c") == 3

    @py_test_mark_asyncio
    async def test_single_source(self, ops, db, sources, key_prefix):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1 = sources[0]
        dest = f"{key_prefix}:out"
        n = await ops.zunionstore_count(dest, z1)
        assert n == 4
        # Every element in z1 should have score 1.
        for m in ("a", "b", "c", "d"):
            assert await db.zscore(dest, m) == 1


class TestZinterstoreCount:
    @py_test_mark_asyncio
    async def test_writes_counts(self, ops, db, sources, key_prefix):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2, z3 = sources
        dest = f"{key_prefix}:out"
        n = await ops.zinterstore_count(dest, z1, z2, z3)
        # Only c is in all 3 sets.
        assert n == 1
        assert await db.zscore(dest, "c") == 3

    @py_test_mark_asyncio
    async def test_two_set_intersection(self, ops, db, sources, key_prefix):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2 = sources[0], sources[1]
        dest = f"{key_prefix}:out"
        n = await ops.zinterstore_count(dest, z1, z2)
        # b and c are in both z1 and z2.
        assert n == 2
        assert await db.zscore(dest, "b") == 2
        assert await db.zscore(dest, "c") == 2


class TestZunionCountRead:
    @py_test_mark_asyncio
    async def test_returns_members(self, ops, db, sources):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2, z3 = sources
        members = await ops.zunion_count(z1, z2, z3)
        # All 6 distinct members in some order.
        assert sorted(members) == ["a", "b", "c", "d", "e", "f"]

    @py_test_mark_asyncio
    async def test_returns_members_with_scores(self, ops, db, sources):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2, z3 = sources
        out = await ops.zunion_count_with_scores(z1, z2, z3)
        # Convert to a dict for easy comparison.
        counts = {m: c for m, c in out}
        assert counts == {"a": 1, "b": 2, "c": 3, "d": 2, "e": 1, "f": 1}


class TestZinterCountRead:
    @py_test_mark_asyncio
    async def test_intersection(self, ops, db, sources):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2, z3 = sources
        members = await ops.zinter_count(z1, z2, z3)
        # Only c is in all three.
        assert members == ["c"]

    @py_test_mark_asyncio
    async def test_intersection_with_scores(self, ops, db, sources):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1, z2, z3 = sources
        out = await ops.zinter_count_with_scores(z1, z2, z3)
        assert out == [("c", 3)]

    @py_test_mark_asyncio
    async def test_empty_intersection(self, ops, db, sources, key_prefix):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z1 = sources[0]
        empty = f"{key_prefix}:empty"
        await db.delete(empty)
        members = await ops.zinter_count(z1, empty)
        assert members == []


class TestEdgeCases:
    @py_test_mark_asyncio
    async def test_empty_sources_raises(self, ops, db):
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        with pytest.raises(ValueError, match="at least one"):
            await ops.zunion_count()


class TestEndToEnd:
    @py_test_mark_asyncio
    async def test_tag_co_occurrence(self, ops, db, key_prefix):
        """Realistic example: tag bitmaps as sorted sets, count overlaps.

        3 docs; 4 tags; each doc has a score = relevance (we use 1 here).
        z_redis has docs tagged 'redis': d1, d2.
        z_cache has docs tagged 'cache': d2, d3.
        z_perf has docs tagged 'perf': d1.
        """
        if not await has_aggregate_count(db):
            pytest.skip("AGGREGATE COUNT requires Redis 8.8+")
        z_redis = f"{key_prefix}:tag_redis"
        z_cache = f"{key_prefix}:tag_cache"
        z_perf = f"{key_prefix}:tag_perf"
        await db.delete(z_redis, z_cache, z_perf)
        await db.zadd(z_redis, {"d1": 1, "d2": 1})
        await db.zadd(z_cache, {"d2": 1, "d3": 1})
        await db.zadd(z_perf, {"d1": 1})
        # How many tags does each doc have?
        out = await ops.zunion_count_with_scores(z_redis, z_cache, z_perf)
        counts = {m: c for m, c in out}
        assert counts == {"d1": 2, "d2": 2, "d3": 1}
