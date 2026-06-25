# type: ignore
"""Tests for ClusterAdmin (CLUSTER SLOT-STATS, CLUSTER MIGRATION — Redis 8.2+).

These commands require cluster mode. Tests skip on standalone instances.
"""

import pytest

from aredis_om import get_redis_connection
from aredis_om.model.cluster_admin import (
    ClusterAdmin,
    has_migration,
    has_slot_stats,
    is_cluster_mode,
)

from .conftest import py_test_mark_asyncio


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def admin(db):
    return ClusterAdmin(db)


class TestCapability:
    @py_test_mark_asyncio
    async def test_not_cluster_mode_on_standalone(self, db):
        # On standalone (the default REDIS_OM_URL) this should be False.
        # On cluster it would be True.
        cluster = await is_cluster_mode(db)
        # We can't assert False universally since CI may run on cluster.
        assert isinstance(cluster, bool)


class TestSlotStats:
    @py_test_mark_asyncio
    async def test_slot_stats_requires_cluster(self, admin, db):
        if not await is_cluster_mode(db):
            pytest.skip("CLUSTER SLOT-STATS requires cluster mode")
        if not await has_slot_stats(db):
            pytest.skip("CLUSTER SLOT-STATS requires Redis 8.2+ cluster")
        stats = await admin.slot_stats()
        assert isinstance(stats, list)


class TestMigration:
    @py_test_mark_asyncio
    async def test_migration_status_shape(self, admin, db):
        if not await is_cluster_mode(db):
            pytest.skip("CLUSTER MIGRATION requires cluster mode")
        if not await has_migration(db):
            pytest.skip("CLUSTER MIGRATION requires Redis 8.2+ cluster")
        status = await admin.migration_status()
        # Either None or a dict — server-dependent.
        assert status is None or isinstance(status, dict)

    @py_test_mark_asyncio
    async def test_migration_log_returns_list(self, admin, db):
        if not await is_cluster_mode(db):
            pytest.skip("CLUSTER MIGRATION requires cluster mode")
        if not await has_migration(db):
            pytest.skip("CLUSTER MIGRATION requires Redis 8.2+ cluster")
        log = await admin.migration_log(count=5)
        assert isinstance(log, list)
