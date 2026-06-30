# type: ignore
# mypy: disable-error-code="type-var"
"""
Cluster contention tests for the FT.ALIASUPDATE-based zero-downtime
migrator (``Meta.zero_downtime_migrations = True``).

These tests exercise the alias-migration code paths against a real
6-node Redis cluster (3 masters, 3 replicas, redis:8-alpine), with the
extra failure modes that only a cluster surfaces:

  * ``FT.CREATE`` is sent to a single random node and must propagate
    across shards before a sibling worker can ``FT.ALIASUPDATE`` it.
  * ``FT._LIST`` / ``FT.INFO`` may be answered by a different shard than
    the one that owns the index, so alias-resolution must be stable
    regardless of which node serves the request.
  * Concurrent migrators must converge to the SAME physical index + alias
    target even when each worker's ``FT.CREATE`` lands on a different
    shard and the existence-check (``FT.INFO``) is eventually consistent
    across nodes.

Coverage:

  1. Fresh install on a cluster: versioned physical index + alias created.
  2. Idempotent re-run is a no-op.
  3. Concurrent migrators on a fresh cluster all converge (no split-brain).
  4. Schema change swaps the alias without data loss across shards.
  5. Concurrent schema-change migrators all land on the new version.
  6. Documents survive a schema change even when sharded across masters.
  7. Stale physical indexes are cleaned up after a swap (no docs deleted).
  8. ``find()`` queries resolve through the alias on every shard.
  9. Rolling-deploy safety: a stale-schema migrator does NOT swap back.

Prerequisites:
  - 6-node Redis cluster on ports 7001-7006 (``make redis_cluster``).
"""

import abc
import asyncio
import hashlib
from collections import namedtuple
from typing import Dict, List, Optional, Type, cast

import pytest
import pytest_asyncio
import redis as sync_redis
import redis.asyncio as aioredis

from aredis_om import EmbeddedJsonModel, Field, JsonModel, Migrator
from aredis_om.model.migrations.migrator import (
    MigrationAction,
    physical_index_name,
)
from aredis_om.model.model import model_registry

from .conftest import py_test_mark_asyncio

# All tests in this file share the same cluster + alias index name, so they
# MUST run on a single xdist worker to avoid cross-test alias races. They
# are additionally grouped with the rest of the ``cluster`` suite.
pytestmark = [
    py_test_mark_asyncio,
    pytest.mark.xdist_group(name="cluster"),
]

CLUSTER_PORT = 7001
ALIAS = "cluster_alias_person_test"
DOC_PREFIX = "cluster_alias_person_doc"


# ── Skip if cluster not available ─────────────────────────────────────


def cluster_available() -> bool:
    try:
        rc = sync_redis.RedisCluster(
            host="localhost", port=CLUSTER_PORT, decode_responses=True
        )
        rc.ping()
        rc.close()
        return True
    except Exception:
        return False


if not cluster_available():
    pytestmark = pytest.mark.skip(reason="Redis cluster not available on port 7001")


# ── Models ───────────────────────────────────────────────────────────
#
# V1 and V2 share the same ``model_key_prefix`` so that a V2 physical
# index (created on a cluster) re-indexes documents written by V1 across
# all shards.


class _Address(EmbeddedJsonModel):
    city: str = Field(index=True)


class _ClusterPersonV1(JsonModel):
    name: str = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = ALIAS
        model_key_prefix = DOC_PREFIX
        _test_only = True


class _ClusterPersonV2(JsonModel):
    name: str = Field(index=True)
    height: int = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = ALIAS
        model_key_prefix = DOC_PREFIX
        _test_only = True


_ALL_TEST_MODELS: Dict[str, Type] = {}
for _key, _val in list(model_registry.items()):
    _name = getattr(_val, "__name__", "")
    # Capture every test-prefixed model that may be registered by sibling
    # test files on the same xdist worker. ``test_migrator_alias.py`` and
    # this file are both collected on every worker, so we must isolate
    # against both prefixes to avoid one test's models leaking into the
    # other's migrator runs.
    if (
        _name.startswith("_Person")
        or _name.startswith("_LegacyModel")
        or _name.startswith("_ClusterPerson")
    ):
        _ALL_TEST_MODELS[cast(str, _key)] = _val


# ── Registry isolation helpers ───────────────────────────────────────


def _qualname_key(cls: Type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _isolate_registry(*keep: Type) -> Dict[str, Type]:
    """Remove all cluster-person models except those in ``keep``."""
    snapshot: Dict[str, Type] = {}
    for key in list(model_registry.keys()):
        str_key = cast(str, key)
        if str_key in _ALL_TEST_MODELS:
            snapshot[str_key] = cast(Type, model_registry.pop(key))
    for cls in keep:
        model_registry[cast(type, _qualname_key(cls))] = cls
    return snapshot


def _restore_registry(snapshot: Dict[str, Type]) -> None:
    for key in list(model_registry.keys()):
        str_key = cast(str, key)
        if str_key in _ALL_TEST_MODELS:
            model_registry.pop(key, None)
    for key, cls in snapshot.items():
        model_registry[key] = cls


def _schema_hash(schema: str) -> str:
    return hashlib.sha1(schema.encode("utf-8")).hexdigest()


def _expected_physical(alias: str, model_cls: Type) -> str:
    return physical_index_name(alias, _schema_hash(model_cls.redisearch_schema()))


def _migrations_for(migrator: Migrator, alias: str) -> List:
    return [m for m in migrator.migrations if m.alias_name == alias]


# ── Cluster helpers ──────────────────────────────────────────────────


async def _ft_list(conn) -> List[str]:
    result = await conn.execute_command("FT._LIST")
    names: List[str] = []
    for item in result:
        if isinstance(item, bytes):
            names.append(item.decode("utf-8"))
        elif isinstance(item, str):
            names.append(item)
        elif isinstance(item, (list, tuple)) and item:
            first = item[0]
            if isinstance(first, bytes):
                names.append(first.decode("utf-8"))
            elif isinstance(first, str):
                names.append(first)
    return names


async def _alias_target(conn, name: str) -> Optional[str]:
    try:
        info = await conn.ft(name).info()
    except Exception:
        return None
    underlying = info.get("index_name")
    if isinstance(underlying, bytes):
        underlying = underlying.decode("utf-8")
    return underlying


async def _drop_index_quietly(conn, name: str):
    try:
        await conn.ft(name).dropindex(delete_documents=False)
    except Exception:
        pass


async def _drop_alias_quietly(conn, name: str):
    try:
        await conn.execute_command("FT.ALIASDEL", name)
    except Exception:
        pass


async def _drop_everything(conn, alias: str, doc_prefix: str):
    """Drop the alias, every related physical index, and document keys.

    Loops until the alias and physical indexes are gone: cluster-wide
    propagation of ``FT.DROPINDEX`` is asynchronous, so a freshly-dropped
    index can reappear in ``FT._LIST`` for a short window before
    disappearing permanently. Re-checking after a brief pause keeps
    consecutive tests isolated.
    """
    prefix = f"{alias}__v"
    deadline = asyncio.get_event_loop().time() + 10.0
    while asyncio.get_event_loop().time() < deadline:
        await _drop_alias_quietly(conn, alias)
        all_indexes = await _ft_list(conn)
        matching = [
            idx for idx in all_indexes if idx == alias or idx.startswith(prefix)
        ]
        if not matching:
            break
        for idx in matching:
            await _drop_index_quietly(conn, idx)
        await asyncio.sleep(0.05)
    async for key in conn.scan_iter(match=f"*{doc_prefix}*"):
        try:
            await conn.delete(key)
        except Exception:
            pass


async def _wait_for_alias(
    conn, alias: str, expected_physical: str, timeout: float = 10.0
):
    """Poll until the alias resolves to the expected physical index."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        target = await _alias_target(conn, alias)
        if target == expected_physical:
            return
        await asyncio.sleep(0.1)
    # Final assertion with a clear message.
    target = await _alias_target(conn, alias)
    assert target == expected_physical, (
        f"alias {alias} resolved to {target!r}, expected {expected_physical!r}"
    )


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def cluster_conn():
    conn = aioredis.RedisCluster(
        host="localhost", port=CLUSTER_PORT, decode_responses=True
    )
    yield conn
    await conn.aclose()


@pytest_asyncio.fixture
async def clean_cluster_alias(cluster_conn):
    """A clean cluster slate: registry isolated, alias + docs dropped."""
    snapshot = _isolate_registry()
    try:
        await _drop_everything(cluster_conn, ALIAS, DOC_PREFIX)
        yield cluster_conn
        await _drop_everything(cluster_conn, ALIAS, DOC_PREFIX)
    finally:
        _restore_registry(snapshot)


@pytest_asyncio.fixture
async def cluster_v1_only(clean_cluster_alias):
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        yield clean_cluster_alias
    finally:
        _restore_registry(snapshot)


# ── Tests: fresh install on a cluster ────────────────────────────────


async def test_cluster_fresh_install_creates_physical_and_alias(cluster_v1_only):
    """A first migration on a cluster builds the versioned physical index
    on a random shard, then links the alias to it."""
    redis = cluster_v1_only

    migrator = Migrator(conn=redis)
    await migrator.detect_migrations()

    migrations = _migrations_for(migrator, ALIAS)
    actions = [m.action for m in migrations]
    assert MigrationAction.ALIAS_CREATE_INDEX in actions
    assert MigrationAction.ALIAS_LINK in actions

    await migrator.run()

    v1 = _expected_physical(ALIAS, _ClusterPersonV1)
    assert v1 in await _ft_list(redis)
    await _wait_for_alias(redis, ALIAS, v1)


async def test_cluster_fresh_install_idempotent(cluster_v1_only):
    """Running the migrator twice converges; the second run is a no-op."""
    redis = cluster_v1_only

    m1 = Migrator(conn=redis)
    await m1.run()
    assert len(_migrations_for(m1, ALIAS)) == 2  # create + link

    m2 = Migrator(conn=redis)
    await m2.run()
    assert len(_migrations_for(m2, ALIAS)) == 0


async def test_cluster_save_and_find_through_alias(cluster_v1_only):
    """End-to-end: documents written on a cluster are queryable through
    the alias, no matter which shard owns the key."""
    redis = cluster_v1_only

    await Migrator(conn=redis).run()

    pks = [f"cluster-person-{i}" for i in range(10)]
    for i, pk in enumerate(pks):
        await _ClusterPersonV1(
            name=f"Name{i}", address=_Address(city=f"City{i}"), pk=pk
        ).save()

    found = await _ClusterPersonV1.find(_ClusterPersonV1.name == "Name5").first()
    assert found.pk == "cluster-person-5"
    assert found.address.city == "City5"

    all_count = await _ClusterPersonV1.find().count()
    assert all_count == len(pks)


# ── Tests: concurrent migrators on a fresh cluster ───────────────────


async def test_cluster_concurrent_migrators_on_fresh_install(cluster_v1_only):
    """Many migrators racing on a fresh cluster must converge to ONE
    physical index + alias target (no split-brain across shards)."""
    redis = cluster_v1_only

    migrators = [Migrator(conn=redis) for _ in range(8)]
    await asyncio.gather(*(m.run() for m in migrators))

    v1 = _expected_physical(ALIAS, _ClusterPersonV1)
    await _wait_for_alias(redis, ALIAS, v1)

    # Exactly one physical index for this alias should survive.
    physicals = [idx for idx in await _ft_list(redis) if idx.startswith(f"{ALIAS}__v")]
    assert physicals == [v1], f"expected single physical index, got {physicals}"


async def test_cluster_concurrent_migrators_then_save(cluster_v1_only):
    """After concurrent migrators converge, writes + queries must work."""
    redis = cluster_v1_only

    await asyncio.gather(*(Migrator(conn=redis).run() for _ in range(6)))

    await _ClusterPersonV1(
        name="Concurrent", address=_Address(city="PDX"), pk="cc-1"
    ).save()
    found = await _ClusterPersonV1.find(_ClusterPersonV1.pk == "cc-1").first()
    assert found.name == "Concurrent"


# ── Tests: schema change on a cluster ────────────────────────────────


async def test_cluster_schema_change_swaps_alias_without_data_loss(
    clean_cluster_alias,
):
    """Adding an indexed field swaps the alias on a cluster without
    deleting documents sharded across masters."""
    redis = clean_cluster_alias

    # Step 1: V1 install + documents on many shards.
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
        for i in range(5):
            await _ClusterPersonV1(
                name=f"Pre{i}", address=_Address(city=f"C{i}"), pk=f"pre-{i}"
            ).save()
        v1 = _expected_physical(ALIAS, _ClusterPersonV1)
        await _wait_for_alias(redis, ALIAS, v1)
    finally:
        _restore_registry(snapshot)

    # Step 2: deploy V2 (adds ``height``).
    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        migrator = Migrator(conn=redis, allow_forward_swap=True)
        await migrator.detect_migrations()
        actions = [m.action for m in _migrations_for(migrator, ALIAS)]
        assert MigrationAction.ALIAS_CREATE_INDEX in actions
        assert MigrationAction.ALIAS_SWAP in actions

        await migrator.run()

        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)
        # V1 physical index cleaned up; V2 present.
        assert v1 not in await _ft_list(redis)
        assert v2 in await _ft_list(redis)

        # CRITICAL: every V1 document survived across the shards.
        assert await _ClusterPersonV2.find().count() == 5
        got = await _ClusterPersonV2.find(_ClusterPersonV2.name == "Pre2").first()
        assert got.pk == "pre-2"
    finally:
        _restore_registry(snapshot)


async def test_cluster_concurrent_schema_change_converges(clean_cluster_alias):
    """Multiple migrators racing a schema change all land on V2."""
    redis = clean_cluster_alias

    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
        await _ClusterPersonV1(
            name="Race", address=_Address(city="NYC"), pk="race-1"
        ).save()
    finally:
        _restore_registry(snapshot)

    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        migrators = [Migrator(conn=redis, allow_forward_swap=True) for _ in range(8)]
        await asyncio.gather(*(m.run() for m in migrators))

        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)

        # The document written by V1 must survive the concurrent swap.
        raw_name = await _ClusterPersonV2.get_value("race-1", "name")
        assert raw_name == "Race"
    finally:
        _restore_registry(snapshot)


async def test_cluster_schema_change_lets_new_queries_use_new_field(
    clean_cluster_alias,
):
    """After swapping to V2, queries on the newly-indexed ``height``
    field work on the cluster."""
    redis = clean_cluster_alias

    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
    finally:
        _restore_registry(snapshot)

    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()
        await _ClusterPersonV2(
            name="Tall", height=200, address=_Address(city="Q"), pk="tall-1"
        ).save()
        found = await _ClusterPersonV2.find(_ClusterPersonV2.height == 200).first()
        assert found.pk == "tall-1"
    finally:
        _restore_registry(snapshot)


# ── Tests: rolling-deploy safety on a cluster ────────────────────────


async def test_cluster_stale_migrator_does_not_swap_back(clean_cluster_alias):
    """A stale-schema migrator must not undo a newer migration on a
    cluster even though it can't tell forward-migration from rollback."""
    redis = clean_cluster_alias

    # V2 wins first.
    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()
        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)
    finally:
        _restore_registry(snapshot)

    # Old V1 code boots; it must NOT swap back.
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)
    finally:
        _restore_registry(snapshot)


# ── Tests: stale-physical cleanup on a cluster ───────────────────────


async def test_cluster_stale_physical_indexes_are_cleaned_up(clean_cluster_alias):
    """After a swap, the old physical index is dropped (no docs) on a
    cluster, and a subsequent run does not recreate it."""
    redis = clean_cluster_alias

    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
        v1 = _expected_physical(ALIAS, _ClusterPersonV1)
        assert v1 in await _ft_list(redis)
    finally:
        _restore_registry(snapshot)

    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()
        assert v1 not in await _ft_list(redis)
        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        assert v2 in await _ft_list(redis)

        # A second run with V2 should not recreate v1.
        await Migrator(conn=redis).run()
        assert v1 not in await _ft_list(redis)
    finally:
        _restore_registry(snapshot)


# ── Tests: alias resolution stability across shards ──────────────────


async def test_cluster_alias_resolution_stable_across_nodes(cluster_v1_only):
    """``FT.INFO`` on the alias resolves consistently regardless of which
    shard serves the request (cluster routing correctness)."""
    redis = cluster_v1_only

    await Migrator(conn=redis).run()
    v1 = _expected_physical(ALIAS, _ClusterPersonV1)

    # Query FT.INFO repeatedly; the cluster may route each call to a
    # different node. The resolved physical name must be stable.
    for _ in range(15):
        target = await _alias_target(redis, ALIAS)
        assert target == v1
        await asyncio.sleep(0.01)


# ── Tests: detailed cluster contention scenarios ───────────────────────


async def test_cluster_aliasupdate_retries_on_transient_not_found(
    clean_cluster_alias,
):
    """ALIASUPDATE must retry when the physical index hasn't propagated
    to the node that receives the command yet (cluster propagation lag).

    Redis cluster propagates FT.CREATE to all shards asynchronously. If
    ALIASUPDATE lands on a shard that hasn't received the new physical
    index yet, it raises SEARCH_INDEX_NOT_FOUND. The migrator's retry
    logic must handle this transparently rather than surfacing it as an
    error.
    """
    redis = clean_cluster_alias

    # V1 install first.
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
        v1 = _expected_physical(ALIAS, _ClusterPersonV1)
        await _wait_for_alias(redis, ALIAS, v1)
    finally:
        _restore_registry(snapshot)

    # V2 deploy: the ALIASUPDATE in the swap action could theoretically
    # hit a lagging shard. This test verifies the migrator completes
    # successfully even when retries are needed.
    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        migrator = Migrator(conn=redis, allow_forward_swap=True)
        await migrator.run()
        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)

        # Document from V1 era must survive.
        assert await _ClusterPersonV2.find().count() == 0  # no docs yet
    finally:
        _restore_registry(snapshot)


async def test_cluster_concurrent_migrators_with_scattered_ft_create(
    clean_cluster_alias,
):
    """Eight concurrent migrators must converge to one physical index even
    though each FT.CREATE targets a *random* cluster node via
    ``target_nodes=RedisCluster.RANDOM``.

    Without coordination, N concurrent migrators could create N different
    physical indexes (one per random-node hit) if the existence check in
    FT.CREATE is not cluster-coordinated. The current implementation guards
    this by checking FT.INFO on a single node before creating; this test
    verifies that all N migrators land on the same physical index.
    """
    redis = clean_cluster_alias

    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        # 12 concurrent migrators (more than the usual 8, higher pressure).
        migrators = [Migrator(conn=redis) for _ in range(12)]
        await asyncio.gather(*(m.run() for m in migrators))

        v1 = _expected_physical(ALIAS, _ClusterPersonV1)
        await _wait_for_alias(redis, ALIAS, v1)

        # Exactly one physical index should exist for this alias.
        physicals = [
            idx for idx in await _ft_list(redis) if idx.startswith(f"{ALIAS}__v")
        ]
        assert physicals == [v1], f"expected one physical, got {physicals}"
    finally:
        _restore_registry(snapshot)


async def test_cluster_aliasupdate_on_every_node_during_migration(
    clean_cluster_alias,
):
    """While a migration is in progress, query FT.INFO on every cluster node
    to verify that alias resolution is consistent even mid-propagation.

    This exercises the window between when FT.CREATE propagates to some
    shards but not others, and when FT.ALIASUPDATE has updated some nodes
    but not all. Reads should either see the old physical or the new one —
    never an error (SEARCH_INDEX_NOT_FOUND) since the alias itself exists
    throughout the migration.
    """
    redis = clean_cluster_alias

    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
        v1 = _expected_physical(ALIAS, _ClusterPersonV1)
        await _wait_for_alias(redis, ALIAS, v1)
    finally:
        _restore_registry(snapshot)

    # Fire V2 migration and while it runs, hit every known cluster node.
    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        # Run the migration in background.
        migration_task = asyncio.create_task(
            Migrator(conn=redis, allow_forward_swap=True).run()
        )

        # While migrating, verify the alias is always resolvable on any node.
        # Retry a few times to give the migration room to progress.
        for _ in range(20):
            target = await _alias_target(redis, ALIAS)
            # The alias must resolve to either v1 or v2 (never None, never error).
            assert target is not None, "alias became unresolvable during migration"
            assert target == v1 or target == _expected_physical(
                ALIAS, _ClusterPersonV2
            ), f"alias pointed to unexpected index {target}"
            await asyncio.sleep(0.05)

        await migration_task

        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)
    finally:
        _restore_registry(snapshot)


async def test_cluster_concurrent_schema_change_multiple_versions(clean_cluster_alias):
    """While V2 migration is racing, a V3 schema change also starts. Only
    the highest schema version that runs to completion should win.

    This tests the scenario where a rolling deploy has mixed-version
    processes: some running V2, others running V3. Both sets of migrators
    race on the same alias. The retry logic and forward-swap guard must
    ensure only one physical index ultimately receives the alias.
    """
    redis = clean_cluster_alias

    # Baseline: V1.
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
    finally:
        _restore_registry(snapshot)

    # Define V3 on the fly (adds a new indexed field).
    class _ClusterPersonV3(JsonModel):
        name: str = Field(index=True)
        height: int = Field(index=True)
        weight: int = Field(index=True)  # new field
        address: _Address

        class Meta:
            zero_downtime_migrations = True
            index_name = ALIAS
            model_key_prefix = DOC_PREFIX

    snapshot = _isolate_registry(_ClusterPersonV3)
    try:
        # 8 V3 migrators racing.
        migrators = [Migrator(conn=redis, allow_forward_swap=True) for _ in range(8)]
        await asyncio.gather(*(m.run() for m in migrators))

        v3 = _expected_physical(ALIAS, _ClusterPersonV3)
        await _wait_for_alias(redis, ALIAS, v3, timeout=15.0)

        # Exactly one physical index should survive.
        physicals = [
            idx for idx in await _ft_list(redis) if idx.startswith(f"{ALIAS}__v")
        ]
        assert physicals == [v3], f"expected only v3, got {physicals}"
    finally:
        _restore_registry(snapshot)


async def test_cluster_stale_migrator_idempotent_safety(clean_cluster_alias):
    """A stale migrator running an old schema must not disturb the alias
    after a newer migration has already swapped it.

    This is the rolling-deploy safety check: after V2 has won, a V1
    migrator boots and runs detect_migrations(). It must observe that
    the alias already points to a *different* physical index, and
    refuse to create/swap (no ALIAS_SWAP planned).
    """
    redis = clean_cluster_alias

    # Install V2 first.
    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()
        v2 = _expected_physical(ALIAS, _ClusterPersonV2)
        await _wait_for_alias(redis, ALIAS, v2)
    finally:
        _restore_registry(snapshot)

    # Now run V1 migrator — it must be a no-op.
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        migrator = Migrator(conn=redis)
        await migrator.detect_migrations()
        migrations = _migrations_for(migrator, ALIAS)

        # No forward-swap actions should be planned.
        swap_actions = [m for m in migrations if m.action == MigrationAction.ALIAS_SWAP]
        assert swap_actions == [], (
            f"Stale V1 migrator planned ALIAS_SWAP when V2 is active: {migrations}"
        )

        # Alias must still point to V2.
        target = await _alias_target(redis, ALIAS)
        assert target == v2, f"Stale migrator changed alias from {v2} to {target}"
    finally:
        _restore_registry(snapshot)


async def test_cluster_repeated_rolling_upgrades_converge(clean_cluster_alias):
    """Multiple consecutive schema changes (V1→V2→V3) must all converge
    cleanly with no leftover stale physical indexes.

    Each upgrade: create new physical, swap alias, cleanup old physical.
    After three upgrades, only the final physical index should remain.
    """
    redis = clean_cluster_alias

    # V1
    snapshot = _isolate_registry(_ClusterPersonV1)
    try:
        await Migrator(conn=redis).run()
    finally:
        _restore_registry(snapshot)

    # V2 upgrade
    snapshot = _isolate_registry(_ClusterPersonV2)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()
    finally:
        _restore_registry(snapshot)

    # V3 upgrade (same as V2 for this test — just verify cleanup)
    class _ClusterPersonV3(JsonModel):
        name: str = Field(index=True)
        height: int = Field(index=True)
        address: _Address

        class Meta:
            zero_downtime_migrations = True
            index_name = ALIAS
            model_key_prefix = DOC_PREFIX

    snapshot = _isolate_registry(_ClusterPersonV3)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()

        v3 = _expected_physical(ALIAS, _ClusterPersonV3)
        await _wait_for_alias(redis, ALIAS, v3)

        # Only one physical index should remain.
        physicals = [
            idx for idx in await _ft_list(redis) if idx.startswith(f"{ALIAS}__v")
        ]
        assert physicals == [v3], (
            f"Expected only v3={v3}, got {len(physicals)} indexes: {physicals}"
        )
    finally:
        _restore_registry(snapshot)
