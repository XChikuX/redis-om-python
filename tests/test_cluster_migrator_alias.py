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


class _ClusterPersonV2(JsonModel):
    name: str = Field(index=True)
    height: int = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = ALIAS
        model_key_prefix = DOC_PREFIX


_ALL_TEST_MODELS: Dict[str, Type] = {}
for _key, _val in list(model_registry.items()):
    if getattr(_val, "__name__", "").startswith("_ClusterPerson"):
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
    """Drop the alias, every related physical index, and document keys."""
    await _drop_alias_quietly(conn, alias)
    for idx in await _ft_list(conn):
        if idx == alias or idx.startswith(f"{alias}__v"):
            await _drop_index_quietly(conn, idx)
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
