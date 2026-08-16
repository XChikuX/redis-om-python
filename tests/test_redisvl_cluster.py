# type: ignore
"""End-to-end RedisVL tests against a real Redis Cluster.

Complements ``test_redisvl_integration.py`` (standalone Redis + mocked
cluster routing). Here the cluster is real (``docker-compose.cluster.yml``,
ports 7001-7006): the RedisVL index is created **directly through
redisvl** (``index.create()``) — not via OM's ``Migrator`` — and both
redisvl queries (vector, filter, ``FT.HYBRID``) and OM's own queries run
against it, proving the two engines are interchangeable on a cluster.

Redis 8 search indexes are cluster-aware: ``FT.CREATE`` sent to one node
propagates cluster-wide, which is why redisvl's default-node create —
and ``hybrid_search()``'s default-node ``FT.HYBRID`` pinning — works.

The whole file shares the ``cluster`` xdist group with the rest of the
cluster suite. Skipped when either the optional ``redisvl`` package or a
cluster on port 7001 is unavailable.
"""

import asyncio
import time
import uuid
from typing import List

import pytest
import pytest_asyncio
import redis as sync_redis
import redis.asyncio as aioredis

try:
    from redisvl.query import FilterQuery, HybridQuery, VectorQuery
    from redisvl.query.filter import Num
except ImportError:  # pragma: no cover - dev extras always include redisvl
    pytest.skip("requires the optional redisvl package", allow_module_level=True)

from aredis_om import Field, JsonModel, VectorFieldOptions
from aredis_om.model.migrations.migrator import _list_indexes
from aredis_om.redisvl import get_redisvl_index, hybrid_search

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xdist_group(name="cluster"),
]

CLUSTER_PORT = 7001
DIMENSIONS = 8
INDEXED_TIMEOUT = 10.0  # seconds


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


# ── Helpers ──────────────────────────────────────────────────────────


def _flat_vector_options(dimension: int = DIMENSIONS):
    return VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=dimension,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )


def _documents(Document) -> List:
    def onehot(i):
        return [1.0 if j == i else 0.0 for j in range(DIMENSIONS)]

    return [
        Document(
            title="running shoes",
            body="light running shoes",
            views=100,
            embedding=onehot(0),
        ),
        Document(
            title="hiking boots",
            body="sturdy hiking boots",
            views=50,
            embedding=onehot(1),
        ),
        Document(
            title="dress shoes",
            body="formal dress shoes",
            views=10,
            embedding=onehot(2),
        ),
    ]


async def _wait_indexed(conn, index_name: str, timeout: float = INDEXED_TIMEOUT):
    """Poll FT.INFO until ``percent_indexed`` reaches 1.0.

    Redis 8 indexes existing keys asynchronously, so a query run right
    after ``FT.CREATE`` (or right after saves) can see partial results.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            info = await conn.ft(index_name).info()
        except Exception:
            await asyncio.sleep(0.05)
            continue
        pct = info.get("percent_indexed")
        if pct is not None:
            try:
                if float(pct) >= 1.0:
                    return True
            except (TypeError, ValueError):
                pass
        await asyncio.sleep(0.05)
    return False


async def _has_ft_hybrid(conn) -> bool:
    """Probe the cluster for FT.HYBRID (Redis 8.4+)."""
    try:
        info = await conn.execute_command(
            "COMMAND",
            "INFO",
            "ft.hybrid",
            target_nodes=aioredis.RedisCluster.DEFAULT_NODE,
        )
        return bool(info and all(info))
    except Exception:
        return False


async def _drop_index_quietly(conn, index_name: str) -> None:
    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
    except Exception:
        pass


async def _delete_prefix(conn, prefix: str) -> None:
    """Delete every key under ``prefix``, one at a time.

    On a cluster a multi-key DELETE fails with CROSSSLOT when the keys
    hash to different slots, so each key is deleted individually.
    """
    keys: List[str] = []
    async for key in conn.scan_iter(match=f"{prefix}*"):
        keys.append(key)
    for key in keys:
        await conn.delete(key)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def cluster_env():
    """A model + RedisVL index wired to the real cluster, cleaned up after.

    The index lifecycle stays with the tests: each test creates it via
    ``index.create()`` (redisvl's cluster path) against a unique index
    name / key prefix, and the teardown drops the index and the keys.
    """
    conn = aioredis.RedisCluster(
        host="localhost", port=CLUSTER_PORT, decode_responses=True
    )
    # redis-py's async cluster initializes its node table lazily on the
    # first command; redisvl's cluster ``FT.INFO`` path reads the node
    # table directly (``get_random_node()``), so warm the client up now.
    await conn.ping()
    tag = uuid.uuid4().hex[:8]
    global_prefix = f"redisvl_cluster:{tag}"
    idx_name = f"redisvl_cluster_{tag}"

    class BaseJsonModel(JsonModel):
        class Meta:
            global_key_prefix = global_prefix
            database = conn

    class Document(BaseJsonModel, index=True):
        title: str = Field(index=True)
        body: str = Field(full_text_search=True)
        views: int = Field(index=True, sortable=True)
        embedding: List[float] = Field(vector_options=_flat_vector_options())

        class Meta:
            index_name = idx_name
            model_key_prefix = "doc"
            _test_only = True

    index = get_redisvl_index(Document)
    yield Document, index, conn, idx_name, global_prefix

    await _drop_index_quietly(conn, idx_name)
    await _delete_prefix(conn, f"{global_prefix}:")
    await conn.aclose()


# ── Tests ────────────────────────────────────────────────────────────


async def test_create_index_directly_on_cluster(cluster_env):
    """``get_redisvl_index`` + ``index.create()`` builds the index on the cluster."""
    Document, index, conn, index_name, global_prefix = cluster_env

    # The generated schema mirrors OM's index layout: same name + prefix.
    assert index.schema.index.name == index_name
    assert index.schema.index.prefix.startswith(global_prefix)

    await index.create()
    assert index_name in await _list_indexes(conn)
    assert await _wait_indexed(conn, index_name)


async def test_vector_query_on_cluster(cluster_env):
    """VectorQuery returns the nearest OM-saved documents on a cluster."""
    Document, index, conn, index_name, _ = cluster_env

    await index.create()
    docs = _documents(Document)
    for doc in docs:
        await doc.save()
    assert await _wait_indexed(conn, index_name)

    results = await index.query(
        VectorQuery(
            vector=[1.0] + [0.0] * (DIMENSIONS - 1),
            vector_field_name="embedding",
            num_results=2,
        )
    )
    assert len(results) == 2
    assert results[0]["id"] == docs[0].key()


async def test_filter_query_on_cluster(cluster_env):
    """FilterQuery filters on a NUMERIC field through redisvl's cluster path."""
    Document, index, conn, index_name, _ = cluster_env

    await index.create()
    for doc in _documents(Document):
        await doc.save()
    assert await _wait_indexed(conn, index_name)

    results = await index.query(
        FilterQuery(
            filter_expression=Num("views") == 100,
            return_fields=["title"],
            num_results=10,
        )
    )
    assert len(results) == 1
    assert results[0]["title"] == "running shoes"


async def test_hybrid_search_on_cluster(cluster_env):
    """``hybrid_search()`` routes FT.HYBRID correctly on a real cluster.

    This exercises the default-node pinning for real — the unit tests in
    ``test_redisvl_integration.py`` only cover it with a mocked client.
    """
    Document, index, conn, index_name, _ = cluster_env
    if not await _has_ft_hybrid(conn):
        pytest.skip("FT.HYBRID requires Redis 8.4+")

    await index.create()
    for doc in _documents(Document):
        await doc.save()
    assert await _wait_indexed(conn, index_name)

    results = await hybrid_search(
        index,
        HybridQuery(
            text="running",
            text_field_name="body_fts",
            vector=[1.0] + [0.0] * (DIMENSIONS - 1),
            vector_field_name="embedding",
            combination_method="LINEAR",
            linear_alpha=0.5,
            num_results=3,
        ),
    )
    assert isinstance(results, list)
    assert len(results) >= 1


async def test_om_queries_against_redisvl_created_index(cluster_env):
    """OM's own query path (FT.SEARCH) resolves the redisvl-created index."""
    Document, index, conn, index_name, _ = cluster_env

    await index.create()
    docs = _documents(Document)
    for doc in docs:
        await doc.save()
    assert await _wait_indexed(conn, index_name)

    by_views = await Document.find(Document.views == 100).all()
    assert [m.pk for m in by_views] == [docs[0].pk]

    by_pk = await Document.find(Document.pk == docs[1].pk).all()
    assert [m.pk for m in by_pk] == [docs[1].pk]


async def test_index_scan_picks_up_existing_documents(cluster_env):
    """Documents saved BEFORE ``index.create()`` are picked up by the scan."""
    Document, index, conn, index_name, _ = cluster_env

    docs = _documents(Document)
    for doc in docs:
        await doc.save()

    await index.create()
    assert await _wait_indexed(conn, index_name)

    results = await index.query(
        VectorQuery(
            vector=[0.0, 1.0] + [0.0] * (DIMENSIONS - 2),
            vector_field_name="embedding",
            num_results=1,
        )
    )
    assert len(results) == 1
    assert results[0]["id"] == docs[1].key()
