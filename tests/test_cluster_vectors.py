# type: ignore
"""Cluster tests for vector fields on HashModel and JsonModel.

Verifies the raw-blob vector path against a 6-node Redis cluster
(3 masters, 3 replicas): FT.CREATE with VECTOR attributes, save/load
round-trips over text and binary cluster connections, KNN hydration
by PK across slots, bulk fan-out, and a JsonModel regression that
vectors are NOT packed to bytes (commit 59c4c65).

Prerequisites: 6-node Redis cluster on ports 7001-7006.
"""

import abc
import asyncio
import struct
import time
from typing import Optional

import pytest
import pytest_asyncio
import redis as sync_redis
import redis.asyncio as aioredis

from aredis_om import (
    Field,
    HashModel,
    JsonModel,
    KNNExpression,
    Migrator,
    VectorFieldOptions,
)
from aredis_om.model.model import model_registry

from .conftest import py_test_mark_asyncio

# Shares the cluster with test_cluster_operations.py: same xdist group so
# both files run serialized on a single worker.
pytestmark = pytest.mark.xdist_group(name="cluster")

CLUSTER_PORT = 7001
VECTOR_PREFIX = "cluster-vec"


def cluster_available():
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


def _f16_vector_opts(dimension: int = 4) -> VectorFieldOptions:
    return VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT16,
        dimension=dimension,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )


def _f32_vector_opts(dimension: int = 4) -> VectorFieldOptions:
    return VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=dimension,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )


def _knn(model, k: int, reference: bytes) -> KNNExpression:
    return KNNExpression(
        k=k,
        vector_field=model.embedding,
        score_field=model.embedding_score,
        reference_vector=reference,
    )


async def _until(func, predicate, timeout: float = 5.0, interval: float = 0.1):
    """Poll async func() until predicate passes — Redis 8 background indexing
    can delay visibility of just-added docs."""
    deadline = time.monotonic() + timeout
    result = None
    while time.monotonic() < deadline:
        result = await func()
        if predicate(result):
            return result
        await asyncio.sleep(interval)
    return result


async def _cleanup(conn):
    keys = [key async for key in conn.scan_iter(f"{VECTOR_PREFIX}:*")]
    if keys:
        await conn.delete(*keys)


@pytest_asyncio.fixture
async def cluster_conn():
    conn = aioredis.RedisCluster(
        host="localhost", port=CLUSTER_PORT, decode_responses=True
    )
    yield conn
    await conn.aclose()


@pytest_asyncio.fixture
async def binary_cluster_conn():
    conn = aioredis.RedisCluster(
        host="localhost", port=CLUSTER_PORT, decode_responses=False
    )
    yield conn
    await conn.aclose()


@pytest_asyncio.fixture
async def cluster_bytes_vector_model(binary_cluster_conn, cluster_conn):
    """bytes vector field on a binary (decode_responses=False) cluster conn."""
    model_registry.clear()

    class BaseHash(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = VECTOR_PREFIX
            database = binary_cluster_conn

    class Chunk(BaseHash):
        content: str = Field(index=True)
        embedding: bytes = Field(vector_options=_f16_vector_opts(dimension=4))
        embedding_score: Optional[float] = Field(None, index=False)

        class Meta:
            model_key_prefix = "cv_bytes_chunk"

    # Migrator on the text conn: avoids bytes keys in _wait_for_index.
    await Migrator(conn=cluster_conn).run()

    yield Chunk

    await _cleanup(cluster_conn)


@pytest_asyncio.fixture
async def cluster_list_vector_model(cluster_conn):
    """list[float] vector field on a text cluster conn — exercises the
    latin-1 str fallback on load."""
    model_registry.clear()

    class BaseHash(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = VECTOR_PREFIX
            database = cluster_conn

    class Chunk(BaseHash):
        content: str = Field(index=True)
        embedding: list[float] = Field(vector_options=_f16_vector_opts(dimension=4))
        embedding_score: Optional[float] = Field(None, index=False)

        class Meta:
            model_key_prefix = "cv_list_chunk"

    await Migrator(conn=cluster_conn).run()

    yield Chunk

    await _cleanup(cluster_conn)


@pytest_asyncio.fixture
async def cluster_json_vector_model(cluster_conn):
    """JsonModel vector field — regression that JsonModel does NOT pack
    vectors to bytes."""
    model_registry.clear()

    class BaseJson(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = VECTOR_PREFIX
            database = cluster_conn

    class Doc(BaseJson):
        content: str = Field(index=True)
        embedding: list[float] = Field(vector_options=_f32_vector_opts(dimension=4))
        embedding_score: Optional[float] = Field(None, index=False)

        class Meta:
            model_key_prefix = "cv_json_doc"

    await Migrator(conn=cluster_conn).run()

    yield Doc

    await _cleanup(cluster_conn)


# ── bytes vector fields ─────────────────────────────────────────────


@py_test_mark_asyncio
async def test_cluster_bytes_vector_save_get_roundtrip(
    cluster_bytes_vector_model,
):
    Chunk = cluster_bytes_vector_model
    blob = struct.pack("<4e", 1.0, 2.0, 3.0, 4.0)

    chunk = await Chunk(content="hello", embedding=blob).save()
    got = await Chunk.get(chunk.pk)

    assert got.embedding == blob
    assert got.content == "hello"


@py_test_mark_asyncio
async def test_cluster_bytes_vector_stored_as_raw_blob(
    cluster_bytes_vector_model, binary_cluster_conn
):
    Chunk = cluster_bytes_vector_model
    blob = struct.pack("<4e", 1.0, 2.0, 3.0, 4.0)

    chunk = await Chunk(content="raw", embedding=blob).save()

    # 4 dims * 2 bytes (FLOAT16) — no base64 or JSON wrapping.
    assert await binary_cluster_conn.hstrlen(chunk.key(), "embedding") == 8


@py_test_mark_asyncio
async def test_cluster_bytes_vector_knn(cluster_bytes_vector_model):
    Chunk = cluster_bytes_vector_model
    ref = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)

    await Chunk.add(
        [
            Chunk(content="near", embedding=ref),
            Chunk(content="mid", embedding=struct.pack("<4e", 0.5, 0.5, 0.0, 0.0)),
            Chunk(content="far", embedding=struct.pack("<4e", -1.0, 0.0, 0.0, 0.0)),
        ]
    )

    results = await _until(
        lambda: Chunk.find(knn=_knn(Chunk, 3, ref)).all(),
        lambda rs: len(rs) >= 3,
    )

    assert [r.content for r in results] == ["near", "mid", "far"]
    scores = [r.embedding_score for r in results]
    assert all(s is not None for s in scores)
    assert scores == sorted(scores)  # ascending: nearest first
    assert scores[0] == pytest.approx(0.0, abs=1e-2)
    assert scores[1] == pytest.approx(0.2929, abs=1e-2)
    assert scores[2] == pytest.approx(2.0, abs=1e-2)
    # Hydrated model carries the full doc — the blob survives byte-for-byte.
    assert results[0].embedding == ref


@py_test_mark_asyncio
async def test_cluster_bytes_vector_knn_with_filter(cluster_bytes_vector_model):
    Chunk = cluster_bytes_vector_model
    ref = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)

    await Chunk.add(
        [
            Chunk(content="py", embedding=ref),
            Chunk(content="rs", embedding=struct.pack("<4e", 0.9, 0.1, 0.0, 0.0)),
        ]
    )

    results = await _until(
        lambda: Chunk.find(Chunk.content == "py", knn=_knn(Chunk, 2, ref)).all(),
        lambda rs: len(rs) >= 1,
    )

    assert len(results) == 1
    assert results[0].content == "py"
    assert results[0].embedding_score is not None


@py_test_mark_asyncio
async def test_cluster_bytes_vector_plain_find_hydrates_blobs(
    cluster_bytes_vector_model,
):
    Chunk = cluster_bytes_vector_model
    blob_a = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)
    blob_b = struct.pack("<4e", 0.0, 1.0, 0.0, 0.0)

    await Chunk.add(
        [
            Chunk(content="alpha", embedding=blob_a),
            Chunk(content="beta", embedding=blob_b),
        ]
    )

    results = await _until(
        lambda: Chunk.find(Chunk.content == "alpha").all(),
        lambda rs: len(rs) >= 1,
    )

    assert len(results) == 1
    assert results[0].embedding == blob_a


@py_test_mark_asyncio
async def test_cluster_bytes_vector_get_many(cluster_bytes_vector_model):
    Chunk = cluster_bytes_vector_model
    blob = struct.pack("<4e", 0.25, 0.5, 0.75, 1.0)

    c1 = await Chunk(content="one", embedding=blob).save()
    c2 = await Chunk(content="two", embedding=blob).save()

    docs = await Chunk.get_many([c1.pk, c2.pk])
    assert len(docs) == 2
    assert all(d.embedding == blob for d in docs)


@py_test_mark_asyncio
async def test_cluster_bytes_vector_bulk_knn_across_slots(
    cluster_bytes_vector_model,
):
    Chunk = cluster_bytes_vector_model
    ref = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)

    # Random ULID pks spread the keys across all hash slots.
    chunks = [Chunk(content=f"chunk_{i}", embedding=ref) for i in range(50)]
    await Chunk.add(chunks)

    results = await _until(
        lambda: Chunk.find(knn=_knn(Chunk, 50, ref)).all(),
        lambda rs: len(rs) >= 50,
        timeout=15.0,
    )

    assert len(results) == 50
    assert all(r.embedding == ref for r in results)
    assert all(r.embedding_score is not None for r in results)


# ── list[float] vector fields ───────────────────────────────────────


@py_test_mark_asyncio
async def test_cluster_list_vector_save_get_roundtrip(
    cluster_list_vector_model, cluster_conn
):
    Chunk = cluster_list_vector_model
    original = [1.0, 2.0, 3.0, 4.0]

    c = await Chunk(content="hello", embedding=original).save()

    # The hash field holds packed FLOAT16 bytes, not a JSON array.
    assert await cluster_conn.hstrlen(c.key(), "embedding") == 8

    got = await Chunk.get(c.pk)
    assert got.embedding == pytest.approx(original, abs=1e-3)
    assert got.content == "hello"


@py_test_mark_asyncio
async def test_cluster_list_vector_knn(cluster_list_vector_model):
    Chunk = cluster_list_vector_model
    ref = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)

    # All-positive floats: FLOAT16 sign bits produce bytes >= 0x80 that a
    # decode_responses=True conn cannot transport (parser-level UTF-8
    # decode fails before the latin-1 fallback runs). "far" is orthogonal
    # to ref instead of negated — cosine distance 1.0 vs 2.0, still last.
    await Chunk.add(
        [
            Chunk(content="near", embedding=[1.0, 0.0, 0.0, 0.0]),
            Chunk(content="mid", embedding=[0.5, 0.5, 0.0, 0.0]),
            Chunk(content="far", embedding=[0.0, 1.0, 0.0, 0.0]),
        ]
    )

    results = await _until(
        lambda: Chunk.find(knn=_knn(Chunk, 3, ref)).all(),
        lambda rs: len(rs) >= 3,
    )

    assert [r.content for r in results] == ["near", "mid", "far"]
    scores = [r.embedding_score for r in results]
    assert all(s is not None for s in scores)
    assert scores == sorted(scores)
    assert isinstance(results[0].embedding, list)
    assert results[0].embedding[0] == pytest.approx(1.0, abs=1e-3)


@py_test_mark_asyncio
async def test_cluster_list_vector_knn_with_filter(cluster_list_vector_model):
    Chunk = cluster_list_vector_model
    ref = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)

    await Chunk.add(
        [
            Chunk(content="py", embedding=[1.0, 0.0, 0.0, 0.0]),
            Chunk(content="rs", embedding=[0.9, 0.1, 0.0, 0.0]),
        ]
    )

    results = await _until(
        lambda: Chunk.find(Chunk.content == "py", knn=_knn(Chunk, 2, ref)).all(),
        lambda rs: len(rs) >= 1,
    )

    assert len(results) == 1
    assert results[0].content == "py"
    assert results[0].embedding_score is not None


@py_test_mark_asyncio
async def test_cluster_list_vector_get_many(cluster_list_vector_model):
    Chunk = cluster_list_vector_model

    c1 = await Chunk(content="one", embedding=[1.0, 2.0, 3.0, 4.0]).save()
    c2 = await Chunk(content="two", embedding=[0.5, 0.5, 0.5, 0.5]).save()

    docs = await Chunk.get_many([c1.pk, c2.pk])
    assert len(docs) == 2
    by_content = {d.content: d.embedding for d in docs}
    assert by_content["one"] == pytest.approx([1.0, 2.0, 3.0, 4.0], abs=1e-3)
    assert by_content["two"] == pytest.approx([0.5, 0.5, 0.5, 0.5], abs=1e-3)


# ── JsonModel regression ────────────────────────────────────────────


@py_test_mark_asyncio
async def test_cluster_json_vector_knn(cluster_json_vector_model):
    """JsonModel keeps float arrays as JSON — NOT packed to bytes (59c4c65)."""
    Doc = cluster_json_vector_model
    ref = struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)

    await Doc.add(
        [
            Doc(content="near", embedding=[1.0, 0.0, 0.0, 0.0]),
            Doc(content="far", embedding=[-1.0, 0.0, 0.0, 0.0]),
        ]
    )

    results = await _until(
        lambda: Doc.find(knn=_knn(Doc, 2, ref)).all(),
        lambda rs: len(rs) >= 2,
    )

    assert len(results) == 2
    assert all(r.embedding_score is not None for r in results)
    assert results[0].content == "near"
    # If JsonModel packed to bytes this would be a str / ValidationError.
    assert isinstance(results[0].embedding, list)
    assert results[0].embedding[0] == pytest.approx(1.0, abs=1e-6)
