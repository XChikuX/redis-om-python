# type: ignore
"""SVS-VAMANA vector field tests (U8).

Covers the schema rendering (golden string, no server), the redisvl mapping
(``to_redisvl_schema`` produces a schema redisvl accepts), and a
server-gated end-to-end check on a Redis 8.2+ deployment (training
threshold >= 1024 enforced by FT.CREATE).
"""

import abc
from typing import List

import pytest
import pytest_asyncio

from aredis_om import Field, HashModel, VectorFieldOptions
from aredis_om.redisvl import to_redisvl_schema

from .conftest import py_test_mark_asyncio


DIM = 8


def test_svs_factory_validates_dtype():
    """``svs()`` rejects unsupported types (SVS-VAMANA is FLOAT16/FLOAT32)."""
    with pytest.raises(ValueError, match="FLOAT16 and FLOAT32"):
        VectorFieldOptions.svs(
            type=VectorFieldOptions.TYPE.INT8,
            dimension=DIM,
            distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        )
    with pytest.raises(ValueError, match="FLOAT16 and FLOAT32"):
        VectorFieldOptions.svs(
            type=VectorFieldOptions.TYPE.FLOAT64,
            dimension=DIM,
            distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        )
    # Sanity: FLOAT32 and FLOAT16 are accepted.
    VectorFieldOptions.svs(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=DIM,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )
    VectorFieldOptions.svs(
        type=VectorFieldOptions.TYPE.FLOAT16,
        dimension=DIM,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )


def test_svs_schema_renders_golden_string():
    """The .schema property renders the exact RediSearch VECTOR SVS-VAMANA clause."""
    opts = VectorFieldOptions.svs(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=DIM,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        compression="LVQ8",
        graph_max_degree=40,
        construction_window_size=200,
    )
    schema = opts.schema
    # The token count (N) = count of attribute pairs. All non-None SVS attrs
    # above (TYPE, DIM, DISTANCE_METRIC, COMPRESSION, GRAPH_MAX_DEGREE,
    # CONSTRUCTION_WINDOW_SIZE) contribute 6 attributes = 12 tokens.
    assert schema.startswith("VECTOR SVS-VAMANA 12 ")
    assert "TYPE FLOAT32" in schema
    assert f"DIM {DIM}" in schema
    assert "DISTANCE_METRIC COSINE" in schema
    assert "COMPRESSION LVQ8" in schema
    assert "GRAPH_MAX_DEGREE 40" in schema
    assert "CONSTRUCTION_WINDOW_SIZE 200" in schema


def test_svs_schema_without_optional_attrs():
    """Bare SVS options: only the three required attributes."""
    opts = VectorFieldOptions.svs(
        type=VectorFieldOptions.TYPE.FLOAT16,
        dimension=DIM,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.L2,
    )
    schema = opts.schema
    assert schema == "VECTOR SVS-VAMANA 6 TYPE FLOAT16 DIM 8 DISTANCE_METRIC L2"


def test_to_redisvl_schema_accepts_svs():
    """redisvl's IndexSchema.from_dict accepts the OM-produced vector attrs."""

    class Doc(HashModel, index=True):
        body: str = Field(index=True)
        embedding: List[float] = Field(
            vector_options=VectorFieldOptions.svs(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIM,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                compression="LVQ8",
            )
        )

    schema = to_redisvl_schema(Doc)
    emb = schema.fields["embedding"]
    assert emb.type.value == "vector"
    assert emb.attrs.algorithm.value == "SVS-VAMANA"
    assert emb.attrs.datatype.value == "FLOAT32"
    assert emb.attrs.compression.value == "LVQ8"  # canonical case


def test_to_redisvl_schema_svs_all_optional_attrs():
    """All SVS attributes round-trip into the redisvl schema verbatim."""

    class Doc(HashModel, index=True):
        embedding: List[float] = Field(
            vector_options=VectorFieldOptions.svs(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIM,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                graph_max_degree=40,
                search_window_size=20,
                construction_window_size=300,
            )
        )

    schema = to_redisvl_schema(Doc)
    emb = schema.fields["embedding"]
    attrs = emb.attrs
    assert attrs.graph_max_degree == 40
    assert attrs.search_window_size == 20
    assert attrs.construction_window_size == 300


def test_to_redisvl_schema_svs_leanvec():
    """LeanVec compression accepts the reduce parameter."""

    class Doc(HashModel, index=True):
        embedding: List[float] = Field(
            vector_options=VectorFieldOptions.svs(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIM,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                compression="LeanVec4x8",
                reduce=4,
            )
        )

    schema = to_redisvl_schema(Doc)
    emb = schema.fields["embedding"]
    assert emb.attrs.compression.value == "LeanVec4x8"
    assert emb.attrs.reduce == 4


# --- Server-gated end-to-end ------------------------------------------------


async def _server_version(redis) -> tuple[int, int, int]:
    info = await redis.info("server")
    raw = info["redis_version"]
    return tuple(int(piece) for piece in raw.split(".")[:3])  # type: ignore


@py_test_mark_asyncio
async def test_svs_end_to_end_on_redis_8_2_plus(redis, key_prefix):
    """Create an SVS-VAMANA-indexed HashModel, save, and KNN-query."""
    v = await _server_version(redis)
    if v < (8, 2, 0):
        pytest.skip(f"SVS-VAMANA requires Redis >= 8.2 (server is {v})")

    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Doc(Base):
        body: str
        embedding: List[float] = Field(
            vector_options=VectorFieldOptions.svs(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIM,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                compression="LVQ8",
                training_threshold=1024,
            )
        )

    from aredis_om import Migrator

    await Migrator().run()
    try:
        docs = [
            Doc(body="alpha", pk=f"a{i}", embedding=[float(i)] * DIM)
            for i in range(DIM)
        ]
        await Doc.add(docs)
        # Just ensure save+index round-trip; KNN syntax is exercised by
        # test_hash_model_vector.py for HNSW.
        got = await Doc.get(docs[0].pk)
        assert got.body == "alpha"
    finally:
        from aredis_om.model.model import model_registry

        async for pk in await Doc.all_pks():
            await Doc.delete(pk)
        model_registry.pop(f"{Doc.__module__}.{Doc.__qualname__}", None)
