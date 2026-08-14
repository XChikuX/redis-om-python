# type: ignore
"""Vector fields on ``HashModel`` stored as raw binary blobs.

The fork lets ``HashModel`` carry ``Field(vector_options=...)`` fields to
avoid the ~4-5x storage penalty of JSON float arrays (Redis JSON stores every
float as an 8-byte double; a 1536-dim FLOAT16 blob is 3072 bytes).
"""

import abc
import struct
from typing import Optional, Type

import pytest
import pytest_asyncio

from aredis_om import (
    Field,
    HashModel,
    KNNExpression,
    Migrator,
    RedisModelError,
    VectorFieldOptions,
)
from aredis_om.model.model import (
    _KIND_VECTOR_RAW_BYTES,
    get_conversion_plan,
)

from ._sync_redis import has_redisearch
from .conftest import py_test_mark_asyncio

if not has_redisearch():
    pytestmark = pytest.mark.skip


def _f16_vector_opts(dimension: int = 4) -> VectorFieldOptions:
    """Tiny FLAT / FLOAT16 vector options used by the integration tests."""
    return VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT16,
        dimension=dimension,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )


def test_hash_model_accepts_bytes_vector_field():
    """Headline RAG fix: vector fields are exempt from the HashModel container ban."""

    class Chunk(HashModel):
        content: str = Field(index=True)
        embedding: bytes = Field(vector_options=_f16_vector_opts())

    schema = Chunk.redisearch_schema()
    assert "embedding VECTOR" in schema, schema
    assert "TYPE FLOAT16" in schema, schema
    assert "DIM 4" in schema, schema


def test_hash_model_bytes_vector_emits_correct_schema():
    """The ``TYPE`` rendered in the schema matches the configured option."""
    for type_ in (
        VectorFieldOptions.TYPE.FLOAT32,
        VectorFieldOptions.TYPE.FLOAT64,
        VectorFieldOptions.TYPE.FLOAT16,
        VectorFieldOptions.TYPE.BFLOAT16,
        VectorFieldOptions.TYPE.INT8,
    ):
        opts = VectorFieldOptions.flat(
            type=type_,
            dimension=8,
            distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        )

        class M(HashModel):
            embedding: bytes = Field(vector_options=opts)

        schema = M.redisearch_schema()
        assert f"TYPE {type_.name}" in schema, (type_, schema)


def test_hash_model_container_ban_still_fires_for_non_vector_lists():
    """The container ban is only relaxed for ``vector_options`` fields — ``list`` without it still raises."""

    with pytest.raises(RedisModelError, match="cannot index set, list"):

        class Bad(HashModel):
            tags: list[str]


def test_hash_model_container_ban_still_fires_for_embedded_models():
    """Embedded-model ban is also unchanged."""

    class Inner(HashModel):
        x: int

    with pytest.raises(RedisModelError, match="cannot index embedded"):

        class Bad(HashModel):
            inner: Inner


def test_bytes_vector_field_uses_raw_bytes_conversion_plan():
    """``bytes`` vector fields get the no-op ``_KIND_VECTOR_RAW_BYTES`` kind — no base64, which would corrupt the dtype."""

    class Chunk(HashModel):
        embedding: bytes = Field(vector_options=_f16_vector_opts())

    plan = get_conversion_plan(Chunk)
    assert plan.fields["embedding"].kind == _KIND_VECTOR_RAW_BYTES


def test_vector_field_options_enum_includes_float16_bfloat16_int8():
    """All five RediSearch types are exposed (FLOAT16/BFLOAT16/INT8 server-side since v2.4)."""
    type_names = {t.name for t in VectorFieldOptions.TYPE}
    assert type_names == {"FLOAT32", "FLOAT64", "FLOAT16", "BFLOAT16", "INT8"}


def test_vector_field_options_enum_members_round_trip_name():
    """Schema renders each type by ``.name``, so enum spellings must match the server command syntax."""
    for t in VectorFieldOptions.TYPE:
        opts = VectorFieldOptions.flat(
            type=t,
            dimension=4,
            distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        )
        assert t.name in opts.schema, (t, opts.schema)


@pytest_asyncio.fixture
async def binary_redis(key_prefix):
    """``decode_responses=False`` connection — the redis-py parser UTF-8-decodes on ``True``, and blobs aren't valid UTF-8."""
    from aredis_om import get_redis_connection

    conn = get_redis_connection(decode_responses=False)
    yield conn


@py_test_mark_asyncio
async def test_hash_model_vector_round_trip_with_bytes_payload(
    binary_redis, key_prefix
):
    """A ``bytes`` vector field round-trips through Redis byte-for-byte."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = binary_redis

    class Chunk(BaseHashModel):
        content: str = Field(index=True)
        embedding: bytes = Field(vector_options=_f16_vector_opts(dimension=4))

    Chunk.Meta.database = binary_redis
    await Migrator().run()

    blob = b"\xde\xad\xbe\xef\x01\x02\x03\x04"
    c = Chunk(content="hello", embedding=blob)
    await c.save()

    got = await Chunk.get(c.pk)
    assert got.embedding == blob
    assert got.content == "hello"


@py_test_mark_asyncio
async def test_hash_model_vector_payload_size_matches_raw_bytes(
    binary_redis, key_prefix
):
    """A 1536-dim FLOAT16 vector occupies 2 * dimension bytes (3072), not ~8 * dimension as a JSON float array."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = binary_redis

    DIM = 1536

    class Chunk(BaseHashModel):
        content: str = Field(index=True)
        embedding: bytes = Field(
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT16,
                dimension=DIM,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            )
        )

    Chunk.Meta.database = binary_redis
    await Migrator().run()

    blob = struct.pack(f"<{DIM}H", *range(DIM))
    assert len(blob) == 2 * DIM

    c = Chunk(content="x", embedding=blob)
    await c.save()

    raw = await binary_redis.hget(c.key(), "embedding")
    assert raw == blob
    assert len(raw) == 3072


@py_test_mark_asyncio
async def test_hash_model_vector_bulk_save(binary_redis, key_prefix):
    """``Model.add()`` bulk save preserves raw vector blobs."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = binary_redis

    class Chunk(BaseHashModel):
        embedding: bytes = Field(vector_options=_f16_vector_opts(dimension=2))

    Chunk.Meta.database = binary_redis
    await Migrator().run()

    items = [
        Chunk(embedding=b"\x01\x02\x03\x04"),
        Chunk(embedding=b"\x05\x06\x07\x08"),
    ]
    await Chunk.add(items)

    for original in items:
        got = await Chunk.get(original.pk)
        assert got.embedding == original.embedding


def test_hash_model_bytes_vector_skips_base64_wrapping():
    """Save-side converters don't base64-wrap vector blobs (legacy ``_KIND_BYTES`` does, for JSON.SET)."""
    from aredis_om.model.model import planned_save_conversions

    class Chunk(HashModel):
        embedding: bytes = Field(vector_options=_f16_vector_opts(dimension=4))

    plan = get_conversion_plan(Chunk)
    raw = b"\xde\xad\xbe\xef\x00\x01\x02\x03"
    converted = planned_save_conversions({"embedding": raw}, plan)
    assert converted["embedding"] == raw


def test_hash_model_load_bytes_vector_returns_unchanged():
    """Load-side ``_KIND_VECTOR_RAW_BYTES`` is also a no-op."""
    from aredis_om.model.model import planned_load_conversions

    class Chunk(HashModel):
        embedding: bytes = Field(vector_options=_f16_vector_opts(dimension=4))

    plan = get_conversion_plan(Chunk)
    raw = b"\xde\xad\xbe\xef\x00\x01\x02\x03"
    loaded = planned_load_conversions({"embedding": raw}, plan, for_hash=True)
    assert loaded["embedding"] == raw


# ── KNN queries over raw-blob HashModel vector fields ─────────────────


@pytest_asyncio.fixture
async def knn_hash_model(binary_redis, key_prefix):
    """A HashModel with a raw-blob FLOAT16 vector field, on a binary connection."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = binary_redis

    class Chunk(BaseHashModel):
        content: str = Field(index=True)
        embedding: bytes = Field(vector_options=_f16_vector_opts(dimension=4))
        # KNN score fields must not be indexed — RediSearch synthesises them
        # at query time via ``KNN ... AS <score_field>``.
        embedding_score: Optional[float] = Field(None, index=False)

    await Migrator().run()

    return Chunk


@py_test_mark_asyncio
async def test_hash_model_knn_returns_hydrated_models_with_scores(
    knn_hash_model: Type[HashModel], binary_redis
):
    """KNN hydrates HashModel results by PK and attaches scores.

    Regression: ``RETURN 2 $ <score>`` returned nothing for ON HASH indexes,
    so ``from_redis`` saw only the score and raised ``ValidationError``.
    """
    Chunk = knn_hash_model

    reference = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)
    chunks = [
        Chunk(
            content="near",
            embedding=struct.pack("<4e", 1.0, 0.0, 0.0, 0.0),
        ),
        Chunk(
            content="far",
            embedding=struct.pack("<4e", -1.0, 0.0, 0.0, 0.0),
        ),
        Chunk(
            content="mid",
            embedding=struct.pack("<4e", 0.5, 0.5, 0.0, 0.0),
        ),
    ]
    await Chunk.add(chunks)

    knn = KNNExpression(
        k=3,
        vector_field=Chunk.embedding,
        score_field=Chunk.embedding_score,
        reference_vector=reference,
    )
    results = await Chunk.find(knn=knn).all()

    assert len(results) == 3
    assert all(r.embedding_score is not None for r in results)
    # KNN results come back sorted by score ascending (nearest first).
    assert results[0].content == "near"
    assert results[-1].content == "far"
    # The hydrated models carry the full document, not just search rows —
    # the raw vector blob must survive byte-for-byte.
    assert results[0].embedding == struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)


@py_test_mark_asyncio
async def test_hash_model_knn_intersects_with_filter_expressions(knn_hash_model):
    """KNN combined with a filter expression hydrates only the matches."""
    Chunk = knn_hash_model

    chunks = [
        Chunk(content="py", embedding=struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)),
        Chunk(content="rs", embedding=struct.pack("<4e", 0.9, 0.1, 0.0, 0.0)),
    ]
    await Chunk.add(chunks)

    knn = KNNExpression(
        k=2,
        vector_field=Chunk.embedding,
        score_field=Chunk.embedding_score,
        reference_vector=struct.pack("<4e", 1.0, 0.0, 0.0, 0.0),
    )
    results = await Chunk.find(Chunk.content == "py", knn=knn).all()

    assert len(results) == 1
    assert results[0].content == "py"
    assert results[0].embedding_score is not None


@py_test_mark_asyncio
async def test_hash_model_vector_field_regular_find_hydrates_blobs(
    knn_hash_model: Type[HashModel],
):
    """Plain ``find()`` also hydrates by PK: inline hash rows are lossily decoded strings that would corrupt blobs."""
    Chunk = knn_hash_model

    blob_a = struct.pack("<4e", 1.0, 0.0, 0.0, 0.0)
    blob_b = struct.pack("<4e", 0.0, 1.0, 0.0, 0.0)
    await Chunk.add(
        [
            Chunk(content="alpha", embedding=blob_a),
            Chunk(content="beta", embedding=blob_b),
        ]
    )

    results = await Chunk.find(Chunk.content == "alpha").all()

    assert len(results) == 1
    assert results[0].content == "alpha"
    assert results[0].embedding == blob_a


@py_test_mark_asyncio
async def test_hash_model_get_many_preserves_vector_blobs(
    knn_hash_model: Type[HashModel],
):
    """``get_many`` preserves blobs on ``decode_responses=False`` connections (bytes keys raise in Pydantic v2; the old fallback corrupted blobs)."""
    Chunk = knn_hash_model

    blob = struct.pack("<4e", 0.25, 0.5, 0.75, 1.0)
    c = Chunk(content="hello", embedding=blob)
    await c.save()

    docs = await Chunk.get_many([c.pk])
    assert len(docs) == 1
    assert docs[0].embedding == blob
    assert docs[0].content == "hello"
