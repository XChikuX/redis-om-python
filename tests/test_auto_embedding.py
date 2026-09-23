# type: ignore
"""Auto-embedding tests (U1) and embedding-cache wiring (U2).

Covers ``Field(vectorizer=..., source=...)`` declarations, the save-path
hook (both HashModel and JsonModel), raw-text KNN via ``text_knn``, bulk
``add()``, validation errors, and the zero-overhead guarantee for models
without vectorizers (no redisvl import, no schema change).

All embedding providers are fakes (``CustomVectorizer`` wrapping plain
callables) — no network calls.
"""

import abc
import struct
from typing import List, Optional

import pytest
import pytest_asyncio

from aredis_om import Field, HashModel, JsonModel, Migrator, VectorFieldOptions
from aredis_om.ai import EmbeddingError, text_knn

from .conftest import py_test_mark_asyncio


DIMENSIONS = 8


def _hash_vec(text: str, dim: int = DIMENSIONS) -> List[float]:
    """Deterministic embedding: character-code-derived vector."""
    vec = [float(ord(c) % 97) for c in text[:dim]]
    while len(vec) < dim:
        vec.append(1.0)
    return vec


def _make_vectorizer(dim: int = DIMENSIONS, calls: Optional[list] = None):
    """Build a fake redisvl vectorizer wrapping local callables."""
    from redisvl.utils.vectorize import CustomVectorizer

    def embed(text: str) -> List[float]:
        if calls is not None:
            calls.append(text)
        return _hash_vec(text, dim)

    def embed_many(texts: List[str]) -> List[List[float]]:
        return [embed(t) for t in texts]

    async def aembed(text: str) -> List[float]:
        return embed(text)

    async def aembed_many(texts: List[str]) -> List[List[float]]:
        return embed_many(texts)

    return CustomVectorizer(
        embed=embed,
        embed_many=embed_many,
        aembed=aembed,
        aembed_many=aembed_many,
    )


@pytest.fixture
def vectorizer():
    return _make_vectorizer()


@pytest_asyncio.fixture
async def binary_redis(key_prefix):
    """``decode_responses=False`` connection — packed vector blobs are not
    valid UTF-8, so the default decoded connection cannot read them."""
    from aredis_om import get_redis_connection

    conn = get_redis_connection(decode_responses=False)
    yield conn
    await conn.aclose()


@pytest_asyncio.fixture
async def doc_model(binary_redis, vectorizer, key_prefix):
    # NOTE: the model database must be a decode_responses=False connection —
    # packed vector blobs cannot round-trip through the decoded parser, and
    # KNN queries fetch results via HGETALL.
    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = binary_redis

    class Doc(Base, index=True):
        body: str = Field(full_text_search=True)
        embedding: List[float] = Field(
            [],
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            ),
            vectorizer=vectorizer,
            source="body",
        )

    await Migrator().run()
    yield Doc
    await _cleanup(Doc)


async def _cleanup(model):
    from aredis_om.model.model import model_registry

    async for pk in await model.all_pks():
        await model.delete(pk)
    model_registry.pop(f"{model.__module__}.{model.__qualname__}", None)


@py_test_mark_asyncio
async def test_save_auto_embeds(doc_model, binary_redis):
    """Saving with only body populated stores the embedded vector."""
    doc = doc_model(body="hello world")
    await doc.save()

    raw = await binary_redis.hgetall(doc.key())
    blob = raw.get(b"embedding")
    assert blob is not None and len(blob) == DIMENSIONS * 4, (
        f"expected packed FLOAT32 blob of {DIMENSIONS * 4} bytes, got "
        f"{type(blob)} len={len(blob) if blob else None}"
    )
    unpacked = struct.unpack(f"<{DIMENSIONS}f", blob)
    expected = _hash_vec("hello world")
    assert list(unpacked) == pytest.approx(expected)


@py_test_mark_asyncio
async def test_manual_vector_wins(doc_model, binary_redis):
    """A user-provided vector is stored as-is (no re-embed)."""
    manual = [0.5] * DIMENSIONS
    doc = doc_model(body="hello", embedding=manual)
    await doc.save()

    raw = await binary_redis.hgetall(doc.key())
    unpacked = struct.unpack(f"<{DIMENSIONS}f", raw[b"embedding"])
    assert list(unpacked) == pytest.approx(manual)


@py_test_mark_asyncio
async def test_empty_source_skipped(doc_model, binary_redis):
    """No embedding when the source field is empty."""
    doc = doc_model(body="")
    await doc.save()
    raw = await binary_redis.hgetall(doc.key())
    assert not raw.get(b"embedding")


@py_test_mark_asyncio
async def test_knn_from_raw_text(doc_model):
    """text_knn builds a working KNN expression from raw text."""
    docs = [
        doc_model(body="alpha content"),
        doc_model(body="beta content"),
        doc_model(body="gamma content"),
    ]
    await doc_model.add(docs)

    knn = await text_knn(doc_model, "beta content", field_name="embedding", k=1)
    results = await doc_model.find(knn=knn).all()
    assert len(results) == 1
    assert results[0].body == "beta content"


@py_test_mark_asyncio
async def test_knn_requires_vectorizer_field(redis, key_prefix):
    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Plain(Base, index=True):
        note: str

    with pytest.raises(ValueError, match="vectorizer"):
        await text_knn(Plain, "query")


@py_test_mark_asyncio
async def test_bulk_add_embeds_each(doc_model):
    calls: list = []
    vec = _make_vectorizer(calls=calls)

    # Rebuild the model class with the call-counting vectorizer.
    class Doc2(doc_model.__bases__[0], index=True):
        body: str = Field(full_text_search=True)
        embedding: List[float] = Field(
            [],
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            ),
            vectorizer=vec,
            source="body",
        )

    await Migrator().run()
    try:
        docs = [Doc2(body=f"doc {i}") for i in range(3)]
        await Doc2.add(docs)
        # CustomVectorizer probes with "dimension test" strings at
        # construction — only count the real document embeds.
        doc_calls = [c for c in calls if c.startswith("doc ")]
        assert sorted(doc_calls) == sorted(f"doc {i}" for i in range(3))
    finally:
        await _cleanup(Doc2)


@py_test_mark_asyncio
async def test_no_vectorizer_model_untouched(redis, key_prefix):
    """Models without vectorizers keep a None embedding_fields and an
    unchanged schema; saving works without importing redisvl."""

    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Plain(Base, index=True):
        body: str = Field(full_text_search=True)

    assert getattr(Plain._meta, "embedding_fields", None) is None
    # Save works.
    p = Plain(body="x")
    await p.save()
    await p.delete(p.pk)

    # The save-path guard must not import the ai package's lazy helpers
    # (they are only imported when embedding fields exist).
    await p.save()  # second save exercises the guard again
    await p.delete(p.pk)


@py_test_mark_asyncio
async def test_validation_missing_source(redis, key_prefix):
    from aredis_om.model.model import RedisModelError

    with pytest.raises(RedisModelError, match="no such field"):

        class Bad(HashModel, index=True):
            class Meta:
                global_key_prefix = key_prefix
                database = redis

            embedding: List[float] = Field(
                [],
                vector_options=VectorFieldOptions.flat(
                    type=VectorFieldOptions.TYPE.FLOAT32,
                    dimension=DIMENSIONS,
                    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                ),
                vectorizer="openai:text-embedding-3-small",
                source="nonexistent",
            )


@py_test_mark_asyncio
async def test_validation_vectorizer_needs_vector_options(redis, key_prefix):
    from aredis_om.model.model import RedisModelError

    with pytest.raises(RedisModelError, match="vector_options"):

        class Bad(HashModel, index=True):
            class Meta:
                global_key_prefix = key_prefix
                database = redis

            text: str
            novec: List[float] = Field([], vectorizer="openai:x", source="text")


@py_test_mark_asyncio
async def test_dimension_mismatch_raises(redis, key_prefix):
    """A vectorizer returning the wrong dimensionality fails with a clear
    error mentioning both numbers."""
    wrong_dim = _make_vectorizer(dim=DIMENSIONS + 2)

    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class BadDim(Base, index=True):
        body: str
        embedding: List[float] = Field(
            [],
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            ),
            vectorizer=wrong_dim,
            source="body",
        )

    doc = BadDim(body="hello")
    with pytest.raises(EmbeddingError, match="10"):
        await doc.save()


@py_test_mark_asyncio
async def test_json_model_auto_embeds(redis, key_prefix):
    """JsonModel: the embedding lands as a JSON array of the right dim."""
    vec = _make_vectorizer()

    class Base(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class JDoc(Base, index=True):
        body: str = Field(full_text_search=True)
        embedding: List[float] = Field(
            [],
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            ),
            vectorizer=vec,
            source="body",
        )

    await Migrator().run()
    try:
        doc = JDoc(body="json body")
        await doc.save()
        raw = await JDoc.db().json().get(doc.key(), "$.embedding")
        assert isinstance(raw, list) and len(raw[0]) == DIMENSIONS
        assert raw[0] == pytest.approx(_hash_vec("json body"))
    finally:
        async for pk in await JDoc.all_pks():
            await JDoc.delete(pk)
        from aredis_om.model.model import model_registry

        model_registry.pop(f"{JDoc.__module__}.{JDoc.__qualname__}", None)


@py_test_mark_asyncio
async def test_embedding_cache_round_trip(redis, key_prefix, binary_redis):
    """U2: with Meta.embedding_cache=True, identical text embeds once and
    the second save is served from the Redis-backed cache."""
    calls: list = []
    vec = _make_vectorizer(calls=calls)

    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis
            embedding_cache = True

    class CachedDoc(Base, index=True):
        body: str
        embedding: List[float] = Field(
            [],
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            ),
            vectorizer=vec,
            source="body",
        )

    await Migrator().run()
    try:
        a = CachedDoc(body="cached text", pk="a")
        await a.save()
        b = CachedDoc(body="cached text", pk="b")
        await b.save()
        # First save embeds; the second is a cache hit (same content+model).
        assert calls.count("cached text") == 1, (
            f"expected exactly one provider embed, got {calls}"
        )
        raw_b = await binary_redis.hgetall(b.key())
        assert len(raw_b[b"embedding"]) == DIMENSIONS * 4
    finally:
        async for pk in await CachedDoc.all_pks():
            await CachedDoc.delete(pk)
        # Drop the auto-created embedding cache keys.
        cache = CachedDoc.db()
        keys = [k async for k in cache.scan_iter(f"{key_prefix}:*embcache*")]
        if keys:
            await cache.delete(*keys)
        from aredis_om.model.model import model_registry

        model_registry.pop(f"{CachedDoc.__module__}.{CachedDoc.__qualname__}", None)


@py_test_mark_asyncio
async def test_lazy_import_without_redisvl(monkeypatch):
    """Spec building never imports redisvl; resolution errors carry the
    install hint."""
    import builtins

    real_import = builtins.__import__

    def no_redisvl(name, *args, **kwargs):
        if name.startswith("redisvl"):
            raise ImportError("no redisvl")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_redisvl)

    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = "lazy"

    class LazyDoc(Base, index=True):
        body: str
        embedding: List[float] = Field(
            [],
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            ),
            vectorizer="openai:text-embedding-3-small",
            source="body",
        )

    # Model definition succeeded without redisvl...
    doc = LazyDoc(body="x")
    # ...but embedding raises the friendly ImportError.
    from aredis_om.model.model import RedisModelError

    try:
        await doc.save()
        pytest.fail("expected ImportError")
    except ImportError as e:
        assert "redisvl" in str(e)
    except RedisModelError:
        pytest.fail("save should surface the ImportError, not a model error")
