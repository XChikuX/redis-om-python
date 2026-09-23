# type: ignore
"""AI extension helper tests (U3–U6, U8).

Covers the ``aredis_om.ai`` helpers over redisvl's LLM extensions:
semantic cache (U3), message history (U4), semantic router (U5),
reranking (U6), and the compression advisor (U8). All vectorizers are
fakes (``CustomVectorizer`` wrapping local callables) — no network calls.
"""

import abc
import asyncio
import inspect
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest
import pytest_asyncio

from aredis_om import Field, HashModel, VectorFieldOptions
from aredis_om.ai import (
    estimate_memory_savings,
    get_message_history,
    get_router,
    get_semantic_cache,
    recommend_compression,
    rerank_results,
)

from .conftest import py_test_mark_asyncio


DIMENSIONS = 8


async def _eventually(fn, predicate, attempts: int = 40, delay: float = 0.02):
    """Poll ``fn()`` until ``predicate`` holds (or attempts run out).

    Redis Search indexes writes asynchronously, so a read immediately after
    a write (e.g. ``add_messages`` → ``get_recent``, ``astore`` → ``acheck``)
    can miss entries. Retrying briefly keeps these tests deterministic
    instead of asserting on a racy first read. Works for sync and async
    ``fn`` (the generated sync mirror only ever sees sync results).
    """
    result = None
    for _ in range(attempts):
        try:
            result = fn()
            if inspect.isawaitable(result):
                result = await result
        except Exception:  # transient index-not-ready errors
            result = None
        if result is not None and predicate(result):
            return result
        await asyncio.sleep(delay)
    return result


def _hash_vec(text: str, dim: int = DIMENSIONS) -> List[float]:
    """Deterministic embedding: character-code-derived vector."""
    vec = [float(ord(c) % 97) for c in text[:dim]]
    while len(vec) < dim:
        vec.append(1.0)
    return vec


def _make_vectorizer(dim: int = DIMENSIONS):
    """Build a deterministic fake vectorizer."""
    from redisvl.utils.vectorize import CustomVectorizer

    def embed(text: str) -> List[float]:
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


@pytest_asyncio.fixture
async def emb_model(redis, key_prefix):
    """A HashModel with one auto-embedding field (vectorizer on the field)."""
    vec = _make_vectorizer()

    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Doc(Base, index=True):
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

    yield Doc


# --- U3: semantic cache -----------------------------------------------------


@py_test_mark_asyncio
async def test_semantic_cache_round_trip(emb_model, key_prefix):
    name = f"{key_prefix}:semcache"
    cache = get_semantic_cache(name, model_cls=emb_model, distance_threshold=0.1)
    try:
        await cache.astore(prompt="What is Redis?", response="An in-memory data store.")
        # Identical prompt → distance 0 → hit. Search indexes writes
        # asynchronously, so poll instead of asserting on the first read.
        hits = await _eventually(
            lambda: cache.acheck(prompt="What is Redis?"),
            lambda r: bool(r),
        )
        assert hits, "expected a cache hit for the identical prompt"
        assert hits[0]["response"] == "An in-memory data store."

        # A very different prompt is beyond the distance threshold → miss.
        misses = await cache.acheck(prompt="z y x w")
        assert not misses, f"unexpected hit for unrelated prompt: {misses}"
    finally:
        cache.delete()


@py_test_mark_asyncio
async def test_semantic_cache_explicit_vectorizer(emb_model, key_prefix):
    """vectorizer= wins and no model resolution happens (None model_cls)."""
    vec = _make_vectorizer()
    name = f"{key_prefix}:semcache-explicit"
    cache = get_semantic_cache(name, vectorizer=vec, distance_threshold=0.1)
    try:
        await cache.astore(prompt="hello", response="world")
        hits = await _eventually(
            lambda: cache.acheck(prompt="hello"),
            lambda r: bool(r),
        )
        assert hits and hits[0]["response"] == "world"
    finally:
        cache.delete()


@py_test_mark_asyncio
async def test_semantic_cache_namespace_collision(emb_model):
    from aredis_om.model.model import RedisModelError

    with pytest.raises(RedisModelError, match="collides"):
        get_semantic_cache(emb_model.Meta.index_name, model_cls=emb_model)


# --- U4: message history ----------------------------------------------------


@py_test_mark_asyncio
async def test_message_history_round_trip(emb_model, key_prefix):
    name = f"{key_prefix}:history"
    history = get_message_history(name, model_cls=emb_model, session_tag="s1")
    try:
        history.add_messages(
            [
                {"role": "user", "content": "Hello there"},
                {"role": "assistant", "content": "Hi! How can I help?"},
            ]
        )
        recent = await _eventually(
            lambda: history.get_recent(as_text=False, session_tag="s1"),
            lambda r: (
                {"Hello there", "Hi! How can I help?"}
                <= {entry["content"] for entry in r}
            ),
        )
        contents = [entry["content"] for entry in recent]
        assert "Hello there" in contents
        assert "Hi! How can I help?" in contents
    finally:
        history.delete()


@py_test_mark_asyncio
async def test_semantic_message_history(emb_model, key_prefix):
    name = f"{key_prefix}:semantic-history"
    history = get_message_history(
        name,
        model_cls=emb_model,
        session_tag="s2",
        semantic=True,
    )
    try:
        history.add_messages([{"role": "user", "content": "hello world"}])
        # Identical prompt → distance 0 → retrieved as relevant context.
        relevant = await _eventually(
            lambda: history.get_relevant("hello world", as_text=True),
            lambda r: any("hello world" in entry for entry in r),
        )
        assert any("hello world" in entry for entry in relevant)
    finally:
        history.delete()


@py_test_mark_asyncio
async def test_message_history_namespace_collision(emb_model):
    from aredis_om.model.model import RedisModelError

    with pytest.raises(RedisModelError, match="collides"):
        get_message_history(emb_model.Meta.index_name, model_cls=emb_model)


# --- U5: semantic router ----------------------------------------------------


@py_test_mark_asyncio
async def test_router_routes_statement(emb_model, key_prefix):
    from redisvl.extensions.router import Route

    routes = [
        Route(name="greet", references=["hello", "hi"]),
        Route(name="goodbye", references=["bye", "farewell"]),
    ]
    router = get_router(
        f"{key_prefix}:router", routes, model_cls=emb_model, overwrite=True
    )
    try:
        # "hello" shares an exact reference vector → distance 0 → greet.
        # Route references load into a Search index, so poll briefly.
        match = await _eventually(
            lambda: router("hello"),
            lambda m: m is not None and m.name == "greet",
        )
        assert match.name == "greet"
        assert match.distance is not None and match.distance <= 0.5

        match = await _eventually(
            lambda: router("farewell"),
            lambda m: m is not None and m.name == "goodbye",
        )
        assert match.name == "goodbye"

        matches = router.route_many("hi", max_k=2)
        assert matches and matches[0].name == "greet"
    finally:
        router.delete()


@py_test_mark_asyncio
async def test_router_namespace_collision(emb_model):
    from aredis_om.model.model import RedisModelError

    with pytest.raises(RedisModelError, match="collides"):
        get_router(emb_model.Meta.index_name, [], model_cls=emb_model)


# --- U6: reranking ----------------------------------------------------------


class FakeReranker:
    """Ranks by keyword-overlap with the query — no network, deterministic."""

    def rank(
        self, query: str, docs: Sequence[Dict[str, Any]], **kwargs: Any
    ) -> Tuple[Sequence[Dict[str, Any]], List[float]]:
        def score(doc: Dict[str, Any]) -> float:
            text = doc.get("text", "")
            return float(sum(1 for word in query.split() if word in text))

        ranked = sorted(docs, key=lambda d: -score(d))
        return ranked, [score(d) for d in ranked]


@py_test_mark_asyncio
async def test_rerank_reorders_models(emb_model):
    docs = [
        emb_model(body="vector search", pk="1"),
        emb_model(body="redis cache", pk="2"),
        emb_model(body="running shoes", pk="3"),
    ]
    reranked, scores = await rerank_results(docs, "redis cache", FakeReranker())
    assert [m.pk for m in reranked] == ["2", "1", "3"]
    assert scores == pytest.approx([2.0, 0.0, 0.0])


@py_test_mark_asyncio
async def test_rerank_dicts():
    docs = [
        {"id": "a", "text": "vector search"},
        {"id": "b", "text": "redis cache"},
    ]
    reranked, scores = await rerank_results(
        docs, "redis cache", FakeReranker(), rank_by=["text"]
    )
    assert [d["id"] for d in reranked] == ["b", "a"]
    assert scores == pytest.approx([2.0, 0.0])


@py_test_mark_asyncio
async def test_rerank_empty_results():
    reranked, scores = await rerank_results([], "anything", FakeReranker())
    assert reranked == [] and scores == []


@py_test_mark_asyncio
async def test_rerank_requires_content_field(redis, key_prefix):
    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Plain(Base):
        note: str

    docs = [Plain(note="x", pk="1")]
    with pytest.raises(ValueError, match="content_field"):
        await rerank_results(docs, "query", FakeReranker())


@py_test_mark_asyncio
async def test_rerank_content_field_default_from_source(emb_model):
    """content_field defaults to the model's single auto-embedding source."""
    docs = [
        emb_model(body="redis cache", pk="1"),
        emb_model(body="kittens", pk="2"),
    ]
    reranked, scores = await rerank_results(docs, "redis cache", FakeReranker())
    assert reranked[0].pk == "1"


@py_test_mark_asyncio
async def test_rerank_limit(emb_model):
    docs = [
        emb_model(body="vector search", pk="1"),
        emb_model(body="redis cache", pk="2"),
        emb_model(body="running shoes", pk="3"),
    ]
    reranked, scores = await rerank_results(
        docs, "redis cache", FakeReranker(), limit=2
    )
    assert len(reranked) == 2 and len(scores) == 2
    assert reranked[0].pk == "2"


# --- U8: compression advisor ------------------------------------------------


def _big_vector_model(base_cls) -> Any:
    class Doc8(base_cls):
        embedding: List[float] = Field(
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=1536,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            )
        )

    return Doc8


@pytest_asyncio.fixture
async def plain_base(redis, key_prefix):
    class Base(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    return Base


@py_test_mark_asyncio
async def test_recommend_compression(plain_base):
    Doc8 = _big_vector_model(plain_base)
    svs_cfg, summary = recommend_compression(Doc8, "embedding", "memory")
    from redisvl.utils.compression import SVSConfig

    assert isinstance(svs_cfg, SVSConfig)
    assert isinstance(summary["compression"], str)
    assert 0 < summary["memory_saving_fraction"] <= 100
    assert summary["priority"] == "memory"

    # The recommendation feeds VectorFieldOptions.svs(...) directly.
    opts = VectorFieldOptions.svs(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=1536,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        compression=summary["compression"],
        reduce=summary["reduce"],
    )
    assert summary["compression"] in opts.schema
    assert "SVS-VAMANA" in opts.schema


@py_test_mark_asyncio
async def test_estimate_memory_savings(plain_base):
    Doc8 = _big_vector_model(plain_base)
    saving = estimate_memory_savings(Doc8, "embedding")
    assert isinstance(saving, float)
    assert 0 < saving <= 100


@py_test_mark_asyncio
async def test_recommend_compression_validates_field(plain_base):
    class NoVector(plain_base):
        body: str

    with pytest.raises(ValueError, match="vector_options"):
        recommend_compression(NoVector, "body")
    with pytest.raises(ValueError, match="does not exist"):
        recommend_compression(NoVector, "embedding")
