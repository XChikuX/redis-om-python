# type: ignore
"""Tests for the RedisVL integration (``aredis_om.redisvl``).

Covers the schema conversion (:func:`to_redisvl_schema`), index
construction (:func:`get_redisvl_index`), the D1 dual-field fidelity
(``body`` TAG + ``body_fts`` TEXT so OM- and RedisVL-created indexes are
interchangeable), the lazy redisvl import, the cluster-aware
``hybrid_search`` routing, and end-to-end redisvl queries against an
OM-created index.
"""

import abc
import importlib
import re
import sys
from types import SimpleNamespace
from typing import List

import pytest
import pytest_asyncio
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

try:
    from redisvl.index import AsyncSearchIndex, SearchIndex
    from redisvl.query import HybridQuery, VectorQuery
    from redisvl.schema import IndexSchema
except ImportError:  # pragma: no cover - dev extras always include redisvl
    pytest.skip("requires the optional redisvl package", allow_module_level=True)

import aredis_om.redisvl as om_redisvl
from aredis_om import (
    Field,
    HashModel,
    JsonModel,
    Migrator,
    VectorFieldOptions,
    get_redis_connection,
)
from aredis_om.redisvl import get_redisvl_index, hybrid_search, to_redisvl_schema

from ._sync_redis import has_redis_json
from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip


DIMENSIONS = 8


def _flat_vector_options(dtype=VectorFieldOptions.TYPE.FLOAT32, dimension=DIMENSIONS):
    return VectorFieldOptions.flat(
        type=dtype,
        dimension=dimension,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )


class _UnconnectedClusterClient(AsyncRedisCluster):
    """Cluster client instance that never opens connections.

    Used only for ``isinstance`` routing in ``hybrid_search`` — every I/O
    method is monkeypatched by the routing tests. The sync mirror's
    ``RedisCluster`` initializes cluster topology eagerly on construction
    (and hangs when the announced node addresses are unreachable), so a
    no-op ``__init__`` keeps the tests network-free in both mirrors.
    """

    def __init__(self, *args, **kwargs):
        pass


async def _has_ft_hybrid(db):
    """Probe the server for FT.HYBRID (Redis 8.4+)."""
    try:
        info = await db.execute_command("COMMAND", "INFO", "ft.hybrid")
        return bool(info and all(info))
    except Exception:
        return False


@pytest_asyncio.fixture
async def json_document_model(key_prefix, redis):
    """Indexed JsonModel with a full-text field and a vector field."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Document(BaseJsonModel, index=True):
        title: str = Field(index=True)
        body: str = Field(full_text_search=True)
        views: int = Field(index=True, sortable=True)
        embedding: List[float] = Field(vector_options=_flat_vector_options())

    await Migrator(conn=redis).run()
    return Document


@pytest_asyncio.fixture
async def hash_product_model(key_prefix, redis):
    """Indexed HashModel with a full-text field."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Product(BaseHashModel, index=True):
        name: str = Field(index=True)
        description: str = Field(full_text_search=True)
        price: float = Field(index=True, sortable=True)
        in_stock: bool = Field(index=True)

    await Migrator(conn=redis).run()
    return Product


@pytest_asyncio.fixture
async def non_indexed_model(key_prefix, redis):
    """Model without ``index=True`` for the error-path test."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class SimpleModel(BaseJsonModel):
        name: str

    return SimpleModel


@pytest_asyncio.fixture
async def redis_bytes():
    """Binary connection (decode_responses=False) for raw vector reads."""
    conn = get_redis_connection(decode_responses=False)
    yield conn
    await conn.aclose()


@pytest_asyncio.fixture
async def hash_vector_model(key_prefix, redis_bytes):
    """Indexed HashModel with a FLOAT64 vector field (raw blob storage)."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis_bytes

    class Item(BaseHashModel, index=True):
        name: str = Field(index=True)
        embedding: List[float] = Field(
            vector_options=_flat_vector_options(
                dtype=VectorFieldOptions.TYPE.FLOAT64, dimension=4
            )
        )

    await Migrator(conn=redis_bytes).run()
    return Item


@pytest_asyncio.fixture
async def cluster_model_and_index(key_prefix):
    """Model whose database is presented as an async cluster client.

    The client never opens connections — redis-py's *sync* cluster client
    initializes cluster topology eagerly on construction, so a no-op
    ``__init__`` subclass keeps these tests network-free and identical in
    the async and sync mirrors. All I/O is monkeypatched; the instance is
    only used for the ``isinstance`` routing inside ``hybrid_search``.
    """
    cluster_client = _UnconnectedClusterClient()

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = cluster_client

    class ClusterDoc(BaseJsonModel, index=True):
        body: str = Field(full_text_search=True)
        embedding: List[float] = Field(vector_options=_flat_vector_options())

    index = get_redisvl_index(ClusterDoc)
    yield ClusterDoc, index, cluster_client


class TestToRedisvlSchema:
    @py_test_mark_asyncio
    async def test_json_model(self, json_document_model):
        Document = json_document_model

        schema = to_redisvl_schema(Document)

        assert isinstance(schema, IndexSchema)
        assert schema.index.name == Document.Meta.index_name
        assert schema.index.storage_type.value == "json"
        assert schema.index.prefix == Document.make_key("")

        field_names = set(schema.fields)
        assert {"pk", "title", "body", "body_fts", "views", "embedding"} <= (
            field_names
        )

        # Vector attrs map one-to-one from VectorFieldOptions.
        vector_attrs = schema.fields["embedding"].attrs
        assert vector_attrs.dims == DIMENSIONS
        assert vector_attrs.algorithm.value == "FLAT"
        assert vector_attrs.datatype.value == "FLOAT32"
        assert vector_attrs.distance_metric.value == "COSINE"

        # The _fts TEXT field aliases the same JSON path.
        assert type(schema.fields["body_fts"]).__name__ == "TextField"
        assert schema.fields["body_fts"].path == "$.body"
        assert schema.fields["views"].attrs.sortable is True

    @py_test_mark_asyncio
    async def test_hash_model(self, hash_product_model):
        Product = hash_product_model

        schema = to_redisvl_schema(Product)

        assert isinstance(schema, IndexSchema)
        assert schema.index.storage_type.value == "hash"
        assert {"pk", "name", "description", "description_fts", "price"} <= set(
            schema.fields
        )

        # Hash storage: the _fts TEXT field must reference the plain hash
        # field name so FT.CREATE renders ``description AS description_fts
        # TEXT`` exactly like OM's Migrator.
        assert schema.fields["description_fts"].path == "description"
        assert "description AS description_fts TEXT" in (Product.redisearch_schema())

    @py_test_mark_asyncio
    async def test_fts_field_name_set_matches_om_schema(self, json_document_model):
        """D1 fidelity: the redisvl schema exposes exactly the fields OM's
        own FT.CREATE does (including the _fts aliases)."""
        Document = json_document_model

        schema = to_redisvl_schema(Document)
        om_field_names = set(re.findall(r"AS (\w+)", Document.redisearch_schema()))

        assert om_field_names == set(schema.fields)

    @py_test_mark_asyncio
    async def test_non_indexed_raises(self, non_indexed_model):
        with pytest.raises(ValueError, match="is not indexed"):
            to_redisvl_schema(non_indexed_model)


class TestGetRedisvlIndex:
    @py_test_mark_asyncio
    async def test_async(self, json_document_model):
        Document = json_document_model

        index = get_redisvl_index(Document, async_client=True)

        assert isinstance(index, AsyncSearchIndex)
        assert index.schema.index.name == Document.Meta.index_name

    @py_test_mark_asyncio
    async def test_sync(self, json_document_model):
        Document = json_document_model

        index = get_redisvl_index(Document, async_client=False)

        assert isinstance(index, SearchIndex)
        assert index.schema.index.name == Document.Meta.index_name

    @py_test_mark_asyncio
    async def test_accepts_cluster_client(self, cluster_model_and_index):
        ClusterDoc, index, cluster_client = cluster_model_and_index

        assert isinstance(index, AsyncSearchIndex)
        assert index.schema.index.name == ClusterDoc.Meta.index_name
        assert index._redis_client is cluster_client


class TestLazyImport:
    def test_module_imports_without_redisvl(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "redisvl", None)
        try:
            importlib.reload(om_redisvl)
        finally:
            monkeypatch.undo()
            importlib.reload(om_redisvl)

    @py_test_mark_asyncio
    async def test_helpers_raise_helpful_import_error(
        self, non_indexed_model, monkeypatch
    ):
        monkeypatch.setitem(sys.modules, "redisvl", None)

        with pytest.raises(ImportError, match="requires the 'redisvl' package"):
            to_redisvl_schema(non_indexed_model)
        with pytest.raises(ImportError, match="requires the 'redisvl' package"):
            om_redisvl._import_redisvl()


class TestVectorRoundTrip:
    @py_test_mark_asyncio
    async def test_hash_model_float64_stored_as_raw_blob(
        self, hash_vector_model, redis_bytes
    ):
        Item = hash_vector_model
        vector = [1.5, -2.25, 3.125, -4.0625]  # exact binary fractions

        item = Item(name="widget", embedding=vector)
        await item.save()

        raw = await redis_bytes.hget(item.key(), "embedding")
        assert isinstance(raw, bytes)
        assert len(raw) == 4 * 8  # FLOAT64 = 8 bytes per dimension

        loaded = await Item.get(item.pk)
        assert loaded.embedding == vector


class TestRedisvlAgainstOmCreatedIndex:
    """End-to-end: redisvl queries an index created by OM's Migrator."""

    @staticmethod
    def _documents(Document):
        onehot = lambda i: [1.0 if j == i else 0.0 for j in range(DIMENSIONS)]  # noqa: E731
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

    @py_test_mark_asyncio
    async def test_vector_query(self, json_document_model):
        Document = json_document_model
        docs = self._documents(Document)
        for doc in docs:
            await doc.save()

        index = get_redisvl_index(Document)
        results = await index.query(
            VectorQuery(
                vector=[1.0] + [0.0] * (DIMENSIONS - 1),
                vector_field_name="embedding",
                num_results=2,
            )
        )

        assert len(results) == 2
        assert results[0]["id"] == docs[0].key()

    @py_test_mark_asyncio
    async def test_text_search_uses_fts_alias(self, json_document_model):
        Document = json_document_model
        for doc in self._documents(Document):
            await doc.save()

        index = get_redisvl_index(Document)
        result = await index.search("@body_fts:(running)")

        total = getattr(result, "total", None)
        if total is None:
            total = len(getattr(result, "docs", []))
        assert total >= 1

    @py_test_mark_asyncio
    async def test_hybrid_search_end_to_end(self, json_document_model, redis):
        if not await _has_ft_hybrid(redis):
            pytest.skip("FT.HYBRID requires Redis 8.4+")

        Document = json_document_model
        docs = self._documents(Document)
        for doc in docs:
            await doc.save()

        index = get_redisvl_index(Document)
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


class TestHybridSearchRouting:
    @py_test_mark_asyncio
    async def test_delegates_on_non_cluster_client(
        self, json_document_model, monkeypatch
    ):
        Document = json_document_model
        index = get_redisvl_index(Document)
        assert not om_redisvl._is_cluster_client(index._redis_client)

        sentinel_results = [{"id": "doc1"}]
        calls = {}

        async def fake_query(q, **kwargs):
            calls["query"] = q
            calls["kwargs"] = kwargs
            return sentinel_results

        monkeypatch.setattr(index, "query", fake_query)

        query = HybridQuery(
            text="shoes",
            text_field_name="body_fts",
            vector=[1.0] * DIMENSIONS,
            vector_field_name="embedding",
        )
        assert await hybrid_search(index, query) is sentinel_results
        assert calls["query"] is query
        assert calls["kwargs"] == {}

        # Regression: redisvl's query() takes no timeout parameter, so the
        # non-cluster path must never forward one (would TypeError).
        assert await hybrid_search(index, query, timeout=500) is (sentinel_results)
        assert calls["kwargs"] == {}

    @py_test_mark_asyncio
    async def test_pins_default_node_on_cluster_client(
        self, cluster_model_and_index, monkeypatch
    ):
        ClusterDoc, index, cluster_client = cluster_model_and_index

        captured = {}
        sentinel_node = object()

        async def fake_execute_command(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            return ["raw-response"]

        class _FakeSearchCommands:
            def get_params_args(self, params):
                return []

            def _parse_results(self, cmd, res, **kwargs):
                return SimpleNamespace(results=[{"id": "doc1"}])

        monkeypatch.setattr(cluster_client, "execute_command", fake_execute_command)
        monkeypatch.setattr(cluster_client, "get_default_node", lambda: sentinel_node)
        monkeypatch.setattr(
            cluster_client, "ft", lambda index_name: _FakeSearchCommands()
        )
        monkeypatch.setattr(
            "redisvl.utils.redis_protocol.get_protocol_version",
            lambda client: "2",
        )
        monkeypatch.setattr(
            "redisvl.index.index._convert_and_drop_empty_rows",
            lambda rows, kind: rows,
        )

        query = HybridQuery(
            text="shoes",
            text_field_name="body_fts",
            vector=[1.0] * DIMENSIONS,
            vector_field_name="embedding",
            num_results=5,
        )
        results = await hybrid_search(index, query, timeout=500)

        args = captured["args"]
        assert args[0] == "FT.HYBRID"
        assert args[1] == ClusterDoc.Meta.index_name
        assert any(a == "TIMEOUT" and b == 500 for a, b in zip(args, args[1:]))
        assert captured["kwargs"]["target_nodes"] == [sentinel_node]
        assert results == [{"id": "doc1"}]
