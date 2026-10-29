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
from typing import List, Optional

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
    Coordinates,
    EmbeddedJsonModel,
    Field,
    HashModel,
    JsonModel,
    Migrator,
    VectorFieldOptions,
    get_redis_connection,
)
from aredis_om.redisvl import get_redisvl_index, hybrid_search, to_redisvl_schema

from ._sync_redis import has_redis_json
from .conftest import REDIS_PY_LT_8, py_test_mark_asyncio, redis_url

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
async def json_field_mapping_model(key_prefix):
    """JSON model exercising every ``_get_field_type`` branch.

    No Migrator run: ``to_redisvl_schema`` is pure schema conversion and the
    exotic fields (dict, Any, embedded) exist to probe conversion, not to be
    indexed by OM's own Migrator.
    """

    class Address(EmbeddedJsonModel):
        street: str = Field(index=True)

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Sink(BaseJsonModel, index=True):
        hidden: str = Field(index=False, default="")
        tags: List[str] = Field(index=True, default_factory=list)
        # Bare ``List`` (no inner type): OM's own schema generation logs a
        # warning and skips the field; the redisvl conversion must skip it
        # too (container with no ``str`` inner type).
        raw_list: List = Field(index=True, default_factory=list)
        address: Optional[Address] = Field(default=None)
        location: Optional[Coordinates] = Field(index=True, default=None)
        blob: dict = Field(index=True, default_factory=dict)
        vec_flat: List[float] = Field(
            default_factory=list,
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                initial_cap=1000,
                block_size=64,
            ),
        )
        vec_hnsw: List[float] = Field(
            default_factory=list,
            vector_options=VectorFieldOptions.hnsw(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=DIMENSIONS,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.L2,
                initial_cap=1000,
                m=32,
                ef_construction=400,
                ef_runtime=50,
                epsilon=0.05,
            ),
        )

    return Sink


@pytest_asyncio.fixture
async def hash_field_mapping_model(key_prefix):
    """Hash model with a List[str] field (the hash-storage variant of the
    tag-list conversion, which takes no JSONPath)."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class SinkHash(BaseHashModel, index=True):
        # ``Optional[List[str]]`` is the supported spelling for hash models:
        # a plain ``List[str]`` annotation raises ``RedisModelError`` in
        # ``HashModel.__init_subclass__`` (hash models cannot index set, list,
        # or mapping fields), while the Optional-wrapped form bypasses that
        # check and OM renders it as a plain TAG.
        tags: Optional[List[str]] = Field(index=True, default=None)

    return SinkHash


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


class TestToRedisvlSchemaFieldMapping:
    """Branch coverage for ``_get_field_type`` via ``to_redisvl_schema``.

    Expected shapes are grounded in the RediSearch docs (FT.CREATE and vector
    search reference pages):

    - GEO fields hold "longitude,latitude" strings.
    - JSON identifiers are JSONPath expressions — arrays use ``$.field[*]``.
    - HNSW exposes M / EF_CONSTRUCTION / EF_RUNTIME / EPSILON with documented
      defaults 16 / 200 / 10 / 0.01. The values below are deliberately
      non-default so the tests prove user values are forwarded, not that the
      server defaults round-trip.
    - FLAT's INITIAL_CAP / BLOCK_SIZE are accepted by the server (verified
      against a live Redis 8.8, which rejects unknown vector attributes) but
      are not echoed by FT.INFO, so they are asserted at the schema level.
    """

    @py_test_mark_asyncio
    async def test_index_false_field_is_skipped(self, json_field_mapping_model):
        schema = to_redisvl_schema(json_field_mapping_model)
        assert "hidden" not in schema.fields

    @py_test_mark_asyncio
    async def test_geo_field(self, json_field_mapping_model):
        field = to_redisvl_schema(json_field_mapping_model).fields["location"]
        assert field.type.value == "geo"

    @py_test_mark_asyncio
    async def test_json_list_of_strings_uses_wildcard_path(
        self, json_field_mapping_model
    ):
        field = to_redisvl_schema(json_field_mapping_model).fields["tags"]
        assert field.type.value == "tag"
        assert field.path == "$.tags[*]"

    @py_test_mark_asyncio
    async def test_json_bare_list_is_skipped(self, json_field_mapping_model):
        """Containers without a ``str`` inner type are skipped (OM's own
        Migrator skips them too, with a warning)."""
        assert "raw_list" not in to_redisvl_schema(json_field_mapping_model).fields

    @py_test_mark_asyncio
    async def test_hash_list_of_strings_has_no_path(self, hash_field_mapping_model):
        field = to_redisvl_schema(hash_field_mapping_model).fields["tags"]
        assert field.type.value == "tag"
        assert not field.path

    @py_test_mark_asyncio
    async def test_embedded_model_field_is_skipped(self, json_field_mapping_model):
        schema = to_redisvl_schema(json_field_mapping_model)
        assert "address" not in schema.fields

    @py_test_mark_asyncio
    async def test_unknown_type_defaults_to_tag(self, json_field_mapping_model):
        field = to_redisvl_schema(json_field_mapping_model).fields["blob"]
        assert field.type.value == "tag"

    @py_test_mark_asyncio
    async def test_flat_vector_knobs_round_trip(self, json_field_mapping_model):
        attrs = to_redisvl_schema(json_field_mapping_model).fields["vec_flat"].attrs
        assert attrs.initial_cap == 1000
        assert attrs.block_size == 64
        assert attrs.algorithm.value == "FLAT"

    @py_test_mark_asyncio
    async def test_hnsw_vector_knobs_round_trip(self, json_field_mapping_model):
        attrs = to_redisvl_schema(json_field_mapping_model).fields["vec_hnsw"].attrs
        assert attrs.algorithm.value == "HNSW"
        assert attrs.m == 32
        assert attrs.ef_construction == 400
        assert attrs.ef_runtime == 50
        assert attrs.epsilon == 0.05
        assert attrs.initial_cap == 1000

    @py_test_mark_asyncio
    async def test_hnsw_attrs_reach_the_server(self, json_field_mapping_model, redis):
        """End to end: the converted schema's FT.CREATE must be accepted by
        the server and echo the user-supplied HNSW knobs back via FT.INFO.

        FT.INFO echoes M / ef_construction / ef_runtime (verified against a
        live Redis 8.8); EPSILON is documented but not echoed.
        """
        index = get_redisvl_index(json_field_mapping_model)
        await index.create()
        try:
            info = await redis.ft(index.schema.index.name).info()
            vector_attrs = None
            for entry in info["attributes"]:
                # RESP2 returns flat [k, v, k, v, ...] lists; RESP3 (and
                # redis-py >= 8 unified responses) return dicts already.
                if isinstance(entry, dict):
                    pairs = entry
                else:
                    pairs = dict(zip(entry[0::2], entry[1::2]))
                if pairs.get("attribute") == "vec_hnsw":
                    vector_attrs = pairs
                    break
            assert vector_attrs is not None
            assert vector_attrs["algorithm"] == "HNSW"
            assert int(vector_attrs["dim"]) == DIMENSIONS
            assert int(vector_attrs["M"]) == 32
            assert int(vector_attrs["ef_construction"]) == 400
            assert int(vector_attrs["ef_runtime"]) == 50
        finally:
            await index.delete()


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


class TestResp3ReplyShapeMatrix:
    """End-to-end over the redis-py reply-shape matrix.

    redis-py picks the FT.SEARCH parser from (``protocol``,
    ``legacy_responses``); only an explicit ``protocol=3`` with legacy
    responses yields the native dict shape redisvl cannot parse (it reads
    ``results.docs``). Verified against redis-py 7.4.1 and 8.1.0, redisvl
    0.27.2 and 0.28.0. These tests pin the *actual* end-to-end behavior:
    redisvl queries must succeed through OM on every connection mode.
    """

    @staticmethod
    def _client(conn, **extra):
        url = redis_url(protocol=None)
        return get_redis_connection(url=url, **extra)

    @staticmethod
    async def _make_model(conn, key_prefix):
        class BaseJsonModel(JsonModel, abc.ABC):
            class Meta:
                global_key_prefix = key_prefix
                database = conn

        class Document(BaseJsonModel, index=True):
            title: str = Field(index=True)
            body: str = Field(full_text_search=True)
            embedding: List[float] = Field(vector_options=_flat_vector_options())

        await Migrator(conn=conn).run()
        await Document(
            title="running shoes",
            body="light running shoes",
            embedding=[1.0] + [0.0] * (DIMENSIONS - 1),
        ).save()
        return Document

    @staticmethod
    async def _vector_query(Document):
        index = get_redisvl_index(Document)
        results = await index.query(
            VectorQuery(
                vector=[1.0] + [0.0] * (DIMENSIONS - 1),
                vector_field_name="embedding",
                num_results=1,
            )
        )
        return results

    @py_test_mark_asyncio
    async def test_default_connection(self, key_prefix):
        conn = self._client(None)
        Document = await self._make_model(conn, key_prefix)
        results = await self._vector_query(Document)
        assert len(results) == 1

    @py_test_mark_asyncio
    async def test_explicit_protocol_2(self, key_prefix):
        conn = self._client(None, protocol=2)
        Document = await self._make_model(conn, key_prefix)
        results = await self._vector_query(Document)
        assert len(results) == 1

    @py_test_mark_asyncio
    async def test_explicit_protocol_3_is_adapted(self, key_prefix):
        """The one mode redisvl cannot parse natively — must be adapted."""
        conn = self._client(None, protocol=3)
        Document = await self._make_model(conn, key_prefix)
        results = await self._vector_query(Document)
        assert len(results) == 1

    @py_test_mark_asyncio
    async def test_protocol_3_with_legacy_responses_false(self, key_prefix):
        """Unified RESP3 replies still carry ``.docs`` — passed through."""
        if REDIS_PY_LT_8:
            pytest.skip("legacy_responses requires redis-py >= 8")
        conn = self._client(None, protocol=3, legacy_responses=False)
        Document = await self._make_model(conn, key_prefix)
        results = await self._vector_query(Document)
        assert len(results) == 1

    @py_test_mark_asyncio
    async def test_decode_responses_false(self, key_prefix):
        conn = self._client(None, decode_responses=False)
        Document = await self._make_model(conn, key_prefix)
        results = await self._vector_query(Document)
        assert len(results) == 1

    @py_test_mark_asyncio
    async def test_decode_responses_false_with_protocol_3(self, key_prefix):
        conn = self._client(None, decode_responses=False, protocol=3)
        Document = await self._make_model(conn, key_prefix)
        results = await self._vector_query(Document)
        assert len(results) == 1

    @py_test_mark_asyncio
    async def test_native_resp3_client_is_adapted_to_default_protocol(self, key_prefix):
        """The adaptation clone keeps RESP3 on the wire (redis-py >= 8)
        instead of downgrading to RESP2."""
        conn = self._client(None, protocol=3)
        from aredis_om.ai._connection import (
            _redisvl_compatible_client,
            _redisvl_native_resp3_shapes,
        )

        assert _redisvl_native_resp3_shapes(conn)
        adapted = _redisvl_compatible_client(conn)
        assert adapted is not conn
        assert not _redisvl_native_resp3_shapes(adapted)
        # The clone uses redis-py's *default* protocol — not a forced RESP2.
        clone_protocol = adapted.connection_pool.connection_kwargs.get("protocol")
        if REDIS_PY_LT_8:
            # redis-py 7.x hard-codes ``protocol=2`` as its constructor default.
            assert clone_protocol == 2
        else:
            assert clone_protocol is None  # -> RESP3 wire, legacy reply shapes
        # Same flavour (async in / async out).
        assert type(adapted) is type(conn)

    @py_test_mark_asyncio
    async def test_hybrid_search_native_resp3(self, key_prefix, redis):
        if not await _has_ft_hybrid(redis):
            pytest.skip("FT.HYBRID requires Redis 8.4+")
        conn = self._client(None, protocol=3)
        Document = await self._make_model(conn, key_prefix)
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
                num_results=1,
            ),
        )
        assert isinstance(results, list)


class TestRedisvlSyncToAsyncTwinCompatibility:
    """RedisVL derives an async client from a sync one by copying the pool's
    ``connection_kwargs`` verbatim
    (``RedisConnectionFactory.sync_to_async_redis``).

    redis-py stores maintenance-notification state in those kwargs whenever
    the connection speaks RESP3, and redis-py < 8.1's *async* ``Connection``
    rejects those kwargs:
    ``TypeError: AbstractConnection.__init__() got an unexpected keyword
    argument 'maint_notifications_config'``. redis-py 8.1+ accepts them.

    The client OM hands redisvl must therefore not carry kwargs the local
    redis-py's async connection cannot accept.
    """

    @staticmethod
    def _sync_client(**extra):
        return get_redis_connection(url=redis_url(protocol=None), **extra)

    @py_test_mark_asyncio
    async def test_sync_client_has_redisvl_compatible_pool_kwargs(self):
        from aredis_om.ai._connection import (
            _MAINT_NOTIFICATION_POOL_KWARGS,
            _async_connection_accepts,
            sync_client_for_model,
        )

        sync_client = self._sync_client()
        try:
            twin = sync_client_for_model(client=sync_client)
            pool_kwargs = twin.connection_pool.connection_kwargs

            for kwarg in _MAINT_NOTIFICATION_POOL_KWARGS:
                if kwarg in pool_kwargs:
                    # Present only when this redis-py accepts it on the
                    # async connection; otherwise OM strips it.
                    assert _async_connection_accepts(kwarg), (
                        f"{kwarg} would break redisvl's async twin on this "
                        "redis-py version"
                    )
        finally:
            twin.close()

    @py_test_mark_asyncio
    async def test_async_twin_can_be_derived_by_redisvl(self):
        """The exact operation redisvl performs: sync client -> async twin."""
        from redisvl.redis.connection import RedisConnectionFactory

        from aredis_om.ai._connection import sync_client_for_model

        sync_client = self._sync_client()
        try:
            twin = sync_client_for_model(client=sync_client)
            async_twin = RedisConnectionFactory.sync_to_async_redis(twin)
            try:
                assert await async_twin.ping() is True
            finally:
                await async_twin.aclose()
        finally:
            twin.close()

    @py_test_mark_asyncio
    async def test_async_twin_derivable_from_explicit_protocol_3(self):
        """Explicit protocol=3 clients hit the same path (RESP3 pool)."""
        from redisvl.redis.connection import RedisConnectionFactory

        from aredis_om.ai._connection import sync_client_for_model

        sync_client = self._sync_client(protocol=3)
        try:
            twin = sync_client_for_model(client=sync_client)
            async_twin = RedisConnectionFactory.sync_to_async_redis(twin)
            try:
                assert await async_twin.ping() is True
            finally:
                await async_twin.aclose()
        finally:
            twin.close()


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

    @py_test_mark_asyncio
    async def test_cluster_combination_method_args_included(
        self, cluster_model_and_index, monkeypatch
    ):
        """The COMBINE clause must be forwarded on the cluster path.

        The FT.HYBRID reference documents ``COMBINE LINEAR count [[ALPHA a]
        [BETA b]]``; redisvl's ``CombineResultsMethod.get_args()`` emits exactly
        that (count prefixes the tokens that follow).
        """
        ClusterDoc, index, cluster_client = cluster_model_and_index

        captured = {}

        async def fake_execute_command(*args, **kwargs):
            captured["args"] = args
            return ["raw-response"]

        class _FakeSearchCommands:
            def get_params_args(self, params):
                return []

            def _parse_results(self, cmd, res, **kwargs):
                return SimpleNamespace(results=[{"id": "doc1"}])

        monkeypatch.setattr(cluster_client, "execute_command", fake_execute_command)
        monkeypatch.setattr(cluster_client, "get_default_node", lambda: object())
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
            combination_method="LINEAR",
            linear_alpha=0.7,
            num_results=5,
        )
        results = await hybrid_search(index, query)

        args = captured["args"]
        assert args[0] == "FT.HYBRID"
        # COMBINE LINEAR count ALPHA a BETA b (documented syntax)
        combine_at = args.index("COMBINE")
        assert args[combine_at + 1] == "LINEAR"
        assert "ALPHA" in args
        assert "BETA" in args
        assert results == [{"id": "doc1"}]


# --------------------------------------------------------------------------
# Maintenance upgrade surfaces (M0).
# --------------------------------------------------------------------------


class TestM0Maintenance:
    """M0-A1 + M0-A2: redisvl version is current, private-API guard works."""

    def test_redisvl_version_meets_floor(self):
        """aredis_om pins ``redisvl>=0.27.2``; runtime must match or exceed."""
        from importlib.metadata import version as _pkg_version

        raw = _pkg_version("redisvl")
        parts = [int(p) for p in raw.split(".")[:3] if p.isdigit()]
        assert (parts + [0, 0, 0])[:3] >= [0, 27, 2], (
            f"redisvl=={raw} but aredis_om requires >=0.27.2"
        )

    def test_public_redisvl_surface_available(self):
        """The public helpers exported by ``aredis_om.redisvl`` exist and
        are imported from the local module, not just redisvl."""
        for name in (
            "to_redisvl_schema",
            "get_redisvl_index",
            "hybrid_search",
            "to_redisvl_mcp_config",
        ):
            assert hasattr(om_redisvl, name), f"missing helper: {name}"

    @py_test_mark_asyncio
    async def test_hybrid_search_guard_raises_helpful_error(
        self, json_document_model, monkeypatch
    ):
        """M0-A2: a future redisvl rename of the private helper we rely on
        surfaces a pinned-version hint, not a bare ``ImportError`` deep in
        the pipeline. The guard fires only on the cluster path — force
        ``_is_cluster_client`` to True so the FT.HYBRID codepath runs."""
        Document = json_document_model
        index = get_redisvl_index(Document)

        # Force the cluster path regardless of the actual index client.
        monkeypatch.setattr(om_redisvl, "_is_cluster_client", lambda c: True)

        # Drop the private symbol redisvl exposes in 0.27.x so the guard
        # triggers even on a server that advertises FT.HYBRID support.
        from redisvl.index import index as redisvl_index_module

        saved = redisvl_index_module.__dict__.pop("_convert_and_drop_empty_rows", None)
        try:
            from redisvl.query import HybridQuery

            query = HybridQuery(
                text="x",
                text_field_name="body_fts",
                vector=[0.0] * DIMENSIONS,
                vector_field_name="embedding",
            )
            with pytest.raises(ImportError, match="hybrid_search"):
                await hybrid_search(index, query)
        finally:
            if saved is not None:
                redisvl_index_module._convert_and_drop_empty_rows = saved


class TestMcpConfig:
    """U7: ``to_redisvl_mcp_config`` shapes a dict that matches redisvl's MCP server config schema."""

    def test_fulltext_binding(self, json_document_model):
        from aredis_om.redisvl import to_redisvl_mcp_config

        Document = json_document_model
        cfg = to_redisvl_mcp_config(
            [Document], redis_url="redis://localhost:6379", search_type="fulltext"
        )
        assert cfg["server"]["redis_url"] == "redis://localhost:6379"
        assert cfg["server"]["builtin_tools"]["upsert-records"] == "disabled"

        binding = cfg["indexes"][Document.__name__]
        assert binding["redis_name"] == Document.Meta.index_name
        assert binding["search"]["type"] == "fulltext"
        assert binding["runtime"]["text_field_name"].endswith("_fts")
        assert "vectorizer" not in binding
        assert binding["read_only"] is True

    def test_vector_binding_requires_vectorizer(self, key_prefix):
        """Models with a vector field but no vectorizer fail with a vectorizer hint."""
        import abc as _abc

        from aredis_om.redisvl import to_redisvl_mcp_config

        class Base(JsonModel, _abc.ABC):
            class Meta:
                global_key_prefix = key_prefix

        class WithVec(Base, index=True):
            body: str
            embedding: List[float] = Field(
                vector_options=VectorFieldOptions.flat(
                    type=VectorFieldOptions.TYPE.FLOAT32,
                    dimension=DIMENSIONS,
                    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                )
            )

        with pytest.raises(ValueError, match="vectorizer"):
            to_redisvl_mcp_config(
                [WithVec],
                redis_url="redis://localhost:6379",
                search_type="vector",
            )

    def test_vectorizer_from_auto_embedding(self, key_prefix):
        """Auto-embedding-derived vectorizer is forwarded verbatim."""
        import abc as _abc

        from aredis_om.ai import build_embedding_specs
        from aredis_om.redisvl import to_redisvl_mcp_config

        class Base(JsonModel, _abc.ABC):
            class Meta:
                global_key_prefix = key_prefix

        # Wire up the spec collection manually since it relies on the model
        # class module path; this keeps the test self-contained.
        class DocWithVec(Base, index=True):
            body: str
            embedding: List[float] = Field(
                vector_options=VectorFieldOptions.flat(
                    type=VectorFieldOptions.TYPE.FLOAT32,
                    dimension=DIMENSIONS,
                    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
                ),
                vectorizer="openai:text-embedding-3-small",
                source="body",
            )

        DocWithVec._meta.embedding_fields = build_embedding_specs(DocWithVec)

        cfg = to_redisvl_mcp_config(
            [DocWithVec], search_type="vector", redis_url="redis://localhost"
        )
        binding = cfg["indexes"][DocWithVec.__name__]
        assert binding["vectorizer"]["class"] == "OpenAITextVectorizer"
        assert binding["vectorizer"]["model"] == "text-embedding-3-small"
        assert binding["runtime"]["vector_field_name"] == "embedding"

    def test_json_schema_override_path_validated(self, json_document_model):
        """JSON overrides must use ``$.<field>`` paths; a path the MCP server
        would reject at startup fails early with a JSON-specific message."""
        from aredis_om.redisvl import to_redisvl_mcp_config

        Document = json_document_model
        binding_id = Document.__name__

        # A matching JSON path is accepted and passed through verbatim.
        ok = to_redisvl_mcp_config(
            [Document],
            search_type="fulltext",
            schema_overrides={
                binding_id: {
                    "fields": [
                        {"name": "body_fts", "type": "text", "path": "$.body_fts"}
                    ]
                }
            },
        )
        assert (
            ok["indexes"][binding_id]["schema_overrides"]["fields"][0]["path"]
            == "$.body_fts"
        )

        # A bare (hash-style) path would be rejected by the server; fail early.
        with pytest.raises(ValueError, match="JSON schema_overrides paths"):
            to_redisvl_mcp_config(
                [Document],
                search_type="fulltext",
                schema_overrides={
                    binding_id: {
                        "fields": [
                            {"name": "body_fts", "type": "text", "path": "body_fts"}
                        ]
                    }
                },
            )
