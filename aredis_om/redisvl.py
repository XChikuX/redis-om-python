"""RedisVL integration for Redis OM.

This module is an *escape hatch* into `RedisVL <https://github.com/redis/redis-vl-python>`_:
it converts a Redis OM model into a RedisVL ``IndexSchema``
(:func:`to_redisvl_schema`) and hands out a ready-to-use
``AsyncSearchIndex``/``SearchIndex`` wired to the model's ``Meta.database``
(:func:`get_redisvl_index`). Advanced search (``FT.HYBRID``, aggregations,
vector policies, SVS-VAMANA) stays in RedisVL — OM does not reimplement it.

The generated schema is *faithful to the index OM's own ``Migrator``
creates*, so indexes built by either engine are interchangeable:

- ``full_text_search=True`` strings produce the same dual fields OM renders
  (``body`` TAG + ``body_fts`` TEXT, the latter aliased to the same JSON
  path / hash field), so ``HybridQuery(text_field_name="body_fts")`` works
  against an OM-migrated index and OM queries keep working on a
  RedisVL-created one.
- ``List[str]`` tag fields use the ``$.field[*]`` JSON path OM emits.
- Vector attrs (dims, distance metric, algorithm, datatype, and the
  FLAT/HNSW tuning knobs) map one-to-one from ``VectorFieldOptions``.

RedisVL is an **optional** dependency — install it with::

    pip install 'pyredis-om[redisvl]'

This module imports cleanly without it; the helpers raise a helpful
``ImportError`` only when called.

Example::

    from aredis_om import Field, JsonModel, VectorFieldOptions
    from aredis_om.redisvl import get_redisvl_index, to_redisvl_schema

    class Document(JsonModel, index=True):
        title: str = Field(index=True)
        body: str = Field(full_text_search=True)
        embedding: list[float] = Field(
            vector_options=VectorFieldOptions.flat(
                type=VectorFieldOptions.TYPE.FLOAT32,
                dimension=384,
                distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
            )
        )

    # A RedisVL IndexSchema for advanced operations
    schema = to_redisvl_schema(Document)

    # Or a ready-to-use index wired to the model's connection
    index = get_redisvl_index(Document)
    results = await index.query(VectorQuery(
        vector=[0.1] * 384,
        vector_field_name="embedding",
        num_results=10,
    ))

    # Hybrid text+vector search (Redis 8.4+, redis-py 7.1+); routes
    # FT.HYBRID correctly when Meta.database is a Redis Cluster client.
    from redisvl.query import HybridQuery

    results = await hybrid_search(index, HybridQuery(
        text="running shoes",
        text_field_name="body_fts",
        vector=[0.1] * 384,
        vector_field_name="embedding",
        combination_method="LINEAR",
        linear_alpha=0.5,
    ))
"""

import datetime
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from ._compat import FieldInfo as PydanticFieldInfo
from .model.model import (
    SINGLE_VALUE_TAG_FIELD_SEPARATOR,
    Coordinates,
    FieldInfo,
    JsonModel,
    RedisModel,
    VectorFieldOptions,
    get_outer_type,
    is_numeric_type,
    is_supported_container_type,
    should_index_field,
)

if TYPE_CHECKING:  # pragma: no cover
    from redisvl.index import AsyncSearchIndex, SearchIndex
    from redisvl.query import HybridQuery
    from redisvl.schema import IndexSchema


log = logging.getLogger(__name__)

_LAZY_IMPORT_MESSAGE = (
    "The RedisVL integration requires the 'redisvl' package. "
    "Install it with: pip install 'pyredis-om[redisvl]'"
)


def _import_redisvl():
    """Import redisvl lazily, raising a helpful error when it is missing."""
    try:
        import redisvl
    except ImportError as e:  # pragma: no cover - exercised without extra
        raise ImportError(_LAZY_IMPORT_MESSAGE) from e
    return redisvl


def _is_cluster_client(client: Any) -> bool:
    """True when ``client`` is an async Redis Cluster client."""
    return isinstance(client, AsyncRedisCluster)


def _get_field_type(
    field_name: str,
    field_type: Any,
    field_info: FieldInfo | PydanticFieldInfo,
    is_json: bool,
) -> List[Dict[str, Any]]:
    """Convert an OM field to RedisVL field definitions.

    Returns a *list* because one OM field can map to several RediSearch
    fields: ``full_text_search=True`` strings are indexed by OM as both a
    TAG (exact match, plain field name) and a TEXT (full-text, ``_fts``
    suffix) field over the same value, and the generated schema mirrors
    that so OM- and RedisVL-created indexes stay interchangeable.
    """
    if not should_index_field(field_info, class_index_default=True):
        return []

    vector_options: Optional[VectorFieldOptions] = getattr(
        field_info, "vector_options", None
    )
    sortable = getattr(field_info, "sortable", False) is True
    full_text_search = getattr(field_info, "full_text_search", False) is True
    case_sensitive = getattr(field_info, "case_sensitive", False) is True
    separator = getattr(field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR)

    # Vector field — attrs map one-to-one from VectorFieldOptions.
    if vector_options:
        attrs: Dict[str, Any] = {
            "dims": vector_options.dimension,
            "distance_metric": vector_options.distance_metric.name.lower(),
            # ``.value`` (not ``.name``) so SVS renders as "svs-vamana" —
            # redisvl's algorithm discriminator string. FLAT/HNSW are
            # unchanged (name == value for them).
            "algorithm": vector_options.algorithm.value.lower(),
            "datatype": vector_options.type.name.lower(),
        }
        if vector_options.initial_cap:
            attrs["initial_cap"] = vector_options.initial_cap
        if vector_options.algorithm.name == "FLAT":
            if vector_options.block_size:
                attrs["block_size"] = vector_options.block_size
        elif vector_options.algorithm.name == "HNSW":
            if vector_options.m:
                attrs["m"] = vector_options.m
            if vector_options.ef_construction:
                attrs["ef_construction"] = vector_options.ef_construction
            if vector_options.ef_runtime:
                attrs["ef_runtime"] = vector_options.ef_runtime
            if vector_options.epsilon:
                attrs["epsilon"] = vector_options.epsilon
        elif vector_options.algorithm.name == "SVS":
            # SVS-VAMANA attributes (Redis 8.2+). ``compression`` must match
            # one of redisvl's enum values exactly: LVQ4, LVQ4x4, LVQ4x8,
            # LVQ8 (all already uppercase) and LeanVec4x8 / LeanVec8x8
            # (mixed case — RedisVL's ``CompressionType`` ``Enum`` values).
            if vector_options.compression:
                attrs["compression"] = vector_options.compression
            if vector_options.construction_window_size:
                attrs["construction_window_size"] = (
                    vector_options.construction_window_size
                )
            if vector_options.graph_max_degree:
                attrs["graph_max_degree"] = vector_options.graph_max_degree
            if vector_options.search_window_size:
                attrs["search_window_size"] = vector_options.search_window_size
            if vector_options.training_threshold:
                attrs["training_threshold"] = vector_options.training_threshold
            if vector_options.reduce:
                attrs["reduce"] = vector_options.reduce
            if vector_options.epsilon:
                attrs["epsilon"] = vector_options.epsilon
        return [{"name": field_name, "type": "vector", "attrs": attrs}]

    # Boolean — OM indexes it as a bare TAG (RediSearch's default
    # separator applies, so no separator attr is needed for fidelity).
    # Must be checked before numeric: ``bool`` subclasses ``int``.
    if field_type is bool:
        return [{"name": field_name, "type": "tag", "attrs": {"sortable": sortable}}]

    # Numeric field (also datetime/date, which OM indexes as NUMERIC).
    if is_numeric_type(field_type) or field_type in (
        datetime.datetime,
        datetime.date,
    ):
        return [
            {"name": field_name, "type": "numeric", "attrs": {"sortable": sortable}}
        ]

    # Geo field (OM's Coordinates type).
    if field_type is Coordinates:
        return [{"name": field_name, "type": "geo", "attrs": {"sortable": sortable}}]

    # String field.
    if isinstance(field_type, type) and issubclass(field_type, str):
        if full_text_search:
            # Dual mapping matching OM's own FT.CREATE rendering:
            # ``body TAG SEPARATOR |`` + ``body AS body_fts TEXT`` (hash)
            # or ``$.body AS body TAG ...`` + ``$.body AS body_fts TEXT``
            # (json). The TEXT field is named ``{name}_fts`` and points at
            # the same underlying value.
            tag_field: Dict[str, Any] = {
                "name": field_name,
                "type": "tag",
                "attrs": {"separator": separator, "case_sensitive": case_sensitive},
            }
            text_field: Dict[str, Any] = {
                "name": f"{field_name}_fts",
                "type": "text",
                "attrs": {"sortable": sortable},
            }
            if is_json:
                text_field["path"] = f"$.{field_name}"
            # For hash storage, RedisVL's IndexSchema strips ``path`` (hash
            # fields don't take JSON paths), so ``to_redisvl_schema`` sets
            # the aliasing path on the field object after construction.
            else:
                text_field["path"] = field_name
            return [tag_field, text_field]
        return [
            {
                "name": field_name,
                "type": "tag",
                "attrs": {
                    "separator": separator,
                    "case_sensitive": case_sensitive,
                    "sortable": sortable,
                },
            }
        ]

    # List of strings -> TAG. OM indexes JSON arrays as ``$.field[*]``.
    if is_supported_container_type(field_type):
        from typing import get_args

        inner_types = get_args(field_type)
        if inner_types and inner_types[0] is str:
            tag_list_field: Dict[str, Any] = {
                "name": field_name,
                "type": "tag",
                "attrs": {"separator": separator, "sortable": sortable},
            }
            if is_json:
                tag_list_field["path"] = f"$.{field_name}[*]"
            return [tag_list_field]
        return []

    # Embedded models are expanded by OM into per-sub-field index entries
    # (``address_city`` etc.). Flattening them here would duplicate OM's
    # recursion, so they are skipped — the schema remains usable for
    # querying an OM-created index.
    if isinstance(field_type, type) and issubclass(field_type, RedisModel):
        log.debug(
            "Skipping embedded model field %r in RedisVL schema conversion; "
            "OM indexes its sub-fields individually.",
            field_name,
        )
        return []

    # Default to tag for unknown types (matches upstream behavior).
    return [{"name": field_name, "type": "tag"}]


def to_redisvl_schema(model_cls: Type[RedisModel]) -> "IndexSchema":
    """Convert a Redis OM model to a RedisVL ``IndexSchema``.

    The schema is faithful to the index OM's ``Migrator`` creates for the
    same model (same index name, key prefix, storage type, and field
    names — including the ``_fts`` TEXT aliases), so indexes built by
    either engine are interchangeable.

    Args:
        model_cls: A HashModel or JsonModel class declared with
            ``index=True``.

    Returns:
        A RedisVL ``IndexSchema`` usable with ``SearchIndex`` /
        ``AsyncSearchIndex``.

    Raises:
        ValueError: If the model is not indexed.
        ImportError: If the optional ``redisvl`` package is not installed.

    Example::

        schema = to_redisvl_schema(MyModel)
        index = AsyncSearchIndex(schema=schema, redis_client=redis)
        results = await index.query(VectorQuery(...))
    """
    _import_redisvl()
    from redisvl.schema import IndexSchema

    # Indexed check: class-level ``index=True`` is stored by the metaclass
    # on ``_meta.index_enabled``; ``model_config['index']`` is the
    # Pydantic-level copy.
    model_config = getattr(model_cls, "model_config", {}) or {}
    is_indexed = bool(
        (isinstance(model_config, dict) and model_config.get("index") is True)
        or getattr(getattr(model_cls, "_meta", None), "index_enabled", False)
    )
    if not is_indexed:
        raise ValueError(
            f"Model {model_cls.__name__} is not indexed. "
            "Use 'class MyModel(JsonModel, index=True):' to enable indexing."
        )

    is_json = issubclass(model_cls, JsonModel)
    storage_type = "json" if is_json else "hash"

    index_name = model_cls.Meta.index_name
    key_prefix = model_cls.make_key("")

    fields: List[Dict[str, Any]] = []
    for name, field in model_cls.model_fields.items():
        field_type = get_outer_type(field)
        if field_type is None:
            # Unreachable for annotations pydantic accepts (``typing.Any`` is a class
            # instance on 3.11+, bare Union forms are rejected at class-creation).
            # Kept as a guard.
            continue  # pragma: no cover

        # Get FieldInfo (Pydantic may wrap in ``Annotated`` metadata).
        # Fork's OM attributes (``vector_options`` etc.) are properties on base FieldInfo,
        # so plain instances (e.g. metaclass-created ``pk``) work too.
        # Pydantic v2 stores FieldInfo in ``model_fields``; metadata branches are defensive.
        if (
            not isinstance(field, PydanticFieldInfo)
            and hasattr(field, "metadata")
            and len(field.metadata) > 0
            and isinstance(field.metadata[0], PydanticFieldInfo)
        ):
            field_info = field.metadata[0]  # pragma: no cover
        elif isinstance(field, PydanticFieldInfo):
            field_info = field
        else:
            continue  # pragma: no cover

        fields.extend(_get_field_type(name, field_type, field_info, is_json))

    schema = IndexSchema.from_dict(
        {
            "index": {
                "name": index_name,
                "prefix": key_prefix,
                "storage_type": storage_type,
            },
            "fields": fields,
        }
    )

    if not is_json:
        # Hash storage: IndexSchema forces ``path=None`` on construction,
        # but RedisVL's field rendering uses ``path`` as the raw field
        # reference with ``name`` as the alias. Setting it afterwards makes
        # the ``_fts`` TEXT field render as ``body AS body_fts TEXT`` —
        # exactly what OM's Migrator emits for hash models.
        for field_name, field_def in schema.fields.items():
            if field_name.endswith("_fts") and field_def.type == "text":
                field_def.path = field_name[: -len("_fts")]

    return schema


def get_redisvl_index(
    model_cls: Type[RedisModel],
    async_client: bool = True,
) -> "AsyncSearchIndex":
    """Get a RedisVL ``SearchIndex`` for a Redis OM model.

    The index is wired to the model's ``Meta.database`` connection — no
    user-managed connections needed. Index lifecycle stays with the
    caller: use ``await index.create()`` (or OM's ``Migrator``) before
    querying.

    Args:
        model_cls: A HashModel or JsonModel class declared with
            ``index=True``.
        async_client: If True (default), return an ``AsyncSearchIndex``.
            If False, return a sync ``SearchIndex``.

    Returns:
        A RedisVL search index connected to Redis.

    Raises:
        ValueError: If the model is not indexed.
        ImportError: If the optional ``redisvl`` package is not installed.

    Example::

        index = get_redisvl_index(MyModel)
        results = await index.query(VectorQuery(
            vector=query_embedding,
            vector_field_name="embedding",
            num_results=10,
        ))
    """
    schema = to_redisvl_schema(model_cls)
    redis_client = model_cls.db()

    if async_client:
        from redisvl.index import AsyncSearchIndex

        return AsyncSearchIndex(schema=schema, redis_client=redis_client)
    else:
        from redisvl.index import SearchIndex

        return SearchIndex(schema=schema, redis_client=redis_client)


async def hybrid_search(
    index: "AsyncSearchIndex",
    query: "HybridQuery",
    timeout: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Execute a RedisVL ``HybridQuery``, with Redis Cluster support.

    ``FT.HYBRID`` (Redis 8.4+, redis-py 7.1+) targets a single index, and
    an index name hashes to a single cluster slot — so on a cluster client
    the command must be pinned to one node. RedisVL ships cluster helpers
    for ``FT.SEARCH``/``FT.CREATE`` but none for ``FT.HYBRID``; this
    function fills the gap by mirroring RedisVL's own
    ``async_cluster_search`` routing (``target_nodes=[default_node]``).

    On non-cluster clients this simply delegates to ``index.query()``.

    Args:
        index: A RedisVL ``AsyncSearchIndex`` (e.g. from
            :func:`get_redisvl_index`).
        query: A RedisVL ``HybridQuery``.
        timeout: Optional server-side timeout in milliseconds. Applied on
            cluster clients only (appended as the ``FT.HYBRID`` ``TIMEOUT``
            argument); the non-cluster path delegates to redisvl's
            ``index.query()``, which takes no timeout parameter.

    Returns:
        The hybrid search results as a list of dicts, ordered by combined
        score — the same shape ``index.query(HybridQuery(...))`` returns.
    """
    client = getattr(index, "_redis_client", None) or await index._get_client()

    if not _is_cluster_client(client):
        # redisvl's ``query()`` takes no timeout parameter, so the timeout
        # can only be honored on the cluster path below, where we build
        # the FT.HYBRID command ourselves.
        return await index.query(query)

    # Cluster path: build the FT.HYBRID command exactly the way redis-py's
    # ``hybrid_search`` does, then pin it to the default node (an index
    # name hashes to a single slot, so fan-out is neither possible nor
    # correct — this mirrors redisvl's async_cluster_search).
    #
    # ``_convert_and_drop_empty_rows`` is a *private* redisvl symbol (0.27.x)
    # and ``get_protocol_version`` is undocumented — both can drift across
    # minor releases. Load them through the compat shim below so a rename
    # raises a clear, actionable error instead of an ImportError deep in
    # this function.
    from redis.client import NEVER_DECODE

    try:
        from redisvl.index.index import _convert_and_drop_empty_rows
        from redisvl.utils.redis_protocol import get_protocol_version
    except ImportError as e:
        raise ImportError(
            "redis-om's hybrid_search() relies on redisvl internals that "
            "changed in this redisvl version (expected in 0.27.x: "
            "redisvl.index.index._convert_and_drop_empty_rows, "
            "redisvl.utils.redis_protocol.get_protocol_version). Pin "
            "redisvl to a compatible version or open an issue at "
            "https://github.com/redis/redis-om-python."
        ) from e

    HYBRID_CMD = "FT.HYBRID"
    index_name = index.schema.index.name
    index._validate_hybrid_query(query)

    ft = client.ft(index_name)
    pieces: List[Any] = [HYBRID_CMD, index_name]
    pieces.extend(query.query.get_args())
    if query.combination_method is not None:
        pieces.extend(query.combination_method.get_args())
    post_processing = query.postprocessing_config
    if post_processing is not None and post_processing.build_args():
        pieces.extend(post_processing.build_args())
    if query.params:
        pieces.extend(ft.get_params_args(query.params))
    if timeout:
        pieces.extend(("TIMEOUT", timeout))

    options: Dict[str, Any] = {}
    if get_protocol_version(client) not in ["3", 3]:
        options[NEVER_DECODE] = True

    node = client.get_default_node()
    res = await client.execute_command(*pieces, target_nodes=[node], **options)
    results = ft._parse_results(HYBRID_CMD, res, **options)
    return _convert_and_drop_empty_rows(results.results, "hybrid")


def _validate_json_schema_overrides(binding_id: str, overrides: Dict[str, Any]) -> None:
    """Validate JSON ``schema_overrides`` paths before the MCP server does.

    The MCP server inspects a JSON index with the field *alias* as ``name``
    and ``$.<alias>`` as the field path, and rejects overrides that change
    the discovered path. Validating here turns a server-start failure into
    a clear, early error.
    """
    for field in overrides.get("fields", []) or []:
        if not isinstance(field, dict):
            continue
        path = field.get("path")
        name = field.get("name")
        if path is not None and path != f"$.{name}":
            raise ValueError(
                f"{binding_id}: JSON schema_overrides paths must be "
                f"'$.<field>' (got {path!r} for field {name!r})."
            )


def to_redisvl_mcp_config(
    model_classes: List[Type[RedisModel]],
    redis_url: Optional[str] = None,
    *,
    read_only: bool = True,
    search_type: str = "vector",
    descriptions: Optional[Dict[str, str]] = None,
    vectorizers: Optional[Dict[str, Dict[str, Any]]] = None,
    runtime_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    schema_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Generate a redisvl MCP server config (YAML-ready dict) for OM models (U7).

    The returned dict matches redisvl's MCPConfig schema — dump it to
    YAML and run rvl mcp --config om_models.yaml (requires
    redisvl[mcp]). OM never owns the server lifecycle (decision D5 in
    PLAN.md); this is pure serialization and imports nothing from
    redisvl.mcp.

    The MCP server reconstructs schemas from *live index metadata*, so each
    model's index must already exist (e.g. via Migrator().run()). For
    vector/hybrid search, vectorizer entries name the vectorizer class
    redisvl should embed queries with; vector attribute reconstruction may
    need schema_overrides (passed through verbatim to the binding).

    Args:
        model_classes: One or more OM model classes (index=True).
        redis_url: Redis connection URL for the MCP server. Defaults to the
            REDIS_OM_URL environment variable.
        read_only: Disable the upsert-records built-in tool (default).
        search_type: "vector", "fulltext", or "hybrid".
        descriptions: Optional per-model binding descriptions
            ({model.__name__: text}).
        vectorizers: Optional per-model vectorizer config
            ({model.__name__: {"class": "OpenAITextVectorizer",
            "model": "text-embedding-3-small", ...extra kwargs}}).
            Required for "vector"/"hybrid" search unless the model
            declares auto-embedding (then derived automatically).
        runtime_overrides: Optional per-model runtime config overrides
            (merged over the generated defaults).
        schema_overrides: Optional per-model schema_overrides passed
            through to the binding verbatim.

    Returns:
        The MCP server configuration as a nested dict (JSON/YAML-serializable).

    Example::

        config = to_redisvl_mcp_config([Doc], read_only=True)
        import yaml, pathlib
        pathlib.Path("mcp.yaml").write_text(yaml.safe_dump(config))
        # then: rvl mcp --config mcp.yaml
    """
    import os

    if redis_url is None:
        redis_url = os.environ.get("REDIS_OM_URL", "redis://localhost:6379")

    descriptions = descriptions or {}
    vectorizers = vectorizers or {}
    runtime_overrides = runtime_overrides or {}
    schema_overrides = schema_overrides or {}

    builtin_tools: Dict[str, str] = {}
    if read_only:
        builtin_tools["upsert-records"] = "disabled"

    indexes: Dict[str, Any] = {}
    for model_cls in model_classes:
        is_json = issubclass(model_cls, JsonModel)
        binding_id = model_cls.__name__

        runtime: Dict[str, Any] = {}
        vectorizer_cfg: Optional[Dict[str, Any]] = None

        # Vector field discovery: first field with vector_options, favoring
        # auto-embedding fields (they carry a known vectorizer).
        specs = getattr(getattr(model_cls, "_meta", None), "embedding_fields", None)
        vector_field_name = None
        if specs:
            vector_field_name = next(iter(specs))
            if binding_id not in vectorizers:
                spec = specs[vector_field_name]
                ref = spec.vectorizer_ref
                if isinstance(ref, str) and ":" in ref:
                    provider, _, model_name = ref.partition(":")
                    class_name = {
                        "openai": "OpenAITextVectorizer",
                        "azure_openai": "AzureOpenAITextVectorizer",
                        "cohere": "CohereTextVectorizer",
                        "vertexai": "VertexAITextVectorizer",
                        "bedrock": "BedrockTextVectorizer",
                        "mistral": "MistralAITextVectorizer",
                        "gemini": "GoogleGenAIVectorizer",
                        "hf": "HFTextVectorizer",
                    }.get(provider)
                    if class_name is not None:
                        vectorizer_cfg = {"class": class_name, "model": model_name}
        else:
            for fname, field in model_cls.model_fields.items():
                if getattr(field, "vector_options", None) is not None:
                    vector_field_name = fname
                    break

        text_field_name = None
        for fname, field in model_cls.model_fields.items():
            if getattr(field, "full_text_search", False) is True:
                text_field_name = f"{fname}_fts"
                break

        if search_type in ("vector", "hybrid"):
            if vector_field_name is None:
                raise ValueError(
                    f"{binding_id} has no vector field; search_type="
                    f"{search_type!r} requires one."
                )
            runtime["vector_field_name"] = vector_field_name
            if vectorizer_cfg is None:
                vectorizer_cfg = vectorizers.get(binding_id)
            if vectorizer_cfg is None:
                raise ValueError(
                    f"{binding_id}: search_type={search_type!r} requires a "
                    "vectorizer. Pass vectorizers="
                    f'{{"{binding_id}": {{"class": "OpenAITextVectorizer", '
                    '"model": "text-embedding-3-small"}}} or declare '
                    "Field(vectorizer=...) on the vector field."
                )
        if search_type in ("fulltext", "hybrid"):
            if text_field_name is None:
                raise ValueError(
                    f"{binding_id} has no full_text_search field; "
                    f"search_type={search_type!r} requires one."
                )
            runtime["text_field_name"] = text_field_name

        runtime.update(runtime_overrides.get(binding_id, {}))

        binding: Dict[str, Any] = {
            "redis_name": model_cls.Meta.index_name,
            "search": {"type": search_type},
            "runtime": runtime,
        }
        if binding_id in descriptions:
            binding["description"] = descriptions[binding_id]
        binding["read_only"] = read_only
        if vectorizer_cfg is not None:
            binding["vectorizer"] = vectorizer_cfg
        if binding_id in schema_overrides:
            if is_json:
                _validate_json_schema_overrides(
                    binding_id, schema_overrides[binding_id]
                )
            binding["schema_overrides"] = schema_overrides[binding_id]
        indexes[binding_id] = binding

    return {
        "server": {
            "redis_url": redis_url,
            "builtin_tools": builtin_tools,
        },
        "indexes": indexes,
    }
