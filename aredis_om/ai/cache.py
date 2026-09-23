"""Cache helpers (U2 embedding cache, U3 semantic LLM cache).

Both helpers follow the :func:`aredis_om.redisvl.get_redisvl_index` pattern:
wired to the model's ``Meta.database`` connection, no user-managed URLs.
Namespace isolation is enforced — the cache indexes these helpers create are
*separate* from any OM model index (see ``_check_namespace``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from aredis_om.ai._connection import _lazy_import_message, sync_client_for_model

if TYPE_CHECKING:  # pragma: no cover
    from aredis_om.model.model import RedisModel
    from redisvl.extensions.cache.embeddings import EmbeddingsCache
    from redisvl.extensions.cache.llm import SemanticCache
    from redisvl.utils.vectorize import BaseVectorizer


def _check_namespace(model_cls: Optional[type], name: str) -> None:
    """Refuse cache names that collide with a model's OM index name."""
    if model_cls is None:
        return
    index_name = getattr(getattr(model_cls, "Meta", None), "index_name", None)
    if index_name and name == index_name:
        from aredis_om.model.model import RedisModelError

        raise RedisModelError(
            f"Cache name {name!r} collides with {model_cls.__name__}'s OM "
            "index name. AI-extension indexes are separate namespaces; pick "
            "a different name."
        )


def _default_cache_name(model_cls: Optional[type], infix: str) -> str:
    """Derive a namespaced cache name from the model's key prefix."""
    prefix = getattr(getattr(model_cls, "Meta", None), "model_key_prefix", None)
    if not prefix:
        return infix
    return f"{prefix}:{infix}"


def get_embedding_cache(
    model_cls: Optional[type] = None,
    name: Optional[str] = None,
    ttl: Optional[int] = None,
) -> "EmbeddingsCache":
    """Get a RedisVL ``EmbeddingsCache`` wired to the model's database (U2).

    Used automatically when ``class Meta: embedding_cache = True`` is set on
    a model with auto-embedding fields; also useful standalone.

    The cache name defaults to ``<model_key_prefix>:embcache`` — a separate
    namespace from the model's own keys and index.

    Args:
        model_cls: OM model class providing the connection (and default
            name). Optional when ``name`` is given — the default connection
            (:func:`aredis_om.get_redis_connection`) is used then.
        name: Cache name override.
        ttl: Optional TTL for cached embeddings (seconds).

    Example::

        cache = get_embedding_cache(Doc)
        vectorizer = OpenAITextVectorizer(model="text-embedding-3-small",
                                          cache=cache)
    """
    try:
        from redisvl.extensions.cache.embeddings import EmbeddingsCache
    except ImportError as e:  # pragma: no cover
        raise ImportError(_lazy_import_message()) from e

    cache_name = name or _default_cache_name(model_cls, "embcache")
    _check_namespace(model_cls, cache_name)

    from aredis_om.ai._connection import is_async_client

    db = model_cls.db() if model_cls is not None else None
    kwargs: dict = {"name": cache_name}
    if ttl is not None:
        kwargs["ttl"] = ttl

    if db is not None:
        if is_async_client(db):
            # Async OM client → hand redisvl the async pool directly; the
            # vectorizers' ``aembed`` path uses it for cache get/set.
            kwargs["async_redis_client"] = db
        else:
            kwargs["redis_client"] = db
    return EmbeddingsCache(**kwargs)


def get_semantic_cache(
    name: str,
    model_cls: Optional[type] = None,
    *,
    vectorizer: Optional["BaseVectorizer"] = None,
    ttl: Optional[int] = None,
    distance_threshold: float = 0.1,
    overwrite: bool = False,
    redis_client: Optional[Any] = None,
) -> "SemanticCache":
    """Get a RedisVL ``SemanticCache`` for LLM prompt/response caching (U3).

    The cache is a **separate index namespace** from OM models — its name
    must differ from any model index (checked). Construction creates the
    cache index via a sync client; use the async methods (``astore``,
    ``acheck``) from async code — redisvl derives an async twin from the
    same connection settings.

    Args:
        name: Cache name (becomes the cache index name prefix).
        model_cls: OM model class whose ``Meta.database`` to wire to.
        vectorizer: Optional redisvl vectorizer for prompt embedding. When
            omitted, ``model_cls``'s auto-embedding vectorizer is used if it
            declares exactly one; otherwise redisvl's default (OpenAI).
        ttl: Optional entry TTL (seconds).
        distance_threshold: Semantic distance below which entries match.
        overwrite: Allow replacing an existing cache index with a different
            schema.
        redis_client: Optional explicit sync client override (required for
            async cluster deployments, which cannot be auto-derived).

    Example::

        cache = get_semantic_cache("answers", model_cls=Doc)
        await cache.astore(prompt="What is Redis?", response="A database.")
        hits = await cache.acheck(prompt="What exactly is Redis?")
    """
    try:
        from redisvl.extensions.cache.llm import SemanticCache
    except ImportError as e:  # pragma: no cover
        raise ImportError(_lazy_import_message()) from e

    _check_namespace(model_cls, name)

    if vectorizer is None and model_cls is not None:
        specs = getattr(getattr(model_cls, "_meta", None), "embedding_fields", None)
        if specs:
            if len(specs) == 1:
                vectorizer = next(iter(specs.values())).resolve(model_cls)
            else:
                from aredis_om.model.model import RedisModelError

                raise RedisModelError(
                    f"{model_cls.__name__} declares multiple auto-embedding "
                    f"fields ({', '.join(sorted(specs))}); pass vectorizer= "
                    "explicitly."
                )

    client = sync_client_for_model(
        model_cls, redis_client, caller="get_semantic_cache"
    )
    kwargs: dict = {
        "name": name,
        "distance_threshold": distance_threshold,
        "overwrite": overwrite,
    }
    if vectorizer is not None:
        kwargs["vectorizer"] = vectorizer
    if ttl is not None:
        kwargs["ttl"] = ttl
    if client is not None:
        kwargs["redis_client"] = client
    return SemanticCache(**kwargs)
