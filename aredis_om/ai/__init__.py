"""AI extensions for Redis OM — connection-reusing helpers over RedisVL.

This package hosts the AI capabilities from the RedisVL integration plan
(see ``PLAN.md``): auto-embedding (U1/U2), semantic caching (U3), LLM
message history (U4), semantic routing (U5), reranking (U6), and the vector
compression advisor (U8).

The package is **lazy**: importing it does not import redisvl or any of the
submodules below. Attribute access resolves on demand::

    from aredis_om.ai import get_semantic_cache, text_knn

RedisVL remains an optional dependency — helpers raise a helpful
``ImportError`` on call when it is missing::

    pip install 'pyredis-om[redisvl]'

All modules sync-mirror into ``redis_om.ai`` via ``make sync``.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from aredis_om.ai.cache import get_embedding_cache, get_semantic_cache
    from aredis_om.ai.compression import (
        estimate_memory_savings,
        recommend_compression,
    )
    from aredis_om.ai.embeddings import (
        EmbeddingError,
        EmbeddingSpec,
        build_embedding_specs,
        embed_model_fields,
        embed_text,
        text_knn,
    )
    from aredis_om.ai.memory import get_message_history
    from aredis_om.ai.rerank import rerank_results
    from aredis_om.ai.router import get_router

_SUBMODULE_EXPORTS = {
    # name -> (module, attribute)
    "get_embedding_cache": ("cache", "get_embedding_cache"),
    "get_semantic_cache": ("cache", "get_semantic_cache"),
    "estimate_memory_savings": ("compression", "estimate_memory_savings"),
    "recommend_compression": ("compression", "recommend_compression"),
    "EmbeddingError": ("embeddings", "EmbeddingError"),
    "EmbeddingSpec": ("embeddings", "EmbeddingSpec"),
    "build_embedding_specs": ("embeddings", "build_embedding_specs"),
    "embed_model_fields": ("embeddings", "embed_model_fields"),
    "embed_text": ("embeddings", "embed_text"),
    "text_knn": ("embeddings", "text_knn"),
    "get_message_history": ("memory", "get_message_history"),
    "rerank_results": ("rerank", "rerank_results"),
    "get_router": ("router", "get_router"),
}

__all__ = list(_SUBMODULE_EXPORTS)


def __getattr__(name: str) -> Any:
    # PEP 562 lazy re-export: ``aredis_om`` must import cleanly (a) without
    # redisvl installed and (b) while ``aredis_om.model.model`` is still
    # initializing — ``ModelMeta`` imports ``aredis_om.ai.embeddings`` during
    # the very first model-class creation. No submodule is imported until an
    # attribute is actually accessed here.
    entry = _SUBMODULE_EXPORTS.get(name)
    if entry is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = entry
    import importlib

    module = importlib.import_module(f"{__package__}.{module_name}")
    return getattr(module, attr)


def __dir__() -> list:
    return sorted(list(globals().keys()) + list(_SUBMODULE_EXPORTS))
