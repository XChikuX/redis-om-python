"""LLM conversation memory helpers (U4).

Wraps redisvl's ``MessageHistory`` / ``SemanticMessageHistory`` with OM-style
connection reuse. Both classes are **sync-only** in redisvl 0.27.x (their
internal index is a sync ``SearchIndex``), so these helpers derive a sync
client from the model's database connection. The history index namespace is
separate from OM model indexes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from aredis_om.ai._connection import _lazy_import_message, sync_client_for_model

if TYPE_CHECKING:  # pragma: no cover
    from redisvl.extensions.message_history import (
        MessageHistory,
        SemanticMessageHistory,
    )
    from redisvl.utils.vectorize import BaseVectorizer


def _check_namespace(model_cls: Optional[type], name: str) -> None:
    index_name = getattr(getattr(model_cls, "Meta", None), "index_name", None)
    if model_cls is not None and index_name and name == index_name:
        from aredis_om.model.model import RedisModelError

        raise RedisModelError(
            f"Message-history name {name!r} collides with "
            f"{model_cls.__name__}'s OM index name. Pick a different name."
        )


def get_message_history(
    name: str,
    *,
    model_cls: Optional[type] = None,
    session_tag: Optional[str] = None,
    semantic: bool = False,
    vectorizer: Optional["BaseVectorizer"] = None,
    distance_threshold: float = 0.3,
    redis_client: Optional[Any] = None,
) -> "MessageHistory | SemanticMessageHistory":
    """Get a redisvl ``MessageHistory`` (or ``SemanticMessageHistory``) (U4).

    Message history stores user prompts and LLM responses per conversation
    session, allowing future prompts to be enriched with session context.

    Args:
        name: History index name (separate namespace from OM indexes).
        model_cls: OM model class whose ``Meta.database`` to wire to.
        session_tag: Tag linking entries to a conversation session.
            Defaults to a generated ULID per instance.
        semantic: Return a ``SemanticMessageHistory`` — retrieves relevant
            past messages by vector similarity (needs a ``vectorizer``).
        vectorizer: Vectorizer for the semantic variant. When omitted, uses
            ``model_cls``'s auto-embedding vectorizer if it declares exactly
            one.
        distance_threshold: Semantic variant's relevance threshold.
        redis_client: Optional explicit sync client override (required for
            async cluster deployments, which cannot be auto-derived).

    Example::

        history = get_message_history("tutor", model_cls=Doc, session_tag="s1")
        history.add_messages([{"role": "user", "content": "Hi"}])
        messages = history.get_recent()
    """
    _check_namespace(model_cls, name)
    client = sync_client_for_model(
        model_cls, redis_client, caller="get_message_history"
    )

    kwargs: dict = {"name": name}
    if session_tag is not None:
        kwargs["session_tag"] = session_tag
    if client is not None:
        kwargs["redis_client"] = client

    try:
        if semantic:
            from redisvl.extensions.message_history import SemanticMessageHistory
        else:
            from redisvl.extensions.message_history import MessageHistory
    except ImportError as e:  # pragma: no cover
        raise ImportError(_lazy_import_message()) from e

    if not semantic:
        return MessageHistory(**kwargs)

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
    kwargs["distance_threshold"] = distance_threshold
    if vectorizer is not None:
        kwargs["vectorizer"] = vectorizer
    return SemanticMessageHistory(**kwargs)
