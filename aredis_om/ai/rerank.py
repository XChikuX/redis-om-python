"""Reranking support (U6).

Delegates to redisvl rerankers (``HFCrossEncoderReranker``, ``CohereReranker``,
``VoyageAIReranker``) to reorder OM query results by relevance to a query.
Pure delegation — no OM query semantics change; without a reranker call,
results are untouched.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple

if TYPE_CHECKING:  # pragma: no cover
    from aredis_om.model.model import FindQuery, RedisModel


async def rerank_results(
    results: Sequence[Any],
    query: str,
    reranker: Any,
    *,
    rank_by: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    content_field: Optional[str] = None,
) -> Tuple[Sequence[Any], Sequence[float]]:
    """Rerank OM query results (models or dicts) with a redisvl reranker.

    Args:
        results: Result sequence — OM model instances (HashModel/JsonModel)
            or plain dicts. For models, the text reranked is drawn from
            ``content_field`` (e.g. ``"body"``); for dicts, redisvl's
            ``rank_by`` semantics apply.
        query: The user query to rank against.
        reranker: A redisvl reranker instance (e.g.
            ``HFCrossEncoderReranker(model="cross-encoder/ms-marco-MiniLM-L-6-v2")``).
        rank_by: Optional dict-field name(s) to rank by (dict results only).
        limit: Optional cap on returned results.
        content_field: Model field whose text is reranked (model results
            only). Defaults to the model's single auto-embedding ``source``
            field when declared.

    Returns:
        ``(reranked_results, scores)`` — same element types as the input,
        ordered best-first; scores are the reranker's relevance scores.

    Example::

        reranker = HFCrossEncoderReranker(limit=3)
        docs = await Doc.find().all()
        top, scores = await rerank_results(docs, "cheap running shoes",
                                           reranker, content_field="body")
    """
    if not results:
        return list(results), []

    first = results[0]
    docs: Sequence[Any]
    is_model = hasattr(first, "model_dump") and hasattr(first, "_meta")

    if is_model:
        if content_field is None:
            specs = (
                getattr(getattr(type(first), "_meta", None), "embedding_fields", None)
                or {}
            )
            sources = [s.source_field for s in specs.values() if s.source_field]
            if len(sources) == 1:
                content_field = sources[0]
        if content_field is None:
            raise ValueError(
                "rerank_results: pass content_field= naming the model field "
                "whose text should be reranked (no auto-embedding source "
                "field found to default to)."
            )
        docs = [
            {"id": getattr(m, "pk", None), "text": getattr(m, content_field, "")}
            for m in results
        ]
    else:
        docs = list(results)

    rank_kwargs: dict = {}
    if rank_by is not None and not is_model:
        rank_kwargs["rank_by"] = list(rank_by)

    ranked, scores = reranker.rank(query=query, docs=docs, **rank_kwargs)
    if limit is not None:
        ranked = ranked[:limit]
        scores = scores[:limit]

    if is_model:
        # Map ranked ids back to the original model instances, preserving
        # the reranker's order.
        by_pk = {getattr(m, "pk", None): m for m in results}
        ordered = [by_pk.get(d.get("id")) for d in ranked]
        if any(m is None for m in ordered):  # pragma: no cover
            return list(results), list(scores)
        return ordered, list(scores)

    return ranked, list(scores)
