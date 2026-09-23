"""Semantic router helper (U5).

Wraps redisvl's ``SemanticRouter`` with OM-style connection reuse. The
router classifies free text against a set of ``Route`` definitions by vector
similarity — useful for dispatching prompts to handlers. Sync-only in
redisvl 0.27.x; the helper derives a sync client from the model's database
connection. The router index namespace is separate from OM model indexes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence

from aredis_om.ai._connection import _lazy_import_message, sync_client_for_model

if TYPE_CHECKING:  # pragma: no cover
    from redisvl.extensions.router import Route, RoutingConfig, SemanticRouter
    from redisvl.utils.vectorize import BaseVectorizer


def get_router(
    name: str,
    routes: Sequence["Route"],
    *,
    model_cls: Optional[type] = None,
    vectorizer: Optional["BaseVectorizer"] = None,
    routing_config: Optional["RoutingConfig"] = None,
    overwrite: bool = False,
    redis_client: Optional[Any] = None,
) -> "SemanticRouter":
    """Get a redisvl ``SemanticRouter`` (U5).

    Args:
        name: Router name (its index namespace; separate from OM indexes).
        routes: ``redisvl.extensions.router.Route`` definitions. Each route
            carries reference phrases the router matches against.
        model_cls: OM model class whose ``Meta.database`` to wire to.
        vectorizer: Vectorizer for embedding route references and queries.
            When omitted, uses ``model_cls``'s auto-embedding vectorizer if
            it declares exactly one.
        routing_config: Optional ``RoutingConfig`` (``max_k``,
            ``aggregation_method``).
        overwrite: Replace an existing router index.
        redis_client: Optional explicit sync client override (required for
            async cluster deployments).

    Example::

        routes = [Route(name="greet", references=["hello", "hi"])]
        router = get_router("dispatch", routes, model_cls=Doc)
        match = router("good morning")
        assert match.name == "greet"
    """
    index_name = getattr(getattr(model_cls, "Meta", None), "index_name", None)
    if model_cls is not None and index_name and name == index_name:
        from aredis_om.model.model import RedisModelError

        raise RedisModelError(
            f"Router name {name!r} collides with {model_cls.__name__}'s OM "
            "index name. Pick a different name."
        )

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

    client = sync_client_for_model(model_cls, redis_client, caller="get_router")

    try:
        from redisvl.extensions.router import SemanticRouter
    except ImportError as e:  # pragma: no cover
        raise ImportError(_lazy_import_message()) from e

    kwargs: dict = {"name": name, "routes": list(routes), "overwrite": overwrite}
    if vectorizer is not None:
        kwargs["vectorizer"] = vectorizer
    if routing_config is not None:
        kwargs["routing_config"] = routing_config
    if client is not None:
        kwargs["redis_client"] = client
    return SemanticRouter(**kwargs)
