"""Connection helpers shared by the ``aredis_om.ai`` extension modules.

RedisVL's LLM extensions (``SemanticCache``, ``MessageHistory``,
``SemanticRouter``) construct a *sync* ``SearchIndex`` in their constructors
and only derive async twins for selected async methods. OM models, however,
hold async clients by default (``redis.asyncio``). These helpers derive a
sync twin client from a model's database connection so the extensions can be
wired to the same Redis without a second user-managed connection URL.

All redisvl imports here are lazy — ``aredis_om`` must import cleanly
without redisvl installed.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Optional, Type

import redis
import redis.cluster
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

if TYPE_CHECKING:  # pragma: no cover
    from aredis_om.model.model import RedisModel


def _lazy_import_message() -> str:
    return (
        "The RedisVL AI extensions require the 'redisvl' package. "
        "Install it with: pip install 'pyredis-om[redisvl]'"
    )


def is_async_client(client: Any) -> bool:
    """True for ``redis.asyncio`` standalone/cluster clients."""
    if isinstance(client, AsyncRedisCluster):
        return True
    # Avoid importing redis.asyncio.Redis directly here: the sync mirror of
    # this module runs with the sync package only.
    mod = getattr(type(client), "__module__", "")
    return mod.startswith("redis.asyncio")


def _redisvl_clone_kwargs(kwargs: dict) -> dict:
    """Sanitize connection kwargs for a clone handed to redisvl.

    Only clients built with an *explicit* ``protocol=3`` produce the
    native/unified RESP3 response shapes redisvl cannot parse — those are
    cloned down to ``protocol=2``. Default (``protocol=None``) redis-py
    >= 8 connections speak RESP3 on the wire but keep legacy response
    shapes, which redisvl handles fine, so their protocol is preserved.

    Also drops RESP3-only options (``maint_notification*`` handlers/
    config, which redis-py rejects with "Maintenance notifications
    handlers on connection are only supported with RESP version 3").
    """
    accepted = inspect.signature(redis.Redis.__init__).parameters
    cleaned = {
        k: v
        for k, v in kwargs.items()
        if k in accepted
        and v is not None
        and k != "self"
        and not k.startswith("maint_notification")
    }
    if str(kwargs.get("protocol")) == "3":
        cleaned["protocol"] = REDISVL_CLIENT_PROTOCOL
    return cleaned


def _explicit_native_resp3(client: Any) -> bool:
    """True when *client* was built with an explicit ``protocol=3``.

    That is the one connection mode producing native/unified RESP3
    response shapes, which redisvl (0.27.x) cannot parse — its reply
    post-processing reads e.g. ``results.docs`` off redis-py's legacy
    ``FT.SEARCH`` result objects and fails with
    ``AttributeError: 'dict' object has no attribute 'docs'``.

    Everything else is left alone: RESP2 connections obviously, and the
    redis-py >= 8 *default* (``protocol=None``) connections that speak
    RESP3 on the wire but preserve legacy response shapes — redisvl
    works with those as-is.
    """
    try:
        pool = getattr(client, "connection_pool", None)
        kwargs = (
            getattr(pool, "connection_kwargs", None)
            or getattr(client, "connection_kwargs", None)
            or {}
        )
        return str(kwargs.get("protocol")) == "3"
    except Exception:
        return False


def _standalone_sync_twin(async_client: Any) -> redis.Redis:
    """Build a sync ``redis.Redis`` sharing an async client's connection kwargs.

    Connections built with an explicit ``protocol=3`` (native/unified
    RESP3 shapes) are cloned down to RESP2 — redisvl cannot parse those
    shapes. Everything else keeps its protocol: redis-py >= 8 default
    connections (``protocol=None``) speak RESP3 on the wire with legacy
    response shapes, which redisvl handles fine.
    """
    kwargs = dict(async_client.connection_pool.connection_kwargs)
    return redis.Redis(**_redisvl_clone_kwargs(kwargs))


# Protocol used when a native-RESP3 client must be adapted for redisvl.
# See ``_explicit_native_resp3`` — only explicit ``protocol=3`` clients
# need it.
REDISVL_CLIENT_PROTOCOL = 2


def _redisvl_compatible_client(client: Any) -> Any:
    """Return *client* unchanged unless it was built with an explicit
    ``protocol=3``, in which case return a same-flavour (sync/async)
    clone speaking RESP2.

    Explicit ``protocol=3`` selects native/unified RESP3 response shapes
    (on every redis-py version tested, 7.4 and 8.1) — the one mode
    redisvl's reply post-processing cannot handle. redis-py >= 8 default
    connections (RESP3 on the wire, legacy shapes) are returned
    untouched: redisvl works with those directly.

    Cluster clients and anything exotic (including test doubles) are
    passed through untouched — cloning them from connection kwargs
    alone is not possible, and the historical behaviour was to hand
    them to redisvl as-is.
    """
    if not _explicit_native_resp3(client):
        return client
    if isinstance(client, redis.cluster.RedisCluster):
        return client
    if isinstance(client, AsyncRedisCluster):
        return client
    try:
        if is_async_client(client):
            import redis.asyncio as aredis

            kwargs = _redisvl_clone_kwargs_async(
                client.connection_pool.connection_kwargs
            )
            return aredis.Redis(**kwargs)
        return _standalone_sync_twin(client)
    except Exception:
        return client


def _redisvl_clone_kwargs_async(kwargs: dict) -> dict:
    """Async twin of :func:`_redisvl_clone_kwargs` (same sanitization, async
    constructor signature)."""
    import inspect as _inspect

    import redis.asyncio as aredis

    accepted = _inspect.signature(aredis.Redis.__init__).parameters
    cleaned = {
        k: v
        for k, v in kwargs.items()
        if k in accepted
        and v is not None
        and k != "self"
        and not k.startswith("maint_notification")
    }
    if str(kwargs.get("protocol")) == "3":
        cleaned["protocol"] = REDISVL_CLIENT_PROTOCOL
    return cleaned


def sync_client_for_model(
    model_cls: Optional[Type["RedisModel"]] = None,
    client: Optional[Any] = None,
    *,
    caller: str = "this helper",
) -> Any:
    """Return a *sync* Redis client for the model's database connection.

    - Model database already sync (sync mirror, or a user-supplied sync
      client in ``Meta.database``) → returned unchanged.
    - Async standalone client → a sync twin built from its connection kwargs
      (same host/port/credentials/decode_responses).
    - Async **cluster** client → ``RedisModelError``: deriving a cluster twin
      reliably is not possible from connection kwargs alone. Pass an explicit
      sync client via ``Meta.database`` (or the helper's ``redis_client=``
      argument where offered) for cluster deployments.

    Args:
        model_cls: OM model class whose ``Meta.database`` to use.
        client: Optional explicit client; wins over ``model_cls``.
        caller: Helper name for error messages.
    """
    if client is None:
        if model_cls is None:
            # Fall back to the default OM connection (``REDIS_OM_URL``). In
            # the async package this yields an async client, converted to a
            # sync twin below; the generated sync mirror gets a sync client
            # back unchanged.
            from aredis_om.connections import get_redis_connection

            client = get_redis_connection()
        else:
            client = model_cls.db()
    if not is_async_client(client):
        # Explicit protocol=3 sync clients get a RESP2 clone for redisvl;
        # everything else (defaults included) is returned unchanged.
        return _redisvl_compatible_client(client)
    if isinstance(client, AsyncRedisCluster):
        from aredis_om.model.model import RedisModelError

        raise RedisModelError(
            f"{caller}: deriving a sync client from an async Redis Cluster "
            "connection is not supported. Pass an explicit sync "
            "RedisCluster client instead."
        )
    return _standalone_sync_twin(client)
