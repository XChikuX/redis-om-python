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


def _redisvl_clone_kwargs(kwargs: dict, constructor: Any) -> dict:
    """Sanitize connection kwargs for a clone handed to redisvl.

    The explicit ``protocol`` is *removed*, so the clone uses redis-py's
    default. That is the setting guaranteed to yield legacy reply shapes
    on every supported redis-py version while keeping RESP3 on the wire
    where possible: redis-py >= 8 defaults negotiate RESP3 yet return the
    legacy shapes redisvl parses; redis-py 7.x speaks RESP2. Forcing
    ``protocol=2`` instead would needlessly downgrade the wire for
    redis-py >= 8 users.

    ``maint_notification*`` options are dropped: redis-py 7.x rejects
    them with "Maintenance notifications handlers on connection are only
    supported with RESP version 3" unless the protocol is *literally*
    3 (its default is 2), and they are irrelevant to redisvl queries.
    """
    accepted = inspect.signature(constructor).parameters
    cleaned = {
        k: v
        for k, v in kwargs.items()
        if k in accepted
        and v is not None
        and k != "self"
        and not k.startswith("maint_notification")
    }
    cleaned.pop("protocol", None)
    return cleaned


# Reply-shape decision shared by every redisvl-facing code path.
#
# redis-py selects the FT.SEARCH reply parser from BOTH the ``protocol``
# and ``legacy_responses`` connection options — see redis-py's
# ``redis/commands/search/commands.py`` (``_register_module_callbacks``).
# The resulting reply shapes were verified empirically against redis-py
# 7.4.1 and 8.1.0, redisvl 0.27.2 and 0.28.0, server 8.10:
#
#   protocol=None (redis-py >= 8 default) -> ``Result`` (has ``.docs``)  OK
#   protocol=2                            -> ``Result``                  OK
#   protocol=3 + legacy_responses=False   -> ``Result`` (unified)        OK
#   protocol=3 + legacy responses (default) -> plain ``dict``          BREAKS
#
# redisvl reads ``results.docs`` off the reply, so only the last row fails
# (``AttributeError: 'dict' object has no attribute 'docs'``).
#
# Note the *negotiated* wire protocol is deliberately NOT consulted:
# redis-py >= 8 defaults negotiate RESP3 on the wire yet keep legacy reply
# shapes, which redisvl consumes fine. Only an explicit ``protocol=3``
# paired with legacy responses selects the native parser.


def _connection_kwargs(client: Any) -> dict:
    """Connection kwargs *client* was built from (standalone or cluster)."""
    pool = getattr(client, "connection_pool", None)
    kwargs = getattr(pool, "connection_kwargs", None)
    if kwargs is None:
        # Cluster clients keep per-node kwargs on the nodes manager.
        nodes_manager = getattr(client, "nodes_manager", None)
        kwargs = getattr(nodes_manager, "connection_kwargs", None)
    return kwargs or {}


def _redisvl_native_resp3_shapes(client: Any) -> bool:
    """True when redis-py will hand redisvl the *native* RESP3 reply shapes
    it cannot parse.

    Mirrors redis-py's own parser selection (see the module comment above):
    an explicit ``protocol=3`` whose ``legacy_responses`` flag is not
    disabled. Default ``protocol=None`` connections — including redis-py
    >= 8 defaults that negotiate RESP3 on the wire — never produce native
    shapes, nor do explicit ``protocol=2`` connections.
    """
    kwargs = _connection_kwargs(client)
    protocol = kwargs.get("protocol")
    if protocol is None:
        return False
    try:
        if int(protocol) != 3:
            return False
    except (TypeError, ValueError):
        return False
    # ``legacy_responses`` defaults to True (and does not exist at all on
    # redis-py < 8). Truthiness matches redis-py's own selection logic.
    return bool(kwargs.get("legacy_responses", True))


def _standalone_sync_twin(async_client: Any) -> redis.Redis:
    """Build a sync ``redis.Redis`` sharing an async client's connection kwargs.

    Clients using the native RESP3 reply shapes (explicit ``protocol=3``
    with legacy responses) are cloned onto redis-py's default protocol so
    redisvl receives reply shapes it can parse — see the module comment
    on the reply-shape matrix. Everything else keeps its connection
    options untouched.
    """
    kwargs = dict(async_client.connection_pool.connection_kwargs)
    if _redisvl_native_resp3_shapes(async_client):
        kwargs = _redisvl_clone_kwargs(kwargs, redis.Redis.__init__)
    else:
        kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in inspect.signature(redis.Redis.__init__).parameters
            and v is not None
            and k != "self"
        }
    return redis.Redis(**kwargs)


def _redisvl_compatible_client(client: Any) -> Any:
    """Return *client* unchanged unless redis-py would hand redisvl the
    native RESP3 reply shapes, in which case return a same-flavour
    (sync/async) clone using redis-py's default protocol.

    Only an explicit ``protocol=3`` connection paired with legacy
    responses produces those native shapes (on every redis-py version
    tested, 7.4 and 8.1). Default connections — RESP3 on the wire for
    redis-py >= 8 — are returned untouched, as are RESP2 connections and
    explicit ``protocol=3`` + ``legacy_responses=False`` (whose unified
    replies are still redis-py ``Result`` objects redisvl reads fine).

    Cluster clients and anything exotic (including test doubles) are
    passed through untouched — cloning them from connection kwargs
    alone is not possible, and the historical behaviour was to hand
    them to redisvl as-is.
    """
    if not _redisvl_native_resp3_shapes(client):
        return client
    if isinstance(client, redis.cluster.RedisCluster):
        return client
    if isinstance(client, AsyncRedisCluster):
        return client
    try:
        if is_async_client(client):
            import redis.asyncio as aredis

            kwargs = _redisvl_clone_kwargs(
                client.connection_pool.connection_kwargs, aredis.Redis.__init__
            )
            return aredis.Redis(**kwargs)
        return _standalone_sync_twin(client)
    except Exception:
        return client


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
        # Clients using native RESP3 reply shapes get a default-protocol
        # clone for redisvl; everything else is returned unchanged.
        return _redisvl_compatible_client(client)
    if isinstance(client, AsyncRedisCluster):
        from aredis_om.model.model import RedisModelError

        raise RedisModelError(
            f"{caller}: deriving a sync client from an async Redis Cluster "
            "connection is not supported. Pass an explicit sync "
            "RedisCluster client instead."
        )
    return _standalone_sync_twin(client)
