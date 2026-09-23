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
from typing import Any, Optional, Type

import redis
import redis.cluster
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster


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


def _standalone_sync_twin(async_client: Any) -> redis.Redis:
    """Build a sync ``redis.Redis`` sharing an async client's connection kwargs."""
    kwargs = dict(async_client.connection_pool.connection_kwargs)
    # Intersect with the sync constructor's accepted parameters — async
    # pools may carry keys the sync client rejects (e.g. some credential or
    # pool bookkeeping entries).
    accepted = inspect.signature(redis.Redis.__init__).parameters
    cleaned = {
        k: v
        for k, v in kwargs.items()
        if k in accepted and v is not None and k not in ("self",)
    }
    return redis.Redis(**cleaned)


def sync_client_for_model(
    model_cls: Optional[type] = None,
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
        return client
    if isinstance(client, AsyncRedisCluster):
        from aredis_om.model.model import RedisModelError

        raise RedisModelError(
            f"{caller}: deriving a sync client from an async Redis Cluster "
            "connection is not supported. Pass an explicit sync "
            "RedisCluster client instead."
        )
    return _standalone_sync_twin(client)
