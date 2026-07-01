"""Bridge between ``aredis_om`` and ``fastapi-redis-sdk``.

``fastapi-redis-sdk`` (`pip install fastapi-redis-sdk`_) is Redis' official
FastAPI integration.  It manages async Redis connection pools via a FastAPI
lifespan and provides dependency-injection-based HTTP caching
(``cache()`` / ``cache_evict()`` / ``cache_put()`` and a ``CacheBackend``).

This module lets your Redis OM models reuse the **same** connection pool
that fastapi-redis-sdk manages, so you don't have to open a second pool to
the same Redis instance.  It is **async-only**: fastapi-redis-sdk manages
``redis.asyncio`` clients, so it pairs with ``aredis_om`` (not the
generated ``redis_om`` sync mirror).

All imports of ``redis_fastapi`` are performed lazily so that ``aredis_om``
keeps working in environments where ``fastapi-redis-sdk`` is not installed.

.. _pip install fastapi-redis-sdk: https://github.com/redis/fastapi-redis-sdk

Example
-------

.. code-block:: python

    from fastapi import FastAPI
    from redis_fastapi import FastAPIRedis

    from aredis_om import HashModel
    from aredis_om.integrations.fastapi_redis_sdk import database_from_app

    app = FastAPI()
    FastAPIRedis(app).lifespan()  # initialises the shared async pool

    class Customer(HashModel):
        first_name: str
        last_name: str

        class Meta:
            # ``database_from_app`` returns a *callable* that resolves to
            # the fastapi-redis-sdk client the first time the model needs
            # it (i.e. after the lifespan has started).
            database = database_from_app(app)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from fastapi import FastAPI

# Redis OM's ``DatabaseProvider`` type alias — a zero-arg callable returning
# a redis client.  Kept as ``Any`` here to avoid importing from the model
# module (which would create a circular import at package import time).
DatabaseProvider = Callable[[], Any]


def database_from_app(app: "FastAPI") -> DatabaseProvider:
    """Return a database provider that resolves to fastapi-redis-sdk's client.

    The returned callable is suitable for assignment to a model's
    ``Meta.database``.  Redis OM invokes it lazily the first time the model
    touches Redis (typically inside a request handler, after the FastAPI
    lifespan has initialised the connection pool).

    Parameters
    ----------
    app:
        A :class:`fastapi.FastAPI` instance that has been configured with
        ``FastAPIRedis(app).lifespan()`` (or the ``redis_lifespan`` context
        manager).  The pool state is read from ``app.state._redis``.

    Returns
    -------
    A zero-argument callable that returns the cached async Redis client
    (``redis.asyncio.Redis`` or ``redis.asyncio.RedisCluster``) managed by
    fastapi-redis-sdk.

    Raises
    ------
    RuntimeError
        If fastapi-redis-sdk is not installed, or if the lifespan has not
        run yet (the pool is not initialised) at the time the provider is
        invoked.  The latter usually means you forgot to call
        ``FastAPIRedis(app).lifespan()`` or the model is being used before
        startup completed.
    """

    def _resolve() -> Any:
        pool_state = getattr(app.state, "_redis", None)
        if pool_state is None:
            try:
                from redis_fastapi.deps import _get_pool_state

                pool_state = _get_pool_state(app)
            except ImportError as exc:  # pragma: no cover - env dependent
                raise RuntimeError(
                    "fastapi-redis-sdk is not installed. Install it with "
                    "`pip install fastapi-redis-sdk` to use "
                    "database_from_app()."
                ) from exc
        # ``get_async_client`` exists on the _PoolState class attached by
        # fastapi-redis-sdk's lifespan.  It raises a helpful RuntimeError
        # of its own if the pool has not been initialised.
        get_client = getattr(pool_state, "get_async_client", None)
        if get_client is None:
            raise RuntimeError(
                "The object stored at app.state._redis does not look like a "
                "fastapi-redis-sdk pool state (no get_async_client() method). "
                "Make sure FastAPIRedis(app).lifespan() has been called."
            )
        return get_client()

    return _resolve


def database_from_fastapi_settings() -> DatabaseProvider:
    """Return a database provider built from fastapi-redis-sdk's settings.

    Unlike :func:`database_from_app`, this does **not** share the
        fastapi-redis-sdk connection pool.  Instead it reads the public
        :class:`redis_fastapi.RedisSettings` (env vars prefixed ``REDIS_``, or a
        ``.env`` file) and constructs a fresh Redis OM connection with
        ``aredis_om.connections.get_redis_connection``.

    Use this when you want Redis OM and fastapi-redis-sdk to honour the
    *same* configuration (e.g. a single ``REDIS_URL``) but keep their
    connection pools isolated, or when you cannot easily pass the
    :class:`~fastapi.FastAPI` instance to the model definition.

    The settings are read lazily on first invocation so environment
    variables set after import time are picked up.

    Raises
    ------
    RuntimeError
        If fastapi-redis-sdk is not installed.
    """

    def _resolve() -> Any:
        try:
            from redis_fastapi import get_settings
        except ImportError as exc:  # pragma: no cover - env dependent
            raise RuntimeError(
                "fastapi-redis-sdk is not installed. Install it with "
                "`pip install fastapi-redis-sdk` to use "
                "database_from_fastapi_settings()."
            ) from exc
        # Imported here (not at module top) to keep this module importable
        # without the rest of aredis_om's model machinery being initialised.
        from aredis_om.connections import get_redis_connection

        settings = get_settings()
        # ``connection_kwargs()`` returns either ``{"url": ...}`` or host/port
        # pairs, plus pool/TLS kwargs.  ``get_redis_connection`` understands
        # both ``url=`` and host/port kwargs, and adds ``decode_responses=True``
        # by default (which Redis OM requires).
        kwargs = settings.connection_kwargs()
        # ``driver_info`` is internal bookkeeping for fastapi-redis-sdk and
        # is not understood by redis-om's get_redis_connection; drop it.
        kwargs.pop("driver_info", None)
        return get_redis_connection(**kwargs)

    return _resolve


__all__ = ["DatabaseProvider", "database_from_app", "database_from_fastapi_settings"]
