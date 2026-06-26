import os
from typing import Union

from . import redis


def get_redis_connection(**kwargs) -> Union[redis.Redis, redis.RedisCluster]:
    # Decode from UTF-8 by default
    if "decode_responses" not in kwargs:
        kwargs["decode_responses"] = True

    # If someone passed in a 'url' parameter, or specified a REDIS_OM_URL
    # environment variable, we'll create the Redis client from the URL.
    url = kwargs.pop("url", os.environ.get("REDIS_OM_URL"))

    # Check if cluster mode is requested via parameter or URL
    cluster = kwargs.pop("cluster", False) or "cluster=true" in str(url).lower()

    if cluster:
        if url:
            # Strip the cluster=true query parameter from the URL so it
            # doesn't get forwarded to RedisCluster.__init__().
            clean_url = _strip_cluster_param(url)
            return redis.RedisCluster.from_url(clean_url, **kwargs)
        return redis.RedisCluster(**kwargs)
    else:
        if url:
            return redis.Redis.from_url(url, **kwargs)
        return redis.Redis(**kwargs)


def _strip_cluster_param(url: str) -> str:
    """Remove 'cluster=true' from URL query parameters."""
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

    parsed = urlparse(url)
    params = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != "cluster"
    ]
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def protocol_version(connection) -> int:
    """Return the active RESP protocol version for a Redis client.

    Looks at the connection pool's negotiated protocol when available and
    falls back to introspecting an established connection. Returns 2 if the
    value cannot be determined (the historical default for redis-py).
    """
    # Prefer the connection pool's negotiated value when present.
    pool = getattr(connection, "connection_pool", None)
    if pool is not None:
        getter = getattr(pool, "get_protocol", None)
        if callable(getter):
            try:
                version = getter()
            except Exception:
                version = None
            else:
                if version in (2, 3):
                    return version

    # Fall back to introspecting the underlying connection class.
    if pool is not None:
        make_connection = getattr(pool, "make_connection", None)
        if callable(make_connection):
            try:
                underlying = make_connection()
            except Exception:
                underlying = None
            else:
                proto = getattr(underlying, "protocol", None)
                if proto in (2, 3):
                    return proto

    # RedisCluster has no top-level connection_pool; use get_connection_kwargs().
    # If protocol was not explicitly set, redis-py defaults to RESP3 (3).
    if pool is None:
        conn_kwargs_fn = getattr(connection, "get_connection_kwargs", None)
        if callable(conn_kwargs_fn):
            kwargs = conn_kwargs_fn()
            version = kwargs.get("protocol")
            if version in (2, 3):
                return version
            # protocol not explicitly set → redis-py defaults to RESP3
            return 3

    return 2
