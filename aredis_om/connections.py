import os
from typing import Union

from . import redis


def get_redis_connection(**kwargs) -> Union[redis.Redis, redis.RedisCluster]:
    # Strip caller-provided kwargs from URL query string to prevent
    # query-string arguments from overriding explicit arguments.
    # Library defaults still defer to URL; only explicit caller args are stripped.
    explicit_kwargs = set(kwargs) - {"url"}

    # Decode from UTF-8 by default
    if "decode_responses" not in kwargs:
        kwargs["decode_responses"] = True

    # redis-py >= 6.0 removed the ``legacy_responses`` connection kwarg.
    # Translate it to the equivalent ``protocol`` setting so callers (and
    # tests) written against redis-py 8.0 keep working on older redis-py:
    #   legacy_responses=False => native RESP3 responses => protocol=3
    #   legacy_responses=True  => RESP2-style flat responses => protocol=2
    legacy = kwargs.pop("legacy_responses", None)
    if legacy is not None:
        kwargs.setdefault("protocol", 3 if not legacy else 2)

    # If someone passed in a 'url' parameter, or specified a REDIS_OM_URL
    # environment variable, we'll create the Redis client from the URL.
    url = kwargs.pop("url", os.environ.get("REDIS_OM_URL"))

    # Check if cluster mode is requested via parameter or URL
    cluster = kwargs.pop("cluster", False) or "cluster=true" in str(url).lower()

    if cluster:
        if url:
            # Strip the cluster=true switch (consumed above) plus any
            # explicitly-passed kwargs so they win over URL query params.
            clean_url = _strip_url_params(url, explicit_kwargs | {"cluster"})
            return redis.RedisCluster.from_url(clean_url, **kwargs)
        return redis.RedisCluster(**kwargs)
    else:
        if url:
            clean_url = _strip_url_params(url, explicit_kwargs)
            return redis.Redis.from_url(clean_url, **kwargs)
        return redis.Redis(**kwargs)


def _strip_cluster_param(url: str) -> str:
    """Remove 'cluster=true' from URL query parameters."""
    return _strip_url_params(url, {"cluster"})


def _strip_url_params(url: str, names) -> str:
    """Remove the named keys from a URL's query string.

    Used to keep explicit ``get_redis_connection(...)`` kwargs from being
    overridden by query-string parameters in the URL (redis-py otherwise
    lets the query string win — see ``get_redis_connection``).
    """
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

    parsed = urlparse(url)
    params = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in names
    ]
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def protocol_version(connection) -> int:
    """Return the active RESP protocol version for a Redis client.

    Looks at the connection pool's negotiated protocol when available and
    falls back to introspecting an established connection. Returns 2 if the
    value cannot be determined (the historical default for redis-py).
    """

    def _coerce(version):
        # redis-py >= 6.0 stores the ``protocol`` kwarg as a string ("2"/"3").
        try:
            return int(version)
        except (TypeError, ValueError):
            return None

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
                version = _coerce(version)
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
                proto = _coerce(getattr(underlying, "protocol", None))
                if proto in (2, 3):
                    return proto

    # RedisCluster has no top-level connection_pool; use get_connection_kwargs().
    # Protocol defaults are version-dependent: redis-py < 8 (pinned by redisvl)
    # defaults every connection to RESP2, while redis-py >= 8 auto-negotiates
    # RESP3 against newer servers without recording it in the kwargs. With the
    # kwarg absent we report RESP2 (the < 8 default); the RESP3 shim sniffs
    # the actual wire shape regardless, so a mis-reported version here can
    # never corrupt response parsing.
    if pool is None:
        conn_kwargs_fn = getattr(connection, "get_connection_kwargs", None)
        if callable(conn_kwargs_fn):
            kwargs = conn_kwargs_fn()
            version = _coerce(kwargs.get("protocol"))
            if version in (2, 3):
                return version
            return 2

    return 2
