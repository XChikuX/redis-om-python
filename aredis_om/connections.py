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
    from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("cluster", None)
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))
