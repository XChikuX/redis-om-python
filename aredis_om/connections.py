import os
from typing import Union

from . import redis


URL = os.environ.get("REDIS_OM_URL", None)


def get_redis_connection(**kwargs) -> Union[redis.Redis, redis.RedisCluster]:
    # Decode from UTF-8 by default
    if "decode_responses" not in kwargs:
        kwargs["decode_responses"] = True

    # Check if cluster mode is requested
    cluster = kwargs.pop("cluster", False)

    # If someone passed in a 'url' parameter, or specified a REDIS_OM_URL
    # environment variable, we'll create the Redis client from the URL.
    url = kwargs.pop("url", URL)

    if cluster:
        if url:
            return redis.RedisCluster.from_url(url, **kwargs)
        return redis.RedisCluster(**kwargs)
    else:
        if url:
            return redis.Redis.from_url(url, **kwargs)
        return redis.Redis(**kwargs)
