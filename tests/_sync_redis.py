import os
from functools import lru_cache

from redis import Redis, RedisCluster
from redis.exceptions import AuthenticationError


def get_sync_redis_connection():
    kwargs = {"decode_responses": True}
    url = os.environ.get("REDIS_OM_URL")
    cluster = "cluster=true" in str(url).lower()

    if cluster:
        if url:
            return RedisCluster.from_url(url, **kwargs)
        return RedisCluster(**kwargs)

    if url:
        return Redis.from_url(url, **kwargs)
    return Redis(**kwargs)


@lru_cache(maxsize=2)
def has_command(cmd):
    conn = get_sync_redis_connection()
    try:
        return all(conn.execute_command("command", "info", cmd))
    except (AuthenticationError, ConnectionError, OSError):
        return False


def has_redis_json():
    return has_command("json.set")


def has_redisearch():
    if has_redis_json():
        return True
    return has_command("ft.search")
