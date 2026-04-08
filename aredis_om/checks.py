from weakref import WeakKeyDictionary

from redis.exceptions import AuthenticationError

from aredis_om.connections import get_redis_connection

_command_cache = WeakKeyDictionary()


def clear_command_cache():
    _command_cache.clear()


async def check_for_command(conn, cmd):
    cache_for_conn = _command_cache.setdefault(conn, {})
    if cmd in cache_for_conn:
        return cache_for_conn[cmd]
    try:
        cmd_info = await conn.execute_command("command", "info", cmd)
        result = all(cmd_info)
    except AuthenticationError:
        result = False
    cache_for_conn[cmd] = result
    return result


async def has_redis_json(conn=None):
    if conn is None:
        conn = get_redis_connection()
    command_exists = await check_for_command(conn, "json.set")
    return command_exists


async def has_redisearch(conn=None):
    if conn is None:
        conn = get_redis_connection()
    if await has_redis_json(conn):
        return True
    command_exists = await check_for_command(conn, "ft.search")
    return command_exists
