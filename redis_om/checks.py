from redis.exceptions import AuthenticationError
from weakref import WeakKeyDictionary

from redis_om.connections import get_redis_connection

_command_cache = WeakKeyDictionary()


def clear_command_cache():
    _command_cache.clear()


def check_for_command(conn, cmd):
    cache_for_conn = _command_cache.setdefault(conn, {})
    if cmd in cache_for_conn:
        return cache_for_conn[cmd]
    try:
        cmd_info = conn.execute_command("command", "info", cmd)
        result = all(cmd_info)
    except AuthenticationError:
        result = False
    cache_for_conn[cmd] = result
    return result


def has_redis_json(conn=None):
    if conn is None:
        conn = get_redis_connection()
    command_exists = check_for_command(conn, "json.set")
    return command_exists


def has_redisearch(conn=None):
    if conn is None:
        conn = get_redis_connection()
    if has_redis_json(conn):
        return True
    command_exists = check_for_command(conn, "ft.search")
    return command_exists
