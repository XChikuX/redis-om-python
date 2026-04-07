from redis.exceptions import AuthenticationError

from redis_om.connections import get_redis_connection


_command_cache = {}


def clear_command_cache():
    _command_cache.clear()


def check_for_command(conn, cmd):
    cache_key = (id(conn), cmd)
    if cache_key in _command_cache:
        return _command_cache[cache_key]
    try:
        cmd_info = conn.execute_command("command", "info", cmd)
        result = all(cmd_info)
    except AuthenticationError:
        result = False
    _command_cache[cache_key] = result
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
