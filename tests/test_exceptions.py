import inspect

import redis.exceptions as redis_exceptions

from aredis_om import (
    AuthenticationError,
    RedisConnectionError,
    RedisError,
    RedisTimeoutError,
    ResponseError,
    exceptions,
)


_BUILTIN_COLLISIONS = {"ConnectionError", "TimeoutError"}


def _redis_exception_names() -> set[str]:
    return {
        name
        for name in dir(redis_exceptions)
        if not name.startswith("_")
        and inspect.isclass(getattr(redis_exceptions, name))
        and issubclass(getattr(redis_exceptions, name), BaseException)
        and name not in _BUILTIN_COLLISIONS
    }


def test_exceptions_mirror_redis_exceptions():
    expected = _redis_exception_names()
    assert expected <= set(exceptions.__all__)
    for name in expected:
        assert getattr(exceptions, name) is getattr(redis_exceptions, name)


def test_builtin_collisions_are_not_exported():
    assert not hasattr(exceptions, "ConnectionError")
    assert not hasattr(exceptions, "TimeoutError")
    assert "ConnectionError" not in exceptions.__all__
    assert "TimeoutError" not in exceptions.__all__


def test_safe_aliases_map_to_redis_errors():
    assert exceptions.RedisConnectionError is redis_exceptions.ConnectionError
    assert exceptions.RedisTimeoutError is redis_exceptions.TimeoutError
    assert "RedisConnectionError" in exceptions.__all__
    assert "RedisTimeoutError" in exceptions.__all__


def test_non_exception_names_are_not_exported():
    assert not hasattr(exceptions, "Enum")
    assert not hasattr(exceptions, "ExceptionType")


def test_top_level_curated_exports():
    assert ResponseError is redis_exceptions.ResponseError
    assert RedisError is redis_exceptions.RedisError
    assert AuthenticationError is redis_exceptions.AuthenticationError
    assert RedisConnectionError is redis_exceptions.ConnectionError
    assert RedisTimeoutError is redis_exceptions.TimeoutError
