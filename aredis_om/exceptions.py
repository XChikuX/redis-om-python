"""Redis exceptions re-exported for redis-om users.

Importing these from ``redis_om`` means application code never has to
reach into ``redis`` directly. The module mirrors ``redis.exceptions``:
every redis-py exception is available under its redis-py name, except
``ConnectionError`` and ``TimeoutError`` which shadow Python builtins.
Use the ``RedisConnectionError`` and ``RedisTimeoutError`` aliases for
those instead.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import redis.exceptions as _redis_exceptions


if TYPE_CHECKING:
    from redis.exceptions import (
        AskError,
        AuthenticationError,
        AuthenticationWrongNumberOfArgsError,
        AuthorizationError,
        BusyLoadingError,
        ChildDeadlockedError,
        ClusterCrossSlotError,
        ClusterDownError,
        ClusterError,
        CrossSlotTransactionError,
        DataError,
        ExecAbortError,
        ExternalAuthProviderError,
        IncorrectPolicyType,
        InvalidPipelineStack,
        InvalidResponse,
        LockError,
        LockNotOwnedError,
        MasterDownError,
        MaxConnectionsError,
        ModuleError,
        MovedError,
        NoPermissionError,
        NoScriptError,
        OutOfMemoryError,
        PubSubError,
        ReadOnlyError,
        RedisClusterException,
        RedisError,
        ResponseError,
        SlotNotCoveredError,
        TryAgainError,
        WatchError,
    )


_BUILTIN_COLLISIONS = frozenset({"ConnectionError", "TimeoutError"})

RedisConnectionError = _redis_exceptions.ConnectionError
RedisTimeoutError = _redis_exceptions.TimeoutError


def _is_exception_class(obj: object) -> bool:
    return inspect.isclass(obj) and issubclass(obj, BaseException)


def _populate(namespace: dict) -> list[str]:
    exported: list[str] = []
    for name in dir(_redis_exceptions):
        if name.startswith("_") or name in _BUILTIN_COLLISIONS:
            continue
        obj = getattr(_redis_exceptions, name)
        if not _is_exception_class(obj):
            continue
        namespace[name] = obj
        exported.append(name)
    return exported


__all__ = sorted(["RedisConnectionError", "RedisTimeoutError", *_populate(globals())])
