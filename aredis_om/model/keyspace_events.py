"""Keyspace event notification flags and helpers (Redis 2.8+).

The Redis keyspace notification system emits Pub/Sub messages when
certain events happen on the data set. Events are configured with the
``notify-keyspace-events`` configuration parameter and via the
``CONFIG SET`` command.

This module centralises the event flags as a constants module and
provides helpers for building flag combinations. It works on any
Redis 2.8+ instance — no special server version is required.

Common flag groups::

    E   Keyevent events (fire on every event type)
    K   Keyspace events (fire on the actual key)
    g   Generic commands (DEL, EXPIRE, RENAME, ...)
    $   String commands
    l   List commands
    s   Set commands
    h   Hash commands
    z   Sorted set commands
    x   Expired events (events generated every time a key expires)
    e   Evicted events (events generated when a key is evicted)
    t   Stream commands
    m   Key miss events
    n   New key events
    A   Alias for "g$lshzxet"

Example::

    from aredis_om.model.keyspace_events import (
        KeyspaceEvents,
        build_flags,
        enable_keyspace_events,
    )

    flags = build_flags(
        KeyspaceEvents.KEYSPACE_PREFIX,
        KeyspaceEvents.GENERIC_COMMANDS,
        KeyspaceEvents.EXPIRED_EVENTS,
    )
    # flags = "Kgx"
    await enable_keyspace_events(db, flags)

Reference: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
"""

from __future__ import annotations

from typing import Any


class KeyspaceEvents:
    """Constants for the keyspace notification flags.

    These flags are toggled via the ``notify-keyspace-events``
    configuration parameter (or the ``CONFIG SET`` command). See the
    [Redis docs][1] for the full specification.

    [1]: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
    """

    # ── prefix toggles ────────────────────────────────────────────────
    KEYSPACE_PREFIX = "K"
    KEYEVENT_PREFIX = "E"
    ALL_KEY_EVENTS_ALIAS = "A"

    # ── category toggles ───────────────────────────────────────────────
    GENERIC_COMMANDS = "g"  # DEL, EXPIRE, RENAME, SORT, ...
    STRING_COMMANDS = "$"  # SET, SETRANGE, APPEND, INCR, ...
    LIST_COMMANDS = "l"  # LPUSH, RPUSH, LPOP, RPOP, ...
    SET_COMMANDS = "s"  # SADD, SREM, SPOP, SMOVE, ...
    HASH_COMMANDS = "h"  # HSET, HDEL, HINCRBY, ...
    SORTED_SET_COMMANDS = "z"  # ZADD, ZREM, ZINCRBY, ...
    STREAM_COMMANDS = "t"  # XADD, XDEL, XTRIM, ...

    # ── special events ────────────────────────────────────────────────
    EXPIRED_EVENTS = "x"  # fired when a key expires
    EVICTED_EVENTS = "e"  # fired when a key is evicted by maxmemory
    KEY_MISS_EVENTS = "m"  # fired when a key is accessed but missing
    NEW_KEY_EVENTS = "n"  # fired when a new key is added

    # ── builtin presets ───────────────────────────────────────────────
    ALL_EVENTS_PRESET = "AKE"  # all events, both prefix styles
    EXPIRATIONS_PRESET = "Ex"  # expired events only, keyevent prefix

    # ── event identifiers (the strings published on the channel) ──────
    GENERIC = "generic"
    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    EXPIRED = "expired"
    EVICTED = "evicted"
    STREAM = "stream"
    MISS = "miss"
    NEW = "new"


# Backwards-compatible alias for older pyredis-om releases.
KeyspaceEventFlags = KeyspaceEvents


def build_flags(*flags: str) -> str:
    """Combine multiple keyspace event flags into a single string.

    Convenience wrapper for building ``notify-keyspace-events`` strings.
    Order does not matter; duplicate flags are removed.

    Example::

        from aredis_om.model.keyspace_events import (
            KeyspaceEvents,
            build_flags,
        )

        flags = build_flags(
            KeyspaceEvents.KEYSPACE_PREFIX,
            KeyspaceEvents.GENERIC_COMMANDS,
            KeyspaceEvents.EXPIRED_EVENTS,
        )
        # → "Kgx"
    """
    seen: set = set()
    out: list = []
    for f in flags:
        for ch in f:
            if ch not in seen:
                seen.add(ch)
                out.append(ch)
    return "".join(out)


async def enable_keyspace_events(db: Any, flags: str) -> bool:
    """Enable the given keyspace event flags via ``CONFIG SET``.

    Args:
        db: Active Redis client.
        flags: A non-empty flag string (e.g. ``"Kgx"``). Pass ``""`` to
            disable notifications.

    Returns ``True`` on success. The setting is server-global; reset it
    explicitly when finished if you don't want to affect other clients
    on the same server.
    """
    await db.execute_command("CONFIG", "SET", "notify-keyspace-events", flags)
    return True


async def disable_keyspace_events(db: Any) -> bool:
    """Disable keyspace notifications (sets flag string to ``""``)."""
    return await enable_keyspace_events(db, "")
