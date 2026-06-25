"""Atomic string operations: compare-and-set, compare-and-delete,
``DIGEST``, and multi-key ``MSETEX``.

These commands — introduced across Redis 8.4 — close gaps that previously
required Lua scripts or ``MULTI``/``EXEC`` pipelines:

* **``SET ... IFEQ``** — atomic compare-and-set (CAS) for string keys.
  Only updates the key if its current value equals the expected value.
* **``SET ... IFNE``** — update only if the current value differs.
* **``DELEX ... IFEQ``** — atomic compare-and-delete (CAD).
* **``DIGEST``** — compute a stable hash of a key's value without
  transferring it (useful for cache validation and change detection).
* **``MSETEX``** — set multiple keys with a shared or per-call expiry in
  a single atomic round trip (replaces a ``SET``/``EXPIRE`` pipeline).

All methods use ``execute_command`` so they work on any redis-py 8.0+
client. On Redis < 8.4 the command-specific methods raise a Redis
``ResponseError`` — they are opt-in and never called automatically.

Example — compare-and-set::

    from aredis_om import AtomicString

    lock = AtomicString(db, "lock:resource1")
    got = await lock.compare_and_set(expected="idle", new="held")
    if got:
        try:
            await do_work()
        finally:
            await lock.compare_and_set(expected="held", new="idle")

Example — bulk set with expiry::

    from aredis_om import msetex

    n = await msetex(
        db,
        {"cache:user:1": "alice", "cache:user:2": "bob"},
        expire=60,
    )
"""

from typing import Any, Mapping, Optional

# Cache: connection id → set of unsupported command names.
_capability_cache: "dict[int, set]" = {}


def clear_atomic_string_cache() -> None:
    """Clear the cached capability results (for testing)."""
    _capability_cache.clear()


class AtomicString:
    """A thin wrapper around atomic string-key operations.

    Wraps ``SET ... IFEQ``/``IFNE``, ``DELEX ... IFEQ``, and ``DIGEST``
    for a single key.
    """

    def __init__(self, db: Any, key: str):
        self._db = db
        self._key = key

    @property
    def key(self) -> str:
        return self._key

    # ── compare-and-set ────────────────────────────────────────────

    async def compare_and_set(
        self, expected: str, new: str, *, expire: Optional[int] = None
    ) -> bool:
        """``SET ... IFEQ`` — atomic compare-and-set.

        Sets the key to ``new`` only if its current value equals
        ``expected``. This is the Redis 8.4 atomic CAS primitive for
        string keys.

        Args:
            expected: The value that must currently be stored.
            new: The value to write.
            expire: Optional TTL in seconds (added after the condition).

        Returns:
            ``True`` if the key was updated, ``False`` if the current
            value did not match ``expected``.
        """
        args: list = ["SET", self._key, new, "IFEQ", expected]
        if expire is not None:
            args += ["EX", int(expire)]
        result = await self._db.execute_command(*args)
        # SET returns "OK" on success, None (nil) on condition failure.
        return result is not None and result is not False

    async def set_if_not_equal(
        self, not_equal: str, new: str, *, expire: Optional[int] = None
    ) -> bool:
        """``SET ... IFNE`` — set only if current value differs.

        Args:
            not_equal: The value that must NOT be currently stored.
            new: The value to write.
            expire: Optional TTL in seconds.

        Returns:
            ``True`` if the key was updated.
        """
        args: list = ["SET", self._key, new, "IFNE", not_equal]
        if expire is not None:
            args += ["EX", int(expire)]
        result = await self._db.execute_command(*args)
        return result is not None and result is not False

    # ── compare-and-delete ─────────────────────────────────────────

    async def compare_and_delete(self, expected: str) -> bool:
        """``DELEX ... IFEQ`` — atomic compare-and-delete.

        Deletes the key only if its current value equals ``expected``.

        Args:
            expected: The value that must currently be stored.

        Returns:
            ``True`` if the key was deleted, ``False`` if the value did
            not match.
        """
        result = await self._db.execute_command(
            "DELEX", self._key, "IFEQ", expected
        )
        return bool(result)

    # ── digest ─────────────────────────────────────────────────────

    async def digest(self) -> Optional[str]:
        """``DIGEST`` — compute a stable hash of the key's value.

        Returns a hex digest string. The key value is never transferred
        to the client, making this useful for cache validation and
        change detection on large values.

        Returns:
            The hex digest, or ``None`` if the key doesn't exist.
        """
        result = await self._db.execute_command("DIGEST", self._key)
        return result if result else None

    # ── convenience accessors ──────────────────────────────────────

    async def get(self) -> Optional[str]:
        """Return the current value, or ``None`` if unset."""
        return await self._db.get(self._key)

    async def set(self, value: str, *, expire: Optional[int] = None) -> bool:
        """Plain ``SET`` (no condition).

        Returns ``True`` on success.
        """
        if expire is not None:
            result = await self._db.set(self._key, value, ex=int(expire))
        else:
            result = await self._db.set(self._key, value)
        return bool(result)

    async def delete(self) -> bool:
        """Delete the key. Returns ``True`` if it existed."""
        return bool(await self._db.delete(self._key))


async def msetex(
    db: Any,
    mapping: Mapping[str, str],
    *,
    expire: Optional[int] = None,
    expire_ms: Optional[int] = None,
    nx: bool = False,
    xx: bool = False,
    keepttl: bool = False,
) -> int:
    """``MSETEX`` — set multiple keys atomically with optional expiry.

    Sets all key-value pairs in ``mapping`` in a single atomic operation.
    Optionally applies a shared expiry and/or a condition (``NX``/``XX``).

    On Redis < 8.4 this raises a Redis ``ResponseError``. As a fallback
    for older servers, callers can use a ``MULTI``/``EXEC`` pipeline,
    but that is not atomic with respect to the condition flags.

    Args:
        db: Active Redis client.
        mapping: ``{key: value, ...}`` pairs to set.
        expire: Shared TTL in seconds (uses ``EX``).
        expire_ms: Shared TTL in milliseconds (uses ``PX``). Mutually
            exclusive with ``expire``.
        nx: Only set keys that don't exist (``NX``).
        xx: Only set keys that already exist (``XX``). Mutually exclusive
            with ``nx``.
        keepttl: Preserve existing TTLs on updated keys (``KEEPTTL``).

    Returns:
        ``1`` if all keys were set, ``0`` if the condition (``NX``/``XX``)
        prevented any writes.

    Example::

        n = await msetex(
            db,
            {"cache:a": "1", "cache:b": "2"},
            expire=60,
        )
    """
    if not mapping:
        return 0
    if nx and xx:
        raise ValueError("nx and xx are mutually exclusive")
    if expire is not None and expire_ms is not None:
        raise ValueError("expire and expire_ms are mutually exclusive")

    args: list = ["MSETEX", len(mapping)]
    for key, value in mapping.items():
        args.extend([key, value])
    # Condition and expiry flags come after all key-value pairs.
    if nx:
        args.append("NX")
    if xx:
        args.append("XX")
    if expire is not None:
        args += ["EX", int(expire)]
    if expire_ms is not None:
        args += ["PX", int(expire_ms)]
    if keepttl:
        args.append("KEEPTTL")

    return int(await db.execute_command(*args))
