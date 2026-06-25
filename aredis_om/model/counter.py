"""Atomic counter backed by the Redis ``INCREX`` command (Redis 8.8+).

``INCREX`` combines increment, bounds checking, optional saturation, and
expiration into a single atomic round trip — ideal for rate limiters,
view/download counters, and bounded counters.

On servers that predate ``INCREX`` (< 8.8) the counter transparently
falls back to ``INCRBY``/``INCRBYFLOAT`` + ``EXPIRE`` (two round trips,
no atomic bounds checking).

Example — rate limiting::

    from aredis_om import AtomicCounter

    counter = AtomicCounter(db, f"ratelimit:{user_id}")

    # Allow at most 100 requests per 60 s window.  ``ENX`` ensures the
    # TTL is set only on the first request in a window.
    new_val, applied = await counter.incr(
        expire=60, bounds=(0, 100), enx=True,
    )
    if applied == 0:
        raise RateLimitExceeded()

Example — float counter::

    total, delta = await counter.incr(amount=0.5)  # uses BYFLOAT
"""

from typing import Any, Optional, Tuple, Union

# Cache: connection id → bool, so we only probe INCREX once per client.
_increx_cache: "dict[int, bool]" = {}


class AtomicCounter:
    """A server-side atomic counter.

    Wraps ``INCREX`` (Redis 8.8+) with graceful degradation to
    ``INCRBY``/``INCRBYFLOAT`` + ``EXPIRE`` on older servers.
    """

    def __init__(self, db: Any, key: str):
        self._db = db
        self._key = key

    @property
    def key(self) -> str:
        return self._key

    # ── internals ──────────────────────────────────────────────────

    async def _increx_available(self) -> bool:
        """Return ``True`` when the server supports ``INCREX``."""
        conn_id = id(self._db)
        cached = _increx_cache.get(conn_id)
        if cached is not None:
            return cached
        try:
            info = await self._db.execute_command("COMMAND", "INFO", "increx")
            available = bool(info and all(info))
        except Exception:
            available = False
        _increx_cache[conn_id] = available
        return available

    @staticmethod
    def _build_increx_args(
        key: str,
        amount: Union[int, float],
        expire: Optional[int],
        bounds: Optional[Tuple],
        saturate: bool,
        enx: bool,
    ) -> list:
        args: list = ["INCREX", key]
        if isinstance(amount, float):
            args += ["BYFLOAT", str(amount)]
        elif amount != 1:
            args += ["BYINT", str(amount)]
        if bounds:
            lower, upper = bounds
            if lower is not None:
                args += ["LBOUND", str(lower)]
            if upper is not None:
                args += ["UBOUND", str(upper)]
        if saturate:
            args.append("SATURATE")
        if expire is not None:
            args += ["EX", str(int(expire))]
        if enx and expire is not None:
            args.append("ENX")
        return args

    # ── public API ─────────────────────────────────────────────────

    async def incr(
        self,
        amount: Union[int, float] = 1,
        *,
        expire: Optional[int] = None,
        bounds: Optional[
            Tuple[Optional[Union[int, float]], Optional[Union[int, float]]]
        ] = None,
        saturate: bool = False,
        enx: bool = False,
    ) -> Tuple[Union[int, float], Union[int, float]]:
        """Atomically increment the counter.

        Args:
            amount: Increment value.  ``int`` → ``BYINT``, ``float`` →
                ``BYFLOAT``.  Default ``1`` (integer increment by one).
            expire: TTL in seconds.  Sets ``EX`` on the key (preserved
                on subsequent calls unless ``PERSIST`` is used).
            bounds: Optional ``(lower, upper)`` pair.  Either element
                may be ``None``.  When the result would fall outside the
                bounds the operation is skipped and
                ``actual_increment`` is ``0`` (or capped when
                ``saturate=True``).
            saturate: Cap the result at the bounds instead of skipping.
            enx: Only set the TTL when the key currently has no TTL.
                Requires ``expire`` to be set.

        Returns:
            ``(new_value, actual_increment)``.  When an out-of-bounds
            result causes the operation to be skipped,
            ``actual_increment`` is ``0`` and ``new_value`` is the
            unchanged current value.
        """
        if await self._increx_available():
            args = self._build_increx_args(
                self._key, amount, expire, bounds, saturate, enx
            )
            result = await self._db.execute_command(*args)
            new_val, actual = result[0], result[1]
            if isinstance(amount, float):
                return float(new_val), float(actual)
            return int(new_val), int(actual)

        # Fallback: two round trips, no atomic bounds.
        return await self._incr_fallback(amount, expire, bounds, saturate)

    async def _incr_fallback(
        self,
        amount: Union[int, float],
        expire: Optional[int],
        bounds: Optional[Tuple],
        saturate: bool,
    ) -> Tuple[Union[int, float], Union[int, float]]:
        if isinstance(amount, float):
            new_val = await self._db.incrbyfloat(self._key, amount)
        else:
            new_val = await self._db.incrby(self._key, amount)

        actual: Union[int, float] = amount

        # Best-effort bounds enforcement (non-atomic).
        if bounds:
            lower, upper = bounds
            if saturate:
                if lower is not None and new_val < lower:
                    new_val = lower
                if upper is not None and new_val > upper:
                    new_val = upper
                await self._db.set(self._key, new_val)
            elif lower is not None and new_val < lower:
                new_val = await self._db.incrby(self._key, -amount)
                actual = 0
            elif upper is not None and new_val > upper:
                new_val = await self._db.incrby(self._key, -amount)
                actual = 0

        if expire is not None:
            await self._db.expire(self._key, expire)

        return new_val, actual

    async def value(self) -> Optional[Union[int, float]]:
        """Return the current counter value, or ``None`` if unset."""
        val = await self._db.get(self._key)
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return float(val)

    async def reset(self) -> None:
        """Set the counter to ``0`` (preserves TTL)."""
        await self._db.set(self._key, 0)

    async def persist(self) -> None:
        """Remove the TTL from the counter key."""
        await self._db.persist(self._key)

    async def delete(self) -> None:
        """Delete the counter key."""
        await self._db.delete(self._key)

    async def ttl(self) -> int:
        """Return the remaining TTL in seconds.

        ``-1`` if the key has no TTL, ``-2`` if the key does not exist.
        """
        return await self._db.ttl(self._key)


def clear_increx_cache() -> None:
    """Clear the cached INCREX capability results (for testing)."""
    _increx_cache.clear()
