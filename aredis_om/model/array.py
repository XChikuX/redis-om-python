"""Sparse, index-addressable array backed by Redis Arrays (Redis 8.8+).

Redis Arrays are a preview data type for sparse, index-addressable
sequences of strings.  Unlike lists, elements are accessed directly by
index without allocating gaps.  This makes arrays well-suited for
timestamped event logs, ring buffers, and sliding-window analytics.

All methods use ``execute_command`` so they work on any redis-py 8.0+
client without depending on experimental high-level method bindings.

Example — event log::

    from aredis_om import RedisArray

    log = RedisArray(db, "events:click")

    await log.set(0, "login", "click", "purchase")
    print(await log.get(0))          # → "login"

    async for idx, val in await log.scan(0, 2):
        print(f"{idx}: {val}")

Example — ring buffer::

    readings = RedisArray(db, "sensor:temp")
    await readings.ring(100, "36.5", "36.7", "36.6")
    latest = await readings.last_items(3)
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union


class RedisArray:
    """A sparse, index-addressable Redis Array.

    Wraps the ``AR*`` command family (Redis 8.8+, currently in preview).
    """

    def __init__(self, db: Any, key: str):
        self._db = db
        self._key = key

    @property
    def key(self) -> str:
        return self._key

    # ── indexed access ─────────────────────────────────────────────

    async def set(self, index: int, *values: str) -> int:
        """``ARSET`` — set contiguous values starting at *index*.

        Returns the number of new slots created.
        """
        return await self._db.execute_command("ARSET", self._key, index, *values)

    async def get(self, index: int) -> Optional[str]:
        """``ARGET`` — get the value at *index*, or ``None`` if unset."""
        return await self._db.execute_command("ARGET", self._key, index)

    async def mset(self, mapping: Dict[int, str]) -> int:
        """``ARMSET`` — set multiple index-value pairs.

        Returns the number of new slots created.
        """
        args: List[Any] = ["ARMSET", self._key]
        for idx in sorted(mapping):
            args += [idx, mapping[idx]]
        return await self._db.execute_command(*args)

    async def mget(self, *indices: int) -> List[Optional[str]]:
        """``ARMGET`` — get values at multiple indices."""
        return await self._db.execute_command("ARMGET", self._key, *indices)

    async def get_range(self, start: int, end: int) -> List[Optional[str]]:
        """``ARGETRANGE`` — all values in ``[start, end]`` (nil for gaps)."""
        return await self._db.execute_command("ARGETRANGE", self._key, start, end)

    # ── iteration / scanning ───────────────────────────────────────

    async def scan(
        self, start: int, end: int, limit: Optional[int] = None
    ) -> List[Tuple[int, str]]:
        """``ARSCAN`` — existing elements only, as ``(index, value)`` pairs."""
        args: List[Any] = ["ARSCAN", self._key, start, end]
        if limit is not None:
            args += ["LIMIT", limit]
        raw = await self._db.execute_command(*args)
        # Accommodate both nested [[idx,val],...] and flat [idx,val,...] layouts.
        if isinstance(raw, list) and raw:
            if isinstance(raw[0], list):
                return [(int(pair[0]), pair[1]) for pair in raw]
            return [(int(raw[i]), raw[i + 1]) for i in range(0, len(raw) - 1, 2)]
        return []

    # ── sequential insertion ───────────────────────────────────────

    async def insert(self, *values: str) -> int:
        """``ARINSERT`` — append at the auto-advancing cursor.

        Returns the last index used.
        """
        return await self._db.execute_command("ARINSERT", self._key, *values)

    async def next_index(self) -> Optional[int]:
        """``ARNEXT`` — the next index ``insert`` would use."""
        return await self._db.execute_command("ARNEXT", self._key)

    async def seek(self, index: int) -> int:
        """``ARSEEK`` — reposition the insert cursor."""
        return await self._db.execute_command("ARSEEK", self._key, index)

    # ── ring buffer ────────────────────────────────────────────────

    async def ring(self, size: int, *values: str) -> int:
        """``ARRING`` — insert into a fixed-size circular buffer.

        Returns the last index written.
        """
        return await self._db.execute_command("ARRING", self._key, size, *values)

    async def last_items(self, count: int, rev: bool = False) -> List[str]:
        """``ARLASTITEMS`` — the *count* most recently inserted elements."""
        args: List[Any] = ["ARLASTITEMS", self._key, count]
        if rev:
            args.append("REV")
        result = await self._db.execute_command(*args)
        return result if isinstance(result, list) else []

    # ── deletion ───────────────────────────────────────────────────

    async def delete_at(self, *indices: int) -> int:
        """``ARDEL`` — delete elements at specific indices."""
        return await self._db.execute_command("ARDEL", self._key, *indices)

    async def delete_range(self, start: int, end: int) -> int:
        """``ARDELRANGE`` — delete all elements in ``[start, end]``."""
        return await self._db.execute_command("ARDELRANGE", self._key, start, end)

    # ── introspection ──────────────────────────────────────────────

    async def length(self) -> int:
        """``ARLEN`` — logical length (max index + 1)."""
        return await self._db.execute_command("ARLEN", self._key)

    async def count(self) -> int:
        """``ARCOUNT`` — number of non-empty elements."""
        return await self._db.execute_command("ARCOUNT", self._key)

    async def info(self, full: bool = False) -> Dict[str, Any]:
        """``ARINFO`` — array metadata (optionally with per-slice stats)."""
        args: List[Any] = ["ARINFO", self._key]
        if full:
            args.append("FULL")
        raw = await self._db.execute_command(*args)
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list):
            return {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        return {}

    # ── aggregation ────────────────────────────────────────────────

    async def aggregate(
        self,
        start: int,
        end: int,
        operation: str,
        value: Optional[str] = None,
    ) -> Any:
        """``AROP`` — single-pass aggregate over ``[start, end]``.

        *operation* is one of ``SUM``, ``MIN``, ``MAX``, ``AND``,
        ``OR``, ``XOR``, ``MATCH``, ``USED``.  For ``MATCH`` supply
        *value*.
        """
        args: List[Any] = ["AROP", self._key, start, end, operation]
        if value is not None:
            args.append(value)
        return await self._db.execute_command(*args)

    # ── text search ────────────────────────────────────────────────

    async def grep(
        self,
        start: Union[int, str],
        end: Union[int, str],
        predicates: Sequence[Tuple[str, str]],
        *,
        nocase: bool = False,
        with_values: bool = False,
        limit: Optional[int] = None,
    ) -> Any:
        """``ARGREP`` — find elements matching textual predicates.

        *predicates* is a list of ``(type, pattern)`` tuples where type
        is ``EXACT``, ``MATCH``, ``GLOB``, or ``RE``.  Multiple
        predicates are combined with ``OR``.

        When *with_values* is ``True``, returns
        ``[[index, value], ...]``; otherwise returns ``[index, ...]``.
        """
        args: List[Any] = ["ARGREP", self._key, start, end]
        for pred_type, pred_value in predicates:
            args += [pred_type, pred_value]
        if nocase:
            args.append("NOCASE")
        if with_values:
            args.append("WITHVALUES")
        if limit is not None:
            args += ["LIMIT", limit]
        return await self._db.execute_command(*args)
