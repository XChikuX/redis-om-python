"""Bitmap operations: new ``BITOP`` variants introduced in Redis 8.2.

Redis 8.2 extended ``BITOP`` with four operators that previously required
either Lua scripts or multiple round trips:

* **``BITOP DIFF dest src1 src2``**  — set difference:
  ``dest := src1 AND NOT src2``.
* **``BITOP DIFF1 dest src1 src2``** — complement of DIFF:
  ``dest := src2 AND NOT src1``.
* **``BITOP ANDOR dest src1 src2 [src3 ...]``** — combined intersection
  and union: ``dest := src1 AND (src2 OR src3 OR ...)``.
* **``BITOP ONE dest src1 src2 [src3 ...]``** — exactly-one (XOR-of-sets
  for low cardinalities): ``dest`` has a bit set iff **exactly one** of
  the sources has that bit set.

All four return the length of the destination string (in bytes), like
the legacy ``AND``/``OR``/``XOR``/``NOT`` operators.

Example::

    import asyncio
    from aredis_om.model.bitmap import BitmapOps

    async def main():
        ops = BitmapOps(db)
        # dest := a AND NOT b
        n = await ops.diff("dest", "a", "b")
"""

from __future__ import annotations

from typing import Any, Sequence


class BitmapOps:
    """Thin wrapper around the ``BITOP`` variants added in Redis 8.2+.

    Each method issues a single ``BITOP <op> dest src...`` command and
    returns the length of the destination string in bytes.
    """

    def __init__(self, db: Any):
        self._db = db

    async def diff(self, dest: str, src1: str, src2: str) -> int:
        """``BITOP DIFF`` — ``dest := src1 AND NOT src2``.

        Equivalent to "bits set in ``src1`` but not in ``src2``".
        Useful for set-difference over co-indexed bitmaps
        (e.g. "users in segment A who are not in segment B").
        """
        return int(
            await self._db.execute_command(
                "BITOP", "DIFF", dest, src1, src2
            )
        )

    async def diff1(self, dest: str, src1: str, src2: str) -> int:
        """``BITOP DIFF1`` — ``dest := src2 AND NOT src1``.

        The complement of :meth:`diff`. Provided as a single round trip
        so callers do not need to swap argument order or compute NOT
        separately.
        """
        return int(
            await self._db.execute_command(
                "BITOP", "DIFF1", dest, src1, src2
            )
        )

    async def andor(self, dest: str, src1: str, *others: str) -> int:
        """``BITOP ANDOR`` — ``dest := src1 AND (others[0] OR others[1] OR ...)``.

        Requires at least one ``others`` key. Equivalent to:

            bitmap_of(src1) ∩ (bitmap_of(others[0]) ∪ bitmap_of(others[1]) ...)

        Useful for "users in segment A who are also in segment B or C".
        """
        if not others:
            raise ValueError("andor() requires at least two source keys")
        return int(
            await self._db.execute_command(
                "BITOP", "ANDOR", dest, src1, *others
            )
        )

    async def one(self, dest: str, *sources: str) -> int:
        """``BITOP ONE`` — exactly one of ``sources`` is set.

        Sets a bit in ``dest`` iff **exactly one** of the ``sources``
        has that bit set. Equivalent to a low-cardinality XOR-of-sets.

        Requires at least two source keys.
        """
        if len(sources) < 2:
            raise ValueError("one() requires at least two source keys")
        return int(
            await self._db.execute_command(
                "BITOP", "ONE", dest, *sources
            )
        )

    # ── legacy passthroughs (for completeness) ─────────────────────────

    async def and_(self, dest: str, *sources: str) -> int:
        """``BITOP AND`` — intersection of all sources."""
        return int(
            await self._db.execute_command(
                "BITOP", "AND", dest, *sources
            )
        )

    async def or_(self, dest: str, *sources: str) -> int:
        """``BITOP OR`` — union of all sources."""
        return int(
            await self._db.execute_command(
                "BITOP", "OR", dest, *sources
            )
        )

    async def xor(self, dest: str, *sources: str) -> int:
        """``BITOP XOR`` — symmetric difference of all sources."""
        return int(
            await self._db.execute_command(
                "BITOP", "XOR", dest, *sources
            )
        )

    async def not_(self, dest: str, source: str) -> int:
        """``BITOP NOT`` — bitwise complement of ``source``."""
        return int(
            await self._db.execute_command(
                "BITOP", "NOT", dest, source
            )
        )


# ── capability probe ────────────────────────────────────────────────────

async def has_bitmap_ops(db: Any) -> bool:
    """Return ``True`` if the server supports ``BITOP DIFF`` (Redis 8.2+).

    Probes by attempting ``BITOP DIFF`` on two throw-away keys; returns
    ``False`` if the server responds with an "unknown BITOP op" error.
    """
    try:
        # COMMAND INFO for BITOP doesn't differentiate by op, so we probe
        # by running DIFF on keys that don't exist (returns 0 bytes).
        n = await db.execute_command(
            "BITOP", "DIFF", "__bitmap_probe__", "__a__", "__b__"
        )
        await db.delete("__bitmap_probe__", "__a__", "__b__")
        return n is not None
    except Exception:
        return False
