"""Sorted set ``AGGREGATE COUNT`` operator (Redis 8.8+).

Redis 8.8 added ``COUNT`` as a new value for the ``AGGREGATE`` argument
on sorted-set union / intersection commands. Previously ``AGGREGATE``
only accepted ``SUM``, ``MIN``, or ``MAX`` — ``COUNT`` now lets you count
how many of the input sorted sets contain each element without
summing scores.

Commands updated:

* ``ZUNION`` / ``ZUNIONSTORE``
* ``ZINTER`` / ``ZINTERSTORE``

``ZDIFF`` / ``ZDIFFSTORE`` are unchanged: by definition, every output
element appears in exactly one source set, so the count is always 1.

Example — count tag co-occurrences::

    import asyncio
    from aredis_om.model.sorted_set import SortedSetOps

    async def main():
        ops = SortedSetOps(db)
        # 3 sorted sets: docs tagged "redis", "cache", "performance".
        # Each doc's score is its popularity.
        ...
        await ops.zunion_count(
            "co_occurrence",
            "redis", "cache", "performance",
        )
        # → result: { doc_id : count_of_3_tags_the_doc_has }
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence, Tuple, Union


class SortedSetOps:
    """Wrapper around ``AGGREGATE COUNT`` for sorted-set unions and
    intersections on Redis 8.8+.

    Each method issues one round trip and returns the parsed result.
    For the read variants (``zunion_count`` / ``zinter_count``) the
    caller may request ``with_scores=True`` to receive ``(member, count)``
    tuples.
    """

    def __init__(self, db: Any):
        self._db = db

    # ── ZUNIONSTORE destination key [key ...] AGGREGATE COUNT ───────────

    async def zunionstore_count(self, dest: str, *sources: str) -> int:
        """``ZUNIONSTORE dest numkeys key... AGGREGATE COUNT`` — write.

        Stores the per-element **count** of input sets that contain each
        member as the score. Returns the number of elements written.
        """
        return int(
            await self._db.execute_command(
                "ZUNIONSTORE",
                dest,
                len(sources),
                *sources,
                "AGGREGATE",
                "COUNT",
            )
        )

    # ── ZINTERSTORE destination key [key ...] AGGREGATE COUNT ───────────

    async def zinterstore_count(self, dest: str, *sources: str) -> int:
        """``ZINTERSTORE dest numkeys key... AGGREGATE COUNT`` — write.

        Stores the per-element count of input sets containing each
        member (equivalent to the intersection size, always a positive
        integer for members present in all source sets). Returns the
        number of elements written.
        """
        return int(
            await self._db.execute_command(
                "ZINTERSTORE",
                dest,
                len(sources),
                *sources,
                "AGGREGATE",
                "COUNT",
            )
        )

    # ── ZUNION numkeys key [key ...] AGGREGATE COUNT ────────────────────

    async def zunion_count(
        self, *sources: str
    ) -> Union[list[str], list[Tuple[str, int]]]:
        """``ZUNION numkeys key... AGGREGATE COUNT`` — read.

        Returns:

        * ``list[str]`` when called with ``with_scores=False`` (default).
        * ``list[tuple[str, int]]`` when called with
          ``with_scores=True``: each tuple is ``(member, count)``.

        Use the ``with_scores`` keyword argument to switch.
        """
        return await self._aggregate_count(
            "ZUNION",
            sources,
            with_scores=False,
        )

    async def zunion_count_with_scores(
            self, *sources: str
        ) -> Union[list[str], list[Tuple[str, int]]]:
            """``ZUNION ... AGGREGATE COUNT WITHSCORES`` — convenience for
            tuple-form results.

            Returns ``list[tuple[str, int]]`` when scores are present; the
            declared ``Union`` widens the type to satisfy mypy because the
            underlying parser may return bare members if the server replies
            without WITHSCORES (it shouldn't, but the union documents that
            ``_aggregate_count`` is the source of truth).
            """
            return await self._aggregate_count(
                "ZUNION",
                sources,
                with_scores=True,
            )

    # ── ZINTER numkeys key [key ...] AGGREGATE COUNT ────────────────────

    async def zinter_count(
        self, *sources: str
    ) -> Union[list[str], list[Tuple[str, int]]]:
        """``ZINTER numkeys key... AGGREGATE COUNT`` — read.

        Members returned are those present in every source set. Scores
        are the count of source sets containing each member — always
        equal to ``len(sources)`` for this command.
        """
        return await self._aggregate_count(
            "ZINTER",
            sources,
            with_scores=False,
        )

    async def zinter_count_with_scores(
            self, *sources: str
        ) -> Union[list[str], list[Tuple[str, int]]]:
            """``ZINTER ... AGGREGATE COUNT WITHSCORES``."""
            return await self._aggregate_count(
                "ZINTER",
                sources,
                with_scores=True,
            )

    # ── internal ────────────────────────────────────────────────────────

    async def _aggregate_count(
        self,
        cmd: str,
        sources: Sequence[str],
        *,
        with_scores: bool,
    ) -> Union[list[str], list[Tuple[str, int]]]:
        if len(sources) < 1:
            raise ValueError(f"{cmd} requires at least one source key")
        args: list = [cmd, len(sources), *sources, "AGGREGATE", "COUNT"]
        if with_scores:
            args.append("WITHSCORES")
        raw = await self._db.execute_command(*args)
        return _parse_zresult(raw, with_scores)


# ── helpers ─────────────────────────────────────────────────────────────


def _parse_zresult(
    raw: Any, with_scores: bool
) -> Union[list[str], list[Tuple[str, int]]]:
    """Normalise a ZUNION/ZINTER reply.

    Handles three shapes:

    * RESP3 / redis-py 8: ``[[member, score], [member, score], ...]``
    * RESP3 dict: ``{member: score}``
    * RESP2 flat: ``[member, score, member, score, ...]``
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
        if not with_scores:
            return [str(k) for k in raw.keys()]
        return [(_str(k), int(_num(v))) for k, v in raw.items()]
    items = list(raw)
    if not items:
        return []

    # RESP3 with WITHSCORES returns a list of [member, score] pairs.
    if isinstance(items[0], (list, tuple)):
        if not with_scores:
            return [_str(pair[0]) for pair in items]
        return [(_str(pair[0]), int(_num(pair[1]))) for pair in items]

    # RESP2 flat list.
    if not with_scores:
        return [_str(x) for x in items]
    out: list[Tuple[str, int]] = []
    for i in range(0, len(items) - 1, 2):
        out.append((_str(items[i]), int(_num(items[i + 1]))))
    return out


def _str(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _num(x: Any) -> int:
    if isinstance(x, (bytes, bytearray)):
        return int(x.decode("utf-8"))
    return int(x)


# ── capability probe ────────────────────────────────────────────────────


async def has_aggregate_count(db: Any) -> bool:
    """Return ``True`` if ``ZUNION ... AGGREGATE COUNT`` is supported (8.8+)."""
    try:
        # Use a temporary key; if COUNT is unknown, server returns
        # ``ERR syntax error`` rather than crashing the client.
        await db.execute_command(
            "ZUNION", 1, "__agg_count_probe__", "AGGREGATE", "COUNT"
        )
        await db.delete("__agg_count_probe__")
        return True
    except Exception:
        return False
