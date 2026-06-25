"""Cluster administration: ``CLUSTER SLOT-STATS`` and ``CLUSTER MIGRATION``.

Redis 8.2+ shipped two long-awaited cluster-observability and
migration-management commands:

* ``CLUSTER SLOT-STATS`` — per-slot CPU, network, and key counters.
  Used for "which shard is hottest" troubleshooting.
* ``CLUSTER MIGRATION`` — start/stop/status introspection for
  slot migrations without using the heavyweight ``MIGRATE`` /
  ``CLUSTER SETSLOT`` dance.

Both commands require cluster mode (the server returns "This instance
has cluster support disabled" on standalone instances). Use the
``has_*`` capability probes to guard caller code.

Example::

    import asyncio
    from aredis_om.model.cluster_admin import ClusterAdmin

    async def main():
        admin = ClusterAdmin(db)
        stats = await admin.slot_stats(order_by="cpu-usec", desc=True, limit=5)
        for s in stats:
            print(f"slot {s['slot']}: {s['cpu-usec']}us")
"""

from __future__ import annotations

from typing import Any, Optional, Sequence, Union


def _str(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _num(x: Any) -> int:
    if isinstance(x, (bytes, bytearray)):
        return int(x.decode("utf-8"))
    return int(x)


class ClusterAdmin:
    """Wrapper around the cluster admin commands added in Redis 8.2+.

    All methods return ``None`` or empty results when invoked against a
    standalone server. Use :meth:`is_cluster_mode` to branch caller code
    on whether cluster features are available.
    """

    def __init__(self, db: Any):
        self._db = db

    # ── CLUSTER SLOT-STATS ──────────────────────────────────────────────

    async def slot_stats(
        self,
        *,
        order_by: Optional[str] = None,
        desc: bool = False,
        limit: Optional[int] = None,
        slot_range: Optional[tuple[int, int]] = None,
    ) -> list[dict]:
        """``CLUSTER SLOT-STATS [ORDERBY field [DESC]] [LIMIT n] [SLOTSRANGE s e]``.

        Returns per-slot stats as a list of dicts. Each dict's keys are
        the field names returned by the server (e.g. ``slot``, ``cpu-usec``,
        ``network-bytes-in``, ``network-bytes-out``, ``keys``).

        Args:
            order_by: Field name to sort by (e.g. ``"cpu-usec"``).
            desc: If True, sort descending (only used with ``order_by``).
            limit: Cap on number of returned slots.
            slot_range: ``(start, end)`` tuple to restrict to a slot range.

        Returns:
            List of dicts, one per slot.
        """
        args: list = ["CLUSTER", "SLOT-STATS"]
        if order_by:
            args += ["ORDERBY", order_by]
            if desc:
                args.append("DESC")
        if limit is not None:
            args += ["LIMIT", int(limit)]
        if slot_range is not None:
            args += ["SLOTSRANGE", int(slot_range[0]), int(slot_range[1])]
        raw = await self._db.execute_command(*args)
        return _parse_slot_stats(raw)

    # ── CLUSTER MIGRATION ───────────────────────────────────────────────

    async def migration_status(self) -> Optional[dict]:
        """``CLUSTER MIGRATION STATUS`` — current migration job state.

        Returns a dict (e.g. ``{"state": "none"}``) or ``None`` if no
        migration has been performed. The exact shape depends on the
        server version; see the [release notes][1] for the field list.

        [1]: https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/
        """
        raw = await self._db.execute_command("CLUSTER", "MIGRATION", "STATUS")
        if raw is None:
            return None
        if isinstance(raw, dict):
            return {k: v for k, v in raw.items()}
        if isinstance(raw, list) and len(raw) == 1 and isinstance(raw[0], dict):
            return dict(raw[0])
        return _pairs_to_dict(raw)

    async def migration_start(
        self, *, slots: Optional[Sequence[int]] = None
    ) -> bool:
        """``CLUSTER MIGRATION START`` — begin a migration.

        Args:
            slots: Optional list of slots to migrate; when omitted the
                server chooses a default target.

        Returns ``True`` on success. The actual slot target can be
        inspected via :meth:`migration_status`.
        """
        args: list = ["CLUSTER", "MIGRATION", "START"]
        if slots is not None:
            args += ["SLOTS", *[int(s) for s in slots]]
        await self._db.execute_command(*args)
        return True

    async def migration_stop(self) -> bool:
        """``CLUSTER MIGRATION STOP`` — halt an in-progress migration."""
        await self._db.execute_command("CLUSTER", "MIGRATION", "STOP")
        return True

    async def migration_abort(self) -> bool:
        """``CLUSTER MIGRATION ABORT`` — cancel and roll back."""
        await self._db.execute_command("CLUSTER", "MIGRATION", "ABORT")
        return True

    async def migration_log(self, count: int = 10) -> list:
        """``CLUSTER MIGRATION LOG [count]`` — recent migration log entries."""
        raw = await self._db.execute_command(
            "CLUSTER", "MIGRATION", "LOG", int(count)
        )
        if raw is None:
            return []
        return list(raw)

    # ── capability probes ───────────────────────────────────────────────

    async def is_cluster_mode(self) -> bool:
        """Return ``True`` if the server is running in cluster mode."""
        try:
            await self._db.execute_command("CLUSTER", "INFO")
            return True
        except Exception as exc:
            if "cluster support disabled" in str(exc).lower():
                return False
            return False

    async def has_slot_stats(self) -> bool:
        """Return ``True`` if ``CLUSTER SLOT-STATS`` is available (Redis 8.2+)."""
        if not await self.is_cluster_mode():
            return False
        try:
            await self._db.execute_command("CLUSTER", "SLOT-STATS")
            return True
        except Exception:
            return False

    async def has_migration(self) -> bool:
        """Return ``True`` if ``CLUSTER MIGRATION`` is available (Redis 8.2+)."""
        if not await self.is_cluster_mode():
            return False
        try:
            await self._db.execute_command("CLUSTER", "MIGRATION", "STATUS")
            return True
        except Exception:
            return False


# ── standalone helpers ──────────────────────────────────────────────────

async def is_cluster_mode(db: Any) -> bool:
    """Return ``True`` if the server is in cluster mode."""
    try:
        await db.execute_command("CLUSTER", "INFO")
        return True
    except Exception as exc:
        if "cluster support disabled" in str(exc).lower():
            return False
        return False


async def has_slot_stats(db: Any) -> bool:
    """Return ``True`` if ``CLUSTER SLOT-STATS`` is available (Redis 8.2+ cluster)."""
    if not await is_cluster_mode(db):
        return False
    try:
        await db.execute_command("CLUSTER", "SLOT-STATS")
        return True
    except Exception:
        return False


async def has_migration(db: Any) -> bool:
    """Return ``True`` if ``CLUSTER MIGRATION`` is available (Redis 8.2+ cluster)."""
    if not await is_cluster_mode(db):
        return False
    try:
        await db.execute_command("CLUSTER", "MIGRATION", "STATUS")
        return True
    except Exception:
        return False


# ── response parsing ────────────────────────────────────────────────────

def _parse_slot_stats(raw: Any) -> list[dict]:
    """Normalise ``CLUSTER SLOT-STATS`` reply to ``list[dict]``.

    The reply is typically a list of [k, v, k, v, ...] pairs under RESP2,
    or a dict under RESP3 / redis-py 8. If multiple slots are returned,
    they may be nested inside a top-level list.
    """
    if raw is None:
        return []
    # Multiple slots: list of dicts or list of pair lists.
    if isinstance(raw, list):
        if not raw:
            return []
        if all(isinstance(x, dict) for x in raw):
            return [dict(x) for x in raw]
        if all(isinstance(x, list) for x in raw):
            return [_pairs_to_dict(x) for x in raw]
        # Single slot reply (flat pairs).
        return [_pairs_to_dict(raw)]
    if isinstance(raw, dict):
        return [dict(raw)]
    return [_pairs_to_dict(raw)]


def _pairs_to_dict(raw: Any) -> dict:
    """Flatten a flat RESP2 ``[k1, v1, k2, v2, ...]`` reply to a dict."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    out: dict = {}
    items = list(raw)
    for i in range(0, len(items) - 1, 2):
        out[_str(items[i])] = items[i + 1]
    return out
