"""Hot-keys observability — `HOTKEYS START/GET/STOP/RESET` (Redis 8.6+).

Redis 8.6 introduced a built-in top-K hot-key tracker that samples command
load per key and reports the keys that account for the most CPU time or
network bytes. Previously this required the (deprecated) `redis-cli
--hotkeys` LFU scan, which only saw keys after eviction pressure and
required `maxmemory-policy` to be LFU-based.

The new tracker is opt-in and lightweight: a sampling probe runs for a
configurable duration (or until manually stopped) and reports ranked
results. It works in both standalone and cluster mode (where it can
target specific hash slots).

Example::

    import asyncio
    from aredis_om.observability import hotkeys_snapshot

    async def main():
        snapshot = await hotkeys_snapshot(db, duration_seconds=5)
        print(snapshot.top_by_cpu)   # [("user:1", 1234), ...]
        print(snapshot.top_by_net)   # [("blob:big", 987654), ...]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence

# ── data classes ────────────────────────────────────────────────────────

# Per-metric results come back as ordered [name, value, name, value, ...].
# Top-K results preserve insertion order (ranked), so we keep them as lists.


@dataclass
class HotKeysSnapshot:
    """Parsed result of ``HOTKEYS GET``.

    Attributes:
        tracking_active: ``True`` if sampling is still running.
        sample_ratio: 1/N sampling ratio (1 = sample every key).
        duration_ms: Total sampling duration in milliseconds.
        total_cpu_user_ms: User-mode CPU time consumed during sampling.
        total_cpu_sys_ms: Kernel-mode CPU time consumed during sampling.
        total_net_bytes: Network bytes attributable to all sampled keys.
        top_by_cpu: List of ``(key, microseconds)`` tuples ranked by CPU.
            Empty when the ``CPU`` metric was not requested.
        top_by_net: List of ``(key, bytes)`` tuples ranked by network bytes.
            Empty when the ``NET`` metric was not requested.
        raw: The raw (normalised-to-dict) server reply, for forward
            compatibility with fields this wrapper doesn't yet model.
    """

    tracking_active: bool = False
    sample_ratio: int = 1
    duration_ms: int = 0
    total_cpu_user_ms: int = 0
    total_cpu_sys_ms: int = 0
    total_net_bytes: int = 0
    top_by_cpu: list = field(default_factory=list)
    top_by_net: list = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"HotKeysSnapshot(active={self.tracking_active}, "
            f"cpu_ms={self.total_cpu_user_ms + self.total_cpu_sys_ms}, "
            f"net_bytes={self.total_net_bytes}, "
            f"top_by_cpu={len(self.top_by_cpu)}, "
            f"top_by_net={len(self.top_by_net)})"
        )


# ── helpers ─────────────────────────────────────────────────────────────

_METRIC_CPU = "CPU"
_METRIC_NET = "NET"


def _pairs_to_tuples(items: Iterable[Any]) -> list[tuple]:
    """Flatten ``[k1, v1, k2, v2, ...]`` into ``[(k1, v1), (k2, v2), ...]``."""
    out: list[tuple] = []
    items = list(items)
    for i in range(0, len(items) - 1, 2):
        out.append((_str(items[i]), _num(items[i + 1])))
    return out


def _str(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _num(x: Any) -> int:
    if isinstance(x, (bytes, bytearray)):
        return int(x.decode("utf-8"))
    return int(x)


# ── low-level commands ──────────────────────────────────────────────────


async def hotkeys_start(
    db: Any,
    *,
    metrics: Sequence[str] = (_METRIC_CPU,),
    top_k: int = 10,
    duration_seconds: int = 0,
    sample_ratio: int = 1,
    slots: Optional[Sequence[int]] = None,
) -> bool:
    """``HOTKEYS START`` — begin sampling hot keys.

    Args:
        db: Active Redis client.
        metrics: One or both of ``"CPU"`` and ``"NET"``. Determines which
            ranked sections appear in the eventual snapshot.
        top_k: Value of ``K`` for top-K tracking (default 10).
        duration_seconds: Sampling duration in seconds. ``0`` (default)
            means track until :func:`hotkeys_stop` is called. Must be
            between 1 and 1,000,000 when non-zero.
        sample_ratio: Sample 1/N keys (default 1 = every key). Higher
            values reduce overhead at the cost of accuracy.
        slots: Cluster mode only — restrict sampling to these hash slots.
            Default is all slots.

    Returns:
        ``True`` on success.
    """
    metrics = [m.upper() for m in metrics]
    for m in metrics:
        if m not in (_METRIC_CPU, _METRIC_NET):
            raise ValueError(f"metrics entries must be 'CPU' or 'NET'; got {m!r}")
    if not metrics:
        raise ValueError("at least one metric must be requested")

    args: list = ["HOTKEYS", "START", "METRICS", len(metrics), *metrics]
    args += ["COUNT", int(top_k)]
    args += ["DURATION", int(duration_seconds)]
    args += ["SAMPLE", int(sample_ratio)]
    if slots:
        args += ["SLOTS", len(slots), *[int(s) for s in slots]]

    await db.execute_command(*args)
    return True


async def hotkeys_stop(db: Any) -> bool:
    """``HOTKEYS STOP`` — stop sampling. Results remain available via GET."""
    await db.execute_command("HOTKEYS", "STOP")
    return True


async def hotkeys_reset(db: Any) -> bool:
    """``HOTKEYS RESET`` — free sampling state. Must be called after STOP."""
    await db.execute_command("HOTKEYS", "RESET")
    return True


async def hotkeys_get(db: Any) -> HotKeysSnapshot:
    """``HOTKEYS GET`` — retrieve the current sampling result."""
    raw = await db.execute_command("HOTKEYS", "GET")
    return _parse_snapshot(raw)


# ── high-level convenience ──────────────────────────────────────────────


async def hotkeys_snapshot(
    db: Any,
    *,
    metrics: Sequence[str] = (_METRIC_CPU, _METRIC_NET),
    top_k: int = 10,
    duration_seconds: int = 1,
    sample_ratio: int = 1,
    slots: Optional[Sequence[int]] = None,
) -> HotKeysSnapshot:
    """Run a complete sampling cycle and return the parsed snapshot.

    Equivalent to::

        await hotkeys_start(db, ...specs...)
        # ... wait for ``duration_seconds`` (the server stops automatically)
        await hotkeys_get(db)
        await hotkeys_reset(db)

    ``duration_seconds`` MUST be >= 1 when using this helper (the server
    enforces a non-zero minimum duration on `START`). To run until
    manually stopped, call :func:`hotkeys_start` directly with
    ``duration_seconds=0`` and poll :func:`hotkeys_get`.

    The caller is responsible for generating load during the sampling
    window. The helper blocks for the requested duration.
    """
    import asyncio

    if duration_seconds < 1:
        raise ValueError(
            "duration_seconds must be >= 1 for hotkeys_snapshot(); "
            "use hotkeys_start() for manual control"
        )

    await hotkeys_start(
        db,
        metrics=metrics,
        top_k=top_k,
        duration_seconds=duration_seconds,
        sample_ratio=sample_ratio,
        slots=slots,
    )
    # The server auto-stops at duration; pad by 1s to ensure completion.
    await asyncio.sleep(duration_seconds + 1)
    snapshot = await hotkeys_get(db)
    await hotkeys_reset(db)
    return snapshot


# ── capability probe ────────────────────────────────────────────────────


def _command_info_present(info: Any, name: str) -> bool:
    """Return True if a COMMAND INFO reply contains ``name``.

    Handles both RESP2 ``[[name, ...]]`` and RESP3 / redis-py 8
    ``{name: {...}}`` shapes.
    """
    if not info:
        return False
    name = name.lower()
    if isinstance(info, dict):
        return name in {k.lower() for k in info.keys()}
    # RESP2 list: ``[entry_or_none, ...]``.
    for entry in info:
        if entry is None:
            continue
        if isinstance(entry, (list, tuple)) and entry:
            if str(entry[0]).lower() == name:
                return True
    return False


async def has_hotkeys(db: Any) -> bool:
    """Return ``True`` if the server supports ``HOTKEYS`` (Redis 8.6+)."""
    try:
        info = await db.execute_command("COMMAND", "INFO", "HOTKEYS")
        return bool(_command_info_present(info, "hotkeys"))
    except Exception:
        return False


# ── parsing ─────────────────────────────────────────────────────────────


def _parse_snapshot(raw: Any) -> HotKeysSnapshot:
    """Normalise a HOTKEYS GET reply into a :class:`HotKeysSnapshot`."""
    snap = HotKeysSnapshot()
    if raw is None:
        return snap

    # The reply is either a dict (RESP3 / redis-py 8) or a flat RESP2 list
    # of pairs. redis-py 8 sometimes wraps the dict in a one-element list.
    if isinstance(raw, list) and len(raw) == 1 and isinstance(raw[0], dict):
        raw = raw[0]

    if isinstance(raw, dict):
        data = {k: v for k, v in raw.items()}
    else:
        data = {}
        items = list(raw)
        for i in range(0, len(items) - 1, 2):
            data[_str(items[i])] = items[i + 1]

    snap.raw = data
    snap.tracking_active = bool(int(data.get("tracking-active", 0) or 0))
    snap.sample_ratio = int(data.get("sample-ratio", 1) or 1)
    snap.duration_ms = int(data.get("collection-duration-ms", 0) or 0)
    snap.total_cpu_user_ms = int(data.get("total-cpu-time-user-ms", 0) or 0)
    snap.total_cpu_sys_ms = int(data.get("total-cpu-time-sys-ms", 0) or 0)
    snap.total_net_bytes = int(data.get("total-net-bytes", 0) or 0)

    cpu_section = data.get("by-cpu-time-us")
    if cpu_section:
        snap.top_by_cpu = _pairs_to_tuples(cpu_section)

    net_section = data.get("by-net-bytes")
    if net_section:
        snap.top_by_net = _pairs_to_tuples(net_section)

    return snap
