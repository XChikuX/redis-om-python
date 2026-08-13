# type: ignore
"""Unit tests for ``aredis_om.hotkeys._parse_snapshot``.

These are pure, offline tests: they feed synthetic ``HOTKEYS GET`` replies
(straight from the Redis 8.6+ documentation) into the parser and assert the
resulting :class:`HotKeysSnapshot`. No live Redis is required.

The point is to pin down the parser independently of any server, so that when
a live test sees an empty snapshot we can tell whether the *server* returned
nothing or whether the *parser* dropped data.
"""

import pytest

from aredis_om.hotkeys import HotKeysSnapshot, _parse_snapshot


def _resp2_flat() -> list:
    """A RESP2 (flat pair list) ``HOTKEYS GET`` reply, per the Redis docs."""
    return [
        "tracking-active",
        0,
        "sample-ratio",
        1,
        "selected-slots",
        [[0, 16383]],
        "all-commands-all-slots-us",
        103,
        "net-bytes-all-commands-all-slots",
        2042,
        "collection-start-time-unix-ms",
        1770824933147,
        "collection-duration-ms",
        1000,
        "total-cpu-time-user-ms",
        23,
        "total-cpu-time-sys-ms",
        7,
        "total-net-bytes",
        2038,
        "by-cpu-time-us",
        ["hotkey_001_counter", 29, "hotkey_001", 25],
        "by-net-bytes",
        ["hotkey_001", 446, "hotkey_002", 328],
    ]


def _resp3_dict() -> dict:
    """A RESP3 (map) ``HOTKEYS GET`` reply.

    Nested sections arrive as flat lists (the parser also handles dict
    sections via :func:`_pairs_to_tuples`).
    """
    return {
        "tracking-active": 0,
        "sample-ratio": 1,
        "collection-duration-ms": 1000,
        "total-cpu-time-user-ms": 23,
        "total-cpu-time-sys-ms": 7,
        "total-net-bytes": 2038,
        "by-cpu-time-us": ["hotkey_001", 25],
        "by-net-bytes": ["hotkey_001", 446],
    }


def test_parse_resp2_flat_list():
    snap = _parse_snapshot(_resp2_flat())

    assert isinstance(snap, HotKeysSnapshot)
    assert snap.tracking_active is False
    assert snap.sample_ratio == 1
    assert snap.duration_ms == 1000
    assert snap.total_cpu_user_ms == 23
    assert snap.total_cpu_sys_ms == 7
    assert snap.total_net_bytes == 2038
    assert snap.top_by_cpu == [("hotkey_001_counter", 29), ("hotkey_001", 25)]
    assert snap.top_by_net == [("hotkey_001", 446), ("hotkey_002", 328)]

    # ``raw`` must contain the metadata keys a live test checks for.
    assert "sample-ratio" in snap.raw
    assert "collection-duration-ms" in snap.raw


def test_parse_resp3_dict():
    snap = _parse_snapshot(_resp3_dict())

    assert snap.sample_ratio == 1
    assert snap.duration_ms == 1000
    assert snap.total_cpu_user_ms == 23
    assert snap.top_by_cpu == [("hotkey_001", 25)]
    assert snap.top_by_net == [("hotkey_001", 446)]
    assert "sample-ratio" in snap.raw


def test_parse_resp3_dict_wrapped_in_list():
    # redis-py 8 sometimes wraps the map in a one-element list.
    snap = _parse_snapshot([_resp3_dict()])
    assert snap.duration_ms == 1000
    assert "sample-ratio" in snap.raw


@pytest.mark.parametrize("empty", [None, [], {}])
def test_parse_empty_reply_returns_default(empty):
    snap = _parse_snapshot(empty)
    assert snap.raw == {}
    assert snap.duration_ms == 0
    assert snap.tracking_active is False
    assert snap.top_by_cpu == []
    assert snap.top_by_net == []
