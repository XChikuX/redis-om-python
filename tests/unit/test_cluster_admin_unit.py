# type: ignore
"""Unit tests for ClusterAdmin — parsers, helpers, and command construction.

These tests do **not** require a running Redis: they exercise pure-Python
parsing logic, capability-probe helpers, error branches in ``is_cluster_mode``,
and ``execute_command`` argument assembly.

For end-to-end coverage of the actual Redis commands see ``test_cluster_admin.py``.
"""

import pytest

from aredis_om.model.cluster_admin import (
    ClusterAdmin,
    _num,
    _pairs_to_dict,
    _parse_slot_stats,
    _str,
    has_migration,
    has_slot_stats,
    is_cluster_mode,
)


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


# ── low-level helpers ─────────────────────────────────────────────────────


class TestStrHelper:
    def test_str_from_bytes(self):
        assert _str(b"hello") == "hello"

    def test_str_from_bytearray(self):
        assert _str(bytearray(b"world")) == "world"

    def test_str_passthrough(self):
        assert _str(42) == "42"

    def test_str_passes_through_str(self):
        assert _str("already") == "already"


class TestNumHelper:
    def test_num_from_bytes(self):
        assert _num(b"42") == 42

    def test_num_from_bytearray(self):
        assert _num(bytearray(b"7")) == 7

    def test_num_passthrough_int(self):
        assert _num(123) == 123

    def test_num_accepts_string(self):
        assert _num("99") == 99


# ── _pairs_to_dict ────────────────────────────────────────────────────────


class TestPairsToDict:
    def test_flat_pairs_to_dict(self):
        raw = [b"slot", b"0", b"cpu-usec", b"1234"]
        out = _pairs_to_dict(raw)
        assert out == {"slot": b"0", "cpu-usec": b"1234"}

    def test_pairs_to_dict_with_str_keys(self):
        raw = ["a", 1, "b", 2]
        assert _pairs_to_dict(raw) == {"a": 1, "b": 2}

    def test_pairs_to_dict_none_returns_empty(self):
        assert _pairs_to_dict(None) == {}

    def test_pairs_to_dict_already_dict_returns_copy(self):
        original = {"x": 1, "y": 2}
        out = _pairs_to_dict(original)
        assert out == original
        assert out is not original

    def test_pairs_to_dict_empty_iterable(self):
        assert _pairs_to_dict([]) == {}

    def test_pairs_to_dict_odd_length_drops_last_key(self):
        # An odd-length list leaves the trailing key unpaired; verify we
        # don't crash and the trailing key is silently dropped.
        out = _pairs_to_dict(["a", 1, "b"])
        assert out == {"a": 1}


# ── _parse_slot_stats ─────────────────────────────────────────────────────


class TestParseSlotStats:
    def test_none_returns_empty(self):
        assert _parse_slot_stats(None) == []

    def test_empty_list_returns_empty(self):
        assert _parse_slot_stats([]) == []

    def test_dict_reply_wrapped_in_list(self):
        raw = {"slot": "0", "cpu-usec": "100"}
        out = _parse_slot_stats(raw)
        assert out == [{"slot": "0", "cpu-usec": "100"}]

    def test_dict_list_multiple_slots(self):
        raw = [
            {"slot": "0", "cpu-usec": "100"},
            {"slot": "1", "cpu-usec": "200"},
        ]
        out = _parse_slot_stats(raw)
        assert len(out) == 2
        assert out[0] == {"slot": "0", "cpu-usec": "100"}
        assert out[1] == {"slot": "1", "cpu-usec": "200"}

    def test_flat_pairs_single_slot(self):
        raw = [b"slot", b"0", b"cpu-usec", b"100"]
        out = _parse_slot_stats(raw)
        assert out == [{"slot": b"0", "cpu-usec": b"100"}]

    def test_list_of_pair_lists_multiple_slots(self):
        raw = [
            [b"slot", b"0", b"cpu-usec", b"100"],
            [b"slot", b"1", b"cpu-usec", b"200"],
        ]
        out = _parse_slot_stats(raw)
        assert len(out) == 2
        assert out[0]["slot"] == b"0"
        assert out[1]["slot"] == b"1"

    def test_unknown_shape_falls_through(self):
        # A bare string has no pairs to extract; the function still returns
        # a single-item list via the fall-through ``_pairs_to_dict``.
        raw = "notalist"
        out = _parse_slot_stats(raw)
        assert isinstance(out, list)


# ── capability helpers — fake ``db`` objects ───────────────────────────────


class _FakeDb:
    """A tiny stand-in for an async redis client.

    Each ``execute_command`` call records its arguments and returns the
    queued response or raises the queued exception.
    """

    def __init__(self, *, side_effects=None, return_values=None):
        self.calls = []
        self._side_effects = list(side_effects or [])
        self._return_values = list(return_values or [])

    async def execute_command(self, *args):
        self.calls.append(args)
        if self._side_effects:
            exc = self._side_effects.pop(0)
            if isinstance(exc, Exception):
                raise exc
            return exc
        if self._return_values:
            return self._return_values.pop(0)
        return None


class TestIsClusterModeStandaloneHelper:
    @py_test_mark_asyncio
    async def test_returns_true_when_command_succeeds(self):
        db = _FakeDb(return_values=[b"cluster_enabled:1\r\n"])
        assert await is_cluster_mode(db) is True
        assert db.calls == [("CLUSTER", "INFO")]

    @py_test_mark_asyncio
    async def test_returns_false_when_cluster_disabled_message(self):
        exc = RuntimeError("ERR This instance has cluster support disabled")
        db = _FakeDb(side_effects=[exc])
        assert await is_cluster_mode(db) is False

    @py_test_mark_asyncio
    async def test_returns_false_when_unexpected_exception(self):
        db = _FakeDb(side_effects=[RuntimeError("connection refused")])
        assert await is_cluster_mode(db) is False

    @py_test_mark_asyncio
    async def test_message_case_insensitive(self):
        exc = RuntimeError("CLUSTER SUPPORT DISABLED in this build")
        db = _FakeDb(side_effects=[exc])
        assert await is_cluster_mode(db) is False


class TestHasSlotStatsStandaloneHelper:
    @py_test_mark_asyncio
    async def test_skips_probe_when_not_cluster(self):
        # First call returns False on is_cluster_mode; has_slot_stats
        # should not even probe CLUSTER SLOT-STATS.
        exc = RuntimeError("cluster support disabled")
        db = _FakeDb(side_effects=[exc])
        assert await has_slot_stats(db) is False
        assert db.calls == [("CLUSTER", "INFO")]

    @py_test_mark_asyncio
    async def test_returns_true_when_command_succeeds(self):
        # First call: is_cluster_mode succeeds. Second call: SLOT-STATS succeeds.
        db = _FakeDb(return_values=["cluster_info_payload", "slot_stats_payload"])
        assert await has_slot_stats(db) is True
        assert db.calls == [("CLUSTER", "INFO"), ("CLUSTER", "SLOT-STATS")]

    @py_test_mark_asyncio
    async def test_returns_false_when_slot_stats_fails(self):
        # Cluster mode yes, but SLOT-STATS raises (e.g. older Redis).
        db = _FakeDb(
            return_values=["cluster_info"],
            side_effects=[RuntimeError("unknown command")],
        )
        assert await has_slot_stats(db) is False


class TestHasMigrationStandaloneHelper:
    @py_test_mark_asyncio
    async def test_skips_probe_when_not_cluster(self):
        exc = RuntimeError("cluster support disabled")
        db = _FakeDb(side_effects=[exc])
        assert await has_migration(db) is False
        assert db.calls == [("CLUSTER", "INFO")]

    @py_test_mark_asyncio
    async def test_returns_true_when_command_succeeds(self):
        db = _FakeDb(return_values=["cluster_info", "migration_status"])
        assert await has_migration(db) is True
        assert db.calls == [("CLUSTER", "INFO"), ("CLUSTER", "MIGRATION", "STATUS")]

    @py_test_mark_asyncio
    async def test_returns_false_when_migration_fails(self):
        db = _FakeDb(
            return_values=["cluster_info"],
            side_effects=[RuntimeError("unknown command")],
        )
        assert await has_migration(db) is False


# ── instance method capability probes ────────────────────────────────────


class TestInstanceProbes:
    @py_test_mark_asyncio
    async def test_is_cluster_mode_method_returns_true(self):
        db = _FakeDb(return_values=[b"ok"])
        admin = ClusterAdmin(db)
        assert await admin.is_cluster_mode() is True

    @py_test_mark_asyncio
    async def test_is_cluster_mode_method_handles_disabled(self):
        db = _FakeDb(side_effects=[RuntimeError("cluster support disabled")])
        admin = ClusterAdmin(db)
        assert await admin.is_cluster_mode() is False

    @py_test_mark_asyncio
    async def test_is_cluster_mode_method_handles_other_exception(self):
        db = _FakeDb(side_effects=[RuntimeError("boom")])
        admin = ClusterAdmin(db)
        # Any non-matching exception → False.
        assert await admin.is_cluster_mode() is False

    @py_test_mark_asyncio
    async def test_has_slot_stats_false_when_not_cluster(self):
        db = _FakeDb(side_effects=[RuntimeError("cluster support disabled")])
        admin = ClusterAdmin(db)
        assert await admin.has_slot_stats() is False

    @py_test_mark_asyncio
    async def test_has_slot_stats_true_when_command_ok(self):
        db = _FakeDb(return_values=["cluster_info", "stats"])
        admin = ClusterAdmin(db)
        assert await admin.has_slot_stats() is True
        # Two calls: CLUSTER INFO then CLUSTER SLOT-STATS.
        assert ("CLUSTER", "SLOT-STATS") in db.calls

    @py_test_mark_asyncio
    async def test_has_slot_stats_false_when_command_errors(self):
        db = _FakeDb(
            return_values=["cluster_info"],
            side_effects=[RuntimeError("unknown")],
        )
        admin = ClusterAdmin(db)
        assert await admin.has_slot_stats() is False

    @py_test_mark_asyncio
    async def test_has_migration_false_when_not_cluster(self):
        db = _FakeDb(side_effects=[RuntimeError("cluster support disabled")])
        admin = ClusterAdmin(db)
        assert await admin.has_migration() is False

    @py_test_mark_asyncio
    async def test_has_migration_true_when_command_ok(self):
        db = _FakeDb(return_values=["cluster_info", "migration_status"])
        admin = ClusterAdmin(db)
        assert await admin.has_migration() is True

    @py_test_mark_asyncio
    async def test_has_migration_false_when_command_errors(self):
        db = _FakeDb(
            return_values=["cluster_info"],
            side_effects=[RuntimeError("unknown")],
        )
        admin = ClusterAdmin(db)
        assert await admin.has_migration() is False


# ── slot_stats command-argument construction ──────────────────────────────


class TestSlotStatsArgs:
    @py_test_mark_asyncio
    async def test_no_options(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats()
        assert db.calls == [("CLUSTER", "SLOT-STATS")]

    @py_test_mark_asyncio
    async def test_order_by_ascending(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(order_by="cpu-usec")
        assert db.calls == [("CLUSTER", "SLOT-STATS", "ORDERBY", "cpu-usec")]

    @py_test_mark_asyncio
    async def test_order_by_descending(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(order_by="cpu-usec", desc=True)
        assert db.calls == [("CLUSTER", "SLOT-STATS", "ORDERBY", "cpu-usec", "DESC")]

    @py_test_mark_asyncio
    async def test_order_by_desc_ignored_without_orderby(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(desc=True)
        assert db.calls == [("CLUSTER", "SLOT-STATS")]

    @py_test_mark_asyncio
    async def test_limit(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(limit=5)
        assert db.calls == [("CLUSTER", "SLOT-STATS", "LIMIT", 5)]

    @py_test_mark_asyncio
    async def test_limit_zero_is_included(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(limit=0)
        assert db.calls == [("CLUSTER", "SLOT-STATS", "LIMIT", 0)]

    @py_test_mark_asyncio
    async def test_slot_range(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(slot_range=(100, 200))
        assert db.calls == [("CLUSTER", "SLOT-STATS", "SLOTSRANGE", 100, 200)]

    @py_test_mark_asyncio
    async def test_all_options_combined(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.slot_stats(
            order_by="keys", desc=True, limit=3, slot_range=(0, 1000)
        )
        assert db.calls == [
            (
                "CLUSTER",
                "SLOT-STATS",
                "ORDERBY",
                "keys",
                "DESC",
                "LIMIT",
                3,
                "SLOTSRANGE",
                0,
                1000,
            )
        ]

    @py_test_mark_asyncio
    async def test_returns_parsed_list(self):
        # The raw RESP2 reply is routed through _parse_slot_stats.
        db = _FakeDb(return_values=[{"slot": "0", "cpu-usec": "100"}])
        admin = ClusterAdmin(db)
        out = await admin.slot_stats()
        assert out == [{"slot": "0", "cpu-usec": "100"}]


# ── migration_status shape branches ──────────────────────────────────────


class TestMigrationStatus:
    @py_test_mark_asyncio
    async def test_status_none(self):
        db = _FakeDb(return_values=[None])
        admin = ClusterAdmin(db)
        assert await admin.migration_status() is None

    @py_test_mark_asyncio
    async def test_status_dict(self):
        db = _FakeDb(return_values=[{"state": "migrating", "job-id": "abc"}])
        admin = ClusterAdmin(db)
        out = await admin.migration_status()
        assert out == {"state": "migrating", "job-id": "abc"}

    @py_test_mark_asyncio
    async def test_status_singleton_list_of_dict(self):
        db = _FakeDb(return_values=[[{"state": "none"}]])
        admin = ClusterAdmin(db)
        out = await admin.migration_status()
        assert out == {"state": "none"}

    @py_test_mark_asyncio
    async def test_status_flat_pairs(self):
        # The realistic RESP2 wire format is a flat list of pairs.
        db = _FakeDb(return_values=[[b"state", b"none"]])
        admin = ClusterAdmin(db)
        out = await admin.migration_status()
        assert out == {"state": b"none"}

    @py_test_mark_asyncio
    async def test_status_calls_correct_command(self):
        db = _FakeDb(return_values=[None])
        admin = ClusterAdmin(db)
        await admin.migration_status()
        assert db.calls == [("CLUSTER", "MIGRATION", "STATUS")]


# ── migration_start / stop / abort / log ──────────────────────────────────


class TestMigrationCommands:
    @py_test_mark_asyncio
    async def test_migration_start_no_slots(self):
        db = _FakeDb(return_values=["OK"])
        admin = ClusterAdmin(db)
        assert await admin.migration_start() is True
        assert db.calls == [("CLUSTER", "MIGRATION", "START")]

    @py_test_mark_asyncio
    async def test_migration_start_with_slots(self):
        db = _FakeDb(return_values=["OK"])
        admin = ClusterAdmin(db)
        assert await admin.migration_start(slots=[100, 200, 300]) is True
        assert db.calls == [("CLUSTER", "MIGRATION", "START", "SLOTS", 100, 200, 300)]

    @py_test_mark_asyncio
    async def test_migration_start_coerces_slot_ints(self):
        db = _FakeDb(return_values=["OK"])
        admin = ClusterAdmin(db)
        # Slots might arrive as strings or numpy ints; ensure coercion works.
        assert await admin.migration_start(slots=["42", 43]) is True
        assert db.calls == [("CLUSTER", "MIGRATION", "START", "SLOTS", 42, 43)]

    @py_test_mark_asyncio
    async def test_migration_stop(self):
        db = _FakeDb(return_values=["OK"])
        admin = ClusterAdmin(db)
        assert await admin.migration_stop() is True
        assert db.calls == [("CLUSTER", "MIGRATION", "STOP")]

    @py_test_mark_asyncio
    async def test_migration_abort(self):
        db = _FakeDb(return_values=["OK"])
        admin = ClusterAdmin(db)
        assert await admin.migration_abort() is True
        assert db.calls == [("CLUSTER", "MIGRATION", "ABORT")]

    @py_test_mark_asyncio
    async def test_migration_log_default_count(self):
        db = _FakeDb(return_values=[[b"entry1", b"entry2"]])
        admin = ClusterAdmin(db)
        out = await admin.migration_log()
        assert out == [b"entry1", b"entry2"]
        assert db.calls == [("CLUSTER", "MIGRATION", "LOG", 10)]

    @py_test_mark_asyncio
    async def test_migration_log_custom_count(self):
        db = _FakeDb(return_values=[[]])
        admin = ClusterAdmin(db)
        await admin.migration_log(count=42)
        assert db.calls == [("CLUSTER", "MIGRATION", "LOG", 42)]

    @py_test_mark_asyncio
    async def test_migration_log_none_returns_empty_list(self):
        db = _FakeDb(return_values=[None])
        admin = ClusterAdmin(db)
        out = await admin.migration_log()
        assert out == []
