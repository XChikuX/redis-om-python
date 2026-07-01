# type: ignore
"""Unit tests for SortedSetOps — parsers, validation, and command construction.

Covers the pure-Python ``_parse_zresult`` parser across RESP2 / RESP3 reply
shapes, the ``_aggregate_count`` input-validation guard, and the public
``SortedSetOps`` methods through fake ``db`` clients.

End-to-end coverage lives in ``tests/test_sorted_set.py``.
"""

import pytest

from aredis_om.model.sorted_set import (
    SortedSetOps,
    _num,
    _parse_zresult,
    _str,
    has_aggregate_count,
)


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


class TestStrHelper:
    def test_bytes(self):
        assert _str(b"x") == "x"

    def test_bytearray(self):
        assert _str(bytearray(b"y")) == "y"

    def test_passthrough(self):
        assert _str("z") == "z"


class TestNumHelper:
    def test_bytes(self):
        assert _num(b"42") == 42

    def test_bytearray(self):
        assert _num(bytearray(b"7")) == 7

    def test_int_passthrough(self):
        assert _num(5) == 5

    def test_string_int(self):
        assert _num("9") == 9


# ── _parse_zresult ────────────────────────────────────────────────────────


class TestParseZresult:
    def test_none(self):
        assert _parse_zresult(None, False) == []
        assert _parse_zresult(None, True) == []

    def test_empty_list(self):
        assert _parse_zresult([], False) == []
        assert _parse_zresult([], True) == []

    def test_dict_no_scores(self):
        raw = {"a": 1, "b": 2}
        assert _parse_zresult(raw, False) == ["a", "b"]

    def test_dict_with_scores(self):
        raw = {"a": 3, "b": 5}
        out = _parse_zresult(raw, True)
        # Dict ordering is insertion order in Python 3.7+, so we can compare.
        assert out == [("a", 3), ("b", 5)]

    def test_resp3_pair_list_no_scores(self):
        raw = [["a", 1], ["b", 2]]
        assert _parse_zresult(raw, False) == ["a", "b"]

    def test_resp3_pair_list_with_scores(self):
        raw = [["a", 1], ["b", 2]]
        assert _parse_zresult(raw, True) == [("a", 1), ("b", 2)]

    def test_resp3_pair_list_bytes(self):
        raw = [[b"a", b"1"], [b"b", b"2"]]
        assert _parse_zresult(raw, True) == [("a", 1), ("b", 2)]

    def test_resp2_flat_no_scores(self):
        raw = ["a", "b", "c"]
        assert _parse_zresult(raw, False) == ["a", "b", "c"]

    def test_resp2_flat_with_scores(self):
        raw = ["a", 1, "b", 2]
        assert _parse_zresult(raw, True) == [("a", 1), ("b", 2)]

    def test_resp2_flat_odd_length_drops_tail(self):
        # A trailing unpaired member is silently dropped.
        raw = ["a", 1, "b"]
        assert _parse_zresult(raw, True) == [("a", 1)]

    def test_resp2_flat_bytes(self):
        raw = [b"a", b"1", b"b", b"2"]
        assert _parse_zresult(raw, True) == [("a", 1), ("b", 2)]


# ── SortedSetOps via fake db ──────────────────────────────────────────────


class _FakeDb:
    def __init__(self, *, return_value=None, side_effect=None):
        self.calls = []
        self._return_value = return_value
        self._side_effect = side_effect

    async def execute_command(self, *args):
        self.calls.append(args)
        if self._side_effect is not None:
            raise self._side_effect
        return self._return_value

    async def delete(self, key):
        self.calls.append(("delete", key))
        return 1


class TestSortedSetOpsCommands:
    @py_test_mark_asyncio
    async def test_zunionstore_count(self):
        db = _FakeDb(return_value=42)
        ops = SortedSetOps(db)
        out = await ops.zunionstore_count("dest", "a", "b", "c")
        assert out == 42
        assert db.calls == [
            ("ZUNIONSTORE", "dest", 3, "a", "b", "c", "AGGREGATE", "COUNT")
        ]

    @py_test_mark_asyncio
    async def test_zunionstore_count_single_source(self):
        db = _FakeDb(return_value=5)
        ops = SortedSetOps(db)
        out = await ops.zunionstore_count("dest", "only")
        assert out == 5
        # numkeys=1, only one source.
        assert db.calls[-1] == (
            "ZUNIONSTORE",
            "dest",
            1,
            "only",
            "AGGREGATE",
            "COUNT",
        )

    @py_test_mark_asyncio
    async def test_zinterstore_count(self):
        db = _FakeDb(return_value=10)
        ops = SortedSetOps(db)
        out = await ops.zinterstore_count("dest", "a", "b")
        assert out == 10
        assert db.calls == [("ZINTERSTORE", "dest", 2, "a", "b", "AGGREGATE", "COUNT")]


class TestAggregateCountReadVariants:
    @py_test_mark_asyncio
    async def test_zunion_count_no_scores(self):
        db = _FakeDb(return_value=["m1", "m2", "m3"])
        ops = SortedSetOps(db)
        out = await ops.zunion_count("a", "b", "c")
        assert out == ["m1", "m2", "m3"]
        # Default zunion_count does NOT pass WITHSCORES.
        assert db.calls[-1] == (
            "ZUNION",
            3,
            "a",
            "b",
            "c",
            "AGGREGATE",
            "COUNT",
        )

    @py_test_mark_asyncio
    async def test_zunion_count_with_scores(self):
        db = _FakeDb(return_value=[["m1", 3], ["m2", 2]])
        ops = SortedSetOps(db)
        out = await ops.zunion_count_with_scores("a", "b", "c")
        assert out == [("m1", 3), ("m2", 2)]
        assert db.calls[-1] == (
            "ZUNION",
            3,
            "a",
            "b",
            "c",
            "AGGREGATE",
            "COUNT",
            "WITHSCORES",
        )

    @py_test_mark_asyncio
    async def test_zinter_count_no_scores(self):
        db = _FakeDb(return_value=["only-common"])
        ops = SortedSetOps(db)
        out = await ops.zinter_count("a", "b")
        assert out == ["only-common"]
        assert db.calls[-1] == (
            "ZINTER",
            2,
            "a",
            "b",
            "AGGREGATE",
            "COUNT",
        )

    @py_test_mark_asyncio
    async def test_zinter_count_with_scores(self):
        db = _FakeDb(return_value=[["only-common", 2]])
        ops = SortedSetOps(db)
        out = await ops.zinter_count_with_scores("a", "b")
        assert out == [("only-common", 2)]
        assert db.calls[-1] == (
            "ZINTER",
            2,
            "a",
            "b",
            "AGGREGATE",
            "COUNT",
            "WITHSCORES",
        )


# ── input validation ─────────────────────────────────────────────────────


class TestAggregateCountValidation:
    @py_test_mark_asyncio
    async def test_no_sources_raises_zunion(self):
        ops = SortedSetOps(_FakeDb())
        with pytest.raises(ValueError, match="at least one source key"):
            await ops.zunion_count()

    @py_test_mark_asyncio
    async def test_no_sources_raises_zinter(self):
        ops = SortedSetOps(_FakeDb())
        with pytest.raises(ValueError, match="at least one source key"):
            await ops.zinter_count()

    @py_test_mark_asyncio
    async def test_error_message_includes_command_name(self):
        ops = SortedSetOps(_FakeDb())
        with pytest.raises(ValueError, match="ZUNION"):
            await ops.zunion_count()
        with pytest.raises(ValueError, match="ZINTER"):
            await ops.zinter_count()


# ── has_aggregate_count probe ─────────────────────────────────────────────


class TestHasAggregateCount:
    @py_test_mark_asyncio
    async def test_true_on_success(self):
        # The probe runs ZUNION 1 key AGGREGATE COUNT and then deletes.
        db = _FakeDb(return_value=0)
        assert await has_aggregate_count(db) is True
        # Two calls: probe + delete.
        assert db.calls[0] == (
            "ZUNION",
            1,
            "__agg_count_probe__",
            "AGGREGATE",
            "COUNT",
        )
        assert db.calls[1] == ("delete", "__agg_count_probe__")

    @py_test_mark_asyncio
    async def test_false_on_exception(self):
        # If the server doesn't know about COUNT, this raises ResponseError.
        db = _FakeDb(side_effect=RuntimeError("ERR syntax error"))
        assert await has_aggregate_count(db) is False
