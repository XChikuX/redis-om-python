# type: ignore
"""Unit tests for RedisArray — pure parsers and command-construction.

Covers ``scan`` (RESP2 vs RESP3 reply normalisation), ``info`` (dict vs
flat-pair reply), and the simple ``set/get/mset`` argument-building paths
through fake ``db`` clients. End-to-end coverage lives in
``tests/test_redis_array.py``.
"""

import pytest

from aredis_om.model.array import RedisArray


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


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


# ── scan parser shapes ───────────────────────────────────────────────────


class TestScan:
    @py_test_mark_asyncio
    async def test_scan_empty_reply(self):
        db = _FakeDb(return_value=[])
        a = RedisArray(db, "k")
        assert await a.scan(0, 10) == []

    @py_test_mark_asyncio
    async def test_scan_none_reply(self):
        db = _FakeDb(return_value=None)
        a = RedisArray(db, "k")
        assert await a.scan(0, 10) == []

    @py_test_mark_asyncio
    async def test_scan_resp3_nested_pairs(self):
        # RESP3 / redis-py 8 returns ``[[idx, val], [idx, val], ...]``.
        db = _FakeDb(return_value=[[0, "a"], [1, "b"], [5, "f"]])
        a = RedisArray(db, "k")
        assert await a.scan(0, 10) == [
            (0, "a"),
            (1, "b"),
            (5, "f"),
        ]

    @py_test_mark_asyncio
    async def test_scan_resp2_flat(self):
        # RESP2 returns ``[idx0, val0, idx1, val1, ...]``.
        db = _FakeDb(return_value=[0, "a", 1, "b"])
        a = RedisArray(db, "k")
        assert await a.scan(0, 10) == [(0, "a"), (1, "b")]

    @py_test_mark_asyncio
    async def test_scan_with_limit(self):
        db = _FakeDb(return_value=[])
        a = RedisArray(db, "k")
        await a.scan(0, 10, limit=20)
        assert db.calls == [("ARSCAN", "k", 0, 10, "LIMIT", 20)]

    @py_test_mark_asyncio
    async def test_scan_without_limit(self):
        db = _FakeDb(return_value=[])
        a = RedisArray(db, "k")
        await a.scan(2, 50)
        # No LIMIT token emitted.
        assert db.calls == [("ARSCAN", "k", 2, 50)]


# ── info parser shapes ───────────────────────────────────────────────────


class TestInfo:
    @py_test_mark_asyncio
    async def test_info_dict_passthrough(self):
        payload = {"length": 100, "encoding": "listpack"}
        db = _FakeDb(return_value=payload)
        a = RedisArray(db, "k")
        out = await a.info()
        assert out == payload

    @py_test_mark_asyncio
    async def test_info_flat_pairs(self):
        # RESP2 returns ``[k0, v0, k1, v1, ...]``.
        db = _FakeDb(return_value=["length", 100, "encoding", "listpack"])
        a = RedisArray(db, "k")
        out = await a.info()
        assert out == {"length": 100, "encoding": "listpack"}

    @py_test_mark_asyncio
    async def test_info_unsupported_type_returns_empty(self):
        # When ARINFO is unavailable the server returns a non-dict/list
        # value; the helper must not crash.
        db = _FakeDb(return_value="not a list")
        a = RedisArray(db, "k")
        assert await a.info() == {}

    @py_test_mark_asyncio
    async def test_info_full_flag(self):
        db = _FakeDb(return_value={"length": 1})
        a = RedisArray(db, "k")
        await a.info(full=True)
        assert db.calls == [("ARINFO", "k", "FULL")]

    @py_test_mark_asyncio
    async def test_info_no_full(self):
        db = _FakeDb(return_value={"length": 1})
        a = RedisArray(db, "k")
        await a.info()
        assert db.calls == [("ARINFO", "k")]


# ── simple wrappers (verify command argument building) ──────────────────


class TestSimpleWrappers:
    @py_test_mark_asyncio
    async def test_set(self):
        db = _FakeDb(return_value=3)
        a = RedisArray(db, "k")
        out = await a.set(5, "v1", "v2", "v3")
        assert out == 3
        assert db.calls == [("ARSET", "k", 5, "v1", "v2", "v3")]

    @py_test_mark_asyncio
    async def test_get(self):
        db = _FakeDb(return_value="v0")
        a = RedisArray(db, "k")
        assert await a.get(0) == "v0"
        assert db.calls == [("ARGET", "k", 0)]

    @py_test_mark_asyncio
    async def test_mset_sorted_pairs(self):
        db = _FakeDb(return_value=2)
        a = RedisArray(db, "k")
        # Insertion order is NOT preserved; the helper sorts by index so
        # the server receives a deterministic layout.
        mapping = {5: "fifth", 1: "first", 3: "third"}
        await a.mset(mapping)
        assert db.calls == [("ARMSET", "k", 1, "first", 3, "third", 5, "fifth")]

    @py_test_mark_asyncio
    async def test_mget(self):
        db = _FakeDb(return_value=["a", None, "c"])
        a = RedisArray(db, "k")
        out = await a.mget(0, 1, 2)
        assert out == ["a", None, "c"]
        assert db.calls == [("ARMGET", "k", 0, 1, 2)]

    @py_test_mark_asyncio
    async def test_get_range(self):
        db = _FakeDb(return_value=[None, "x", "y"])
        a = RedisArray(db, "k")
        await a.get_range(0, 2)
        assert db.calls == [("ARGETRANGE", "k", 0, 2)]

    @py_test_mark_asyncio
    async def test_insert(self):
        db = _FakeDb(return_value=10)
        a = RedisArray(db, "k")
        await a.insert("v1", "v2")
        assert db.calls == [("ARINSERT", "k", "v1", "v2")]

    @py_test_mark_asyncio
    async def test_next_index(self):
        db = _FakeDb(return_value=42)
        a = RedisArray(db, "k")
        assert await a.next_index() == 42
        assert db.calls == [("ARNEXT", "k")]

    @py_test_mark_asyncio
    async def test_seek(self):
        db = _FakeDb(return_value=42)
        a = RedisArray(db, "k")
        await a.seek(42)
        assert db.calls == [("ARSEEK", "k", 42)]

    @py_test_mark_asyncio
    async def test_ring(self):
        db = _FakeDb(return_value=2)
        a = RedisArray(db, "k")
        await a.ring(100, "a", "b")
        assert db.calls == [("ARRING", "k", 100, "a", "b")]

    @py_test_mark_asyncio
    async def test_last_items_default(self):
        db = _FakeDb(return_value=["a", "b"])
        a = RedisArray(db, "k")
        out = await a.last_items(2)
        assert out == ["a", "b"]
        assert db.calls == [("ARLASTITEMS", "k", 2)]

    @py_test_mark_asyncio
    async def test_last_items_reverse(self):
        db = _FakeDb(return_value=["b", "a"])
        a = RedisArray(db, "k")
        await a.last_items(2, rev=True)
        assert db.calls == [("ARLASTITEMS", "k", 2, "REV")]

    @py_test_mark_asyncio
    async def test_last_items_non_list_returns_empty(self):
        # When the server returns a non-list (e.g. error), the helper
        # guards against attribute errors.
        db = _FakeDb(return_value=None)
        a = RedisArray(db, "k")
        assert await a.last_items(5) == []

    @py_test_mark_asyncio
    async def test_delete_at(self):
        db = _FakeDb(return_value=1)
        a = RedisArray(db, "k")
        await a.delete_at(0, 1, 2)
        assert db.calls == [("ARDEL", "k", 0, 1, 2)]

    @py_test_mark_asyncio
    async def test_delete_range(self):
        db = _FakeDb(return_value=5)
        a = RedisArray(db, "k")
        await a.delete_range(0, 10)
        assert db.calls == [("ARDELRANGE", "k", 0, 10)]

    @py_test_mark_asyncio
    async def test_length(self):
        db = _FakeDb(return_value=100)
        a = RedisArray(db, "k")
        assert await a.length() == 100
        assert db.calls == [("ARLEN", "k")]

    @py_test_mark_asyncio
    async def test_count(self):
        db = _FakeDb(return_value=42)
        a = RedisArray(db, "k")
        assert await a.count() == 42
        assert db.calls == [("ARCOUNT", "k")]

    @py_test_mark_asyncio
    async def test_aggregate_no_value(self):
        db = _FakeDb(return_value=42)
        a = RedisArray(db, "k")
        await a.aggregate(0, 10, "SUM")
        assert db.calls == [("AROP", "k", 0, 10, "SUM")]

    @py_test_mark_asyncio
    async def test_aggregate_with_value(self):
        db = _FakeDb(return_value=2)
        a = RedisArray(db, "k")
        await a.aggregate(0, 10, "MATCH", "abc*")
        assert db.calls == [("AROP", "k", 0, 10, "MATCH", "abc*")]

    @py_test_mark_asyncio
    async def test_grep_predicates_only(self):
        db = _FakeDb(return_value=[0, 1])
        a = RedisArray(db, "k")
        await a.grep(0, 100, [("EXACT", "hello"), ("MATCH", "world*")])
        assert db.calls == [
            (
                "ARGREP",
                "k",
                0,
                100,
                "EXACT",
                "hello",
                "MATCH",
                "world*",
            )
        ]

    @py_test_mark_asyncio
    async def test_grep_nocase_with_values(self):
        db = _FakeDb(return_value=[[0, "foo"]])
        a = RedisArray(db, "k")
        await a.grep(0, 100, [("EXACT", "x")], nocase=True, with_values=True)
        assert db.calls == [
            (
                "ARGREP",
                "k",
                0,
                100,
                "EXACT",
                "x",
                "NOCASE",
                "WITHVALUES",
            )
        ]

    @py_test_mark_asyncio
    async def test_grep_with_limit(self):
        db = _FakeDb(return_value=[])
        a = RedisArray(db, "k")
        await a.grep(0, 100, [("EXACT", "x")], limit=50)
        assert db.calls[-1][-2:] == ("LIMIT", 50)

    def test_key_property(self):
        db = _FakeDb()
        a = RedisArray(db, "my-key")
        assert a.key == "my-key"
