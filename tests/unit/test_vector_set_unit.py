# type: ignore
"""Unit tests for VectorSet — parsers, helpers, and command construction.

These tests exercise pure-Python helpers (``_parse_vsim``, ``_coerce_attrs``,
``_fmt_vector``, ``_command_info_present``) and the ``add`` validation/
argument-building paths via fake ``db`` objects. They do **not** require a
live Redis.

For end-to-end coverage of the actual VADD / VSIM / etc. commands see
``test_vector_set.py``.
"""

import pytest

from aredis_om.model.vector_set import (
    VectorSet,
    _capability_cache,
    _coerce_attrs,
    _command_info_present,
    _fmt_vector,
    _num,
    _pairs_to_dict,
    _parse_vsim,
    _probe,
    _str,
    clear_vector_set_cache,
    has_vector_sets,
)


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


# ── low-level helpers ─────────────────────────────────────────────────────


class TestStrHelper:
    def test_from_bytes(self):
        assert _str(b"hello") == "hello"

    def test_from_bytearray(self):
        assert _str(bytearray(b"abc")) == "abc"

    def test_passthrough_str(self):
        assert _str("already-str") == "already-str"

    def test_passthrough_other(self):
        assert _str(42) == "42"


class TestNumHelper:
    def test_from_bytes(self):
        assert _num(b"3.14") == 3.14

    def test_from_bytearray(self):
        assert _num(bytearray(b"2.5")) == 2.5

    def test_passthrough_int(self):
        assert _num(7) == 7

    def test_passthrough_float(self):
        assert _num(0.5) == 0.5

    def test_passthrough_string_int(self):
        assert _num("9") == 9.0


class TestFmtVector:
    def test_basic(self):
        out = _fmt_vector([1.0, 2.0, 3.0])
        assert out == ["VALUES", 3, 1.0, 2.0, 3.0]

    def test_coerces_ints_to_float(self):
        out = _fmt_vector([1, 2, 3])
        # Ensure ints are rendered as floats via float(v).
        assert all(isinstance(v, float) for v in out[2:])
        assert out == ["VALUES", 3, 1.0, 2.0, 3.0]

    def test_empty_vector(self):
        out = _fmt_vector([])
        assert out == ["VALUES", 0]


# ── _pairs_to_dict ────────────────────────────────────────────────────────


class TestPairsToDict:
    def test_pair_list(self):
        assert _pairs_to_dict([b"a", 1, b"b", 2]) == {"a": 1, "b": 2}

    def test_none_returns_empty(self):
        assert _pairs_to_dict(None) == {}

    def test_single_pair(self):
        assert _pairs_to_dict(["k", b"v"]) == {"k": b"v"}

    def test_empty_iterable(self):
        assert _pairs_to_dict([]) == {}


# ── _command_info_present ─────────────────────────────────────────────────


class TestCommandInfoPresent:
    def test_resp3_dict_match(self):
        info = {"vadd": {"arity": -4}, "get": {"arity": 1}}
        assert _command_info_present(info, "vadd") is True

    def test_resp3_dict_no_match(self):
        info = {"get": {"arity": 1}}
        assert _command_info_present(info, "vadd") is False

    def test_resp2_list_match(self):
        info = [["vadd", -4, 1, 1, 1], ["get", 1, 1, 1, 1]]
        assert _command_info_present(info, "vadd") is True

    def test_resp2_list_no_match(self):
        info = [["get", 1, 1, 1, 1]]
        assert _command_info_present(info, "vadd") is False

    def test_resp2_list_with_none_entries(self):
        info = [None, ["vadd", -4, 1, 1, 1]]
        assert _command_info_present(info, "vadd") is True

    def test_empty_info(self):
        assert _command_info_present(None, "vadd") is False
        assert _command_info_present([], "vadd") is False
        assert _command_info_present({}, "vadd") is False

    def test_case_insensitive(self):
        info = {"VADD": {}}
        assert _command_info_present(info, "vadd") is True
        assert _command_info_present(info, "VADD") is True


# ── _coerce_attrs ─────────────────────────────────────────────────────────


class TestCoerceAttrs:
    def test_none_returns_none(self):
        assert _coerce_attrs(None) is None

    def test_bytes_decoded(self):
        raw = b'{"color": "red"}'
        out = _coerce_attrs(raw)
        assert out == {"color": "red"}

    def test_json_string(self):
        out = _coerce_attrs('{"foo": 1}')
        assert out == {"foo": 1}

    def test_invalid_json_returns_string(self):
        # Plain text falls back to the raw string.
        out = _coerce_attrs("not json")
        assert out == "not json"

    def test_dict_passthrough(self):
        d = {"k": 1}
        assert _coerce_attrs(d) == d

    def test_other_type_passthrough(self):
        # An int or other JSON-decodable value should be returned as-is.
        assert _coerce_attrs(42) == 42


# ── _parse_vsim — all combinations ────────────────────────────────────────


class TestParseVsim:
    """VSIM returns different shapes based on (RESP2/RESP3) × (flags).

    ``_parse_vsim`` normalises them all into Python lists. We exercise every
    combination so the parser is fully covered.
    """

    # ── RESP3 / dict shape ────────────────────────────────────────────────

    def test_dict_no_flags(self):
        raw = {"a": 1, "b": 2}
        assert _parse_vsim(raw, False, False) == ["a", "b"]

    def test_dict_with_scores(self):
        raw = {"a": 0.9, "b": 0.1}
        assert _parse_vsim(raw, True, False) == [("a", 0.9), ("b", 0.1)]

    def test_dict_with_attributes(self):
        raw = {"a": {"color": "red"}, "b": {"color": "blue"}}
        out = _parse_vsim(raw, False, True)
        assert out == [("a", {"color": "red"}), ("b", {"color": "blue"})]

    def test_dict_with_scores_and_attributes(self):
        # Server packs [score, attrs] under each key when both flags are set.
        raw = {"a": [0.9, {"c": 1}], "b": [0.1, {"c": 2}]}
        out = _parse_vsim(raw, True, True)
        assert out == [
            ("a", 0.9, {"c": 1}),
            ("b", 0.1, {"c": 2}),
        ]

    def test_dict_scores_only_value_present(self):
        # Edge case: a dict with no attrs entry under a key.
        raw = {"a": [0.9]}
        out = _parse_vsim(raw, True, True)
        assert out == [("a", 0.9, None)]

    # ── RESP2 / list shape ────────────────────────────────────────────────

    def test_list_no_flags(self):
        raw = ["a", "b", "c"]
        assert _parse_vsim(raw, False, False) == ["a", "b", "c"]

    def test_list_with_scores(self):
        raw = ["a", 0.9, "b", 0.1]
        assert _parse_vsim(raw, True, False) == [("a", 0.9), ("b", 0.1)]

    def test_list_with_attributes(self):
        raw = ["a", {"c": 1}, "b", {"c": 2}]
        out = _parse_vsim(raw, False, True)
        assert out == [("a", {"c": 1}), ("b", {"c": 2})]

    def test_list_with_scores_and_attributes(self):
        raw = ["a", 0.9, {"c": 1}, "b", 0.1, {"c": 2}]
        out = _parse_vsim(raw, True, True)
        assert out == [
            ("a", 0.9, {"c": 1}),
            ("b", 0.1, {"c": 2}),
        ]

    def test_bytes_keys_and_scores(self):
        raw = [b"a", b"0.9", b"b", b"0.1"]
        out = _parse_vsim(raw, True, False)
        assert out == [("a", 0.9), ("b", 0.1)]

    # ── None reply ────────────────────────────────────────────────────────

    def test_none(self):
        assert _parse_vsim(None, False, False) == []
        assert _parse_vsim(None, True, True) == []


# ── _probe + has_vector_sets — fake db ────────────────────────────────────


class _FakeDb:
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


class TestHasVectorSets:
    @py_test_mark_asyncio
    async def test_true_when_command_present(self):
        info = {"vadd": {"arity": -4}}
        db = _FakeDb(return_values=[info])
        assert await has_vector_sets(db) is True
        assert db.calls == [("COMMAND", "INFO", "VADD")]

    @py_test_mark_asyncio
    async def test_false_when_command_missing(self):
        info = {"get": {"arity": 1}}
        db = _FakeDb(return_values=[info])
        assert await has_vector_sets(db) is False

    @py_test_mark_asyncio
    async def test_false_when_raises(self):
        db = _FakeDb(side_effects=[RuntimeError("connection refused")])
        assert await has_vector_sets(db) is False


class TestProbe:
    @py_test_mark_asyncio
    async def test_first_call_probes_command(self):
        clear_vector_set_cache()
        db = _FakeDb(return_values=[["VADD", -4]])
        assert await _probe(db, "VADD") is True
        # The probe only caches *unsupported* commands, so a subsequent call
        # for a supported command re-probes (the helper is intended only
        # for one-shot detection).
        assert await _probe(db, "VADD") is True
        assert db.calls == [
            ("COMMAND", "INFO", "VADD"),
            ("COMMAND", "INFO", "VADD"),
        ]

    @py_test_mark_asyncio
    async def test_command_missing_caches_and_returns_false(self):
        clear_vector_set_cache()
        db = _FakeDb(side_effects=[RuntimeError("unknown command")])
        assert await _probe(db, "VCARD") is False
        # Subsequent call for an unsupported command hits the cache.
        assert await _probe(db, "VCARD") is False
        assert db.calls == [("COMMAND", "INFO", "VCARD")]


class TestClearCache:
    def test_clears_cache(self):
        _capability_cache[id({})] = {"VADD"}
        assert _capability_cache
        clear_vector_set_cache()
        assert _capability_cache == {}


# ── VectorSet instance: input validation + arg construction (fake db) ────


class TestAddArgumentBuilding:
    @py_test_mark_asyncio
    async def test_invalid_quant_raises(self):
        db = _FakeDb()
        vs = VectorSet(db, "k")
        with pytest.raises(ValueError, match="quant must be one of"):
            await vs.add([1.0, 2.0], "elem", quant="WRONG")

    @py_test_mark_asyncio
    async def test_minimal_add(self):
        db = _FakeDb(return_values=[True])
        vs = VectorSet(db, "k")
        got = await vs.add([1.0, 2.0], "e1")
        assert got is True
        assert db.calls == [("VADD", "k", "VALUES", 2, 1.0, 2.0, "e1")]

    @py_test_mark_asyncio
    async def test_add_with_reduce(self):
        db = _FakeDb(return_values=[True])
        vs = VectorSet(db, "k")
        await vs.add([1.0, 2.0, 3.0], "e1", reduce_to_dim=2)
        assert db.calls == [
            ("VADD", "k", "REDUCE", 2, "VALUES", 3, 1.0, 2.0, 3.0, "e1")
        ]

    @py_test_mark_asyncio
    async def test_add_with_cas(self):
        db = _FakeDb(return_values=[False])
        vs = VectorSet(db, "k")
        await vs.add([1.0], "e1", cas=True)
        assert db.calls == [("VADD", "k", "VALUES", 1, 1.0, "e1", "CAS")]

    @py_test_mark_asyncio
    async def test_add_with_quant_noquant(self):
        db = _FakeDb(return_values=[True])
        vs = VectorSet(db, "k")
        await vs.add([1.0, 2.0], "e1", quant="NOQUANT")
        assert db.calls == [("VADD", "k", "VALUES", 2, 1.0, 2.0, "e1", "NOQUANT")]

    @py_test_mark_asyncio
    async def test_add_with_quant_bin(self):
        db = _FakeDb(return_values=[True])
        vs = VectorSet(db, "k")
        await vs.add([1.0], "e1", quant="BIN")
        assert db.calls == [("VADD", "k", "VALUES", 1, 1.0, "e1", "BIN")]

    @py_test_mark_asyncio
    async def test_add_with_ef(self):
        db = _FakeDb(return_values=[True])
        vs = VectorSet(db, "k")
        await vs.add([1.0, 2.0], "e1", ef=100)
        assert db.calls == [("VADD", "k", "VALUES", 2, 1.0, 2.0, "e1", "EF", 100)]

    @py_test_mark_asyncio
    async def test_add_with_all_options(self):
        db = _FakeDb(return_values=[True])
        vs = VectorSet(db, "k")
        await vs.add(
            [1.0, 2.0, 3.0],
            "e1",
            reduce_to_dim=2,
            cas=True,
            quant="Q8",
            ef=200,
        )
        assert db.calls == [
            (
                "VADD",
                "k",
                "REDUCE",
                2,
                "VALUES",
                3,
                1.0,
                2.0,
                3.0,
                "e1",
                "CAS",
                "Q8",
                "EF",
                200,
            )
        ]

    @py_test_mark_asyncio
    async def test_add_returns_truthy_coerced_to_bool(self):
        # The wrapper always coerces the raw reply via bool(), so any truthy
        # reply becomes True and any falsy reply becomes False.
        db = _FakeDb(return_values=[1])
        vs = VectorSet(db, "k")
        assert (await vs.add([1.0], "e1")) is True

        db = _FakeDb(return_values=[0])
        vs = VectorSet(db, "k")
        assert (await vs.add([1.0], "e2")) is False


# ── VectorSet command-construction for query/mutation methods ─────────────


class TestOtherCommands:
    @py_test_mark_asyncio
    async def test_remove(self):
        db = _FakeDb(return_values=[1])
        vs = VectorSet(db, "k")
        assert (await vs.remove("e1")) is True
        assert db.calls == [("VREM", "k", "e1")]

    @py_test_mark_asyncio
    async def test_set_attribute(self):
        db = _FakeDb(return_values=[1])
        vs = VectorSet(db, "k")
        attrs = {"color": "red", "tags": ["a", "b"]}
        got = await vs.set_attribute("e1", attrs)
        assert got is True
        call = db.calls[0]
        assert call[0] == "VSETATTR"
        assert call[1] == "k"
        assert call[2] == "e1"
        assert call[3] == '{"color": "red", "tags": ["a", "b"]}'

    @py_test_mark_asyncio
    async def test_set_attribute_empty_dict(self):
        db = _FakeDb(return_values=[1])
        vs = VectorSet(db, "k")
        await vs.set_attribute("e1", {})
        call = db.calls[0]
        assert call[3] == "{}"

    @py_test_mark_asyncio
    async def test_get_attribute_returns_parsed(self):
        db = _FakeDb(return_values=[b'{"color": "red"}'])
        vs = VectorSet(db, "k")
        out = await vs.get_attribute("e1")
        assert out == {"color": "red"}
        assert db.calls == [("VGETATTR", "k", "e1")]

    @py_test_mark_asyncio
    async def test_get_attribute_missing(self):
        db = _FakeDb(return_values=[None])
        vs = VectorSet(db, "k")
        assert await vs.get_attribute("missing") is None

    @py_test_mark_asyncio
    async def test_similar_no_flags(self):
        db = _FakeDb(return_values=[["a", "b"]])
        vs = VectorSet(db, "k")
        out = await vs.similar([1.0, 2.0])
        assert out == ["a", "b"]
        assert db.calls == [("VSIM", "k", "VALUES", 2, 1.0, 2.0)]

    @py_test_mark_asyncio
    async def test_similar_all_flags(self):
        # RESP2 flat shape with name, score, attrs triplets.
        db = _FakeDb(
            return_values=[
                [
                    "a",
                    0.9,
                    {"c": 1},
                    "b",
                    0.1,
                    {"c": 2},
                ]
            ]
        )
        vs = VectorSet(db, "k")
        out = await vs.similar(
            [1.0, 2.0],
            count=10,
            ef=100,
            filter_expr='.color == "red"',
            epsilon=0.01,
            with_scores=True,
            with_attributes=True,
        )
        assert out == [
            ("a", 0.9, {"c": 1}),
            ("b", 0.1, {"c": 2}),
        ]
        assert db.calls == [
            (
                "VSIM",
                "k",
                "VALUES",
                2,
                1.0,
                2.0,
                "COUNT",
                10,
                "EF",
                100,
                "FILTER",
                '.color == "red"',
                "EPSILON",
                0.01,
                "WITHSCORES",
                "WITHATTRIBS",
            )
        ]

    @py_test_mark_asyncio
    async def test_card(self):
        db = _FakeDb(return_values=[42])
        vs = VectorSet(db, "k")
        assert await vs.card() == 42
        assert db.calls == [("VCARD", "k")]

    @py_test_mark_asyncio
    async def test_dim(self):
        db = _FakeDb(return_values=[128])
        vs = VectorSet(db, "k")
        assert await vs.dim() == 128
        assert db.calls == [("VDIM", "k")]

    @py_test_mark_asyncio
    async def test_info_dict(self):
        db = _FakeDb(return_values=[{"quant-type": "int8", "hnsw-m": 16}])
        vs = VectorSet(db, "k")
        info = await vs.info()
        assert info == {"quant-type": "int8", "hnsw-m": 16}
        assert db.calls == [("VINFO", "k")]

    @py_test_mark_asyncio
    async def test_info_pair_list(self):
        # When the server replies with flat RESP2 pairs, _pairs_to_dict
        # decodes bytes keys to str.
        db = _FakeDb(return_values=[[b"quant-type", b"int8", b"hnsw-m", b"16"]])
        vs = VectorSet(db, "k")
        info = await vs.info()
        assert info == {"quant-type": b"int8", "hnsw-m": b"16"}

    @py_test_mark_asyncio
    async def test_embedding(self):
        db = _FakeDb(return_values=[[1.0, 2.0, 3.0]])
        vs = VectorSet(db, "k")
        emb = await vs.embedding("e1")
        assert emb == [1.0, 2.0, 3.0]
        assert db.calls == [("VEMB", "k", "e1")]

    @py_test_mark_asyncio
    async def test_links_layers(self):
        db = _FakeDb(return_values=[[["a", "b"], ["c"], None, "x"]])
        vs = VectorSet(db, "k")
        links = await vs.links("e1")
        # Each non-None layer is stringified; None layers become empty lists.
        assert links == [["a", "b"], ["c"], [], ["x"]]
        assert db.calls == [("VLINKS", "k", "e1")]

    @py_test_mark_asyncio
    async def test_links_empty(self):
        db = _FakeDb(return_values=[[]])
        vs = VectorSet(db, "k")
        assert await vs.links("e1") == []

    @py_test_mark_asyncio
    async def test_random_member_single(self):
        # With decode_responses=True the reply is already a str, which
        # passes through ``str(raw)`` unchanged.
        db = _FakeDb(return_values=["only-element"])
        vs = VectorSet(db, "k")
        got = await vs.random_member()
        assert got == "only-element"
        assert db.calls == [("VRANDMEMBER", "k")]

    @py_test_mark_asyncio
    async def test_random_member_empty_set(self):
        db = _FakeDb(return_values=[None])
        vs = VectorSet(db, "k")
        assert await vs.random_member() is None

    @py_test_mark_asyncio
    async def test_random_member_with_count(self):
        db = _FakeDb(return_values=[["a", "b", "c"]])
        vs = VectorSet(db, "k")
        got = await vs.random_member(count=3)
        assert got == ["a", "b", "c"]
        assert db.calls == [("VRANDMEMBER", "k", 3)]

    @py_test_mark_asyncio
    async def test_random_member_with_count_empty(self):
        db = _FakeDb(return_values=[[]])
        vs = VectorSet(db, "k")
        assert await vs.random_member(count=5) == []

    @py_test_mark_asyncio
    async def test_key_property(self):
        db = _FakeDb()
        vs = VectorSet(db, "my-key")
        assert vs.key == "my-key"
