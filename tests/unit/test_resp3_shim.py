# type: ignore
"""Unit tests for the protocol-aware RediSearch response parsers.

These tests cover the ``resp3_shim`` module which normalises both RESP2 and
RESP3 wire shapes produced by ``FT.SEARCH`` / ``FT.AGGREGATE`` /
``FT.AGGREGATE WITHCURSOR`` into a single legacy flat-pair representation
that the rest of pyredis-om can iterate uniformly.

The tests do not require a running Redis instance; they feed synthetic raw
responses to the parser functions and assert the normalised output.
"""

import pytest

from aredis_om.model.resp3_shim import (
    _decode_dict_keys,
    extract_key_from_row,
    is_resp3_search_response,
    split_cursor_response,
    split_search_response,
)

# ── is_resp3_search_response ────────────────────────────────────────────


class TestIsResp3SearchResponse:
    def test_returns_true_for_dict_with_results(self):
        raw = {"total_results": 1, "results": []}
        assert is_resp3_search_response(raw) is True

    def test_returns_false_for_flat_list(self):
        # RESP2 layout: [count, key, fields, ...]
        assert is_resp3_search_response([1, "k1", ["name", "Alice"]]) is False

    def test_returns_false_for_empty_dict(self):
        assert is_resp3_search_response({}) is False

    def test_returns_false_for_none(self):
        assert is_resp3_search_response(None) is False

    def test_returns_true_for_bytes_keys(self):
        # Regression: redis-py surfaces RESP3 map keys as bytes for raw
        # ``execute_command`` callers even when ``decode_responses=True``.
        raw = {
            b"attributes": [],
            b"format": b"STRING",
            b"results": [],
            b"total_results": 0,
            b"warning": [],
        }
        assert is_resp3_search_response(raw) is True

    def test_returns_true_for_bytes_results_key_only(self):
        raw = {b"results": []}
        assert is_resp3_search_response(raw) is True

    def test_returns_true_for_bytes_total_results_key_only(self):
        raw = {b"total_results": 0}
        assert is_resp3_search_response(raw) is True


# ── _decode_dict_keys ───────────────────────────────────────────────────


class TestDecodeDictKeys:
    def test_decodes_bytes_keys_to_str(self):
        raw = {b"results": [], b"total_results": 1}
        out = _decode_dict_keys(raw)
        assert set(out.keys()) == {"results", "total_results"}
        assert out["results"] == []
        assert out["total_results"] == 1

    def test_preserves_str_keys(self):
        raw = {"results": [], "total_results": 1}
        out = _decode_dict_keys(raw)
        assert out == raw

    def test_mixed_keys(self):
        raw = {"results": [], b"total_results": 1, b"extra": b"value"}
        out = _decode_dict_keys(raw)
        assert set(out.keys()) == {"results", "total_results", "extra"}
        # Values are left untouched.
        assert out["extra"] == b"value"

    def test_passes_through_non_dict(self):
        assert _decode_dict_keys([1, 2, 3]) == [1, 2, 3]
        assert _decode_dict_keys(None) is None
        assert _decode_dict_keys("abc") == "abc"

    def test_invalid_utf8_keys_do_not_raise(self):
        # ``errors="ignore"`` means non-UTF-8 bytes don't crash the decoder.
        raw = {b"\xff\xfe": "value"}
        out = _decode_dict_keys(raw)
        assert "value" in out.values()


# ── split_search_response — RESP2 ───────────────────────────────────────


class TestSplitSearchResponseResp2:
    def test_basic_three_rows(self):
        raw = [
            3,
            "doc:1",
            ["name", "Alice", "age", "30"],
            "doc:2",
            ["name", "Bob", "age", "25"],
            "doc:3",
            ["name", "Carol", "age", "40"],
        ]
        total, rows = split_search_response(raw, protocol=2, command="search")
        assert total == 3
        assert len(rows) == 3
        # Each row is a flat pair list with __key prepended.
        assert rows[0] == ["__key", "doc:1", "name", "Alice", "age", "30"]
        assert rows[1] == ["__key", "doc:2", "name", "Bob", "age", "25"]
        assert rows[2] == ["__key", "doc:3", "name", "Carol", "age", "40"]

    def test_empty_results(self):
        total, rows = split_search_response([0], protocol=2, command="search")
        assert total == 0
        assert rows == []

    def test_none_fields_skipped(self):
        # NOCONTENT rows have fields=None.
        raw = [2, "doc:1", None, "doc:2", ["name", "Alice"]]
        total, rows = split_search_response(raw, protocol=2, command="search")
        assert total == 2
        assert len(rows) == 1
        assert rows[0] == ["__key", "doc:2", "name", "Alice"]

    def test_aggregate_layout_uses_flat_pair_rows(self):
        # RESP2 FT.AGGREGATE returns ``[count, row1_pairs, row2_pairs, ...]``
        # (each row is already a flat-pair list, not a key + fields pair).
        raw = [
            2,
            ["__key", "doc:1", "count", "5"],
            ["__key", "doc:2", "count", "10"],
        ]
        total, rows = split_search_response(raw, protocol=2, command="aggregate")
        assert total == 2
        assert rows == [
            ["__key", "doc:1", "count", "5"],
            ["__key", "doc:2", "count", "10"],
        ]

    def test_protocol_sniffing_picks_resp2_for_list(self):
        raw = [1, "doc:1", ["name", "Alice"]]
        total, rows = split_search_response(raw, command="search")
        assert total == 1
        assert rows == [["__key", "doc:1", "name", "Alice"]]


# ── split_search_response — RESP3 ───────────────────────────────────────


class TestSplitSearchResponseResp3:
    def test_basic_three_rows(self):
        raw = {
            "total_results": 3,
            "format": "STRING",
            "attributes": [],
            "warning": [],
            "results": [
                {
                    "id": "doc:1",
                    "extra_attributes": {"name": "Alice", "age": "30"},
                    "values": [],
                },
                {
                    "id": "doc:2",
                    "extra_attributes": {"name": "Bob", "age": "25"},
                    "values": [],
                },
                {
                    "id": "doc:3",
                    "extra_attributes": {"name": "Carol", "age": "40"},
                    "values": [],
                },
            ],
        }
        total, rows = split_search_response(raw, protocol=3, command="search")
        assert total == 3
        assert len(rows) == 3
        # First row has id + extra_attributes flattened.
        assert rows[0] == ["id", "doc:1", "name", "Alice", "age", "30"]
        assert rows[1] == ["id", "doc:2", "name", "Bob", "age", "25"]
        assert rows[2] == ["id", "doc:3", "name", "Carol", "age", "40"]

    def test_score_values_appear_after_extra_attributes(self):
        raw = {
            "total_results": 1,
            "results": [
                {
                    "id": "doc:1",
                    "extra_attributes": {"name": "Alice"},
                    "values": [["__score", "0.95"]],
                }
            ],
        }
        total, rows = split_search_response(raw, protocol=3, command="search")
        assert total == 1
        assert rows[0] == ["id", "doc:1", "name", "Alice", "__score", "0.95"]

    def test_empty_results(self):
        total, rows = split_search_response(
            {"total_results": 0, "results": []}, protocol=3
        )
        assert total == 0
        assert rows == []

    def test_missing_total_results_defaults_to_zero(self):
        total, rows = split_search_response({"results": []}, protocol=3)
        assert total == 0
        assert rows == []

    def test_bytes_keys_are_decoded_values_preserved(self):
        # Field names are always decoded to ``str`` for use as dict keys;
        # values are left in their native form so the caller can decide
        # whether to decode them (this matches the historical pyredis-om
        # behaviour where ``convert_timestamp_to_datetime`` and friends
        # operate on the raw value).
        raw = {
            "total_results": 1,
            "results": [
                {
                    "id": b"doc:1",
                    "extra_attributes": {b"name": b"Alice"},
                    "values": [],
                }
            ],
        }
        total, rows = split_search_response(raw, protocol=3, command="search")
        assert total == 1
        assert rows[0] == ["id", b"doc:1", "name", b"Alice"]

    def test_top_level_bytes_keys_with_empty_results(self):
        # Regression for issue #N: redis-py can surface the entire RESP3
        # FT.SEARCH dict with bytes keys (b"results", b"total_results",
        # b"attributes", b"format", b"warning").  Without bytes-aware
        # detection this would fall through to the RESP2 path and raise
        # ``KeyError: 2``.
        raw = {
            b"attributes": [],
            b"format": b"STRING",
            b"results": [],
            b"total_results": 0,
            b"warning": [],
        }
        total, rows = split_search_response(raw, protocol=3, command="search")
        assert total == 0
        assert rows == []

    def test_top_level_bytes_keys_protocol_sniffing(self):
        # Even with ``protocol=None`` the bytes-keyed dict must be detected
        # as RESP3 (so callers that don't know the protocol still work).
        raw = {
            b"attributes": [],
            b"format": b"STRING",
            b"results": [],
            b"total_results": 0,
            b"warning": [],
        }
        total, rows = split_search_response(raw, command="search")
        assert total == 0
        assert rows == []

    def test_top_level_bytes_keys_with_data(self):
        raw = {
            b"attributes": [],
            b"format": b"STRING",
            b"results": [
                {
                    b"id": b"doc:1",
                    b"extra_attributes": {b"name": b"Alice", b"age": b"30"},
                    b"values": [],
                },
                {
                    b"id": b"doc:2",
                    b"extra_attributes": {b"name": b"Bob", b"age": b"25"},
                    b"values": [],
                },
            ],
            b"total_results": 2,
            b"warning": [],
        }
        total, rows = split_search_response(raw, protocol=3, command="search")
        assert total == 2
        assert rows[0] == ["id", b"doc:1", "name", b"Alice", "age", b"30"]
        assert rows[1] == ["id", b"doc:2", "name", b"Bob", "age", b"25"]

    def test_aggregate_layout_no_id_field(self):
        # FT.AGGREGATE rows don't have an ``id`` field; __key appears in
        # ``extra_attributes`` instead.
        raw = {
            "total_results": 2,
            "results": [
                {
                    "extra_attributes": {"__key": "doc:1", "count": "5"},
                    "values": [],
                },
                {
                    "extra_attributes": {"__key": "doc:2", "count": "10"},
                    "values": [],
                },
            ],
        }
        total, rows = split_search_response(raw, protocol=3, command="aggregate")
        assert total == 2
        assert rows[0] == ["__key", "doc:1", "count", "5"]
        assert rows[1] == ["__key", "doc:2", "count", "10"]


# ── split_cursor_response — RESP2 ───────────────────────────────────────


class TestSplitCursorResponseResp2:
    def test_wrapped_with_cursor_id(self):
        raw = [
            [
                2,
                ["__key", "doc:1"],
                ["__key", "doc:2"],
            ],
            12345,
        ]
        rows, cursor_id = split_cursor_response(raw, protocol=2)
        assert cursor_id == 12345
        assert rows == [["__key", "doc:1"], ["__key", "doc:2"]]

    def test_cursor_id_zero_when_no_more_pages(self):
        raw = [[1, ["__key", "doc:1"]], 0]
        rows, cursor_id = split_cursor_response(raw, protocol=2)
        assert cursor_id == 0
        assert rows == [["__key", "doc:1"]]


# ── split_cursor_response — RESP3 ───────────────────────────────────────


class TestSplitCursorResponseResp3:
    def test_dict_with_cursor_id(self):
        raw = [
            {
                "total_results": 2,
                "results": [
                    {"extra_attributes": {"__key": "doc:1"}, "values": []},
                    {"extra_attributes": {"__key": "doc:2"}, "values": []},
                ],
            },
            12345,
        ]
        rows, cursor_id = split_cursor_response(raw, protocol=3)
        assert cursor_id == 12345
        assert rows == [
            ["__key", "doc:1"],
            ["__key", "doc:2"],
        ]

    def test_dict_alone_means_no_more_pages(self):
        raw = {"total_results": 1, "results": []}
        rows, cursor_id = split_cursor_response(raw, protocol=3)
        assert cursor_id == 0
        assert rows == []

    def test_empty_response(self):
        rows, cursor_id = split_cursor_response(None, protocol=3)
        assert rows == []
        assert cursor_id == 0

    def test_string_cursor_id_is_coerced(self):
        # redis-py may surface cursor IDs as strings on some Redis versions.
        raw = [{"total_results": 0, "results": []}, "0"]
        rows, cursor_id = split_cursor_response(raw, protocol=3)
        assert cursor_id == 0


# ── extract_key_from_row ────────────────────────────────────────────────


class TestExtractKeyFromRow:
    def test_string_key_in_flat_row(self):
        assert extract_key_from_row(["__key", "doc:1", "name", "Alice"]) == "doc:1"

    def test_bytes_key_in_flat_row(self):
        assert extract_key_from_row([b"__key", b"doc:1"]) == "doc:1"

    def test_missing_key_returns_none(self):
        assert extract_key_from_row(["name", "Alice"]) is None

    def test_empty_row_returns_none(self):
        assert extract_key_from_row([]) is None
