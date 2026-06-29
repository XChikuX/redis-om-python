# type: ignore
"""Unit tests for ``RedisModel.from_redis`` RESP3 handling.

These tests target the regression where redis-py surfaces RESP3 ``FT.SEARCH``
map keys as ``bytes`` (e.g. ``b"results"`` instead of ``"results"``) when
``decode_responses=False`` is in effect.

The historical ``from_redis`` implementation only inspected ``str`` keys,
so a bytes-keyed RESP3 dict was mis-classified as a RESP2 list and the code
raised ``KeyError: 2`` while iterating.

The first half of the file is pure unit tests that feed synthetic raw
responses straight into ``from_redis`` — they need no live Redis.  The
second half (gated on a live Redis with RediSearch) captures actual wire
shapes from Redis under RESP2 and RESP3 to prove the fix covers every
protocol/decode combination.
"""

import pytest

from aredis_om import Field, HashModel, JsonModel
from aredis_om.connections import get_redis_connection
from tests._sync_redis import has_redisearch as sync_has_redisearch

from .conftest import py_test_mark_asyncio

HAS_REDISEARCH = sync_has_redisearch()

pytestmark = pytest.mark.skipif(
    not HAS_REDISEARCH,
    reason="requires a running Redis with RediSearch for the live portion",
)


def _make_hash_model(name: str) -> type:
    """Build a HashModel with email/name fields.

    Uses ``exec`` so Pydantic v2 can resolve annotations cleanly (the same
    pattern the rest of the test suite uses).
    """
    ns = {"HashModel": HashModel, "Field": Field}
    code = f"""
class {name}(HashModel):
    email: str = Field(index=True)
    name: str
"""
    exec(code, ns)
    return ns[name]


def _make_json_model(name: str) -> type:
    ns = {"JsonModel": JsonModel, "Field": Field}
    code = f"""
class {name}(JsonModel):
    email: str = Field(index=True)
    name: str
"""
    exec(code, ns)
    return ns[name]


# ── Regression: the exact bytes-keyed empty dict from the bug report ────


class TestResp3BytesKeysRegression:
    """Reproduces the original issue exactly and confirms the fix."""

    def _issue_dict(self):
        # This is verbatim what the user saw in the locals dump.
        return {
            b"attributes": [],
            b"format": b"STRING",
            b"results": [],
            b"total_results": 0,
            b"warning": [],
        }

    def test_hash_model_no_keyerror_protocol_explicit(self):
        M = _make_hash_model("_Resp3BytesRegressionHash")
        # Before the fix: KeyError: 2.
        assert M.from_redis(self._issue_dict(), protocol=3) == []

    def test_hash_model_no_keyerror_protocol_sniff(self):
        M = _make_hash_model("_Resp3BytesRegressionHashSniff")
        # ``protocol=None`` must also sniff the bytes-keyed dict correctly.
        assert M.from_redis(self._issue_dict(), protocol=None) == []

    def test_json_model_no_keyerror_protocol_explicit(self):
        M = _make_json_model("_Resp3BytesRegressionJson")
        assert M.from_redis(self._issue_dict(), protocol=3) == []

    def test_json_model_no_keyerror_protocol_sniff(self):
        M = _make_json_model("_Resp3BytesRegressionJsonSniff")
        assert M.from_redis(self._issue_dict(), protocol=None) == []


# ── RESP3 with bytes keys + actual rows ─────────────────────────────────


class TestResp3BytesKeysWithRows:
    def _dict_with_rows(self, rows):
        return {
            b"attributes": [],
            b"format": b"STRING",
            b"results": rows,
            b"total_results": len(rows),
            b"warning": [],
        }

    def test_hash_model_single_row_bytes_keys(self):
        M = _make_hash_model("_Resp3BytesHashSingle")
        raw = self._dict_with_rows(
            [
                {
                    b"id": b"doc:1",
                    b"extra_attributes": {
                        b"email": b"lalaland7@gmail.com",
                        b"name": b"Rain",
                    },
                    b"values": [],
                }
            ]
        )
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "lalaland7@gmail.com"
        assert docs[0].name == "Rain"

    def test_hash_model_multiple_rows_bytes_keys(self):
        M = _make_hash_model("_Resp3BytesHashMulti")
        raw = self._dict_with_rows(
            [
                {
                    b"id": b"doc:1",
                    b"extra_attributes": {b"email": b"a@x", b"name": b"Alice"},
                    b"values": [],
                },
                {
                    b"id": b"doc:2",
                    b"extra_attributes": {b"email": b"b@x", b"name": b"Bob"},
                    b"values": [],
                },
            ]
        )
        docs = M.from_redis(raw, protocol=3)
        assert sorted(d.name for d in docs) == ["Alice", "Bob"]
        assert sorted(d.email for d in docs) == ["a@x", "b@x"]

    def test_json_model_single_row_bytes_keys(self):
        M = _make_json_model("_Resp3BytesJsonSingle")
        raw = self._dict_with_rows(
            [
                {
                    b"id": b"doc:1",
                    b"extra_attributes": {b"$": b'{"email": "z@x", "name": "Zed"}'},
                    b"values": [],
                }
            ]
        )
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "z@x"
        assert docs[0].name == "Zed"

    def test_protocol_sniffing_with_bytes_keys(self):
        # Without ``protocol=3``, the sniff path must still detect the dict
        # as RESP3 via ``is_resp3_search_response``.
        M = _make_hash_model("_Resp3BytesHashSniffData")
        raw = self._dict_with_rows(
            [
                {
                    b"id": b"doc:1",
                    b"extra_attributes": {b"email": b"x@y", b"name": b"Z"},
                    b"values": [],
                }
            ]
        )
        docs = M.from_redis(raw, protocol=None)
        assert len(docs) == 1
        assert docs[0].name == "Z"


# ── Mixed and edge cases ────────────────────────────────────────────────


class TestFromRedisEdgeCases:
    def test_resp3_str_keys_still_work(self):
        """The original (str-keyed) RESP3 path must keep working."""
        M = _make_hash_model("_Resp3StrKeysHash")
        raw = {
            "total_results": 1,
            "results": [
                {
                    "id": "doc:1",
                    "extra_attributes": {"email": "a@b", "name": "Alice"},
                    "values": [],
                }
            ],
        }
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "a@b"
        assert docs[0].name == "Alice"

    def test_resp2_empty_list_returns_empty(self):
        M = _make_hash_model("_Resp2EmptyHash")
        assert M.from_redis([0], protocol=2) == []

    def test_resp2_list_still_works(self):
        M = _make_hash_model("_Resp2DataHash")
        raw = [1, "doc:1", ["email", "a@b", "name", "Alice"]]
        docs = M.from_redis(raw, protocol=2)
        assert len(docs) == 1
        assert docs[0].email == "a@b"
        assert docs[0].name == "Alice"

    def test_resp2_bytes_list_still_works(self):
        # ``decode_responses=False`` produces bytes values inside the flat
        # list; the RESP2 path's ``to_string`` helper already decodes them.
        M = _make_hash_model("_Resp2BytesDataHash")
        raw = [1, b"doc:1", [b"email", b"a@b", b"name", b"Alice"]]
        docs = M.from_redis(raw, protocol=2)
        assert len(docs) == 1
        assert docs[0].email == "a@b"
        assert docs[0].name == "Alice"

    def test_unrecognised_dict_returns_empty(self):
        # If we ever get a dict that doesn't look like RESP3 (no results /
        # total_results keys, in either str or bytes form), we must NOT raise
        # KeyError.  Returning [] matches the historical contract for
        # unparseable responses.
        M = _make_hash_model("_Resp3Unknown")
        assert M.from_redis({"random": "dict"}, protocol=2) == []

    def test_score_fields_attached(self):
        # Score handling only kicks in for JSON models (the ``$`` marker is
        # what triggers the score-attachment branch in ``from_redis``).
        M = _make_json_model("_Resp3ScoreJson")
        raw = {
            "total_results": 1,
            "results": [
                {
                    "id": "doc:1",
                    "extra_attributes": {
                        "$": '{"email": "a@b", "name": "Alice"}',
                        "__score": "0.95",
                    },
                    "values": [],
                }
            ],
        }
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "a@b"
        assert docs[0].name == "Alice"
        assert docs[0]._score == pytest.approx(0.95)

    def test_mixed_bytes_and_str_keys_in_one_dict(self):
        # Defensive: in practice the keys are uniform, but we shouldn't crash
        # if the response is mixed.
        M = _make_hash_model("_Resp3MixedKeys")
        raw = {
            b"total_results": 1,
            "results": [
                {
                    "id": "doc:1",
                    "extra_attributes": {b"email": b"m@x", b"name": b"M"},
                    "values": [],
                }
            ],
        }
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "m@x"
        assert docs[0].name == "M"


# ── Live-Redis parity: prove the fix works for every protocol/decode combo


class TestLiveRespParity:
    """Capture real FT.SEARCH wire shapes from Redis and feed them to
    ``from_redis`` for all four protocol/decode combinations.

    This is what answers the user's question: are RESP2 results also affected
    by the bytes-key regression?  (Spoiler: no, RESP2 uses a flat list shape
    whose bytes values were already handled by the ``to_string`` helper.)

    Notes on how redis-py surfaces RESP3 dict keys:

    * ``Redis.from_url(..., decode_responses=False)`` -> str keys (the URL
      parser forces decoding of the dict keys even when ``decode_responses``
      is off).
    * ``Redis(host=..., decode_responses=False)`` (positional/kwarg) ->
      bytes keys (this is what the user's bug report shows in the traceback).
    """

    async def _setup_index(self, db, idx: str) -> None:
        try:
            await db.execute_command("FT.DROPINDEX", idx, "DD")
        except Exception:
            pass
        await db.execute_command(
            "FT.CREATE",
            idx,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            f"{idx}:",
            "SCHEMA",
            "email",
            "TAG",
            "name",
            "TEXT",
        )
        await db.hset(
            f"{idx}:1", mapping={"email": "lalaland7@gmail.com", "name": "Rain"}
        )
        # Make sure the doc is indexed before the test queries.  RediSearch
        # indexes synchronously on HSET, but a brief wait guards against
        # edge cases when the same index name is dropped and recreated
        # rapidly across tests.
        for _ in range(10):
            raw = await db.execute_command("FT.SEARCH", idx, "*")
            if isinstance(raw, dict):
                if raw.get("total_results", 0) > 0:
                    break
            elif isinstance(raw, list) and raw and raw[0] > 0:
                break

    @py_test_mark_asyncio
    async def test_resp2_decoded_works(self):
        """RESP2 wire + decode_responses=True (the default) is a flat list.

        This case was *already* robust: the legacy ``to_string`` helper
        decodes bytes values, and the flat list shape doesn't use dict keys.
        """
        db = get_redis_connection(
            url="redis://localhost:6380?decode_responses=True&protocol=2"
        )
        idx = "live_resp2_dec"
        await self._setup_index(db, idx)
        raw = await db.execute_command("FT.SEARCH", idx, "*")
        assert isinstance(raw, list), f"expected list, got {type(raw).__name__}"
        M = _make_hash_model("_LiveResp2Dec")
        docs = M.from_redis(raw, protocol=2)
        assert len(docs) == 1
        assert docs[0].email == "lalaland7@gmail.com"
        assert docs[0].name == "Rain"

    @py_test_mark_asyncio
    async def test_resp2_bytes_works(self):
        """RESP2 wire + decode_responses=False produces bytes values inside
        the flat list.  The historical ``to_string`` helper already handled
        this; this test pins the behaviour down so future changes don't
        regress it.
        """
        # Positional construction is the only way to actually get bytes
        # values out of redis-py; the URL path decodes dict keys even when
        # decode_responses=False is in the query string.
        from redis import asyncio as aioredis

        db = aioredis.Redis(
            host="localhost", port=6380, decode_responses=False, protocol=2
        )
        idx = "live_resp2_raw"
        await self._setup_index(db, idx)
        raw = await db.execute_command("FT.SEARCH", idx, "*")
        assert isinstance(raw, list)
        assert any(isinstance(item, bytes) for item in raw)
        M = _make_hash_model("_LiveResp2Raw")
        docs = M.from_redis(raw, protocol=2)
        assert len(docs) == 1
        assert docs[0].email == "lalaland7@gmail.com"
        assert docs[0].name == "Rain"

    @py_test_mark_asyncio
    async def test_resp3_decoded_works(self):
        """RESP3 wire + decode_responses=True (the default)."""
        db = get_redis_connection(url="redis://localhost:6380?decode_responses=True")
        idx = "live_resp3_dec"
        await self._setup_index(db, idx)
        raw = await db.execute_command("FT.SEARCH", idx, "*")
        assert isinstance(raw, dict)
        # ``decode_responses=True`` -> str keys.
        assert all(isinstance(k, str) for k in raw)
        M = _make_hash_model("_LiveResp3Dec")
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "lalaland7@gmail.com"
        assert docs[0].name == "Rain"

    @py_test_mark_asyncio
    async def test_resp3_bytes_works_regression(self):
        """RESP3 wire + decode_responses=False (the user's exact setup).

        Before the fix this raised ``KeyError: 2`` because ``from_redis``
        checked ``"results" in res`` (str only) and the dict keys arrive as
        ``bytes`` from redis-py's RESP3 parser.
        """
        # Direct positional construction is required: Redis.from_url with
        # decode_responses=False still surfaces dict keys as str.
        from redis import asyncio as aioredis

        db = aioredis.Redis(host="localhost", port=6380, decode_responses=False)
        idx = "live_resp3_raw"
        await self._setup_index(db, idx)
        raw = await db.execute_command("FT.SEARCH", idx, "*")
        assert isinstance(raw, dict)
        assert all(isinstance(k, bytes) for k in raw)
        M = _make_hash_model("_LiveResp3Raw")
        docs = M.from_redis(raw, protocol=3)
        assert len(docs) == 1
        assert docs[0].email == "lalaland7@gmail.com"
        assert docs[0].name == "Rain"

    @py_test_mark_asyncio
    async def test_resp3_bytes_empty_results_regression(self):
        """The exact empty-results payload from the user's bug report."""
        from redis import asyncio as aioredis

        db = aioredis.Redis(host="localhost", port=6380, decode_responses=False)
        idx = "live_resp3_raw_empty"
        await self._setup_index(db, idx)
        raw = await db.execute_command("FT.SEARCH", idx, r"@email:{does\@not\@exist}")
        assert isinstance(raw, dict)
        # Direct positional construction surfaces bytes keys; verify and
        # also confirm ``from_redis`` doesn't blow up on the empty branch.
        assert raw[b"total_results"] == 0
        assert raw[b"results"] == []
        M = _make_hash_model("_LiveResp3RawEmpty")
        # Before the fix: KeyError: 2.
        assert M.from_redis(raw, protocol=3) == []

    @py_test_mark_asyncio
    async def test_resp2_bytes_empty_results(self):
        """RESP2 with empty results is just ``[0]``; confirm parity."""
        from redis import asyncio as aioredis

        db = aioredis.Redis(
            host="localhost", port=6380, decode_responses=False, protocol=2
        )
        idx = "live_resp2_raw_empty"
        await self._setup_index(db, idx)
        raw = await db.execute_command("FT.SEARCH", idx, r"@email:{does\@not\@exist}")
        assert raw == [0]
        M = _make_hash_model("_LiveResp2RawEmpty")
        assert M.from_redis(raw, protocol=2) == []


# ── End-to-end through the public FindQuery API ─────────────────────────


class TestEndToEndViaFindQuery:
    """The user's actual entry point is ``Model.find(...).all()``.

    This test class exercises the full pipeline (FindQuery.execute →
    execute_command → from_redis) against a model that talks to Redis
    directly, using the same ``decode_responses=False`` configuration that
    surfaced the bug.
    """

    @pytest.fixture
    def raw_user_model(self):
        # Direct positional construction is required: this is the only
        # configuration that surfaces RESP3 dict keys as bytes.  ``exec``
        # keeps Pydantic v2 happy by giving the model a proper module
        # namespace.
        from redis import asyncio as aioredis

        db = aioredis.Redis(host="localhost", port=6380, decode_responses=False)
        ns = {"HashModel": HashModel, "Field": Field, "db": db}
        code = """
class _E2EUser(HashModel):
    email: str = Field(index=True)
    name: str

    class Meta:
        database = db
"""
        exec(code, ns)
        return ns["_E2EUser"]

    @py_test_mark_asyncio
    async def test_find_all_with_decode_false_resp3(self, raw_user_model):
        from aredis_om import Migrator

        M = raw_user_model
        await Migrator().run()
        # Clean any leftover keys
        async for pk in await M.all_pks():
            await M.delete(pk)

        await M(email="lalaland7@gmail.com", name="Rain").save()

        # This is exactly the user's call: RedisUser.find(... email == ...).all()
        results = await M.find(M.email == "lalaland7@gmail.com").all()
        assert len(results) == 1
        assert results[0].name == "Rain"

        # The empty-results case (the user's traceback was triggered by the
        # first ``find().all()`` returning an empty RESP3 bytes-keyed dict).
        results = await M.find(M.email == "does@not.exist").all()
        assert results == []

        # Cleanup
        async for pk in await M.all_pks():
            await M.delete(pk)
