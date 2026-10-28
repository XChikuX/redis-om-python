# type: ignore
"""Tests for the RedisStream (X* commands) helper.

Standard stream commands run on any Redis version.  Newer commands
(``XACKDEL``, ``XDELEX``, ``XNACK``, ``IDMP``) require Redis 8.2+/8.6+/
8.8+ and fall back gracefully on older servers.  The Redis 8.10
``MAXCOUNT``/``MAXSIZE`` read caps degrade to plain reads (with a
one-time ``UserWarning``) on older servers, so all tests run regardless
of server version.
"""

import warnings
from unittest import mock

import pytest

from aredis_om import RedisStream, get_redis_connection
from aredis_om.model.stream import (
    _DEFAULT_PRODUCER_ID,
    _parse_entries,
    _parse_read_response,
    clear_stream_capability_cache,
)
from aredis_om.util import ASYNC_MODE
from redis import ResponseError

from .conftest import py_test_mark_asyncio


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def stream(db, key_prefix):
    s = RedisStream(db, f"{key_prefix}:stream")
    yield s
    clear_stream_capability_cache()


class TestStreamProduce:
    @py_test_mark_asyncio
    async def test_add_auto_id(self, stream):
        eid = await stream.add({"event": "created"})
        assert isinstance(eid, str)
        assert "-" in eid

    @py_test_mark_asyncio
    async def test_add_explicit_id(self, stream):
        eid = await stream.add({"event": "x"}, id="1-0")
        assert eid == "1-0"

    @py_test_mark_asyncio
    async def test_add_returns_unique_ids(self, stream):
        eid1 = await stream.add({"v": "1"})
        eid2 = await stream.add({"v": "2"})
        assert eid1 != eid2

    @py_test_mark_asyncio
    async def test_add_with_maxlen(self, stream):
        for i in range(10):
            await stream.add({"i": str(i)}, maxlen=3, approximate=False)
        assert await stream.length() <= 3

    @py_test_mark_asyncio
    async def test_add_idempotent_no_error(self, stream):
        # On Redis < 8.6 the IDMP option is dropped with a one-time
        # warning; on 8.6+ the append is deduplicated.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            eid = await stream.add({"event": "x"}, idempotent=True)
        assert isinstance(eid, str)


class TestStreamRead:
    @py_test_mark_asyncio
    async def test_read_from_start(self, stream):
        await stream.add({"a": "1"})
        await stream.add({"a": "2"})
        entries = await stream.read(last_id="0")
        assert len(entries) == 2
        assert entries[0].fields["a"] == "1"
        assert entries[1].fields["a"] == "2"

    @py_test_mark_asyncio
    async def test_read_count_limit(self, stream):
        for i in range(5):
            await stream.add({"i": str(i)})
        entries = await stream.read(last_id="0", count=2)
        assert len(entries) == 2

    @py_test_mark_asyncio
    async def test_read_empty(self, stream):
        entries = await stream.read(last_id="0")
        assert entries == []


class TestStreamRange:
    @py_test_mark_asyncio
    async def test_range_full(self, stream):
        await stream.add({"i": "1"}, id="1-0")
        await stream.add({"i": "2"}, id="2-0")
        entries = await stream.range()
        assert len(entries) == 2
        assert entries[0].id == "1-0"

    @py_test_mark_asyncio
    async def test_revrange(self, stream):
        await stream.add({"i": "1"}, id="1-0")
        await stream.add({"i": "2"}, id="2-0")
        entries = await stream.revrange()
        assert entries[0].id == "2-0"
        assert entries[1].id == "1-0"

    @py_test_mark_asyncio
    async def test_length(self, stream):
        assert await stream.length() == 0
        await stream.add({"i": "1"})
        assert await stream.length() == 1


class TestStreamConsumerGroups:
    @py_test_mark_asyncio
    async def test_create_group(self, stream):
        await stream.add({"x": "1"})  # ensure stream exists
        ok = await stream.create_group("g1", id="0", mkstream=True)
        assert ok is True
        # Creating again is a no-op (returns False).
        ok2 = await stream.create_group("g1", id="0")
        assert ok2 is False

    @py_test_mark_asyncio
    async def test_read_group_consumes_entries(self, stream):
        await stream.add({"task": "a"}, id="1-0")
        await stream.add({"task": "b"}, id="2-0")
        await stream.create_group("workers", id="0", mkstream=True)
        entries = await stream.read_group("workers", "w1", count=10)
        assert len(entries) == 2
        assert entries[0].fields["task"] == "a"

    @py_test_mark_asyncio
    async def test_ack(self, stream):
        await stream.add({"task": "a"}, id="1-0")
        await stream.create_group("workers", id="0", mkstream=True)
        entries = await stream.read_group("workers", "w1", count=10)
        n = await stream.ack("workers", entries[0].id)
        assert n == 1

    @py_test_mark_asyncio
    async def test_destroy_group(self, stream):
        await stream.add({"x": "1"})
        await stream.create_group("g1", id="0", mkstream=True)
        n = await stream.destroy_group("g1")
        assert n == 1


class TestStreamAckAndDelete:
    @py_test_mark_asyncio
    async def test_ack_and_delete_fallback(self, stream):
        """ack_and_delete always removes the entry, even on old Redis."""
        await stream.add({"task": "a"}, id="1-0")
        await stream.create_group("workers", id="0", mkstream=True)
        entries = await stream.read_group("workers", "w1", count=10)
        n = await stream.ack_and_delete("workers", entries[0].id)
        assert n == 1
        # The entry should be gone from the stream.
        assert await stream.length() == 0

    @py_test_mark_asyncio
    async def test_ack_and_delete_strategy_kwarg_accepted(self, stream):
        """The strategy kwarg should not raise on any server version."""
        await stream.add({"task": "a"}, id="1-0")
        await stream.create_group("workers", id="0", mkstream=True)
        entries = await stream.read_group("workers", "w1", count=10)
        n = await stream.ack_and_delete("workers", entries[0].id, strategy="delref")
        assert n >= 0


class TestStreamDeleteEx:
    @py_test_mark_asyncio
    async def test_delete_ex_fallback(self, stream):
        await stream.add({"i": "1"}, id="1-0")
        n = await stream.delete_ex("1-0")
        assert n >= 0
        assert await stream.length() == 0


class TestStreamNack:
    @py_test_mark_asyncio
    async def test_nack_no_error_on_old_redis(self, stream):
        await stream.add({"i": "1"}, id="1-0")
        await stream.create_group("workers", id="0", mkstream=True)
        await stream.read_group("workers", "w1", count=10)
        # nack returns 0 on old Redis, >= 0 on 8.8+
        n = await stream.nack("workers", "w1", "1-0")
        assert n >= 0


class TestStreamClaim:
    @py_test_mark_asyncio
    async def test_claim(self, stream):
        await stream.add({"task": "a"}, id="1-0")
        await stream.create_group("workers", id="0", mkstream=True)
        await stream.read_group("workers", "w1", count=10)
        # Claim with min-idle 0ms — should transfer immediately.
        claimed = await stream.claim("workers", "w2", 0, "1-0")
        assert len(claimed) == 1
        assert claimed[0].id == "1-0"

    @py_test_mark_asyncio
    async def test_claim_justid(self, stream):
        await stream.add({"task": "a"}, id="1-0")
        await stream.create_group("workers", id="0", mkstream=True)
        await stream.read_group("workers", "w1", count=10)
        claimed = await stream.claim("workers", "w2", 0, "1-0", justid=True)
        assert len(claimed) == 1
        # redis-py post-processes JUSTID responses; the id is at least
        # the timestamp portion of the original entry.
        assert str(claimed[0].id).startswith("1")


class TestStreamTrim:
    @py_test_mark_asyncio
    async def test_trim_maxlen(self, stream):
        for i in range(5):
            await stream.add({"i": str(i)}, id=f"{i + 1}-0")
        removed = await stream.trim(maxlen=2, approximate=False)
        assert removed >= 3
        assert await stream.length() <= 2


class TestStreamInfo:
    @py_test_mark_asyncio
    async def test_info(self, stream):
        await stream.add({"x": "1"})
        info = await stream.info()
        # ``XINFO STREAM`` returns many fields; at least length should be present.
        assert info  # non-empty


# ---------------------------------------------------------------------------
# MAXCOUNT / MAXSIZE (Redis 8.10+) — unit tests (no Redis required)
# ---------------------------------------------------------------------------


def _mock_db(key, read_limits=True, idmp=True):
    """Build an AsyncMock db that records ``execute_command`` calls.

    The capability probes are matched by their exact argument lists:

    - read limits: ``XREAD MAXCOUNT 1 STREAMS <key> $``
    - idempotency: ``XADD <key>:__idmp_probe__ NOMKSTREAM IDMPAUTO
      probe * probe probe`` (side-effect-free trial, mirroring Redis
      8.10+ vs pre-8.6 behaviour).

    Each returns ``None`` when the corresponding flag is ``True`` and
    raises the ``ResponseError`` a pre-8.6/pre-8.10 server would raise
    when not.  Real ``XADD`` calls return a fixed entry id; everything
    else returns an empty read response.
    """
    db = mock.AsyncMock()
    calls = []
    read_probe = ["XREAD", "MAXCOUNT", 1, "STREAMS", key, "$"]
    idmp_probe = [
        "XADD",
        f"{key}:__idmp_probe__",
        "NOMKSTREAM",
        "IDMPAUTO",
        "probe",
        "*",
        "probe",
        "probe",
    ]

    def _record(*args):
        calls.append(list(args))
        if list(args) == read_probe:
            if not read_limits:
                raise ResponseError("ERR syntax error")
            return None
        if list(args) == idmp_probe:
            if not idmp:
                raise ResponseError(
                    "ERR Invalid stream ID specified as stream command argument"
                )
            return None
        if args and args[0] == "XADD":
            return "1-0"
        return []

    db.execute_command = mock.AsyncMock(side_effect=_record)
    return db, calls


asyncio_skip = pytest.mark.skipif(
    not ASYNC_MODE, reason="AsyncMock-based unit tests are async-only"
)


class TestStreamReadLimitsUnit:
    @pytest.fixture
    def reset_cache(self):
        clear_stream_capability_cache()
        yield
        clear_stream_capability_cache()

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_read_maxcount_args(self, reset_cache):
        """MAXCOUNT is sent after the capability probe succeeds."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.read(last_id="0", max_count=5)
        assert calls[0] == ["XREAD", "MAXCOUNT", 1, "STREAMS", "orders", "$"]
        assert calls[1] == ["XREAD", "MAXCOUNT", 5, "STREAMS", "orders", "0"]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_read_count_maxcount_maxsize_arg_order(self, reset_cache):
        """COUNT → MAXCOUNT → MAXSIZE → BLOCK → STREAMS (canonical order)."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.read(count=2, max_count=5, max_size=1024, block=100)
        assert calls[1] == [
            "XREAD",
            "COUNT",
            2,
            "MAXCOUNT",
            5,
            "MAXSIZE",
            1024,
            "BLOCK",
            100,
            "STREAMS",
            "orders",
            "$",
        ]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_read_maxsize_only(self, reset_cache):
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.read(max_size=2048)
        assert calls[1] == ["XREAD", "MAXSIZE", 2048, "STREAMS", "orders", "$"]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_read_group_maxcount_maxsize_args(self, reset_cache):
        """XREADGROUP probes XREAD (same 8.10 release) and keeps its own
        option order: COUNT → MAXCOUNT → MAXSIZE → BLOCK → NOACK → CLAIM."""
        db, calls = _mock_db("tasks")
        stream = RedisStream(db, "tasks")
        await stream.read_group(
            "workers",
            "w1",
            count=2,
            max_count=5,
            max_size=1024,
            noack=True,
            claim_min_idle=100,
        )
        # The probe is an XREAD, not an XREADGROUP.
        assert calls[0][0] == "XREAD"
        assert calls[1] == [
            "XREADGROUP",
            "GROUP",
            "workers",
            "w1",
            "COUNT",
            2,
            "MAXCOUNT",
            5,
            "MAXSIZE",
            1024,
            "NOACK",
            "CLAIM",
            100,
            "STREAMS",
            "tasks",
            ">",
        ]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_probe_cached_per_connection(self, reset_cache):
        """The capability probe runs once; subsequent reads reuse it."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.read(max_count=5)
        await stream.read(max_count=5)
        probes = [c for c in calls if c[2] == 1]  # probe uses MAXCOUNT 1
        assert len(probes) == 1
        assert len(calls) == 3  # 1 probe + 2 reads

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_no_probe_without_caps(self, reset_cache):
        """Plain reads never probe — zero overhead when caps are unused."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.read(last_id="0")
        assert calls == [["XREAD", "STREAMS", "orders", "0"]]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_unsupported_server_drops_caps_and_warns_once(self, reset_cache):
        """On pre-8.10 servers the caps are dropped with a single warning."""
        db, calls = _mock_db("orders", read_limits=False)
        stream = RedisStream(db, "orders")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", UserWarning)
            await stream.read(last_id="0", max_count=5, max_size=1024)
            await stream.read(last_id="0", max_count=5, max_size=1024)
        # calls[0] is the failed probe; both reads went out without caps...
        assert calls[1] == ["XREAD", "STREAMS", "orders", "0"]
        assert calls[2] == ["XREAD", "STREAMS", "orders", "0"]
        # ...but the warning fired exactly once (per connection).
        user_warnings = [w for w in caught if w.category is UserWarning]
        assert len(user_warnings) == 1
        assert "MAXCOUNT" in str(user_warnings[0].message)

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_clear_cache_reprobes(self, reset_cache):
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.read(max_count=5)
        clear_stream_capability_cache()
        await stream.read(max_count=5)
        probes = [c for c in calls if c[2] == 1]  # probe uses MAXCOUNT 1
        assert len(probes) == 2


# ---------------------------------------------------------------------------
# MAXCOUNT / MAXSIZE (Redis 8.10+) — integration tests (any server version)
# ---------------------------------------------------------------------------


class TestStreamReadLimits:
    """Version-tolerant: caps take effect on Redis 8.10+, degrade on older."""

    async def _seed(self, stream, n=5, pad=None):
        for i in range(n):
            fields = {"i": str(i)}
            if pad is not None:
                fields["pad"] = pad
            await stream.add(fields)

    @py_test_mark_asyncio
    async def test_read_max_count_caps_entries(self, stream):
        await self._seed(stream)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            entries = await stream.read(last_id="0", max_count=2)
        if await stream._supports_read_limits():
            assert len(entries) == 2
        else:
            assert len(entries) == 5

    @py_test_mark_asyncio
    async def test_read_max_count_with_count(self, stream):
        await self._seed(stream)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            # Per-stream COUNT governs when both are given.
            entries = await stream.read(last_id="0", count=1, max_count=4)
            assert len(entries) == 1
            # MAXCOUNT alone caps the cumulative reply.
            entries = await stream.read(last_id="0", max_count=4)
        if await stream._supports_read_limits():
            assert len(entries) == 4
        else:
            assert len(entries) == 5

    @py_test_mark_asyncio
    async def test_read_max_size_caps_reply(self, stream):
        await self._seed(stream, pad="x" * 64)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            entries = await stream.read(last_id="0", max_size=64)
        if await stream._supports_read_limits():
            # Each entry exceeds 64 bytes; at least one is always returned.
            assert len(entries) == 1
        else:
            assert len(entries) == 5

    @py_test_mark_asyncio
    async def test_read_group_max_count(self, stream):
        await self._seed(stream)
        await stream.create_group("workers", id="0", mkstream=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            entries = await stream.read_group("workers", "w1", max_count=2)
        if await stream._supports_read_limits():
            assert len(entries) == 2
        else:
            assert len(entries) == 5

    @py_test_mark_asyncio
    async def test_read_group_max_size(self, stream):
        await self._seed(stream, pad="x" * 64)
        await stream.create_group("workers", id="0", mkstream=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            entries = await stream.read_group("workers", "w1", max_size=64)
        if await stream._supports_read_limits():
            assert len(entries) == 1
        else:
            assert len(entries) == 5

    @py_test_mark_asyncio
    async def test_read_group_max_count_noack(self, stream):
        """Caps compose with NOACK and CLAIM options."""
        await self._seed(stream)
        await stream.create_group("workers", id="0", mkstream=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            entries = await stream.read_group("workers", "w1", max_count=3, noack=True)
        if await stream._supports_read_limits():
            assert len(entries) == 3
        else:
            assert len(entries) == 5

    @py_test_mark_asyncio
    async def test_no_warning_when_caps_unused(self, stream):
        await self._seed(stream, n=1)
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            entries = await stream.read(last_id="0")
        assert len(entries) == 1


# ---------------------------------------------------------------------------
# IDMP / IDMPAUTO (Redis 8.6+) — unit tests (no Redis required)
# ---------------------------------------------------------------------------

_IDMP_PROBE_SUFFIX = ":__idmp_probe__"


class TestStreamIdmpUnit:
    @pytest.fixture
    def reset_cache(self):
        clear_stream_capability_cache()
        yield
        clear_stream_capability_cache()

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_idempotent_auto_args(self, reset_cache):
        """idempotent=True probes once, then sends IDMPAUTO + default pid."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        eid = await stream.add({"event": "x"}, idempotent=True)
        assert eid == "1-0"
        assert calls[0] == [
            "XADD",
            f"orders{_IDMP_PROBE_SUFFIX}",
            "NOMKSTREAM",
            "IDMPAUTO",
            "probe",
            "*",
            "probe",
            "probe",
        ]
        assert calls[1] == [
            "XADD",
            "orders",
            "IDMPAUTO",
            _DEFAULT_PRODUCER_ID,
            "*",
            "event",
            "x",
        ]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_idempotent_explicit_ids(self, reset_cache):
        """producer_id + idempotent_id switch to the IDMP form."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.add(
            {"event": "x"},
            idempotent=True,
            producer_id="producer-1",
            idempotent_id="order-123",
        )
        assert calls[1] == [
            "XADD",
            "orders",
            "IDMP",
            "producer-1",
            "order-123",
            "*",
            "event",
            "x",
        ]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_idempotent_before_maxlen(self, reset_cache):
        """Canonical order: IDMP/IDMPAUTO precede MAXLEN/MINID."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.add({"event": "x"}, idempotent=True, maxlen=100)
        assert calls[1] == [
            "XADD",
            "orders",
            "IDMPAUTO",
            _DEFAULT_PRODUCER_ID,
            "MAXLEN",
            "~",
            100,
            "*",
            "event",
            "x",
        ]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_idempotent_rejects_explicit_entry_id(self, reset_cache):
        """The server requires auto-generated ids with IDMP (verified)."""
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        with pytest.raises(ValueError, match="auto-generated"):
            await stream.add({"event": "x"}, idempotent=True, id="1-0")
        assert calls == []  # nothing sent — not even the probe

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_unsupported_idmp_drops_and_warns_once(self, reset_cache):
        """Pre-8.6 servers: IDMP dropped, plain XADD, one-time warning."""
        db, calls = _mock_db("orders", idmp=False)
        stream = RedisStream(db, "orders")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", UserWarning)
            await stream.add({"event": "x"}, idempotent=True)
            await stream.add({"event": "y"}, idempotent=True)
        # calls[0] is the failed probe; both adds went out without IDMP.
        assert calls[1] == ["XADD", "orders", "*", "event", "x"]
        assert calls[2] == ["XADD", "orders", "*", "event", "y"]
        user_warnings = [w for w in caught if w.category is UserWarning]
        assert len(user_warnings) == 1
        assert "IDMP" in str(user_warnings[0].message)

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_plain_never_probes(self, reset_cache):
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.add({"event": "x"})
        assert calls == [["XADD", "orders", "*", "event", "x"]]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_add_idempotent_probe_cached(self, reset_cache):
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.add({"event": "x"}, idempotent=True)
        await stream.add({"event": "y"}, idempotent=True)
        probes = [
            c for c in calls if len(c) == 8 and c[0] == "XADD" and c[3] == "IDMPAUTO"
        ]
        assert len(probes) == 1
        assert len(calls) == 3  # 1 probe + 2 adds

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_configure_idempotency_args(self, reset_cache):
        db, calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        await stream.configure_idempotency(duration=300, maxsize=1000)
        assert calls[-1] == [
            "XCFGSET",
            "orders",
            "IDMP-DURATION",
            300,
            "IDMP-MAXSIZE",
            1000,
        ]
        await stream.configure_idempotency(duration=500)
        assert calls[-1] == ["XCFGSET", "orders", "IDMP-DURATION", 500]

    @py_test_mark_asyncio
    @asyncio_skip
    async def test_configure_idempotency_requires_argument(self, reset_cache):
        db, _calls = _mock_db("orders")
        stream = RedisStream(db, "orders")
        with pytest.raises(ValueError):
            await stream.configure_idempotency()


# ---------------------------------------------------------------------------
# IDMP / IDMPAUTO (Redis 8.6+) — integration tests (any server version)
# ---------------------------------------------------------------------------


class TestStreamIdmp:
    """Version-tolerant: dedup works on Redis 8.6+, degrades on older."""

    @py_test_mark_asyncio
    async def test_idempotent_add_dedups_retries(self, stream):
        """The core fix: identical idempotent adds return the same id."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            eid1 = await stream.add({"event": "created"}, idempotent=True)
            eid2 = await stream.add({"event": "created"}, idempotent=True)
        if await stream._supports_idmp():
            assert eid1 == eid2
            assert await stream.length() == 1
        else:
            assert eid1 != eid2
            assert await stream.length() == 2

    @py_test_mark_asyncio
    async def test_idempotent_add_explicit_idempotent_id(self, stream):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            eid1 = await stream.add(
                {"event": "x"},
                idempotent=True,
                producer_id="p1",
                idempotent_id="order-123",
            )
            eid2 = await stream.add(
                {"event": "different-fields"},
                idempotent=True,
                producer_id="p1",
                idempotent_id="order-123",
            )
            eid3 = await stream.add(
                {"event": "x"},
                idempotent=True,
                producer_id="p1",
                idempotent_id="order-456",
            )
        if await stream._supports_idmp():
            # Same (pid, iid) dedups even with different fields.
            assert eid1 == eid2
            assert eid3 != eid1
            assert await stream.length() == 2
        else:
            assert await stream.length() == 3

    @py_test_mark_asyncio
    async def test_idempotent_add_producer_isolation(self, stream):
        """Same fields but different producers do not deduplicate."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            eid1 = await stream.add({"event": "x"}, idempotent=True, producer_id="p1")
            eid2 = await stream.add({"event": "x"}, idempotent=True, producer_id="p2")
        if await stream._supports_idmp():
            assert eid1 != eid2
            assert await stream.length() == 2
        else:
            assert await stream.length() == 2

    @py_test_mark_asyncio
    async def test_idempotent_add_rejects_explicit_entry_id(self, stream):
        with pytest.raises(ValueError, match="auto-generated"):
            await stream.add({"event": "x"}, idempotent=True, id="1-0")

    @py_test_mark_asyncio
    async def test_configure_idempotency_reflected_in_info(self, stream):
        await stream.add({"event": "x"})  # XCFGSET requires an existing stream
        if not await stream._supports_idmp():
            pytest.skip("XCFGSET requires Redis 8.6+")
        await stream.configure_idempotency(duration=500, maxsize=500)
        info = await stream.info()
        assert info["idmp-duration"] == 500
        assert info["idmp-maxsize"] == 500

    @py_test_mark_asyncio
    async def test_probe_leaves_no_side_effects(self, stream):
        """The NOMKSTREAM trial probe must not create keys or entries."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            await stream.add({"event": "x"}, idempotent=True)
        # Only the real entry exists; the probe key was never created.
        assert await stream.length() == 1
        probe_key = f"{stream.key}:__idmp_probe__"
        exists = await stream._db.exists(probe_key)
        assert not exists


# ---------------------------------------------------------------------------
# Reply-shape parsing: RESP2/RESP3 x decode_responses (pure unit tests)
# ---------------------------------------------------------------------------


class TestParseReadResponseShapes:
    """_parse_read_response must handle every redis-py reply shape:

    RESP2: [[key, [(id, fields), ...]], ...]
    RESP3: {key: [[(id, fields), ...]]} (extra list nesting)
    decode_responses=False: keys/ids/field names as bytes
    """

    def test_resp2_str_keys(self):
        raw = [["orders", [("1-0", {"f": "v"})]]]
        entries = _parse_read_response(raw, "orders")
        assert len(entries) == 1
        assert entries[0].id == "1-0"
        assert entries[0].fields == {"f": "v"}

    def test_resp2_skips_other_streams(self):
        raw = [["other", [("1-0", {"f": "v"})]], ["orders", [("2-0", {"g": "w"})]]]
        entries = _parse_read_response(raw, "orders")
        assert len(entries) == 1
        assert entries[0].id == "2-0"

    def test_resp3_dict_shape(self):
        raw = {"orders": [[("1-0", {"f": "v"}), ("2-0", {"g": "w"})]]}
        entries = _parse_read_response(raw, "orders")
        assert len(entries) == 2
        assert entries[1].fields == {"g": "w"}

    def test_resp3_single_entry_nesting(self):
        # One entry: [[("1-0", {...})]] — the wrapper list must not be
        # mistaken for an entry.
        raw = {"orders": [[("1-0", {"f": "v"})]]}
        entries = _parse_read_response(raw, "orders")
        assert len(entries) == 1
        assert entries[0].fields == {"f": "v"}

    def test_resp2_bytes_keys(self):
        raw = [[b"orders", [(b"1-0", {b"f": b"v"})]]]
        entries = _parse_read_response(raw, "orders")
        assert len(entries) == 1
        assert entries[0].id == b"1-0"
        assert entries[0].fields == {b"f": b"v"}

    def test_resp3_bytes_keys(self):
        raw = {b"orders": [[(b"1-0", {b"f": b"v"})]]}
        entries = _parse_read_response(raw, "orders")
        assert len(entries) == 1
        assert entries[0].fields == {b"f": b"v"}

    def test_empty_shapes(self):
        assert _parse_read_response(None, "k") == []
        assert _parse_read_response([], "k") == []
        assert _parse_read_response({}, "k") == []

    def test_none_entries_skipped(self):
        # redis-py pads XCLAIM misses with (None, None).
        entries = _parse_entries([("1-0", {"f": "v"}), (None, None)])
        assert len(entries) == 1
