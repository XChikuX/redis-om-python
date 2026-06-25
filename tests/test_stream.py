# type: ignore
"""Tests for the RedisStream (X* commands) helper.

Standard stream commands run on any Redis version.  Newer commands
(``XACKDEL``, ``XDELEX``, ``XNACK``, ``IDMP``) require Redis 8.2+/8.6+/
8.8+ and fall back gracefully on older servers, so all tests run
regardless of server version.
"""

import pytest

from aredis_om import RedisStream, get_redis_connection
from aredis_om.model.stream import clear_stream_capability_cache

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
        # On Redis < 8.6 the IDMP keyword is silently dropped.
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
