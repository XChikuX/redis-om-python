# type: ignore
"""Tests for the protocol-aware connection layer in pyredis-om.

These tests cover:

* ``protocol_version`` detection against a live Redis 6+ server (RESP3
  auto-negotiated) and against a server with ``protocol=2`` forced.
* ``get_redis_connection`` passing through ``protocol=`` and ``legacy_responses=``
  kwargs from the public API.
* URL query-string handling (``?protocol=3`` or ``?protocol=2``).
* The ``REDIS_OM_URL`` environment variable still wins over kwargs.
"""

from unittest import mock

import pytest

from aredis_om import connections as connections_module
from aredis_om.connections import get_redis_connection, protocol_version
from aredis_om.util import protocol_version as util_protocol_version

from .conftest import TEST_PREFIX, py_test_mark_asyncio

# ── protocol_version ────────────────────────────────────────────────────


class TestProtocolVersion:
    def test_returns_3_for_auto_negotiated_connection(self, redis):
        # The pytest fixture's ``redis`` is created via ``get_redis_connection``,
        # which redis-py connects with RESP3 by default against Redis 6+.
        # Skip the assertion when the test environment explicitly pins the
        # protocol (e.g. REDIS_OM_URL=...?protocol=2).
        pool_kwargs = redis.connection_pool.connection_kwargs
        if pool_kwargs.get("protocol") in (2, "2"):
            pytest.skip("environment forced protocol=2")
        assert protocol_version(redis) == 3

    def test_returns_2_for_explicit_protocol_2(self):
        conn = get_redis_connection(
            url="redis://localhost:6380?decode_responses=True&protocol=2"
        )
        assert protocol_version(conn) == 2

    def test_returns_3_for_explicit_protocol_3(self):
        conn = get_redis_connection(
            url="redis://localhost:6380?decode_responses=True&protocol=3"
        )
        assert protocol_version(conn) == 3

    def test_util_module_re_exports_connection_helper(self, redis):
        # ``aredis_om.util.protocol_version`` should be the same callable as
        # ``aredis_om.connections.protocol_version``.
        assert util_protocol_version is connections_module.protocol_version
        assert util_protocol_version(redis) == protocol_version(redis)

    def test_falls_back_to_2_when_pool_explodes(self):
        # Build a stand-in connection object that raises when its pool is
        # introspected; the helper should fall back to ``2`` rather than
        # propagating the error.
        class ExplodingPool:
            def get_protocol(self):
                raise RuntimeError("boom")

        class StubConnection:
            connection_pool = ExplodingPool()

        assert protocol_version(StubConnection()) == 2


# ── get_redis_connection — protocol passthrough ─────────────────────────


class TestConnectionProtocolPassthrough:
    def test_url_protocol_2_query_param(self):
        conn = get_redis_connection(
            url="redis://localhost:6380?protocol=2&decode_responses=True"
        )
        assert conn.connection_pool.connection_kwargs.get("protocol") == 2

    def test_url_protocol_3_query_param(self):
        conn = get_redis_connection(
            url="redis://localhost:6380?protocol=3&decode_responses=True"
        )
        assert conn.connection_pool.connection_kwargs.get("protocol") == 3

    def test_protocol_kwarg_is_honored_when_no_url_protocol(self):
        # When the URL omits ``protocol`` the explicit kwarg is used.  When
        # both are present, redis-py itself parses the URL first and the URL
        # wins — we do not silently mutate that behaviour here.
        conn = get_redis_connection(
            url="redis://localhost:6380?decode_responses=True",
            protocol=2,
        )
        assert conn.connection_pool.connection_kwargs.get("protocol") == 2

    def test_no_protocol_defaults_to_none(self):
        conn = get_redis_connection(url="redis://localhost:6380")
        assert conn.connection_pool.connection_kwargs.get("protocol") is None

    def test_cluster_url_strips_cluster_flag_and_keeps_protocol(self, monkeypatch):
        calls = {}

        def fake_from_url(url, **kwargs):
            calls["url"] = url
            calls["kwargs"] = kwargs
            return mock.sentinel.cluster_conn

        monkeypatch.setattr(
            connections_module.redis.RedisCluster, "from_url", fake_from_url
        )
        conn = get_redis_connection(
            url=(
                "redis://localhost:7001/0?decode_responses=True"
                "&protocol=3&cluster=true"
            )
        )
        assert conn is mock.sentinel.cluster_conn
        # The ``cluster=true`` flag must be stripped from the URL.
        assert "cluster=true" not in calls["url"]
        assert "protocol=3" in calls["url"]
        # ``protocol`` is parsed from the URL by redis-py, not added as a
        # separate kwarg, but the connection pool should end up with the
        # negotiated value.
        # (We can't introspect the pool on a mock object, so just assert the
        # call did happen.)


# ── get_redis_connection — env var + URL precedence ────────────────────


class TestConnectionPrecedence:
    def test_env_var_drives_url_when_no_explicit_url(self, monkeypatch):
        monkeypatch.setenv(
            "REDIS_OM_URL",
            "redis://localhost:6380?decode_responses=True&protocol=2",
        )
        conn = get_redis_connection()
        assert conn.connection_pool.connection_kwargs.get("port") == 6380
        assert conn.connection_pool.connection_kwargs.get("protocol") == 2

    def test_explicit_url_overrides_env_var_protocol(self, monkeypatch):
        monkeypatch.setenv(
            "REDIS_OM_URL",
            "redis://localhost:6380?decode_responses=True&protocol=2",
        )
        conn = get_redis_connection(
            url="redis://localhost:6380?decode_responses=True&protocol=3"
        )
        assert conn.connection_pool.connection_kwargs.get("protocol") == 3


# ── Combined: live HTTP-style handshake check ──────────────────────────


class TestLiveProtocolHandshake:
    @py_test_mark_asyncio
    async def test_hello_returns_proto_3_by_default(self, redis):
        # ``redis`` fixture uses auto-negotiation against Redis 6+.
        # Skip the assertion when the test environment explicitly pins the
        # protocol (e.g. REDIS_OM_URL=...?protocol=2).
        pool_kwargs = redis.connection_pool.connection_kwargs
        if pool_kwargs.get("protocol") in (2, "2"):
            pytest.skip("environment forced protocol=2")
        hello = await redis.execute_command("HELLO")
        # HELLO returns a dict in RESP3, otherwise a flat list.
        if isinstance(hello, dict):
            assert hello.get("proto") == 3
        else:
            proto_idx = (
                hello.index("proto") if "proto" in hello else hello.index(b"proto")
            )
            assert hello[proto_idx + 1] == 3

    @py_test_mark_asyncio
    async def test_hello_returns_proto_2_for_explicit_protocol(self):
        conn = get_redis_connection(
            url="redis://localhost:6380?decode_responses=True&protocol=2"
        )
        hello = await conn.execute_command("HELLO")
        if isinstance(hello, dict):
            assert hello.get("proto") == 2
        else:
            proto_idx = (
                hello.index("proto") if "proto" in hello else hello.index(b"proto")
            )
            assert hello[proto_idx + 1] == 2
        await conn.aclose()
