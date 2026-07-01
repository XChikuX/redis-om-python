# type: ignore
"""Tests for the optional fastapi-redis-sdk integration helper.

These tests deliberately do NOT require ``fastapi-redis-sdk`` to be
installed.  They exercise the two helper functions with lightweight fakes
so the bridge logic is covered without pulling in the third-party package.
"""

import sys
import types
from unittest import mock

import pytest

from aredis_om.integrations import fastapi_redis_sdk as bridge


# ---------------------------------------------------------------------------
# Fakes for the fastapi-redis-sdk machinery
# ---------------------------------------------------------------------------


class _FakeAsyncClient:
    """Stand-in for ``redis.asyncio.Redis`` / ``RedisCluster``."""


class _FakePoolState:
    """Mimics fastapi-redis-sdk's ``_PoolState`` (see deps.py)."""

    def __init__(self, client=None):
        self._client = client or _FakeAsyncClient()

    def get_async_client(self):
        return self._client


class _FakeApp:
    """Mimics ``fastapi.FastAPI`` just enough for ``app.state`` access."""

    def __init__(self, pool_state=None):
        # ``fastapi.FastAPI`` exposes a ``state`` attribute holding arbitrary
        # attributes; the bridge reads ``app.state._redis``.
        self.state = types.SimpleNamespace(_redis=pool_state)


def _install_fake_redis_fastapi(monkeypatch, settings=None):
    """Register a fake ``redis_fastapi`` module for the duration of a test."""

    fake_pkg = types.ModuleType("redis_fastapi")
    fake_deps = types.ModuleType("redis_fastapi.deps")

    def _get_pool_state(app):
        pool_state = getattr(app.state, "_redis", None)
        if pool_state is None:
            pool_state = _FakePoolState()
            app.state._redis = pool_state
        return pool_state

    fake_deps._get_pool_state = _get_pool_state
    fake_pkg.deps = fake_deps

    fake_settings_obj = settings or types.SimpleNamespace()

    def _get_settings():
        return fake_settings_obj

    fake_pkg.get_settings = _get_settings
    # Expose the fake settings class type too for completeness.
    fake_pkg.RedisSettings = object

    # Connection kwargs helper used by database_from_fastapi_settings.
    if not hasattr(fake_settings_obj, "connection_kwargs"):

        def _connection_kwargs(self=fake_settings_obj):
            return {"url": "redis://localhost:6379/0"}

        fake_settings_obj.connection_kwargs = _connection_kwargs

    monkeypatch.setitem(sys.modules, "redis_fastapi", fake_pkg)
    monkeypatch.setitem(sys.modules, "redis_fastapi.deps", fake_deps)
    return fake_pkg


# ---------------------------------------------------------------------------
# database_from_app
# ---------------------------------------------------------------------------


class TestDatabaseFromApp:
    def test_returns_callable(self):
        app = _FakeApp(pool_state=_FakePoolState())
        provider = bridge.database_from_app(app)
        assert callable(provider)

    def test_resolves_to_pool_client(self):
        sentinel = _FakeAsyncClient()
        app = _FakeApp(pool_state=_FakePoolState(client=sentinel))
        provider = bridge.database_from_app(app)
        assert provider() is sentinel

    def test_raises_when_pool_state_missing(self):
        # ``app.state._redis`` is None and no fake redis_fastapi registered,
        # so the bridge should raise a RuntimeError on resolution.
        app = _FakeApp(pool_state=None)
        provider = bridge.database_from_app(app)
        with pytest.raises(RuntimeError):
            provider()

    def test_raises_when_pool_state_has_no_get_client(self):
        app = _FakeApp(pool_state=types.SimpleNamespace())  # no get_async_client
        provider = bridge.database_from_app(app)
        with pytest.raises(RuntimeError, match="get_async_client"):
            provider()

    def test_uses_redis_fastapi_get_pool_state_when_state_missing(self, monkeypatch):
        _install_fake_redis_fastapi(monkeypatch)
        app = _FakeApp(pool_state=None)  # _get_pool_state will lazily create one
        provider = bridge.database_from_app(app)
        client = provider()
        assert isinstance(client, _FakeAsyncClient)


# ---------------------------------------------------------------------------
# database_from_fastapi_settings
# ---------------------------------------------------------------------------


class TestDatabaseFromFastapiSettings:
    def test_returns_callable(self, monkeypatch):
        _install_fake_redis_fastapi(monkeypatch)
        provider = bridge.database_from_fastapi_settings()
        assert callable(provider)

    def test_invokes_get_redis_connection_with_settings_kwargs(self, monkeypatch):
        settings = types.SimpleNamespace()

        def _connection_kwargs():
            return {"url": "redis://example:6380/2", "driver_info": object()}

        settings.connection_kwargs = _connection_kwargs
        _install_fake_redis_fastapi(monkeypatch, settings=settings)

        captured = {}

        def _fake_get_redis_connection(**kwargs):
            captured.update(kwargs)
            return "fake-conn"

        monkeypatch.setattr(
            "aredis_om.connections.get_redis_connection",
            _fake_get_redis_connection,
        )

        provider = bridge.database_from_fastapi_settings()
        assert provider() == "fake-conn"
        # driver_info must be stripped — get_redis_connection does not accept it.
        assert "driver_info" not in captured
        assert captured["url"] == "redis://example:6380/2"

    def test_raises_when_fastapi_redis_sdk_missing(self, monkeypatch):
        # Ensure import of the real/fake package fails.
        monkeypatch.setitem(sys.modules, "redis_fastapi", None)
        provider = bridge.database_from_fastapi_settings()
        with pytest.raises(RuntimeError, match="fastapi-redis-sdk is not installed"):
            provider()
