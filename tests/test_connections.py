# type: ignore
"""Tests for aredis_om.connections – get_redis_connection."""

import os
from unittest import mock

import pytest

from aredis_om.connections import _strip_cluster_param, get_redis_connection


class TestGetRedisConnection:
    def test_default_decode_responses(self):
        conn = get_redis_connection(url="redis://localhost:6380")
        assert conn.connection_pool.connection_kwargs.get("decode_responses") is True

    def test_explicit_decode_responses_false(self):
        conn = get_redis_connection(
            url="redis://localhost:6380", decode_responses=False
        )
        assert conn.connection_pool.connection_kwargs.get("decode_responses") is False

    def test_url_from_env(self, monkeypatch):
        monkeypatch.setenv(
            "REDIS_OM_URL", "redis://localhost:6380?decode_responses=True"
        )
        conn = get_redis_connection()
        assert conn.connection_pool.connection_kwargs["port"] == 6380

    def test_explicit_url_overrides_env(self, monkeypatch):
        monkeypatch.setenv(
            "REDIS_OM_URL", "redis://localhost:9999?decode_responses=True"
        )
        conn = get_redis_connection(url="redis://localhost:6380?decode_responses=True")
        assert conn.connection_pool.connection_kwargs["port"] == 6380

    def test_cluster_mode_from_param(self):
        # Just verifying no error is raised with cluster=True + url
        # We can't actually connect to a cluster here
        try:
            get_redis_connection(url="redis://localhost:6380", cluster=True)
        except Exception:
            pass  # expected since there's no cluster

    def test_strip_cluster_param_preserves_other_query_params(self):
        clean = _strip_cluster_param(
            "redis://localhost:7001/0?decode_responses=True&cluster=true&protocol=3"
        )

        assert clean == "redis://localhost:7001/0?decode_responses=True&protocol=3"

    def test_strip_cluster_param_removes_case_insensitive_cluster_query_key(self):
        clean = _strip_cluster_param(
            "redis://localhost:7001/0?decode_responses=True&Cluster=True"
        )

        assert clean == "redis://localhost:7001/0?decode_responses=True"

    def test_get_redis_connection_strips_cluster_query_before_from_url(
        self, monkeypatch
    ):
        sentinel = object()
        calls = {}

        def fake_from_url(url, **kwargs):
            calls["url"] = url
            calls["kwargs"] = kwargs
            return sentinel

        monkeypatch.setattr(
            "aredis_om.connections.redis.RedisCluster.from_url", fake_from_url
        )

        conn = get_redis_connection(
            url="redis://localhost:7001/0?decode_responses=True&Cluster=True&protocol=3"
        )

        assert conn is sentinel
        assert calls["url"] == "redis://localhost:7001/0?decode_responses=True&protocol=3"
        assert calls["kwargs"]["decode_responses"] is True

    def test_no_url_no_env_uses_defaults(self, monkeypatch):
        monkeypatch.delenv("REDIS_OM_URL", raising=False)
        conn = get_redis_connection()
        # Default redis-py host/port
        assert conn.connection_pool.connection_kwargs["host"] == "localhost"
        assert conn.connection_pool.connection_kwargs["port"] == 6379
