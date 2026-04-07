# type: ignore

import abc
import asyncio
import datetime
from unittest import mock

import pytest
from click.testing import CliRunner

from aredis_om import EmbeddedJsonModel, Field, HashModel, JsonModel, Migrator
from aredis_om.checks import clear_command_cache, has_redis_json, has_redisearch
from aredis_om.connections import get_redis_connection
from aredis_om.model.cli import migrate as migrate_cli_module
from aredis_om.model.model import (
    Expression,
    convert_datetime_to_timestamp,
    convert_timestamp_to_datetime,
)
from aredis_om.model.query_resolver import Not
from tests._sync_redis import has_redis_json as sync_has_redis_json
from tests._sync_redis import has_redisearch as sync_has_redisearch

from .conftest import py_test_mark_asyncio


HAS_REDIS_JSON = sync_has_redis_json()
HAS_REDISEARCH = sync_has_redisearch()


class FakeAsyncConn:
    def __init__(self):
        self.calls = []

    async def execute_command(self, *args):
        self.calls.append(args)
        return [1]


def test_get_redis_connection_uses_latest_env_value(monkeypatch):
    monkeypatch.setenv("REDIS_OM_URL", "redis://localhost:6380/0?decode_responses=True")
    first = get_redis_connection()

    monkeypatch.setenv("REDIS_OM_URL", "redis://localhost:6381/0?decode_responses=True")
    second = get_redis_connection()

    assert first.connection_pool.connection_kwargs["port"] == 6380
    assert second.connection_pool.connection_kwargs["port"] == 6381


@py_test_mark_asyncio
async def test_async_checks_cache_results_per_connection():
    clear_command_cache()
    conn = FakeAsyncConn()

    assert await has_redis_json(conn) is True
    assert await has_redis_json(conn) is True
    assert await has_redisearch(conn) is True

    assert [call[2] for call in conn.calls] == ["json.set"]


def test_not_query_no_longer_returns_placeholder():
    expr_one = mock.Mock(spec=Expression)
    expr_two = mock.Mock(spec=Expression)

    assert Not(expr_one, expr_two).query == {"-": [expr_one, expr_two]}


def test_expression_proxy_returns_isolated_parent_chains():
    class Player(EmbeddedJsonModel):
        username: str = Field(index=True)

    class Match(JsonModel):
        player1: Player
        player2: Player

    player1_username = Match.player1.username
    player2_username = Match.player2.username

    assert [parent[0] for parent in player1_username.parents] == ["player1"]
    assert [parent[0] for parent in player2_username.parents] == ["player2"]


def test_datetime_conversion_uses_utc_stable_round_trip():
    naive_dt = datetime.datetime(2024, 1, 2, 3, 4, 5)
    naive_date = datetime.date(2024, 1, 2)

    encoded = convert_datetime_to_timestamp(
        {"created_on": naive_dt, "join_date": naive_date}
    )
    model_fields = {
        "created_on": mock.Mock(annotation=datetime.datetime),
        "join_date": mock.Mock(annotation=datetime.date),
    }

    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert (
        encoded["created_on"]
        == datetime.datetime(
            2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc
        ).timestamp()
    )
    assert decoded["created_on"] == naive_dt
    assert decoded["join_date"] == naive_date


def test_async_cli_migrate_runs_coroutines(monkeypatch):
    state = {"detected": 0, "ran": 0}

    class FakeMigrator:
        def __init__(self, module=None, conn=None):
            self.module = module
            self.conn = conn
            self.migrations = []

        async def detect_migrations(self):
            state["detected"] += 1
            self.migrations = ["migration-1"]

        async def run(self):
            state["ran"] += 1

    monkeypatch.setattr(migrate_cli_module, "Migrator", FakeMigrator)

    def safe_run(coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(migrate_cli_module.asyncio, "run", safe_run)
    runner = CliRunner()
    result = runner.invoke(
        migrate_cli_module.migrate, ["--module", "tests"], input="y\n"
    )

    assert result.exit_code == 0
    assert "Pending migrations:" in result.output
    assert state == {"detected": 1, "ran": 1}


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_aggregate_ct_handles_decode_response_strings(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Product(BaseJsonModel, index=True):
        name: str = Field(index=True)
        category: str = Field(index=True)

    await Migrator(conn=redis).run()

    await Product(name="ball", category="toy").save()
    await Product(name="bear", category="toy").save()
    await Product(name="chair", category="furniture").save()

    assert await Product.find(Product.category == "toy").aggregate_ct() == 2
