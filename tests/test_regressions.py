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
from aredis_om.model import model as model_module
from aredis_om.model.cli import migrate as migrate_cli_module
from aredis_om.model.migrations import migrator as migrator_module
from aredis_om.model.model import (
    Expression,
    ExpressionProxy,
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


def test_model_db_is_resolved_lazily(monkeypatch):
    calls = {"count": 0}
    sentinel = object()

    def fake_get_redis_connection():
        calls["count"] += 1
        return sentinel

    monkeypatch.setattr(model_module, "get_redis_connection", fake_get_redis_connection)

    class LazyModel(JsonModel, abc.ABC):
        name: str

    assert calls["count"] == 0
    assert LazyModel.db() is sentinel
    assert LazyModel.db() is sentinel
    assert calls["count"] == 1


def test_model_meta_database_can_be_assigned_after_class_creation(monkeypatch):
    def should_stay_lazy():
        raise AssertionError("should stay lazy")

    monkeypatch.setattr(
        model_module,
        "get_redis_connection",
        should_stay_lazy,
    )
    runtime_connection = object()

    class RuntimeConfiguredModel(HashModel, abc.ABC):
        name: str

    RuntimeConfiguredModel.Meta.database = runtime_connection

    assert RuntimeConfiguredModel.db() is runtime_connection


def test_model_meta_database_callable_is_cached(monkeypatch):
    def should_use_callable():
        raise AssertionError("callable should be used")

    monkeypatch.setattr(
        model_module,
        "get_redis_connection",
        should_use_callable,
    )
    calls = {"count": 0}
    sentinel = object()

    def connection_factory():
        calls["count"] += 1
        return sentinel

    class CallableConfiguredModel(JsonModel, abc.ABC):
        name: str

        class Meta:
            database = connection_factory

    assert CallableConfiguredModel.db() is sentinel
    assert CallableConfiguredModel.db() is sentinel
    assert calls["count"] == 1


@py_test_mark_asyncio
async def test_async_checks_cache_results_per_connection():
    clear_command_cache()
    conn = FakeAsyncConn()

    assert await has_redis_json(conn) is True
    assert await has_redis_json(conn) is True
    assert await has_redisearch(conn) is True

    # ``has_redisearch`` probes ``ft.search`` directly (it no longer shortcuts
    # to ``True`` when RedisJSON is detected). The second ``has_redis_json``
    # call hits the cache and is not re-issued.
    assert [call[2] for call in conn.calls] == ["json.set", "ft.search"]


def test_not_query_no_longer_returns_placeholder():
    """Not(...) renders a real RediSearch query string, not the old placeholder dict."""
    from aredis_om import FindQuery

    expr_one = mock.Mock(spec=Expression)
    expr_two = mock.Mock(spec=Expression)

    with mock.patch.object(FindQuery, "resolve_redisearch_query") as mocked:
        mocked.side_effect = ["@price:[-inf 10]", "@category:{Sweets}"]
        result = Not(expr_one, expr_two).query

    assert result == "-(@price:[-inf 10]) -(@category:{Sweets})"


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


@py_test_mark_asyncio
async def test_cluster_create_index_targets_one_random_node(monkeypatch):
    calls = []
    writes = []

    class MissingIndexStub:
        async def info(self):
            raise migrator_module.redis.ResponseError("missing index")

    class FakeClusterConn:
        def ft(self, _index_name):
            return MissingIndexStub()

        async def execute_command(self, *args, **kwargs):
            calls.append((args, kwargs))
            return "OK"

        async def set(self, key, value):
            writes.append((key, value))
            return True

    conn = FakeClusterConn()

    await migrator_module._create_index_cluster(
        conn,
        "test-index",
        "ON HASH PREFIX 1 test: SCHEMA name TAG",
        "schema-hash",
    )

    assert calls == [
        (
            (
                "ft.create",
                "test-index",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "test:",
                "SCHEMA",
                "name",
                "TAG",
            ),
            {"target_nodes": migrator_module.redis.RedisCluster.RANDOM},
        )
    ]
    assert writes == [("test-index:hash", "schema-hash")]


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@pytest.mark.xdist_group(name="migrator")
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


@py_test_mark_asyncio
async def test_find_query_warns_about_indexing_failures(monkeypatch, caplog):
    class FakeIndex:
        async def info(self):
            return {
                "hash_indexing_failures": 2,
                "Index Errors": {
                    "last indexing error": "Invalid numeric value",
                    "last indexing error key": "Product:broken",
                },
            }

    class FakeConnection:
        def ft(self, _index_name):
            return FakeIndex()

        async def execute_command(self, *_args):
            return [0]

    fake_connection = FakeConnection()

    async def fake_has_redisearch(_conn):
        return True

    monkeypatch.setattr(model_module, "has_redisearch", fake_has_redisearch)

    class Product(JsonModel, abc.ABC):
        name: str = Field(index=True)

        class Meta:
            database = fake_connection

    health = await Product.check_index_health()

    with caplog.at_level("WARNING"):
        assert await Product.find(Product.name == "ball").all() == []

    assert health["indexing_failures"] == 2
    assert health["last_indexing_error"] == "Invalid numeric value"
    assert health["last_indexing_error_key"] == "Product:broken"
    assert "RediSearch index" in caplog.text
    assert "indexing failures" in caplog.text
    assert "Invalid numeric value" in caplog.text
    assert "Product:broken" in caplog.text


def test_embedded_model_pk_is_not_expression_proxy():
    """Metaclass must NOT replace the pk class attribute with ExpressionProxy on
    embedded models.

    When Pydantic v2 validates a nested embedded sub-document it may use the
    class-level default for any field absent from the input dict.  If that
    default is an ExpressionProxy the validator raises a ValidationError because
    ExpressionProxy is not Optional[str].  The fix in ModelMeta.__new__ skips
    the ExpressionProxy setup for the pk field on embedded models entirely.
    """

    class Address(EmbeddedJsonModel):
        street: str = Field(index=True)

    # The class-level attribute must NOT be an ExpressionProxy.
    assert not isinstance(Address.pk, ExpressionProxy), (
        "pk class attribute on an EmbeddedJsonModel must not be an ExpressionProxy; "
        "it would cause Pydantic v2 to raise a ValidationError when the field is "
        "absent from the input dict."
    )


def test_embedded_model_instantiation_without_pk():
    """Directly instantiating an EmbeddedJsonModel must not raise ValidationError."""

    class Profile(EmbeddedJsonModel):
        bio: str

    p = Profile(bio="hello")
    assert p.pk is None
    assert p.bio == "hello"


def test_embedded_model_instantiation_with_stale_pk_in_dict():
    """Loading stale Redis data that includes a pk entry must not crash.

    Old records written before the embedded-pk fix may carry a ``pk`` key.
    The _strip_pk model_validator (defence-in-depth) must silently drop it.
    """
    from typing import List

    class Tag(EmbeddedJsonModel):
        name: str

    class Container(JsonModel):
        tags: List[Tag]

    # Simulate stale data: pk present with an invalid type (empty list)
    stale = {"name": "python", "pk": []}
    tag = Tag.model_validate(stale)
    assert tag.pk is None
    assert tag.name == "python"


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@pytest.mark.xdist_group(name="migrator")
@py_test_mark_asyncio
async def test_migrator_dry_run_does_not_apply_migrations(key_prefix, redis, capsys):
    """``Migrator.run(dry_run=True)`` prints the plan without executing it."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class DryRunProduct(BaseJsonModel):
        name: str = Field(index=True)

    migrator = Migrator(conn=redis)
    await migrator.detect_migrations()
    assert len(migrator.migrations) >= 1

    await migrator.run(dry_run=True)

    captured = capsys.readouterr()
    assert "Dry run" in captured.out
    assert "CREATE" in captured.out

    # Index must not actually exist after a dry run.
    index_name = DryRunProduct.Meta.index_name
    try:
        await redis.ft(index_name).info()
        pytest.fail("Index should not exist after dry run")
    except migrator_module.redis.ResponseError:
        pass


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@pytest.mark.xdist_group(name="migrator")
@py_test_mark_asyncio
async def test_migrator_records_history_in_redis(key_prefix, redis):
    """``Migrator.run()`` appends a JSON record per applied migration."""
    import json

    # Use a per-test history key so concurrent migrations don't pollute
    # the assertion (the default key is shared across all migrators).
    history_key = f"{key_prefix}:migration_history"

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class HistoryProduct(BaseJsonModel):
        name: str = Field(index=True)

    migrator = Migrator(conn=redis, history_key=history_key)
    await migrator.run()

    raw = await redis.lrange(history_key, 0, -1)
    assert raw, "expected at least one history entry"
    # Each entry should be valid JSON with the documented fields.
    record = json.loads(raw[-1])
    assert record["action"] == "CREATE"
    assert record["index"] == HistoryProduct.Meta.index_name
    assert "timestamp" in record
    assert "hash" in record


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@pytest.mark.xdist_group(name="migrator")
@py_test_mark_asyncio
async def test_migrator_record_history_can_be_disabled(key_prefix, redis):
    """``record_history=False`` skips writing to the history list."""
    # Use a per-test history key so concurrent migrations don't pollute
    # the assertion (the default key is shared across all migrators).
    history_key = f"{key_prefix}:migration_history"

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class NoHistoryProduct(BaseJsonModel):
        name: str = Field(index=True)

    migrator = Migrator(conn=redis, history_key=history_key)
    await migrator.run(record_history=False)

    after = await redis.llen(history_key)
    assert after == 0, "history must not grow when record_history=False"


def test_index_migration_summary_and_history_record_shape():
    """``IndexMigration.summary()`` and ``history_record()`` format."""
    from aredis_om.model.migrations.migrator import IndexMigration, MigrationAction

    migration = IndexMigration(
        model_name="m",
        index_name="idx",
        schema="SCHEMA",
        hash="abc",
        action=MigrationAction.CREATE,
        conn=None,
        previous_hash="prev",
    )
    summary = migration.summary()
    assert "CREATE" in summary
    assert "idx" in summary
    assert "abc" in summary
    assert "prev" in summary

    record = migration.history_record()
    assert record["action"] == "CREATE"
    assert record["model"] == "m"
    assert record["index"] == "idx"
    assert record["hash"] == "abc"
    assert record["previous_hash"] == "prev"
    assert "timestamp" in record
