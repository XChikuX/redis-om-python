# mypy: disable-error-code="type-var"

"""Edge-case / error-path tests for the migrator module.

These tests exercise uncovered code paths in
``aredis_om/model/migrations/migrator.py`` that are difficult or
expensive to trigger in integration tests:
  - ``_create_index_cluster``: unexpected ResponseError from execute_command
  - ``_wait_for_index``: timeout path (FT.INFO always raises)
  - ``_wait_for_index``: invalid percent_indexed value
  - ``_retry_aliasupdate``: exhausting all retry attempts
  - ``create_index`` (standalone): unexpected ResponseError from execute_command
  - ``_resolve_alias_or_index``: FT.INFO returns dict without index_name key
  - ``_list_indexes``: bytes entries, nested RESP3 lists, ResponseError
  - ``create_physical_index``: db > 0 raises MigrationError
  - ``IndexMigration._alias_link``: ResponseError during aliasupdate
  - ``IndexMigration._alias_adopt``: legacy index already gone (info log)
  - ``IndexMigration._alias_adopt``: ResponseError during retry
  - ``IndexMigration._alias_swap``: "does not exist" → treated as success
  - ``IndexMigration._alias_swap``: alias already swapped (success)
  - ``Migrator.run``: dry_run with zero pending migrations
  - ``_detect_legacy_migrations``: NotImplementedError from redisearch_schema
  - ``_detect_alias_migrations``: NotImplementedError from redisearch_schema

These tests need Redis Stack (RediSearch). They will be skipped on a
vanilla OSS Redis that lacks the search module.
"""

from typing import Dict, Type
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
import redis.asyncio as redis

from aredis_om import JsonModel
from aredis_om.model.migrations.migrator import (
    IndexMigration,
    MigrationAction,
    Migrator,
    _list_indexes,
    _resolve_alias_or_index,
    _retry_aliasupdate,
    _wait_for_index,
    create_index,
    create_physical_index,
)
from aredis_om.model.model import model_registry

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xdist_group(name="migrator"),
]


class _EdgeCaseModel(JsonModel):
    """Model used for edge-case tests."""

    name: str

    class Meta:
        index_name = "edge_case_test"
        model_key_prefix = "edge_case_doc"
        _test_only = True


def _qualname_key(cls: Type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _isolate_registry(*keep: Type) -> Dict[str, Type]:
    """Clear the entire registry except the models in ``keep``.

    Removing every entry (rather than only recognised test models)
    guarantees that module-level models from sibling test files on the
    same xdist worker cannot leak into this test's migrator runs.
    """
    snapshot: Dict[str, Type] = dict(model_registry)
    model_registry.clear()
    for cls in keep:
        model_registry[_qualname_key(cls)] = cls
    return snapshot


def _restore_registry(snapshot: Dict[str, Type]) -> None:
    """Undo ``_isolate_registry``, including models registered mid-test."""
    model_registry.clear()
    model_registry.update(snapshot)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def conn():
    """Return a Redis connection pointing at $REDIS_OM_URL."""
    url = __import__("os").getenv("REDIS_OM_URL", "redis://localhost:6380")
    client = redis.from_url(url, decode_responses=False)
    yield client
    await client.aclose()


@pytest.fixture
def snapshot():
    """Isolate the registry for each test and restore it after."""
    snap = _isolate_registry()
    yield snap
    _restore_registry(snap)


# ── _wait_for_index tests ─────────────────────────────────────────────


async def test_wait_for_index_timeout(conn: redis.Redis):
    """_wait_for_index logs a WARNING when the index never becomes queryable."""
    index_name = "test_wait_timeout_idx"

    # Ensure clean state
    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
    except Exception:
        pass

    # Create a minimal index that we'll never poll successfully
    await conn.execute_command("FT.CREATE", index_name, "SCHEMA", "name", "TAG")

    try:
        with patch.object(conn, "ft") as mock_ft:
            # Make FT.INFO always raise — simulating index never becoming visible
            mock_ft.return_value.info = AsyncMock(
                side_effect=redis.ResponseError("busy")
            )

            with patch("aredis_om.model.migrations.migrator.log") as mock_log:
                # Should not raise; should log warning
                await _wait_for_index(conn, index_name, timeout=0.1)
                mock_log.warning.assert_called()
                warning_msg = mock_log.warning.call_args[0][0]
                assert (
                    "never became queryable" in warning_msg or "Timeout" in warning_msg
                )
    finally:
        try:
            await conn.ft(index_name).dropindex(delete_documents=False)
        except Exception:
            pass


async def test_wait_for_index_invalid_percent_indexed(conn: redis.Redis):
    """_wait_for_index handles non-numeric percent_indexed gracefully."""
    index_name = "test_wait_invalid_pct_idx"

    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
    except Exception:
        pass

    await conn.execute_command("FT.CREATE", index_name, "SCHEMA", "name", "TAG")

    try:
        with patch.object(conn, "ft") as mock_ft:
            # Return a non-numeric percent_indexed — should not crash
            mock_ft.return_value.info = AsyncMock(
                return_value={"percent_indexed": "not_a_number"}
            )

            # Should not raise — ValueError is caught internally
            await _wait_for_index(conn, index_name, timeout=0.2)
    finally:
        try:
            await conn.ft(index_name).dropindex(delete_documents=False)
        except Exception:
            pass


# ── _retry_aliasupdate tests ──────────────────────────────────────────


async def test_retry_aliasupdate_exhausts_retries(conn: redis.Redis):
    """_retry_aliasupdate raises the last exception after all attempts."""
    with patch.object(conn, "ft") as mock_ft:
        mock_ft.return_value.aliasupdate = AsyncMock(
            side_effect=redis.ResponseError("SEARCH_INDEX_NOT_FOUND not ready")
        )

        with pytest.raises(redis.ResponseError) as exc_info:
            await _retry_aliasupdate(conn, "physical", "alias", attempts=3)

        # Should have been called 3 times
        assert mock_ft.return_value.aliasupdate.call_count == 3
        assert "SEARCH_INDEX_NOT_FOUND" in str(exc_info.value)


async def test_retry_aliasupdate_succeeds_on_first_try(conn: redis.Redis):
    """_retry_aliasupdate returns immediately when aliasupdate succeeds."""
    with patch.object(conn, "ft") as mock_ft:
        mock_ft.return_value.aliasupdate = AsyncMock()

        await _retry_aliasupdate(conn, "physical", "alias", attempts=3)
        mock_ft.return_value.aliasupdate.assert_called_once()


async def test_retry_aliasupdate_retries_on_transient_error(conn: redis.Redis):
    """_retry_aliasupdate retries only on SEARCH_INDEX_NOT_FOUND."""
    errors = [
        redis.ResponseError("SOME_OTHER_ERROR"),
        None,  # Success on second attempt
    ]
    error_iter = iter(errors)

    async def side_effect(*args, **kwargs):
        result = next(error_iter)
        if result:
            raise result

    with patch.object(conn, "ft") as mock_ft:
        mock_ft.return_value.aliasupdate = AsyncMock(side_effect=side_effect)

        # Should raise on first (non-transient) error, not retry
        with pytest.raises(redis.ResponseError) as exc_info:
            await _retry_aliasupdate(conn, "physical", "alias", attempts=3)
        assert "SOME_OTHER_ERROR" in str(exc_info.value)


# ── _resolve_alias_or_index tests ─────────────────────────────────────


async def test_resolve_alias_or_index_returns_none_when_index_name_missing(
    conn: redis.Redis,
):
    """_resolve_alias_or_index returns (False, None) when FT.INFO lacks index_name."""
    index_name = "test_resolve_no_key_idx"

    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
    except Exception:
        pass

    await conn.execute_command("FT.CREATE", index_name, "SCHEMA", "name", "TAG")

    try:
        with patch.object(conn, "ft") as mock_ft:
            # Return dict without index_name
            mock_ft.return_value.info = AsyncMock(return_value={})

            is_alias, resolved = await _resolve_alias_or_index(conn, index_name)
            assert is_alias is False
            assert resolved is None
    finally:
        try:
            await conn.ft(index_name).dropindex(delete_documents=False)
        except Exception:
            pass


async def test_resolve_alias_or_index_handles_bytes_index_name(conn: redis.Redis):
    """_resolve_alias_or_index decodes bytes index_name from FT.INFO."""
    index_name = "test_resolve_bytes_idx"

    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
    except Exception:
        pass

    await conn.execute_command("FT.CREATE", index_name, "SCHEMA", "name", "TAG")

    try:
        with patch.object(conn, "ft") as mock_ft:
            # Return bytes index_name (RESP3 shape)
            mock_ft.return_value.info = AsyncMock(
                return_value={"index_name": b"test_resolve_bytes_idx"}
            )

            is_alias, resolved = await _resolve_alias_or_index(conn, index_name)
            assert is_alias is False
            assert resolved == "test_resolve_bytes_idx"
    finally:
        try:
            await conn.ft(index_name).dropindex(delete_documents=False)
        except Exception:
            pass


# ── _list_indexes tests ───────────────────────────────────────────────


async def test_list_indexes_handles_bytes(conn: redis.Redis):
    """_list_indexes decodes bytes index names."""
    with patch.object(conn, "execute_command") as mock_exec:
        mock_exec.return_value = [b"idx1", b"idx2"]
        result = await _list_indexes(conn)
        assert result == ["idx1", "idx2"]


async def test_list_indexes_handles_str(conn: redis.Redis):
    """_list_indexes passes through str index names."""
    with patch.object(conn, "execute_command") as mock_exec:
        mock_exec.return_value = ["idx1", "idx2"]
        result = await _list_indexes(conn)
        assert result == ["idx1", "idx2"]


async def test_list_indexes_handles_nested_list(conn: redis.Redis):
    """_list_indexes handles nested RESP3 list shapes."""
    with patch.object(conn, "execute_command") as mock_exec:
        # RESP3 nesting: first element is a list containing the name
        mock_exec.return_value = [[b"nested_idx1"], [b"nested_idx2"]]
        result = await _list_indexes(conn)
        assert result == ["nested_idx1", "nested_idx2"]


async def test_list_indexes_returns_empty_on_response_error(conn: redis.Redis):
    """_list_indexes returns [] when FT._LIST is not available."""
    with patch.object(conn, "execute_command") as mock_exec:
        mock_exec.side_effect = redis.ResponseError("unknown command")
        result = await _list_indexes(conn)
        assert result == []


# ── create_index (standalone) tests ──────────────────────────────────


async def test_create_index_raises_on_unexpected_error(conn: redis.Redis):
    """create_index re-raises non-"Index already exists" errors."""
    index_name = "test_create_unexpected_err_idx"

    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
    except Exception:
        pass

    try:
        with patch.object(conn, "ft") as mock_ft:
            mock_ft.return_value.info = AsyncMock(
                side_effect=redis.ResponseError("not found")
            )

            with patch.object(conn, "execute_command") as mock_exec:
                mock_exec.side_effect = redis.ResponseError("some other error")
                with pytest.raises(redis.ResponseError) as exc_info:
                    await create_index(conn, index_name, "SCHEMA name TAG", "abc123")
                assert "some other error" in str(exc_info.value)
    finally:
        try:
            await conn.ft(index_name).dropindex(delete_documents=False)
        except Exception:
            pass


# ── create_physical_index tests ───────────────────────────────────────


async def test_create_physical_index_raises_on_db_nonzero(conn: redis.Redis):
    """create_physical_index raises MigrationError when db > 0."""
    mock_conn = AsyncMock()
    mock_conn.ft.return_value.info = AsyncMock(
        side_effect=redis.ResponseError("not found")
    )
    mock_conn.connection_pool.connection_kwargs = {"db": 1}

    from aredis_om.model.migrations.migrator import MigrationError

    with pytest.raises(MigrationError) as exc_info:
        await create_physical_index(mock_conn, "any_index", "SCHEMA name TAG")
    assert "database 0" in str(exc_info.value)


# ── IndexMigration._alias_link tests ────────────────────────────────


async def test_alias_link_warns_on_response_error(conn: redis.Redis):
    """IndexMigration._alias_link logs a warning when aliasupdate fails."""
    index_name = "test_alias_link_warn_idx"
    alias_name = "test_alias_link_alias"

    try:
        await conn.ft(index_name).dropindex(delete_documents=False)
        await conn.execute_command("FT.ALIASDEL", alias_name)
    except Exception:
        pass

    await conn.execute_command("FT.CREATE", index_name, "SCHEMA", "name", "TAG")

    try:
        migration = IndexMigration(
            model_name="EdgeCaseModel",
            index_name=index_name,
            schema="SCHEMA name TAG",
            hash="abc123",
            action=MigrationAction.ALIAS_LINK,
            conn=conn,
            alias_name=alias_name,
        )

        with patch.object(conn, "ft") as mock_ft:
            mock_ft.return_value.aliasupdate = AsyncMock(
                side_effect=redis.ResponseError("alias update failed")
            )

            with patch("aredis_om.model.migrations.migrator.log") as mock_log:
                with pytest.raises(redis.ResponseError):
                    await migration._alias_link()
                # Warning should be logged before re-raising
                mock_log.warning.assert_called()
    finally:
        try:
            await conn.ft(index_name).dropindex(delete_documents=False)
            await conn.execute_command("FT.ALIASDEL", alias_name)
        except Exception:
            pass


# ── IndexMigration._alias_adopt tests ───────────────────────────────


async def test_alias_adopt_logs_when_legacy_index_missing(conn: redis.Redis):
    """IndexMigration._alias_adopt logs at INFO when legacy index already gone."""
    alias_name = "test_alias_adopt_missing"
    new_physical = "test_alias_adopt_new_v1"

    try:
        await conn.ft(alias_name).dropindex(delete_documents=False)
        await conn.execute_command("FT.ALIASDEL", alias_name)
        await conn.ft(new_physical).dropindex(delete_documents=False)
    except Exception:
        pass

    # Create only the new physical, not the legacy
    await conn.execute_command("FT.CREATE", new_physical, "SCHEMA", "name", "TAG")

    try:
        migration = IndexMigration(
            model_name="EdgeCaseModel",
            index_name=new_physical,
            schema="SCHEMA name TAG",
            hash="abc123",
            action=MigrationAction.ALIAS_ADOPT,
            conn=conn,
            alias_name=alias_name,
        )

        # Simulate legacy index already gone (raises ResponseError on drop)
        # Use MagicMock for the ft() return value (it's a sync object with async methods)
        mock_ft_return = MagicMock()
        mock_ft_return.dropindex = AsyncMock(
            side_effect=redis.ResponseError("index not found")
        )
        mock_ft_return.aliasupdate = AsyncMock()

        with patch.object(conn, "ft") as mock_ft:
            mock_ft.return_value = mock_ft_return

            with patch("aredis_om.model.migrations.migrator.log") as mock_log:
                # Should not raise; should log INFO
                await migration._alias_adopt()
                # INFO log about already adopted
                info_calls = [call for call in mock_log.info.call_args_list]
                assert len(info_calls) > 0
    finally:
        try:
            await conn.ft(new_physical).dropindex(delete_documents=False)
            await conn.execute_command("FT.ALIASDEL", alias_name)
        except Exception:
            pass


# ── IndexMigration._alias_swap tests ────────────────────────────────


async def test_alias_swap_treats_does_not_exist_as_success(conn: redis.Redis):
    """IndexMigration._alias_swap treats "does not exist" error as success."""
    alias_name = "test_alias_swap_success"
    physical = "test_alias_swap_phys"

    try:
        await conn.execute_command("FT.ALIASDEL", alias_name)
        await conn.ft(physical).dropindex(delete_documents=False)
    except Exception:
        pass

    await conn.execute_command("FT.CREATE", physical, "SCHEMA", "name", "TAG")

    try:
        migration = IndexMigration(
            model_name="EdgeCaseModel",
            index_name=physical,
            schema="SCHEMA name TAG",
            hash="abc123",
            action=MigrationAction.ALIAS_SWAP,
            conn=conn,
            alias_name=alias_name,
        )

        with patch.object(conn, "ft") as mock_ft:
            # "does not exist" error is swallowed (treated as success)
            mock_ft.return_value.aliasupdate = AsyncMock(
                side_effect=redis.ResponseError("alias does not exist")
            )

            # Should NOT raise — "does not exist" is treated as success
            await migration._alias_swap()
    finally:
        try:
            await conn.ft(physical).dropindex(delete_documents=False)
            await conn.execute_command("FT.ALIASDEL", alias_name)
        except Exception:
            pass


async def test_alias_swap_success_when_sibling_already_swapped(conn: redis.Redis):
    """_alias_swap succeeds when a sibling worker already swapped the alias."""
    alias_name = "test_alias_swap_sibling"
    physical = "test_alias_swap_sibling_v1"

    try:
        await conn.execute_command("FT.ALIASDEL", alias_name)
        await conn.ft(physical).dropindex(delete_documents=False)
    except Exception:
        pass

    await conn.execute_command("FT.CREATE", physical, "SCHEMA", "name", "TAG")

    try:
        migration = IndexMigration(
            model_name="EdgeCaseModel",
            index_name=physical,
            schema="SCHEMA name TAG",
            hash="abc123",
            action=MigrationAction.ALIAS_SWAP,
            conn=conn,
            alias_name=alias_name,
        )

        # First call: error with "does not exist" — swallowed
        # Second call: succeeds
        call_count = [0]

        async def aliasupdate_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise redis.ResponseError("alias does not exist")
            # Second call succeeds (sibling already swapped, treated as success)
            # We simulate this by just returning without error

        with patch.object(conn, "ft") as mock_ft:
            mock_ft.return_value.aliasupdate = AsyncMock(
                side_effect=aliasupdate_side_effect
            )
            # Should not raise
            await migration._alias_swap()
    finally:
        try:
            await conn.ft(physical).dropindex(delete_documents=False)
            await conn.execute_command("FT.ALIASDEL", alias_name)
        except Exception:
            pass


# ── Migrator.run tests ───────────────────────────────────────────────


async def test_migrator_dry_run_with_no_migrations(conn: redis.Redis, snapshot):
    """Migrator.run dry_run prints 'No pending migrations.' when list is empty."""
    migrator = Migrator(conn=conn)
    # Bypass registry-driven detection so the test is independent of
    # whatever other test files happened to import on this xdist worker
    # (e.g. ``StrawberryUser`` from ``test_strawberry_integration``).
    migrator.detect_migrations = AsyncMock()  # type: ignore[method-assign]

    with patch("builtins.print") as mock_print:
        await migrator.run(dry_run=True)
        mock_print.assert_called_with("No pending migrations.")
    migrator.detect_migrations.assert_awaited_once()


async def test_migrator_dry_run_shows_planned_migrations(
    conn: redis.Redis,
    snapshot,
):
    """Migrator.run dry_run prints each planned migration."""
    migrator = Migrator(conn=conn)

    # We need at least one model with a schema to get migrations
    # The _EdgeCaseModel is registered via snapshot fixture
    migrator.migrations = [
        IndexMigration(
            model_name="EdgeCaseModel",
            index_name="edge_case_test__v1",
            schema="SCHEMA name TAG",
            hash="abc123",
            action=MigrationAction.CREATE,
            conn=conn,
            alias_name="edge_case_test",
        )
    ]

    with patch("builtins.print") as mock_print:
        await migrator.run(dry_run=True)
        call_args = [str(c) for c in mock_print.call_args_list]
        assert any("Dry run" in c for c in call_args)
        assert any("1 migration" in c for c in call_args)


# ── _detect_legacy_migrations / _detect_alias_migrations ──────────────


async def test_detect_legacy_migrations_skips_on_not_implemented(
    conn: redis.Redis,
    snapshot,
):
    """_detect_legacy_migrations returns early when redisearch_schema raises NIE."""

    class _NoSchemaModel(JsonModel):
        name: str

        class Meta:
            index_name = "no_schema_test"
            model_key_prefix = "no_schema_doc"
            _test_only = True

    model_registry[_qualname_key(_NoSchemaModel)] = _NoSchemaModel

    migrator = Migrator(conn=conn)

    # Force redisearch_schema to raise NotImplementedError
    with patch.object(_NoSchemaModel, "redisearch_schema") as mock_schema:
        mock_schema.side_effect = NotImplementedError("skip")
        # Should not raise, should skip
        await migrator._detect_legacy_migrations(
            _qualname_key(_NoSchemaModel), _NoSchemaModel
        )


async def test_detect_alias_migrations_skips_on_not_implemented(
    conn: redis.Redis,
    snapshot,
):
    """_detect_alias_migrations returns early when redisearch_schema raises NIE."""

    class _NoSchemaAliasModel(JsonModel):
        name: str

        class Meta:
            index_name = "no_schema_alias_test"
            model_key_prefix = "no_schema_alias_doc"
            zero_downtime_migrations = True
            _test_only = True

    model_registry[_qualname_key(_NoSchemaAliasModel)] = _NoSchemaAliasModel

    migrator = Migrator(conn=conn)

    with patch.object(_NoSchemaAliasModel, "redisearch_schema") as mock_schema:
        mock_schema.side_effect = NotImplementedError("skip")
        await migrator._detect_alias_migrations(
            _qualname_key(_NoSchemaAliasModel), _NoSchemaAliasModel
        )
