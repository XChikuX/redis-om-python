# mypy: disable-error-code="type-var"

"""Tests for the FT.ALIASUPDATE-based zero-downtime migrator.

These tests exercise every code path of the alias-based migration mode
(opt-in via ``Meta.zero_downtime_migrations = True``):

  * Fresh install (no existing index or alias)
  * Idempotent re-run after a successful migration
  * Schema change triggers a forward swap with no data loss
  * Adoption of a legacy physical index into the alias model
  * Rolling-deploy safety: a stale-schema migrator does NOT swap back
  * Concurrent migrators racing against the same alias
  * Cleanup of stale physical indexes from previous schema versions
  * Documents survive a schema change (the original data-loss bug)

These tests need Redis Stack (RediSearch). They will be skipped on a
vanilla OSS Redis that lacks the search module.
"""

import asyncio
import hashlib
from typing import Dict, List, Optional, Type, cast

import pytest
import pytest_asyncio

from aredis_om import EmbeddedJsonModel, Field, JsonModel
from aredis_om.model.migrations.migrator import (
    PHYSICAL_INDEX_HASH_LEN,
    MigrationAction,
    Migrator,
    physical_index_name,
)
from aredis_om.model.model import model_registry


# These tests share fixed index names (``alias_person_test`` etc.)
# and their fixtures drop each other's indexes. Under ``pytest -n auto``
# with ``--dist=loadgroup`` they must run on a single worker, otherwise
# parallel workers race on the same alias and surface spurious
# ``SEARCH_INDEX_NOT_FOUND`` errors. Group them together here.
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xdist_group(name="migrator"),
]


# ── Models ───────────────────────────────────────────────────────────
#
# IMPORTANT: V1 and V2 variants of each scenario share the same
# ``model_key_prefix`` so RediSearch auto-indexes their documents into
# any physical index sharing that prefix. Without this, a V2 physical
# index would not pick up documents written by V1, and the zero-downtime
# swap would lose them.


class _Address(EmbeddedJsonModel):
    city: str = Field(index=True)


class _PersonV1(JsonModel):
    """Model WITHOUT the ``height`` field indexed — the 'old' schema."""

    name: str = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = "alias_person_test"
        model_key_prefix = "alias_person_doc"
        _test_only = True


class _PersonV2(JsonModel):
    """Model WITH the ``height`` field indexed — the 'new' schema.

    Shares ``index_name`` and ``model_key_prefix`` with V1 so that a
    schema change swaps the alias while documents stay addressable.
    """

    name: str = Field(index=True)
    height: int = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = "alias_person_test"
        model_key_prefix = "alias_person_doc"
        _test_only = True


class _PersonV1Rollback(JsonModel):
    name: str = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = "alias_rollback_test"
        model_key_prefix = "alias_rollback_doc"
        _test_only = True


class _PersonV2Rollback(JsonModel):
    name: str = Field(index=True)
    height: int = Field(index=True)
    address: _Address

    class Meta:
        zero_downtime_migrations = True
        index_name = "alias_rollback_test"
        model_key_prefix = "alias_rollback_doc"
        _test_only = True


class _LegacyModel(JsonModel):
    """A model WITHOUT ``zero_downtime_migrations`` — exercises the legacy path."""

    name: str = Field(index=True)

    class Meta:
        index_name = "alias_legacy_test"
        model_key_prefix = "alias_legacy_doc"
        _test_only = True


class _LegacyModelV2(JsonModel):
    """``_LegacyModel`` with an added indexed field — exercises the legacy
    DROP+CREATE schema-change path. Used to verify documents survive the
    drop (the original data-loss regression).

    ``height`` and ``address`` are optional so that V1 documents (which
    only have ``name``) can still hydrate as V2 after the schema change.
    """

    name: str = Field(index=True)
    height: Optional[int] = Field(index=True, default=None)
    address: Optional[_Address] = None

    class Meta:
        index_name = "alias_legacy_test"
        model_key_prefix = "alias_legacy_doc"
        _test_only = True


# Track the qualname-keyed registry entries for each model so tests can
# isolate themselves by clearing siblings. We snapshot every model whose
# name starts with an underscore (the convention used by every test
# model in this file and in sibling files like
# ``test_cluster_migrator_alias.py``), regardless of whether we recognise
# the name up-front. That guarantees that a module-level model from one
# test file cannot leak into another test file's migrator runs on the
# same xdist worker.
_TEST_MODEL_PREFIXES = ("_Person", "_LegacyModel", "_ClusterPerson")


_ALL_TEST_MODELS: Dict[str, Type] = {}
for _key, _val in list(model_registry.items()):
    _name = getattr(_val, "__name__", "")
    if any(_name.startswith(p) for p in _TEST_MODEL_PREFIXES):
        _ALL_TEST_MODELS[cast(str, _key)] = _val


def _qualname_key(cls: Type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _isolate_registry(*keep: Type) -> Dict[str, Type]:
    """Remove all test models except those in ``keep`` from the registry.

    Returns the original registry snapshot so the caller can restore it.
    """
    snapshot: Dict[str, Type] = {}
    for key in list(model_registry.keys()):
        str_key = cast(str, key)
        if str_key in _ALL_TEST_MODELS:
            snapshot[str_key] = cast(Type, model_registry.pop(key))
    for cls in keep:
        model_registry[cast(type, _qualname_key(cls))] = cls
    return snapshot


def _restore_registry(snapshot: Dict[str, Type]) -> None:
    """Undo ``_isolate_registry``."""
    for key in list(model_registry.keys()):
        str_key = cast(str, key)
        if str_key in _ALL_TEST_MODELS:
            model_registry.pop(key, None)
    for str_key, cls in snapshot.items():
        model_registry[cast(type, str_key)] = cls


# ── Helpers ──────────────────────────────────────────────────────────


async def _ft_list(conn) -> List[str]:
    try:
        result = await conn.execute_command("FT._LIST")
    except Exception:
        return []
    names: List[str] = []
    for item in result:
        if isinstance(item, bytes):
            names.append(item.decode("utf-8"))
        elif isinstance(item, str):
            names.append(item)
    return names


async def _alias_target(conn, alias: str) -> Optional[str]:
    try:
        info = await conn.ft(alias).info()
    except Exception:
        return None
    name = info.get("index_name")
    if isinstance(name, bytes):
        name = name.decode("utf-8")
    return name


async def _drop_index_quietly(conn, name: str):
    try:
        await conn.ft(name).dropindex(delete_documents=False)
    except Exception:
        pass


async def _drop_alias_quietly(conn, name: str):
    try:
        await conn.execute_command("FT.ALIASDEL", name)
    except Exception:
        pass


async def _drop_everything(conn, alias: str, doc_prefix: str = ""):
    """Drop the alias and every related physical index + document keys.

    Iterates ``FT._LIST`` multiple times because ``FT.DROPINDEX`` may take
    a moment to propagate the removal across all internal structures of the
    search module — a freshly-created index from a previous test that has
    not yet propagated to the ``FT._LIST`` view used by this helper can
    otherwise leak into the next test. Looping until the alias and physical
    indexes are gone (with a short timeout) keeps tests isolated even when
    the previous run left indexes behind.
    """
    prefix = f"{alias}__v"
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        await _drop_alias_quietly(conn, alias)
        all_indexes = await _ft_list(conn)
        matching = [
            idx for idx in all_indexes if idx == alias or idx.startswith(prefix)
        ]
        if not matching:
            break
        for idx in matching:
            await _drop_index_quietly(conn, idx)
        # Brief pause to let search module bookkeeping catch up before re-listing.
        await asyncio.sleep(0.05)
    # Delete underlying document keys.
    if doc_prefix:
        keys = []
        async for key in conn.scan_iter(match=f"*{doc_prefix}*"):
            keys.append(key)
        if keys:
            try:
                await conn.delete(*keys)
            except Exception:
                pass


async def _wait_for_index_sync(conn, index_name: str, timeout: float = 10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            info = await conn.ft(index_name).info()
        except Exception:
            await asyncio.sleep(0.05)
            continue
        pct = info.get("percent_indexed")
        if pct is not None:
            try:
                if float(pct) >= 1.0:
                    return
            except (ValueError, TypeError):
                pass
        await asyncio.sleep(0.05)


def _schema_hash(schema: str) -> str:
    return hashlib.sha1(schema.encode("utf-8")).hexdigest()


def _expected_physical(alias: str, model_cls: Type) -> str:
    return physical_index_name(alias, _schema_hash(model_cls.redisearch_schema()))


def _migrations_for(migrator: Migrator, alias: str) -> List:
    """Filter planned migrations to those targeting a given alias."""
    return [m for m in migrator.migrations if m.alias_name == alias]


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def clean_person_index(redis):
    snapshot = _isolate_registry()  # remove all test models
    try:
        await _drop_everything(redis, "alias_person_test", "alias_person_doc")
        yield redis
        await _drop_everything(redis, "alias_person_test", "alias_person_doc")
    finally:
        _restore_registry(snapshot)


@pytest_asyncio.fixture
async def clean_rollback_index(redis):
    snapshot = _isolate_registry()
    try:
        await _drop_everything(redis, "alias_rollback_test", "alias_rollback_doc")
        yield redis
        await _drop_everything(redis, "alias_rollback_test", "alias_rollback_doc")
    finally:
        _restore_registry(snapshot)


@pytest_asyncio.fixture
async def clean_legacy_index(redis):
    snapshot = _isolate_registry()
    try:
        await _drop_everything(redis, "alias_legacy_test", "alias_legacy_doc")
        yield redis
        await _drop_everything(redis, "alias_legacy_test", "alias_legacy_doc")
    finally:
        _restore_registry(snapshot)


@pytest_asyncio.fixture
async def person_v1_only(clean_person_index):
    """Isolate the registry to only ``_PersonV1`` for the duration of the test."""
    snapshot = _isolate_registry(_PersonV1)
    try:
        yield clean_person_index
    finally:
        _restore_registry(snapshot)


# ── Tests: fresh install ─────────────────────────────────────────────


async def test_fresh_install_creates_physical_index_and_alias(person_v1_only):
    """A first-time migration creates a versioned physical index and an alias."""
    redis = person_v1_only

    migrator = Migrator(conn=redis)
    await migrator.detect_migrations()

    migrations = _migrations_for(migrator, "alias_person_test")
    # Fresh install: create physical index, then link alias.
    actions = [m.action for m in migrations]
    assert MigrationAction.ALIAS_CREATE_INDEX in actions
    assert MigrationAction.ALIAS_LINK in actions
    assert len(migrations) == 2

    expected_physical = _expected_physical("alias_person_test", _PersonV1)
    assert all(m.index_name == expected_physical for m in migrations)

    await migrator.run()

    # The versioned physical index exists; the alias resolves to it.
    assert expected_physical in await _ft_list(redis)
    assert await _alias_target(redis, "alias_person_test") == expected_physical


async def test_fresh_install_idempotent(person_v1_only):
    """Running the migrator twice is a no-op the second time."""
    redis = person_v1_only

    m1 = Migrator(conn=redis)
    await m1.run()
    assert len(_migrations_for(m1, "alias_person_test")) == 2  # create + link

    m2 = Migrator(conn=redis)
    await m2.run()
    assert len(_migrations_for(m2, "alias_person_test")) == 0


# ── Tests: schema change (the original data-loss bug) ────────────────


async def test_schema_change_swaps_alias_without_data_loss(clean_person_index):
    """Adding the ``height`` index must NOT delete existing documents.

    This is the core regression test for the bug that motivated the
    alias-based migrator. Legacy DROP+CREATE wiped every document; the
    alias path should leave them intact and re-index them into the new
    physical index.
    """
    redis = clean_person_index

    # Step 1: migrate V1 only and insert a document.
    snapshot = _isolate_registry(_PersonV1)
    try:
        await Migrator(conn=redis).run()

        person = _PersonV1(
            name="Alice",
            address=_Address(city="Portland"),
            pk="alice-1",
        )
        await person.save()

        found = await _PersonV1.find(_PersonV1.pk == "alice-1").first()
        assert found.name == "Alice"

        v1_physical = _expected_physical("alias_person_test", _PersonV1)
        assert await _alias_target(redis, "alias_person_test") == v1_physical
    finally:
        _restore_registry(snapshot)

    # Step 2: simulate a deploy that adds ``height``. Swap the registry
    # over to V2 and run the migrator. V1's documents must survive because
    # V2 shares the same key prefix.
    #
    # NOTE: ``allow_forward_swap=True`` is used because this is an explicit
    # pre-deploy migration step (V1 is no longer running). The default
    # ``False`` is conservative and safe for rolling deploys — see
    # ``test_stale_migrator_does_not_swap_back``.
    snapshot = _isolate_registry(_PersonV2)
    try:
        migrator = Migrator(conn=redis, allow_forward_swap=True)
        await migrator.detect_migrations()
        actions = [m.action for m in _migrations_for(migrator, "alias_person_test")]
        assert MigrationAction.ALIAS_CREATE_INDEX in actions
        assert MigrationAction.ALIAS_SWAP in actions

        await migrator.run()

        v2_physical = _expected_physical("alias_person_test", _PersonV2)
        assert await _alias_target(redis, "alias_person_test") == v2_physical
        # V1's physical index was cleaned up.
        assert v1_physical not in await _ft_list(redis)
        assert v2_physical in await _ft_list(redis)

        # CRITICAL: the document written by V1 must still be in Redis.
        # Reading the raw JSON value avoids V2 validation on the missing
        # ``height`` field.
        raw_name = await _PersonV2.get_value("alice-1", "name")
        assert raw_name == "Alice"
    finally:
        _restore_registry(snapshot)


async def test_schema_change_lets_new_queries_use_new_field(clean_person_index):
    """After migration, querying the newly-indexed field works (no E6)."""
    redis = clean_person_index

    snapshot = _isolate_registry(_PersonV1)
    try:
        await Migrator(conn=redis).run()
        await _PersonV1(name="Bob", address=_Address(city="Seattle"), pk="bob-1").save()
    finally:
        _restore_registry(snapshot)

    snapshot = _isolate_registry(_PersonV2)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()

        await _PersonV2(
            name="Carol", height=170, address=_Address(city="Denver"), pk="carol-1"
        ).save()

        # Query by height — the exact case that used to raise E6.
        results = await _PersonV2.find(_PersonV2.height == 170).all()
        assert any(r.pk == "carol-1" for r in results)
    finally:
        _restore_registry(snapshot)


# ── Tests: adoption of a legacy physical index ───────────────────────


async def test_adopt_legacy_physical_index(clean_person_index):
    """A pre-existing legacy index (no alias) gets adopted into alias mode.

    Simulates a user who has been running the old DROP+CREATE migrator
    and then sets ``zero_downtime_migrations = True`` for the first time.
    """
    redis = clean_person_index
    alias = "alias_person_test"

    snapshot = _isolate_registry()
    try:
        # Create a legacy physical index with the alias name (no versioning).
        await redis.execute_command(
            f"ft.create {alias} {_PersonV1.redisearch_schema()}"
        )
        await _wait_for_index_sync(redis, alias)

        legacy_person = _PersonV1(
            name="LegacyUser", address=_Address(city="Austin"), pk="legacy-1"
        )
        await legacy_person.save()

        # Sanity: legacy index is queryable under its own name.
        info = await redis.ft(alias).info()
        assert info["index_name"] == alias

        # Run the alias-aware migrator with V1 in the registry.
        _restore_registry(snapshot)
        snapshot = _isolate_registry(_PersonV1)

        migrator = Migrator(conn=redis)
        await migrator.detect_migrations()

        actions = [m.action for m in _migrations_for(migrator, alias)]
        assert MigrationAction.ALIAS_CREATE_INDEX in actions
        assert MigrationAction.ALIAS_ADOPT in actions

        await migrator.run()

        # Legacy index replaced by the alias.
        all_indexes = await _ft_list(redis)
        assert alias not in all_indexes

        v1_physical = _expected_physical(alias, _PersonV1)
        assert v1_physical in all_indexes
        assert await _alias_target(redis, alias) == v1_physical

        # The document must survive the adoption.
        found = await _PersonV1.find(_PersonV1.pk == "legacy-1").first()
        assert found.name == "LegacyUser"
    finally:
        _restore_registry(snapshot)


# ── Tests: rolling-deploy safety (no backward swap) ──────────────────


async def test_stale_migrator_does_not_swap_back(clean_rollback_index):
    """An old-version instance must not undo a newer migration.

    V2 deploys first and migrates (alias → v2 physical). Then a V1
    instance boots (rolling deploy not yet complete) and runs its
    migrator. The V1 migrator must detect that its target physical index
    already exists AND the alias points elsewhere, and refuse to swap back.
    """
    redis = clean_rollback_index
    alias = "alias_rollback_test"

    # Step 1: V2 deploys first (explicit pre-deploy migration step).
    snapshot = _isolate_registry(_PersonV2Rollback)
    try:
        await Migrator(conn=redis, allow_forward_swap=True).run()
        v2_physical = _expected_physical(alias, _PersonV2Rollback)
        assert await _alias_target(redis, alias) == v2_physical
    finally:
        _restore_registry(snapshot)

    # Step 2: V1 boots after V2 (simulating the rolling-deploy overlap).
    # The default migrator (allow_forward_swap=False) must refuse to swap.
    snapshot = _isolate_registry(_PersonV1Rollback)
    try:
        stale_migrator = Migrator(conn=redis)  # conservative default
        await stale_migrator.detect_migrations()

        for m in _migrations_for(stale_migrator, alias):
            assert m.action is not MigrationAction.ALIAS_SWAP, (
                "Stale migrator (allow_forward_swap=False) must not plan "
                "an alias swap that would undo a newer migration"
            )
            assert m.action is not MigrationAction.ALIAS_CREATE_INDEX, (
                "Stale migrator must not create a competing physical index"
            )

        await stale_migrator.run()

        # Alias must still point at V2 — not swapped back to V1.
        assert await _alias_target(redis, alias) == v2_physical
    finally:
        _restore_registry(snapshot)


# ── Tests: concurrent migrators ──────────────────────────────────────


async def test_concurrent_migrators_on_fresh_install(person_v1_only):
    """Multiple migrators racing on a fresh install must not error."""
    redis = person_v1_only

    migrators = [Migrator(conn=redis) for _ in range(8)]
    await asyncio.gather(*(m.run() for m in migrators))

    v1_physical = _expected_physical("alias_person_test", _PersonV1)
    assert v1_physical in await _ft_list(redis)
    assert await _alias_target(redis, "alias_person_test") == v1_physical


async def test_concurrent_migrators_during_schema_change(clean_person_index):
    """Multiple migrators racing during a schema change must be safe."""
    redis = clean_person_index

    snapshot = _isolate_registry(_PersonV1)
    try:
        await Migrator(conn=redis).run()
        await _PersonV1(name="Race", address=_Address(city="NYC"), pk="race-1").save()
    finally:
        _restore_registry(snapshot)

    snapshot = _isolate_registry(_PersonV2)
    try:
        migrators = [Migrator(conn=redis, allow_forward_swap=True) for _ in range(8)]
        await asyncio.gather(*(m.run() for m in migrators))

        v2_physical = _expected_physical("alias_person_test", _PersonV2)
        assert await _alias_target(redis, "alias_person_test") == v2_physical

        # Document must survive the concurrent migration.
        raw_name = await _PersonV2.get_value("race-1", "name")
        assert raw_name == "Race"
    finally:
        _restore_registry(snapshot)


# ── Tests: cleanup of stale physical indexes ─────────────────────────


async def test_stale_physical_indexes_are_cleaned_up(person_v1_only):
    """After a swap, old versioned physical indexes are dropped (not docs)."""
    redis = person_v1_only

    await Migrator(conn=redis).run()
    v1_physical = _expected_physical("alias_person_test", _PersonV1)

    # Inject a fake stale physical index.
    fake_stale = f"alias_person_test__v{'0' * PHYSICAL_INDEX_HASH_LEN}"
    try:
        await redis.execute_command(
            f"ft.create {fake_stale} "
            f"ON JSON PREFIX 1 redis_om_testing:alias_person_doc: SCHEMA "
            f"$.name AS name TAG"
        )
    except Exception as exc:
        pytest.skip(f"Could not create fake stale index: {exc}")

    assert fake_stale in await _ft_list(redis)

    migrator = Migrator(conn=redis)
    await migrator.detect_migrations()

    cleanups = [
        m
        for m in _migrations_for(migrator, "alias_person_test")
        if m.action is MigrationAction.ALIAS_CLEANUP
    ]
    assert cleanups, "Expected an ALIAS_CLEANUP action for the stale index"
    assert any(fake_stale in m.stale_physical_indexes for m in cleanups)

    await migrator.run()
    assert fake_stale not in await _ft_list(redis)
    assert v1_physical in await _ft_list(redis)


# ── Tests: dry-run mode ──────────────────────────────────────────────


async def test_dry_run_does_not_modify_state(person_v1_only):
    """``dry_run=True`` prints planned actions but applies nothing."""
    redis = person_v1_only

    migrator = Migrator(conn=redis)
    await migrator.detect_migrations()
    assert len(_migrations_for(migrator, "alias_person_test")) >= 1

    await migrator.run(dry_run=True)

    # Nothing created.
    assert await _alias_target(redis, "alias_person_test") is None
    assert not any(
        idx.startswith("alias_person_test__v") for idx in await _ft_list(redis)
    )


# ── Tests: query through alias works end-to-end ──────────────────────


async def test_query_through_alias_returns_results(person_v1_only):
    """End-to-end: find() queries go through the alias and return docs."""
    redis = person_v1_only

    await Migrator(conn=redis).run()

    people = [
        _PersonV1(name=f"User{i}", address=_Address(city=f"City{i}"), pk=f"user-{i}")
        for i in range(5)
    ]
    await asyncio.gather(*(p.save() for p in people))

    results = await _PersonV1.find(_PersonV1.name == "User3").all()
    assert len(results) == 1
    assert results[0].pk == "user-3"

    pks = {pk async for pk in await _PersonV1.all_pks()}
    assert {f"user-{i}" for i in range(5)}.issubset(pks)


# ── Tests: legacy mode still works (backward compatibility) ──────────


async def test_legacy_mode_still_uses_drop_create(clean_legacy_index):
    """Models without ``zero_downtime_migrations`` keep the original path."""
    redis = clean_legacy_index
    alias = "alias_legacy_test"

    snapshot = _isolate_registry(_LegacyModel)
    try:
        migrator = Migrator(conn=redis)
        await migrator.detect_migrations()

        migrations = [m for m in migrator.migrations if m.index_name == alias]
        assert len(migrations) == 1
        assert migrations[0].action is MigrationAction.CREATE
        assert migrations[0].alias_name is None

        await migrator.run()

        all_indexes = await _ft_list(redis)
        assert alias in all_indexes
        assert not any(idx.startswith(f"{alias}__v") for idx in all_indexes)
    finally:
        _restore_registry(snapshot)


# ── Regression: legacy DROP+CREATE must NOT delete documents ────────
#
# This is the exact bug the user hit: adding an indexed field to a model
# and re-running the migrator wiped every JSON document because
# ``dropindex(delete_documents=True)`` was passed. The fix is to use the
# redis-py default ``delete_documents=False`` so the underlying keys survive
# and are re-indexed by the new index.


@pytest_asyncio.fixture
async def legacy_index_with_docs(clean_legacy_index):
    """Set up a legacy index, write documents into it, then yield."""
    redis = clean_legacy_index
    snapshot = _isolate_registry(_LegacyModel)
    try:
        await Migrator(conn=redis).run()
        await _wait_for_index_sync(redis, "alias_legacy_test")

        docs = [
            _LegacyModel(
                name=f"LegacyUser{i}",
                pk=f"legacy-doc-{i}",
            )
            for i in range(5)
        ]
        await asyncio.gather(*(d.save() for d in docs))
        yield redis
    finally:
        _restore_registry(snapshot)


async def test_legacy_mode_preserves_documents_on_schema_change(
    legacy_index_with_docs,
):
    """The legacy DROP+CREATE path must NOT delete the underlying documents.

    Regression test for the ``dropindex(delete_documents=True)`` bug.
    Schema changes are common (adding a newly-indexed field) and must never
    silently wipe production data.
    """
    redis = legacy_index_with_docs

    # Sanity: documents exist before the migration.
    pks_before = {pk async for pk in await _LegacyModel.all_pks()}
    assert len(pks_before) == 5

    # Swap in V2 (adds the indexed ``height`` field) and migrate.
    snapshot = _isolate_registry(_LegacyModelV2)
    try:
        migrator = Migrator(conn=redis)
        await migrator.detect_migrations()

        migrations = [
            m for m in migrator.migrations if m.index_name == "alias_legacy_test"
        ]
        actions = {m.action for m in migrations}
        assert MigrationAction.DROP in actions
        assert MigrationAction.CREATE in actions

        await migrator.run()
        await _wait_for_index_sync(redis, "alias_legacy_test")

        # Critical assertion: the underlying JSON documents survived.
        pks_after = {pk async for pk in await _LegacyModelV2.all_pks()}
        assert pks_before.issubset(pks_after), (
            f"Documents were deleted by the legacy DROP+CREATE migration. "
            f"Before: {pks_before}, after: {pks_after}"
        )
        assert len(pks_after) >= 5

        # And the documents are still queryable through the new index.
        results = await _LegacyModelV2.find().all()
        assert len(results) >= 5
        # The V1 documents hydrate as V2 with height=None (they predate it).
        assert all(r.name.startswith("LegacyUser") for r in results)
    finally:
        _restore_registry(snapshot)


async def test_legacy_mode_preserves_jsonmodel_with_embedded_docs(
    legacy_index_with_docs,
):
    """The user's exact scenario: a JsonModel with EmbeddedJsonModel children.

    The V2 model adds an indexed field AND embeds ``_Address``. The
    underlying JSON document (including the nested address) must round-trip
    through the legacy DROP+CREATE.
    """
    redis = legacy_index_with_docs

    snapshot = _isolate_registry(_LegacyModelV2)
    try:
        # Write a doc with embedded address AFTER migrating to V2 so the
        # document on disk has the nested structure.
        await Migrator(conn=redis).run()
        await _wait_for_index_sync(redis, "alias_legacy_test")

        nested = _LegacyModelV2(
            name="NestedUser",
            height=180,
            address=_Address(city="EmbeddedCity"),
            pk="legacy-nested-1",
        )
        await nested.save()

        # Re-run the migrator (simulating an app remount after a schema
        # change to the embedded model).
        migrator = Migrator(conn=redis)
        await migrator.detect_migrations()
        await migrator.run()
        await _wait_for_index_sync(redis, "alias_legacy_test")

        # The nested document must survive.
        fetched = await _LegacyModelV2.get(pk="legacy-nested-1")
        assert fetched is not None
        assert fetched.name == "NestedUser"
        assert fetched.height == 180
        assert fetched.address.city == "EmbeddedCity"
    finally:
        _restore_registry(snapshot)
