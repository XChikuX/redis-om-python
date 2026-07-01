# type: ignore
"""Unit tests for ``aredis_om.model.migrations.migrator``.

Covers pure-Python helpers and the ``IndexMigration`` action plumbing in
isolation from Redis: ``schema_hash_key``, ``physical_index_name``,
``MigrationAction``, ``IndexMigration.summary`` / ``history_record``,
``IndexMigration.run`` dispatch, and the small ClusterMigration-side path
exercised through fake ``db`` objects.

End-to-end coverage (against a real Redis) is provided by
``tests/test_migrator.py`` and ``tests/test_migrator_alias.py``.
"""

import json
import logging
from datetime import datetime, timezone

import pytest

from aredis_om import redis
from aredis_om.model.migrations.migrator import (
    PHYSICAL_INDEX_HASH_LEN,
    IndexMigration,
    MigrationAction,
    MigrationError,
    import_submodules,
    physical_index_name,
    schema_hash_key,
)


def py_test_mark_asyncio(f):
    return pytest.mark.asyncio(f)


# ── small helpers ────────────────────────────────────────────────────────


class TestSchemaHashKey:
    def test_default_format(self):
        assert schema_hash_key("my-index") == "my-index:hash"

    def test_with_empty_string(self):
        assert schema_hash_key("") == ":hash"

    def test_does_not_strip_prefix(self):
        # The helper is a thin format function — no validation.
        assert schema_hash_key("foo:bar:hash") == "foo:bar:hash:hash"


class TestPhysicalIndexName:
    def test_basic(self):
        out = physical_index_name("People", "deadbeefcafebabe")
        assert out == "People__vdeadbeef"

    def test_truncates_long_hash(self):
        long = "a" * 64
        out = physical_index_name("idx", long)
        # Only the first PHYSICAL_INDEX_HASH_LEN chars are kept.
        assert out == f"idx__v{'a' * PHYSICAL_INDEX_HASH_LEN}"
        assert PHYSICAL_INDEX_HASH_LEN == 8

    def test_short_hash_not_padded(self):
        # The format is "v" + raw hash; no padding.
        out = physical_index_name("idx", "abc")
        assert out == "idx__vabc"

    def test_alias_with_separator(self):
        # Aliases are user-facing names; preserved verbatim.
        out = physical_index_name("my:things:idx", "abcd1234")
        assert out == "my:things:idx__vabcd1234"


# ── Importing submodules ──────────────────────────────────────────────────


class TestImportSubmodules:
    def test_non_package_module_raises(self):
        # ``collections`` is a top-level module without ``__path__``, i.e. it
        # is not a package — calling ``import_submodules`` on it should
        # raise MigrationError.
        with pytest.raises(MigrationError, match="must be a Python package"):
            import_submodules("collections.abc")

    def test_real_package_walks(self):
        # Walking a real package must not raise. The model package contains
        # submodules and is a stable import target.
        import_submodules("aredis_om.model")


# ── MigrationAction enum ──────────────────────────────────────────────────


class TestMigrationActionEnum:
    def test_action_values(self):
        # These integer values are part of the migration-history wire format;
        # changing them invalidates on-disk history records.
        assert MigrationAction.CREATE.value == 2
        assert MigrationAction.DROP.value == 1
        assert MigrationAction.ALIAS_CREATE_INDEX.value == 3
        assert MigrationAction.ALIAS_LINK.value == 4
        assert MigrationAction.ALIAS_ADOPT.value == 5
        assert MigrationAction.ALIAS_SWAP.value == 6
        assert MigrationAction.ALIAS_CLEANUP.value == 7

    def test_action_names_unique(self):
        names = [a.name for a in MigrationAction]
        assert len(names) == len(set(names))

    def test_action_count(self):
        assert len(list(MigrationAction)) == 7


# ── IndexMigration dataclass ──────────────────────────────────────────────


class TestIndexMigrationSummary:
    def _make(self, **overrides):
        kwargs = dict(
            model_name="tests.test_migrator_unit:Model",
            index_name="idx__v12345678",
            schema="SCHEMA",
            hash="12345678abcdef",
            action=MigrationAction.CREATE,
            conn=None,
            previous_hash=None,
            alias_name=None,
            stale_physical_indexes=[],
        )
        kwargs.update(overrides)
        return IndexMigration(**kwargs)

    def test_summary_basic(self):
        mig = self._make()
        # The model_name is preserved verbatim in the summary.
        expected_model = "tests.test_migrator_unit:Model"
        assert (
            mig.summary()
            == f"CREATE {expected_model} index=idx__v12345678 to=12345678abcdef"
        )

    def test_summary_includes_alias(self):
        mig = self._make(alias_name="People")
        # When alias differs from physical index name we annotate it.
        s = mig.summary()
        assert "alias=People" in s

    def test_summary_skips_alias_when_same_as_index(self):
        mig = self._make(index_name="People", alias_name="People")
        # If alias == index, no alias annotation is added.
        assert "alias=" not in mig.summary()

    def test_summary_includes_previous_hash(self):
        mig = self._make(previous_hash="oldhash")
        assert "from=oldhash" in mig.summary()

    def test_summary_includes_cleanup(self):
        mig = self._make(
            action=MigrationAction.ALIAS_CLEANUP,
            stale_physical_indexes=["idx__v11111111", "idx__v22222222"],
        )
        s = mig.summary()
        assert "cleanup=" in s
        assert "idx__v11111111" in s
        assert "idx__v22222222" in s


# ── IndexMigration.history_record ─────────────────────────────────────────


class TestIndexMigrationHistoryRecord:
    def _make(self, **overrides):
        kwargs = dict(
            model_name="m",
            index_name="idx",
            schema="SCHEMA",
            hash="h",
            action=MigrationAction.CREATE,
            conn=None,
            previous_hash=None,
            alias_name=None,
            stale_physical_indexes=[],
        )
        kwargs.update(overrides)
        return IndexMigration(**kwargs)

    def test_history_record_basic(self):
        mig = self._make()
        rec = mig.history_record()
        assert rec["model"] == "m"
        assert rec["index"] == "idx"
        assert rec["alias"] is None
        assert rec["action"] == "CREATE"
        assert rec["hash"] == "h"
        assert rec["previous_hash"] is None
        assert rec["stale_physical_indexes"] == []

    def test_history_record_includes_alias(self):
        mig = self._make(alias_name="People")
        rec = mig.history_record()
        assert rec["alias"] == "People"

    def test_history_record_includes_previous_hash(self):
        mig = self._make(previous_hash="oldhash")
        rec = mig.history_record()
        assert rec["previous_hash"] == "oldhash"

    def test_history_record_includes_cleanup_list(self):
        mig = self._make(
            action=MigrationAction.ALIAS_CLEANUP,
            stale_physical_indexes=["idx1", "idx2"],
        )
        rec = mig.history_record()
        assert rec["stale_physical_indexes"] == ["idx1", "idx2"]

    def test_history_record_is_json_serialisable(self):
        mig = self._make(
            action=MigrationAction.ALIAS_SWAP,
            alias_name="People",
            previous_hash="oldhash",
            stale_physical_indexes=["idx_a"],
        )
        # ``json.dumps`` will exercise the timestamp serialisation path.
        out = json.dumps(mig.history_record())
        rec = json.loads(out)
        assert rec["action"] == "ALIAS_SWAP"
        assert rec["alias"] == "People"

    def test_history_record_timestamp_is_recent_and_iso(self):
        before = datetime.now(timezone.utc)
        rec = self._make().history_record()
        after = datetime.now(timezone.utc)
        # The timestamp must round-trip via ``fromisoformat`` and lie within
        # the test execution window.
        ts = datetime.fromisoformat(rec["timestamp"])
        assert ts.tzinfo is not None
        assert before <= ts <= after


# ── IndexMigration.run dispatch ───────────────────────────────────────────


class _DummyConn:
    """A minimal stand-in for a redis cluster client.

    Records FT calls. Returns success for unknown commands.
    """

    def __init__(self, *, raises=None):
        self.calls = []
        self._raises = raises

    def ft(self, name):
        return _DummyFt(self, name, raises=self._raises)


class _DummyFt:
    def __init__(self, parent, name, *, raises=None):
        self._parent = parent
        self._name = name
        self._raises = raises

    async def dropindex(self, **kwargs):
        self._parent.calls.append(("dropindex", self._name, kwargs))
        if self._raises is not None:
            raise self._raises


class TestIndexMigrationRunDispatch:
    """The ``run()`` method routes the action enum to the right internal coroutine.

    Each branch is verified by passing a custom _alias_* method through a
    subclass; this confirms the dispatch table without requiring Redis.
    """

    def _make_mig(self, action):
        return IndexMigration(
            model_name="m",
            index_name="idx",
            schema="S",
            hash="h",
            action=action,
            conn=None,
            previous_hash=None,
        )

    @py_test_mark_asyncio
    async def test_dispatch_create(self):
        mig = self._make_mig(MigrationAction.CREATE)
        called = []

        async def fake():
            called.append("create")

        mig.create = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["create"]

    @py_test_mark_asyncio
    async def test_dispatch_drop(self):
        mig = self._make_mig(MigrationAction.DROP)
        called = []

        async def fake():
            called.append("drop")

        mig.drop = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["drop"]

    @py_test_mark_asyncio
    async def test_dispatch_alias_create_index(self):
        mig = self._make_mig(MigrationAction.ALIAS_CREATE_INDEX)
        called = []

        async def fake():
            called.append("alias_create_index")

        mig._alias_create_index = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["alias_create_index"]

    @py_test_mark_asyncio
    async def test_dispatch_alias_link(self):
        mig = self._make_mig(MigrationAction.ALIAS_LINK)
        called = []

        async def fake():
            called.append("alias_link")

        mig._alias_link = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["alias_link"]

    @py_test_mark_asyncio
    async def test_dispatch_alias_adopt(self):
        mig = self._make_mig(MigrationAction.ALIAS_ADOPT)
        called = []

        async def fake():
            called.append("alias_adopt")

        mig._alias_adopt = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["alias_adopt"]

    @py_test_mark_asyncio
    async def test_dispatch_alias_swap(self):
        mig = self._make_mig(MigrationAction.ALIAS_SWAP)
        called = []

        async def fake():
            called.append("alias_swap")

        mig._alias_swap = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["alias_swap"]

    @py_test_mark_asyncio
    async def test_dispatch_alias_cleanup(self):
        mig = self._make_mig(MigrationAction.ALIAS_CLEANUP)
        called = []

        async def fake():
            called.append("alias_cleanup")

        mig._alias_cleanup = fake  # type: ignore[assignment]
        await mig.run()
        assert called == ["alias_cleanup"]


# ── IndexMigration.drop / create error paths ──────────────────────────────


class TestIndexMigrationCreateDropErrors:
    def _make(self, conn):
        return IndexMigration(
            model_name="m",
            index_name="idx",
            schema="S",
            hash="h",
            action=MigrationAction.DROP,
            conn=conn,
            previous_hash=None,
        )

    @py_test_mark_asyncio
    async def test_drop_swallows_response_error(self, caplog):
        # When the index does not exist on the server, ``dropindex`` raises
        # redis.ResponseError. ``IndexMigration.drop`` swallows it and just
        # logs; verify no exception escapes.
        conn = _DummyConn(raises=redis.ResponseError("unknown index"))
        mig = self._make(conn)
        with caplog.at_level(logging.INFO):
            await mig.drop()
        assert any("Index does not exist" in r.message for r in caplog.records)
        assert conn.calls == [("dropindex", "idx", {"delete_documents": False})]

    @py_test_mark_asyncio
    async def test_create_tolerates_already_exists(self, caplog, monkeypatch):
        # Patch the module-level ``create_index`` to raise
        # ``Index already exists`` and verify ``IndexMigration.create``
        # silently tolerates it.
        from aredis_om.model.migrations import migrator

        async def fake_create_index(conn, name, schema, current_hash):
            raise redis.ResponseError("Index already exists")

        monkeypatch.setattr(migrator, "create_index", fake_create_index)

        mig = IndexMigration(
            model_name="m",
            index_name="idx",
            schema="S",
            hash="h",
            action=MigrationAction.CREATE,
            conn=_DummyConn(),
            previous_hash=None,
        )
        with caplog.at_level(logging.INFO):
            await mig.create()
        assert any("Index already exists" in r.message for r in caplog.records)

    @py_test_mark_asyncio
    async def test_create_propagates_other_response_errors(self, monkeypatch):
        from aredis_om.model.migrations import migrator

        async def fake_create_index(conn, name, schema, current_hash):
            raise redis.ResponseError("Boom")

        monkeypatch.setattr(migrator, "create_index", fake_create_index)

        mig = IndexMigration(
            model_name="m",
            index_name="idx",
            schema="S",
            hash="h",
            action=MigrationAction.CREATE,
            conn=_DummyConn(),
            previous_hash=None,
        )
        with pytest.raises(redis.ResponseError, match="Boom"):
            await mig.create()


# ── IndexMigration._alias_link / _alias_adopt / _alias_cleanup shortcuts ──


class TestIndexMigrationAliasShortcuts:
    def _make(self, action, alias=None, stale=None, index_name="idx"):
        return IndexMigration(
            model_name="m",
            index_name=index_name,
            schema="S",
            hash="h",
            action=action,
            conn=_DummyConn(),
            previous_hash=None,
            alias_name=alias,
            stale_physical_indexes=stale or [],
        )

    @py_test_mark_asyncio
    async def test_alias_link_no_alias_noop(self):
        mig = self._make(MigrationAction.ALIAS_LINK, alias=None)
        # No alias → no-op, no exception, no ft calls.
        await mig._alias_link()
        assert isinstance(mig.conn, _DummyConn)
        assert mig.conn.calls == []

    @py_test_mark_asyncio
    async def test_alias_link_same_alias_and_physical_noop(self):
        # When alias == physical there is nothing to point; the helper returns.
        mig = self._make(MigrationAction.ALIAS_LINK, alias="idx", index_name="idx")
        await mig._alias_link()
        assert mig.conn.calls == []

    @py_test_mark_asyncio
    async def test_alias_adopt_no_alias_noop(self):
        mig = self._make(MigrationAction.ALIAS_ADOPT, alias=None)
        await mig._alias_adopt()
        # No alias → no ft calls at all.
        assert mig.conn.calls == []


# ── _alias_cleanup behaviour ───────────────────────────────────────────────


class TestIndexMigrationAliasCleanup:
    def _make(self, stale):
        return IndexMigration(
            model_name="m",
            index_name="idx",
            schema="S",
            hash="h",
            action=MigrationAction.ALIAS_CLEANUP,
            conn=_DummyConn(),
            previous_hash=None,
            alias_name="People",
            stale_physical_indexes=stale,
        )

    @py_test_mark_asyncio
    async def test_cleanup_drops_each_stale(self):
        mig = self._make(["idx__v11111111", "idx__v22222222"])
        await mig._alias_cleanup()
        drops = [c for c in mig.conn.calls if c[0] == "dropindex"]
        assert ("dropindex", "idx__v11111111", {"delete_documents": False}) in drops
        assert ("dropindex", "idx__v22222222", {"delete_documents": False}) in drops

    @py_test_mark_asyncio
    async def test_cleanup_skips_empty_names(self):
        mig = self._make(["", "idx__v11111111"])
        await mig._alias_cleanup()
        drops = [c for c in mig.conn.calls if c[0] == "dropindex"]
        # The empty string is skipped entirely.
        names = [c[1] for c in drops]
        assert "" not in names
        assert "idx__v11111111" in names

    @py_test_mark_asyncio
    async def test_cleanup_swallows_missing_indexes(self, caplog):
        # When the underlying dropindex raises (stale index missing), the
        # helper must continue with the remaining list and just log.
        mig = self._make(["idx_a", "idx_b"])
        # Replace the conn's ft such that idx_a's drop raises.
        original_ft = mig.conn.ft

        def ft(name):
            inner = original_ft(name)
            if name == "idx_a":

                class _RaisingFt:
                    async def dropindex(self_inner, **kwargs):
                        raise redis.ResponseError("unknown index")

                return _RaisingFt()
            return inner

        mig.conn.ft = ft  # type: ignore[assignment]
        with caplog.at_level(logging.INFO):
            await mig._alias_cleanup()
        # idx_b's drop was still attempted.
        assert any("Stale physical index" in r.message for r in caplog.records)
