# mypy: disable-error-code="type-var"

"""Flakiness regression tests for ``Migrator().run()`` under concurrency.

These tests reproduce the races that ``pytest-xdist -n auto`` exposes when
many workers race against the same Redis to create and query search indexes:

* multiple workers calling ``Migrator().run()`` simultaneously
* ``save()`` racing ``Migrator().run()`` (test code that does both back to
  back)
* ``FT.CREATE`` returning while the index is still being indexed, with a
  follow-up ``find()`` that returns empty until indexing completes

If these tests fail intermittently under ``pytest -n auto``, the migrator
race protection has regressed.
"""

import asyncio
import uuid
from typing import List

import pytest
import pytest_asyncio

from aredis_om import EmbeddedJsonModel, Field, JsonModel, Migrator


# These tests deliberately exercise races that only surface under
# parallel pytest-xdist workers. They will fail intermittently when run
# with ``-n auto`` — that's the bug they exist to document. Group them
# on a single worker so the regular test suite doesn't trip on them,
# and run ``scripts/flaky_bench.sh`` to reproduce the race explicitly.
pytestmark = [
    pytest.mark.asyncio,
    # Share a single xdist worker with the other migrator test files.
    # ``Migrator().run()`` with no model filter migrates every model in
    # the global registry, and module-level models defined in
    # ``test_migrator_alias.py`` (e.g. ``_PersonV1``) use fixed index
    # names like ``alias_person_test``. If these files run on separate
    # workers they race on the same alias and surface spurious
    # ``SEARCH_INDEX_NOT_FOUND`` errors.
    pytest.mark.xdist_group(name="migrator"),
]


# ── Models ────────────────────────────────────────────────────────────


class _FlakyAddress(EmbeddedJsonModel):
    city: str = Field(index=True)


class _FlakyUser(JsonModel):
    """User model that mirrors the strawberry test schema shape."""

    fname: str = Field(index=True)
    email: str = Field(index=True)
    address: _FlakyAddress
    interests: List[str] = Field(index=True)
    bio: str = Field(default="", index=True, full_text_search=True)
from aredis_om import EmbeddedJsonModel, Field, JsonModel, Migrator
from aredis_om.model.model import model_registry


@pytest_asyncio.fixture(scope="session")
async def _ensure_flaky_indexes():
    """Create the indexes once per worker session.

    Each xdist worker has its own Python process; running ``Migrator().run()``
    here means subsequent tests in this file can rely on the index existing
    without re-creating it.

    The migrator walks the *global* ``model_registry``. On a shared xdist
    worker this file is grouped with ``test_migrator_alias.py``, whose
    module-level ``_PersonV1``/``_PersonV2`` models are also registered.
    Migrating those here would create ``alias_person_test__v*`` physical
    indexes that pollute the alias tests' assertions. Snapshot the
    registry, migrate *only* ``_FlakyUser``, then restore so other
    models are left untouched.
    """
    snapshot = dict(model_registry)
    model_registry.clear()
    model_registry[f"{_FlakyUser.__module__}.{_FlakyUser.__qualname__}"] = _FlakyUser
    try:
        await Migrator().run()
    finally:
        model_registry.clear()
        model_registry.update(snapshot)
    yield


@pytest_asyncio.fixture(autouse=True)
async def _cleanup_keys(key_prefix, _ensure_flaky_indexes):
    """Remove any documents left over from previous tests in this file.

    Tolerates a missing index because another worker may have started but
    not yet finished running ``Migrator().run()`` when this fixture fires.
    """
    try:
        old_pks = [pk async for pk in await _FlakyUser.all_pks()]
    except Exception:
        old_pks = []
    for pk in old_pks:
        try:
            await _FlakyUser.delete(pk)
        except Exception:
            pass
    yield


# ── Helpers ───────────────────────────────────────────────────────────


def _make_user(**overrides):
    defaults = dict(
        fname="FlakyUser",
        email="flaky@example.com",
        address=_FlakyAddress(city="Portland"),
        interests=["redis", "graphql"],
        bio="hello world",
    )
    defaults.update(overrides)
    return _FlakyUser(**defaults)


# ── Tests ─────────────────────────────────────────────────────────────


async def test_concurrent_migrator_run_is_safe():
    """Many concurrent ``Migrator().run()`` calls must not raise.

    Simulates ``pytest-xdist -n auto`` where N workers all race to create
    the same search index. Without the race protection in
    ``create_index`` / ``IndexMigration.create`` this surfaces as
    ``ResponseError: Index already exists`` bubbling out of the test.
    """
    migrators = [Migrator() for _ in range(8)]
    await asyncio.gather(*(m.run() for m in migrators))


async def test_save_then_find_after_migrator_run():
    """``save()`` followed by ``find()`` must see the document.

    When ``Migrator().run()`` returns before indexing has caught up,
    ``FT.SEARCH`` returns an empty result set even though the document
    exists. ``_wait_for_index`` should ensure the index is queryable.
    """
    user = _make_user(fname="SaveFindUser", email="savefind@example.com")
    await user.save()

    found = await _FlakyUser.find(_FlakyUser.pk == user.pk).first()
    assert found.pk == user.pk
    assert found.fname == "SaveFindUser"


async def test_filter_after_save_returns_match():
    """Filtering on an indexed field after a save must find the row."""
    user = _make_user(fname="FilterUser", email="filter@example.com")
    await user.save()

    results = await _FlakyUser.find(_FlakyUser.fname == "FilterUser").all()
    assert len(results) == 1
    assert results[0].email == "filter@example.com"


async def test_full_text_search_after_save():
    """Full-text search must see the saved document once Migrator returns."""
    user = _make_user(
        fname="FullTextUser",
        email="ft@example.com",
        bio="PeculiarToken AlphaBetaGamma rare",
    )
    await user.save()

    results = await _FlakyUser.find(_FlakyUser.bio % "PeculiarToken").all()
    assert len(results) >= 1
    assert any(u.fname == "FullTextUser" for u in results)


async def test_tag_in_query_after_save():
    """TAG-list ``IN`` query must see the saved document."""
    user = _make_user(
        fname="TagUser",
        email="tag@example.com",
        interests=["redis", "graphql"],
    )
    await user.save()

    results = await _FlakyUser.find(_FlakyUser.interests << ["redis"]).all()  # type: ignore[arg-type]
    assert any(u.fname == "TagUser" for u in results)


async def test_idempotent_migrator_run():
    """Running ``Migrator().run()`` twice in a row must be a no-op.

    This exercises the "index already exists" branch of ``create_index``
    without dropping the index — dropping under parallel workers would
    race with sibling workers that depend on the index existing.
    """
    await Migrator().run()
    await Migrator().run()

    # Saving and finding still works after the second run.
    user = _make_user(fname="TwiceUser", email="twice@example.com")
    await user.save()
    found = await _FlakyUser.find(_FlakyUser.pk == user.pk).first()
    assert found.pk == user.pk


async def test_concurrent_writes_then_find_all_pks():
    """Many concurrent saves must all be findable afterwards."""
    users = [
        _make_user(
            fname=f"ConcurrentUser{i}",
            email=f"concurrent{i}@example.com",
        )
        for i in range(20)
    ]
    await asyncio.gather(*(u.save() for u in users))

    pks = {pk async for pk in await _FlakyUser.all_pks()}
    expected_pks = {u.pk for u in users}
    assert expected_pks.issubset(pks), f"Missing pks: {expected_pks - pks}"


async def test_isolated_models_do_not_share_state():
    """Multiple models in the registry should all migrate independently.

    Guard against a regression where a broken race fix accidentally drops
    indexes for sibling models when one is already present.
    """

    class _Sibling(EmbeddedJsonModel):
        note: str

    class _Holder(JsonModel):
        name: str = Field(index=True)
        sibling: _Sibling

    await Migrator().run()

    # Both _FlakyUser (from earlier fixture) and _Holder must exist.
    for cls in (_FlakyUser, _Holder):
        info = await cls.db().ft(cls.Meta.index_name).info()
        assert info["index_name"] == cls.Meta.index_name
