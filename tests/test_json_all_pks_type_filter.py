# type: ignore
"""Tests for ``JsonModel.all_pks()`` JSON type-name handling.

This covers the CLAUDE.md "Remaining known issues" item:

    ``ReJSON-RL`` type filter in ``JsonModel.all_pks()`` may need updating
    for Redis 8.x which may report the type as ``"JSON"``.

The ReJSON module has historically reported its keys' ``TYPE`` as
``ReJSON-RL``.  Redis 8.x and forks/variants *may* instead report them as
``"JSON"``, in which case ``SCAN ... TYPE ReJSON-RL`` would silently miss
every key.  ``all_pks()`` must therefore tolerate both type names.

The mock-based tests run without Redis.  The integration tests are skipped
automatically when RediSearch / RedisJSON is unavailable.
"""

import abc
import datetime
from typing import Optional
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import Field, JsonModel, Migrator

from .conftest import py_test_mark_asyncio

try:
    from tests._sync_redis import has_redis_json, has_redisearch

    HAS_REDISEARCH = has_redisearch()
    HAS_REDIS_JSON = has_redis_json()
except Exception:
    # Redis is not available — mock-based tests still run; integration
    # tests are skipped via the ``skipif`` decorators below.
    HAS_REDISEARCH = False
    HAS_REDIS_JSON = False


needs_redis = pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON),
    reason="Requires RediSearch + RedisJSON",
)


# ---------------------------------------------------------------------------
# Mock-based tests (no Redis required)
#
# These prove the type-filter logic without depending on which type name the
# running Redis server happens to emit.  A real server reports exactly one
# of ``ReJSON-RL`` / ``JSON``; the mock lets us simulate each.
# ---------------------------------------------------------------------------


class _BaseJsonModel(JsonModel, abc.ABC):
    class Meta:
        global_key_prefix = "test:allpks:typefilter:"


class _Thing(_BaseJsonModel):
    name: str = Field(index=True)
    note: Optional[str] = None


def _make_scan_iter(results_by_type):
    """Return a Mock ``db.scan_iter`` keyed on the ``_type`` kwarg.

    ``results_by_type`` maps a type name (``"ReJSON-RL"`` / ``"JSON"``) to the
    list of key strings that a SCAN with that type filter would yield.
    """

    def scan_iter(pattern, **kwargs):
        t = kwargs.get("_type")
        keys = results_by_type.get(t, [])

        async def _gen():
            for k in keys:
                yield k

        return _gen()

    m = mock.Mock()
    m.scan_iter.side_effect = scan_iter
    return m


@py_test_mark_asyncio
async def test_all_pks_legacy_rejson_rl_type():
    """Servers that report ``ReJSON-RL`` (the historical name) must work."""
    key_prefix = _Thing.make_key(_Thing._meta.primary_key_pattern.format(pk=""))
    keys = [f"{key_prefix}aa", f"{key_prefix}bb"]
    db = _make_scan_iter({"ReJSON-RL": keys})

    with mock.patch.object(_Thing, "db", return_value=db):
        pks = [pk async for pk in await _Thing.all_pks()]

    assert sorted(pks) == ["aa", "bb"]


@py_test_mark_asyncio
async def test_all_pks_new_json_type_name():
    """Servers that report ``JSON`` (e.g. some Redis 8.x / forks) must work.

    This is the regression that the CLAUDE.md note flags: a server that emits
    ``"JSON"`` instead of ``"ReJSON-RL"`` previously caused ``all_pks()`` to
    silently return no primary keys.
    """
    key_prefix = _Thing.make_key(_Thing._meta.primary_key_pattern.format(pk=""))
    keys = [f"{key_prefix}cc", f"{key_prefix}dd"]
    db = _make_scan_iter({"JSON": keys})

    with mock.patch.object(_Thing, "db", return_value=db):
        pks = [pk async for pk in await _Thing.all_pks()]

    assert sorted(pks) == ["cc", "dd"]


@py_test_mark_asyncio
async def test_all_pks_dedupes_across_type_names():
    """If a server somehow reports a key under both type names, return once."""
    key_prefix = _Thing.make_key(_Thing._meta.primary_key_pattern.format(pk=""))
    dup = f"{key_prefix}ee"
    db = _make_scan_iter({"ReJSON-RL": [dup], "JSON": [dup]})

    with mock.patch.object(_Thing, "db", return_value=db):
        pks = [pk async for pk in await _Thing.all_pks()]

    assert pks == ["ee"]


@py_test_mark_asyncio
async def test_all_pks_no_keys_returns_empty():
    """When no JSON keys exist, ``all_pks()`` must return nothing."""
    db = _make_scan_iter({})

    with mock.patch.object(_Thing, "db", return_value=db):
        pks = [pk async for pk in await _Thing.all_pks()]

    assert pks == []


@py_test_mark_asyncio
async def test_all_pks_count_kwarg_forwarded():
    """``count`` must be forwarded to every SCAN call."""
    key_prefix = _Thing.make_key(_Thing._meta.primary_key_pattern.format(pk=""))
    db = _make_scan_iter({"ReJSON-RL": [f"{key_prefix}0"]})

    with mock.patch.object(_Thing, "db", return_value=db):
        pks = [pk async for pk in await _Thing.all_pks(count=123)]

    assert pks == ["0"]
    for call in db.scan_iter.call_args_list:
        assert call.kwargs.get("count") == 123


# ---------------------------------------------------------------------------
# Integration tests (require Redis with RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def int_model(key_prefix, redis):
    class Base(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Thing(Base):
        name: str = Field(index=True)
        created: datetime.datetime

    await Migrator().run()
    return Thing


@needs_redis
@py_test_mark_asyncio
async def test_all_pks_integration_returns_saved_pk(int_model):
    """Saving a JsonModel and calling all_pks() must surface its primary key.

    This is a regression guard: it passes on Redis 8.0.x (which still emits
    ``ReJSON-RL``) and will continue to pass if/when a server starts emitting
    ``JSON`` instead, because ``all_pks()`` now tolerates both names.
    """
    Thing = int_model
    t = Thing(name="widget", created=datetime.datetime(2024, 1, 2, 3, 4, 5))
    await t.save()

    pks = [pk async for pk in await Thing.all_pks()]
    assert t.pk in pks
