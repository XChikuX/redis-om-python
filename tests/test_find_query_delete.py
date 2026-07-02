# type: ignore
"""Tests for ``FindQuery.delete()`` error handling.

The method swallows ``ResponseError`` (commonly raised on Redis Cluster
when ``DEL`` hits keys in different slots) and reports it as ``0``. The
behaviour is documented as intentional, but the swallowed error is now
also logged at WARNING so failures are debuggable from server logs.
"""

import abc
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import Field, JsonModel, Migrator
from aredis_om.model.model import FindQuery
from aredis_om.util import ASYNC_MODE
from redis import ResponseError

from .conftest import py_test_mark_asyncio

try:
    from tests._sync_redis import has_redis_json, has_redisearch

    HAS_REDISEARCH = has_redisearch()
    HAS_REDIS_JSON = has_redis_json()
except Exception:
    HAS_REDISEARCH = False
    HAS_REDIS_JSON = False


needs_redis = pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON),
    reason="Requires RediSearch + RedisJSON",
)


# ---------------------------------------------------------------------------
# Unit tests (no Redis required)
# ---------------------------------------------------------------------------


def _make_query(model_name, keys, delete_result=None, delete_side_effect=None):
    """Build a mocked FindQuery whose .all() yields the given keys.

    ``fake_query.model.db().delete(...)`` is the real awaited call site.
    These unit tests are async-only (skipped in sync mode) because
    ``AsyncMock`` has no sync equivalent in the standard library.
    """
    fake_model = mock.Mock()
    fake_model.__name__ = model_name
    fake_db = mock.Mock()
    if delete_side_effect is not None:
        fake_db.delete = mock.AsyncMock(side_effect=delete_side_effect)
    else:
        fake_db.delete = mock.AsyncMock(return_value=delete_result)
    fake_model.db = mock.Mock(return_value=fake_db)

    # ``all()`` is a coroutine returning a list of models. Each model has
    # a ``.key()`` method returning the Redis key string.
    fake_query = mock.Mock(spec=FindQuery)
    fake_query.model = fake_model
    fake_query.all = mock.AsyncMock(
        return_value=[mock.Mock(key=lambda k=k: k) for k in keys]
    )
    return fake_query


@py_test_mark_asyncio
@pytest.mark.skipif(not ASYNC_MODE, reason="AsyncMock-based unit tests are async-only")
async def test_delete_returns_zero_on_response_error(caplog):
    """A ``ResponseError`` from DEL is swallowed and reported as 0."""
    from aredis_om.model.model import log

    fake_query = _make_query(
        "FakeModel",
        ["k1", "k2"],
        delete_side_effect=ResponseError("CROSSSLOT keys don't hash to the same slot"),
    )

    with caplog.at_level("WARNING", logger=log.name):
        result = await FindQuery.delete(fake_query)

    assert result == 0


@py_test_mark_asyncio
@pytest.mark.skipif(not ASYNC_MODE, reason="AsyncMock-based unit tests are async-only")
async def test_delete_logs_warning_on_response_error(caplog):
    """The swallowed ``ResponseError`` is also logged at WARNING."""
    from aredis_om.model.model import log

    fake_query = _make_query(
        "FakeModel", ["k1"], delete_side_effect=ResponseError("boom")
    )

    with caplog.at_level("WARNING", logger=log.name):
        await FindQuery.delete(fake_query)

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings, "Expected a WARNING log on swallowed ResponseError"
    assert any("FakeModel" in r.getMessage() for r in warnings)
    assert any("boom" in r.getMessage() for r in warnings)


@py_test_mark_asyncio
@pytest.mark.skipif(not ASYNC_MODE, reason="AsyncMock-based unit tests are async-only")
async def test_delete_returns_count_on_success():
    """The DEL response is forwarded untouched on success."""
    fake_query = _make_query("FakeModel", ["k1", "k2", "k3"], delete_result=3)

    result = await FindQuery.delete(fake_query)
    assert result == 3
    fake_query.model.db.return_value.delete.assert_awaited_once_with("k1", "k2", "k3")


# ---------------------------------------------------------------------------
# Integration test (requires Redis)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def int_model(key_prefix, redis):
    class Base(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Doc(Base):
        name: str = Field(index=True)

    await Migrator().run()
    return Doc


@needs_redis
@py_test_mark_asyncio
async def test_integration_delete_returns_count(int_model):
    """End-to-end: ``FindQuery.delete()`` returns the deleted count."""
    Doc = int_model
    for i in range(3):
        await Doc(name=f"d{i}").save()

    count = await Doc.find().delete()
    assert count == 3
    remaining = [d async for d in Doc.find()]
    assert remaining == []
