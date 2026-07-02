# type: ignore
"""Tests for ``List[datetime]`` / ``List[date]`` round-trip conversion.

These cover the CLAUDE.md "Remaining known issues" item:

    ``List[datetime]`` silent data corruption.  ``convert_datetime_to_timestamp``
    converts ``List[datetime]`` items to numeric timestamps on save, but
    ``convert_timestamp_to_datetime`` does not convert the list items back
    to ``datetime`` on load.

The pure-function tests do not require Redis. The JsonModel/HashModel
integration tests are skipped automatically when RediSearch / RedisJSON
is unavailable.
"""

import abc
import datetime
from typing import List, Optional
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import Field, HashModel, JsonModel, Migrator
from aredis_om.model.model import (
    convert_datetime_to_timestamp,
    convert_timestamp_to_datetime,
)

from .conftest import py_test_mark_asyncio

try:
    from tests._sync_redis import has_redis_json, has_redisearch

    HAS_REDISEARCH = has_redisearch()
    HAS_REDIS_JSON = has_redis_json()
except Exception:
    # Redis is not available — pure-function tests still run; integration
    # tests are skipped via the ``skipif`` decorators below.
    HAS_REDISEARCH = False
    HAS_REDIS_JSON = False


# ---------------------------------------------------------------------------
# Pure-function conversion tests (no Redis required)
# ---------------------------------------------------------------------------


def test_list_datetime_round_trip():
    """``List[datetime]`` must survive an encode/decode cycle."""
    dts = [
        datetime.datetime(2024, 1, 2, 3, 4, 5),
        datetime.datetime(2024, 6, 7, 8, 9, 10),
    ]
    encoded = convert_datetime_to_timestamp({"timestamps": dts})
    model_fields = {
        "timestamps": mock.Mock(annotation=List[datetime.datetime]),
    }
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert decoded["timestamps"] == dts
    assert all(isinstance(x, datetime.datetime) for x in decoded["timestamps"])


def test_list_date_round_trip():
    """``List[date]`` must survive an encode/decode cycle."""
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 6, 7)]
    encoded = convert_datetime_to_timestamp({"dates": dates})
    model_fields = {
        "dates": mock.Mock(annotation=List[datetime.date]),
    }
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert decoded["dates"] == dates
    assert all(isinstance(x, datetime.date) for x in decoded["dates"])


def test_optional_list_datetime_round_trip():
    """``Optional[List[datetime]]`` must survive an encode/decode cycle."""
    dts = [datetime.datetime(2024, 1, 2, 3, 4, 5)]
    encoded = convert_datetime_to_timestamp({"timestamps": dts})
    model_fields = {
        "timestamps": mock.Mock(annotation=Optional[List[datetime.datetime]]),
    }
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert decoded["timestamps"] == dts
    assert isinstance(decoded["timestamps"][0], datetime.datetime)


def test_optional_list_date_round_trip():
    """``Optional[List[date]]`` must survive an encode/decode cycle."""
    dates = [datetime.date(2024, 1, 2)]
    encoded = convert_datetime_to_timestamp({"dates": dates})
    model_fields = {
        "dates": mock.Mock(annotation=Optional[List[datetime.date]]),
    }
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert decoded["dates"] == dates
    assert isinstance(decoded["dates"][0], datetime.date)


def test_empty_list_datetime_round_trip():
    """An empty ``List[datetime]`` must round-trip as an empty list."""
    encoded = convert_datetime_to_timestamp({"timestamps": []})
    model_fields = {
        "timestamps": mock.Mock(annotation=List[datetime.datetime]),
    }
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert decoded["timestamps"] == []


def test_list_datetime_preserves_unconvertible_items():
    """Non-numeric items in a ``List[datetime]`` field pass through unchanged."""
    encoded = convert_datetime_to_timestamp(
        {"timestamps": ["not-a-timestamp", None, 1_704_153_845.0]}
    )
    model_fields = {
        "timestamps": mock.Mock(annotation=List[datetime.datetime]),
    }
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    # The unconvertible items are left as-is; the numeric one is converted.
    assert decoded["timestamps"][0] == "not-a-timestamp"
    assert decoded["timestamps"][1] is None
    assert isinstance(decoded["timestamps"][2], datetime.datetime)


def test_dict_of_datetime_values_still_works():
    """Regression guard: plain scalar datetime/date fields are unaffected."""
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

    assert decoded["created_on"] == naive_dt
    assert decoded["join_date"] == naive_date


def test_list_of_embedded_models_with_datetime_still_works():
    """Regression guard: ``List[ModelWithDatetime]`` was already supported."""
    encoded = convert_datetime_to_timestamp(
        {"orders": [{"created_on": datetime.datetime(2024, 1, 1)}]}
    )

    class _FakeModel:
        model_fields = {"created_on": mock.Mock(annotation=datetime.datetime)}

    model_fields = {"orders": mock.Mock(annotation=List[_FakeModel])}
    decoded = convert_timestamp_to_datetime(encoded, model_fields)

    assert decoded["orders"][0]["created_on"] == datetime.datetime(2024, 1, 1)


# ---------------------------------------------------------------------------
# JsonModel integration tests (require Redis with RediSearch + RedisJSON)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def dt_models(key_prefix, redis):
    class Base(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class EventLog(Base):
        name: str = Field(index=True)
        timestamps: List[datetime.datetime]
        dates: List[datetime.date]
        optional_timestamps: Optional[List[datetime.datetime]] = None

    await Migrator().run()
    return EventLog


@pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON),
    reason="requires RediSearch + RedisJSON",
)
@py_test_mark_asyncio
async def test_json_model_list_datetime_roundtrip(dt_models):
    EventLog = dt_models
    dts = [
        datetime.datetime(2024, 1, 2, 3, 4, 5),
        datetime.datetime(2024, 6, 7, 8, 9, 10),
    ]
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 6, 7)]

    log = EventLog(name="evt", timestamps=dts, dates=dates)
    await log.save()
    loaded = await EventLog.get(log.pk)

    assert loaded.timestamps == dts
    assert all(isinstance(x, datetime.datetime) for x in loaded.timestamps)
    assert loaded.dates == dates
    assert all(isinstance(x, datetime.date) for x in loaded.dates)


@pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON),
    reason="requires RediSearch + RedisJSON",
)
@py_test_mark_asyncio
async def test_json_model_optional_list_datetime_roundtrip(dt_models):
    EventLog = dt_models
    dts = [datetime.datetime(2024, 3, 4, 5, 6, 7)]

    log = EventLog(
        name="opt",
        timestamps=[datetime.datetime(2020, 1, 1)],
        dates=[datetime.date(2020, 1, 1)],
        optional_timestamps=dts,
    )
    await log.save()
    loaded = await EventLog.get(log.pk)

    assert loaded.optional_timestamps == dts
    assert isinstance(loaded.optional_timestamps[0], datetime.datetime)


# ---------------------------------------------------------------------------
# HashModel integration test
#
# Note: HashModel rejects list fields at class-definition time, so there is
# no direct ``List[datetime]`` HashModel test. The pure-function tests above
# cover the conversion logic for both model types.
# ---------------------------------------------------------------------------
