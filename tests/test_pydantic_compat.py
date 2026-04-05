# type: ignore

import abc
import datetime
from unittest import mock

import pytest

from aredis_om import Field, HashModel, JsonModel, Migrator
from aredis_om.model.model import convert_timestamp_to_datetime, validate_model_data
from redis_om import has_redis_json

from .conftest import py_test_mark_asyncio


def test_convert_timestamp_to_datetime_uses_v1_fields_fallback():
    class EmbeddedModel:
        __fields__ = {"created_on": mock.Mock(annotation=datetime.datetime)}

    model_fields = {"note": mock.Mock(annotation=EmbeddedModel)}

    converted = convert_timestamp_to_datetime(
        {"note": {"created_on": 1_700_000_000}}, model_fields
    )

    assert isinstance(converted["note"]["created_on"], datetime.datetime)


def test_validate_model_data_uses_parse_obj_fallback():
    class V1StyleModel:
        def __init__(self, values):
            self.values = values

        @classmethod
        def parse_obj(cls, values):
            return cls(values)

    result = validate_model_data(V1StyleModel, {"field": "value"})

    assert isinstance(result, V1StyleModel)
    assert result.values == {"field": "value"}


@py_test_mark_asyncio
@pytest.mark.skipif(not has_redis_json(), reason="RedisJSON required")
async def test_json_model_get_uses_v1_field_fallback(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Event(BaseJsonModel):
        name: str = Field(index=True)
        created_on: datetime.datetime = Field(index=True)

    await Migrator().run()

    event = Event(name="launch", created_on=datetime.datetime(2024, 1, 2, 3, 4, 5))
    await event.save()

    retrieved = await Event.get(event.pk)

    assert retrieved == event
    assert isinstance(retrieved.created_on, datetime.datetime)


@py_test_mark_asyncio
async def test_hash_model_get_uses_v1_field_fallback(key_prefix, redis):
    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Person(BaseHashModel):
        name: str = Field(index=True)
        joined_on: datetime.date = Field(index=True)

    await Migrator().run()

    person = Person(name="Ada", joined_on=datetime.date(2024, 1, 2))
    await person.save()

    retrieved = await Person.get(person.pk)

    assert retrieved == person
    assert isinstance(retrieved.joined_on, datetime.date)
