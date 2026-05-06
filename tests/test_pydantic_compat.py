# type: ignore

import abc
import datetime
from typing import List, Self
from unittest import mock

import pytest
from pydantic import field_validator, model_validator

from aredis_om import EmbeddedJsonModel, Field, HashModel, JsonModel, Migrator
from aredis_om.model.model import convert_timestamp_to_datetime, validate_model_data
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

HAS_REDIS_JSON = has_redis_json()


def test_convert_timestamp_to_datetime_uses_model_fields():
    class EmbeddedModel:
        model_fields = {"created_on": mock.Mock(annotation=datetime.datetime)}

    model_fields = {"note": mock.Mock(annotation=EmbeddedModel)}

    converted = convert_timestamp_to_datetime(
        {"note": {"created_on": 1_700_000_000}}, model_fields
    )

    assert isinstance(converted["note"]["created_on"], datetime.datetime)


def test_validate_model_data_uses_model_validate():
    class V2StyleModel:
        def __init__(self, values):
            self.values = values

        @classmethod
        def model_validate(cls, values):
            return cls(values)

    result = validate_model_data(V2StyleModel, {"field": "value"})

    assert isinstance(result, V2StyleModel)
    assert result.values == {"field": "value"}


def test_model_validator_on_embedded_hashmodel():
    class EmbeddedLike(HashModel):
        user_id: str
        liked_user_id: str

        @model_validator(mode="after")
        def assign_pk(self) -> Self:
            # 'self' is the already-validated instance
            if self.pk is None:
                self.pk = ":".join(sorted([self.user_id, self.liked_user_id]))
            return self

        class Meta:
            embedded = True

    class Operation(EmbeddedJsonModel):
        likes: List[EmbeddedLike] = []

    class RedisUser(JsonModel):
        operations: Operation = Field(...)

    like = EmbeddedLike(user_id="alice", liked_user_id="bob")
    assert like.pk == "alice:bob"

    op = Operation(likes=[like])
    op_dict = op.model_dump()
    assert "pk" not in op_dict
    assert op_dict["likes"][0]["pk"] == "alice:bob"

    user = RedisUser(operations=op)
    user_dict = user.model_dump()
    assert "pk" not in user_dict["operations"]
    assert user_dict["operations"]["likes"][0]["pk"] == "alice:bob"


def test_field_validator_on_hashmodel():
    class TaggedItem(HashModel):
        name: str
        tag: str = "default"

        @field_validator("tag")
        @classmethod
        def normalize_tag(cls, v):
            return v.upper()

        class Meta:
            embedded = True

    item = TaggedItem(name="test", tag="hello")
    assert item.tag == "HELLO"


def test_model_validator_on_embedded_json_model():
    class Date(EmbeddedJsonModel):
        date: datetime.datetime | None = None
        utc: datetime.datetime | None = None

        @model_validator(mode="before")
        @classmethod
        def utc_conversion(cls, values):
            if values.get("utc") is None and values.get("date"):
                date_value = values["date"]
                if date_value.tzinfo is None:
                    date_value = date_value.replace(tzinfo=datetime.timezone.utc)
                values["utc"] = date_value.astimezone(datetime.timezone.utc)
            return values

    dt = datetime.datetime(2024, 1, 2, 3, 4, 5)
    result = Date(date=dt)

    assert result.utc == dt.replace(tzinfo=datetime.timezone.utc)


@py_test_mark_asyncio
@pytest.mark.skipif(not HAS_REDIS_JSON, reason="RedisJSON required")
async def test_json_model_get_uses_model_fields(key_prefix, redis):
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
async def test_hash_model_get_uses_model_fields(key_prefix, redis):
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
