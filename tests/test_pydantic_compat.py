# type: ignore

import abc
import datetime
from typing import List
from unittest import mock

import pytest

from aredis_om import EmbeddedJsonModel, Field, HashModel, JsonModel, Migrator
from aredis_om._compat import PYDANTIC_V2
from aredis_om.model.model import convert_timestamp_to_datetime, validate_model_data

try:
    from redis_om import has_redis_json

    HAS_REDIS_JSON = has_redis_json()
except (ImportError, ConnectionError, OSError):
    HAS_REDIS_JSON = False

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


@pytest.mark.skipif(not PYDANTIC_V2, reason="pydantic v2 compat only")
def test_v2_root_validator_on_embedded_hashmodel():
    """pydantic v2's @root_validator must work on HashModel with embedded=True.

    When users import root_validator from pydantic (v2), it creates a
    PydanticDescriptorProxy that pydantic v1's metaclass cannot handle.
    ModelMeta should transparently convert these to v1 root validators.
    """
    from pydantic import root_validator

    class EmbeddedLike(HashModel):
        user_id: str
        liked_user_id: str

        @root_validator(skip_on_failure=True)
        def assign_pk(cls, values):
            values["pk"] = ":".join(
                sorted([values["user_id"], values["liked_user_id"]])
            )
            return values

        class Meta:
            embedded = True

    class Operation(EmbeddedJsonModel):
        likes: List[EmbeddedLike] = []

    class RedisUser(JsonModel):
        operations: Operation = Field(...)

    like = EmbeddedLike(user_id="alice", liked_user_id="bob")
    # The root_validator sets a custom pk which overrides validate_pk's None
    assert like.pk == "alice:bob"

    op = Operation(likes=[like])
    op_dict = op.dict()
    assert "pk" not in op_dict
    assert op_dict["likes"][0]["pk"] == "alice:bob"

    user = RedisUser(operations=op)
    user_dict = user.dict()
    assert "pk" not in user_dict["operations"]
    assert user_dict["operations"]["likes"][0]["pk"] == "alice:bob"
    assert user.operations.likes[0].user_id == "alice"
    assert user.operations.likes[0].liked_user_id == "bob"
    assert user.operations.likes[0].pk == "alice:bob"


@pytest.mark.skipif(not PYDANTIC_V2, reason="pydantic v2 compat only")
def test_v2_validator_on_hashmodel():
    """pydantic v2's @validator must also be converted for HashModel."""
    from pydantic import validator as v2_validator

    class TaggedItem(HashModel):
        name: str
        tag: str = "default"

        @v2_validator("tag", always=True, allow_reuse=True)
        def normalize_tag(cls, v):
            return v.upper()

        class Meta:
            embedded = True

    item = TaggedItem(name="test", tag="hello")
    assert item.tag == "HELLO"


@py_test_mark_asyncio
@pytest.mark.skipif(not HAS_REDIS_JSON, reason="RedisJSON required")
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
