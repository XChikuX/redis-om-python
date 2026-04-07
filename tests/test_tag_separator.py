# type: ignore

import abc

import pytest

from aredis_om import Field, HashModel, JsonModel, Migrator
from redis_om import has_redis_json, has_redisearch

from .conftest import py_test_mark_asyncio


HAS_REDISEARCH = has_redisearch()
HAS_REDIS_JSON = has_redis_json()


def test_separator_parameter_accepted():
    field = Field(index=True, separator=";")

    assert field.separator == ";"


def test_separator_default_value():
    field = Field(index=True)

    assert field.separator == "|"


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_separator_in_hash_schema_and_query(key_prefix, redis):
    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Contact(BaseHashModel, index=True):
        email: str = Field(index=True, separator=";")

    assert "email TAG SEPARATOR ;" in Contact.redisearch_schema()

    await Migrator(conn=redis).run()

    first = Contact(email="a;b@example.com")
    second = Contact(email="a;villain@example.com")
    await first.save()
    await second.save()

    assert await Contact.find(Contact.email == "a;b@example.com").all() == [first]
    assert await Contact.find(Contact.email == "a;villain@example.com").all() == [
        second
    ]


@pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON), reason="requires RedisJSON and RediSearch"
)
@py_test_mark_asyncio
async def test_separator_in_json_schema_and_query(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Contact(BaseJsonModel, index=True):
        email: str = Field(index=True, separator=";")

    assert "AS email TAG SEPARATOR ;" in Contact.redisearch_schema()

    await Migrator(conn=redis).run()

    first = Contact(email="a;b@example.com")
    second = Contact(email="a;villain@example.com")
    await first.save()
    await second.save()

    assert await Contact.find(Contact.email == "a;b@example.com").all() == [first]
    assert await Contact.find(Contact.email == "a;villain@example.com").all() == [
        second
    ]
