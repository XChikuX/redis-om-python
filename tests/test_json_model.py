# type: ignore

import abc
import asyncio
import dataclasses
import datetime
import decimal
import uuid
from collections import namedtuple
from typing import Annotated, Dict, List, Optional, Set, Union
from unittest import mock

import pytest
import pytest_asyncio
from pydantic import StringConstraints

from aredis_om import (
    Coordinates,
    EmbeddedJsonModel,
    Field,
    GeoFilter,
    JsonModel,
    Migrator,
    NotFoundError,
    QueryNotSupportedError,
    QuerySyntaxError,
    RedisModelError,
)
from aredis_om.model.model import SINGLE_VALUE_TAG_FIELD_SEPARATOR
from tests._compat import EmailStr, PositiveInt, ValidationError
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip

today = datetime.date.today()


@pytest_asyncio.fixture
async def m(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Note(EmbeddedJsonModel):
        # ``description`` is indexed as TAG (default for ``str`` with
        # ``index=True``). Full-text search on embedded documents is not
        # supported, so we use TAG-only indexing here to exercise recursive
        # embedded-field query resolution rather than FTS.
        description: str = Field(index=True)
        created_on: datetime.datetime

    class Address(EmbeddedJsonModel):
        address_line_1: str
        address_line_2: Optional[str] = None
        city: str = Field(index=True)
        state: str
        country: str
        postal_code: str = Field(index=True)
        note: Optional[Note] = None

    class Item(EmbeddedJsonModel):
        price: decimal.Decimal
        name: str = Field(index=True)

    class Order(EmbeddedJsonModel):
        items: List[Item]
        created_on: datetime.datetime

    class Member(BaseJsonModel):
        first_name: str = Field(index=True, case_sensitive=True)
        last_name: str = Field(index=True)
        email: Optional[EmailStr] = Field(index=True, default=None)
        join_date: datetime.date
        age: Optional[PositiveInt] = Field(index=True, sortable=True, default=None)
        bio: Optional[str] = Field(index=True, full_text_search=True, default="")

        # Creates an embedded model.
        address: Address

        # Creates an embedded list of models.
        orders: Optional[List[Order]] = None

    await Migrator().run()

    return namedtuple(
        "Models", ["BaseJsonModel", "Note", "Address", "Item", "Order", "Member"]
    )(BaseJsonModel, Note, Address, Item, Order, Member)


@pytest.fixture()
def address(m):
    try:
        yield m.Address(
            address_line_1="1 Main St.",
            city="Portland",
            state="OR",
            country="USA",
            postal_code="11111",
        )
    except Exception as e:
        raise e


@pytest_asyncio.fixture()
async def members(address, m):
    member1 = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        age=38,
        join_date=today,
        address=address,
    )

    member2 = m.Member(
        first_name="Kim",
        last_name="Brookins",
        email="k@example.com",
        age=34,
        join_date=today,
        address=address,
    )

    member3 = m.Member(
        first_name="Andrew",
        last_name="Smith",
        email="as@example.com",
        age=100,
        join_date=today,
        address=address,
    )

    await member1.save()
    await member2.save()
    await member3.save()

    yield member1, member2, member3


@py_test_mark_asyncio
async def test_validate_bad_email(address, m):
    # Raises ValidationError as email is malformed
    with pytest.raises(ValidationError):
        m.Member(
            first_name="Andrew",
            last_name="Brookins",
            zipcode="97086",
            join_date=today,
            email="foobarbaz",
        )


@py_test_mark_asyncio
async def test_validate_bad_age(address, m):
    # Raises ValidationError as email is malformed
    with pytest.raises(ValidationError):
        m.Member(
            first_name="Andrew",
            last_name="Brookins",
            zipcode="97086",
            join_date=today,
            email="foo@bar.com",
            address=address,
            age=-5,
        )


@py_test_mark_asyncio
async def test_validates_required_fields(address, m):
    # Raises ValidationError address is required
    with pytest.raises(ValidationError):
        m.Member(
            first_name="Andrew",
            last_name="Brookins",
            zipcode="97086",
            join_date=today,
        )


@py_test_mark_asyncio
async def test_validates_field(address, m):
    # Raises ValidationError: join_date is not a date
    with pytest.raises(ValidationError):
        m.Member(
            first_name="Andrew",
            last_name="Brookins",
            join_date="yesterday",
            address=address,
        )


@py_test_mark_asyncio
async def test_validation_passes(address, m):
    member = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        age=38,
        address=address,
    )
    assert member.first_name == "Andrew"


@py_test_mark_asyncio
async def test_saves_model_and_creates_pk(address, m, redis):
    await Migrator().run()

    member = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        age=38,
        address=address,
    )
    # Save a model instance to Redis
    await member.save()

    member2 = await m.Member.get(member.pk)
    assert member2 == member
    assert member2.address == address


@py_test_mark_asyncio
async def test_get_restores_missing_pk_from_requested_key(address, m):
    member = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        age=38,
        address=address,
    )

    await member.save()

    raw = await m.Member.db().json().get(member.key())
    raw.pop("pk", None)
    await m.Member.db().json().set(member.key(), ".", raw)

    reloaded = await m.Member.get(member.pk)

    assert reloaded.pk == member.pk
    assert reloaded.address == address


@py_test_mark_asyncio
async def test_get_many_restores_missing_pks_from_requested_keys(address, m):
    members = [
        m.Member(
            first_name="Andrew",
            last_name="Brookins",
            email="a@example.com",
            join_date=today,
            age=38,
            address=address,
        ),
        m.Member(
            first_name="Kim",
            last_name="Brookins",
            email="k@example.com",
            join_date=today,
            age=34,
            address=address,
        ),
    ]

    for member in members:
        await member.save()
        raw = await m.Member.db().json().get(member.key())
        raw.pop("pk", None)
        await m.Member.db().json().set(member.key(), ".", raw)

    reloaded = await m.Member.get_many([member.pk for member in members])

    assert [member.pk for member in reloaded] == [member.pk for member in members]
    assert [member.address for member in reloaded] == [address, address]


@py_test_mark_asyncio
async def test_all_pks(address, m, redis):
    member = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        age=38,
        address=address,
    )

    await member.save()

    member1 = m.Member(
        first_name="Simon",
        last_name="Prickett",
        email="s@example.com",
        join_date=today,
        age=99,
        address=address,
    )

    await member1.save()

    pk_list = []
    async for pk in await m.Member.all_pks():
        pk_list.append(pk)

    assert sorted(pk_list) == sorted([member.pk, member1.pk])


@py_test_mark_asyncio
async def test_all_pks_passes_count(m):
    key_prefix = m.Member.make_key(m.Member._meta.primary_key_pattern.format(pk=""))

    async def scan_results():
        yield f"{key_prefix}0"
        yield f"{key_prefix}1"

    db = mock.Mock()
    db.scan_iter.return_value = scan_results()

    with mock.patch.object(m.Member, "db", return_value=db):
        pk_list = []
        async for pk in await m.Member.all_pks(count=500):
            pk_list.append(pk)

    db.scan_iter.assert_called_once_with(f"{key_prefix}*", _type="ReJSON-RL", count=500)
    assert pk_list == ["0", "1"]


@py_test_mark_asyncio
async def test_all_pks_with_complex_pks(key_prefix):
    class City(JsonModel):
        name: str

        class Meta:
            global_key_prefix = key_prefix
            model_key_prefix = "city"

    city1 = City(
        pk="ca:on:toronto",
        name="Toronto",
    )

    await city1.save()

    city2 = City(
        pk="ca:qc:montreal",
        name="Montreal",
    )

    await city2.save()

    pk_list = []
    async for pk in await City.all_pks():
        pk_list.append(pk)

    assert sorted(pk_list) == ["ca:on:toronto", "ca:qc:montreal"]


@py_test_mark_asyncio
async def test_delete(address, m, redis):
    member = m.Member(
        first_name="Simon",
        last_name="Prickett",
        email="s@example.com",
        join_date=today,
        age=38,
        address=address,
    )

    await member.save()
    response = await m.Member.delete(member.pk)
    assert response == 1


@py_test_mark_asyncio
async def test_saves_many_implicit_pipeline(address, m):
    member1 = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        address=address,
        age=38,
    )
    member2 = m.Member(
        first_name="Kim",
        last_name="Brookins",
        email="k@example.com",
        join_date=today,
        address=address,
        age=34,
    )
    members = [member1, member2]
    result = await m.Member.add(members)
    assert result == [member1, member2]

    assert await m.Member.get(pk=member1.pk) == member1
    assert await m.Member.get(pk=member2.pk) == member2


@py_test_mark_asyncio
async def test_saves_many_explicit_transaction(address, m):
    member1 = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        address=address,
        age=38,
    )
    member2 = m.Member(
        first_name="Kim",
        last_name="Brookins",
        email="k@example.com",
        join_date=today,
        address=address,
        age=34,
    )
    members = [member1, member2]
    result = await m.Member.add(members)
    assert result == [member1, member2]

    assert await m.Member.get(pk=member1.pk) == member1
    assert await m.Member.get(pk=member2.pk) == member2

    # Test the explicit pipeline path -- here, we add multiple Members
    # using a single Redis transaction, with MULTI/EXEC.
    async with m.Member.db().pipeline(transaction=True) as pipeline:
        await m.Member.add(members, pipeline=pipeline)
        assert result == [member1, member2]
        assert await pipeline.execute() == [True, True]

        assert await m.Member.get(pk=member1.pk) == member1
        assert await m.Member.get(pk=member2.pk) == member2


@py_test_mark_asyncio
async def test_delete_many_implicit_pipeline(address, m):
    member1 = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        join_date=today,
        address=address,
        age=38,
    )
    member2 = m.Member(
        first_name="Kim",
        last_name="Brookins",
        email="k@example.com",
        join_date=today,
        address=address,
        age=34,
    )
    members = [member1, member2]
    result = await m.Member.add(members)
    assert result == [member1, member2]
    result = await m.Member.delete_many(members)
    assert result == 2
    with pytest.raises(NotFoundError):
        await m.Member.get(pk=member2.pk)


async def save(members):
    for m in members:
        await m.save()
    return members


@py_test_mark_asyncio
async def test_updates_a_model(members, m):
    member1, member2, member3 = await save(members)

    # Update a field directly on the model
    await member1.update(last_name="Apples to oranges")
    member = await m.Member.get(member1.pk)
    assert member.last_name == "Apples to oranges"

    # Update a field in an embedded model
    await member2.update(address__city="Happy Valley")
    member = await m.Member.get(member2.pk)
    assert member.address.city == "Happy Valley"


@py_test_mark_asyncio
async def test_paginate_query(members, m):
    member1, member2, member3 = members
    actual = await m.Member.find().sort_by("age").all(batch_size=1)
    assert actual == [member2, member1, member3]


@py_test_mark_asyncio
async def test_iter_cursor_iterates_json_results(members, m):
    member1, member2, member3 = members

    cursor = await m.Member.find().sort_by("age").iter_cursor(count=1)
    actual = []

    async for member in cursor:
        actual.append(member)

    assert cursor.total == 3
    assert actual == [member2, member1, member3]
    assert cursor.exhausted


@py_test_mark_asyncio
async def test_iter_cursor_pages_json_results(members, m):
    member1, member2, member3 = members

    cursor = await m.Member.find().sort_by("age").iter_cursor(count=2)

    assert cursor.total == 3
    assert await cursor.read() == [member2, member1]
    assert await cursor.read() == [member3]
    await cursor.close()
    assert cursor.exhausted


@py_test_mark_asyncio
async def test_access_result_by_index_cached(members, m):
    member1, member2, member3 = members
    query = m.Member.find().sort_by("age")
    # Load the cache, throw away the result.
    assert query._model_cache == []
    await query.execute()
    assert query._model_cache == [member2, member1, member3]

    # Access an item that should be in the cache.
    with mock.patch.object(query.model, "db") as mock_db:
        assert await query.get_item(0) == member2
        assert not mock_db.called


@py_test_mark_asyncio
async def test_access_result_by_index_not_cached(members, m):
    member1, member2, member3 = members
    query = m.Member.find().sort_by("age")

    # Assert that we don't have any models in the cache yet -- we
    # haven't made any requests of Redis.
    assert query._model_cache == []
    assert await query.get_item(0) == member2
    assert await query.get_item(1) == member1
    assert await query.get_item(2) == member3


@py_test_mark_asyncio
async def test_in_query(members, m):
    member1, member2, member3 = members
    actual = await (
        m.Member.find(m.Member.pk << [member1.pk, member2.pk, member3.pk])
        .sort_by("age")
        .all()
    )
    assert actual == [member2, member1, member3]


@py_test_mark_asyncio
async def test_not_in_query(members, m):
    member1, member2, member3 = members
    actual = await (
        m.Member.find(m.Member.pk >> [member2.pk, member3.pk]).sort_by("age").all()
    )
    assert actual == [member1]


@py_test_mark_asyncio
async def test_update_query(members, m):
    member1, member2, member3 = members
    await m.Member.find(m.Member.pk << [member1.pk, member2.pk, member3.pk]).update(
        first_name="Bobby"
    )
    actual = await (
        m.Member.find(m.Member.pk << [member1.pk, member2.pk, member3.pk])
        .sort_by("age")
        .all()
    )
    assert len(actual) == 3
    assert all([m.first_name == "Bobby" for m in actual])


@py_test_mark_asyncio
async def test_exact_match_queries(members, m):
    member1, member2, member3 = members

    actual = await m.Member.find(m.Member.last_name == "Brookins").sort_by("age").all()
    assert actual == [member2, member1]

    actual = await m.Member.find(
        (m.Member.last_name == "Brookins") & ~(m.Member.first_name == "Andrew")
    ).all()
    assert actual == [member2]

    actual = await m.Member.find(~(m.Member.last_name == "Brookins")).all()
    assert actual == [member3]

    actual = await m.Member.find(m.Member.last_name != "Brookins").all()
    assert actual == [member3]

    actual = await (
        m.Member.find(
            (m.Member.last_name == "Brookins") & (m.Member.first_name == "Andrew")
            | (m.Member.first_name == "Kim")
        )
        .sort_by("age")
        .all()
    )
    assert actual == [member2, member1]

    actual = await m.Member.find(
        m.Member.first_name == "Kim", m.Member.last_name == "Brookins"
    ).all()
    assert actual == [member2]

    actual = (
        await m.Member.find(m.Member.address.city == "Portland").sort_by("age").all()
    )
    assert actual == [member2, member1, member3]


@py_test_mark_asyncio
async def test_recursive_query_expression_resolution(members, m):
    member1, member2, member3 = members

    actual = await (
        m.Member.find(
            (m.Member.last_name == "Brookins")
            | (m.Member.age == 100) & (m.Member.last_name == "Smith")
        )
        .sort_by("age")
        .all()
    )
    assert actual == [member2, member1, member3]


@py_test_mark_asyncio
async def test_recursive_query_field_resolution(members, m):
    member1, _, _ = members
    member1.address.note = m.Note(
        description="Weird house", created_on=datetime.datetime.now()
    )
    await member1.save()
    actual = await m.Member.find(
        m.Member.address.note.description == "Weird house"
    ).all()
    assert actual == [member1]

    member1.orders = [
        m.Order(
            items=[m.Item(price=10.99, name="Ball")],
            total=10.99,
            created_on=datetime.datetime.now(),
        )
    ]
    await member1.save()
    actual = await m.Member.find(m.Member.orders.items.name == "Ball").all()
    assert actual == [member1]
    assert actual[0].orders[0].items[0].name == "Ball"


@py_test_mark_asyncio
async def test_full_text_search(members, m):
    member1, member2, _ = members
    await member1.update(bio="Hates sunsets, likes beaches")
    await member2.update(bio="Hates beaches, likes forests")

    actual = await m.Member.find(m.Member.bio % "beaches").sort_by("age").all()
    assert actual == [member2, member1]

    actual = await m.Member.find(m.Member.bio % "forests").all()
    assert actual == [member2]


@py_test_mark_asyncio
async def test_tag_queries_boolean_logic(members, m):
    member1, member2, member3 = members

    actual = (
        await m.Member.find(
            (m.Member.first_name == "Andrew") & (m.Member.last_name == "Brookins")
            | (m.Member.last_name == "Smith")
        )
        .sort_by("age")
        .all()
    )
    assert actual == [member1, member3]


@py_test_mark_asyncio
async def test_tag_queries_punctuation(address, m):
    member1 = m.Member(
        first_name="Andrew, the Michael",
        last_name="St. Brookins-on-Pier",
        email="a|b@example.com",  # NOTE: This string uses the TAG field separator.
        age=38,
        join_date=today,
        address=address,
    )
    await member1.save()

    member2 = m.Member(
        first_name="Bob",
        last_name="the Villain",
        email="a|villain@example.com",  # NOTE: This string uses the TAG field separator.
        age=38,
        join_date=today,
        address=address,
    )
    await member2.save()

    assert (
        await m.Member.find(m.Member.first_name == "Andrew, the Michael").first()
        == member1
    )
    assert (
        await m.Member.find(m.Member.last_name == "St. Brookins-on-Pier").first()
        == member1
    )

    # Notice that when we index and query multiple values that use the internal
    # TAG separator for single-value exact-match fields, like an indexed string,
    # the queries will succeed. We apply a workaround that queries for the union
    # of the two values separated by the tag separator.
    assert await m.Member.find(m.Member.email == "a|b@example.com").all() == [member1]
    assert await m.Member.find(m.Member.email == "a|villain@example.com").all() == [
        member2
    ]


@py_test_mark_asyncio
async def test_tag_queries_negation(members, m):
    member1, member2, member3 = members

    """
           ┌first_name
     NOT EQ┤
           └Andrew

    """
    query = m.Member.find(~(m.Member.first_name == "Andrew"))
    assert await query.all() == [member2]

    """
               ┌first_name
        ┌NOT EQ┤
        |      └Andrew
     AND┤
        |  ┌last_name
        └EQ┤
           └Brookins

    """
    query = m.Member.find(
        ~(m.Member.first_name == "Andrew") & (m.Member.last_name == "Brookins")
    )
    assert await query.all() == [member2]

    """
               ┌first_name
        ┌NOT EQ┤
        |      └Andrew
     AND┤
        |     ┌last_name
        |  ┌EQ┤
        |  |  └Brookins
        └OR┤
           |  ┌last_name
           └EQ┤
              └Smith
    """
    query = m.Member.find(
        ~(m.Member.first_name == "Andrew")
        & ((m.Member.last_name == "Brookins") | (m.Member.last_name == "Smith"))
    )
    assert await query.all() == [member2]

    """
                  ┌first_name
           ┌NOT EQ┤
           |      └Andrew
       ┌AND┤
       |   |  ┌last_name
       |   └EQ┤
       |      └Brookins
     OR┤
       |  ┌last_name
       └EQ┤
          └Smith
    """
    query = m.Member.find(
        ~(m.Member.first_name == "Andrew") & (m.Member.last_name == "Brookins")
        | (m.Member.last_name == "Smith")
    )
    assert await query.sort_by("age").all() == [member2, member3]

    actual = await m.Member.find(
        (m.Member.first_name == "Andrew") & ~(m.Member.last_name == "Brookins")
    ).all()
    assert actual == [member3]


@py_test_mark_asyncio
async def test_numeric_queries(members, m):
    member1, member2, member3 = members

    actual = await m.Member.find(m.Member.age == 34).all()
    assert actual == [member2]

    actual = await m.Member.find(m.Member.age > 34).sort_by("age").all()
    assert actual == [member1, member3]

    actual = await m.Member.find(m.Member.age < 35).all()
    assert actual == [member2]

    actual = await m.Member.find(m.Member.age <= 34).all()
    assert actual == [member2]

    actual = await m.Member.find(m.Member.age >= 100).all()
    assert actual == [member3]

    actual = await m.Member.find(~(m.Member.age == 100)).sort_by("age").all()
    assert actual == [member2, member1]

    actual = (
        await m.Member.find(m.Member.age > 30, m.Member.age < 40).sort_by("age").all()
    )
    assert actual == [member2, member1]

    actual = await m.Member.find(m.Member.age != 34).sort_by("age").all()
    assert actual == [member1, member3]


@py_test_mark_asyncio
async def test_sorting(members, m):
    member1, member2, member3 = members

    actual = await m.Member.find(m.Member.age > 34).sort_by("age").all()
    assert actual == [member1, member3]

    actual = await m.Member.find(m.Member.age > 34).sort_by("-age").all()
    assert actual == [member3, member1]

    with pytest.raises(QueryNotSupportedError):
        # This field does not exist.
        await m.Member.find().sort_by("not-a-real-field").all()

    with pytest.raises(QueryNotSupportedError):
        # This field is not sortable.
        await m.Member.find().sort_by("join_date").all()


@py_test_mark_asyncio
async def test_sorting_by_embedded_sortable_field(key_prefix):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Metrics(EmbeddedJsonModel):
        score: int = Field(index=True, sortable=True)

    class Book(BaseJsonModel):
        title: str = Field(index=True)
        metrics: Metrics

    await Migrator().run()

    low = Book(title="Low", metrics=Metrics(score=1))
    high = Book(title="High", metrics=Metrics(score=5))
    await low.save()
    await high.save()

    actual = await Book.find(Book.metrics.score > 0).sort_by("metrics.score").all()
    assert actual == [low, high]

    # Support both dotted user-facing paths and the existing "__" alias syntax.
    actual = await Book.find(Book.metrics.score > 0).sort_by("-metrics__score").all()
    assert actual == [high, low]


@py_test_mark_asyncio
async def test_copy_preserves_resolved_embedded_sort_fields(key_prefix):
    """Regression: FindQuery.copy() must not re-validate already-resolved
    embedded sort field names. The sort_by("metrics.score") path resolves
    to the flattened "metrics_score" alias used by RediSearch SORTBY. The
    transparent pagination loop in FindQuery.execute() calls copy() on
    every page, which previously fed the flattened name back through
    validate_sort_fields() and raised QueryNotSupportedError because the
    flattened name does not exist on the model.
    """

    class Metrics(EmbeddedJsonModel):
        score: int = Field(index=True, sortable=True)

    class Book(JsonModel):
        class Meta:
            global_key_prefix = key_prefix

        title: str = Field(index=True)
        metrics: Metrics

    await Migrator().run()

    query = Book.find().sort_by("metrics.score")
    # The original query stores the resolved (flattened) name.
    assert query.sort_fields == ["metrics_score"]

    # Copying without overriding sort_fields must preserve the resolved
    # form without re-validating it. This mirrors the pagination loop.
    paginated = query.copy(offset=1000)
    assert paginated.sort_fields == ["metrics_score"]

    # Copying with an explicit sort_fields override must still validate
    # and resolve the new value (so users can pass dotted paths to copy).
    overridden = query.copy(sort_fields=["-metrics.score"])
    assert overridden.sort_fields == ["-metrics_score"]

    # End-to-end: exercise the FindQuery.execute() exhaust loop by forcing
    # a small page size so execute() actually calls copy(offset=...) for
    # subsequent pages. Before the fix this raised QueryNotSupportedError.
    low = Book(title="Low", metrics=Metrics(score=1))
    mid = Book(title="Mid", metrics=Metrics(score=3))
    high = Book(title="High", metrics=Metrics(score=5))
    await low.save()
    await mid.save()
    await high.save()

    paginated_query = Book.find().sort_by("metrics.score")
    paginated_query.page_size = 1  # force the exhaust loop to span pages
    results = await paginated_query.execute()
    assert [b.title for b in results] == ["Low", "Mid", "High"]


@py_test_mark_asyncio
async def test_default_ttl_is_applied_to_json_models_on_save(key_prefix):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            default_ttl = 120

    class Session(BaseJsonModel):
        name: str = Field(index=True)

    await Migrator().run()

    session = Session(name="cached")
    await session.save()

    ttl = await Session.db().ttl(session.key())
    assert 0 < ttl <= 120


@py_test_mark_asyncio
async def test_case_sensitive(members, m):
    member1, member2, member3 = members

    actual = await m.Member.find(m.Member.first_name == "Andrew").all()
    assert sorted([a.pk for a in actual]) == sorted([member1.pk, member3.pk])

    actual = await m.Member.find(m.Member.first_name == "andrew").all()
    assert actual == []


@py_test_mark_asyncio
async def test_not_found(m):
    with pytest.raises(NotFoundError):
        # This ID does not exist.
        await m.Member.get(1000)


@py_test_mark_asyncio
async def test_list_field_limitations(m, redis):
    with pytest.raises(RedisModelError):

        class SortableTarotWitch(m.BaseJsonModel):
            # We support indexing lists of strings for quality and membership
            # queries. Sorting is not supported, but is planned.
            tarot_cards: List[str] = Field(index=True, sortable=True)

    with pytest.raises(RedisModelError):

        class SortableFullTextSearchAlchemicalWitch(m.BaseJsonModel):
            # Sorting multi-value fields is not supported, including when the
            # same field is also indexed for full-text search.
            potions: List[str] = Field(index=True, full_text_search=True, sortable=True)

    with pytest.raises(RedisModelError):

        class NumerologyWitch(m.BaseJsonModel):
            # We don't support indexing a list of numbers. Support for this
            # feature is To Be Determined.
            lucky_numbers: List[int] = Field(index=True)

    # Note: ``List[Model]`` where the embedded model has numeric fields IS
    # now supported — the schema generator correctly emits NUMERIC for int
    # fields inside ``List[EmbeddedJsonModel]`` (see fix #6 in CLAUDE.md and
    # ``test_json_schema_accepts_list_of_model_with_int_field`` in
    # ``tests/test_audit_fixes.py"). The previous restriction was a
    # preview-release limitation that has been lifted.

    class TarotWitch(m.BaseJsonModel):
        # We support indexing lists of strings for quality and membership
        # queries. Sorting is not supported, but is planned.
        tarot_cards: List[str] = Field(index=True)

    # We need to import and run this manually because we defined
    # our model classes within a function that runs after the test
    # suite's migrator has already looked for migrations to run.
    await Migrator().run()

    witch = TarotWitch(tarot_cards=["death"])
    await witch.save()
    actual = await TarotWitch.find(TarotWitch.tarot_cards << "death").all()
    assert actual == [witch]


@py_test_mark_asyncio
async def test_string_list_field_allows_full_text_search(m):
    class AlchemicalWitch(m.BaseJsonModel):
        potions: List[str] = Field(index=True, full_text_search=True)

    assert (
        f"$.potions[*] AS potions TAG SEPARATOR {SINGLE_VALUE_TAG_FIELD_SEPARATOR} "
        "$.potions[*] AS potions_fts TEXT" in AlchemicalWitch.redisearch_schema()
    )

    await Migrator().run()

    old_pks = [pk async for pk in await AlchemicalWitch.all_pks()]
    for pk in old_pks:
        await AlchemicalWitch.delete(pk)

    first = AlchemicalWitch(potions=["healing", "mana"])
    second = AlchemicalWitch(potions=["invisibility", "speed"])
    await first.save()
    await second.save()

    assert await AlchemicalWitch.find(AlchemicalWitch.potions << ["mana"]).all() == [
        first
    ]
    assert await AlchemicalWitch.find(
        AlchemicalWitch.potions % "invisibility"
    ).all() == [second]


@py_test_mark_asyncio
async def test_allows_dataclasses(m):
    @dataclasses.dataclass
    class Address:
        address_line_1: str

    class ValidMember(m.BaseJsonModel):
        address: Address

    address = Address(address_line_1="hey")
    member = ValidMember(address=address)
    await member.save()

    member2 = await ValidMember.get(member.pk)
    assert member2 == member
    assert member2.address.address_line_1 == "hey"


@py_test_mark_asyncio
async def test_allows_and_serializes_dicts(m):
    class ValidMember(m.BaseJsonModel):
        address: Dict[str, str]

    member = ValidMember(address={"address_line_1": "hey"})
    await member.save()

    member2 = await ValidMember.get(member.pk)
    assert member2 == member
    assert member2.address["address_line_1"] == "hey"


@py_test_mark_asyncio
async def test_allows_and_serializes_sets(m):
    class ValidMember(m.BaseJsonModel):
        friend_ids: Set[int]

    member = ValidMember(friend_ids={1, 2})
    await member.save()

    member2 = await ValidMember.get(member.pk)
    assert member2 == member
    assert member2.friend_ids == {1, 2}


@py_test_mark_asyncio
async def test_allows_and_serializes_lists(m):
    class ValidMember(m.BaseJsonModel):
        friend_ids: List[int]

    member = ValidMember(friend_ids=[1, 2])
    await member.save()

    member2 = await ValidMember.get(member.pk)
    assert member2 == member
    assert member2.friend_ids == [1, 2]


@py_test_mark_asyncio
async def test_schema(m, key_prefix):
    # We need to build the key prefix because it will differ based on whether
    # these tests were copied into the tests_sync folder and unasynce'd.
    key_prefix = m.Member.make_key(m.Member._meta.primary_key_pattern.format(pk=""))
    assert m.Member.redisearch_schema() == (
        f"ON JSON PREFIX 1 {key_prefix} SCHEMA "
        "$.pk AS pk TAG SEPARATOR | "
        "$.first_name AS first_name TAG SEPARATOR | CASESENSITIVE "
        "$.last_name AS last_name TAG SEPARATOR | "
        "$.email AS email TAG SEPARATOR |  "
        "$.age AS age NUMERIC SORTABLE "
        "$.bio AS bio TAG SEPARATOR | "
        "$.bio AS bio_fts TEXT "
        "$.address.city AS address_city TAG SEPARATOR | "
        "$.address.postal_code AS address_postal_code TAG SEPARATOR | "
        "$.address.note.description AS address_note_description TAG SEPARATOR | "
        "$.orders[*].items[*].name AS orders_items_name TAG SEPARATOR |"
    )


@py_test_mark_asyncio
async def test_annotated_embedded_field_is_indexed(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Inner(EmbeddedJsonModel):
        annotated_tag: Annotated[str, StringConstraints(pattern=r"^(x|y|z)$")] = Field(
            index=True
        )

    class Parent(BaseJsonModel, index=True):
        name: str = Field(index=True)
        inner: Inner

    with pytest.raises(ValidationError):
        Inner(annotated_tag="invalid")

    schema = Parent.redisearch_schema()
    assert "$.inner.annotated_tag AS inner_annotated_tag" in schema
    assert "inner_annotated_tag TAG" in schema

    await Migrator().run()
    await Parent(name="test", inner=Inner(annotated_tag="x")).save()

    results = await Parent.find(Parent.inner.annotated_tag == "x").all()
    assert len(results) == 1


@py_test_mark_asyncio
async def test_pep604_optional_embedded_field_is_queryable(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Inner(EmbeddedJsonModel):
        optional_tag: str | None = Field(None, index=True)
        mixed_tag: str | int | None = Field(None, index=True)

    class Parent(BaseJsonModel, index=True):
        name: str = Field(index=True)
        inner: Inner

    await Migrator().run()
    await Parent(
        name="test", inner=Inner(optional_tag="hello", mixed_tag="mixed")
    ).save()

    results = await Parent.find(Parent.inner.optional_tag == "hello").all()
    assert len(results) == 1
    assert results[0].inner.optional_tag == "hello"

    mixed_results = await Parent.find(Parent.inner.mixed_tag == "mixed").all()
    assert len(mixed_results) == 1
    assert mixed_results[0].inner.mixed_tag == "mixed"


@py_test_mark_asyncio
async def test_list_annotated_without_none_is_queryable(key_prefix, redis):
    """List[Annotated[str, StringConstraints(...)]] without `| None` must
    remain queryable (regression for the existing supported pattern)."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Inner(EmbeddedJsonModel):
        tags: List[Annotated[str, StringConstraints(max_length=20)]] = Field(
            [], index=True, full_text_search=True
        )

    class Parent(BaseJsonModel, index=True):
        name: str = Field(index=True)
        inner: Inner

    await Migrator().run()
    await Parent(name="test", inner=Inner(tags=["hello", "world"])).save()

    results = await Parent.find(Parent.inner.tags % "hello").all()
    assert len(results) == 1


@py_test_mark_asyncio
async def test_list_annotated_pep604_optional_on_embedded_is_queryable(
    key_prefix, redis
):
    """Regression for `List[Annotated[...]] | None` on an EmbeddedJsonModel field."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Inner(EmbeddedJsonModel):
        optional_tags: List[Annotated[str, StringConstraints(max_length=20)]] | None = (
            Field(None, index=True)
        )

    class Parent(BaseJsonModel, index=True):
        name: str = Field(index=True)
        inner: Inner

    await Migrator().run()
    await Parent(name="test", inner=Inner(optional_tags=["target"])).save()

    results = await Parent.find(Parent.inner.optional_tags << ["target"]).all()
    assert len(results) == 1
    assert results[0].inner.optional_tags == ["target"]


@py_test_mark_asyncio
async def test_list_annotated_pep604_optional_on_parent_is_queryable(key_prefix, redis):
    """Regression for `List[Annotated[...]] | None` on the parent JsonModel."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Parent(BaseJsonModel, index=True):
        name: str = Field(index=True)
        favorites: List[Annotated[str, StringConstraints(min_length=1)]] | None = Field(
            None, index=True
        )

    await Migrator().run()
    await Parent(name="test", favorites=["item1"]).save()

    results = await Parent.find(Parent.favorites << ["item1"]).all()
    assert len(results) == 1
    assert results[0].favorites == ["item1"]

    # Optional[List[...]] equivalent should also work.
    class Parent2(BaseJsonModel, index=True):
        name: str = Field(index=True)
        favorites: Optional[List[Annotated[str, StringConstraints(min_length=1)]]] = (
            Field(None, index=True)
        )

    await Migrator().run()
    await Parent2(name="test", favorites=["abc"]).save()
    assert len(await Parent2.find(Parent2.favorites << ["abc"]).all()) == 1


@py_test_mark_asyncio
async def test_count(members, m):
    # member1, member2, member3 = members
    actual_count = await m.Member.find(
        (m.Member.first_name == "Andrew") & (m.Member.last_name == "Brookins")
        | (m.Member.last_name == "Smith")
    ).count()
    assert actual_count == 2

    actual_count = await m.Member.find(
        m.Member.first_name == "Kim", m.Member.last_name == "Brookins"
    ).count()
    assert actual_count == 1


@py_test_mark_asyncio
async def test_type_with_union(members, m):
    class TypeWithUnion(m.BaseJsonModel):
        field: Union[str, int]

    twu_str = TypeWithUnion(field="hello world")
    res = await twu_str.save()
    assert res.pk == twu_str.pk
    twu_str_rematerialized = await TypeWithUnion.get(twu_str.pk)
    assert (
        isinstance(twu_str_rematerialized.field, str)
        and twu_str_rematerialized.pk == twu_str.pk
    )

    twu_int = TypeWithUnion(field=42)
    await twu_int.save()
    twu_int_rematerialized = await TypeWithUnion.get(twu_int.pk)
    assert (
        isinstance(twu_int_rematerialized.field, int)
        and twu_int_rematerialized.pk == twu_int.pk
    )


@py_test_mark_asyncio
async def test_type_with_uuid(key_prefix):
    class TypeWithUuid(JsonModel):
        uuid: uuid.UUID

        class Meta:
            global_key_prefix = key_prefix

    item = TypeWithUuid(uuid=uuid.uuid4())

    await item.save()


@py_test_mark_asyncio
async def test_xfix_queries(m):
    await m.Member(
        first_name="Steve",
        last_name="Lorello",
        email="s@example.com",
        join_date=today,
        bio="Steve is a two-bit hacker who loves Redis.",
        address=m.Address(
            address_line_1="42 foo bar lane",
            city="Satellite Beach",
            state="FL",
            country="USA",
            postal_code="32999",
        ),
        age=34,
    ).save()

    result = await m.Member.find(
        m.Member.first_name.startswith("Ste") and m.Member.first_name == "Steve"
    ).first()
    assert result.first_name == "Steve"

    result = await m.Member.find(
        m.Member.last_name.endswith("llo") and m.Member.first_name == "Steve"
    ).first()
    assert result.first_name == "Steve"

    result = await m.Member.find(
        m.Member.address.city.contains("llite") and m.Member.first_name == "Steve"
    ).first()
    assert result.first_name == "Steve"

    result = await m.Member.find(
        m.Member.bio % "tw*" and m.Member.first_name == "Steve"
    ).first()
    assert result.first_name == "Steve"

    result = await m.Member.find(
        m.Member.bio % "*cker" and m.Member.first_name == "Steve"
    ).first()
    assert result.first_name == "Steve"

    result = await m.Member.find(
        m.Member.bio % "*ack*" and m.Member.first_name == "Steve"
    ).first()
    assert result.first_name == "Steve"


@py_test_mark_asyncio
async def test_none(key_prefix):
    class ModelWithNoneDefault(JsonModel):
        test: Optional[str] = Field(index=True, default=None)

        class Meta:
            global_key_prefix = key_prefix

    class ModelWithStringDefault(JsonModel):
        test: Optional[str] = Field(index=True, default="None")

        class Meta:
            global_key_prefix = key_prefix

    await Migrator().run()

    a = ModelWithNoneDefault()
    await a.save()
    res = await ModelWithNoneDefault.find(ModelWithNoneDefault.pk == a.pk).first()
    assert res.test is None

    b = ModelWithStringDefault()
    await b.save()
    res = await ModelWithStringDefault.find(ModelWithStringDefault.pk == b.pk).first()
    assert res.test == "None"


@py_test_mark_asyncio
async def test_update_validation(key_prefix):
    class Embedded(EmbeddedJsonModel):
        price: float
        name: str = Field(index=True)

    class TestUpdatesClass(JsonModel):
        name: str
        age: int
        embedded: Embedded

        class Meta:
            global_key_prefix = key_prefix

    await Migrator().run()
    embedded = Embedded(price=3.14, name="foo")
    t = TestUpdatesClass(name="str", age=42, embedded=embedded)
    await t.save()

    update_dict = dict()
    update_dict["age"] = "foo"
    with pytest.raises(ValidationError):
        await t.update(**update_dict)

    t.age = 42
    update_dict.clear()
    update_dict["embedded"] = "hello"
    with pytest.raises(ValidationError):
        await t.update(**update_dict)

    rematerialized = await TestUpdatesClass.find(TestUpdatesClass.pk == t.pk).first()
    assert rematerialized.age == 42


@py_test_mark_asyncio
async def test_model_with_dict(key_prefix):
    class EmbeddedJsonModelWithDict(EmbeddedJsonModel):
        metadata: Dict

    class ModelWithDict(JsonModel):
        embedded_model: EmbeddedJsonModelWithDict
        info: Dict

        class Meta:
            global_key_prefix = key_prefix

    await Migrator().run()
    d = dict()
    inner_dict = dict()
    d["foo"] = "bar"
    inner_dict["bar"] = "foo"
    embedded_model = EmbeddedJsonModelWithDict(metadata=inner_dict)
    item = ModelWithDict(info=d, embedded_model=embedded_model)
    await item.save()

    rematerialized = await ModelWithDict.find(ModelWithDict.pk == item.pk).first()
    assert rematerialized.pk == item.pk
    assert rematerialized.info["foo"] == "bar"
    assert rematerialized.embedded_model.metadata["bar"] == "foo"


@py_test_mark_asyncio
async def test_boolean(key_prefix):
    class Example(JsonModel):
        b: bool = Field(index=True)
        d: datetime.date = Field(index=True)
        name: str = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix

    await Migrator().run()

    ex = Example(b=True, name="steve", d=datetime.date.today())
    exFalse = Example(b=False, name="foo", d=datetime.date.today())
    await ex.save()
    await exFalse.save()
    res = await Example.find(Example.b == True).first()  # noqa: E712
    assert res.name == "steve"

    res = await Example.find(Example.b == False).first()  # noqa: E712
    assert res.name == "foo"

    true_filter = Example.b == True  # noqa: E712
    res = await Example.find((Example.d == ex.d) & true_filter).first()
    assert res.name == ex.name


@py_test_mark_asyncio
async def test_int_pk(key_prefix):
    class ModelWithIntPk(JsonModel):
        my_id: int = Field(index=True, primary_key=True)

        class Meta:
            global_key_prefix = key_prefix

    await Migrator().run()
    await ModelWithIntPk(my_id=42).save()

    m = await ModelWithIntPk.find(ModelWithIntPk.my_id == 42).first()
    assert m.my_id == 42


@py_test_mark_asyncio
async def test_pagination(key_prefix):
    class Test(JsonModel):
        id: str = Field(primary_key=True, index=True)
        num: int = Field(sortable=True, index=True)

        @classmethod
        async def get_page(cls, offset, limit):
            return await cls.find().sort_by("num").page(limit=limit, offset=offset)

        class Meta:
            global_key_prefix = key_prefix

    await Migrator().run()

    pipe = Test.Meta.database.pipeline()
    for i in range(0, 1000):
        await Test(num=i, id=str(i)).save(pipeline=pipe)

    await pipe.execute()
    res = await Test.get_page(100, 100)
    assert len(res) == 100
    assert res[0].num == 100
    res = await Test.get_page(10, 30)
    assert len(res) == 30
    assert res[0].num == 10


@py_test_mark_asyncio
async def test_literals(key_prefix):
    from typing import Literal

    class TestLiterals(JsonModel):
        flavor: Literal["apple", "pumpkin"] = Field(index=True, default="apple")

        class Meta:
            global_key_prefix = key_prefix

    schema = TestLiterals.redisearch_schema()

    expected_schema_prefix = TestLiterals.make_key(
        TestLiterals._meta.primary_key_pattern.format(pk="")
    )
    assert schema == (
        f"ON JSON PREFIX 1 {expected_schema_prefix} SCHEMA $.pk AS pk TAG SEPARATOR | "
        "$.flavor AS flavor TAG SEPARATOR |"
    )
    await Migrator().run()

    item = TestLiterals(flavor="pumpkin")
    await item.save()
    rematerialized = await TestLiterals.find(TestLiterals.flavor == "pumpkin").first()
    assert rematerialized.pk == item.pk


@py_test_mark_asyncio
async def test_can_search_on_coordinates(key_prefix, redis):
    class Location(JsonModel, index=True):
        coordinates: Coordinates = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    await Migrator().run()

    latitude = 45.5231
    longitude = -122.6765

    loc = Location(coordinates=(latitude, longitude))

    await loc.save()

    rematerialized: Location = await Location.find(
        Location.coordinates
        == GeoFilter(longitude=longitude, latitude=latitude, radius=10, unit="mi")
    ).first()

    assert rematerialized.pk == loc.pk
    assert rematerialized.coordinates.latitude == latitude
    assert rematerialized.coordinates.longitude == longitude


@py_test_mark_asyncio
async def test_does_not_return_coordinates_if_outside_radius(key_prefix, redis):
    class Location(JsonModel, index=True):
        coordinates: Coordinates = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    await Migrator().run()

    latitude = 45.5231
    longitude = -122.6765

    loc = Location(coordinates=(latitude, longitude))

    await loc.save()

    with pytest.raises(NotFoundError):
        await Location.find(
            Location.coordinates
            == GeoFilter(longitude=0, latitude=0, radius=0.1, unit="mi")
        ).first()


@py_test_mark_asyncio
async def test_does_not_return_coordinates_if_location_is_none(key_prefix, redis):
    class Location(JsonModel, index=True):
        coordinates: Optional[Coordinates] = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    await Migrator().run()

    loc = Location(coordinates=None)

    await loc.save()

    with pytest.raises(NotFoundError):
        await Location.find(
            Location.coordinates
            == GeoFilter(longitude=0, latitude=0, radius=0.1, unit="mi")
        ).first()


@py_test_mark_asyncio
async def test_can_search_on_multiple_fields_with_geo_filter(key_prefix, redis):
    class Location(JsonModel, index=True):
        coordinates: Coordinates = Field(index=True)
        name: str = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    await Migrator().run()

    latitude = 45.5231
    longitude = -122.6765

    loc1 = Location(coordinates=(latitude, longitude), name="Portland")
    # Offset by 0.01 degrees (~1.1 km at this latitude) to create a nearby location
    # This ensures "Nearby" is within the 10 mile search radius but not at the exact same location
    loc2 = Location(coordinates=(latitude + 0.01, longitude + 0.01), name="Nearby")

    await loc1.save()
    await loc2.save()

    rematerialized: List[Location] = await Location.find(
        (
            Location.coordinates
            == GeoFilter(longitude=longitude, latitude=latitude, radius=10, unit="mi")
        )
        & (Location.name == "Portland")
    ).all()

    assert len(rematerialized) == 1
    assert rematerialized[0].pk == loc1.pk


@py_test_mark_asyncio
async def test_merged_model_error(key_prefix, redis):
    """Test that OR queries on two embedded models produce correct field prefixes (#657)."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Player(EmbeddedJsonModel):
        username: str = Field(index=True)
        score: int = Field(index=True)

    class Game(BaseJsonModel, index=True):
        name: str = Field(index=True)
        player1: Player
        player2: Player

    await Migrator().run()

    game1 = Game(
        name="Game1",
        player1=Player(username="alice", score=100),
        player2=Player(username="bob", score=200),
    )
    game2 = Game(
        name="Game2",
        player1=Player(username="charlie", score=150),
        player2=Player(username="dave", score=250),
    )
    game3 = Game(
        name="Game3",
        player1=Player(username="alice", score=300),
        player2=Player(username="eve", score=400),
    )

    await game1.save()
    await game2.save()
    await game3.save()

    results = await Game.find(
        (Game.player1.username == "alice") | (Game.player2.username == "eve")
    ).all()
    assert len(results) == 2
    game_names = {r.name for r in results}
    assert game_names == {"Game1", "Game3"}

    results = await Game.find(
        (Game.player1.score >= 200) | (Game.player2.score < 300)
    ).all()
    assert len(results) == 3


@py_test_mark_asyncio
async def test_bytes_field_with_binary_data(key_prefix, redis):
    """Test storing/retrieving non-UTF8 bytes data (#783)."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class BinaryData(BaseJsonModel, index=True):
        name: str = Field(index=True)
        data: bytes
        optional_data: Optional[bytes] = None

    await Migrator().run()

    png_header = b"\x89PNG\r\n\x1a\n"
    binary_data = b"\x00\x01\x02\x03\xff\xfe\xfd\xfc"

    doc1 = BinaryData(name="png_header", data=png_header)
    doc2 = BinaryData(name="binary_data", data=binary_data)
    doc3 = BinaryData(name="with_optional", data=png_header, optional_data=binary_data)
    doc4 = BinaryData(name="none_optional", data=png_header, optional_data=None)

    await doc1.save()
    await doc2.save()
    await doc3.save()
    await doc4.save()

    retrieved1 = await BinaryData.get(doc1.pk)
    assert retrieved1.name == "png_header"
    assert retrieved1.data == png_header

    retrieved2 = await BinaryData.get(doc2.pk)
    assert retrieved2.name == "binary_data"
    assert retrieved2.data == binary_data

    retrieved3 = await BinaryData.get(doc3.pk)
    assert retrieved3.name == "with_optional"
    assert retrieved3.data == png_header
    assert retrieved3.optional_data == binary_data

    retrieved4 = await BinaryData.get(doc4.pk)
    assert retrieved4.name == "none_optional"
    assert retrieved4.data == png_header
    assert retrieved4.optional_data is None


@py_test_mark_asyncio
async def test_optional_bytes_field(key_prefix, redis):
    """Test Optional[bytes] with None and binary data (#783)."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class OptionalBinaryModel(BaseJsonModel, index=True):
        name: str = Field(index=True)
        data: Optional[bytes] = None

    await Migrator().run()

    doc1 = OptionalBinaryModel(name="none_value", data=None)
    await doc1.save()

    retrieved1 = await OptionalBinaryModel.get(doc1.pk)
    assert retrieved1.name == "none_value"
    assert retrieved1.data is None

    binary_content = b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
    doc2 = OptionalBinaryModel(name="binary_value", data=binary_content)
    await doc2.save()

    retrieved2 = await OptionalBinaryModel.get(doc2.pk)
    assert retrieved2.name == "binary_value"
    assert retrieved2.data == binary_content


@py_test_mark_asyncio
async def test_pep604_optional_bytes_field(key_prefix, redis):
    """PEP 604 `bytes | None` must round-trip like Optional[bytes] (#783 regression)."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Doc(BaseJsonModel, index=True):
        name: str = Field(index=True)
        data: bytes | None = None
        when: datetime.datetime | None = None

    await Migrator().run()

    binary_content = b"\x89PNG\r\n\x1a\n"
    now = datetime.datetime(2025, 1, 2, 3, 4, 5)
    saved = Doc(name="binary", data=binary_content, when=now)
    await saved.save()

    retrieved = await Doc.get(saved.pk)
    assert retrieved.data == binary_content
    assert retrieved.when == now

    none_doc = Doc(name="none", data=None, when=None)
    await none_doc.save()
    none_retrieved = await Doc.get(none_doc.pk)
    assert none_retrieved.data is None
    assert none_retrieved.when is None


@py_test_mark_asyncio
async def test_list_of_embedded_json_model_indexed_string_fields(key_prefix, redis):
    """Regression for issues #244/#480: JsonModel with List[EmbeddedJsonModel].

    String fields inside the embedded model marked ``index=True`` must appear
    in the RediSearch schema and be queryable through the parent, including
    by the issue #480 containment-style query form.
    """

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Item(EmbeddedJsonModel):
        name: str = Field(index=True)
        sku: str = Field(index=True)
        description: str = ""

    class Order(BaseJsonModel, index=True):
        customer: str = Field(index=True)
        items: List[Item] = Field(index=True)

    schema = Order.redisearch_schema()
    assert "$.items[*].name AS items_name TAG" in schema
    assert "$.items[*].sku AS items_sku TAG" in schema

    await Migrator().run()

    o1 = Order(
        customer="alice",
        items=[Item(name="apple", sku="A1"), Item(name="pear", sku="P1")],
    )
    o2 = Order(customer="bob", items=[Item(name="banana", sku="B1")])
    await o1.save()
    await o2.save()

    by_name = await Order.find(Order.items.name == "apple").all()
    assert len(by_name) == 1 and by_name[0].pk == o1.pk

    combined = await Order.find(
        (Order.customer == "alice") & (Order.items.sku == "P1")
    ).all()
    assert len(combined) == 1 and combined[0].pk == o1.pk

    contained_by_dict = await Order.find(Order.items << {"name": "apple"}).all()
    assert len(contained_by_dict) == 1 and contained_by_dict[0].pk == o1.pk

    contained_by_model = await Order.find(
        Order.items << Item(name="pear", sku="P1")
    ).all()
    assert len(contained_by_model) == 1 and contained_by_model[0].pk == o1.pk

    contained_by_any = await Order.find(
        Order.items << [{"name": "banana"}, {"sku": "P1"}]
    ).all()
    assert {order.pk for order in contained_by_any} == {o1.pk, o2.pk}

    with pytest.raises(QueryNotSupportedError):
        await Order.find(Order.items << {"description": "not indexed"}).all()

    with pytest.raises(QuerySyntaxError):
        await Order.find(Order.items << []).all()


@py_test_mark_asyncio
async def test_bytes_field_in_embedded_model(key_prefix, redis):
    """Test bytes inside EmbeddedJsonModel (#783)."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class BinaryContent(EmbeddedJsonModel):
        content_type: str
        data: bytes
        metadata: Optional[bytes] = None

    class Document(BaseJsonModel, index=True):
        title: str = Field(index=True)
        content: BinaryContent

    await Migrator().run()

    pdf_header = b"%PDF-1.4\n"
    metadata = b"\x00\x01\x02\x03"

    doc = Document(
        title="PDF Document",
        content=BinaryContent(
            content_type="application/pdf", data=pdf_header, metadata=metadata
        ),
    )

    await doc.save()

    retrieved = await Document.get(doc.pk)
    assert retrieved.title == "PDF Document"
    assert retrieved.content.content_type == "application/pdf"
    assert retrieved.content.data == pdf_header
    assert retrieved.content.metadata == metadata


def test_embedded_model_pk_not_in_model_dump():
    """EmbeddedJsonModel pk must never appear in model_dump, even if set."""

    class Inner(EmbeddedJsonModel):
        name: str

    class Outer(JsonModel):
        inner: Inner

    inner = Inner(name="test")
    # pk should be None for embedded models
    assert inner.pk is None
    dumped = inner.model_dump()
    assert "pk" not in dumped

    # Even if a validator sets pk internally, it must still be excluded
    class InnerWithPk(EmbeddedJsonModel):
        name: str

        def model_post_init(self, __context):
            super().model_post_init(__context)
            object.__setattr__(self, "pk", "forced_pk")

    inner2 = InnerWithPk(name="test")
    assert inner2.pk == "forced_pk"
    dumped2 = inner2.model_dump()
    assert "pk" not in dumped2

    outer = Outer(inner=inner2)
    outer_dumped = outer.model_dump()
    assert "pk" not in outer_dumped["inner"]


def test_json_model_pk_generated_but_embedded_pk_none():
    """JsonModel gets an auto-generated pk; EmbeddedJsonModel pk stays None."""

    class Inner(EmbeddedJsonModel):
        value: int

    class Outer(JsonModel):
        inner: Inner

    outer = Outer(inner=Inner(value=1))
    assert outer.pk is not None
    assert isinstance(outer.pk, str)
    assert outer.inner.pk is None

    dumped = outer.model_dump()
    assert "pk" in dumped
    assert dumped["pk"] == outer.pk
    assert "pk" not in dumped["inner"]


def test_nested_embedded_model_pk_exclusion():
    """Deeply nested embedded models must also exclude pk from dumps."""

    class Level2(EmbeddedJsonModel):
        data: str

    class Level1(EmbeddedJsonModel):
        level2: Level2

    class Root(JsonModel):
        level1: Level1

    root = Root(level1=Level1(level2=Level2(data="deep")))
    dumped = root.model_dump()
    assert "pk" not in dumped["level1"]
    assert "pk" not in dumped["level1"]["level2"]


def test_embedded_list_pk_exclusion():
    """List items that are embedded models must exclude pk from dumps."""

    class Item(EmbeddedJsonModel):
        name: str

    class Container(JsonModel):
        items: List[Item] = []

    container = Container(items=[Item(name="a"), Item(name="b")])
    dumped = container.model_dump()
    for item in dumped["items"]:
        assert "pk" not in item


@pytest_asyncio.fixture()
async def member_with_orders(address, m):
    """A saved Member that includes nested embedded models and lists, plus a
    note with a datetime, used to exercise sub-value retrieval."""
    note = m.Note(
        description="A note", created_on=datetime.datetime(2021, 5, 1, 12, 0, 0)
    )
    address_with_note = m.Address(
        address_line_1="1 Main St.",
        city="Portland",
        state="OR",
        country="USA",
        postal_code="11111",
        note=note,
    )
    member = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        age=38,
        join_date=today,
        address=address_with_note,
        orders=[
            m.Order(
                items=[m.Item(price=decimal.Decimal("1.5"), name="item-a")],
                created_on=datetime.datetime(2022, 1, 1, 0, 0, 0),
            ),
            m.Order(
                items=[m.Item(price=decimal.Decimal("2.5"), name="item-b")],
                created_on=datetime.datetime(2022, 2, 1, 0, 0, 0),
            ),
        ],
    )
    await member.save()
    yield member


# ---------------------------------------------------------------------------
# JSON sub-value retrieval (JsonModel.get_value)
# ---------------------------------------------------------------------------


def test_resolve_field_path_builds_targeted_jsonpath(m):
    # A simple scalar nested in an embedded model.
    path, _, crosses = m.Member._resolve_field_path("address__city")
    assert path == "$.address.city"
    assert crosses is False

    # A top-level scalar.
    path, _, crosses = m.Member._resolve_field_path("first_name")
    assert path == "$.first_name"
    assert crosses is False

    # A scalar reached by descending through one or more lists.
    path, _, crosses = m.Member._resolve_field_path("orders__items__name")
    assert path == "$.orders[*].items[*].name"
    assert crosses is True

    # A raw JSONPath is passed through unchanged.
    path, value_type, crosses = m.Member._resolve_field_path("$.address.city")
    assert path == "$.address.city"
    assert value_type is None
    assert crosses is False


@py_test_mark_asyncio
async def test_get_value_scalar(member_with_orders, m):
    value = await m.Member.get_value(member_with_orders.pk, "address__city")
    assert value == "Portland"


@py_test_mark_asyncio
async def test_get_value_top_level_scalar(member_with_orders, m):
    assert await m.Member.get_value(member_with_orders.pk, "first_name") == "Andrew"
    assert await m.Member.get_value(member_with_orders.pk, "age") == 38


@py_test_mark_asyncio
async def test_get_value_converts_date(member_with_orders, m):
    value = await m.Member.get_value(member_with_orders.pk, "join_date")
    assert value == today
    assert isinstance(value, datetime.date)


@py_test_mark_asyncio
async def test_get_value_converts_nested_datetime(member_with_orders, m):
    value = await m.Member.get_value(member_with_orders.pk, "address__note__created_on")
    assert value == datetime.datetime(2021, 5, 1, 12, 0, 0)
    assert isinstance(value, datetime.datetime)


@py_test_mark_asyncio
async def test_get_value_embedded_model_returns_dict(member_with_orders, m):
    value = await m.Member.get_value(member_with_orders.pk, "address")
    assert value["city"] == "Portland"
    # Nested datetimes within the embedded model are converted too.
    assert value["note"]["created_on"] == datetime.datetime(2021, 5, 1, 12, 0, 0)


@py_test_mark_asyncio
async def test_get_value_list_path_returns_list(member_with_orders, m):
    value = await m.Member.get_value(member_with_orders.pk, "orders__created_on")
    assert value == [
        datetime.datetime(2022, 1, 1, 0, 0, 0),
        datetime.datetime(2022, 2, 1, 0, 0, 0),
    ]


@py_test_mark_asyncio
async def test_get_value_nested_list_path_returns_flattened_list(member_with_orders, m):
    value = await m.Member.get_value(member_with_orders.pk, "orders__items__name")
    assert value == ["item-a", "item-b"]


@py_test_mark_asyncio
async def test_get_value_raw_jsonpath(member_with_orders, m):
    assert (
        await m.Member.get_value(member_with_orders.pk, "$.address.city") == "Portland"
    )


@py_test_mark_asyncio
async def test_get_value_raw_flag_returns_unwrapped_list(member_with_orders, m):
    # With raw=True we get back exactly what Redis returns: a list of matches
    # for the enhanced JSONPath, with no conversion or unwrapping.
    assert await m.Member.get_value(
        member_with_orders.pk, "address__city", raw=True
    ) == ["Portland"]


@py_test_mark_asyncio
async def test_get_value_missing_optional_scalar_returns_none(address, m):
    member = m.Member(
        first_name="Kim",
        last_name="Brookins",
        join_date=today,
        address=address,
    )
    await member.save()
    assert await m.Member.get_value(member.pk, "age") is None


@py_test_mark_asyncio
async def test_get_value_missing_list_returns_empty_list(address, m):
    member = m.Member(
        first_name="Kim",
        last_name="Brookins",
        join_date=today,
        address=address,
    )
    await member.save()
    assert await m.Member.get_value(member.pk, "orders__created_on") == []


@py_test_mark_asyncio
async def test_get_value_not_found_raises(m):
    with pytest.raises(NotFoundError):
        await m.Member.get_value("does-not-exist", "address__city")


@py_test_mark_asyncio
async def test_get_value_invalid_field_raises(member_with_orders, m):
    with pytest.raises(QuerySyntaxError):
        await m.Member.get_value(member_with_orders.pk, "address__nonexistent")


@py_test_mark_asyncio
async def test_get_value_descend_into_non_model_raises(member_with_orders, m):
    with pytest.raises(QuerySyntaxError):
        # first_name is a string, not an embedded model.
        await m.Member.get_value(member_with_orders.pk, "first_name__foo")


@py_test_mark_asyncio
async def test_get_value_converts_bytes(key_prefix, redis):
    class BaseModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Blob(BaseModel):
        payload: bytes

    await Migrator().run()
    blob = Blob(payload=b"\x00\x01\x02binary")
    await blob.save()
    assert await Blob.get_value(blob.pk, "payload") == b"\x00\x01\x02binary"


@py_test_mark_asyncio
async def test_get_value_requests_only_the_subpath(member_with_orders, m):
    """get_value must fetch only the targeted JSONPath via JSON.GET, not the
    whole document at "$"."""
    conn = m.Member.db()
    original_json = conn.json
    captured_paths = []

    def spy_json(*args, **kwargs):
        client = original_json(*args, **kwargs)
        original_get = client.get

        def get(name, *get_args, **get_kwargs):
            captured_paths.append(list(get_args))
            return original_get(name, *get_args, **get_kwargs)

        client.get = get
        return client

    with mock.patch.object(conn, "json", spy_json):
        value = await m.Member.get_value(member_with_orders.pk, "address__city")

    assert value == "Portland"
    # The targeted JSONPath was passed to JSON.GET; the whole document ("$")
    # was never requested.
    assert ["$.address.city"] in captured_paths
    assert ["$"] not in captured_paths
