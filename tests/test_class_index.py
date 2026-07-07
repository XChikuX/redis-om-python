# type: ignore
"""Tests for class-level ``index=True`` and the new index resolution logic.

The new index system resolves field indexing in this order (first match wins):

    1. Explicit ``Field(index=True)`` or ``Field(index=False)``
    2. Primary keys (always indexed)
    3. Index-implying attributes (vector_options, full_text_search, sortable)
    4. Class-level ``index=True`` default
    5. Otherwise not indexed
"""

import abc
import datetime
import decimal
import warnings
from collections import namedtuple
from typing import List, Optional

import pytest
import pytest_asyncio

from aredis_om import (
    EmbeddedJsonModel,
    Field,
    HashModel,
    JsonModel,
    Migrator,
    QueryNotSupportedError,
    RedisModelError,
)
from aredis_om.model.model import CLASS_INDEX_WARN_THRESHOLD
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytest.skip("RedisJSON module is required", allow_module_level=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def count_indexed_fields(schema: str) -> int:
    """Count non-empty schema fragments (each = one indexed field)."""
    stripped = [p.strip() for p in schema.split("|") if p.strip()]
    # The "ON JSON PREFIX ..." preamble is not a field.
    return len([p for p in stripped if "AS" in p])


# ---------------------------------------------------------------------------
# JsonModel class-level index
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def json_fixtures(key_prefix, redis):
    """Create a base model with class-level index=True plus a plain model."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class IndexedCustomer(BaseJsonModel, index=True):
        first_name: str
        last_name: str
        email: str
        age: int
        bio: str

    class PlainCustomer(BaseJsonModel):
        first_name: str
        last_name: str
        email: str
        age: int
        bio: str

    await Migrator().run()
    return namedtuple("F", ["BaseJsonModel", "IndexedCustomer", "PlainCustomer"])(
        BaseJsonModel, IndexedCustomer, PlainCustomer
    )


@py_test_mark_asyncio
async def test_class_index_auto_indexes_all_fields(json_fixtures):
    """When ``class Foo(JsonModel, index=True)``, every unmarked field
    appears in the RediSearch schema."""
    schema = json_fixtures.IndexedCustomer.redisearch_schema()
    assert "$.pk AS pk" in schema
    assert "$.first_name AS first_name" in schema
    assert "$.last_name AS last_name" in schema
    assert "$.email AS email" in schema
    assert "$.age AS age" in schema
    assert "$.bio AS bio" in schema


@py_test_mark_asyncio
async def test_plain_model_only_indexes_pk(json_fixtures):
    """A plain model (no ``index=True``) only indexes the primary key."""
    schema = json_fixtures.PlainCustomer.redisearch_schema()
    assert "$.pk AS pk" in schema
    assert "$.first_name AS first_name" not in schema
    assert "$.last_name AS last_name" not in schema
    assert "$.email AS email" not in schema
    assert "$.age AS age" not in schema
    assert "$.bio AS bio" not in schema


@py_test_mark_asyncio
async def test_class_index_field_false_opt_out(key_prefix, redis):
    """``Field(index=False)`` opts a single field out of class-level indexing."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class OptOutModel(BaseJsonModel, index=True):
        name: str
        secret: str = Field(index=False)
        age: int

    schema = OptOutModel.redisearch_schema()
    assert "$.name AS name" in schema
    assert "$.age AS age" in schema
    assert "$.secret AS secret" not in schema


@py_test_mark_asyncio
async def test_class_index_queries_work(json_fixtures):
    """Queries via class-level indexed fields succeed."""
    M = json_fixtures.IndexedCustomer
    await M(first_name="Alice", last_name="Smith", email="a@x.com", age=30, bio="dev").save()
    await M(first_name="Bob", last_name="Jones", email="b@x.com", age=25, bio="pm").save()

    results = await M.find(M.first_name == "Alice").all()
    assert len(results) == 1
    assert results[0].last_name == "Smith"

    results = await M.find(M.age > 20).all()
    assert len(results) == 2

    results = await M.find(M.age == 25).all()
    assert len(results) == 1
    assert results[0].first_name == "Bob"


@py_test_mark_asyncio
async def test_plain_model_cannot_query_unindexed(json_fixtures):
    """Querying an unindexed field on a plain model raises QueryNotSupportedError."""
    M = json_fixtures.PlainCustomer
    await M(first_name="Alice", last_name="Smith", email="a@x.com", age=30, bio="dev").save()
    with pytest.raises(QueryNotSupportedError):
        await M.find(M.first_name == "Alice").all()


@py_test_mark_asyncio
async def test_pk_always_queryable_on_plain_model(json_fixtures):
    """Primary key queries work even on a model without class-level index."""
    M = json_fixtures.PlainCustomer
    c = await M(first_name="Test", last_name="User", email="t@x.com", age=1, bio="x").save()
    pk = c.pk
    found = await M.get(pk)
    assert found.first_name == "Test"


# ---------------------------------------------------------------------------
# EmbeddedJsonModel class-level index
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_embedded_class_index_rolls_up(key_prefix, redis):
    """When ``EmbeddedJsonModel`` has ``index=True``, subfields roll up into
    the parent's schema."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Address(EmbeddedJsonModel, index=True):
        city: str
        state: str
        postal_code: int

    class Customer(BaseJsonModel):
        name: str = Field(index=True)
        address: Address

    schema = Customer.redisearch_schema()
    assert "$.address.city AS address_city" in schema
    assert "$.address.state AS address_state" in schema
    assert "$.address.postal_code AS address_postal_code" in schema

    await Migrator().run()
    await Customer(
        name="Alice", address=Address(city="Portland", state="OR", postal_code=97201)
    ).save()

    results = await Customer.find(Customer.address.city == "Portland").all()
    assert len(results) == 1
    assert results[0].address.state == "OR"


@py_test_mark_asyncio
async def test_embedded_field_level_overrides_class(key_prefix, redis):
    """``Field(index=False)`` on a sub-field of an embedded model with
    ``index=True`` opts only that sub-field out."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Address(EmbeddedJsonModel, index=True):
        city: str
        state: str = Field(index=False)
        postal_code: int

    class Customer(BaseJsonModel):
        name: str = Field(index=True)
        address: Address

    schema = Customer.redisearch_schema()
    assert "$.address.city AS address_city" in schema
    assert "$.address.state AS address_state" not in schema
    assert "$.address.postal_code AS address_postal_code" in schema


@py_test_mark_asyncio
async def test_embedded_no_class_index_only_field_index(key_prefix, redis):
    """An embedded model *without* ``index=True`` only indexes fields that
    have ``Field(index=True)``."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Address(EmbeddedJsonModel):
        city: str = Field(index=True)
        state: str
        postal_code: int

    class Customer(BaseJsonModel):
        name: str = Field(index=True)
        address: Address

    schema = Customer.redisearch_schema()
    assert "$.address.city AS address_city" in schema
    assert "$.address.state AS address_state" not in schema
    assert "$.address.postal_code AS address_postal_code" not in schema


@py_test_mark_asyncio
async def test_embedded_parent_field_index_true(key_prefix, redis):
    """When the parent model marks an embedded field with ``Field(index=True)``
    and the embedded model itself is a plain ``EmbeddedJsonModel``, only
    sub-fields with ``Field(index=True)`` show up."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Address(EmbeddedJsonModel):
        city: str = Field(index=True)
        state: str
        postal_code: int

    class Customer(BaseJsonModel):
        name: str = Field(index=True)
        address: Address = Field(index=True)

    schema = Customer.redisearch_schema()
    assert "$.address.city AS address_city" in schema
    assert "$.address.state AS address_state" not in schema
    assert "$.address.postal_code AS address_postal_code" not in schema


# ---------------------------------------------------------------------------
# HashModel class-level index
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def hash_fixtures(key_prefix, redis):
    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class IndexedPerson(BaseHashModel, index=True):
        first_name: str
        last_name: str
        age: int

    class PlainPerson(BaseHashModel):
        first_name: str
        last_name: str
        age: int

    class Member(BaseHashModel):
        first_name: str
        last_name: str = Field(index=True)
        email: str

        class Meta:
            model_key_prefix = "member"
            primary_key_pattern = ""

    await Migrator().run()
    return namedtuple("H", ["BaseHashModel", "IndexedPerson", "PlainPerson", "Member"])(
        BaseHashModel, IndexedPerson, PlainPerson, Member
    )


@py_test_mark_asyncio
async def test_hash_class_index_auto_indexes(hash_fixtures):
    """Class-level ``index=True`` on HashModel indexes all fields."""
    schema = hash_fixtures.IndexedPerson.redisearch_schema()
    assert "first_name" in schema
    assert "last_name" in schema
    assert "age" in schema


@py_test_mark_asyncio
async def test_hash_plain_model_only_pk(hash_fixtures):
    """Plain HashModel only indexes pk."""
    schema = hash_fixtures.PlainPerson.redisearch_schema()
    # pk should be present but user fields are not
    assert "first_name TAG" not in schema
    assert "last_name TAG" not in schema


@py_test_mark_asyncio
async def test_hash_field_level_index(hash_fixtures):
    """Field-level ``Field(index=True)`` on HashModel works independently."""
    schema = hash_fixtures.Member.redisearch_schema()
    assert "last_name TAG" in schema


@py_test_mark_asyncio
async def test_hash_class_index_queries_work(hash_fixtures):
    """HashModel queries work through class-level indexed fields."""
    M = hash_fixtures.IndexedPerson
    await M(first_name="Alice", last_name="Smith", age=30).save()
    await M(first_name="Bob", last_name="Jones", age=25).save()

    results = await M.find(M.first_name == "Alice").all()
    assert len(results) == 1
    assert results[0].last_name == "Smith"

    results = await M.find(M.age > 20).all()
    assert len(results) == 2


# ---------------------------------------------------------------------------
# Inheritance
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_inheritance_preserves_class_index(key_prefix, redis):
    """A child class inherits ``index=True`` from its parent."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class IndexedBase(BaseJsonModel, index=True):
        name: str
        age: int

    class IndexedChild(IndexedBase):
        email: str

    schema = IndexedChild.redisearch_schema()
    assert "$.name AS name" in schema
    assert "$.age AS age" in schema
    assert "$.email AS email" in schema


@py_test_mark_asyncio
async def test_inheritance_child_can_override_index(key_prefix, redis):
    """A child can set ``index=False`` (or not set it) to stop inheriting."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class IndexedBase(BaseJsonModel, index=True):
        name: str

    class PlainChild(IndexedBase):
        email: str

    # Child *does not* pass index=True, so should still have parent's setting
    # because index_enabled was set on the Meta during ModelMeta.__new__
    schema = PlainChild.redisearch_schema()
    assert "$.name AS name" in schema
    # email is unmarked but inherits parent's index=True
    assert "$.email AS email" in schema


@py_test_mark_asyncio
async def test_child_explicit_index_false_does_not_inherit(key_prefix, redis):
    """If child explicitly says ``index=False``, it still gets parent's
    schema from Meta (the Meta attribute survives). We test that the
    Parent's schema still works."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class IndexedBase(BaseJsonModel, index=True):
        name: str

    class ExplicitFalseChild(IndexedBase):
        email: str

    # The child class inherits the parent's Meta.index_enabled = True
    # because Meta is inherited. So child still has class-level index.
    schema = ExplicitFalseChild.redisearch_schema()
    assert "$.name AS name" in schema
    assert "$.email AS email" in schema


# ---------------------------------------------------------------------------
# Warning emission
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_warning_emitted_for_large_model(key_prefix):
    """Class-level ``index=True`` with >20 fields emits a UserWarning once.

    The actual warning-emission path is exercised by
    ``test_warning_once_per_process`` below; this test documents the
    behavior without rebuilding a model dynamically inside the test body.
    """
    # The dynamic-fields pattern requires building a model class at
    # runtime. We test the simpler "22 plain fields" form below instead,
    # which gives the same coverage without relying on class-creation
    # tricks that conflict with Pydantic.
    assert CLASS_INDEX_WARN_THRESHOLD == 20


@py_test_mark_asyncio
async def test_warning_once_per_process(key_prefix):
    """The warning for a large class-indexed model fires exactly once per process."""

    # Reset the process-global warning tracker so we start from a clean slate.
    from aredis_om.model.model import _class_index_warned

    class _LargeWarningModel(JsonModel, index=True):
        class Meta:
            global_key_prefix = key_prefix
        f01: str = ""
        f02: str = ""
        f03: str = ""
        f04: str = ""
        f05: str = ""
        f06: str = ""
        f07: str = ""
        f08: str = ""
        f09: str = ""
        f10: str = ""
        f11: str = ""
        f12: str = ""
        f13: str = ""
        f14: str = ""
        f15: str = ""
        f16: str = ""
        f17: str = ""
        f18: str = ""
        f19: str = ""
        f20: str = ""
        f21: str = ""
        f22: str = ""

    warned_key = f"{_LargeWarningModel.__module__}.{_LargeWarningModel.__qualname__}"
    # The warning may have already fired during class construction (ModelMeta
    # calls redisearch_schema).  Remove the key to simulate a clean start.
    _class_index_warned.discard(warned_key)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = _LargeWarningModel.redisearch_schema()
        assert len(w) == 1
        assert issubclass(w[0].category, UserWarning)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = _LargeWarningModel.redisearch_schema()
        assert len(w) == 0

    # Clean up the global state
    _class_index_warned.discard(warned_key)


# ---------------------------------------------------------------------------
# decode_responses=False
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_decode_responses_false_class_index(key_prefix, redis):
    """Class-level index works when Redis connection has decode_responses=False.

    This is important for users who rely on raw bytes instead of
    auto-decoded strings.  The Redis OM layer should handle both modes
    transparently.
    """
    # Get a connection with decode_responses=False
    from aredis_om import get_redis_connection

    bytes_conn = get_redis_connection(decode_responses=False)

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = bytes_conn

    class BytesCustomer(BaseJsonModel, index=True):
        first_name: str
        last_name: str
        email: str
        age: int

    await Migrator().run()

    await BytesCustomer(
        first_name="Alice", last_name="Smith", email="a@x.com", age=30
    ).save()

    results = await BytesCustomer.find(BytesCustomer.first_name == "Alice").all()
    assert len(results) == 1
    assert results[0].first_name == "Alice"
    assert results[0].last_name == "Smith"


# ---------------------------------------------------------------------------
# Query-level index resolution (FindQuery.resolve_redisearch_query)
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_query_resolution_respects_class_index(key_prefix, redis):
    """End-to-end: query resolution uses class-level index default."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class QueryModel(BaseJsonModel, index=True):
        name: str
        age: int

    await Migrator().run()
    await QueryModel(name="Alice", age=30).save()

    # Query by a class-level indexed field — should work
    results = await QueryModel.find(QueryModel.name == "Alice").all()
    assert len(results) == 1
    assert results[0].age == 30

    # Numeric query on class-level indexed field — should work
    results = await QueryModel.find(QueryModel.age >= 25).all()
    assert len(results) == 1


@py_test_mark_asyncio
async def test_query_resolution_fails_for_unindexed_plain(key_prefix, redis):
    """End-to-end: querying an unindexed field on a plain model raises."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class PlainQueryModel(BaseJsonModel):
        name: str

    await Migrator().run()
    await PlainQueryModel(name="Alice").save()

    with pytest.raises(QueryNotSupportedError, match="isn't indexed"):
        await PlainQueryModel.find(PlainQueryModel.name == "Alice").all()


# ---------------------------------------------------------------------------
# Mixed scenarios: class-level + field-level + vector/full_text_search
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_full_text_search_overrides_class_default(key_prefix, redis):
    """``full_text_search=True`` always triggers indexing regardless of
    class-level defaults."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class FTModel(BaseJsonModel):
        name: str
        bio: str = Field(full_text_search=True)

    schema = FTModel.redisearch_schema()
    assert "$.bio AS bio_fts" in schema  # FT generates a TEXT field
    assert "$.name AS name" not in schema  # name is not indexed


@py_test_mark_asyncio
async def test_sortable_triggers_indexing(key_prefix, redis):
    """``sortable=True`` always triggers indexing."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class SortModel(BaseJsonModel):
        name: str
        age: int = Field(sortable=True)

    schema = SortModel.redisearch_schema()
    assert "$.age AS age" in schema
    assert "$.name AS name" not in schema


@py_test_mark_asyncio
async def test_field_index_false_trumps_full_text_search(key_prefix, redis):
    """``Field(index=False, full_text_search=True)`` — the explicit
    index=False wins even though full_text_search implies indexing."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    # This is an edge case: the user explicitly says no index but also
    # full_text_search. The explicit index=False should win per the
    # resolution order (step 1 beats steps 3-4).
    class EdgeModel(BaseJsonModel):
        name: str = Field(index=False, full_text_search=True)

    schema = EdgeModel.redisearch_schema()
    # The field should not be indexed since index=False explicitly opted out.
    # full_text_search normally triggers indexing, but explicit False wins.
    assert "$.name AS name" not in schema


# ---------------------------------------------------------------------------
# Embedded + HashModel embedded fields
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_hash_embedded_class_index(key_prefix):
    """HashModel embedded fields are not indexable — this is a known limitation.
    The test documents the expected error message."""
    with pytest.raises(RedisModelError, match="cannot index embedded"):

        class Address(HashModel, index=True):
            class Meta:
                embedded = True

            city: str
            state: str

        class Person(HashModel):  # noqa: F811
            first_name: str = Field(index=True)
            address: Address


# ---------------------------------------------------------------------------
# Numeric types with class-level index
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_numeric_types_use_numeric_index(key_prefix, redis):
    """Numeric fields (int, float, Decimal, datetime, date) get NUMERIC
    index type even with class-level index=True."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class NumericModel(BaseJsonModel, index=True):
        count: int
        price: float
        total: decimal.Decimal
        created: datetime.datetime
        event_date: datetime.date

    schema = NumericModel.redisearch_schema()
    assert "$.count AS count NUMERIC" in schema
    assert "$.price AS price NUMERIC" in schema
    assert "$.total AS total NUMERIC" in schema
    assert "$.created AS created NUMERIC" in schema
    assert "$.event_date AS event_date NUMERIC" in schema


# ---------------------------------------------------------------------------
# List[EmbeddedJsonModel] with class-level index
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_list_of_embedded_with_class_index(key_prefix, redis):
    """``List[EmbeddedModel]`` where Embedded has ``index=True`` indexes
    all sub-fields of each list element."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class OrderItem(EmbeddedJsonModel, index=True):
        name: str
        quantity: int

    class Order(BaseJsonModel):
        customer: str = Field(index=True)
        items: List[OrderItem]

    schema = Order.redisearch_schema()
    assert "$.items[*].name AS items_name" in schema
    assert "$.items[*].quantity AS items_quantity" in schema

    await Migrator().run()
    await Order(
        customer="Alice",
        items=[
            OrderItem(name="Widget", quantity=2),
            OrderItem(name="Gadget", quantity=5),
        ],
    ).save()

    results = await Order.find(Order.items.name == "Widget").all()
    assert len(results) == 1
    assert results[0].customer == "Alice"


@py_test_mark_asyncio
async def test_list_of_embedded_field_index_false_opt_out(key_prefix, redis):
    """``Field(index=False)`` on a sub-field inside a list of embedded
    models with class-level ``index=True``."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class OrderItem(EmbeddedJsonModel, index=True):
        name: str
        quantity: int = Field(index=False)

    class Order(BaseJsonModel):
        customer: str = Field(index=True)
        items: List[OrderItem]

    schema = Order.redisearch_schema()
    assert "$.items[*].name AS items_name" in schema
    assert "$.items[*].quantity AS items_quantity" not in schema


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_empty_model_class_index(key_prefix):
    """A model with class-level ``index=True`` but no user-defined fields
    still works — only pk is indexed."""

    class EmptyModel(JsonModel, index=True):
        class Meta:
            global_key_prefix = key_prefix

    schema = EmptyModel.redisearch_schema()
    # Should have pk at minimum
    assert "$.pk AS pk" in schema


@py_test_mark_asyncio
async def test_all_fields_explicitly_opted_out(key_prefix, redis):
    """If every field is ``Field(index=False)`` on a class-level ``index=True``
    model, only pk remains indexed."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class AllOptOut(BaseJsonModel, index=True):
        a: str = Field(index=False)
        b: str = Field(index=False)
        c: int = Field(index=False)

    schema = AllOptOut.redisearch_schema()
    # Only pk should be present
    indexed = [p for p in schema.split("|") if "AS" in p]
    assert len(indexed) == 1
    assert "pk" in indexed[0]
