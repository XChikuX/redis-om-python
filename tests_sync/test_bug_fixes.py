# type: ignore
"""Tests for bug fixes #108, #254, and #499."""

from enum import Enum, IntEnum
from typing import Optional

import pytest

from redis_om import Field, HashModel, JsonModel, Migrator

# We need to run this check as sync code (during tests) even in async mode
from redis_om import has_redisearch

from .conftest import py_test_mark_sync

if not has_redisearch():
    pytestmark = pytest.mark.skip


class Status(Enum):
    """Regular Enum with int values - this was broken in #108."""

    PENDING = 1
    ACTIVE = 2
    COMPLETED = 3


class Priority(IntEnum):
    """IntEnum - this worked correctly."""

    LOW = 1
    MEDIUM = 2
    HIGH = 3


@pytest.fixture
def models_for_bug_fixes(key_prefix, redis):
    """Fixture providing models for testing bug fixes."""

    # Model for #108 - Enum with int values
    class Task(JsonModel):
        name: str = Field(index=True)
        status: int = Field(index=True)  # Store as int, query with Enum
        priority: int = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    # Model for #254 - Optional field in HashModel
    class Person(HashModel):
        name: str = Field(index=True)
        age: int
        weight: Optional[float] = None
        nickname: Optional[str] = None

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    # Model for #499 - IN operator with NUMERIC fields
    class Product(JsonModel):
        name: str = Field(index=True)
        price: int = Field(index=True)
        quantity: int = Field(index=True)

        class Meta:
            global_key_prefix = key_prefix
            database = redis

    Migrator().run()

    return {
        "Task": Task,
        "Person": Person,
        "Product": Product,
    }


@py_test_mark_sync
def test_enum_int_value_query(models_for_bug_fixes):
    """Test that Enum with int values works in queries (#108)."""
    Task = models_for_bug_fixes["Task"]

    task1 = Task(name="Task 1", status=Status.PENDING.value, priority=Priority.HIGH)
    task2 = Task(name="Task 2", status=Status.ACTIVE.value, priority=Priority.MEDIUM)
    task3 = Task(name="Task 3", status=Status.COMPLETED.value, priority=Priority.LOW)

    task1.save()
    task2.save()
    task3.save()

    results = Task.find(Task.status == Status.ACTIVE).all()
    assert len(results) == 1
    assert results[0].name == "Task 2"

    results = Task.find(Task.priority == Priority.HIGH).all()
    assert len(results) == 1
    assert results[0].name == "Task 1"

    results = Task.find(Task.status >= Status.ACTIVE).all()
    assert len(results) == 2


@py_test_mark_sync
def test_optional_field_none_hashmodel(models_for_bug_fixes):
    """Test that Optional fields with None values can be retrieved (#254)."""
    Person = models_for_bug_fixes["Person"]

    person = Person(name="Joe", age=20, weight=None, nickname=None)
    person.save()

    retrieved = Person.get(person.pk)

    assert retrieved.name == "Joe"
    assert retrieved.age == 20
    assert retrieved.weight is None
    assert retrieved.nickname is None


@py_test_mark_sync
def test_optional_field_with_value_hashmodel(models_for_bug_fixes):
    """Test that Optional fields with actual values still work correctly."""
    Person = models_for_bug_fixes["Person"]

    person = Person(name="Jane", age=25, weight=65.5, nickname="JJ")
    person.save()

    retrieved = Person.get(person.pk)

    assert retrieved.name == "Jane"
    assert retrieved.age == 25
    assert retrieved.weight == 65.5
    assert retrieved.nickname == "JJ"


@py_test_mark_sync
def test_in_operator_numeric_field(models_for_bug_fixes):
    """Test that IN operator works with NUMERIC fields (#499)."""
    Product = models_for_bug_fixes["Product"]

    p1 = Product(name="Widget", price=10, quantity=100)
    p2 = Product(name="Gadget", price=20, quantity=50)
    p3 = Product(name="Gizmo", price=30, quantity=75)
    p4 = Product(name="Doohickey", price=40, quantity=25)

    p1.save()
    p2.save()
    p3.save()
    p4.save()

    results = Product.find(Product.price << [10, 30]).all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Widget", "Gizmo"}

    results = Product.find(Product.quantity << [50, 75, 100]).all()
    assert len(results) == 3
    names = {r.name for r in results}
    assert names == {"Widget", "Gadget", "Gizmo"}


@py_test_mark_sync
def test_enum_int_value_ne_query(models_for_bug_fixes):
    """Test that not-equal query with Enum values works correctly (#792)."""
    Task = models_for_bug_fixes["Task"]

    task1 = Task(name="Task 1", status=Status.PENDING.value, priority=Priority.MEDIUM)
    task2 = Task(name="Task 2", status=Status.ACTIVE.value, priority=Priority.HIGH)
    task3 = Task(name="Task 3", status=Status.COMPLETED.value, priority=Priority.LOW)

    task1.save()
    task2.save()
    task3.save()

    # Not-equal query with enum values
    results = Task.find(Task.status != Status.ACTIVE).all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Task 1", "Task 3"}

    results = Task.find(Task.priority != Priority.HIGH).all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Task 1", "Task 3"}


@py_test_mark_sync
def test_issue_499_in_operator_single_value(models_for_bug_fixes):
    """Test IN operator with a single value."""
    Product = models_for_bug_fixes["Product"]

    p1 = Product(name="Solo", price=99, quantity=1)
    p1.save()

    results = Product.find(Product.price << [99]).all()
    assert len(results) == 1
    assert results[0].name == "Solo"


@py_test_mark_sync
def test_issue_108_enum_with_in_operator(models_for_bug_fixes):
    """Test that Enum values work with IN operator on NUMERIC fields."""
    Task = models_for_bug_fixes["Task"]

    task1 = Task(name="Task A", status=Status.PENDING.value, priority=Priority.LOW)
    task2 = Task(name="Task B", status=Status.ACTIVE.value, priority=Priority.MEDIUM)
    task3 = Task(name="Task C", status=Status.COMPLETED.value, priority=Priority.HIGH)

    task1.save()
    task2.save()
    task3.save()

    results = Task.find(Task.status << [Status.PENDING, Status.COMPLETED]).all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Task A", "Task C"}


@py_test_mark_sync
def test_not_in_operator_numeric_field(models_for_bug_fixes):
    """Test that NOT_IN operator works with NUMERIC fields (#499)."""
    Product = models_for_bug_fixes["Product"]

    p1 = Product(name="Widget", price=10, quantity=100)
    p2 = Product(name="Gadget", price=20, quantity=50)
    p3 = Product(name="Gizmo", price=30, quantity=75)
    p4 = Product(name="Doohickey", price=40, quantity=25)

    p1.save()
    p2.save()
    p3.save()
    p4.save()

    results = Product.find(Product.price >> [20, 40]).all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Widget", "Gizmo"}

    results = Product.find(Product.quantity >> [25, 75]).all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Widget", "Gadget"}
