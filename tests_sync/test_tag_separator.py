# type: ignore
"""Tests for custom TAG field separator functionality (PR #800)."""

import abc
from typing import Optional

import pytest

from redis_om import Field, HashModel, JsonModel, Migrator

# We need to run this check as sync code (during tests) even in async mode
from redis_om import has_redisearch

from .conftest import py_test_mark_sync

if not has_redisearch():
    pytestmark = pytest.mark.skip


@pytest.fixture
def models_for_separator(key_prefix, redis):
    """Fixture providing models for testing custom separators."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    # HashModel with custom separator
    class HashDocument(BaseHashModel, index=True):
        name: str = Field(index=True)
        tags: str = Field(index=True, separator=";")
        categories: str = Field(index=True, separator=":")

    # JsonModel with custom separator
    class JsonDocument(BaseJsonModel, index=True):
        title: str = Field(index=True)
        labels: str = Field(index=True, separator=",")
        groups: str = Field(index=True, separator="/")

        # Test with full_text_search
        description: str = Field(index=True, full_text_search=True, separator="|")

    Migrator(conn=redis).run()

    return {
        "HashDocument": HashDocument,
        "JsonDocument": JsonDocument,
    }


@py_test_mark_sync
def test_separator_parameter_accepted():
    """Test that Field() accepts separator parameter."""
    # Should not raise an error
    field1 = Field(index=True, separator=";")
    field2 = Field(index=True, separator=":")
    field3 = Field(index=True)  # Default separator

    assert field1.separator == ";"
    assert field2.separator == ":"
    assert field3.separator == "|"


@py_test_mark_sync
def test_separator_default_value():
    """Test that default separator is |."""
    field = Field(index=True)
    assert field.separator == "|"


@py_test_mark_sync
def test_separator_in_hash_schema(models_for_separator):
    """Test that custom separator appears in HashModel schema."""
    HashDocument = models_for_separator["HashDocument"]

    schema = HashDocument.redisearch_schema()

    # Check that custom separators are in the schema
    assert "tags TAG SEPARATOR ;" in schema
    assert "categories TAG SEPARATOR :" in schema
    # Default separator for name field
    assert "name TAG SEPARATOR |" in schema


@py_test_mark_sync
def test_separator_in_json_schema(models_for_separator):
    """Test that custom separator appears in JsonModel schema."""
    JsonDocument = models_for_separator["JsonDocument"]

    schema = JsonDocument.redisearch_schema()

    # Check that custom separators are in the schema
    assert "labels TAG SEPARATOR ," in schema
    assert "groups TAG SEPARATOR /" in schema
    # Default separator for title field
    assert "title TAG SEPARATOR |" in schema
    # Full-text search field should also have separator
    assert "description TAG SEPARATOR |" in schema


@py_test_mark_sync
def test_separator_save_and_query_hash(models_for_separator):
    """Test end-to-end save/query with custom separator in HashModel."""
    HashDocument = models_for_separator["HashDocument"]

    # Create documents with separator-containing values
    doc1 = HashDocument(name="Doc1", tags="tag1;tag2;tag3", categories="cat1:cat2")
    doc2 = HashDocument(name="Doc2", tags="tag3;tag4", categories="cat3:cat4:cat5")

    doc1.save()
    doc2.save()

    # Query by individual tags
    results = HashDocument.find(HashDocument.tags == "tag1").all()
    assert len(results) == 1
    assert results[0].name == "Doc1"

    results = HashDocument.find(HashDocument.tags == "tag3").all()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Doc1", "Doc2"}

    # Query by individual categories
    results = HashDocument.find(HashDocument.categories == "cat1").all()
    assert len(results) == 1
    assert results[0].name == "Doc1"

    results = HashDocument.find(HashDocument.categories == "cat4").all()
    assert len(results) == 1
    assert results[0].name == "Doc2"


@py_test_mark_sync
def test_separator_save_and_query_json(models_for_separator):
    """Test end-to-end save/query with custom separator in JsonModel."""
    JsonDocument = models_for_separator["JsonDocument"]

    # Create documents with separator-containing values
    doc1 = JsonDocument(
        title="Title1", labels="label1,label2,label3", groups="group1/group2"
    )
    doc2 = JsonDocument(title="Title2", labels="label2,label4", groups="group3/group4")

    doc1.save()
    doc2.save()

    # Query by individual labels
    results = JsonDocument.find(JsonDocument.labels == "label1").all()
    assert len(results) == 1
    assert results[0].title == "Title1"

    results = JsonDocument.find(JsonDocument.labels == "label2").all()
    assert len(results) == 2
    titles = {r.title for r in results}
    assert titles == {"Title1", "Title2"}

    # Query by individual groups
    results = JsonDocument.find(JsonDocument.groups == "group1").all()
    assert len(results) == 1
    assert results[0].title == "Title1"


@py_test_mark_sync
def test_separator_individual_tag_query(models_for_separator):
    """Test querying individual tags with custom separator."""
    HashDocument = models_for_separator["HashDocument"]

    doc = HashDocument(name="MultiTag", tags="a;b;c;d;e", categories="x:y:z")
    doc.save()

    # Each tag should be individually searchable
    for tag in ["a", "b", "c", "d", "e"]:
        results = HashDocument.find(HashDocument.tags == tag).all()
        assert len(results) == 1
        assert results[0].name == "MultiTag"

    # Each category should be individually searchable
    for category in ["x", "y", "z"]:
        results = HashDocument.find(HashDocument.categories == category).all()
        assert len(results) == 1
        assert results[0].name == "MultiTag"


@py_test_mark_sync
def test_separator_with_full_text_search(models_for_separator):
    """Test that separator works alongside full_text_search=True."""
    JsonDocument = models_for_separator["JsonDocument"]

    doc1 = JsonDocument(
        title="FTS Test 1",
        labels="search,fts",
        groups="test/group1",
        description="word1|word2|word3",
    )
    doc2 = JsonDocument(
        title="FTS Test 2",
        labels="other,test",
        groups="test/group2",
        description="word2|word4",
    )

    doc1.save()
    doc2.save()

    # Tag-based queries should work
    results = JsonDocument.find(JsonDocument.labels == "fts").all()
    assert len(results) == 1
    assert results[0].title == "FTS Test 1"

    # Full-text search should work on the same field
    results = JsonDocument.find(JsonDocument.description % "word1").all()
    assert len(results) == 1
    assert results[0].title == "FTS Test 1"

    results = JsonDocument.find(JsonDocument.description % "word2").all()
    assert len(results) == 2
    titles = {r.title for r in results}
    assert titles == {"FTS Test 1", "FTS Test 2"}


@py_test_mark_sync
def test_multiple_fields_different_separators(models_for_separator):
    """Test multiple fields with different separators in the same model."""
    JsonDocument = models_for_separator["JsonDocument"]

    doc = JsonDocument(
        title="Multi Sep Test",
        labels="a,b,c",  # Comma separator
        groups="x/y/z",  # Forward slash separator
        description="p|q|r",  # Pipe separator (default for full-text search)
    )
    doc.save()

    # Test each field with its separator
    results = JsonDocument.find(JsonDocument.labels == "a").all()
    assert len(results) == 1
    assert results[0].title == "Multi Sep Test"

    results = JsonDocument.find(JsonDocument.groups == "y").all()
    assert len(results) == 1
    assert results[0].title == "Multi Sep Test"

    results = JsonDocument.find(JsonDocument.description % "q").all()
    assert len(results) == 1
    assert results[0].title == "Multi Sep Test"


@py_test_mark_sync
def test_primary_key_separator():
    """Test that primary key field uses default separator."""

    class TestModel(JsonModel, index=True):
        pk: str = Field(primary_key=True, index=True)
        tags: str = Field(index=True, separator=";")

        class Meta:
            global_key_prefix = "test"

    schema = TestModel.redisearch_schema()

    # Primary key should use default separator |
    assert "pk TAG SEPARATOR |" in schema
    # Custom field should use custom separator
    assert "tags TAG SEPARATOR ;" in schema


@py_test_mark_sync
def test_separator_query_edge_cases(models_for_separator):
    """Test edge cases with separator queries."""
    HashDocument = models_for_separator["HashDocument"]

    # Test with empty values
    doc1 = HashDocument(name="Empty", tags="", categories="")
    doc1.save()

    # Test with single value (no separator present)
    doc2 = HashDocument(name="Single", tags="solo", categories="single")
    doc2.save()

    # Single value should be queryable
    results = HashDocument.find(HashDocument.tags == "solo").all()
    assert len(results) == 1
    assert results[0].name == "Single"

    results = HashDocument.find(HashDocument.categories == "single").all()
    assert len(results) == 1
    assert results[0].name == "Single"
