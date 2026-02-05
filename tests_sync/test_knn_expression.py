# type: ignore
import abc
import struct
from typing import Optional, Type

import pytest

from redis_om import Field, JsonModel, KNNExpression, Migrator, VectorFieldOptions

from .conftest import py_test_mark_sync


DIMENSIONS = 768


vector_field_options = VectorFieldOptions.flat(
    type=VectorFieldOptions.TYPE.FLOAT32,
    dimension=DIMENSIONS,
    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
)


@pytest.fixture
def m(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Member(BaseJsonModel, index=True):
        name: str
        embeddings: list[float] = Field([], vector_options=vector_field_options)
        embeddings_score: Optional[float] = None

    Migrator().run()

    return Member


@pytest.fixture
def n(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Member(BaseJsonModel, index=True):
        name: str
        nested: list[list[float]] = Field([], vector_options=vector_field_options)
        embeddings_score: Optional[float] = None

    Migrator().run()

    return Member


def to_bytes(vectors: list[float]) -> bytes:
    return struct.pack(f"<{len(vectors)}f", *vectors)


@py_test_mark_sync
def test_vector_field(m: Type[JsonModel]):
    # Create a new instance of the Member model
    vectors = [0.3 for _ in range(DIMENSIONS)]
    member = m(name="seth", embeddings=vectors)

    # Save the member to Redis
    member.save()

    knn = KNNExpression(
        k=1,
        vector_field=m.embeddings,
        score_field=m.embeddings_score,
        reference_vector=to_bytes(vectors),
    )

    query = m.find(knn=knn)

    members = query.all()

    assert len(members) == 1
    assert members[0].embeddings_score is not None


@py_test_mark_sync
def test_nested_vector_field(n: Type[JsonModel]):
    # Create a new instance of the Member model
    vectors = [0.3 for _ in range(DIMENSIONS)]
    member = n(name="seth", nested=[vectors])

    # Save the member to Redis
    member.save()

    knn = KNNExpression(
        k=1,
        vector_field=n.nested,
        score_field=n.embeddings_score,
        reference_vector=to_bytes(vectors),
    )

    query = n.find(knn=knn)

    members = query.all()

    assert len(members) == 1
    assert members[0].embeddings_score is not None


@pytest.fixture
def complex_models(key_prefix, redis):
    """Fixture providing models for testing OR expressions with KNN."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Document(BaseJsonModel, index=True):
        title: str = Field(index=True)
        category: str = Field(index=True)
        embeddings: list[float] = Field([], vector_options=vector_field_options)
        embeddings_score: Optional[float] = None

    Migrator().run()
    return Document


@py_test_mark_sync
def test_or_expression_with_knn(complex_models):
    """Test that OR expressions combined with KNN produce valid syntax (#787)."""
    Document = complex_models

    # Create documents with different categories
    vectors1 = [0.1 for _ in range(DIMENSIONS)]
    vectors2 = [0.9 for _ in range(DIMENSIONS)]
    vectors3 = [0.5 for _ in range(DIMENSIONS)]

    doc1 = Document(title="Doc1", category="tech", embeddings=vectors1)
    doc2 = Document(title="Doc2", category="business", embeddings=vectors2)
    doc3 = Document(title="Doc3", category="tech", embeddings=vectors3)

    doc1.save()
    doc2.save()
    doc3.save()

    # Create KNN expression searching for documents similar to doc1
    knn = KNNExpression(
        k=2,
        vector_field=Document.embeddings,
        score_field=Document.embeddings_score,
        reference_vector=to_bytes(vectors1),
    )

    # Test OR expression: category="tech" OR KNN similarity
    # This should produce valid RediSearch syntax without syntax errors
    query = Document.find((Document.category == "tech") | knn)

    results = query.all()

    # Should find documents that are either in "tech" category OR similar to doc1
    assert len(results) >= 2  # At least doc1 (tech) and one similar doc

    # All results should have embeddings_score set (KNN results) or be in tech category
    for result in results:
        assert result.category == "tech" or result.embeddings_score is not None

    # Test the reverse order: KNN OR category filter
    query2 = Document.find(knn | (Document.category == "business"))

    results2 = query2.all()
    assert len(results2) >= 2  # At least doc2 (business) and one similar doc

    # All results should have embeddings_score set or be in business category
    for result in results2:
        assert result.category == "business" or result.embeddings_score is not None


@py_test_mark_sync
def test_or_expression_with_multiple_knn(key_prefix, redis):
    """Test OR expressions with multiple KNN expressions."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Product(BaseJsonModel, index=True):
        name: str = Field(index=True)
        brand: str = Field(index=True)
        embeddings: list[float] = Field([], vector_options=vector_field_options)
        embeddings_score: Optional[float] = None

    Migrator().run()

    # Create products
    vectors1 = [0.1 for _ in range(DIMENSIONS)]
    vectors2 = [0.9 for _ in range(DIMENSIONS)]

    product1 = Product(name="Phone", brand="Apple", embeddings=vectors1)
    product2 = Product(name="Laptop", brand="Dell", embeddings=vectors2)
    product3 = Product(name="Tablet", brand="Apple", embeddings=vectors1)

    product1.save()
    product2.save()
    product3.save()

    # Two different KNN expressions
    knn1 = KNNExpression(
        k=1,
        vector_field=Product.embeddings,
        score_field=Product.embeddings_score,
        reference_vector=to_bytes(vectors1),
    )

    knn2 = KNNExpression(
        k=1,
        vector_field=Product.embeddings,
        score_field=Product.embeddings_score,
        reference_vector=to_bytes(vectors2),
    )

    # Test: brand="Apple" OR KNN1 OR KNN2
    query = Product.find((Product.brand == "Apple") | knn1 | knn2)

    results = query.all()

    # Should find all products: Apple products OR similar to either vector
    assert len(results) == 3

    # Verify we have the expected results
    result_names = {r.name for r in results}
    assert result_names == {"Phone", "Laptop", "Tablet"}
