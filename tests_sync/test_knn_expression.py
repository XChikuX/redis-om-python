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