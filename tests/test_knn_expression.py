# type: ignore
import abc
import struct
from typing import Optional, Type

import pytest
import pytest_asyncio

from aredis_om import (
    Field,
    JsonModel,
    KNNExpression,
    Migrator,
    RedisModelError,
    VectorFieldOptions,
)

# We need to run this check as sync code (during tests) even in async mode
# because we call it in the top-level module scope.
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip

DIMENSIONS = 768


vector_field_options = VectorFieldOptions.flat(
    type=VectorFieldOptions.TYPE.FLOAT32,
    dimension=DIMENSIONS,
    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
)


@pytest_asyncio.fixture
async def m(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Member(BaseJsonModel, index=True):
        name: str
        embeddings: list[float] = Field([], vector_options=vector_field_options)
        # KNN score fields must not be indexed — RediSearch synthesises them
        # at query time.  Marking them with ``index=False`` prevents the
        # class-level ``index=True`` from auto-indexing them, which would
        # cause a name collision.
        embeddings_score: Optional[float] = Field(None, index=False)

    await Migrator().run()

    return Member


@pytest_asyncio.fixture
async def n(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Member(BaseJsonModel, index=True):
        name: str
        nested: list[list[float]] = Field([], vector_options=vector_field_options)
        embeddings_score: Optional[float] = Field(None, index=False)

    await Migrator().run()

    return Member


def to_bytes(vectors: list[float]) -> bytes:
    return struct.pack(f"<{len(vectors)}f", *vectors)


@py_test_mark_asyncio
async def test_vector_field(m: Type[JsonModel]):
    # Create a new instance of the Member model
    vectors = [0.3 for _ in range(DIMENSIONS)]
    member = m(name="seth", embeddings=vectors)

    # Save the member to Redis
    await member.save()

    knn = KNNExpression(
        k=1,
        vector_field=m.embeddings,
        score_field=m.embeddings_score,
        reference_vector=to_bytes(vectors),
    )

    query = m.find(knn=knn)

    members = await query.all()

    assert len(members) == 1
    assert members[0].embeddings_score is not None


@py_test_mark_asyncio
async def test_nested_vector_field(n: Type[JsonModel]):
    # Create a new instance of the Member model
    vectors = [0.3 for _ in range(DIMENSIONS)]
    member = n(name="seth", nested=[vectors])

    # Save the member to Redis
    await member.save()

    knn = KNNExpression(
        k=1,
        vector_field=n.nested,
        score_field=n.embeddings_score,
        reference_vector=to_bytes(vectors),
    )

    query = n.find(knn=knn)

    members = await query.all()

    assert len(members) == 1
    assert members[0].embeddings_score is not None


@pytest_asyncio.fixture
async def album_model(key_prefix, redis):
    """Fixture for testing OR expressions with KNN."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    vector_options = VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=2,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )

    class Album(BaseJsonModel, index=True):
        title: str = Field(primary_key=True)
        tags: str = Field(index=True)
        title_embeddings: list[float] = Field(
            [], index=True, vector_options=vector_options
        )
        embeddings_score: Optional[float] = Field(None, index=False)

    await Migrator(conn=redis).run()

    return Album


@py_test_mark_asyncio
async def test_or_expression_with_knn(album_model):
    """Test that OR expressions work correctly with KNN.

    Regression test for GitHub issue #557: Using an OR expression with a
    KNN expression raises ResponseError with syntax error.
    """
    Album = album_model

    # Create test data
    albums = [
        Album(
            title="Rumours",
            tags="Genre:rock|Decade:70s",
            title_embeddings=[0.7, 0.3],
        ),
        Album(
            title="Abbey Road",
            tags="Genre:rock|Decade:60s",
            title_embeddings=[0.6, 0.4],
        ),
        Album(
            title="The Dark Side Of The Moon",
            tags="Genre:prog-rock|Decade:70s",
            title_embeddings=[0.5, 0.5],
        ),
    ]
    for album in albums:
        await album.save()

    # Create OR expression
    or_expr = (Album.tags == "Genre:rock|Decade:70s") | (
        Album.tags == "Genre:rock|Decade:60s"
    )

    # Create KNN expression
    knn = KNNExpression(
        k=3,
        vector_field=Album.title_embeddings,
        score_field=Album.embeddings_score,
        reference_vector=to_bytes([0.65, 0.35]),
    )

    # Query with just OR expression (should work)
    or_results = await Album.find(or_expr).all()
    assert len(or_results) == 2

    # Query with just KNN (should work)
    knn_results = await Album.find(knn=knn).all()
    assert len(knn_results) == 3

    # Query with OR expression AND KNN (this was failing before the fix)
    combined_results = await Album.find(or_expr, knn=knn).all()
    # Should return only the 2 albums matching the OR expression
    assert len(combined_results) == 2
    # All results should have an embeddings score from KNN
    for result in combined_results:
        assert result.embeddings_score is not None


def test_knn_score_field_collision_raises(key_prefix):
    """When class-level ``index=True`` auto-indexes a score field that KNN
    wants to synthesise, ``Model.find(knn=...)`` should raise a clear
    ``RedisModelError`` at query construction time — not a confusing
    ``Property '...' already exists in schema`` at index time.
    """

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    DIMENSIONS = 4
    opts = VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=DIMENSIONS,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )

    class BadDocument(BaseJsonModel, index=True):
        name: str
        embedding: list[float] = Field([], vector_options=opts)
        # No Field(index=False) — the score field will be auto-indexed by
        # the class-level ``index=True``, causing a name collision with
        # the synthesised KNN score field of the same name.
        embedding_score: Optional[float] = None

    vectors = [0.1 for _ in range(DIMENSIONS)]
    ref = struct.pack(f"<{len(vectors)}f", *vectors)

    knn = KNNExpression(
        k=1,
        vector_field=BadDocument.embedding,
        score_field=BadDocument.embedding_score,
        reference_vector=ref,
    )

    with pytest.raises(RedisModelError) as exc_info:
        BadDocument.find(knn=knn)
    # Error message should mention the score field, the model, and the
    # recommended fix so users can resolve it without reading the source.
    msg = str(exc_info.value)
    assert "embedding_score" in msg
    assert "BadDocument" in msg
    assert "Field(index=False)" in msg


def test_knn_score_field_no_collision_when_index_false(key_prefix):
    """When the score field is explicitly opted out with
    ``Field(index=False)``, no validation error is raised."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    DIMENSIONS = 4
    opts = VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=DIMENSIONS,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )

    class GoodDocument(BaseJsonModel, index=True):
        name: str
        embedding: list[float] = Field([], vector_options=opts)
        embedding_score: Optional[float] = Field(None, index=False)

    vectors = [0.1 for _ in range(DIMENSIONS)]
    ref = struct.pack(f"<{len(vectors)}f", *vectors)

    knn = KNNExpression(
        k=1,
        vector_field=GoodDocument.embedding,
        score_field=GoodDocument.embedding_score,
        reference_vector=ref,
    )

    # Should construct without raising.
    q = GoodDocument.find(knn=knn)
    assert q.knn is knn


def test_knn_default_score_field_no_collision(key_prefix):
    """When the user does not pass ``score_field``, KNN synthesises a
    ``__<vector_field>_score`` name that doesn't collide with any user
    field — no validation error should be raised."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    DIMENSIONS = 4
    opts = VectorFieldOptions.flat(
        type=VectorFieldOptions.TYPE.FLOAT32,
        dimension=DIMENSIONS,
        distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    )

    class DefaultDocument(BaseJsonModel, index=True):
        name: str
        embedding: list[float] = Field([], vector_options=opts)

    vectors = [0.1 for _ in range(DIMENSIONS)]
    ref = struct.pack(f"<{len(vectors)}f", *vectors)

    knn = KNNExpression(
        k=1, vector_field=DefaultDocument.embedding, reference_vector=ref
    )

    # Default score field name is "__embedding_score" — doesn't collide.
    q = DefaultDocument.find(knn=knn)
    assert q.knn is knn
    assert knn.score_field == "__embedding_score"
