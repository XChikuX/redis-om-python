# type: ignore
"""Tests for deeply nested model structures and complex multi-condition queries.

These tests cover:
- 3-4 level deep embedded model hierarchies
- Complex multi-condition queries combining age, location, personality, interests
- Embedded model field queries (location.city, personality.mbti)
- IN operator for TAG fields on embedded models
- Full-text search combined with embedded field queries
- Negated expressions on nested fields
- AND/OR/NOT combinations on deeply nested structures
- GeoFilter with nested model data
- NegatedExpression property coverage (left, right, op, name, tree)
- Expression property coverage (name, tree)
- FindQuery methods (sort_by, page, count, delete, update, get_item, __aiter__)
"""

import abc
import datetime
from typing import List, Optional

import pytest
import pytest_asyncio

from aredis_om import (
    Coordinates,
    EmbeddedJsonModel,
    Field,
    FindQuery,
    GeoFilter,
    JsonModel,
    Migrator,
    NotFoundError,
    QueryNotSupportedError,
)
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip

today = datetime.date.today()


# ---------------------------------------------------------------------------
# Deeply nested model hierarchy (4 levels)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def nested_models(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Personality(EmbeddedJsonModel):
        """Level 3: personality with MBTI."""

        mbti: str = Field(index=True)
        openness: Optional[float] = Field(index=True, default=None)

    class Location(EmbeddedJsonModel):
        """Level 3: location with city/country/coordinates."""

        city: str = Field(index=True)
        country: str = Field(index=True)
        coordinates: Optional[Coordinates] = Field(index=True, default=None)

    class Gender(EmbeddedJsonModel):
        """Level 3: gender info."""

        pgender: str = Field(index=True)

    class RedisUser(BaseJsonModel):
        name: str = Field(index=True)
        age: int = Field(index=True)
        height: float = Field(index=True)
        bio: Optional[str] = Field(index=True, full_text_search=True, default="")
        gender: Gender
        ethnicity: str = Field(index=True)
        personality: Personality
        location: Location
        interests: List[str] = Field(index=True)

    await Migrator().run()

    return {
        "BaseJsonModel": BaseJsonModel,
        "RedisUser": RedisUser,
        "Personality": Personality,
        "Location": Location,
        "Gender": Gender,
    }


@pytest_asyncio.fixture
async def users(nested_models):
    RedisUser = nested_models["RedisUser"]
    Personality = nested_models["Personality"]
    Location = nested_models["Location"]
    Gender = nested_models["Gender"]

    alice = RedisUser(
        name="Alice",
        age=28,
        height=5.6,
        bio="Alice is a software engineer who loves hiking",
        gender=Gender(pgender="female"),
        ethnicity="Asian",
        personality=Personality(mbti="INTJ", openness=0.8),
        location=Location(
            city="Portland",
            country="USA",
            coordinates=Coordinates(latitude=45.5231, longitude=-122.6765),
        ),
        interests=["hiking", "coding", "reading"],
    )

    bob = RedisUser(
        name="Bob",
        age=35,
        height=6.1,
        bio="Bob is a data scientist working in machine learning",
        gender=Gender(pgender="male"),
        ethnicity="Caucasian",
        personality=Personality(mbti="ENTP", openness=0.9),
        location=Location(
            city="Seattle",
            country="USA",
            coordinates=Coordinates(latitude=47.6062, longitude=-122.3321),
        ),
        interests=["gaming", "cooking", "coding"],
    )

    carol = RedisUser(
        name="Carol",
        age=42,
        height=5.4,
        bio="Carol is an architect specializing in sustainable design",
        gender=Gender(pgender="female"),
        ethnicity="Hispanic",
        personality=Personality(mbti="ISFJ", openness=0.5),
        location=Location(
            city="Tokyo",
            country="Japan",
            coordinates=Coordinates(latitude=35.6762, longitude=139.6503),
        ),
        interests=["design", "travel", "photography"],
    )

    dave = RedisUser(
        name="Dave",
        age=22,
        height=5.9,
        bio="Dave is a recent graduate interested in AI research",
        gender=Gender(pgender="male"),
        ethnicity="Asian",
        personality=Personality(mbti="INTJ", openness=0.7),
        location=Location(
            city="Portland",
            country="USA",
            coordinates=Coordinates(latitude=45.5231, longitude=-122.6765),
        ),
        interests=["AI", "reading", "gaming"],
    )

    await alice.save()
    await bob.save()
    await carol.save()
    await dave.save()

    yield {"alice": alice, "bob": bob, "carol": carol, "dave": dave}


# ---------------------------------------------------------------------------
# Basic nested queries
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_query_embedded_city(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.location.city == "Portland").all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["dave"].pk in pks
    assert users["bob"].pk not in pks


@py_test_mark_asyncio
async def test_query_embedded_country(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.location.country == "Japan").all()
    assert len(results) == 1
    assert results[0].pk == users["carol"].pk


@py_test_mark_asyncio
async def test_query_embedded_personality_mbti(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.personality.mbti == "INTJ").all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["dave"].pk in pks
    assert len(results) == 2


@py_test_mark_asyncio
async def test_query_embedded_gender(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.gender.pgender == "female").all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["carol"].pk in pks
    assert len(results) == 2


# ---------------------------------------------------------------------------
# IN operator on embedded models
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_in_operator_embedded_mbti(nested_models, users):
    """IN operator (<<) on embedded model TAG field."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.personality.mbti << ["INTJ", "ENTP"]).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["bob"].pk in pks
    assert users["dave"].pk in pks
    assert users["carol"].pk not in pks


@py_test_mark_asyncio
async def test_in_operator_top_level_interests(nested_models, users):
    """IN operator (<<) on top-level list TAG field."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.interests << ["hiking"]).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks


@py_test_mark_asyncio
async def test_in_operator_ethnicity(nested_models, users):
    """IN operator on top-level TAG field."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.ethnicity << ["Asian", "Hispanic"]).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["carol"].pk in pks
    assert users["dave"].pk in pks
    assert users["bob"].pk not in pks


# ---------------------------------------------------------------------------
# Full-text search combined with embedded field queries
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_fulltext_search_bio(nested_models, users):
    """Full-text search (%) on bio field."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.bio % "engineer").all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks


@py_test_mark_asyncio
async def test_fulltext_search_combined_with_embedded(nested_models, users):
    """Full-text search + embedded field query."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        RedisUser.bio % "engineer",
        RedisUser.location.city == "Portland",
    ).all()
    assert len(results) == 1
    assert results[0].pk == users["alice"].pk


# ---------------------------------------------------------------------------
# Complex multi-condition queries (matching the problem statement pattern)
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_complex_age_height_filter(nested_models, users):
    """Filter by age range AND height range."""
    RedisUser = nested_models["RedisUser"]
    conditions = []
    conditions.append((RedisUser.age >= 25) & (RedisUser.age <= 40))
    conditions.append((RedisUser.height >= 5.5) & (RedisUser.height <= 6.5))

    results = await RedisUser.find(*conditions).all()
    pks = {r.pk for r in results}
    # Alice: age 28, height 5.6 ✓
    # Bob: age 35, height 6.1 ✓
    assert users["alice"].pk in pks
    assert users["bob"].pk in pks


@py_test_mark_asyncio
async def test_complex_age_gender_ethnicity_filter(nested_models, users):
    """Filter by age + gender + ethnicity."""
    RedisUser = nested_models["RedisUser"]
    conditions = []
    conditions.append((RedisUser.age >= 20) & (RedisUser.age <= 30))
    conditions.append(RedisUser.gender.pgender == "female")
    conditions.append(RedisUser.ethnicity == "Asian")

    results = await RedisUser.find(*conditions).all()
    assert len(results) == 1
    assert results[0].pk == users["alice"].pk


@py_test_mark_asyncio
async def test_complex_city_mbti_bio_filter(nested_models, users):
    """Filter by city + MBTI IN + bio search."""
    RedisUser = nested_models["RedisUser"]
    conditions = []
    conditions.append(RedisUser.location.city == "Portland")
    conditions.append(RedisUser.personality.mbti << ["INTJ", "ENTP"])
    conditions.append(RedisUser.bio % "engineer")

    results = await RedisUser.find(*conditions).all()
    assert len(results) == 1
    assert results[0].pk == users["alice"].pk


@py_test_mark_asyncio
async def test_complex_all_conditions(nested_models, users):
    """The full query pattern from the problem statement."""
    RedisUser = nested_models["RedisUser"]
    conditions = []

    # Age filter
    conditions.append((RedisUser.age >= 20) & (RedisUser.age <= 50))
    # Height filter
    conditions.append((RedisUser.height >= 5.0) & (RedisUser.height <= 7.0))
    # Gender filter
    conditions.append(RedisUser.gender.pgender == "female")
    # Ethnicity filter
    conditions.append(RedisUser.ethnicity == "Asian")
    # Bio search
    conditions.append(RedisUser.bio % "software")
    # City filter
    conditions.append(RedisUser.location.city == "Portland")
    # MBTI compatibility filter
    conditions.append(RedisUser.personality.mbti << ["INTJ", "ENTP", "INFJ"])

    results = await RedisUser.find(*conditions).all()
    assert len(results) == 1
    assert results[0].pk == users["alice"].pk


@py_test_mark_asyncio
async def test_complex_country_filter_no_city(nested_models, users):
    """Country filter when city is not specified."""
    RedisUser = nested_models["RedisUser"]
    conditions = []
    conditions.append(RedisUser.location.country == "USA")
    conditions.append((RedisUser.age >= 20) & (RedisUser.age <= 50))

    results = await RedisUser.find(*conditions).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["bob"].pk in pks
    assert users["dave"].pk in pks
    assert users["carol"].pk not in pks


# ---------------------------------------------------------------------------
# Negated expressions on nested fields
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_negated_embedded_city(nested_models, users):
    """NOT query on embedded city."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(~(RedisUser.location.city == "Portland")).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk not in pks
    assert users["dave"].pk not in pks
    assert users["bob"].pk in pks
    assert users["carol"].pk in pks


@py_test_mark_asyncio
async def test_negated_in_operator_mbti(nested_models, users):
    """NOT IN query on embedded MBTI."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.personality.mbti >> ["INTJ"]).all()
    pks = {r.pk for r in results}
    # Everyone except INTJ users
    assert users["alice"].pk not in pks
    assert users["dave"].pk not in pks
    assert users["bob"].pk in pks
    assert users["carol"].pk in pks


@py_test_mark_asyncio
async def test_negated_combined_with_and(nested_models, users):
    """Negated expression combined with AND."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        ~(RedisUser.location.city == "Tokyo"),
        RedisUser.ethnicity == "Asian",
    ).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["dave"].pk in pks
    assert users["carol"].pk not in pks


# ---------------------------------------------------------------------------
# OR expressions on nested fields
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_or_expression_nested_cities(nested_models, users):
    """OR expression combining two embedded field queries."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        (RedisUser.location.city == "Portland") | (RedisUser.location.city == "Seattle")
    ).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["bob"].pk in pks
    assert users["dave"].pk in pks
    assert users["carol"].pk not in pks


@py_test_mark_asyncio
async def test_or_and_nested_combination(nested_models, users):
    """Complex OR + AND on nested fields."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        (
            (RedisUser.location.city == "Portland")
            & (RedisUser.personality.mbti == "INTJ")
        )
        | (RedisUser.location.city == "Tokyo")
    ).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["dave"].pk in pks
    assert users["carol"].pk in pks
    assert users["bob"].pk not in pks


@py_test_mark_asyncio
async def test_negated_or_expression_nested(nested_models, users):
    """Negated OR on nested fields."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        ~(
            (RedisUser.location.city == "Portland")
            | (RedisUser.location.city == "Seattle")
        )
    ).all()
    pks = {r.pk for r in results}
    # Only Carol (Tokyo) remains
    assert users["carol"].pk in pks
    assert users["alice"].pk not in pks
    assert users["bob"].pk not in pks


# ---------------------------------------------------------------------------
# GeoFilter with nested model data
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_geo_filter_with_nested_coordinates(nested_models, users):
    """GeoFilter search on embedded coordinates."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        RedisUser.location.coordinates
        == GeoFilter(longitude=-122.6765, latitude=45.5231, radius=50, unit="mi")
    ).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks
    assert users["dave"].pk in pks
    assert users["bob"].pk not in pks  # Seattle is ~170 mi away
    assert users["carol"].pk not in pks


@py_test_mark_asyncio
async def test_geo_filter_combined_with_other_conditions(nested_models, users):
    """GeoFilter + other conditions on nested fields."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        RedisUser.location.coordinates
        == GeoFilter(longitude=-122.6765, latitude=45.5231, radius=200, unit="mi"),
        RedisUser.personality.mbti == "ENTP",
    ).all()
    pks = {r.pk for r in results}
    # Bob (Seattle) is within 200mi of Portland, and has MBTI ENTP
    assert users["bob"].pk in pks
    # Alice & Dave have INTJ, not ENTP
    assert users["alice"].pk not in pks


@py_test_mark_asyncio
async def test_geo_filter_large_radius(nested_models, users):
    """GeoFilter with large radius to include multiple cities."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(
        RedisUser.location.coordinates
        == GeoFilter(longitude=-122.5, latitude=46.0, radius=500, unit="km")
    ).all()
    pks = {r.pk for r in results}
    # Portland and Seattle are within 500km of each other
    assert users["alice"].pk in pks or users["bob"].pk in pks


# ---------------------------------------------------------------------------
# Sort/page/count operations on nested queries
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_sort_by_age_on_nested_query(nested_models, users):
    """Sort by age combined with nested field query."""
    RedisUser = nested_models["RedisUser"]
    results = (
        await RedisUser.find(RedisUser.location.country == "USA").sort_by("age").all()
    )
    ages = [r.age for r in results]
    assert ages == sorted(ages)


@py_test_mark_asyncio
async def test_sort_by_age_desc(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    results = (
        await RedisUser.find(RedisUser.location.country == "USA").sort_by("-age").all()
    )
    ages = [r.age for r in results]
    assert ages == sorted(ages, reverse=True)


@py_test_mark_asyncio
async def test_page_with_nested_query(nested_models, users):
    """page() with nested field query."""
    RedisUser = nested_models["RedisUser"]
    results = (
        await RedisUser.find(RedisUser.location.country == "USA")
        .sort_by("age")
        .page(offset=0, limit=2)
    )
    assert len(results) == 2


@py_test_mark_asyncio
async def test_count_with_nested_query(nested_models, users):
    """count() on nested field query."""
    RedisUser = nested_models["RedisUser"]
    count = await RedisUser.find(RedisUser.location.country == "USA").count()
    assert count == 3


@py_test_mark_asyncio
async def test_first_with_nested_query(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    result = await RedisUser.find(RedisUser.location.city == "Tokyo").first()
    assert result.pk == users["carol"].pk


@py_test_mark_asyncio
async def test_first_not_found_raises(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    with pytest.raises(NotFoundError):
        await RedisUser.find(RedisUser.location.city == "NonexistentCity").first()


# ---------------------------------------------------------------------------
# FindQuery.get_item() for async users
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_get_item_on_nested_query(nested_models, users):
    RedisUser = nested_models["RedisUser"]
    result = (
        await RedisUser.find(RedisUser.location.country == "USA")
        .sort_by("age")
        .get_item(0)
    )
    # First by age ascending among USA users
    assert result.age == 22  # Dave


# ---------------------------------------------------------------------------
# FindQuery async iteration
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_async_iteration_nested_query(nested_models, users):
    """async for over FindQuery results."""
    RedisUser = nested_models["RedisUser"]
    pks = set()
    async for user in RedisUser.find(RedisUser.location.country == "USA"):
        pks.add(user.pk)
    assert len(pks) == 3
    assert users["alice"].pk in pks
    assert users["bob"].pk in pks
    assert users["dave"].pk in pks


# ---------------------------------------------------------------------------
# FindQuery.update() on nested query results
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_find_update_on_nested_query(nested_models, users):
    """update() changes matching records."""
    RedisUser = nested_models["RedisUser"]
    await RedisUser.find(RedisUser.location.city == "Tokyo").update(ethnicity="updated")

    carol = await RedisUser.get(users["carol"].pk)
    assert carol.ethnicity == "updated"


# ---------------------------------------------------------------------------
# FindQuery.delete() on nested query results
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_find_delete_on_nested_query(nested_models, users):
    """delete() removes matching records."""
    RedisUser = nested_models["RedisUser"]
    # Delete Tokyo users
    await RedisUser.find(RedisUser.location.city == "Tokyo").delete()

    # Carol should be gone
    with pytest.raises(NotFoundError):
        await RedisUser.get(users["carol"].pk)


# ---------------------------------------------------------------------------
# Expression/NegatedExpression property coverage
# ---------------------------------------------------------------------------


def test_expression_name_property(nested_models):
    """Expression.name returns the operator as string."""
    RedisUser = nested_models["RedisUser"]
    expr = RedisUser.age == 30
    assert expr.name == "EQ"


def test_expression_tree_property(nested_models):
    """Expression.tree returns render_tree output."""
    RedisUser = nested_models["RedisUser"]
    expr = RedisUser.age == 30
    tree_str = expr.tree
    assert isinstance(tree_str, str)
    assert "EQ" in tree_str


def test_negated_expression_properties(nested_models):
    """NegatedExpression exposes left, right, op, name, tree."""
    RedisUser = nested_models["RedisUser"]
    expr = RedisUser.age == 30
    neg = ~expr
    assert neg.left == expr.left
    assert neg.right == expr.right
    assert neg.op == expr.op
    # For EQ, name should be "NOT EQ"
    assert neg.name == "NOT EQ"
    assert isinstance(neg.tree, str)


def test_negated_expression_name_non_eq(nested_models):
    """NegatedExpression.name for non-EQ operator."""
    RedisUser = nested_models["RedisUser"]
    expr = RedisUser.age > 30
    neg = ~expr
    assert neg.name == "GT NOT"


def test_double_negation_returns_original(nested_models):
    """~~expr returns the original expression."""
    RedisUser = nested_models["RedisUser"]
    expr = RedisUser.age == 30
    double_neg = ~(~expr)
    assert double_neg is expr


def test_negated_expression_and(nested_models):
    """NegatedExpression & other creates AND expression."""
    RedisUser = nested_models["RedisUser"]
    neg = ~(RedisUser.age == 30)
    other = RedisUser.name == "test"
    combined = neg & other
    assert combined.op.name == "AND"


def test_negated_expression_or(nested_models):
    """NegatedExpression | other creates OR expression."""
    RedisUser = nested_models["RedisUser"]
    neg = ~(RedisUser.age == 30)
    other = RedisUser.name == "test"
    combined = neg | other
    assert combined.op.name == "OR"


# ---------------------------------------------------------------------------
# FindQuery query string generation for complex nested queries
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_find_query_generation_complex_nested(nested_models):
    """Verify the generated RediSearch query string for complex nested conditions."""
    RedisUser = nested_models["RedisUser"]

    model_name, fq = await FindQuery(
        expressions=[
            (RedisUser.age >= 20) & (RedisUser.age <= 50),
            RedisUser.location.city == "Portland",
            RedisUser.personality.mbti << ["INTJ", "ENTP"],
        ],
        model=RedisUser,
    ).get_query()

    query_str = fq[2]
    assert "@age:" in query_str
    assert "@location_city:{Portland}" in query_str
    assert "@personality_mbti:{INTJ|ENTP}" in query_str


@py_test_mark_asyncio
async def test_find_query_generation_or_nested(nested_models):
    """OR query on nested fields generates correct query string."""
    RedisUser = nested_models["RedisUser"]

    model_name, fq = await FindQuery(
        expressions=[
            (RedisUser.location.city == "Portland")
            | (RedisUser.location.city == "Seattle")
        ],
        model=RedisUser,
    ).get_query()

    query_str = fq[2]
    assert "@location_city:{Portland}" in query_str
    assert "@location_city:{Seattle}" in query_str
    assert "|" in query_str


@py_test_mark_asyncio
async def test_find_query_generation_negated_nested(nested_models):
    """Negated nested field generates correct query string."""
    RedisUser = nested_models["RedisUser"]

    model_name, fq = await FindQuery(
        expressions=[~(RedisUser.personality.mbti == "INTJ")],
        model=RedisUser,
    ).get_query()

    query_str = fq[2]
    assert "-(@personality_mbti:{INTJ})" == query_str


# ---------------------------------------------------------------------------
# Sort by empty fields (no-op)
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_sort_by_empty_is_noop(nested_models, users):
    """sort_by() with no fields returns self."""
    RedisUser = nested_models["RedisUser"]
    query = RedisUser.find(RedisUser.age > 0)
    sorted_query = query.sort_by()
    # Should return self (no-op)
    results = await sorted_query.all()
    assert len(results) == 4


# ---------------------------------------------------------------------------
# aggregate_ct() on nested queries
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_aggregate_ct_nested_query(nested_models, users):
    """aggregate_ct() returns approximate count."""
    RedisUser = nested_models["RedisUser"]
    ct = await RedisUser.find(RedisUser.location.country == "USA").aggregate_ct()
    assert ct >= 3


# ---------------------------------------------------------------------------
# Deep nesting: 4-level query through personality.skills
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_save_and_retrieve_nested_personality(nested_models, users):
    """Verify nested personality data round-trips correctly."""
    RedisUser = nested_models["RedisUser"]
    alice = await RedisUser.get(users["alice"].pk)
    assert alice.personality.mbti == "INTJ"
    assert alice.personality.openness == 0.8


@py_test_mark_asyncio
async def test_embedded_model_pk_never_written(nested_models, users):
    """Embedded models must never have a pk key written to Redis."""
    RedisUser = nested_models["RedisUser"]
    alice = users["alice"]

    raw = await RedisUser.db().json().get(alice.key())

    # Top-level model has pk; embedded models must not.
    assert "pk" in raw, "top-level RedisUser should have pk in Redis"
    assert "pk" not in raw["personality"], "embedded personality must not have pk"
    assert "pk" not in raw["location"], "embedded location must not have pk"
    assert "pk" not in raw["gender"], "embedded gender must not have pk"


@pytest.mark.parametrize("stray_pk", [[], "stale-id", ""])
@py_test_mark_asyncio
async def test_embedded_model_stray_pk_ignored_on_get(nested_models, users, stray_pk):
    """Stray pk values injected into embedded model data are silently dropped."""
    RedisUser = nested_models["RedisUser"]
    alice = users["alice"]

    raw = await RedisUser.db().json().get(alice.key())
    raw["personality"]["pk"] = stray_pk
    await RedisUser.db().json().set(alice.key(), ".", raw)

    reloaded = await RedisUser.get(alice.pk)

    assert reloaded.personality.pk is None
    assert reloaded.personality.mbti == alice.personality.mbti


@pytest.mark.parametrize("stray_pk", [[], "stale-id", ""])
@py_test_mark_asyncio
async def test_embedded_model_stray_pk_ignored_on_query(
    nested_models, users, stray_pk
):
    """Query results silently drop stray pk from embedded model data."""
    RedisUser = nested_models["RedisUser"]
    alice = users["alice"]

    raw = await RedisUser.db().json().get(alice.key())
    raw["personality"]["pk"] = stray_pk
    await RedisUser.db().json().set(alice.key(), ".", raw)

    results = await RedisUser.find(RedisUser.name == "Alice").all()

    assert len(results) == 1
    assert results[0].personality.pk is None
    assert results[0].personality.mbti == alice.personality.mbti


@py_test_mark_asyncio
async def test_save_and_retrieve_nested_location(nested_models, users):
    """Verify nested location data round-trips correctly."""
    RedisUser = nested_models["RedisUser"]
    carol = await RedisUser.get(users["carol"].pk)
    assert carol.location.city == "Tokyo"
    assert carol.location.country == "Japan"
    assert carol.location.coordinates.latitude == 35.6762


@py_test_mark_asyncio
async def test_query_openness_numeric_on_embedded(nested_models, users):
    """NUMERIC query on embedded float field (personality.openness)."""
    RedisUser = nested_models["RedisUser"]
    results = await RedisUser.find(RedisUser.personality.openness >= 0.8).all()
    pks = {r.pk for r in results}
    assert users["alice"].pk in pks  # openness 0.8
    assert users["bob"].pk in pks  # openness 0.9
    assert users["carol"].pk not in pks  # openness 0.5
