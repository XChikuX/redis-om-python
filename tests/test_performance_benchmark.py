# type: ignore
"""
Comprehensive performance benchmark tests for redis-om-python.

These tests establish baseline performance metrics for single-instance Redis
operations covering all major features: CRUD, queries, pipelines, JSON models,
Hash models, embedded models, GEO operations, full-text search, and more.

Each test records elapsed time and asserts correctness. Timing results are
printed at the end of the session for human review and saved to a file for
cluster comparison.
"""

import abc
import dataclasses
import datetime
import decimal
import json
import os
import tempfile
import time
from collections import namedtuple
from typing import Dict, List, Optional

import pytest
import pytest_asyncio

from aredis_om import (
    Coordinates,
    EmbeddedJsonModel,
    Field,
    GeoFilter,
    HashModel,
    JsonModel,
    Migrator,
    NotFoundError,
    get_redis_connection,
)
from aredis_om.model.model import model_registry
from tests._sync_redis import has_redis_json, has_redisearch

from .conftest import py_test_mark_asyncio

if not has_redisearch() or not has_redis_json():
    pytestmark = pytest.mark.skip

# ── Global benchmark storage ──────────────────────────────────────────

BENCHMARK_RESULTS: Dict[str, Dict] = {}


def record_benchmark(name: str, elapsed: float, ops: int = 1):
    """Record a benchmark result."""
    BENCHMARK_RESULTS[name] = {
        "elapsed_s": round(elapsed, 6),
        "ops": ops,
        "ops_per_sec": round(ops / elapsed, 2) if elapsed > 0 else float("inf"),
    }


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def hash_models(key_prefix, redis):
    """Create Hash models fresh per test to avoid event-loop caching issues."""
    model_registry.clear()

    class BaseHash(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class SimpleHash(BaseHash):
        name: str = Field(index=True)
        value: int = Field(index=True, sortable=True)

        class Meta:
            model_key_prefix = "bench_hash_simple"

    class FullHash(BaseHash):
        first_name: str = Field(index=True, case_sensitive=True)
        last_name: str = Field(index=True)
        email: str = Field(index=True)
        age: int = Field(index=True, sortable=True)
        score: float = Field(index=True, sortable=True)
        status: int = Field(index=True)
        bio: str = Field(index=True, full_text_search=True)
        created_at: datetime.datetime
        location: Coordinates = Field(index=True)

        class Meta:
            model_key_prefix = "bench_hash_full"

    class OptionalHash(BaseHash):
        name: str = Field(index=True)
        optional_score: Optional[float] = Field(index=True, default=None)

        class Meta:
            model_key_prefix = "bench_hash_opt"

    await Migrator().run()

    return namedtuple("HashModels", ["SimpleHash", "FullHash", "OptionalHash"])(
        SimpleHash, FullHash, OptionalHash
    )


@pytest_asyncio.fixture
async def json_models(key_prefix, redis):
    """Create JSON models fresh per test to avoid event-loop caching issues."""
    model_registry.clear()

    class BaseJson(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Address(EmbeddedJsonModel):
        street: str
        city: str = Field(index=True)
        state: str = Field(index=True)
        zip_code: str = Field(index=True)

    class Item(EmbeddedJsonModel):
        name: str = Field(index=True)
        price: decimal.Decimal

    class Order(EmbeddedJsonModel):
        items: List[Item]
        total: decimal.Decimal
        created_on: datetime.datetime

    class SimpleJson(BaseJson):
        name: str = Field(index=True)
        value: int = Field(index=True, sortable=True)

        class Meta:
            model_key_prefix = "bench_json_simple"

    class FullJson(BaseJson):
        first_name: str = Field(index=True, case_sensitive=True)
        last_name: str = Field(index=True)
        email: str = Field(index=True)
        age: int = Field(index=True, sortable=True)
        score: float = Field(index=True, sortable=True)
        bio: str = Field(index=True, full_text_search=True)
        address: Address
        orders: Optional[List[Order]] = None
        location: Coordinates = Field(index=True)

        class Meta:
            model_key_prefix = "bench_json_full"

    class GeoJson(BaseJson):
        name: str = Field(index=True)
        location: Coordinates = Field(index=True)

        class Meta:
            model_key_prefix = "bench_json_geo"

    await Migrator().run()

    return namedtuple(
        "JsonModels",
        ["SimpleJson", "FullJson", "GeoJson", "Address", "Item", "Order"],
    )(SimpleJson, FullJson, GeoJson, Address, Item, Order)


# ── Helper factories ──────────────────────────────────────────────────


def make_address(Address, city="San Francisco", state="CA"):
    return Address(street="123 Main St", city=city, state=state, zip_code="94105")


def make_json_full(m, i: int):
    """Create a FullJson instance."""
    cities = ["San Francisco", "New York", "Chicago", "Boston", "Austin"]
    states = ["CA", "NY", "IL", "MA", "TX"]
    return m.FullJson(
        first_name=f"First{i}",
        last_name=f"Last{i}",
        email=f"user{i}@example.com",
        age=20 + (i % 50),
        score=float(50 + (i % 100)),
        bio=f"Biography text for user number {i} with interesting details.",
        address=make_address(m.Address, city=cities[i % 5], state=states[i % 5]),
        orders=[
            m.Order(
                items=[
                    m.Item(name=f"Widget{j}", price=decimal.Decimal(f"{10 + j}.99"))
                    for j in range(2)
                ],
                total=decimal.Decimal("31.98"),
                created_on=datetime.datetime(2024, 1, 1, 12, 0, 0),
            )
        ],
        location=Coordinates(
            longitude=-122.4194 + (i % 10) * 0.01,
            latitude=37.7749 + (i % 10) * 0.01,
        ),
    )


def make_hash_full(FullHash, i: int):
    return FullHash(
        first_name=f"First{i}",
        last_name=f"Last{i}",
        email=f"user{i}@example.com",
        age=20 + (i % 50),
        score=float(50 + (i % 100)),
        status=1 + (i % 3),
        bio=f"Biography text for user number {i} with interesting details about life.",
        created_at=datetime.datetime(2024, 1, 1, 12, 0, 0),
        location=Coordinates(
            longitude=-122.4194 + (i % 10) * 0.01,
            latitude=37.7749 + (i % 10) * 0.01,
        ),
    )


# ══════════════════════════════════════════════════════════════════════
# HASH MODEL BENCHMARKS
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_bench_hash_single_save(hash_models):
    """Benchmark: Save a single HashModel."""
    m = hash_models
    model = m.SimpleHash(name="bench_single", value=42)
    start = time.perf_counter()
    result = await model.save()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_single_save", elapsed)
    assert result.pk is not None
    assert result.name == "bench_single"


@py_test_mark_asyncio
async def test_bench_hash_single_get(hash_models):
    """Benchmark: Get a single HashModel by pk."""
    m = hash_models
    model = m.SimpleHash(name="bench_get", value=99)
    await model.save()

    start = time.perf_counter()
    fetched = await m.SimpleHash.get(model.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_single_get", elapsed)
    assert fetched.name == "bench_get"
    assert fetched.value == 99


@py_test_mark_asyncio
async def test_bench_hash_bulk_save_50(hash_models):
    """Benchmark: Bulk save 50 HashModel instances."""
    m = hash_models
    models = [m.SimpleHash(name=f"bulk_{i}", value=i) for i in range(50)]
    start = time.perf_counter()
    await m.SimpleHash.add(models)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_bulk_save_50", elapsed, ops=50)
    for model in models:
        assert model.pk is not None


@py_test_mark_asyncio
async def test_bench_hash_bulk_save_200(hash_models):
    """Benchmark: Bulk save 200 HashModel instances."""
    m = hash_models
    models = [m.SimpleHash(name=f"bulk200_{i}", value=i) for i in range(200)]
    start = time.perf_counter()
    await m.SimpleHash.add(models)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_bulk_save_200", elapsed, ops=200)


@py_test_mark_asyncio
async def test_bench_hash_get_many(hash_models):
    """Benchmark: get_many for 50 HashModel instances."""
    m = hash_models
    models = [m.SimpleHash(name=f"getmany_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleHash.get_many(pks)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_get_many_50", elapsed, ops=50)
    assert len(results) == 50


@py_test_mark_asyncio
async def test_bench_hash_all_pks(hash_models):
    """Benchmark: Iterate all HashModel primary keys with a custom SCAN count."""
    m = hash_models
    models = [m.SimpleHash(name=f"allpk_{i}", value=i) for i in range(200)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    pks = [pk async for pk in await m.SimpleHash.all_pks(count=100)]
    elapsed = time.perf_counter() - start
    record_benchmark("hash_all_pks_count_100", elapsed, ops=len(pks))
    assert len(pks) == 200


@py_test_mark_asyncio
async def test_bench_hash_delete(hash_models):
    """Benchmark: Delete a single HashModel."""
    m = hash_models
    model = m.SimpleHash(name="to_delete", value=0)
    await model.save()

    start = time.perf_counter()
    result = await m.SimpleHash.delete(model.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_single_delete", elapsed)
    assert result == 1


@py_test_mark_asyncio
async def test_bench_hash_delete_many(hash_models):
    """Benchmark: Delete 50 HashModel instances at once."""
    m = hash_models
    models = [m.SimpleHash(name=f"delmany_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    count = await m.SimpleHash.delete_many(models)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_delete_many_50", elapsed, ops=50)
    assert count >= 50


@py_test_mark_asyncio
async def test_bench_hash_find_eq(hash_models):
    """Benchmark: Find HashModel by equality query."""
    m = hash_models
    model = m.SimpleHash(name="find_eq_target", value=777)
    await model.save()

    start = time.perf_counter()
    results = await m.SimpleHash.find(m.SimpleHash.name == "find_eq_target").all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_find_eq", elapsed)
    assert any(r.value == 777 for r in results)


@py_test_mark_asyncio
async def test_bench_hash_find_range(hash_models):
    """Benchmark: Find HashModel by numeric range query."""
    m = hash_models
    models = [m.SimpleHash(name=f"range_{i}", value=i) for i in range(100)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find(
        (m.SimpleHash.value >= 10) & (m.SimpleHash.value <= 30)
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_find_range", elapsed, ops=len(results))
    assert len(results) >= 21


@py_test_mark_asyncio
async def test_bench_hash_find_sort(hash_models):
    """Benchmark: Find HashModel with sort_by."""
    m = hash_models
    models = [m.SimpleHash(name=f"sort_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find().sort_by("value").all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_find_sort", elapsed, ops=len(results))
    if len(results) >= 2:
        assert results[0].value <= results[1].value


@py_test_mark_asyncio
async def test_bench_hash_find_page(hash_models):
    """Benchmark: Find HashModel with pagination."""
    m = hash_models
    models = [m.SimpleHash(name=f"page_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find().sort_by("value").page(offset=0, limit=10)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_find_page", elapsed)
    assert len(results) <= 10


@py_test_mark_asyncio
async def test_bench_hash_find_count(hash_models):
    """Benchmark: Count HashModel matches."""
    m = hash_models
    models = [m.SimpleHash(name=f"cnt_{i}", value=i) for i in range(30)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    count = await m.SimpleHash.find().count()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_find_count", elapsed)
    assert count >= 30


@py_test_mark_asyncio
async def test_bench_hash_full_model_save_get(hash_models):
    """Benchmark: Save and retrieve a full HashModel with all field types."""
    m = hash_models
    model = make_hash_full(m.FullHash, 0)
    start = time.perf_counter()
    await model.save()
    fetched = await m.FullHash.get(model.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_full_save_get", elapsed, ops=2)
    assert fetched.first_name == "First0"
    assert fetched.age == 20


@py_test_mark_asyncio
async def test_bench_hash_full_text_search(hash_models):
    """Benchmark: Full-text search on HashModel."""
    m = hash_models
    model = make_hash_full(m.FullHash, 999)
    model.bio = "remarkable outstanding excellent performance benchmark"
    await model.save()

    start = time.perf_counter()
    results = await m.FullHash.find(m.FullHash.bio % "remarkable").all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_full_text_search", elapsed)
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_bench_hash_geo_filter(hash_models):
    """Benchmark: GEO filter on HashModel."""
    m = hash_models
    models = [make_hash_full(m.FullHash, i) for i in range(20)]
    await m.FullHash.add(models)

    start = time.perf_counter()
    results = await m.FullHash.find(
        m.FullHash.location
        == GeoFilter(longitude=-122.4194, latitude=37.7749, radius=50, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_geo_filter", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_bench_hash_optional_fields(hash_models):
    """Benchmark: HashModel with Optional fields (None roundtrip)."""
    m = hash_models
    model_none = m.OptionalHash(name="opt_none")
    model_value = m.OptionalHash(name="opt_value", optional_score=99.5)

    start = time.perf_counter()
    await model_none.save()
    await model_value.save()
    r1 = await m.OptionalHash.get(model_none.pk)
    r2 = await m.OptionalHash.get(model_value.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("hash_optional_fields", elapsed, ops=4)
    assert r1.optional_score is None
    assert r2.optional_score == 99.5


@py_test_mark_asyncio
async def test_bench_hash_update(hash_models):
    """Benchmark: Update a HashModel field."""
    m = hash_models
    model = m.SimpleHash(name="to_update", value=1)
    await model.save()

    start = time.perf_counter()
    model.value = 999
    await model.save()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_update", elapsed)
    fetched = await m.SimpleHash.get(model.pk)
    assert fetched.value == 999


@py_test_mark_asyncio
async def test_bench_hash_or_query(hash_models):
    """Benchmark: OR query on HashModel."""
    m = hash_models
    m1 = m.SimpleHash(name="or_a", value=1000)
    m2 = m.SimpleHash(name="or_b", value=2000)
    await m.SimpleHash.add([m1, m2])

    start = time.perf_counter()
    results = await m.SimpleHash.find(
        (m.SimpleHash.name == "or_a") | (m.SimpleHash.name == "or_b")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_or_query", elapsed, ops=len(results))
    assert len(results) >= 2


@py_test_mark_asyncio
async def test_bench_hash_not_eq_query(hash_models):
    """Benchmark: NOT-equal query on HashModel."""
    m = hash_models
    models = [m.SimpleHash(name=f"neq_{i}", value=i) for i in range(20)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find(m.SimpleHash.name != "neq_0").all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_not_eq_query", elapsed, ops=len(results))
    assert len(results) >= 19


@py_test_mark_asyncio
async def test_bench_hash_in_query(hash_models):
    """Benchmark: IN query on HashModel numeric field."""
    m = hash_models
    models = [m.SimpleHash(name=f"in_{i}", value=3000 + i) for i in range(10)]
    await m.SimpleHash.add(models)

    target_vals = [3001, 3003, 3005, 3007]
    start = time.perf_counter()
    results = await m.SimpleHash.find(m.SimpleHash.value << target_vals).all()
    elapsed = time.perf_counter() - start
    record_benchmark("hash_in_query", elapsed, ops=len(results))
    assert len(results) >= 4


# ══════════════════════════════════════════════════════════════════════
# JSON MODEL BENCHMARKS
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_bench_json_single_save(json_models):
    """Benchmark: Save a single JsonModel."""
    m = json_models
    model = m.SimpleJson(name="json_bench", value=42)
    start = time.perf_counter()
    result = await model.save()
    elapsed = time.perf_counter() - start
    record_benchmark("json_single_save", elapsed)
    assert result.pk is not None


@py_test_mark_asyncio
async def test_bench_json_single_get(json_models):
    """Benchmark: Get a single JsonModel by pk."""
    m = json_models
    model = m.SimpleJson(name="json_get", value=99)
    await model.save()

    start = time.perf_counter()
    fetched = await m.SimpleJson.get(model.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("json_single_get", elapsed)
    assert fetched.name == "json_get"


@py_test_mark_asyncio
async def test_bench_json_bulk_save_50(json_models):
    """Benchmark: Bulk save 50 JsonModel instances."""
    m = json_models
    models = [m.SimpleJson(name=f"jbulk_{i}", value=i) for i in range(50)]
    start = time.perf_counter()
    await m.SimpleJson.add(models)
    elapsed = time.perf_counter() - start
    record_benchmark("json_bulk_save_50", elapsed, ops=50)


@py_test_mark_asyncio
async def test_bench_json_bulk_save_200(json_models):
    """Benchmark: Bulk save 200 JsonModel instances."""
    m = json_models
    models = [m.SimpleJson(name=f"jbulk200_{i}", value=i) for i in range(200)]
    start = time.perf_counter()
    await m.SimpleJson.add(models)
    elapsed = time.perf_counter() - start
    record_benchmark("json_bulk_save_200", elapsed, ops=200)


@py_test_mark_asyncio
async def test_bench_json_get_many(json_models):
    """Benchmark: get_many for 50 JsonModel instances."""
    m = json_models
    models = [m.SimpleJson(name=f"jgetmany_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleJson.get_many(pks)
    elapsed = time.perf_counter() - start
    record_benchmark("json_get_many_50", elapsed, ops=50)
    assert len(results) == 50


@py_test_mark_asyncio
async def test_bench_json_all_pks(json_models):
    """Benchmark: Iterate all JsonModel primary keys with a custom SCAN count."""
    m = json_models
    models = [m.SimpleJson(name=f"jallpk_{i}", value=i) for i in range(200)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    pks = [pk async for pk in await m.SimpleJson.all_pks(count=100)]
    elapsed = time.perf_counter() - start
    record_benchmark("json_all_pks_count_100", elapsed, ops=len(pks))
    assert len(pks) == 200


@py_test_mark_asyncio
async def test_bench_json_delete(json_models):
    """Benchmark: Delete a single JsonModel."""
    m = json_models
    model = m.SimpleJson(name="jdel", value=0)
    await model.save()

    start = time.perf_counter()
    result = await m.SimpleJson.delete(model.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("json_single_delete", elapsed)
    assert result == 1


@py_test_mark_asyncio
async def test_bench_json_delete_many(json_models):
    """Benchmark: Delete 50 JsonModel instances."""
    m = json_models
    models = [m.SimpleJson(name=f"jdelmany_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    count = await m.SimpleJson.delete_many(models)
    elapsed = time.perf_counter() - start
    record_benchmark("json_delete_many_50", elapsed, ops=50)
    assert count >= 50


@py_test_mark_asyncio
async def test_bench_json_find_eq(json_models):
    """Benchmark: Find JsonModel by equality."""
    m = json_models
    model = m.SimpleJson(name="jfind_eq", value=888)
    await model.save()

    start = time.perf_counter()
    results = await m.SimpleJson.find(m.SimpleJson.name == "jfind_eq").all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_find_eq", elapsed)
    assert any(r.value == 888 for r in results)


@py_test_mark_asyncio
async def test_bench_json_find_range(json_models):
    """Benchmark: Find JsonModel by numeric range."""
    m = json_models
    models = [m.SimpleJson(name=f"jrange_{i}", value=i) for i in range(100)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find(
        (m.SimpleJson.value >= 25) & (m.SimpleJson.value <= 75)
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_find_range", elapsed, ops=len(results))
    assert len(results) >= 51


@py_test_mark_asyncio
async def test_bench_json_find_sort(json_models):
    """Benchmark: Find JsonModel with sort_by."""
    m = json_models
    models = [m.SimpleJson(name=f"jsort_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find().sort_by("value").all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_find_sort", elapsed, ops=len(results))


@py_test_mark_asyncio
async def test_bench_json_find_page(json_models):
    """Benchmark: Find JsonModel with pagination."""
    m = json_models
    models = [m.SimpleJson(name=f"jpage_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find().sort_by("value").page(offset=0, limit=10)
    elapsed = time.perf_counter() - start
    record_benchmark("json_find_page", elapsed)
    assert len(results) <= 10


@py_test_mark_asyncio
async def test_bench_json_find_count(json_models):
    """Benchmark: Count JsonModel matches."""
    m = json_models
    models = [m.SimpleJson(name=f"jcnt_{i}", value=i) for i in range(30)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    count = await m.SimpleJson.find().count()
    elapsed = time.perf_counter() - start
    record_benchmark("json_find_count", elapsed)
    assert count >= 30


@py_test_mark_asyncio
async def test_bench_json_full_model_save_get(json_models):
    """Benchmark: Save and retrieve a full JsonModel with embedded models."""
    m = json_models
    model = make_json_full(m, 0)
    start = time.perf_counter()
    await model.save()
    fetched = await m.FullJson.get(model.pk)
    elapsed = time.perf_counter() - start
    record_benchmark("json_full_save_get", elapsed, ops=2)
    assert fetched.first_name == "First0"
    assert fetched.address.city == "San Francisco"


@py_test_mark_asyncio
async def test_bench_json_embedded_query(json_models):
    """Benchmark: Query on embedded model fields."""
    m = json_models
    models = [make_json_full(m, i) for i in range(25)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(m.FullJson.address.city == "New York").all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_embedded_query", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_bench_json_full_text_search(json_models):
    """Benchmark: Full-text search on JsonModel."""
    m = json_models
    model = make_json_full(m, 998)
    model.bio = "spectacular exceptional phenomenal benchmark query"
    await model.save()

    start = time.perf_counter()
    results = await m.FullJson.find(m.FullJson.bio % "spectacular").all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_full_text_search", elapsed)
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_bench_json_geo_filter(json_models):
    """Benchmark: GEO filter on JsonModel."""
    m = json_models
    models = [make_json_full(m, i) for i in range(20)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(
        m.FullJson.location
        == GeoFilter(longitude=-122.4194, latitude=37.7749, radius=50, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_geo_filter", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_bench_json_combined_query(json_models):
    """Benchmark: Combined embedded + age query."""
    m = json_models
    models = [make_json_full(m, i) for i in range(50)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(
        (m.FullJson.address.city == "Chicago") & (m.FullJson.age >= 25)
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_combined_query", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_bench_json_or_query(json_models):
    """Benchmark: OR query on JsonModel."""
    m = json_models
    m1 = m.SimpleJson(name="jor_a", value=4000)
    m2 = m.SimpleJson(name="jor_b", value=5000)
    await m.SimpleJson.add([m1, m2])

    start = time.perf_counter()
    results = await m.SimpleJson.find(
        (m.SimpleJson.name == "jor_a") | (m.SimpleJson.name == "jor_b")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_or_query", elapsed, ops=len(results))
    assert len(results) >= 2


@py_test_mark_asyncio
async def test_bench_json_update(json_models):
    """Benchmark: Update a JsonModel nested field."""
    m = json_models
    model = make_json_full(m, 100)
    await model.save()

    start = time.perf_counter()
    await model.update(address__city="Updated City")
    elapsed = time.perf_counter() - start
    record_benchmark("json_update_nested", elapsed)
    fetched = await m.FullJson.get(model.pk)
    assert fetched.address.city == "Updated City"


@py_test_mark_asyncio
async def test_bench_json_in_query(json_models):
    """Benchmark: IN query on JsonModel."""
    m = json_models
    models = [m.SimpleJson(name=f"jin_{i}", value=6000 + i) for i in range(10)]
    await m.SimpleJson.add(models)

    target_vals = [6001, 6003, 6005]
    start = time.perf_counter()
    results = await m.SimpleJson.find(m.SimpleJson.value << target_vals).all()
    elapsed = time.perf_counter() - start
    record_benchmark("json_in_query", elapsed, ops=len(results))
    assert len(results) >= 3


# ══════════════════════════════════════════════════════════════════════
# GEO-SPECIFIC BENCHMARKS
# ══════════════════════════════════════════════════════════════════════

CITIES = [
    ("New York", -74.0060, 40.7128),
    ("Los Angeles", -118.2437, 34.0522),
    ("Chicago", -87.6298, 41.8781),
    ("Houston", -95.3698, 29.7604),
    ("Phoenix", -112.0740, 33.4484),
    ("Philadelphia", -75.1652, 39.9526),
    ("San Antonio", -98.4936, 29.4241),
    ("San Diego", -117.1611, 32.7157),
    ("Dallas", -96.7970, 32.7767),
    ("San Jose", -121.8863, 37.3382),
]


@py_test_mark_asyncio
async def test_bench_geo_json_cities(json_models):
    """Benchmark: Save and query geo-located city models."""
    m = json_models
    models = []
    for name, lon, lat in CITIES:
        models.append(
            m.GeoJson(name=name, location=Coordinates(longitude=lon, latitude=lat))
        )
    await m.GeoJson.add(models)

    # Query cities within 500km of NYC
    start = time.perf_counter()
    results = await m.GeoJson.find(
        m.GeoJson.location
        == GeoFilter(longitude=-74.0060, latitude=40.7128, radius=500, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("geo_json_500km_nyc", elapsed, ops=len(results))
    city_names = {r.name for r in results}
    assert "New York" in city_names
    assert "Philadelphia" in city_names


@py_test_mark_asyncio
async def test_bench_geo_json_small_radius(json_models):
    """Benchmark: GEO query with a small radius (10km)."""
    m = json_models
    for name, lon, lat in CITIES:
        model = m.GeoJson(name=name, location=Coordinates(longitude=lon, latitude=lat))
        await model.save()

    start = time.perf_counter()
    results = await m.GeoJson.find(
        m.GeoJson.location
        == GeoFilter(longitude=-74.0060, latitude=40.7128, radius=10, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("geo_json_10km_nyc", elapsed, ops=len(results))
    city_names = {r.name for r in results}
    assert "New York" in city_names


@py_test_mark_asyncio
async def test_bench_geo_json_large_radius(json_models):
    """Benchmark: GEO query with large radius (5000km, whole US)."""
    m = json_models
    for name, lon, lat in CITIES:
        model = m.GeoJson(name=name, location=Coordinates(longitude=lon, latitude=lat))
        await model.save()

    start = time.perf_counter()
    results = await m.GeoJson.find(
        m.GeoJson.location
        == GeoFilter(longitude=-98.5795, latitude=39.8283, radius=5000, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("geo_json_5000km_us", elapsed, ops=len(results))
    assert len(results) == 10


# ══════════════════════════════════════════════════════════════════════
# PIPELINE BENCHMARKS
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_bench_pipeline_json_100(json_models):
    """Benchmark: Pipeline save and get 100 JsonModel instances."""
    m = json_models
    models = [m.SimpleJson(name=f"pipe_{i}", value=i) for i in range(100)]
    await m.SimpleJson.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleJson.get_many(pks)
    elapsed = time.perf_counter() - start
    record_benchmark("pipeline_json_get_100", elapsed, ops=100)
    assert len(results) == 100


@py_test_mark_asyncio
async def test_bench_pipeline_hash_100(hash_models):
    """Benchmark: Pipeline save and get 100 HashModel instances."""
    m = hash_models
    models = [m.SimpleHash(name=f"hpipe_{i}", value=i) for i in range(100)]
    await m.SimpleHash.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleHash.get_many(pks)
    elapsed = time.perf_counter() - start
    record_benchmark("pipeline_hash_get_100", elapsed, ops=100)
    assert len(results) == 100


@py_test_mark_asyncio
async def test_bench_pipeline_mixed_operations(json_models, hash_models):
    """Benchmark: Pipeline with mixed save, get, delete operations."""
    jm = json_models
    hm = hash_models
    json_ms = [jm.SimpleJson(name=f"mixed_j_{i}", value=i) for i in range(30)]
    hash_ms = [hm.SimpleHash(name=f"mixed_h_{i}", value=i) for i in range(30)]

    start = time.perf_counter()
    await jm.SimpleJson.add(json_ms)
    await hm.SimpleHash.add(hash_ms)

    j_pks = [model.pk for model in json_ms]
    h_pks = [model.pk for model in hash_ms]
    j_results = await jm.SimpleJson.get_many(j_pks)
    h_results = await hm.SimpleHash.get_many(h_pks)

    await jm.SimpleJson.delete_many(json_ms)
    await hm.SimpleHash.delete_many(hash_ms)
    elapsed = time.perf_counter() - start

    record_benchmark("pipeline_mixed_ops", elapsed, ops=180)
    assert len(j_results) == 30
    assert len(h_results) == 30


# ══════════════════════════════════════════════════════════════════════
# COMPLEX QUERY BENCHMARKS
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_bench_complex_and_or_query(json_models):
    """Benchmark: Complex AND + OR + range query."""
    m = json_models
    models = [make_json_full(m, i) for i in range(50)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(
        (
            (m.FullJson.address.city == "San Francisco")
            | (m.FullJson.address.city == "New York")
        )
        & (m.FullJson.age >= 30)
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("complex_and_or_query", elapsed, ops=len(results))


@py_test_mark_asyncio
async def test_bench_complex_sort_page_filter(json_models):
    """Benchmark: Complex query with filter + sort + pagination."""
    m = json_models
    models = [make_json_full(m, i) for i in range(50)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = (
        await m.FullJson.find(m.FullJson.age >= 25)
        .sort_by("score")
        .page(offset=0, limit=10)
    )
    elapsed = time.perf_counter() - start
    record_benchmark("complex_sort_page_filter", elapsed)
    assert len(results) <= 10


@py_test_mark_asyncio
async def test_bench_complex_negation_query(json_models):
    """Benchmark: Negation query."""
    m = json_models
    models = [make_json_full(m, i) for i in range(30)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(~(m.FullJson.address.city == "Chicago")).all()
    elapsed = time.perf_counter() - start
    record_benchmark("complex_negation_query", elapsed, ops=len(results))


@py_test_mark_asyncio
async def test_bench_complex_geo_plus_filter(json_models):
    """Benchmark: GEO filter combined with other conditions."""
    m = json_models
    models = [make_json_full(m, i) for i in range(30)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(
        (
            m.FullJson.location
            == GeoFilter(longitude=-122.4194, latitude=37.7749, radius=100, unit="km")
        )
        & (m.FullJson.age >= 25)
    ).all()
    elapsed = time.perf_counter() - start
    record_benchmark("complex_geo_plus_filter", elapsed, ops=len(results))


# ══════════════════════════════════════════════════════════════════════
# REPORT: Print all benchmark results
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_zzz_print_benchmark_results(key_prefix, redis):
    """Print all benchmark results (runs last due to alphabetical ordering)."""
    print("\n" + "=" * 80)
    print("PERFORMANCE BENCHMARK RESULTS (Single Instance)")
    print("=" * 80)
    print(f"{'Benchmark':<40} {'Elapsed (s)':<15} {'Ops':<8} {'Ops/sec':<12}")
    print("-" * 80)
    for name, data in sorted(BENCHMARK_RESULTS.items()):
        print(
            f"{name:<40} {data['elapsed_s']:<15} {data['ops']:<8} {data['ops_per_sec']:<12}"
        )
    print("=" * 80)

    # Write results to a file for cluster comparison
    results_file = os.path.join(tempfile.gettempdir(), "single_instance_benchmarks.txt")
    with open(results_file, "w") as f:
        for name, data in sorted(BENCHMARK_RESULTS.items()):
            f.write(
                f"{name}\t{data['elapsed_s']}\t{data['ops']}\t{data['ops_per_sec']}\n"
            )
    print(f"\nResults saved to {results_file}")

    # Basic sanity: we should have recorded many benchmarks
    assert (
        len(BENCHMARK_RESULTS) >= 30
    ), f"Expected at least 30 benchmark results, got {len(BENCHMARK_RESULTS)}"
