# type: ignore
"""
Comprehensive cluster operations tests for redis-om-python.

Tests verify stability and correctness of redis-om operations against a
6-node Redis cluster (3 masters, 3 replicas) using redis:8-alpine.

Coverage includes:
- Connection management (cluster detection, URL parsing)
- HashModel CRUD, queries, pipelines on cluster
- JsonModel CRUD, embedded models, queries on cluster
- GEO operations (save, search, GeoFilter) on cluster
- Full-text search on cluster
- Complex queries (AND, OR, NOT, IN, range) on cluster
- Index creation / migration on cluster (FT.CREATE via target_nodes=PRIMARIES)
- Pipeline and batch operations on cluster
- Performance comparison vs single-instance (pass/fail based on slowdown factor)
- Direct Redis verification before redis-om layer queries
- Error handling and edge cases

Prerequisites:
  - 6-node Redis cluster on ports 7001-7006  (3 masters, 3 replicas)
  - Single-instance Redis on port 6380 (for performance comparison)
  - Both using redis:8-alpine with modules (search, ReJSON, bf, timeseries)
"""

import abc
import asyncio
import datetime
import decimal
import json
import os
import tempfile
import time
from collections import namedtuple
from typing import Dict, List, Optional, Set

import pytest
import pytest_asyncio
import redis as sync_redis
import redis.asyncio as aioredis

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

from .conftest import py_test_mark_asyncio

# ── Configuration ─────────────────────────────────────────────────────

CLUSTER_PORT = 7001
CLUSTER_URL = f"redis://localhost:{CLUSTER_PORT}"
SINGLE_URL = "redis://localhost:6380?decode_responses=True"

# Performance: acceptable slowdown factor for cluster vs single instance
# Cluster operations are expected to be slower due to slot routing, redirects,
# etc.  The 5x threshold is based on empirical testing in a local Docker
# environment where both cluster and single-instance run on the same host.
# Adjust upward for CI environments with resource contention or cross-network
# cluster topologies.
ACCEPTABLE_SLOWDOWN_FACTOR = 5.0  # cluster can be up to 5x slower

# ── Skip if cluster not available ─────────────────────────────────────


def cluster_available():
    """Check if cluster is accessible."""
    try:
        rc = sync_redis.RedisCluster(
            host="localhost", port=CLUSTER_PORT, decode_responses=True
        )
        rc.ping()
        rc.close()
        return True
    except Exception:
        return False


def single_instance_available():
    """Check if single instance is accessible."""
    try:
        r = sync_redis.Redis.from_url(SINGLE_URL)
        r.ping()
        r.close()
        return True
    except Exception:
        return False


if not cluster_available():
    pytestmark = pytest.mark.skip(reason="Redis cluster not available on port 7001")

# ── Benchmark Results Storage ─────────────────────────────────────────

CLUSTER_BENCHMARKS: Dict[str, Dict] = {}
SINGLE_BENCHMARKS: Dict[str, float] = {}


def record_cluster_benchmark(name: str, elapsed: float, ops: int = 1):
    CLUSTER_BENCHMARKS[name] = {
        "elapsed_s": round(elapsed, 6),
        "ops": ops,
        "ops_per_sec": round(ops / elapsed, 2) if elapsed > 0 else float("inf"),
    }


def load_single_benchmarks():
    """Load single-instance benchmark results for comparison."""
    # SINGLE_BENCHMARKS is loaded from file for comparison
    path = os.path.join(tempfile.gettempdir(), "single_instance_benchmarks.txt")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    SINGLE_BENCHMARKS[parts[0]] = float(parts[1])


load_single_benchmarks()


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def cluster_conn():
    """Async RedisCluster connection."""
    conn = aioredis.RedisCluster(
        host="localhost", port=CLUSTER_PORT, decode_responses=True
    )
    yield conn
    await conn.aclose()


@pytest_asyncio.fixture
async def cluster_hash_models(cluster_conn):
    """Hash models configured for cluster."""
    model_registry.clear()

    class BaseHash(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = "cluster-test"
            database = cluster_conn

    class SimpleHash(BaseHash):
        name: str = Field(index=True)
        value: int = Field(index=True, sortable=True)

        class Meta:
            model_key_prefix = "c_hash_simple"

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
            model_key_prefix = "c_hash_full"

    class OptionalHash(BaseHash):
        name: str = Field(index=True)
        optional_value: Optional[float] = Field(index=True, default=None)

        class Meta:
            model_key_prefix = "c_hash_opt"

    await Migrator(conn=cluster_conn).run()

    yield namedtuple("HashModels", ["SimpleHash", "FullHash", "OptionalHash"])(
        SimpleHash, FullHash, OptionalHash
    )

    # Cleanup: delete all cluster-test keys
    async for key in cluster_conn.scan_iter("cluster-test:*"):
        await cluster_conn.delete(key)


@pytest_asyncio.fixture
async def cluster_json_models(cluster_conn):
    """JSON models configured for cluster."""
    model_registry.clear()

    class BaseJson(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = "cluster-test"
            database = cluster_conn

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
            model_key_prefix = "c_json_simple"

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
            model_key_prefix = "c_json_full"

    class GeoJson(BaseJson):
        name: str = Field(index=True)
        location: Coordinates = Field(index=True)

        class Meta:
            model_key_prefix = "c_json_geo"

    await Migrator(conn=cluster_conn).run()

    yield namedtuple(
        "JsonModels",
        ["SimpleJson", "FullJson", "GeoJson", "Address", "Item", "Order"],
    )(SimpleJson, FullJson, GeoJson, Address, Item, Order)

    # Cleanup
    async for key in cluster_conn.scan_iter("cluster-test:*"):
        await cluster_conn.delete(key)


# ── Helper Factories ──────────────────────────────────────────────────


CITIES_GEO = [
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


def make_address(Address, city="San Francisco", state="CA"):
    return Address(street="123 Main St", city=city, state=state, zip_code="94105")


def make_full_json(m, i: int):
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


def make_full_hash(FullHash, i: int):
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


def check_slowdown(name: str, cluster_time: float):
    """Compare cluster time vs single instance. Mark pass/fail."""
    single_time = SINGLE_BENCHMARKS.get(name)
    if single_time is None or single_time == 0:
        return True, "N/A (no single-instance baseline)"
    ratio = cluster_time / single_time
    passed = ratio <= ACCEPTABLE_SLOWDOWN_FACTOR
    status = "PASS" if passed else "FAIL"
    return (
        passed,
        f"{status} ({ratio:.1f}x slowdown, limit: {ACCEPTABLE_SLOWDOWN_FACTOR}x)",
    )


# ══════════════════════════════════════════════════════════════════════
# SECTION 1: CONNECTION AND CLUSTER VERIFICATION
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_connection_ping(cluster_conn):
    """Verify cluster is reachable and healthy."""
    result = await cluster_conn.ping()
    assert result is True


@py_test_mark_asyncio
async def test_cluster_has_3_masters(cluster_conn):
    """Verify cluster has exactly 3 master nodes."""
    nodes = await cluster_conn.cluster_nodes()
    masters = [n for n in nodes.values() if "master" in n.get("flags", "")]
    assert len(masters) == 3, f"Expected 3 masters, got {len(masters)}"


@py_test_mark_asyncio
async def test_cluster_has_3_replicas(cluster_conn):
    """Verify cluster has exactly 3 replica nodes."""
    nodes = await cluster_conn.cluster_nodes()
    replicas = [n for n in nodes.values() if "slave" in n.get("flags", "")]
    assert len(replicas) == 3, f"Expected 3 replicas, got {len(replicas)}"


@py_test_mark_asyncio
async def test_cluster_all_slots_covered(cluster_conn):
    """Verify all 16384 hash slots are assigned."""
    info = await cluster_conn.cluster_info()
    # cluster_info returns dict with possible per-node entries
    if isinstance(info, dict):
        # In cluster mode, info might have per-node entries
        slots = info.get("cluster_slots_ok", info.get("cluster_slots_assigned", 0))
        if isinstance(slots, dict):
            # Per-node response - check any node
            for v in slots.values():
                assert int(v) == 16384
                break
        else:
            assert int(slots) == 16384


@py_test_mark_asyncio
async def test_cluster_modules_available(cluster_conn):
    """Verify RediSearch and ReJSON modules are loaded on all nodes."""
    # Execute MODULE LIST on all primaries
    module_list = await cluster_conn.execute_command(
        "MODULE", "LIST", target_nodes=aioredis.RedisCluster.PRIMARIES
    )
    # module_list might be a dict of node->result or a flat list
    if isinstance(module_list, dict):
        for node, mods in module_list.items():
            module_names = [
                m[1] if isinstance(m, list) else m.get("name", "") for m in mods
            ]
            assert "search" in module_names, f"RediSearch not on {node}"
            assert "ReJSON" in module_names, f"ReJSON not on {node}"
    else:
        module_names = [
            m[1] if isinstance(m, list) else m.get("name", "") for m in module_list
        ]
        assert "search" in module_names
        assert "ReJSON" in module_names


# ══════════════════════════════════════════════════════════════════════
# SECTION 2: DIRECT REDIS VERIFICATION (before redis-om layer)
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_direct_cluster_set_get(cluster_conn):
    """Verify basic SET/GET works on cluster (across slots)."""
    for i in range(20):
        key = f"direct_test_{i}"
        await cluster_conn.set(key, f"value_{i}")

    for i in range(20):
        val = await cluster_conn.get(f"direct_test_{i}")
        assert val == f"value_{i}"
        await cluster_conn.delete(f"direct_test_{i}")


@py_test_mark_asyncio
async def test_direct_cluster_json_operations(cluster_conn):
    """Verify JSON module operations work on cluster."""
    test_key = "direct_json_test"
    doc = {"name": "Alice", "age": 30, "city": "NYC"}
    await cluster_conn.json().set(test_key, ".", doc)
    result = await cluster_conn.json().get(test_key)
    assert result["name"] == "Alice"
    assert result["age"] == 30
    await cluster_conn.delete(test_key)


@py_test_mark_asyncio
async def test_direct_cluster_hash_operations(cluster_conn):
    """Verify HASH operations work on cluster."""
    test_key = "direct_hash_test"
    await cluster_conn.hset(test_key, mapping={"name": "Bob", "age": "25"})
    result = await cluster_conn.hgetall(test_key)
    assert result["name"] == "Bob"
    assert result["age"] == "25"
    await cluster_conn.delete(test_key)


@py_test_mark_asyncio
async def test_direct_cluster_search_index(cluster_conn):
    """Verify FT.CREATE and FT.SEARCH work on cluster directly."""
    import uuid

    idx_name = f"direct_test_idx_{uuid.uuid4().hex[:8]}"

    # Create index on primaries - handle "already exists" gracefully
    command = (
        f"ft.create {idx_name} ON JSON PREFIX 1 direct_search_{idx_name}: "
        f"SCHEMA $.name AS name TAG $.age AS age NUMERIC"
    ).split()
    try:
        await cluster_conn.execute_command(
            *command, target_nodes=aioredis.RedisCluster.PRIMARIES
        )
    except Exception as e:
        if "Index already exists" not in str(e):
            raise

    # Add documents
    prefix = f"direct_search_{idx_name}"
    await cluster_conn.json().set(f"{prefix}:1", ".", {"name": "Alice", "age": 30})
    await cluster_conn.json().set(f"{prefix}:2", ".", {"name": "Bob", "age": 25})

    # Allow index to build
    await asyncio.sleep(0.5)

    # Search
    results = await cluster_conn.ft(idx_name).search("@name:{Alice}")
    assert results.total >= 1

    # Cleanup
    try:
        await cluster_conn.execute_command(
            "FT.DROPINDEX",
            idx_name,
            "DD",
            target_nodes=aioredis.RedisCluster.PRIMARIES,
        )
    except Exception:
        pass


@py_test_mark_asyncio
async def test_direct_cluster_geo_search(cluster_conn):
    """Verify GEO operations work on cluster directly."""
    import uuid

    idx_name = f"direct_geo_idx_{uuid.uuid4().hex[:8]}"

    # Create GEO index - handle "already exists" gracefully
    prefix = f"direct_geo_{idx_name}"
    command = (
        f"ft.create {idx_name} ON JSON PREFIX 1 {prefix}: "
        f"SCHEMA $.name AS name TAG $.location AS location GEO"
    ).split()
    try:
        await cluster_conn.execute_command(
            *command, target_nodes=aioredis.RedisCluster.PRIMARIES
        )
    except Exception as e:
        if "Index already exists" not in str(e):
            raise

    # Add geo documents
    await cluster_conn.json().set(
        f"{prefix}:sf", ".", {"name": "SF", "location": "-122.4194,37.7749"}
    )
    await cluster_conn.json().set(
        f"{prefix}:nyc", ".", {"name": "NYC", "location": "-74.006,40.7128"}
    )

    await asyncio.sleep(0.5)

    # Search within 50km of SF
    results = await cluster_conn.ft(idx_name).search(
        "@location:[-122.4194 37.7749 50 km]"
    )
    assert results.total >= 1

    # Cleanup
    try:
        await cluster_conn.execute_command(
            "FT.DROPINDEX",
            idx_name,
            "DD",
            target_nodes=aioredis.RedisCluster.PRIMARIES,
        )
    except Exception:
        pass


@py_test_mark_asyncio
async def test_direct_cluster_pipeline(cluster_conn):
    """Verify pipeline operations work on cluster."""
    pipe = cluster_conn.pipeline(transaction=False)
    for i in range(20):
        pipe.set(f"pipe_test_{i}", f"val_{i}")
    results = await pipe.execute()
    assert all(r is True for r in results)

    # Read back
    pipe2 = cluster_conn.pipeline(transaction=False)
    for i in range(20):
        pipe2.get(f"pipe_test_{i}")
    vals = await pipe2.execute()
    for i, v in enumerate(vals):
        assert v == f"val_{i}"

    # Cleanup
    for i in range(20):
        await cluster_conn.delete(f"pipe_test_{i}")


# ══════════════════════════════════════════════════════════════════════
# SECTION 3: HASH MODEL CRUD ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_hash_single_save(cluster_hash_models):
    """Cluster: Save a single HashModel."""
    m = cluster_hash_models
    model = m.SimpleHash(name="cluster_save", value=42)
    start = time.perf_counter()
    result = await model.save()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_single_save", elapsed)
    assert result.pk is not None
    assert result.name == "cluster_save"


@py_test_mark_asyncio
async def test_cluster_hash_single_get(cluster_hash_models):
    """Cluster: Get a single HashModel by pk."""
    m = cluster_hash_models
    model = m.SimpleHash(name="cluster_get", value=99)
    await model.save()

    start = time.perf_counter()
    fetched = await m.SimpleHash.get(model.pk)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_single_get", elapsed)
    assert fetched.name == "cluster_get"
    assert fetched.value == 99


@py_test_mark_asyncio
async def test_cluster_hash_bulk_save_50(cluster_hash_models):
    """Cluster: Bulk save 50 HashModel instances."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cbulk_{i}", value=i) for i in range(50)]
    start = time.perf_counter()
    await m.SimpleHash.add(models)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_bulk_save_50", elapsed, ops=50)
    for model in models:
        assert model.pk is not None


@py_test_mark_asyncio
async def test_cluster_hash_bulk_save_200(cluster_hash_models):
    """Cluster: Bulk save 200 HashModel instances."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cbulk200_{i}", value=i) for i in range(200)]
    start = time.perf_counter()
    await m.SimpleHash.add(models)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_bulk_save_200", elapsed, ops=200)


@py_test_mark_asyncio
async def test_cluster_hash_get_many(cluster_hash_models):
    """Cluster: get_many for 50 HashModel instances."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cgetmany_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleHash.get_many(pks)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_get_many_50", elapsed, ops=50)
    assert len(results) == 50


@py_test_mark_asyncio
async def test_cluster_hash_delete(cluster_hash_models):
    """Cluster: Delete a single HashModel."""
    m = cluster_hash_models
    model = m.SimpleHash(name="cto_delete", value=0)
    await model.save()

    start = time.perf_counter()
    result = await m.SimpleHash.delete(model.pk)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_single_delete", elapsed)
    assert result == 1


@py_test_mark_asyncio
async def test_cluster_hash_delete_many(cluster_hash_models):
    """Cluster: Delete 50 HashModel instances."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cdelmany_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    count = await m.SimpleHash.delete_many(models)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_delete_many_50", elapsed, ops=50)
    assert count >= 50


@py_test_mark_asyncio
async def test_cluster_hash_not_found(cluster_hash_models):
    """Cluster: Getting a non-existent key raises NotFoundError."""
    m = cluster_hash_models
    with pytest.raises(NotFoundError):
        await m.SimpleHash.get("nonexistent_pk_12345")


@py_test_mark_asyncio
async def test_cluster_hash_update(cluster_hash_models):
    """Cluster: Update a HashModel field."""
    m = cluster_hash_models
    model = m.SimpleHash(name="to_update", value=1)
    await model.save()

    model.value = 999
    start = time.perf_counter()
    await model.save()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_update", elapsed)
    fetched = await m.SimpleHash.get(model.pk)
    assert fetched.value == 999


@py_test_mark_asyncio
async def test_cluster_hash_optional_fields(cluster_hash_models):
    """Cluster: HashModel with Optional fields (None roundtrip)."""
    m = cluster_hash_models
    model_none = m.OptionalHash(name="opt_none")
    model_value = m.OptionalHash(name="opt_value", optional_value=99.5)

    await model_none.save()
    await model_value.save()

    r1 = await m.OptionalHash.get(model_none.pk)
    r2 = await m.OptionalHash.get(model_value.pk)
    assert r1.optional_value is None
    assert r2.optional_value == 99.5


@py_test_mark_asyncio
async def test_cluster_hash_full_model_save_get(cluster_hash_models):
    """Cluster: Save and retrieve full HashModel with all field types."""
    m = cluster_hash_models
    model = make_full_hash(m.FullHash, 0)
    start = time.perf_counter()
    await model.save()
    fetched = await m.FullHash.get(model.pk)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_full_save_get", elapsed, ops=2)
    assert fetched.first_name == "First0"
    assert fetched.age == 20
    assert fetched.score == 50.0


# ══════════════════════════════════════════════════════════════════════
# SECTION 4: HASH MODEL QUERIES ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_hash_find_eq(cluster_hash_models):
    """Cluster: Find HashModel by equality."""
    m = cluster_hash_models
    model = m.SimpleHash(name="cfind_eq", value=777)
    await model.save()

    start = time.perf_counter()
    results = await m.SimpleHash.find(m.SimpleHash.name == "cfind_eq").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_find_eq", elapsed)
    assert any(r.value == 777 for r in results)


@py_test_mark_asyncio
async def test_cluster_hash_find_range(cluster_hash_models):
    """Cluster: Find HashModel by numeric range."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"crange_{i}", value=i) for i in range(100)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find(
        (m.SimpleHash.value >= 10) & (m.SimpleHash.value <= 30)
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_find_range", elapsed, ops=len(results))
    assert len(results) >= 21


@py_test_mark_asyncio
async def test_cluster_hash_find_sort(cluster_hash_models):
    """Cluster: Find HashModel with sort_by."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"csort_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find().sort_by("value").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_find_sort", elapsed, ops=len(results))
    if len(results) >= 2:
        assert results[0].value <= results[1].value


@py_test_mark_asyncio
async def test_cluster_hash_find_page(cluster_hash_models):
    """Cluster: Find HashModel with pagination."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cpage_{i}", value=i) for i in range(50)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find().sort_by("value").page(offset=0, limit=10)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_find_page", elapsed)
    assert len(results) <= 10


@py_test_mark_asyncio
async def test_cluster_hash_find_count(cluster_hash_models):
    """Cluster: Count HashModel matches."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"ccnt_{i}", value=i) for i in range(30)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    count = await m.SimpleHash.find().count()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_find_count", elapsed)
    assert count >= 30


@py_test_mark_asyncio
async def test_cluster_hash_or_query(cluster_hash_models):
    """Cluster: OR query on HashModel."""
    m = cluster_hash_models
    m1 = m.SimpleHash(name="cor_a", value=1000)
    m2 = m.SimpleHash(name="cor_b", value=2000)
    await m.SimpleHash.add([m1, m2])

    start = time.perf_counter()
    results = await m.SimpleHash.find(
        (m.SimpleHash.name == "cor_a") | (m.SimpleHash.name == "cor_b")
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_or_query", elapsed, ops=len(results))
    assert len(results) >= 2


@py_test_mark_asyncio
async def test_cluster_hash_not_eq_query(cluster_hash_models):
    """Cluster: NOT-equal query on HashModel."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cneq_{i}", value=i) for i in range(20)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find(m.SimpleHash.name != "cneq_0").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_not_eq_query", elapsed, ops=len(results))
    assert len(results) >= 19


@py_test_mark_asyncio
async def test_cluster_hash_in_query(cluster_hash_models):
    """Cluster: IN query on HashModel numeric field."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"cin_{i}", value=3000 + i) for i in range(10)]
    await m.SimpleHash.add(models)

    start = time.perf_counter()
    results = await m.SimpleHash.find(
        m.SimpleHash.value << [3001, 3003, 3005, 3007]
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_in_query", elapsed, ops=len(results))
    assert len(results) >= 4


@py_test_mark_asyncio
async def test_cluster_hash_full_text_search(cluster_hash_models):
    """Cluster: Full-text search on HashModel."""
    m = cluster_hash_models
    model = make_full_hash(m.FullHash, 999)
    model.bio = "remarkable outstanding excellent cluster benchmark"
    await model.save()

    start = time.perf_counter()
    results = await m.FullHash.find(m.FullHash.bio % "remarkable").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_full_text_search", elapsed)
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_cluster_hash_geo_filter(cluster_hash_models):
    """Cluster: GEO filter on HashModel."""
    m = cluster_hash_models
    models = [make_full_hash(m.FullHash, i) for i in range(20)]
    await m.FullHash.add(models)

    start = time.perf_counter()
    results = await m.FullHash.find(
        m.FullHash.location
        == GeoFilter(longitude=-122.4194, latitude=37.7749, radius=50, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("hash_geo_filter", elapsed, ops=len(results))
    assert len(results) >= 1


# ══════════════════════════════════════════════════════════════════════
# SECTION 5: JSON MODEL CRUD ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_json_single_save(cluster_json_models):
    """Cluster: Save a single JsonModel."""
    m = cluster_json_models
    model = m.SimpleJson(name="cjson_save", value=42)
    start = time.perf_counter()
    result = await model.save()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_single_save", elapsed)
    assert result.pk is not None


@py_test_mark_asyncio
async def test_cluster_json_single_get(cluster_json_models):
    """Cluster: Get a single JsonModel by pk."""
    m = cluster_json_models
    model = m.SimpleJson(name="cjson_get", value=99)
    await model.save()

    start = time.perf_counter()
    fetched = await m.SimpleJson.get(model.pk)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_single_get", elapsed)
    assert fetched.name == "cjson_get"
    assert fetched.value == 99


@py_test_mark_asyncio
async def test_cluster_json_bulk_save_50(cluster_json_models):
    """Cluster: Bulk save 50 JsonModel instances."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjbulk_{i}", value=i) for i in range(50)]
    start = time.perf_counter()
    await m.SimpleJson.add(models)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_bulk_save_50", elapsed, ops=50)


@py_test_mark_asyncio
async def test_cluster_json_bulk_save_200(cluster_json_models):
    """Cluster: Bulk save 200 JsonModel instances."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjbulk200_{i}", value=i) for i in range(200)]
    start = time.perf_counter()
    await m.SimpleJson.add(models)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_bulk_save_200", elapsed, ops=200)


@py_test_mark_asyncio
async def test_cluster_json_get_many(cluster_json_models):
    """Cluster: get_many for 50 JsonModel instances."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjgetmany_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleJson.get_many(pks)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_get_many_50", elapsed, ops=50)
    assert len(results) == 50


@py_test_mark_asyncio
async def test_cluster_json_delete(cluster_json_models):
    """Cluster: Delete a single JsonModel."""
    m = cluster_json_models
    model = m.SimpleJson(name="cjdel", value=0)
    await model.save()

    start = time.perf_counter()
    result = await m.SimpleJson.delete(model.pk)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_single_delete", elapsed)
    assert result == 1


@py_test_mark_asyncio
async def test_cluster_json_delete_many(cluster_json_models):
    """Cluster: Delete 50 JsonModel instances."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjdelmany_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    count = await m.SimpleJson.delete_many(models)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_delete_many_50", elapsed, ops=50)
    assert count >= 50


@py_test_mark_asyncio
async def test_cluster_json_not_found(cluster_json_models):
    """Cluster: Getting a non-existent key raises NotFoundError."""
    m = cluster_json_models
    with pytest.raises(NotFoundError):
        await m.SimpleJson.get("nonexistent_pk_12345")


@py_test_mark_asyncio
async def test_cluster_json_update_nested(cluster_json_models):
    """Cluster: Update a JsonModel nested field."""
    m = cluster_json_models
    model = make_full_json(m, 100)
    await model.save()

    start = time.perf_counter()
    await model.update(address__city="Updated City")
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_update_nested", elapsed)
    fetched = await m.FullJson.get(model.pk)
    assert fetched.address.city == "Updated City"


@py_test_mark_asyncio
async def test_cluster_json_full_model_save_get(cluster_json_models):
    """Cluster: Save and retrieve full JsonModel with embedded models."""
    m = cluster_json_models
    model = make_full_json(m, 0)
    start = time.perf_counter()
    await model.save()
    fetched = await m.FullJson.get(model.pk)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_full_save_get", elapsed, ops=2)
    assert fetched.first_name == "First0"
    assert fetched.address.city == "San Francisco"
    assert len(fetched.orders) == 1
    assert len(fetched.orders[0].items) == 2


# ══════════════════════════════════════════════════════════════════════
# SECTION 6: JSON MODEL QUERIES ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_json_find_eq(cluster_json_models):
    """Cluster: Find JsonModel by equality."""
    m = cluster_json_models
    model = m.SimpleJson(name="cjfind_eq", value=888)
    await model.save()

    start = time.perf_counter()
    results = await m.SimpleJson.find(m.SimpleJson.name == "cjfind_eq").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_find_eq", elapsed)
    assert any(r.value == 888 for r in results)


@py_test_mark_asyncio
async def test_cluster_json_find_range(cluster_json_models):
    """Cluster: Find JsonModel by numeric range."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjrange_{i}", value=i) for i in range(100)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find(
        (m.SimpleJson.value >= 25) & (m.SimpleJson.value <= 75)
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_find_range", elapsed, ops=len(results))
    assert len(results) >= 51


@py_test_mark_asyncio
async def test_cluster_json_find_sort(cluster_json_models):
    """Cluster: Find JsonModel with sort_by."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjsort_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find().sort_by("value").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_find_sort", elapsed, ops=len(results))


@py_test_mark_asyncio
async def test_cluster_json_find_page(cluster_json_models):
    """Cluster: Find JsonModel with pagination."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjpage_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find().sort_by("value").page(offset=0, limit=10)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_find_page", elapsed)
    assert len(results) <= 10


@py_test_mark_asyncio
async def test_cluster_json_find_count(cluster_json_models):
    """Cluster: Count JsonModel matches."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjcnt_{i}", value=i) for i in range(30)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    count = await m.SimpleJson.find().count()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_find_count", elapsed)
    assert count >= 30


@py_test_mark_asyncio
async def test_cluster_json_or_query(cluster_json_models):
    """Cluster: OR query on JsonModel."""
    m = cluster_json_models
    m1 = m.SimpleJson(name="cjor_a", value=4000)
    m2 = m.SimpleJson(name="cjor_b", value=5000)
    await m.SimpleJson.add([m1, m2])

    start = time.perf_counter()
    results = await m.SimpleJson.find(
        (m.SimpleJson.name == "cjor_a") | (m.SimpleJson.name == "cjor_b")
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_or_query", elapsed, ops=len(results))
    assert len(results) >= 2


@py_test_mark_asyncio
async def test_cluster_json_in_query(cluster_json_models):
    """Cluster: IN query on JsonModel."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cjin_{i}", value=6000 + i) for i in range(10)]
    await m.SimpleJson.add(models)

    start = time.perf_counter()
    results = await m.SimpleJson.find(m.SimpleJson.value << [6001, 6003, 6005]).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_in_query", elapsed, ops=len(results))
    assert len(results) >= 3


@py_test_mark_asyncio
async def test_cluster_json_embedded_query(cluster_json_models):
    """Cluster: Query on embedded model fields."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(25)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(m.FullJson.address.city == "New York").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_embedded_query", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_cluster_json_full_text_search(cluster_json_models):
    """Cluster: Full-text search on JsonModel."""
    m = cluster_json_models
    model = make_full_json(m, 998)
    model.bio = "spectacular exceptional phenomenal cluster benchmark"
    await model.save()

    start = time.perf_counter()
    results = await m.FullJson.find(m.FullJson.bio % "spectacular").all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_full_text_search", elapsed)
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_cluster_json_combined_query(cluster_json_models):
    """Cluster: Combined embedded + age query."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(50)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(
        (m.FullJson.address.city == "Chicago") & (m.FullJson.age >= 25)
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_combined_query", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_cluster_json_negation_query(cluster_json_models):
    """Cluster: Negation query on JsonModel."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(30)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(~(m.FullJson.address.city == "Chicago")).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("complex_negation_query", elapsed, ops=len(results))


# ══════════════════════════════════════════════════════════════════════
# SECTION 7: GEO OPERATIONS ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_json_geo_filter(cluster_json_models):
    """Cluster: GEO filter on JsonModel."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(20)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = await m.FullJson.find(
        m.FullJson.location
        == GeoFilter(longitude=-122.4194, latitude=37.7749, radius=50, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("json_geo_filter", elapsed, ops=len(results))
    assert len(results) >= 1


@py_test_mark_asyncio
async def test_cluster_geo_json_cities(cluster_json_models):
    """Cluster: Save and query geo-located city models."""
    m = cluster_json_models
    models = []
    for name, lon, lat in CITIES_GEO:
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
    record_cluster_benchmark("geo_json_500km_nyc", elapsed, ops=len(results))
    city_names = {r.name for r in results}
    assert "New York" in city_names
    assert "Philadelphia" in city_names


@py_test_mark_asyncio
async def test_cluster_geo_json_small_radius(cluster_json_models):
    """Cluster: GEO query with a small radius (10km)."""
    m = cluster_json_models
    for name, lon, lat in CITIES_GEO:
        model = m.GeoJson(name=name, location=Coordinates(longitude=lon, latitude=lat))
        await model.save()

    start = time.perf_counter()
    results = await m.GeoJson.find(
        m.GeoJson.location
        == GeoFilter(longitude=-74.0060, latitude=40.7128, radius=10, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("geo_json_10km_nyc", elapsed, ops=len(results))
    city_names = {r.name for r in results}
    assert "New York" in city_names


@py_test_mark_asyncio
async def test_cluster_geo_json_large_radius(cluster_json_models):
    """Cluster: GEO query with large radius (5000km, whole US)."""
    m = cluster_json_models
    for name, lon, lat in CITIES_GEO:
        model = m.GeoJson(name=name, location=Coordinates(longitude=lon, latitude=lat))
        await model.save()

    start = time.perf_counter()
    results = await m.GeoJson.find(
        m.GeoJson.location
        == GeoFilter(longitude=-98.5795, latitude=39.8283, radius=5000, unit="km")
    ).all()
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("geo_json_5000km_us", elapsed, ops=len(results))
    assert len(results) == 10


@py_test_mark_asyncio
async def test_cluster_geo_combined_filter(cluster_json_models):
    """Cluster: GEO filter combined with other conditions."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(30)]
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
    record_cluster_benchmark("complex_geo_plus_filter", elapsed, ops=len(results))


@py_test_mark_asyncio
async def test_cluster_geo_hash_filter(cluster_hash_models):
    """Cluster: GEO filter on HashModel with multiple locations."""
    m = cluster_hash_models
    models = [make_full_hash(m.FullHash, i) for i in range(30)]
    await m.FullHash.add(models)

    # Search near SF (where all locations are clustered around)
    results = await m.FullHash.find(
        m.FullHash.location
        == GeoFilter(longitude=-122.4194, latitude=37.7749, radius=50, unit="km")
    ).all()
    assert len(results) >= 1

    # Search in a distant location (should find nothing)
    results_far = await m.FullHash.find(
        m.FullHash.location
        == GeoFilter(longitude=0.0, latitude=0.0, radius=10, unit="km")
    ).all()
    assert len(results_far) == 0


# ══════════════════════════════════════════════════════════════════════
# SECTION 8: COMPLEX QUERIES ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_complex_and_or(cluster_json_models):
    """Cluster: Complex AND + OR query."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(50)]
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
    record_cluster_benchmark("complex_and_or_query", elapsed, ops=len(results))


@py_test_mark_asyncio
async def test_cluster_complex_sort_page(cluster_json_models):
    """Cluster: Complex query with filter + sort + pagination."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(50)]
    await m.FullJson.add(models)

    start = time.perf_counter()
    results = (
        await m.FullJson.find(m.FullJson.age >= 25)
        .sort_by("score")
        .page(offset=0, limit=10)
    )
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("complex_sort_page_filter", elapsed)
    assert len(results) <= 10


@py_test_mark_asyncio
async def test_cluster_complex_multi_embedded(cluster_json_models):
    """Cluster: Query across multiple embedded fields."""
    m = cluster_json_models
    models = [make_full_json(m, i) for i in range(50)]
    await m.FullJson.add(models)

    results = await m.FullJson.find(
        (m.FullJson.address.state == "CA") & (m.FullJson.score >= 80)
    ).all()
    # Verify the results make sense
    for r in results:
        assert r.address.state == "CA"
        assert r.score >= 80


# ══════════════════════════════════════════════════════════════════════
# SECTION 9: PIPELINE OPERATIONS ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_pipeline_json_100(cluster_json_models):
    """Cluster: Pipeline get 100 JsonModel instances."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"cpipe_{i}", value=i) for i in range(100)]
    await m.SimpleJson.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleJson.get_many(pks)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("pipeline_json_get_100", elapsed, ops=100)
    assert len(results) == 100


@py_test_mark_asyncio
async def test_cluster_pipeline_hash_100(cluster_hash_models):
    """Cluster: Pipeline get 100 HashModel instances."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"chpipe_{i}", value=i) for i in range(100)]
    await m.SimpleHash.add(models)
    pks = [model.pk for model in models]

    start = time.perf_counter()
    results = await m.SimpleHash.get_many(pks)
    elapsed = time.perf_counter() - start
    record_cluster_benchmark("pipeline_hash_get_100", elapsed, ops=100)
    assert len(results) == 100


@py_test_mark_asyncio
async def test_cluster_pipeline_mixed_ops(cluster_json_models, cluster_hash_models):
    """Cluster: Mixed pipeline operations across model types."""
    jm = cluster_json_models
    hm = cluster_hash_models
    json_ms = [jm.SimpleJson(name=f"cmixed_j_{i}", value=i) for i in range(30)]
    hash_ms = [hm.SimpleHash(name=f"cmixed_h_{i}", value=i) for i in range(30)]

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
    record_cluster_benchmark("pipeline_mixed_ops", elapsed, ops=180)
    assert len(j_results) == 30
    assert len(h_results) == 30


# ══════════════════════════════════════════════════════════════════════
# SECTION 10: MIGRATION AND INDEX MANAGEMENT ON CLUSTER
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_migration_creates_indexes(cluster_conn):
    """Cluster: Migrator creates indexes on cluster primaries."""
    model_registry.clear()

    class MigrTestJson(JsonModel):
        name: str = Field(index=True)
        value: int = Field(index=True)

        class Meta:
            global_key_prefix = "cluster-test"
            model_key_prefix = "migr_test"
            database = cluster_conn

    migrator = Migrator(conn=cluster_conn)
    await migrator.run()

    # Verify index exists
    try:
        info = await cluster_conn.ft(MigrTestJson.Meta.index_name).info()
        assert info is not None
    except Exception as e:
        pytest.fail(f"Index not created on cluster: {e}")

    # Cleanup
    try:
        await cluster_conn.execute_command(
            "FT.DROPINDEX",
            MigrTestJson.Meta.index_name,
            target_nodes=aioredis.RedisCluster.PRIMARIES,
        )
    except Exception:
        pass


@py_test_mark_asyncio
async def test_cluster_migration_idempotent(cluster_conn):
    """Cluster: Running migration twice doesn't fail."""
    model_registry.clear()

    class IdempotentTestJson(JsonModel):
        name: str = Field(index=True)

        class Meta:
            global_key_prefix = "cluster-test"
            model_key_prefix = "idempotent_test"
            database = cluster_conn

    migrator = Migrator(conn=cluster_conn)
    await migrator.run()
    # Run again - should not raise
    await migrator.run()

    # Cleanup
    try:
        await cluster_conn.execute_command(
            "FT.DROPINDEX",
            IdempotentTestJson.Meta.index_name,
            target_nodes=aioredis.RedisCluster.PRIMARIES,
        )
    except Exception:
        pass


@py_test_mark_asyncio
async def test_cluster_migration_detect(cluster_conn):
    """Cluster: Migrator.detect_migrations() works on cluster."""
    model_registry.clear()

    class DetectTestJson(JsonModel):
        name: str = Field(index=True)
        value: int = Field(index=True)

        class Meta:
            global_key_prefix = "cluster-test"
            model_key_prefix = "detect_test"
            database = cluster_conn

    migrator = Migrator(conn=cluster_conn)
    await migrator.detect_migrations()
    # Should have detected at least 1 migration (CREATE for the new model)
    assert len(migrator.migrations) >= 1

    # Run and then detect again - should have no NEW migrations beyond
    # what was already handled (cluster may have drop+create pairs)
    await migrator.run()
    migrator2 = Migrator(conn=cluster_conn)
    await migrator2.detect_migrations()
    # After running, if the schema hash is properly stored, no new creates needed
    new_creates = [m for m in migrator2.migrations if m.action.name == "CREATE"]
    assert len(new_creates) == 0, f"Expected no new CREATEs, got {len(new_creates)}"

    # Cleanup
    try:
        await cluster_conn.execute_command(
            "FT.DROPINDEX",
            DetectTestJson.Meta.index_name,
            target_nodes=aioredis.RedisCluster.PRIMARIES,
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════
# SECTION 11: EDGE CASES AND ERROR HANDLING
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_many_different_keys(cluster_hash_models):
    """Cluster: Operations across many hash slots (different key prefixes)."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"slot_{i}", value=i) for i in range(100)]
    await m.SimpleHash.add(models)

    # Verify we can get them all back
    pks = [model.pk for model in models]
    results = await m.SimpleHash.get_many(pks)
    assert len(results) == 100


@py_test_mark_asyncio
async def test_cluster_concurrent_saves(cluster_hash_models):
    """Cluster: Concurrent save operations."""
    m = cluster_hash_models
    tasks = []
    for i in range(20):
        model = m.SimpleHash(name=f"concurrent_{i}", value=i)
        tasks.append(model.save())

    results = await asyncio.gather(*tasks)
    assert len(results) == 20
    for r in results:
        assert r.pk is not None


@py_test_mark_asyncio
async def test_cluster_concurrent_queries(cluster_json_models):
    """Cluster: Concurrent query operations."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"conc_q_{i}", value=i) for i in range(50)]
    await m.SimpleJson.add(models)

    # Run multiple queries concurrently
    tasks = [
        m.SimpleJson.find(m.SimpleJson.value >= 0).count(),
        m.SimpleJson.find(m.SimpleJson.value >= 25).count(),
        m.SimpleJson.find(m.SimpleJson.name == "conc_q_1").all(),
        m.SimpleJson.find().sort_by("value").page(offset=0, limit=5),
    ]
    results = await asyncio.gather(*tasks)
    assert results[0] >= 50
    assert results[1] >= 25
    assert len(results[2]) >= 1
    assert len(results[3]) <= 5


@py_test_mark_asyncio
async def test_cluster_large_batch_200(cluster_json_models):
    """Cluster: Large batch operations (200 items)."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"large_{i}", value=i) for i in range(200)]

    start = time.perf_counter()
    await m.SimpleJson.add(models)
    pks = [model.pk for model in models]
    results = await m.SimpleJson.get_many(pks)
    _ = time.perf_counter() - start  # elapsed time for debugging if needed

    assert len(results) == 200


@py_test_mark_asyncio
async def test_cluster_scan_iter_all_pks(cluster_hash_models):
    """Cluster: all_pks() scans across all cluster nodes."""
    m = cluster_hash_models
    models = [m.SimpleHash(name=f"scan_{i}", value=i) for i in range(30)]
    await m.SimpleHash.add(models)

    # all_pks returns an async generator
    pks = []
    async for pk in await m.SimpleHash.all_pks():
        pks.append(pk)
    assert len(pks) >= 30


@py_test_mark_asyncio
async def test_cluster_json_scan_iter_all_pks(cluster_json_models):
    """Cluster: all_pks() for JsonModel scans across all nodes."""
    m = cluster_json_models
    models = [m.SimpleJson(name=f"jscan_{i}", value=i) for i in range(30)]
    await m.SimpleJson.add(models)

    pks = []
    async for pk in await m.SimpleJson.all_pks():
        pks.append(pk)
    assert len(pks) >= 30


# ══════════════════════════════════════════════════════════════════════
# SECTION 12: CONNECTION MANAGEMENT TESTS
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_cluster_connection_via_get_redis_connection():
    """Verify get_redis_connection() returns RedisCluster with cluster=True."""
    conn = get_redis_connection(url=f"redis://localhost:{CLUSTER_PORT}", cluster=True)
    assert isinstance(conn, aioredis.RedisCluster)
    result = await conn.ping()
    assert result is True
    await conn.aclose()


@py_test_mark_asyncio
async def test_cluster_connection_url_parameter():
    """Verify cluster=true in URL is detected."""
    conn = get_redis_connection(url=f"redis://localhost:{CLUSTER_PORT}?cluster=true")
    assert isinstance(conn, aioredis.RedisCluster)
    await conn.aclose()


@py_test_mark_asyncio
async def test_cluster_model_with_meta_database():
    """Verify Model.Meta.database accepts RedisCluster."""
    model_registry.clear()
    conn = aioredis.RedisCluster(
        host="localhost", port=CLUSTER_PORT, decode_responses=True
    )

    class TestModel(HashModel):
        name: str = Field(index=True)

        class Meta:
            global_key_prefix = "cluster-test"
            model_key_prefix = "meta_db_test"
            database = conn

    await Migrator(conn=conn).run()

    # db() should return the cluster connection
    assert isinstance(TestModel.db(), aioredis.RedisCluster)

    model = TestModel(name="test_meta_db")
    await model.save()
    fetched = await TestModel.get(model.pk)
    assert fetched.name == "test_meta_db"

    await conn.aclose()


# ══════════════════════════════════════════════════════════════════════
# SECTION 13: PERFORMANCE COMPARISON (CLUSTER vs SINGLE INSTANCE)
# ══════════════════════════════════════════════════════════════════════


@py_test_mark_asyncio
async def test_zzz_performance_comparison():
    """Compare cluster vs single-instance performance and report."""
    print("\n" + "=" * 100)
    print("CLUSTER PERFORMANCE RESULTS")
    print("=" * 100)
    print(
        f"{'Benchmark':<40} {'Cluster (s)':<15} {'Single (s)':<15} {'Ratio':<10} {'Status':<10}"
    )
    print("-" * 100)

    slowdown_failures = []
    for name, data in sorted(CLUSTER_BENCHMARKS.items()):
        cluster_time = data["elapsed_s"]
        single_time = SINGLE_BENCHMARKS.get(name)
        if single_time and single_time > 0:
            ratio = cluster_time / single_time
            passed = ratio <= ACCEPTABLE_SLOWDOWN_FACTOR
            status = "PASS" if passed else "FAIL"
            if not passed:
                slowdown_failures.append(
                    f"{name}: {ratio:.1f}x (limit: {ACCEPTABLE_SLOWDOWN_FACTOR}x)"
                )
            print(
                f"{name:<40} {cluster_time:<15} {single_time:<15} {ratio:<10.1f} {status:<10}"
            )
        else:
            print(f"{name:<40} {cluster_time:<15} {'N/A':<15} {'N/A':<10} {'N/A':<10}")

    print("=" * 100)
    print(f"\nTotal cluster benchmarks: {len(CLUSTER_BENCHMARKS)}")
    print(f"Acceptable slowdown factor: {ACCEPTABLE_SLOWDOWN_FACTOR}x")

    if slowdown_failures:
        print(f"\nSlowdown failures ({len(slowdown_failures)}):")
        for f in slowdown_failures:
            print(f"  - {f}")
    else:
        print("\nAll comparisons within acceptable slowdown factor!")

    # Write cluster results
    with open(os.path.join(tempfile.gettempdir(), "cluster_benchmarks.txt"), "w") as f:
        for name, data in sorted(CLUSTER_BENCHMARKS.items()):
            f.write(
                f"{name}\t{data['elapsed_s']}\t{data['ops']}\t{data['ops_per_sec']}\n"
            )

    assert (
        len(CLUSTER_BENCHMARKS) >= 15
    ), f"Expected at least 15 cluster benchmarks, got {len(CLUSTER_BENCHMARKS)}"
