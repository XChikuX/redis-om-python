# type: ignore
"""Tests for complex pipeline operations.

These tests cover:
- Multi-model pipeline operations
- Pipeline combining geo operations with model queries
- Pipeline save/get_many/delete_many patterns
- Large batch pipeline operations
- Pipeline with mixed JSON and Hash operations
- delete_many() coverage
- Pipeline error handling
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
    GeoFilter,
    HashModel,
    JsonModel,
    Migrator,
    NotFoundError,
)
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip

today = datetime.date.today()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def json_models(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Category(EmbeddedJsonModel):
        name: str = Field(index=True)
        priority: int = Field(index=True)

    class Product(BaseJsonModel):
        name: str = Field(index=True)
        price: float = Field(index=True)
        category: Category
        tags: List[str] = Field(index=True)

    class Store(BaseJsonModel):
        name: str = Field(index=True)
        coordinates: Coordinates = Field(index=True)
        rating: float = Field(index=True)

    class Customer(BaseJsonModel):
        name: str = Field(index=True)
        email: str = Field(index=True)
        age: int = Field(index=True)
        bio: Optional[str] = Field(index=True, full_text_search=True, default="")

    await Migrator().run()

    return {
        "BaseJsonModel": BaseJsonModel,
        "Category": Category,
        "Product": Product,
        "Store": Store,
        "Customer": Customer,
    }


@pytest_asyncio.fixture
async def hash_models(key_prefix, redis):
    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class HashProduct(BaseHashModel):
        name: str = Field(index=True)
        price: float = Field(index=True)

    class HashStore(BaseHashModel):
        name: str = Field(index=True)
        coordinates: Coordinates = Field(index=True)

    await Migrator().run()

    return {
        "BaseHashModel": BaseHashModel,
        "HashProduct": HashProduct,
        "HashStore": HashStore,
    }


# ---------------------------------------------------------------------------
# Multi-model pipeline: Save + retrieve different types
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_save_multiple_model_types(json_models):
    """Save different model types via pipeline, then query each."""
    Product = json_models["Product"]
    Customer = json_models["Customer"]
    Category = json_models["Category"]

    products = [
        Product(
            name="Laptop",
            price=999.99,
            category=Category(name="Electronics", priority=1),
            tags=["tech", "computers"],
        ),
        Product(
            name="Book",
            price=19.99,
            category=Category(name="Education", priority=2),
            tags=["reading", "education"],
        ),
    ]
    customers = [
        Customer(name="Alice", email="alice@test.com", age=30, bio="loves tech"),
        Customer(name="Bob", email="bob@test.com", age=25, bio="avid reader"),
    ]

    # Save via add()
    await Product.add(products)
    await Customer.add(customers)

    # Query each type
    product_results = await Product.find(Product.price > 500).all()
    assert len(product_results) == 1
    assert product_results[0].name == "Laptop"

    customer_results = await Customer.find(Customer.age >= 30).all()
    assert len(customer_results) == 1
    assert customer_results[0].name == "Alice"


# ---------------------------------------------------------------------------
# Pipeline: Batch create + get_many round-trip
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_batch_create_and_get_many(json_models):
    """Create 50 models via add(), then retrieve all via get_many()."""
    Product = json_models["Product"]
    Category = json_models["Category"]

    products = [
        Product(
            name=f"Product_{i}",
            price=float(i * 10),
            category=Category(name="Bulk", priority=1),
            tags=[f"tag_{i}"],
        )
        for i in range(50)
    ]
    await Product.add(products)

    pks = [p.pk for p in products]
    results = await Product.get_many(pks)
    assert len(results) == 50

    result_names = {r.name for r in results}
    for i in range(50):
        assert f"Product_{i}" in result_names


@py_test_mark_asyncio
async def test_hash_pipeline_batch_create_and_get_many(hash_models):
    """Batch create + get_many for HashModel."""
    HashProduct = hash_models["HashProduct"]

    products = [
        HashProduct(name=f"HProduct_{i}", price=float(i * 5)) for i in range(30)
    ]
    await HashProduct.add(products)

    pks = [p.pk for p in products]
    results = await HashProduct.get_many(pks)
    assert len(results) == 30


# ---------------------------------------------------------------------------
# Pipeline: GEO operations combined with model operations
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_geo_and_product_queries(json_models):
    """Pipeline combining GEO radius search with product queries."""
    Store = json_models["Store"]
    Product = json_models["Product"]
    Category = json_models["Category"]

    # Create stores at known locations
    store_portland = Store(
        name="Portland Store",
        coordinates=Coordinates(latitude=45.5231, longitude=-122.6765),
        rating=4.5,
    )
    store_seattle = Store(
        name="Seattle Store",
        coordinates=Coordinates(latitude=47.6062, longitude=-122.3321),
        rating=4.2,
    )
    store_tokyo = Store(
        name="Tokyo Store",
        coordinates=Coordinates(latitude=35.6762, longitude=139.6503),
        rating=4.8,
    )

    await store_portland.save()
    await store_seattle.save()
    await store_tokyo.save()

    # Create products
    products = [
        Product(
            name="Widget",
            price=9.99,
            category=Category(name="Gadgets", priority=1),
            tags=["small"],
        ),
        Product(
            name="Gizmo",
            price=29.99,
            category=Category(name="Gadgets", priority=1),
            tags=["medium"],
        ),
    ]
    await Product.add(products)

    # GEO query to find nearby stores
    nearby_stores = await Store.find(
        Store.coordinates
        == GeoFilter(longitude=-122.6765, latitude=45.5231, radius=500, unit="km")
    ).all()
    nearby_pks = {s.pk for s in nearby_stores}
    assert store_portland.pk in nearby_pks
    assert store_seattle.pk in nearby_pks
    assert store_tokyo.pk not in nearby_pks

    # Also get products in same pipeline round-trip pattern
    product_results = await Product.find(Product.price < 20).all()
    assert len(product_results) == 1
    assert product_results[0].name == "Widget"


# ---------------------------------------------------------------------------
# Pipeline: delete_many
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_delete_many_json_models(json_models):
    """delete_many() removes a batch of models."""
    Customer = json_models["Customer"]

    customers = [
        Customer(name=f"User_{i}", email=f"u{i}@test.com", age=20 + i)
        for i in range(10)
    ]
    await Customer.add(customers)

    # Verify all exist
    pks = [c.pk for c in customers]
    results = await Customer.get_many(pks)
    assert len(results) == 10

    # Delete all
    deleted = await Customer.delete_many(customers)
    assert deleted == 10

    # Verify all gone
    results = await Customer.get_many(pks)
    assert len(results) == 0


@py_test_mark_asyncio
async def test_delete_many_hash_models(hash_models):
    """delete_many() for HashModel."""
    HashProduct = hash_models["HashProduct"]

    products = [HashProduct(name=f"HP_{i}", price=float(i)) for i in range(5)]
    await HashProduct.add(products)

    deleted = await HashProduct.delete_many(products)
    assert deleted == 5

    for p in products:
        with pytest.raises(NotFoundError):
            await HashProduct.get(p.pk)


# ---------------------------------------------------------------------------
# Pipeline: Mixed raw pipeline + get_many
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_mixed_raw_pipeline_and_get_many(json_models):
    """Mix raw pipeline commands with get_many pattern."""
    Store = json_models["Store"]
    Customer = json_models["Customer"]

    store = Store(
        name="Test Store",
        coordinates=Coordinates(latitude=45.5, longitude=-122.6),
        rating=4.0,
    )
    await store.save()

    customers = [
        Customer(name=f"C_{i}", email=f"c{i}@test.com", age=20 + i) for i in range(3)
    ]
    await Customer.add(customers)

    db = Store.db()

    # Raw pipeline: fetch store + GEO data
    pipe = db.pipeline(transaction=False)
    pipe.json().get(store.key())
    for c in customers:
        pipe.json().get(c.key())
    results = await pipe.execute()

    # First result is the store
    assert results[0]["name"] == "Test Store"
    # Remaining results are customers
    for i, result in enumerate(results[1:]):
        assert result["name"] == f"C_{i}"


# ---------------------------------------------------------------------------
# Pipeline: Hash model raw pipeline operations
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_hash_model_raw_pipeline(hash_models):
    """Raw pipeline operations with HashModel."""
    HashProduct = hash_models["HashProduct"]

    products = [HashProduct(name=f"P_{i}", price=float(i * 10)) for i in range(5)]
    for p in products:
        await p.save()

    db = HashProduct.db()
    pipe = db.pipeline(transaction=False)
    for p in products:
        pipe.hgetall(p.key())
    results = await pipe.execute()

    assert len(results) == 5
    for i, result in enumerate(results):
        assert result["name"] == f"P_{i}"


# ---------------------------------------------------------------------------
# Pipeline: GEO radius + get_many combined
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_georadius_get_many_combined(json_models):
    """Full pipeline: GEOADD → GEORADIUSBYMEMBER → get_many."""
    Store = json_models["Store"]

    stores = [
        Store(
            name="Portland",
            coordinates=Coordinates(latitude=45.5231, longitude=-122.6765),
            rating=4.5,
        ),
        Store(
            name="Seattle",
            coordinates=Coordinates(latitude=47.6062, longitude=-122.3321),
            rating=4.2,
        ),
        Store(
            name="Tokyo",
            coordinates=Coordinates(latitude=35.6762, longitude=139.6503),
            rating=4.8,
        ),
    ]
    for s in stores:
        await s.save()

    db = Store.db()
    geo_key = f"{stores[0].key()}:pipeline_geo"

    # Build GEO index
    await db.geoadd(
        geo_key,
        [
            -122.6765,
            45.5231,
            stores[0].pk,
            -122.3321,
            47.6062,
            stores[1].pk,
            139.6503,
            35.6762,
            stores[2].pk,
        ],
    )

    # Query nearby PKs
    nearby_pks = await db.georadiusbymember(
        geo_key, stores[0].pk, 500, unit="km", count=10
    )
    nearby_pks = [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in nearby_pks
    ]

    assert stores[0].pk in nearby_pks
    assert stores[1].pk in nearby_pks

    # Batch fetch nearby stores
    nearby_stores = await Store.get_many(nearby_pks)
    assert len(nearby_stores) == 2
    names = {s.name for s in nearby_stores}
    assert "Portland" in names
    assert "Seattle" in names

    await db.delete(geo_key)


# ---------------------------------------------------------------------------
# Pipeline: Large batch operations
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_large_batch_json_pipeline(json_models):
    """Create 100 items and query across them."""
    Product = json_models["Product"]
    Category = json_models["Category"]

    products = [
        Product(
            name=f"LargeItem_{i}",
            price=float(i),
            category=Category(name="Large", priority=1),
            tags=[f"batch_{i}"],
        )
        for i in range(100)
    ]
    await Product.add(products)

    # Query with range
    results = await Product.find((Product.price >= 50) & (Product.price <= 70)).all()
    assert len(results) == 21  # 50, 51, ... 70


@py_test_mark_asyncio
async def test_large_batch_hash_pipeline(hash_models):
    """Create 100 hash items and get_many."""
    HashProduct = hash_models["HashProduct"]

    products = [HashProduct(name=f"HLarge_{i}", price=float(i)) for i in range(100)]
    await HashProduct.add(products)

    pks = [p.pk for p in products]
    results = await HashProduct.get_many(pks)
    assert len(results) == 100


# ---------------------------------------------------------------------------
# Pipeline: Explicit pipeline passed to get_many
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_get_many_with_explicit_pipeline_hash(hash_models):
    """get_many with explicit pipeline (HashModel)."""
    HashProduct = hash_models["HashProduct"]

    products = [HashProduct(name=f"EP_{i}", price=float(i)) for i in range(3)]
    for p in products:
        await p.save()

    async with HashProduct.db().pipeline(transaction=False) as pipe:
        await HashProduct.get_many([products[0].pk, products[1].pk], pipeline=pipe)
        # When explicit pipeline, caller handles execution
        results = await pipe.execute()
        # Two HGETALL commands were queued
        assert len(results) == 2
        assert results[0]["name"] == "EP_0"
        assert results[1]["name"] == "EP_1"


# ---------------------------------------------------------------------------
# Pipeline: Save with pipeline parameter
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_save_with_explicit_pipeline(json_models):
    """Save models using explicit pipeline."""
    Product = json_models["Product"]
    Category = json_models["Category"]

    products = [
        Product(
            name=f"PipeItem_{i}",
            price=float(i * 10),
            category=Category(name="Pipe", priority=1),
            tags=[f"pipe_{i}"],
        )
        for i in range(5)
    ]

    async with Product.db().pipeline(transaction=False) as pipe:
        for p in products:
            await p.save(pipeline=pipe)
        results = await pipe.execute()
        # Each save queues commands in the pipeline
        assert len(results) > 0

    # Verify items were saved
    for p in products:
        retrieved = await Product.get(p.pk)
        assert retrieved.name == p.name


# ---------------------------------------------------------------------------
# Pipeline: Interleaved saves and queries
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_interleaved_save_query(json_models):
    """Save items, query, save more, query again."""
    Customer = json_models["Customer"]

    batch1 = [
        Customer(name=f"B1_{i}", email=f"b1_{i}@test.com", age=20 + i) for i in range(5)
    ]
    await Customer.add(batch1)

    results1 = await Customer.find(Customer.age >= 20).all()
    initial_count = len(results1)

    batch2 = [
        Customer(name=f"B2_{i}", email=f"b2_{i}@test.com", age=30 + i) for i in range(5)
    ]
    await Customer.add(batch2)

    results2 = await Customer.find(Customer.age >= 20).all()
    assert len(results2) >= initial_count + 5


# ---------------------------------------------------------------------------
# Pipeline: Combined GEO search + embedded model query
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_geo_search_combined_with_embedded_query(json_models):
    """GEO search on stores followed by product query with embedded category."""
    Store = json_models["Store"]
    Product = json_models["Product"]
    Category = json_models["Category"]

    store = Store(
        name="Nearby Store",
        coordinates=Coordinates(latitude=45.5, longitude=-122.6),
        rating=4.5,
    )
    await store.save()

    product = Product(
        name="Special Widget",
        price=49.99,
        category=Category(name="Premium", priority=1),
        tags=["special"],
    )
    await product.save()

    # GEO query for stores
    nearby = await Store.find(
        Store.coordinates
        == GeoFilter(longitude=-122.6, latitude=45.5, radius=10, unit="km")
    ).all()
    assert len(nearby) >= 1

    # Query products by embedded category
    premium = await Product.find(Product.category.name == "Premium").all()
    assert len(premium) >= 1
    assert premium[0].name == "Special Widget"


# ---------------------------------------------------------------------------
# Pipeline: Transaction mode
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_transaction_mode(json_models):
    """Pipeline with transaction=True (MULTI/EXEC)."""
    Customer = json_models["Customer"]

    customers = [
        Customer(name=f"TX_{i}", email=f"tx{i}@test.com", age=20 + i) for i in range(3)
    ]

    db = Customer.db()
    pipe = db.pipeline(transaction=True)
    for c in customers:
        await c.save(pipeline=pipe)
    results = await pipe.execute()
    assert len(results) > 0

    # Verify saved
    for c in customers:
        retrieved = await Customer.get(c.pk)
        assert retrieved.name == c.name


# ---------------------------------------------------------------------------
# Pipeline: Multiple geo indexes in single pipeline
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_multiple_geo_indexes_pipeline(json_models):
    """Multiple GEO indexes queried in a single pipeline."""
    Store = json_models["Store"]

    stores = [
        Store(
            name="S1",
            coordinates=Coordinates(latitude=45.5, longitude=-122.6),
            rating=4.0,
        ),
        Store(
            name="S2",
            coordinates=Coordinates(latitude=47.6, longitude=-122.3),
            rating=4.5,
        ),
        Store(
            name="S3",
            coordinates=Coordinates(latitude=35.6, longitude=139.6),
            rating=4.8,
        ),
    ]
    for s in stores:
        await s.save()

    db = Store.db()
    geo_key_1 = f"{stores[0].key()}:geo1"
    geo_key_2 = f"{stores[0].key()}:geo2"

    # Add to two different geo sets
    pipe = db.pipeline(transaction=False)
    pipe.geoadd(geo_key_1, [-122.6, 45.5, stores[0].pk, -122.3, 47.6, stores[1].pk])
    pipe.geoadd(geo_key_2, [139.6, 35.6, stores[2].pk, -122.6, 45.5, stores[0].pk])
    await pipe.execute()

    # Query both geo indexes in one pipeline
    pipe = db.pipeline(transaction=False)
    pipe.georadiusbymember(geo_key_1, stores[0].pk, 500, unit="km", count=10)
    pipe.georadiusbymember(geo_key_2, stores[2].pk, 100, unit="km", count=10)
    results = await pipe.execute()

    # First geo: Portland radius should include Portland + Seattle
    nearby_1 = [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in results[0]
    ]
    assert stores[0].pk in nearby_1

    # Second geo: Tokyo radius should only include Tokyo
    nearby_2 = [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in results[1]
    ]
    assert stores[2].pk in nearby_2

    await db.delete(geo_key_1)
    await db.delete(geo_key_2)


# ---------------------------------------------------------------------------
# Pipeline: Concurrent JSON + Hash operations
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_json_and_hash_concurrent(json_models, hash_models):
    """Save JSON and Hash models, then query both."""
    Product = json_models["Product"]
    Category = json_models["Category"]
    HashProduct = hash_models["HashProduct"]

    json_product = Product(
        name="JSON_Product",
        price=100.0,
        category=Category(name="Mixed", priority=1),
        tags=["json"],
    )
    hash_product = HashProduct(name="Hash_Product", price=200.0)

    await json_product.save()
    await hash_product.save()

    # Query JSON model
    json_results = await Product.find(Product.name == "JSON_Product").all()
    assert len(json_results) == 1

    # Query Hash model
    hash_result = await HashProduct.get(hash_product.pk)
    assert hash_result.name == "Hash_Product"

    # Get both via get_many
    json_many = await Product.get_many([json_product.pk])
    hash_many = await HashProduct.get_many([hash_product.pk])
    assert len(json_many) == 1
    assert len(hash_many) == 1
