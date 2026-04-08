# type: ignore
"""Tests for pipeline operations and GEORADIUS integration.

These tests cover:
- ``get_many()`` pipeline-based bulk retrieval (upstream issue #523)
- Mixed pipeline operations combining GEORADIUS with regular commands
- GEORADIUS/GEOSEARCH integration with redis-om models
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

from tests._sync_redis import has_redis_json, has_redisearch

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

    class UserLocation(BaseJsonModel):
        name: str = Field(index=True)
        coordinates: Coordinates = Field(index=True)

    class SimpleItem(BaseJsonModel):
        title: str = Field(index=True)
        price: float = Field(index=True)

    await Migrator().run()

    return {
        "BaseJsonModel": BaseJsonModel,
        "UserLocation": UserLocation,
        "SimpleItem": SimpleItem,
    }


@pytest_asyncio.fixture
async def hash_models(key_prefix, redis):
    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class HashUserLocation(BaseHashModel):
        name: str = Field(index=True)
        coordinates: Coordinates = Field(index=True)

    class HashItem(BaseHashModel):
        title: str = Field(index=True)
        price: float = Field(index=True)

    await Migrator().run()

    return {
        "BaseHashModel": BaseHashModel,
        "HashUserLocation": HashUserLocation,
        "HashItem": HashItem,
    }


# ---------------------------------------------------------------------------
# get_many() tests — JsonModel
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_json_get_many_returns_all_models(json_models):
    SimpleItem = json_models["SimpleItem"]
    items = [
        SimpleItem(title="Widget", price=9.99),
        SimpleItem(title="Gadget", price=19.99),
        SimpleItem(title="Gizmo", price=29.99),
    ]
    for item in items:
        await item.save()

    pks = [item.pk for item in items]
    results = await SimpleItem.get_many(pks)

    assert len(results) == 3
    result_pks = {r.pk for r in results}
    for item in items:
        assert item.pk in result_pks


@py_test_mark_asyncio
async def test_json_get_many_skips_missing_keys(json_models):
    SimpleItem = json_models["SimpleItem"]
    item = SimpleItem(title="Existing", price=5.0)
    await item.save()

    results = await SimpleItem.get_many([item.pk, "nonexistent-pk-12345"])
    assert len(results) == 1
    assert results[0].pk == item.pk


@py_test_mark_asyncio
async def test_json_get_many_empty_list(json_models):
    SimpleItem = json_models["SimpleItem"]
    results = await SimpleItem.get_many([])
    assert results == []


@py_test_mark_asyncio
async def test_json_get_many_with_explicit_pipeline(json_models):
    SimpleItem = json_models["SimpleItem"]
    items = [
        SimpleItem(title="A", price=1.0),
        SimpleItem(title="B", price=2.0),
    ]
    for item in items:
        await item.save()

    # When an explicit pipeline is passed, get_many queues commands and
    # the caller is responsible for executing.
    async with SimpleItem.db().pipeline(transaction=False) as pipe:
        await SimpleItem.get_many([items[0].pk], pipeline=pipe)
        # The test just verifies no exception; actual results come from
        # pipeline execution handled by the caller.


# ---------------------------------------------------------------------------
# get_many() tests — HashModel
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_hash_get_many_returns_all_models(hash_models):
    HashItem = hash_models["HashItem"]
    items = [
        HashItem(title="Hammer", price=12.50),
        HashItem(title="Screwdriver", price=8.00),
        HashItem(title="Wrench", price=15.00),
    ]
    for item in items:
        await item.save()

    pks = [item.pk for item in items]
    results = await HashItem.get_many(pks)

    assert len(results) == 3
    result_pks = {r.pk for r in results}
    for item in items:
        assert item.pk in result_pks


@py_test_mark_asyncio
async def test_hash_get_many_skips_missing_keys(hash_models):
    HashItem = hash_models["HashItem"]
    item = HashItem(title="Present", price=3.0)
    await item.save()

    results = await HashItem.get_many([item.pk, "nonexistent-pk-67890"])
    assert len(results) == 1
    assert results[0].pk == item.pk


@py_test_mark_asyncio
async def test_hash_get_many_empty_list(hash_models):
    HashItem = hash_models["HashItem"]
    results = await HashItem.get_many([])
    assert results == []


# ---------------------------------------------------------------------------
# GEORADIUS + pipeline combined operations
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_with_georadius_and_get(json_models):
    """Pipeline a GEORADIUS lookup together with regular GET commands.

    This is the pattern from the problem statement — query nearby user PKs
    using GEOADD / GEORADIUSBYMEMBER and then batch-fetch models, all in
    minimal round-trips.
    """
    UserLocation = json_models["UserLocation"]

    # Create users at known locations
    portland = UserLocation(
        name="Portland User",
        coordinates=(45.5231, -122.6765),
    )
    seattle = UserLocation(
        name="Seattle User",
        coordinates=(47.6062, -122.3321),
    )
    tokyo = UserLocation(
        name="Tokyo User",
        coordinates=(35.6762, 139.6503),
    )
    await portland.save()
    await seattle.save()
    await tokyo.save()

    db = UserLocation.db()

    # Step 1: Add users to a GEO sorted set (simulating the pattern from
    # the problem statement).
    geo_key = f"{portland.key()}:geo_index"
    await db.geoadd(
        geo_key,
        [
            -122.6765,
            45.5231,
            portland.pk,
            -122.3321,
            47.6062,
            seattle.pk,
            139.6503,
            35.6762,
            tokyo.pk,
        ],
    )

    # Step 2: Pipeline a GEORADIUSBYMEMBER + multiple HGET/JSON.GET in one
    # network round-trip.
    pipe = db.pipeline(transaction=False)

    # Queue the geo radius query
    pipe.georadiusbymember(geo_key, portland.pk, 500, unit="km", count=10)

    # Also queue a regular get for a specific known user
    pipe.json().get(tokyo.key())

    results = await pipe.execute()

    # results[0] = GEORADIUSBYMEMBER response (list of member names)
    nearby_pks = results[0]
    assert portland.pk in [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in nearby_pks
    ]
    assert seattle.pk in [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in nearby_pks
    ]
    # Tokyo is ~8,000 km away so should NOT be in the 500km radius
    assert tokyo.pk not in [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in nearby_pks
    ]

    # results[1] = JSON.GET response for Tokyo
    assert results[1] is not None
    assert results[1]["name"] == "Tokyo User"

    # Clean up geo key
    await db.delete(geo_key)


@py_test_mark_asyncio
async def test_pipeline_georadius_then_get_many(json_models):
    """Full pipeline flow: GEORADIUSBYMEMBER → get_many() for nearby users."""
    UserLocation = json_models["UserLocation"]

    # Create users
    user_a = UserLocation(name="User A", coordinates=(40.7128, -74.0060))  # NYC
    user_b = UserLocation(name="User B", coordinates=(40.7580, -73.9855))  # Midtown
    user_c = UserLocation(name="User C", coordinates=(34.0522, -118.2437))  # LA

    await user_a.save()
    await user_b.save()
    await user_c.save()

    db = UserLocation.db()
    geo_key = f"{user_a.key()}:geo_nearby"

    # Build a GEO index in one pipeline call
    pipe = db.pipeline(transaction=False)
    pipe.geoadd(
        geo_key,
        [
            -74.0060,
            40.7128,
            user_a.pk,
            -73.9855,
            40.7580,
            user_b.pk,
            -118.2437,
            34.0522,
            user_c.pk,
        ],
    )
    await pipe.execute()

    # Query nearby PKs
    nearby_pks = await db.georadiusbymember(geo_key, user_a.pk, 50, unit="km", count=10)
    nearby_pks = [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in nearby_pks
    ]

    # Should include NYC and Midtown, not LA
    assert user_a.pk in nearby_pks
    assert user_b.pk in nearby_pks
    assert user_c.pk not in nearby_pks

    # Use get_many to fetch all nearby models in one pipeline
    nearby_users = await UserLocation.get_many(nearby_pks)
    assert len(nearby_users) == 2
    nearby_names = {u.name for u in nearby_users}
    assert "User A" in nearby_names
    assert "User B" in nearby_names

    await db.delete(geo_key)


@py_test_mark_asyncio
async def test_pipeline_geosearch_with_hash_model(hash_models):
    """GEOSEARCH with HashModel — the same pipeline pattern works."""
    HashUserLocation = hash_models["HashUserLocation"]

    london = HashUserLocation(
        name="London",
        coordinates=(51.5074, -0.1278),
    )
    paris = HashUserLocation(
        name="Paris",
        coordinates=(48.8566, 2.3522),
    )
    sydney = HashUserLocation(
        name="Sydney",
        coordinates=(-33.8688, 151.2093),
    )

    await london.save()
    await paris.save()
    await sydney.save()

    db = HashUserLocation.db()
    geo_key = f"{london.key()}:geo_index"
    await db.geoadd(
        geo_key,
        [
            -0.1278,
            51.5074,
            london.pk,
            2.3522,
            48.8566,
            paris.pk,
            151.2093,
            -33.8688,
            sydney.pk,
        ],
    )

    # Pipeline: geo search + hgetall for specific model
    pipe = db.pipeline(transaction=False)
    pipe.georadiusbymember(geo_key, london.pk, 500, unit="km", count=10)
    pipe.hgetall(sydney.key())
    results = await pipe.execute()

    nearby_pks = [
        pk.decode("utf-8") if isinstance(pk, bytes) else pk for pk in results[0]
    ]
    assert london.pk in nearby_pks
    assert paris.pk in nearby_pks
    assert sydney.pk not in nearby_pks

    # The hgetall result for Sydney is still valid
    assert results[1]["name"] == "Sydney"

    # Use get_many for the nearby locations
    nearby = await HashUserLocation.get_many(nearby_pks)
    assert len(nearby) == 2

    await db.delete(geo_key)


# ---------------------------------------------------------------------------
# Coordinates serialization round-trip
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_json_coordinates_save_and_get(json_models):
    """Coordinates survive a save/get round-trip via JsonModel."""
    UserLocation = json_models["UserLocation"]

    loc = UserLocation(name="Test", coordinates=(45.5231, -122.6765))
    await loc.save()

    retrieved = await UserLocation.get(loc.pk)
    assert retrieved.coordinates.latitude == 45.5231
    assert retrieved.coordinates.longitude == -122.6765


@py_test_mark_asyncio
async def test_hash_coordinates_save_and_get(hash_models):
    """Coordinates survive a save/get round-trip via HashModel."""
    HashUserLocation = hash_models["HashUserLocation"]

    loc = HashUserLocation(name="Test", coordinates=(51.5074, -0.1278))
    await loc.save()

    retrieved = await HashUserLocation.get(loc.pk)
    assert retrieved.coordinates.latitude == 51.5074
    assert retrieved.coordinates.longitude == -0.1278


@py_test_mark_asyncio
async def test_json_coordinates_geo_filter_search(json_models):
    """GeoFilter search works after Coordinates fix."""
    UserLocation = json_models["UserLocation"]

    loc = UserLocation(name="Portland", coordinates=(45.5231, -122.6765))
    await loc.save()

    found = await UserLocation.find(
        UserLocation.coordinates
        == GeoFilter(longitude=-122.6765, latitude=45.5231, radius=10, unit="mi")
    ).first()
    assert found.pk == loc.pk
    assert found.name == "Portland"


@py_test_mark_asyncio
async def test_hash_coordinates_geo_filter_search(hash_models):
    """GeoFilter search works for HashModel after Coordinates fix."""
    HashUserLocation = hash_models["HashUserLocation"]

    loc = HashUserLocation(name="London", coordinates=(51.5074, -0.1278))
    await loc.save()

    found = await HashUserLocation.find(
        HashUserLocation.coordinates
        == GeoFilter(longitude=-0.1278, latitude=51.5074, radius=10, unit="mi")
    ).first()
    assert found.pk == loc.pk
    assert found.name == "London"


# ---------------------------------------------------------------------------
# Pipeline + save/delete combined operations
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_pipeline_save_and_get_many_json(json_models):
    """Save many items via pipeline, then retrieve with get_many."""
    SimpleItem = json_models["SimpleItem"]

    items = [SimpleItem(title=f"Item {i}", price=float(i)) for i in range(10)]
    await SimpleItem.add(items)

    pks = [item.pk for item in items]
    results = await SimpleItem.get_many(pks)
    assert len(results) == 10
    for i, result in enumerate(sorted(results, key=lambda x: x.price)):
        assert result.price == float(i)


@py_test_mark_asyncio
async def test_pipeline_save_and_get_many_hash(hash_models):
    """Save many items via pipeline, then retrieve with get_many."""
    HashItem = hash_models["HashItem"]

    items = [HashItem(title=f"Tool {i}", price=float(i)) for i in range(10)]
    await HashItem.add(items)

    pks = [item.pk for item in items]
    results = await HashItem.get_many(pks)
    assert len(results) == 10
    for i, result in enumerate(sorted(results, key=lambda x: x.price)):
        assert result.price == float(i)
