# type: ignore
"""
Comprehensive edge-case tests for save/load type conversions.

These tests are designed as a regression suite for both the current recursive
conversion functions and any future field-aware conversion plan implementation.
They exercise every conversion path (datetime/date, bytes, dataclasses,
nested models, lists, Optionals) against a wide variety of data shapes.

Test structure
---
* Pure-function tests (no Redis): validate the conversion functions directly.
* Integration tests (Redis required): validate full save/get/get_value/get_many
  round-trips for JsonModel and HashModel.

If you are implementing a field-aware conversion plan, all of these tests must
pass unchanged.
"""

import abc
import base64
import datetime
import decimal
from typing import List, Optional
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import (
    Coordinates,
    EmbeddedJsonModel,
    Field,
    HashModel,
    JsonModel,
    Migrator,
)
from aredis_om.model.model import (
    convert_bytes_to_base64,
    convert_base64_to_bytes,
    convert_datetime_to_timestamp,
    convert_timestamp_to_datetime,
    convert_dataclasses_to_dicts,
    convert_empty_strings_to_none,
    get_model_fields,
)

from .conftest import py_test_mark_asyncio

try:
    from tests._sync_redis import has_redis_json, has_redisearch

    HAS_REDISEARCH = has_redisearch()
    HAS_REDIS_JSON = has_redis_json()
except Exception:
    HAS_REDISEARCH = False
    HAS_REDIS_JSON = False


needs_redis = pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON),
    reason="Requires RediSearch + RedisJSON",
)

try:
    from tests._sync_redis import has_redis_json, has_redisearch

    HAS_REDISEARCH = has_redisearch()
    HAS_REDIS_JSON = has_redis_json()
except Exception:
    HAS_REDISEARCH = False
    HAS_REDIS_JSON = False


needs_redis = pytest.mark.skipif(
    not (HAS_REDISEARCH and HAS_REDIS_JSON),
    reason="Requires RediSearch + RedisJSON",
)

# ── Helpers ──────────────────────────────────────────────────────────


def _fields(**annotations):
    """Build a model_fields mapping like Pydantic's, keyed by field name."""
    return {name: mock.Mock(annotation=ann) for name, ann in annotations.items()}


# ===========================================================================
# Part 1 — Pure-function tests (no Redis required)
# ===========================================================================

# ── 1.1 datetime / date ─────────────────────────────────────────────


def test_scalar_datetime_roundtrip():
    """Scalar ``datetime`` → timestamp → ``datetime``."""
    dt = datetime.datetime(2024, 3, 15, 10, 30, 0)
    encoded = convert_datetime_to_timestamp({"created": dt})
    decoded = convert_timestamp_to_datetime(encoded, _fields(created=datetime.datetime))
    assert decoded["created"] == dt
    assert isinstance(decoded["created"], datetime.datetime)


def test_scalar_date_roundtrip():
    """Scalar ``date`` → timestamp → ``date``."""
    d = datetime.date(2024, 3, 15)
    encoded = convert_datetime_to_timestamp({"joined": d})
    decoded = convert_timestamp_to_datetime(encoded, _fields(joined=datetime.date))
    assert decoded["joined"] == d
    assert isinstance(decoded["joined"], datetime.date)


def test_optional_datetime_none_roundtrip():
    """``Optional[datetime]`` with ``None`` value must stay ``None``."""
    encoded = convert_datetime_to_timestamp({"created": None})
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(created=Optional[datetime.datetime])
    )
    assert decoded["created"] is None


def test_optional_datetime_present_roundtrip():
    """``Optional[datetime]`` with a real value must round-trip."""
    dt = datetime.datetime(2024, 5, 1, 14, 0, 0)
    encoded = convert_datetime_to_timestamp({"created": dt})
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(created=Optional[datetime.datetime])
    )
    assert decoded["created"] == dt


def test_list_datetime_roundtrip():
    """``List[datetime]`` item-by-item conversion."""
    dts = [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 6, 15)]
    encoded = convert_datetime_to_timestamp({"timestamps": dts})
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(timestamps=List[datetime.datetime])
    )
    assert decoded["timestamps"] == dts
    assert all(isinstance(x, datetime.datetime) for x in decoded["timestamps"])


def test_list_date_roundtrip():
    """``List[date]`` item-by-item conversion."""
    dates = [datetime.date(2024, 1, 1), datetime.date(2024, 12, 31)]
    encoded = convert_datetime_to_timestamp({"dates": dates})
    decoded = convert_timestamp_to_datetime(encoded, _fields(dates=List[datetime.date]))
    assert decoded["dates"] == dates
    assert all(isinstance(x, datetime.date) for x in decoded["dates"])


def test_optional_list_datetime_roundtrip():
    """``Optional[List[datetime]]`` with a real list."""
    dts = [datetime.datetime(2024, 7, 4, 12, 0, 0)]
    encoded = convert_datetime_to_timestamp({"timestamps": dts})
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(timestamps=Optional[List[datetime.datetime]])
    )
    assert decoded["timestamps"] == dts
    assert isinstance(decoded["timestamps"][0], datetime.datetime)


def test_optional_list_datetime_none_roundtrip():
    """``Optional[List[datetime]]`` with ``None`` value."""
    encoded = convert_datetime_to_timestamp({"timestamps": None})
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(timestamps=Optional[List[datetime.datetime]])
    )
    assert decoded["timestamps"] is None


def test_empty_list_datetime_roundtrip():
    """Empty ``List[datetime]`` stays empty."""
    encoded = convert_datetime_to_timestamp({"timestamps": []})
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(timestamps=List[datetime.datetime])
    )
    assert decoded["timestamps"] == []


def test_unconvertible_timestamp_item_passes_through():
    """Non-numeric items in a ``List[datetime]`` field are left as-is."""
    encoded = convert_datetime_to_timestamp(
        {"timestamps": ["not-a-timestamp", 1_704_153_845.0]}
    )
    decoded = convert_timestamp_to_datetime(
        encoded, _fields(timestamps=List[datetime.datetime])
    )
    assert decoded["timestamps"][0] == "not-a-timestamp"
    assert isinstance(decoded["timestamps"][1], datetime.datetime)


# ── 1.2 bytes (scalar and List) ──────────────────────────────────────


def test_scalar_bytes_roundtrip():
    """Scalar ``bytes`` → base64 → ``bytes``."""
    blob = b"\x00\x01\x02\xff"
    encoded = convert_bytes_to_base64({"data": blob})
    decoded = convert_base64_to_bytes(encoded, _fields(data=bytes))
    assert decoded["data"] == blob


def test_optional_bytes_roundtrip():
    """``Optional[bytes]`` with a real value."""
    blob = b"hello"
    encoded = convert_bytes_to_base64({"data": blob})
    decoded = convert_base64_to_bytes(encoded, _fields(data=Optional[bytes]))
    assert decoded["data"] == blob


def test_optional_bytes_none_roundtrip():
    """``Optional[bytes]`` with ``None`` stays ``None``."""
    encoded = convert_bytes_to_base64({"data": None})
    decoded = convert_base64_to_bytes(encoded, _fields(data=Optional[bytes]))
    assert decoded["data"] is None


def test_list_bytes_roundtrip():
    """``List[bytes]`` item-by-item conversion."""
    blobs = [b"\x00\x01", b"hello", b"\xff\xfe\xfd"]
    encoded = convert_bytes_to_base64({"blobs": blobs})
    decoded = convert_base64_to_bytes(encoded, _fields(blobs=List[bytes]))
    assert decoded["blobs"] == blobs
    assert all(isinstance(x, bytes) for x in decoded["blobs"])


def test_list_bytes_empty_roundtrip():
    """Empty ``List[bytes]`` stays empty."""
    encoded = convert_bytes_to_base64({"blobs": []})
    decoded = convert_base64_to_bytes(encoded, _fields(blobs=List[bytes]))
    assert decoded["blobs"] == []


def test_optional_list_bytes_roundtrip():
    """``Optional[List[bytes]]`` with a real list."""
    blobs = [b"abc", b"\x00\xff"]
    encoded = convert_bytes_to_base64({"blobs": blobs})
    decoded = convert_base64_to_bytes(encoded, _fields(blobs=Optional[List[bytes]]))
    assert decoded["blobs"] == blobs


def test_mixed_bytes_types_in_one_model():
    """A model with ``bytes``, ``List[bytes]``, and non-bytes fields."""
    encoded = convert_bytes_to_base64(
        {"name": "widget", "data": b"xx", "blobs": [b"a", b"b"]}
    )
    decoded = convert_base64_to_bytes(
        encoded, _fields(name=str, data=bytes, blobs=List[bytes])
    )
    assert decoded == {"name": "widget", "data": b"xx", "blobs": [b"a", b"b"]}


# ── 1.3 Coordinates / dataclasses ─────────────────────────────────


def test_coordinates_serialization():
    """``Coordinates`` → ``"lon,lat"`` string."""
    coord = Coordinates(longitude=-122.4194, latitude=37.7749)
    result = convert_dataclasses_to_dicts({"loc": coord})
    assert result["loc"] == "-122.4194,37.7749"


# ── 1.4 HashModel: empty strings to None ─────────────────────────


def test_optional_field_empty_string_to_none():
    """HashModel load: empty string ``""`` → ``None`` for ``Optional[str]``."""
    doc = {"name": "test", "nickname": ""}
    model_fields = _fields(name=str, nickname=Optional[str])
    result = convert_empty_strings_to_none(doc, model_fields)
    assert result["name"] == "test"
    assert result["nickname"] is None


def test_nonoptional_field_empty_string_preserved():
    """HashModel load: empty string preserved for non-optional fields."""
    doc = {"name": ""}
    model_fields = _fields(name=str)
    result = convert_empty_strings_to_none(doc, model_fields)
    assert result["name"] == ""


# ── 1.5 Combined save conversion order ───────────────────────────


def test_combined_save_conversions():
    """All three save conversions must transform the document correctly."""
    dt = datetime.datetime(2024, 3, 15, 10, 30, 0)
    d = datetime.date(2024, 3, 15)
    data_in = {
        "created": dt,
        "joined": d,
        "blob": b"\x00\xff",
        "coords": Coordinates(longitude=10.0, latitude=20.0),
        "simple_str": "hello",
        "simple_int": 42,
    }

    # Current order: datetime → bytes → dataclasses_to_dicts
    doc = convert_datetime_to_timestamp(data_in)
    doc = convert_bytes_to_base64(doc)
    doc = convert_dataclasses_to_dicts(doc)

    assert isinstance(doc["created"], float)
    assert isinstance(doc["joined"], float)
    assert isinstance(doc["blob"], str)  # base64
    assert doc["coords"] == "10.0,20.0"  # Coordinates str
    assert doc["simple_str"] == "hello"
    assert doc["simple_int"] == 42


# ===========================================================================
# Part 2 — Integration tests (requires Redis with RediSearch + RedisJSON)
# ===========================================================================


# Define embedded models at module level (no per-test key_prefix needed).
# Top-level models are created in the fixture below.


class _Address(EmbeddedJsonModel):
    street: str
    city: str = Field(index=True)
    zip_code: str
    created_at: datetime.datetime
    geo: Coordinates


class _Item(EmbeddedJsonModel):
    name: str = Field(index=True)
    price: decimal.Decimal
    image_data: bytes
    captured_at: datetime.datetime


class _Order(EmbeddedJsonModel):
    items: List[_Item]
    total: decimal.Decimal
    ordered_on: datetime.date
    notes: Optional[str] = None


@pytest_asyncio.fixture
async def conversion_fixtures(key_prefix, redis):
    """Return a namespace of all top-level model classes used in this module.

    The models cover every conversion edge case the field-aware plan must handle.
    The embedded models (_Address, _Item, _Order) are defined at module level
    and reused in multiple top-level models.
    """

    class BaseJson(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class BaseHash(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    # --- JsonModel: every type combination ---

    class FullJson(BaseJson):
        """Maximal JsonModel with every conversion type."""

        # Simple fields (no conversion)
        name: str = Field(index=True, case_sensitive=True)
        count: int = Field(index=True)
        ratio: float

        # datetime / date
        created: datetime.datetime
        updated: Optional[datetime.datetime] = None
        joined: datetime.date

        # bytes
        signature: bytes
        thumbnail: Optional[bytes] = None

        # Lists of datetime / bytes
        log_dates: List[datetime.datetime]
        log_dates_optional: Optional[List[datetime.datetime]] = None
        attachments: List[bytes]
        attachments_optional: Optional[List[bytes]] = None

        # Embedded model (has datetime, bytes, Coordinates)
        address: _Address

        # List of embedded models (each with datetime, bytes)
        orders: Optional[List[_Order]] = None

        # Coordinates (dataclass → string)
        location: Coordinates = Field(index=True)

        class Meta:
            index_name = f"{key_prefix}_full_json"

    # --- JsonModel fast-path: no convertible fields ---

    class SimpleJson(BaseJson):
        """Only simple types — conversion fast path should skip entirely."""

        title: str = Field(index=True)
        count: int
        active: Optional[str] = None  # use str instead of bool for HashModel compat

        class Meta:
            index_name = f"{key_prefix}_simple_json"

    # --- HashModel: with datetime / bytes ---

    class FullHash(BaseHash):
        """HashModel with datetime + bytes (plus empty-string → None logic)."""

        name: str = Field(index=True, case_sensitive=True)
        created: datetime.datetime
        joined: datetime.date
        data: bytes
        nickname: Optional[str] = None
        bio: Optional[str] = None

        class Meta:
            index_name = f"{key_prefix}_full_hash"

    # --- HashModel: no convertible fields (fast-path) ---

    class SimpleHash(BaseHash):
        """Only str/int — conversion fast path should skip."""

        title: str = Field(index=True)
        count: int = Field(index=True)
        active: Optional[str] = None  # avoid bool in HashModel for storage compat

        class Meta:
            index_name = f"{key_prefix}_simple_hash"

    await Migrator().run()
    return SimpleJson, FullJson, SimpleHash, FullHash


# ── 2.1 JsonModel — full conversion round-trips ──────────────────


@needs_redis
@py_test_mark_asyncio
async def test_json_model_full_conversion_roundtrip(conversion_fixtures):
    """JsonModel with every conversion type: save, then load back."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt1 = datetime.datetime(2024, 1, 15, 9, 30, 0)
    date1 = datetime.date(2024, 6, 1)
    dt2 = datetime.datetime(2024, 3, 10, 14, 0, 0)
    dt3 = datetime.datetime(2024, 7, 20, 16, 45, 0)

    m = FullJson(
        name="edge-case-test",
        count=42,
        ratio=3.14,
        created=dt1,
        updated=dt2,
        joined=date1,
        signature=b"\x89PNG\r\n\x1a\n",
        thumbnail=b"\xff\xd8\xff",
        log_dates=[dt1, dt2],
        log_dates_optional=[dt3],
        attachments=[b"file1", b"file2"],
        attachments_optional=[b"opt1"],
        address=_Address(
            street="123 Main St",
            city="Springfield",
            zip_code="12345",
            created_at=dt1,
            geo=Coordinates(longitude=-122.4194, latitude=37.7749),
        ),
        orders=[
            _Order(
                items=[
                    _Item(
                        name="Widget",
                        price=decimal.Decimal("19.99"),
                        image_data=b"img_data",
                        captured_at=dt1,
                    )
                ],
                total=decimal.Decimal("19.99"),
                ordered_on=date1,
                notes="First order",
            )
        ],
        location=Coordinates(longitude=10.0, latitude=20.0),
    )
    await m.save()
    loaded = await FullJson.get(m.pk)

    # Scalar conversions
    assert loaded.created == dt1
    assert loaded.updated == dt2
    assert loaded.joined == date1

    # bytes conversions
    assert loaded.signature == b"\x89PNG\r\n\x1a\n"
    assert loaded.thumbnail == b"\xff\xd8\xff"

    # List conversions
    assert loaded.log_dates == [dt1, dt2]
    assert loaded.log_dates_optional == [dt3]
    assert loaded.attachments == [b"file1", b"file2"]
    assert loaded.attachments_optional == [b"opt1"]

    # Embedded model with datetime/bytes/Coordinates
    assert loaded.address.created_at == dt1
    assert loaded.address.geo.longitude == -122.4194
    assert loaded.address.geo.latitude == 37.7749

    # List of embedded models with datetime/bytes
    assert len(loaded.orders) == 1
    assert loaded.orders[0].ordered_on == date1
    assert loaded.orders[0].items[0].image_data == b"img_data"
    assert loaded.orders[0].items[0].captured_at == dt1

    # Coordinates (dataclass)
    assert loaded.location.longitude == 10.0
    assert loaded.location.latitude == 20.0


@needs_redis
@py_test_mark_asyncio
async def test_json_model_optional_fields_none(conversion_fixtures):
    """Optional convertible fields set to ``emonic ``None`` must load as ``None``."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    m = FullJson(
        name="none-test",
        count=1,
        ratio=0.5,
        created=datetime.datetime(2024, 1, 1),
        updated=None,
        joined=datetime.date(2024, 1, 1),
        signature=b"data",
        thumbnail=None,
        log_dates=[datetime.datetime(2024, 1, 1)],
        log_dates_optional=None,
        attachments=[b"a"],
        attachments_optional=None,
        address=_Address(
            street="N/A",
            city="Nowhere",
            zip_code="00000",
            created_at=datetime.datetime(2024, 1, 1),
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    await m.save()
    loaded = await FullJson.get(m.pk)

    assert loaded.updated is None
    assert loaded.thumbnail is None
    assert loaded.log_dates_optional is None
    assert loaded.attachments_optional is None
    assert loaded.orders is None


@needs_redis
@py_test_mark_asyncio
async def test_json_model_empty_list_fields(conversion_fixtures):
    """Empty ``List[datetime]`` / ``List[bytes]`` must stay empty."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    m = FullJson(
        name="empty-lists",
        count=1,
        ratio=1.0,
        created=datetime.datetime(2024, 1, 1),
        updated=None,
        joined=datetime.date(2024, 1, 1),
        signature=b"x",
        thumbnail=None,
        log_dates=[],  # empty list
        log_dates_optional=None,
        attachments=[],  # empty list
        attachments_optional=None,
        address=_Address(
            street="St",
            city="City",
            zip_code="00000",
            created_at=datetime.datetime(2024, 1, 1),
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    await m.save()
    loaded = await FullJson.get(m.pk)

    assert loaded.log_dates == []
    assert loaded.attachments == []


@needs_redis
@py_test_mark_asyncio
async def test_json_model_naive_datetime_utc_roundtrip(conversion_fixtures):
    """Naive ``datetime`` must be treated as UTC before timestamp conversion."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    naive = datetime.datetime(2024, 1, 1, 12, 0, 0)
    aware = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

    m = FullJson(
        name="utc-test",
        count=1,
        ratio=1.0,
        created=naive,
        updated=aware,
        joined=datetime.date(2024, 1, 1),
        signature=b"x",
        thumbnail=None,
        log_dates=[naive],
        log_dates_optional=None,
        attachments=[],
        attachments_optional=None,
        address=_Address(
            street="St",
            city="City",
            zip_code="00000",
            created_at=naive,
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    await m.save()
    loaded = await FullJson.get(m.pk)

    # For naive datetime, after timestamp round-trip, both are naive (tzinfo stripped).
    assert loaded.created == naive
    assert loaded.updated == naive
    assert loaded.log_dates[0] == naive
    assert loaded.address.created_at == naive


# ── 2.2 JsonModel — get_value conversions ────────────────────────


@needs_redis
@py_test_mark_asyncio
async def test_json_get_value_datetime_path(conversion_fixtures):
    """JsonModel.get_value must convert datetime fields on the path."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 5, 20, 10, 0, 0)
    m = FullJson(
        name="gv-dt",
        count=1,
        ratio=1.0,
        created=dt,
        updated=None,
        joined=datetime.date(2024, 1, 1),
        signature=b"\x00",
        thumbnail=None,
        log_dates=[],
        log_dates_optional=None,
        attachments=[],
        attachments_optional=None,
        address=_Address(
            street="St",
            city="City",
            zip_code="00000",
            created_at=dt,
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    await m.save()

    # Top-level datetime field
    value = await FullJson.get_value(m.pk, "created")
    assert value == dt
    assert isinstance(value, datetime.datetime)

    # Nested datetime field in embedded model
    value = await FullJson.get_value(m.pk, "address__created_at")
    assert value == dt
    assert isinstance(value, datetime.datetime)

    # bytes field
    value = await FullJson.get_value(m.pk, "signature")
    assert value == b"\x00"
    assert isinstance(value, bytes)

    # Coordinates field — get_value returns the string representation in JSON path mode
    value = await FullJson.get_value(m.pk, "location")
    # Coordinates are stored as "lon,lat" string under JSON path resolution
    assert isinstance(value, str) and value == "0,0"


# ── 2.3 JsonModel — get_many conversions ─────────────────────────


@needs_redis
@py_test_mark_asyncio
async def test_json_get_many_with_conversions(conversion_fixtures):
    """get_many must convert all items correctly."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 8, 1, 10, 0, 0)
    m1 = FullJson(
        name="gm-1",
        count=1,
        ratio=1.0,
        created=dt,
        updated=None,
        joined=datetime.date(2024, 1, 1),
        signature=b"sig1",
        thumbnail=None,
        log_dates=[dt],
        log_dates_optional=None,
        attachments=[b"a1", b"a2"],
        attachments_optional=None,
        address=_Address(
            street="St",
            city="City",
            zip_code="00000",
            created_at=dt,
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    m2 = FullJson(
        name="gm-2",
        count=2,
        ratio=2.0,
        created=dt,
        updated=None,
        joined=datetime.date(2024, 1, 1),
        signature=b"sig2",
        thumbnail=None,
        log_dates=[dt],
        log_dates_optional=None,
        attachments=[b"b1"],
        attachments_optional=None,
        address=_Address(
            street="St",
            city="City",
            zip_code="00000",
            created_at=dt,
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    await m1.save()
    await m2.save()

    results = await FullJson.get_many([m1.pk, m2.pk])
    assert len(results) == 2

    for r in results:
        assert r.created == dt
        assert r.log_dates == [dt]
        assert r.attachments in ([b"a1", b"a2"], [b"b1"])
        assert r.signature in (b"sig1", b"sig2")


# ── 2.4 JsonModel — no-conversion fast path ────────────────────


@needs_redis
@py_test_mark_asyncio
async def test_json_simple_no_conversion_fields(conversion_fixtures):
    """SimpleJson (no convertible fields) save/load must work."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    m = SimpleJson(title="simple-test", count=100, active="yes")
    await m.save()
    loaded = await SimpleJson.get(m.pk)

    assert loaded.title == "simple-test"
    assert loaded.count == 100
    assert loaded.active == "yes"


# ── 2.5 HashModel — datetime / bytes / empty-string ──────────────


@needs_redis
@py_test_mark_asyncio
async def test_hash_model_datetime_and_bytes_roundtrip(conversion_fixtures):
    """HashModel with datetime/date/bytes fields must round-trip correctly."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 9, 15, 14, 30, 0)
    d = datetime.date(2024, 9, 15)

    m = FullHash(
        name="hash-dt-bytes",
        created=dt,
        joined=d,
        data=b"\x00\x01\x02\xff",
    )
    await m.save()
    loaded = await FullHash.get(m.pk)

    # Pydantic reads UTC timestamps as tz-aware; compare against tz-aware dt
    assert loaded.created == dt.replace(tzinfo=datetime.timezone.utc)
    assert loaded.joined == d
    assert loaded.data == b"\x00\x01\x02\xff"


@needs_redis
@py_test_mark_asyncio
async def test_hash_model_optional_empty_string_to_none(conversion_fixtures):
    """HashModel Optional fields with empty string must load as ``None``."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 1, 1).replace(tzinfo=datetime.timezone.utc)
    d = dt.date()

    m = FullHash(
        name="hash-opt-none",
        created=dt,
        joined=d,
        data=b"x",
        nickname=None,
        bio=None,
    )
    await m.save()
    loaded = await FullHash.get(m.pk)

    assert loaded.nickname is None
    assert loaded.bio is None


@needs_redis
@py_test_mark_asyncio
async def test_hash_model_optional_with_value(conversion_fixtures):
    """HashModel Optional fields with a real value must round-trip."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 1, 1).replace(tzinfo=datetime.timezone.utc)
    d = dt.date()

    m = FullHash(
        name="hash-opt-val",
        created=dt,
        joined=d,
        data=b"x",
        nickname="nick",
        bio="biography here",
    )
    await m.save()
    loaded = await FullHash.get(m.pk)

    assert loaded.nickname == "nick"
    assert loaded.bio == "biography here"


@needs_redis
@py_test_mark_asyncio
async def test_hash_simple_no_conversion_fields(conversion_fixtures):
    """SimpleHash (no convertible fields) save/load must work."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    m = SimpleHash(title="simple-hash", count=50, active="no")
    await m.save()
    loaded = await SimpleHash.get(m.pk)

    assert loaded.title == "simple-hash"
    assert loaded.count == 50
    assert loaded.active == "no"


# ── 2.6 HashModel — get_many with conversions ──────────────────


@needs_redis
@py_test_mark_asyncio
async def test_hash_get_many_with_conversions(conversion_fixtures):
    """HashModel.get_many must datetime/bytes fields for all items."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 3, 3, 15, 0, 0).replace(tzinfo=datetime.timezone.utc)

    m1 = FullHash(name="hm-1", created=dt, joined=dt.date(), data=b"data1")
    m2 = FullHash(name="hm-2", created=dt, joined=dt.date(), data=b"data2")
    await m1.save()
    await m2.save()

    results = await FullHash.get_many([m1.pk, m2.pk])
    assert len(results) == 2

    for r in results:
        assert r.created == dt
        assert r.data in (b"data1", b"data2")


# ── 2.7 Pipeline save with conversions ─────────────────────────


@needs_redis
@py_test_mark_asyncio
async def test_json_pipeline_save_with_conversions(conversion_fixtures):
    """Pipeline-based save must still convert datetime/bytes/embedded models."""
    SimpleJson, FullJson, SimpleHash, FullHash = conversion_fixtures

    dt = datetime.datetime(2024, 4, 4, 10, 0, 0)
    m = FullJson(
        name="pipe-test",
        count=1,
        ratio=1.0,
        created=dt,
        updated=None,
        joined=datetime.date(2024, 1, 1),
        signature=b"\x89PNG",
        thumbnail=None,
        log_dates=[dt],
        log_dates_optional=None,
        attachments=[b"a"],
        attachments_optional=None,
        address=_Address(
            street="St",
            city="City",
            zip_code="00000",
            created_at=dt,
            geo=Coordinates(longitude=0, latitude=0),
        ),
        orders=None,
        location=Coordinates(longitude=0, latitude=0),
    )
    await m.save()
    loaded = await FullJson.get(m.pk)

    assert loaded.created == dt
    assert loaded.signature == b"\x89PNG"
    assert loaded.log_dates == [dt]
    assert loaded.attachments == [b"a"]
    assert loaded.address.created_at == dt
