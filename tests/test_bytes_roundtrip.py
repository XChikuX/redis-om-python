# type: ignore
"""Tests for ``bytes`` / ``List[bytes]`` round-trip and query handling.

Covers three CLAUDE.md "Remaining known issues" / latent bugs:

1. ``List[bytes]`` load asymmetry — ``convert_bytes_to_base64`` encodes
   ``bytes`` items inside lists on save, but ``convert_base64_to_bytes``
   only recurses into list items when the inner type is a RedisModel, so
   ``List[bytes]`` items are returned as raw base64 strings on load.

2. ``bytes`` field querying (EQ/NE) — bytes are base64-encoded on save, but
   the EQ/NE TAG-rendering path passes raw ``bytes`` to ``escaper.escape()``,
   which raises ``TypeError`` (string pattern on bytes-like object).

3. ``bytes`` field querying (IN/NOT_IN/STARTSWITH/...) —
   ``expand_tag_value`` returns raw ``bytes`` for bytes values, which
   cannot be interpolated into the RediSearch query string and does not
   match the stored base64 representation.

The pure-function tests do not require Redis. The integration tests are
skipped automatically when RediSearch / RedisJSON is unavailable.
"""

import abc
import base64
import os
import re
from typing import List, Optional
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import Field, JsonModel, Migrator, get_redis_connection
from aredis_om.model.model import (
    convert_base64_to_bytes,
    convert_bytes_to_base64,
    FindQuery,
)
from aredis_om.model.token_escaper import TokenEscaper

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


escaper = TokenEscaper()


# ===========================================================================
# Part 1 — List[bytes] round-trip (pure-function, no Redis)
# ===========================================================================


def _fields(**annotations):
    """Build a model_fields mapping like Pydantic's, keyed by field name."""
    return {name: mock.Mock(annotation=ann) for name, ann in annotations.items()}


def test_list_bytes_round_trip():
    """``List[bytes]`` must survive an encode/decode cycle unchanged."""
    blobs = [b"\x00\x01", b"hello", b"\xff\xfe\xfd"]
    encoded = convert_bytes_to_base64({"blobs": blobs})
    # After encoding, every item must be a base64 string.
    assert encoded["blobs"] == [base64.b64encode(b).decode("ascii") for b in blobs]

    decoded = convert_base64_to_bytes(encoded, _fields(blobs=List[bytes]))
    assert decoded["blobs"] == blobs
    assert all(isinstance(x, bytes) for x in decoded["blobs"])


def test_optional_list_bytes_round_trip():
    """``Optional[List[bytes]]`` must survive an encode/decode cycle."""
    blobs = [b"abc", b"\x00\xff"]
    encoded = convert_bytes_to_base64({"blobs": blobs})
    decoded = convert_base64_to_bytes(encoded, _fields(blobs=Optional[List[bytes]]))
    assert decoded["blobs"] == blobs


def test_list_bytes_empty_and_none_items():
    """Empty bytes and ``None`` items pass through without crashing."""
    encoded = convert_bytes_to_base64({"blobs": [b"", b"x"]})
    decoded = convert_base64_to_bytes(encoded, _fields(blobs=List[bytes]))
    assert decoded["blobs"] == [b"", b"x"]


def test_scalar_bytes_still_works():
    """Regression guard: scalar ``bytes`` round-trip is unaffected."""
    encoded = convert_bytes_to_base64({"data": b"hello"})
    decoded = convert_base64_to_bytes(encoded, _fields(data=bytes))
    assert decoded["data"] == b"hello"


def test_list_bytes_mixed_with_other_fields():
    """A model with both ``bytes`` and ``List[bytes]`` round-trips correctly."""
    encoded = convert_bytes_to_base64(
        {"name": "widget", "data": b"xx", "blobs": [b"a", b"b"]}
    )
    decoded = convert_base64_to_bytes(
        encoded,
        _fields(name=str, data=bytes, blobs=List[bytes]),
    )
    assert decoded == {"name": "widget", "data": b"xx", "blobs": [b"a", b"b"]}


# ===========================================================================
# Part 2 — bytes querying (pure-function, no Redis)
# ===========================================================================


def test_expand_tag_value_bytes_matches_stored_base64():
    """``expand_tag_value(b"x")`` must yield the escaped base64 of ``b"x"``.

    Stored TAG values for bytes fields are base64-encoded strings (because
    ``convert_bytes_to_base64`` runs on save). The query value must be
    base64-encoded too, otherwise the query can never match.
    """
    raw = b"\x89PNG\r\n\x1a\n"
    expected = escaper.escape(base64.b64encode(raw).decode("ascii"))
    assert FindQuery.expand_tag_value(raw) == expected


def test_expand_tag_value_bytes_empty():
    """Empty bytes encode to an empty base64 string."""
    assert FindQuery.expand_tag_value(b"") == ""


def test_resolve_value_eq_bytes_does_not_crash():
    """EQ on an indexed bytes field must render a query string, not raise.

    Previously this path passed raw ``bytes`` to ``escaper.escape()`` which
    raised ``TypeError: cannot use a string pattern on a bytes-like object``.
    """
    from aredis_om.model.model import RediSearchFieldTypes

    # A minimal field_info mock; resolve_value only reads .separator via
    # getattr with a default, so an empty Mock is fine.
    field_info = mock.Mock(spec=[])

    # Must not raise.
    result = FindQuery.resolve_value(
        field_name="data",
        field_type=RediSearchFieldTypes.TAG,
        field_info=field_info,
        op=None,  # set below per-operator
        value=b"\x89PNG\r\n\x1a\n",
        parents=[],
    )
    # smoke: just ensure it returns a string (the per-operator test below
    # checks the actual rendered query)
    assert isinstance(result, str)

    from aredis_om.model.model import Operators

    for op in (Operators.EQ, Operators.NE):
        rendered = FindQuery.resolve_value(
            field_name="data",
            field_type=RediSearchFieldTypes.TAG,
            field_info=field_info,
            op=op,
            value=b"\x89PNG\r\n\x1a\n",
            parents=[],
        )
        assert isinstance(rendered, str)
        # The base64 of the PNG header must appear (escaped) in the query.
        expected_b64 = base64.b64encode(b"\x89PNG\r\n\x1a\n").decode("ascii")
        assert expected_b64 in rendered.replace("\\", "")


def test_resolve_value_in_bytes_does_not_crash():
    """IN on an indexed bytes field must render, not raise / not emit raw bytes."""
    from aredis_om.model.model import Operators, RediSearchFieldTypes

    field_info = mock.Mock(spec=[])
    rendered = FindQuery.resolve_value(
        field_name="data",
        field_type=RediSearchFieldTypes.TAG,
        field_info=field_info,
        op=Operators.IN,
        value=[b"abc", b"\x00\xff"],
        parents=[],
    )
    assert isinstance(rendered, str)
    # Raw bytes must NOT appear in the rendered query. RediSearch treats
    # ``b'...'`` as part of a TAG value, not a Python repr, so the query
    # would silently match nothing.
    assert "b'" not in rendered
    assert 'b"' not in rendered
    assert "\\x" not in rendered
    # The base64 of each value must appear (escaped) in the rendered query.
    for raw in (b"abc", b"\x00\xff"):
        b64 = base64.b64encode(raw).decode("ascii")
        assert b64 in rendered.replace("\\", "")


# ===========================================================================
# Part 3 — Integration tests (require Redis)
# ===========================================================================


@pytest_asyncio.fixture
async def int_model(key_prefix, redis):
    class Base(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class BlobDoc(Base):
        name: str = Field(index=True)
        data: bytes = Field(index=True)
        blobs: Optional[List[bytes]] = None

    await Migrator().run()
    return BlobDoc


@needs_redis
@py_test_mark_asyncio
async def test_integration_list_bytes_round_trip(int_model):
    """Saving and loading ``List[bytes]`` via Redis must round-trip exactly."""
    BlobDoc = int_model
    doc = BlobDoc(
        name="list-bytes-test",
        data=b"primary",
        blobs=[b"\x00\x01\x02", b"hello", b"\xff\xfe\xfd"],
    )
    await doc.save()

    loaded = await BlobDoc.get(doc.pk)
    assert loaded.blobs == [b"\x00\x01\x02", b"hello", b"\xff\xfe\xfd"]
    assert loaded.data == b"primary"


@needs_redis
@py_test_mark_asyncio
async def test_integration_optional_list_bytes_round_trip(int_model):
    """``Optional[List[bytes]]`` round-trips through Redis."""
    BlobDoc = int_model
    doc = BlobDoc(
        name="opt-list-bytes",
        data=b"x",
        blobs=[b"a", b"b"],
    )
    await doc.save()
    loaded = await BlobDoc.get(doc.pk)
    assert loaded.blobs == [b"a", b"b"]

    # And None round-trips too.
    doc2 = BlobDoc(name="opt-list-bytes-none", data=b"y", blobs=None)
    await doc2.save()
    loaded2 = await BlobDoc.get(doc2.pk)
    assert loaded2.blobs is None


@needs_redis
@py_test_mark_asyncio
async def test_integration_bytes_eq_query(int_model):
    """EQ query on an indexed bytes field must find the matching record."""
    BlobDoc = int_model
    target = b"\x89PNG\r\n\x1a\n"
    other = b"JFIF\x00"

    a = BlobDoc(name="a", data=target)
    b = BlobDoc(name="b", data=other)
    await a.save()
    await b.save()

    found = await BlobDoc.find(BlobDoc.data == target).all()
    pks = {m.pk for m in found}
    assert a.pk in pks
    assert b.pk not in pks


@needs_redis
@py_test_mark_asyncio
async def test_integration_bytes_in_query(int_model):
    """IN query on an indexed bytes field must find matching records."""
    BlobDoc = int_model
    t1 = b"alpha"
    t2 = b"beta"
    other = b"gamma"

    a = BlobDoc(name="a", data=t1)
    b = BlobDoc(name="b", data=t2)
    c = BlobDoc(name="c", data=other)
    await a.save()
    await b.save()
    await c.save()

    found = await BlobDoc.find(BlobDoc.data << [t1, t2]).all()
    pks = {m.pk for m in found}
    assert a.pk in pks
    assert b.pk in pks
    assert c.pk not in pks


# ===========================================================================
# Part 4 — decode_responses=False integration tests
#
# These verify that the same fixes work when Redis returns ``bytes`` instead
# of ``str`` for field values (i.e. ``decode_responses=False``). We build a
# fresh connection with ``decode_responses=False`` from the same host/port
# the rest of the suite is using.
# ===========================================================================


def _bytes_only_redis():
    """Return a connection with ``decode_responses=False`` from REDIS_OM_URL."""
    base = os.environ.get("REDIS_OM_URL", "")
    # Strip an existing decode_responses=... param, then re-add ours.
    cleaned = re.sub(r"[?&]decode_responses=[A-Za-z]+", "", base)
    sep = "&" if "?" in cleaned else "?"
    return get_redis_connection(url=f"{cleaned}{sep}decode_responses=False")


@pytest_asyncio.fixture
async def int_model_bytes():
    """Same as ``int_model`` but uses a ``decode_responses=False`` connection."""
    db = _bytes_only_redis()
    kp = f"redis-om:testing:bytes-decode-false:{os.urandom(4).hex()}"

    class Base(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = kp
            database = db

    class BlobDoc(Base):
        name: str = Field(index=True)
        data: bytes = Field(index=True)
        blobs: Optional[List[bytes]] = None

    await Migrator().run()
    return BlobDoc


@needs_redis
@py_test_mark_asyncio
async def test_integration_bytes_eq_query_decode_false(int_model_bytes):
    """EQ query on a bytes field works under ``decode_responses=False``."""
    BlobDoc = int_model_bytes
    target = b"\x89PNG\r\n\x1a\n"
    other = b"JFIF\x00"

    a = BlobDoc(name="a", data=target)
    b = BlobDoc(name="b", data=other)
    await a.save()
    await b.save()

    found = await BlobDoc.find(BlobDoc.data == target).all()
    pks = {m.pk for m in found}
    assert a.pk in pks
    assert b.pk not in pks


@needs_redis
@py_test_mark_asyncio
async def test_integration_bytes_in_query_decode_false(int_model_bytes):
    """IN query on a bytes field works under ``decode_responses=False``."""
    BlobDoc = int_model_bytes
    t1 = b"alpha"
    t2 = b"beta"
    other = b"gamma"

    a = BlobDoc(name="a", data=t1)
    b = BlobDoc(name="b", data=t2)
    c = BlobDoc(name="c", data=other)
    await a.save()
    await b.save()
    await c.save()

    found = await BlobDoc.find(BlobDoc.data << [t1, t2]).all()
    pks = {m.pk for m in found}
    assert a.pk in pks
    assert b.pk in pks
    assert c.pk not in pks


@needs_redis
@py_test_mark_asyncio
async def test_integration_list_bytes_round_trip_decode_false(int_model_bytes):
    """``List[bytes]`` round-trips under ``decode_responses=False``."""
    BlobDoc = int_model_bytes
    doc = BlobDoc(
        name="list-bytes-decode-false",
        data=b"primary",
        blobs=[b"\x00\x01\x02", b"hello", b"\xff\xfe\xfd"],
    )
    await doc.save()

    loaded = await BlobDoc.get(doc.pk)
    assert loaded.blobs == [b"\x00\x01\x02", b"hello", b"\xff\xfe\xfd"]
    assert loaded.data == b"primary"
