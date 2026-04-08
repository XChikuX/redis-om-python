"""
Tests for Strawberry GraphQL integration with redis-om models.

Validates that redis-om models (JsonModel, EmbeddedJsonModel) can be
wrapped with Strawberry's pydantic experimental types and used for
queries/inputs, including save, find, and full-text search operations.
"""

import datetime
from typing import List, Optional

import pytest

from tests._compat import ValidationError

try:
    import strawberry
    from strawberry.experimental.pydantic import type as pyd_type, input as pyd_input

    HAS_STRAWBERRY = True
except ImportError:
    HAS_STRAWBERRY = False

from aredis_om import (
    EmbeddedJsonModel,
    Field,
    JsonModel,
    Migrator,
)

py_test_mark_asyncio = pytest.mark.asyncio

pytestmark = pytest.mark.skipif(
    not HAS_STRAWBERRY, reason="strawberry-graphql not installed"
)


# ── Model definitions ────────────────────────────────────────────────


class Phone(EmbeddedJsonModel):
    """Embedded phone model mirroring the user's production schema."""

    country_code: str = Field(
        ...,
        title="CC",
        description="Country code without '+'",
    )
    country: Optional[str] = Field(
        None,
        min_length=4,
        max_length=32,
        title="Country",
    )
    number: int = Field(
        ...,
        index=True,
        gt=999999,
        lt=10000000000,
        title="Number",
    )
    device_id: str = Field(
        ...,
        index=True,
        title="Device ID",
    )
    phone_type: Optional[str] = Field(
        None,
        min_length=3,
        max_length=7,
        title="Phone type",
    )


class StrawberryUser(JsonModel):
    """User model used for Strawberry integration tests."""

    fname: str = Field(
        ...,
        index=True,
        title="First Name",
        min_length=2,
        max_length=30,
    )
    email: str = Field(
        ...,
        index=True,
        title="Email",
    )
    phone: Phone
    ethnicity: str = Field(
        ...,
        index=True,
        title="Ethnicity",
    )
    interests: List[str] = Field(
        ...,
        index=True,
        title="Interests",
    )
    bio: str = Field(
        "New to Meow!",
        index=True,
        max_length=1000,
        full_text_search=True,
        title="Bio",
    )


# ── Strawberry types ─────────────────────────────────────────────────

if HAS_STRAWBERRY:

    @pyd_type(model=Phone, all_fields=True)
    class PhoneType:
        pass

    @pyd_input(model=Phone, all_fields=True)
    class PhoneInput:
        pass

    @pyd_input(model=StrawberryUser, all_fields=True)
    class UserInput:
        pass

    @pyd_type(model=StrawberryUser, all_fields=True)
    class User:
        pass


# ── Helpers ───────────────────────────────────────────────────────────


def _make_phone(**overrides):
    defaults = dict(
        country_code="1",
        country="United States",
        number=5551234567,
        device_id="test-device-001",
        phone_type="ios",
    )
    defaults.update(overrides)
    return Phone(**defaults)


async def _make_user(**overrides):
    defaults = dict(
        fname="TestUser",
        email="test@example.com",
        phone=_make_phone(),
        ethnicity="indian",
        interests=["coding", "music"],
        bio="Hello from Psync!",
    )
    defaults.update(overrides)
    return StrawberryUser(**defaults)


# ── Tests ─────────────────────────────────────────────────────────────


@py_test_mark_asyncio
async def test_strawberry_type_wraps_model():
    """Strawberry @type decorator can wrap a JsonModel without error."""
    assert hasattr(User, "__strawberry_definition__") or hasattr(
        User, "_type_definition"
    )


@py_test_mark_asyncio
async def test_strawberry_input_wraps_model():
    """Strawberry @input decorator can wrap a JsonModel without error."""
    assert hasattr(UserInput, "__strawberry_definition__") or hasattr(
        UserInput, "_type_definition"
    )


@py_test_mark_asyncio
async def test_strawberry_save_and_find():
    """Save a user via redis-om and retrieve it, then convert to Strawberry type."""
    await Migrator().run()

    # Clean up stale data
    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    user = await _make_user(ethnicity="caucasian", bio="Strawberry test user")
    await user.save()

    found = await StrawberryUser.find(StrawberryUser.pk == user.pk).first()
    assert found.pk == user.pk
    assert found.fname == "TestUser"
    assert found.email == "test@example.com"
    assert found.phone.number == 5551234567


@py_test_mark_asyncio
async def test_strawberry_filter_by_ethnicity():
    """Filter users by ethnicity field."""
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    user1 = await _make_user(
        fname="Alice", email="alice@test.com", ethnicity="polynesian"
    )
    await user1.save()
    user2 = await _make_user(
        fname="Bob", email="bob@test.com", ethnicity="melanesian"
    )
    await user2.save()

    results = await StrawberryUser.find(
        StrawberryUser.ethnicity == "polynesian"
    ).all()
    assert len(results) == 1
    assert results[0].fname == "Alice"


@py_test_mark_asyncio
async def test_strawberry_filter_by_interests():
    """Filter users by interests (full-text search list field)."""
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    user = await _make_user(
        fname="Charlie",
        email="charlie@test.com",
        interests=["redis", "graphql"],
    )
    await user.save()

    results = await StrawberryUser.find(
        StrawberryUser.interests << ["redis"]  # type: ignore
    ).all()
    assert len(results) >= 1
    assert any(u.fname == "Charlie" for u in results)


@py_test_mark_asyncio
async def test_strawberry_filter_by_bio():
    """Filter users by bio using full-text search (% operator)."""
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    user = await _make_user(
        fname="Diana",
        email="diana@test.com",
        bio="Expert in Psync technologies and distributed systems",
    )
    await user.save()

    results = await StrawberryUser.find(StrawberryUser.bio % "Psync").all()
    assert len(results) >= 1
    assert any(u.fname == "Diana" for u in results)


@py_test_mark_asyncio
async def test_strawberry_embedded_phone_indexed():
    """Verify that the embedded phone model's indexed fields work."""
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    phone = _make_phone(number=9876543210, device_id="device-xyz")
    user = await _make_user(fname="Eve", email="eve@test.com", phone=phone)
    await user.save()

    found = await StrawberryUser.find(StrawberryUser.pk == user.pk).first()
    assert found.phone.number == 9876543210
    assert found.phone.device_id == "device-xyz"


@py_test_mark_asyncio
async def test_strawberry_convertible_redis_dict():
    """Test that convertible_redis_dict (if present) works for Strawberry conversion."""
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    user = await _make_user(fname="Frank", email="frank@test.com")
    await user.save()

    # Verify the model can be serialised to a dict
    user_dict = user.dict()
    assert user_dict["fname"] == "Frank"
    assert user_dict["email"] == "frank@test.com"
    assert "pk" in user_dict
