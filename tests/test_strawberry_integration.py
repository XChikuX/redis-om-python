# mypy: disable-error-code="type-var"

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
    from strawberry.experimental.pydantic import input as pyd_input
    from strawberry.experimental.pydantic import type as pyd_type

    HAS_STRAWBERRY = True
except ImportError:
    HAS_STRAWBERRY = False

from aredis_om import EmbeddedJsonModel, Field, JsonModel, Migrator

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


@pytest.mark.asyncio
async def test_strawberry_type_wraps_model():
    """Strawberry @type decorator can wrap a JsonModel without error."""
    assert hasattr(User, "__strawberry_definition__") or hasattr(
        User, "_type_definition"
    )


@pytest.mark.asyncio
async def test_strawberry_input_wraps_model():
    """Strawberry @input decorator can wrap a JsonModel without error."""
    assert hasattr(UserInput, "__strawberry_definition__") or hasattr(
        UserInput, "_type_definition"
    )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
    user2 = await _make_user(fname="Bob", email="bob@test.com", ethnicity="melanesian")
    await user2.save()

    results = await StrawberryUser.find(StrawberryUser.ethnicity == "polynesian").all()
    assert len(results) == 1
    assert results[0].fname == "Alice"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_strawberry_input_to_model_validate(key_prefix, redis):
    """Validate that a strawberry pydantic input can be converted to a redis-om model.

    This reproduces the scenario where a GraphQL resolver receives a
    strawberry Input type (backed by a redis-om model) and needs to
    convert it to the underlying model via model_validate.
    """
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    # Simulate what a GraphQL resolver receives: a strawberry Input instance.
    # strawberry-graphql's pydantic integration lets you use the input type
    # as a resolver argument, and the resolver receives the already-
    # constructed pydantic model instance.
    phone_input = PhoneInput(
        country_code="1",
        country="United States",
        number=5551234567,
        device_id="test-device-001",
        phone_type="ios",
    )
    user_input = UserInput(
        fname="Ada",
        email="ada@test.com",
        phone=phone_input,
        ethnicity="asian",
        interests=["python", "redis"],
        bio="GraphQL + Redis enthusiast",
    )

    # strawberry's pydantic input uses to_pydantic() to convert to the underlying
    # pydantic model; .dict() on that gives the exact data for model_validate.
    input_dict = user_input.to_pydantic().dict()

    # This should NOT raise a ValidationError about ExpressionProxy in pk.
    user = StrawberryUser.model_validate(input_dict)
    assert user.fname == "Ada"
    assert user.email == "ada@test.com"
    assert user.phone.number == 5551234567
    assert user.pk is not None
    assert isinstance(user.pk, str)


@pytest.mark.asyncio
async def test_strawberry_input_with_explicit_pk_to_model_validate(key_prefix, redis):
    """model_validate should accept an explicit pk even when the input comes from strawberry.

    This tests the path where a resolver passes an existing pk (e.g. for updates).
    """
    await Migrator().run()

    old_pks = [pk async for pk in await StrawberryUser.all_pks()]
    for pk in old_pks:
        await StrawberryUser.delete(pk)

    # Simulate a resolver that already has the pk (update scenario)
    phone_input = PhoneInput(
        country_code="1",
        number=5559999999,
        device_id="device-update",
    )
    # Even with pk=None from the input, model_validate must work
    user = StrawberryUser.model_validate({
        "fname": "Bob",
        "email": "bob@test.com",
        "phone": phone_input.to_pydantic().dict(),
        "ethnicity": "caucasian",
        "interests": ["go"],
        "bio": "update test",
        "pk": None,
    })
    assert user.pk is not None


@pytest.mark.asyncio
async def test_strawberry_input_pk_not_in_dict(key_prefix, redis):
    """Verify that pk is not present in the strawberry input dict.

    This confirms the input type does not carry the class-level ExpressionProxy.
    """
    phone_input = PhoneInput(
        country_code="1",
        number=5550000001,
        device_id="device-pk-test",
    )
    input_dict = phone_input.to_pydantic().dict()
    assert "pk" not in input_dict


@pytest.mark.asyncio
async def test_strawberry_input_with_expression_proxy_pk_stripped(key_prefix, redis):
    """If a strawberry input receives pk=User.pk (the ExpressionProxy), it must
    be gracefully stripped so that to_pydantic() succeeds.

    The __init__ accepts the ExpressionProxy (strawberry defers pydantic validation
    to to_pydantic()).  Our model_validator strips it before Pydantic validates,
    so to_pydantic() produces a valid model with an auto-generated pk.
    """
    inp = UserInput(
        pk=StrawberryUser.pk,
        fname="StrippedInput",
        email="stripped@test.com",
        phone=PhoneInput(country_code="1", number=5550000000, device_id="bad"),
        ethnicity="test",
        interests=["test"],
        bio="bad",
    )
    # to_pydantic() should succeed now — ExpressionProxy pk is stripped
    pydantic_model = inp.to_pydantic()
    assert pydantic_model.fname == "StrippedInput"
    assert pydantic_model.pk is not None
    assert isinstance(pydantic_model.pk, str)


@pytest.mark.asyncio
async def test_strawberry_input_with_expression_proxy_to_pydantic_succeeds(key_prefix, redis):
    """to_pydantic() with an ExpressionProxy pk should succeed and auto-generate pk.

    The model_validator on RedisModel strips the ExpressionProxy before
    Pydantic validates the fields, so the model gets a proper auto-generated pk.
    """
    inp = UserInput(
        pk=StrawberryUser.pk,
        fname="AutoPK",
        email="autopk@test.com",
        phone=PhoneInput(country_code="1", number=5550000000, device_id="auto"),
        ethnicity="test",
        interests=["test"],
        bio="auto pk test",
    )
    pydantic_model = inp.to_pydantic()
    assert pydantic_model.fname == "AutoPK"
    assert pydantic_model.pk is not None
    assert isinstance(pydantic_model.pk, str)
    # Round-trip through model_validate also works
    user = StrawberryUser.model_validate(pydantic_model.dict())
    assert user.pk is not None
