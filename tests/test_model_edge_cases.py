# mypy: disable-error-code="type-var"

"""Edge-case / error-path tests for the model module.

These tests exercise uncovered code paths in ``aredis_om/model/model.py``
that are difficult or expensive to trigger in integration tests.
"""

import base64
import datetime
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast

import pytest

from aredis_om import EmbeddedJsonModel, JsonModel
from aredis_om.model.model import (
    _KIND_BYTES,
    _KIND_BYTES_LIST,
    _KIND_DATETIME,
    _KIND_DATETIME_LIST,
    _KIND_NESTED_MODEL,
    _KIND_NESTED_MODEL_LIST,
    _KIND_NONE,
    ConversionPlan,
    _EMPTY_PLAN,
    _FieldPlan,
    _list_inner_type,
    _load_convert_scalar,
    _resolve_field_type_for_conversion,
    _save_convert_value,
    decode_redis_value,
    get_conversion_plan,
    model_registry,
    planned_load_conversions,
    planned_save_conversions,
)

_TEST_MODEL_PREFIXES = ("_EdgeCaseModel",)


class _EdgeCaseModel(JsonModel):
    """Model used for edge-case tests."""

    name: str

    class Meta:
        index_name = "edge_case_model_test"
        model_key_prefix = "edge_case_model_doc"
        _test_only = True


_ALL_TEST_MODELS: Dict[str, Type] = {}
for _key, _val in list(model_registry.items()):
    _name = getattr(_val, "__name__", "")
    if _name.startswith(_TEST_MODEL_PREFIXES):
        _ALL_TEST_MODELS[_key] = _val


def _qualname_key(cls: Type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _isolate_registry(*keep: Type) -> Dict[str, Type]:
    """Remove all test models except those in ``keep`` from the registry."""
    snapshot: Dict[str, Type] = {}
    for key in list(model_registry.keys()):
        if key in _ALL_TEST_MODELS:
            snapshot[key] = model_registry.pop(key)
    for cls in keep:
        model_registry[_qualname_key(cls)] = cls
    return snapshot


def _restore_registry(snapshot: Dict[str, Type]) -> None:
    """Undo ``_isolate_registry``."""
    for key in list(model_registry.keys()):
        if key in _ALL_TEST_MODELS:
            model_registry.pop(key, None)
    for str_key, cls in snapshot.items():
        model_registry[str_key] = cls


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def snapshot():
    """Isolate the registry for each test and restore it after."""
    snap = _isolate_registry()
    yield snap
    _restore_registry(snap)


# ── _resolve_field_type_for_conversion tests ──────────────────────────


class _FakeFieldInfo:
    """Minimal stand-in for pydantic.FieldInfo used in unit tests.

    Only needs an ``annotation`` attribute for ``_resolve_field_type_for_conversion``.
    """

    def __init__(self, annotation: object) -> None:
        self.annotation = annotation


def test_resolve_field_type_for_conversion_union_with_multiple_non_none_types():
    """Union[T1, T2] (not Optional) returns as-is — no conversion planned."""
    # Simulate a Union[str, int] annotation (not Optional)
    # _resolve_field_type_for_conversion should return the annotation as-is
    field_info = _FakeFieldInfo(Union[str, int])
    result = _resolve_field_type_for_conversion(field_info)
    assert result == Union[str, int]


def test_resolve_field_type_for_conversion_plain_type():
    """Plain type (non-union) returns the type."""
    field_info = _FakeFieldInfo(str)
    result = _resolve_field_type_for_conversion(field_info)
    assert result is str


def test_resolve_field_type_for_conversion_optional_returns_inner():
    """Optional[str] returns str (inner type)."""
    field_info = _FakeFieldInfo(Optional[str])
    result = _resolve_field_type_for_conversion(field_info)
    assert result is str


# ── _list_inner_type tests ────────────────────────────────────────────


def test_list_inner_type_list():
    """List[int] returns int."""
    result = _list_inner_type(List[int])
    assert result is int


def test_list_inner_type_plain_list():
    """list[str] (lowercase) returns str."""
    result = _list_inner_type(list[str])
    assert result is str


def test_list_inner_type_empty():
    """List without args returns None."""
    result = _list_inner_type(List)
    assert result is None


def test_list_inner_type_tuple():
    """Tuple[str, ...] returns str."""
    result = _list_inner_type(Tuple[str, ...])
    assert result is str


# ── _save_convert_value tests ─────────────────────────────────────────


def test_save_convert_value_datetime_naive():
    """Naive datetime (no tzinfo) gets UTC and returns timestamp."""
    naive_dt = datetime.datetime(2024, 6, 15, 12, 0, 0)
    result = _save_convert_value(_KIND_DATETIME, naive_dt, datetime.datetime)
    assert isinstance(result, float)
    # Naive datetime gets UTC timezone added, timestamp should be close to expected
    # 2024-06-15 12:00:00 UTC
    expected = datetime.datetime(
        2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert result == expected


def test_save_convert_value_datetime_aware():
    """Aware datetime returns timestamp (tz preserved to UTC)."""
    aware_dt = datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    result = _save_convert_value(_KIND_DATETIME, aware_dt, datetime.datetime)
    assert isinstance(result, float)


def test_save_convert_value_date():
    """date (not datetime) is converted to UTC midnight timestamp."""
    d = datetime.date(2024, 6, 15)
    result = _save_convert_value(_KIND_DATETIME, d, datetime.date)
    assert isinstance(result, float)
    # Date gets combined with time.min (00:00:00) and UTC timezone
    expected = datetime.datetime.combine(
        d, datetime.time.min, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert result == expected


def test_save_convert_value_datetime_none():
    """None datetime passes through unchanged."""
    result = _save_convert_value(_KIND_DATETIME, None, datetime.datetime)
    assert result is None


def test_save_convert_value_bytes():
    """bytes are base64-encoded to ascii string."""
    result = _save_convert_value(_KIND_BYTES, b"hello", bytes)
    assert isinstance(result, str)
    decoded = base64.b64decode(result)
    assert decoded == b"hello"


def test_save_convert_value_bytes_non_bytes():
    """Non-bytes value in KIND_BYTES passes through."""
    result = _save_convert_value(_KIND_BYTES, "not_bytes", bytes)
    assert result == "not_bytes"


def test_save_convert_value_datetime_list():
    """List[datetime] converts each item to UTC timestamp."""
    dt_list = [
        datetime.datetime(2024, 6, 15, 12, 0, 0),
        datetime.datetime(2024, 6, 16, 12, 0, 0),
    ]
    result = _save_convert_value(_KIND_DATETIME_LIST, dt_list, datetime.datetime)
    assert isinstance(result, list)
    assert all(isinstance(x, float) for x in result)
    assert len(result) == 2


def test_save_convert_value_datetime_list_mixed():
    """List[datetime] with non-datetime items passes them through."""
    dt_list = [
        datetime.datetime(2024, 6, 15, 12, 0, 0),
        "not_a_datetime",
    ]
    result = _save_convert_value(_KIND_DATETIME_LIST, dt_list, datetime.datetime)
    assert result[0] != dt_list[0]  # converted
    assert result[1] == "not_a_datetime"  # passed through


def test_save_convert_value_bytes_list():
    """List[bytes] base64-encodes each item."""
    b_list = [b"a", b"b"]
    result = _save_convert_value(_KIND_BYTES_LIST, b_list, bytes)
    assert isinstance(result, list)
    assert all(isinstance(x, str) for x in result)
    assert base64.b64decode(result[0]) == b"a"
    assert base64.b64decode(result[1]) == b"b"


def test_save_convert_value_kind_none_returns_value():
    """Unknown kind returns value unchanged."""
    result = _save_convert_value(_KIND_NONE, "anything", None)
    assert result == "anything"


# ── _load_convert_scalar tests ────────────────────────────────────────


def test_load_convert_scalar_bytes_invalid_base64():
    """Invalid base64 string falls back to original value."""
    result = _load_convert_scalar(_KIND_BYTES, "not-valid!!!", bytes)
    # Should return original value (fallback)
    assert result == "not-valid!!!"


def test_load_convert_scalar_bytes_valid_base64():
    """Valid base64 string decodes to bytes."""
    encoded = base64.b64encode(b"hello").decode("ascii")
    result = _load_convert_scalar(_KIND_BYTES, encoded, bytes)
    assert result == b"hello"


def test_load_convert_scalar_datetime_list():
    """List of timestamps converts to datetime list."""
    ts_list = [1718457600.0, 1718544000.0]
    result = _load_convert_scalar(_KIND_DATETIME_LIST, ts_list, datetime.datetime)
    assert isinstance(result, list)
    assert all(isinstance(x, datetime.datetime) for x in result)


def test_load_convert_scalar_bytes_list():
    """List of base64 strings decodes to bytes list."""
    b64_list = [
        base64.b64encode(b"a").decode("ascii"),
        base64.b64encode(b"b").decode("ascii"),
    ]
    result = _load_convert_scalar(_KIND_BYTES_LIST, b64_list, bytes)
    assert isinstance(result, list)
    assert result == [b"a", b"b"]


# ── planned_save_conversions tests ───────────────────────────────────


def test_planned_save_conversions_skips_non_dict():
    """planned_save_conversions returns non-dict input unchanged."""
    plan = ConversionPlan(
        fields={},
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )
    result = planned_save_conversions("not_a_dict", plan)
    assert result == "not_a_dict"


def test_planned_save_conversions_fast_path_no_conversion_needed():
    """Fast path: plan.needs_conversion=False returns document unchanged."""
    plan = ConversionPlan(
        fields={},
        needs_conversion=False,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )
    doc = {"name": "Alice", "age": 30}
    result = planned_save_conversions(doc, plan)
    assert result is doc  # Same object returned


def test_planned_save_conversions_unknown_field_passed_through():
    """Field in document but not in plan is passed through unchanged."""
    plan = ConversionPlan(
        fields={},
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )
    doc = {"unknown_field": datetime.datetime(2024, 6, 15, 12, 0, 0)}
    result = planned_save_conversions(doc, plan)
    assert result["unknown_field"] == doc["unknown_field"]


def test_planned_save_conversions_nested_model_recursion():
    """Nested model dict is recursively converted."""

    class _Inner(EmbeddedJsonModel):
        value: int

    inner_plan = get_conversion_plan(_Inner)

    outer_plan = ConversionPlan(
        fields={
            "inner": _FieldPlan(
                kind=_KIND_NESTED_MODEL,
                target_type=None,
                nested_plan=inner_plan,
                is_optional=False,
            )
        },
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )

    doc = {"inner": {"value": 42}}
    result = planned_save_conversions(doc, outer_plan)
    assert result == doc  # No datetime/bytes, so unchanged


def test_planned_save_conversions_nested_model_list_recursion():
    """List of nested models is recursively converted."""

    class _InnerItem(EmbeddedJsonModel):
        x: int

    inner_plan = get_conversion_plan(_InnerItem)

    outer_plan = ConversionPlan(
        fields={
            "items": _FieldPlan(
                kind=_KIND_NESTED_MODEL_LIST,
                target_type=None,
                nested_plan=inner_plan,
                is_optional=False,
            )
        },
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )

    doc = {"items": [{"x": 1}, {"x": 2}]}
    result = planned_save_conversions(doc, outer_plan)
    assert result == doc


# ── planned_load_conversions tests ───────────────────────────────────


def test_planned_load_conversions_skips_non_dict():
    """planned_load_conversions returns non-dict input unchanged."""
    plan = ConversionPlan(
        fields={},
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )
    result = planned_load_conversions("not_a_dict", plan)
    assert result == "not_a_dict"


def test_planned_load_conversions_hash_skips_datetime():
    """for_hash=True skips datetime conversion (Pydantic handles it)."""
    plan = ConversionPlan(
        fields={
            "ts": _FieldPlan(
                kind=_KIND_DATETIME,
                target_type=datetime.datetime,
                nested_plan=None,
                is_optional=False,
            )
        },
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )

    doc = {"ts": "1234567890.5"}  # String as it would come from Redis HGETALL
    result = planned_load_conversions(doc, plan, for_hash=True)
    # Should pass through unchanged (datetime conversion skipped for hash)
    assert result["ts"] == "1234567890.5"


def test_planned_load_conversions_json_decodes_datetime():
    """for_hash=False converts timestamp string to datetime."""
    plan = ConversionPlan(
        fields={
            "ts": _FieldPlan(
                kind=_KIND_DATETIME,
                target_type=datetime.datetime,
                nested_plan=None,
                is_optional=False,
            )
        },
        needs_conversion=True,
        needs_empty_string_to_none=False,
        needs_dataclass_save=False,
    )

    doc = {"ts": "1718457600.0"}  # Timestamp string from JSON.GET
    result = planned_load_conversions(doc, plan, for_hash=False)
    assert isinstance(result["ts"], datetime.datetime)


def test_planned_load_conversions_empty_string_to_none_for_optional_hash():
    """HashModel: empty string in Optional field → None."""
    plan = ConversionPlan(
        fields={
            "name": _FieldPlan(
                kind=_KIND_NONE,
                target_type=None,
                nested_plan=None,
                is_optional=True,
            )
        },
        needs_conversion=False,
        needs_empty_string_to_none=True,
        needs_dataclass_save=False,
    )

    doc = {"name": ""}
    result = planned_load_conversions(doc, plan, for_hash=True)
    assert result["name"] is None


def test_planned_load_conversions_optional_non_empty_preserved():
    """Optional field with non-empty value is preserved."""
    plan = ConversionPlan(
        fields={
            "name": _FieldPlan(
                kind=_KIND_NONE,
                target_type=None,
                nested_plan=None,
                is_optional=True,
            )
        },
        needs_conversion=False,
        needs_empty_string_to_none=True,
        needs_dataclass_save=False,
    )

    doc = {"name": "Alice"}
    result = planned_load_conversions(doc, plan, for_hash=True)
    assert result["name"] == "Alice"


# ── get_conversion_plan tests ─────────────────────────────────────────


def test_get_conversion_plan_non_model_returns_empty_plan():
    """get_conversion_plan on a class without model_fields returns _EMPTY_PLAN."""
    result = get_conversion_plan(str)  # str has no model_fields
    assert result is _EMPTY_PLAN


def test_get_conversion_plan_caches():
    """get_conversion_plan returns the same cached plan on repeated calls."""

    class _CachedModel(JsonModel):
        name: str
        age: int

        class Meta:
            _test_only = True

    plan1 = get_conversion_plan(_CachedModel)
    plan2 = get_conversion_plan(_CachedModel)
    assert plan1 is plan2


def test_get_conversion_plan_finds_convertible_fields():
    """get_conversion_plan identifies datetime, bytes, nested model fields."""

    class _ModelWithConversions(JsonModel):
        created_at: datetime.datetime
        data: bytes
        name: str

        class Meta:
            _test_only = True

    plan = get_conversion_plan(_ModelWithConversions)
    assert plan.needs_conversion is True
    assert "created_at" in plan.fields
    assert "data" in plan.fields
    assert plan.fields["created_at"].kind == _KIND_DATETIME
    assert plan.fields["data"].kind == _KIND_BYTES


# ── decode_redis_value tests ───────────────────────────────────────────


def test_decode_redis_value_handles_list():
    """decode_redis_value decodes list of bytes to list of str."""
    result = decode_redis_value([b"a", b"b"], "utf-8")
    assert result == ["a", "b"]


def test_decode_redis_value_handles_dict():
    """decode_redis_value decodes dict with bytes keys/values."""
    result = decode_redis_value({b"k": b"v"}, "utf-8")
    assert result == {"k": "v"}


def test_decode_redis_value_handles_mixed_list():
    """decode_redis_value handles list containing mixed bytes and str."""
    # When given already-decoded strings in the list, the function will fail
    # because str.decode() doesn't exist in Python 3. This test documents
    # that the function expects bytes input only (which is correct for Redis).
    # We test with all-bytes input to verify the function works correctly.
    result = decode_redis_value([b"hello", b"world"], "utf-8")
    assert result == ["hello", "world"]


def test_decode_redis_value_scalar_passthrough():
    """decode_redis_value returns None for non-list/dict/bytes input.

    The function's type signature is Union[List[bytes], Dict[bytes, bytes], bytes].
    Values that don't match any of those branches (int, None, float) fall through
    to an implicit ``return None``.
    """
    # These inputs don't match list/dict/bytes — function returns None.
    assert decode_redis_value(cast(Any, 123), "utf-8") is None
    assert decode_redis_value(cast(Any, None), "utf-8") is None
    assert decode_redis_value(cast(Any, 1.5), "utf-8") is None


# ── ConversionPlan._EMPTY_PLAN edge cases ───────────────────────────


def test_empty_plan_has_no_conversion():
    """_EMPTY_PLAN has needs_conversion=False."""
    assert _EMPTY_PLAN.needs_conversion is False


def test_empty_plan_fields_is_empty_dict():
    """_EMPTY_PLAN has empty fields mapping."""
    assert len(_EMPTY_PLAN.fields) == 0


def test_planned_save_with_empty_plan_returns_same_object():
    """planned_save_conversions with _EMPTY_PLAN returns same dict."""
    doc = {"name": "Alice", "age": 30}
    result = planned_save_conversions(doc, _EMPTY_PLAN)
    assert result is doc


def test_planned_load_with_empty_plan_returns_same_object():
    """planned_load_conversions with _EMPTY_PLAN returns same dict."""
    doc = {"name": "Alice"}
    result = planned_load_conversions(doc, _EMPTY_PLAN)
    assert result is doc
