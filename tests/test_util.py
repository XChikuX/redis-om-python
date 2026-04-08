# type: ignore
"""Tests for aredis_om.util – numeric type helpers."""

import datetime
import decimal
from typing import List, Optional, Union

from aredis_om.util import ASYNC_MODE, has_numeric_inner_type, is_numeric_type

# ---------------------------------------------------------------------------
# is_numeric_type
# ---------------------------------------------------------------------------


class TestIsNumericType:
    def test_int_is_numeric(self):
        assert is_numeric_type(int) is True

    def test_float_is_numeric(self):
        assert is_numeric_type(float) is True

    def test_decimal_is_numeric(self):
        assert is_numeric_type(decimal.Decimal) is True

    def test_datetime_is_numeric(self):
        assert is_numeric_type(datetime.datetime) is True

    def test_date_is_numeric(self):
        assert is_numeric_type(datetime.date) is True

    def test_str_not_numeric(self):
        assert is_numeric_type(str) is False

    def test_bool_is_numeric(self):
        # bool is subclass of int so it should be numeric
        assert is_numeric_type(bool) is True

    def test_none_type_not_numeric(self):
        assert is_numeric_type(type(None)) is False

    def test_generic_type_returns_false(self):
        # e.g. List[int] is not a raw type, issubclass raises TypeError
        assert is_numeric_type(List[int]) is False


# ---------------------------------------------------------------------------
# has_numeric_inner_type
# ---------------------------------------------------------------------------


class TestHasNumericInnerType:
    def test_list_int(self):
        assert has_numeric_inner_type(List[int]) is True

    def test_list_float(self):
        assert has_numeric_inner_type(List[float]) is True

    def test_list_str(self):
        assert has_numeric_inner_type(List[str]) is False

    def test_optional_int(self):
        # Optional[int] = Union[int, None], first arg is int
        assert has_numeric_inner_type(Optional[int]) is True

    def test_optional_str(self):
        assert has_numeric_inner_type(Optional[str]) is False

    def test_plain_int(self):
        # No type args
        assert has_numeric_inner_type(int) is False

    def test_union_int_str(self):
        # Union[int, str] → first arg is int
        assert has_numeric_inner_type(Union[int, str]) is True

    def test_plain_str(self):
        assert has_numeric_inner_type(str) is False


# ---------------------------------------------------------------------------
# ASYNC_MODE
# ---------------------------------------------------------------------------


def test_async_mode_is_bool():
    assert isinstance(ASYNC_MODE, bool)
