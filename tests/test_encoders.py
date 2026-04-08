# type: ignore
"""Tests for aredis_om.model.encoders – jsonable_encoder utility."""

import dataclasses
from collections import OrderedDict
from enum import Enum
from pathlib import PurePath, PurePosixPath
from typing import Optional

import pytest

from aredis_om.model.encoders import generate_encoders_by_class_tuples, jsonable_encoder

# ---------------------------------------------------------------------------
# Primitive pass-through
# ---------------------------------------------------------------------------


def test_encode_none():
    assert jsonable_encoder(None) is None


def test_encode_string():
    assert jsonable_encoder("hello") == "hello"


def test_encode_int():
    assert jsonable_encoder(42) == 42


def test_encode_float():
    assert jsonable_encoder(3.14) == 3.14


# ---------------------------------------------------------------------------
# Enum
# ---------------------------------------------------------------------------


class Color(Enum):
    RED = "red"
    GREEN = "green"


def test_encode_enum():
    assert jsonable_encoder(Color.RED) == "red"


class IntEnum(Enum):
    ONE = 1
    TWO = 2


def test_encode_int_enum():
    assert jsonable_encoder(IntEnum.TWO) == 2


# ---------------------------------------------------------------------------
# PurePath
# ---------------------------------------------------------------------------


def test_encode_purepath():
    p = PurePosixPath("/usr/local/bin")
    assert jsonable_encoder(p) == "/usr/local/bin"


# ---------------------------------------------------------------------------
# Dict encoding
# ---------------------------------------------------------------------------


def test_encode_dict_simple():
    d = {"a": 1, "b": "two"}
    assert jsonable_encoder(d) == {"a": 1, "b": "two"}


def test_encode_dict_nested_enum():
    d = {"color": Color.GREEN}
    assert jsonable_encoder(d) == {"color": "green"}


def test_encode_dict_exclude_none():
    d = {"a": 1, "b": None}
    result = jsonable_encoder(d, exclude_none=True)
    assert result == {"a": 1}


def test_encode_dict_exclude():
    d = {"a": 1, "b": 2, "c": 3}
    result = jsonable_encoder(d, exclude={"b"})
    assert result == {"a": 1, "c": 3}


def test_encode_dict_sqlalchemy_safe_skips_sa_keys():
    d = {"_sa_internal": "skip", "name": "keep"}
    result = jsonable_encoder(d, sqlalchemy_safe=True)
    assert result == {"name": "keep"}


def test_encode_dict_sqlalchemy_safe_false_keeps_sa_keys():
    d = {"_sa_internal": "keep", "name": "keep"}
    result = jsonable_encoder(d, sqlalchemy_safe=False)
    assert result == {"_sa_internal": "keep", "name": "keep"}


# ---------------------------------------------------------------------------
# List / tuple / set / frozenset / generator
# ---------------------------------------------------------------------------


def test_encode_list():
    assert jsonable_encoder([1, "two", Color.RED]) == [1, "two", "red"]


def test_encode_tuple():
    assert jsonable_encoder((1, 2)) == [1, 2]


def test_encode_set():
    result = jsonable_encoder({1})
    assert result == [1]


def test_encode_frozenset():
    result = jsonable_encoder(frozenset([1]))
    assert result == [1]


def test_encode_generator():
    def gen():
        yield 1
        yield 2

    assert jsonable_encoder(gen()) == [1, 2]


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Point:
    x: int
    y: int


def test_encode_dataclass():
    p = Point(1, 2)
    assert jsonable_encoder(p) == {"x": 1, "y": 2}


# ---------------------------------------------------------------------------
# Object that can be converted to dict
# ---------------------------------------------------------------------------


class DictLike:
    """Object that supports dict() conversion."""

    def __init__(self):
        self.a = 1
        self.b = 2

    def keys(self):
        return ["a", "b"]

    def __getitem__(self, key):
        return getattr(self, key)


def test_encode_dict_like_object():
    result = jsonable_encoder(DictLike())
    assert result == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# Object that falls back to vars()
# ---------------------------------------------------------------------------


class PlainObj:
    def __init__(self):
        self.x = 10


def test_encode_object_fallback_to_vars():
    result = jsonable_encoder(PlainObj())
    assert result == {"x": 10}


# ---------------------------------------------------------------------------
# Object that raises ValueError
# ---------------------------------------------------------------------------


class Unencodable:
    """Can't be dict()ed or vars()ed."""

    __slots__ = ()

    def __iter__(self):
        raise TypeError("nope")


def test_encode_unencodable_raises_value_error():
    with pytest.raises(ValueError):
        jsonable_encoder(Unencodable())


# ---------------------------------------------------------------------------
# Custom encoder
# ---------------------------------------------------------------------------


class MyClass:
    def __init__(self, val):
        self.val = val


def test_custom_encoder_by_exact_type():
    custom = {MyClass: lambda o: {"custom": o.val}}
    result = jsonable_encoder(MyClass(42), custom_encoder=custom)
    assert result == {"custom": 42}


def test_custom_encoder_by_isinstance_fallback():
    class SubClass(MyClass):
        pass

    custom = {MyClass: lambda o: {"inherited": o.val}}
    result = jsonable_encoder(SubClass(7), custom_encoder=custom)
    assert result == {"inherited": 7}


# ---------------------------------------------------------------------------
# generate_encoders_by_class_tuples
# ---------------------------------------------------------------------------


def test_generate_encoders_by_class_tuples():
    encoder_fn = str
    mapping = {int: encoder_fn, float: encoder_fn}
    result = generate_encoders_by_class_tuples(mapping)
    assert encoder_fn in result
    assert int in result[encoder_fn]
    assert float in result[encoder_fn]
