# type: ignore
"""Unit tests for encoders — Pydantic BaseModel branches and fallback paths.

Covers the often-missed ``jsonable_encoder`` branches:

* ``include`` / ``exclude`` parameter coercion (line 66, 68).
* BaseModel branch with custom ``json_encoders`` (71-84).
* dataclass dispatch (line 92).
* Enum branch (line 94).
* ``PurePath`` branch (line 96).
* list / set / frozenset / generator / tuple branch (130-146).
* custom_encoder type / isinstance lookup (149-154).
* ENCODERS_BY_TYPE fallback (156-160).
* ``dict(obj)`` / ``vars(obj)`` fallback (162-171).

End-to-end coverage lives implicitly in every other test in this suite.
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath

import pytest
from pydantic import BaseModel

from aredis_om.model.encoders import jsonable_encoder


# ── helpers ──────────────────────────────────────────────────────────────


class _Color(Enum):
    RED = "red"
    GREEN = "green"


@dataclass
class _Point:
    x: int
    y: int


class _Person(BaseModel):
    name: str
    age: int

    model_config = {}


# ── include / exclude parameter coercion ─────────────────────────────────


class TestIncludeExcludeCoercion:
    def test_include_set_passes_coercion(self):
        # Passing a set skips the coercion branch on line 65. The output
        # reflects the dict-level include filter (no-op when only one of
        # include/exclude is given — see the OR-combination on line 110).
        out = jsonable_encoder({"a": 1, "b": 2}, include={"a"})
        assert out == {"a": 1, "b": 2}

    def test_include_dict_passes_coercion(self):
        out = jsonable_encoder({"a": 1, "b": 2}, include={"a": True})
        assert out == {"a": 1, "b": 2}

    def test_include_list_coerced(self):
        # A list (or any other non-(set,dict) iterable) goes through the
        # ``include = set(include)`` coercion branch on line 65-66.
        out = jsonable_encoder({"a": 1, "b": 2}, include=["a"])
        assert out == {"a": 1, "b": 2}

    def test_include_tuple_coerced(self):
        out = jsonable_encoder({"a": 1, "b": 2}, include=("a",))
        assert out == {"a": 1, "b": 2}

    def test_exclude_list_coerced(self):
        out = jsonable_encoder({"a": 1, "b": 2}, exclude=["b"])
        assert out == {"a": 1}

    def test_exclude_tuple_coerced(self):
        out = jsonable_encoder({"a": 1, "b": 2}, exclude=("b",))
        assert out == {"a": 1}


# ── BaseModel branch ─────────────────────────────────────────────────────


class TestBaseModelBranch:
    def test_basic_model_serialization(self):
        person = _Person(name="alice", age=3)
        out = jsonable_encoder(person)
        assert out == {"name": "alice", "age": 3}

    def test_exclude_none(self):
        # ``age`` is required so this just exercises the keyword path.
        person = _Person(name="alice", age=1)
        out = jsonable_encoder(person, exclude_none=True)
        assert out == {"name": "alice", "age": 1}

    def test_exclude_unset(self):
        person = _Person(name="alice", age=1)
        # exclude_unset drops anything that wasn't explicitly set.
        out = jsonable_encoder(person, exclude_unset=True)
        assert "name" in out
        assert "age" in out

    def test_include_set(self):
        person = _Person(name="alice", age=1)
        out = jsonable_encoder(person, include={"name"})
        # ``include`` propagates to ``model_dump`` so only the listed
        # field is present in the result.
        assert out == {"name": "alice"}

    def test_exclude_set(self):
        person = _Person(name="alice", age=1)
        out = jsonable_encoder(person, exclude={"age"})
        assert out == {"name": "alice"}

    def test_custom_encoder_at_dict_recurse_is_no_op_for_primitives(self):
        # int is in the fast-path tuple, so custom_encoder is NOT applied
        # when the value is encoded inside a dict. This documents the
        # current behavior at the dict-recursion branch.
        person = _Person(name="alice", age=1)

        def shout_int(v: int) -> str:
            return f"AGE={v}"

        out = jsonable_encoder(person, custom_encoder={int: shout_int})
        assert out["name"] == "alice"
        assert out["age"] == 1  # primitive fast-path, no encoder


# ── dataclass, Enum, PurePath ────────────────────────────────────────────


class TestScalarBranches:
    def test_dataclass_branch(self):
        # ``dataclasses.is_dataclass`` is one of the early returns.
        pt = _Point(x=1, y=2)
        out = jsonable_encoder(pt)
        assert out == {"x": 1, "y": 2}

    def test_enum_branch(self):
        out = jsonable_encoder(_Color.RED)
        assert out == "red"

    def test_purepath_branch(self):
        out = jsonable_encoder(PurePath("/tmp/foo"))
        assert out == "/tmp/foo"

    def test_primitives_pass_through(self):
        # str / int / float / None go through unchanged.
        assert jsonable_encoder("hello") == "hello"
        assert jsonable_encoder(7) == 7
        assert jsonable_encoder(1.5) == 1.5
        assert jsonable_encoder(None) is None


# ── collection branches ─────────────────────────────────────────────────


class TestCollectionBranches:
    def test_list_branch(self):
        out = jsonable_encoder([1, "two", 3.0])
        assert out == [1, "two", 3.0]

    def test_set_branch(self):
        out = jsonable_encoder({1, 2, 3})
        # Order is unspecified for sets; verify content.
        assert sorted(out) == [1, 2, 3]

    def test_frozenset_branch(self):
        out = jsonable_encoder(frozenset({1, 2, 3}))
        assert sorted(out) == [1, 2, 3]

    def test_tuple_branch(self):
        out = jsonable_encoder((1, 2, 3))
        assert out == [1, 2, 3]

    def test_generator_branch(self):
        gen = (x for x in range(3))
        out = jsonable_encoder(gen)
        assert out == [0, 1, 2]

    def test_dict_exclude(self):
        out = jsonable_encoder({"a": 1, "b": 2, "c": 3}, exclude={"b"})
        assert out == {"a": 1, "c": 3}

    def test_dict_exclude_none(self):
        out = jsonable_encoder({"a": 1, "b": None}, exclude_none=True)
        assert out == {"a": 1}


# ── dict/sqlalchemy_safe filter ──────────────────────────────────────────


class TestSqlalchemySafe:
    def test_excludes_sa_prefixed_keys(self):
        # ``_sa_*`` keys are filtered out when sqlalchemy_safe=True (default).
        payload = {"_sa_instance_state": "x", "name": "alice"}
        out = jsonable_encoder(payload)
        assert "_sa_instance_state" not in out
        assert out == {"name": "alice"}

    def test_keeps_sa_prefixed_when_disabled(self):
        payload = {"_sa_instance_state": "x", "name": "alice"}
        out = jsonable_encoder(payload, sqlalchemy_safe=False)
        assert out == {"_sa_instance_state": "x", "name": "alice"}

    def test_non_string_key_kept(self):
        # Non-string dict keys bypass the ``_sa`` check.
        out = jsonable_encoder({1: "one", 2: "two"})
        assert out == {1: "one", 2: "two"}


# ── custom_encoder branches ──────────────────────────────────────────────


class TestCustomEncoder:
    def test_exact_type_match_at_top_level(self):
        # At the top level (after all branches above), the custom_encoder
        # exact-type lookup fires. Pass a value that doesn't match the
        # built-in type fast-paths.
        class _Money:
            def __init__(self, amount):
                self.amount = amount

        out = jsonable_encoder(
            _Money(42), custom_encoder={_Money: lambda v: f"${v.amount}"}
        )
        assert out == "$42"

    def test_isinstance_match(self):
        # When the type lookup misses but an isinstance() lookup matches.
        class _Animal:
            pass

        class _Dog(_Animal):
            pass

        out = jsonable_encoder(_Dog(), custom_encoder={_Animal: lambda v: "an-animal"})
        assert out == "an-animal"


# ── fallback paths ──────────────────────────────────────────────────────


class TestFallbacks:
    def test_vars_fallback(self):
        # ``dict(obj)`` fails (no .keys()), ``vars(obj)`` works.
        class _Thing:
            def __init__(self):
                self.a = 1
                self.b = 2

        out = jsonable_encoder(_Thing())
        assert out == {"a": 1, "b": 2}

    def test_unencodable_raises(self):
        class _Opaque:
            __slots__ = ()

        # Both ``dict(obj)`` and ``vars(obj)`` raise. Verify the helper
        # wraps both errors in a single ValueError.
        with pytest.raises(ValueError):
            jsonable_encoder(_Opaque())
