# type: ignore
"""Tests for aredis_om.model.query_resolver – Or, And, Not operators."""

from unittest import mock

import pytest

from aredis_om.model.model import Expression
from aredis_om.model.query_resolver import And, Not, Or, QueryResolver


def _make_expr():
    return mock.Mock(spec=Expression)


# ---------------------------------------------------------------------------
# Or
# ---------------------------------------------------------------------------


class TestOr:
    def test_or_returns_pipe_operator(self):
        e1, e2 = _make_expr(), _make_expr()
        result = Or(e1, e2)
        assert result.query == {"|": [e1, e2]}

    def test_or_single_expression(self):
        e = _make_expr()
        result = Or(e)
        assert result.query == {"|": [e]}

    def test_or_three_expressions(self):
        exprs = [_make_expr() for _ in range(3)]
        result = Or(*exprs)
        assert result.query == {"|": exprs}

    def test_or_empty_raises(self):
        result = Or()
        with pytest.raises(AttributeError, match="At least one expression"):
            result.query

    def test_or_operator_attribute(self):
        assert Or.operator == "|"


# ---------------------------------------------------------------------------
# And
# ---------------------------------------------------------------------------


class TestAnd:
    def test_and_returns_space_operator(self):
        e1, e2 = _make_expr(), _make_expr()
        result = And(e1, e2)
        assert result.query == {" ": [e1, e2]}

    def test_and_single_expression(self):
        e = _make_expr()
        result = And(e)
        assert result.query == {" ": [e]}

    def test_and_empty_raises(self):
        result = And()
        with pytest.raises(AttributeError, match="At least one expression"):
            result.query

    def test_and_operator_attribute(self):
        assert And.operator == " "


# ---------------------------------------------------------------------------
# Not
# ---------------------------------------------------------------------------


class TestNot:
    def test_not_returns_minus_operator(self):
        e1, e2 = _make_expr(), _make_expr()
        result = Not(e1, e2)
        assert result.query == {"-": [e1, e2]}

    def test_not_single_expression(self):
        e = _make_expr()
        result = Not(e)
        assert result.query == {"-": [e]}

    def test_not_empty_raises(self):
        result = Not()
        with pytest.raises(AttributeError, match="At least one expression"):
            result.query

    def test_not_operator_attribute(self):
        assert Not.operator == "-"


# ---------------------------------------------------------------------------
# QueryResolver
# ---------------------------------------------------------------------------


class TestQueryResolver:
    def test_resolver_stores_expressions(self):
        e1, e2 = _make_expr(), _make_expr()
        qr = QueryResolver(e1, e2)
        assert qr.expressions == (e1, e2)

    def test_resolve_returns_none(self):
        e = _make_expr()
        qr = QueryResolver(e)
        # The current implementation returns None (stub)
        assert qr.resolve() is None

    def test_resolver_no_expressions(self):
        qr = QueryResolver()
        assert qr.expressions == ()
        assert qr.resolve() is None


# ---------------------------------------------------------------------------
# Inheritance checks
# ---------------------------------------------------------------------------


def test_or_is_subclass_of_expression():
    assert issubclass(Or, Expression)


def test_and_is_subclass_of_expression():
    assert issubclass(And, Expression)


def test_not_is_subclass_of_expression():
    assert issubclass(Not, Expression)
