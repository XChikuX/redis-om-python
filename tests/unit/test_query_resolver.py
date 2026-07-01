# type: ignore
"""Tests for aredis_om.model.query_resolver – Or, And, Not operators."""

from unittest import mock

import pytest

from aredis_om.model.model import Expression, FindQuery
from aredis_om.model.query_resolver import And, Not, Or, QueryResolver


def _make_expr():
    return mock.Mock(spec=Expression)


@pytest.fixture
def mock_render():
    """Patch resolve_redisearch_query so tests assert combination logic only.

    Each leaf Expression is meaningless on its own in a pure unit test, so we
    stub the shared renderer and control what it returns per call.
    """
    with mock.patch.object(FindQuery, "resolve_redisearch_query") as mocked:
        yield mocked


# ---------------------------------------------------------------------------
# Or
# ---------------------------------------------------------------------------


class TestOr:
    def test_or_combines_with_pipe(self, mock_render):
        mock_render.side_effect = ["@price:[-inf 10]", "@category:{Sweets}"]
        assert Or(_make_expr(), _make_expr()).query == (
            "(@price:[-inf 10]) | (@category:{Sweets})"
        )

    def test_or_single_expression(self, mock_render):
        mock_render.return_value = "@price:[-inf 10]"
        assert Or(_make_expr()).query == "(@price:[-inf 10])"

    def test_or_three_expressions(self, mock_render):
        mock_render.side_effect = ["@a:1", "@b:2", "@c:3"]
        result = Or(*[_make_expr() for _ in range(3)]).query
        assert result == "(@a:1) | (@b:2) | (@c:3)"

    def test_or_empty_raises(self):
        with pytest.raises(AttributeError, match="At least one expression"):
            Or().query

    def test_or_operator_attribute(self):
        assert Or.operator == "|"


# ---------------------------------------------------------------------------
# And
# ---------------------------------------------------------------------------


class TestAnd:
    def test_and_combines_with_space(self, mock_render):
        mock_render.side_effect = ["@price:[-inf 10]", "@category:{Sweets}"]
        assert And(_make_expr(), _make_expr()).query == (
            "(@price:[-inf 10]) (@category:{Sweets})"
        )

    def test_and_single_expression(self, mock_render):
        mock_render.return_value = "@price:[-inf 10]"
        assert And(_make_expr()).query == "(@price:[-inf 10])"

    def test_and_three_expressions(self, mock_render):
        mock_render.side_effect = ["@a:1", "@b:2", "@c:3"]
        result = And(*[_make_expr() for _ in range(3)]).query
        assert result == "(@a:1) (@b:2) (@c:3)"

    def test_and_empty_raises(self):
        with pytest.raises(AttributeError, match="At least one expression"):
            And().query

    def test_and_operator_attribute(self):
        assert And.operator == " "


# ---------------------------------------------------------------------------
# Not
# ---------------------------------------------------------------------------


class TestNot:
    def test_not_prefixes_each_with_minus(self, mock_render):
        mock_render.side_effect = ["@price:[-inf 10]", "@category:{Sweets}"]
        assert Not(_make_expr(), _make_expr()).query == (
            "-(@price:[-inf 10]) -(@category:{Sweets})"
        )

    def test_not_single_expression(self, mock_render):
        mock_render.return_value = "@price:[-inf 10]"
        assert Not(_make_expr()).query == "-(@price:[-inf 10])"

    def test_not_three_expressions(self, mock_render):
        mock_render.side_effect = ["@a:1", "@b:2", "@c:3"]
        result = Not(*[_make_expr() for _ in range(3)]).query
        assert result == "-(@a:1) -(@b:2) -(@c:3)"

    def test_not_empty_raises(self):
        with pytest.raises(AttributeError, match="At least one expression"):
            Not().query

    def test_not_operator_attribute(self):
        assert Not.operator == "-"


# ---------------------------------------------------------------------------
# Nesting
# ---------------------------------------------------------------------------


class TestNesting:
    def test_nested_or_inside_and_uses_query_not_renderer(self, mock_render):
        """A nested Or must render via its own .query, not resolve_redisearch_query.

        Three leaf expressions reach the patched renderer: the two inside the
        nested Or, plus the one passed directly to the outer And. The nested
        Or itself is handled by _render_expression and never calls the
        renderer.
        """
        mock_render.side_effect = ["@a:1", "@b:2", "@c:3"]
        nested = Or(_make_expr(), _make_expr())
        result = And(nested, _make_expr()).query
        assert result == "((@a:1) | (@b:2)) (@c:3)"
        # The nested Or contributed two renders; the outer And's own leaf
        # contributed one.
        assert mock_render.call_count == 3

    def test_nested_not_inside_or(self, mock_render):
        mock_render.side_effect = ["@a:1", "@b:2", "@c:3"]
        nested = Not(_make_expr(), _make_expr())
        result = Or(nested, _make_expr()).query
        assert result == "(-(@a:1) -(@b:2)) | (@c:3)"


# ---------------------------------------------------------------------------
# QueryResolver
# ---------------------------------------------------------------------------


class TestQueryResolver:
    def test_resolver_stores_expressions(self):
        e1, e2 = _make_expr(), _make_expr()
        qr = QueryResolver(e1, e2)
        assert qr.expressions == (e1, e2)

    def test_resolve_single_expression(self, mock_render):
        mock_render.return_value = "@price:[-inf 10]"
        assert QueryResolver(_make_expr()).resolve() == "@price:[-inf 10]"

    def test_resolve_multiple_combines_with_implicit_and(self, mock_render):
        mock_render.side_effect = ["@a:1", "@b:2"]
        assert QueryResolver(_make_expr(), _make_expr()).resolve() == "@a:1 @b:2"

    def test_resolver_no_expressions(self):
        qr = QueryResolver()
        assert qr.expressions == ()
        assert qr.resolve() is None


# ---------------------------------------------------------------------------
# FindQuery.resolve_redisearch_query integration
# ---------------------------------------------------------------------------


class TestResolveRedisearchQueryIntegration:
    """Or/And/Not must be passable directly to FindQuery / find().

    resolve_redisearch_query delegates to the operator's ``.query`` property
    rather than trying to read the ``op``/``left``/``right`` dataclass fields,
    which a LogicalOperatorForListOfExpressions does not populate.
    """

    def test_resolve_redisearch_query_delegates_to_or_query(self):
        op = Or(_make_expr(), _make_expr())
        with mock.patch.object(type(op), "query", new_callable=mock.PropertyMock) as q:
            q.return_value = "RENDERED"
            assert FindQuery.resolve_redisearch_query(op) == "RENDERED"
        q.assert_called_once()

    def test_resolve_redisearch_query_delegates_to_and_query(self):
        op = And(_make_expr(), _make_expr())
        with mock.patch.object(type(op), "query", new_callable=mock.PropertyMock) as q:
            q.return_value = "RENDERED"
            assert FindQuery.resolve_redisearch_query(op) == "RENDERED"
        q.assert_called_once()

    def test_resolve_redisearch_query_delegates_to_not_query(self):
        op = Not(_make_expr(), _make_expr())
        with mock.patch.object(type(op), "query", new_callable=mock.PropertyMock) as q:
            q.return_value = "RENDERED"
            assert FindQuery.resolve_redisearch_query(op) == "RENDERED"
        q.assert_called_once()


# ---------------------------------------------------------------------------
# Inheritance checks
# ---------------------------------------------------------------------------


def test_or_is_subclass_of_expression():
    assert issubclass(Or, Expression)


def test_and_is_subclass_of_expression():
    assert issubclass(And, Expression)


def test_not_is_subclass_of_expression():
    assert issubclass(Not, Expression)
