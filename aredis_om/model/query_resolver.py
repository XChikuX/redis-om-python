from typing import List, Optional, Union

from aredis_om.model.model import (
    Expression,
    FindQuery,
    KNNExpression,
    NegatedExpression,
)


# A single sub-expression accepted by the logical operators. Besides plain
# ``Expression`` / ``NegatedExpression`` instances, a nested logical operator
# (``Or`` / ``And`` / ``Not``) or a ``KNNExpression`` can also be combined.
ExpressionLike = Union[
    Expression, NegatedExpression, KNNExpression, "LogicalOperatorForListOfExpressions"
]


def _render_expression(expression: ExpressionLike) -> str:
    """Render a single sub-expression to a RediSearch query string.

    Nested logical operators are rendered via their own ``query`` property;
    everything else is delegated to ``FindQuery.resolve_redisearch_query``,
    which is the single source of truth for turning an ``Expression`` tree into
    RediSearch syntax.
    """
    if isinstance(expression, LogicalOperatorForListOfExpressions):
        return expression.query
    # After filtering out LogicalOperatorForListOfExpressions, the remaining
    # types are Expression | NegatedExpression, which resolve_redisearch_query
    # accepts. KNNExpression is handled separately by the KNN path and never
    # reaches this function through Or/And/Not combination.
    return FindQuery.resolve_redisearch_query(expression)  # type: ignore[arg-type]


class LogicalOperatorForListOfExpressions(Expression):
    """Base class for the ``Or`` / ``And`` / ``Not`` logical query operators.

    Each subclass declares the ``operator`` symbol used in RediSearch syntax
    and implements :meth:`_combine` to join the rendered sub-queries.
    """

    operator: str = ""

    def __init__(self, *expressions: ExpressionLike):
        self.expressions = list(expressions)

    def _combine(self, parts: List[str]) -> str:
        """Join already-parenthesized, rendered sub-query parts."""
        raise NotImplementedError

    @property
    def query(self) -> str:
        """Render this logical expression as a RediSearch query string.

        Each sub-expression is resolved to a RediSearch fragment, wrapped in
        parentheses (so operator precedence is preserved when nesting), and
        finally combined by the subclass-specific :meth:`_combine`.
        """
        if not self.expressions:
            raise AttributeError("At least one expression must be provided")
        parts = [f"({_render_expression(expr)})" for expr in self.expressions]
        return self._combine(parts)


class Or(LogicalOperatorForListOfExpressions):
    """
    Logical OR query operator

    Example:

    ```python
    class Product(JsonModel):
        price: float
        category: str

    Or(Product.price < 10, Product.category == "Sweets")
    ```

    Will return RediSearch query string like:

    ```
    (@price:[-inf 10]) | (@category:{Sweets})
    ```
    """

    operator = "|"

    def _combine(self, parts: List[str]) -> str:
        return " | ".join(parts)


class And(LogicalOperatorForListOfExpressions):
    """
    Logical AND query operator

    Example:

    ```python
    class Product(Document):
        price: float
        category: str

    And(Product.price < 10, Product.category == "Sweets")
    ```

    Will return a query string like:

    ```
    (@price:[-inf 10]) (@category:{Sweets})
    ```

    Note that in RediSearch, AND is implied with multiple terms.
    """

    operator = " "

    def _combine(self, parts: List[str]) -> str:
        return " ".join(parts)


class Not(LogicalOperatorForListOfExpressions):
    """
    Logical NOT query operator

    Example:

    ```python
    class Product(Document):
        price: float
        category: str

    Not(Product.price<10, Product.category=="Sweets")
    ```

    Will return a query string like:

    ```
    -(@price:[-inf 10]) -(@category:{Sweets})
    ```
    """

    operator = "-"

    def _combine(self, parts: List[str]) -> str:
        return " ".join(f"-{part}" for part in parts)


class QueryResolver:
    """Resolve one or more expressions into a single RediSearch query string.

    With a single expression the rendered string is returned directly. With
    multiple expressions they are combined with implicit AND (a space in
    RediSearch syntax), which is the safest default for combining independent
    field predicates. With no expressions, ``None`` is returned so callers can
    distinguish "no query" from an empty string.
    """

    def __init__(self, *expressions: ExpressionLike):
        self.expressions = expressions

    def resolve(self) -> Optional[str]:
        """Resolve expressions to a RediSearch query string."""
        if not self.expressions:
            return None
        rendered = [_render_expression(expr) for expr in self.expressions]
        if len(rendered) == 1:
            return rendered[0]
        # Multiple top-level expressions: combine with implicit AND (space).
        return " ".join(rendered)
