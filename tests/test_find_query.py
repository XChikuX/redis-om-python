# type: ignore

import abc
import base64
import dataclasses
import datetime
import decimal
import uuid
from collections import namedtuple
from typing import Dict, List, Optional, Set, Union
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import (
    EmbeddedJsonModel,
    Field,
    FindQuery,
    HashModel,
    JsonModel,
    Migrator,
    NotFoundError,
    QueryNotSupportedError,
    QuerySyntaxError,
    RedisModelError,
)
from aredis_om.model.query_resolver import And, Not, Or
from tests._compat import EmailStr, PositiveInt, ValidationError
from tests._sync_redis import has_redis_json

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip

today = datetime.date.today()


@pytest_asyncio.fixture
async def m(key_prefix, redis):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Note(EmbeddedJsonModel):
        # ``description`` is indexed as TAG (default for ``str`` with
        # ``index=True``). Full-text search on embedded documents is not
        # supported, so we use TAG-only indexing here to exercise recursive
        # embedded-field query resolution rather than FTS.
        description: str = Field(index=True)
        created_on: datetime.datetime

    class Address(EmbeddedJsonModel):
        address_line_1: str
        address_line_2: Optional[str] = None
        city: str = Field(index=True)
        state: str
        country: str
        postal_code: str = Field(index=True)
        note: Optional[Note] = None

    class Item(EmbeddedJsonModel):
        price: decimal.Decimal
        name: str = Field(index=True)

    class Order(EmbeddedJsonModel):
        items: List[Item]
        created_on: datetime.datetime

    class Member(BaseJsonModel):
        first_name: str = Field(index=True, case_sensitive=True)
        last_name: str = Field(index=True)
        email: Optional[EmailStr] = Field(index=True, default=None)
        join_date: datetime.date
        age: Optional[PositiveInt] = Field(index=True, sortable=True, default=None)
        bio: Optional[str] = Field(index=True, full_text_search=True, default="")

        # Creates an embedded model.
        address: Address

        # Creates an embedded list of models.
        orders: Optional[List[Order]] = None

    await Migrator().run()

    return namedtuple(
        "Models", ["BaseJsonModel", "Note", "Address", "Item", "Order", "Member"]
    )(BaseJsonModel, Note, Address, Item, Order, Member)


@pytest.fixture()
def address(m):
    try:
        yield m.Address(
            address_line_1="1 Main St.",
            city="Portland",
            state="OR",
            country="USA",
            postal_code="11111",
        )
    except Exception as e:
        raise e


@pytest_asyncio.fixture()
async def members(address, m):
    member1 = m.Member(
        first_name="Andrew",
        last_name="Brookins",
        email="a@example.com",
        age=38,
        join_date=today,
        address=address,
        bio="Andrew is a software engineer",
    )

    member2 = m.Member(
        first_name="Kim",
        last_name="Brookins",
        email="k@example.com",
        age=34,
        join_date=today,
        address=address,
        bio="Kim is a newer hire",
    )

    member3 = m.Member(
        first_name="Andrew",
        last_name="Smith",
        email="as@example.com",
        age=100,
        join_date=today,
        address=address,
        bio="Andrew is old",
    )

    await member1.save()
    await member2.save()
    await member3.save()

    yield member1, member2, member3


@py_test_mark_asyncio
async def test_find_query_in(members, m):
    # << means "in"
    member1, member2, member3 = members
    model_name, fq = await FindQuery(
        expressions=[m.Member.pk << [member1.pk, member2.pk, member3.pk]],
        model=m.Member,
    ).get_query()
    in_str = (
        "(@pk:{"
        + str(member1.pk)
        + "|"
        + str(member2.pk)
        + "|"
        + str(member3.pk)
        + "})"
    )
    assert fq == ["FT.SEARCH", model_name, in_str, "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_not_in(members, m):
    # >> means "not in"
    member1, member2, member3 = members
    model_name, fq = await FindQuery(
        expressions=[m.Member.pk >> [member2.pk, member3.pk]], model=m.Member
    ).get_query()
    not_in_str = "-(@pk:{" + str(member2.pk) + "|" + str(member3.pk) + "})"
    assert fq == ["FT.SEARCH", model_name, not_in_str, "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_rejects_sequence_for_scalar_operator(m):
    """Passing a sequence to a scalar operator raises QueryNotSupportedError.

    Only IN (<<) and NOT_IN (>>) accept sequence right-hand sides. Other
    operators receive a single value, so a list/tuple is treated as a
    likely user mistake.
    """
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name == ["Andrew", "Bob"]], model=m.Member
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name != ["Andrew", "Bob"]], model=m.Member
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.age > [10, 20]], model=m.Member
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.age >= (10, 20)], model=m.Member
        ).get_query()
    # LT/LE for the numeric path (covers the second operator branch).
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.age < [10, 20]], model=m.Member
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.age <= (10, 20)], model=m.Member
        ).get_query()
    # String operators: LIKE (requires full_text_search=True), STARTSWITH,
    # ENDSWITH, CONTAINS. ``bio`` is the only FTS-enabled string field.
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.bio % ["and", "bob"]], model=m.Member
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name.startswith(["and", "bob"])],
            model=m.Member,
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name.endswith(("and", "bob"))],
            model=m.Member,
        ).get_query()
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name.contains(["and", "bob"])],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_find_query_accepts_string_and_bytes_for_scalar_operator(m):
    """``str`` and ``bytes`` are NOT considered sequences by the validator.

    We use ``collections.abc.Sequence`` to exclude ``str`` and ``bytes`` from
    the validation, so passing a plain string to EQ/NE/etc. continues to work
    and renders as a regular TAG field lookup. ``bytes`` is likewise allowed
    through the validator and is base64-encoded downstream (bytes are
    base64-encoded on save, so the query must encode them too to match the
    stored TAG value).
    """
    # str renders as a plain TAG lookup.
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == "Andrew"], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@first_name:{Andrew}", "LIMIT", 0, 1000]

    # bytes renders as the base64 of the value, RediSearch-escaped. For
    # ``b"Andrew"`` the base64 is ``"QW5kcmV3"`` and contains no RediSearch
    # special characters, so the escaper leaves it unchanged.
    expected_b64 = base64.b64encode(b"Andrew").decode("ascii")
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == b"Andrew"], model=m.Member
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "@first_name:{" + expected_b64 + "}",
        "LIMIT",
        0,
        1000,
    ]


# experssion testing; (==, !=, <, <=, >, >=, |, &, ~)
@py_test_mark_asyncio
async def test_find_query_eq(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == "Andrew"], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@first_name:{Andrew}", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_eq_embedded_field(key_prefix):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Address(EmbeddedJsonModel):
        city: str = Field(index=True)

    class Member(BaseJsonModel):
        address: Address

    model_name, fq = await FindQuery(
        expressions=[Member.address.city == "Portland"], model=Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@address_city:{Portland}", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_in_embedded_field(key_prefix):
    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class Personality(EmbeddedJsonModel):
        mbti: str = Field(index=True)

    class Member(BaseJsonModel):
        personality: Personality

    model_name, fq = await FindQuery(
        expressions=[Member.personality.mbti << ["INTJ", "ENTP"]],
        model=Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "(@personality_mbti:{INTJ|ENTP})",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_find_query_ne(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name != "Andrew"], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "-(@first_name:{Andrew})", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_lt(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age < 40], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@age:[-inf (40]", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_le(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age <= 38], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@age:[-inf 38]", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_gt(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age > 38], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@age:[(38 +inf]", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_ge(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age >= 38], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@age:[38 +inf]", "LIMIT", 0, 1000]


# tests for sorting and text search with and, or, not
@py_test_mark_asyncio
async def test_find_query_sort(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age > 0], model=m.Member, sort_fields=["age"]
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "@age:[(0 +inf]",
        "LIMIT",
        0,
        1000,
        "SORTBY",
        "age",
        "asc",
    ]


@py_test_mark_asyncio
async def test_find_query_sort_desc(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age > 0], model=m.Member, sort_fields=["-age"]
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "@age:[(0 +inf]",
        "LIMIT",
        0,
        1000,
        "SORTBY",
        "age",
        "desc",
    ]


@py_test_mark_asyncio
async def test_find_query_text_search(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.bio == "test"], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@bio:{test}", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_text_search_and(m, members):
    model_name, fq = await FindQuery(
        expressions=[m.Member.age < 40, m.Member.first_name == "Andrew"], model=m.Member
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "(@age:[-inf (40]) (@first_name:{Andrew})",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_find_query_text_search_or(m, members):
    model_name, fq = await FindQuery(
        expressions=[(m.Member.age < 40) | (m.Member.first_name == "Andrew")],
        model=m.Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "(@age:[-inf (40])| (@first_name:{Andrew})",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_find_query_text_search_not(m):
    model_name, fq = await FindQuery(
        expressions=[~(m.Member.first_name == "Andrew")], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "-(@first_name:{Andrew})", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_text_search_not_and(m, members):
    model_name, fq = await FindQuery(
        expressions=[~((m.Member.first_name == "Andrew") & (m.Member.age < 40))],
        model=m.Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "-((@first_name:{Andrew}) (@age:[-inf (40]))",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_find_query_text_search_not_or(m, members):
    model_name, fq = await FindQuery(
        expressions=[~((m.Member.first_name == "Andrew") | (m.Member.age < 40))],
        model=m.Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "-((@first_name:{Andrew})| (@age:[-inf (40]))",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_find_query_text_search_not_or_and(m, members):
    model_name, fq = await FindQuery(
        expressions=[
            ~(
                ((m.Member.first_name == "Andrew") | (m.Member.age < 40))
                & (m.Member.last_name == "Brookins")
            )
        ],
        model=m.Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "-(((@first_name:{Andrew})| (@age:[-inf (40])) (@last_name:{Brookins}))",
        "LIMIT",
        0,
        1000,
    ]


# text search operators; contains, startswith, endswith, fuzzy
@py_test_mark_asyncio
async def test_find_query_text_contains(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name.contains("drew")], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "(@first_name:{*drew*})", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_text_startswith(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name.startswith("An")], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "(@first_name:{An*})", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_text_endswith(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name.endswith("ew")], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "(@first_name:{*ew})", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_test_fuzzy(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.bio % "%newb%"], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@bio_fts:%newb%", "LIMIT", 0, 1000]


# limit, offset, page_size
@py_test_mark_asyncio
async def test_find_query_limit_one(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == "Andrew"], model=m.Member, limit=1
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@first_name:{Andrew}", "LIMIT", 0, 1]


@py_test_mark_asyncio
async def test_find_query_limit_offset(m):
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == "Andrew"], model=m.Member, limit=1, offset=1
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@first_name:{Andrew}", "LIMIT", 1, 1]


@py_test_mark_asyncio
async def test_find_query_page_size(m):
    # note that this test in unintuitive.
    # page_size gets resolved in a while True loop that makes copies of the intial query and adds the limit and offset each time
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == "Andrew"], model=m.Member, page_size=1
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@first_name:{Andrew}", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_find_query_monster(m):
    # test monster query with everything everywhere all at once
    # including ors, nots, ands, less thans, greater thans, text search
    model_name, fq = await FindQuery(
        expressions=[
            ~(
                ((m.Member.first_name == "Andrew") | (m.Member.age < 40))
                & (
                    m.Member.last_name.contains("oo")
                    | ~(m.Member.email.startswith("z"))
                )
            )
        ],
        model=m.Member,
        limit=1,
        offset=1,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "-(((@first_name:{Andrew})| (@age:[-inf (40])) (((@last_name:{*oo*}))| -((@email:{z*}))))",
        "LIMIT",
        1,
        1,
    ]


# ---------------------------------------------------------------------------
# Sequence-value validation in compound / nested expressions.
#
# The validator in ``FindQuery.resolve_redisearch_query`` runs on every leaf
# expression because both the ``|`` / ``&`` / ``~`` operator path (which
# recurses through ``resolve_redisearch_query``) and the explicit ``Or`` /
# ``And`` / ``Not`` classes (which call ``_render_expression`` →
# ``resolve_redisearch_query`` for each leaf) end up dispatching to the same
# scalar-rendering branch. These tests pin down that a sequence passed to ANY
# scalar operator, no matter how deeply nested, is rejected.
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_sequence_rejected_in_pipe_operator_or(m):
    """``(a == [list]) | (b == x)`` rejects the sequence on the left."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                (m.Member.first_name == ["a", "b"]) | (m.Member.last_name == "x")
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_pipe_operator_or_right(m):
    """``(a == x) | (b == [list])`` rejects the sequence on the right."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                (m.Member.first_name == "x") | (m.Member.last_name == ["a", "b"])
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_ampersand_operator_and(m):
    """``(a == [list]) & (b == x)`` rejects the sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                (m.Member.first_name == ["a", "b"]) & (m.Member.last_name == "x")
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_tilde_operator_not(m):
    """``~(a == [list])`` rejects the sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[~(m.Member.first_name == ["a", "b"])],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_nested_tilde_and(m):
    """``~((a == [list]) & (b == x))`` rejects the sequence inside the negation."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                ~((m.Member.first_name == ["a", "b"]) & (m.Member.last_name == "x"))
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_explicit_or_left(m):
    """``Or(a == [list], b == x)`` rejects the sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                Or(m.Member.first_name == ["a", "b"], m.Member.last_name == "x")
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_explicit_or_right(m):
    """``Or(a == x, b == [list])`` rejects the sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                Or(m.Member.first_name == "x", m.Member.last_name == ["a", "b"])
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_explicit_and(m):
    """``And(a == [list], b == x)`` rejects the sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                And(m.Member.first_name == ["a", "b"], m.Member.last_name == "x")
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_explicit_not(m):
    """``Not(a == [list])`` rejects the sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[Not(m.Member.first_name == ["a", "b"])],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_deeply_nested_explicit(m):
    """``Or(And(a == [list], b), c)`` rejects the deeply nested sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                Or(
                    And(m.Member.first_name == ["a", "b"], m.Member.last_name == "y"),
                    m.Member.age == 30,
                )
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_deeply_nested_explicit_right(m):
    """``Or(And(a, b), c == [list])`` rejects the sequence in the right operand."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                Or(
                    And(m.Member.first_name == "a", m.Member.last_name == "y"),
                    m.Member.age == [30, 40],
                )
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_triple_nested_not(m):
    """``Or(And(Not(a == [list])), b)`` rejects the triple-nested sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                Or(And(Not(m.Member.first_name == ["a"])), m.Member.age == 30)
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_sequence_rejected_in_nested_explicit_or(m):
    """``Or(Or(valid, a == [list]), valid)`` rejects the nested-Or sequence."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[
                Or(
                    Or(m.Member.first_name == "a", m.Member.last_name == ["x", "y"]),
                    m.Member.age == 30,
                )
            ],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_valid_nested_query_still_renders(m):
    """Sanity check: equivalent nested queries with NO sequences render fine.

    This guards against a regression where the validator accidentally rejects
    valid compound expressions because of an over-broad type check.
    """
    model_name, fq = await FindQuery(
        expressions=[Or(And(Not(m.Member.first_name == "a")), m.Member.age == 30)],
        model=m.Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "((-(@first_name:{a}))) | (@age:[30 30])",
        "LIMIT",
        0,
        1000,
    ]


# ---------------------------------------------------------------------------
# Edge-case sequence types.
#
# ``collections.abc.Sequence`` accepts ``list``, ``tuple``, ``range``,
# ``collections.deque``, and the immutable variants, but NOT ``str`` / ``bytes``
# (which are special-cased) and NOT generators / iterators. These tests pin the
# boundary so a future refactor of the validator doesn't silently widen or
# narrow the rejection set.
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_range_is_rejected_as_sequence(m):
    """``range`` is a ``collections.abc.Sequence`` and must be rejected."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name == range(3)], model=m.Member
        ).get_query()


@py_test_mark_asyncio
async def test_deque_is_rejected_as_sequence(m):
    """``collections.deque`` is a ``collections.abc.Sequence`` and must be rejected."""
    import collections

    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name == collections.deque(["a", "b"])],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_empty_list_is_rejected_as_sequence(m):
    """An empty list is still a Sequence and must be rejected.

    An empty IN/NOT_IN is fine, but an empty list passed to a scalar operator
    is almost certainly a bug and should surface the validator's error rather
    than render an empty TAG lookup.
    """
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name == []], model=m.Member
        ).get_query()


@py_test_mark_asyncio
async def test_single_element_list_is_rejected_as_sequence(m):
    """A single-element list is still a Sequence and must be rejected.

    Users sometimes write ``field == [value]`` expecting an IN-style match;
    the validator steers them toward ``field << [value]`` (or just ``field ==
    value``) with a clear error.
    """
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name == ["a"]], model=m.Member
        ).get_query()


@py_test_mark_asyncio
async def test_generator_is_not_caught_by_validator(m):
    """Generators are NOT ``collections.abc.Sequence`` so the validator skips them.

    The generator therefore reaches the rendering stage and fails there with a
    ``TypeError``. This documents the boundary: the validator only catches
    materialized sequence containers, not arbitrary iterables. A stricter
    check using ``Iterable`` would also reject ``str`` / ``bytes``, which is
    why the implementation uses ``Sequence``.
    """
    with pytest.raises(TypeError):
        await FindQuery(
            expressions=[m.Member.first_name == (x for x in ["a", "b"])],
            model=m.Member,
        ).get_query()


@py_test_mark_asyncio
async def test_tuple_is_rejected_as_sequence(m):
    """``tuple`` is a ``collections.abc.Sequence`` and must be rejected."""
    with pytest.raises(QueryNotSupportedError, match="sequence value"):
        await FindQuery(
            expressions=[m.Member.first_name == ("a", "b")], model=m.Member
        ).get_query()


# ---------------------------------------------------------------------------
# Positive cases: ``str`` and ``bytes`` are explicitly allowed by the validator
# (``bytes`` rendering is a separate known issue; here we only assert the
# validator itself does not raise).
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_str_passes_validator_and_renders(m):
    """A plain ``str`` is allowed and renders as a TAG lookup."""
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == "Andrew"], model=m.Member
    ).get_query()
    assert fq == ["FT.SEARCH", model_name, "@first_name:{Andrew}", "LIMIT", 0, 1000]


@py_test_mark_asyncio
async def test_bytes_passes_validator_and_renders(m):
    """``bytes`` is explicitly allowed by the sequence validator and renders
    as a base64-encoded TAG lookup.

    Bytes values are base64-encoded on save (``convert_bytes_to_base64``), so
    the query path base64-encodes them too (see ``expand_tag_value``) to match
    the stored representation. There is no "known issue" here anymore; the
    bytes querying asymmetry was fixed and this test pins the fixed behavior.
    """
    expected_b64 = base64.b64encode(b"Andrew").decode("ascii")
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name == b"Andrew"], model=m.Member
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "@first_name:{" + expected_b64 + "}",
        "LIMIT",
        0,
        1000,
    ]
    # Sanity check: the rendered query must not contain a Python bytes repr
    # (``b'Andrew'``), which would indicate the raw bytes leaked through.
    assert "b'" not in fq[2]
    assert 'b"' not in fq[2]


# ---------------------------------------------------------------------------
# Sequence validation interacts correctly with IN / NOT_IN — they are exempt.
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_in_operator_accepts_sequence(m):
    """``<<`` (IN) must accept a sequence without triggering the validator."""
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name << ["Andrew", "Kim"]], model=m.Member
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "(@first_name:{Andrew|Kim})",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_not_in_operator_accepts_sequence(m):
    """``>>`` (NOT_IN) must accept a sequence without triggering the validator."""
    model_name, fq = await FindQuery(
        expressions=[m.Member.first_name >> ["Andrew", "Kim"]], model=m.Member
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "-(@first_name:{Andrew|Kim})",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_in_inside_or_does_not_trigger_validator(m):
    """An ``IN`` query nested in an ``Or`` must render, not be rejected."""
    model_name, fq = await FindQuery(
        expressions=[Or(m.Member.first_name << ["Andrew", "Kim"], m.Member.age == 30)],
        model=m.Member,
    ).get_query()
    assert fq == [
        "FT.SEARCH",
        model_name,
        "((@first_name:{Andrew|Kim})) | (@age:[30 30])",
        "LIMIT",
        0,
        1000,
    ]


@py_test_mark_asyncio
async def test_not_in_inside_not_does_not_trigger_validator(m):
    """A ``NOT_IN`` query nested in a ``Not`` must render, not be rejected."""
    model_name, fq = await FindQuery(
        expressions=[Not(m.Member.first_name >> ["Andrew"])],
        model=m.Member,
    ).get_query()
    # Not(...) wraps the rendered inner query in ``-(... (-(...)) )``. The
    # exact shape is exercised here to keep the test honest.
    assert fq[0] == "FT.SEARCH"
    assert fq[1] == model_name
    assert "@first_name:{Andrew}" in fq[2]
    assert "-" in fq[2]


# ---------------------------------------------------------------------------
# End-to-end combination tests.
#
# These exercises the full pipeline: build a compound query with ``Or`` /
# ``And`` / ``Not`` and the ``|`` / ``&`` / ``~`` operators, render it, send it
# to Redis, and verify the returned members match expectations. They guard
# against regressions where a query *renders* but returns the wrong result set
# (e.g. because of parenthesis placement or operator precedence).
#
# Fixture ``members`` inserts:
#   member1: Andrew Brookins, age 38
#   member2: Kim    Brookins, age 34
#   member3: Andrew Smith,    age 100
# ---------------------------------------------------------------------------


@py_test_mark_asyncio
async def test_explicit_or_returns_union(members, m):
    """``Or(first_name == Andrew, last_name == Brookins)`` matches all 3.

    Andrew appears twice in first_name; Brookins appears twice in last_name;
    the union is all three members.
    """
    member1, member2, member3 = members
    found = await m.Member.find(
        Or(m.Member.first_name == "Andrew", m.Member.last_name == "Brookins")
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member1.pk, member2.pk, member3.pk}


@py_test_mark_asyncio
async def test_explicit_and_returns_intersection(members, m):
    """``And(first_name == Andrew, last_name == Smith)`` matches only member3."""
    member1, member2, member3 = members
    found = await m.Member.find(
        And(m.Member.first_name == "Andrew", m.Member.last_name == "Smith")
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member3.pk}


@py_test_mark_asyncio
async def test_explicit_not_excludes_matches(members, m):
    """``Not(first_name == Andrew)`` matches only member2 (Kim)."""
    member1, member2, member3 = members
    found = await m.Member.find(Not(m.Member.first_name == "Andrew")).all()
    pks = {member.pk for member in found}
    assert pks == {member2.pk}


@py_test_mark_asyncio
async def test_pipe_operator_or_returns_union(members, m):
    """``(age == 100) | (age == 34)`` matches member2 and member3."""
    member1, member2, member3 = members
    found = await m.Member.find((m.Member.age == 100) | (m.Member.age == 34)).all()
    pks = {member.pk for member in found}
    assert pks == {member2.pk, member3.pk}


@py_test_mark_asyncio
async def test_ampersand_operator_and_returns_intersection(members, m):
    """``(first_name == Andrew) & (age == 100)`` matches only member3."""
    member1, member2, member3 = members
    found = await m.Member.find(
        (m.Member.first_name == "Andrew") & (m.Member.age == 100)
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member3.pk}


@py_test_mark_asyncio
async def test_tilde_operator_not_excludes(members, m):
    """``~(last_name == Brookins)`` matches only member3 (Smith)."""
    member1, member2, member3 = members
    found = await m.Member.find(~(m.Member.last_name == "Brookins")).all()
    pks = {member.pk for member in found}
    assert pks == {member3.pk}


@py_test_mark_asyncio
async def test_nested_or_inside_and_end_to_end(members, m):
    """``And(Or(first == Andrew, first == Kim), last == Brookins)``.

    The Or matches all three (Andrew×2, Kim×1); intersecting with
    ``last_name == Brookins`` (member1, member2) yields member1 + member2.
    """
    member1, member2, member3 = members
    found = await m.Member.find(
        And(
            Or(m.Member.first_name == "Andrew", m.Member.first_name == "Kim"),
            m.Member.last_name == "Brookins",
        )
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member1.pk, member2.pk}


@py_test_mark_asyncio
async def test_not_inside_or_end_to_end(members, m):
    """``Or(Not(first == Andrew), age == 100)``.

    Not(first == Andrew) → member2 (Kim).
    age == 100 → member3.
    Union → member2 + member3.
    """
    member1, member2, member3 = members
    found = await m.Member.find(
        Or(Not(m.Member.first_name == "Andrew"), m.Member.age == 100)
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member2.pk, member3.pk}


@py_test_mark_asyncio
async def test_in_query_combined_with_or_end_to_end(members, m):
    """Mixing ``<<`` (IN) with a scalar inside an ``Or``.

    ``Or(age << [34, 100], first_name == Andrew)`` should match all three:
    IN matches member2 (34) and member3 (100); the Andrew branch matches
    member1 and member3. Union = all three.
    """
    member1, member2, member3 = members
    found = await m.Member.find(
        Or(m.Member.age << [34, 100], m.Member.first_name == "Andrew")
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member1.pk, member2.pk, member3.pk}


@py_test_mark_asyncio
async def test_negated_compound_query_end_to_end(members, m):
    """``~((first == Andrew) | (age < 40))``.

    first == Andrew → member1, member3.
    age < 40 → member1 (38), member2 (34).
    Union → all three.
    Negation → empty set.
    """
    found = await m.Member.find(
        ~((m.Member.first_name == "Andrew") | (m.Member.age < 40))
    ).all()
    assert found == []


@py_test_mark_asyncio
async def test_deeply_nested_query_end_to_end(members, m):
    """Deep nesting: ``Or(And(Not(last == Smith), age >= 34), first == Kim)``.

    Not(last == Smith) → member1, member2.
    age >= 34 → member1 (38), member2 (34), member3 (100).
    And → member1, member2.
    Or(first == Kim → member2) → member1, member2.
    """
    member1, member2, member3 = members
    found = await m.Member.find(
        Or(
            And(Not(m.Member.last_name == "Smith"), m.Member.age >= 34),
            m.Member.first_name == "Kim",
        )
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member1.pk, member2.pk}


@py_test_mark_asyncio
async def test_explicit_and_with_in_operator_end_to_end(members, m):
    """``And(first_name << [Andrew], last_name << [Brookins, Smith])``.

    first_name IN [Andrew] → member1, member3.
    last_name IN [Brookins, Smith] → all three.
    And → member1, member3.
    """
    member1, member2, member3 = members
    found = await m.Member.find(
        And(
            m.Member.first_name << ["Andrew"],
            m.Member.last_name << ["Brookins", "Smith"],
        )
    ).all()
    pks = {member.pk for member in found}
    assert pks == {member1.pk, member3.pk}
