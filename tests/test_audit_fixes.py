# type: ignore
"""Regression tests for the 2026-07-01 deep-audit fixes.

Each test corresponds to a finding documented in CLAUDE.md under
"Recent bug fixes and deep-review findings (2026-07-01)". Tests that need
Redis are skipped automatically when RediSearch / RedisJSON is unavailable.
"""

import abc
from typing import List, Optional
from unittest import mock

import pytest
import pytest_asyncio

from aredis_om import (
    EmbeddedJsonModel,
    Field,
    FindQueryCursor,
    HashModel,
    JsonModel,
    Migrator,
    RedisModelError,
)
from aredis_om.model.model import FindQuery, RediSearchFieldTypes
from tests._sync_redis import has_redis_json, has_redisearch

from .conftest import py_test_mark_asyncio

HAS_REDISEARCH = has_redisearch()
HAS_REDIS_JSON = has_redis_json()


# ---------------------------------------------------------------------------
# #1 has_redisearch no longer conflates RedisJSON with RediSearch
# ---------------------------------------------------------------------------


async def _async_false(*_a, **_kw):
    return False


async def _async_true(*_a, **_kw):
    return True


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_has_redisearch_does_not_shortcut_on_redisjson(monkeypatch):
    """``has_redisearch`` must check ``ft.search`` directly, not shortcut to
    ``True`` when RedisJSON is present."""
    from aredis_om import checks

    captured = []

    async def fake_check_for_command(conn, cmd):
        captured.append(cmd)
        return cmd == "ft.search"

    monkeypatch.setattr(checks, "has_redis_json", _async_true)
    monkeypatch.setattr(checks, "check_for_command", fake_check_for_command)

    result = await checks.has_redisearch(conn=object())

    assert result is True
    # Must have actually probed ft.search, not just json.set.
    assert "ft.search" in captured


# ---------------------------------------------------------------------------
# #2 __getitem__ / get_item off-by-one (cache boundary)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_get_item_returns_last_cached_element(key_prefix, redis):
    """If the cache holds exactly N elements, requesting index N-1 must be
    served from cache (the old ``>=`` check treated N-1+1 == N as out of range
    when N == item, causing an IndexError)."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Item(BaseHashModel, index=True):
        name: str = Field(index=True)

    await Migrator(conn=redis).run()

    saved = []
    for i in range(5):
        it = Item(name=f"item-{i}")
        await it.save()
        saved.append(it)

    q = Item.find()
    # Force a query execution that fills the cache with exactly 5 elements.
    await q.execute()
    assert len(q._model_cache) == 5

    # Index 4 is the last cached element. The old ``>= 4`` (5 >= 4) returned
    # cache[4] correctly; the bug only bit when item == len(cache). We test
    # the boundary where item == len(cache) - 1 to ensure no off-by-one
    # regression *and* the boundary where item == len(cache) which must NOT
    # read past the cache.
    last_cached = await q.get_item(4)
    assert last_cached.pk == saved[4].pk

    # item == len(cache) must NOT use the cache (would IndexError under the
    # old code); it must issue a new query and return None (offset beyond
    # result set yields empty results -> IndexError from [0]).
    with pytest.raises(IndexError):
        await q.get_item(5)


# ---------------------------------------------------------------------------
# #3 validate_primary_key threshold (> 2 is intentional)
# ---------------------------------------------------------------------------


def test_validate_primary_key_allows_one_custom_pk():
    """A model that overrides the inherited ``pk`` with exactly one custom
    ``primary_key=True`` field must be valid. The inherited ``pk`` field
    counts toward the total, so the threshold is ``> 2`` (not ``> 1``)."""

    class WithCustomPK(HashModel, abc.ABC):
        custom_id: str = Field(primary_key=True, index=True)
        name: str

    # Should NOT raise: total PK count is 2 (inherited pk + custom_id).
    WithCustomPK.validate_primary_key()
    assert WithCustomPK._meta.primary_key.name == "custom_id"


def test_validate_primary_key_rejects_two_custom_pks():
    """Two user-defined ``primary_key=True`` fields (total count 3 with the
    inherited pk) must raise."""

    class WithTwoCustomPKs(HashModel, abc.ABC):
        id: str = Field(primary_key=True, index=True)
        alt_id: str = Field(primary_key=True, index=True)
        name: str

    with pytest.raises(RedisModelError, match="only one primary key"):
        WithTwoCustomPKs.validate_primary_key()


def test_validate_primary_key_rejects_zero_pks(monkeypatch):
    """A model with no primary key field at all must raise."""

    class HasPK(HashModel, abc.ABC):
        name: str

    # Simulate "no primary keys" by making validate_primary_key see zero.
    # We patch model_fields to return fields with primary_key falsy.
    fake_fields = {}
    for name, fi in HasPK.model_fields.items():
        fake = mock.Mock(spec=fi)
        fake.primary_key = False
        fake_fields[name] = fake

    monkeypatch.setattr(HasPK, "model_fields", fake_fields)
    with pytest.raises(RedisModelError, match="must define a primary key"):
        HasPK.validate_primary_key()


# ---------------------------------------------------------------------------
# #4 TEXT field EQ/NE values are escaped
# ---------------------------------------------------------------------------


def test_text_field_eq_escapes_quotes():
    """A TEXT-field EQ value containing a double-quote must be escaped in the
    rendered RediSearch query; otherwise it would terminate the quoted
    phrase and produce malformed syntax.

    Note: in normal model queries ``str`` fields resolve to TAG for EQ/NE,
    so this exercises ``resolve_value`` directly to cover the TEXT code path.
    """
    from aredis_om.model.model import Operators

    class Doc(JsonModel, abc.ABC):
        body: str = Field(index=True, full_text_search=True)

    rendered = FindQuery.resolve_value(
        field_name="body",
        field_type=RediSearchFieldTypes.TEXT,
        field_info=Doc.model_fields["body"],
        op=Operators.EQ,
        value='hello "world"',
        parents=[],
    )
    # The raw " must have been escaped by TokenEscaper (it is in the
    # DEFAULT_ESCAPED_CHARS regex). The result should contain a backslash
    # before the inner quote.
    assert '@body_fts:"' in rendered
    assert '\\"' in rendered  # escaped quote present


def test_text_field_ne_escapes_quotes():
    from aredis_om.model.model import Operators

    class Doc(JsonModel, abc.ABC):
        body: str = Field(index=True, full_text_search=True)

    rendered = FindQuery.resolve_value(
        field_name="body",
        field_type=RediSearchFieldTypes.TEXT,
        field_info=Doc.model_fields["body"],
        op=Operators.NE,
        value='oops "quoted"',
        parents=[],
    )
    assert rendered.startswith("-(")
    assert '\\"' in rendered


# ---------------------------------------------------------------------------
# #5 Integer primary-key queries use TAG syntax on TAG-indexed fields
# ---------------------------------------------------------------------------


def test_int_pk_query_uses_numeric_range_syntax():
    """An ``int`` primary key is indexed as NUMERIC (not TAG) by the schema
    generator, even though ``resolve_field_type`` returns TAG for every PK.
    The query must therefore use NUMERIC range syntax ``@id:[5 5]``, not
    TAG exact-match syntax ``@id:{5}`` (which returns zero results against
    a NUMERIC-indexed field)."""

    class Doc(HashModel, abc.ABC):
        id: int = Field(primary_key=True, index=True)

    # Sanity check: the schema really is NUMERIC.
    assert "id NUMERIC" in Doc.redisearch_schema()

    expr = Doc.id == 5
    rendered = FindQuery.resolve_redisearch_query(expr)
    assert "@id:[5 5]" in rendered
    # Must NOT use TAG syntax on a NUMERIC-indexed field.
    assert "@id:{5}" not in rendered


# ---------------------------------------------------------------------------
# #6 JsonModel schema generation accepts List[Model] with non-str fields
# ---------------------------------------------------------------------------


def test_json_schema_accepts_list_of_model_with_int_field():
    """``List[Address]`` where ``Address`` has an ``int`` field must produce a
    schema without raising RedisModelError."""

    class Address(EmbeddedJsonModel):
        street: str = Field(index=True)
        zip_code: int = Field(index=True)

    class Person(JsonModel, abc.ABC):
        name: str = Field(index=True)
        addresses: List[Address]

    schema = Person.redisearch_schema()
    # The int field inside the list of embedded models must be indexed
    # as NUMERIC, not rejected.
    assert "addresses_zip_code NUMERIC" in schema
    assert "addresses_street TAG" in schema


# ---------------------------------------------------------------------------
# #7 TAG separator-split queries join segments with spaces (implicit AND)
# ---------------------------------------------------------------------------


def test_tag_separator_split_joins_with_spaces():
    """When a queried value contains the TAG separator, the segments must be
    joined with a space (implicit AND), producing valid RediSearch syntax.
    The old code concatenated with no separator: ``@f:{a}@f:{b}`` which is
    malformed."""

    class Member(HashModel, abc.ABC):
        tags: str = Field(index=True, separator=";")

    expr = Member.tags == "a;b"
    rendered = FindQuery.resolve_redisearch_query(expr)
    # Must contain two field queries joined by a space.
    assert "@tags:{a} @tags:{b}" in rendered


# ---------------------------------------------------------------------------
# #8 Meta value resolution: empty string means "use the default"
#
# The original audit incorrectly proposed changing ``if not getattr(...)`` to
# ``if getattr(...) is None``. That broke ``primary_key_pattern = ""``, which
# is an established sentinel in the codebase (see ``tests/test_oss_redis_features.py``
# and the ``m`` fixture in ``tests/test_hash_model.py``) meaning "fall back to
# the inherited ``{pk}`` pattern." Reverting to ``if not getattr(...)`` keeps
# the library working. These tests pin the intended behavior.
# ---------------------------------------------------------------------------


def test_empty_string_primary_key_pattern_uses_default():
    """``primary_key_pattern = ""`` must fall back to the inherited ``{pk}``,
    NOT be preserved as empty (which would cause key collisions)."""

    class WithEmptyPattern(HashModel, abc.ABC):
        custom_id: int = Field(primary_key=True, index=True)

        class Meta:
            primary_key_pattern = ""

    # Empty string must be overwritten with the default "{pk}".
    assert WithEmptyPattern._meta.primary_key_pattern == "{pk}"
    # Keys must include the primary-key value.
    assert WithEmptyPattern.make_primary_key(42).endswith(":42")


def test_empty_string_model_key_prefix_uses_default():
    """``model_key_prefix = ""`` must fall back to the generated default."""

    class WithEmptyPrefix(HashModel, abc.ABC):
        name: str

        class Meta:
            model_key_prefix = ""

    # Empty string must be overwritten with the module-qualified default.
    assert WithEmptyPrefix._meta.model_key_prefix != ""
    assert (
        "." in WithEmptyPrefix._meta.model_key_prefix
    )  # e.g. "__main__.WithEmptyPrefix"


def test_empty_string_key_separator_uses_default():
    """``key_separator = ""`` must fall back to the inherited ``":"`` so that
    keys remain correctly delimited."""

    class WithEmptySeparator(HashModel, abc.ABC):
        name: str

        class Meta:
            key_separator = ""

    assert WithEmptySeparator._meta.key_separator == ":"


def test_explicit_non_empty_meta_values_are_preserved():
    """Non-empty custom values must NOT be overwritten by the inherited default."""

    class WithCustomValues(HashModel, abc.ABC):
        name: str

        class Meta:
            model_key_prefix = "custom_prefix"
            primary_key_pattern = "id::{pk}"
            key_separator = "|"

    assert WithCustomValues._meta.model_key_prefix == "custom_prefix"
    assert WithCustomValues._meta.primary_key_pattern == "id::{pk}"
    assert WithCustomValues._meta.key_separator == "|"
    # Verify the custom pattern is actually used in key generation.
    assert "id::42" in WithCustomValues.make_primary_key(42)
    # key_separator flows into index_name (not make_key, which is fixed).
    assert "|" in WithCustomValues._meta.index_name


# ---------------------------------------------------------------------------
# #9 Pagination increments offset by ``limit`` not ``page_size``
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_exhaust_results_with_limit_smaller_than_page_size(key_prefix, redis):
    """``execute(exhaust_results=True)`` paginates by ``limit``. When limit
    differs from page_size, the old code stepped by page_size and either
    skipped results (page_size > limit) or re-fetched (page_size < limit)."""

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Item(BaseHashModel, index=True):
        name: str = Field(index=True)

    await Migrator(conn=redis).run()

    created = []
    for i in range(10):
        it = Item(name=f"item-{i}")
        await it.save()
        created.append(it)

    # Query with limit=3 but page_size=10. The exhaust loop must step by 3.
    results = (
        await Item.find().copy(limit=3, page_size=10).execute(exhaust_results=True)
    )
    pks = {r.pk for r in results}
    expected = {it.pk for it in created}
    # Every created item must be present (no gaps from wrong step size).
    assert pks == expected, (
        f"expected {len(expected)} results, got {len(pks)}; pagination gap"
    )


# ---------------------------------------------------------------------------
# #10 FindQuery.delete() swallows ResponseError (documented behavior)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_delete_swallows_response_error(key_prefix, redis):
    """``FindQuery.delete()`` returns 0 on ``ResponseError`` by design."""
    from redis import ResponseError

    class BaseHashModel(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Item(BaseHashModel, index=True):
        name: str = Field(index=True)

    await Migrator(conn=redis).run()

    saved = Item(name="a")
    await saved.save()

    q = Item.find(Item.name == "a")

    # Mock the db().delete() to raise ResponseError. We patch the whole
    # ``db`` classmethod to return a fake whose delete raises; ``all()``
    # is replaced with a coroutine that returns the saved record so we
    # skip the FT.SEARCH round-trip (the real db cannot delete the key
    # if we intercept delete).
    class FakeDB:
        async def delete(self, *keys):
            raise ResponseError("simulated")

    async def fake_all():
        return [saved]

    with (
        mock.patch.object(Item, "db", return_value=FakeDB()),
        mock.patch.object(q, "all", fake_all),
    ):
        result = await q.delete()

    assert result == 0


# ---------------------------------------------------------------------------
# #11 ExpressionProxy __eq__/__ne__ use ``# type: ignore`` (not ``# ty:``)
# ---------------------------------------------------------------------------


def test_expression_proxy_eq_ne_pragma():
    """The mypy suppression pragma must be ``# type: ignore``, not the
    unrecognized ``# ty: ignore``."""
    import inspect

    from aredis_om.model.model import ExpressionProxy

    src = inspect.getsource(ExpressionProxy.__eq__)
    assert "# type: ignore[override]" in src
    assert "# ty:" not in src

    src = inspect.getsource(ExpressionProxy.__ne__)
    assert "# type: ignore[override]" in src
    assert "# ty:" not in src


# ---------------------------------------------------------------------------
# #12 HashModel.schema_for_type has no misleading List[int] comment
# ---------------------------------------------------------------------------


def test_schema_for_type_has_no_misleading_comment():
    """The misleading comment claiming ``List[int]`` maps to TAG (it actually
    maps to NUMERIC, and HashModel rejects list fields at class-definition
    time anyway) must be absent."""
    import inspect

    from aredis_om.model.model import HashModel

    src = inspect.getsource(HashModel.schema_for_type)
    # The misleading comment block must have been removed.
    assert "Container-of-scalars handling" not in src
    assert "RediSearch indexes arrays as multi-value TAG fields" not in src


# ---------------------------------------------------------------------------
# #13 FindQueryCursor uses collections.deque (O(1) popleft)
# ---------------------------------------------------------------------------


def test_find_query_cursor_buffer_is_deque():
    """The cursor buffer must be a ``collections.deque`` (not a list) so that
    ``popleft`` is O(1)."""
    import collections

    cursor = FindQueryCursor.__new__(FindQueryCursor)
    FindQueryCursor.__init__(
        cursor,
        model=mock.Mock(),
        index_name="idx",
        cursor_id=0,
        count=10,
        results=[mock.Mock(), mock.Mock()],
    )
    assert isinstance(cursor._buffer, collections.deque)
    assert len(cursor._buffer) == 2


@pytest.mark.skipif(not HAS_REDISEARCH, reason="requires RediSearch")
@py_test_mark_asyncio
async def test_cursor_read_clears_buffer(key_prefix, redis):
    """``FindQueryCursor.read()`` must return the buffered items and clear the
    buffer so subsequent reads fetch fresh results from Redis."""

    class BaseJsonModel(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix
            database = redis

    class Item(BaseJsonModel, index=True):
        name: str = Field(index=True)

    await Migrator(conn=redis).run()

    a = Item(name="a")
    await a.save()

    cursor = FindQueryCursor(
        model=Item,
        index_name=Item.Meta.index_name,
        cursor_id=0,
        count=10,
        results=[a],
    )
    assert len(cursor._buffer) == 1
    out = await cursor.read()
    assert len(out) == 1
    assert len(cursor._buffer) == 0
