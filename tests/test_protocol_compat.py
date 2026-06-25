# type: ignore
"""End-to-end integration tests for the RESP2/RESP3 protocol accommodation.

These tests run against a live Redis instance and exercise the public
``HashModel`` and ``JsonModel`` APIs under both RESP2 (forced via
``?protocol=2``) and RESP3 (auto-negotiated against Redis 6+).  The goal is
to confirm that user-visible behaviour is identical regardless of the wire
protocol.
"""

import pytest

from aredis_om import Field, HashModel, JsonModel, Migrator, get_redis_connection
from aredis_om.model.model import model_registry

from .conftest import py_test_mark_asyncio


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Clear the model registry around each test so the ``Migrator`` does not
    pick up models from previous tests (which may have left Redis
    connections on a now-closed event loop).
    """
    saved = dict(model_registry)
    model_registry.clear()
    yield
    model_registry.clear()
    model_registry.update(saved)


# ── Helpers ─────────────────────────────────────────────────────────────


def _make_hash_model(prefix: str, db, name: str = "_Hash"):
    """Build a HashModel bound to a specific database connection.

    The ``name`` argument disambiguates the class (and therefore its index
    name) when multiple fixtures want to use independent indexes.  We use
    ``exec`` so Pydantic can resolve the field annotations correctly.
    """
    ns = {
        "HashModel": HashModel,
        "Field": Field,
        "prefix": prefix,
        "db": db,
    }
    code = f"""
class {name}(HashModel):
    name: str = Field(index=True)
    age: int = Field(index=True, sortable=True)

    class Meta:
        global_key_prefix = prefix
        database = db
"""
    exec(code, ns)
    return ns[name]


def _make_json_model(prefix: str, db, name: str = "_Json"):
    """Build a JsonModel bound to a specific database connection."""

    ns = {
        "JsonModel": JsonModel,
        "Field": Field,
        "prefix": prefix,
        "db": db,
    }
    code = f"""
class {name}(JsonModel):
    name: str = Field(index=True)
    age: int = Field(index=True, sortable=True)

    class Meta:
        global_key_prefix = prefix
        database = db
"""
    exec(code, ns)
    return ns[name]


@pytest.fixture
def resp2_redis():
    """A Redis client pinned to RESP2."""
    return get_redis_connection(
        url="redis://localhost:6380?decode_responses=True&protocol=2"
    )


@pytest.fixture
def resp3_redis():
    """A Redis client using the default (auto-negotiated, RESP3) protocol."""
    return get_redis_connection(url="redis://localhost:6380?decode_responses=True")


# ── HashModel CRUD parity ────────────────────────────────────────────────


class TestHashModelCrudParity:
    @py_test_mark_asyncio
    async def test_save_and_get_resp2(self, key_prefix, resp2_redis):
        M = _make_hash_model(key_prefix, resp2_redis, name="_Resp2HashSave")
        await Migrator().run()

        m = M(name="Alice", age=30)
        await m.save()
        loaded = await M.get(m.pk)
        assert loaded.name == "Alice"
        assert loaded.age == 30
        await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_save_and_get_resp3(self, key_prefix, resp3_redis):
        M = _make_hash_model(key_prefix, resp3_redis, name="_Resp3HashSave")
        await Migrator().run()

        m = M(name="Bob", age=25)
        await m.save()
        loaded = await M.get(m.pk)
        assert loaded.name == "Bob"
        assert loaded.age == 25
        await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_find_eq_resp2(self, key_prefix, resp2_redis):
        M = _make_hash_model(key_prefix, resp2_redis, name="_Resp2HashFind")
        await Migrator().run()

        for n, age in [("a", 1), ("b", 2), ("c", 3)]:
            await M(name=n, age=age).save()
        results = await M.find(M.age == 2).all()
        assert [r.name for r in results] == ["b"]
        for r in await M.find().all():
            await M.delete(r.pk)

    @py_test_mark_asyncio
    async def test_find_eq_resp3(self, key_prefix, resp3_redis):
        M = _make_hash_model(key_prefix, resp3_redis, name="_Resp3HashFind")
        await Migrator().run()

        for n, age in [("a", 1), ("b", 2), ("c", 3)]:
            await M(name=n, age=age).save()
        results = await M.find(M.age == 2).all()
        assert [r.name for r in results] == ["b"]
        for r in await M.find().all():
            await M.delete(r.pk)


# ── JsonModel CRUD parity ────────────────────────────────────────────────


class TestJsonModelCrudParity:
    @py_test_mark_asyncio
    async def test_save_and_get_resp2(self, key_prefix, resp2_redis):
        M = _make_json_model(key_prefix, resp2_redis, name="_Resp2JsonSave")
        await Migrator().run()

        m = M(name="Alice", age=30)
        await m.save()
        loaded = await M.get(m.pk)
        assert loaded.name == "Alice"
        assert loaded.age == 30
        await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_save_and_get_resp3(self, key_prefix, resp3_redis):
        M = _make_json_model(key_prefix, resp3_redis, name="_Resp3JsonSave")
        await Migrator().run()

        m = M(name="Bob", age=25)
        await m.save()
        loaded = await M.get(m.pk)
        assert loaded.name == "Bob"
        assert loaded.age == 25
        await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_get_many_resp2(self, key_prefix, resp2_redis):
        M = _make_json_model(key_prefix, resp2_redis, name="_Resp2JsonMany")
        await Migrator().run()

        pks = []
        for n in ["x", "y", "z"]:
            m = M(name=n, age=10)
            await m.save()
            pks.append(m.pk)
        loaded = await M.get_many(pks)
        names = sorted(r.name for r in loaded)
        assert names == ["x", "y", "z"]
        for pk in pks:
            await M.delete(pk)

    @py_test_mark_asyncio
    async def test_get_many_resp3(self, key_prefix, resp3_redis):
        M = _make_json_model(key_prefix, resp3_redis, name="_Resp3JsonMany")
        await Migrator().run()

        pks = []
        for n in ["x", "y", "z"]:
            m = M(name=n, age=10)
            await m.save()
            pks.append(m.pk)
        loaded = await M.get_many(pks)
        names = sorted(r.name for r in loaded)
        assert names == ["x", "y", "z"]
        for pk in pks:
            await M.delete(pk)


# ── Cursor pagination parity ─────────────────────────────────────────────


class TestCursorParity:
    @py_test_mark_asyncio
    async def test_iter_cursor_resp2(self, key_prefix, resp2_redis):
        M = _make_json_model(key_prefix, resp2_redis, name="_Resp2JsonCursor")
        await Migrator().run()

        for i in range(5):
            await M(name=f"c2_{i}", age=i).save()
        cursor = await M.find().sort_by("age").iter_cursor(count=2)
        all_results = []
        async for m in cursor:
            all_results.append(m)
        assert len(all_results) == 5
        assert cursor.total == 5
        await cursor.close()
        for m in await M.find().all():
            await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_iter_cursor_resp3(self, key_prefix, resp3_redis):
        M = _make_json_model(key_prefix, resp3_redis, name="_Resp3JsonCursor")
        await Migrator().run()

        for i in range(5):
            await M(name=f"c3_{i}", age=i).save()
        cursor = await M.find().sort_by("age").iter_cursor(count=2)
        all_results = []
        async for m in cursor:
            all_results.append(m)
        assert len(all_results) == 5
        assert cursor.total == 5
        await cursor.close()
        for m in await M.find().all():
            await M.delete(m.pk)


# ── find().count() parity ───────────────────────────────────────────────


class TestCountParity:
    @py_test_mark_asyncio
    async def test_nocontent_count_resp2(self, key_prefix, resp2_redis):
        M = _make_hash_model(key_prefix, resp2_redis, name="_Resp2HashCount")
        await Migrator().run()

        for i in range(7):
            await M(name=f"h2_{i}", age=i).save()
        n = await M.find().count()
        assert n == 7
        for m in await M.find().all():
            await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_nocontent_count_resp3(self, key_prefix, resp3_redis):
        M = _make_hash_model(key_prefix, resp3_redis, name="_Resp3HashCount")
        await Migrator().run()

        for i in range(7):
            await M(name=f"h3_{i}", age=i).save()
        n = await M.find().count()
        assert n == 7
        for m in await M.find().all():
            await M.delete(m.pk)


# ── aggregate_ct parity ────────────────────────────────────────────────


class TestAggregateCtParity:
    @py_test_mark_asyncio
    async def test_aggregate_ct_resp2(self, key_prefix, resp2_redis):
        M = _make_json_model(key_prefix, resp2_redis, name="_Resp2JsonAgg")
        await Migrator().run()

        for n in ["a", "a", "b", "b", "b"]:
            await M(name=n, age=1).save()
        n = await M.find().aggregate_ct()
        assert n == 5
        for m in await M.find().all():
            await M.delete(m.pk)

    @py_test_mark_asyncio
    async def test_aggregate_ct_resp3(self, key_prefix, resp3_redis):
        M = _make_json_model(key_prefix, resp3_redis, name="_Resp3JsonAgg")
        await Migrator().run()

        for n in ["a", "a", "b", "b", "b"]:
            await M(name=n, age=1).save()
        n = await M.find().aggregate_ct()
        assert n == 5
        for m in await M.find().all():
            await M.delete(m.pk)
