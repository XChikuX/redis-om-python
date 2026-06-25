# type: ignore
"""RESP2 vs RESP3 performance comparison benchmarks.

This file is intentionally separate from the regular benchmark suite so the
RESP2-vs-RESP3 timing comparison can be run on demand with
``pytest tests/test_protocol_benchmark.py -v`` without polluting the
single-instance benchmark report.

Each test runs the same operation against two Redis connections (one forced
to RESP2 via ``?protocol=2`` and one using the default RESP3-auto-negotiation)
and records the elapsed wall-clock time for both.  At the end of the
session, ``test_zzz_print_protocol_comparison`` prints a side-by-side
comparison table.
"""

import time
from typing import Dict

import pytest

from aredis_om import Field, HashModel, JsonModel, Migrator, get_redis_connection
from aredis_om.model.model import model_registry

from .conftest import py_test_mark_asyncio

PROTOCOL_RESULTS: Dict[str, Dict[str, float]] = {}


def record(name: str, resp2_s: float, resp3_s: float):
    PROTOCOL_RESULTS[name] = {
        "resp2_s": round(resp2_s, 6),
        "resp3_s": round(resp3_s, 6),
    }


def _pct_diff(resp2: float, resp3: float) -> float:
    if resp2 == 0:
        return 0.0
    return round((resp2 - resp3) / resp2 * 100, 2)


@pytest.fixture
def resp2_redis():
    return get_redis_connection(
        url="redis://localhost:6380?decode_responses=True&protocol=2"
    )


@pytest.fixture
def resp3_redis():
    return get_redis_connection(url="redis://localhost:6380?decode_responses=True")


def _make_hash_model(prefix, db, name):
    ns = {
        "HashModel": HashModel,
        "Field": Field,
        "prefix": prefix,
        "db": db,
    }
    code = (
        "class " + name + "(HashModel):\n"
        "    name: str = Field(index=True)\n"
        "    age: int = Field(index=True, sortable=True)\n"
        "    class Meta:\n"
        "        global_key_prefix = prefix\n"
        "        database = db\n"
    )
    exec(code, ns)
    return ns[name]


def _make_json_model(prefix, db, name):
    ns = {
        "JsonModel": JsonModel,
        "Field": Field,
        "prefix": prefix,
        "db": db,
    }
    code = (
        "class " + name + "(JsonModel):\n"
        "    name: str = Field(index=True)\n"
        "    age: int = Field(index=True, sortable=True)\n"
        "    class Meta:\n"
        "        global_key_prefix = prefix\n"
        "        database = db\n"
    )
    exec(code, ns)
    return ns[name]


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(model_registry)
    model_registry.clear()
    yield
    model_registry.clear()
    model_registry.update(saved)


async def _run(label, resp2_conn, resp3_conn, key_prefix, body):
    """Run ``body(M)`` on both protocols and record timings."""
    saved = dict(model_registry)
    model_registry.clear()

    M2 = _make_hash_model(key_prefix, resp2_conn, name="_Bench2_" + label)
    await Migrator().run()
    start = time.perf_counter()
    await body(M2)
    t2 = time.perf_counter() - start
    model_registry.clear()

    M3 = _make_hash_model(key_prefix, resp3_conn, name="_Bench3_" + label)
    await Migrator().run()
    start = time.perf_counter()
    await body(M3)
    t3 = time.perf_counter() - start

    record(label, t2, t3)
    model_registry.clear()
    model_registry.update(saved)


@py_test_mark_asyncio
async def test_bench_hash_single_save(key_prefix, resp2_redis, resp3_redis):
    async def body(M):
        m = M(name="bench", age=42)
        await m.save()
        await M.delete(m.pk)

    await _run("hash_single_save", resp2_redis, resp3_redis, key_prefix, body)


@py_test_mark_asyncio
async def test_bench_hash_single_get(key_prefix, resp2_redis, resp3_redis):
    async def body(M):
        m = M(name="bench", age=42)
        await m.save()
        for _ in range(50):
            await M.get(m.pk)
        await M.delete(m.pk)

    await _run("hash_single_get", resp2_redis, resp3_redis, key_prefix, body)


@py_test_mark_asyncio
async def test_bench_hash_find_all(key_prefix, resp2_redis, resp3_redis):
    async def body(M):
        for i in range(50):
            await M(name="bench_" + str(i), age=i).save()
        for _ in range(5):
            results = await M.find().all()
            assert len(results) == 50
        for r in await M.find().all():
            await M.delete(r.pk)

    await _run("hash_find_all", resp2_redis, resp3_redis, key_prefix, body)


@py_test_mark_asyncio
async def test_bench_hash_find_count(key_prefix, resp2_redis, resp3_redis):
    async def body(M):
        for i in range(50):
            await M(name="bench_" + str(i), age=i).save()
        for _ in range(10):
            n = await M.find().count()
            assert n == 50
        for r in await M.find().all():
            await M.delete(r.pk)

    await _run("hash_find_count", resp2_redis, resp3_redis, key_prefix, body)


@py_test_mark_asyncio
async def test_bench_json_single_save(key_prefix, resp2_redis, resp3_redis):
    async def body2(M):
        m = M(name="bench", age=42)
        await m.save()
        await M.delete(m.pk)

    async def body3(M):
        m = M(name="bench", age=42)
        await m.save()
        await M.delete(m.pk)

    saved = dict(model_registry)
    model_registry.clear()
    M2 = _make_json_model(key_prefix, resp2_redis, name="_JsonBench2_save")
    await Migrator().run()
    start = time.perf_counter()
    await body2(M2)
    t2 = time.perf_counter() - start
    model_registry.clear()

    M3 = _make_json_model(key_prefix, resp3_redis, name="_JsonBench3_save")
    await Migrator().run()
    start = time.perf_counter()
    await body3(M3)
    t3 = time.perf_counter() - start
    record("json_single_save", t2, t3)
    model_registry.clear()
    model_registry.update(saved)


@py_test_mark_asyncio
async def test_bench_json_find_all(key_prefix, resp2_redis, resp3_redis):
    async def body(M):
        for i in range(50):
            await M(name="bench_" + str(i), age=i).save()
        for _ in range(5):
            results = await M.find().all()
            assert len(results) == 50
        for r in await M.find().all():
            await M.delete(r.pk)

    saved = dict(model_registry)
    model_registry.clear()
    M2 = _make_json_model(key_prefix, resp2_redis, name="_JsonBench2_find_all")
    await Migrator().run()
    start = time.perf_counter()
    await body(M2)
    t2 = time.perf_counter() - start
    model_registry.clear()

    M3 = _make_json_model(key_prefix, resp3_redis, name="_JsonBench3_find_all")
    await Migrator().run()
    start = time.perf_counter()
    await body(M3)
    t3 = time.perf_counter() - start
    record("json_find_all", t2, t3)
    model_registry.clear()
    model_registry.update(saved)


@py_test_mark_asyncio
async def test_zzz_print_protocol_comparison():
    """Print a side-by-side RESP2 vs RESP3 comparison table."""
    if not PROTOCOL_RESULTS:
        pytest.skip("No benchmark results recorded yet")

    print("\n" + "=" * 78)
    print("RESP2 vs RESP3 PROTOCOL COMPARISON")
    print("=" * 78)
    print(f"{'Benchmark':<28} {'RESP2 (s)':<14} {'RESP3 (s)':<14} {'Delta':<10}")
    print("-" * 78)
    for name in sorted(PROTOCOL_RESULTS.keys()):
        data = PROTOCOL_RESULTS[name]
        delta = _pct_diff(data["resp2_s"], data["resp3_s"])
        delta_str = "%+.2f%%" % delta
        print(f"{name:<28} {data['resp2_s']:<14} {data['resp3_s']:<14} {delta_str:<10}")
    print("=" * 78)
    print("Positive delta % means RESP3 was faster than RESP2.")
    print("Negative delta % means RESP3 was slower than RESP2.")
