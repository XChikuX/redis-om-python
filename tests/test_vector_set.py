# type: ignore
"""Tests for VectorSet (VADD, VSIM, VINFO, etc., Redis 8.8+).

These commands require the @vectorset preview module shipped in Redis 8.8.
Tests skip gracefully when commands are unavailable.
"""

import pytest

from aredis_om import VectorSet, get_redis_connection

from .conftest import py_test_mark_asyncio


def _has_command(db, command):
    """Probe the server for a command's availability."""

    async def _check():
        try:
            info = await db.execute_command("COMMAND", "INFO", command)
            return bool(info and all(info))
        except Exception:
            return False

    return _check


@pytest.fixture
def db():
    return get_redis_connection()


@pytest.fixture
def vs(db, key_prefix):
    return VectorSet(db, f"{key_prefix}:vset")


class TestAddRemove:
    @py_test_mark_asyncio
    async def test_add_returns_true_for_new(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        got = await vs.add([1.0, 2.0, 3.0], "doc1")
        assert got is True

    @py_test_mark_asyncio
    async def test_add_duplicate_returns_false(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        # Same element: returns 0 (no new element).
        got = await vs.add([1.0, 2.0, 3.0], "doc1")
        assert got is False

    @py_test_mark_asyncio
    async def test_add_cas_replaces_existing(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        got = await vs.add([9.0, 9.0, 9.0], "doc1", cas=True)
        assert got is False  # No new element added.
        emb = await vs.embedding("doc1")
        assert emb == [9.0, 9.0, 9.0]

    @py_test_mark_asyncio
    async def test_add_with_ef(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        got = await vs.add([1.0, 2.0], "doc1", ef=200)
        assert got is True

    @py_test_mark_asyncio
    async def test_add_with_quant(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        # Q8 quantization is the default, but explicit should work.
        got = await vs.add([1.0, 2.0], "doc1", quant="Q8")
        assert got is True
        info = await vs.info()
        assert info["quant-type"] == "int8"

    @py_test_mark_asyncio
    async def test_add_with_noquant(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        got = await vs.add([1.0, 2.0], "doc1", quant="NOQUANT")
        assert got is True
        info = await vs.info()
        assert info["quant-type"] == "f32"

    @py_test_mark_asyncio
    async def test_add_with_bin(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        got = await vs.add([1.0, 2.0], "doc1", quant="BIN")
        assert got is True
        info = await vs.info()
        assert info["quant-type"] == "bin"

    @py_test_mark_asyncio
    async def test_add_invalid_quant_raises(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        with pytest.raises(ValueError, match="quant"):
            await vs.add([1.0, 2.0], "doc1", quant="INVALID")

    @py_test_mark_asyncio
    async def test_add_reduce_dimensionality(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        # 4-dim input projected down to 2-dim storage.
        got = await vs.add([1.0, 2.0, 3.0, 4.0], "doc1", reduce_to_dim=2)
        assert got is True
        assert await vs.dim() == 2
        info = await vs.info()
        assert info["projection-input-dim"] == 4

    @py_test_mark_asyncio
    async def test_remove_existing(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        got = await vs.remove("doc1")
        assert got is True
        assert await vs.card() == 0

    @py_test_mark_asyncio
    async def test_remove_missing(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        got = await vs.remove("nonexistent")
        assert got is False


class TestSimilar:
    @py_test_mark_asyncio
    async def test_similar_basic(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        await vs.add([4.0, 5.0, 6.0], "doc2")
        results = await vs.similar([1.0, 2.0, 3.0])
        assert "doc1" in results
        assert "doc2" in results

    @py_test_mark_asyncio
    async def test_similar_with_scores(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        await vs.add([4.0, 5.0, 6.0], "doc2")
        results = await vs.similar([1.0, 2.0, 3.0], with_scores=True)
        assert isinstance(results, list)
        # Result shape: list of (name, score) tuples.
        names = [r[0] for r in results]
        scores = [r[1] for r in results]
        assert "doc1" in names
        assert all(0.0 <= s <= 1.0 + 1e-6 for s in scores)
        # The exact-match vector should have score ~1.0.
        idx = names.index("doc1")
        assert abs(scores[idx] - 1.0) < 1e-6

    @py_test_mark_asyncio
    async def test_similar_with_count(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        for i in range(5):
            await vs.add([float(i), float(i + 1), float(i + 2)], f"doc{i}")
        results = await vs.similar([0.0, 1.0, 2.0], count=2)
        assert len(results) == 2

    @py_test_mark_asyncio
    async def test_similar_with_ef(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        results = await vs.similar([1.0, 2.0], ef=50, with_scores=True)
        assert results[0][0] == "doc1"

    @py_test_mark_asyncio
    async def test_similar_with_filter(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        await vs.add([1.0, 2.0], "doc2")
        await vs.set_attribute("doc1", {"color": "red"})
        # FILTER matches doc1.
        results = await vs.similar(
            [1.0, 2.0], filter_expr='.color == "red"'
        )
        assert results == ["doc1"]

    @py_test_mark_asyncio
    async def test_similar_with_filter_excludes(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        await vs.add([1.0, 2.0], "doc2")
        await vs.set_attribute("doc1", {"color": "red"})
        # FILTER that doesn't match anything.
        results = await vs.similar(
            [1.0, 2.0], filter_expr='.color == "blue"'
        )
        assert results == []

    @py_test_mark_asyncio
    async def test_similar_with_attributes(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        await vs.set_attribute("doc1", {"color": "red"})
        results = await vs.similar([1.0, 2.0], with_attributes=True)
        assert isinstance(results, list)
        name, attrs = results[0]
        assert name == "doc1"
        assert attrs == {"color": "red"}

    @py_test_mark_asyncio
    async def test_similar_with_scores_and_attributes(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        # NOQUANT so the self-similarity score is exactly 1.0.
        await vs.add([1.0, 2.0], "doc1", quant="NOQUANT")
        await vs.set_attribute("doc1", {"color": "red"})
        results = await vs.similar(
            [1.0, 2.0], with_scores=True, with_attributes=True
        )
        name, score, attrs = results[0]
        assert name == "doc1"
        assert abs(score - 1.0) < 1e-6
        assert attrs == {"color": "red"}

    @py_test_mark_asyncio
    async def test_similar_by_query_vector(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        await vs.add([1.0, 2.0, 3.0], "doc2")
        # Query with the same vector.
        results = await vs.similar([1.0, 2.0, 3.0])
        assert "doc1" in results
        assert "doc2" in results

    @py_test_mark_asyncio
    async def test_similar_with_epsilon(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        results = await vs.similar([1.0, 2.0], epsilon=0.5)
        assert results == ["doc1"]

    @py_test_mark_asyncio
    async def test_similar_requires_vector(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        # vector is a required positional arg now.
        with pytest.raises(TypeError):
            await vs.similar()  # type: ignore[call-arg]


class TestAttributes:
    @py_test_mark_asyncio
    async def test_set_attribute_returns_true(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        got = await vs.set_attribute("doc1", {"color": "red"})
        assert got is True

    @py_test_mark_asyncio
    async def test_set_attribute_missing_element(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        got = await vs.set_attribute("nonexistent", {"color": "red"})
        assert got is False

    @py_test_mark_asyncio
    async def test_get_attribute_returns_dict(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        await vs.set_attribute("doc1", {"color": "red", "year": 2024})
        attrs = await vs.get_attribute("doc1")
        assert attrs == {"color": "red", "year": 2024}

    @py_test_mark_asyncio
    async def test_get_attribute_missing(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        attrs = await vs.get_attribute("nonexistent")
        assert attrs is None

    @py_test_mark_asyncio
    async def test_get_attribute_unset(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        attrs = await vs.get_attribute("doc1")
        assert attrs is None


class TestIntrospection:
    @py_test_mark_asyncio
    async def test_card(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        assert await vs.card() == 0
        await vs.add([1.0, 2.0], "doc1")
        assert await vs.card() == 1
        await vs.add([1.0, 2.0], "doc2")
        assert await vs.card() == 2

    @py_test_mark_asyncio
    async def test_dim(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        assert await vs.dim() == 3

    @py_test_mark_asyncio
    async def test_info_keys(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        info = await vs.info()
        assert "quant-type" in info
        assert "vector-dim" in info
        assert info["vector-dim"] == 2
        assert "size" in info
        assert info["size"] == 1

    @py_test_mark_asyncio
    async def test_embedding(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1", quant="NOQUANT")
        emb = await vs.embedding("doc1")
        assert len(emb) == 2
        # With NOQUANT, stored values are close to original.
        assert abs(emb[0] - 1.0) < 0.1
        assert abs(emb[1] - 2.0) < 0.1

    @py_test_mark_asyncio
    async def test_links(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        # Single element: no neighbours yet.
        links = await vs.links("doc1")
        assert isinstance(links, list)

    @py_test_mark_asyncio
    async def test_random_member_single(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        # No count: returns bare string.
        name = await vs.random_member()
        assert name == "doc1"

    @py_test_mark_asyncio
    async def test_random_member_multiple(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        await vs.add([1.0, 2.0], "doc2")
        names = await vs.random_member(count=2)
        assert sorted(names) == ["doc1", "doc2"]

    @py_test_mark_asyncio
    async def test_random_member_explicit_one(self, vs, db):
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        await vs.add([1.0, 2.0], "doc1")
        # count=1 still returns a list.
        names = await vs.random_member(count=1)
        assert names == ["doc1"]


class TestEndToEnd:
    @py_test_mark_asyncio
    async def test_full_workflow(self, vs, db):
        """A complete add → query → attribute → remove cycle."""
        if not await _has_command(db, "vadd")():
            pytest.skip("VADD requires Redis 8.8+")
        # Add several vectors.
        await vs.add([1.0, 0.0], "cat", quant="NOQUANT")
        await vs.add([0.0, 1.0], "dog", quant="NOQUANT")
        await vs.add([0.95, 0.05], "tiger", quant="NOQUANT")
        # Tag with attributes.
        await vs.set_attribute("cat", {"kind": "feline"})
        await vs.set_attribute("dog", {"kind": "canine"})
        await vs.set_attribute("tiger", {"kind": "feline"})
        # Query nearest neighbours of "cat".
        results = await vs.similar([1.0, 0.0], with_scores=True)
        names = [r[0] for r in results]
        assert "cat" in names
        # Tiger should be closer to cat than dog is.
        cat_idx = names.index("cat")
        tiger_idx = names.index("tiger")
        dog_idx = names.index("dog")
        scores = [r[1] for r in results]
        assert scores[tiger_idx] > scores[dog_idx]
        assert abs(scores[cat_idx] - 1.0) < 1e-6
        # Filter by attribute.
        felines = await vs.similar(
            [1.0, 0.0], filter_expr='.kind == "feline"'
        )
        assert set(felines) == {"cat", "tiger"}
        # Remove tiger and verify card drops.
        assert await vs.remove("tiger") is True
        assert await vs.card() == 2
