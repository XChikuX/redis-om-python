# PLAN.md — RedisVL Integration

**Status:** Approved for implementation
**Scope:** Port the upstream RedisVL escape-hatch integration (redis/redis-om-python PR #791) into this fork, with fixes for the defects found during review.
**Last verified against upstream:** 2026-08-13 (PR #791 commit `31ded2e`; redisvl `redis-vl-python@main`; redis-py `master`)

---

## 1. Executive summary

Upstream PR #791 gives Redis OM an **escape hatch** into RedisVL: convert an OM model into a RedisVL `IndexSchema` (`to_redisvl_schema()`) and get a ready-to-use `AsyncSearchIndex`/`SearchIndex` wired to the model's `Meta.database` (`get_redisvl_index()`). It deliberately does **not** reimplement advanced search in OM — `FT.HYBRID`, aggregations, SVS-VAMANA, and vector policies stay in redisvl (per upstream RFC #790).

This fork already contains the *prerequisite plumbing* (`VectorFieldOptions`, `Field(vector_options=...)`, `VECTOR` schema rendering, `KNNExpression`, index-implying `should_index_field()`). What is missing is:

1. The conversion module itself (`aredis_om/redisvl.py`).
2. `HashModel` vector byte storage (pack `list[float]` → float32 bytes on save; unpack on load).
3. A `HashModel` `__init_subclass__` exemption for vector list fields.
4. The `redisvl` dependency (optional extra).
5. Tests + docs.

We port the upstream design, then fix **three defects found during review** (see §5): the `_fts` field-name mismatch, cluster routing for hybrid search, and the stale docs API.

---

## 2. Goals and non-goals

### Goals

- `to_redisvl_schema(Model)` produces a RedisVL `IndexSchema` that is **faithful to the index OM's own `Migrator` creates** (dual TAG+TEXT fields, `_fts` aliases included).
- `get_redisvl_index(Model)` returns an async/sync RedisVL index reusing `Meta.database` — no user-managed connections.
- Users can run redisvl `VectorQuery`, `RangeQuery`, `HybridQuery` (Redis 8.4+), and `AggregateHybridQuery` (Redis 7.4.x) against OM-defined models.
- `HashModel` vector fields round-trip through `save()`/`get()` with byte-exact float32 packing.
- Sync parity via `make sync` (the `redis_om/` mirror).
- All existing tests keep passing; coverage baseline (≥88%) maintained.

### Non-goals

- **Not** reimplementing `FT.HYBRID`, `FT.AGGREGATE`, SVS-VAMANA, or vector policies in OM (delegation per RFC #790).
- **Not** making redisvl a hard dependency (fork docs promise a soft/optional extra).
- **Not** replacing OM's query engine or `Migrator` — the integration is additive.
- **Not** adding `algorithm="svs-vamana"` to `VectorFieldOptions` in this pass (redisvl-side; tracked separately).
- No changes to `EmbeddedJsonModel` semantics.

---

## 3. Background: what upstream PR #791 shipped (verified)

Merged 2026-01-23 (7 commits → `31ded2e`). Reference implementation read in full:

| Component | Upstream location | Behavior |
| --- | --- | --- |
| `_get_field_type()` | `aredis_om/redisvl.py` | Maps an OM `FieldInfo` → RedisVL field dict: vector → `{"type": "vector", "attrs": {dims, distance_metric, algorithm, datatype, ...}}`; numeric → `{"type": "numeric", "attrs": {"sortable"}}`; bool/str/`List[str]` → `tag`; `full_text_search` str → `text` |
| `to_redisvl_schema()` | same | Validates `index=True` (else `ValueError("... is not indexed ...")`), picks `storage_type` json/hash, uses `Meta.index_name` + `make_key("")` prefix, returns `IndexSchema.from_dict(...)` |
| `get_redisvl_index()` | same | `to_redisvl_schema()` + `model_cls.db()` → `AsyncSearchIndex` (default) or `SearchIndex` |
| `VectorFieldOptions` | `aredis_om/model/model.py` | Dataclass + `ALGORITHM` (FLAT/HNSW), `TYPE` (FLOAT32/FLOAT64), `DISTANCE_METRIC` (L2/IP/COSINE); `.flat()`/`.hnsw()` constructors; `.schema` property rendering the `VECTOR ...` FT.CREATE fragment |
| HashModel vector storage | `convert_vector_to_bytes()` / `convert_bytes_to_vector()` | `struct.pack("<Nf")` little-endian float32 on save; unpack on load (`latin-1` re-encode when `decode_responses=True`) |
| HashModel list exemption | `HashModel.__init_subclass__` | List fields with `vector_options` bypass the "cannot index list" error |
| Dependency | `pyproject.toml` | Optional `[redisvl]` extra first, then **required** in commit `a97cc01` |
| Tests | `tests/test_redisvl_integration.py` | 5 tests: schema conversion (json/hash), non-indexed raises, async/sync index retrieval; skip when no RedisJSON |

Upstream defects (verified by reading source — see §5 for our fixes):

- **D1:** `to_redisvl_schema` emits the full-text field under its plain name (`body`), but OM's `FT.CREATE` renders `body_fts` TEXT (plus `body` TAG). Mismatch breaks `HybridQuery(text_field_name=...)` and dual-engine compat.
- **D2:** redisvl has `cluster_search`/`async_cluster_search`/`cluster_create_index` (all `target_nodes=[default_node]`) but **no cluster helper for `FT.HYBRID`**; redis-py's `hybrid_search()` takes no `target_nodes` kwarg. Hybrid on `RedisCluster` relies on fragile default-node routing.
- **D3:** Our docs (`docs/redisvl.mdx`, `docs/pending_features.mdx`) use a hybrid API that never existed (`text_field=`, `vector_field=`) plus a pre-0.13 kwarg (`alpha=`).

---

## 4. Current state of this fork (gap analysis)

### Already present — no work needed

| Piece | Location |
| --- | --- |
| `VectorFieldOptions` (FLAT/HNSW, `.flat()`/`.hnsw()`, `.schema`) | `aredis_om/model/model.py` ~L3190 |
| `Field(vector_options=...)` + `FieldInfo.vector_options` + `REDIS_OM_FIELD_DEFAULTS` | `aredis_om/model/model.py` L2876–2934, L3305+ |
| `should_index_field()` treats `vector_options` as index-implying | L3045+ |
| `VECTOR` schema rendering (Hash `schema_for_type` L4545; Json L5072) | `aredis_om/model/model.py` |
| Json `is_vector` detection (`has_numeric_inner_type`) + `RedisModelError` for non-containers | L4958+ |
| `KNNExpression` (+ score-field collision handling in `ModelMeta`) | L507+ |
| Exports: `VectorFieldOptions` in `aredis_om/__init__.py`, `aredis_om/model/__init__.py` | — |
| `_fts` alias rendering for full-text-search fields | Hash L4557; Json L5100, L5122 |
| Cluster plumbing: `get_redis_connection` (`cluster=true` → `RedisCluster`), `_strip_cluster_param` | `aredis_om/connections.py` |
| `ConversionPlan` system (`planned_save_conversions`/`planned_load_conversions`, `_FieldPlan` kinds) | `aredis_om/model/model.py` L1030–1450 |

### Missing — the actual work

| # | Piece | Upstream basis | Notes for our port |
| --- | --- | --- | --- |
| M1 | `aredis_om/redisvl.py` (`_get_field_type`, `to_redisvl_schema`, `get_redisvl_index`) | `aredis_om/redisvl.py` | Add `_fts` fix (D1) + cluster-hybrid helper (D2) + lazy redisvl import |
| M2 | HashModel vector byte storage | `convert_vector_to_bytes`/`convert_bytes_to_vector` | Integrate into the `ConversionPlan` system (not standalone legacy converters — see §6.2) |
| M3 | `HashModel.__init_subclass__` vector-list exemption | upstream `model.py` | Fork's L4125 currently rejects *all* list fields |
| M4 | `redisvl` optional dependency | upstream `pyproject.toml` | `[project.optional-dependencies] redisvl = [...]`; bump `uv.lock` |
| M5 | Sync-mirror mapping for the new module | upstream commit `456f316` | `make_sync.py` `POST_SYNC_FIXES` entry (surgical, not global) |
| M6 | Tests | upstream `tests/test_redisvl_integration.py` | Plus `_fts`-fidelity, hash round-trip, hybrid-skip guards, cluster test |
| M7 | Docs | — | Fix D3 in `docs/redisvl.mdx` + `docs/pending_features.mdx`; README/index links |

---

## 5. Design decisions (with rationale)

### D1-fix: faithful `_fts` field mapping (deviation from upstream — deliberate)

OM's schema for `body: str = Field(full_text_search=True)` is **two** fields:
- `$.body AS body TAG SEPARATOR |` (exact match)
- `$.body AS body_fts TEXT` (BM25/full-text)

Upstream's converter emits **one** field named `body` of type `text`. Consequences of the upstream behavior:
- `HybridQuery(text_field_name="body")` silently matches nothing against an OM-migrated index.
- If redisvl creates the index from its schema instead, the TAG alias is lost and OM's own `Model.find(M.body == ...)` engine breaks.

**Decision:** `_get_field_type()` emits the *same dual fields OM creates*: for `full_text_search=True` strings → a `tag` entry named `body` **and** a `text` entry named `body_fts`. For HashModel the same rule applies (`body` TAG + `body_fts` TEXT). This makes a redisvl-created index byte-compatible with an OM-created one — both engines work on either index.

### D2-fix: cluster-aware hybrid search helper

We add `_hybrid_search_*` cluster handling in our module's thin wrapper where we control the client:

- If `redis_client` is `redis.RedisCluster`/`AsyncRedisCluster`: route `FT.HYBRID` via `execute_command("FT.HYBRID", ..., target_nodes=[default_node])` — mirroring redisvl's own `cluster_search` pattern — or document clearly that the index name's hash slot must resolve to one node.
- Concretely, `get_redisvl_index()` will store the client; a small `_cluster_aware_hybrid()` helper in `aredis_om/redisvl.py` wraps `index.query(HybridQuery(...))` for cluster clients. (If redisvl upstream adds `async_cluster_hybrid_search` first, we drop the local helper and depend on it.)

Fallback for now: the plain `index.query()` path works on cluster because `FT.HYBRID <index>` routes by index name to the default node; the helper exists to make this explicit and raise a friendly error otherwise.

### D3-fix: current redisvl `HybridQuery` API in docs

Verified against `redisvl/query/hybrid.py@main`:
- `HybridQuery(text=..., text_field_name=..., vector=..., vector_field_name=..., combination_method="LINEAR"|"RRF"|None, linear_alpha=0.3, rrf_window=20, rrf_constant=60, yield_text_score_as=..., yield_vsim_score_as=..., yield_combined_score_as=..., num_results=10, return_fields=..., stopwords="english", text_weights=..., filter_expression=..., dtype="float32", knn_ef_runtime=10, range_radius=..., range_epsilon=0.01)`.
- Requires **Redis 8.4.0+** and **redis-py ≥ 7.1.0** (`_IMPORT_ERROR_MESSAGE = "Hybrid queries require Redis >= 8.4.0 and redis-py>=7.1.0"`).
- `text_field=` / `vector_field=` have never existed in any redisvl release (verified against 0.5.1 and latest). `alpha=` only exists pre-0.13.
- Docs must show `text_field_name="body_fts"` (the aliased TEXT field, per D1).

### Dependency policy: optional extra, lazy import

- `[project.optional-dependencies] redisvl = ["redisvl>=0.13.0,<1.0"]` (distribution name to confirm: official docs say `pip install redisvl`; fork docs link `pypi.org/project/redis-vl` — resolve during implementation).
- `aredis_om/redisvl.py` must **not** import redisvl at module import time. Both public functions do the import inside the function body and raise a helpful `ImportError("pip install 'redis-om[redisvl]'")` if missing.
- Rationale: fork docs (`docs/redisvl.mdx` §"What changes for existing users") promise soft/optional; upstream later made it required, which we deliberately do not copy.

### Vector storage: integrate with `ConversionPlan`, not legacy converters

Upstream uses standalone recursive converters. This fork's hot path is the field-aware `ConversionPlan` system (single pass, precomputed per-field metadata, 1.5–4.8x faster). We extend the plan instead of adding a third recursive pass:

- New `_FieldPlan` kind for vector fields (e.g. `_KIND_VECTOR`), populated in `build_conversion_plan()` when `field_info.vector_options is not None`.
- Save: `list[float]` → `struct.pack(f"<{len}f", *values)` (little-endian float32, matching the `VECTOR ... TYPE FLOAT32` schema RediSearch expects). `FLOAT64` → `"<Nd"`.
- Load: bytes → list; when `decode_responses=True` Redis returns str, re-encode `latin-1` before unpacking (upstream's approach).
- The legacy recursive `convert_vector_to_bytes`/`convert_bytes_to_vector` are added **only** for the `get_value()`/test API parity path (matching the fork's existing "legacy converters remain" convention) — or skipped if no caller needs them.

### Storage type / index detection details

- `storage_type`: `"json" if issubclass(model_cls, JsonModel) else "hash"` (upstream logic).
- Indexed check: use the fork's canonical sources — `model_config.get("index") is True` **or** `getattr(model_cls._meta, "index_enabled", False)` — because this fork stores class-level `index=True` in `_meta.index_enabled` (see CLAUDE.md §6). Raise `ValueError(f"Model {name} is not indexed. Use 'class MyModel(JsonModel, index=True):' to enable indexing.")`.
- `index_name`: `model_cls.Meta.index_name` (lazily resolved by the fork's `ModelMeta`; includes global prefix).
- `key_prefix`: `model_cls.make_key("")` (upstream logic; yields `"{global}:{model}:"`).

### `get_redisvl_index` signature

```python
def get_redisvl_index(model_cls, async_client: bool = True):
    schema = to_redisvl_schema(model_cls)
    redis_client = model_cls.db()
    if async_client:
        from redisvl.index import AsyncSearchIndex
        return AsyncSearchIndex(schema=schema, redis_client=redis_client)
    from redisvl.index import SearchIndex
    return SearchIndex(schema=schema, redis_client=redis_client)
```

No `await index.create()` inside — callers control lifecycle (matches upstream; docs show `await index.create()` explicitly).

---

## 6. Implementation phases

Conventions: **all manual edits in `aredis_om/` and `tests/` only**; `redis_om/` + `tests_sync/` are generated via `make sync`; `uv.lock` bump via `uv lock`; run `read_file` before any `edit_file`.

### Phase 0 — Dependency (M4)

1. `pyproject.toml`: add
   ```toml
   [project.optional-dependencies]
   redisvl = ["redisvl>=0.13.0,<1.0"]
   ```
   (also add `redisvl` to the `dev` extra so CI/tests exercise the real library).
2. `uv lock` → commit updated `uv.lock`.

### Phase 1 — HashModel vector storage (M2, M3)

Files: `aredis_om/model/model.py`.

1. Extend `_FieldPlan` with a vector kind + `vector_dtype` (float32/float64 from `VectorFieldOptions.TYPE`).
2. In `build_conversion_plan()`: when `field_info.vector_options is not None`, mark the field as vector (container `list[float]`/`list` types).
3. Save side (`planned_save_conversions`): pack list → bytes via `struct.pack`.
4. Load side (`planned_load_conversions`): unpack bytes (re-encode `latin-1` for str) → `list[float]`; tolerate `None`/empty.
5. `HashModel.__init_subclass__` (currently rejecting all lists at ~L4125): exempt list fields whose `FieldInfo.vector_options is not None` (mirror upstream's `_has_vector_options` check over both `cls.__dict__` and `model_fields`).
6. Confirm `HashModel.save()` and `get()` call the planned converters (they do per CLAUDE.md pipelines); verify no `jsonable_encoder` pass mangles the packed bytes (bytes are already JSON-safe for hash storage; upstream base64-encodes — decide: hash storage of packed float32 bytes directly, matching upstream's `convert_vector_to_bytes` **before** base64 pass. Follow upstream ordering: vector packing happens before base64, and base64 is applied to other bytes fields only.)

### Phase 2 — The conversion module (M1, D1, D2)

New file: `aredis_om/redisvl.py`.

Structure (upstream layout, with our fixes):

1. `_get_field_type(field_name, field_type, field_info, is_json) -> Optional[dict]` — port upstream logic; **changes**:
   - `full_text_search=True` str → return the dual mapping (`tag` named `body` + `text` named `body_fts`) — see §5 D1-fix.
   - `should_index_field(field_info)` gate first (upstream does).
   - Vector attrs: `dims`, `distance_metric`, `algorithm`, `datatype` (lowercased enum names); FLAT adds `block_size`; HNSW adds `m`, `ef_construction`, `ef_runtime`, `epsilon`; common `initial_cap`. (Same as upstream.)
2. `to_redisvl_schema(model_cls) -> IndexSchema` — lazy import redisvl inside; indexed check via fork's canonical sources; iterate `model_cls.model_fields`, unwrap metadata-wrapped `FieldInfo` (Pydantic 2.12+ `Annotated` case) exactly like upstream; build `{"index": {"name", "prefix", "storage_type"}, "fields": [...]}`; `IndexSchema.from_dict(...)`.
3. `get_redisvl_index(model_cls, async_client=True)` — as §5 signature; lazy import; no create.
4. `_cluster_aware_hybrid(...)` helper (D2) — if the client is a cluster client, route `FT.HYBRID` with `target_nodes=[default_node]`; else delegate to `index.query(...)`. Keep it minimal; prefer dropping it if redisvl upstream ships the helper before we land.
5. Module docstring with usage examples (async + sync + hybrid).

### Phase 3 — Exports & sync mirror (M5)

1. Export from `aredis_om/redisvl.py` only (module-level API, like upstream). Do **not** re-export at `aredis_om/__init__.py` top level (keeps redisvl import lazy; upstream does the same).
2. `make_sync.py`: add a `POST_SYNC_FIXES` entry for `redis_om/redisvl.py` (surgical — a global `ADDITIONAL_REPLACEMENTS` entry would corrupt the dual import):
   ```python
   "redis_om/redisvl.py": {
       "from redisvl.index import AsyncSearchIndex": "from redisvl.index import SearchIndex",
       "AsyncSearchIndex(": "SearchIndex(",
   }
   ```
3. Run `make sync`; inspect generated `redis_om/redisvl.py` for correctness (no double-import collision).

### Phase 4 — Tests (M6)

New file: `tests/test_redisvl_integration.py` (async source; sync mirror auto-generated).

Port upstream's 5 tests, then add fork-specific coverage:

| Test | Asserts |
| --- | --- |
| JsonModel → `IndexSchema` | name, `storage_type == "json"`, fields incl. `embedding` vector attrs |
| HashModel → schema | `storage_type == "hash"`, fields present |
| Non-indexed model | `ValueError("is not indexed")` |
| `get_redisvl_index(async_client=True/False)` | `AsyncSearchIndex` / `SearchIndex`, name matches |
| **`_fts` fidelity (D1)** | FTS field produces `body_fts` TEXT **and** `body` TAG entries; equals OM `redisearch_schema()` field-name set |
| **HashModel vector round-trip** | `save()` → raw `HGET` shows packed float32 bytes; `get()` → identical `list[float]`; `FLOAT64` variant |
| **Hybrid query construction** | `pytest.importorskip("redisvl")` + `skipif(redis_version < "8.4")`; `HybridQuery(text_field_name="body_fts", ...)` args render `FT.HYBRID` with correct field names |
| **Lazy import** | importing `aredis_om.redisvl` without redisvl installed doesn't raise; calling helpers raises helpful `ImportError` |
| Cluster | `get_redisvl_index` against `RedisCluster` client returns index; `FT.HYBRID` routes to default node (or helper raises friendly error) |

Use existing fixtures/patterns (`py_test_mark_asyncio`, `key_prefix`, `redis`, `Migrator(conn=redis).run()`); skip module when `not has_redis_json()` (upstream pattern). Redis version detection: reuse the fork's version helpers from `aredis_om/checks.py` or `protocol_version()`.

### Phase 5 — Docs (M7, D3)

1. `docs/redisvl.mdx`:
   - Fix `Field(vector_field=...)` → `Field(vector_options=...)`; drop SVS-VAMANA `algorithm="svs-vamana"` claim (upstream enum has FLAT/HNSW only).
   - Replace hybrid example with current API: `HybridQuery(text=..., text_field_name="body_fts", vector=..., vector_field_name="embedding", combination_method="LINEAR", linear_alpha=0.5)`; note Redis 8.4+ / redis-py ≥ 7.1 requirement; add `AggregateHybridQuery` note for 7.4.x.
   - Update "What's in the integration" table: mark `to_redisvl_schema`/`get_redisvl_index` as **shipped**, keep `FT.HYBRID`/`FT.AGGREGATE` as redisvl-delegated.
   - Dependency note: optional extra `pip install 'redis-om[redisvl]'`.
2. `docs/pending_features.mdx`: same API fixes; adjust status lines to reference the shipped escape hatch.
3. `README.md` (L237) + `docs/index.mdx` (L63–64): link labels — README's "Pending Features (RedisVL)" pointing at `redisvl.mdx` should be "RedisVL Integration"; pending list stays at `pending_features.mdx`.
4. `CLAUDE.md`: add a short §"RedisVL Integration" noting the module, lazy-import rule, and `_fts` fidelity invariant.

### Phase 6 — Validation & landing

1. `uv sync --extra dev`
2. `make sync` (idempotent; diff clean)
3. `make lint` (ruff check + format + mypy)
4. `make test` (Compose up → async+sync+coverage → Compose down); confirm ≥88% coverage.
5. `make test_cluster` (6-node cluster: schema creation + `get_redisvl_index` + hybrid-if-available smoke).
6. Manual smoke on local `redis:8-alpine` (6380): create model → `Migrator().run()` → `get_redisvl_index()` → `VectorQuery`; if server ≥8.4, `HybridQuery`.
7. `make benchmark` — confirm no conversion-plan regression (vector packing is in the hot path now).

---

## 7. Cluster & hybrid search reference (verified facts for the implementation)

### redisvl cluster mechanics (`redisvl/redis/utils.py`, `redisvl/index/index.py`)

- Index commands target **one node**: `cluster_create_index`, `cluster_search`, `async_cluster_search` all use `target_nodes=[default_node]` — the index name hashes to a single slot, so no fan-out is possible/correct.
- Multi-key ops: `_keys_share_hash_tag()` guard raises `ValueError("All keys must share a hash tag when using Redis Cluster.")`; `_delete_batch`/`_unlink_batch`/`_apply_update_batch` loop **per-key** on cluster to avoid `CROSSSLOT`.
- `FT.DROPINDEX` + `drop=True` on cluster: redisvl clears keys first, then drops without `DD`.
- Known gaps: `listall()` on async cluster reads one random node only; `batch_search()` pipelines run on the default node.
- **`FT.HYBRID` has no cluster helper** in redisvl (verified); redis-py `hybrid_search()` has no `target_nodes` kwarg. → our D2 helper.

### Hybrid search facts

- `FT.HYBRID` = Redis **8.4.0+**, redis-py **≥ 7.1.0**, redisvl **≥ 0.13**.
- `AggregateHybridQuery` (redisvl ≥ 0.5) = aggregation-based fusion for Redis 7.4.x / search 2.10.5+.
- Combination: `LINEAR` (ALPHA+BETA, server normalizes; redisvl computes `BETA = 1 - ALPHA`) or `RRF` (WINDOW/CONSTANT); server default when unset = RRF.
- Result: redisvl `_hybrid_search` → `list[dict]` wrapped in `SearchResults` (list subclass with `dropped_count`/`complete` for the Redis 8.8+ background-expiry race; plain list ops return plain lists).
- Decoding: HYBRID uses `NEVER_DECODE`; per-field opt-in via `HybridPostProcessingConfig.load("field", decode_field=True)` — vector fields default to bytes.
- Cursors: `HybridCursorQuery(count, max_idle)` → `HybridCursorResult(search_cursor_id, vsim_cursor_id)` — a **dual** cursor (text + vector sides), no single-API pagination wrapper yet.
- `vector_param_name` defaults to `"vector"` — distinct names needed for concurrent queries sharing a client.

---

## 8. Risks & open questions

| Risk | Mitigation |
| --- | --- |
| Pydantic 2.12+ strips custom `FieldInfo` attrs on `Annotated` fields | Follow upstream's metadata-unwrapping pattern + fork's existing `original_field_infos` capture in `ModelMeta`; test with `Annotated`-typed vector fields |
| `make sync` corrupts dual `SearchIndex` import | Surgical `POST_SYNC_FIXES` (Phase 3); inspect generated file; add CI check |
| Vector packing regresses conversion hot path | Phase 6 benchmark; plan-based dispatch is O(1) per field |
| redisvl API drift (docs pin 0.13+, latest may differ) | Pin `redisvl>=0.13,<1.0`; docs examples verified against 0.13+; note `HybridQuery` is marked experimental in redis-py |
| PyPI distribution name ambiguity (`redisvl` vs `redis-vl`) | Resolve at Phase 0 via `pip index` / docs; use the name `pip install redisvl` works with |
| Redis < 8.4 CI (hybrid tests) | `skipif` on server version; hybrid tests separate from core integration tests |
| `Meta.database` on cluster → `RedisCluster` accepted by redisvl? | Verified: redisvl `validate_sync_redis` accepts `(Redis, RedisCluster)`; `AsyncSearchIndex` accepts `(AsyncRedis, AsyncRedisCluster)` |
| Index-name vs hash-slot conflicts across models on cluster | Document that each OM model's `index_name` maps to one slot; no code change |
| fork docs promise vs upstream reality (optional vs required dep) | Keep optional extra (decision §5); note divergence from upstream commit `a97cc01` in CLAUDE.md |

---

## 9. Definition of done

- [ ] `aredis_om/redisvl.py` exists with `to_redisvl_schema` + `get_redisvl_index`; lazy imports; `_fts`-faithful schema (D1).
- [ ] `HashModel` vector fields round-trip byte-exactly through `save()`/`get()` (M2); list exemption in `__init_subclass__` (M3).
- [ ] `pyproject.toml` + `uv.lock` include the optional `redisvl` extra (M4).
- [ ] `make sync` produces a correct `redis_om/redisvl.py` mirror (M5).
- [ ] `tests/test_redisvl_integration.py` (+ sync mirror) passes on Redis 8; hybrid tests skip cleanly < 8.4.
- [ ] `docs/redisvl.mdx`, `docs/pending_features.mdx`, `README.md`, `docs/index.mdx` use the current redisvl API (D3).
- [ ] `make lint`, `make test`, `make test_cluster` green; coverage ≥ 88%.
- [ ] `make benchmark` shows no regression from the conversion-plan changes.

---

## 10. References (all verified 2026-08-13)

- [Upstream PR #791](https://github.com/redis/redis-om-python/pull/791) — reference integration (7 commits, merged `31ded2e`)
- [Upstream RFC #790](https://github.com/redis/redis-om-python/issues/790) — delegation rationale
- [Upstream issue #258](https://github.com/redis/redis-om-python/issues/258) — FT.AGGREGATE / hybrid tracking
- [redisvl (redis-vl-python)](https://github.com/redis/redis-vl-python) — `redisvl/index/index.py`, `redisvl/redis/utils.py`, `redisvl/redis/connection.py`, `redisvl/query/hybrid.py`
- [redis-py FT.HYBRID impl](https://github.com/redis/redis-py/blob/master/redis/commands/search/commands.py) — `hybrid_search()`, `HYBRID_CMD`, parsers
- [FT.HYBRID command docs](https://redis.io/docs/latest/commands/ft.hybrid/) — Redis 8.4+
- [redisvl hybrid docs](https://docs.redisvl.com/en/latest/user_guide/11_advanced_queries.html)
- This repo: `CLAUDE.md` (conventions, pipelines, ConversionPlan), `make_sync.py` (POST_SYNC_FIXES), `docs/redisvl.mdx`, `docs/pending_features.mdx`
