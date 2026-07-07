# Redis OM Python — Agent Guide

Object mapping library for Redis built on Pydantic v2, utilizing Redis Search and Redis JSON.

* **Published Package:** `pyredis-om`
* **Async API (Source):** `aredis_om`
* **Sync API (Generated):** `redis_om`

## 1. Project Overview & Conventions

### External Research

* **Docs:** Use Context7 if available.
* **Web Search:** Limit to one `brave-web-search` call at a time to avoid rate limits (fallback to standard fetch).

### Repository Facts

* **Version / Python:** `pyproject.toml` is authoritative. Python `>=3.10,<4.0` (CI tests 3.10–3.14).
* **Source of Truth:** All manual edits belong in `aredis_om/` or `tests/`.
* **Generated Artifacts:** `redis_om/` and `tests_sync/` (generated via `make sync`; git-ignored).
* **Tooling:** `uv` manages dependencies/workflows. `uv.lock` is committed (commit `4dcd256`) to enable CI caching; treat it as the source of truth for reproducible installs and bump it via `uv lock` when `pyproject.toml` changes.
* **Test Dependencies:** `pytest`, `pytest-asyncio`, `pytest-xdist`, `pytest-cov`, `pytest-codspeed`, `mypy`, `ruff`, `unasync`, `ipdb`, `coverage`, `email-validator`, `tox`, `strawberry-graphql`, `twine` (via `uv sync --extra dev`).
* **Redis Targets:** * Local: `redis:8-alpine` (6380), `valkey/valkey:9-alpine` (6381) via Compose.
* CI: `redis:8-alpine` (6379).


* **RESP3 Shim:** `aredis_om/model/resp3_shim.py` normalizes RESP3 dicts from `FT.SEARCH`, `FT.AGGREGATE`, and `WITHCURSOR` to RESP2 flat-pair shapes. (Tested in `tests/test_protocol_compat.py`, `negotiation`, `resp3_shim`, `benchmark`).

### Repository Structure

```text
aredis_om/                 # Async source of truth
├── model/                 # Core models, queries, migrations, rendering, escaping, encoders
├── connections.py         # Redis(Cluster) connection factories
├── checks.py              # Command capability checks/cache
├── _compat.py             # Pydantic v2 shims
└── util.py                # Type helpers
tests/                     # Async/benchmark tests
redis_om/ & tests_sync/    # GENERATED: Do not edit manually
make_sync.py & Makefile    # Sync generation and workflow commands
docker-compose(.cluster).yml # Single-node (6380/6381) and 6-node Cluster

```

## 2. Development Workflow

1. **Edit:** Only modify `aredis_om/` and `tests/`.
2. **Generate:** Run `make sync` to rebuild sync mirrors.
3. **Local Redis:** `docker compose up -d` → `export REDIS_OM_URL="redis://localhost:6380?decode_responses=True"`.
4. **Commands:**
* Install: `uv sync --extra dev`
* Verify: `make sync`, `make lint` (runs `ruff check`, `ruff format --check`, `mypy`).
* Test: `make test` (starts Compose → async/sync + coverage → stops Compose).
* Cluster Test: `make test_cluster` (starts both Compose files → cluster tests).
* Benchmark: `make benchmark` (runs `tests/test_performance_benchmark.py --codspeed` for local validation; CI regression tracking via `.github/workflows/codspeed.yml`).
* Direct Pytest: `uv run pytest tests/test_hash_model.py -vv` (use `-k` for filtering).


5. **Stale Tools:** `tox.ini` is not used by CI (which uses `uv` + `Makefile` directly). It still declares a `uv`-based tox env for local use but is not a gating path.

## 3. Core Architecture

### Connections (`aredis_om/connections.py`)

* `get_redis_connection(kwargs)` returns `redis.Redis` or `redis.RedisCluster`.
* Defaults to `decode_responses=True`.
* `cluster=True` (or `true`) in URL triggers Cluster mode; `_strip_cluster_param()` sanitizes it for redis-py.

### Schema & Mapping (`aredis_om/model/model.py`)

| Python Type | RediSearch (`RediSearchFieldTypes`) |
| --- | --- |
| `bool`, `str`, Other | `TAG` |
| `int`, `float`, `Decimal`, `date`, `datetime` | `NUMERIC` |
| `str` (if `full_text_search=True`) | `TEXT` |
| `Coordinates` | `GEO` |
| Embedded Models | Recursively generated |

* **Generation Entry Points:** `[Hash|Json]Model.redisearch_schema()` and `.schema_for_fields()`.

### Data Pipelines

* **Save (Hash):** `model_dump()` → Date(time) to timestamp → Bytes to base64 → Dataclass to dict → `jsonable_encoder()` → Redis hash.
* **Save (Json):** `model_dump()` → Date(time) to timestamp → Bytes to base64 → Dataclass to dict → Redis JSON.
* **Load (Hash):** Empty strings to `None` (for optionals) → base64 to Bytes → Pydantic validation.
* **Load (Json):** Timestamp to Date(time) → base64 to Bytes → Pydantic validation.

### Query Path

* **Components:** Built via `Expression`, `NegatedExpression`, `ExpressionProxy`, `FindQuery`. Query fragments cache on instance.
* **Security:** `TokenEscaper` sanitizes RediSearch values. KNN params use `PARAMS` (non-wildcards wrapped before KNN syntax).
* **Pagination:** `FindQuery.iter_cursor()` uses `FT.AGGREGATE WITHCURSOR`, fetches models by PK (`__key`).
* **Tokens:** `FindQueryCursor.token(secret=...)` (URL-safe, preferred for web) restored via `.from_token()`.
* **JSON Fetch:** `JsonModel.get_value(pk, field_path, raw=False)` grabs single/nested values via `JSON.GET` without full doc loads. Supports `__`-separated paths or raw `$`-prefixed JSONPath.

### Bulk & Pipeline

* **Batched Fetch:** `[Hash|Json]Model.get_many(pks, pipeline=None)` uses `HGETALL` or `JSON.GET`.
* **Bulk Mutate:** `RedisModel.add()` supports pipelines. `delete_many()` chunks via `more_itertools.ichunked(..., 100)`.
* **Cluster Routing:** `_is_cluster_pipeline()` detects cluster pipelines (must queue without awaiting individual returns).

## 4. Embedded Model PK Behavior

| Behavior | `EmbeddedJsonModel` | `HashModel` (`Meta: embedded = True`) |
| --- | --- | --- |
| **Purpose** | Dedicated JSON sub-document type | Regular HashModel reused as embedded data |
| **`pk` in dump** | Always excluded | Preserved if explicitly set; excluded if null/proxy |
| **Stale `pk**` | Stripped before validation | Stripped via normal RedisModel query-proxy logic |

## 5. Recent Features & Coverage

* **Database:** Lazy `Meta.database` resolution, callable providers, runtime reassignment.
* **Core CRUD:** `Meta.default_ttl`, embedded JSON sorting (dotted/underscore paths), bulk `get_many()`, binary `bytes` round-tripping (base64), explicit pipeline composition.
* **Query Capabilities:** Enum numeric queries, NUMERIC `IN`/`NOT_IN`, custom `TAG` separators, embedded model query prefix isolation, KNN + OR syntax wrapping.
* **Explicit Logicals:** `Or`, `And`, `Not` now render true RediSearch syntax (`|`, space, `-`). Resolves to strings; delegates to `.query` for end-to-end `Model.find()`.
* **Cluster & Integrity:** Index health warnings (`FT.INFO`), Cluster-safe migrations, RESP3 parity (protocol-aware parsers, URL kwarg passthrough).
* **Advanced Redis Features Supported:**
* *Streams:* `RedisStream` (Redis 8.2-8.8: XADD/READ/READGROUP, XACKDEL/XDELEX, CLAIM, IDMP/IDMPAUTO, XNACK).
* *Hash TTL:* `HashModel` field TTLs (Redis 7.4+).
* *Atomic Strings:* SET IFEQ/IFNE, DELEX IFEQ, DIGEST, `msetex` (Redis 8.4+).
* *Vector Sets:* VADD/VSIM/VINFO/VCARD/VDIM/VEMB/VLINKS/VRANDMEMBER/VREM/VSETATTR/VGETATTR (Redis 8.8+).
* *Performance Tracking:* `hotkeys_snapshot` (HOTKEYS START/GET/STOP/RESET — Redis 8.6+).
* *Bitmaps:* `BitmapOps` (BITOP DIFF/DIFF1/ANDOR/ONE — Redis 8.2+).
* *Sorted Sets:* `SortedSetOps` (ZUNION/ZINTER AGGREGATE COUNT — Redis 8.8+).
* *Cluster Admin:* `ClusterAdmin` (SLOT-STATS, MIGRATION STATUS/START/STOP/ABORT/LOG — Redis 8.2+).
* *Events:* `KeyspaceEvents`, `build_flags`, `enable_keyspace_events` (Redis 2.8+).


* **TODO Cleanup:** All TODOs resolved and documented inline (Sets explicitly unsupported, Geos use `Coordinates`, bytes in TAG are base64, `*` negation rejected, Non-IN right-hands raise `QueryNotSupportedError`, `Meta.key_separator` defaults to `:`, `verify_pipeline_response` intentionally minimal).

## 6. Class-Level `index=True`

`class Foo(Model, index=True)` enables "index everything" mode on a model. All fields default to `index=True` unless explicitly overridden with `Field(index=False)`.

**Resolution order (most specific wins):**

1. `Field(index=False)` on a specific field
2. Class-level `index=True` / `index=False`
3. Model-level defaults (numeric/date/bool → index; str without `full_text_search=True` → no index; embedded → recursive)

**Key mechanics:**
- `ModelMeta.__new__` pops `index` kwarg before calling `redisearch_schema()` so `__init_subclass__` runs after schema is frozen.
- `_meta.index_enabled = bool(class_index)` applied BEFORE `redisearch_schema()` so recursive schema generation sees it.
- `_index_explicitly_set` marker persisted via `json_schema_extra` to survive Pydantic's `FieldInfo` reconstruction on subclassing.
- `_field_index_explicitly_set()` falls back to `primary_key=True` if marker lost (Pydantic strips private attrs on subclass).
- `CLASS_INDEX_WARN_THRESHOLD = 20`: emitting `UserWarning` once per process when class-indexed model produces >20 indexed fields.
- **KNN score field collision:** `class JsonModel(index=True)` auto-indexes `embeddings_score: Optional[float] = None` as NUMERIC, but KNN query-time synthesis creates a field with the same name → `Property 'embeddings_score' already exists in schema`. Fix: mark the field `Field(index=False)` + `KNNExpression.validate_score_field_not_indexed()` runtime check raises `RedisModelError` with friendly message.

## 7. Audit Findings: Bug Fixes (2026-07)

* **`checks.py`:** `has_redisearch` strictly checks `ft.search`, removing false positives on standalone RedisJSON.
* **Pagination/Loops:** Fixed `__getitem__` cache off-by-one (`>=` to `>`). Pagination offset increments by `limit` instead of `page_size`. `FindQueryCursor.__anext__` upgraded to O(1) `collections.deque`.
* **Primary Keys:** `validate_primary_key` threshold strictly `> 2` (preserves 1 user-defined PK + 1 default inherited PK). Integer PKs correctly render as NUMERIC ranges `[5 5]` to match schema.
* **Query Parsing:** `TEXT` fields now properly run through `escaper.escape()`. `TAG` separator-split queries now correctly join with implicit-AND spaces.
* **Configuration:** Empty-string `Meta` configs (`""`) intentionally treated as "use default" (preserves inherited `{pk}` pattern).
* **Schema:** `JsonModel` inner-model field types no longer mistakenly restrict to `str`. Fixed invalid `HashModel` list mapping comment.
* **Data Integrity (Round-Tripping):**
* `List[datetime]` / `List[date]` now recurse item-by-item during load instead of returning raw timestamps.
* `List[bytes]` now decode item-by-item from base64 during load.
* `bytes` querying now properly base64-encodes and token-escapes raw queries for `EQ/NE/IN/CONTAINS/etc` to match stored DB state.


* **JSON Scanning:** `JsonModel.all_pks()` falls back from `_type="ReJSON-RL"` to `_type="JSON"` for modern Redis 8 compatibility.
* **Error Handling:** `FindQuery.delete()` now logs swallowed `ResponseError`s as `WARNING` for easier Cluster slot-debugging.
* **Valkey OSS Support:** `tests/test_oss_redis_features.py` fixture removed `Migrator().run()` (calls `FT.CREATE` which requires search modules absent on plain Valkey 9). The 9 HashModel CRUD tests now pass on both Redis and Valkey without modules.

## 8. Audits (Performance & Security)

### Performance

* **Strengths:** Async-first, generated sync parity, lazy connections, query-string caching.
* **Bottleneck Risks:** `FindQuery.copy()` (rebuilds via dict; profile on large sets). Datetime/bytes conversions deeply walk nested structures.
* **Memory Risk:** `FindQuery.execute(exhaust_results=True)` paginates without max limits.
* **CI Gap:** ~~Benchmarks run but lack enforced regression thresholds.~~ Addressed — `.github/workflows/codspeed.yml` runs the benchmark suite under `pytest-codspeed` walltime mode on every push/PR with CodSpeed regression tracking.

### Security

* **Mitigations:** `TokenEscaper` sanitizes strings. Vector queries utilize `PARAMS`. Pydantic handles primary data boundaries.
* **Tooling:** CodeQL active; `ruff` + `mypy` for lint/type-checking. `uv.lock` is committed for reproducible installs.
* **State / Risks:** Global mutable state for command caches/registries (safe at import, unsafe for runtime mutation). Local Cluster compose uses unprotected host networking (dev only). Schema migration `FT.CREATE` construction remains an internal-only path.
