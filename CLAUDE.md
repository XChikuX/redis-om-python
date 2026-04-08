# Redis OM Python

Object mapping library for Redis built on Pydantic, using RediSearch and RedisJSON modules.

## Repository Structure

```
aredis_om/               # ASynchronous version
├── model/
│   ├── model.py       # Core: RedisModel, HashModel, JsonModel
│   ├── migrations/    # Migration tools (migrator.py)
│   ├── query_resolver.py
│   ├── render_tree.py
│   ├── token_escaper.py
│   └── encoders.py    # JSON encoding utilities
├── connections.py     # Redis connection management (standalone + cluster)
├── _compat.py         # Pydantic v1/v2 compatibility
├── checks.py
└── util.py

redis_om/             # Sync version (mirrors aredis_om structure)
tests/                 # Async tests
tests_sync/            # Synchronous tests
```

## Key Architecture

### Connection Management (`redis_om/connections.py`)
- `get_redis_connection(**kwargs)` returns `Union[redis.Redis, redis.RedisCluster]`
- Pass `cluster=True` for cluster mode; `url=` for URL-based connections
- Defaults: `decode_responses=True`, URL from `REDIS_OM_URL` env var

### Schema & Field Types (`redis_om/model/model.py`)
- `RediSearchFieldTypes`: TEXT, TAG, NUMERIC, GEO
- Type mapping for indexing:
  - `bool` → TAG
  - `int`, `float`, `decimal.Decimal` → NUMERIC
  - `datetime.date`, `datetime.datetime` → NUMERIC
  - `str` → TAG (+ TEXT if `full_text_search=True`)
  - `Coordinates` → GEO
  - Embedded models → recursive field processing
  - Fallback → TAG
- Schema generation: `HashModel.schema_for_type` and `JsonModel.schema_for_type`

### Data Conversion Pipeline (save/get)
- **Save order (HashModel):** `model.dict()` → `convert_datetime_to_timestamp()` → `convert_bytes_to_base64()` → `convert_dataclasses_to_dicts()` → `jsonable_encoder()` → Redis
- **Save order (JsonModel):** `model.dict()` → `convert_datetime_to_timestamp()` → `convert_bytes_to_base64()` → `convert_dataclasses_to_dicts()` → Redis
- **Get order (HashModel):** Redis → `convert_empty_strings_to_none()` → `convert_base64_to_bytes()` → `parse_obj()`
- **Get order (JsonModel):** Redis → `convert_timestamp_to_datetime()` → `convert_base64_to_bytes()` → `model_validate()`

### Bulk fetch / pipeline support
- `HashModel.get_many(pks, pipeline=None)` batches `HGETALL` calls in a pipeline
- `JsonModel.get_many(pks, pipeline=None)` batches `JSON.GET` calls in a pipeline
- Both support composing with raw Redis commands (for example `GEORADIUSBYMEMBER`) in a single explicit pipeline

## Bug Fixes Applied

### PR #657 — ExpressionProxy embedded model query prefixing
- **Issue:** OR queries on embedded models shared parent lists, causing malformed field prefixes like `@player1_player2_username` instead of correct `@player1_username` / `@player2_username`
- **Fix:** `ExpressionProxy.__init__` copies parents list; `__getattr__` uses isolated parent chains; `resolve_redisearch_query` builds field names using expression-specific parents
- **Files:** `redis_om/model/model.py`, `aredis_om/model/model.py`

### PR #783 — bytes fields base64 encoding
- **Issue:** Storing `bytes` fields with non-UTF8 data caused `UnicodeDecodeError`
- **Fix:** Added `convert_bytes_to_base64()` / `convert_base64_to_bytes()` in save/get pipeline for both HashModel and JsonModel
- **Files:** `redis_om/model/model.py`, `aredis_om/model/model.py`

### PR #787 — OR expression with KNN syntax error
- **Issue:** Combining OR expressions with KNN queries produced invalid RediSearch syntax where KNN only applied to the second OR term
- **Fix:** Always wrap non-wildcard filter in parentheses before appending KNN clause
- **Files:** `redis_om/model/model.py`, `aredis_om/model/model.py`

### PR #792 — Enum queries, IN for NUMERIC, Optional HashModel fields
- **Issue #108:** Enum values produced `@status:[Status.ACTIVE Status.ACTIVE]` instead of `@status:[2 2]`
- **Issue #499:** IN operator (`<<`) only worked for TAG fields, not NUMERIC
- **Issue #254:** HashModel stores None as `""`, causing ValidationError on retrieval of Optional fields
- **Fix:** Added `convert_numeric_value()` for Enum extraction; IN/NOT_IN handling for NUMERIC fields; `convert_empty_strings_to_none()` in HashModel.get()
- **Files:** `redis_om/model/model.py`, `aredis_om/model/model.py`

### PR #800 — Custom TAG field separator
- **Issue:** TAG separator was hardcoded to `|`, ignoring user-specified separators
- **Fix:** Added `separator` parameter to `FieldInfo` and `Field()`. All schema generation uses `getattr(field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR)`
- **Files:** `redis_om/model/model.py`, `aredis_om/model/model.py`

## Required Tests

### test_bug_fixes.py (PR #792)
- `test_enum_int_value_query` — Enum with int values produces correct NUMERIC query syntax
- `test_enum_int_value_ne_query` — Not-equal query with Enum values
- `test_optional_field_none_hashmodel` — Save/retrieve Optional[float] as None in HashModel
- `test_optional_field_with_value_hashmodel` — Save/retrieve Optional[float] with actual value
- `test_in_operator_numeric_field` — IN operator (`<<`) with list of ints on NUMERIC field
- `test_not_in_operator_numeric_field` — NOT_IN operator (`>>`) with list of ints on NUMERIC field

### test_knn_expression.py (PR #787)
- `test_or_expression_with_knn` — OR expressions combined with KNN produce valid syntax

### test_tag_separator.py (PR #800)
- `test_separator_parameter_accepted` — Field() accepts separator parameter
- `test_separator_default_value` — Default separator is `|`
- `test_separator_in_hash_schema` — Custom separator appears in HashModel schema
- `test_separator_in_json_schema` — Custom separator appears in JsonModel schema
- `test_separator_save_and_query` — End-to-end save/query with custom separator
- `test_separator_individual_tag_query` — Query individual tags with custom separator
- `test_separator_with_full_text_search` — Separator works alongside full_text_search=True
- `test_multiple_fields_different_separators` — Multiple fields with different separators
- `test_primary_key_separator` — Primary key field uses default separator

### test_json_model.py (PR #657)
- `test_merged_model_error` — OR queries on two embedded models produce correct field prefixes

### test_hash_model.py / test_json_model.py (PR #783)
- `test_bytes_field_with_binary_data` — Store/retrieve non-UTF8 bytes (e.g., PNG headers)
- `test_optional_bytes_field` — Optional[bytes] with None and binary data
- `test_bytes_field_in_embedded_model` — bytes inside EmbeddedJsonModel (JsonModel only)

## Technical Debt

### DateTime
- Timezone handling relies on Pydantic's native datetime handling
- Existing datetime data stored as strings needs migration for NUMERIC indexing

### Cluster
- RediSearch on cluster requires search index on each shard
- Models use hash tags for same-slot guarantee
- Pipeline/transaction operations may have cluster-specific constraints
- Cluster-specific tests needed when cluster test environment is available

## Version
- **Current Version:** 0.4.4
- **Branch:** main


# Additional info:

# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

Project: redis-om-python (fork: pyredis-om)

Overview
- Python library providing object mapping (OM) for Redis, with both async (primary) and generated sync APIs.
- The async package lives under aredis_om/. A sync mirror is generated into redis_om/ via unasync (see make_sync.py). Tests are mirrored similarly from tests/ to tests_sync/.
- Tooling is Poetry for packaging and venv management, pytest for tests, isort/black/flake8/mypy/bandit for lint, and tox for matrix runs. Local Redis services are provided via docker-compose.

Prerequisites
- Python >= 3.10
- Poetry available on PATH
- Docker installed (to run local Redis services)

Quick start
- Create the virtualenv and install dependencies:
  poetry install
- Generate sync modules and mirrored tests (also done implicitly by many make targets):
  make sync
- Bring up Redis services (`redis:8-alpine` on 6380; OSS Redis on 6381):
  docker-compose up -d
- Set the default test connection URL (bash/WSL):
  export REDIS_OM_URL="redis://localhost:6380?decode_responses=True"

Common commands
- Install deps and prepare environment:
  make install
- Generate sync package/tests:
  make sync
- Lint (isort, black check, flake8, mypy, bandit) and build dist first:
  make lint
- Auto-format (isort + black):
  make format
- Run full test suite (async + sync) against the module-enabled local Redis service:
  make test
  # Produces coverage, brings Redis up via docker-compose and tears it down
- Run tests specifically against OSS Redis (no modules):
  make test_oss
- Open a Poetry shell:
  make shell
- Build a source/wheel distribution:
  make dist
- Clean generated artifacts and containers:
  make clean

Running tests directly with pytest
- Ensure Redis is running and REDIS_OM_URL is set (see Quick start). Then:
  poetry run pytest -n auto -vv tests/ tests_sync/ --cov-report term-missing --cov aredis_om redis_om
- Run a single test file:
  poetry run pytest tests/test_hash_model.py -vv
- Run a single test by node id:
  poetry run pytest tests/test_hash_model.py::test_basic_crud -vv
- Filter by expression:
  poetry run pytest -k "json and not oss" -vv

Using tox
- Tox runs with Poetry in each env and passes REDIS_OM_URL through:
  tox
  # envlist: py310, py311, py312, py313

Local Redis services
- redis:8-alpine (modules included) on localhost:6380
- redis (OSS) on localhost:6381
- Compose file:
  docker-compose.yml
- Bring services up/down:
  docker-compose up -d
  docker-compose down
- Typical test URL (bash/WSL):
  export REDIS_OM_URL="redis://localhost:6380?decode_responses=True"

CLI entry points
- Migrations CLI is exposed via Poetry script:
  poetry run migrate
  # Entry point: redis_om.model.cli.migrate:migrate

High-level architecture
- Two packages, one source of truth:
  - aredis_om/: Primary async implementation. Core modules:
    - async_redis.py, sync_redis.py: Thin Redis client wrappers for async/sync usage (async is authoritative here).
    - connections.py: Connection management and URL parsing; get_redis_connection entry points.
    - model/: Data modeling layer.
      - model.py: Base model types (HashModel, JsonModel, EmbeddedJsonModel), persistence, CRUD, indexing metadata, PK handling.
      - migrations/migrator.py: Index creation/migration management; used by Migrator and CLI.
      - encoders.py: Serialization logic for model fields and nested structures.
      - query_resolver.py: Translates Pythonic expression trees into RediSearch query syntax.
      - render_tree.py and token_escaper.py: Expression rendering and token escaping for safe query construction.
      - cli/migrate.py: Implements the migrate CLI for index setup.
    - checks.py, util.py, _compat.py: Helpers and compatibility shims.
  - redis_om/: Generated sync mirror from aredis_om via unasync. Do not edit by hand; use make sync to regenerate.
- Generation pipeline:
  - make_sync.py defines unasync rules mapping aredis_om -> redis_om and tests -> tests_sync with additional string replacements (e.g., async_redis -> sync_redis, pytest_asyncio -> pytest). The Makefile’s make sync runs this.
- Tests layout:
  - tests/: Async-first tests.
  - tests_sync/: Generated sync tests via unasync. Keep edits in tests/ and regenerate.

Development workflow notes
- Edit only aredis_om/ and tests/; then run make sync to refresh the sync package and mirrored tests.
- Many targets (lint, test, dist) call make sync automatically, but running it explicitly before imports avoids stale mirrors in editor sessions.
- REDIS_OM_URL must point to a Redis compatible with the features you intend to test:
  - For RediSearch/RedisJSON features use the local `redis:8-alpine` service (default compose: 6380).
  - For OSS-only scenarios use 6381 and avoid module-dependent features.

CI reference
- GitHub Actions uses Poetry, runs make sync, installs, lints (make dist; make lint), then tests on ubuntu with a redis/redis-stack service. Coverage is uploaded to Codecov. Matrix across Python 3.10–3.13.

Release
- Version is managed in pyproject.toml (tool.poetry.version). GitHub release workflow updates it from the tag and runs poetry publish. Local build artifacts are produced by make dist or poetry build.

Key files
- pyproject.toml: Poetry config, package metadata, dependencies, CLI scripts.
- Makefile: Primary developer entry points for install, sync, lint, test, dist.
- docker-compose.yml: Local Redis services (stack and OSS) and ports.
- make_sync.py: unasync rules to generate sync code/tests.
- pytest.ini: asyncio mode configuration (strict).
- tox.ini: Test env matrix using Poetry.
