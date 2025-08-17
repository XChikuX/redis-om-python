# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

Project: redis-om-python (fork: pyredis-om)

Overview
- Python library providing object mapping (OM) for Redis, with both async (primary) and generated sync APIs.
- The async package lives under aredis_om/. A sync mirror is generated into redis_om/ via unasync (see make_sync.py). Tests are mirrored similarly from tests/ to tests_sync/.
- Tooling is Poetry for packaging and venv management, pytest for tests, isort/black/flake8/mypy/bandit for lint, and tox for matrix runs. Redis (and Redis Stack) are provided via docker-compose.

Prerequisites
- Python >= 3.10
- Poetry available on PATH
- Docker installed (to run local Redis/Redis Stack)

Quick start
- Create the virtualenv and install dependencies:
  poetry install
- Generate sync modules and mirrored tests (also done implicitly by many make targets):
  make sync
- Bring up Redis services (Redis Stack on 6380; OSS Redis on 6381):
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
- Run full test suite (async + sync) against Redis Stack:
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
- redis/redis-stack (modules) on localhost:6380
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
  - For RediSearch/RedisJSON features use Redis Stack (default compose: 6380).
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

