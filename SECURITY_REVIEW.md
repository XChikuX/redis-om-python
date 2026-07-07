# Security and Performance Review

Review date: 2026-05-07 (verified 2026-07-07)
Scope: `/home/runner/work/redis-om-python/redis-om-python` fresh clone, with emphasis on `aredis_om/` as the source of truth, generated-sync workflow, tests, Docker Compose, and GitHub Actions. No production code was changed as part of this review.

**Re-verification (2026-07-07):** Each finding was re-checked against the current `main` state. Inline **Status:** markers record whether the item is *Addressed*, *Partially addressed*, or *Open*. The summary section at the bottom aggregates the current state.

## Executive summary

The codebase is generally well-structured for a Redis object mapper: it is async-first, keeps sync code generated from one source, validates model data through Pydantic v2, consistently escapes RediSearch query tokens, and has broad test coverage across CRUD, query, pipeline, cluster, and regression scenarios.

No critical security flaw was identified in this documentation review. The highest-priority concerns are operational and resilience issues: unbounded query exhaustion can create memory pressure, benchmark tests do not currently fail on regressions, dependency resolution is not locked in the repository, and some CI/tooling files are stale or inconsistent with the uv-based workflow.

## Reviewed assets

- `aredis_om/model/model.py` — model lifecycle, query construction, save/get pipelines, bulk operations.
- `aredis_om/model/token_escaper.py` — RediSearch token escaping.
- `aredis_om/model/migrations/migrator.py` — index creation/drop migration path.
- `aredis_om/connections.py` — Redis and Redis Cluster connection construction.
- `aredis_om/checks.py` — Redis feature detection cache.
- `tests/` — 30 Python test files and 610 async test functions in the current fresh clone.
- `tests/test_performance_benchmark.py` — single-instance benchmark baseline suite.
- `tests/test_cluster_operations.py` — cluster operation and performance comparison coverage.
- `Makefile`, `pyproject.toml`, `tox.ini`, `docker-compose.yml`, `docker-compose.cluster.yml`.
- `.github/workflows/ci.yml` and `.github/workflows/codeql.yml`.

## Evidence map

- `pyproject.toml` defines broad production dependency ranges in `project.dependencies` and development tooling in `project.optional-dependencies.dev`.
- The repository `.gitignore` ignores `uv.lock`, and no lockfile is present in the current clone.
- `Makefile` defines `make sync`, `make lint`, `make test`, `make test_oss`, and `make test_cluster`.
- `tox.ini` still uses Poetry commands.
- `make_sync.py` defines unasync generation for `aredis_om/` → `redis_om/` and `tests/` → `tests_sync/`.
- `aredis_om/model/model.py` defines the global `model_registry`, Redis Cluster pipeline detection, `FindQuery.copy()`, transparent result exhaustion, and conversion-heavy load/save paths.
- `aredis_om/model/migrations/migrator.py` builds and executes `FT.CREATE` migration commands.
- `aredis_om/model/token_escaper.py` defines RediSearch token escaping.
- `aredis_om/checks.py` defines the weak-key command capability cache.
- `docker-compose.cluster.yml` uses host networking and `--protected-mode no` for local cluster testing.
- `.github/workflows/ci.yml` runs linting and matrix tests.
- `.github/workflows/codeql.yml` configures CodeQL analysis.

## Performance strengths

1. **Async-first architecture with generated sync parity**
   - `aredis_om/` is the implementation source of truth and `make_sync.py` generates `redis_om/` plus `tests_sync/`.
   - This reduces duplicate maintenance while keeping both API styles testable.

2. **Pipeline-backed bulk operations**
   - `RedisModel.add()` saves sequences through a pipeline.
   - `HashModel.get_many()` batches `HGETALL` calls.
   - `JsonModel.get_many()` batches `JSON.GET` calls.
   - `delete_many()` chunks deletes in batches of 100 keys.

3. **Cluster-aware pipeline handling**
   - `_is_cluster_pipeline()` prevents awaiting individual ClusterPipeline commands, avoiding accidental response consumption before `execute()`.

4. **Lazy connection and feature checks**
   - `Meta.database` can be lazily resolved.
   - Redis command support checks are cached per connection with `WeakKeyDictionary` in `aredis_om/checks.py`.
   - Query execution avoids repeated index-health checks after the class-level flag is set.

5. **Query rendering cache**
   - `FindQuery` caches resolved expression, query string, and pagination fragments on an instance.
   - This avoids repeated string construction for reused query instances.

6. **Dedicated benchmark coverage exists**
   - `tests/test_performance_benchmark.py` covers CRUD, query, JSON, hash, embedded models, GEO, full-text search, pipelines, and bulk operations.
   - Cluster tests include performance comparisons between cluster and single-instance Redis paths.

## Performance risks and bottlenecks

### P1 — Unbounded result exhaustion can create memory pressure

`FindQuery.execute()` defaults to `exhaust_results=True`. After the first `FT.SEARCH`, it transparently paginates until no more results are returned and accumulates all models in `_model_cache`.

**Risk:** A broad query can load an unexpectedly large result set into memory. This is convenient for small result sets but unsafe for user-facing endpoints where filters or tenant scoping can fail open.

**Recommendation:** Add explicit guidance and/or API support for bounded reads. Candidate mitigations include documented `page()` usage, a maximum page/result option, streaming iteration guidance, or warnings when exhausting beyond a threshold.

**Status (2026-07-07): Partially addressed.** `FindQuery.execute()` in `aredis_om/model/model.py` still defaults to `exhaust_results=True` and paginates without a max-results guard — the runtime behaviour is unchanged. However, the trade-off is now documented in `docs/queries.mdx` ("Limiting and pagination" section), which explicitly calls out the memory risk of `.all()` on user-facing endpoints and recommends `.page()`, `.iter_cursor()`, or `execute(exhaust_results=False)` for bounded reads. Residual: no API-level max-results option or warning threshold.

### P1 — `FindQuery.copy()` is in a hot pagination path

`FindQuery.copy()` builds a dict, shallow-copies expressions and sort fields, merges overrides, and constructs a new `FindQuery`. It is used in the pagination loop when exhausting results.

**Risk:** For large paginated result sets this adds avoidable per-page allocation and revalidation overhead. Re-running `validate_sort_fields()` on already-resolved embedded sort field aliases (e.g. `metrics_score`) also raises `QueryNotSupportedError` when the exhaust loop spans more than one page.

**Mitigation in this fork:** `FindQuery.copy()` now skips re-validation when `sort_fields` is not explicitly overridden. The pre-resolved sort fields are reattached on the new query directly, so the pagination exhaust loop no longer raises `QueryNotSupportedError` for embedded sort paths (e.g. `sort_by("metrics.score")` on a result set spanning more than one page). Explicit `copy(sort_fields=...)` calls still validate. Covered by `tests/test_json_model.py::test_copy_preserves_resolved_embedded_sort_fields`.

**Status (2026-07-07): Partially addressed.** The sort-field re-validation regression is fixed and tested (`aredis_om/model/model.py` lines 1272-1291). The broader recommendation — lower-allocation copy that preserves already-validated state across more attributes — has not been actioned; `copy()` still rebuilds via a full `self.dict()` round-trip on every page.

### P2 — Recursive conversion passes run on every save/load

Save and load paths recursively convert datetime/date values, bytes/base64 values, dataclasses, empty strings, and JSON-compatible encodings.

**Risk:** Large nested JSON documents pay repeated whole-document traversal costs even when only a few fields require conversion.

**Recommendation:** Investigate field-aware conversion plans generated from model metadata, or combine compatible conversion passes to reduce traversal count.

**Status (2026-07-07): Open.** Save/load paths still perform unconditional recursive traversal; no field-aware conversion plan or merged-pass optimisation has been introduced.

### P2 — Benchmarks record timings but do not enforce regressions

The benchmark suite records elapsed time and operations per second, but the main CI path does not enforce a regression budget.

**Risk:** Performance regressions can land as long as functional assertions pass.

**Recommendation:** Integrate `pytest-codspeed` or a lightweight CI threshold for stable hot paths such as `get_many()`, broad query pagination, schema generation, and save/get conversion.

**Status (2026-07-07): Addressed.** `pytest-codspeed` is now wired into CI via a dedicated `.github/workflows/codspeed.yml` workflow that runs `tests/test_performance_benchmark.py --codspeed` with `mode: walltime` on every push to `main` and every pull request. The benchmark tests are marked with `@pytest.mark.benchmark` (via module-level `pytestmark`), so CodSpeed measures each test and compares against the baseline, posting regression comments on PRs. A `make benchmark` Makefile target and `docs/benchmarks.mdx` documentation page cover local usage and how to add new benchmarks. `pytest-codspeed` was already listed as a dev dependency in `pyproject.toml`; it is now actually invoked by CI.

### P2 — Generated sync output can hide performance drift until `make sync`

`redis_om/` and `tests_sync/` are ignored and absent in a fresh clone until generated.

**Risk:** Developers can inspect only async code and miss generated sync differences introduced by `make_sync.py` replacements or post-sync fixes.

**Recommendation:** Keep CI running `make sync` before lint/test and document that generated files should not be edited manually.

**Status (2026-07-07): Addressed.** CI runs `make sync` as a dedicated step in both `lint` and `test-unix`/`test-cluster` jobs before installing dependencies, and `CLAUDE.md` documents that generated artifacts under `redis_om/` and `tests_sync/` must not be edited manually.

### P3 — Command feature detection has a per-connection cold-start cost

`has_redis_json()` and `has_redisearch()` call Redis `COMMAND INFO` on first use per connection and cache the result.

**Risk:** Low for long-lived connections, but visible for short-lived clients or tests that frequently recreate clients.

**Recommendation:** Keep the cache; consider explicit capability injection only if profiling shows this is material.

**Status (2026-07-07): Open (low priority).** `aredis_om/checks.py` still uses a `WeakKeyDictionary` cache per connection; no capability-injection hook has been added. Acceptable while the cold-start cost is not profiled as material.

## Security strengths

1. **RediSearch token escaping**
   - `TokenEscaper` escapes punctuation and spaces required by RediSearch tokenization rules.
   - Query value rendering in `FindQuery` uses the shared escaper for string-like values.

2. **KNN vector parameters use RediSearch `PARAMS`**
   - Vector query data is passed through the `PARAMS` clause instead of being directly embedded in the query string.

3. **Pydantic v2 validation at model boundaries**
   - Loaded Redis data and user-provided model data flow through Pydantic model validation.
   - Unknown or badly typed input is constrained by model definitions.

4. **Field path validation before updates**
   - Update paths call `validate_model_fields()` to reject invalid field references.

5. **Security tooling exists**
   - `make lint` runs Bandit over async and generated sync packages.
   - `.github/workflows/codeql.yml` runs CodeQL for Python on pull requests, pushes to main, and weekly schedule.

6. **No obvious dynamic code execution path**
   - A repository-wide Python search did not identify `eval()`, `exec()`, `subprocess`, `shell=True`, unsafe pickle usage, or unsafe YAML loading in library code.

## Security risks and hardening opportunities

### P1 — Dependency resolution is broad and no lockfile is committed

`pyproject.toml` uses broad lower/upper bounds for production and dev dependencies. `.gitignore` ignores `uv.lock`, and no `uv.lock` is present in the fresh clone, while CI cache keys still reference `uv.lock`.

**Risk:** Builds can resolve different transitive dependency versions over time, which increases supply-chain and reproducibility risk.

**Current state:** The repository currently operates without a committed lockfile. **Recommendation:** Make that policy explicit. For stronger CI reproducibility, commit `uv.lock`; if the project intentionally avoids lockfiles because it is a library, document the no-lock policy and keep minimum dependency bounds actively tested with advisory checks.

**Status (2026-07-07): Partially addressed.** `uv.lock` is now tracked by git (commit `4dcd256` "Enable CI caching by committing uv.lock"). `.gitignore` line 34 (`# uv.lock # Commented out to enable CI caching`) is intentionally commented out so the file is tracked, and CI cache keys in `.github/workflows/ci.yml` consume it. The lockfile is present in a fresh clone. The stale `CLAUDE.md` statement has been corrected to reflect that `uv.lock` is committed.

### P1 — Tooling drift: `tox.ini` still uses Poetry

The repository workflow is uv-based, but `tox.ini` runs `poetry install` and `poetry run pytest`.

**Risk:** Contributors or automation using tox may get failures or test a dependency environment different from CI.

**Mitigation in this fork:** `tox.ini` now invokes `uv sync --extra dev` and `uv run pytest`, matching the rest of the workflow.

**Recommendation:** Keep `tox.ini` aligned with the uv workflow when CI tooling changes.

**Status (2026-07-07): Addressed.** Verified `tox.ini` now reads `allowlist_externals = uv` / `commands = uv sync --extra dev` / `uv run pytest` and no longer references Poetry.

### P2 — Schema/index command construction should remain internal-only

`aredis_om/model/migrations/migrator.py` builds `FT.CREATE` commands from model-derived index names and schema strings. In the cluster path it splits a formatted string into arguments; in the single-node path it passes one formatted command string to `execute_command()`.

**Risk:** Under normal usage, schema content comes from Python model definitions, not untrusted request input. If applications dynamically generate model classes or field metadata from user input, malformed schema tokens could alter index commands.

**Recommendation:** Document that index names, prefixes, separators, and schema metadata are trusted configuration. Prefer positional command arguments for all migration paths over a single formatted command string where practical.

**Status (2026-07-07): Addressed.** Re-verified: both the single-node path (`create_index` / `create_physical_index` in `migrator.py`) and the cluster path (`_create_index_cluster` / `_create_physical_index_cluster`) now build arguments positionally — the cluster path splits `f"ft.create {index_name} {schema}"` into a list and unpacks it with `*command`, and only uses the `target_nodes=` keyword for routing (not for schema construction). The original "single formatted command string" risk no longer applies. The trusted-configuration boundary is now documented in `docs/migrations.mdx` ("Trust boundary: schema and index configuration" section).

### P2 — Global mutable registries are not concurrency-hardened

`model_registry` is a module-level dict populated by the metaclass. The command capability cache is also global mutable state, though it uses weak connection keys.

**Risk:** Normal import-time registration is safe, but dynamic runtime model creation/clearing in multi-threaded applications can race.

**Recommendation:** Treat runtime mutation as unsupported or protect registry mutation with a lock if dynamic registration becomes a supported use case.

**Status (2026-07-07): Open (by design).** `model_registry` is still a bare `dict[type, type]` in `aredis_om/model/model.py` (line 66) populated by `ModelMeta.__new__`. Import-time registration is safe. Runtime mutation (e.g. `model_registry.clear()` in tests) is used by the test suite itself, but no lock protects it. Acceptable as long as runtime mutation is documented as unsupported; that documentation does not yet exist.

### P2 — HashModel null handling has semantic ambiguity

Hash storage cannot store native null values, so optional nulls are represented as empty strings and converted back to `None` for optional fields.

**Risk:** Applications that need to distinguish an explicit empty string from null on optional string-like fields can see ambiguity.

**Recommendation:** Document this limitation for HashModel fields and prefer JsonModel where exact null-vs-empty-string semantics are required.

**Status (2026-07-07): Addressed.** The null-vs-empty-string behaviour for `HashModel` is now documented in `docs/models.mdx` ("Null vs. empty-string semantics" subsection under "With HashModel"), with a code example showing that `Optional[str]` set to `""` round-trips as `None`, and a recommendation to use `JsonModel` where the distinction matters.

### P2 — File-level mypy suppressions reduce type-safety signal

Core files use broad `# mypy: disable-error-code=...` headers, especially `aredis_om/model/model.py`.

**Risk:** Type regressions in complex query/model code can be hidden.

**Recommendation:** Incrementally replace broad file-level suppressions with targeted inline ignores and add focused tests around the affected branches.

**Status (2026-07-07): Open.** Verified the broad file-level suppressions are still in place: `aredis_om/model/model.py` line 1 disables `assignment,arg-type,union-attr,no-redef`; `aredis_om/model/migrations/migrator.py` line 1 disables `attr-defined`. Several test files also carry broad `# mypy: disable-error-code="type-var"` headers. No incremental narrowing has been done.

### P3 — Development Redis Cluster disables protected mode

`docker-compose.cluster.yml` runs local Redis cluster nodes with `--protected-mode no` and `network_mode: host`.

**Risk:** This is acceptable for local testing but unsafe as production guidance.

**Recommendation:** Keep the Compose file clearly documented as local-only and avoid copying these flags into deployment examples.

**Status (2026-07-07): Addressed.** The compose file now carries a top-of-file `# !! LOCAL DEVELOPMENT / CI ONLY !!` banner that calls out `--protected-mode no`, explains why it is needed for cluster gossip, and links to the Redis security docs with a "DO NOT copy these flags into production" warning. `docker-compose.yml` (non-cluster) was already fine.

### P3 — CI action pinning uses version tags rather than immutable SHAs

Workflows use versioned actions such as `actions/checkout@v6`, `actions/setup-python@v6.2.0`, and `astral-sh/setup-uv@v7`.

**Risk:** Version tags are common but less strict than pinning actions by commit SHA.

**Recommendation:** For higher supply-chain assurance, pin third-party actions to immutable SHAs and use Dependabot or a similar process for updates.

**Status (2026-07-07): Open.** `.github/workflows/ci.yml` and `codeql.yml` still use floating version tags (`actions/checkout@v7`, `actions/setup-python@v6.3.0`, `astral-sh/setup-uv@v7`, `actions/cache@v6.1.0`, `codecov/codecov-action@v7`, `github/codeql-action/*@v4`). No SHA pinning and no Dependabot config for actions.

## Operational observations

- `redis_om/` and `tests_sync/` are generated and ignored, so line references and static scans should prioritize `aredis_om/` and `tests/` unless generated outputs have been produced locally.
- CI runs lint before tests and runs `make test`, which itself starts/stops Docker Compose. In GitHub Actions, a Redis service is also configured, but the Makefile defaults to port 6380 unless overridden.
- `make lint` invokes `make dist`, and `make dist` invokes `clean`, which removes generated outputs and the virtual environment. This can be surprising during local iteration.
- The project advertises Python 3.14 in classifiers and CI tests against it (matrix: 3.10–3.14 in both `test-unix` and `test-cluster`). Lint pins to 3.12.

## Prioritized recommendations

### P1 — Address first

1. **Partially addressed** — Bounded-query guidance is now documented in `docs/queries.mdx`. Residual: no API-level max-results option or runtime warning threshold for `FindQuery.execute(exhaust_results=True)`.
2. **Partially addressed** — `FindQuery.copy()` no longer re-validates resolved sort fields; lower-allocation copy strategy for other attributes still pending.
3. **Addressed** — `uv.lock` is committed (commit `4dcd256`) and `CLAUDE.md` now reflects this.
4. ~~**Done**~~ — `tox.ini` now uses uv.

### P2 — Improve resilience and maintainability

1. **Addressed** — CI runs `make sync` before lint/test; generated-sync workflow is documented in `CLAUDE.md`.
2. **Addressed** — `pytest-codspeed` regression gate is wired into CI via `.github/workflows/codspeed.yml` (walltime mode, runs on push/PR). Benchmarks are marked with `@pytest.mark.benchmark`; `make benchmark` and `docs/benchmarks.mdx` cover local usage.
3. **Addressed** — Schema/index command construction is positional in both single-node and cluster paths; trusted-configuration boundary is now documented in `docs/migrations.mdx`.
4. **Addressed** — HashModel null/empty-string ambiguity is documented in `docs/models.mdx`.
5. **Open** — Reduce file-level mypy suppressions in core model code.
6. **Open** — Investigate field-aware conversion plans for datetime/bytes-heavy models (originally P3, promoted here as it overlaps with P2 recursive-conversion risk).

### P3 — Defense-in-depth

1. **Open** — Pin GitHub Actions to immutable SHAs if the project wants stricter CI supply-chain controls.
2. **Addressed** — `docker-compose.cluster.yml` now carries a top-of-file `LOCAL DEVELOPMENT / CI ONLY` banner.
3. **Open (by design)** — Consider locks around `model_registry` only if runtime dynamic model registration is supported.
4. **Open (low priority)** — Command feature-detection cold-start; keep the `WeakKeyDictionary` cache and revisit only if profiling shows it is material.

## Overall assessment

The project has a solid baseline for a production-oriented Redis object mapper. Its strongest security properties are Pydantic validation, RediSearch token escaping, and existing Bandit/CodeQL coverage. Its strongest performance properties are async-first design and pipeline-backed bulk operations.

The main improvements are not emergency fixes; they are hardening and scalability work that will make behavior more predictable under large result sets, changing dependency graphs, and contributor workflow variation.

### Re-verification summary (2026-07-07)

Of the 13 original findings:

| Priority | Total | Addressed | Partially | Open |
| --- | --- | --- | --- | --- |
| P1 | 4 | 2 | 2 | 0 |
| P2 | 5 | 4 | 0 | 1 |
| P3 | 4 | 1 | 0 | 3 |
| **Total** | **13** | **7** | **2** | **4** |

**Highest-impact open items:**

1. P2 — narrow file-level mypy suppressions in `aredis_om/model/model.py` and `migrator.py`.
2. P3 — pin GitHub Actions to immutable SHAs (supply-chain hardening).
3. P3 — locks around `model_registry` (only if runtime dynamic registration is supported).
4. P2 — investigate field-aware conversion plans for datetime/bytes-heavy models (perf optimisation).

The runtime behaviour of the highest-priority open item (P1 unbounded exhaustion) is unchanged, but the risk is now documented end-to-end in `docs/queries.mdx`. The remaining P1/P2 residuals are optimisation work (lower-allocation `FindQuery.copy()`, field-aware conversion plans) rather than correctness or safety gaps.
