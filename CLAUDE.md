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
- **Save order:** `model.dict()` → `convert_datetime_to_timestamp()` → `convert_bytes_to_base64()` → `jsonable_encoder()` (HashModel only) → Redis
- **Get order (HashModel):** Redis → `convert_empty_strings_to_none()` → `convert_base64_to_bytes()` → `parse_obj()`
- **Get order (JsonModel):** Redis → `convert_timestamp_to_datetime()` → `convert_base64_to_bytes()` → `model_validate()`

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
- **Current Version:** 0.4.1b3
- **Branch:** main
