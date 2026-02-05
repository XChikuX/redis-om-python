# Redis OM Python

Object mapping library for Redis built on Pydantic, using RediSearch and RedisJSON modules.

## Repository Structure

```
redis_om/               # Synchronous version
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

aredis_om/             # Async version (mirrors redis_om structure)
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

## Technical Debt

### DateTime
- Timezone handling relies on Pydantic's native datetime handling
- Existing datetime data stored as strings needs migration for NUMERIC indexing
- Consider adding timestamp conversion helpers

### Cluster
- RediSearch on cluster requires search index on each shard
- Models use hash tags for same-slot guarantee
- Pipeline/transaction operations may have cluster-specific constraints
- Cluster-specific tests needed when cluster test environment is available

## Version
- **Current Version:** 0.4.1b3
- **Branch:** main
