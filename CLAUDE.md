# Redis OM Python - Claude Documentation

## Project Overview

Redis OM Python is an object mapping library that provides high-level abstractions for modeling and querying data in Redis using modern Python applications. It builds on top of Pydantic for data validation and uses RediSearch and RedisJSON modules for advanced querying capabilities.

### Key Features
- Declarative object mapping for Redis objects
- Declarative secondary-index generation using RediSearch
- Fluent APIs for querying Redis
- Support for both Hash and JSON models
- Pydantic-based data validation
- GEO spatial queries (Coordinates and GeoFilter)
- Full-text search capabilities
- Embedded models support
- DateTime field querying with range queries and sorting (v0.4.1b2+)
- Redis Cluster support for horizontal scaling (v0.4.1b2+)

### Repository Structure
```
redis_om/               # Synchronous version
├── model/
│   ├── model.py       # Core model classes (RedisModel, HashModel, JsonModel)
│   ├── migrations/    # Migration tools
│   └── encoders.py    # JSON encoding utilities
├── connections.py     # Redis connection management
└── ...

aredis_om/             # Async version (mirrors redis_om structure)
tests/                 # Async tests
tests_sync/            # Synchronous tests
```

## Current Architecture

### Connection Management
File: `redis_om/connections.py`

Supports both standalone Redis and Redis Cluster connections:
```python
def get_redis_connection(**kwargs) -> Union[redis.Redis, redis.RedisCluster]:
    if "decode_responses" not in kwargs:
        kwargs["decode_responses"] = True

    cluster = kwargs.pop("cluster", False)
    url = kwargs.pop("url", URL)

    if cluster:
        if url:
            return redis.RedisCluster.from_url(url, **kwargs)
        return redis.RedisCluster(**kwargs)
    else:
        if url:
            return redis.Redis.from_url(url, **kwargs)
        return redis.Redis(**kwargs)
```

### Field Types and Indexing
File: `redis_om/model/model.py:401-406`

RediSearch field types currently supported:
```python
class RediSearchFieldTypes(Enum):
    TEXT = "TEXT"
    TAG = "TAG"
    NUMERIC = "NUMERIC"
    GEO = "GEO"
```

### Schema Generation
Files:
- `redis_om/model/model.py:1834-1896` (HashModel.schema_for_type)
- `redis_om/model/model.py:2026-2200+` (JsonModel.schema_for_type)

Type mapping for indexing:
- `bool` → `TAG`
- Numeric types (`int`, `float`, `decimal.Decimal`) → `NUMERIC`
- DateTime types (`datetime.date`, `datetime.datetime`) → `NUMERIC` ✅ (added in v0.4.1b2)
- `str` → `TAG` (or `TAG` + `TEXT` if `full_text_search=True`)
- `Coordinates` → `GEO`
- Embedded models → Recursive field processing
- **Everything else** → `TAG` (fallback)

## Implemented Features

### 1. Redis Cluster Support ✅

**Current Status:** Implemented in version 0.4.1b2

**Implementation:**
The connection management now supports both standalone Redis and Redis Cluster connections through the `cluster` parameter.

**Updated Files:**
- `redis_om/connections.py` - Added cluster support with `Union[redis.Redis, redis.RedisCluster]` return type
- `aredis_om/connections.py` - Added async cluster support

**Implementation Details:**
```python
def get_redis_connection(**kwargs) -> Union[redis.Redis, redis.RedisCluster]:
    """Get Redis connection (standalone or cluster)"""
    if "decode_responses" not in kwargs:
        kwargs["decode_responses"] = True

    cluster = kwargs.pop("cluster", False)
    url = kwargs.pop("url", URL)

    if cluster:
        if url:
            return redis.RedisCluster.from_url(url, **kwargs)
        return redis.RedisCluster(**kwargs)
    else:
        if url:
            return redis.Redis.from_url(url, **kwargs)
        return redis.Redis(**kwargs)
```

**Usage:**
```python
# Standalone connection (default)
redis_conn = get_redis_connection(url="redis://localhost:6379")

# Cluster connection
redis_conn = get_redis_connection(cluster=True, host="localhost", port=6379)
```

**Notes:**
- RediSearch on cluster has special requirements (search index on each shard)
- All documents for a model use hash tags for same-slot guarantee
- Some operations may have cluster-specific constraints (pipelines, transactions)
- Migration runner may need cluster-aware handling

### 2. DateTime Field Querying ✅

**Current Status:** Implemented in version 0.4.1b2

**Implementation:**
DateTime fields (`datetime.date` and `datetime.datetime`) are now properly indexed as `NUMERIC` fields, enabling range queries and proper chronological sorting.

**Updated Files:**
- `redis_om/model/model.py` - Added datetime support in both `HashModel.schema_for_type` and `JsonModel.schema_for_type`
- `aredis_om/model/model.py` - Added datetime support in async versions

**Implementation Details:**
Both `HashModel` and `JsonModel` now detect datetime types and map them to NUMERIC fields:

```python
# In HashModel.schema_for_type (line ~1876)
elif typ in (datetime.date, datetime.datetime):
    schema = f"{name} NUMERIC"

# In JsonModel.schema_for_type (line ~2200)
elif typ in (datetime.date, datetime.datetime):
    schema = f"{path} AS {index_field_name} NUMERIC"
```

**Benefits:**
Users can now:
- Find records created after a certain date using range queries
- Find records between two dates
- Sort properly by datetime fields (chronological order)
- Build time-based queries (e.g., "users who joined in the last 30 days")

**Usage Example:**
```python
# Range queries now work
today = datetime.date.today()
recent_members = Member.find(
    Member.join_date >= (today - datetime.timedelta(days=30))
).all()

# Date range queries
start_date = datetime.date(2024, 1, 1)
end_date = datetime.date(2024, 12, 31)
members_2024 = Member.find(
    (Member.join_date >= start_date) & (Member.join_date <= end_date)
).all()

# Proper chronological sorting
oldest_members = Member.find().sort_by("join_date").all()
newest_members = Member.find().sort_by("-join_date").all()
```

## Technical Debt & Considerations

### DateTime Implementation Notes:
1. **Timezone handling:** Current implementation uses Pydantic's native datetime handling
2. **Precision:** Follows Redis numeric precision
3. **Null handling:** Optional datetime fields work with standard Pydantic Optional types
4. **Backwards compatibility:** Existing datetime data stored as strings will need migration
5. **Future improvements:** Consider adding timestamp conversion helpers for better control

### Cluster Implementation Notes:
1. **Hash tags:** Models should use consistent key patterns for same-slot guarantee
2. **Search index:** Be aware of RediSearch cluster mode requirements
3. **Atomic operations:** Some pipeline/transaction operations may have cluster-specific constraints
4. **Testing:** Cluster-specific tests should be added when cluster test environment is available

## Version Information

- **Last Updated:** 2025-11-23
- **Repository:** redis-om-python
- **Current Version:** 0.4.1b2
- **Branch:** claude/implement-missing-features-01Mrn7m8CkACNrQ5hWW8d6t4

## Quick Reference

### Files Modified for DateTime Support (v0.4.1b2):
1. `redis_om/model/model.py` - Added datetime type detection in schema_for_type methods
2. `aredis_om/model/model.py` - Added datetime type detection in async version

### Files Modified for Cluster Support (v0.4.1b2):
1. `redis_om/connections.py` - Added cluster parameter and RedisCluster support
2. `aredis_om/connections.py` - Added cluster parameter and RedisCluster support (async)
