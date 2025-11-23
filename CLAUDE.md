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

Currently supports only standalone Redis connections:
```python
def get_redis_connection(**kwargs) -> redis.Redis:
    url = kwargs.pop("url", URL)
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
- `str` → `TAG` (or `TAG` + `TEXT` if `full_text_search=True`)
- `Coordinates` → `GEO`
- Embedded models → Recursive field processing
- **Everything else** → `TAG` (fallback)

## Missing Features

### 1. Redis Cluster Support ❌

**Current Status:** Not implemented

**Problem:**
- The connection management only supports `redis.Redis` and `redis.Redis.from_url()`
- Redis Cluster requires `redis.RedisCluster` which has a different API
- No cluster detection or routing logic exists

**Impact:**
- Users cannot deploy Redis OM with Redis Cluster
- No horizontal scaling support
- Limited to single-node or master-replica setups

**Affected Files:**
- `redis_om/connections.py:9-20`
- `aredis_om/connections.py` (async version)

**Implementation Requirements:**
1. Detect cluster vs standalone mode
2. Use `redis.RedisCluster` for cluster connections
3. Handle cluster-specific limitations:
   - All keys for a model should ideally hash to the same slot (hash tags)
   - Transactions/pipelines have cluster-specific constraints
   - SCAN operations work differently across cluster nodes
4. Update migration logic to work with cluster
5. Handle cross-slot operations (or document limitations)

**Example Implementation Approach:**
```python
def get_redis_connection(**kwargs) -> Union[redis.Redis, redis.RedisCluster]:
    """Get Redis connection (standalone or cluster)"""
    is_cluster = kwargs.pop("cluster", False)
    url = kwargs.pop("url", URL)

    if is_cluster:
        if url:
            return redis.RedisCluster.from_url(url, **kwargs)
        return redis.RedisCluster(**kwargs)
    else:
        if url:
            return redis.Redis.from_url(url, **kwargs)
        return redis.Redis(**kwargs)
```

**Challenges:**
- RediSearch on cluster has special requirements (search index on each shard)
- Need to ensure all documents for a model use hash tags: `{model_prefix}:pk`
- Pipeline operations need cluster-aware handling
- Migration runner needs to execute on all cluster nodes or use a coordinator

### 2. DateTime Field Querying ❌

**Current Status:** DateTime fields can be stored but **cannot be queried or sorted effectively**

**Problem:**
- `datetime.date` and `datetime.datetime` fields fall through to the `else` case in `schema_for_type()`
- They get indexed as `TAG` fields instead of `NUMERIC` fields
- TAG fields only support exact match queries, not range queries
- Cannot query: `Member.find(Member.join_date >= some_date)`
- Cannot meaningfully sort by datetime fields

**Evidence:**
- Tests only sort by `join_date` but never filter by it (see `tests/test_hash_model.py:387`, `tests/test_json_model.py:752`)
- No test cases for datetime range queries exist
- `datetime.date` and `datetime.datetime` are not in the type mapping logic

**Affected Files:**
- `redis_om/model/model.py:1834-1896` (HashModel.schema_for_type)
- `redis_om/model/model.py:2026+` (JsonModel.schema_for_type)
- `redis_om/model/encoders.py` (may need datetime→timestamp conversion)

**Impact:**
Users cannot:
- Find records created after a certain date
- Find records between two dates
- Sort meaningfully by datetime (TAG sorting is lexicographic, not chronological)
- Build time-based queries (e.g., "users who joined in the last 30 days")

**Implementation Requirements:**

1. **Type Detection:** Detect `datetime.date`, `datetime.datetime`, and `datetime.time` types

2. **Schema Mapping:** Map datetime types to `NUMERIC` in RediSearch schema
   ```python
   import datetime

   # In schema_for_type():
   elif typ in (datetime.date, datetime.datetime, datetime.time):
       schema = f"{name} NUMERIC"
   ```

3. **Encoding/Decoding:** Convert datetime objects to Unix timestamps (numeric) for storage
   ```python
   # Encoding (before storing)
   if isinstance(value, datetime.datetime):
       value = value.timestamp()
   elif isinstance(value, datetime.date):
       value = datetime.datetime.combine(value, datetime.time.min).timestamp()

   # Decoding (after retrieving)
   if field_type == datetime.datetime:
       value = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
   elif field_type == datetime.date:
       value = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).date()
   ```

4. **Query Support:** Enable range queries on datetime fields
   ```python
   # Should work after implementation:
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
   ```

5. **Sorting:** Proper chronological sorting
   ```python
   # Should work correctly after implementation:
   oldest_members = Member.find().sort_by("join_date").all()
   newest_members = Member.find().sort_by("-join_date").all()
   ```

**Implementation Files to Modify:**

1. `redis_om/model/model.py`:
   - `HashModel.schema_for_type()` (line 1834-1896)
   - `JsonModel.schema_for_type()` (line 2026+)
   - Add datetime type detection before the `else` fallback

2. `redis_om/model/encoders.py`:
   - Add datetime → timestamp conversion in `jsonable_encoder()`
   - Handle timezone-aware vs naive datetimes

3. `redis_om/model/model.py` (decoder logic):
   - `HashModel.get()` (line 1740+)
   - `JsonModel.get()`
   - Convert timestamps back to datetime objects based on field type

4. Add comprehensive tests in:
   - `tests/test_hash_model.py`
   - `tests/test_json_model.py`
   - Test range queries, sorting, timezone handling, date vs datetime

**Example Test Cases Needed:**
```python
async def test_datetime_range_query(m):
    # Test date range filtering
    old_member = m.Member(
        first_name="Old",
        last_name="User",
        join_date=datetime.date(2020, 1, 1),
        age=50
    )
    new_member = m.Member(
        first_name="New",
        last_name="User",
        join_date=datetime.date(2024, 1, 1),
        age=25
    )
    await old_member.save()
    await new_member.save()
    await Migrator().run()

    # Find members who joined after 2023
    recent = await m.Member.find(
        m.Member.join_date >= datetime.date(2023, 1, 1)
    ).all()

    assert len(recent) == 1
    assert recent[0].first_name == "New"

async def test_datetime_sorting(m):
    # Test chronological sorting
    members = await m.Member.find().sort_by("join_date").all()

    # Verify they're in chronological order
    for i in range(len(members) - 1):
        assert members[i].join_date <= members[i + 1].join_date
```

## Technical Debt & Considerations

### DateTime Implementation Considerations:
1. **Timezone handling:** Decide on UTC-only vs timezone-aware support
2. **Precision:** Timestamp precision (seconds vs milliseconds vs microseconds)
3. **Null handling:** How to index `Optional[datetime.date]` fields
4. **Backwards compatibility:** Existing data stored as strings would break
5. **Migration path:** Provide migration tool for existing datetime data

### Cluster Implementation Considerations:
1. **Hash tags:** Auto-wrap primary keys with `{prefix}` for same-slot guarantee
2. **Search index:** RediSearch cluster mode requirements
3. **Atomic operations:** Document which operations won't work in cluster mode
4. **Testing:** Need a cluster test environment (multiple Redis nodes)

## Related Issues & References

### DateTime Support:
- README.md shows `join_date: datetime.date` in examples but no query examples
- All test fixtures include datetime fields but only test storage/retrieval, not querying
- Users likely expect datetime querying to work based on the examples

### Cluster Support:
- No documentation mentions cluster support or limitations
- High-scale production deployments typically require cluster mode
- Current architecture assumes single-node or simple replication

## Recommendations

### Priority 1: DateTime Field Querying
**Rationale:** More commonly needed, affects existing example code in README
**Effort:** Medium (requires encoder/decoder changes + schema changes)
**Risk:** Medium (backwards compatibility with existing data)

### Priority 2: Redis Cluster Support
**Rationale:** Required for production scalability, but fewer users need it initially
**Effort:** High (requires connection rewrite, pipeline changes, testing infrastructure)
**Risk:** High (many edge cases, cluster-specific behaviors)

## Version Information

- **Last Updated:** 2025-11-23
- **Repository:** redis-om-python
- **Branch:** claude/add-cluster-datetime-support-01UqXCzLpTdXRxr1nUx8gREZ
- **Latest Commit:** e712c02 "Revv 0.4.1b1"

## Quick Reference

### Files to Modify for DateTime Support:
1. `redis_om/model/model.py` (schema_for_type in both HashModel and JsonModel)
2. `redis_om/model/encoders.py` (jsonable_encoder)
3. `aredis_om/model/model.py` (async version)
4. `aredis_om/model/encoders.py` (async version)
5. Tests in `tests/` and `tests_sync/`

### Files to Modify for Cluster Support:
1. `redis_om/connections.py`
2. `aredis_om/connections.py`
3. `redis_om/model/model.py` (pipeline handling, key generation)
4. `redis_om/model/migrations/migrator.py`
5. Documentation updates
6. Test infrastructure for cluster testing
