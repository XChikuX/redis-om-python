# RedisArray (Redis 8.8+ Arrays)

`RedisArray` is a Python wrapper around the Redis 8.8+ preview data
type **Arrays** — a sparse, index-addressable sequence of strings.
Unlike `LIST`s, an `AR*` element is accessed directly by its index, so
writes to non-contiguous indices don't allocate the gaps in between.

Arrays are well-suited for:

- **Timestamped event logs** — append a record at the auto-advance cursor.
- **Ring buffers / sliding windows** — bounded circular storage with
  `ARRING` and `ARLASTITEMS`.
- **Sparse numeric series** — `AROP` aggregates (`SUM`, `MIN`, `MAX`,
  `AND`, `OR`, `XOR`) without loading the whole array.
- **Inline search** — `ARGREP` finds elements matching textual predicates
  (`EXACT`, `MATCH`, `GLOB`, `RE`) without a RediSearch index.

> **Server requirement**: Redis 8.8+. Arrays are currently a preview
> data type. The methods here use `execute_command` directly so they
> work with any redis-py 8.0+ client without depending on experimental
> high-level bindings.

```python
from aredis_om import RedisArray

events = RedisArray(db, "events:click")

await events.set(0, "login", "click", "purchase")
print(await events.get(0))          # → "login"

async for idx, val in await events.scan(0, 10):
    print(f"{idx}: {val}")
```

The class is also available from the sync mirror (`from redis_om import
RedisArray`) — drop `await` and the rest of the API is identical.

## Installation

`RedisArray` ships with `pyredis-om`; nothing extra to install.

## Indexed access

```python
import asyncio
from aredis_om import RedisArray, get_redis_connection


async def main():
    db = get_redis_connection()
    events = RedisArray(db, "demo:events")

    # ARSET — set contiguous values starting at the given index.
    created = await events.set(0, "login", "click", "purchase")
    print(f"created {created} slots")
    # > created 3 slots

    # ARGET — get a single value.
    val = await events.get(0)
    print(f"index 0: {val}")
    # > index 0: login

    # ARGETRANGE — all values in [start, end], nil for gaps.
    vals = await events.get_range(0, 2)
    print(f"range [0,2]: {vals}")
    # > range [0,2]: ['login', 'click', 'purchase']

    # ARMSET — set multiple index/value pairs at once.
    await events.mset({10: "logout", 11: "relogin"})

    # ARMGET — fetch several indices in one round trip.
    vals = await events.mget(0, 10, 11, 99)
    print(f"mget: {vals}")
    # > mget: ['login', 'logout', 'relogin', None]

    await db.delete("demo:events")


asyncio.run(main())
```

`get()` returns `None` for unset indices; `mget()` returns a list with
`None` for each missing slot.

## Sequential insertion (cursor)

Use `insert()` to append at the array's auto-advancing cursor. The
cursor starts at `0` and advances with every insert.

```python
import asyncio
from aredis_om import RedisArray, get_redis_connection


async def main():
    db = get_redis_connection()
    arr = RedisArray(db, "demo:cursor")

    # ARINSERT — append at the cursor.
    idx = await arr.insert("a", "b", "c")
    print(f"wrote through index {idx}")
    # > wrote through index 2

    # ARNEXT — the next index insert() would use.
    print(f"next: {await arr.next_index()}")
    # > next: 3

    # ARSEEK — reposition the cursor.
    await arr.seek(100)
    await arr.insert("x")            # → index 100
    print(f"after seek+insert: {await arr.next_index()}")
    # > after seek+insert: 101

    await db.delete("demo:cursor")


asyncio.run(main())
```

## Scanning

`scan()` returns only the **existing** elements in the range, as
`(index, value)` pairs:

```python
async for idx, val in await arr.scan(0, 100):
    print(f"{idx}: {val}")
```

Or with an upper bound on the number of results returned:

```python
pairs = await arr.scan(0, 10_000, limit=500)
```

The class accommodates both the **nested** `[[idx, val], ...]` layout
shipped with Redis 8.8+ and the **flat** `[idx, val, ...]` layout from
older preview builds — your code does not need to branch.

## Ring buffers

`ring(size, *values)` pushes values into a fixed-size circular buffer.
Once `size` items have been written, older items are overwritten.

```python
import asyncio
from aredis_om import RedisArray, get_redis_connection


async def main():
    db = get_redis_connection()
    readings = RedisArray(db, "demo:sensor:temp")

    # Keep the last 100 readings, push 3 new ones.
    last = await readings.ring(100, "36.5", "36.7", "36.6")
    print(f"wrote last idx={last}")
    # > wrote last idx=2

    # ARLASTITEMS — the N most recent items.
    print(f"latest 3: {await readings.last_items(3)}")
    # > latest 3: ['36.5', '36.7', '36.6']

    await db.delete("demo:sensor:temp")


asyncio.run(main())
```

## Aggregation (`AROP`)

Single-pass aggregates over a range, without loading the array into
memory:

```python
total = await readings.aggregate(0, last, "SUM")
print(f"sum: {total}")
# > sum: 109.8
```

Supported operations: `SUM`, `MIN`, `MAX`, `AND`, `OR`, `XOR`, `MATCH`,
`USED`. Pass a value for `MATCH` (the third argument to `aggregate()`).

## Text search (`ARGREP`)

`grep()` runs a server-side textual search across the array. Each
predicate is a `(type, pattern)` tuple where `type` is one of:

| Type | Behavior |
| --- | --- |
| `EXACT` | Strict equality (case-sensitive). |
| `MATCH` | Substring match. |
| `GLOB` | Redis-style glob (`*`, `?`, `[abc]`). |
| `RE` | Regex (server-compiled). |

Multiple predicates are combined with logical **OR**. Pass
`with_values=True` to return `[[index, value], ...]` instead of a flat
index list:

```python
import asyncio
from aredis_om import RedisArray, get_redis_connection


async def main():
    db = get_redis_connection()
    arr = RedisArray(db, "demo:grep")
    await arr.set(0, "hello world", "goodbye world", "hello again", "40.0")

    # Substring match — returns indices.
    indices = await arr.grep(0, 3, [("MATCH", "hello")])
    print(f"indices matching 'hello': {indices}")
    # > indices matching 'hello': [0, 2]

    # Glob — with values
    matches = await arr.grep(
        0, 3, [("GLOB", "*world*")], with_values=True
    )
    print(f"glob *world*: {matches}")
    # > glob *world*: [[0, 'hello world'], [1, 'goodbye world']]

    # Combined: world OR again
    matches = await arr.grep(
        0, 3,
        [("MATCH", "world"), ("MATCH", "again")],
        with_values=True,
    )
    print(f"OR predicates: {matches}")
    # > OR predicates: [[0, 'hello world'], [1, 'goodbye world'], [2, 'hello again']]

    await db.delete("demo:grep")


asyncio.run(main())
```

Pass `nocase=True` to make `MATCH`/`GLOB` case-insensitive.

## Deletion

```python
# ARDEL — delete specific indices.
deleted = await arr.delete_at(10, 11, 12)

# ARDELRANGE — delete all indices in [start, end].
deleted = await arr.delete_range(0, 99)
```

## Introspection

```python
# ARLEN — logical length (max index + 1).
length = await arr.length()

# ARCOUNT — number of non-empty elements.
count = await arr.count()

# ARINFO — array metadata. Pass full=True for per-slice stats.
info = await arr.info()
print(list(info.keys()))
# > ['count', 'len', 'next-insert-index', 'slices', ...]
```

## API reference

### `RedisArray(db, key)`

| Argument | Type | Description |
| --- | --- | --- |
| `db` | `redis.asyncio.Redis` (or sync `redis.Redis`) | Active Redis client. |
| `key` | `str` | The Redis key that holds the array. |

### Indexed access

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.set(index, *values)` | `int` | `ARSET`. Sets values contiguously starting at `index`. Returns the number of new slots. |
| `await arr.get(index)` | `str` or `None` | `ARGET`. |
| `await arr.mset(mapping)` | `int` | `ARMSET`. `mapping` is `{index: value, ...}`. |
| `await arr.mget(*indices)` | `list[str \| None]` | `ARMGET`. |
| `await arr.get_range(start, end)` | `list[str \| None]` | `ARGETRANGE`. `None` for gaps. |

### Iteration

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.scan(start, end, limit=None)` | `list[(int, str)]` | `ARSCAN`. Existing elements only. |

### Sequential insertion

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.insert(*values)` | `int` | `ARINSERT`. Appends at the cursor. Returns the last index used. |
| `await arr.next_index()` | `int` or `None` | `ARNEXT`. The cursor's current position. |
| `await arr.seek(index)` | `int` | `ARSEEK`. Repositions the cursor. |

### Ring buffer

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.ring(size, *values)` | `int` | `ARRING`. Circular buffer of `size` items. Returns the last index written. |
| `await arr.last_items(count, rev=False)` | `list[str]` | `ARLASTITEMS`. Most recent `count` items. |

### Deletion

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.delete_at(*indices)` | `int` | `ARDEL`. |
| `await arr.delete_range(start, end)` | `int` | `ARDELRANGE`. |

### Aggregation / search

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.aggregate(start, end, op, value=None)` | varies | `AROP`. `op` is `SUM`/`MIN`/`MAX`/`AND`/`OR`/`XOR`/`MATCH`/`USED`. |
| `await arr.grep(start, end, predicates, *, nocase=False, with_values=False, limit=None)` | varies | `ARGREP`. `predicates` is a list of `(type, pattern)` tuples. |

### Introspection

| Method | Returns | Notes |
| --- | --- | --- |
| `await arr.length()` | `int` | `ARLEN`. Logical length (`max_index + 1`). |
| `await arr.count()` | `int` | `ARCOUNT`. Number of non-empty elements. |
| `await arr.info(full=False)` | `dict` | `ARINFO`. Array metadata. |

## Full source

See [`aredis_om/model/array.py`][array-source] for the implementation
and [`tests/test_redis_array.py`][array-tests] for the full test suite
(15 tests).

[array-source]: https://github.com/XChikuX/redis-om-python/blob/main/aredis_om/model/array.py
[array-tests]: https://github.com/XChikuX/redis-om-python/blob/main/tests/test_redis_array.py