![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/XChikuX/redis-om-python?utm_source=oss&utm_medium=github&utm_campaign=XChikuX%2Fredis-om-python&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)

<div align="center">
  <br/>
  <br/>
  <img width="360" src="https://raw.githubusercontent.com/XChikuX/redis-om-python/main/images/logo.svg" alt="Redis OM" />
  <br/>
  <br/>
</div>

<p align="center">
    <p align="center">
        Object mapping, and more, for Redis and Python
    </p>
</p>

---

[![Version][version-svg]][package-url]
[![License][license-image]][license-url]
[![Build Status][ci-svg]][ci-url]

**Redis OM Python** makes it easy to model Redis data in your Python applications.

Install the package from PyPI as `pyredis-om`, then import `aredis_om` for the async API or `redis_om` for the generated sync mirror. This release targets **Pydantic v2**.

📚 **The full documentation lives in [`docs/`](docs/index.mdx).** This README is just the essentials.

<details>
  <summary><strong>Table of contents</strong></summary>

- [💡 Why Redis OM?](#-why-redis-om)
- [⚡ Why `execute_command`?](#-why-execute_command)
- [💻 Installation](#-installation)
- [🏁 Getting started](#-getting-started)
- [📇 Modeling your data](#-modeling-your-data)
- [🔎 Queries, embedded models, and GEO](#-queries-embedded-models-and-geo)
- [🧩 Pipelines and raw commands](#-pipelines-and-raw-commands)
- [📚 Documentation](#-documentation)
- [❤️ Contributing](#-contributing)
- [📝 License](#-license)

</details>

## 💡 Why Redis OM?

Redis OM provides high-level abstractions that make it easy to model and query data in Redis with modern Python applications.

The current release includes:

- Declarative object mapping for Redis objects
- Declarative secondary-index generation
- Fluent APIs for querying Redis
- Async-first APIs with a generated sync mirror
- Lazy `Meta.database` resolution, callable connection providers, runtime reassignment
- Default model TTLs via `Meta.default_ttl`
- Bulk fetches with `get_many()`, explicit pipeline composition
- Redis Cluster (`cluster=True` or `?cluster=true` in the URL)
- Embedded JSON sorting, GEO queries, vector similarity search (FLAT/HNSW)
- Embedded list containment queries (`Workspace.users << User(name="John")`)
- Comprehensive token escaping for TAG and TEXT fields
- GEO queries with `Coordinates` / `GeoFilter`, plus raw `GEO*` access — see [`docs/geo_queries.mdx`](docs/geo_queries.mdx)
- **AtomicCounter** backed by Redis 8.8 `INCREX` — see [`docs/atomic_counter.mdx`](docs/atomic_counter.mdx)
- **RedisArray** for Redis 8.8+ sparse, index-addressable arrays — see [`docs/redis_arrays.mdx`](docs/redis_arrays.mdx)
- **Hash field TTL** (`HEXPIRE` / `HGETEX` / `HGETDEL` / `HSETEX`) on `HashModel` for Redis 7.4+ / 8.0+ — see [`docs/hash_field_ttl.mdx`](docs/hash_field_ttl.mdx)
- **RedisStream** wrapper around the `X*` family with 8.2/8.4/8.6/8.8 extensions (`XACKDEL`, `XDELEX`, `XNACK`, `IDMP`, `XREADGROUP ... CLAIM`) — see [`docs/streams.mdx`](docs/streams.mdx)
- **AtomicString + MSETEX** (`SET IFEQ` / `IFNE`, `DELEX`, `DIGEST`, bulk `MSETEX`) for Redis 8.4+ — see [`docs/atomic_strings.mdx`](docs/atomic_strings.mdx)
- **OpenTelemetry observability** wrapper around redis-py 8.0 instrumentation — see [`docs/observability.mdx`](docs/observability.mdx)

## ⚡ Why `execute_command`?

This fork deliberately does not wrap every redis-py high-level binding (`db.ft(...).search(...)`, `db.geoadd(...)`, etc.). For hot paths like RediSearch, `INCREX`, and the `AR*` array commands we call `db.execute_command("FT.SEARCH", ...)` (or `"GEOADD"`, `"INCREX"`, ...) directly.

| Reason | What it means in practice |
| --- | --- |
| **Faster** | No per-call method dispatch or argument coercion; the command name and args go straight to the socket. |
| **More predictable** | Argument order matches the [Redis command reference](https://redis.io/commands/) exactly. `db.geoadd(... nx=True, xx=True)` raised in some redis-py 5.x versions — `execute_command` doesn't. |
| **Universal** | Works the moment Redis ships a command. `INCREX` (Redis 8.8+), the `AR*` family (8.8+ preview), and `FT.AGGREGATE WITHCURSOR` options all worked here before redis-py shipped typed bindings. |
| **Cluster-safe** | The same call works on `redis.Redis` and `redis.RedisCluster` with no API differences. |

The cost is that the caller is responsible for getting the argument order right. See [`docs/pipelines.mdx`](docs/pipelines.mdx) for tested examples.

## 💻 Installation

```sh
# pip
pip install pyredis-om

# uv
uv add pyredis-om
```

## 🏁 Getting started

### Start Redis

```sh
docker run -p 6379:6379 redis:8-alpine

export REDIS_OM_URL="redis://localhost:6379?decode_responses=True"
```

The `redis:8-alpine` image includes the RedisJSON and RediSearch modules Redis OM needs for JSON and search features. See [`docs/redis_modules.mdx`](docs/redis_modules.mdx) for other options including Redis Enterprise and OSS-only setups.

### Connect

```python
from aredis_om import get_redis_connection

redis_conn = get_redis_connection()
# Or pass an explicit URL:
redis_conn = get_redis_connection(url="redis://localhost:6379?decode_responses=True")
```

For Redis Cluster, see [`docs/cluster.mdx`](docs/cluster.mdx). For RESP2/RESP3 protocol negotiation, see [`docs/protocol.mdx`](docs/protocol.mdx).

### Define, save, query

```python
from redis_om import Field, HashModel, Migrator


class Customer(HashModel):
    first_name: str
    last_name: str = Field(index=True)
    age: int = Field(index=True)


Migrator().run()

andrew = Customer(first_name="Andrew", last_name="Brookins", age=38)
andrew.save()

# Reload by primary key
Customer.get(andrew.pk)

# Query — `<<` is the IN operator for TAG fields
Customer.find(Customer.last_name == "Brookins").all()
Customer.find(Customer.age >= 35).sort_by("age").page(offset=0, limit=10)
```

That's the whole shape. Full reference: [`docs/models.mdx`](docs/models.mdx), [`docs/queries.mdx`](docs/queries.mdx).

## 📇 Modeling your data

Two model classes cover most needs:

```python
from typing import Optional
from redis_om import HashModel, JsonModel, Field, EmbeddedJsonModel


class Customer(HashModel):
    first_name: str
    last_name: str = Field(index=True)
    age: int = Field(index=True)
    email: Optional[str] = Field(index=True, default=None)
```

- `HashModel` — flat, fast, stored as a Redis hash. **No `List`/`Dict` fields.**
- `JsonModel` — for nested structures, embedded models, `List[T]`/`Dict[K, V]`.
- `EmbeddedJsonModel` — a sub-document for `JsonModel.address` style fields.

Full details, including the lazy `Meta.database`, `Meta.default_ttl`, vector fields, and embedded `List[EmbeddedJsonModel]`: [`docs/models.mdx`](docs/models.mdx).

## 🔎 Queries, embedded models, and GEO

```python
# Equality, range, AND/OR/NOT
Customer.find(Customer.age >= 35).all()
Customer.find(
    (Customer.last_name == "Brookins") | (Customer.first_name == "Kim")
).all()

# IN / NOT IN on TAG fields
Customer.find(Customer.last_name << ["Brookins", "Smith"]).all()
Customer.find(Customer.last_name != "Brookins").all()

# Embedded JsonModel fields
Customer.find(Customer.address.city == "San Antonio").all()

# GEO queries
from redis_om import Coordinates, GeoFilter

class Store(HashModel):
    name: str = Field(index=True)
    coordinates: Coordinates = Field(index=True)

Store.find(
    Store.coordinates == GeoFilter(
        longitude=-73.9851, latitude=40.7589, radius=2, unit="mi",
    )
).all()
```

Full syntax — sorting, pagination, cursors, KNN vector search, prefix matches, embedded list containment, GEO + TAG combinations: [`docs/queries.mdx`](docs/queries.mdx), [`docs/geo_queries.mdx`](docs/geo_queries.mdx).

## 🧩 Pipelines and raw commands

Compose model queries with raw Redis commands in one round trip:

```python
from aredis_om import HashModel, Field

class Customer(HashModel):
    first_name: str
    last_name: str = Field(index=True)


# Bulk save + atomic counter increment, in one round trip
pipe = Customer.db().pipeline(transaction=False)
pipe.incr("metrics:signups")
await Customer.add(new_customers, pipeline=pipe)
results = await pipe.execute()
```

Why `execute_command` (and not the redis-py typed bindings): see [⚡ Why `execute_command`?](#-why-execute_command) above. Full pipeline patterns — bulk fetches + secondary key lookups, GEO model + raw `GEO*` storage, KNN + stream publish, rate limiting + writes, cluster hash tags: [`docs/pipelines.mdx`](docs/pipelines.mdx).

## 📚 Documentation

**The full documentation lives in [`docs/`](docs/index.mdx).** Highlights:

- **Getting started** — [Overview](docs/index.mdx), [Getting Started](docs/getting_started.mdx), [Connecting to Redis](docs/connections.mdx)
- **Models and queries** — [Models and Fields](docs/models.mdx), [Queries and Vector Search](docs/queries.mdx), [Validation](docs/validation.mdx), [Error Messages](docs/errors.mdx)
- **Operations** — [Bulk Operations](docs/bulk_operations.mdx), [Streams](docs/streams.mdx), [Geospatial Queries](docs/geo_queries.mdx), [Hash Field Expiration](docs/hash_field_ttl.mdx), [Pipelines and `execute_command`](docs/pipelines.mdx), [Migrations](docs/migrations.mdx)
- **Redis 8.x features** — [AtomicCounter (`INCREX`)](docs/atomic_counter.mdx), [Redis Arrays](docs/redis_arrays.mdx), [Atomic Strings (`CAS`, `MSETEX`)](docs/atomic_strings.mdx), [OpenTelemetry Observability](docs/observability.mdx)
- **Deployment** — [Redis Cluster](docs/cluster.mdx), [Protocol Selection](docs/protocol.mdx), [Redis Modules](docs/redis_modules.mdx), [FastAPI Integration](docs/fastapi_integration.mdx)
- **Reference** — [Upstream Issues Fixed](docs/upstream_fixes.mdx)

## ❤️ Contributing

See [`CLAUDE.md`](CLAUDE.md) for the contributor workflow (async source of truth, `make sync` regeneration), and [`SECURITY_REVIEW.md`](SECURITY_REVIEW.md) for design notes. [Open an issue on GitHub](https://github.com/XChikuX/redis-om-python/issues/new) to get started.

Current local coverage baseline: **88% overall** across `aredis_om/` and the generated `redis_om/` mirror, with **1100+ passing async + sync tests**. RESP2 vs RESP3 parity is exercised end-to-end by `tests/test_protocol_compat.py`.

## 📝 License

Redis OM uses the [MIT license][license-url].

<!-- Badges -->

[version-svg]: https://img.shields.io/pypi/v/pyredis-om?style=flat-square
[package-url]: https://pypi.org/project/pyredis-om/
[ci-svg]: https://img.shields.io/github/actions/workflow/status/XChikuX/redis-om-python/ci.yml?branch=main&style=flat-square
[ci-url]: https://github.com/XChikuX/redis-om-python/actions
[license-image]: https://img.shields.io/github/license/XChikuX/redis-om-python?style=flat-square
[license-url]: LICENSE
[redisearch-url]: https://redis.io/docs/stack/search/
[redis-json-url]: https://redis.io/docs/stack/json/
[redis-enterprise-url]: https://redis.com/redis-enterprise/
[pydantic-url]: https://docs.pydantic.dev/