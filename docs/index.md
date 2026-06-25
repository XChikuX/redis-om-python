# Redis OM for Python

Welcome! This is the documentation for redis-om-python.

A declarative, async-first object mapper for Redis built on [Pydantic v2](https://docs.pydantic.dev/),
with support for rich queries via RediSearch, JSON-backed embedded models,
vector similarity search (KNN), bulk operations, atomic counters
(`INCREX`), sparse Redis Arrays, and OpenTelemetry observability.

## Getting Started

- [Getting Started tutorial](getting_started.md) — install Redis OM, define a model, save and query data.
- [Connecting to Redis](connections.md) — URL configuration, clusters, RESP2/RESP3.

## Core features

- [Models and Fields](models.md) — `HashModel`, `JsonModel`, `EmbeddedJsonModel`, `Field`, the `Meta` object, and vector fields.
- [Validation](validation.md) — Pydantic-backed validation, strict types, custom validators.
- [Error Messages](errors.md) — common errors and how to fix them.

## Redis 8.8+ features

- [AtomicCounter (`INCREX`)](atomic_counter.md) — atomic increment with bounds, saturation, expiration, and `ENX`. Useful for rate limiters and capped counters.
- [Redis Arrays](redis_arrays.md) — sparse, index-addressable arrays with ring-buffer, aggregate (`AROP`), and inline grep (`ARGREP`) operations.
- [OpenTelemetry Observability](observability.md) — opt-in OTel metrics via redis-py 8.0 instrumentation.

## Querying and Vector Search

Learn how to query models with expressions, sort/paginate, and run vector
similarity searches in [getting_started.md#querying-for-models-with-expressions](getting_started.md#querying-for-models-with-expressions)
and the dedicated KNN/vector section in [models.md#vector-fields-for-similarity-search](models.md#vector-fields-for-similarity-search).

## Redis Modules

Read how to get the RediSearch and RedisJSON modules at [redis_modules.md](redis_modules.md).

## FastAPI Integration

Redis OM is designed to integrate with the FastAPI web framework. See how this
works at [fastapi_integration.md](fastapi_integration.md).
