# Redis OM for Python

Welcome! This is the documentation for redis-om-python.

A declarative, async-first object mapper for Redis built on [Pydantic v2](https://docs.pydantic.dev/),
with support for rich queries via RediSearch, JSON-backed embedded models,
vector similarity search (KNN), and bulk operations.

## Getting Started

Read the Getting Started tutorial at [getting_started.md](getting_started.md).

## Connecting to Redis

Read about connecting to Redis at [connections.md](connections.md).

## Models and Fields

Learn how to create model instances and define fields in [models.md](models.md).

## Validating Data

Read about how to use Redis OM models to validate data in [validation.md](validation.md).

## Querying and Vector Search

Learn how to query models with expressions, sort/paginate, and run vector
similarity searches in [getting_started.md#querying-for-models-with-expressions](getting_started.md#querying-for-models-with-expressions)
and the dedicated KNN/vector section.

## Redis Modules

Read how to get the RediSearch and RedisJSON modules at [redis_modules.md](redis_modules.md).

## FastAPI Integration

Redis OM is designed to integrate with the FastAPI web framework. See how this
works at [fastapi_integration.md](fastapi_integration.md).

## Error Messages

Get help with (some of) the error messages you might see from Redis OM:
[errors.md](errors.md)
