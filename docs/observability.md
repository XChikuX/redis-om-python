# OpenTelemetry Observability

`pyredis-om` ships a thin wrapper around redis-py 8.0's native
OpenTelemetry instrumentation. When you opt in, every Redis command
issued by a redis-om connection is automatically timed, counted, and
exported via your configured `MeterProvider`.

**Zero cost when disabled.** The OpenTelemetry stack is imported lazily
inside `enable_observability()`, so applications that never call it
don't pay any import-time overhead.

## Installation

```sh
pip install 'pyredis-om[otel]'
# or, with uv:
uv add 'pyredis-om[otel]'
```

This installs the `opentelemetry-api` and `opentelemetry-sdk` packages
that redis-py's instrumentation depends on.

## Quick start

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)

from aredis_om import enable_observability, HashModel, Migrator


# 1. Set up the OTel MeterProvider exactly the way your app normally does.
provider = MeterProvider(
    metric_readers=[
        PeriodicExportingMetricReader(
            ConsoleMetricExporter(), export_interval_millis=60_000
        ),
    ],
)
metrics.set_meter_provider(provider)

# 2. Enable redis-py instrumentation.  Must be called AFTER the
#    MeterProvider is configured and BEFORE any Redis commands run.
enable_observability()

# 3. Use Redis OM as usual — metrics are recorded automatically.
class Customer(HashModel):
    first_name: str
    last_name: str
    email: str


await Migrator().run()
await Customer(first_name="Andrew", last_name="Brookins", email="a@example.com").save()
```

After `enable_observability()` runs, the configured
`PeriodicExportingMetricReader` will start emitting a small set of
metrics on its interval — typically one per command family plus a
`redis.connection.*` family.

## What metrics are recorded?

The instrumentation wraps every Redis call with three OpenTelemetry
instruments (selected via `metric_groups=`):

| Metric group | What it tracks |
| --- | --- |
| `CONNECTION_BASIC` | Connection acquisition, release, and creation. |
| `RESILIENCY` | Retries, timeouts, and circuit-breaker actions (where used). |
| `POOL` | Connection-pool utilization and waits. |
| `COMMAND` | Per-command timing and counts. |
| `NETWORK` | Bytes sent / received. |
| `PUBLISH` | Pub/Sub publishes and receives. |

The default when you call `enable_observability()` with no config is
`CONNECTION_BASIC | RESILIENCY` (cheap, no per-command label
cardinality).

## Configuration

```python
from aredis_om import enable_observability, ObservabilityConfig


# Full options:
config = ObservabilityConfig(
    # Restrict to specific command groups.  Defaults to
    # CONNECTION_BASIC | RESILIENCY.
    metric_groups=["CONNECTION_BASIC", "RESILIENCY", "COMMAND"],

    # Record metrics only for these commands (case-insensitive).
    include_commands=["get", "set", "hset", "hgetall"],

    # Never record metrics for these commands (takes precedence
    # over include_commands).
    exclude_commands=["ping"],

    # Don't include Pub/Sub channel names in labels (avoid leaking
    # sensitive channel identifiers).
    hide_pubsub_channel_names=True,

    # Don't include stream names in label values.
    hide_stream_names=True,
)

enable_observability(config)
```

You can use `include_commands=` / `exclude_commands=` to limit
cardinality: in production, recording `COMMAND` metrics for every
Redis call generates a separate label per command name, which is fine
for most apps but expensive if your code calls hundreds of distinct
commands.

## Toggling off

Call `disable_observability()` to flush and shut down the redis-py
instrumentation. After this call, no further metrics are recorded:

```python
from aredis_om import disable_observability

disable_observability()
```

## Scoped use

The `observability_context()` context manager enables metrics for a
specific block and disables them on exit. This is handy for tests,
one-off scripts, and benchmarks that want metrics around a single
operation:

```python
import asyncio
from aredis_om import AtomicCounter, observability_context, get_redis_connection


async def main():
    db = get_redis_connection()
    counter = AtomicCounter(db, "demo:scoped")
    with observability_context():
        for _ in range(100):
            await counter.incr()
    # Metrics were collected for the 100 increments above; outside the
    # `with` block, no metrics are recorded.


asyncio.run(main())
```

`observability_context(config=None)` accepts the same
`ObservabilityConfig` as `enable_observability()`.

## Working without a configured `MeterProvider`

If you call `enable_observability()` without first configuring a
`MeterProvider`, redis-py falls back to a no-op exporter. The
instrumentation still wraps the Redis calls (the overhead is
negligible), but the metrics are dropped on the floor. This is useful
in development: code paths work the same in production and on your
laptop.

```python
# No MeterProvider configured — instrumentation is "enabled" but
# metrics are not exported anywhere.
enable_observability()
```

## Combining with FastAPI

If you already wire OpenTelemetry into FastAPI, redis-om picks up
your existing `MeterProvider` automatically:

```python
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry import metrics

from aredis_om import enable_observability

# Your existing setup ...
app = FastAPI()
FastAPIInstrumentor.instrument_app(app)
# ... configure MeterProvider ...

# Now turn on Redis OM instrumentation:
enable_observability()
```

## Full source

See [`aredis_om/observability.py`][otel-source] for the implementation
and [`tests/test_observability.py`][otel-tests] for the full test suite
(7 tests).

[otel-source]: https://github.com/XChikuX/redis-om-python/blob/main/aredis_om/observability.py
[otel-tests]: https://github.com/XChikuX/redis-om-python/blob/main/tests/test_observability.py