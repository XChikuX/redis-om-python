"""OpenTelemetry observability support for redis-om connections.

Wraps redis-py 8.0's native OTel instrumentation behind a simple opt-in
API.  Zero cost when not enabled: the ``redis.observability`` module is
imported lazily only inside :func:`enable_observability`.

Usage::

    from aredis_om import enable_observability

    # 1. Configure the OpenTelemetry MeterProvider first (your app code).
    # 2. Enable redis-py instrumentation:
    enable_observability()

    # 3. Use models as usual — metrics are collected automatically.
    await model.save()

Requires: ``pip install redis[otel]``
"""

from contextlib import contextmanager
from typing import Any, List, Optional, Sequence, Union


class ObservabilityConfig:
    """Declarative configuration for redis-py OTel metrics.

    This is a plain data container that is converted to
    ``redis.observability.OTelConfig`` lazily inside
    :func:`enable_observability`.  Keeping it separate avoids importing
    the OTel stack until the user explicitly opts in.
    """

    def __init__(
        self,
        metric_groups: Optional[Sequence[str]] = None,
        include_commands: Optional[Sequence[str]] = None,
        exclude_commands: Optional[Sequence[str]] = None,
        hide_pubsub_channel_names: bool = False,
        hide_stream_names: bool = False,
    ):
        self.metric_groups = metric_groups
        self.include_commands = include_commands
        self.exclude_commands = exclude_commands
        self.hide_pubsub_channel_names = hide_pubsub_channel_names
        self.hide_stream_names = hide_stream_names


def _to_otel_config(config: Optional[ObservabilityConfig]):
    """Convert :class:`ObservabilityConfig` → ``redis.observability.OTelConfig``.

    Imports the OTel classes lazily so the cost is paid only when
    observability is actually enabled.
    """
    from redis.observability import MetricGroup, OTelConfig

    if config is None:
        return OTelConfig()

    kwargs: dict = {}
    if config.metric_groups is not None:
        kwargs["metric_groups"] = [
            MetricGroup[g] if isinstance(g, str) else g for g in config.metric_groups
        ]
    if config.include_commands is not None:
        kwargs["include_commands"] = list(config.include_commands)
    if config.exclude_commands is not None:
        kwargs["exclude_commands"] = list(config.exclude_commands)
    if config.hide_pubsub_channel_names:
        kwargs["hide_pubsub_channel_names"] = True
    if config.hide_stream_names:
        kwargs["hide_stream_names"] = True

    return OTelConfig(**kwargs)


def enable_observability(config: Optional[ObservabilityConfig] = None):
    """Enable redis-py's native OpenTelemetry metrics collection.

    Call **once** at application startup, after configuring the
    OpenTelemetry ``MeterProvider``.  All subsequent Redis operations
    through redis-om connections are instrumented automatically.

    Requires ``pip install redis[otel]``.

    Args:
        config: Optional :class:`ObservabilityConfig`.  When ``None``
            redis-py defaults are used (``CONNECTION_BASIC | RESILIENCY``
            metric groups).

    Raises:
        ImportError: If ``redis[otel]`` extras are not installed.
    """
    from redis.observability import get_observability_instance

    otel_config = _to_otel_config(config)
    otel = get_observability_instance()
    otel.init(otel_config)


def disable_observability():
    """Shut down OTel metrics collection and flush pending data."""
    from redis.observability import get_observability_instance

    otel = get_observability_instance()
    otel.shutdown()


@contextmanager
def observability_context(config: Optional[ObservabilityConfig] = None):
    """Context manager that enables observability for a scoped block.

    Automatically calls :func:`disable_observability` (flush + shutdown)
    on exit.

    Example::

        with observability_context():
            await model.save()
    """
    enable_observability(config)
    try:
        yield
    finally:
        disable_observability()
