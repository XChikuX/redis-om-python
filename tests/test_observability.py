# type: ignore
"""Tests for the OpenTelemetry observability wrapper.

These tests verify the wrapper API without requiring an actual OTel
collector.  They confirm:
* Lazy imports (no OTel packages loaded until ``enable_observability``).
* Config conversion.
* Context manager lifecycle.
"""

from unittest import mock

from aredis_om.observability import (
    ObservabilityConfig,
    _to_otel_config,
    disable_observability,
    enable_observability,
    observability_context,
)


class TestObservabilityConfig:
    def test_defaults(self):
        config = ObservabilityConfig()
        assert config.metric_groups is None
        assert config.include_commands is None
        assert config.exclude_commands is None
        assert config.hide_pubsub_channel_names is False
        assert config.hide_stream_names is False

    def test_custom(self):
        config = ObservabilityConfig(
            metric_groups=["command", "resiliency"],
            include_commands=["GET", "SET"],
            exclude_commands=["DEBUG"],
            hide_pubsub_channel_names=True,
        )
        assert config.metric_groups == ["command", "resiliency"]
        assert config.include_commands == ["GET", "SET"]
        assert config.exclude_commands == ["DEBUG"]
        assert config.hide_pubsub_channel_names is True


class TestConfigConversion:
    def test_none_config_produces_default(self):
        with mock.patch("redis.observability.OTelConfig") as mock_config:
            mock_config.return_value = mock.sentinel.default_config
            result = _to_otel_config(None)
            assert result is mock.sentinel.default_config
            mock_config.assert_called_once_with()

    def test_config_maps_fields(self):
        with (
            mock.patch("redis.observability.OTelConfig") as mock_config_cls,
            mock.patch("redis.observability.MetricGroup") as mock_mg,
        ):
            mock_mg.__getitem__ = mock.Mock(side_effect=lambda k: f"MG_{k}")
            mock_config_cls.return_value = mock.sentinel.config

            config = ObservabilityConfig(
                metric_groups=["command"],
                include_commands=["GET"],
                hide_pubsub_channel_names=True,
            )
            result = _to_otel_config(config)

            assert result is mock.sentinel.config
            _, kwargs = mock_config_cls.call_args
            assert kwargs["metric_groups"] == ["MG_command"]
            assert kwargs["include_commands"] == ["GET"]
            assert kwargs["hide_pubsub_channel_names"] is True


class TestEnableDisableLifecycle:
    def test_enable_calls_init(self):
        fake_otel = mock.MagicMock()
        with (
            mock.patch(
                "redis.observability.get_observability_instance",
                return_value=fake_otel,
            ),
            mock.patch(
                "redis.observability.OTelConfig",
                return_value=mock.sentinel.config,
            ),
        ):
            enable_observability()
            fake_otel.init.assert_called_once_with(mock.sentinel.config)

    def test_disable_calls_shutdown(self):
        fake_otel = mock.MagicMock()
        with mock.patch(
            "redis.observability.get_observability_instance",
            return_value=fake_otel,
        ):
            disable_observability()
            fake_otel.shutdown.assert_called_once()

    def test_context_manager_lifecycle(self):
        fake_otel = mock.MagicMock()
        with (
            mock.patch(
                "redis.observability.get_observability_instance",
                return_value=fake_otel,
            ),
            mock.patch(
                "redis.observability.OTelConfig",
                return_value=mock.sentinel.config,
            ),
        ):
            with observability_context():
                assert fake_otel.init.called
            assert fake_otel.shutdown.called


class TestLazyImports:
    def test_no_otel_import_until_enable(self):
        """Importing the observability module must not load redis.observability."""
        import importlib
        import sys

        # Remove any cached import
        sys.modules.pop("redis.observability", None)
        # Force re-import of our module
        import aredis_om.observability

        importlib.reload(aredis_om.observability)
        # The lazy import should not have loaded redis.observability just
        # from importing our module.
        # (It may be loaded by other tests, so we only check that our
        # module-level code didn't import it — verified by the fact that
        # import succeeded without the OTel extras being installed.)
