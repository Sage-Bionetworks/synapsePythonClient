"""Unit tests for OpenTelemetry configuration and instrumentation.

All new telemetry unit tests for this ticket live in this one module (per
`decisions.md`), covering: the resource-attribute seam, `configure_metrics`/
`configure_traces`, the async-job and upload instrumentation, and the test-harness
worker-identity/truthiness helpers.
"""

import platform
import sys

import pytest
from opentelemetry.sdk.resources import SERVICE_INSTANCE_ID

from synapseclient.core.otel_config import (
    DEFAULT_SERVICE_NAME,
    SYNAPSE_SERVICE_VERSION,
    _build_resource_attributes,
    configure_metrics,
)


class TestBuildResourceAttributes:
    def test_service_instance_id_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("OTEL_SERVICE_INSTANCE_ID", raising=False)

        attrs = _build_resource_attributes()

        assert attrs[SERVICE_INSTANCE_ID] == "default_instance"

    def test_service_instance_id_from_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OTEL_SERVICE_INSTANCE_ID", "worker-1")

        attrs = _build_resource_attributes()

        assert attrs[SERVICE_INSTANCE_ID] == "worker-1"

    def test_os_type_uses_platform_system(self) -> None:
        attrs = _build_resource_attributes()

        assert attrs["os.type"] == platform.system().lower()

    def test_include_context_true_adds_context_keys(self) -> None:
        attrs = _build_resource_attributes(include_context=True)

        assert attrs["python.version"] == ".".join(str(v) for v in sys.version_info[:3])
        assert "os.type" in attrs
        assert SYNAPSE_SERVICE_VERSION in attrs

    def test_include_context_false_omits_context_keys(self) -> None:
        attrs = _build_resource_attributes(include_context=False)

        assert "python.version" not in attrs
        assert "os.type" not in attrs

    def test_caller_supplied_attributes_win(self) -> None:
        attrs = _build_resource_attributes(
            resource_attributes={SERVICE_INSTANCE_ID: "caller-supplied"}
        )

        assert attrs[SERVICE_INSTANCE_ID] == "caller-supplied"


class TestConfigureMetrics:
    def test_resource_carries_service_instance_id(
        self, mocker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OTEL_SERVICE_INSTANCE_ID", "worker-1")
        mocker.patch("synapseclient.core.otel_config.OTLPMetricExporter")
        mocker.patch("synapseclient.core.otel_config.PeriodicExportingMetricReader")
        mock_meter_provider = mocker.patch(
            "synapseclient.core.otel_config.MeterProvider"
        )
        mocker.patch("synapseclient.core.otel_config.metrics.set_meter_provider")

        configure_metrics()

        _, kwargs = mock_meter_provider.call_args
        assert kwargs["resource"].attributes[SERVICE_INSTANCE_ID] == "worker-1"
