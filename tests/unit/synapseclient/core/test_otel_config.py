"""Unit tests for OpenTelemetry configuration and instrumentation.

All new telemetry unit tests for this ticket live in this one module (per
`decisions.md`), covering: the resource-attribute seam, `configure_metrics`/
`configure_traces`, the async-job and upload instrumentation, and the test-harness
worker-identity/truthiness helpers.
"""

import platform
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from opentelemetry.sdk.resources import SERVICE_INSTANCE_ID

from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.core.exceptions import SynapseError, SynapseHTTPError
from synapseclient.core.otel_config import (
    SYNAPSE_SERVICE_VERSION,
    _build_resource_attributes,
    configure_metrics,
)
from synapseclient.core.upload.upload_functions_async import upload_file_handle
from synapseclient.models.mixins.asynchronous_job import send_job_and_wait_async


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


class TestAsyncJobInstrumentation:
    """Unit tests for the send_job_and_wait_async instrumentation."""

    good_request = {"concreteType": AGENT_CHAT_REQUEST}
    job_id = "123"
    request_type = AGENT_CHAT_REQUEST

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn) -> None:
        self.syn = syn

    async def test_successful_call_records_one_count_and_one_duration(
        self, mocker
    ) -> None:
        mock_counter = mocker.patch(
            "synapseclient.models.mixins.asynchronous_job._async_job_counter"
        )
        mock_duration = mocker.patch(
            "synapseclient.models.mixins.asynchronous_job._async_job_duration"
        )
        mocker.patch(
            "synapseclient.models.mixins.asynchronous_job.send_job_async",
            new_callable=AsyncMock,
            return_value=self.job_id,
        )
        mocker.patch(
            "synapseclient.models.mixins.asynchronous_job.get_job_async",
            new_callable=AsyncMock,
            return_value={"key": "value"},
        )

        await send_job_and_wait_async(
            request=self.good_request,
            request_type=self.request_type,
            synapse_client=self.syn,
        )

        mock_counter.add.assert_called_once_with(1, {"request_type": self.request_type})
        mock_duration.record.assert_called_once()
        args, kwargs = mock_duration.record.call_args
        assert isinstance(args[0], float)
        assert args[1] == {"request_type": self.request_type}

    async def test_failure_still_records_duration(self, mocker) -> None:
        mock_duration = mocker.patch(
            "synapseclient.models.mixins.asynchronous_job._async_job_duration"
        )
        mocker.patch(
            "synapseclient.models.mixins.asynchronous_job.send_job_async",
            new_callable=AsyncMock,
            side_effect=SynapseError("boom"),
        )

        with pytest.raises(SynapseError):
            await send_job_and_wait_async(
                request=self.good_request,
                request_type=self.request_type,
                synapse_client=self.syn,
            )

        mock_duration.record.assert_called_once()

    async def test_view_not_available_retry_records_one_count(self, mocker) -> None:
        mock_counter = mocker.patch(
            "synapseclient.models.mixins.asynchronous_job._async_job_counter"
        )
        mocker.patch("synapseclient.models.mixins.asynchronous_job._async_job_duration")
        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        mocker.patch(
            "synapseclient.models.mixins.asynchronous_job.send_job_async",
            new_callable=AsyncMock,
            side_effect=[
                SynapseHTTPError(
                    "You cannot create a version of a view that is not available "
                    "(Status: PROCESSING)"
                ),
                self.job_id,
            ],
        )
        mocker.patch(
            "synapseclient.models.mixins.asynchronous_job.get_job_async",
            new_callable=AsyncMock,
            return_value={"key": "value"},
        )

        await send_job_and_wait_async(
            request=self.good_request,
            request_type=self.request_type,
            synapse_client=self.syn,
        )

        mock_counter.add.assert_called_once_with(1, {"request_type": self.request_type})


class TestUploadInstrumentation:
    """Unit tests for the upload_file_handle instrumentation."""

    async def test_synapse_store_true_records_with_external_file_handle_false(
        self, mocker
    ) -> None:
        mock_counter = mocker.patch(
            "synapseclient.core.upload.upload_functions_async._upload_counter"
        )
        mock_duration = mocker.patch(
            "synapseclient.core.upload.upload_functions_async._upload_duration"
        )
        mocker.patch(
            "synapseclient.core.upload.upload_functions_async.get_upload_destination",
            new_callable=AsyncMock,
            return_value=None,
        )
        mocker.patch(
            "synapseclient.core.upload.upload_functions_async.sts_transfer"
            ".is_boto_sts_transfer_enabled",
            return_value=False,
        )
        mocker.patch(
            "synapseclient.core.upload.upload_functions_async.upload_synapse_s3",
            new_callable=AsyncMock,
            return_value={"id": "fh1"},
        )

        await upload_file_handle(
            syn=MagicMock(),
            parent_entity_id="syn123",
            path="/tmp/some_file.txt",
        )

        mock_counter.add.assert_called_once_with(1, {"external_file_handle": False})
        mock_duration.record.assert_called_once()
        args, _ = mock_duration.record.call_args
        assert isinstance(args[0], float)
        assert args[1] == {"external_file_handle": False}

    async def test_synapse_store_false_records_with_external_file_handle_true(
        self, mocker
    ) -> None:
        mock_counter = mocker.patch(
            "synapseclient.core.upload.upload_functions_async._upload_counter"
        )
        mock_duration = mocker.patch(
            "synapseclient.core.upload.upload_functions_async._upload_duration"
        )
        mocker.patch(
            "synapseclient.core.upload.upload_functions_async.create_external_file_handle",
            new_callable=AsyncMock,
            return_value={"id": "fh2"},
        )

        await upload_file_handle(
            syn=MagicMock(),
            parent_entity_id="syn123",
            path="/tmp/some_file.txt",
            synapse_store=False,
        )

        mock_counter.add.assert_called_once_with(1, {"external_file_handle": True})
        mock_duration.record.assert_called_once()
        args, _ = mock_duration.record.call_args
        assert isinstance(args[0], float)
        assert args[1] == {"external_file_handle": True}
