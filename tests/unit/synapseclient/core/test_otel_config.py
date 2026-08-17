"""Unit tests for OpenTelemetry configuration and instrumentation.

All new telemetry unit tests for this ticket live in this one module (per
`decisions.md`), covering: the resource-attribute seam, `configure_metrics`/
`configure_traces`, the async-job and upload instrumentation, and the test-harness
worker-identity/truthiness helpers.
"""

import logging
import platform
import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from opentelemetry.sdk.resources import SERVICE_INSTANCE_ID

from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseHTTPError,
    SynapseTimeoutError,
)
from synapseclient.core.otel_config import (
    SYNAPSE_SERVICE_VERSION,
    _build_resource_attributes,
    configure_metrics,
)
from synapseclient.core.upload.upload_functions_async import upload_file_handle
from synapseclient.models.mixins.asynchronous_job import send_job_and_wait_async
from tests.integration.helpers import (
    ExportFailureRecorder,
    export_failure_summary,
    telemetry_enabled,
    worker_telemetry_env,
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

        expected_attributes = {
            "request_type": self.request_type,
            "outcome": "success",
        }
        mock_counter.add.assert_called_once_with(1, expected_attributes)
        mock_duration.record.assert_called_once()
        args, kwargs = mock_duration.record.call_args
        assert isinstance(args[0], float)
        assert args[1] == expected_attributes
        # Same dict instance passed to both instruments.
        assert mock_counter.add.call_args[0][1] is args[1]

    async def test_failure_records_error_outcome_on_both_instruments(
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
            side_effect=SynapseError("boom"),
        )

        with pytest.raises(SynapseError):
            await send_job_and_wait_async(
                request=self.good_request,
                request_type=self.request_type,
                synapse_client=self.syn,
            )

        expected_attributes = {"request_type": self.request_type, "outcome": "error"}
        mock_counter.add.assert_called_once_with(1, expected_attributes)
        mock_duration.record.assert_called_once()
        args, _ = mock_duration.record.call_args
        assert args[1] == expected_attributes

    async def test_timeout_records_timeout_outcome_on_both_instruments(
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
            side_effect=SynapseTimeoutError("timed out"),
        )

        with pytest.raises(SynapseTimeoutError):
            await send_job_and_wait_async(
                request=self.good_request,
                request_type=self.request_type,
                synapse_client=self.syn,
            )

        expected_attributes = {
            "request_type": self.request_type,
            "outcome": "timeout",
        }
        mock_counter.add.assert_called_once_with(1, expected_attributes)
        mock_duration.record.assert_called_once()
        args, _ = mock_duration.record.call_args
        assert args[1] == expected_attributes

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

        mock_counter.add.assert_called_once_with(
            1, {"request_type": self.request_type, "outcome": "success"}
        )


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


class TestTelemetryEnabled:
    """Unit tests for tests.integration.helpers.telemetry_enabled."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, False),
            ("", False),
            ("0", False),
            ("false", False),
            ("False", False),
            ("no", False),
            ("off", False),
            ("1", True),
            ("true", True),
            ("TRUE", True),
            ("yes", True),
            ("on", True),
            ("ON", True),
        ],
    )
    def test_telemetry_enabled(self, value: Optional[str], expected: bool) -> None:
        env = {} if value is None else {"SYNAPSE_INTEGRATION_TEST_OTEL_ENABLED": value}

        assert telemetry_enabled(env) is expected


class TestWorkerTelemetryEnv:
    """Unit tests for tests.integration.helpers.worker_telemetry_env."""

    def test_different_workers_yield_different_instance_ids(self) -> None:
        env_a = {"PYTEST_XDIST_WORKER": "gw0"}
        env_b = {"PYTEST_XDIST_WORKER": "gw1"}

        result_a = worker_telemetry_env(env_a)
        result_b = worker_telemetry_env(env_b)

        assert result_a["OTEL_SERVICE_INSTANCE_ID"] == "gw0"
        assert result_b["OTEL_SERVICE_INSTANCE_ID"] == "gw1"
        assert (
            result_a["OTEL_SERVICE_INSTANCE_ID"] != result_b["OTEL_SERVICE_INSTANCE_ID"]
        )

    def test_operator_base_survives_as_prefix(self) -> None:
        env = {
            "PYTEST_XDIST_WORKER": "gw3",
            "OTEL_SERVICE_INSTANCE_ID": "my-base",
        }

        result = worker_telemetry_env(env)

        assert result["OTEL_SERVICE_INSTANCE_ID"] == "my-base-gw3"

    def test_no_xdist_leaves_base_unchanged(self) -> None:
        env = {"OTEL_SERVICE_INSTANCE_ID": "my-base"}

        result = worker_telemetry_env(env)

        assert result["OTEL_SERVICE_INSTANCE_ID"] == "my-base"

    def test_no_xdist_and_no_base_omits_instance_id(self) -> None:
        result = worker_telemetry_env({})

        assert "OTEL_SERVICE_INSTANCE_ID" not in result

    def test_existing_resource_attributes_appended_not_replaced(self) -> None:
        env = {"OTEL_RESOURCE_ATTRIBUTES": "existing.key=existing.value"}

        result = worker_telemetry_env(env)

        assert result["OTEL_RESOURCE_ATTRIBUTES"].startswith(
            "existing.key=existing.value,"
        )

    def test_xdist_workers_from_worker_count(self) -> None:
        env = {"PYTEST_XDIST_WORKER_COUNT": "4"}

        result = worker_telemetry_env(env)

        assert "xdist.workers=4" in result["OTEL_RESOURCE_ATTRIBUTES"]

    def test_git_sha_present_only_when_github_sha_set(self) -> None:
        without_sha = worker_telemetry_env({})
        with_sha = worker_telemetry_env({"GITHUB_SHA": "abc123"})

        assert "git.sha" not in without_sha["OTEL_RESOURCE_ATTRIBUTES"]
        assert "git.sha=abc123" in with_sha["OTEL_RESOURCE_ATTRIBUTES"]


class TestExportFailureSummary:
    """Unit tests for tests.integration.helpers.export_failure_summary."""

    def test_empty_messages_is_none(self) -> None:
        assert export_failure_summary([]) is None

    def test_one_message_names_count_and_message(self) -> None:
        summary = export_failure_summary(["401 Unauthorized"])

        assert "1" in summary
        assert "401 Unauthorized" in summary

    def test_several_messages_names_count_and_first_message(self) -> None:
        summary = export_failure_summary(["401 Unauthorized", "connection refused"])

        assert "2" in summary
        assert "401 Unauthorized" in summary
        assert "connection refused" not in summary


class TestExportFailureRecorder:
    """Unit tests for tests.integration.helpers.ExportFailureRecorder."""

    def test_captures_error_record(self) -> None:
        recorder = ExportFailureRecorder()
        logger = logging.getLogger("test.export_failure_recorder.error")
        logger.addHandler(recorder)

        logger.error("export rejected: 401")

        assert recorder.messages == ["export rejected: 401"]

    def test_ignores_warning_record(self) -> None:
        recorder = ExportFailureRecorder()
        logger = logging.getLogger("test.export_failure_recorder.warning")
        logger.addHandler(recorder)

        logger.warning("retrying export")

        assert recorder.messages == []
