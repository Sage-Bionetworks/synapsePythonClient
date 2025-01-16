"""Unit tests for Asynchronous Job logic."""

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.core.exceptions import SynapseError, SynapseTimeoutError
from synapseclient.models.mixins import asynchronous_job
from synapseclient.models.mixins.asynchronous_job import (
    ASYNC_JOB_URIS,
    AsynchronousJobState,
    AsynchronousJobStatus,
    get_job_async,
    send_job_and_wait_async,
    send_job_async,
)


class TestSendJobAsync:
    """Unit tests for send_job_async."""

    good_request = {"concreteType": AGENT_CHAT_REQUEST}
    bad_request_no_concrete_type = {"otherKey": "otherValue"}
    bad_request_invalid_concrete_type = {"concreteType": "InvalidConcreteType"}
    request_type = AGENT_CHAT_REQUEST

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_send_job_async_when_request_is_missing(self) -> None:
        with pytest.raises(ValueError, match="request must be provided."):
            # WHEN I call send_job_async without a request
            # THEN I should get a ValueError
            await send_job_async(request=None)

    async def test_send_job_async_when_request_is_missing_concrete_type(self) -> None:
        with pytest.raises(ValueError, match="Unsupported request type: None"):
            # GIVEN a request with no concrete type
            # WHEN I call send_job_async
            # THEN I should get a ValueError
            await send_job_async(request=self.bad_request_no_concrete_type)

    async def test_send_job_async_when_request_is_invalid_concrete_type(self) -> None:
        with pytest.raises(
            ValueError, match="Unsupported request type: InvalidConcreteType"
        ):
            # GIVEN a request with an invalid concrete type
            # WHEN I call send_job_async
            # THEN I should get a ValueError
            await send_job_async(request=self.bad_request_invalid_concrete_type)

    async def test_send_job_async_when_request_is_valid(self) -> None:
        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ) as mock_get_client,
            patch(
                "synapseclient.Synapse.rest_post_async",
                new_callable=AsyncMock,
                return_value={"token": "123"},
            ) as mock_rest_post_async,
        ):
            # WHEN I call send_job_async with a good request
            job_id = await send_job_async(
                request=self.good_request, synapse_client=self.syn
            )
            # THEN the return value should be the token
            assert job_id == "123"
            # AND get_client should have been called
            mock_get_client.assert_called_once_with(synapse_client=self.syn)
            # AND rest_post_async should have been called with the correct arguments
            mock_rest_post_async.assert_called_once_with(
                uri=f"{ASYNC_JOB_URIS[self.request_type]}/start",
                body=json.dumps(self.good_request),
            )


class TestGetJobAsync:
    """Unit tests for get_job_async."""

    request_type = AGENT_CHAT_REQUEST
    job_id = "123"

    processing_job_status = AsynchronousJobStatus(
        state=AsynchronousJobState.PROCESSING,
        progress_message="Processing",
        progress_current=1,
        progress_total=100,
    )
    failed_job_status = AsynchronousJobStatus(
        state=AsynchronousJobState.FAILED,
        progress_message="Failed",
        progress_current=1,
        progress_total=100,
        error_message="Error",
        error_details="Details",
        id="123",
    )

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_get_job_async_when_job_fails(self) -> None:
        with (
            patch(
                "synapseclient.Synapse.rest_get_async",
                new_callable=AsyncMock,
                return_value={},
            ) as mock_rest_get_async,
            patch.object(
                AsynchronousJobStatus,
                "fill_from_dict",
                return_value=self.failed_job_status,
            ) as mock_fill_from_dict,
        ):
            with pytest.raises(
                SynapseError,
                match=f"{self.failed_job_status.error_message}\n{self.failed_job_status.error_details}",
            ):
                # WHEN I call get_job_async
                # AND the job fails in the Synapse API
                # THEN I should get a SynapseError with the error message and details
                await get_job_async(
                    job_id="123",
                    request_type=AGENT_CHAT_REQUEST,
                    synapse_client=self.syn,
                    sleep=1,
                    timeout=60,
                    endpoint=None,
                )
                # AND rest_get_async should have been called once with the correct arguments
                mock_rest_get_async.assert_called_once_with(
                    uri=f"{ASYNC_JOB_URIS[AGENT_CHAT_REQUEST]}/get/{self.job_id}",
                    endpoint=None,
                )
                # AND fill_from_dict should have been called once with the correct arguments
                mock_fill_from_dict.assert_called_once_with(
                    async_job_status=mock_rest_get_async.return_value,
                )

    async def test_get_job_async_when_job_times_out(self) -> None:
        with (
            patch(
                "synapseclient.Synapse.rest_get_async",
                new_callable=AsyncMock,
                return_value={},
            ) as mock_rest_get_async,
            patch.object(
                AsynchronousJobStatus,
                "fill_from_dict",
                return_value=self.processing_job_status,
            ) as mock_fill_from_dict,
        ):
            with pytest.raises(
                SynapseTimeoutError, match="Timeout waiting for query results:"
            ):
                # WHEN I call get_job_async
                # AND the job does not complete or progress within the timeout interval
                # THEN I should get a SynapseTimeoutError
                await get_job_async(
                    job_id=self.job_id,
                    request_type=self.request_type,
                    synapse_client=self.syn,
                    endpoint=None,
                    timeout=0,
                    sleep=1,
                )
                # AND rest_get_async should not have been called
                mock_rest_get_async.assert_not_called()
                # AND fill_from_dict should not have been called
                mock_fill_from_dict.assert_not_called()


class TestSendJobAndWaitAsync:
    """Unit tests for send_job_and_wait_async."""

    good_request = {"concreteType": AGENT_CHAT_REQUEST}
    job_id = "123"
    request_type = AGENT_CHAT_REQUEST

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_send_job_and_wait_async(self) -> None:
        with (
            patch(
                "synapseclient.models.mixins.asynchronous_job.send_job_async",
                new_callable=AsyncMock,
                return_value=self.job_id,
            ) as mock_send_job_async,
            patch(
                "synapseclient.models.mixins.asynchronous_job.get_job_async",
                new_callable=AsyncMock,
                return_value={
                    "key": "value",
                },
            ) as mock_get_job_async,
        ):
            # WHEN I call send_job_and_wait_async with a good request
            # THEN the return value should be a dictionary with the job ID
            # and response key value pair(s)
            assert await send_job_and_wait_async(
                request=self.good_request,
                request_type=self.request_type,
                synapse_client=self.syn,
                endpoint=None,
            ) == {
                "jobId": self.job_id,
                "key": "value",
            }
            # AND send_job_async should have been called once with the correct arguments
            mock_send_job_async.assert_called_once_with(
                request=self.good_request,
                synapse_client=self.syn,
            )
            # AND get_job_async should have been called once with the correct arguments
            mock_get_job_async.assert_called_once_with(
                job_id=self.job_id,
                request_type=self.request_type,
                synapse_client=self.syn,
                endpoint=None,
            )


class TestAsynchronousJobStatus:
    """Unit tests for AsynchronousJobStatus."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a dictionary with job status information
        async_job_status_dict = {
            "jobState": AsynchronousJobState.PROCESSING,
            "jobCanceling": False,
            "requestBody": {"key": "value"},
            "responseBody": {"key": "value"},
            "etag": "123",
            "jobId": "123",
            "startedByUserId": "123",
            "startedOn": "123",
            "changedOn": "123",
            "progressMessage": "Processing",
            "progressCurrent": 1,
            "progressTotal": 100,
            "exception": None,
            "errorMessage": None,
            "errorDetails": None,
            "runtimeMs": 1000,
            "callersContext": None,
        }
        # WHEN I call fill_from_dict on it
        async_job_status = AsynchronousJobStatus().fill_from_dict(async_job_status_dict)
        # THEN the resulting AsynchronousJobStatus object
        # should have the correct attribute values
        assert async_job_status.state == AsynchronousJobState.PROCESSING
        assert async_job_status.canceling is False
        assert async_job_status.request_body == {"key": "value"}
        assert async_job_status.response_body == {"key": "value"}
        assert async_job_status.etag == "123"
        assert async_job_status.id == "123"
        assert async_job_status.started_by_user_id == "123"
        assert async_job_status.started_on == "123"
        assert async_job_status.changed_on == "123"
        assert async_job_status.progress_message == "Processing"
        assert async_job_status.progress_current == 1
        assert async_job_status.progress_total == 100
        assert async_job_status.exception is None
        assert async_job_status.error_message is None
        assert async_job_status.error_details is None
        assert async_job_status.runtime_ms == 1000
        assert async_job_status.callers_context is None
