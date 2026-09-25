"""Unit tests for Asynchronous Job logic."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import (
    AGENT_CHAT_REQUEST,
    COMPUTE_TASK_EXECUTION_REQUEST,
    QUERY_BUNDLE_REQUEST,
    TABLE_UPDATE_TRANSACTION_REQUEST,
)
from synapseclient.core.exceptions import SynapseError, SynapseTimeoutError
from synapseclient.models.mixins.asynchronous_job import (
    ASYNC_JOB_URIS,
    AsynchronousJobState,
    AsynchronousJobStatus,
    _resolve_async_job_uri,
    get_job_async,
    send_job_and_wait_async,
    send_job_async,
)


class TestResolveAsyncJobUri:
    """Unit tests for _resolve_async_job_uri."""

    @pytest.mark.parametrize(
        "request_type,job_request,expected_uri",
        [
            (AGENT_CHAT_REQUEST, None, "/agent/chat/async"),
            (
                AGENT_CHAT_REQUEST,
                {"concreteType": AGENT_CHAT_REQUEST, "sessionId": "123"},
                "/agent/chat/async",
            ),
            (
                COMPUTE_TASK_EXECUTION_REQUEST,
                {"concreteType": COMPUTE_TASK_EXECUTION_REQUEST, "taskId": 42},
                "/curation/task/42/execute/async",
            ),
            (
                COMPUTE_TASK_EXECUTION_REQUEST,
                {"concreteType": COMPUTE_TASK_EXECUTION_REQUEST, "taskId": "42"},
                "/curation/task/42/execute/async",
            ),
            (
                COMPUTE_TASK_EXECUTION_REQUEST,
                {
                    "concreteType": COMPUTE_TASK_EXECUTION_REQUEST,
                    "taskId": 42,
                    "unrelatedKey": "ignored",
                },
                "/curation/task/42/execute/async",
            ),
            (
                TABLE_UPDATE_TRANSACTION_REQUEST,
                {
                    "concreteType": TABLE_UPDATE_TRANSACTION_REQUEST,
                    "entityId": "syn123",
                    "changes": [],
                },
                "/entity/syn123/table/transaction/async",
            ),
        ],
        ids=[
            "static_uri_needs_no_request",
            "static_uri_ignores_request",
            "placeholder_filled_from_request",
            "placeholder_value_is_a_string",
            "unrelated_keys_are_ignored",
            "entity_id_placeholder",
        ],
    )
    def test_uri_is_resolved(
        self, request_type: str, job_request: dict | None, expected_uri: str
    ) -> None:
        """A registered request type maps to a uri with no placeholders left in it."""
        uri = _resolve_async_job_uri(request_type=request_type, request=job_request)
        assert uri == expected_uri

    @pytest.mark.parametrize(
        "request_type,job_request,expected_error",
        [
            (
                "InvalidConcreteType",
                {},
                "Unsupported request type: InvalidConcreteType",
            ),
            (None, {}, "Unsupported request type: None"),
            (COMPUTE_TASK_EXECUTION_REQUEST, None, "no request provided"),
            (
                COMPUTE_TASK_EXECUTION_REQUEST,
                {"concreteType": COMPUTE_TASK_EXECUTION_REQUEST},
                "missing taskId in request",
            ),
            (
                COMPUTE_TASK_EXECUTION_REQUEST,
                {"concreteType": COMPUTE_TASK_EXECUTION_REQUEST, "taskId": None},
                "missing taskId in request",
            ),
            (
                COMPUTE_TASK_EXECUTION_REQUEST,
                {"concreteType": COMPUTE_TASK_EXECUTION_REQUEST, "taskId": ""},
                "missing taskId in request",
            ),
        ],
        ids=[
            "unregistered_request_type",
            "no_request_type",
            "placeholder_with_no_request",
            "placeholder_absent_from_request",
            "placeholder_none_in_request",
            "placeholder_empty_in_request",
        ],
    )
    def test_unresolvable_uri_raises(
        self, request_type: str | None, job_request: dict | None, expected_error: str
    ) -> None:
        """A uri that cannot be completed fails loudly instead of being requested."""
        with pytest.raises(ValueError, match=expected_error):
            _resolve_async_job_uri(request_type=request_type, request=job_request)

    @pytest.mark.parametrize(
        "uri",
        [
            "/foo/{entity-id}/async",
            "/foo/{taskId}/{entity-id}/async",
        ],
        ids=["only_placeholder_is_unparseable", "one_placeholder_is_unparseable"],
    )
    def test_placeholder_that_is_not_an_identifier_raises(
        self, monkeypatch: pytest.MonkeyPatch, uri: str
    ) -> None:
        """A placeholder the resolver cannot fill must raise, not reach the server."""
        # GIVEN a registered uri with a placeholder that is not a plain identifier.
        fake_request_type = "org.sagebionetworks.repo.model.FakeRequest"
        monkeypatch.setitem(ASYNC_JOB_URIS, fake_request_type, uri)

        # WHEN I resolve it with a request that carries every value the uri names
        # THEN I should get a ValueError naming what could not be resolved, rather
        # than a uri that still contains braces and would be sent to Synapse as-is
        with pytest.raises(ValueError, match="cannot name a request key"):
            _resolve_async_job_uri(
                request_type=fake_request_type,
                request={
                    "concreteType": fake_request_type,
                    "taskId": 42,
                    "entity-id": "syn123",
                },
            )

    def test_synapse_ids_are_not_altered_by_encoding(self) -> None:
        """Percent-encoding leaves the ids callers actually pass untouched."""
        # GIVEN a request carrying an ordinary synId
        uri = _resolve_async_job_uri(
            request_type=QUERY_BUNDLE_REQUEST,
            request={"concreteType": QUERY_BUNDLE_REQUEST, "entityId": "syn123"},
        )

        # THEN the synId should appear verbatim: percent-encoding must not disturb
        # the values that are actually used in practice
        assert uri == "/entity/syn123/table/query/async"

    @pytest.mark.parametrize(
        "task_id,expected",
        [
            ("123/../../../admin", "123%2F..%2F..%2F..%2Fadmin"),
            ("123?foo=bar", "123%3Ffoo%3Dbar"),
            ("123 456", "123%20456"),
        ],
        ids=["path_traversal", "query_injection", "space"],
    )
    def test_placeholder_is_encoded_as_a_single_path_segment(
        self, task_id: str, expected: str
    ) -> None:
        """A placeholder value cannot change which endpoint is called."""
        # GIVEN a request whose id carries characters that are significant in a uri.
        # task_id is annotated int but dataclasses do not enforce annotations, so a
        # string can reach this point.
        uri = _resolve_async_job_uri(
            request_type=COMPUTE_TASK_EXECUTION_REQUEST,
            request={
                "concreteType": COMPUTE_TASK_EXECUTION_REQUEST,
                "taskId": task_id,
            },
        )

        # THEN the value should be percent-encoded, leaving the surrounding path
        # structure intact: the id still occupies exactly one segment
        assert uri == f"/curation/task/{expected}/execute/async"
        assert uri.split("/") == ["", "curation", "task", expected, "execute", "async"]


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
                match=(
                    f"{self.failed_job_status.error_message}\n"
                    f"{self.failed_job_status.error_details}"
                ),
            ):
                # WHEN I call get_job_async
                # AND the job fails in the Synapse API
                # THEN I should get a SynapseError with the error message and details
                await get_job_async(
                    job_id="123",
                    request_type=AGENT_CHAT_REQUEST,
                    synapse_client=self.syn,
                    sleep=1,
                    timeout=120,
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
                SynapseTimeoutError, match="Timeout waiting for results:"
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
                timeout=120,
                request=self.good_request,
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
