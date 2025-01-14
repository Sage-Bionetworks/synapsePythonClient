import asyncio
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional

from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.core.exceptions import SynapseError, SynapseTimeoutError

if TYPE_CHECKING:
    from synapseclient import Synapse


class AsynchronousJobState(str, Enum):
    """Enum representing the state of a Synapse Asynchronous Job:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchJobState.html>

    - PROCESSING: The job is being processed.
    - FAILED: The job has failed.
    - COMPLETE: The job has been completed.
    """

    PROCESSING = "PROCESSING"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


class CallersContext(str, Enum):
    """Enum representing information about a web service call:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/auth/CallersContext.html>

    - SESSION_ID: Each web service request is issued a unique session ID (UUID) that is included in the call's access record.
            Events that are triggered by a web service request should include the session ID so that they can be linked to
            each other and the call's access record.
    """

    SESSION_ID = "SESSION_ID"


@dataclass
class AsynchronousJobStatus:
    """Represents a Synapse Asynchronous Job Status object:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html>

    Attributes:
        state: The state of the job. Either PROCESSING, FAILED, or COMPLETE.
        canceling: Whether the job has been requested to be cancelled.
        request_body: The body of an Asynchronous job request. Will be one of the models described here:
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>
        response_body: The body of an Asynchronous job response. Will be one of the models described here:
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousResponseBody.html>
        etag: The etag of the job status. Changes whenever the status changes.
        id: The ID if the job issued when this job was started.
        started_by_user_id: The ID of the user that started the job.
        started_on: The date-time when the status was last changed to PROCESSING.
        changed_on: The date-time when the status of this job was last changed.
        progress_message: The current message of the progress tracker.
        progress_current: A value indicating how much progress has been made.
                I.e. a value of 50 indicates that 50% of the work has been
                completed if progress_total is 100.
        progress_total: A value indicating the total amount of work to complete.
        exception: The exception that needs to be thrown if the job fails.
        error_message: A one-line error message when the job fails.
        error_details: Full stack trace of the error when the job fails.
        runtime_ms: The number of milliseconds from the start to completion of this job.
        callers_context: Contextual information about a web service call.
    """

    state: Optional["AsynchronousJobState"] = None
    """The state of the job. Either PROCESSING, FAILED, or COMPLETE."""

    canceling: Optional[bool] = False
    """Whether the job has been requested to be cancelled."""

    request_body: Optional[dict] = None
    """The body of an Asynchronous job request. Will be one of the models described here:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>"""

    response_body: Optional[dict] = None
    """The body of an Asynchronous job response. Will be one of the models described here:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousResponseBody.html>"""

    etag: Optional[str] = None
    """The etag of the job status. Changes whenever the status changes."""

    id: Optional[str] = None
    """The ID if the job issued when this job was started."""

    started_by_user_id: Optional[int] = None
    """The ID of the user that started the job."""

    started_on: Optional[str] = None
    """The date-time when the status was last changed to PROCESSING."""

    changed_on: Optional[str] = None
    """The date-time when the status of this job was last changed."""

    progress_message: Optional[str] = None
    """The current message of the progress tracker."""

    progress_current: Optional[int] = None
    """A value indicating how much progress has been made.
        I.e. a value of 50 indicates that 50% of the work has been
        completed if progress_total is 100."""

    progress_total: Optional[int] = None
    """A value indicating the total amount of work to complete."""

    exception: Optional[str] = None
    """The exception that needs to be thrown if the job fails."""

    error_message: Optional[str] = None
    """A one-line error message when the job fails."""

    error_details: Optional[str] = None
    """Full stack trace of the error when the job fails."""

    runtime_ms: Optional[int] = None
    """The number of milliseconds from the start to completion of this job."""

    callers_context: Optional["CallersContext"] = None
    """Contextual information about a web service call."""

    def fill_from_dict(self, async_job_status: dict) -> "AsynchronousJobStatus":
        """Converts a response from the REST API into this dataclass.

        Arguments:
            async_job_status: The response from the REST API.

        Returns:
            A AsynchronousJobStatus object.
        """
        self.state = async_job_status.get("jobState", None)
        self.canceling = async_job_status.get("jobCanceling", None)
        self.request_body = async_job_status.get("requestBody", None)
        self.response_body = async_job_status.get("responseBody", None)
        self.etag = async_job_status.get("etag", None)
        self.id = async_job_status.get("jobId", None)
        self.started_by_user_id = async_job_status.get("startedByUserId", None)
        self.started_on = async_job_status.get("startedOn", None)
        self.changed_on = async_job_status.get("changedOn", None)
        self.progress_message = async_job_status.get("progressMessage", None)
        self.progress_current = async_job_status.get("progressCurrent", None)
        self.progress_total = async_job_status.get("progressTotal", None)
        self.exception = async_job_status.get("exception", None)
        self.error_message = async_job_status.get("errorMessage", None)
        self.error_details = async_job_status.get("errorDetails", None)
        self.runtime_ms = async_job_status.get("runtimeMs", None)
        self.callers_context = async_job_status.get("callersContext", None)
        return self


class AsynchronousJob:
    """
    Mixin for objects that can have Asynchronous Jobs.
    """

    ASYNC_JOB_URIS = {
        AGENT_CHAT_REQUEST: "/agent/chat/async",
    }

    async def send_job_async(
        self,
        request_type: str,
        session_id: str,
        prompt: str,
        enable_trace: bool,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """
        Sends the job to the Synapse API. Request body matches:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>
        Returns the job ID.

        Arguments:
            request_type: The type of the job.
            session_id: The ID of the session to send the prompt to.
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The job ID retrieved from the response.
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsyncJobId.html>
        """
        request = {
            "concreteType": request_type,
            "sessionId": session_id,
            "chatText": prompt,
            "enableTrace": enable_trace,
        }
        response = await synapse_client.rest_post_async(
            uri=f"{self.ASYNC_JOB_URIS[request_type]}/start", body=json.dumps(request)
        )
        return response["token"]

    async def get_job_async(
        self,
        job_id: str,
        request_type: str,
        synapse_client: "Synapse",
        endpoint: str = None,
    ) -> Dict[str, Any]:
        """
        Gets the job from the server using its ID. Handles progress tracking, failures and timeouts.

        Arguments:
            job_id: The ID of the job to get.
            request_type: The type of the job.
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.
            endpoint: The endpoint to use for the request. Defaults to None.

        Returns:
            The response body matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousResponseBody.html>

        Raises:
            SynapseError: If the job fails.
            SynapseTimeoutError: If the job does not complete within the timeout.
        """
        start_time = asyncio.get_event_loop().time()
        SLEEP = 1
        TIMEOUT = 60

        last_message = ""
        last_progress = 0
        last_total = 1
        progressed = False

        while asyncio.get_event_loop().time() - start_time < TIMEOUT:
            result = await synapse_client.rest_get_async(
                uri=f"{self.ASYNC_JOB_URIS[request_type]}/get/{job_id}",
                endpoint=endpoint,
            )
            job_status = AsynchronousJobStatus().fill_from_dict(async_job_status=result)
            if job_status.state == AsynchronousJobState.PROCESSING:
                # TODO: Is this adequate to determine if the endpoint tracks progress?
                progress_tracking = any(
                    [
                        job_status.progress_message,
                        job_status.progress_current,
                        job_status.progress_total,
                    ]
                )
                progressed = (
                    job_status.progress_message != last_message
                    or last_progress != job_status.progress_current
                )
                if progress_tracking and progressed:
                    last_message = job_status.progress_message
                    last_progress = job_status.progress_current
                    last_total = job_status.progress_total

                    synapse_client._print_transfer_progress(
                        last_progress,
                        last_total,
                        prefix=last_message,
                        isBytes=False,
                    )
                    start_time = asyncio.get_event_loop().time()
                await asyncio.sleep(SLEEP)
            elif job_status.state == AsynchronousJobState.FAILED:
                raise SynapseError(
                    f"{job_status.error_message}\n{job_status.error_details}",
                    async_job_status=job_status.id,
                )
            else:
                break
        else:
            raise SynapseTimeoutError(
                f"Timeout waiting for query results: {time.time() - start_time} seconds"
            )

        return result
