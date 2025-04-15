import asyncio
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import (
    AGENT_CHAT_REQUEST,
    TABLE_UPDATE_TRANSACTION_REQUEST,
)
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseHTTPError,
    SynapseTimeoutError,
)

ASYNC_JOB_URIS = {
    AGENT_CHAT_REQUEST: "/agent/chat/async",
    TABLE_UPDATE_TRANSACTION_REQUEST: "/entity/{entityId}/table/transaction/async",
}


class AsynchronousCommunicator:
    """Mixin to handle communication with the Synapse Asynchronous Job service."""

    def to_synapse_request(self) -> None:
        """Converts the request to a request expected of the Synapse REST API."""
        raise NotImplementedError("to_synapse_request must be implemented.")

    def fill_from_dict(self, synapse_response: Dict[str, str]) -> "Self":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            An instance of this class.
        """
        raise NotImplementedError("fill_from_dict must be implemented.")

    async def _post_exchange_async(
        self, synapse_client: Optional[Synapse] = None, **kwargs
    ) -> None:
        """Any additional logic to run after the exchange with Synapse.

        Arguments:
            synapse_client: The Synapse client to use for the request.
            **kwargs: Additional arguments to pass to the request.
        """
        pass

    async def send_job_and_wait_async(
        self,
        post_exchange_args: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Send the job to the Asynchronous Job service and wait for it to complete.
        Intended to be called by a class inheriting from this mixin to start a job
        in the Synapse API and wait for it to complete. The inheriting class needs to
        represent an asynchronous job request and response and include all necessary attributes.
        This was initially implemented to be used in the AgentPrompt class which can be used
        as an example.

        Arguments:
            post_exchange_args: Additional arguments to pass to the request.
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 60.
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            An instance of this class.

        Example: Using this function
            This function was initially implemented to be used in the AgentPrompt class
            to send a prompt to an AI agent and wait for the response. It can also be used
            in any other class that needs to use an Asynchronous Job.

            The inheriting class (AgentPrompt) will typically not be used directly, but rather
            through a higher level class (AgentSession), but this example shows how you would
            use this function.

                from synapseclient import Synapse
                from synapseclient.models.agent import AgentPrompt

                syn = Synapse()
                syn.login()

                agent_prompt = AgentPrompt(
                    id=None,
                    session_id="123",
                    prompt="Hello",
                    response=None,
                    enable_trace=True,
                    trace=None,
                )
                # This will fill the id, response, and trace
                # attributes with the response from the API
                agent_prompt.send_job_and_wait_async()
        """
        results = await send_job_and_wait_async(
            request=self.to_synapse_request(),
            request_type=self.concrete_type,
            timeout=timeout,
            synapse_client=synapse_client,
        )
        if "results" in results:
            failure_messages = []
            for result in results["results"]:
                if "updateResults" in result:
                    for update_result in result["updateResults"]:
                        failure_code = update_result.get("failureCode", None)
                        failure_message = update_result.get("failureMessage", None)
                        if failure_code or failure_message:
                            client = Synapse.get_client(synapse_client=synapse_client)
                            failure_messages.append(update_result)
            if failure_messages:
                client.logger.warning(
                    f"Failed to send a portion of the async job to Synapse: {failure_messages}"
                )
        self.fill_from_dict(synapse_response=results)
        if not post_exchange_args:
            post_exchange_args = {}
        await self._post_exchange_async(
            **post_exchange_args, synapse_client=synapse_client
        )
        return self


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

    - SESSION_ID: Each web service request is issued a unique session ID (UUID)
        that is included in the call's access record.
        Events that are triggered by a web service request should include the session ID
        so that they can be linked to each other and the call's access record.
    """

    SESSION_ID = "SESSION_ID"


@dataclass
class AsynchronousJobStatus:
    """Represents a Synapse Asynchronous Job Status object:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html>

    Attributes:
        state: The state of the job. Either PROCESSING, FAILED, or COMPLETE.
        canceling: Whether the job has been requested to be cancelled.
        request_body: The body of an Asynchronous job request.
            Will be one of the models described here:
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>
        response_body: The body of an Asynchronous job response.
            Will be one of the models described here:
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
        self.state = (
            AsynchronousJobState(async_job_status.get("jobState"))
            if async_job_status.get("jobState")
            else None
        )
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


async def send_job_and_wait_async(
    request: Dict[str, Any],
    request_type: str,
    endpoint: str = None,
    timeout: int = 60,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Sends the job to the Synapse API and waits for the response. Request body matches:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>

    Arguments:
        request: A request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>.
        endpoint: The endpoint to use for the request. Defaults to None.
        timeout: The number of seconds to wait for the job to complete or progress
            before raising a SynapseTimeoutError. Defaults to 60.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The response body matching
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousResponseBody.html>

    Raises:
        SynapseError: If the job fails.
        SynapseTimeoutError: If the job does not complete within the timeout.
    """
    start_time = time.time()
    retry_interval = 5  # Retry every 5 seconds
    max_wait_time = timeout * 5  # Maximum total wait time of 5 minutes

    while time.time() - start_time < max_wait_time:
        try:
            job_id = await send_job_async(
                request=request, synapse_client=synapse_client
            )
            result = {
                "jobId": job_id,
                **await get_job_async(
                    job_id=job_id,
                    request_type=request_type,
                    synapse_client=synapse_client,
                    endpoint=endpoint,
                    timeout=timeout,
                    request=request,
                ),
            }
            return result
        except SynapseHTTPError as e:
            if (
                "You cannot create a version of a view that is not available (Status: PROCESSING)"
                in str(e)
            ):
                if time.time() - start_time < max_wait_time:
                    await asyncio.sleep(retry_interval)
                    continue
            raise  # Re-raise any other SynapseHTTPError or if max wait time reached
        except Exception:
            raise  # Re-raise any other exceptions

    raise SynapseError(f"Failed to create view version after {max_wait_time} seconds")


async def send_job_async(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Sends the job to the Synapse API. Request body matches:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>
    Returns the job ID.

    Arguments:
        request: A request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousRequestBody.html>.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The job ID retrieved from the response.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsyncJobId.html>
    """
    if not request:
        raise ValueError("request must be provided.")

    request_type = request.get("concreteType")

    if not request_type or request_type not in ASYNC_JOB_URIS:
        raise ValueError(f"Unsupported request type: {request_type}")

    client = Synapse.get_client(synapse_client=synapse_client)
    uri = ASYNC_JOB_URIS[request_type]
    if "{entityId}" in uri:
        if "entityId" not in request:
            raise ValueError(f"Attempting to send job with missing id in uri: {uri}")
        uri = uri.format(entityId=request["entityId"])

    response = await client.rest_post_async(
        uri=f"{uri}/start", body=json.dumps(request)
    )
    return response["token"]


async def get_job_async(
    job_id: str,
    request_type: str,
    endpoint: str = None,
    sleep: int = 1,
    timeout: int = 60,
    request: Dict[str, Any] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets the job from the server using its ID. Handles progress tracking, failures and timeouts.

    Arguments:
        job_id: The ID of the job to get.
        request_type: The type of the job.
        endpoint: The endpoint to use for the request. Defaults to None.
        sleep: The number of seconds to wait between requests. Defaults to 1.
        timeout: The number of seconds to wait for the job to complete or progress
            before raising a SynapseTimeoutError. Defaults to 60.
        request: The original request that was sent to the server that created the job.
            Required if the request type is one that requires additional information.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The response body matching
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousResponseBody.html>

    Raises:
        SynapseError: If the job fails.
        SynapseTimeoutError: If the job does not complete or progress within the timeout interval.
    """
    client = Synapse.get_client(synapse_client=synapse_client)
    start_time = time.time()

    last_message = ""
    last_progress = 0
    last_total = 1
    progressed = False
    progress_bar = tqdm(
        total=last_total,
        unit_scale=True,
        smoothing=0,
        leave=None,
    )
    with logging_redirect_tqdm(loggers=[client.logger]):
        while time.time() - start_time < timeout:
            uri = ASYNC_JOB_URIS[request_type]
            if "{entityId}" in uri:
                if not request:
                    raise ValueError("Attempting to get job with missing request.")
                if "entityId" not in request:
                    raise ValueError(
                        f"Attempting to get job with missing id in uri: {uri}"
                    )
                uri = uri.format(entityId=request["entityId"])
            progress_bar.desc = uri
            result = await client.rest_get_async(
                uri=f"{uri}/get/{job_id}",
                endpoint=endpoint,
            )

            job_status = AsynchronousJobStatus().fill_from_dict(async_job_status=result)
            if job_status.state == AsynchronousJobState.PROCESSING:
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
                    progress_bar.update(job_status.progress_current - last_progress)
                    last_message = job_status.progress_message
                    last_progress = job_status.progress_current
                    last_total = job_status.progress_total
                    updated = False

                    if progress_bar.desc != last_message:
                        progress_bar.desc = last_message
                        updated = True

                    if progress_bar.total != last_total:
                        progress_bar.total = last_total
                        updated = True

                    if updated:
                        progress_bar.refresh()
                    start_time = time.time()
                await asyncio.sleep(sleep)
            elif job_status.state == AsynchronousJobState.FAILED:
                progress_bar.close()
                raise SynapseError(
                    f"{job_status.error_message}\n{job_status.error_details}",
                )
            else:
                break
        else:
            progress_bar.close()
            raise SynapseTimeoutError(
                f"Timeout waiting for results: {time.time() - start_time} seconds"
            )

        progress_bar.update(progress_bar.total - progress_bar.n)
        progress_bar.refresh()
        progress_bar.close()
        return result
