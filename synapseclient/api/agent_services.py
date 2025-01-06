"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.AgentController>
"""

import json
import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from async_lru import alru_cache

if TYPE_CHECKING:
    from synapseclient import Synapse

from synapseclient.core.exceptions import SynapseTimeoutError


async def register_agent(
    request: Dict[str, Any],
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Registers an agent with Synapse OR gets existing agent registration.

    Arguments:
        request: The request for the agent matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistrationRequest.html>
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested agent matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistration.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri="/agent/registration", body=json.dumps(request)
    )


async def get_agent(
    registration_id: str, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Any]:
    """
    Gets information about an existing agent registration.

    Arguments:
        registration_id: The ID of the agent registration to get.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested agent registration matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistration.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/agent/registration/{registration_id}")


async def start_session(
    request: Dict[str, Any],
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Starts a new chat session with an agent.

    Arguments:
        request: The request for the session matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/CreateAgentSessionRequest.html>
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(uri="/agent/session", body=json.dumps(request))


async def get_session(
    session_id: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets information about an existing chat session.

    Arguments:
        session_id: The ID of the session to get.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested session matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentSession.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/agent/session/{session_id}")


async def update_session(
    request: Dict[str, Any],
    session_id: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Updates the access level for a chat session.

    Arguments:
        request: The request for the session matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/UpdateAgentSessionRequest.html>
        session_id: The ID of the session to update.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/agent/session/{session_id}", body=json.dumps(request)
    )


async def send_prompt(
    request: Dict[str, Any],
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Sends a prompt to an agent starting an asyncronous job.

    Arguments:
        request: The request for the prompt matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentChatRequest.html>
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The response matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsyncJobId.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/agent/chat/async/start", body=json.dumps(request)
    )


async def get_response(
    prompt_token: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets the response to a prompt.

    Arguments:
        prompt_token: The token of the prompt to get the response for.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The response matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentChatResponse.html>
        If the reponse is ready. Else, it will return a reponse matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html>

    Raises:
        TimeoutError: If the response is not ready after 1 minute
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    start_time = asyncio.get_event_loop().time()
    TIMEOUT = 60

    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT:
            raise SynapseTimeoutError(
                f"Timeout waiting for response: {TIMEOUT} seconds"
            )

        response = await client.rest_get_async(
            uri=f"/agent/chat/async/get/{prompt_token}"
        )
        if response.get("jobState") != "PROCESSING":
            return response
        await asyncio.sleep(0.5)


async def get_trace(
    request: Dict[str, Any],
    prompt_token: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets the trace of a prompt.

    Arguments:
        request: The request for the trace matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEventsRequest.html>
        prompt_token: The token of the prompt to get the trace for.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The trace matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEventsResponse.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri=f"/agent/chat/trace/{prompt_token}", body=json.dumps(request)
    )
