"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.AgentController>
"""

import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse

from synapseclient.core.exceptions import SynapseTimeoutError


async def register_agent(
    cloud_agent_id: str,
    cloud_alias_id: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Registers an agent with Synapse OR gets existing agent registration.

    Arguments:
        cloud_agent_id: The cloud provider ID of the agent to register.
        cloud_alias_id: The cloud provider alias ID of the agent to register.
                        In the Synapse API, this defaults to 'TSTALIASID'.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The registered agent matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistration.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistrationRequest.html>
    request = {
        "awsAgentId": cloud_agent_id,
        "awsAliasId": cloud_alias_id if cloud_alias_id else None,
    }
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
    access_level: str,
    agent_registration_id: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Starts a new chat session with an agent.

    Arguments:
        access_level: The access level of the agent.
        agent_registration_id: The ID of the agent registration to start the session for.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/CreateAgentSessionRequest.html>
    request = {
        "agentAccessLevel": access_level,
        "agentRegistrationId": agent_registration_id,
    }
    return await client.rest_post_async(uri="/agent/session", body=json.dumps(request))


async def get_session(
    id: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets information about an existing chat session.

    Arguments:
        id: The ID of the session to get.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested session matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentSession.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/agent/session/{id}")


async def update_session(
    id: str,
    access_level: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Updates the access level for a chat session.

    Arguments:
        id: The ID of the session to update.
        access_level: The access level of the agent.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/UpdateAgentSessionRequest.html>
    request = {
        "sessionId": id,
        "agentAccessLevel": access_level,
    }
    return await client.rest_put_async(
        uri=f"/agent/session/{id}", body=json.dumps(request)
    )


async def send_prompt(
    id: str,
    prompt: str,
    enable_trace: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Sends a prompt to an agent starting an asyncronous job.

    Arguments:
        id: The ID of the session to send the prompt to.
        prompt: The prompt to send to the agent.
        enable_trace: Whether to enable trace for the prompt. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The response matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsyncJobId.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentChatRequest.html>
    request = {
        "concreteType": "org.sagebionetworks.repo.model.agent.AgentChatRequest",
        "sessionId": id,
        "chatText": prompt,
        "enableTrace": enable_trace,
    }
    return await client.rest_post_async(
        uri="/agent/chat/async/start", body=json.dumps(request)
    )


async def get_response(
    prompt_id: str,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets the response to a prompt. If the response is not ready, the endpoint will return a reponse matching
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html>.
    In this case, the response retrieval is retried every second until the response is ready or a timeout is reached.

    Arguments:
        prompt_id: The token of the prompt to get the response for.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The response matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentChatResponse.html>

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

        response = await client.rest_get_async(uri=f"/agent/chat/async/get/{prompt_id}")
        if response.get("jobState") == "PROCESSING":
            await asyncio.sleep(1)
            continue

        return response


async def get_trace(
    prompt_id: str,
    newer_than: Optional[int] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets the trace of a prompt.

    Arguments:
        prompt_id: The token of the prompt to get the trace for.
        newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The trace matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEventsResponse.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEventsRequest.html>
    request = {
        "jobId": prompt_id,
        "newerThanTimestamp": newer_than,
    }
    return await client.rest_post_async(
        uri=f"/agent/chat/trace/{prompt_id}", body=json.dumps(request)
    )
