"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.AgentController>
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def register_agent(
    cloud_agent_id: str,
    cloud_alias_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Registers an agent with Synapse OR gets existing agent registration.
    Sends a request matching
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistrationRequest.html>

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

    request = {"awsAgentId": cloud_agent_id}
    if cloud_alias_id:
        request["awsAliasId"] = cloud_alias_id
    return await client.rest_put_async(
        uri="/agent/registration", body=json.dumps(request)
    )


async def get_agent(
    registration_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
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
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Starts a new chat session with an agent.
    Sends a request matching
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/CreateAgentSessionRequest.html>

    Arguments:
        access_level: The access level of the agent.
        agent_registration_id: The ID of the agent registration to start the session for.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request = {
        "agentAccessLevel": access_level,
        "agentRegistrationId": agent_registration_id,
    }
    return await client.rest_post_async(uri="/agent/session", body=json.dumps(request))


async def get_session(
    id: str,
    *,
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
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Updates the access level for a chat session.
    Sends a request matching
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/UpdateAgentSessionRequest.html>

    Arguments:
        id: The ID of the session to update.
        access_level: The access level of the agent.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request = {
        "sessionId": id,
        "agentAccessLevel": access_level,
    }
    return await client.rest_put_async(
        uri=f"/agent/session/{id}", body=json.dumps(request)
    )


async def get_trace(
    prompt_id: str,
    *,
    newer_than: Optional[int] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Gets the trace of a prompt.
    Sends a request matching
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEventsRequest.html>

    Arguments:
        prompt_id: The token of the prompt to get the trace for.
        newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
                    Timestamps should be in milliseconds since the epoch per the API documentation.
                    https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEvent.html
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The trace matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/TraceEventsResponse.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request = {
        "jobId": prompt_id,
        "newerThanTimestamp": newer_than,
    }
    return await client.rest_post_async(
        uri=f"/agent/chat/trace/{prompt_id}", body=json.dumps(request)
    )
