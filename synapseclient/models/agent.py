from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from synapseclient import Synapse
from synapseclient.api import (
    get_agent,
    get_response,
    get_session,
    get_trace,
    register_agent,
    send_prompt,
    start_session,
    update_session,
)
from synapseclient.core.async_utils import otel_trace_method


class AgentType(Enum):
    """
    Enum representing the type of agent as defined in
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentType.html>
    'BASELINE' is a default agent provided by Synapse.
    'CUSTOM' is a custom agent that has been registered by a user.
    """

    BASELINE = "BASELINE"
    CUSTOM = "CUSTOM"


class AgentSessionAccessLevel(Enum):
    """
    Enum representing the access level of the agent session as defined in
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentAccessLevel.html>
    """

    PUBLICLY_ACCESSIBLE = "PUBLICLY_ACCESSIBLE"
    READ_YOUR_PRIVATE_DATA = "READ_YOUR_PRIVATE_DATA"
    WRITE_YOUR_PRIVATE_DATA = "WRITE_YOUR_PRIVATE_DATA"


@dataclass
class AgentPrompt:
    """Represents a prompt, response, and metadata within an AgentSession.

    Attributes:
        id: The unique ID of the agent prompt.
        prompt: The prompt to send to the agent.
        response: The response from the agent.
        trace: The trace of the agent session.
    """

    id: Optional[str] = None
    """The unique ID of the agent prompt."""

    prompt: Optional[str] = None
    """The prompt sent to the agent."""

    response: Optional[str] = None
    """The response from the agent."""

    trace: Optional[str] = None
    """The trace or "though process" of the agent when responding to the prompt."""


@dataclass
class AgentSession:
    """Represents a [Synapse Agent Session](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentSession.html)

    Attributes:
        id: The unique ID of the agent session. Can only be used by the user that created it.
        access_level: The access level of the agent session.
            One of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA, or WRITE_YOUR_PRIVATE_DATA.
        started_on: The date the agent session was started.
        started_by: The ID of the user who started the agent session.
        modified_on: The date the agent session was last modified.
        agent_registration_id: The registration ID of the agent that will be used for this session.
        etag: The etag of the agent session.
    """

    id: Optional[str] = None
    """The unique ID of the agent session. Can only be used by the user that created it."""

    access_level: Optional[AgentSessionAccessLevel] = (
        AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE
    )
    """The access level of the agent session.
        One of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA, or WRITE_YOUR_PRIVATE_DATA.
        Defaults to PUBLICLY_ACCESSIBLE.
    """

    started_on: Optional[datetime] = None
    """The date the agent session was started."""

    started_by: Optional[int] = None
    """The ID of the user who started the agent session."""

    modified_on: Optional[datetime] = None
    """The date the agent session was last modified."""

    agent_registration_id: Optional[int] = None
    """The registration ID of the agent that will be used for this session."""

    etag: Optional[str] = None
    """The etag of the agent session."""

    chat_history: List[AgentPrompt] = field(default_factory=list)
    """A list of AgentPrompt objects."""

    def fill_from_dict(self, synapse_agent_session: Dict[str, str]) -> "AgentSession":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_agent_session: The response from the REST API.

        Returns:
            The AgentSession object.
        """
        self.id = synapse_agent_session.get("sessionId", None)
        self.access_level = synapse_agent_session.get("agentAccessLevel", None)
        self.started_on = synapse_agent_session.get("startedOn", None)
        self.started_by = synapse_agent_session.get("startedBy", None)
        self.modified_on = synapse_agent_session.get("modifiedOn", None)
        self.agent_registration_id = synapse_agent_session.get(
            "agentRegistrationId", None
        )
        self.etag = synapse_agent_session.get("etag", None)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Start_Session: {self.id}"
    )
    async def start_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        """Starts an agent session.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The new AgentSession object.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)
        session_response = await start_session(
            access_level=self.access_level.value,
            agent_registration_id=self.agent_registration_id,
            synapse_client=syn,
        )
        return self.fill_from_dict(session_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Session: {self.id}"
    )
    async def get_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        """Gets an agent session.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The retrieved AgentSession object.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)
        session_response = await get_session(
            id=self.id,
            synapse_client=syn,
        )
        return self.fill_from_dict(synapse_agent_session=session_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Update_Session: {self.id}"
    )
    async def update_async(
        self,
        *,
        access_level: AgentSessionAccessLevel,
        synapse_client: Optional[Synapse] = None,
    ) -> "AgentSession":
        """Updates an agent session. Only updates to the access level are currently supported.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The updated AgentSession object.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)

        self.access_level = access_level
        session_response = await update_session(
            id=self.id,
            access_level=self.access_level.value,
            synapse_client=syn,
        )
        return self.fill_from_dict(session_response)

    @otel_trace_method(method_to_trace_name=lambda self, **kwargs: f"Prompt: {self.id}")
    async def prompt_async(
        self,
        *,
        prompt: str,
        enable_trace: bool = False,
        newer_than: Optional[int] = None,
        print_response: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Sends a prompt to the agent and adds the response to the AgentSession's chat history.

        Arguments:
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt.
            newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
            print: Whether to print the response to the console.
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)
        prompt_response = await send_prompt(
            id=self.id,
            prompt=prompt,
            enable_trace=enable_trace,
            synapse_client=syn,
        )
        prompt_id = prompt_response["token"]

        answer_response = await get_response(
            prompt_id=prompt_id,
            synapse_client=syn,
        )
        response = answer_response["responseText"]

        if enable_trace:
            trace_response = await get_trace(
                prompt_id=prompt_id,
                newer_than=newer_than,
                synapse_client=syn,
            )
            trace = trace_response["page"][0]["message"]

        self.chat_history.append(
            AgentPrompt(
                id=prompt_id,
                prompt=prompt,
                response=response,
                trace=trace,
            )
        )

        if print_response:
            print(f"PROMPT:\n{prompt}\n")
            print(f"RESPONSE:\n{response}\n")
            if enable_trace:
                print(f"TRACE:\n{trace}")


@dataclass
class Agent:
    """Represents a [Synapse Agent Registration](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistration.html)

    Attributes:
        cloud_agent_id: The unique ID of the agent in the cloud provider.
        cloud_alias_id: The alias ID of the agent in the cloud provider.
                    In the Synapse API, this defaults to 'TSTALIASID'.
        synapse_registration_id: The ID number of the agent assigned by Synapse.
        registered_on: The date the agent was registered.
        type: The type of agent.
    """

    cloud_agent_id: Optional[str] = None
    """The unique ID of the agent in the cloud provider."""

    cloud_alias_id: Optional[str] = None
    """The alias ID of the agent in the cloud provider. In the Synapse API, this defaults to 'TSTALIASID'."""

    registration_id: Optional[int] = None
    """The ID number of the agent assigned by Synapse."""

    registered_on: Optional[datetime] = None
    """The date the agent was registered."""

    type: Optional[AgentType] = None
    """The type of agent. One of either BASELINE or CUSTOM."""

    sessions: Dict[str, AgentSession] = field(default_factory=dict)
    """A dictionary of AgentSession objects, keyed by session ID."""

    current_session: Optional[str] = None
    """The ID of the current session. Prompts will be sent to this session by default."""

    def fill_from_dict(self, agent_registration: Dict[str, str]) -> "Agent":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            agent_registration: The response from the REST API.

        Returns:
            The Agent object.
        """
        self.cloud_agent_id = agent_registration.get("awsAgentId", None)
        self.cloud_alias_id = agent_registration.get("awsAliasId", None)
        self.registration_id = agent_registration.get("agentRegistrationId", None)
        self.registered_on = agent_registration.get("registeredOn", None)
        self.type = agent_registration.get("type", None)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Register_Agent: {self.registration_id}"
    )
    async def register_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "Agent":
        """Registers an agent with the Synapse API. If agent exists, it will be retrieved.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The registered or existing Agent object.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)
        agent_response = await register_agent(
            cloud_agent_id=self.cloud_agent_id,
            cloud_alias_id=self.cloud_alias_id,
            synapse_client=syn,
        )
        return self.fill_from_dict(agent_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Agent: {self.registration_id}"
    )
    async def get_async(self, *, synapse_client: Optional[Synapse] = None) -> "Agent":
        """Gets an existing agent.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The existing Agent object.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)
        agent_response = await get_agent(
            registration_id=self.registration_id,
            synapse_client=syn,
        )
        return self.fill_from_dict(agent_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Start_Agent_Session: {self.registration_id}"
    )
    async def start_session_async(
        self,
        *,
        access_level: Optional[
            AgentSessionAccessLevel
        ] = AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
        synapse_client: Optional[Synapse] = None,
    ) -> "AgentSession":
        """Starts an agent session.

        Arguments:
            access_level: The access level of the agent session.
                Must be one of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA, or WRITE_YOUR_PRIVATE_DATA.
                Defaults to PUBLICLY_ACCESSIBLE.
            synapse_client: The Synapse client to use for the request.
                If None, the default client will be used.

        Returns:
            The new AgentSession object.
        """
        access_level = AgentSessionAccessLevel(access_level)
        syn = Synapse.get_client(synapse_client=synapse_client)
        session = await AgentSession(
            agent_registration_id=self.registration_id, access_level=access_level
        ).start_async(synapse_client=syn)
        self.sessions[session.id] = session
        self.current_session = session.id
        return session

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Agent_Session: {self.registration_id}"
    )
    async def get_session_async(
        self, *, session_id: str, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        syn = Synapse.get_client(synapse_client=synapse_client)
        session = await AgentSession(id=session_id).get_async(synapse_client=syn)
        if session.id not in self.sessions:
            self.sessions[session.id] = session
        self.current_session = session.id
        return session

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Prompt_Agent_Session: {self.registration_id}"
    )
    async def prompt(
        self,
        *,
        session_id: Optional[str] = None,
        prompt: str,
        enable_trace: bool = False,
        newer_than: Optional[int] = None,
        print_response: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Sends a prompt to the agent for the current session.
            If no session is currently active, a new session will be started.

        Arguments:
            session_id: The ID of the session to send the prompt to. If None, the current session will be used.
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt.
            newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
            print_response: Whether to print the response to the console.
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)

        # TODO: Iron this out. It's a little confusing.
        if session_id:
            if session_id not in self.sessions:
                await self.get_session_async(session_id=session_id, synapse_client=syn)
            else:
                self.current_session = session_id
        else:
            if not self.current_session:
                await self.start_session_async(synapse_client=syn)

        await self.sessions[self.current_session].prompt_async(
            prompt=prompt,
            enable_trace=enable_trace,
            newer_than=newer_than,
            print_response=print_response,
            synapse_client=syn,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Agent_Session_Chat_History: {self.registration_id}"
    )
    def get_chat_history(self) -> List[AgentPrompt]:
        """Gets the chat history for the current session."""
        # TODO: Is this the best way to do this?
        return self.sessions[self.current_session].chat_history
