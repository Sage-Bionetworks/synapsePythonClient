from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from synapseclient import Synapse
from synapseclient.api import (
    get_agent,
    get_session,
    get_trace,
    register_agent,
    start_session,
    update_session,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.models.mixins import AsynchronousCommunicator
from synapseclient.models.protocols.agent_protocol import (
    AgentSessionSynchronousProtocol,
    AgentSynchronousProtocol,
)


class AgentType(str, Enum):
    """
    Enum representing the type of agent as defined in
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentType.html>

    - BASELINE is a default agent provided by Synapse.
    - CUSTOM is a custom agent that has been registered by a user.
    """

    BASELINE = "BASELINE"
    CUSTOM = "CUSTOM"


class AgentSessionAccessLevel(str, Enum):
    """
    Enum representing the access level of the agent session as defined in
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentAccessLevel.html>

    - PUBLICLY_ACCESSIBLE: The agent can only access publicly accessible data.
    - READ_YOUR_PRIVATE_DATA: The agent can read the user's private data.
    - WRITE_YOUR_PRIVATE_DATA: The agent can write to the user's private data.
    """

    PUBLICLY_ACCESSIBLE = "PUBLICLY_ACCESSIBLE"
    READ_YOUR_PRIVATE_DATA = "READ_YOUR_PRIVATE_DATA"
    WRITE_YOUR_PRIVATE_DATA = "WRITE_YOUR_PRIVATE_DATA"


@dataclass
class AgentPrompt(AsynchronousCommunicator):
    """Represents a prompt, response, and metadata within an AgentSession.

    Attributes:
        id: The unique ID of the agent prompt.
        session_id: The ID of the session that the prompt is associated with.
        prompt: The prompt to send to the agent.
        response: The response from the agent.
        enable_trace: Whether tracing is enabled for the prompt.
        trace: The trace of the agent session.
    """

    concrete_type: str = AGENT_CHAT_REQUEST

    id: Optional[str] = None
    """The unique ID of the agent prompt."""

    session_id: Optional[str] = None
    """The ID of the session that the prompt is associated with."""

    prompt: Optional[str] = None
    """The prompt sent to the agent."""

    response: Optional[str] = None
    """The response from the agent."""

    enable_trace: Optional[bool] = False
    """Whether tracing is enabled for the prompt."""

    trace: Optional[str] = None
    """The trace or "thought process" of the agent when responding to the prompt."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "concreteType": self.concrete_type,
            "sessionId": self.session_id,
            "chatText": self.prompt,
            "enableTrace": self.enable_trace,
        }

    def fill_from_dict(self, synapse_response: Dict[str, str]) -> "AgentPrompt":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The AgentPrompt object.
        """
        self.id = synapse_response.get("jobId", None)
        self.session_id = synapse_response.get("sessionId", None)
        self.response = synapse_response.get("responseText", None)
        return self

    async def _post_exchange_async(
        self, *, synapse_client: Optional[Synapse] = None, **kwargs
    ) -> None:
        """Retrieves information about the trace of this prompt with the agent.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.
        """
        if self.enable_trace:
            trace_response = await get_trace(
                prompt_id=self.id,
                newer_than=kwargs.get("newer_than", None),
                synapse_client=synapse_client,
            )
            self.trace = trace_response["page"][0]["message"]


@dataclass
@async_to_sync
class AgentSession(AgentSessionSynchronousProtocol):
    """Represents a [Synapse Agent Session](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentSession.html)

    Attributes:
        id: The unique ID of the agent session.
            Can only be used by the user that created it.
        access_level: The access level of the agent session.
            One of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA,
            or WRITE_YOUR_PRIVATE_DATA.
        started_on: The date the agent session was started.
        started_by: The ID of the user who started the agent session.
        modified_on: The date the agent session was last modified.
        agent_registration_id: The registration ID of the agent that will
            be used for this session.
        etag: The etag of the agent session.

    Note: It is recommended to use the `Agent` class to conduct chat sessions,
    but you are free to use AgentSession directly if you wish.

    Example: Start a session and send a prompt.
        Start a session with a custom agent by providing the agent's registration ID and calling `start()`.
        Then, send a prompt to the agent.

            from synapseclient import Synapse
            from synapseclient.models.agent import AgentSession

            syn = Synapse()
            syn.login()

            my_session = AgentSession(agent_registration_id="foo").start()
            my_session.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
            )

    Example: Get an existing session and send a prompt.
        Retrieve an existing session by providing the session ID and calling `get()`.
        Then, send a prompt to the agent.

            from synapseclient import Synapse
            from synapseclient.models.agent import AgentSession

            syn = Synapse()
            syn.login()

            my_session = AgentSession(id="foo").get()
            my_session.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
            )

    Example: Update the access level of an existing session.
        Retrieve an existing session by providing the session ID and calling `get()`.
        Then, update the access level of the session and call `update()`.

            from synapseclient import Synapse
            from synapseclient.models.agent import AgentSession, AgentSessionAccessLevel

            syn = Synapse()
            syn.login()

            my_session = AgentSession(id="foo").get()
            my_session.access_level = AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
            my_session.update()
    """

    id: Optional[str] = None
    """The unique ID of the agent session.
    Can only be used by the user that created it."""

    access_level: Optional[
        AgentSessionAccessLevel
    ] = AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE
    """The access level of the agent session.
        One of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA, or
        WRITE_YOUR_PRIVATE_DATA. Defaults to PUBLICLY_ACCESSIBLE.
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

    @otel_trace_method(method_to_trace_name=lambda self, **kwargs: "Start_Session")
    async def start_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        """Starts an agent session.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The new AgentSession object.

        Example: Start a session and send a prompt.
            Start a session with a custom agent by providing the agent's registration ID and calling `start()`.
            Then, send a prompt to the agent.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import AgentSession

                syn = Synapse()
                syn.login()

                async def main():
                    my_session = await AgentSession(agent_registration_id="foo").start_async()
                    await my_session.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        session_response = await start_session(
            access_level=self.access_level,
            agent_registration_id=self.agent_registration_id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(synapse_agent_session=session_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Session: {self.id}"
    )
    async def get_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        """Gets an agent session.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The retrieved AgentSession object.

        Example: Get an existing session and send a prompt.
            Retrieve an existing session by providing the session ID and calling `get()`.
            Then, send a prompt to the agent.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import AgentSession

                syn = Synapse()
                syn.login()

                async def main():
                    my_session = await AgentSession(id="foo").get_async()
                    await my_session.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        session_response = await get_session(
            id=self.id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(synapse_agent_session=session_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Update_Session: {self.id}"
    )
    async def update_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "AgentSession":
        """Updates an agent session.
        Only updates to the access level are currently supported.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The updated AgentSession object.

        Example: Update the access level of an existing session.
            Retrieve an existing session by providing the session ID and calling `get()`.
            Then, update the access level of the session and call `update()`.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import AgentSession, AgentSessionAccessLevel

                syn = Synapse()
                syn.login()

                async def main():
                    my_session = await AgentSession(id="foo").get_async()
                    my_session.access_level = AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
                    await my_session.update_async()

                asyncio.run(main())
        """
        session_response = await update_session(
            id=self.id,
            access_level=self.access_level,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(synapse_agent_session=session_response)

    @otel_trace_method(method_to_trace_name=lambda self, **kwargs: f"Prompt: {self.id}")
    async def prompt_async(
        self,
        prompt: str,
        enable_trace: bool = False,
        print_response: bool = False,
        newer_than: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AgentPrompt:
        """Sends a prompt to the agent and adds the response to the AgentSession's
        chat history. A session must be started before sending a prompt.

        Arguments:
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt.
            print_response: Whether to print the response to the console.
            newer_than: The timestamp to get trace results newer than.
                Defaults to None (all results).
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Example: Send a prompt within an existing session.
            Retrieve an existing session by providing the session ID and calling `get()`.
            Then, send a prompt to the agent.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import AgentSession

                syn = Synapse()
                syn.login()

                async def main():
                    my_session = await AgentSession(id="foo").get_async()
                    await my_session.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        agent_prompt = await AgentPrompt(
            prompt=prompt, session_id=self.id, enable_trace=enable_trace
        ).send_job_and_wait_async(
            synapse_client=synapse_client, post_exchange_args={"newer_than": newer_than}
        )
        self.chat_history.append(agent_prompt)
        if print_response:
            client = Synapse.get_client(synapse_client=synapse_client)
            client.logger.info(f"PROMPT:\n{prompt}\n")
            client.logger.info(f"RESPONSE:\n{agent_prompt.response}\n")
            if enable_trace:
                client.logger.info(f"TRACE:\n{agent_prompt.trace}")
        return agent_prompt


@dataclass
@async_to_sync
class Agent(AgentSynchronousProtocol):
    """Represents a [Synapse Agent Registration](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/agent/AgentRegistration.html)

    Attributes:
        cloud_agent_id: The unique ID of the agent in the cloud provider.
        cloud_alias_id: The alias ID of the agent in the cloud provider.
                    Defaults to 'TSTALIASID' in the Synapse API.
        registration_id: The ID number of the agent assigned by Synapse.
        registered_on: The date the agent was registered.
        type: The type of agent.
        sessions: A dictionary of AgentSession objects, keyed by session ID.
        current_session: The current session. Prompts will be sent to this session by default.

    Example: Chat with the baseline Synapse Agent
        You can chat with the same agent which is available in the Synapse UI
        at https://www.synapse.org/Chat:default. By default, this "baseline" agent
        is used when a registration ID is not provided. In the background,
        the Agent class will start a session and set that new session as the
        current session if one is not already set.

            from synapseclient import Synapse
            from synapseclient.models.agent import Agent

            syn = Synapse()
            syn.login()

            my_agent = Agent()
            my_agent.prompt(
                prompt="Can you tell me about the AD Knowledge Portal dataset?",
                enable_trace=True,
                print_response=True,
            )

    Example: Register and chat with a custom agent
        **Only available for internal users (Sage Bionetworks employees)**

        Alternatively, you can register a custom agent and chat with it provided
        you have already created it.

            from synapseclient import Synapse
            from synapseclient.models.agent import Agent

            syn = Synapse()
            syn.login()

            my_agent = Agent(cloud_agent_id="foo")
            my_agent.register()
            my_agent.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
            )

    Example: Get and chat with an existing agent
        Retrieve an existing agent by providing the agent's registration ID and calling `get()`.
        Then, send a prompt to the agent.

            from synapseclient import Synapse
            from synapseclient.models.agent import Agent

            syn = Synapse()
            syn.login()

            my_agent = Agent(registration_id="foo").get()
            my_agent.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
            )

    Advanced Example: Start and prompt multiple sessions
        Here, we connect to a custom agent and start one session with the prompt "Hello".
        In the background, this first session is being set as the current session
        and future prompts will be sent to this session by default. If we want to send a
        prompt to a different session, we can do so by starting it and calling prompt again,
        but with our new session as an argument. We now have two sessions, both stored in the
        `my_agent.sessions` dictionary. After the second prompt, `my_second_session` is now
        the current session.

            syn = Synapse()
            syn.login()

            my_agent = Agent(registration_id="foo").get()

            my_agent.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
            )

            my_second_session = my_agent.start_session()
            my_agent.prompt(
                prompt="Hello again",
                enable_trace=True,
                print_response=True,
                session=my_second_session,
            )
    """

    cloud_agent_id: Optional[str] = None
    """The unique ID of the agent in the cloud provider."""

    cloud_alias_id: Optional[str] = None
    """The alias ID of the agent in the cloud provider.
        Defaults to 'TSTALIASID' in the Synapse API.
    """

    registration_id: Optional[int] = None
    """The ID number of the agent assigned by Synapse."""

    registered_on: Optional[datetime] = None
    """The date the agent was registered."""

    type: Optional[AgentType] = None
    """The type of agent. One of either BASELINE or CUSTOM."""

    sessions: Dict[str, AgentSession] = field(default_factory=dict)
    """A dictionary of AgentSession objects, keyed by session ID."""

    current_session: Optional[AgentSession] = None
    """The current session. Prompts will be sent to this session by default."""

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
        self.type = (
            AgentType(agent_registration.get("type"))
            if agent_registration.get("type", None)
            else None
        )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Register_Agent: {self.registration_id}"
    )
    async def register_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "Agent":
        """Registers an agent with the Synapse API.
        If agent already exists, it will be retrieved.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The registered or existing Agent object.

        Example: Register and chat with a custom agent
            **Only available for internal users (Sage Bionetworks employees)**

            Alternatively, you can register a custom agent and chat with it provided
            you have already created it.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent(cloud_agent_id="foo")
                    await my_agent.register_async()
                    await my_agent.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        agent_response = await register_agent(
            cloud_agent_id=self.cloud_agent_id,
            cloud_alias_id=self.cloud_alias_id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(agent_registration=agent_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Agent: {self.registration_id}"
    )
    async def get_async(self, *, synapse_client: Optional[Synapse] = None) -> "Agent":
        """Gets an existing custom agent. There is no need to use this method
        if you are trying to use the baseline agent.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The existing Agent object.

        Example: Get and chat with an existing agent
            Retrieve an existing custom agent by providing the agent's registration ID and calling `get_async()`.
            Then, send a prompt to the agent.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Agent, AgentSessionAccessLevel

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = await Agent(registration_id="foo").get_async()
                    await my_agent.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        if self.registration_id is None:
            raise ValueError(
                "Registration ID is required to retrieve a custom agent. "
                "If you are trying to use the baseline agent, you do not need to "
                "use `get` or `get_async`. Instead, simply create an `Agent` object "
                "and start prompting `my_agent = Agent(); my_agent.prompt(...)`.",
            )
        agent_response = await get_agent(
            registration_id=self.registration_id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(agent_registration=agent_response)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Start_Agent_Session: {self.registration_id}"
    )
    async def start_session_async(
        self,
        access_level: Optional[
            AgentSessionAccessLevel
        ] = AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "AgentSession":
        """Starts an agent session.
        Adds the session to the Agent's sessions dictionary and sets it as the current session.

        Arguments:
            access_level: The access level of the agent session.
                Must be one of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA,
                or WRITE_YOUR_PRIVATE_DATA.
                Defaults to PUBLICLY_ACCESSIBLE.
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The new AgentSession object.

        Example: Start a session and send a prompt with the baseline Synapse Agent.
            The baseline Synapse Agent is the default agent used when a registration ID is not provided.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent()
                    await my_agent.start_session_async()
                    await my_agent.prompt_async(
                        prompt="Can you tell me about the AD Knowledge Portal dataset?",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())

        Example: Start a session and send a prompt with a custom agent.
            The baseline Synapse Agent is the default agent used when a registration ID is not provided.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent(cloud_agent_id="foo")
                    await my_agent.start_session_async()
                    await my_agent.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        access_level = AgentSessionAccessLevel(access_level)
        session = await AgentSession(
            agent_registration_id=self.registration_id, access_level=access_level
        ).start_async(synapse_client=synapse_client)
        self.sessions[session.id] = session
        self.current_session = session
        return session

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Agent_Session: {self.registration_id}"
    )
    async def get_session_async(
        self, session_id: str, *, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        """Gets an existing agent session.
        Adds the session to the Agent's sessions dictionary and
        sets it as the current session.

        Arguments:
            session_id: The ID of the session to get.
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The existing AgentSession object.

        Example: Get an existing session and send a prompt.
            Retrieve an existing session by providing the session ID and calling `get()`.
            Then, send a prompt to the agent.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_session = await Agent().get_session_async(session_id="foo")
                    await my_session.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())
        """
        session = await AgentSession(id=session_id).get_async(
            synapse_client=synapse_client
        )
        if session.id not in self.sessions:
            self.sessions[session.id] = session
        self.current_session = session
        return session

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Prompt_Agent_Session: {self.registration_id}"
    )
    async def prompt_async(
        self,
        prompt: str,
        enable_trace: bool = False,
        print_response: bool = False,
        session: Optional[AgentSession] = None,
        newer_than: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AgentPrompt:
        """Sends a prompt to the agent for the current session.
            If no session is currently active, a new session will be started.

        Arguments:
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt.
            print_response: Whether to print the response to the console.
            session_id: The ID of the session to send the prompt to.
                If None, the current session will be used.
            newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Example: Prompt the baseline Synapse Agent.
            The baseline Synapse Agent is equivilent to the Agent available in the Synapse UI.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent()
                    await my_agent.prompt_async(
                        prompt="Can you tell me about the AD Knowledge Portal dataset?",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())

        Example: Prompt a custom agent.
            If you have already registered a custom agent, you can prompt it by providing the agent's registration ID.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent(registration_id="foo")
                    await my_agent.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                asyncio.run(main())

        Advanced Example: Start and prompt multiple sessions
            Here, we connect to a custom agent and start one session with the prompt "Hello".
            In the background, this first session is being set as the current session
            and future prompts will be sent to this session by default. If we want to send a
            prompt to a different session, we can do so by starting it and calling prompt again,
            but with our new session as an argument. We now have two sessions, both stored in the
            `my_agent.sessions` dictionary. After the second prompt, `my_second_session` is now
            the current session.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent(registration_id="foo").get()
                    await my_agent.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )

                    my_second_session = await my_agent.start_session_async()
                    await my_agent.prompt_async(
                        prompt="Hello again",
                        enable_trace=True,
                        print_response=True,
                        session=my_second_session,
                    )

                asyncio.run(main())
        """
        if session:
            await self.get_session_async(
                session_id=session.id, synapse_client=synapse_client
            )
        else:
            if not self.current_session:
                await self.start_session_async(synapse_client=synapse_client)

        return await self.current_session.prompt_async(
            prompt=prompt,
            enable_trace=enable_trace,
            newer_than=newer_than,
            print_response=print_response,
            synapse_client=synapse_client,
        )

    def get_chat_history(self) -> Union[List[AgentPrompt], None]:
        """Gets the chat history for the current session.

        Example: Get the chat history for the current session.
            First, send a prompt to the agent.
            Then, retrieve the chat history for the current session by calling `get_chat_history()`.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                async def main():
                    my_agent = Agent()
                    await my_agent.prompt_async(
                        prompt="Hello",
                        enable_trace=True,
                        print_response=True,
                    )
                    print(my_agent.get_chat_history())

                asyncio.run(main())
        """
        return self.current_session.chat_history if self.current_session else None
