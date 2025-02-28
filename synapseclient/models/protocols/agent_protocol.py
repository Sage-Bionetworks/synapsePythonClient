"""Protocol for the methods of the Agent and AgentSession classes that have
synchronous counterparts generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import (
        Agent,
        AgentPrompt,
        AgentSession,
        AgentSessionAccessLevel,
    )


class AgentSessionSynchronousProtocol(Protocol):
    """Protocol for the methods of the AgentSession class that have synchronous counterparts
    generated at runtime."""

    def start(self, *, synapse_client: Optional[Synapse] = None) -> "AgentSession":
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
        """
        return self

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "AgentSession":
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
        """
        return self

    def update(self, *, synapse_client: Optional[Synapse] = None) -> "AgentSession":
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

                from synapseclient import Synapse
                from synapseclient.models.agent import AgentSession, AgentSessionAccessLevel

                syn = Synapse()
                syn.login()

                my_session = AgentSession(id="foo").get()
                my_session.access_level = AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
                my_session.update()
        """
        return self

    def prompt(
        self,
        prompt: str,
        enable_trace: bool = False,
        print_response: bool = False,
        newer_than: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "AgentPrompt":
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
        """
        return AgentPrompt()


class AgentSynchronousProtocol(Protocol):
    """Protocol for the methods of the Agent class that have synchronous counterparts
    generated at runtime."""

    def register(self, *, synapse_client: Optional[Synapse] = None) -> "Agent":
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
        """
        return self

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "Agent":
        """Gets an existing agent.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The existing Agent object.

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
        """
        return self

    def start_session(
        self,
        access_level: Optional["AgentSessionAccessLevel"] = "PUBLICLY_ACCESSIBLE",
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

                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                my_agent = Agent()
                my_agent.start_session()
                my_agent.prompt(
                    prompt="Can you tell me about the AD Knowledge Portal dataset?",
                    enable_trace=True,
                    print_response=True,
                )

        Example: Start a session and send a prompt with a custom agent.
            The baseline Synapse Agent is the default agent used when a registration ID is not provided.

                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                my_agent = Agent(cloud_agent_id="foo")
                my_agent.start_session()
                my_agent.prompt(
                    prompt="Hello",
                    enable_trace=True,
                    print_response=True,
                )
        """
        return AgentSession()

    def get_session(
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

                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                my_session = Agent().get_session(session_id="foo")
                my_session.prompt(
                    prompt="Hello",
                    enable_trace=True,
                    print_response=True,
                )
        """
        return AgentSession()

    def prompt(
        self,
        prompt: str,
        enable_trace: bool = False,
        print_response: bool = False,
        session: Optional["AgentSession"] = None,
        newer_than: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "AgentPrompt":
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

                from synapseclient import Synapse
                from synapseclient.models import Agent

                syn = Synapse()
                syn.login()

                my_agent = Agent()
                my_agent.prompt(
                    prompt="Can you tell me about the AD Knowledge Portal dataset?",
                    enable_trace=True,
                    print_response=True,
                )

        Example: Prompt a custom agent.
            If you have already registered a custom agent, you can prompt it by providing the agent's registration ID.

                from synapseclient import Synapse
                from synapseclient.models.agent import Agent

                syn = Synapse()
                syn.login()

                my_agent = Agent(registration_id="foo")
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
        return AgentPrompt()
