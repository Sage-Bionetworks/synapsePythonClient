"""Protocol for the methods of the Agent and AgentSession classes that have
synchronous counterparts generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Agent, AgentSession, AgentSessionAccessLevel


class AgentSessionSynchronousProtocol(Protocol):
    """Protocol for the methods of the AgentSession class that have synchronous counterparts
    generated at runtime."""

    def start(self, *, synapse_client: Optional[Synapse] = None) -> "AgentSession":
        """Starts an agent session.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The new AgentSession object.
        """
        return self

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "AgentSession":
        """Gets an existing agent session.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The existing AgentSession object.
        """
        return self

    def update(self, *, synapse_client: Optional[Synapse] = None) -> "AgentSession":
        """Updates an existing agent session.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The updated AgentSession object.
        """
        return self

    def prompt(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Sends a prompt to the agent and adds the response to the AgentSession's chat history.

        Arguments:
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt.
            print_response: Whether to print the response to the console.
            newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.
        """
        return None


class AgentSynchronousProtocol(Protocol):
    """Protocol for the methods of the Agent class that have synchronous counterparts
    generated at runtime."""

    def register(self, *, synapse_client: Optional[Synapse] = None) -> "Agent":
        """Registers an agent with the Synapse API. If agent exists, it will be retrieved.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The registered or existing Agent object.
        """
        return self

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "Agent":
        """Gets an existing agent.

        Arguments:
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The existing Agent object.
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
                Must be one of PUBLICLY_ACCESSIBLE, READ_YOUR_PRIVATE_DATA, or WRITE_YOUR_PRIVATE_DATA.
                Defaults to PUBLICLY_ACCESSIBLE.
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.

        Returns:
            The new AgentSession object.
        """
        return AgentSession()

    def get_session(
        self, session_id: str, *, synapse_client: Optional[Synapse] = None
    ) -> "AgentSession":
        """Gets an existing agent session.
        Adds the session to the Agent's sessions dictionary and sets it as the current session.

        Arguments:
            session_id: The ID of the session to get.
            synapse_client: The Synapse client to use for the request.
                If None, the default client will be used.

        Returns:
            The existing AgentSession object.
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
    ) -> None:
        """Sends a prompt to the agent for the current session.
        If no session is currently active, a new session will be started.

        Arguments:
            prompt: The prompt to send to the agent.
            enable_trace: Whether to enable trace for the prompt.
            print_response: Whether to print the response to the console.
            session_id: The ID of the session to send the prompt to. If None, the current session will be used.
            newer_than: The timestamp to get trace results newer than. Defaults to None (all results).
            synapse_client: The Synapse client to use for the request. If None, the default client will be used.
        """
        return None
