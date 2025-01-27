"""Integration tests for the synchronous methods of the AgentSession and Agent classes."""

import pytest

from synapseclient import Synapse
from synapseclient.models.agent import Agent, AgentSession, AgentSessionAccessLevel

# These are the ID values for a "Hello World" agent registered on Synapse.
# The Bedrock agent is hosted on Sage Bionetworks AWS infrastructure.
# CFN Template:
# https://raw.githubusercontent.com/Sage-Bionetworks-Workflows/dpe-agents/refs/heads/main/client_integration_test/template.json
CLOUD_AGENT_ID = "QOTV3KQM1X"
AGENT_REGISTRATION_ID = "29"


class TestAgentSession:
    """Integration tests for the synchronous methods of the AgentSession class."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_start(self) -> None:
        # GIVEN an agent session with a valid agent registration id
        agent_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID)

        # WHEN the start method is called
        result_session = agent_session.start(synapse_client=self.syn)

        # THEN the result should be an AgentSession object
        # with expected attributes including an empty chat history
        assert result_session.id is not None
        assert (
            result_session.access_level == AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE
        )
        assert result_session.started_on is not None
        assert result_session.started_by is not None
        assert result_session.modified_on is not None
        assert result_session.agent_registration_id == str(AGENT_REGISTRATION_ID)
        assert result_session.etag is not None
        assert result_session.chat_history == []

    async def test_get(self) -> None:
        # GIVEN an agent session with a valid agent registration id
        agent_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID)
        # WHEN I start a session
        agent_session.start(synapse_client=self.syn)
        # THEN I expect to be able to get the session with its id
        new_session = AgentSession(id=agent_session.id).get(synapse_client=self.syn)
        assert new_session == agent_session

    async def test_update(self) -> None:
        # GIVEN an agent session with a valid agent registration id and access level set
        agent_session = AgentSession(
            agent_registration_id=AGENT_REGISTRATION_ID,
            access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
        )
        # WHEN I start a session
        agent_session.start(synapse_client=self.syn)
        # AND I update the access level of the session
        agent_session.access_level = AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
        agent_session.update(synapse_client=self.syn)
        # THEN I expect the access level to be updated
        updated_session = AgentSession(id=agent_session.id).get(synapse_client=self.syn)
        assert (
            updated_session.access_level
            == AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
        )

    async def test_prompt(self) -> None:
        # GIVEN an agent session with a valid agent registration id
        agent_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID)
        # WHEN I start a session
        agent_session.start(synapse_client=self.syn)
        # THEN I expect to be able to prompt the agent
        agent_session.prompt(
            prompt="hello",
            enable_trace=True,
        )
        # AND I expect the chat history to be updated with the prompt and response
        assert len(agent_session.chat_history) == 1
        assert agent_session.chat_history[0].prompt == "hello"
        assert agent_session.chat_history[0].response is not None
        assert agent_session.chat_history[0].trace is not None


class TestAgent:
    """Integration tests for the synchronous methods of the Agent class."""

    def get_test_agent(self) -> Agent:
        return Agent(
            cloud_agent_id=CLOUD_AGENT_ID,
            cloud_alias_id="TSTALIASID",
            registration_id=AGENT_REGISTRATION_ID,
            registered_on="2025-01-16T18:57:35.680Z",
            type="CUSTOM",
            sessions={},
            current_session=None,
        )

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_register(self) -> None:
        # GIVEN an Agent with a valid agent AWS id
        agent = Agent(cloud_agent_id=CLOUD_AGENT_ID)
        # WHEN I register the agent
        agent.register(synapse_client=self.syn)
        # THEN I expect the agent to be registered
        expected_agent = self.get_test_agent()
        assert agent == expected_agent

    async def test_get(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID)
        # WHEN I get the agent
        agent.get(synapse_client=self.syn)
        # THEN I expect the agent to be returned
        expected_agent = self.get_test_agent()
        assert agent == expected_agent

    async def test_get_no_registration_id(self) -> None:
        # GIVEN an Agent with no registration id
        agent = Agent()
        # WHEN I get the agent, I expect a ValueError to be raised
        with pytest.raises(ValueError, match="Registration ID is required"):
            agent.get(synapse_client=self.syn)

    async def test_start_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID).get(
            synapse_client=self.syn
        )
        # WHEN I start a session
        agent.start_session(synapse_client=self.syn)
        # THEN I expect a current session to be set
        assert agent.current_session is not None
        # AND I expect the session to be in the sessions dictionary
        assert agent.sessions[agent.current_session.id] == agent.current_session

    async def test_get_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID).get(
            synapse_client=self.syn
        )
        # WHEN I start a session
        session = agent.start_session(synapse_client=self.syn)
        # THEN I expect to be able to get the session with its id
        existing_session = agent.get_session(session_id=session.id)
        # AND I expect those sessions to be the same
        assert existing_session == session
        # AND I expect it to be the current session
        assert existing_session == agent.current_session

    async def test_prompt_with_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID).get(
            synapse_client=self.syn
        )
        # AND a session started separately
        session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID).start(
            synapse_client=self.syn
        )
        # WHEN I prompt the agent with a session
        agent.prompt(prompt="hello", enable_trace=True, session=session)
        test_session = agent.sessions[session.id]
        # THEN I expect the chat history to be updated with the prompt and response
        assert len(test_session.chat_history) == 1
        assert test_session.chat_history[0].prompt == "hello"
        assert test_session.chat_history[0].response is not None
        assert test_session.chat_history[0].trace is not None
        # AND I expect the current session to be the session provided
        assert agent.current_session.id == session.id

    async def test_prompt_no_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID).get(
            synapse_client=self.syn
        )
        # WHEN I prompt the agent without a current session set
        # and no session provided
        agent.prompt(prompt="hello", enable_trace=True)
        # THEN I expect a new session to be started and set as the current session
        assert agent.current_session is not None
        # AND I expect the chat history to be updated with the prompt and response
        assert len(agent.current_session.chat_history) == 1
        assert agent.current_session.chat_history[0].prompt == "hello"
        assert agent.current_session.chat_history[0].response is not None
        assert agent.current_session.chat_history[0].trace is not None
