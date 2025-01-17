"""Integration tests for the asynchronous methods of the AgentPrompt, AgentSession, and Agent classes."""

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.models.agent import (
    Agent,
    AgentPrompt,
    AgentSession,
    AgentSessionAccessLevel,
)

AGENT_AWS_ID = "QOTV3KQM1X"
AGENT_REGISTRATION_ID = "29"


class TestAgentPrompt:
    """Integration tests for the synchronous methods of the AgentPrompt class."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_send_job_and_wait_async_with_post_exchange_args(self) -> None:
        # GIVEN an AgentPrompt with a valid concrete type, prompt, and enable_trace
        test_prompt = AgentPrompt(
            concrete_type=AGENT_CHAT_REQUEST,
            prompt="hello",
            enable_trace=True,
        )
        # AND the ID of an existing agent session
        test_session = await AgentSession(
            agent_registration_id=AGENT_REGISTRATION_ID
        ).start_async(synapse_client=self.syn)
        test_prompt.session_id = test_session.id
        # WHEN I send the job and wait for it to complete
        await test_prompt.send_job_and_wait_async(
            post_exchange_args={"newer_than": 0},
            synapse_client=self.syn,
        )
        # THEN I expect the AgentPrompt to be updated with the response and trace
        assert test_prompt.response is not None
        assert test_prompt.trace is not None


class TestAgentSession:
    """Integration tests for the synchronous methods of the AgentSession class."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_start(self) -> None:
        # GIVEN an agent session with a valid agent registration id
        agent_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID)

        # WHEN the start method is called
        result_session = await agent_session.start_async(synapse_client=self.syn)

        # THEN the result should be an AgentSession object
        # with expected attributes including an empty chat history
        assert result_session.id is not None
        assert (
            result_session.access_level == AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE
        )
        assert result_session.started_on is not None
        assert result_session.started_by is not None
        assert result_session.modified_on is not None
        assert result_session.agent_registration_id == AGENT_REGISTRATION_ID
        assert result_session.etag is not None
        assert result_session.chat_history == []

    async def test_get(self) -> None:
        # GIVEN an agent session with a valid agent registration id
        agent_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID)
        # WHEN I start a session
        await agent_session.start_async(synapse_client=self.syn)
        # THEN I expect to be able to get the session with its id
        new_session = await AgentSession(id=agent_session.id).get_async(
            synapse_client=self.syn
        )
        assert new_session == agent_session

    async def test_update(self) -> None:
        # GIVEN an agent session with a valid agent
        # registration id and access level set
        agent_session = AgentSession(
            agent_registration_id=AGENT_REGISTRATION_ID,
            access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
        )
        # WHEN I start a session
        await agent_session.start_async(synapse_client=self.syn)
        # AND I update the access level of the session
        agent_session.access_level = AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
        await agent_session.update_async(synapse_client=self.syn)
        # THEN I expect the access level to be updated
        assert (
            agent_session.access_level == AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
        )

    async def test_prompt(self) -> None:
        # GIVEN an agent session with a valid agent registration id
        agent_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID)
        # WHEN I start a session
        await agent_session.start_async(synapse_client=self.syn)
        # THEN I expect to be able to prompt the agent
        await agent_session.prompt_async(
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
            cloud_agent_id="QOTV3KQM1X",
            cloud_alias_id="TSTALIASID",
            registration_id="29",
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
        agent = Agent(cloud_agent_id=AGENT_AWS_ID)
        # WHEN I register the agent
        await agent.register_async(synapse_client=self.syn)
        # THEN I expect the agent to be registered
        expected_agent = self.get_test_agent()
        assert agent == expected_agent

    async def test_get(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID)
        # WHEN I get the agent
        await agent.get_async(synapse_client=self.syn)
        # THEN I expect the agent to be returned
        expected_agent = self.get_test_agent()
        assert agent == expected_agent

    async def test_start_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID)
        # WHEN I start a session
        await agent.start_session_async(synapse_client=self.syn)
        # THEN I expect a current session to be set
        assert agent.current_session is not None
        # AND I expect the session to be in the sessions dictionary
        assert agent.sessions[agent.current_session.id] == agent.current_session

    async def test_get_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = Agent(registration_id=AGENT_REGISTRATION_ID)
        # WHEN I start a session
        await agent.start_session_async(synapse_client=self.syn)
        # THEN I expect to be able to get the session with its id
        existing_session = await agent.get_session_async(
            session_id=agent.current_session.id
        )
        # AND I expect those sessions to be the same
        assert existing_session == agent.current_session

    async def test_prompt_with_session(self) -> None:
        # GIVEN an Agent with a valid agent registration id
        agent = await Agent(registration_id=AGENT_REGISTRATION_ID).get_async(
            synapse_client=self.syn
        )
        # AND a session started separately
        session = await AgentSession(
            agent_registration_id=AGENT_REGISTRATION_ID
        ).start_async(synapse_client=self.syn)
        # WHEN I prompt the agent with a session
        await agent.prompt_async(prompt="hello", enable_trace=True, session=session)
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
        agent = await Agent(registration_id=AGENT_REGISTRATION_ID).get_async(
            synapse_client=self.syn
        )
        # WHEN I prompt the agent without a current session set
        # and no session provided
        await agent.prompt_async(prompt="hello", enable_trace=True)
        # THEN I expect a new session to be started and set as the current session
        assert agent.current_session is not None
        # AND I expect the chat history to be updated with the prompt and response
        assert len(agent.current_session.chat_history) == 1
        assert agent.current_session.chat_history[0].prompt == "hello"
        assert agent.current_session.chat_history[0].response is not None
        assert agent.current_session.chat_history[0].trace is not None
