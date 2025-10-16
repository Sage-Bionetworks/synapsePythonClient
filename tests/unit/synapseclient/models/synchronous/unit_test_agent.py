"""Unit tests for Synchronous methods in Agent, AgentSession, and AgentPrompt classes."""

from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import AGENT_CHAT_REQUEST
from synapseclient.models.agent import (
    Agent,
    AgentPrompt,
    AgentSession,
    AgentSessionAccessLevel,
    AgentType,
)


class TestAgentPrompt:
    """Unit tests for the AgentPrompt class' synchronous methods."""

    test_prompt = AgentPrompt(
        concrete_type=AGENT_CHAT_REQUEST,
        session_id="456",
        prompt="Hello",
        enable_trace=True,
    )
    prompt_request = {
        "concreteType": test_prompt.concrete_type,
        "sessionId": test_prompt.session_id,
        "chatText": test_prompt.prompt,
        "enableTrace": test_prompt.enable_trace,
    }
    prompt_response = {
        "jobId": "123",
        "sessionId": "456",
        "responseText": "World",
    }

    def test_to_synapse_request(self) -> None:
        # GIVEN an existing AgentPrompt
        # WHEN I call to_synapse_request
        result_request = self.test_prompt.to_synapse_request()
        # THEN the result should be a dictionary with the correct keys and values
        assert result_request == self.prompt_request

    def test_fill_from_dict(self) -> None:
        # GIVEN an existing AgentPrompt
        # WHEN I call fill_from_dict
        result_prompt = self.test_prompt.fill_from_dict(self.prompt_response)
        # THEN the result should be an AgentPrompt with the correct values
        assert result_prompt == self.test_prompt


class TestAgentSession:
    """Unit tests for the AgentSession class' synchronous methods."""

    test_session = AgentSession(
        id="123",
        access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
        started_on="2024-01-01T00:00:00Z",
        started_by="123456789",
        modified_on="2024-01-01T00:00:00Z",
        agent_registration_id="0",
        etag="11111111-1111-1111-1111-111111111111",
    )

    session_response = {
        "sessionId": test_session.id,
        "agentAccessLevel": test_session.access_level,
        "startedOn": test_session.started_on,
        "startedBy": test_session.started_by,
        "modifiedOn": test_session.modified_on,
        "agentRegistrationId": test_session.agent_registration_id,
        "etag": test_session.etag,
    }

    updated_test_session = AgentSession(
        id=test_session.id,
        access_level=AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA,
        started_on=test_session.started_on,
        started_by=test_session.started_by,
        modified_on=test_session.modified_on,
        agent_registration_id=test_session.agent_registration_id,
        etag=test_session.etag,
    )

    updated_session_response = {
        "sessionId": updated_test_session.id,
        "agentAccessLevel": updated_test_session.access_level,
        "startedOn": updated_test_session.started_on,
        "startedBy": updated_test_session.started_by,
        "modifiedOn": updated_test_session.modified_on,
        "agentRegistrationId": updated_test_session.agent_registration_id,
        "etag": updated_test_session.etag,
    }

    test_prompt_trace_enabled = AgentPrompt(
        concrete_type=AGENT_CHAT_REQUEST,
        session_id="456",
        prompt="Hello",
        enable_trace=True,
        response="World",
        trace="Trace",
    )

    test_prompt_trace_disabled = AgentPrompt(
        concrete_type=AGENT_CHAT_REQUEST,
        session_id="456",
        prompt="Hello",
        enable_trace=False,
        response="World",
        trace=None,
    )

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self) -> None:
        # WHEN I call fill_from_dict on an empty AgentSession with a synapse_response
        result_session = AgentSession().fill_from_dict(self.session_response)
        # THEN the result should be an AgentSession with the correct values
        assert result_session == self.test_session

    def test_start(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.start_session",
                return_value=self.session_response,
            ) as mock_start_session,
            patch.object(
                AgentSession,
                "fill_from_dict",
                return_value=self.test_session,
            ) as mock_fill_from_dict,
        ):
            # GIVEN an AgentSession with access_level and agent_registration_id
            initial_session = AgentSession(
                access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
                agent_registration_id=0,
            )
            # WHEN I call start
            result_session = initial_session.start(synapse_client=self.syn)
            # THEN the result should be an AgentSession with the correct values
            assert result_session == self.test_session
            # AND start_session should have been called once with the correct arguments
            mock_start_session.assert_called_once_with(
                access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
                agent_registration_id=0,
                synapse_client=self.syn,
            )
            # AND fill_from_dict should have been called once with the correct arguments
            mock_fill_from_dict.assert_called_once_with(
                synapse_agent_session=self.session_response
            )

    def test_get(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.get_session",
                return_value=self.session_response,
            ) as mock_get_session,
            patch.object(
                AgentSession,
                "fill_from_dict",
                return_value=self.test_session,
            ) as mock_fill_from_dict,
        ):
            # GIVEN an AgentSession with an agent_registration_id
            initial_session = AgentSession(
                agent_registration_id=0,
            )
            # WHEN I call get
            result_session = initial_session.get(synapse_client=self.syn)
            # THEN the result should be an AgentSession with the correct values
            assert result_session == self.test_session
            # AND get_session should have been called once with the correct arguments
            mock_get_session.assert_called_once_with(
                id=initial_session.id,
                synapse_client=self.syn,
            )
            # AND fill_from_dict should have been called once with the correct arguments
            mock_fill_from_dict.assert_called_once_with(
                synapse_agent_session=self.session_response
            )

    def test_update(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.update_session",
                return_value=self.updated_session_response,
            ) as mock_update_session,
            patch.object(
                AgentSession,
                "fill_from_dict",
                return_value=self.updated_test_session,
            ) as mock_fill_from_dict,
        ):
            # GIVEN an AgentSession with an updated access_level
            # WHEN I call update
            result_session = self.updated_test_session.update(synapse_client=self.syn)
            # THEN the result should be an AgentSession with the correct values
            assert result_session == self.updated_test_session
            # AND update_session should have been called once with the correct arguments
            mock_update_session.assert_called_once_with(
                id=self.updated_test_session.id,
                access_level=AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA,
                synapse_client=self.syn,
            )
            # AND fill_from_dict should have been called once with the correct arguments
            mock_fill_from_dict.assert_called_once_with(
                synapse_agent_session=self.updated_session_response
            )

    def test_prompt_trace_enabled_print_response(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.AgentPrompt.send_job_and_wait_async",
                return_value=self.test_prompt_trace_enabled,
            ) as mock_send_job_and_wait_async,
            patch.object(
                self.syn.logger,
                "info",
            ) as mock_logger_info,
        ):
            # GIVEN an existing AgentSession
            # WHEN I call prompt with trace enabled and print_response enabled
            self.test_session.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
                newer_than=0,
                synapse_client=self.syn,
            )
            # THEN the result should be an AgentPrompt with the correct values appended to the chat history
            assert self.test_prompt_trace_enabled in self.test_session.chat_history
            # AND send_job_and_wait_async should have been called once with the correct arguments
            mock_send_job_and_wait_async.assert_called_once_with(
                timeout=120,
                synapse_client=self.syn,
                post_exchange_args={"newer_than": 0},
            )
            # AND the trace should be printed
            mock_logger_info.assert_called_with(
                f"TRACE:\n{self.test_prompt_trace_enabled.trace}"
            )

    def test_prompt_trace_disabled_no_print(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.AgentPrompt.send_job_and_wait_async",
                return_value=self.test_prompt_trace_disabled,
            ) as mock_send_job_and_wait_async,
            patch.object(
                self.syn.logger,
                "info",
            ) as mock_logger_info,
        ):
            # WHEN I call prompt with trace disabled and print_response disabled
            self.test_session.prompt(
                prompt="Hello",
                enable_trace=False,
                print_response=False,
                newer_than=0,
                synapse_client=self.syn,
            )
            # THEN the result should be an AgentPrompt with the correct values appended to the chat history
            assert self.test_prompt_trace_disabled in self.test_session.chat_history
            # AND send_job_and_wait_async should have been called once with the correct arguments
            mock_send_job_and_wait_async.assert_called_once_with(
                timeout=120,
                synapse_client=self.syn,
                post_exchange_args={"newer_than": 0},
            )
            # AND print should not have been called
            mock_logger_info.assert_not_called()


class TestAgent:
    """Unit tests for the Agent class' synchronous methods."""

    def get_example_agent(self) -> Agent:
        return Agent(
            cloud_agent_id="123",
            cloud_alias_id="456",
            registration_id=0,
            type=AgentType.BASELINE,
            registered_on="2024-01-01T00:00:00Z",
            sessions={},
            current_session=None,
        )

    test_agent = Agent(
        cloud_agent_id="123",
        cloud_alias_id="456",
        registration_id=0,
        type=AgentType.BASELINE,
        registered_on="2024-01-01T00:00:00Z",
        sessions={},
        current_session=None,
    )

    agent_response = {
        "awsAgentId": test_agent.cloud_agent_id,
        "awsAliasId": test_agent.cloud_alias_id,
        "agentRegistrationId": test_agent.registration_id,
        "registeredOn": test_agent.registered_on,
        "type": test_agent.type,
    }

    test_session = AgentSession(
        id="123",
        access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
        started_on="2024-01-01T00:00:00Z",
        started_by="123456789",
        modified_on="2024-01-01T00:00:00Z",
        agent_registration_id="0",
        etag="11111111-1111-1111-1111-111111111111",
    )

    test_prompt = AgentPrompt(
        concrete_type=AGENT_CHAT_REQUEST,
        session_id="456",
        prompt="Hello",
        enable_trace=True,
        response="World",
        trace="Trace",
    )

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self) -> None:
        # GIVEN an empty Agent
        empty_agent = Agent()
        # WHEN I call fill_from_dict on an empty Agent with a synapse_response
        result_agent = empty_agent.fill_from_dict(
            agent_registration=self.agent_response
        )
        # THEN the result should be an Agent with the correct values
        assert result_agent == self.test_agent

    def test_register(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.register_agent",
                return_value=self.agent_response,
            ) as mock_register_agent,
            patch.object(
                Agent,
                "fill_from_dict",
                return_value=self.test_agent,
            ) as mock_fill_from_dict,
        ):
            # GIVEN an Agent with a cloud_agent_id
            initial_agent = Agent(
                cloud_agent_id="123",
                cloud_alias_id="456",
            )
            # WHEN I call register
            result_agent = initial_agent.register(synapse_client=self.syn)
            # THEN the result should be an Agent with the correct values
            assert result_agent == self.test_agent
            # AND register_agent should have been called once with the correct arguments
            mock_register_agent.assert_called_once_with(
                cloud_agent_id="123",
                cloud_alias_id="456",
                synapse_client=self.syn,
            )
            # AND fill_from_dict should have been called once with the correct arguments
            mock_fill_from_dict.assert_called_once_with(
                agent_registration=self.agent_response
            )

    def test_get(self) -> None:
        with (
            patch(
                "synapseclient.models.agent.get_agent",
                return_value=self.agent_response,
            ) as mock_get_agent,
            patch.object(
                Agent,
                "fill_from_dict",
                return_value=self.test_agent,
            ) as mock_fill_from_dict,
        ):
            # GIVEN an Agent with a registration_id
            initial_agent = Agent(
                registration_id=0,
            )
            # WHEN I call get
            result_agent = initial_agent.get(synapse_client=self.syn)
            # THEN the result should be an Agent with the correct values
            assert result_agent == self.test_agent
            # AND get_agent should have been called once with the correct arguments
            mock_get_agent.assert_called_once_with(
                registration_id=0,
                synapse_client=self.syn,
            )
            # AND fill_from_dict should have been called once with the correct arguments
            mock_fill_from_dict.assert_called_once_with(
                agent_registration=self.agent_response
            )

    def test_start_session(self) -> None:
        with patch.object(
            AgentSession,
            "start_async",
            return_value=self.test_session,
        ) as mock_start_session:
            # GIVEN an existing Agent
            my_agent = self.get_example_agent()
            # WHEN I call start_session
            result_session = my_agent.start_session(
                access_level=AgentSessionAccessLevel.PUBLICLY_ACCESSIBLE,
                synapse_client=self.syn,
            )
            # THEN the result should be an AgentSession with the correct values
            assert result_session == self.test_session
            # AND start_session should have been called once with the correct arguments
            mock_start_session.assert_called_once_with(
                synapse_client=self.syn,
            )
            # AND the current_session should be set to the new session
            assert my_agent.current_session == self.test_session
            # AND the sessions dictionary should have the new session
            assert my_agent.sessions[self.test_session.id] == self.test_session

    def test_get_session(self) -> None:
        with patch.object(
            AgentSession,
            "get_async",
            return_value=self.test_session,
        ) as mock_get_session:
            # GIVEN an existing AgentSession
            my_agent = self.get_example_agent()
            # WHEN I call get_session
            result_session = my_agent.get_session(
                session_id="123", synapse_client=self.syn
            )
            # THEN the result should be an AgentSession with the correct values
            assert result_session == self.test_session
            # AND get_session should have been called once with the correct arguments
            mock_get_session.assert_called_once_with(
                synapse_client=self.syn,
            )
            # AND the current_session should be set to the session
            assert my_agent.current_session == self.test_session
            # AND the sessions dictionary should have the session
            assert my_agent.sessions[self.test_session.id] == self.test_session

    def test_prompt_session_selected(self) -> None:
        with (
            patch.object(
                AgentSession,
                "get_async",
                return_value=self.test_session,
            ) as mock_get_async,
            patch.object(
                Agent,
                "start_session_async",
            ) as mock_start_session,
            patch.object(
                AgentSession,
                "prompt_async",
            ) as mock_prompt_async,
        ):
            # GIVEN an existing AgentSession
            my_agent = self.get_example_agent()
            # WHEN I call prompt with a session selected
            my_agent.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
                session=self.test_session,
                newer_than=0,
                synapse_client=self.syn,
            )
            # AND get_session_async should have been called once with the correct arguments
            mock_get_async.assert_called_once_with(
                synapse_client=self.syn,
            )
            # AND start_session_async should not have been called
            mock_start_session.assert_not_called()
            # AND prompt_async should have been called once with the correct arguments
            mock_prompt_async.assert_called_once_with(
                prompt="Hello",
                enable_trace=True,
                newer_than=0,
                print_response=True,
                timeout=120,
                synapse_client=self.syn,
            )

    def test_prompt_session_none_current_session_none(self) -> None:
        with (
            patch.object(
                Agent,
                "get_session_async",
            ) as mock_get_session,
            patch.object(
                AgentSession,
                "start_async",
                return_value=self.test_session,
            ) as mock_start_async,
            patch.object(
                AgentSession,
                "prompt_async",
            ) as mock_prompt_async,
        ):
            # GIVEN an existing Agent with no current session
            my_agent = self.get_example_agent()
            # WHEN I call prompt with no session selected and no current session set
            my_agent.prompt(
                prompt="Hello",
                enable_trace=True,
                print_response=True,
                newer_than=0,
                synapse_client=self.syn,
            )
            # THEN get_session_async should not have been called
            mock_get_session.assert_not_called()
            # AND start_session_async should have been called once with the correct arguments
            mock_start_async.assert_called_once_with(
                synapse_client=self.syn,
            )
            # AND prompt_async should have been called once with the correct arguments
            mock_prompt_async.assert_called_once_with(
                prompt="Hello",
                enable_trace=True,
                newer_than=0,
                print_response=True,
                timeout=120,
                synapse_client=self.syn,
            )

    def test_prompt_session_none_current_session_present(self) -> None:
        with (
            patch.object(
                Agent,
                "get_session_async",
            ) as mock_get_session,
            patch.object(
                AgentSession,
                "start_async",
            ) as mock_start_async,
            patch.object(
                AgentSession,
                "prompt_async",
            ) as mock_prompt_async,
        ):
            # GIVEN an existing Agent with a current session
            my_agent = self.get_example_agent()
            my_agent.current_session = self.test_session
            # WHEN I call prompt with no session selected and a current session set
            my_agent.prompt(
                prompt="Hello",
                enable_trace=True,
                newer_than=0,
                print_response=True,
                synapse_client=self.syn,
            )
            # THEN get_session_async and start_session_async should not have been called
            mock_get_session.assert_not_called()
            mock_start_async.assert_not_called()
            # AND prompt_async should have been called once with the correct arguments
            mock_prompt_async.assert_called_once_with(
                prompt="Hello",
                enable_trace=True,
                newer_than=0,
                print_response=True,
                timeout=120,
                synapse_client=self.syn,
            )

    def test_get_chat_history_when_current_session_none(self) -> None:
        # GIVEN an existing Agent with no current session
        my_agent = self.get_example_agent()
        # WHEN I call get_chat_history
        result_chat_history = my_agent.get_chat_history()
        # THEN the result should be None
        assert result_chat_history is None

    def test_get_chat_history_when_current_session_and_chat_history_present(
        self,
    ) -> None:
        # GIVEN an existing Agent with a current session and chat history
        my_agent = self.get_example_agent()
        my_agent.current_session = self.test_session
        my_agent.current_session.chat_history = [self.test_prompt]
        # WHEN I call get_chat_history
        result_chat_history = my_agent.get_chat_history()
        # THEN the result should be the chat history
        assert self.test_prompt in result_chat_history
