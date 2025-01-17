"""
The purpose of this script is to demonstrate how to use the new OOP interface for Synapse AI Agents.

1. Register and send a prompt to a custom agent
2. Get a baseline agent instance and send a prompt to it
"""

import synapseclient
from synapseclient.models import Agent, AgentSession, AgentSessionAccessLevel

CLOUD_AGENT_ID = "QOTV3KQM1X"
AGENT_REGISTRATION_ID = 29

syn = synapseclient.Synapse(debug=True)
syn.login()

# Using the Agent class


# Register a custom agent and send a prompt to it
def register_and_send_prompt_to_custom_agent():
    my_custom_agent = Agent(cloud_agent_id=CLOUD_AGENT_ID)
    my_custom_agent.register(synapse_client=syn)
    my_custom_agent.prompt(
        prompt="Hello", enable_trace=True, print_response=True, synapse_client=syn
    )


# Create an Agent Object and prompt.
# By default, this will send a prompt to a new session with the baseline Synapse Agent.
def get_baseline_agent_and_send_prompt_to_it():
    baseline_agent = Agent()
    baseline_agent.prompt(
        prompt="What is Synapse?",
        enable_trace=True,
        print_response=True,
        synapse_client=syn,
    )


# Conduct more than one session with the same agent
def conduct_multiple_sessions_with_same_agent():
    my_agent = Agent(registration_id=AGENT_REGISTRATION_ID).get(synapse_client=syn)
    my_agent.prompt(
        prompt="Hello",
        enable_trace=True,
        print_response=True,
        synapse_client=syn,
    )
    my_second_session = my_agent.start_session(synapse_client=syn)
    my_agent.prompt(
        prompt="Hello again",
        enable_trace=True,
        print_response=True,
        session=my_second_session,
        synapse_client=syn,
    )


# Using the AgentSession class


# Start a new session with a custom agent and send a prompt to it
def start_new_session_with_custom_agent_and_send_prompt_to_it():
    my_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID).start(
        synapse_client=syn
    )
    my_session.prompt(
        prompt="Hello", enable_trace=True, print_response=True, synapse_client=syn
    )


# Start a new session with the baseline Synapse Agent and send a prompt to it
def start_new_session_with_baseline_agent_and_send_prompt_to_it():
    my_session = AgentSession().start(synapse_client=syn)
    my_session.prompt(
        prompt="What is Synapse?",
        enable_trace=True,
        print_response=True,
        synapse_client=syn,
    )


# Start a new session with a custom agent and then update what the agent has access to
def start_new_session_with_custom_agent_and_update_access_to_it():
    my_session = AgentSession(agent_registration_id=AGENT_REGISTRATION_ID).start(
        synapse_client=syn
    )
    print(f"Access level before update: {my_session.access_level}")
    my_session.access_level = AgentSessionAccessLevel.READ_YOUR_PRIVATE_DATA
    my_session.update(synapse_client=syn)
    print(f"Access level after update: {my_session.access_level}")


register_and_send_prompt_to_custom_agent()
get_baseline_agent_and_send_prompt_to_it()
conduct_multiple_sessions_with_same_agent()
start_new_session_with_baseline_agent_and_send_prompt_to_it()
start_new_session_with_custom_agent_and_update_access_to_it()
