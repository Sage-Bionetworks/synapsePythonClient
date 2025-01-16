"""
The purpose of this script is to demonstrate how to use the new OOP interface for Synapse AI Agents.

1. Register and send a prompt to a custom agent
2. Get a baseline agent instance and send a prompt to it
"""

from synapseclient import Synapse
from synapseclient.models import Agent

CLOUD_AGENT_ID = "my_agent_id"
AGENT_REGISTRATION_ID = 123

syn = Synapse()
syn.login()


def register_and_send_prompt_to_custom_agent():
    my_custom_agent = Agent(cloud_agent_id=CLOUD_AGENT_ID)
    my_custom_agent.register(synapse_client=syn)
    my_custom_agent.prompt(prompt="Hello, how are you?")


def get_baseline_agent_and_send_prompt_to_it():
    baseline_agent = Agent().get(synapse_client=syn)
    baseline_agent.prompt(prompt="Hello, how are you?")


register_and_send_prompt_to_custom_agent()
get_baseline_agent_and_send_prompt_to_it()
