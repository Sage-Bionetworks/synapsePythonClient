import asyncio
from synapseclient import Synapse
import json
from synapseclient.api import (
    register_agent,
    get_agent,
    start_session,
    get_session,
    update_session,
    send_prompt,
    get_response,
    get_trace,
)


AWS_AGENT_ID = "APLZVUZ4HR"


async def main():
    syn = Synapse()
    syn.login()

    # Register the agent
    agent_registration_response = await register_agent(
        request={
            "awsAgentId": AWS_AGENT_ID,
        },
        synapse_client=syn,
    )
    print("AGENT REGISTERED:")
    print(agent_registration_response)
    print("--------------------------------")

    # Get the agent information
    agent_response = await get_agent(
        registration_id=agent_registration_response["agentRegistrationId"],
        synapse_client=syn,
    )
    print("AGENT INFORMATION:")
    print(agent_response)
    print("--------------------------------")

    # Start a chat session
    session_response = await start_session(
        request={
            "agentAccessLevel": "PUBLICLY_ACCESSIBLE",
            "agentRegistrationId": agent_response["agentRegistrationId"],
        },
        synapse_client=syn,
    )
    print("SESSION STARTED:")
    print(session_response)
    print("--------------------------------")

    # Get the session information
    session_response = await get_session(
        session_id=session_response["sessionId"],
        synapse_client=syn,
    )
    print("SESSION INFORMATION:")
    print(session_response)
    print("--------------------------------")

    # Update the session access level
    session_response = await update_session(
        request={
            "sessionId": session_response["sessionId"],
            "agentAccessLevel": "READ_YOUR_PRIVATE_DATA",
        },
        session_id=session_response["sessionId"],
        synapse_client=syn,
    )
    print("SESSION UPDATED:")
    print(session_response)
    print("--------------------------------")

    # Send a prompt to the agent
    prompt_response = await send_prompt(
        request={
            "concreteType": "org.sagebionetworks.repo.model.agent.AgentChatRequest",
            "sessionId": session_response["sessionId"],
            "chatText": "What is your purpose?",
            "enableTrace": True,
        },
        synapse_client=syn,
    )
    print("PROMPT SENT:")
    print(prompt_response)
    print("--------------------------------")

    import time

    # Wait for the agent to respond
    time.sleep(20)

    # Get the response from the agent
    response_response = await get_response(
        prompt_token=prompt_response["token"],
        synapse_client=syn,
    )
    print("RESPONSE:")
    print(response_response)
    print("--------------------------------")

    # Get the trace of the prompt
    trace_response = await get_trace(
        request={
            "jobId": prompt_response["token"],
            "newerThanTimestamp": 0,
        },
        prompt_token=prompt_response["token"],
        synapse_client=syn,
    )
    print("TRACE:")
    print(trace_response)
    print("--------------------------------")


if __name__ == "__main__":
    asyncio.run(main())
