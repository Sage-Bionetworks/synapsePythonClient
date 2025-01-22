# Agent

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

::: synapseclient.models.Agent
    options:
        members:
            - register_async
            - get_async
            - start_session_async
            - get_session_async
            - prompt_async
            - get_chat_history
---
::: synapseclient.models.AgentSession
    options:
        members:
            - start_async
            - get_async
            - update_async
            - prompt_async
---
::: synapseclient.models.AgentPrompt
    options:
        inherited_members: true
        members:
            - send_job_and_wait_async
---
