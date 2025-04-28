[](){ #agent-reference-sync }
# Agent

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script:

<details class="quote">
  <summary>Working with Synapse agents</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_agent.py!}
```
</details>

## API Reference

::: synapseclient.models.Agent
    options:
        inherited_members: true
        members:
            - register
            - get
            - start_session
            - get_session
            - prompt
            - get_chat_history
---
[](){ #agent-session-reference-sync }
::: synapseclient.models.AgentSession
    options:
        inherited_members: true
        members:
            - start
            - get
            - update
            - prompt
---
[](){ #agent-prompt-reference-sync }
::: synapseclient.models.AgentPrompt
    options:
        inherited_members: true
