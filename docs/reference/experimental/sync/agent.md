[](){ #agent-reference-sync }
# Agent

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
