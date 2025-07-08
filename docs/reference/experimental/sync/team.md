[](){ #team-reference-sync }
# Team

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with teams</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_team.py!}
```
</details>

## API Reference

::: synapseclient.models.Team
    options:
        inherited_members: true
        members:
            - create
            - delete
            - from_id
            - from_name
            - members
            - invite
            - open_invitations
---

::: synapseclient.models.TeamMember
