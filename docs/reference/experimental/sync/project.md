[](){ #project-reference-sync }
# Project

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with a project</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_project.py!}
```
</details>

## API reference

::: synapseclient.models.Project
    options:
        inherited_members: true
        members:
        - get
        - store
        - delete
        - sync_from_synapse
        - get_permissions
        - get_acl
        - set_permissions
        - delete_permissions
        - list_acl
