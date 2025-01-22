# Activity

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with activities</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_activity.py!}
```
</details>

## API Reference

::: synapseclient.models.Activity
    options:
      inherited_members: true
      members:
        - from_parent
        - store
        - delete
---
::: synapseclient.models.UsedEntity
    options:
      filters:
      - "!"
---
::: synapseclient.models.UsedURL
    options:
      filters:
      - "!"
