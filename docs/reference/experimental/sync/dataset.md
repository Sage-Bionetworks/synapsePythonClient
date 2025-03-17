# Dataset

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script:

<details class="quote">
  <summary>Working with Synapse datasets</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_dataset.py!}
```
</details>

## API reference

::: synapseclient.models.EntityRef
---
::: synapseclient.models.Dataset
    options:
        inherited_members: true
        members:
            - add_item
            - remove_item
            - store
            - get
            - delete
            - update_rows
            - snapshot
            - query
            - query_part_mask
            - add_column
            - delete_column
            - reorder_column
            - rename_column
            - get_permissions
            - get_acl
            - set_permissions
---
