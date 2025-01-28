# Table

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with tables</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_table.py!}
```
</details>

## API Reference

::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get
        - store_schema
        - store_rows_from_csv
        - delete_rows
        - query
        - delete
        - get_permissions
        - get_acl
        - set_permissions
