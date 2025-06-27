[](){ #folder-reference-sync }
# Folder

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with folders</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_folder.py!}
```
</details>

## API Reference

::: synapseclient.models.Folder
    options:
        inherited_members: true
        members:
        - get
        - store
        - delete
        - copy
        - sync_from_synapse
        - get_permissions
        - get_acl
        - set_permissions
        - delete_permissions
        - bind_schema
        - get_schema
        - unbind_schema
        - validate_schema
        - get_schema_derived_keys
        - get_schema_validation_statistics
        - get_invalid_validation
