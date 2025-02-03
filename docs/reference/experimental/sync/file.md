# File

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with files</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_file.py!}
```
</details>

## API Reference

::: synapseclient.models.File
    options:
        inherited_members: true
        members:
        - get
        - store
        - copy
        - delete
        - from_id
        - from_path
        - change_metadata
        - get_permissions
        - get_acl
        - set_permissions
---
::: synapseclient.models.file.FileHandle
    options:
      filters:
      - "!"
