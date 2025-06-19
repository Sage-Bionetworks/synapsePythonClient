# File

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API Reference

[](){ #file-reference-async }
::: synapseclient.models.File
    options:
        inherited_members: true
        members:
        - get_async
        - store_async
        - copy_async
        - delete_async
        - from_id_async
        - from_path_async
        - change_metadata_async
        - get_permissions_async
        - get_acl_async
        - set_permissions_async
        - delete_permissions_async
        - bind_schema_async
        - get_schema_async
        - unbind_schema_async
        - validate_schema_async
        - get_schema_derived_keys_async
---
[](){ #filehandle-reference-async }
::: synapseclient.models.file.FileHandle
    options:
      filters:
      - "!"
