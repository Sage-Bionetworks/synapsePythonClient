# Curator

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

[](){ #curation-task-reference }
::: synapseclient.models.CurationTask
    options:
        inherited_members: true
        members:
            - get
            - delete
            - store
            - list
---

[](){ #RecordSet-reference }
::: synapseclient.models.RecordSet
    options:
        inherited_members: true
        members:
            - get
            - store
            - delete
            - get_acl
            - get_permissions
            - set_permissions
            - delete_permissions
            - list_acl
            - bind_schema
            - get_schema
            - unbind_schema
            - validate_schema
            - get_schema_derived_keys
---
[](){ #RecordBasedMetadataTaskProperties-reference }
::: synapseclient.models.RecordBasedMetadataTaskProperties
    options:
        inherited_members: true
        members:
---
[](){ #FileBasedMetadataTaskProperties-reference }
::: synapseclient.models.FileBasedMetadataTaskProperties
    options:
        inherited_members: true
        members:
---
[](){ #grid-reference }
::: synapseclient.models.Grid
    options:
        inherited_members: true
        members:
            - create
            - export_to_record_set
---
[](){ #query-reference }
::: synapseclient.models.Query
    options:
        inherited_members: true
        members:
---
