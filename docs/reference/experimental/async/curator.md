# Curator

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

[](){ #curation-task-reference-async }
::: synapseclient.models.CurationTask
    options:
        inherited_members: true
        members:
            - get_async
            - delete_async
            - store_async
            - list_async
---

[](){ #RecordSet-reference-async }
::: synapseclient.models.RecordSet
    options:
        inherited_members: true
        members:
            - get_async
            - store_async
            - delete_async
            - change_metadata_async
---
[](){ #RecordBasedMetadataTaskProperties-reference-async }
::: synapseclient.models.RecordBasedMetadataTaskProperties
    options:
        inherited_members: true
        members:
---
[](){ #FileBasedMetadataTaskProperties-reference-async }
::: synapseclient.models.FileBasedMetadataTaskProperties
    options:
        inherited_members: true
        members:
---
[](){ #grid-reference-async }
::: synapseclient.models.Grid
    options:
        inherited_members: true
        members:
            - create_async
            - export_to_record_set_async
---
[](){ #query-reference-async }
::: synapseclient.models.Query
    options:
        inherited_members: true
        members:
---
