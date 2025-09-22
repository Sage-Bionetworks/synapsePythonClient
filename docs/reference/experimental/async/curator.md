# Agent

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

[](){ #curation-task-reference-async }
::: synapseclient.models.CurationTask
    options:
        members:
            - get_async
            - delete_async
            - store_async
            - list_async
---

[](){ #RecordSet-reference-async }
::: synapseclient.models.RecordSet
    options:
        members:
            - get_async
            - store_async
            - delete_async
            - change_metadata_async
---
[](){ #RecordBasedMetadataTaskProperties-reference-async }
::: synapseclient.models.RecordBasedMetadataTaskProperties
    options:
        members:
---
[](){ #FileBasedMetadataTaskProperties-reference-async }
::: synapseclient.models.FileBasedMetadataTaskProperties
    options:
        members:
---
[](){ #grid-reference-async }
::: synapseclient.models.Grid
    options:
        members:
            - create_async
            - export_to_record_set_async
---
[](){ #query-reference-async }
::: synapseclient.models.Query
    options:
        members:
---
