# Wiki

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

::: synapseclient.models.WikiOrderHint
    options:
        inherited_members: true
        members:
        - store_async
        - get_async
::: synapseclient.models.WikiHistorySnapshot
    options:
        inherited_members: true
        members:
        - get_async
::: synapseclient.models.WikiHeader
    options:
        inherited_members: true
        members:
        - get_async
::: synapseclient.models.WikiPage
    options:
        inherited_members: true
        members:
        - store_async
        - restore_async
        - get_async
        - delete_async
        - get_attachment_handles_async
        - get_attachment_async
        - get_attachment_preview_async
        - get_markdown_file_async
