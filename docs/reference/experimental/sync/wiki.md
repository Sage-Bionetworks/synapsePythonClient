[](){ #wiki-reference-sync }
# Wiki
Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

::: synapseclient.models.WikiOrderHint
    options:
        inherited_members: true
        members:
        - store
        - get
::: synapseclient.models.WikiHistorySnapshot
    options:
        inherited_members: true
        members:
        - get
::: synapseclient.models.WikiHeader
    options:
        inherited_members: true
        members:
        - get
::: synapseclient.models.WikiPage
    options:
        inherited_members: true
        members:
        - store
        - restore
        - get
        - delete
        - get_attachment_handles
        - get_attachment
        - get_attachment_preview
        - get_markdown_file
