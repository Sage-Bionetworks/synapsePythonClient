Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

These APIs also introduce [AsyncIO](https://docs.python.org/3/library/asyncio.html) to
the client.

## Sample Scripts:
See [this page for sample scripts](models.md#sample-scripts).
The sample scripts are from a synchronous context,
replace any of the method calls with the async counter-party and they will be
functionally equivalent.

## API reference

::: synapseclient.models.Project
    options:
        inherited_members: true
        members:
        - get_async
        - store_async
        - delete_async
        - sync_from_synapse_async
        - get_permissions_async
        - get_acl_async
        - set_permissions_async
---
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
---
::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get_async
        - store_schema_async
        - store_rows_from_csv_async
        - delete_rows_async
        - query_async
        - delete_async
        - get_permissions_async
        - get_acl_async
        - set_permissions_async
---
::: synapseclient.models.Activity
    options:
      members:
      - from_parent_async
      - store_async
      - delete_async

---
::: synapseclient.models.Team
    options:
        members:
        - create_async
        - delete_async
        - from_id_async
        - from_name_async
        - members_async
        - invite_async
        - open_invitations_async
---
::: synapseclient.models.UserProfile
    options:
      members:
      - get_async
      - from_id_async
      - from_username_async
      - is_certified_async
