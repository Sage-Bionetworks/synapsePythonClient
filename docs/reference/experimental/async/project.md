# Project

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API reference

::: synapseclient.models.Project
    options:
        inherited_members: true
        members:
        - get_async
        - store_async
        - delete_async
        - walk_async
        - sync_from_synapse_async
        - sync_to_synapse_async
        - flatten_file_list
        - map_directory_to_all_contained_files
        - get_permissions_async
        - get_acl_async
        - set_permissions_async
        - delete_permissions_async
        - list_acl_async
        - bind_schema_async
        - get_schema_async
        - unbind_schema_async
        - validate_schema_async
        - get_schema_derived_keys_async
        - get_schema_validation_statistics_async
        - get_invalid_validation_async
        - set_storage_location_async
        - get_project_setting_async
        - delete_project_setting_async
        - get_sts_storage_token_async
        - index_files_for_migration_async
        - migrate_indexed_files_async
