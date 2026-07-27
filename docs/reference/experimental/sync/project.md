[](){ #project-reference-sync }
# Project

## Example Script

<details class="quote">
  <summary>Working with a project</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_project.py!}
```
</details>

## API reference

::: synapseclient.models.Project
    options:
        inherited_members: true
        members:
        - get
        - store
        - delete
        - walk
        - sync_from_synapse
        - sync_to_synapse
        - generate_sync_manifest
        - flatten_file_list
        - map_directory_to_all_contained_files
        - get_permissions
        - get_acl
        - list_acl
        - set_permissions
        - delete_permissions
        - bind_schema
        - get_schema
        - unbind_schema
        - validate_schema
        - get_schema_derived_keys
        - get_schema_validation_statistics
        - get_invalid_validation
        - set_storage_location
        - get_project_setting
        - delete_project_setting
        - get_sts_storage_token
        - index_files_for_migration
        - migrate_indexed_files
