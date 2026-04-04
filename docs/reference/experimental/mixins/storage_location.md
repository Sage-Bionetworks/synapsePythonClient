# storage_location_mixin

The `storage_location_mixin` module provides two mixins for managing storage locations
on entities (Projects and Folders):

- **`StorageLocationConfigurable`** — base mixin providing STS credentials and file migration.
- **`ProjectSettingsMixin`** — extends `StorageLocationConfigurable` with project-level storage settings.

For architecture diagrams and design documentation, see
[Storage Location Architecture](../../../explanations/storage_location_architecture.md).

## Methods Overview

### `StorageLocationConfigurable`

| Method | Description |
|--------|-------------|
| `get_sts_storage_token` | Get STS credentials for direct S3 access |
| `index_files_for_migration` | Index files for migration to a new storage location |
| `migrate_indexed_files` | Migrate previously indexed files |

### `ProjectSettingsMixin` (extends `StorageLocationConfigurable`)

| Method | Description |
|--------|-------------|
| `set_storage_location` | Set the upload storage location for this entity (destructive replace) |
| `get_project_setting` | Get project settings (upload, external_sync, etc.) |
| `delete_project_setting` | Delete a project setting |

All methods have async equivalents with an `_async` suffix (e.g. `get_sts_storage_token_async`).

## Migration Workflow

Migration is a two-step process:

1. **Index** — call `index_files_for_migration` to scan the entity and record files to migrate in a local SQLite database.
2. **Migrate** — call `migrate_indexed_files` with the database path to perform the actual copy.

### `index_files_for_migration` parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dest_storage_location_id` | required | Destination storage location ID |
| `db_path` | `None` | Path for the SQLite tracking database; a temp path is used if omitted |
| `source_storage_location_ids` | `None` | Restrict to files in these source locations; `None` means all locations |
| `file_version_strategy` | `"new"` | `"new"` / `"all"` / `"latest"` / `"skip"` |
| `include_table_files` | `False` | Whether to include files attached to tables |
| `continue_on_error` | `False` | Record errors and continue rather than raising |

Returns a `MigrationResult` — access `result.db_path` to pass to the next step.

### `migrate_indexed_files` parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_path` | required | Path returned by `index_files_for_migration` |
| `create_table_snapshots` | `True` | Create table snapshots before migrating table files |
| `continue_on_error` | `False` | Record errors and continue rather than raising |
| `force` | `False` | Skip the interactive confirmation prompt (required for non-interactive/CI use) |

Returns a `MigrationResult`, or `None` if migration was aborted (user declined the prompt,
or the session is non-interactive and `force=False`).

## Usage Examples


### Set a storage location

```python
from synapseclient.models import Folder

# Replace all storage locations on a folder
folder = Folder(id="syn123").get()
folder.set_storage_location(storage_location_id=12345)

# Add a storage location without removing existing ones
setting = folder.get_project_setting(setting_type="upload")
if setting:
    setting.locations.append(67890)
    setting.store()
```

### Get STS credentials
Note: Entity must have an STS-enabled storage location
```python
credentials = folder.get_sts_storage_token(
    permission="read_write",
    output_format="boto",
)
```
### Migrate files to a new storage location

```python
import asyncio
from synapseclient import Synapse
from synapseclient.models import Project

syn = Synapse()
syn.login()

async def main():
    project = await Project(id="syn123").get_async()

    # Step 1: index
    index_result = await project.index_files_for_migration_async(
        dest_storage_location_id=12345,
    )
    print(f"Database path: {index_result.db_path}")

    # Step 2: migrate (force=True for non-interactive scripts)
    result = await project.migrate_indexed_files_async(
        db_path=index_result.db_path,
        force=True,
    )
    print(result.counts_by_status)

asyncio.run(main())
```
::: synapseclient.models.mixins.StorageLocationConfigurable

---

::: synapseclient.models.mixins.ProjectSettingsMixin

---

::: synapseclient.models.protocols.storage_location_mixin_protocol.StorageLocationConfigurableSynchronousProtocol
