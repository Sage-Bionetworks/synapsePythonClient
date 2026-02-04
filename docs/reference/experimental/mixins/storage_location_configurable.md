# StorageLocationConfigurable

The `StorageLocationConfigurable` mixin provides methods for managing storage locations
on entities (Projects and Folders).

For architecture diagrams and design documentation, see
[Storage Location Architecture](../../../explanations/storage_location_architecture.md).

This mixin includes:

- Setting upload storage locations
- Getting and deleting project settings
- Obtaining STS credentials for direct S3 access
- Migrating files to new storage locations

## Methods Overview

| Method | Description |
|--------|-------------|
| `set_storage_location` | Set the upload storage location for this entity |
| `get_project_setting` | Get project settings (upload, external_sync, etc.) |
| `delete_project_setting` | Delete a project setting |
| `get_sts_storage_token` | Get STS credentials for direct S3 access |
| `index_files_for_migration` | Index files for migration to a new storage location |
| `migrate_indexed_files` | Migrate previously indexed files |

## Usage Example

```python
from synapseclient.models import Folder, StorageLocation, StorageLocationType

# Create a storage location
storage = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket="my-bucket",
    sts_enabled=True,
).store()

# Set storage location on a folder
folder = Folder(id="syn123").get()
folder.set_storage_location(storage_location_id=storage.storage_location_id)

# Get STS credentials
credentials = folder.get_sts_storage_token(
    permission="read_write",
    output_format="boto",
)
```

::: synapseclient.models.mixins.StorageLocationConfigurable

---

::: synapseclient.models.protocols.storage_location_mixin_protocol.StorageLocationConfigurableSynchronousProtocol
