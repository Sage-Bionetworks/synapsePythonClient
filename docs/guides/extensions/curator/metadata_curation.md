# How to Create Metadata Curation Workflows

This guide shows you how to set up a metadata curation workflows in Synapse using the curator extension. You'll learn to find appropriate schemas, create curation tasks for your research data.

## What you'll accomplish

By following this guide, you will:

- Find and select the right JSON schema for your data type
- Create a metadata curation workflow with automatic validation
- Set up either file-based or record-based metadata collection
- Configure curation tasks that guide collaborators through metadata entry

## Prerequisites

- A Synapse account with project creation permissions
- Python environment with synapseclient and the `curator` extension installed (ie. `pip install --upgrade "synapseclient[curator]"`)
- An existing Synapse project and folder where you want to manage metadata
- A JSON Schema registered in Synapse (many schemas are already available for Sage-affiliated projects, or you can register your own by following the [JSON Schema tutorial](../../../tutorials/python/json_schema.md))

## Step 1: Authenticate and import required functions

```python
from synapseclient.extensions.curator import (
    create_record_based_metadata_task,
    create_file_based_metadata_task,
    query_schema_registry
)
from synapseclient import Synapse

syn = Synapse()
syn.login()
```

## Step 2: Find the right schema for your data

Before creating a curation task, identify which JSON schema matches your data type. Many schemas are already registered in Synapse for Sage-affiliated projects. The schema registry contains validated schemas organized by data coordination center (DCC) and data type.

**If you need to register your own schema**, follow the [JSON Schema tutorial](../../../tutorials/python/json_schema.md) to understand the registration process.

```python
# Find the latest schema for your specific data type
schema_uri = query_schema_registry(
    synapse_client=syn,
    dcc="ad",  # Your data coordination center
    datatype="IndividualAnimalMetadataTemplate"  # Your specific data type
)

print("Latest schema URI:", schema_uri)
```

**When to use this approach:** You know your DCC and data type, you want the most current schema version, and it has already been registered into <https://www.synapse.org/Synapse:syn69735275/tables/>.

**Alternative - browse available schemas:**
```python
# Get all versions to see what's available
all_schemas = query_schema_registry(
    synapse_client=syn,
    dcc="ad",
    datatype="IndividualAnimalMetadataTemplate",
    return_latest_only=False
)
```

## Step 3: Choose your metadata workflow type

### Option A: Record-based metadata

Use this when metadata describes individual data files and is stored as annotations directly on each file.

```python
record_set, curation_task, data_grid = create_record_based_metadata_task(
    synapse_client=syn,
    project_id="syn123456789",         # Your project ID
    folder_id="syn987654321",          # Folder where files are stored
    record_set_name="AnimalMetadata_Records",
    record_set_description="Centralized metadata for animal study data",
    curation_task_name="AnimalMetadata_Curation", # Must be unique within the project
    upsert_keys=["StudyKey"],          # Fields that uniquely identify records
    instructions="Complete all required fields according to the schema. Use StudyKey to link records to your data files.",
    schema_uri=schema_uri,             # Schema found in Step 2
    bind_schema_to_record_set=True
)

print(f"Created RecordSet: {record_set.id}")
print(f"Created CurationTask: {curation_task.task_id}")
```

**What this creates:**

- A RecordSet where metadata is stored as structured records (like a spreadsheet)
- A CurationTask that guides users through completing the metadata
- Automatic schema binding for validation
- A data grid interface for easy metadata entry

### Option B: File-based metadata (for unique per-file metadata)

Use this when metadata is normalized in structured records to eliminate duplication and ensure consistency.

```python
entity_view_id, task_id = create_file_based_metadata_task(
    synapse_client=syn,
    folder_id="syn987654321",          # Folder containing your data files
    curation_task_name="FileMetadata_Curation", # Must be unique within the project
    instructions="Annotate each file with metadata according to the schema requirements.",
    attach_wiki=True,                  # Creates a wiki in the folder with the entity view
    entity_view_name="Animal Study Files View",
    schema_uri=schema_uri              # Schema found in Step 2
)

print(f"Created EntityView: {entity_view_id}")
print(f"Created CurationTask: {task_id}")
```

**What this creates:**

- An EntityView that displays all files in the folder
- A CurationTask for guided metadata entry
- Automatic schema binding to the folder for validation
- Optional wiki attached to the folder

## Complete example script

Here's the full script that demonstrates both workflow types:

```python
from pprint import pprint
from synapseclient.extensions.curator import (
    create_record_based_metadata_task,
    create_file_based_metadata_task,
    query_schema_registry
)
from synapseclient import Synapse

# Step 1: Authenticate
syn = Synapse()
syn.login()

# Step 2: Find schema
schema_uri = query_schema_registry(
    synapse_client=syn,
    dcc="ad",
    datatype="IndividualAnimalMetadataTemplate"
)
print("Using schema:", schema_uri)

# Step 3A: Create record-based workflow
record_set, curation_task, data_grid = create_record_based_metadata_task(
    synapse_client=syn,
    project_id="syn123456789",
    folder_id="syn987654321",
    record_set_name="AnimalMetadata_Records",
    record_set_description="Centralized animal study metadata",
    curation_task_name="AnimalMetadata_Curation",
    upsert_keys=["StudyKey"],
    instructions="Complete metadata for all study animals using StudyKey to link records to data files.",
    schema_uri=schema_uri,
    bind_schema_to_record_set=True
)

print(f"Record-based workflow created:")
print(f"  RecordSet: {record_set.id}")
print(f"  CurationTask: {curation_task.task_id}")

# Step 3B: Create file-based workflow
entity_view_id, task_id = create_file_based_metadata_task(
    synapse_client=syn,
    folder_id="syn987654321",
    curation_task_name="FileMetadata_Curation",
    instructions="Annotate each file with complete metadata according to schema.",
    attach_wiki=True,
    entity_view_name="Animal Study Files View",
    schema_uri=schema_uri
)

print(f"File-based workflow created:")
print(f"  EntityView: {entity_view_id}")
print(f"  CurationTask: {task_id}")
```

## Additional utilities

### Validate schema binding on folders

Use this script to verify the schema on a folder against the items contained within that folder:

```python
from synapseclient import Synapse
from synapseclient.models import Folder

# The Synapse ID of the entity you want to bind the JSON Schema to. This should be the ID of a Folder where you want to enforce the schema.
FOLDER_ID = ""

syn = Synapse()
syn.login()

folder = Folder(id=FOLDER_ID).get()
schema_validation = folder.validate_schema()

print(f"Schema validation result for folder {FOLDER_ID}: {schema_validation}")
```

### List existing curation tasks

Use this script to see all curation tasks in a project:

```python
from pprint import pprint
from synapseclient import Synapse
from synapseclient.models.curation import CurationTask

PROJECT_ID = ""  # The Synapse ID of the project to list tasks from

syn = Synapse()
syn.login()

for curation_task in CurationTask.list(
    project_id=PROJECT_ID
):
    pprint(curation_task)
```

## References

### API Documentation

- [query_schema_registry][synapseclient.extensions.curator.query_schema_registry] - Search for schemas in the registry
- [create_record_based_metadata_task][synapseclient.extensions.curator.create_record_based_metadata_task] - Create RecordSet-based curation workflows
- [create_file_based_metadata_task][synapseclient.extensions.curator.create_file_based_metadata_task] - Create EntityView-based curation workflows
- [Folder.bind_schema][synapseclient.models.Folder.bind_schema] - Bind schemas to folders
- [Folder.validate_schema][synapseclient.models.Folder.validate_schema] - Validate folder schema compliance
- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project

### Related Documentation

- [JSON Schema Tutorial](../../../tutorials/python/json_schema.md) - Learn how to register schemas
- [Schema Registry](https://synapse.org/Synapse:syn69735275/tables/) - Browse available schemas
