# How to Set Up Metadata Curation Workflows

This guide is for **curation administrators** — the person responsible for designing a curation workflow: choosing a JSON schema, deciding whether metadata is record-based or file-based, creating the `CurationTask`, and reviewing the validation results contributors submit.

If you're a data contributor opening a task an administrator has already created, see [How to Enter and Update Metadata for a Curation Task](metadata_contribution.md) instead.

## What you'll accomplish

By following this guide, you will:

- Find and select the right JSON schema for your data type
- Create a record-based or file-based metadata curation workflow
- Configure curation tasks that guide contributors through metadata entry

## Prerequisites

- A Synapse account with project creation permissions
- Python environment with synapseclient and the `curator` extension installed (`pip install --upgrade "synapseclient[curator]"`)
- An existing Synapse project and folder where you want to manage metadata
- A JSON Schema registered in Synapse (many schemas are already available for Sage-affiliated projects, or you can register your own by following the [JSON Schema tutorial](../../../tutorials/python/json_schema.md))
  - If you are using the [Curator CSV data model](../../../explanations/curator_data_model.md), you can create JSON schemas by following this [guide](schema_operations.md)
- (Optional) An existing Synapse team if you want multiple users to collaborate on the same Grid session. Pass the team's ID as `assignee_principal_id` when creating the curation task.

## Step 1: Authenticate and import required functions

```python
from synapseclient.extensions.curator import (
    create_record_based_metadata_task,
    create_file_based_metadata_task,
    query_schema_registry
)
from synapseclient import Synapse
from synapseclient.models import Grid
from synapseclient.models.table_components import Query

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
    dcc="ad",  # Your data coordination center, check out the `syn69735275` table if you do not know your code
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

!!! note
    The way Grid sessions are created in this step will change in the near future. Expect updates to the Grid creation API and to this guide.
    Currently Data Contributers should create their own Grids due to how permissions work.
    This will be fixed in the near future.

### Option A: Record-based metadata

Use this when metadata is normalized in structured records to eliminate duplication and ensure consistency.

```python
record_set, curation_task, grid = create_record_based_metadata_task(
    synapse_client=syn,
    folder_id="syn987654321",          # Folder where RecordSet Entity will be stored
    record_set_name="AnimalMetadata_Records",
    record_set_description="Centralized metadata for animal study data",
    curation_task_name="AnimalMetadata_Curation", # Must be unique within the project
    upsert_keys=["StudyKey"],          # Fields that uniquely identify records
    instructions="Complete all required fields according to the schema. Use StudyKey to link records to your data files.",
    schema_uri=schema_uri,             # Schema found in Step 2
    bind_schema_to_record_set=True,
    assignee_principal_id=123456     # Optional: Assign to a user or team
)

print(f"Created RecordSet: {record_set.id}")
print(f"Created CurationTask: {curation_task.task_id}")
```

**What this creates:**

- A RecordSet where metadata is stored as structured records (like a spreadsheet)
- A CurationTask that guides users through completing the metadata
- Automatic schema binding for validation

### Option B: File-based metadata (for unique per-file metadata)

Use this when metadata describes individual data files and is stored as annotations directly on each file.

```python
entity_view_id, task_id = create_file_based_metadata_task(
    synapse_client=syn,
    folder_id="syn987654321",          # Folder containing your data files
    curation_task_name="FileMetadata_Curation", # Must be unique within the project
    instructions="Annotate each file with metadata according to the schema requirements.",
    attach_wiki=False,                 # Creates a wiki in the folder with the entity view (Defaults to False)
    entity_view_name="Animal Study Files View",
    schema_uri=schema_uri,             # Schema found in Step 2
    assignee_principal_id=123456     # Optional: Assign to a user or team
)

print(f"Created EntityView: {entity_view_id}")
print(f"Created CurationTask: {task_id}")

```

**What this creates:**

- An EntityView that displays all files in the folder
- A CurationTask for guided metadata entry
- Automatic schema binding to the folder for validation
- Optional wiki attached to the folder
- A Grid session for interactive metadata editing

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
record_set, curation_task, grid = create_record_based_metadata_task(
    synapse_client=syn,
    folder_id="syn987654321",
    record_set_name="AnimalMetadata_Records",
    record_set_description="Centralized animal study metadata",
    curation_task_name="AnimalMetadata_Curation",
    upsert_keys=["StudyKey"],
    instructions="Complete metadata for all study animals using StudyKey to link records to data files.",
    schema_uri=schema_uri,
    bind_schema_to_record_set=True,
    assignee_principal_id=123456  # Optional: Assign to a user or team
)

print("Record-based workflow created:")
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
    schema_uri=schema_uri,
    assignee_principal_id=123456  # Optional: Assign to a user or team
)

print("File-based workflow created:")
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
- [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results] - Get detailed validation results for RecordSet data
- [Grid.create][synapseclient.models.curation.Grid.create] - Create a Grid session from a RecordSet or EntityView
- [Grid.export_to_record_set][synapseclient.models.curation.Grid.export_to_record_set] - Export Grid data back to RecordSet and generate validation results
- [Folder.bind_schema][synapseclient.models.Folder.bind_schema] - Bind schemas to folders
- [Folder.validate_schema][synapseclient.models.Folder.validate_schema] - Validate folder schema compliance
- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project

### Related Documentation

- [How to Enter and Update Metadata for a Curation Task](metadata_contribution.md) - The contributor-facing companion to this guide
- [JSON Schema Tutorial](../../../tutorials/python/json_schema.md) - Learn how to register schemas
- [Schema Registry](https://synapse.org/Synapse:syn69735275/tables/) - Browse available schemas
