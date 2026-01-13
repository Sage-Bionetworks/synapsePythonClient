# How to Create Metadata Curation Workflows

This guide shows you how to set up a metadata curation workflows in Synapse using the curator extension. You'll learn to find appropriate schemas, create curation tasks for your research data.

## What you'll accomplish

By following this guide, you will:

- Find and select the right JSON schema for your data type
- Create a metadata curation workflow with automatic validation
- Set up either file-based or record-based metadata collection
- Configure curation tasks that guide collaborators through metadata entry
- Retrieve and analyze detailed validation results to identify data quality issues

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
    attach_wiki=True,                  # Creates a wiki in the folder with the entity view (Defaults to False)
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

## Step 4: Work with metadata and validate (Record-based workflow)

After creating a record-based metadata task, collaborators can enter metadata through the Grid interface. Once metadata entry is complete, you'll want to validate the data against your schema and identify any issues.

### The metadata curation workflow

1. **Data Entry**: Collaborators use the Grid interface (via the curation task link in the Synapse web UI) to enter metadata
2. **Grid Export**: Export the Grid session back to the RecordSet to save changes (this can be done via the web UI or programmatically)
3. **Validation**: Retrieve detailed validation results to identify schema violations
4. **Correction**: Fix any validation errors and repeat as needed

### Creating and exporting a Grid session

Validation results are only generated when a Grid session is exported back to the RecordSet. This triggers Synapse to validate each row against the bound schema. You have two options:

**Option A: Via the Synapse web UI (most common)**

Users can access the curation task through the Synapse web interface, enter/edit data in the Grid, and click the export button. This automatically generates validation results.

**Option B: Programmatically create and export a Grid session**

```python
from synapseclient import Synapse
from synapseclient.models import RecordSet
from synapseclient.models.curation import Grid

syn = Synapse()
syn.login()

# Get your RecordSet (must have a schema bound)
record_set = RecordSet(id="syn987654321").get()

# Create a Grid session from the RecordSet
grid = Grid(record_set_id=record_set.id).create()

# At this point, users can interact with the Grid (either programmatically or via web UI)
# When ready to save changes and generate validation results, export back to RecordSet
grid.export_to_record_set()

# Clean up the Grid session
grid.delete()

# Re-fetch the RecordSet to get the updated validation_file_handle_id
record_set = RecordSet(id=record_set.id).get()
```

**Important**: The `validation_file_handle_id` attribute is only populated after a Grid export operation. Until then, `get_detailed_validation_results()` will return `None`.

### Getting detailed validation results

After exporting from a Grid session with a bound schema, Synapse automatically validates each row against the schema and generates a detailed validation report. Here's how to retrieve and analyze those results:

```python
from synapseclient import Synapse
from synapseclient.models import RecordSet

syn = Synapse()
syn.login()

# After Grid export (either via web UI or programmatically)
# retrieve the updated RecordSet
record_set = RecordSet(id="syn987654321").get()

# Get detailed validation results as a pandas DataFrame
validation_results = record_set.get_detailed_validation_results()

if validation_results is not None:
    print(f"Total rows validated: {len(validation_results)}")

    # Filter for valid and invalid rows
    valid_rows = validation_results[validation_results['is_valid'] == True]
    invalid_rows = validation_results[validation_results['is_valid'] == False]

    print(f"Valid rows: {len(valid_rows)}")
    print(f"Invalid rows: {len(invalid_rows)}")

    # Display details of any validation errors
    if len(invalid_rows) > 0:
        print("\nRows with validation errors:")
        for idx, row in invalid_rows.iterrows():
            print(f"\nRow {row['row_index']}:")
            print(f"  Error: {row['validation_error_message']}")
            print(f"  ValidationError: {row['all_validation_messages']}")
else:
    print("No validation results available. The Grid session must be exported to generate validation results.")
```

### Example: Complete validation workflow for animal study metadata

This example demonstrates the full workflow from creating a curation task through validating the submitted metadata:

```python
from synapseclient import Synapse
from synapseclient.extensions.curator import create_record_based_metadata_task, query_schema_registry
from synapseclient.models import RecordSet
from synapseclient.models.curation import Grid
import pandas as pd
import tempfile
import os
import time

syn = Synapse()
syn.login()

# Step 1: Find the schema
schema_uri = query_schema_registry(
    synapse_client=syn,
    dcc="ad",
    datatype="IndividualAnimalMetadataTemplate"
)

# Step 1.5: Create initial test data with validation examples
# Row 1: VALID - all required fields present and valid
# Row 2: INVALID - missing required field 'genotype'
# Row 3: INVALID - invalid enum value for 'sex' ("other" not in enum)
test_data = pd.DataFrame({
    "individualID": ["ANIMAL001", "ANIMAL002", "ANIMAL003"],
    "species": ["Mouse", "Mouse", "Mouse"],
    "sex": ["female", "male", "other"],  # Row 3: invalid enum
    "genotype": ["5XFAD", None, "APOE4KI"],  # Row 2: missing required field
    "genotypeBackground": ["C57BL/6J", "C57BL/6J", "C57BL/6J"],
    "modelSystemName": ["5XFAD", "5XFAD", "APOE4KI"],
    "dateBirth": ["2024-01-15", "2024-02-20", "2024-03-10"],
    "individualIdSource": ["JAX", "JAX", "JAX"],
})

# Create a temporary CSV file with the test data
temp_fd, temp_csv = tempfile.mkstemp(suffix=".csv")
os.close(temp_fd)
test_data.to_csv(temp_csv, index=False)

# Step 2: Create the curation task (this creates an empty template RecordSet)
record_set, curation_task, data_grid = create_record_based_metadata_task(
    synapse_client=syn,
    project_id="syn123456789",
    folder_id="syn987654321",
    record_set_name="AnimalMetadata_Records",
    record_set_description="Animal study metadata with validation",
    curation_task_name="AnimalMetadata_Validation_Example",
    upsert_keys=["individualID"],
    instructions="Enter metadata for each animal. All required fields must be completed.",
    schema_uri=schema_uri,
    bind_schema_to_record_set=True,
)

time.sleep(10)

print(f"Curation task created with ID: {curation_task.task_id}")
print(f"RecordSet created with ID: {record_set.id}")

# Step 2.5: Upload the test data to the RecordSet
record_set = RecordSet(id=record_set.id).get(synapse_client=syn)
print("\nUploading test data to RecordSet...")
record_set.path = temp_csv
record_set = record_set.store(synapse_client=syn)
print(f"Test data uploaded to RecordSet {record_set.id}")

# Step 3: Collaborators enter data via the web UI, OR you can create/export a Grid programmatically
# For demonstration, here's the programmatic approach:
print("\nCreating Grid session for data entry...")
grid = Grid(record_set_id=record_set.id).create()
print("Grid session created. Users can now enter data.")

# After data entry is complete (either via web UI or programmatically),
# export the Grid to generate validation results
print("\nExporting Grid to RecordSet to generate validation results...")
grid.export_to_record_set()

# Clean up the Grid session
grid.delete()
print("Grid session exported and deleted.")

# Step 4: Refresh the RecordSet to get the latest validation results
print("\nRefreshing RecordSet to retrieve validation results...")
record_set = RecordSet(id=record_set.id).get()

# Step 5: Analyze validation results
validation_df = record_set.get_detailed_validation_results()

if validation_df is not None:
    # Summary statistics
    total_rows = len(validation_df)
    valid_count = (validation_df['is_valid'] == True).sum()  # noqa: E712
    invalid_count = (validation_df['is_valid'] == False).sum()  # noqa: E712

    print("\n=== Validation Summary ===")
    print(f"Total records: {total_rows}")
    print(f"Valid records: {valid_count} ({valid_count}/{total_rows})")
    print(f"Invalid records: {invalid_count} ({invalid_count}/{total_rows})")

    # Group errors by type for better understanding
    if invalid_count > 0:
        invalid_rows = validation_df[validation_df['is_valid'] == False]  # noqa: E712

        # Export detailed error report for review
        error_report = invalid_rows[['row_index', 'validation_error_message', 'all_validation_messages']]
        error_report_path = "validation_errors_report.csv"
        error_report.to_csv(error_report_path, index=False)
        print(f"\nDetailed error report saved to: {error_report_path}")

        # Show first few errors as examples
        print("\n=== Sample Validation Errors ===")
        for idx, row in error_report.head(3).iterrows():
            print(f"\nRow {row['row_index']}:")
            print(f"  Error: {row['validation_error_message']}")
            print(f"  ValidationError: {row['all_validation_messages']}")

# Clean up temporary file
if os.path.exists(temp_csv):
    os.unlink(temp_csv)
```

In this example you would expect to get results like:

```
=== Sample Validation Errors ===

Row 0:
  Error: expected type: String, found: Long
  ValidationError: ["#/dateBirth: expected type: String, found: Long"]

Row 1:
  Error: 2 schema violations found
  ValidationError: ["#/genotype: expected type: String, found: Null","#/dateBirth: expected type: String, found: Long"]

Row 2:
  Error: 2 schema violations found
  ValidationError: ["#/dateBirth: expected type: String, found: Long","#/sex: other is not a valid enum value"]
```

**Key points about validation results:**

- **Automatic generation**: Validation results are created automatically when you export data from a Grid session with a bound schema
- **Row-level detail**: Each row in your RecordSet gets its own validation status and error messages
- **Multiple violations**: The `all_validation_messages` column contains all schema violations for a row, not just the first one
- **Iterative correction**: Use the validation results to identify issues, make corrections in the Grid, export again, and re-validate

### When validation results are available

Validation results are only available after:
1. A JSON schema has been bound to the RecordSet (set `bind_schema_to_record_set=True` when creating the task)
2. Data has been entered through a Grid session
3. **The Grid session has been exported back to the RecordSet** - This is the critical step that triggers validation and populates the `validation_file_handle_id` attribute

The export can happen in two ways:
- **Via the Synapse web UI**: Users click the export/save button in the Grid interface
- **Programmatically**: Call `grid.export_to_record_set()` after creating a Grid session

If `get_detailed_validation_results()` returns `None`, the most common reason is that the Grid session hasn't been exported yet. Check that `record_set.validation_file_handle_id` is not `None` after exporting.

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
- [Grid.create][synapseclient.models.curation.Grid.create] - Create a Grid session from a RecordSet
- [Grid.export_to_record_set][synapseclient.models.curation.Grid.export_to_record_set] - Export Grid data back to RecordSet and generate validation results
- [Folder.bind_schema][synapseclient.models.Folder.bind_schema] - Bind schemas to folders
- [Folder.validate_schema][synapseclient.models.Folder.validate_schema] - Validate folder schema compliance
- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project

### Related Documentation

- [JSON Schema Tutorial](../../../tutorials/python/json_schema.md) - Learn how to register schemas
- [Schema Registry](https://synapse.org/Synapse:syn69735275/tables/) - Browse available schemas
