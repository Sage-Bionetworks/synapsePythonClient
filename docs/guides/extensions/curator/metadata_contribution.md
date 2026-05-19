# How to Enter and Update Metadata for a Curation Task

This guide shows how to programmatically complete file-based and record-based metadata curation tasks, including adding, editing, and validating metadata.

## Overview

By following this guide, you will:

- List curation tasks in a Synapse project
- Create a Grid if needed for a curation task
- Download record-based metadata locally as a csv
- Upsert your edits back into the grid (record-based tasks)
- Apply your changes so the administrator can validate them
- Review the validation report for your submission so you can fix issues before handing the task back (record-based tasks)
- Mark the curation task as COMPLETED to signal the administrator that you're done

## Prerequisites

- A Synapse account with access to the project containing the curation task
- Python environment with synapseclient and the `curator` extension installed (`pip install --upgrade "synapseclient[curator]"`)
- The Synapse ID of the project where the administrator created the curation tasks
- (Optional) The `task_id` of a specific `CurationTask` you've been pointed at

### Step 1: Authenticate

```python
from synapseclient import Synapse

syn = Synapse()
syn.login()
```

### Step 2: List all curation tasks in your project

Each `CurationTask` carries the information you need to decide which workflow to follow — most importantly, `task_properties` tells you whether it's a record-based task (you'll see `record_set_id`) or a file-based task (you'll see `file_view_id`).

```python
from pprint import pprint
from synapseclient.models import CurationTask

PROJECT_ID = "syn123456789"  # The Synapse ID of the project to list tasks from

all_tasks = list(CurationTask.list(project_id=PROJECT_ID))
for task in all_tasks:
    pprint(task)
```

### Step 3: Create a Grid session for the task

Ask the task to create a new Grid session for you — it picks the correct seed (record_set_id for record-based tasks, file_view_id for file-based tasks) automatically and links the session back to the task. If the task already has an active session linked, calling this replaces the link with the new session.

```python
from synapseclient.models import CurationTask


# Option A: take a curation task from the list above
curation_task = all_tasks[0]

# Option B (alternative): get a curation task directly by id — uncomment to use
# curation_task = CurationTask(task_id=12345)
# curation_task.get()

latest_grid = curation_task.create_grid_session()
```

### Step 4: Pull the grid down as a CSV

Download the current grid contents so you can edit them locally — in pandas, Excel, or any tool that reads CSV.

```python
csv_path = latest_grid.download_csv(destination=".", file_name="grid_export.csv")
print(f"Grid downloaded to: {csv_path}")
```

Open the CSV, make your edits, and save it back to a local path. For example, with pandas:

```python
import pandas as pd
import numpy as np

df = pd.read_csv(csv_path)
print(df)
# The grid you downloaded may be empty — the columns shown here will match
# whatever the curation administrator configured for the task's schema.

# Smoke-test stand-in: fills 4 rows with random integers regardless of column type.
# Replace this with real edits that match your task's schema before importing —
# schema validation runs in Step 6 and will reject values that don't fit.
df = pd.DataFrame(
    np.random.randint(0, 100, size=(4, len(df.columns))),
    columns=df.columns,
)

edited_path = "./grid_edited.csv"
df.to_csv(edited_path, index=False)
```

### Step 5: Upsert your edits back into the grid

> **Note:** Grid import via `import_csv` is currently only supported for record-based tasks. File-based tasks do not support CSV import.

`import_csv` upserts rows into the grid based on the `upsert_keys` the administrator configured on the curation task. Existing rows matching on those keys are updated; new rows are inserted.

```python
latest_grid = latest_grid.import_csv(path=edited_path)
print(f"Upserted edits into grid session: https://www.synapse.org/Grid:default?sessionId={latest_grid.session_id}")
```

### Step 6: Apply the changes to the source entity

> **Important:** Until you call `export_to_record_set()` (or `synchronize()` for file-based tasks), your edits live only inside the Grid session — they aren't visible on the RecordSet and won't be validated. Apply changes whenever you reach a logical checkpoint.

Applying the changes is what triggers schema validation and makes your edits visible to administrators and other contributors.

- For **record-based** tasks, export the grid back to the RecordSet. This creates a new version of the RecordSet and generates the validation report that administrators can retrieve via [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results].

    ```python
    latest_grid.export_to_record_set()
    print(f"Exported to RecordSet version: {latest_grid.record_set_version_number}")
    ```

- For **file-based** tasks, synchronize the grid against the file view to push annotation changes back to the underlying files.

    ```python
    latest_grid.synchronize()
    ```

### Step 7: Review your validation results (record-based tasks)

When you exported the grid in Step 6, Synapse validated each row against the JSON schema bound to the RecordSet and generated a row-level report. Reviewing this report before handing the task back to the administrator lets you catch and fix problems in your own data first — saving a round trip.

> **File-based tasks:** Validation for file-based tasks is enforced by the JSON schema bound to the folder containing the files, not by a row-level report on the Grid export. After you call synchronize(), the administrator verifies schema compliance using [Folder.validate_schema][synapseclient.models.Folder.validate_schema] — there is no per-row report for you to inspect from the contributor side.

#### Prerequisites for validation results

A validation report is only generated when **all** of the following are true:

1. A JSON schema has been bound to the RecordSet by the administrator who set up the task
2. You have entered data through a Grid session
3. The Grid session has been exported back to the RecordSet — this is the step that triggers validation and populates the RecordSet's validation_file_handle_id

If the Grid was never exported (Step 6), there is nothing to review yet.

#### Retrieve and inspect the results

Validation results live on the RecordSet itself, so you can retrieve them whether or not the Grid session is still open. Use the record_set_id from your CurationTask, re-fetch the RecordSet to pick up the latest validation_file_handle_id, and pull the detailed report as a pandas DataFrame:

```python
from synapseclient.models import RecordBasedMetadataTaskProperties, RecordSet

if isinstance(curation_task.task_properties, RecordBasedMetadataTaskProperties):
    record_set = RecordSet(id=curation_task.task_properties.record_set_id).get()

    validation_df = record_set.get_detailed_validation_results()

    if validation_df is None:
        print("No validation results yet — make sure the Grid was exported in Step 6.")
    else:
        total = len(validation_df)
        valid = validation_df["is_valid"].sum()
        invalid = (~validation_df["is_valid"]).sum()

        print(f"Total records: {total}")
        print(f"Valid records: {valid}")
        print(f"Invalid records: {invalid}")

        invalid_rows = validation_df[~validation_df["is_valid"]]
        for _, row in invalid_rows.iterrows():
            print(f"\nRow {row['row_index']}:")
            print(f"  Error: {row['validation_error_message']}")
            print(f"  All messages: {row['all_validation_messages']}")
```

Each row of the report carries:

- row_index — the row in the RecordSet that was validated
- is_valid — boolean indicating whether the row passes the schema
- validation_error_message — the primary schema violation for that row (if any)
- all_validation_messages — every schema violation for that row; a row may fail on multiple fields

Sample output for a submission with errors looks like:

```text
Row 1:
  Error: expected type: String, found: Null
  All messages: ["#/genotype: expected type: String, found: Null"]

Row 2:
  Error: other is not a valid enum value
  All messages: ["#/sex: other is not a valid enum value"]
```

#### Fix and re-export

If any rows are invalid, recreate a Grid session against the RecordSet (see Step 3), correct the offending rows, and re-run Steps 4–6 to re-export. The validation report is regenerated on each export, so iterate until the report is clean before letting the administrator know your task is ready.

> **If get_detailed_validation_results returns None after exporting:** check that record_set.validation_file_handle_id is set after the re-fetch. If it isn't, the export did not complete — re-run export_to_record_set() on an active Grid session against the same RecordSet.

#### Clean up the session

Once validation is clean and you're done, delete the session:

```python
latest_grid.delete()
```

Deleting is permanent — you can no longer re-export from this session. If you spot more issues later, create a new Grid session via Step 3.

### Step 8: Mark the curation task as COMPLETED

Once your validation report is clean and you've cleaned up the Grid session, transition the curation task to COMPLETED. This signals the administrator that the task is ready for their review — they can list tasks in the project and pick up the ones whose status is COMPLETED.

```python
curation_task.set_task_state(state="COMPLETED")
```

## References

### API Documentation

- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project
- [CurationTask.get][synapseclient.models.CurationTask.get] - Fetch a CurationTask by id
- [CurationTask.create_grid_session][synapseclient.models.CurationTask.create_grid_session] - Create a Grid session for a CurationTask and link it to the task status
- [CurationTask.set_task_state][synapseclient.models.CurationTask.set_task_state] - Set the state on a CurationTask's status
- [Grid.download_csv][synapseclient.models.Grid.download_csv] - Download Grid contents as a local CSV
- [Grid.import_csv][synapseclient.models.Grid.import_csv] - Upsert CSV edits back into a Grid session (record-based grids only)
- [Grid.export_to_record_set][synapseclient.models.Grid.export_to_record_set] - Export Grid data back to RecordSet and generate validation results
- [Grid.synchronize][synapseclient.models.Grid.synchronize] - Synchronize a file-based Grid against its source file view
- [Grid.delete][synapseclient.models.Grid.delete] - Delete a Grid session
- [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results] - Retrieve the row-level validation report for a RecordSet

### Related Documentation

- [How to Set Up Metadata Curation Workflows](metadata_curation.md) - The administrator-facing companion to this guide
