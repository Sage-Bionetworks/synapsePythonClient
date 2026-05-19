# How to Enter and Update Metadata for a Curation Task

This guide shows how to programmatically complete file-based and record-based metadata curation tasks, including adding, editing, and validating metadata.

## What you'll accomplish

By following this guide, you will:

- List the curation tasks in your project to see what's been assigned
- Attach to an existing Grid session for a task, or create one if none exists
- Pull the grid contents down to a local CSV so you can edit in bulk
- Upsert your edits back into the grid (record-based tasks)
- Apply your changes so the administrator can validate them
- Review the validation report for your submission so you can fix issues before handing the task back (record-based tasks)

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
from synapseclient.models.curation import CurationTask

PROJECT_ID = "syn123456789"  # The Synapse ID of the project to list tasks from

all_tasks = list(CurationTask.list(project_id=PROJECT_ID))
for task in all_tasks:
    pprint(task)
```

### Step 3: Get the Grid session for a task, or create one if it doesn't exist

For each task you plan to work on, determine the **source entity** (the `record_set_id` for record-based tasks, or the `file_view_id` for file-based tasks), then look for an active Grid session on it. If one exists, attach to it; otherwise, create a new one.

```python
from synapseclient.models import Grid
from synapseclient.models.table_components import Query
from synapseclient.models.curation import (
    CurationTask,
    FileBasedMetadataTaskProperties,
    RecordBasedMetadataTaskProperties,
)


# Option A: take a curation task from the list above (Python lists are 0-indexed)
curation_task = all_tasks[0]

# Option B (alternative): get a curation task directly by id
# curation_task = CurationTask(task_id=12345)
# curation_task.get()

if isinstance(curation_task.task_properties, RecordBasedMetadataTaskProperties):
    source_id = curation_task.task_properties.record_set_id

    grid_sessions = list(Grid.list(source_id=source_id))
    if grid_sessions:
        latest_grid = grid_sessions[0]
        print(f"Active grid sessions: {len(grid_sessions)}. Getting latest grid session: {latest_grid.session_id}")
    else:
        print(f"No active grid session for {source_id}; creating one.")
        latest_grid = Grid(record_set_id=source_id)
        latest_grid.create()

elif isinstance(curation_task.task_properties, FileBasedMetadataTaskProperties):
    source_id = curation_task.task_properties.file_view_id
    grid_sessions = list(Grid.list(source_id=source_id))
    if grid_sessions:
        latest_grid = grid_sessions[0]
        print(f"Active grid sessions: {len(grid_sessions)}. Getting latest grid session: {latest_grid.session_id}")
    else:
        print(f"No active grid session for {source_id}; creating one.")
        latest_grid = Grid(initial_query=Query(sql=f"SELECT * FROM {source_id}"))
        latest_grid.create()
```

> **Why prefer an existing session?** Grid sessions can be owned by a team (when the administrator set `assignee_principal_id` to a team ID), so teammates can share progress on the same session instead of forking divergent copies.

### Step 4: Pull the grid down as a CSV

Download the current grid contents so you can edit them locally — in pandas, Excel, or any tool that reads CSV.

```python
csv_path = latest_grid.download_csv(destination=".", file_name="temp.csv")
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

# Add 4 rows of placeholder numerical values using whatever columns the grid has.
df = pd.DataFrame(
    np.random.randint(0, 100, size=(4, len(df.columns))),
    columns=df.columns,
)

edited_path = "./grid_edited.csv"
df.to_csv(edited_path, index=False)
```

### Step 5: Upsert your edits back into the grid

`import_csv` upserts rows into the grid based on the `upsert_keys` the administrator configured on the curation task. Existing rows matching on those keys are updated; new rows are inserted.

```python
latest_grid = latest_grid.import_csv(path=edited_path)
print(f"Upserted edits into grid session: https://www.synapse.org/Grid:default?sessionId={latest_grid.session_id}")
```

### Step 6: Apply the changes to the source entity

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

Once your changes are applied and you're done with the session, you can clean it up:

```python
latest_grid.delete()
```

> **Important:** Until you call `export_to_record_set()` (or `synchronize()` for file-based tasks), your edits live only inside the Grid session — they aren't visible on the RecordSet and won't be validated. Apply changes whenever you reach a logical checkpoint.

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
from synapseclient.models import RecordSet

if isinstance(curation_task.task_properties, RecordBasedMetadataTaskProperties):
    record_set = RecordSet(id=curation_task.task_properties.record_set_id).get()

    validation_df = record_set.get_detailed_validation_results()

    if validation_df is None:
        print("No validation results yet — make sure the Grid was exported in Step 6.")
    else:
        total = len(validation_df)
        valid = (validation_df["is_valid"] == True).sum()  # noqa: E712
        invalid = (validation_df["is_valid"] == False).sum()  # noqa: E712

        print(f"Total records: {total}")
        print(f"Valid records: {valid}")
        print(f"Invalid records: {invalid}")

        invalid_rows = validation_df[validation_df["is_valid"] == False]  # noqa: E712
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

## References

### API Documentation

- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project
- [Grid.list][synapseclient.models.curation.Grid.list] - List active Grid sessions, optionally filtered by source entity
- [Grid.create][synapseclient.models.curation.Grid.create] - Create a Grid session from a RecordSet or EntityView
- [Grid.download_csv][synapseclient.models.curation.Grid.download_csv] - Download Grid contents as a local CSV
- [Grid.import_csv][synapseclient.models.curation.Grid.import_csv] - Upsert CSV edits back into a Grid session (record-based grids only)
- [Grid.export_to_record_set][synapseclient.models.curation.Grid.export_to_record_set] - Export Grid data back to RecordSet and generate validation results
- [Grid.synchronize][synapseclient.models.curation.Grid.synchronize] - Synchronize a file-based Grid against its source file view
- [Grid.delete][synapseclient.models.curation.Grid.delete] - Delete a Grid session
- [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results] - Retrieve the row-level validation report for a RecordSet

### Related Documentation

- [How to Set Up Metadata Curation Workflows](metadata_curation.md) - The administrator-facing companion to this guide
