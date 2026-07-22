# How to Enter and Update Metadata for a Record-Based Curation Task

This guide shows how to programmatically complete a record-based metadata curation task, including adding, editing, validating, and submitting metadata.

## Overview

By following this guide, you will:

- List curation tasks in a Synapse project
- Create a Grid session for a record-based curation task
- Download metadata from the Grid to a local CSV
- Edit the metadata locally
- Upload the metadata back into the Grid
- Export the Grid to the RecordSet to trigger schema validation
- Review the validation report
- Mark the curation task as COMPLETED to signal the administrator that you're done

## Requirements

- A Synapse account
- Completion of the certification quiz
- A minimum of **view** access on the Synapse project
- A minimum of **edit** access on the folder containing the RecordSet entity
- Python environment with synapseclient and the `curator` extension installed (`pip install --upgrade "synapseclient[curator]"`)
- The Synapse ID of the project where the administrator created the curation tasks
- (Optional) The `task_id` of a specific `CurationTask` you've been pointed at

### Step 1: Authenticate

```python
from synapseclient import Synapse

syn = Synapse()
syn.login()
```

### Step 2: Find a curation task

Each `CurationTask` carries the information you need. For record-based tasks, `task_properties` will contain a `record_set_id`.

Choose whichever approach fits your situation. Whichever you pick, the goal is the same: end up with a single `curation_task` object to use in Step 3. `CurationTask.list()` returns fully-populated tasks — each one already carries its `task_properties`, so there is no need to call `.get()` again on a task you got from a list.

#### Option A: List all tasks in the project

Use this when you don't know the task ID yet and want to browse what's available. List the tasks, inspect them, and pick the one you want.

```python
from pprint import pprint
from synapseclient.models import CurationTask

PROJECT_ID = "syn123456789"  # The Synapse ID of the project to list tasks from

all_tasks = list(CurationTask.list(project_id=PROJECT_ID))
for task in all_tasks:
    pprint(task)

# Select the task you want to work on (here, the first one as an example)
curation_task = all_tasks[0]
```

#### Option B: Filter the list by assignee, state, or name

Use this when you want to find tasks assigned to you, tasks in a specific state, or locate a task by name. Each filter still returns a list — pick the one you want from it.

```python
from synapseclient.models import CurationTask

PROJECT_ID = "syn123456789"

# Find all tasks assigned to the currently logged-in user
my_tasks = list(CurationTask.list(project_id=PROJECT_ID, assigned_to_me=True))

# Find tasks assigned to specific users or teams (by principal ID)
team_tasks = list(CurationTask.list(project_id=PROJECT_ID, assignee_ids=["1234567", "7654321"]))

# Find all tasks that are currently in progress
in_progress_tasks = list(
    CurationTask.list(project_id=PROJECT_ID, state_filter=["IN_PROGRESS"])
)

# Find a task by name (list() does not support name filtering directly — filter after listing)
target_name = "AnimalMetadata_Curation"
named_tasks = [
    task
    for task in CurationTask.list(project_id=PROJECT_ID)
    if task.name == target_name
]

# Select the task you want from whichever list you built above
curation_task = my_tasks[0]
```

#### Option C: Fetch a task directly by ID

Use this when the administrator has given you a specific task ID.

```python
from synapseclient.models import CurationTask

curation_task = CurationTask(task_id=12345).get()
```

### Step 3: Create a Grid session for the task

Each option in Step 2 leaves you with a single `curation_task`. Start a new Grid session on it — it picks the `record_set_id` from the task properties automatically and links the session back to the task. If the task already has an active Grid session linked, calling this replaces the link with the new session.

```python
latest_grid = curation_task.create_grid_session()
```

### Step 4: Download record-based metadata as a local CSV

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

# Example only: fills 4 rows with random integers regardless of column type.
# Replace this with real edits that match your task's schema before importing —
# schema validation runs in Step 7 and will reject values that don't fit.
df = pd.DataFrame(
    np.random.randint(0, 100, size=(4, len(df.columns))),
    columns=df.columns,
)

edited_path = "./grid_edited.csv"
df.to_csv(edited_path, index=False)
```

### Step 5: Import edited record-based metadata to Synapse

`import_csv` upserts rows into the grid based on the `upsert_keys` the administrator configured when setting up the `RecordSet`. Existing rows matching on those keys are updated; new rows are inserted.

```python
latest_grid = latest_grid.import_csv(path=edited_path)
print(f"Upserted edits into grid session: https://www.synapse.org/Grid:default?sessionId={latest_grid.session_id}")
```

### Step 6: Validate your edits in-session (before exporting)

Before you export, you can check your edits against the JSON schema bound to the RecordSet directly on the Grid session — without creating a new RecordSet version. This lets you catch and fix problems while iterating, then export once (Step 7) with clean data.

Open the session with `connect()` (or `connect_async()`), which binds a replica for the duration of the `with` block, then call `validate_rows()` with a `QueryRequest`. Each returned row carries its own `validation_results`. `SelectAll()` returns every column, so each row's full data comes back alongside its validation results.

Reuse the `latest_grid` session you've been editing:

```python
from synapseclient.models.curation import GridQuery, QueryRequest, SelectAll

with latest_grid.connect() as grid:
    query_request = QueryRequest(query=GridQuery(column_selection=[SelectAll()]))
    query_result = grid.validate_rows(query_request=query_request)

    if query_result.rows is None:
        print("No rows matched the query.")
    else:
        for row in query_result.rows:
            print(f"Row ID: {row.row_id}, Validation Result: {row.validation_results}")
```

If you're landing here with only a `record_set_id` (no session yet), `connect()` will create — or, with `.connect(attach_to_previous_session=True)`, reattach to — a session for you:

```python
from synapseclient.models import Grid
from synapseclient.models.curation import GridQuery, QueryRequest, SelectAll

with Grid(record_set_id="syn123456789").connect() as grid:
    query_request = QueryRequest(query=GridQuery(column_selection=[SelectAll()]))
    query_result = grid.validate_rows(query_request=query_request)

    for row in query_result.rows:
        print(f"Row ID: {row.row_id}, Validation Result: {row.validation_results}")
```

To focus on just the rows that currently fail, add a `RowIsValidFilter` and set `include_validation_messages=True` so each row carries the full `all_validation_messages` list:

```python
from synapseclient.models.curation import (
    GridQuery,
    QueryRequest,
    RowIsValidFilter,
    SelectAll,
)

with latest_grid.connect() as grid:
    query_request = QueryRequest(
        query=GridQuery(
            column_selection=[SelectAll()],
            filters=[RowIsValidFilter(value=False)],
            include_validation_messages=True,
        )
    )
    query_result = grid.validate_rows(query_request=query_request)

    for row in query_result.rows:
        print(f"Invalid row {row.row_id}: {row.validation_results}")
```

!!! note "Requires a bound schema"
    In-session validation only produces results when the administrator has bound a
    JSON schema to the RecordSet. Without one, rows still return but their
    `validation_results` is `None` for each row.

In-session `validate_rows()` checks the current Grid rows without creating a new RecordSet version — use it to iterate quickly. The export report reviewed in Step 8 (`get_detailed_validation_results()`) reflects the last export from Step 7. Each method has a matching `_async` counterpart (`connect_async`, `validate_rows_async`) for async contexts.

### Step 7: Export the grid back to the RecordSet

> **Important:** Until you call `export_to_record_set()`, your edits live only inside the Grid session — they aren't visible on the RecordSet and won't be validated. Apply changes whenever you reach a logical checkpoint.

Exporting triggers schema validation and makes your edits visible to administrators and other contributors. It creates a new version of the RecordSet and generates the validation report.

```python
latest_grid.export_to_record_set()
print(f"Exported to RecordSet version: {latest_grid.record_set_version_number}")
```

### Step 8: Review your validation results

When you exported the grid in Step 7, Synapse validated each row against the JSON schema bound to the RecordSet and generated a row-level report. Reviewing this report before handing the task back to the administrator lets you catch and fix problems in your own data first — saving a round trip. (For a quick check before you commit an export, use the in-session validation in Step 6 instead.)

#### Prerequisites for validation results

A validation report is only generated when **all** of the following are true:

1. A JSON schema has been bound to the RecordSet by the administrator who set up the task
2. You have entered data through a Grid session
3. The Grid session has been exported back to the RecordSet — this is the step that triggers validation and populates the RecordSet's validation_file_handle_id

If the Grid was never exported (Step 7), there is nothing to review yet.

#### Retrieve and inspect the results

Validation results live on the RecordSet itself, so you can retrieve them whether or not the Grid session is still open. Use the record_set_id from your CurationTask, re-fetch the RecordSet to pick up the latest validation_file_handle_id, and pull the detailed report as a pandas DataFrame:

```python
from synapseclient.models import RecordBasedMetadataTaskProperties, RecordSet

if isinstance(curation_task.task_properties, RecordBasedMetadataTaskProperties):
    record_set = RecordSet(id=curation_task.task_properties.record_set_id).get()

    validation_df = record_set.get_detailed_validation_results()

    if validation_df is None:
        print("No validation results yet — make sure the Grid was exported in Step 7.")
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

!!! note "Older CurationTasks without task properties"
    CurationTasks created before task properties were introduced will not have a
    `taskProperties` field in the Synapse response. Attempting to retrieve such a task
    via `get()`, `store()`, or `list()` will raise a `ValueError`. If you encounter
    this error, delete the task with `task.delete(delete_source=False)` and recreate
    it with the appropriate task properties.

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

If any rows are invalid, recreate a Grid session against the RecordSet (see Step 3), correct the offending rows, and re-run Steps 4–7 to re-export. The validation report is regenerated on each export, so iterate until the report is clean before letting the administrator know your task is ready.

> **If get_detailed_validation_results returns None after exporting:** check that record_set.validation_file_handle_id is set after the re-fetch. If it isn't, the export did not complete — re-run export_to_record_set() on an active Grid session against the same RecordSet.

### Step 9: Mark the curation task as COMPLETED

Once your validation report is clean and you've cleaned up the Grid session, transition the curation task to COMPLETED. This signals the administrator that the task is ready for their review — they can list tasks in the project and pick up the ones whose status is COMPLETED.

```python
curation_task.set_task_state(state="COMPLETED")
```

## File-Based Curation Tasks

File-based tasks follow the same overall flow as record-based tasks (Steps 1–9 above), with three key differences:

**No CSV import.** `import_csv` is not currently supported for file-based grids. Instead, you can either:

- Download the CSV (Step 4) as a local reference, make your edits locally, then copy-paste the values back into the Grid UI
- Make edits directly in the Synapse Grid UI — Step 3 prints the session URL (`https://www.synapse.org/Grid:default?sessionId=...`) after creating the session

**Use `synchronize()` instead of `export_to_record_set()`.** After editing in the Grid UI, push your changes back to the underlying files:

```python
latest_grid.synchronize()
```

This writes the Grid annotation values back to each file as Synapse annotations. There is no versioned RecordSet — the files themselves are updated in place.

**No per-row export report — but in-session validation still works.** There is no versioned RecordSet, so the export report reviewed in Step 8 (`export_to_record_set()` → `get_detailed_validation_results()`) does not apply. However, the in-session validation from Step 6 works identically for file-based grids: a file-based session created from an `initial_query` still carries a bound JSON schema (`grid_json_schema_id`), so `connect()` + `validate_rows()` returns the same per-row `validation_results`. This is your primary contributor-side check for file-based tasks — run it before `synchronize()`.

```python
from synapseclient.models.curation import GridQuery, QueryRequest, SelectAll

with latest_grid.connect() as grid:
    query_request = QueryRequest(query=GridQuery(column_selection=[SelectAll()]))
    query_result = grid.validate_rows(query_request=query_request)

    for row in query_result.rows:
        print(f"Row ID: {row.row_id}, Validation Result: {row.validation_results}")
```

After you call `synchronize()`, the administrator also verifies schema compliance on their end. If they report violations, correct the flagged annotations in the Grid UI and re-synchronize.

## Appendix

### Cleaning up a Grid session

```python
latest_grid.delete()
```

Deleting is permanent — you can no longer re-export from this session. If you spot more issues later, create a new Grid session via Step 3.

## References

### API Documentation

<!-- markdownlint-disable MD052 -->
- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project
- [CurationTask.get][synapseclient.models.CurationTask.get] - Fetch a CurationTask by id
- [CurationTask.create_grid_session][synapseclient.models.CurationTask.create_grid_session] - Create a Grid session for a CurationTask and link it to the task status
- [CurationTask.set_task_state][synapseclient.models.CurationTask.set_task_state] - Set the state on a CurationTask's status
- [Grid.connect][synapseclient.models.Grid.connect] - Connect to a Grid session and bind a replica for in-session validation
- [Grid.validate_rows][synapseclient.models.Grid.validate_rows] - Validate a Grid session's rows against the bound JSON schema
- [Grid.download_csv][synapseclient.models.Grid.download_csv] - Download Grid contents as a local CSV
- [Grid.import_csv][synapseclient.models.Grid.import_csv] - Upsert CSV edits back into a Grid session (record-based grids only)
- [Grid.export_to_record_set][synapseclient.models.Grid.export_to_record_set] - Export Grid data back to RecordSet and generate validation results
- [Grid.synchronize][synapseclient.models.Grid.synchronize] - Synchronize a file-based Grid against its source file view
- [Grid.delete][synapseclient.models.Grid.delete] - Delete a Grid session
- [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results] - Retrieve the row-level validation report for a RecordSet
<!-- markdownlint-enable MD052 -->

### Related Documentation

- [How to Set Up Metadata Curation Workflows](metadata_curation.md) - The administrator-facing companion to this guide
