# How to Enter and Update Metadata for a Record-Based Curation Task

This guide shows how to programmatically complete a record-based metadata curation task, including adding, editing, validating, and submitting metadata.

## Overview

By following this guide, you will:

- List curation tasks in a Synapse project
- Create a Grid session for a record-based curation task
- Synchronize the Grid session to pick up schema changes made after the session was created
- Download metadata from the Grid to a local CSV
- Edit the metadata locally
- Upload the metadata back into the Grid
- Validate your edits in-session against the bound JSON schema
- Push the Grid back to the RecordSet (synchronize with `PULL_PUSH`)
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

#### Option B: Filter the list by assignee, state, or data type

Use this when you want to find tasks assigned to you, tasks in a specific state, or locate a task by its data type. Each filter still returns a list — pick the one you want from it.

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

# Find a task by data type (list() does not support data type filtering directly — filter after listing)
target_data_type = "AnimalMetadata"
matching_tasks = [
    task
    for task in CurationTask.list(project_id=PROJECT_ID)
    if task.data_type == target_data_type
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

### Step 3: Get or create a Grid session for the task

Each option in Step 2 leaves you with a single `curation_task`. Open its Grid with [get_or_create_curator_grid][synapseclient.extensions.curator.get_or_create_curator_grid]. Given the task's ID, it returns the Grid already linked to the task, or — if none is linked yet — creates a new Grid session, links it to the task, and returns it. The first call starts the session; every subsequent call returns that same session, so you can pick up where you left off without accidentally starting over.

The `record_set_id` and the authorization mode are taken from the task properties automatically, so you do not need to specify them here.

```python
from synapseclient.extensions.curator import get_or_create_curator_grid

latest_grid = get_or_create_curator_grid(task_id=curation_task.task_id)
```

### Step 4: Synchronize the grid session to pick up schema updates

A Grid session captures the JSON schema in place at the moment it's created — it does not automatically pick up a newer schema version. If the administrator adds or changes a column in the schema *after* you created your session in Step 3, synchronize the session to pull in the latest schema and data from the RecordSet before you continue working.

For record-based grids, choose `sync_type` explicitly — `"PULL"` or `"PULL_PUSH"` — rather than omitting it:

```python
latest_grid = latest_grid.synchronize(sync_type="PULL")
```

- **`"PULL"`** — refreshes the grid session with the latest schema and data from the RecordSet, without writing anything back. Use this to preview an incoming schema or data change (for example, a newly added column) before you've made any edits of your own, or simply to catch the session up to the current RecordSet state.
- **`"PULL_PUSH"`** — does the same pull, then immediately writes the grid session's current data back to the RecordSet as a new version. Use this once you're ready to commit your in-progress edits together with the refreshed schema/data.

> **Important:** `sync_type` is optional here, not enforced — omitting it doesn't raise an error, it silently defaults to `"PULL_PUSH"`. For a RecordSet-backed grid that can mean committing a new version and overwriting the RecordSet with your local session's current state when you only meant to preview incoming changes. Passing it explicitly avoids that surprise.

> **Note:** Run this step any time you suspect the schema has changed since you opened the session — for example, if the administrator mentions they've published a new schema version, or if a field you expect to see is missing from your downloaded CSV in Step 5.

If your session is already current (no schema changes since Step 3), you can skip this step entirely.

### Step 5: Download record-based metadata as a local CSV

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

### Step 6: Import edited record-based metadata to Synapse

`import_csv` upserts rows into the grid based on the `upsert_keys` the administrator configured when setting up the `RecordSet`. Existing rows matching on those keys are updated; new rows are inserted.

```python
latest_grid = latest_grid.import_csv(path=edited_path)
print(f"Upserted edits into grid session: https://www.synapse.org/Grid:default?sessionId={latest_grid.session_id}")
```

### Step 6: Validate your edits in-session

Before you push, you can check your edits against the JSON schema bound to the RecordSet directly on the Grid session — without creating a new RecordSet version. This lets you catch and fix problems while iterating, then push once (Step 7) with clean data.

Open the session with `connect()` (or `connect_async()`), which binds a replica for the duration of the `with` block, then call `validate_rows()` with a `QueryRequest`. Each returned row carries its own `validation_results`. `SelectAll()` returns every column, so each row's full data comes back alongside its validation results.

Reuse the `latest_grid` session you've been editing:

```python
from synapseclient.models.curation import GridQuery, QueryRequest, SelectAll

with latest_grid.connect() as grid:
    query_request = QueryRequest(query=GridQuery(column_selection=[SelectAll()]))
    query_result = grid.validate_rows(query_request=query_request)

    if not query_result.rows:
        print("No rows matched the query.")
    else:
        for row in query_result.rows:
            print(f"Row ID: {row.row_id}, Validation Result: {row.validation_results}")
```

If you're landing here with only a `record_set_id` (no session yet), `connect()` will create — or, with `.connect(attach_to_previous_session=True)`, reattach to a session for you:

```python
from synapseclient.models import Grid
from synapseclient.models.curation import GridQuery, QueryRequest, SelectAll

with Grid(record_set_id="syn123456789").connect() as grid:
    query_request = QueryRequest(query=GridQuery(column_selection=[SelectAll()]))
    query_result = grid.validate_rows(query_request=query_request)

    for row in query_result.rows:
        print(f"Row ID: {row.row_id}, Validation Result: {row.validation_results}")
```

To focus on just the rows that currently fail, add a `RowIsValidFilter` and set `include_validation_messages=True` so each row’s `validation_results` includes the full `all_validation_messages` list:

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

In-session `validate_rows()` checks the current Grid rows without creating a new RecordSet version — this is where you should do the bulk of your iterating: edit, check, edit again, with no push required until you're confident the data is clean. Pushing (Step 7) creates a new RecordSet version each time, so treat it as something you do once you're done rather than as your primary feedback loop. The validation report reviewed in Step 8 (`get_detailed_validation_results()`) reflects the last `PULL_PUSH` synchronize from Step 7. Each method has a matching `_async` counterpart (`connect_async`, `validate_rows_async`) for async contexts.

### Step 7: Push the grid back to the RecordSet

> **Important:** Until you synchronize with `sync_type=SyncType.PULL_PUSH`, your edits live only inside the Grid session — they aren't visible on the RecordSet, and there is no new RecordSet version or persisted validation report for them (Step 8). You can still check them against the schema beforehand with in-session `validate_rows()` (Step 6). Apply changes whenever you reach a logical checkpoint.

Pushing creates a new version of the RecordSet and makes your edits visible to administrators and other contributors. Schema validation results are generated from this push, but may not be available right away — see Step 8 for how to check them once they're ready.

```python
from synapseclient.models.curation import SyncType

latest_grid = latest_grid.synchronize(sync_type=SyncType.PULL_PUSH)
```

> **Note:** `export_to_record_set()` is deprecated in favor of `synchronize()`. Unlike the deprecated method, `synchronize()` does not return the new RecordSet version number on `latest_grid`. To look it up, re-fetch the RecordSet via `record_set_id` (see Step 8) after synchronizing.

### Step 8: Review your validation results

When you synchronized the grid with `PULL_PUSH` in Step 7, Synapse validated each row against the JSON schema bound to the RecordSet and generated a row-level report. If you already cleared Step 6's in-session validation before pushing, this report should come back clean — treat it as confirmation that what's now live on the RecordSet matches what you validated, not as your first chance to find problems.

#### Prerequisites for validation results

A validation report is only generated when **all** of the following are true:

1. A JSON schema has been bound to the RecordSet by the administrator who set up the task
2. You have entered data through a Grid session
3. The Grid session has been synchronized back to the RecordSet with `sync_type=SyncType.PULL_PUSH` — this is the step that triggers validation and populates the RecordSet's validation_file_handle_id

If the Grid was never synchronized with `PULL_PUSH` (Step 7), there is nothing to review yet.

#### Retrieve and inspect the results

Validation results live on the RecordSet itself, so you can retrieve them whether or not the Grid session is still open. Use the record_set_id from your CurationTask, re-fetch the RecordSet to pick up the latest validation_file_handle_id, and pull the detailed report as a pandas DataFrame:

```python
from synapseclient.models import RecordBasedMetadataTaskProperties, RecordSet

if isinstance(curation_task.task_properties, RecordBasedMetadataTaskProperties):
    record_set = RecordSet(id=curation_task.task_properties.record_set_id).get()
    print(f"RecordSet version: {record_set.version_number}")

    validation_df = record_set.get_detailed_validation_results()

    if validation_df is None:
        print("No validation results yet — make sure the Grid was synchronized with PULL_PUSH in Step 7.")
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

#### If this report still shows invalid rows

This shouldn't happen if Step 6 was clean immediately before you pushed — treat it as a signal something changed in between (for example, you kept editing after your last in-session check, or the schema itself changed). Re-open the Grid session (see Step 3 — `get_or_create_curator_grid` returns the session already linked to the task) and correct the offending rows.

Rather than pushing again right away, go back to Step 6 and re-run in-session `validate_rows()` first — confirm the fix is clean there, with no new RecordSet version created in the process — and only push (Step 7) once it is. Repeating push-then-check-the-report as your correction loop costs a new RecordSet version and a round trip through validation on every attempt; checking in-session first is faster and doesn't leave a trail of throwaway versions on the RecordSet.

> **If get_detailed_validation_results returns None after pushing:** check that record_set.validation_file_handle_id is set after the re-fetch. If it isn't, the push did not complete — re-run `synchronize(sync_type=SyncType.PULL_PUSH)` on an active Grid session against the same RecordSet.

### Step 9: Mark the curation task as COMPLETED

Once your validation report is clean and you've cleaned up the Grid session, transition the curation task to COMPLETED. This signals the administrator that the task is ready for their review — they can list tasks in the project and pick up the ones whose status is COMPLETED.

```python
curation_task.set_task_state(state="COMPLETED")
```

## File-Based Curation Tasks

File-based tasks follow the same overall flow as record-based tasks (Steps 1–9 above), with three key differences:

**No CSV import.** `import_csv` is not currently supported for file-based grids. Instead, you can either:

- Download the CSV (Step 5) as a local reference, make your edits locally, then copy-paste the values back into the Grid UI
- Make edits directly in the Synapse Grid UI — Step 3 prints the session URL (`https://www.synapse.org/Grid:default?sessionId=...`) after creating the session

**`synchronize()` writes straight to the files, not a RecordSet.** After editing in the Grid UI, push your changes back to the underlying files the same way record-based tasks do in Step 7:

```python
latest_grid = latest_grid.synchronize()
```

This writes the Grid annotation values back to each file as Synapse annotations. There is no versioned RecordSet — the files themselves are updated in place.

Note: for file-based grids, the value you pass for `sync_type` (or omitting it) makes no difference — synchronizing always behaves as `"PULL_PUSH"`; there is no separate preview (`"PULL"`) step. This is unlike record-based grids (Step 4), where the value genuinely changes behavior, so it's worth choosing deliberately there.

**No per-row export report — but in-session validation still works.** There is no versioned RecordSet, so the validation report reviewed in Step 8 (`synchronize(sync_type=SyncType.PULL_PUSH)` → `get_detailed_validation_results()`) does not apply. However, the in-session validation from Step 6 works identically for file-based grids: a file-based session created from an `initial_query` still carries a bound JSON schema (`grid_json_schema_id`), so `connect()` + `validate_rows()` returns the same per-row `validation_results`. This is your primary contributor-side check for file-based tasks — run it before `synchronize()`.

## Appendix

### Cleaning up a Grid session

```python
# Delete the grid session
latest_grid.delete()

# Remove the deleted session's reference from the task, so the task no longer
# points at a session that no longer exists
status = curation_task.get_status()
status.execution_details = None
curation_task.update_status(curation_task_status=status)
```

Deleting is permanent — you can no longer synchronize from this session. If you spot more issues later, get a fresh Grid session via Step 3: `get_or_create_curator_grid` creates a new one and links it to the task.

## References

### API Documentation

<!-- markdownlint-disable MD052 -->
- [CurationTask.list][synapseclient.models.CurationTask.list] - List curation tasks in a project
- [CurationTask.get][synapseclient.models.CurationTask.get] - Fetch a CurationTask by id
- [get_or_create_curator_grid][synapseclient.extensions.curator.get_or_create_curator_grid] - Get the Grid attached to a CurationTask, creating and linking one if needed
- [CurationTask.create_grid_session][synapseclient.models.CurationTask.create_grid_session] - Always create a new Grid session for a CurationTask and link it to the task status
- [CurationTask.set_task_state][synapseclient.models.CurationTask.set_task_state] - Set the state on a CurationTask's status
- [Grid.connect][synapseclient.models.Grid.connect] - Connect to a Grid session and bind a replica for in-session validation
- [Grid.validate_rows][synapseclient.models.Grid.validate_rows] - Validate a Grid session's rows against the bound JSON schema
- [Grid.download_csv][synapseclient.models.Grid.download_csv] - Download Grid contents as a local CSV
- [Grid.import_csv][synapseclient.models.Grid.import_csv] - Upsert CSV edits back into a Grid session (record-based grids only)
- [Grid.synchronize][synapseclient.models.Grid.synchronize] - Synchronize a Grid session against its source RecordSet or file view, pulling in schema/data changes and (for `PULL_PUSH`) writing edits back and generating validation results
- [Grid.delete][synapseclient.models.Grid.delete] - Delete a Grid session
- [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results] - Retrieve the row-level validation report for a RecordSet
<!-- markdownlint-enable MD052 -->

### Related Documentation

- [How to Set Up Metadata Curation Workflows](metadata_curation.md) - The administrator-facing companion to this guide
