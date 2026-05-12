# How to Enter and Update Metadata for a Curation Task

This guide is for **data contributors** — someone who has been assigned a `CurationTask` and needs to fill in metadata against it (see [How to Set Up Metadata Curation Workflows](metadata_curation.md)). Your job is to open the work assigned to you, make your edits, and apply them so they can be validated.

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

## The high-level flow

1. **List** the curation tasks in your project to see what's been assigned
2. **Get the Grid session** for each task — attach to an existing one if it exists, otherwise create one
3. **Pull the grid down** as a CSV so you can edit in bulk with the tools of your choice
4. **Upsert your edits** back into the grid (record-based tasks only — file-based tasks edit through the Grid web UI; see notes below)
5. **Apply the changes** — export the grid back to the RecordSet (record-based) or synchronize it against the file view (file-based) so changes take effect
6. **Check the validation results** — confirm your submission passes the schema before handing the task back to the administrator (record-based tasks only)

## Step 1: Authenticate

```python
from synapseclient import Synapse

syn = Synapse()
syn.login()
```

## Step 2: List the curation tasks in your project

Each `CurationTask` carries the information you need to decide which workflow to follow — most importantly, `task_properties` tells you whether it's a record-based task (you'll see `record_set_id`) or a file-based task (you'll see `file_view_id`).

```python
from pprint import pprint
from synapseclient.models import CurationTask

PROJECT_ID = "syn123456789"  # The Synapse ID of the project to list tasks from

for curation_task in CurationTask.list(project_id=PROJECT_ID):
    pprint(curation_task)
```

> **Coming in v4.13.0 — filter to just your tasks:** `CurationTask.list` will accept an `assigned_to_me` flag that narrows results to tasks assigned to you or to any team you belong to. Pass `assigned_to_me=True` to skip tasks owned by other contributors. This filter cannot be combined with an explicit `assignee_ids` list — use one or the other. See the [ListCurationTaskRequest](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/ListCurationTaskRequest.html) REST reference for the underlying API contract.

## Step 3: Get the Grid session for a task

The administrator who set up the curation task should have already created a Grid session for it. You'll need its `session_id`.

### Find the session_id from a CurationTask

If no one handed you a `session_id` directly, you can discover it from the `CurationTask`. There is no direct pointer from a task to a Grid session — you go through the task's **source entity** (the `record_set_id` for record-based tasks, or the `file_view_id` for file-based tasks) and list the active Grid sessions for it.

```python
from synapseclient.models import (
    FileBasedMetadataTaskProperties,
    Grid,
    RecordBasedMetadataTaskProperties,
)

# `curation_task` is a CurationTask from Step 2.
if isinstance(curation_task.task_properties, RecordBasedMetadataTaskProperties):
    source_id = curation_task.task_properties.record_set_id
else:  # FileBasedMetadataTaskProperties
    source_id = curation_task.task_properties.file_view_id

grid_sessions = list(Grid.list(source_id=source_id))
for session in grid_sessions:
    print(
        f"session_id={session.session_id} "
        f"started_by={session.started_by} "
        f"modified_on={session.modified_on}"
    )
```

`Grid.list` may return zero, one, or many sessions. If there are several — for instance, when multiple contributors or teams are working against the same source entity — use `started_by` and `modified_on` to identify the session that belongs to your work, or coordinate with the task's administrator to confirm which one to use. Team-owned sessions (created when the task's `assignee_principal_id` points at a team) are joinable by any member of that team.

### Get the Grid Session

Once you have the `session_id` — whether from the lookup above or because someone shared it with you — you cna then get the session.

```python
from synapseclient.models import Grid

latest_grid = Grid(session_id="abc-123-def")
```

## Step 4: Pull the grid down as a CSV

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

## Step 5: Upsert your edits back into the grid (record-based tasks)

`import_csv` upserts rows into the grid based on the `upsert_keys` the administrator configured on the curation task. Existing rows matching on those keys are updated; new rows are inserted.

```python
latest_grid = latest_grid.import_csv(path=edited_path)
print(
    "Upserted edits into grid session: "
    f"https://www.synapse.org/Grid:default?sessionId={latest_grid.session_id}"
)
```

> **File-based tasks:** `Grid.import_csv` is currently supported only for grids created from a RecordSet. For file-based tasks, edit metadata directly in the Grid through the Synapse web UI, or update annotations on the underlying files via the EntityView (see [`EntityView.update_rows`][synapseclient.models.EntityView.update_rows]). Then proceed to Step 6.

## Step 6: Apply the changes to the source entity

Applying the changes is what makes your edits visible beyond the Grid session and — for record-based tasks — triggers schema validation.

- For **record-based** tasks, export the grid back to the RecordSet. This creates a new version of the RecordSet and generates the row-level validation report that administrators retrieve in [Step 4 of the administrator guide](metadata_curation.md#step-4-review-validation-results-from-contributor-submissions).

    ```python
    latest_grid.export_to_record_set()
    print(f"Exported to RecordSet version: {latest_grid.record_set_version_number}")
    ```

- For **file-based** tasks, synchronize the grid against the file view to push annotation changes back to the underlying files.

    ```python
    latest_grid.synchronize()
    ```

    Validation for file-based tasks is enforced by the JSON schema bound to the folder containing the files, not by a row-level export report. After synchronizing, the administrator can verify schema compliance using [`Folder.validate_schema`][synapseclient.models.Folder.validate_schema] — see [Validate schema binding on folders](metadata_curation.md#validate-schema-binding-on-folders) in the administrator guide.

Once your changes are applied and you're done with the session, you can clean it up:

```python
latest_grid.delete()
```

> **Important:** Until you call `export_to_record_set()` (or `synchronize()` for file-based tasks), your edits live only inside the Grid session — they aren't visible on the RecordSet or the underlying files and won't be validated. Apply changes whenever you reach a logical checkpoint.

## Step 7: Review your validation results (record-based tasks)

When you exported the grid in Step 6, Synapse validated each row against the JSON schema bound to the RecordSet and generated a row-level report. Reviewing this report before handing the task back to the administrator lets you catch and fix problems in your own data first — saving a round trip.

> **File-based tasks:** Validation for file-based tasks is enforced by the JSON schema bound to the folder containing the files, not by a row-level report on the Grid export. After you call synchronize(), the administrator verifies schema compliance using [Folder.validate_schema][synapseclient.models.Folder.validate_schema] — there is no per-row report for you to inspect from the contributor side.

### Prerequisites for validation results

A validation report is only generated when **all** of the following are true:

1. A JSON schema has been bound to the RecordSet by the administrator who set up the task
2. You have entered data through a Grid session
3. The Grid session has been exported back to the RecordSet — this is the step that triggers validation and populates the RecordSet's validation_file_handle_id

If the Grid was never exported (Step 6), there is nothing to review yet.

### Retrieve and inspect the results

Validation results live on the RecordSet itself, so you can retrieve them whether or not the Grid session is still open. Use the record_set_id from your CurationTask, re-fetch the RecordSet to pick up the latest validation_file_handle_id, and pull the detailed report as a pandas DataFrame:

```python
from synapseclient.models import RecordSet

# Use the record_set_id from the CurationTask you've been working on.
record_set = RecordSet(id="syn987654321").get()

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

### Fix and re-export

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
