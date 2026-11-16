# How to Create and Fill Curation Tasks in Bulk

This guide is for **curation administrators** who run a hub with several contributing groups. It shows you how to create a record-based curation task for each group, **fill each task with that group's records**, and load later revisions of those records safely. One manifest file feeds  `synapse curate plan` and `synapse curate apply`, plus the matching Python functions.

If you're setting up a single task, see [How to Set Up Metadata Curation Workflows](metadata_curation.md) instead. If you're a contributor filling in a task, see [How to Enter and Update Metadata for a Record-Based Curation Task](metadata_contribution.md).

## What you'll accomplish

By following this guide, you will:

- Describe every group's task in one manifest CSV
- Preview what a run would do with `synapse curate plan`
- Create and fill the tasks with `synapse curate apply`
- Rerun the manifest when a group sends a revised file, with or without `--overwrite`
- Archive the old task that a new one replaces
- Find out why a task was blocked, and keep a full report of a run
- Do the same from Python

## Prerequisites

- A Synapse account that can edit every folder named in the manifest
- Python environment with synapseclient and the `curator` extension installed (`pip install --upgrade "synapseclient[curator]"`).
- For each group: an existing Synapse project, a folder in it to hold the task's table, and a team who should receive the assigned task. The command does not create projects, folders or teams, and does not set permissions.
- **A registered JSON schema with a version.** The command does not register schemas; see [How to Generate JSONschemas from Curator CSV data models](schema_operations.md) or the [`register-json-schema`](../../../tutorials/command_line_client.md#register-json-schema) command.
- One CSV of records per group whose header uses the schema's property names

The command signs in with your own credentials, found the way every other `synapse` command finds them (see [Authentication](../../../tutorials/authentication.md)). Credentials never go in the manifest and never appear in the output or the report.

## Example

A hub collects biospecimen records from two labs. Each lab has its own project, team and CSV file. Every task uses the same schema, `org.namhub-biospecimen-1.2.0`, and identifies a row by its `specimenID`. Lab A already has a task on an older schema version, `Lab A biospecimens v1`, which the new task replaces. Lab A's file also carries an `internal_notes` column that is not in the schema.

### Step 1: Write the manifest

The manifest is a CSV with a header row and one row per task. Save it beside the labs' files as `manifest.csv`:

```csv
name,project_id,folder_id,curation_task_name,schema_uri,upsert_keys,assignee_principal_id,authorization_mode,instructions,path,leave_out_columns,archive
lab_a,syn111,syn112,Lab A biospecimens,org.namhub-biospecimen-1.2.0,specimenID,3412345,SOURCE_BENEFACTOR,"Fill one row per specimen.",lab_a.csv,internal_notes,Lab A biospecimens v1
lab_b,syn221,syn222,Lab B biospecimens,org.namhub-biospecimen-1.2.0,specimenID,3412346,SOURCE_BENEFACTOR,"Fill one row per specimen.",lab_b.csv,,
```

| Column | Required | Value |
|---|---|---|
| `name` | yes | A short name for the task, used with `--task`. Unique in the manifest. |
| `project_id` | yes | The task's project, such as `syn111` |
| `folder_id` | yes | The folder inside that project that holds the task's table |
| `curation_task_name` | yes | The task's name in Curator. Unique per project in the manifest. |
| `record_set_name` | no | The table's name. Leave it blank to use `curation_task_name`. |
| `schema_uri` | yes | A registered schema with its version, `ORG-NAME-VERSION` |
| `upsert_keys` | yes | The key columns that identify a row, separated by `;` |
| `assignee_principal_id` | yes | The ID of the team the task is assigned to |
| `authorization_mode` | yes | `SESSION_OWNER` or `SOURCE_BENEFACTOR`; see [Controlling who can access the grid session](metadata_curation.md#controlling-who-can-access-the-grid-session) |
| `instructions` | yes | The instructions contributors see |
| `path` | yes | The group's input CSV, absolute or relative to the manifest's folder. `~` and environment variables are expanded. |
| `leave_out_columns` | no | Input columns not to load, separated by `;` |
| `archive` | no | Names of tasks in the same project to archive, separated by `;` |

Column order does not matter. Surrounding whitespace in a manifest cell is ignored, and a value can hold commas or line breaks inside double quotes. A value that applies to every task, such as the schema, is repeated in each row.

The manifest is checked before Synapse is contacted. If anything is wrong, the run stops and lists every problem by line, such as `line 3 (lab_b)`. The manifest is rejected when:

- a required column is missing, or a column is unknown, unnamed, or appears twice
- a row has more cells than the header
- a required value is blank, a Synapse ID is not `syn` followed by digits, or `authorization_mode` is not one of the two values
- an input file does not exist
- two rows share a `name`, the same task name in the same project, or the same table name in the same folder
- a task's name is in the `archive` list of a row in the same project

### The input files

Each input CSV has a header of schema property names, plus any columns named in `leave_out_columns`. A cell that is empty or holds only whitespace is blank. Every other cell is loaded exactly as it reads in the file, including quoted values with commas or line breaks.

A schema property missing from the file still becomes a column in a new task's table, with blank cells, and the plan notes it. An input column that is neither a schema property nor left out blocks that task.

### Step 2: Preview the run

```bash
synapse curate plan manifest.csv
```

`plan` reads the manifest, the input files and Synapse, and changes nothing. It prints one line per task:

- **`create`**: no task of that name exists in the project, so one would be created.
- **`exists`**: the task exists and would be filled. Settings that differ from the manifest are listed under the table.
- **`blocked`**: nothing would be written to this task. The reason is listed under the table. See [What blocks a task](#what-blocks-a-task).

For each task the plan counts the rows to add, the rows unchanged, the rows that differ from Synapse, and the rows in Synapse that are absent from the file. It also lists the old tasks it would archive. Problems name their task, file, line, key and column where they apply.

`plan` does not report validation problems. Synapse validates rows only once they are loaded into the task's table, so those appear when you apply.

To plan only some tasks, name them:

```bash
synapse curate plan manifest.csv --task lab_b
```

`--task` takes several names and can be repeated. A name the manifest does not define rejects the command.

### Step 3: Create and fill the tasks

```bash
synapse curate apply manifest.csv
```

`apply` does what `plan` describes for the same manifest, options and Synapse state, without asking for confirmation. For each task to create, it:

1. creates the table in the named folder, bound to the schema, with the named key columns. The table shows the key columns first, then the other columns in the input file's order, then any schema properties the file lacks.
2. creates the task, assigned to the named team, with the instructions and authorization mode.
3. loads the file's rows, leaving out the columns in `leave_out_columns`.
4. reads the table back and confirms that every row it wrote is there.
5. archives the old tasks the row names.

It then reports the validation problems Synapse finds in each task it wrote to, against the task's schema. These are Synapse's own results, the same ones contributors see in Curator. Rows that fail validation are still loaded, and validation problems never block a task.

The summary looks like this:

```text
TASK   RESULT   ADD  UNCHANGED  DIFFERS  ABSENT  VALIDATION  ARCHIVE                         NOTES
lab_a  created  48   0          0        0       3           Lab A biospecimens v1: archived
lab_b  blocked  -    -          -        -       -           -                               1 blocking problem

Blocking problems:
  lab_b: input file lab_b.csv line 1: column 'site' is not a property of schema org.namhub-biospecimen-1.2.0; list it in leave_out_columns to leave it out

2 tasks: 1 created, 1 blocked; 3 validation problems; archives: 1 archived
```

Each task ends as `created`, `updated`, `unchanged`, `blocked` or `failed`. A problem in one task does not stop the others. The command exits with status 1 when the manifest or command line is rejected, when any task is blocked or fails, or when any archive fails. Validation problems alone leave the exit status at 0.

### Step 4: Rerun when a file changes

When a group sends a revised file, update the file and run the same manifest again, `plan` first:

```bash
synapse curate plan manifest.csv
synapse curate apply manifest.csv
```

A rerun never creates a second task with the same name in a project. It matches each file row to a table row on the key columns. For a task that already exists, those are the table's own key columns, even when the manifest names different ones; the plan lists the difference and the task is not blocked.

- A row whose key is not in the table is added.
- A row that matches the table is left as it is.
- A row that **differs**, because a non-blank cell in the file holds a different value from Synapse, is left unchanged. The plan lists it with its differing columns. This protects edits contributors made in Curator.
- A row in the table whose key is not in the file stays in place, and the plan counts it.

Running `apply` a second time with no changes anywhere leaves Synapse unchanged.

A task's instructions, assignee, authorization mode and key columns are set only when the task is created. If they differ from the manifest on a task that exists, the plan lists them and the task keeps its values. A schema property in the file that an existing table lacks is not loaded, and the plan notes it.

If an `apply` is interrupted, run it again: it finishes the remaining work without duplicating what was done. If it stopped after a task's table was stored but before the task was, the rerun blocks that task and names the table; remove or rename that table yourself, then rerun.

### Overwrite differing rows

To make differing rows take the file's values, add `--overwrite`. Preview it first with the same option:

```bash
synapse curate plan manifest.csv --overwrite
synapse curate apply manifest.csv --overwrite
```

With `--overwrite`, the file's non-blank values replace the values in Synapse. A blank cell in the file never clears a value in Synapse. After writing, `apply` reads the table back and confirms that every overwritten cell holds the value written.

Synapse keeps every version of a task's table, so an overwrite you regret can be recovered from an earlier version. The command keeps no backups of its own.

### Step 5: Archive the task a new one replaces

To retire an old task when its replacement is ready, put the old task's name in the new task's `archive` column, as row `lab_a` does with `Lab A biospecimens v1`. Several names are separated by `;`, and each must be a task in the same project.

`plan` lists each old task as to be archived, already archived, or not found. `apply` archives it only when the new task was created or already existed, was filled and confirmed, and was not blocked and did not fail in that run. That way a group is never left without a live task.

- Archiving leaves the old task's table and rows unchanged and readable.
- An old task that is already archived is left alone.
- An old task that is not found is noted, and the rest of that row's work goes ahead.
- If archiving fails, the run reports it and exits with status 1, the new task keeps its result, and a rerun tries the archive again.

## What blocks a task

A task is blocked when:
- an input column is neither a schema property nor listed in `leave_out_columns`
- two rows in the input file share a key, or a row's key is blank or only whitespace. The plan names the rows.
- the folder, team or schema the row names does not exist, or you lack permission on the folder
- a task of that name exists but its table uses a different schema, or lives in a different folder, than the manifest says. Nothing is rebound or moved.
- a task of that name exists but is archived. A task is never reopened.
- the run would add or overwrite rows in a task its contributors have marked complete
- the run would add or overwrite rows in a task that has an attached grid session. The block names the session, because edits in the session that are not yet synchronized are not in the table the plan compares against.

If a task is blocked, all other tasks go ahead, and **the run exits with status 1**.

Since `--overwrite` writes more rows, `plan --overwrite` may block tasks that `plan` alone does not. A grid session that was deleted in Synapse no longer counts as attached.

A task **fails**, rather than being blocked, when Synapse cannot import its input file, or when the read-back finds a written row missing or an overwritten cell not holding the value written. A failed task archives nothing, and a rerun fills it. When Synapse cannot import the file, nothing is saved to the task's table.

## What the command does not do

- It never deletes a task, a table or a row.
- It does not register or generate schemas, and does not upgrade an existing task to a new schema version in place. To move a group to a new schema, create a new task and archive the old one.
- It does not change an existing task's assignee, instructions, authorization mode or key columns, or any task state other than archiving.
- It does not rename input columns to match the schema.

## Keep a report of the run

The terminal shows a summary. To keep the full detail, name a folder:

```bash
synapse curate apply manifest.csv --report-dir reports/2026-11
```

The folder is created if needed, and these three CSV files are written into it, replacing any earlier copies. Nothing else in the folder is touched.

| File | What it holds |
|---|---|
| `summary.csv` | One row per task: the command, whether `--overwrite` was given, the task and table IDs, the action and result, the row counts, the number of validation problems, the archives, the settings that differ, the key columns, the missing and skipped columns, and whether the input file was empty |
| `differences.csv` | One row per differing cell: the task, the row's key, the input file line, the column, the file's value, the Synapse value, and whether the file's value was written |
| `problems.csv` | One row per problem, validation problems included: the task, the kind (`input`, `synapse` or `validation`), whether it blocks the task, and the file, line, key, column and message |

Without `--report-dir`, the command writes no files. A rejected manifest writes no report.

## Use the Python functions

Everything the command does is available from Python. The functions return the plan and the outcome of each task as data; they print nothing and write no files.

```python
import synapseclient
from synapseclient.extensions.curator import (
    CurationTaskManifest,
    apply_curation_tasks,
    plan_curation_tasks,
)

syn = synapseclient.Synapse()
syn.login()

manifest = CurationTaskManifest.from_csv("manifest.csv")

plans = plan_curation_tasks(manifest, synapse_client=syn)
for plan in plans:
    print(plan.row.name, plan.action, plan.rows.to_add, len(plan.rows.differing))
    for problem in plan.problems:
        print("  ", problem.message)

if not any(plan.blocked for plan in plans):
    outcomes = apply_curation_tasks(manifest, synapse_client=syn)
    for outcome in outcomes:
        print(outcome.plan.row.name, outcome.result, outcome.rows_written)
        for problem in outcome.validation_problems:
            print("  ", problem.key, problem.message)
```

Both functions take `tasks=["lab_b"]` to limit the run and `overwrite=True` to overwrite differing rows. Reading or checking the manifest raises [CurationManifestError][synapseclient.extensions.curator.CurationManifestError], whose `problems` attribute lists every problem; so does naming a task the manifest does not define. A Synapse error while planning a task becomes a blocking problem on that task's plan, not an exception.

The async forms take the same arguments:

```python
import asyncio

import synapseclient
from synapseclient.extensions.curator import (
    CurationTaskManifest,
    apply_curation_tasks_async,
    plan_curation_tasks_async,
)

syn = synapseclient.Synapse()
syn.login()


async def main():
    manifest = CurationTaskManifest.from_csv("manifest.csv")
    plans = await plan_curation_tasks_async(manifest, synapse_client=syn)
    outcomes = await apply_curation_tasks_async(manifest, synapse_client=syn)
    return plans, outcomes


plans, outcomes = asyncio.run(main())
```

## References

### API Documentation

- [CurationTaskManifest][synapseclient.extensions.curator.CurationTaskManifest] - Read and check a task manifest
- [plan_curation_tasks][synapseclient.extensions.curator.plan_curation_tasks] - Plan the tasks in a manifest
- [apply_curation_tasks][synapseclient.extensions.curator.apply_curation_tasks] - Create, fill and archive the tasks in a manifest
- [TaskPlan][synapseclient.extensions.curator.TaskPlan] - What a run would do for one task
- [TaskOutcome][synapseclient.extensions.curator.TaskOutcome] - What a run did for one task
- [create_record_based_metadata_task][synapseclient.extensions.curator.create_record_based_metadata_task] - Create a single record-based task
- [CurationTask.set_task_state][synapseclient.models.CurationTask.set_task_state] - Update the lifecycle state of a curation task

### Related Documentation

- [How to Set Up Metadata Curation Workflows](metadata_curation.md) - Create a single curation task and work with its RecordSet
- [How to Enter and Update Metadata for a Record-Based Curation Task](metadata_contribution.md) - The contributor's side of a task
- [How to Generate JSONschemas from Curator CSV data models](schema_operations.md) - Generate and register the schema a task uses
- [Command line client: `curate`](../../../tutorials/command_line_client.md#curate) - The command's options
