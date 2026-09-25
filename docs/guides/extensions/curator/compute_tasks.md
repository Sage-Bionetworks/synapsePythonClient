# How to Run Compute Tasks

This guide is for **curation administrators** who want Synapse to produce metadata automatically instead of asking a contributor to type it into a Grid. A *compute task* is a `CurationTask` whose properties describe a computation, and which you run with [CurationTask.execute][synapseclient.models.CurationTask.execute].

If you're setting up a task for a person to fill in by hand, see [How to Set Up Metadata Curation Workflows](metadata_curation.md) instead. The two kinds of task live side by side in the same project — a compute task usually *writes into* the RecordSet of a record-based task created by that guide.

## What you'll accomplish

By following this guide, you will:

- Choose between the two kinds of compute task Synapse supports
- Create a compute task and point it at a destination RecordSet
- Run the computation and wait for it to finish
- Read the execution details, including the error message when a run fails

## Prerequisites

- A Synapse account, and either assignment to the task or `UPDATE` access on the task's project
- Python environment with synapseclient and the `curator` extension installed (`pip install --upgrade "synapseclient[curator]"`)
- A **destination task**: an existing record-based `CurationTask` whose RecordSet will receive the output. Create one with [create_record_based_metadata_task][synapseclient.extensions.curator.create_record_based_metadata_task] as described in [How to Set Up Metadata Curation Workflows](metadata_curation.md). The JSON Schema bound to that RecordSet defines the shape of the output
- Depending on the kind of compute task, either a file-based `CurationTask` to read annotations from, or a folder of source files to transform

## Step 1: Authenticate and import

```python
from synapseclient import Synapse
from synapseclient.models import (
    CurationTask,
    RecordSetGenerationExecutionProperties,
    SampleSheetGenerationExecutionProperties,
)

syn = Synapse()
syn.login()
```

## Step 2: Choose the kind of compute task

### Option A: Sample sheet generation

Use this when the metadata you need already exists as annotations on files, and you want it reshaped into a sample sheet. Synapse reads the FileView of an existing **file-based** curation task and writes a sample sheet into the destination RecordSet.

```python
task = CurationTask(
    project_id="syn9876543",
    data_type="animal_sample_sheet",   # Must be unique within the project
    instructions="Generate the sample sheet from the annotated sequencing files.",
    task_properties=SampleSheetGenerationExecutionProperties(
        input_task_id=123,        # A file-based CurationTask; its FileView is the source
        destination_task_id=456,  # A record-based CurationTask; its RecordSet is the target
    ),
).store()

print(f"Created compute task: {task.task_id}")
```

`input_task_id` must refer to a task with `FileBasedMetadataTaskProperties` — the annotations shown in its FileView are the source data. The JSON Schema bound to the destination task's RecordSet establishes the target sample sheet format.

### Option B: Record set generation

Use this when the metadata is locked up in documents rather than annotations. Synapse reads the files in a folder, follows your free-text instructions to transform them into a CSV, and writes that CSV to the destination RecordSet.

```python
task = CurationTask(
    project_id="syn9876543",
    data_type="clinical_records_from_pdfs",   # Must be unique within the project
    instructions="Extract the clinical measurements from the uploaded study reports.",
    task_properties=RecordSetGenerationExecutionProperties(
        folder_id="syn987654321",  # Folder holding the source files
        instructions=(
            "Each PDF is one subject visit. Produce one row per visit with the "
            "subject identifier, visit date, and every measurement recorded in the "
            "summary table."
        ),
        destination_task_id=456,   # A record-based CurationTask; its RecordSet is the target
    ),
).store()

print(f"Created compute task: {task.task_id}")
```

!!! note "Limits on the source folder"
    The transformation reads the **direct children** of `folder_id` only — files in subfolders are ignored. There is a maximum of 20 files, each under 100 MB, and each must be a PDF, CSV, TXT, or JSON file.

The `instructions` on the properties describe the transformation to perform and are what the computation acts on. The `instructions` on the task itself are the human-facing description shown in the Synapse web UI, the same as for any other curation task.

## Step 3: Mark the task as executable

A newly created task has no execution details, and Synapse refuses to dispatch a task that has none:

```
400 Client Error: Task does not have ExecutableTaskExecutionDetails. Cannot dispatch for execution.
```

Attach the execution details that match the task's properties with [CurationTask.set_execution_details][synapseclient.models.CurationTask.set_execution_details] — `SampleSheetGenerationExecutionDetails` for Option A, `RecordSetGenerationExecutionDetails` for Option B. Create them empty; Synapse fills in `async_job_id`, `started_by`, and `started_on` as the job runs, and the error fields if it fails.

```python
from synapseclient.models import RecordSetGenerationExecutionDetails

task.set_execution_details(
    execution_details=RecordSetGenerationExecutionDetails()
)
```

This is a one-time step per task. It does not change the task's state, which must still be `NOT_STARTED` when you run it.

## Step 4: Run the computation

[CurationTask.execute][synapseclient.models.CurationTask.execute] starts the job and blocks until it finishes, then returns the task's execution details.

```python
details = task.execute()

print(f"Started by: {details.started_by}")
print(f"Started on: {details.started_on}")
```

The task must be in the `NOT_STARTED` state. A task that is already running, or one whose properties describe manual metadata entry, cannot be executed.

Generation can take a while. `execute` waits up to `timeout` seconds for the job to complete *or report progress*, so a long-running job that keeps reporting progress will not trip the timeout. Raise it for large inputs:

```python
details = task.execute(timeout=600)
```

### Running asynchronously

Every method has an `_async` counterpart, which is the better choice when you're running several computations at once:

```python
import asyncio
from synapseclient import Synapse
from synapseclient.models import CurationTask

syn = Synapse()
syn.login()

async def main():
    results = await asyncio.gather(
        CurationTask(task_id=111).execute_async(timeout=600),
        CurationTask(task_id=222).execute_async(timeout=600),
    )
    for details in results:
        print(details.started_on)

asyncio.run(main())
```

## Step 5: Check the outcome

The returned execution details record how the run went. The same details are attached to the task's status, so a run that finished earlier — or one whose client was interrupted mid-wait — can be inspected at any time.

!!! warning "A failed run may not raise"
    When the computation itself fails, the error is recorded on the task's execution details and the task stays in `NOT_STARTED`, but the failure is not always reported back through the async job. `execute` can keep polling well past its `timeout`, since the timeout only fires when a job stops reporting progress. If a run appears to hang, interrupt it and read `execution_details.error_message` — the job runs server-side and is unaffected by the client stopping.

```python
from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError, SynapseTimeoutError
from synapseclient.models import CurationTask

syn = Synapse()
syn.login()

task = CurationTask(task_id=789)

try:
    details = task.execute(timeout=600)
    print("Execution finished.")
except SynapseTimeoutError:
    print("Still running; check the task status later.")
except SynapseError as error:
    print(f"Execution failed: {error}")

# The status carries the same execution details, so you can check a run at any time
status = task.get_status()
print(f"State: {status.state}")

if status.execution_details and status.execution_details.error_message:
    print(f"Error: {status.execution_details.error_message}")
    print(f"Details: {status.execution_details.error_details}")
```

A successful run writes its output as a **new version** of the destination task's RecordSet, leaving the previous version intact, and the compute task moves to `IN_REVIEW` — the results are waiting for a human to look at them.

The output lands in the *destination* task's RecordSet, not in anything owned by the compute task, so read the RecordSet's synId off that task rather than hardcoding it:

```python
from synapseclient.models import RecordSet

destination_task = CurationTask(task_id=456).get()
record_set = RecordSet(id=destination_task.task_properties.record_set_id).get()
print(f"Now at version {record_set.version_number}")
```

Generating a version does not validate it: `validation_file_handle_id` stays empty until a Grid session is exported back to the RecordSet. To check the generated records against the bound schema, open a Grid on the RecordSet and export it, then read the results as described in [Getting detailed validation results](metadata_curation.md#getting-detailed-validation-results).

Once you're satisfied with the output, close the task out:

```python
from synapseclient.models import TaskState

CurationTask(task_id=789).set_task_state(state=TaskState.COMPLETED)
```

## Complete example scripts

### Record set generation

This script creates a record set generation task, runs it, and reports the outcome:

```python
from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError
from synapseclient.models import (
    CurationTask,
    RecordSetGenerationExecutionDetails,
    RecordSetGenerationExecutionProperties,
    TaskState,
)

PROJECT_ID = "syn9876543"          # The project both tasks belong to
SOURCE_FOLDER_ID = "syn987654321"  # Folder of PDFs to transform
DESTINATION_TASK_ID = 456          # A record-based CurationTask

syn = Synapse()
syn.login()

# Step 1: Create the compute task
task = CurationTask(
    project_id=PROJECT_ID,
    data_type="clinical_records_from_pdfs",
    instructions="Extract the clinical measurements from the uploaded study reports.",
    task_properties=RecordSetGenerationExecutionProperties(
        folder_id=SOURCE_FOLDER_ID,
        instructions=(
            "Each PDF is one subject visit. Produce one row per visit with the "
            "subject identifier, visit date, and every measurement recorded in the "
            "summary table."
        ),
        destination_task_id=DESTINATION_TASK_ID,
    ),
).store()

print(f"Created compute task {task.task_id}")

# Step 2: Mark the task as executable. Synapse refuses to dispatch a task that
# has no ExecutableTaskExecutionDetails, and a new task has none.
task.set_execution_details(execution_details=RecordSetGenerationExecutionDetails())

# Step 3: Run it, allowing 10 minutes
try:
    details = task.execute(timeout=600)
    print(f"Execution started on {details.started_on} by {details.started_by}")
except SynapseError as error:
    print(f"Execution failed: {error}")

# Step 4: Report the outcome
status = task.get_status()
print(f"Task state: {status.state}")

if status.state == TaskState.IN_REVIEW:
    print("Results are ready for review in the destination RecordSet.")
elif status.execution_details and status.execution_details.error_message:
    print(f"Error: {status.execution_details.error_message}")
```

### Sample sheet generation

The same four steps, reading from a file-based task instead of a folder. Note that there are no transformation instructions to write: the schema bound to the destination RecordSet is what determines the sample sheet format.

```python
from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError
from synapseclient.models import (
    CurationTask,
    SampleSheetGenerationExecutionDetails,
    SampleSheetGenerationExecutionProperties,
    TaskState,
)

PROJECT_ID = "syn9876543"   # The project the tasks belong to
INPUT_TASK_ID = 123         # A file-based CurationTask; its FileView is the source
DESTINATION_TASK_ID = 456   # A record-based CurationTask; its RecordSet is the target

syn = Synapse()
syn.login()

# Step 1: Create the compute task
task = CurationTask(
    project_id=PROJECT_ID,
    data_type="patient_sample_sheet",
    instructions="Generate the sample sheet from the annotated sequencing files.",
    task_properties=SampleSheetGenerationExecutionProperties(
        input_task_id=INPUT_TASK_ID,
        destination_task_id=DESTINATION_TASK_ID,
    ),
).store()

print(f"Created compute task {task.task_id}")

# Step 2: Mark the task as executable. Synapse refuses to dispatch a task that
# has no ExecutableTaskExecutionDetails, and a new task has none.
task.set_execution_details(execution_details=SampleSheetGenerationExecutionDetails())

# Step 3: Run it, allowing 10 minutes
try:
    details = task.execute(timeout=600)
    print(f"Execution started on {details.started_on} by {details.started_by}")
except SynapseError as error:
    print(f"Execution failed: {error}")

# Step 4: Report the outcome
status = task.get_status()
print(f"Task state: {status.state}")

if status.state == TaskState.IN_REVIEW:
    print("Results are ready for review in the destination RecordSet.")
elif status.execution_details and status.execution_details.error_message:
    print(f"Error: {status.execution_details.error_message}")
```

The files behind `INPUT_TASK_ID` must already carry the annotations the sample sheet is built from — the computation reads them through that task's FileView, not from the file contents.

## Task states for compute tasks

Compute tasks move through the same lifecycle as any other curation task, using two states that manual tasks rarely see:

| State | Meaning |
|---|---|
| `NOT_STARTED` | The task has been created but not run. This is the only state `execute` accepts. A run that fails leaves the task here, with the reason on its execution details, so it can simply be run again. |
| `EXECUTING` | An automated execution is currently running. |
| `IN_REVIEW` | The execution completed successfully and the results are pending human review. |
| `COMPLETED` | The task has been completed and verified. |
| `CANCELED` | The task has been canceled and is no longer needed. |

## References

### API Documentation

- [CurationTask.execute][synapseclient.models.CurationTask.execute] - Run a compute task and wait for the result
- [CurationTask.store][synapseclient.models.CurationTask.store] - Create or update a curation task
- [CurationTask.get_status][synapseclient.models.CurationTask.get_status] - Read a task's state and execution details
- [CurationTask.set_task_state][synapseclient.models.CurationTask.set_task_state] - Update the lifecycle state of a curation task
- [SampleSheetGenerationExecutionProperties][SampleSheetGenerationExecutionProperties-reference] - Properties for a sample sheet generation task
- [RecordSetGenerationExecutionProperties][RecordSetGenerationExecutionProperties-reference] - Properties for a record set generation task
- [SampleSheetGenerationExecutionDetails][SampleSheetGenerationExecutionDetails-reference] - Execution details for a sample sheet generation run
- [RecordSetGenerationExecutionDetails][RecordSetGenerationExecutionDetails-reference] - Execution details for a record set generation run

### Related Documentation

- [How to Set Up Metadata Curation Workflows](metadata_curation.md) - Create the record-based task that receives the output
- [How to Enter and Update Metadata for a Curation Task](metadata_contribution.md) - The contributor-facing guide for manual tasks
- [JSON Schema Tutorial](../../../tutorials/python/json_schema.md) - Learn how to register the schemas that define the output format
