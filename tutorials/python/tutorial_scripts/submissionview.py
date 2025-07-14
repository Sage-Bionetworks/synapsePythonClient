"""
Here is where you'll find the code for the SubmissionView tutorial.

A SubmissionView allows you to aggregate and query submissions from one or more
evaluation queues in a tabular format, similar to other view types in Synapse.

This tutorial also shows how to create an evaluation queue, submit a file to it, and
update the submission status. The example uses a temporary file for the submission,
which is automatically cleaned up after the script runs. You can modify the file
path and name as needed.

"""

import tempfile

import pandas as pd

from synapseclient import Evaluation, Synapse
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    File,
    Project,
    SubmissionView,
    UsedURL,
)

syn = Synapse()
syn.login()

# Retrieve the project ID
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = my_project.id
print(f"My project ID is: {project_id}")

# Step 1: Set up and create an evaluation queue - Must be globally unique
evaluation_name = "Test Evaluation Queue for Alzheimer conference"
evaluation_description = "Evaluation queue for testing submission view"
evaluation = Evaluation(
    name=evaluation_name, description=evaluation_description, contentSource=project_id
)
evaluation = syn.store(evaluation)
print(f"Created evaluation queue with ID: {evaluation.id}")

# Step 2: Create a SubmissionView for the evaluation queue
view = SubmissionView(
    name="SubmissionView for Alzheimer conference",
    parent_id=project_id,
    scope_ids=[evaluation.id],
    include_default_columns=True,
    columns=[
        Column(
            name="metric_A",
            column_type=ColumnType.DOUBLE,
        ),
        Column(
            name="metric_B",
            column_type=ColumnType.DOUBLE,
        ),
    ],
    activity=Activity(
        name="Submission Review Analysis",
        description="Analysis of Q1 2025 challenge submissions",
        used=[
            UsedURL(
                name="Challenge Homepage",
                url="https://sagebionetworks.org/community/challenges-portal",
            )
        ],
    ),
).store()

print(f"My SubmissionView ID is: {view.id}")

# Reorder columns for better display
view.reorder_column(name="name", index=2)
view.reorder_column(name="status", index=3)
view.reorder_column(name="evaluationid", index=4)
view.store()

print("Available columns in the view:", list(view.columns.keys()))

# Step 3: Create and submit a file to the evaluation queue
with tempfile.NamedTemporaryFile(
    mode="w", suffix=".txt", delete=True, delete_on_close=False
) as temp_file:
    with open(temp_file.name, "w") as opened_temp_file:
        opened_temp_file.write("This is a test submission file.")
    temp_file_path = temp_file.name

    # Upload the temporary file to Synapse
    submission_file = File(
        path=temp_file_path, parent_id=project_id, name="Test Submission"
    ).store()

    # Submit the file to the evaluation queue
    submission = syn.submit(
        evaluation=evaluation,
        entity=submission_file,
        name="Test Submission",
        submitterAlias="Participant 1",
    )

    print(f"Created submission with ID: {submission.id}")

# Step 4: Query and update the submission status
# Query the SubmissionView to see our submission
query = f"SELECT * FROM {view.id} WHERE id = '{submission.id}'"
results_as_dataframe: pd.DataFrame = view.query(query=query)
# Due to the eventual consistency of the system, we need to perform 2 queries
results_as_dataframe: pd.DataFrame = view.query(query=query)

print("Query results:")
print(results_as_dataframe)

# Update the status to indicate it's been scored
submission_status = syn.getSubmissionStatus(submission=submission.id)
print(f"Submission status: {submission_status.status}")
submission_status.status = "SCORED"
submission_status.submissionAnnotations["metric_A"] = 90
submission_status.submissionAnnotations["metric_B"] = 80
submission_status.score = 0.7
submission_status = syn.store(submission_status)
print(f"Updated submission status to: {submission_status.status}")

# Step 5: Modify the SubmissionView scope
# First let's make sure we have the latest view from Synapse:
view.get()

# Create another evaluation queue to demonstrate adding to the scope
second_evaluation = Evaluation(
    name="Second Test Evaluation Queue for Alzheimer conference",  # Must be globally unique
    description="Another evaluation queue for testing submission view",
    contentSource=project_id,
)
second_evaluation = syn.store(second_evaluation)
print(f"Created second evaluation queue with ID: {second_evaluation.id}")

# Add the new evaluation queue to the view's scope
view.scope_ids.append(second_evaluation.id)
view.store()  # Store the updated view
print("Updated SubmissionView scope. Current scope IDs:", view.scope_ids)

# Step 6: Create a snapshot of the view
snapshot_info = view.snapshot(
    comment="Initial submission review snapshot",
)
print("Created snapshot of the SubmissionView:")
print(snapshot_info)
snapshot_version = snapshot_info.snapshot_version_number
print(f"Snapshot version number: {snapshot_version}")

# Step 7: Query the snapshot we just created
# You may also get the snapshot version from the view object directly by looking at the version number
# (which is the latest version of the view) and subtracting 1.
# snapshot_version = view.version_number - 1

snapshot_query = f"SELECT * FROM {view.id}.{snapshot_version}"
snapshot_results = view.query(snapshot_query)
print("Query results from the snapshot:")
print(snapshot_results)
