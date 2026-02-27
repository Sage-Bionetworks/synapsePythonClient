"""
Script: Managing curation tasks.
Covers listing, updating, and deleting curation tasks.
"""

from synapseclient import Synapse
from synapseclient.models import CurationTask

syn = Synapse()
syn.login()

# List tasks in a project
for task in CurationTask.list(project_id="syn123456789"):
    print(f"Task {task.task_id}: {task.data_type}")
    print(f"  Instructions: {task.instructions}")
    if task.assignee_principal_id:
        print(f"  Assigned to: {task.assignee_principal_id}")

# Update a task
task = CurationTask(task_id=42).get()
task.instructions = "Updated instructions for data contributors"
task = task.store()

# Delete a task (simple)
task = CurationTask(task_id=42)
task.delete()

# Delete a task and clean up the associated EntityView (file-based only)
task = CurationTask(task_id=42)
task.delete(delete_file_view=True)
