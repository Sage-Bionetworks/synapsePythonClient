"""
Here is where you'll find the code for the Activity/Provenance tutorial.
"""

# Step 1: Add a new Activity to your File
# --8<-- [start:retrieve_project_folder_file]
import os
import tempfile

import synapseclient
from synapseclient.models import Activity, File, Folder, Project, UsedEntity, UsedURL

syn = synapseclient.login()

# Set project and folder name that exists within the project
PROJECT_NAME = "Dark Side Of The Moon"
FOLDER_NAME = "biospecimen_experiment_1"

# Retrieve the project and folder IDs
my_project_id = Project(name=PROJECT_NAME).get().id

biospecimen_experiment_1_folder = Folder(
    name=FOLDER_NAME, parent_id=my_project_id
).get()

with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
    tmp.write("First biospecimen data - post-QC analysis results")
    tmp_path = tmp.name
# Store a first version of the file in Synapse
my_file = File(
    path=tmp_path,
    name="biospecimen_data.txt",
    parent_id=biospecimen_experiment_1_folder.id,
)
my_file.store()

# --8<-- [end:retrieve_project_folder_file]

# --8<-- [start:create_activity]
# Create an Activity describing the analysis step that produced this file
analysis_activity = Activity(
    name="Quality Control Analysis",
    description="Initial QC analysis of biospecimen data using the FastQC pipeline.",
    used=[
        UsedURL(
            name="FastQC v0.12.1",
            url="https://github.com/s-andrews/FastQC/releases/tag/v0.12.1",
        ),
        UsedEntity(target_id=my_project_id),
    ],
    executed=[
        UsedURL(
            name="QC Analysis Script",
            url="https://github.com/Sage-Bionetworks/analysis-scripts/blob/v1.0/qc_analysis.py",
        ),
    ],
)

# Attach the activity to the file and store it
my_file.activity = analysis_activity
my_file = my_file.store()

first_version_number = my_file.version_number
print(
    f"Stored file: {my_file.name} (version {first_version_number}) "
    f"with activity: {my_file.activity.name}"
)
# --8<-- [end:create_activity]

# --8<-- [start:add_activity_to_version]
# Step 2: Add a new Activity to a specific version of your File
# Each time you store an updated file, Synapse creates a new version.
# You can track a different activity for each version to capture the
# full history of what was done to produce each version of the file.

# Create a dummy file and upload it as a new version
with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
    tmp.write("Updated biospecimen data - post-QC analysis results")
    tmp_path = tmp.name

updated_file = File(
    path=tmp_path,
    name="biospecimen_data.txt",
    parent_id=biospecimen_experiment_1_folder.id,
)
updated_file.store()
second_version_number = updated_file.version_number

downstream_activity = Activity(
    name="Downstream Analysis",
    description="Downstream analysis of QC-passed biospecimen samples.",
    used=[
        UsedURL(
            name="Seurat v5.0.0",
            url="https://github.com/satijalab/seurat/releases/tag/v5.0.0",
        ),
        UsedEntity(
            target_id=my_file.id,
            target_version_number=first_version_number,
        ),
    ],
    executed=[
        UsedURL(
            name="Downstream Analysis Script",
            url="https://github.com/Sage-Bionetworks/analysis-scripts/blob/v1.0/downstream_analysis.py",
        ),
    ],
)

# Store the activity on the new version using Activity.store()
downstream_activity.store(parent=updated_file)
print(
    f"Stored activity '{downstream_activity.name}' on file "
    f"{updated_file.name} (version {second_version_number})"
)
# --8<-- [end:add_activity_to_version]

# --8<-- [start:print_activities]
# Step 3: Print stored activities on your File
# Retrieve and print the activity on the latest version of the file
current_activity = Activity.from_parent(parent=my_file)
print(f"\nActivity on latest version (v{my_file.version_number}):")
print(f"  Name: {current_activity.name}")
print(f"  Description: {current_activity.description}")
for item in current_activity.used:
    print(f"  Used: {item}")
for item in current_activity.executed:
    print(f"  Executed: {item}")

# Retrieve and print the activity for the first version
first_activity = Activity.from_parent(
    parent=my_file,
    parent_version_number=first_version_number,
)
print(f"\nActivity on version {first_version_number}:")
print(f"  Name: {first_activity.name}")
print(f"  Description: {first_activity.description}")
# --8<-- [end:print_activities]

# --8<-- [start:delete_activity]
# Step 4: Delete an activity
# Deleting an activity disassociates it from the entity and removes it from
# Synapse once it is no longer referenced by any entity.

current_activity.disassociate_from_entity(parent=updated_file)
current_activity.delete(parent=updated_file)
print(
    f"\nDeleted activity from: {updated_file.name} (version {updated_file.version_number})"
)

# Verify the activity was removed
deleted_activity = Activity.from_parent(
    parent=updated_file, parent_version_number=updated_file.version_number
)
print(f"Activity after deletion: {deleted_activity}")
# --8<-- [end:delete_activity]
