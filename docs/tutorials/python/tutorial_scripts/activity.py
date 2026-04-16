"""
Here is where you'll find the code for the Activity/Provenance tutorial.
"""

# Step 1: Add a new Activity to your File
# --8<-- [start:retrieve_project_folder_file]
import synapseclient
from synapseclient.models import Activity, File, Folder, Project, UsedEntity, UsedURL

syn = synapseclient.login()

PROJECT_NAME = "My uniquely named project about Alzheimer's Disease"
FOLDER_NAME = "biospecimen_experiment_1"

# Retrieve the project and folder IDs
my_project_id = Project(name=PROJECT_NAME).get().id

biospecimen_experiment_1_folder = Folder(
    name=FOLDER_NAME, parent_id=my_project_id
).get()

# Retrieve an existing file from the project
my_file = File(
    name="fileA.txt",
    parent_id=biospecimen_experiment_1_folder.id,
).get()
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

# Store the activity directly on the file using Activity.store()
downstream_activity.store(parent=my_file)
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
Activity.delete(parent=my_file)
print(f"\nDeleted activity from: {my_file.name} (version {my_file.version_number})")

# Verify the activity was removed
deleted_activity = Activity.from_parent(parent=my_file)
print(f"Activity after deletion: {deleted_activity}")
# --8<-- [end:delete_activity]
