"""
Here is where you'll find the code for the downloading data in bulk tutorial.
"""

import os

import synapseclient
from synapseclient.models import Folder, Project

syn = synapseclient.Synapse()
syn.login()

# Create some constants to store the paths to the data
DIRECTORY_TO_SYNC_PROJECT_TO = os.path.expanduser(os.path.join("~", "my_ad_project"))
FOLDER_NAME_TO_SYNC = "biospecimen_experiment_1"
DIRECTORY_TO_SYNC_FOLDER_TO = os.path.join(
    DIRECTORY_TO_SYNC_PROJECT_TO, FOLDER_NAME_TO_SYNC
)

# Step 1: Create an instance of the container I want to sync the data from and sync
project = Project(name="My uniquely named project about Alzheimer's Disease")

# We'll set the `if_collision` to `keep.local` so that we don't overwrite any files
project.sync_from_synapse(path=DIRECTORY_TO_SYNC_PROJECT_TO, if_collision="keep.local")

# Print out the contents of the directory where the data was synced to
# Explore the directory to see the contents have been recursively synced.
print(os.listdir(DIRECTORY_TO_SYNC_PROJECT_TO))

# Step 2: The same as step 1, but for a single folder
folder = Folder(name=FOLDER_NAME_TO_SYNC, parent_id=project.id)

folder.sync_from_synapse(path=DIRECTORY_TO_SYNC_FOLDER_TO, if_collision="keep.local")

print(os.listdir(os.path.expanduser(DIRECTORY_TO_SYNC_FOLDER_TO)))

# Step 3: Loop over all files/folders on the project/folder object instances
for folder_at_root in project.folders:
    print(f"Folder at root: {folder_at_root.name}")

    for file_in_root_folder in folder_at_root.files:
        print(f"File in {folder_at_root.name}: {file_in_root_folder.name}")

    for folder_in_folder in folder_at_root.folders:
        print(f"Folder in {folder_at_root.name}: {folder_in_folder.name}")
        for file_in_folder in folder_in_folder.files:
            print(f"File in {folder_in_folder.name}: {file_in_folder.name}")
