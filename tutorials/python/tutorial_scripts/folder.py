"""
Here is where you'll find the code for the Folder tutorial.
"""

# Step 1: Create a new folder
import synapseclient
from synapseclient.models import Folder, Project

syn = synapseclient.login()

# Retrieve the project ID
my_project = Project(name="My uniquely named project about Alzheimer's Disease")

# Create a Folder object and store it
my_scrnaseq_batch_1_folder = Folder(
    name="single_cell_RNAseq_batch_1", parent_id=my_project.id
)
my_scrnaseq_batch_1_folder.store()

my_scrnaseq_batch_2_folder = Folder(
    name="single_cell_RNAseq_batch_2", parent_id=my_project.id
)
my_scrnaseq_batch_2_folder.store()

biospecimen_experiment_1_folder = Folder(
    name="biospecimen_experiment_1", parent_id=my_project.id
)
biospecimen_experiment_1_folder.store()

biospecimen_experiment_2_folder = Folder(
    name="biospecimen_experiment_2", parent_id=my_project.id
)
biospecimen_experiment_2_folder.store()

# Step 2: Print stored attributes about your folder
my_scrnaseq_batch_1_folder_id = my_scrnaseq_batch_1_folder.id
print(f"My folder ID is: {my_scrnaseq_batch_1_folder_id}")

print(f"The parent ID of my folder is: {my_scrnaseq_batch_1_folder.parent_id}")

print(f"I created my folder on: {my_scrnaseq_batch_1_folder.created_on}")

print(
    f"The ID of the user that created my folder is: {my_scrnaseq_batch_1_folder.created_by}"
)

print(f"My folder was last modified on: {my_scrnaseq_batch_1_folder.modified_on}")

# Step 3: Create 2 sub-folders
hierarchical_root_folder = Folder(name="experiment_notes", parent_id=my_project.id)
hierarchical_root_folder.store()

folder_notes_2023 = Folder(name="notes_2023", parent_id=hierarchical_root_folder.id)
folder_notes_2023.store()

folder_notes_2022 = Folder(name="notes_2022", parent_id=hierarchical_root_folder.id)
folder_notes_2022.store()
