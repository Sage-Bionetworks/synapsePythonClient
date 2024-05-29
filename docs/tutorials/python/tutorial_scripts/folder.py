"""
Here is where you'll find the code for the Folder tutorial.
"""

# Step 1: Create a new folder
import synapseclient
from synapseclient import Folder

syn = synapseclient.login()

# Retrieve the project ID
my_project_id = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Create a Folder object and store it
my_scrnaseq_batch_1_folder = Folder(
    name="single_cell_RNAseq_batch_1", parent=my_project_id
)
my_scrnaseq_batch_1_folder = syn.store(obj=my_scrnaseq_batch_1_folder)

my_scrnaseq_batch_2_folder = Folder(
    name="single_cell_RNAseq_batch_2", parent=my_project_id
)
my_scrnaseq_batch_2_folder = syn.store(obj=my_scrnaseq_batch_2_folder)

biospecimen_experiment_1_folder = Folder(
    name="biospecimen_experiment_1", parent=my_project_id
)
biospecimen_experiment_1_folder = syn.store(obj=biospecimen_experiment_1_folder)

biospecimen_experiment_2_folder = Folder(
    name="biospecimen_experiment_2", parent=my_project_id
)
biospecimen_experiment_2_folder = syn.store(obj=biospecimen_experiment_2_folder)

# Step 2: Print stored attributes about your folder
my_scrnaseq_batch_1_folder_id = my_scrnaseq_batch_1_folder.id
print(f"My folder ID is: {my_scrnaseq_batch_1_folder_id}")

print(f"The parent ID of my folder is: {my_scrnaseq_batch_1_folder.parentId}")

print(f"I created my folder on: {my_scrnaseq_batch_1_folder.createdOn}")

print(
    f"The ID of the user that created my folder is: {my_scrnaseq_batch_1_folder.createdBy}"
)

print(f"My folder was last modified on: {my_scrnaseq_batch_1_folder.modifiedOn}")

# Step 3: Create 2 sub-folders
hierarchical_root_folder = Folder(name="experiment_notes", parent=my_project_id)
hierarchical_root_folder = syn.store(obj=hierarchical_root_folder)

folder_notes_2023 = Folder(name="notes_2023", parent=hierarchical_root_folder.id)
folder_notes_2023 = syn.store(obj=folder_notes_2023)

folder_notes_2022 = Folder(name="notes_2022", parent=hierarchical_root_folder.id)
folder_notes_2022 = syn.store(obj=folder_notes_2022)
