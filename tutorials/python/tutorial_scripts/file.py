"""
Here is where you'll find the code for the File tutorial.
"""

# Step 1: Upload several files to Synapse
import os

import synapseclient
import synapseutils
from synapseclient.models import File, Folder, Project

syn = synapseclient.login()

# Retrieve the project ID
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()

# Retrieve the IDs of the folders I want to upload to
batch_1_folder = Folder(
    parent_id=my_project.id, name="single_cell_RNAseq_batch_1"
).get()
batch_2_folder = Folder(
    parent_id=my_project.id, name="single_cell_RNAseq_batch_2"
).get()
biospecimen_experiment_1_folder = Folder(
    parent_id=my_project.id, name="biospecimen_experiment_1"
).get()
biospecimen_experiment_2_folder = Folder(
    parent_id=my_project.id, name="biospecimen_experiment_2"
).get()

# Create a File object for each file I want to upload
biospecimen_experiment_1_a_2022 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_1/fileA.txt"),
    parent_id=biospecimen_experiment_1_folder.id,
)
biospecimen_experiment_1_b_2022 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_1/fileB.txt"),
    parent_id=biospecimen_experiment_1_folder.id,
)

biospecimen_experiment_2_c_2023 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_2/fileC.txt"),
    parent_id=biospecimen_experiment_2_folder.id,
)
biospecimen_experiment_2_d_2023 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_2/fileD.txt"),
    parent_id=biospecimen_experiment_2_folder.id,
)

batch_1_scrnaseq_file_1 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.gz"
    ),
    parent_id=batch_1_folder.id,
)
batch_1_scrnaseq_file_2 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR12345678_R2.fastq.gz"
    ),
    parent_id=batch_1_folder.id,
)

batch_2_scrnaseq_file_1 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_2/SRR12345678_R1.fastq.gz"
    ),
    parent_id=batch_2_folder.id,
)
batch_2_scrnaseq_file_2 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_2/SRR12345678_R2.fastq.gz"
    ),
    parent_id=batch_2_folder.id,
)

# Upload each file to Synapse
biospecimen_experiment_1_a_2022.store()
biospecimen_experiment_1_b_2022.store()
biospecimen_experiment_2_c_2023.store()
biospecimen_experiment_2_d_2023.store()
batch_1_scrnaseq_file_1.store()
batch_1_scrnaseq_file_2.store()
batch_2_scrnaseq_file_1.store()
batch_2_scrnaseq_file_2.store()

# Step 2: Print stored attributes about your file
batch_1_scrnaseq_file_1_id = batch_1_scrnaseq_file_1.id
print(f"My file ID is: {batch_1_scrnaseq_file_1_id}")

print(f"The parent ID of my file is: {batch_1_scrnaseq_file_1.parent_id}")

print(f"I created my file on: {batch_1_scrnaseq_file_1.created_on}")

print(
    f"The ID of the user that created my file is: {batch_1_scrnaseq_file_1.created_by}"
)

print(f"My file was last modified on: {batch_1_scrnaseq_file_1.modified_on}")

# Step 3: List all Folders and Files within my project
my_project.sync_from_synapse(download_file=False)
dir_mapping = my_project.map_directory_to_all_contained_files("./")
for directory_name, file_entities in dir_mapping.items():
    print(f"Directory: {directory_name}")
    for file_entity in file_entities:
        print(f"\tFile: {file_entity.name}, ID: {file_entity.id}")
