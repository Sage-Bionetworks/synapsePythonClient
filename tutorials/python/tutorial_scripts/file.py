"""
Here is where you'll find the code for the File tutorial.
"""

# Step 1: Upload several files to Synapse
import os

import synapseclient
import synapseutils
from synapseclient import File

syn = synapseclient.login()

# Retrieve the project ID
my_project_id = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Retrieve the IDs of the folders I want to upload to
batch_1_folder = syn.findEntityId(
    parent=my_project_id, name="single_cell_RNAseq_batch_1"
)
batch_2_folder = syn.findEntityId(
    parent=my_project_id, name="single_cell_RNAseq_batch_2"
)
biospecimen_experiment_1_folder = syn.findEntityId(
    parent=my_project_id, name="biospecimen_experiment_1"
)
biospecimen_experiment_2_folder = syn.findEntityId(
    parent=my_project_id, name="biospecimen_experiment_2"
)

# Create a File object for each file I want to upload
biospecimen_experiment_1_a_2022 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_1/fileA.txt"),
    parent=biospecimen_experiment_1_folder,
)
biospecimen_experiment_1_b_2022 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_1/fileB.txt"),
    parent=biospecimen_experiment_1_folder,
)

biospecimen_experiment_2_c_2023 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_2/fileC.txt"),
    parent=biospecimen_experiment_2_folder,
)
biospecimen_experiment_2_d_2023 = File(
    path=os.path.expanduser("~/my_ad_project/biospecimen_experiment_2/fileD.txt"),
    parent=biospecimen_experiment_2_folder,
)

batch_1_scrnaseq_file_1 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.gz"
    ),
    parent=batch_1_folder,
)
batch_1_scrnaseq_file_2 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR12345678_R2.fastq.gz"
    ),
    parent=batch_1_folder,
)

batch_2_scrnaseq_file_1 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_2/SRR12345678_R1.fastq.gz"
    ),
    parent=batch_2_folder,
)
batch_2_scrnaseq_file_2 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_2/SRR12345678_R2.fastq.gz"
    ),
    parent=batch_2_folder,
)

# Upload each file to Synapse
biospecimen_experiment_1_a_2022 = syn.store(obj=biospecimen_experiment_1_a_2022)
biospecimen_experiment_1_b_2022 = syn.store(obj=biospecimen_experiment_1_b_2022)
biospecimen_experiment_2_c_2023 = syn.store(obj=biospecimen_experiment_2_c_2023)
biospecimen_experiment_2_d_2023 = syn.store(obj=biospecimen_experiment_2_d_2023)
batch_1_scrnaseq_file_1 = syn.store(obj=batch_1_scrnaseq_file_1)
batch_1_scrnaseq_file_2 = syn.store(obj=batch_1_scrnaseq_file_2)
batch_2_scrnaseq_file_1 = syn.store(obj=batch_2_scrnaseq_file_1)
batch_2_scrnaseq_file_2 = syn.store(obj=batch_2_scrnaseq_file_2)

# Step 2: Print stored attributes about your file
batch_1_scrnaseq_file_1_id = batch_1_scrnaseq_file_1.id
print(f"My file ID is: {batch_1_scrnaseq_file_1_id}")

print(f"The parent ID of my file is: {batch_1_scrnaseq_file_1.parentId}")

print(f"I created my file on: {batch_1_scrnaseq_file_1.createdOn}")

print(
    f"The ID of the user that created my file is: {batch_1_scrnaseq_file_1.createdBy}"
)

print(f"My file was last modified on: {batch_1_scrnaseq_file_1.modifiedOn}")

# Step 3: List all Folders and Files within my project
for directory_path, directory_names, file_name in synapseutils.walk(
    syn=syn, synId=my_project_id, includeTypes=["file"]
):
    for directory_name in directory_names:
        print(
            f"Directory ({directory_name[1]}): {directory_path[0]}/{directory_name[0]}"
        )

    for file in file_name:
        print(f"File ({file[1]}): {directory_path[0]}/{file[0]}")
