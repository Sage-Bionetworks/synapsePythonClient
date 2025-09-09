"""
Here is where you'll find the code for the Annotation tutorial.
"""

# Step 1: Add several annotations to stored files
import os

import synapseclient
from synapseclient.models import File, Folder, Project

syn = synapseclient.login()

# Retrieve the project ID
my_project_id = (
    Project(name="My uniquely named project about Alzheimer's Disease").get().id
)

# Retrieve the folders I want to annotate files in
batch_1_folder = Folder(
    name="single_cell_RNAseq_batch_1", parent_id=my_project_id
).get()

print(f"Batch 1 Folder ID: {batch_1_folder.id}")


# Define the annotations I want to set
annotation_values = {
    "species": "Homo sapiens",
    "dataType": "geneExpression",
    "assay": "SCRNA-seq",
    "fileFormat": "fastq",
}

batch_1_folder.sync_from_synapse(download_file=False)
# Loop over all of the files and set their annotations
for file_batch_1 in batch_1_folder.files:
    # Grab and print the existing annotations this File may already have
    existing_annotations_for_file = file_batch_1.annotations

    print(
        f"Got the annotations for File: {file_batch_1.name}, ID: {file_batch_1.id}, Annotations: {existing_annotations_for_file}"
    )

    # Merge the new annotations with anything existing
    existing_annotations_for_file.update(annotation_values)

    file_batch_1.annotations = existing_annotations_for_file
    file_batch_1.store()
    print(
        f"Set the annotations for File: {file_batch_1.name}, ID: {file_batch_1.id}, Annotations: {existing_annotations_for_file}"
    )

# Step 2: Upload 2 new files and set the annotations at the same time
# In order for the following script to work please replace the files with ones that
# already exist on your local machine.
batch_1_scrnaseq_new_file_1 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR92345678_R1.fastq.gz"
    ),
    parent_id=batch_1_folder.id,
    annotations=annotation_values,
)
batch_1_scrnaseq_new_file_2 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR92345678_R2.fastq.gz"
    ),
    parent_id=batch_1_folder.id,
    annotations=annotation_values,
)
batch_1_scrnaseq_new_file_1.store()
batch_1_scrnaseq_new_file_2.store()

print(
    f"Stored file: {batch_1_scrnaseq_new_file_1.name}, ID: {batch_1_scrnaseq_new_file_1.id}, Annotations: {batch_1_scrnaseq_new_file_1.annotations}"
)
print(
    f"Stored file: {batch_1_scrnaseq_new_file_2.name}, ID: {batch_1_scrnaseq_new_file_2.id}, Annotations: {batch_1_scrnaseq_new_file_2.annotations}"
)
