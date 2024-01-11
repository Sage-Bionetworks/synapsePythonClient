"""
Here is where you'll find the code for the Annotation tutorial.
"""
# Step 1: Add several annotations to stored files
import os
import synapseclient
from synapseclient import File

syn = synapseclient.login()

# Retrieve the project ID
my_project_id = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Retrieve the folders I want to annotate files in
batch_1_folder_id = syn.findEntityId(
    name="single_cell_RNAseq_batch_1", parent=my_project_id
)

print(f"Batch 1 Folder ID: {batch_1_folder_id}")


# Define the annotations I want to set
annotation_values = {
    "species": "Homo sapiens",
    "dataType": "geneExpression",
    "assay": "SCRNA-seq",
    "fileFormat": "fastq",
}

# Loop over all of the files and set their annotations
for file_batch_1 in syn.getChildren(parent=batch_1_folder_id, includeTypes=["file"]):
    # Grab and print the existing annotations this File may already have
    existing_annotations_for_file = syn.get_annotations(entity=file_batch_1)

    print(
        f"Got the annotations for File: {file_batch_1['name']}, ID: {file_batch_1['id']}, Annotations: {existing_annotations_for_file}"
    )

    # Merge the new annotations with anything existing
    existing_annotations_for_file.update(annotation_values)

    existing_annotations_for_file = syn.set_annotations(
        annotations=existing_annotations_for_file
    )

    print(
        f"Set the annotations for File: {file_batch_1['name']}, ID: {file_batch_1['id']}, Annotations: {existing_annotations_for_file}"
    )

# Step 2: Upload 2 new files and set the annotations at the same time
batch_1_scrnaseq_new_file_1 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR92345678_R1.fastq.gz"
    ),
    parent=batch_1_folder_id,
    annotations=annotation_values,
)
batch_1_scrnaseq_new_file_2 = File(
    path=os.path.expanduser(
        "~/my_ad_project/single_cell_RNAseq_batch_1/SRR92345678_R2.fastq.gz"
    ),
    parent=batch_1_folder_id,
    annotations=annotation_values,
)
batch_1_scrnaseq_new_file_1 = syn.store(obj=batch_1_scrnaseq_new_file_1)
batch_1_scrnaseq_new_file_2 = syn.store(obj=batch_1_scrnaseq_new_file_2)

print(
    f"Stored file: {batch_1_scrnaseq_new_file_1['name']}, ID: {batch_1_scrnaseq_new_file_1['id']}, Annotations: {batch_1_scrnaseq_new_file_1['annotations']}"
)
print(
    f"Stored file: {batch_1_scrnaseq_new_file_2['name']}, ID: {batch_1_scrnaseq_new_file_2['id']}, Annotations: {batch_1_scrnaseq_new_file_2['annotations']}"
)
