# Files in Synapse
Synapse files can be created by uploading content from your local computer or linking to digital files on the web.

Files in Synapse always have a “parent”, which could be a project or a folder. You can organize collections of files into folders and sub-folders, just as you would on your local computer.

[Read more](../../explanations/domain_models_of_synapse.md#files)


This tutorial will follow a [Flattened Data Layout](../../explanations/structuring_your_project.md#flattened-data-layout-example). With this example layout:
```
.
├── experiment_notes
│   ├── notes_2022
│   │   ├── fileA.txt
│   │   └── fileB.txt
│   └── notes_2023
│       ├── fileC.txt
│       └── fileD.txt
├── single_cell_RNAseq_batch_1
│   ├── SRR12345678_R1.fastq.gz
│   └── SRR12345678_R2.fastq.gz
└── single_cell_RNAseq_batch_2
    ├── SRR12345678_R1.fastq.gz
    └── SRR12345678_R2.fastq.gz
```

## Tutorial Purpose
In this tutorial you will:

1. Upload several files to Synapse
1. Print stored attributes about your files
1. List all Folders and Files within my project


## Prerequisites
* Make sure that you have completed the [Folder](./folder.md) tutorial.
* The tutorial assumes you have a number of files ready to upload. If you do not, create a test or dummy file.


## 1. Upload several files to Synapse

#### First let's retrieve all of the Synapse IDs we are going to use
```python
import synapseclient
import synapseutils
import os
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
experiment_notes_folder = syn.findEntityId(
    parent=my_project_id, name="experiment_notes"
)
note_2022_folder = syn.findEntityId(parent=experiment_notes_folder, name="notes_2022")
note_2023_folder = syn.findEntityId(parent=experiment_notes_folder, name="notes_2023")
```

#### Next let's create all of the File objects to upload content

```python
file_a_2022 = File(
    path=os.path.expanduser("~/my_ad_project/experiment_notes/notes_2022/fileA.txt"),
    parent=note_2022_folder,
)
file_b_2022 = File(
    path=os.path.expanduser("~/my_ad_project/experiment_notes/notes_2022/fileB.txt"),
    parent=note_2022_folder,
)

file_c_2023 = File(
    path=os.path.expanduser("~/my_ad_project/experiment_notes/notes_2023/fileC.txt"),
    parent=note_2023_folder,
)
file_d_2023 = File(
    path=os.path.expanduser("~/my_ad_project/experiment_notes/notes_2023/fileD.txt"),
    parent=note_2023_folder,
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
```

#### Finally we'll store the files in Synapse

```python
file_a_2022 = syn.store(obj=file_a_2022)
file_b_2022 = syn.store(obj=file_b_2022)
file_c_2023 = syn.store(obj=file_c_2023)
file_d_2023 = syn.store(obj=file_d_2023)
batch_1_scrnaseq_file_1 = syn.store(obj=batch_1_scrnaseq_file_1)
batch_1_scrnaseq_file_2 = syn.store(obj=batch_1_scrnaseq_file_2)
batch_2_scrnaseq_file_1 = syn.store(obj=batch_2_scrnaseq_file_1)
batch_2_scrnaseq_file_2 = syn.store(obj=batch_2_scrnaseq_file_2)
```


<details class="example">
  <summary>Each file being uploaded has an upload progress bar:</summary>

```
##################################################
 Uploading file to Synapse storage
##################################################

Uploading [####################]100.00%   2.0bytes/2.0bytes (1.8bytes/s) SRR12345678_R1.fastq.gz Done...
```

</details>


## 2. Print stored attributes about your files

```python
batch_1_scrnaseq_file_1_id = batch_1_scrnaseq_file_1.id
print(f"My file ID is: {batch_1_scrnaseq_file_1_id}")

print(f"The parent ID of my file is: {batch_1_scrnaseq_file_1.parentId}")

print(f"I created my file on: {batch_1_scrnaseq_file_1.createdOn}")

print(
    f"The ID of the user that created my file is: {batch_1_scrnaseq_file_1.createdBy}"
)

print(f"My file was last modified on: {batch_1_scrnaseq_file_1.modifiedOn}")
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
My file ID is: syn53205687
The parent ID of my file is: syn53205629
I created my file on: 2023-12-28T21:55:17.971Z
The ID of the user that created my file is: 3481671
My file was last modified on: 2023-12-28T21:55:17.971Z
```
</details>


## 3. List all Folders and Files within my project

Now that your project has a number of Folders and Files let's explore how we can traverse the content stored within the Project.

```python
for directory_path, directory_names, file_name in synapseutils.walk(
    syn=syn, synId=my_project_id, includeTypes=["file"]
):
    for directory_name in directory_names:
        print(
            f"Directory ({directory_name[1]}): {directory_path[0]}/{directory_name[0]}"
        )

    for file in file_name:
        print(f"File ({file[1]}): {directory_path[0]}/{file[0]}")
```


<details class="example">
  <summary>The result of walking your project structure should look something like:</summary>
```
Directory (syn53205630): My uniquely named project about Alzheimer's Disease/experiment_notes
Directory (syn53205629): My uniquely named project about Alzheimer's Disease/single_cell_RNAseq_batch_1
Directory (syn53205656): My uniquely named project about Alzheimer's Disease/single_cell_RNAseq_batch_2
Directory (syn53205632): My uniquely named project about Alzheimer's Disease/experiment_notes/notes_2022
Directory (syn53205631): My uniquely named project about Alzheimer's Disease/experiment_notes/notes_2023
File (syn53205683): My uniquely named project about Alzheimer's Disease/experiment_notes/notes_2022/fileA.txt
File (syn53205684): My uniquely named project about Alzheimer's Disease/experiment_notes/notes_2022/fileB.txt
File (syn53205685): My uniquely named project about Alzheimer's Disease/experiment_notes/notes_2023/fileC.txt
File (syn53205686): My uniquely named project about Alzheimer's Disease/experiment_notes/notes_2023/fileD.txt
File (syn53205687): My uniquely named project about Alzheimer's Disease/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.gz
File (syn53205688): My uniquely named project about Alzheimer's Disease/single_cell_RNAseq_batch_1/SRR12345678_R2.fastq.gz
File (syn53205689): My uniquely named project about Alzheimer's Disease/single_cell_RNAseq_batch_2/SRR12345678_R1.fastq.gz
File (syn53205690): My uniquely named project about Alzheimer's Disease/single_cell_RNAseq_batch_2/SRR12345678_R2.fastq.gz
```
</details>


## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/file.py!}
```
</details>

## References used in this tutorial

- [File][synapseclient.File]
- [syn.login][synapseclient.Synapse.login]
- [syn.findEntityId][synapseclient.Synapse.findEntityId]
- [syn.store][synapseclient.Synapse.store]
- [synapseutils.walk][]
