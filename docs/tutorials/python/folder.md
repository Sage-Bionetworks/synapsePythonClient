# Folders in Synapse
Similar to Projects, Folders are “containers” that offer an additional way to organize your data. Instead of uploading a bunch of single files into your project, you can create folders to separate your data in a systematic way.

Folders in Synapse always have a “parent”, which could be a project or a folder. You can organize collections of folders and sub-folders, just as you would on your local computer.

[Read more about Folders](../../explanations/domain_models_of_synapse.md#folders)


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

1. Create a new folder
1. Print stored attributes about your folder
1. Create 2 sub-folders


## Prerequisites
* Make sure that you have completed the [Project](./project.md) tutorial.


## 1. Create a new folder

```python
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
```

## 2. Print stored attributes about your folder

```python
my_scrnaseq_batch_1_folder_id = my_scrnaseq_batch_1_folder.id
print(f"My folder ID is: {my_scrnaseq_batch_1_folder_id}")

print(f"The parent ID of my folder is: {my_scrnaseq_batch_1_folder.parentId}")

print(f"I created my folder on: {my_scrnaseq_batch_1_folder.createdOn}")

print(
    f"The ID of the user that created my folder is: {my_scrnaseq_batch_1_folder.createdBy}"
)

print(f"My folder was last modified on: {my_scrnaseq_batch_1_folder.modifiedOn}")
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
My folder ID is: syn53205629
The parent ID of my folder is: syn53185532
I created my folder on: 2023-12-28T20:52:50.193Z
The ID of the user that created my folder is: 3481671
My folder was last modified on: 2023-12-28T20:52:50.193Z
```
</details>


## 3. Create 2 sub-folders

```python
hierarchical_root_folder = Folder(name="experiment_notes", parent=my_project_id)
hierarchical_root_folder = syn.store(obj=hierarchical_root_folder)

folder_notes_2023 = Folder(name="notes_2023", parent=hierarchical_root_folder.id)
folder_notes_2023 = syn.store(obj=folder_notes_2023)

folder_notes_2022 = Folder(name="notes_2022", parent=hierarchical_root_folder.id)
folder_notes_2022 = syn.store(obj=folder_notes_2022)
```

## Results
Now that you have created your folders you'll be able to inspect this on the Files tab of your project in the synapse web UI. It should look similar to:

![folder](./tutorial_screenshots/folder.png)


## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/folder.py!}
```
</details>

## References used in this tutorial

- [Folder][synapseclient.Folder]
- [syn.login][synapseclient.Synapse.login]
- [syn.findEntityId][synapseclient.Synapse.findEntityId]
- [syn.store][synapseclient.Synapse.store]
