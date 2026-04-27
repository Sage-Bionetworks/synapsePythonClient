[](){ #tutorial-downloading-data-in-bulk }
# Downloading data in bulk
Contained within this tutorial is an experimental interface for working with the
Synapse Python Client. These interfaces are subject to change at any time.
Use at your own risk.


This tutorial will follow a
[Flattened Data Layout](../../explanations/structuring_your_project.md#flattened-data-layout-example).
With a project that has this example layout:
```
.
├── biospecimen_experiment_1
│   ├── fileA.txt
│   └── fileB.txt
├── biospecimen_experiment_2
│   ├── fileC.txt
│   └── fileD.txt
├── single_cell_RNAseq_batch_1
│   ├── SRR12345678_R1.fastq.gz
│   └── SRR12345678_R2.fastq.gz
└── single_cell_RNAseq_batch_2
    ├── SRR12345678_R1.fastq.gz
    └── SRR12345678_R2.fastq.gz
```

## Tutorial Purpose
In this tutorial you will:

1. Download all files/folder from a project
1. Control manifest CSV generation during download
1. Download all files/folders for a specific folder within the project
1. Loop over all files/folders on the project/folder object instances


## Prerequisites
* Make sure that you have completed the following tutorials:
    * [Folder](./folder.md)
    * [File](./file.md)
* This tutorial is setup to download the data to `~/my_ad_project`, make sure that this or
another desired directory exists.


## 1. Download all files/folder from a project

#### First let's set up some constants we'll use in this script
```python
--8<-- "docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py:setup"
```

#### Next we'll create an instance of the Project we are going to sync
```python
--8<-- "docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py:get_project"
```

#### Finally we'll sync the project from synapse to your local machine
```python
--8<-- "docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py:sync_project"
```

<details class="example">
  <summary>While syncing your project you'll see results like:</summary>
```
[syn74583648:My uniquely named project about Alzheimer's Disease]: Syncing Project from Synapse.
[syn74584000:biospecimen_experiment_1]: Syncing Folder from Synapse.
[syn74584007:single_cell_RNAseq_batch_2]: Syncing Folder from Synapse.
[syn74584001:biospecimen_experiment_2]: Syncing Folder from Synapse.
[syn74584006:single_cell_RNAseq_batch_1]: Syncing Folder from Synapse.
[syn74584146]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_1/fileB.png
[syn74584154]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_2/fileD.png
[syn74584155]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_2/fileC.png
[syn74584188]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.png
[syn74584147]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_1/fileA.png
[syn74584206]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_2/SRR12345678_R1.fastq.png
[syn74584189]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_1/SRR12345678_R2.fastq.png
[syn74584207]: Downloaded to <your_DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_2/SRR12345678_R2.fastq.png
Downloading files: 100%|████████████████████| 1.31M/1.31M [00:02<00:00, 606kB/s]
Project(id='syn74583648', name="My uniquely named project about Alzheimer's Disease", files=[], folders=[
  Folder(id='syn74584000', name='biospecimen_experiment_1', parent_id='syn74583648', files=[
    File(id='syn74584147', name='fileA.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_1/fileA.png', parent_id='syn74584000', ...),
    File(id='syn74584146', name='fileB.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_1/fileB.png', parent_id='syn74584000', ...)
  ], folders=[], ...),
  Folder(id='syn74584001', name='biospecimen_experiment_2', parent_id='syn74583648', files=[
    File(id='syn74584155', name='fileC.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_2/fileC.png', parent_id='syn74584001', ...),
    File(id='syn74584154', name='fileD.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/biospecimen_experiment_2/fileD.png', parent_id='syn74584001', ...)
  ], folders=[], ...),
  Folder(id='syn74584006', name='single_cell_RNAseq_batch_1', parent_id='syn74583648', files=[
    File(id='syn74584188', name='SRR12345678_R1.fastq.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.png', parent_id='syn74584006', ...),
    File(id='syn74584189', name='SRR12345678_R2.fastq.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_1/SRR12345678_R2.fastq.png', parent_id='syn74584006', ...)
  ], folders=[], ...),
  Folder(id='syn74584007', name='single_cell_RNAseq_batch_2', parent_id='syn74583648', files=[
    File(id='syn74584206', name='SRR12345678_R1.fastq.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_2/SRR12345678_R1.fastq.png', parent_id='syn74584007', ...),
    File(id='syn74584207', name='SRR12345678_R2.fastq.png', path='<DIRECTORY_TO_SYNC_PROJECT_TO>/single_cell_RNAseq_batch_2/SRR12345678_R2.fastq.png', parent_id='syn74584007', ...)
  ], folders=[], ...)
], ...)
```
</details>

## 2. Control manifest CSV generation during download

By default (`manifest="all"`), `sync_from_synapse` writes a `manifest.csv` into every
synced directory. The manifest.csv is interoperable with sync_to_synapse, the Synapse UI download cart, and `download_list_files`.

Use `manifest="root"` to write a single manifest at the root path, or
`manifest="suppress"` to skip manifest generation entirely.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py:sync_project_with_root_manifest"
```

## 3. Download all files/folders for a specific folder within the project

Following the same set of steps let's sync a specific folder

```python
--8<-- "docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py:sync_folder"
```

<details class="example">
  <summary>While syncing your folder you'll see results like:</summary>
```
Syncing Folder (syn53205630:experiment_notes) from Synapse.
Syncing Folder (syn53205632:notes_2022) from Synapse.
Syncing Folder (syn53205631:notes_2023) from Synapse.
['notes_2022', 'notes_2023']
```
</details>


You'll notice that no files are downloaded. This is because the client will
see that you already have the content within this folder and will not attempt to
download the content again. If you were to use an `if_collision` of `"overwrite.local"`
you would see that when the content on your machine does not match Synapse the file
will be overwritten.

## 4. Loop over all files/folders on the project/folder object instances
Using `sync_from_synapse` will load into memory the state of all Folders and Files
retrieved from Synapse. This will allow you to loop over the contents of your container.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py:loop_over_project_folder"
```

<details class="example">
  <summary>The result of traversing some of your project structure should look like:</summary>
```
Folder at root: experiment_notes
Folder in experiment_notes: notes_2022
File in notes_2022: fileA.txt
File in notes_2022: fileB.txt
Folder in experiment_notes: notes_2023
File in notes_2023: fileC.txt
File in notes_2023: fileD.txt
Folder at root: single_cell_RNAseq_batch_1
File in single_cell_RNAseq_batch_1: SRR12345678_R1.fastq.gz
File in single_cell_RNAseq_batch_1: SRR12345678_R2.fastq.gz
File in single_cell_RNAseq_batch_1: SRR92345678_R1.fastq.gz
File in single_cell_RNAseq_batch_1: SRR92345678_R2.fastq.gz
Folder at root: single_cell_RNAseq_batch_2
File in single_cell_RNAseq_batch_2: SRR12345678_R1.fastq.gz
File in single_cell_RNAseq_batch_2: SRR12345678_R2.fastq.gz
```
</details>

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py!}
```
</details>
