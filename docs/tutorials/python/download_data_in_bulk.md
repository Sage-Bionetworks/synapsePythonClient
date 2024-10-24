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
{!docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py!lines=5-19}
```

#### Next we'll create an instance of the Project we are going to sync
```python
{!docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py!lines=20-22}
```

#### Finally we'll sync the project from synapse to your local machine
```python
{!docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py!lines=23-28}
```

<details class="example">
  <summary>While syncing your project you'll see results like:</summary>
```
Syncing Project (syn53185532:My uniquely named project about Alzheimer's Disease) from Synapse.
Syncing Folder (syn53205630:experiment_notes) from Synapse.
Syncing Folder (syn53205632:notes_2022) from Synapse.
Syncing Folder (syn53205629:single_cell_RNAseq_batch_1) from Synapse.
Syncing Folder (syn53205656:single_cell_RNAseq_batch_2) from Synapse.
Syncing Folder (syn53205631:notes_2023) from Synapse.
Downloading  [####################]100.00%   4.0bytes/4.0bytes (1.8kB/s) fileA.txt Done...
Downloading  [####################]100.00%   3.0bytes/3.0bytes (1.1kB/s) SRR92345678_R1.fastq.gz Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (1.7kB/s) SRR12345678_R1.fastq.gz Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (1.9kB/s) fileC.txt Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (2.7kB/s) fileB.txt Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (2.7kB/s) SRR12345678_R2.fastq.gz Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (2.6kB/s) SRR12345678_R2.fastq.gz Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (1.8kB/s) SRR12345678_R1.fastq.gz Done...
Downloading  [####################]100.00%   3.0bytes/3.0bytes (1.5kB/s) SRR92345678_R2.fastq.gz Done...
Downloading  [####################]100.00%   4.0bytes/4.0bytes (1.6kB/s) fileD.txt Done...
['single_cell_RNAseq_batch_2', 'single_cell_RNAseq_batch_1', 'experiment_notes']
```
</details>

## 2. Download all files/folders for a specific folder within the project

Following the same set of steps let's sync a specific folder

```python
{!docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py!lines=30-36}
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

## 3. Loop over all files/folders on the project/folder object instances
Using `sync_from_synapse` will load into memory the state of all Folders and Files
retrieved from Synapse. This will allow you to loop over the contents of your container.

```python
{!docs/tutorials/python/tutorial_scripts/download_data_in_bulk.py!lines=37-47}
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
