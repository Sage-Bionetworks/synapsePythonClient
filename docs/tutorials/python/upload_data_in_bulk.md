# Uploading data in bulk

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

1. Set up constants for your project
1. Create a manifest CSV file to upload data in bulk
1. Upload all of the files for our project
1. Add an annotation to all of our files
1. Add a provenance/activity record to one of our files

!!! tip "Preferred API"
    The recommended way to upload files in bulk is
    [`Project.sync_to_synapse`][synapseclient.models.mixins.StorableContainer.sync_to_synapse]
    (or `Folder.sync_to_synapse`).
    The legacy `synapseutils.syncToSynapse` is deprecated and will be removed in v5.0.0.

!!! warning "Uploading Very Large Files"
    The bulk upload approach using `Project.sync_to_synapse()` is optimized for uploading many files efficiently. However, if you are uploading very large files (>100 GiB each), consider using **sequential uploads with async API** instead.

    For very large file uploads, see the `execute_walk_file_sequential()` function in [uploadBenchmark.py](https://github.com/Sage-Bionetworks/synapsePythonClient/blob/develop/docs/scripts/uploadBenchmark.py#L286) as a reference implementation. This approach uses `asyncio.run(file.store_async())` with the newer async API, which has been optimized for handling very large files efficiently. In benchmarks, this pattern successfully uploaded 45 files of 100 GB each (4.5 TB total) in approximately 20.6 hours.


## Prerequisites

* Make sure that you have completed the following tutorials:
    * [Project](./project.md)
* This tutorial is setup to upload the data from `~/my_ad_project`, make sure that this or
another desired directory exists.
* Pandas is used in this tutorial. Refer to our
[installation guide](../installation.md#pypi) to install it. Feel free to skip this
portion of the tutorial if you do not wish to use Pandas. You may also use external
tools to open and manipulate CSV files.


## 1. Set up constants

First let's set up some constants we'll use in this script
```python
{!docs/tutorials/python/tutorial_scripts/upload_data_in_bulk.py!lines=5-15}
```

## 2. Create a manifest CSV file to upload data in bulk

We use `synapseutils.generate_sync_manifest` to walk our local directory and produce a
manifest that maps each file to the correct parent folder in Synapse (creating folders
as needed). The output is a TSV with a `parent` column, so we convert it to CSV and
rename `parent` to `parentId` for use with `sync_to_synapse`.

!!! note
    `sync_to_synapse` does **not** create folders automatically — every `parentId` in
    the manifest must already exist in Synapse. `generate_sync_manifest` handles this
    by creating the folder hierarchy and writing the correct parent IDs.

```python
{!docs/tutorials/python/tutorial_scripts/upload_data_in_bulk.py!lines=20-34}
```

<details class="example">
  <summary>After this has been run if you inspect the CSV file created you'll see it will look
similar to this:</summary>
```
path,parentId
/home/user_name/my_ad_project/single_cell_RNAseq_batch_2/SRR12345678_R2.fastq.gz,syn60109537
/home/user_name/my_ad_project/single_cell_RNAseq_batch_2/SRR12345678_R1.fastq.gz,syn60109537
/home/user_name/my_ad_project/biospecimen_experiment_2/fileD.txt,syn60109543
/home/user_name/my_ad_project/biospecimen_experiment_2/fileC.txt,syn60109543
/home/user_name/my_ad_project/single_cell_RNAseq_batch_1/SRR12345678_R2.fastq.gz,syn60109534
/home/user_name/my_ad_project/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.gz,syn60109534
/home/user_name/my_ad_project/biospecimen_experiment_1/fileA.txt,syn60109540
/home/user_name/my_ad_project/biospecimen_experiment_1/fileB.txt,syn60109540
```
</details>

## 3. Upload the data in bulk
```python
{!docs/tutorials/python/tutorial_scripts/upload_data_in_bulk.py!lines=36-38}
```


<details class="example">
  <summary>While this is running you'll see output in your console similar to:</summary>
```
Validating manifest: /home/user_name/manifest-for-upload.csv
Validating that all paths exist...
Validating that all files are unique...
Validating that all the files are not empty...
Validating file names...
Validating provenance and parent containers...
About to upload 8 files with a total size of 8 bytes.
Uploading 8 files: 100%|███████████████████| 8.00/8.00 [00:01<00:00, 6.09B/s]
```
</details>



## 4. Add an annotation to our manifest file
At this point in the tutorial we will use pandas to manipulate the CSV manifest. If
you are not comfortable with pandas you may use any tool that can open and manipulate
CSV files such as Excel or Google Sheets.

```python
{!docs/tutorials/python/tutorial_scripts/upload_data_in_bulk.py!lines=40-54}
```

Now that you have uploaded and annotated your files you'll be able to inspect your data
on the Files tab of your project in the synapse web UI. Each file will have a single
annotation that you added in the previous step. In more advanced workflows you'll likely
need to build a more complex manifest file, but this should give you a good starting
point.


## 5. Create an Activity/Provenance
Let's create an [Activity/Provenance](../../explanations/domain_models_of_synapse.md#activityprovenance)
record for one of our files. In otherwords, we will record the steps taken to generate
the file.

In this code we are finding a row in our CSV file and pointing to the file path of
another file within our manifest. By doing this we are creating a relationship between
the two files. This is a simple example of how you can create a provenance record in
Synapse. Additionally we'll link off to a sample URL that describes a process that we
may have executed to generate the file.

```python
{!docs/tutorials/python/tutorial_scripts/upload_data_in_bulk.py!lines=56-83}
```

After running this code we may again inspect the synapse web UI. In this screenshot i've
navigated to the Files tab and selected the file that we added a Provenance record to.


![edit provenance button](./tutorial_screenshots/edit_provenance_button.png)


![edit provenance screen](./tutorial_screenshots/edit_provenance_screen.png)


## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/upload_data_in_bulk.py!}
```
</details>

## References used in this tutorial

- [syn.login][synapseclient.Synapse.login]
- [synapseutils.generate_sync_manifest][]
- [Project.sync_to_synapse][synapseclient.models.mixins.StorableContainer.sync_to_synapse]
- [Manifest CSV format](../../explanations/manifest_csv.md)
- [Activity/Provenance](../../explanations/domain_models_of_synapse.md#activityprovenance)
