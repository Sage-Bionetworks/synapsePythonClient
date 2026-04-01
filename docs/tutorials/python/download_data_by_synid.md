[](){ #tutorial-downloading-a-file }
# Downloading data by Synapse ID

This tutorial shows how to download any set of files from Synapse using their
Synapse IDs. Rather than syncing an entire project or folder, this approach lets
you target exactly the files you need and download them **concurrently** — even
directing each file to a different local directory.


## Tutorial Purpose
In this tutorial you will:

1. Build a mapping of Synapse IDs to local download directories
1. Download all files concurrently using the async API


## Prerequisites
* Make sure that you have completed the following tutorials:
    * [Folder](./folder.md)
    * [File](./file.md)
* The target directories (`~/temp/subdir1`, etc.) must exist before running the
  script. Create them or replace them with directories of your choice.


## 1. Build a mapping of Synapse IDs to download directories

Create a dictionary that maps each Synapse ID to the local path where that file
should be saved. Files can be directed to different directories as needed.

```python
{!docs/tutorials/python/tutorial_scripts/download_data_by_synid.py!lines=13-30}
```


## 2. Download all files concurrently

Use `File.get_async()` together with `asyncio.gather` to kick off every download
at the same time and wait for them all to finish.

```python
{!docs/tutorials/python/tutorial_scripts/download_data_by_synid.py!lines=31-43}
```

<details class="example">
  <summary>After all downloads finish you'll see output like:</summary>
```
Retrieved 12 files
```
</details>


## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/download_file.py!}
```
</details>

## References used in this tutorial

- [File][synapseclient.models.File]
- [File.get_async][synapseclient.models.File.get_async]
- [syn.login][synapseclient.Synapse.login]
