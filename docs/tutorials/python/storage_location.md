# Storage Locations in Synapse

Storage locations allow you to configure where files uploaded to Synapse are
stored. By default, files are stored in Synapse's internal S3 storage, but you
can configure projects or folders to use your own AWS S3 buckets, Google Cloud
Storage buckets, or other external storage.

This tutorial demonstrates how to use the Python client to manage storage
locations using the new object-oriented models.

[Read more about Custom Storage Locations](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)

## Tutorial Purpose
In this tutorial you will:

1. Create an external S3 storage location and assign it to a folder
2. Create a Google Cloud Storage location and assign it to a folder
3. Create an SFTP storage location and assign it to a folder
4. Create an HTTPS storage location and assign it to a folder
5. Create an External Object Store location and assign it to a folder
6. Create a Proxy storage location, register a proxy file handle, and assign it to a folder
7. Retrieve and inspect storage location settings
8. Update a storage location (create a replacement and reassign)
9. Index and migrate files to a new storage location

## Prerequisites

* Make sure that you have completed the [Installation](../installation.md) and
  [Authentication](../authentication.md) setup.
* You must have a [Project](./project.md) created and replace the one used in
  this tutorial.
* An AWS S3 bucket properly configured for use with Synapse, including an
  `owner.txt` file. See
  [Custom Storage Locations](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html).
* (Optional) `boto3` installed for STS credential examples.
* For SFTP: `pysftp` installed (`pip install "synapseclient[pysftp]"`).
* For Object Store: AWS credentials configured in your environment.
* For Proxy: a running proxy server and its shared secret key.

## Understanding Storage Location Types

Synapse supports several types of storage locations:

- **SYNAPSE_S3**: Synapse-managed S3 storage (default)
- **EXTERNAL_S3**: User-owned AWS S3 bucket, accessed by Synapse on
  your behalf. Synapse transfers the data for uploads and downloads. Requires
  an `owner.txt` file in the bucket to verify ownership.
- **EXTERNAL_GOOGLE_CLOUD**: User-owned Google Cloud Storage bucket
- **EXTERNAL_SFTP**: External SFTP server
- **EXTERNAL_HTTPS**: External HTTPS server (uploading via client is **not**
  supported right now.)
- **EXTERNAL_OBJECT_STORE**: An S3-compatible store (e.g., MinIO, OpenStack
  Swift) that Synapse does **not** access. The client transfers data directly
  to the object store using credentials configured in your environment; Synapse
  only stores the file metadata.
- **PROXY**: A proxy server that controls access to the underlying storage

## Storage Location Settings

Each storage type exposes a different set of configuration fields on
`StorageLocation`. When you retrieve a stored location, only the fields
relevant to its type are populated:

| Type | Key fields |
|------|-----------|
| `SYNAPSE_S3` | `base_key`, `sts_enabled` |
| `EXTERNAL_S3` | `bucket`, `base_key`, `sts_enabled`, `endpoint_url` |
| `EXTERNAL_GOOGLE_CLOUD` | `bucket`, `base_key` |
| `EXTERNAL_SFTP` / `EXTERNAL_HTTPS` | `url`, `supports_subfolders` |
| `EXTERNAL_OBJECT_STORE` | `bucket`, `endpoint_url` |
| `PROXY` | `proxy_url`, `secret_key`, `benefactor_id` |

Common attributes are:  `concrete_type`, `storage_location_id`, `storage_type`, `upload_type`, `banner`, `description`, `etag`, `created_on`, `created_by`

## Data Migration Between Storage Locations

Files in a project or folder can be migrated from one storage location to another using
`index_files_for_migration` followed by `migrate_indexed_files`. Migration is
currently supported **only** between S3 storage locations (both Synapse-managed
`SYNAPSE_S3` and external `EXTERNAL_S3`) that reside in the **same AWS
region**.

Migration is a two-phase process:

1. **Index** — scan the project/folder and record every file that needs to move into a
   local SQLite database.
2. **Migrate** — read the index database and move each file to the destination
   storage location.

Separating the phases lets you review what will be migrated before committing
to the move.

## 1. Set up and get project

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=4-18}
```

## 2. Create an external S3 storage location

Create a storage location backed by your own S3 bucket. The bucket must be
properly configured with an `owner.txt` file. Synapse will transfer data
directly to and from this bucket on the user's behalf.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=20-33}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Created storage location: 12345
storage location type: StorageLocationType.EXTERNAL_S3
```
</details>

## 3. Set up a folder with external S3 storage

Create a folder and assign it the S3 storage location. All files uploaded into
this folder will be stored in your S3 bucket.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=39-51}
```

## 4. Create a Google Cloud Storage location

Create a storage location backed by a Google Cloud Storage bucket and assign it
to a folder.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=54-75}
```

## 5. Create an SFTP storage location

SFTP storage locations point to an external SFTP server, where files are stored outside of Synapse. Synapse only manages the metadata and does not handle the file transfer itself. This setup requires the pysftp package, and files must be uploaded separately through the **client** once configured.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=78-102}
```

## 6. Create an HTTPS storage location

`EXTERNAL_HTTPS` uses the same underlying API type as `EXTERNAL_SFTP` but is
used when the external server is accessed over HTTPS. Note that the Python
client does NOT support uploading files to HTTPS storage locations directly yet. To add files, use the Synapse REST API directly.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=107-128}
```

## 7. Create an External Object Store storage location

Use `EXTERNAL_OBJECT_STORE` for S3-compatible stores that are not directly
accessed by Synapse. Unlike `EXTERNAL_S3`, the Python client transfers data
directly to the object store using locally configured AWS credentials —
Synapse is never involved in the data transfer, only in storing the metadata.

You can add a profile to work with s3 in `~/.synapseConfig`

Add a section matching your endpoint+bucket URL:
```
[https://s3.us-east-1.amazonaws.com/test-external-object-store]
profile_name = my-s3-profile
```
Then ensure my-s3-profile exists in `~/.aws/config` with valid keys:

```
[my-s3-profile]
aws_access_key_id = ...
aws_secret_access_key = ...
```

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=135-164}
```

## 8. Create a Proxy storage location

Proxy storage locations delegate file access to a proxy server that controls
authentication and access to the underlying storage. Files are registered by
creating a `ProxyFileHandle` via the REST API. Then, files can be uploaded via store function with data_file_handle_id.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=168-226}
```

## 9. Retrieve and inspect storage location settings

You can retrieve a storage location by ID. Only fields relevant to the storage
type are populated.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=230-236}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Retrieved storage location ID: 12345
Storage type: StorageLocationType.EXTERNAL_S3
Bucket: my-synapse-bucket
Base key: synapse-data
```
</details>

## 10. Update a storage location

Storage locations are immutable — individual fields cannot be edited after
creation. To "update" a storage location, create a new one with the desired
settings and reassign it to the folder or project.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=245-280}
```

## 11. Index and migrate files to a new storage location

> **Warning:** This will migrate files associated with the folder. Run against a
> test project first and review the index result before migrating production data.

Phase 1. indexes all files that need to move into a local SQLite database. This will return a MigrationResults object. You can use the `as_csv` to check the details of indexing status.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=288-298}
```

Index results can be checked in the index results csv
![indexresults](./tutorial_screenshots/index_results.png)

Phase 2. reads that database and performs the actual migration. This will return a MigrationResults object. You can use the `as_csv` to check the details of migration status and errors if any.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=300-310}
```

Currently, detailed Traceback is saved in the exception columns of the csv.
![migrationresults](./tutorial_screenshots/migration_results.png)

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!}
```
</details>

## References used in this tutorial

- [StorageLocation][synapseclient.models.StorageLocation]
- [StorageLocationType][synapseclient.models.StorageLocationType]
- [Folder][synapseclient.models.Folder]
- [File][synapseclient.models.File]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [Custom Storage Locations Documentation](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)

## See also

- [Storage Location Architecture](../../explanations/storage_location_architecture.md) -
  In-depth architecture diagrams and design documentation
