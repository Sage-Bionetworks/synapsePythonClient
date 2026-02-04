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

1. Create an external S3 storage location
2. Set up a folder backed by external S3 storage
3. Create an STS-enabled storage location for direct S3 access
4. Use STS credentials with boto3
5. Retrieve and inspect storage location settings

## Prerequisites

* Make sure that you have completed the [Installation](../installation.md) and
  [Authentication](../authentication.md) setup.
* You must have a [Project](./project.md) created and replace the one used in
  this tutorial.
* An AWS S3 bucket properly configured for use with Synapse, including an
  `owner.txt` file. See
  [Custom Storage Locations](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html).
* (Optional) `boto3` installed for STS credential examples.

## Understanding Storage Location Types

Synapse supports several types of storage locations:

- **SYNAPSE_S3**: Synapse-managed S3 storage (default)
- **EXTERNAL_S3**: User-owned Amazon S3 bucket accessed by Synapse
- **EXTERNAL_GOOGLE_CLOUD**: User-owned Google Cloud Storage bucket
- **EXTERNAL_SFTP**: External SFTP server not accessed by Synapse
- **EXTERNAL_OBJECT_STORE**: S3-like bucket (e.g., OpenStack) not accessed by Synapse
- **PROXY**: A proxy server that controls access to storage

## STS-Enabled Storage

STS (AWS Security Token Service) enabled storage locations allow users to get
temporary AWS credentials for direct S3 access. This is useful for:

- Uploading large files directly to S3
- Using AWS tools like the AWS CLI or boto3
- Performing bulk operations on files

## 1. Set up and get project

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=5-12}
```

## 2. Create an external S3 storage location

Create a storage location backed by your own S3 bucket. The bucket must be
properly configured with an `owner.txt` file.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=14-27}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Created storage location: 12345
Type: StorageLocationType.EXTERNAL_S3
Bucket: my-synapse-bucket
```
</details>

## 3. Set up a folder with external S3 storage

The `setup_s3` convenience method handles creating the folder, storage location,
and project settings in a single call.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=29-38}
```

## 4. Create an STS-enabled storage location

STS-enabled storage locations allow you to get temporary AWS credentials for
direct S3 access.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=40-50}
```

## 5. Use STS credentials with boto3

Once you have an STS-enabled folder, you can get temporary credentials to
access the underlying S3 bucket directly.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=52-72}
```

## 6. Retrieve and inspect storage location settings

You can retrieve your storage location settings and inspect their configuration.

```python
{!docs/tutorials/python/tutorial_scripts/storage_location.py!lines=74-86}
```

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
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [Custom Storage Locations Documentation](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)

## See also

- [Storage Location Architecture](../../explanations/storage_location_architecture.md) -
  In-depth architecture diagrams and design documentation
