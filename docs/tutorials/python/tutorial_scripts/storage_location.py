"""
Tutorial code for the Storage Location and project settings.
"""
import asyncio
import hashlib
import json
import os

import synapseclient
from synapseclient.models import (
    File,
    Folder,
    Project,
    StorageLocation,
    StorageLocationType,
)

syn = synapseclient.login()

# Step 1: Retrieve the project
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()

# Step 2: Create an External S3 Storage Location that in the same region as the current storage location
# Replace with your S3 bucket name (must have owner.txt configured)
MY_BUCKET_NAME = "my-synapse-bucket"
MY_BASE_KEY = "synapse-data"

external_s3_storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket=MY_BUCKET_NAME,
    base_key=MY_BASE_KEY,
    description="External S3 storage location",
).store()

print(f"Created storage location: {external_s3_storage_location.storage_location_id}")
print(f"storage location type: {external_s3_storage_location.storage_type}")

# Step 3. Create a Folder with the new storage location
external_s3_folder = Folder(name="my-folder-for-external-s3", parent_id=my_project.id)
external_s3_folder = external_s3_folder.store()

# Set the storage location for the folder
external_s3_folder.set_storage_location(
    storage_location_id=external_s3_storage_location.storage_location_id
)
external_s3_folder_storage_location = external_s3_folder.get_project_setting()
# Verify the storage location is set correctly
assert (
    external_s3_folder_storage_location["locations"][0]
    == external_s3_storage_location.storage_location_id
), "Folder storage location does not match the storage location"

# Step 4: Create a Google Cloud Storage location
MY_GCS_BUCKET = "my-gcs-bucket"
MY_GCS_BASE_KEY = "synapse-data"
gcs_storage = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
    bucket=MY_GCS_BUCKET,
    base_key=MY_GCS_BASE_KEY,
    description="External Google Cloud Storage location",
).store()

print(f"Created GCS storage location: {gcs_storage.storage_location_id}")
print(f"storage location type: {gcs_storage.storage_type}")

gcs_folder = Folder(name="my-folder-for-gcs", parent_id=my_project.id)
gcs_folder = gcs_folder.store()

# Set the storage location for the folder
gcs_folder.set_storage_location(storage_location_id=gcs_storage.storage_location_id)
gcs_folder_storage_location = gcs_folder.get_project_setting()
# Verify the storage location is set correctly
assert (
    gcs_folder_storage_location["locations"][0] == gcs_storage.storage_location_id
), "Folder storage location does not match the storage location"

# Step 5: Create an SFTP storage location
MY_SFTP_URL = "sftp://your-sftp-server.example.com/upload"
sftp_storage = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_SFTP,
    url=MY_SFTP_URL,
    supports_subfolders=True,
    description="External SFTP server",
).store()

print(f"Created SFTP storage location: {sftp_storage.storage_location_id}")
print(f"storage location type: {sftp_storage.storage_type}")

sftp_folder = Folder(name="my-folder-for-sftp", parent_id=my_project.id)
sftp_folder = sftp_folder.store()

# Set the storage location for the folder
sftp_folder.set_storage_location(storage_location_id=sftp_storage.storage_location_id)
sftp_folder_storage_location = sftp_folder.get_project_setting()
# Verify the storage location is set correctly
assert (
    sftp_folder_storage_location["locations"][0] == sftp_storage.storage_location_id
), "Folder storage location does not match the storage location"

# Add a file to the sftp folder, need 'pysftp' package installed.
file = File(path="/path/to/your/file.csv", parent_id=sftp_folder.id)
file = file.store()

# Step 6: Create an HTTPS storage location
# EXTERNAL_HTTPS shares the same underlying API type as EXTERNAL_SFTP but is used
# when the external server is accessed over HTTPS rather than SFTP.
my_https_folder = Folder(name="my-folder-for-https", parent_id=my_project.id)
my_https_folder = my_https_folder.store()

my_https_url = "https://my-https-server.example.com"

https_storage = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_HTTPS,
    url=my_https_url,
    description="External HTTPS server",
).store()

print(f"Created HTTPS storage location: {https_storage.storage_location_id}")
print(f"storage location type: {https_storage.storage_type}")

my_https_folder.set_storage_location(
    storage_location_id=https_storage.storage_location_id
)
my_https_folder_storage_location = my_https_folder.get_project_setting()
assert (
    my_https_folder_storage_location["locations"][0]
    == https_storage.storage_location_id
), "Folder storage location does not match the storage location"

# Note: The Python client does not support uploading files directly to HTTPS
# storage locations. To add files, use the Synapse web UI or REST API directly.

# Step 7: Create an External Object Store storage location
# Use this for S3-compatible stores (e.g. OpenStack Swift) not accessed by Synapse.
MY_OBJECT_STORE_BUCKET = "test-external-object-store"
MY_OBJECT_STORE_ENDPOINT_URL = "https://s3.us-east-1.amazonaws.com"

object_store_storage = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
    bucket=MY_OBJECT_STORE_BUCKET,
    endpoint_url=MY_OBJECT_STORE_ENDPOINT_URL,
    description="External S3-compatible object store",
).store()

print(f"Created object store location: {object_store_storage.storage_location_id}")
print(f"storage location type: {object_store_storage.storage_type}")

# create a folder with the object store storage location
object_store_folder = Folder(name="my-folder-for-object-store", parent_id=my_project.id)
object_store_folder = object_store_folder.store()

object_store_folder.set_storage_location(
    storage_location_id=object_store_storage.storage_location_id
)
object_store_folder_storage_location = object_store_folder.get_project_setting()
assert (
    object_store_folder_storage_location["locations"][0]
    == object_store_storage.storage_location_id
), "Folder storage location does not match the storage location"

# Add a file to the object store folder.
# Requires AWS credentials (access key and secret key) configured in your environment.
file = File(path="/path/to/your/file.csv", parent_id=object_store_folder.id)
file = file.store()

# Step 8: Create a Proxy storage location
# Use this when a proxy server controls access to the underlying storage.
my_proxy_folder = Folder(name="my-folder-for-proxy", parent_id=my_project.id)
my_proxy_folder = my_proxy_folder.store()
MY_PROXY_URL = "https://my-proxy-server.example.com"
proxy_storage = StorageLocation(
    storage_type=StorageLocationType.PROXY,
    proxy_url=MY_PROXY_URL,
    secret_key=my_proxy_secret_key,
    benefactor_id=my_project.id,
    description="Proxy-controlled storage",
).store()

print(f"Created proxy storage location: {proxy_storage.storage_location_id}")
print(f"  Proxy URL: {proxy_storage.proxy_url}")
print(f"  Benefactor ID: {proxy_storage.benefactor_id}")

my_proxy_folder.set_storage_location(
    storage_location_id=proxy_storage.storage_location_id
)
my_proxy_folder_storage_location = my_proxy_folder.get_project_setting()
assert (
    my_proxy_folder_storage_location["locations"][0]
    == proxy_storage.storage_location_id
), "Folder storage location does not match the storage location"

# Add a file to the proxy folder, need a proxy file handle id
# Create ProxyFileHandle via REST API
file_path = "/path/to/your/file.csv"
with open(file_path, "rb") as f:
    content_md5 = hashlib.md5(f.read(), usedforsecurity=False).hexdigest()
file_size = os.path.getsize(file_path)


async def create_proxy_file_handle():
    proxy_file_handle = await syn.rest_post_async(
        "/externalFileHandle/proxy",
        body=json.dumps(
            {
                "concreteType": "org.sagebionetworks.repo.model.file.ProxyFileHandle",
                "storageLocationId": proxy_storage.storage_location_id,
                "filePath": "test.csv",  # relative path served by your proxy
                "fileName": "test.csv",
                "contentType": "text/csv",
                "contentMd5": content_md5,
                "contentSize": file_size,
            }
        ),
        endpoint=syn.fileHandleEndpoint,
    )
    print(f"File handle ID: {proxy_file_handle['id']}")
    return proxy_file_handle["id"]


proxy_file_handle_id = asyncio.run(create_proxy_file_handle())
# Associate the ProxyFileHandle with a Synapse File entity
proxy_file = File(
    parent_id=my_proxy_folder.id,
    name="test.csv",
    data_file_handle_id=proxy_file_handle_id,
).store()
print(f"Synapse entity: {proxy_file.id}")

# Step 9: Retrieve and inspect storage location settings
# Only fields that belong to the storage type are populated after retrieval.
retrieved_storage = StorageLocation(
    storage_location_id=external_s3_storage_location.storage_location_id
).get()
print(f"Retrieved storage location ID: {retrieved_storage.storage_location_id}")
print(f"Storage type: {retrieved_storage.storage_type}")
print(f"Bucket: {retrieved_storage.bucket}")
print(f"Base key: {retrieved_storage.base_key}")


# Step 10: Index and migrate files to the new storage location
#
# WARNING: This will actually migrate files associated with the project/folder.
# Run against a test project first and review the index (MigrationResult) before
# migrating production data.

# Phase 1: Index files for migration
my_migration_folder = Folder(
    name="my-data-migration-folder", parent_id=my_project.id
).get()
index_result = my_migration_folder.index_files_for_migration(
    dest_storage_location_id=external_s3_storage_location.storage_location_id,
    db_path="/path/to/your/migration.db",
    include_table_files=False,  # Set True if you also want table-attached files
)
print(f"Migration index database: {index_result.db_path}")
print(f"Indexed counts by status: {index_result.counts_by_status}")

# Phase 2: Migrate indexed files
migrate_result = my_migration_folder.migrate_indexed_files(
    db_path="/path/to/your/migration.db",
    continue_on_error=True,
    force=True,  # Skip interactive confirmation for tutorial purposes
)

if migrate_result is not None:
    print(f"Migrated counts by status: {migrate_result.counts_by_status}")
else:
    print("Migration was aborted (confirmation declined).")
