"""
Tutorial code for the Storage Location and project settings.
"""

# --8<-- [start:setup]
import synapseclient
from synapseclient.models import Folder, Project, StorageLocation, StorageLocationType

syn = synapseclient.login()

# Step 1: Retrieve the project
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()
# --8<-- [end:setup]

# Step 2: Create an External S3 Storage Location that in the same region as the current storage location
# Replace with your S3 bucket name (must have owner.txt configured)
# --8<-- [start:create_s3_storage_location]
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
# --8<-- [end:create_s3_storage_location]

# Step 3. Create a Folder with the new storage location
# --8<-- [start:create_folder_with_s3_storage_location]
external_s3_folder = Folder(name="my-folder-for-external-s3", parent_id=my_project.id)
external_s3_folder = external_s3_folder.store()

# Set the storage location for the folder
external_s3_folder.set_storage_location(
    storage_location_id=external_s3_storage_location.storage_location_id
)
# --8<-- [end:create_folder_with_s3_storage_location]

# Step 4: Create a Google Cloud Storage location
# --8<-- [start:create_gcs_storage_location]
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
# --8<-- [end:create_gcs_storage_location]

# Step 5: Create an SFTP storage location
# --8<-- [start:create_sftp_storage_location]
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
# --8<-- [end:create_sftp_storage_location]

# Step 6: Create an HTTPS storage location
# EXTERNAL_HTTPS shares the same underlying API type as EXTERNAL_SFTP but is used
# when the external server is accessed over HTTPS rather than SFTP.
# --8<-- [start:create_https_storage_location]
MY_HTTPS_URL = "https://my-https-server.example.com"

https_storage = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_HTTPS,
    url=MY_HTTPS_URL,
    description="External HTTPS server",
).store()

print(f"Created HTTPS storage location: {https_storage.storage_location_id}")
print(f"storage location type: {https_storage.storage_type}")

my_https_folder = Folder(name="my-folder-for-https", parent_id=my_project.id)
my_https_folder = my_https_folder.store()

# Set the storage location for the folder
my_https_folder.set_storage_location(
    storage_location_id=https_storage.storage_location_id
)
# --8<-- [end:create_https_storage_location]

# Note: The Python client does not support uploading files directly to HTTPS
# storage locations. To add files, use the Synapse web UI or REST API directly.

# Step 7: Create an External Object Store storage location
# Use this for S3-compatible stores not accessed by Synapse.
# --8<-- [start:create_object_store_storage_location]
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

# Set the storage location for the folder
object_store_folder.set_storage_location(
    storage_location_id=object_store_storage.storage_location_id
)
# --8<-- [end:create_object_store_storage_location]

# Step 8: Create a Proxy storage location
# Use this when a proxy server controls access to the underlying storage.
# --8<-- [start:create_proxy_storage_location]

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

my_proxy_folder = Folder(name="my-folder-for-proxy", parent_id=my_project.id)
my_proxy_folder = my_proxy_folder.store()

# Set the storage location for the folder
my_proxy_folder.set_storage_location(
    storage_location_id=proxy_storage.storage_location_id
)
# --8<-- [end:create_proxy_storage_location]

# Step 9: Retrieve and inspect storage location settings
# Only fields that belong to the storage type are populated after retrieval.
# --8<-- [start:retrieve_storage_location]
retrieved_storage = StorageLocation(
    storage_location_id=external_s3_storage_location.storage_location_id
).get()
print(f"Retrieved storage location ID: {retrieved_storage.storage_location_id}")
print(f"Storage type: {retrieved_storage.storage_type}")
print(f"Bucket: {retrieved_storage.bucket}")
print(f"Base key: {retrieved_storage.base_key}")
# --8<-- [end:retrieve_storage_location]

# Step 10: Update a storage location

# Storage locations are immutable in Synapse — individual fields cannot be edited
# after creation. To "update" a storage location, create a new one with the desired
# settings and reassign it to the folder or project.
# --8<-- [start:update_storage_location]
# Example: change the base key of the External S3 storage location used by
# external_s3_folder from MY_BASE_KEY to "synapse-data-v2".

updated_s3_storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket=MY_BUCKET_NAME,
    base_key="synapse-data-v2",
    description="External S3 storage location (updated base key)",
).store()

print(f"New storage location ID: {updated_s3_storage_location.storage_location_id}")

# Reassign the folder to point at the new storage location
external_s3_folder.set_storage_location(
    storage_location_id=updated_s3_storage_location.storage_location_id
)
updated_folder_setting = external_s3_folder.get_project_setting()

print(
    f"Folder now uses the updated storage location: {updated_s3_storage_location.storage_location_id}"
)

# Step 10b: Partial update — add a storage location without removing existing ones
#
# `set_storage_location` is a destructive replacement. To append a new location
# while keeping the ones already configured, read the current ProjectSetting,
# append to its `locations` list, and call store() on the setting directly.

setting = external_s3_folder.get_project_setting()
if setting is not None:
    setting.locations.append(gcs_storage.storage_location_id)
    setting.store()
    print(f"Updated locations after partial update: {setting.locations}")
# --8<-- [end:update_storage_location]
