"""
Here is where you'll find the code for the Storage Location tutorial.
"""

# Step 1: Create an External S3 Storage Location
import synapseclient
from synapseclient.models import Project, StorageLocation, StorageLocationType

syn = synapseclient.login()

# Retrieve the project
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()

# Step 2: Create an External S3 Storage Location
# Replace with your S3 bucket name (must have owner.txt configured)
MY_BUCKET_NAME = "my-synapse-bucket"
MY_BASE_KEY = "synapse-data"

storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket=MY_BUCKET_NAME,
    base_key=MY_BASE_KEY,
).store()

print(f"Created storage location: {storage_location.storage_location_id}")
print(f"Type: {storage_location.storage_type}")
print(f"Bucket: {storage_location.bucket}")

# Step 3: Set up a folder with external S3 storage
folder, storage = StorageLocation.setup_s3(
    folder_name="my-external-storage-folder",
    parent=my_project.id,
    bucket_name=MY_BUCKET_NAME,
    base_key="folder-specific-prefix",
)

print(f"Created folder: {folder.id}")
print(f"Storage location ID: {storage.storage_location_id}")

# Step 4: Create an STS-enabled storage location
sts_folder, sts_storage = StorageLocation.setup_s3(
    folder_name="my-sts-enabled-folder",
    parent=my_project.id,
    bucket_name=MY_BUCKET_NAME,
    base_key="sts-data",
    sts_enabled=True,
)

print(f"Created STS-enabled folder: {sts_folder.id}")
print(f"STS enabled: {sts_storage.sts_enabled}")

# Step 5: Use STS credentials with boto3
credentials = sts_folder.get_sts_storage_token(
    permission="read_write",
    output_format="boto",
)

print(f"AWS Access Key ID: {credentials['aws_access_key_id'][:10]}...")
print("Credentials expire: check 'expiration' in json format")

try:
    import boto3

    s3_client = boto3.client("s3", **credentials)
    response = s3_client.list_objects_v2(
        Bucket=MY_BUCKET_NAME,
        Prefix="sts-data/",
        MaxKeys=10,
    )
    print(f"Found {response.get('KeyCount', 0)} objects")
except ImportError:
    print("boto3 not installed, skipping S3 client example")

# Step 6: Retrieve and inspect storage location settings
retrieved_storage = StorageLocation(
    storage_location_id=storage_location.storage_location_id
).get()

print("Retrieved storage location:")
print(f"  ID: {retrieved_storage.storage_location_id}")
print(f"  Type: {retrieved_storage.storage_type}")
print(f"  Bucket: {retrieved_storage.bucket}")
print(f"  Base Key: {retrieved_storage.base_key}")
print(f"  STS Enabled: {retrieved_storage.sts_enabled}")
print(f"  Created By: {retrieved_storage.created_by}")
print(f"  Created On: {retrieved_storage.created_on}")
