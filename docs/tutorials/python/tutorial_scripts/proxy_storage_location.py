"""Tutorial code for creating a Proxy storage location and uploading a file via ProxyFileHandle."""

# --8<-- [start:setup]
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

my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()
# --8<-- [end:setup]

# --8<-- [start:create_proxy_storage_location]
# Replace with your proxy server URL and shared secret key
MY_PROXY_URL = "https://my-proxy-server.example.com"

# Replace with the path to a local file to register via the proxy
FILE_PATH = "/path/to/your/file.csv"

# --8<-- [start:create_proxy_storage_location]
# Use this when a proxy server controls access to the underlying storage.
my_proxy_folder = Folder(name="my-folder-for-proxy", parent_id=my_project.id)
my_proxy_folder = my_proxy_folder.store()

proxy_storage = StorageLocation(
    storage_type=StorageLocationType.PROXY,
    proxy_url=MY_PROXY_URL,
    secret_key=MY_PROXY_SECRET_KEY,
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
# --8<-- [end:create_proxy_storage_location]

# Register a file in the proxy storage location by creating a ProxyFileHandle via
# the REST API, then associate it with a Synapse File entity.
# --8<-- [start:create_proxy_file_handle]
with open(FILE_PATH, "rb") as f:
    content_md5 = hashlib.md5(f.read(), usedforsecurity=False).hexdigest()
file_size = os.path.getsize(FILE_PATH)
file_name = os.path.basename(FILE_PATH)


async def create_proxy_file_handle() -> str:
    proxy_file_handle = await syn.rest_post_async(
        "/externalFileHandle/proxy",
        body=json.dumps(
            {
                "concreteType": "org.sagebionetworks.repo.model.file.ProxyFileHandle",
                "storageLocationId": proxy_storage.storage_location_id,
                "filePath": file_name,
                "fileName": file_name,
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
# --8<-- [end:create_proxy_file_handle]
# --8<-- [start:associate_proxy_file_handle]
proxy_file = File(
    parent_id=my_proxy_folder.id,
    name=file_name,
    data_file_handle_id=proxy_file_handle_id,
).store()
print(f"Synapse entity: {proxy_file.id}")
# --8<-- [end:associate_proxy_file_handle]
