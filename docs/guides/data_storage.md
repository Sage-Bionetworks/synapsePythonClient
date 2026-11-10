# Data Storage

## S3 Storage Features

Synapse can use a variety of storage mechanisms to store content, however the most common storage solution is AWS S3. This article illustrates some special features that can be used with S3 storage and how they interact with the Python client. In particular it covers:

1. Linking External storage locations to new/existing projects or folders
2. Migration of existing projects or folders to new external storage locations
3. Creating STS enabled storage locations
4. Resuming uploads that stopped
5. Using SFTP

## External storage locations

Synapse projects or folders can be configured to use custom implementations for their underlying data storage. More information on this feature can be found [here](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html). The most common implementation of this is to configure a folder to store data in a user controlled AWS S3 bucket rather than Synapse's default internal S3 storage.

### Creating a new folder backed by a user specified S3 bucket

The following illustrates creating a new folder backed by a user specified S3 bucket. Note: An existing folder also works.

If you are changing the storage location of an existing folder to a user specified S3 bucket none of the files will be migrated. In order to migrate the files to the new storage location see the section [Migrating programmatically](#migrating-programmatically). When you change the storage location for a folder only NEW files uploaded to the folder are uploaded to the user specific S3 bucket.

1. Ensure that the bucket is [properly configured](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html#CustomStorageLocations-SettingUpanExternalAWSS3Bucket).

2. Create a folder and configure it to use external S3 storage:

```python
from synapseclient.models import Folder, StorageLocation, StorageLocationType

# create a new folder to use with external S3 storage
folder = Folder(name=folder_name, parent_id=parent).store()
# You may also retrieve an existing folder like:
# folder = Folder(id="syn123").get()

# create the external S3 storage location
storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket='my-external-synapse-bucket',
    base_key='path/within/bucket',
).store()

# assign the storage location to the folder
folder.set_storage_location(storage_location_id=storage_location.storage_location_id)

# if needed the unique storage location identifier can be obtained e.g.
storage_location_id = storage_location.storage_location_id
```

### Creating a new project backed by a user specified S3 bucket
The following illustrates creating a new project backed by a user specified S3 bucket. Note: An existing project also works.

If you are changing the storage location of an existing project to a user specified S3 bucket none of the files will be migrated. In order to migrate the files to the new storage location see the documentation further down in this article labeled 'Migrating programmatically'. When you change the storage location for a project only NEW files uploaded to the project are uploaded to the user specific S3 bucket.

1. Ensure that the bucket is [properly configured](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html#CustomStorageLocations-SettingUpanExternalAWSS3Bucket).

2. Create a project and configure it to use external S3 storage:

```python
from synapseclient.models import Project, StorageLocation, StorageLocationType

# create a new, or retrieve an existing project to use with external S3 storage
project = Project(name="my_project_name").store()

# create the external S3 storage location
storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket='my-external-synapse-bucket',
    base_key='path/within/bucket',
).store()

# assign the storage location to the project
project.set_storage_location(storage_location_id=storage_location.storage_location_id)

# if needed the unique storage location identifier can be obtained e.g.
storage_location_id = storage_location.storage_location_id
```

Once an external S3 storage folder exists, you can interact with it as you would any other folder using Synapse tools. If you wish to add an object that is stored within the bucket to Synapse you can do that by adding a file handle for that object using the Python client and then storing the file to that handle.

```python
import boto3
from synapseclient import Synapse
from synapseclient.models import File

syn = Synapse()
syn.login()

parent_synapse_folder_id = 'syn123'
local_file_path = '/path/to/local/file'
bucket = 'my-external-synapse-bucket'
s3_key = 'path/within/bucket/file'

# in this example we use boto to create a file independently of Synapse
s3_client = boto3.client('s3')
s3_client.upload_file(
    Filename=local_file_path,
    Bucket=bucket,
    Key=s3_key
)

# Create an external S3 file handle for that object. There is not yet an
# object-oriented equivalent for this call, so the Synapse client method is used.
file_handle = syn.create_external_s3_file_handle(
    bucket,
    s3_key,
    local_file_path,
    parent=parent_synapse_folder_id,
)

# now store a File that points at the existing file handle
file_entity = File(
    parent_id=parent_synapse_folder_id,
    data_file_handle_id=file_handle['id'],
).store()
```

## Storage location migration

There are circumstances where it can be useful to move the files underlying Synapse entities from one storage location to another without impacting the structure or identifiers of the Synapse entities themselves. An example scenario is needing to use STS features with an existing Synapse Project that was not initially configured with an STS enabled [custom storage location](#sts-storage-locations).

The Synapse client has utilities for migrating entities to a new storage location without having to download the content locally and re-uploading it which can be slow, and may alter the meta data associated with the entities in undesirable ways.

During the migration it is recommended that uploads and downloads are blocked to prevent possible conflicts or race conditions. This can be done by setting permissions to `Can view` for the project or folder being migrated. After the migration is complete set the permissions back to their original values.

Expected time to migrate data is around 13 minutes per 100Gb as of 11/21/2023.

### Migrating programmatically

Migrating a Synapse project or folder programmatically is a two step process.

First ensure that you know the id of the storage location you want to migrate to. More info on storage locations can be found above and [here](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html).

Once the storage location is known, the first step to migrate the project or folder is to create a migratable index of its contents using the `index_files_for_migration` method on the [Folder][folder-reference-sync] or [Project][project-reference-sync] model, e.g.

When specifying the `.db` file for the migratable indexes you need to specify a `.db` file that does not already exist for another synapse project or folder on disk. It is the best practice to specify a unique name for the file by including the synapse id in the name of the file, or other unique identifier.

```python
from synapseclient.models import Folder

# a Synapse entity whose contents need to be migrated, e.g. a Project or Folder
entity = Folder(id='syn123').get()
dest_storage_location_id = 12345  # the id of the destination storage location being migrated to

# a path on disk where this utility can create a sqlite database to store its index.
# nothing needs to exist at this path, but it must be a valid path on a volume with sufficient
# disk space to store a meta data listing of all the contents in the indexed entity.
# a rough rule of thumb is 100kB per 1000 entities indexed.
db_path = '/tmp/foo/syn123_bar.db'

result = entity.index_files_for_migration(
    dest_storage_location_id,
    db_path,

    # optional args, see the method documentation for a description of these parameters
    source_storage_location_ids=[54321, 98765],
    file_version_strategy='new',
    include_table_files=False,
    continue_on_error=True,
)
```

If called on a container (e.g. a Project or Folder) the *index_files_for_migration* method will recursively index all of the children of that container (including its subfolders). Once the entity has been indexed you can optionally programmatically inspect the contents of the index via the returned `MigrationResult` object (e.g. `index_result.counts_by_status`) or write it to a csv file with `index_result.as_csv(...)` in order to manually inspect it.

The next step to trigger the migration from the indexed files is using the `migrate_indexed_files` method on the same model, e.g.

```python
result = entity.migrate_indexed_files(
    db_path,

    # optional args, see the method documentation for a description of these parameters
    create_table_snapshots=True,
    continue_on_error=False,
    force=True,
)
```

The result can be again be inspected as above to see the results of the migration.

Note that above the *force* parameter is necessary if running from a non-interactive shell. Proceeding with a migration requires confirmation in the form of user prompt. If running programmatically this parameter instead confirms your intention to proceed with the migration.

#### Putting all the migration pieces together

```python
import os

import synapseclient
from synapseclient.models import Folder, StorageLocation, StorageLocationType

my_synapse_project_or_folder_to_migrate = "syn123"

external_bucket_name = "my-external-synapse-bucket"
external_bucket_base_key = "path/within/bucket/"

my_user_id = "1234"

# a path on disk where this utility can create a sqlite database to store its index.
# nothing needs to exist at this path, but it must be a valid path on a volume with sufficient
# disk space to store a meta data listing of all the contents in the indexed entity.
# a rough rule of thumb is 100kB per 1000 entities indexed.
db_path = os.path.expanduser(
    f"~/synapseMigration/{my_synapse_project_or_folder_to_migrate}_my.db"
)

syn = synapseclient.Synapse()

# Log-in with ~.synapseConfig `authToken`
syn.login()

# The project or folder I want to migrate everything to this S3 storage location.
# Use Project(id=...).get() instead if you are migrating a project.
project_or_folder = Folder(id=my_synapse_project_or_folder_to_migrate).get()

# Create the destination external S3 storage location and assign it
storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket=external_bucket_name,
    base_key=external_bucket_base_key,
).store()
project_or_folder.set_storage_location(
    storage_location_id=storage_location.storage_location_id
)

# The id of the destination storage location being migrated to
storage_location_id = storage_location.storage_location_id
print(
    f"Indexing: {project_or_folder.id} for migration to storage_id: {storage_location_id} at: {db_path}"
)

try:
    index_result = project_or_folder.index_files_for_migration(
        storage_location_id,
        db_path,
        file_version_strategy="all",
    )

    print(f"Indexing result: {index_result.counts_by_status}")

    print("Migrating files...")

    migrate_result = project_or_folder.migrate_indexed_files(
        db_path,
        force=True,
    )

    print(f"Migration result: {migrate_result.counts_by_status}")
    syn.sendMessage(
        userIds=[my_user_id],
        messageSubject=f"Migration success for {project_or_folder.id}",
        messageBody=f"Migration result: {migrate_result.counts_by_status}",
    )
except Exception as e:
    syn.sendMessage(
        userIds=[my_user_id],
        messageSubject=f"Migration failed for {project_or_folder.id}",
        messageBody=f"Migration failed with error: {e}",
    )
```

The result of running this should look like

```python
Indexing: syn123 for migration to storage_id: 11111 at: /home/user/synapseMigration/syn123_my.db
Indexing result: {'INDEXED': 100, 'MIGRATED': 0, 'ALREADY_MIGRATED': 0, 'ERRORED': 0}
Migrating files...
Migration result: {'INDEXED': 0, 'MIGRATED': 100, 'ALREADY_MIGRATED': 0, 'ERRORED': 0}
```

### Migrating from the command line

Synapse entities can also be migrated from the command line. The options are similar to above.
Whereas migrating programatically involves two separate function calls, from the command line
there is a single `migrate` command with the *dryRun* argument providing the option
to generate the index only without proceeding onto the migration.

Note that as above, confirmation is required before a migration starts. As above, this must either be
in the form of confirming via a prompt if running the command from an interactive shell, or using the *force*
command.

The optional *csv_log_path* argument will output the results to a csv file for record keeping, and is recommended.

```bash
synapse migrate syn123 54321 /tmp/migrate.db --csv_log_path /tmp/migrate.csv
```

Sample output:

```bash
Indexing Project syn123
Indexing file entity syn888
Indexing file entity syn999
Indexed 2 items, 2 needing migration, 0 already stored in destination storage location (54321). Encountered 0 errors.
21 items for migration to 54321. Proceed? (y/n)? y
Creating new version for file entity syn888
Creating new version for file entity syn999
Completed migration of syn123. 2 files migrated. 0 errors encountered
Writing csv log to /tmp/migrate.csv
```

## STS Storage Locations

Create an STS enabled folder to use [AWS Security Token Service](https://help.synapse.org/docs/Compute-Directly-on-Data-in-Synapse-or-S3.2048426057.html#ComputeDirectlyonDatainSynapseorS3-Synapse-ManagedSTSStorageLocations) credentials with S3 storage locations. These credentials can be scoped to access individual Synapse files or folders and can be used with external S3 tools such as the awscli and the boto3 library separately from Synapse to read and write files to and from Synapse storage. At this time read and write capabilities are supported for external storage locations, while default Synapse storage is limited to read only. Please read the linked documentation for a complete understanding of the capabilities and restrictions of STS enabled folders.

### Creating an STS enabled folder

Creating an STS enabled folder is similar to creating an external storage folder as described above, but this time passing an additional **sts_enabled=True** keyword parameter. The **bucket_name** and **base_key** parameters apply to external storage locations and can be omitted to use Synapse internal storage. Note also that STS can only be enabled on an empty folder.

```python
from synapseclient.models import Folder, StorageLocation, StorageLocationType

# create a new folder to use with STS and external S3 storage
folder = Folder(name=folder_name, parent_id=parent).store()

# create an STS-enabled external S3 storage location and assign it
storage_location = StorageLocation(
    storage_type=StorageLocationType.EXTERNAL_S3,
    bucket='my-external-synapse-bucket',
    base_key='path/within/bucket',
    sts_enabled=True,
).store()
folder.set_storage_location(storage_location_id=storage_location.storage_location_id)
```

### Using credentials with the awscli

This example illustrates obtaining STS credentials and using them with the awscli command line tool. The first command outputs the credentials as shell commands to execute which will then be picked up by subsequent aws cli commands. Note that the bucket-owner-full-control ACL is required when putting an object via STS credentials. This ensures that the object ownership will be transferred to the owner of the AWS bucket.

```bash
$ synapse get-sts-token -o shell syn123 read_write

export SYNAPSE_STS_S3_LOCATION="s3://my-external-synapse-bucket/path/within/bucket"
export AWS_ACCESS_KEY_ID="<access_key_id>"
export AWS_SECRET_ACCESS_KEY="<secret_access_key>"
export AWS_SESSION_TOKEN="<session_token>

# if the above are executed in the shell, the awscli will automatically apply them

# e.g. copy a file directly to the bucket using the exported credentials
$ aws s3 cp /path/to/local/file $SYNAPSE_STS_S3_LOCATION --acl bucket-owner-full-control
```

!!! warning
    [`aws s3 cp`](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/cp.html) cannot resume an upload. When an upload fails, the AWS CLI aborts the S3 multipart upload, so the next run sends the file again from byte 0. For large files, use the script in [Resume a direct S3 upload with STS credentials](#resume-a-direct-s3-upload-with-sts-credentials).

### Using credentials with boto3 in python

This example illustrates retrieving STS credentials and using them with boto3 within python code, in this case to upload a file.  Note that the bucket-owner-full-control ACL is required when putting an object via STS credentials. This ensures that the object ownership will be transferred to the owner of the AWS bucket.

```python
import boto3
from synapseclient.models import Folder

# the boto output_format is compatible with the boto3 session api.
credentials = Folder(id='syn123').get_sts_storage_token(
    permission='read_write', output_format='boto'
)

s3_client = boto3.client('s3', **credentials)
s3_client.upload_file(
    Filename='/path/to/local/file',
    Bucket='my-external-synapse-bucket',
    Key='path/within/bucket/file',
    ExtraArgs={'ACL': 'bucket-owner-full-control'},
)
```

!!! warning
    [`upload_file`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html) cannot resume an upload. It aborts the S3 multipart upload when it fails, so the next run sends the file again from byte 0. For large files, use the script in the next section.

### Resume a direct S3 upload with STS credentials

`aws s3 cp` and boto3 `upload_file` split a large file into an [S3 multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html), but they do not keep a record of it after they stop. The script below calls the S3 multipart API directly and saves the upload ID in a local state file, so a second run can continue from the parts that S3 already has.

The script does these steps:

1. Gets `read_write` STS credentials for the folder with [Folder.get_sts_storage_token][synapseclient.models.Folder.get_sts_storage_token]. The credentials are valid for 12 hours by default ([`GET /entity/{id}/sts`](https://rest-docs.synapse.org/rest/GET/entity/id/sts.html)). The Synapse client caches them and gets new credentials when less than one hour remains, so the script asks for them before each part.
2. Starts the upload with [`create_multipart_upload`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_multipart_upload.html) and writes the bucket, key, `UploadId`, part size, and file size and modification time to the state file. The script must keep the `UploadId` itself, because STS credentials usually cannot list the multipart uploads in the bucket.
3. On a second run, reads the state file and gets the parts that S3 has with the [`list_parts`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/paginator/ListParts.html) paginator.
4. Sends only the other parts with [`upload_part`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_part.html), then calls [`complete_multipart_upload`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/complete_multipart_upload.html).
5. Adds the S3 object to the folder as a Synapse File with [post_external_s3_file_handle][synapseclient.api.file_services.post_external_s3_file_handle] and [File.store][synapseclient.models.File.store], then deletes the state file.

```python
import asyncio
import json
import math
import os

import boto3
from botocore.exceptions import ClientError

from synapseclient import Synapse
from synapseclient.api import post_external_s3_file_handle
from synapseclient.models import File, Folder

# An STS-enabled folder that uses external S3 storage.
FOLDER_ID = "syn123"
LOCAL_PATH = "/path/to/large/file"
# The script saves the S3 upload ID here. Keep this file until the script finishes.
STATE_PATH = LOCAL_PATH + ".upload-state.json"
# S3 parts must be 5 MiB to 5 GiB, and an upload can have at most 10,000 parts.
PART_SIZE = 100 * 1024 * 1024

syn = Synapse()
syn.login()
folder = Folder(id=FOLDER_ID)


def get_s3_client():
    """Return an S3 client and the STS credentials for the folder. The Synapse
    client caches the credentials and gets new ones when less than one hour
    remains, so it is safe to call this before each part."""
    credentials = folder.get_sts_storage_token(
        permission="read_write", output_format="json"
    )
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=credentials["accessKeyId"],
        aws_secret_access_key=credentials["secretAccessKey"],
        aws_session_token=credentials["sessionToken"],
    )
    return s3_client, credentials


def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


def load_state(file_stat):
    """Return the saved upload, or None if there is no saved upload."""
    if not os.path.exists(STATE_PATH):
        return None
    with open(STATE_PATH) as f:
        state = json.load(f)
    if [state["size"], state["mtime"]] != [file_stat.st_size, file_stat.st_mtime]:
        raise RuntimeError(
            f"{LOCAL_PATH} changed after the upload started. "
            f"Delete {STATE_PATH} to start the upload again."
        )
    return state


def start_upload(file_stat):
    s3_client, credentials = get_s3_client()
    bucket = credentials["bucket"]
    key = "/".join(
        filter(None, [credentials.get("baseKey"), os.path.basename(LOCAL_PATH)])
    )
    response = s3_client.create_multipart_upload(
        Bucket=bucket, Key=key, ACL="bucket-owner-full-control"
    )
    state = {
        "bucket": bucket,
        "key": key,
        "upload_id": response["UploadId"],
        "part_size": PART_SIZE,
        "size": file_stat.st_size,
        "mtime": file_stat.st_mtime,
        "complete": False,
    }
    save_state(state)
    return state


def get_uploaded_parts(state):
    """Return {part number: ETag} for the parts that S3 has, or None if S3 no
    longer has the upload (for example, a lifecycle rule deleted it)."""
    s3_client, _ = get_s3_client()
    parts = {}
    try:
        for page in s3_client.get_paginator("list_parts").paginate(
            Bucket=state["bucket"], Key=state["key"], UploadId=state["upload_id"]
        ):
            for part in page.get("Parts", []):
                parts[part["PartNumber"]] = part["ETag"]
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchUpload":
            return None
        raise
    return parts


def upload_missing_parts(state, parts):
    part_count = max(1, math.ceil(state["size"] / state["part_size"]))
    with open(LOCAL_PATH, "rb") as f:
        for part_number in range(1, part_count + 1):
            if part_number in parts:
                continue
            f.seek((part_number - 1) * state["part_size"])
            s3_client, _ = get_s3_client()
            response = s3_client.upload_part(
                Bucket=state["bucket"],
                Key=state["key"],
                UploadId=state["upload_id"],
                PartNumber=part_number,
                Body=f.read(state["part_size"]),
            )
            parts[part_number] = response["ETag"]
            print(f"Uploaded part {part_number} of {part_count}")

    s3_client, _ = get_s3_client()
    s3_client.complete_multipart_upload(
        Bucket=state["bucket"],
        Key=state["key"],
        UploadId=state["upload_id"],
        MultipartUpload={
            "Parts": [
                {"PartNumber": number, "ETag": parts[number]}
                for number in sorted(parts)
            ]
        },
    )
    state["complete"] = True
    save_state(state)


file_stat = os.stat(LOCAL_PATH)
state = load_state(file_stat)
if state is None:
    state = start_upload(file_stat)

if not state["complete"]:
    parts = get_uploaded_parts(state)
    if parts is None:
        print("S3 no longer has the upload. Starting again from byte 0.")
        state = start_upload(file_stat)
        parts = {}
    upload_missing_parts(state, parts)

# Add the S3 object to the folder as a Synapse File.
file_handle = asyncio.run(
    post_external_s3_file_handle(
        bucket_name=state["bucket"],
        s3_file_key=state["key"],
        file_path=LOCAL_PATH,
        parent=FOLDER_ID,
    )
)
File(parent_id=FOLDER_ID, data_file_handle_id=file_handle["id"]).store()
os.remove(STATE_PATH)
```

To use the script:

1. Set `FOLDER_ID` to an STS-enabled folder that uses external S3 storage (see [Creating an STS enabled folder](#creating-an-sts-enabled-folder)), and `LOCAL_PATH` to the file to upload.
2. Run the script. If it stops (for example, a network failure, sleep, Ctrl-C, or a crash), run it again with the same values. It prints each part that it sends, so a second run starts after the last part that S3 has.
3. Do not change the file until the script finishes. If the size or modification time changes, the script stops and tells you to delete the state file.

If the bucket has an [`AbortIncompleteMultipartUpload` lifecycle rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html), S3 deletes uploads that are not complete after the number of days in the rule. If that occurs, the script prints a message and starts again from byte 0. For more about why the AWS tools cannot resume, see [Upload Resume](../explanations/upload_resume.md#direct-s3-uploads-with-sts-credentials).

### Automatic transfers to/from STS storage locations using boto3 with synapseclient

The Python Synapse client can be configured to automatically use STS tokens to perform uploads and downloads to enabled storage locations using an installed boto3 library rather than through the traditional Synapse client APIs. This can improve performance in certain situations, particularly uploads of large files, as the data transfer itself can be conducted purely against the AWS S3 APIs, only invoking the Synapse APIs to retrieve the necessary token and to update Synapse metadata in the case of an upload. Once configured to do so, retrieval of STS tokens for supported operations occurs automatically without any change in synapseclient usage.

To enable STS/boto3 transfers on all `get` and `store` operations, do the following:

1. Ensure that boto3 is installed in the same Python installation as synapseclient.

```bash
pip install boto3
```

2. To enable automatic transfers on all uploads and downloads, update your Synapse client configuration file (typically “.synapseConfig” in your $HOME directory, unless otherwise configured) with the [transfer] section, if it is not already present. To leverage STS/boto3 transfers on a per Synapse client object basis, set the **use_boto_sts_transfers** property.

```python
# add to .synapseConfig to automatically apply as default for all synapse client instances
[transfer]
use_boto_sts=true

# alternatively set on a per instance basis within python code
syn.use_boto_sts_transfers = True
```

Note that if boto3 is not installed, then these settings will have no effect.

!!! warning
    Uploads with automatic boto3 STS transfers cannot resume. Each upload uses a new S3 key, and boto3 `upload_file` aborts the S3 multipart upload when it fails, so the next run sends the file again from byte 0. To upload large files that can resume, set `use_boto_sts_transfers` to `False`. See [Resume an upload that stopped](#resume-an-upload-that-stopped).

## Resume an upload that stopped

When [File.store][synapseclient.models.File.store] or the [`synapse store`](../tutorials/command_line_client.md#store) command uploads a file to Synapse S3 storage, external S3 storage, or external Google Cloud storage, the client uses the [Synapse multipart upload API](https://rest-docs.synapse.org/rest/POST/file/multipart.html). Synapse keeps a record of the upload and of each part that it received. If the upload stops, run the same upload again: the client finds the earlier upload and sends only the parts that Synapse does not have.

For example, this script uploads a large file. If the upload stops for any reason (a network failure, sleep, Ctrl-C, or a crash), run the same script again:

```python
from synapseclient import Synapse
from synapseclient.models import File

syn = Synapse()
syn.login()

# If this upload stops, run this script again with the same path and parent.
# The client sends only the parts that Synapse does not have.
file = File(path="/path/to/large/file", parent_id="syn123").store()
print(f"Uploaded {file.id}")
```

The command line equivalent is:

```bash
synapse store /path/to/large/file --parentid syn123
```

If the upload fails, the error ends with a note that tells you how to resume:

```text
synapseclient.core.exceptions.SynapseUploadFailedException: Part upload failed
To resume this upload, run the same command again within 30 days. Do not change the file. The client sends only the parts that Synapse does not have. See https://python-docs.synapse.org/en/stable/explanations/upload_resume/
```

The second run resumes the upload only if these items are the same as in the first run:

* The Synapse account that you log in with.
* The file content, the file name, and the destination folder or project.

Synapse deletes an upload that is not complete 30 days after the last part was added to it. After that, the upload starts again from byte 0.

Before the client sends the first part, it reads the full file to calculate its MD5, because the MD5 identifies the upload. For a large file this takes some time, but it is usually much shorter than the upload itself.

### If the log shows a lost-upload warning

The client can log this warning when it resumes an upload to external S3 storage:

```text
Synapse has a record of this upload, but the storage deleted its parts. The upload starts again from byte 0.
```

This occurs when S3 deleted the parts before Synapse did, for example because of an [`AbortIncompleteMultipartUpload` lifecycle rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html) on the bucket. You do not need to do anything: the client starts a new upload and sends all the parts.

### STS-enabled folders

If [`use_boto_sts_transfers`](#automatic-transfersto-from-sts-storage-locations-using-boto3-with-synapseclient) is `True`, uploads to an STS-enabled folder use boto3 and cannot resume. Set it to `False` to use the Synapse multipart upload API, which can resume:

```python
syn.use_boto_sts_transfers = False
```

If you set `use_boto_sts=true` in the `[transfer]` section of your `.synapseConfig` file, remove that line or set it to `false`.

### Storage that cannot resume

Uploads to [SFTP](#sftp) storage and to client-authenticated S3-compatible storage (external object stores) always send the full file again. For the reasons, see [Upload Resume](../explanations/upload_resume.md).

## SFTP

### Installation

Installing the extra libraries that the Python client uses to communicate with SFTP servers may add a few steps to the installation process.

The required libraries are:

- [pysftp](https://pypi.python.org/pypi/pysftp)
- [paramiko](http://www.paramiko.org/)
- [pycrypto](https://www.dlitz.net/software/pycrypto/)
- [ecdsa](https://pypi.python.org/pypi/ecdsa/)

#### Installing on Unix variants

Building these libraries on Unix OS's is straightforward, but you need the Python development headers and libraries. For example, in Debian or Ubuntu distributions:

```bash
sudo apt-get install python-dev
```

Once this requirement is met, `sudo pip install synapseclient` should be able to build pycrypto.

#### Installing on Windows

[Binary distributions of pycrypto](http://www.voidspace.org.uk/python/modules.shtml#pycrypto) built for Windows is available from Michael Foord at Voidspace. Install this before installing the Python client.

After running the pycrypto installer, `sudo pip install synapseclient` should work.

Another option is to build your own binary with either the [free developer tools from Microsoft](http://www.visualstudio.com/en-us/products/visual-studio-community-vs) or the [MinGW compiler](http://www.mingw.org/).

### Configure your client

Make sure you configure your [~/.synapseConfig](../tutorials/authentication.md#use-synapseconfig) file to connect to your SFTP server.
