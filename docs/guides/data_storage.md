# Data Storage

## S3 Storage Features

Synapse can use a variety of storage mechanisms to store content, however the most common storage solution is AWS S3. This article illustrates some special features that can be used with S3 storage and how they interact with the Python client. In particular it covers:

1. Linking External storage locations to new/existing projects or folders
2. Migration of existing projects or folders to new external storage locations
3. Creating STS enabled storage locations
4. Using SFTP

## External storage locations

Synapse projects or folders can be configured to use custom implementations for their underlying data storage. More information on this feature can be found [here](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html). The most common implementation of this is to configure a folder to store data in a user controlled AWS S3 bucket rather than Synapse's default internal S3 storage.

### Creating a new folder backed by a user specified S3 bucket

The following illustrates creating a new folder backed by a user specified S3 bucket. Note: An existing folder also works.

If you are changing the storage location of an existing folder to a user specified S3 bucket none of the files will be migrated. In order to migrate the files to the new storage location see the section [Migrating programmatically](#migrating-programmatically). When you change the storage location for a folder only NEW files uploaded to the folder are uploaded to the user specific S3 bucket.

1. Ensure that the bucket is [properly configured](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html#CustomStorageLocations-SettingUpanExternalAWSS3Bucket).

2. Create a folder and configure it to use external S3 storage:

```python
# create a new folder to use with external S3 storage
folder = syn.store(Folder(name=folder_name, parent=parent))
# You may also use an existing folder like:
# folder = syn.get("syn123")
folder, storage_location, project_setting = syn.create_s3_storage_location(
    folder=folder,
    bucket_name='my-external-synapse-bucket',
    base_key='path/within/bucket',
 )

# if needed the unique storage location identifier can be obtained e.g.
storage_location_id = storage_location['storageLocationId']
```

### Creating a new project backed by a user specified S3 bucket
The following illustrates creating a new project backed by a user specified S3 bucket. Note: An existing project also works.

If you are changing the storage location of an existing project to a user specified S3 bucket none of the files will be migrated. In order to migrate the files to the new storage location see the documentation further down in this article labeled 'Migrating programmatically'. When you change the storage location for a project only NEW files uploaded to the project are uploaded to the user specific S3 bucket.

1. Ensure that the bucket is [properly configured](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html#CustomStorageLocations-SettingUpanExternalAWSS3Bucket).

2. Create a project and configure it to use external S3 storage:

```python
# create a new, or retrieve an existing project to use with external S3 storage
project = syn.store(Project(name="my_project_name"))
project_storage, storage_location, project_setting = syn.create_s3_storage_location(
    # Despite the KW argument name, this can be a project or folder
    folder=project,
    bucket_name='my-external-synapse-bucket',
    base_key='path/within/bucket',
)

# if needed the unique storage location identifier can be obtained e.g.
storage_location_id = storage_location['storageLocationId']
```

Once an external S3 storage folder exists, you can interact with it as you would any other folder using Synapse tools. If you wish to add an object that is stored within the bucket to Synapse you can do that by adding a file handle for that object using the Python client and then storing the file to that handle.

```python
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

# now we add a file handle for that file and store the file to that handle
file_handle = syn.create_external_s3_file_handle(
    bucket,
    s3_key,
    local_file_path,
    parent=parent_synapse_folder_id,
)
file = File(parentId=folder['id'], dataFileHandleId=file_handle['id'])
file_entity = syn.store(file)
```

## Storage location migration

There are circumstances where it can be useful to move the files underlying Synapse entities from one storage location to another without impacting the structure or identifiers of the Synapse entities themselves. An example scenario is needing to use STS features with an existing Synapse Project that was not initially configured with an STS enabled [custom storage location](#sts-storage-locations).

The Synapse client has utilities for migrating entities to a new storage location without having to download the content locally and re-uploading it which can be slow, and may alter the meta data associated with the entities in undesirable ways.

During the migration it is recommended that uploads and downloads are blocked to prevent possible conflicts or race conditions. This can be done by setting permissions to `Can view` for the project or folder being migrated. After the migration is complete set the permissions back to their original values.

Expected time to migrate data is around 13 minutes per 100Gb as of 11/21/2023.

### Migrating programmatically

Migrating a Synapse project or folder programmatically is a two step process.

First ensure that you know the id of the storage location you want to migrate to. More info on storage locations can be found above and [here](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html).

Once the storage location is known, the first step to migrate the project or folder is to create a migratable index of its contents using the [index_files_for_migration][synapseutils.migrate_functions.index_files_for_migration] function, e.g.

When specifying the `.db` file for the migratable indexes you need to specify a `.db` file that does not already exist for another synapse project or folder on disk. It is the best practice to specify a unique name for the file by including the synapse id in the name of the file, or other unique identifier.

```python
import synapseutils

entity_id = 'syn123'  # a Synapse entity whose contents need to be migrated, e.g. a Project or Folder
dest_storage_location_id = '12345'  # the id of the destination storage location being migrated to

# a path on disk where this utility can create a sqlite database to store its index.
# nothing needs to exist at this path, but it must be a valid path on a volume with sufficient
# disk space to store a meta data listing of all the contents in the indexed entity.
# a rough rule of thumb is 100kB per 1000 entities indexed.
db_path = '/tmp/foo/syn123_bar.db'

result = synapseutils.index_files_for_migration(
    syn,
    entity_id,
    dest_storage_location_id,
    db_path,

    # optional args, see function documentation linked above for a description of these parameters
    source_storage_location_ids=['54321', '98765'],
    file_version_strategy='new',
    include_table_files=False,
    continue_on_error=True
)
```

If called on a container (e.g. a Project or Folder) the *index_files_for_migration* function will recursively index all of the children of that container (including its subfolders). Once the entity has been indexed you can optionally programmatically inspect the the contents of the index or output its contents to a csv file in order to manually inspect it using the [available methods][synapseutils.migrate_functions.MigrationResult] on the returned result object.

The next step to trigger the migration from the indexed files is using the [migrate_indexed_files][synapseutils.migrate_functions.migrate_indexed_files] function, e.g.

```python
result = synapseutils.migrate_indexed_files(
    syn,
    db_path,

    # optional args, see function documentation linked above for a description of these parameters
    create_table_snapshots=True,
    continue_on_error=False,
    force=True
)
```

The result can be again be inspected as above to see the results of the migration.

Note that above the *force* parameter is necessary if running from a non-interactive shell. Proceeding with a migration requires confirmation in the form of user prompt. If running programmatically this parameter instead confirms your intention to proceed with the migration.

#### Putting all the migration pieces together

```python
import os
import synapseutils
import synapseclient

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

# The project or folder I want to migrate everything to this S3 storage location
project_or_folder = syn.get(my_synapse_project_or_folder_to_migrate)

project_or_folder, storage_location, project_setting = syn.create_s3_storage_location(
    # Despite the KW argument name, this can be a project or folder
    folder=project_or_folder,
    bucket_name=external_bucket_name,
    base_key=external_bucket_base_key,
)

# The id of the destination storage location being migrated to
storage_location_id = storage_location["storageLocationId"]
print(
    f"Indexing: {project_or_folder.id} for migration to storage_id: {storage_location_id} at: {db_path}"
)

try:
    result = synapseutils.index_files_for_migration(
        syn,
        project_or_folder.id,
        storage_location_id,
        db_path,
        file_version_strategy="all",
    )

    print(f"Indexing result: {result.get_counts_by_status()}")

    print("Migrating files...")

    result = synapseutils.migrate_indexed_files(
        syn,
        db_path,
        force=True,
    )

    print(f"Migration result: {result.get_counts_by_status()}")
    syn.sendMessage(
        userIds=[my_user_id],
        messageSubject=f"Migration success for {project_or_folder.id}",
        messageBody=f"Migration result: {result.get_counts_by_status()}",
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
# create a new folder to use with STS and external S3 storage
folder = syn.store(Folder(name=folder_name, parent=parent))
folder, storage_location, project_setting = syn.create_s3_storage_location(
    folder=folder,
    bucket_name='my-external-synapse-bucket',
    base_key='path/within/bucket',
    sts_enabled=True,
)
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

### Using credentials with boto3 in python

This example illustrates retrieving STS credentials and using them with boto3 within python code, in this case to upload a file.  Note that the bucket-owner-full-control ACL is required when putting an object via STS credentials. This ensures that the object ownership will be transferred to the owner of the AWS bucket.

```python
# the boto output_format is compatible with the boto3 session api.
credentials = syn.get_sts_storage_token('syn123', 'read_write', output_format='boto')

s3_client = boto3.client('s3', **credentials)
s3_client.upload_file(
    Filename='/path/to/local/file,
    Bucket='my-external-synapse-bucket',
    Key='path/within/bucket/file',
    ExtraArgs={'ACL': 'bucket-owner-full-control'},
)
```

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
