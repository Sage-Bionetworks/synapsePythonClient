===================
S3 Storage Features
===================

Synapse can use a variety of storage mechanisms to store content, however the most common
storage solution is AWS S3. This article illustrates some special features that can be used with S3 storage
and how they interact with the Python client.

External storage locations
==========================

Synapse folders can be configured to use custom implementations for their underlying data storage.
More information on this feature can be found
`here <https://docs.synapse.org/articles/custom_storage_location.html>`__.
The most common implementation of this is to configure a folder to store data in a user controlled AWS S3 bucket
rather than Synapse's default internal S3 storage.

The following illustrates creating a new folder backed by a user specified S3 bucket.

#. Ensure that the bucket is `properly configured
   <https://docs.synapse.org/articles/custom_storage_location.html#setting-up-an-external-aws-s3-bucket>`__.

#. Create a folder and configure it to use external S3 storage:

  .. code-block::

    # create a new folder to use with external S3 storage
    folder = syn.store(Folder(name=folder_name, parent=parent))
    syn.create_s3_storage_location(
        folder=folder,
        bucket_name='my-external-synapse-bucket',
        base_key='path/within/bucket',
     )

Once an external S3 storage folder exists, you can interact with it as you would any other folder using
Synapse tools. If you wish to add an object that is stored within the bucket to Synapse you can do that by adding
a file handle for that object using the Python client and then storing the file to that handle.

  .. code-block::

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


.. _sts_storage_locations:

STS Storage Locations
=====================

Create an STS enabled folder to use
`AWS Security Token Service <https://docs.synapse.org/articles/aws_sts_storage_locations.html>`__ credentials
with S3 storage locations. These credentials can be scoped to access individual Synapse files or folders and can be used
with external S3 tools such as the awscli and the boto3 library separately from Synapse to read and write files to and
from Synapse storage. At this time read and write capabilities are supported for external storage locations, while default
Synapse storage is limited to read only. Please read the linked documentation for a complete understanding of the capabilities
and restrictions of STS enabled folders.

Creating an STS enabled folder
------------------------------
Creating an STS enabled folder is similar to creating an external storage folder as described above, but this
time passing an additional **sts_enabled=True** keyword parameter. The **bucket_name** and **base_key**
parameters apply to external storage locations and can be omitted to use Synapse internal storage.
Note also that STS can only be enabled on an empty folder.

  .. code-block::

    # create a new folder to use with STS and external S3 storage
    folder = syn.store(Folder(name=folder_name, parent=parent))
    syn.create_s3_storage_location(
        folder=folder,
        bucket_name='my-external-synapse-bucket',
        base_key='path/within/bucket',
        sts_enabled=True,
     )

Using credentials with the awscli
---------------------------------
This example illustrates obtaining STS credentials and using them with the awscli command line tool.
The first command outputs the credentials as shell commands to execute which will then be picked up
by subsequent aws cli commands.

  .. code-block::

    $ synapse get-sts-token -o shell syn123 read_write

    export SYNAPSE_STS_S3_LOCATION="s3://my-external-synapse-bucket/path/within/bucket"
    export AWS_ACCESS_KEY_ID="<access_key_id>"
    export AWS_SECRET_ACCESS_KEY="<secret_access_key>"
    export AWS_SESSION_TOKEN="<session_token>

    # if the above are executed in the shell, the awscli will automatically apply them

    # e.g. copy a file directly to the bucket using the exported credentials
    $ aws s3 cp /path/to/local/file $SYNAPSE_STS_S3_LOCATION

Using credentials with boto3 in python
--------------------------------------
This example illustrates retrieving STS credentials and using them with boto3 within python code,
in this case to upload a file.

  .. code-block::

    # the boto output_format is compatible with the boto3 session api.
    credentials = syn.get_sts_storage_token('syn123', 'read_write', output_format='boto')

    s3_client = boto3.client('s3', **credentials)
    s3_client.upload_file(
        Filename='/path/to/local/file,
        Bucket='my-external-synapse-bucket',
        Key='path/within/bucket/file',
    )

Automatic transfers to/from STS storage locations using boto3 with synapseclient
--------------------------------------------------------------------------------

The Python Synapse client can be configured to automatically use STS tokens to perform uploads and downloads to enabled
storage locations using an installed boto3 library rather than through the traditional Synapse client APIs.
This can improve performance in certain situations, particularly uploads of large files, as the data transfer itself
can be conducted purely against the AWS S3 APIs, only invoking the Synapse APIs to retrieve the necessary token and
to update Synapse metadata in the case of an upload. Once configured to do so, retrieval of STS tokens for supported
operations occurs automatically without any change in synapseclient usage.

To enable STS/boto3 transfers on all `get` and `store` operations, do the following:

1. Ensure that boto3 is installed in the same Python installation as synapseclient.

  .. code-block::

    pip install boto3

2. To enable automatic transfers on all uploads and downloads, update your Synapse client configuration file
   (typically “.synapseConfig” in your $HOME directory, unless otherwise configured) with the [transfer] section,
   if it is not already present. To leverage STS/boto3 transfers on a per Synapse client object basis, set
   the **use_boto_sts_transfers** property.

  .. code-block::

    # add to .synapseConfig to automatically apply as default for all synapse client instances
    [transfer]
    use_boto_sts=true

    # alternatively set on a per instance basis within python code
    syn.use_boto_sts_transfers = True

Note that if boto3 is not installed, then these settings will have no effect.


Storage location migration
==========================

There are circumstances where it can be useful to move the files underlying Synapse entities from one storage
location to another without impacting the structure or identifiers of the Synapse entities themselves. An example
scenario is needing to use `STS <S3Storage.html#sts-storage-locations>`__ features with an existing Synapse Project
that was not initially configured with an STS enabled
`custom storage location <S3Storage.html#external-storage-locations>`__.

The Synapse client has utilities for migrating entities to a new storage location without having to download
the content locally and re-uploading it which can be slow, and may alter the meta data associated with the entities
in undesirable ways.

Migrating programmatically
--------------------------

Migrating a Synapse project or folder programatically is a two step process.

First ensure that you know the id of the storage location you want to migrate to. More info on storage
locations can be found above and `here <https://docs.synapse.org/articles/custom_storage_location.html>`__.

Once the storage location is known, the first step to migrate an entity is create a migratable index
of its contents using the
`index_files_for_migration <synapseutils.html#synapseutils.migrate_functions.index_files_for_migration>`__ function, e.g.

  .. code-block::

    import synapseutils

    entity_id = 'syn123'  # a Synapse entity whose contents need to be migrated, e.g. a Project or Folder
    storage_location_id = '12345'  # the id of the storage location being migrated to

    # a path on disk where this utility can create a sqlite database to store its index.
    # nothing needs to exist at this path, but it must be a valid path on a volume with sufficient
    # disk space to store a meta data listing of all the contents in the indexed entity.
    db_path = '/tmp/foo/bar.db'

    result = synapseutils.index_files_for_migration(
        syn,
        entity_id,
        storage_location_id,
        db_path,

        # optional args, see function documentation linked above for a description of these parameters
        file_version_strategy='new',
        include_table_files=false,
        continue_on_error=true
    )

Once the entity has been indexed you can optionally programmatically inspect the the contents of the index
or output its contents to a csv file in order to manually inspect it using the `available methods <synapseutils.html#synapseutils.migrate_functions.MigrationResult>`__
on the returned result object.

The next step to trigger the migration from the indexed files is using the `migrate_indexed_files <synapseutils.html#synapseutils.migrate_functions.migrate_indexed_files>`__ function, e.g.

  .. code-block::

    result = synapseutils.migrate_indexed_files(
        syn,
        db_path,

        # optional args, see function documentation linked above for a description of these parameters
        create_table_snapshots=True,
        continue_on_error=False,
        force=True
    )

The result can be again be inspected as above to see the results of the migration.

Note that above the *force* parameter is necessary if running from a non-interactive shell. Proceeding
with a migration requires confirmation in the form of user prompt. If running programtically this parameter
instead confirms your intention to proceed with the migration.


Migrating from the command line
-------------------------------

Synapse entities can also be migrated from the command line. The options are similar to above.
Whereas migrating programatically involves two separate function calls, from the command line
there is a single `migrate <CommandLineClient.html#migrate>`__ command with the *dryRun* argument providing the option
to generate the index only without proceeding onto the migration.

Note that as above, confirmation is required before a migration starts. As above, this must either be
in the form of confirming via a prompt if running the command from an interactive shell, or using the *force*
command.

The optional *csv_log_path* argument will output the results to a csv file for record keeping, and is recommended.

  .. code-block::

    synapse migrate syn123 54321 /tmp/migrate.db --csv_log_path /tmp/migrate.csv

Sample output:
  .. code-block::

    Indexing Project syn123
    Indexing file entity syn888
    Indexing file entity syn999
    Indexed 2 items, 2 needing migration, 0 already stored in destination storage location (54321). Encountered 0 errors.
    21 items for migration to 54321. Proceed? (y/n)? y
    Creating new version for file entity syn888
    Creating new version for file entity syn999
    Completed migration of syn123. 2 files migrated. 0 errors encountered
    Writing csv log to /tmp/migrate.csv
