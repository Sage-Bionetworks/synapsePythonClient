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
`AWS Security Token Service <https://docs.synapse.org/articles/sts_storage_locations.html>`__ credentials
with S3 storage locations. These credentials can be used with external S3 tools such as the awscli and the boto3
library separately from Synapse to read and write files to and from Synapse storage. Please read the linked
documentation for a complete understanding of the capabilities and restrictions of STS enabled folders.

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
