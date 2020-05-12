import importlib
import os
import tempfile
import uuid

from synapseclient import File, Folder
from synapseclient.core.retry import with_retry

from nose.tools import assert_equal, assert_raises, assert_true
import unittest
from tests import integration

try:
    boto3 = importlib.import_module('boto3')
except ImportError:
    boto3 = None


def check_test_preconditions():
    # in order ot run the tests in this file boto3 must be available
    # and we must have configuration indicating where to store external
    # storage for testing

    skip_tests = False
    reason = ''
    if not boto3:
        skip_tests = True
        reason = 'boto3 not available'

    elif not (
        os.environ.get('EXTERNAL_S3_BUCKET_NAME') and
        os.environ.get('EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID') and
        os.environ.get('EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY')
    ):
        skip_tests = True
        reason = 'External bucket access not defined in environment'

    return skip_tests, reason

def setup(module):
    module.syn = integration.syn
    module.project = integration.project


def get_aws_env():
    return os.environ['EXTERNAL_S3_BUCKET_NAME'], {
        'aws_access_key_id': os.environ['EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID'],
        'aws_secret_access_key': os.environ['EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY'],
    }

@unittest.skipIf(*check_test_preconditions())
class ExernalStorageTest(unittest.TestCase):

    @classmethod
    def _make_temp_file(cls,contents=None, **kwargs):
        tmp_file = tempfile.NamedTemporaryFile(**kwargs)

        if contents:
            with open(tmp_file.name, 'w') as f:
                f.write(contents)

        return tmp_file

    def _prepare_bucket_location(self, key_prefix):
        bucket_name, aws_creds = get_aws_env()
        s3_client = boto3.client('s3', **aws_creds)
        s3_client.put_object(
            Body=syn.credentials.username,
            Bucket=bucket_name,
            Key=f"{key_prefix}/owner.txt"
        )

        return s3_client

    def _configure_storage_location(self, sts_enabled=False):
        folder_name = str(uuid.uuid4())
        s3_client = self._prepare_bucket_location(folder_name)

        folder = syn.store(Folder(name=folder_name, parent=integration.project))

        storage_type = 'ExternalS3Storage'
        bucket_name, _ = get_aws_env()

        storage_location = syn.createStorageLocationSetting(
            storage_type,
            bucket=bucket_name,
            baseKey=folder_name,
            stsEnabled=sts_enabled,
        )

        storage_location_id = storage_location['storageLocationId']
        syn.setStorageLocation(
            folder,
            storage_location_id,
        )

        return s3_client, folder, storage_location_id

    def test_set_external_storage_location(self):
        """Test configuring an external storage location,
        saving a file there, and confirm that it is created and
        accessible as expected."""

        s3_client, folder, _ = self._configure_storage_location()

        file_contents = 'foo'
        upload_file = self._make_temp_file(contents=file_contents)

        file = File(path=upload_file.name, parent=folder)
        file_entity = syn.store(file)

        # verify we can download the file via synapse
        file_entity = syn.get(file_entity['id'])
        with open(file_entity.path, 'r') as f:
            return f.read()
        assert_equal(file_contents, downloaded_content)

        # now verify directly using boto that the file is in the external storage
        # location as we expect it to be
        file_handle = syn._get_file_handle_as_creator(file_entity['dataFileHandleId'])

        # will raise an error if he key doesn't exist
        bucket_name, _ = get_aws_env()
        s3_client.get_object(Bucket=bucket_name, Key=file_handle['key'])

    def test_sts_external_storage_location(self):
        bucket_name, _ = get_aws_env()
        _, folder, storage_location_id = self._configure_storage_location(sts_enabled=True)

        sts_read_creds = syn.get_sts(folder['id'], 'read_only', output_format='boto')
        sts_write_creds = syn.get_sts(folder['id'], 'read_write', output_format='boto')

        s3_read_client = boto3.client('s3', **sts_read_creds)
        s3_write_client = boto3.client('s3', **sts_write_creds)

        # put an object directly using our sts creds
        file_contents = 'saved using sts'
        temp_file = self._make_temp_file(contents=file_contents, suffix='.txt')

        remote_key = f"{folder.name}/sts_saved"

        # verify that the read credentials are in fact read only
        with assert_raises(Exception) as ex_cm:
            s3_read_client.upload_file(
                Filename=temp_file.name,
                Bucket=bucket_name,
                Key=remote_key,
            )
        assert_true('Access Denied' in str(ex_cm.exception))

        # now create a file directly in s3 using our STS creds
        s3_write_client.upload_file(
            Filename=temp_file.name,
            Bucket=bucket_name,
            Key=remote_key,
        )

        # now read the file using our read credentials
        # S3 is not ACID so we add a retry here to try to ensure our
        # object will be available before we try to create the handle
        with_retry(
            lambda: s3_read_client.get_object(
                Bucket=bucket_name,
                Key=remote_key
            ),
            retry_exceptions=[s3_read_client.exceptions.NoSuchKey]
        )

        # create an external file handle so we can read it via synapse
        file_handle = syn._createExternalS3FileHandle(
            bucket_name,
            remote_key,
            temp_file.name,
            storage_location_id,
        )
        file = File(parentId=folder['id'], dataFileHandleId=file_handle['id'])
        file_entity = syn.store(file)

        # now should be able to retrieve the file via synapse
        retrieved_file_entity = syn.get(file_entity['id'])
        with open(retrieved_file_entity.path, 'r') as f:
            assert_equal(file_contents, f.read())
