import importlib
import os
import tempfile
import uuid

from synapseclient import File
from synapseclient.core.retry import with_retry

import pytest
import unittest
from unittest import mock

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


def get_aws_env():
    return os.environ['EXTERNAL_S3_BUCKET_NAME'], {
        'aws_access_key_id': os.environ['EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID'],
        'aws_secret_access_key': os.environ['EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY'],
    }


@unittest.skipIf(*check_test_preconditions())
class ExernalStorageTest(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def _syn(self, syn):
        self.syn = syn

    @pytest.fixture(autouse=True)
    def _project(self, project):
        self.project = project

    @classmethod
    def _make_temp_file(cls, contents=None, **kwargs):
        # delete=False for Windows
        tmp_file = tempfile.NamedTemporaryFile(**kwargs, delete=False)
        if contents:
            with open(tmp_file.name, 'w') as f:
                f.write(contents)

        return tmp_file

    def _prepare_bucket_location(self, key_prefix):
        bucket_name, aws_creds = get_aws_env()
        s3_client = boto3.client('s3', **aws_creds)
        s3_client.put_object(
            Body=self.syn.credentials.username,
            Bucket=bucket_name,
            Key=f"{key_prefix}/owner.txt"
        )

        return s3_client

    def _configure_storage_location(self, sts_enabled=False):
        folder_name = str(uuid.uuid4())
        s3_client = self._prepare_bucket_location(folder_name)

        bucket_name, _ = get_aws_env()
        folder, storage_location_setting, _ = self.syn.create_s3_storage_location(
            parent=self.project,
            folder_name=folder_name,
            bucket_name=bucket_name,
            base_key=folder_name,
            sts_enabled=sts_enabled
        )

        return s3_client, folder, storage_location_setting['storageLocationId']

    def test_set_external_storage_location(self):
        """Test configuring an external storage location,
        saving a file there, and confirm that it is created and
        accessible as expected."""

        s3_client, folder, _ = self._configure_storage_location()

        file_contents = 'foo'
        upload_file = self._make_temp_file(contents=file_contents)

        file = File(path=upload_file.name, parent=folder)
        file_entity = self.syn.store(file)

        # verify we can download the file via synapse
        file_entity = self.syn.get(file_entity['id'])
        with open(file_entity.path, 'r') as f:
            downloaded_content = f.read()
        assert file_contents == downloaded_content

        # now verify directly using boto that the file is in the external storage
        # location as we expect it to be
        file_handle = self.syn._get_file_handle_as_creator(file_entity['dataFileHandleId'])

        # will raise an error if he key doesn't exist
        bucket_name, _ = get_aws_env()
        s3_client.get_object(Bucket=bucket_name, Key=file_handle['key'])

    def test_sts_external_storage_location(self):
        """Test creating and using an external STS storage location.
        A custom storage location is created with sts enabled,
        a file is uploaded directly via boto using STS credentials,
        a file handle is created for it, and then it is read directly
        via boto using STS read credentials.
        """
        bucket_name, _ = get_aws_env()
        _, folder, storage_location_id = self._configure_storage_location(sts_enabled=True)

        sts_read_creds = self.syn.get_sts_storage_token(folder['id'], 'read_only', output_format='boto')
        sts_write_creds = self.syn.get_sts_storage_token(folder['id'], 'read_write', output_format='boto')

        s3_read_client = boto3.client('s3', **sts_read_creds)
        s3_write_client = boto3.client('s3', **sts_write_creds)

        # put an object directly using our sts creds
        file_contents = 'saved using sts'
        temp_file = self._make_temp_file(contents=file_contents, suffix='.txt')

        remote_key = f"{folder.name}/sts_saved"

        # verify that the read credentials are in fact read only
        with pytest.raises(Exception) as ex_cm:
            s3_read_client.upload_file(
                Filename=temp_file.name,
                Bucket=bucket_name,
                Key=remote_key,
            )
        assert 'Access Denied' in str(ex_cm.value)

        # now create a file directly in s3 using our STS creds
        s3_write_client.upload_file(
            Filename=temp_file.name,
            Bucket=bucket_name,
            Key=remote_key,
            ExtraArgs={'ACL': 'bucket-owner-full-control'},
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
        file_handle = self.syn.create_external_s3_file_handle(
            bucket_name,
            remote_key,
            temp_file.name,
            storage_location_id=storage_location_id,
        )
        file = File(parentId=folder['id'], dataFileHandleId=file_handle['id'])
        file_entity = self.syn.store(file)

        # now should be able to retrieve the file via synapse
        retrieved_file_entity = self.syn.get(file_entity['id'])
        with open(retrieved_file_entity.path, 'r') as f:
            assert file_contents == f.read()

    def test_boto_upload__acl(self):
        """Verify when we store a Synapse object using boto we apply a bucket-owner-full-control ACL to the object"""
        bucket_name, _ = get_aws_env()
        _, folder, storage_location_id = self._configure_storage_location(sts_enabled=True)

        file_contents = str(uuid.uuid4())
        upload_file = self._make_temp_file(contents=file_contents)

        # mock the sts setting so that we upload this file using boto regardless of test configuration
        with mock.patch.object(self.syn, 'use_boto_sts_transfers', new_callable=mock.PropertyMock(return_value=True)):
            file = self.syn.store(File(path=upload_file.name, parent=folder))

        s3_read_client = boto3.client('s3', **get_aws_env()[1])
        bucket_acl = s3_read_client.get_bucket_acl(Bucket=bucket_name)
        bucket_grantee = bucket_acl['Grants'][0]['Grantee']
        assert bucket_grantee['Type'] == 'CanonicalUser'
        bucket_owner_id = bucket_grantee['ID']

        # with_retry to avoid acidity issues of an S3 put
        object_acl = with_retry(
            lambda: s3_read_client.get_object_acl(
                Bucket=bucket_name,
                Key=file['_file_handle']['key']
            ),
            retry_exceptions=[s3_read_client.exceptions.NoSuchKey]
        )
        grants = object_acl['Grants']
        assert len(grants) == 1
        grant = grants[0]
        grantee = grant['Grantee']
        assert grantee['Type'] == 'CanonicalUser'
        assert grantee['ID'] == bucket_owner_id
        assert grant['Permission'] == 'FULL_CONTROL'
