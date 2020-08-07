import os

from unittest import mock

from synapseclient.core.constants import concrete_types
from synapseclient.core.upload import upload_functions


class TestUploadFileHandle:

    # TODO backfill unit tests for file handle download via other existing channels

    @mock.patch.object(upload_functions, 'upload_synapse_sts_boto_s3')
    @mock.patch.object(upload_functions, 'sts_transfer')
    def test_upload_handle__sts_boto(self, mock_sts_transfer, mock_upload_synapse_sts_boto_s3):
        """Verify that that boto is picked as the upload method when conditions are met
        (configured, boto installed, and storage location supports it"""

        syn = mock.Mock()
        parent_entity = 'syn_12345'
        path = '/tmp/upload_me'

        mock_sts_transfer.is_boto_sts_transfer_enabled.return_value = True
        mock_sts_transfer.is_storage_location_sts_enabled.return_value = True

        location = {'concreteType': concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION}
        syn._getDefaultUploadDestination.return_value = location

        upload_functions.upload_file_handle(syn, parent_entity, path)

        mock_sts_transfer.is_boto_sts_transfer_enabled.assert_called_once_with(syn)
        mock_sts_transfer.is_storage_location_sts_enabled.assert_called_once_with(syn, parent_entity, location)
        syn._getDefaultUploadDestination.assert_called_once_with(parent_entity)
        mock_upload_synapse_sts_boto_s3.assert_called_once_with(
            syn,
            parent_entity,
            location,
            path,
            mimetype=None
        )


@mock.patch.object(upload_functions, 'sts_transfer')
@mock.patch.object(upload_functions, 'S3ClientWrapper')
@mock.patch('uuid.uuid4')
def test_upload_synapse_sts_boto_s3(mock_uuid4, mock_s3_client_wrapper, mock_sts_transfer):
    """Verify that when we upload using boto the file is uploaded through our S3 wrapper
    and an external S3 file handle is created for the uploaded file."""

    syn = mock.Mock()
    parent_id = 'syn_12345'
    storage_location_id = 1234
    bucket_name = 'foo'
    base_key = '/bar/baz'
    local_path = '/tmp/location/to/path'
    upload_destination = {
        'bucket': bucket_name,
        'baseKey': base_key,
        'storageLocationId': storage_location_id,
    }
    credentials = {
        'aws_access_key_id': 'foo',
        'aws_secret_access_key': 'bar',
        'aws_session_token': 'baz',
    }

    key_prefix = mock_uuid4.return_value = 'af88c590-dfd2-4ab9-a36f-2829c44b5239'

    def mock_with_boto_sts_credentials(upload_fn, syn, objectId, permission):
        assert permission == 'read_write'
        assert parent_id == objectId
        return upload_fn(credentials)

    mock_sts_transfer.with_boto_sts_credentials = mock_with_boto_sts_credentials

    returned_file_handle = upload_functions.upload_synapse_sts_boto_s3(
        syn,
        parent_id,
        upload_destination,
        local_path,
    )
    assert returned_file_handle == syn.create_external_s3_file_handle.return_value
    remote_file_key = "/".join([base_key, key_prefix, os.path.basename(local_path)])
    syn.create_external_s3_file_handle.assert_called_once_with(
        bucket_name,
        remote_file_key,
        local_path,
        storage_location_id=storage_location_id,
        mimetype=None,
    )
