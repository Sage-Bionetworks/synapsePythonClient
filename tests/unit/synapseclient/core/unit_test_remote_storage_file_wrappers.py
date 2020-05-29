import botocore.exceptions
import os.path

from unittest import mock
from nose.tools import assert_equal, assert_false, assert_raises, assert_true

from synapseclient.core import remote_file_storage_wrappers
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper


class TestS3ClientWrapper:

    @mock.patch.object(remote_file_storage_wrappers, 'attempt_import')
    def test_download__import_error(self, mock_attempt_import):
        """Verify an error importing boto3 is raised as expected"""
        mock_attempt_import.side_effect = ImportError("can't import boto3")

        bucket_name = 'foo_bucket'
        remote_file_key = 'foo/bar/baz'
        download_file_path = '/tmp/download'
        endpoint_url = 'http://foo.s3.amazon.com'

        with assert_raises(ImportError) as ex:
            S3ClientWrapper.download_file(bucket_name, endpoint_url, remote_file_key, download_file_path)

    @staticmethod
    def _download_test(**kwargs):
        bucket_name = 'foo_bucket'
        remote_file_key = 'foo/bar/baz'
        download_file_path = '/tmp/download'
        endpoint_url = 'http://foo.s3.amazon.com'

        with mock.patch('boto3.session.Session') as mock_boto_session,\
                mock.patch.object(S3ClientWrapper, '_create_progress_callback_func') as mock_create_progress_callback,\
                mock.patch('boto3.s3.transfer.TransferConfig') as mock_TransferConfig:

            returned_download_file_path = S3ClientWrapper.download_file(
                bucket_name,
                endpoint_url,
                remote_file_key,
                download_file_path,
                **kwargs
            )

            if 'profile_name' in kwargs:
                mock_boto_session.assert_called_once_with(profile_name=kwargs['profile_name'])
            else:
                mock_boto_session.assert_called_once_with(**kwargs['credentials'])

            resource = mock_boto_session.return_value.resource
            resource.assert_called_once_with('s3', endpoint_url=endpoint_url)
            s3 = resource.return_value
            s3.Object.assert_called_once_with(bucket_name, remote_file_key)
            s3_object = s3.Object.return_value

            mock_TransferConfig.assert_called_once_with(**kwargs.get('transfer_config_kwargs', {}))
            transfer_config = mock_TransferConfig.return_value

            progress_callback = None
            if kwargs.get('show_progress', True):
                s3_object.load.assert_called_once_with()
                mock_create_progress_callback.assert_called_once_with(
                    s3_object.content_length,
                    os.path.basename(download_file_path),
                    prefix='Downloading'
                )
                progress_callback = mock_create_progress_callback.return_value
            else:
                assert_false(mock_create_progress_callback.called)

            s3_object.download_file.assert_called_once_with(
                download_file_path,
                Callback=progress_callback,
                Config=transfer_config
            )

            # why do we return something we passed...?
            assert_equal(download_file_path, returned_download_file_path)

    def test_download__profile(self):
        """Verify downloading using a profile name passes through to to the session."""
        self._download_test(profile_name='foo', transfer_config_kwargs={'max_concurency': 10})

    def test_download__credentials(self):
        """Verify downloading using a supplied credential dictionary"""
        credentials = {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_session_token': 'baz',
        }
        self._download_test(credentials=credentials, show_progress=False)

    @staticmethod
    def _download_error_test(exception, raised_type):
        bucket_name = 'foo_bucket'
        remote_file_key = '/foo/bar/baz'
        download_file_path = '/tmp/download'
        endpoint_url = 'http://foo.s3.amazon.com'

        with mock.patch('boto3.session.Session') as mock_boto_session:
            resource = mock_boto_session.return_value.resource
            s3 = resource.return_value
            s3.Object.side_effect = exception

            with assert_raises(raised_type):
                S3ClientWrapper.download_file(
                    bucket_name,
                    endpoint_url,
                    remote_file_key,
                    download_file_path,
                )

    def test_download__404(self):
        """Verify 404 is transformed into a ValueError"""
        thrown_ex = botocore.exceptions.ClientError(
            {'Error': {'Code': '404'}},
            'S#Download'
        )
        self._download_error_test(thrown_ex, ValueError)

    def test_other_botocore_error(self):
        """Verify another botocore error is raised straight"""
        thrown_ex = botocore.exceptions.ClientError(
            {'Error': {'Code': '500'}},
            'S#Download'
        )
        self._download_error_test(thrown_ex, botocore.exceptions.ClientError)

    def test_download__error(self):
        """Verify a non-botocore error is raised straight"""
        ex = ValueError('boom')
        self._download_error_test(ex, ex.__class__)

    @mock.patch.object(remote_file_storage_wrappers, 'os')
    def test_upload__path_doesnt_exist(self, mock_os):
        mock_os.path.isfile.return_value = False

        upload_path = '/tmp/upload_file'
        with assert_raises(ValueError) as ex_cm:
            S3ClientWrapper.upload_file(
                'foo_bucket',
                'http://foo.s3.amazon.com',
                '/foo/bar/baz',
                upload_path
            )
        assert_true('does not exist' in str(ex_cm.exception))
        mock_os.path.isfile.assert_called_once_with(upload_path)

    @staticmethod
    def _upload_test(**kwargs):
        bucket_name = 'foo_bucket'
        remote_file_key = 'foo/bar/baz'
        upload_file_path = '/tmp/upload_file'
        endpoint_url = 'http://foo.s3.amazon.com'

        with mock.patch('boto3.session.Session') as mock_boto_session,\
                mock.patch.object(S3ClientWrapper, '_create_progress_callback_func') as mock_create_progress_callback,\
                mock.patch('boto3.s3.transfer.TransferConfig') as mock_TransferConfig,\
                mock.patch.object(remote_file_storage_wrappers, 'os') as mock_os:

            returned_upload_path = S3ClientWrapper.upload_file(
                bucket_name,
                endpoint_url,
                remote_file_key,
                upload_file_path,
                **kwargs
            )

            if 'profile_name' in kwargs:
                mock_boto_session.assert_called_once_with(profile_name=kwargs['profile_name'])
            else:
                mock_boto_session.assert_called_once_with(**kwargs['credentials'])

            resource = mock_boto_session.return_value.resource
            resource.assert_called_once_with('s3', endpoint_url=endpoint_url)
            s3 = resource.return_value
            s3.Bucket.assert_called_once_with(bucket_name)
            s3_bucket = s3.Bucket.return_value

            mock_TransferConfig.assert_called_once_with(**kwargs.get('transfer_config_kwargs', {}))
            transfer_config = mock_TransferConfig.return_value

            progress_callback = None
            if kwargs.get('show_progress', True):
                mock_os.stat.assert_called_once_with(upload_file_path)
                file_size = mock_os.stat.return_value

                mock_os.path.basename.assert_called_once_with(upload_file_path)
                filename = mock_os.path.basename(upload_file_path)
                progress_callback = S3ClientWrapper._create_progress_callback_func(file_size, filename,
                                                                                   prefix='Uploading')
            else:
                assert_false(mock_create_progress_callback.called)

            s3_bucket.upload_file.assert_called_once_with(
                upload_file_path,
                remote_file_key,
                Callback=progress_callback,
                Config=transfer_config
            )

            # why do we return something we passed...?
            assert_equal(upload_file_path, returned_upload_path)

    def test_upload__profile(self):
        """Verify uploading using a profile name passes through to to the session."""
        self._upload_test(profile_name='foo', transfer_config_kwargs={'max_concurency': 10})

    def test_upload__credentials(self):
        """Verify uploading using a supplied credential dictionary"""
        credentials = {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_session_token': 'baz',
        }
        self._upload_test(credentials=credentials, show_progress=False)
