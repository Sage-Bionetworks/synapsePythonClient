import urllib.parse as urllib_parse
from unittest import mock

import botocore.exceptions
import pytest
from tqdm import tqdm

from synapseclient.core import remote_file_storage_wrappers
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper


class TestS3ClientWrapper:
    @mock.patch.object(remote_file_storage_wrappers, "attempt_import")
    def test_download__import_error(self, mock_attempt_import) -> None:
        """Verify an error importing boto3 is raised as expected"""
        mock_attempt_import.side_effect = ImportError("can't import boto3")

        bucket_name = "foo_bucket"
        remote_file_key = "foo/bar/baz"
        download_file_path = "/tmp/download"
        endpoint_url = "http://foo.s3.amazon.com"

        with pytest.raises(ImportError):
            S3ClientWrapper.download_file(
                bucket_name, endpoint_url, remote_file_key, download_file_path
            )

    @staticmethod
    def _download_test(**kwargs) -> None:
        bucket_name = "foo_bucket"
        remote_file_key = "foo/bar/baz"
        download_file_path = "/tmp/download"
        endpoint_url = "http://foo.s3.amazon.com"
        show_progress = kwargs.pop("show_progress", True)

        with mock.patch(
            "boto3.session.Session"
        ) as mock_boto_session, mock.patch.object(
            S3ClientWrapper, "_create_progress_callback_func"
        ) as mock_create_progress_callback, mock.patch(
            "boto3.s3.transfer.TransferConfig"
        ) as mock_TransferConfig:
            # Create a mock object for s3.Object with content_length set to an integer
            mock_s3_object = mock.Mock(content_length=1234)
            # Make resource().Object return the mock object
            mock_boto_session.return_value.resource.return_value.Object.return_value = (
                mock_s3_object
            )

            returned_download_file_path = S3ClientWrapper.download_file(
                bucket_name,
                endpoint_url,
                remote_file_key,
                download_file_path,
                progress_bar=tqdm(leave=None) if show_progress else None,
                **kwargs,
            )

            if "profile_name" in kwargs:
                mock_boto_session.assert_called_once_with(
                    profile_name=kwargs["profile_name"]
                )
            else:
                mock_boto_session.assert_called_once_with(**kwargs["credentials"])

            resource = mock_boto_session.return_value.resource
            resource.assert_called_once_with("s3", endpoint_url=endpoint_url)
            s3 = resource.return_value
            s3.Object.assert_called_once_with(bucket_name, remote_file_key)
            s3_object = s3.Object.return_value

            mock_TransferConfig.assert_called_once_with(
                **kwargs.get("transfer_config_kwargs", {})
            )
            transfer_config = mock_TransferConfig.return_value

            progress_callback = None
            if show_progress:
                s3_object.load.assert_called_once_with()
                mock_create_progress_callback.assert_called_once()
                progress_callback = mock_create_progress_callback.return_value
            else:
                assert not mock_create_progress_callback.called

            s3_object.download_file.assert_called_once_with(
                download_file_path, Callback=progress_callback, Config=transfer_config
            )

            # why do we return something we passed...?
            assert download_file_path == returned_download_file_path

    def test_download__profile(self) -> None:
        """Verify downloading using a profile name passes through to to the session."""
        self._download_test(
            profile_name="foo", transfer_config_kwargs={"max_concurency": 10}
        )

    def test_download__credentials(self) -> None:
        """Verify downloading using a supplied credential dictionary"""
        credentials = {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_session_token": "baz",
        }
        self._download_test(credentials=credentials, show_progress=False)

    @staticmethod
    def _download_error_test(exception, raised_type) -> None:
        bucket_name = "foo_bucket"
        remote_file_key = "/foo/bar/baz"
        download_file_path = "/tmp/download"
        endpoint_url = "http://foo.s3.amazon.com"

        with mock.patch("boto3.session.Session") as mock_boto_session:
            resource = mock_boto_session.return_value.resource
            s3 = resource.return_value
            s3.Object.side_effect = exception

            with pytest.raises(raised_type):
                S3ClientWrapper.download_file(
                    bucket_name,
                    endpoint_url,
                    remote_file_key,
                    download_file_path,
                )

    def test_download__404(self):
        """Verify 404 is transformed into a ValueError"""
        thrown_ex = botocore.exceptions.ClientError(
            {"Error": {"Code": "404"}}, "S#Download"
        )
        self._download_error_test(thrown_ex, ValueError)

    def test_other_botocore_error(self):
        """Verify another botocore error is raised straight"""
        thrown_ex = botocore.exceptions.ClientError(
            {"Error": {"Code": "500"}}, "S#Download"
        )
        self._download_error_test(thrown_ex, botocore.exceptions.ClientError)

    def test_download__error(self):
        """Verify a non-botocore error is raised straight"""
        ex = ValueError("boom")
        self._download_error_test(ex, ex.__class__)

    @mock.patch.object(remote_file_storage_wrappers, "os")
    def test_upload__path_doesnt_exist(self, mock_os):
        mock_os.path.isfile.return_value = False

        upload_path = "/tmp/upload_file"
        with pytest.raises(ValueError) as ex_cm:
            S3ClientWrapper.upload_file(
                "foo_bucket", "http://foo.s3.amazon.com", "/foo/bar/baz", upload_path
            )
        assert "does not exist" in str(ex_cm.value)
        mock_os.path.isfile.assert_called_once_with(upload_path)

    @staticmethod
    def _upload_test(**kwargs):
        bucket_name = "foo_bucket"
        remote_file_key = "foo/bar/baz"
        upload_file_path = "/tmp/upload_file"
        endpoint_url = "http://foo.s3.amazon.com"

        with mock.patch(
            "boto3.session.Session"
        ) as mock_boto_session, mock.patch.object(
            S3ClientWrapper, "_create_progress_callback_func"
        ) as mock_create_progress_callback, mock.patch(
            "boto3.s3.transfer.TransferConfig"
        ) as mock_TransferConfig, mock.patch.object(
            remote_file_storage_wrappers, "os"
        ) as mock_os:
            mock_os.stat.return_value = mock.Mock(st_size=1234)
            returned_upload_path = S3ClientWrapper.upload_file(
                bucket_name, endpoint_url, remote_file_key, upload_file_path, **kwargs
            )

            if "profile_name" in kwargs:
                mock_boto_session.assert_called_once_with(
                    profile_name=kwargs["profile_name"]
                )
            else:
                mock_boto_session.assert_called_once_with(**kwargs["credentials"])

            resource = mock_boto_session.return_value.resource
            resource.assert_called_once_with("s3", endpoint_url=endpoint_url)
            s3 = resource.return_value
            s3.Bucket.assert_called_once_with(bucket_name)
            s3_bucket = s3.Bucket.return_value

            mock_TransferConfig.assert_called_once_with(
                **kwargs.get("transfer_config_kwargs", {})
            )
            transfer_config = mock_TransferConfig.return_value

            if kwargs.get("show_progress", True):
                mock_os.stat.assert_called_once_with(upload_file_path)

                mock_os.path.basename.assert_called_once_with(upload_file_path)
            else:
                assert not mock_create_progress_callback.called

            s3_bucket.upload_file.assert_called_once_with(
                upload_file_path,
                remote_file_key,
                Callback=mock.ANY,
                Config=transfer_config,
                ExtraArgs={"ACL": "bucket-owner-full-control"},
            )

            # why do we return something we passed...?
            assert upload_file_path == returned_upload_path

    def test_upload__profile(self):
        """Verify uploading using a profile name passes through to to the session."""
        self._upload_test(
            profile_name="foo", transfer_config_kwargs={"max_concurency": 10}
        )

    def test_upload__credentials(self):
        """Verify uploading using a supplied credential dictionary"""
        credentials = {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_session_token": "baz",
        }
        self._upload_test(credentials=credentials, show_progress=False)


class TestSftpClientWrapper:
    @mock.patch.object(remote_file_storage_wrappers, "_retry_pysftp_connection")
    @mock.patch.object(remote_file_storage_wrappers, "os")
    def test_download_file(self, mock_os, mock_retry_pysftp_connection) -> None:
        """
        Verify the download_file method that pass in the callback function according to the boolean show_progress
        """

        mock_sftp = mock.Mock()
        mock_retry_pysftp_connection.return_value.__enter__.return_value = mock_sftp
        mock_url = "sftp://foo.com:/bar/baz"
        mock_local_file_path = "test_path"
        mock_os.path.isdir.return_value = False

        path = urllib_parse.unquote(SFTPWrapper._parse_for_sftp(mock_url).path)

        SFTPWrapper.download_file(mock_url, localFilepath=mock_local_file_path)
        mock_sftp.get.assert_called_once_with(
            path, mock_local_file_path, preserve_mtime=True, callback=None
        )

        SFTPWrapper.download_file(
            mock_url,
            localFilepath=mock_local_file_path,
        )
        mock_sftp.get.assert_called_with(
            path,
            mock_local_file_path,
            preserve_mtime=True,
            callback=mock.ANY,
        )

        # test if localFilepath is None
        mock_os.getcwd.return_value = "/home/foo"
        SFTPWrapper.download_file(mock_url)
        mock_sftp.get.assert_called_with(
            path, "/home/foo", preserve_mtime=True, callback=None
        )

        # test if mock_os.path.isdir is True
        mock_os.path.isdir.return_value = True
        mock_os.path.join.return_value = "/home/foo/bar"
        SFTPWrapper.download_file(mock_url, localFilepath=mock_local_file_path)
        mock_sftp.get.assert_called_with(
            path,
            "/home/foo/bar",
            preserve_mtime=True,
            callback=mock.ANY,
        )

    @mock.patch.object(remote_file_storage_wrappers, "_retry_pysftp_connection")
    def test_upload_file(self, mock_retry_pysftp_connection) -> None:
        """
        Verify the upload_file method that working correctly with valid input path and url
        """

        mock_sftp = mock.Mock()
        with mock.patch.object(mock_sftp, "cd") as mock_cd:
            mock_retry_pysftp_connection.return_value.__enter__.return_value = mock_sftp
            mock_url = "sftp://foo.com:/bar/baz"
            mock_local_file_path = "/home/foo/bar"
            parsed_URL = SFTPWrapper._parse_for_sftp(mock_url)
            mock_cd.return_value.__enter__.return_value = mock.Mock()

            SFTPWrapper.upload_file(mock_local_file_path, mock_url)
            mock_sftp.makedirs.call_once_with(parsed_URL.path)
            mock_cd.call_once_with(parsed_URL.path)
            mock_sftp.put.assert_called_once_with(
                mock_local_file_path,
                preserve_mtime=True,
                callback=mock.ANY,
            )
