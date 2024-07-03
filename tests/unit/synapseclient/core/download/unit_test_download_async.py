"""Unit tests for synapseclient.core.download.download_async."""

import datetime
import unittest.mock as mock

import pytest

import synapseclient.core.download.download_async as download_async
from synapseclient import Synapse
from synapseclient.core.download import (
    DownloadRequest,
    PresignedUrlInfo,
    PresignedUrlProvider,
)


class TestPresignedUrlProvider:
    """Unit tests for PresignedUrlProvider."""

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        """Setup"""
        self.mock_synapse_client = mock.create_autospec(Synapse)
        self.download_request = DownloadRequest(123, "456", "FileEntity", "/myFakepath")

    async def test_get_info_not_expired(self) -> None:
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)

        info = PresignedUrlInfo(
            "myFile.txt",
            "https://synapse.org/somefile.txt",
            expiration_utc=utc_now + datetime.timedelta(seconds=6),
        )

        with mock.patch.object(
            PresignedUrlProvider, "_get_pre_signed_info", return_value=info
        ) as mock_get_presigned_info, mock.patch.object(
            download_async, "datetime", wraps=datetime
        ) as mock_datetime:
            mock_datetime.datetime.now.return_value = utc_now

            presigned_url_provider = PresignedUrlProvider(
                self.mock_synapse_client, self.download_request
            )
            presigned_url_provider._cached_info = info
            assert info == presigned_url_provider.get_info()

            mock_get_presigned_info.assert_not_called()
            mock_datetime.datetime.now.assert_called_once()

    async def test_get_info_expired(self) -> None:
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)

        # expires in the past
        expired_info = PresignedUrlInfo(
            "myFile.txt",
            "https://synapse.org/somefile.txt",
            expiration_utc=utc_now - datetime.timedelta(seconds=5),
        )
        unexpired_date = utc_now + datetime.timedelta(seconds=6)
        unexpired_info = PresignedUrlInfo(
            file_name="myFile.txt",
            url="https://synapse.org/somefile.txt",
            expiration_utc=unexpired_date,
        )

        with mock.patch.object(
            PresignedUrlProvider,
            "_get_pre_signed_info",
            side_effect=[unexpired_info],
        ) as mock_get_presigned_info, mock.patch(
            "synapseclient.core.download.download_async.get_file_handle_for_download",
            return_value={
                "fileHandle": {"fileName": "myFile.txt"},
                "preSignedURL": f"https://synapse.org?X-Amz-Date={unexpired_date.strftime('%Y%m%dT%H%M%SZ')}&X-Amz-Expires=5&X-Amz-Signature=123456",
            },
        ):
            presigned_url_provider = PresignedUrlProvider(
                self.mock_synapse_client, request=self.download_request
            )
            presigned_url_provider._cached_info = expired_info
            info = presigned_url_provider.get_info()
            assert unexpired_info == info

            assert 1 == mock_get_presigned_info.call_count

    async def test_get_pre_signed_info(self) -> None:
        fake_exp_time = datetime.datetime.now(tz=datetime.timezone.utc)
        fake_url = "https://synapse.org/foo.txt"
        fake_file_name = "foo.txt"

        with mock.patch.object(
            download_async,
            "_pre_signed_url_expiration_time",
            return_value=fake_exp_time,
        ) as mock_pre_signed_url_expiration_time, mock.patch(
            "synapseclient.core.download.download_async.get_file_handle_for_download",
            return_value={
                "fileHandle": {"fileName": "myFile.txt"},
                "preSignedURL": f"https://synapse.org?X-Amz-Date={fake_exp_time.strftime('%Y%m%dT%H%M%SZ')}&X-Amz-Expires=5&X-Amz-Signature=123456",
            },
        ) as mock_file_handle_download:
            fake_file_handle_response = {
                "fileHandle": {"fileName": fake_file_name},
                "preSignedURL": fake_url,
            }

            mock_file_handle_download.return_value = fake_file_handle_response

            presigned_url_provider = PresignedUrlProvider(
                self.mock_synapse_client, self.download_request
            )

            expected = PresignedUrlInfo(
                file_name=fake_file_name, url=fake_url, expiration_utc=fake_exp_time
            )
            assert expected == presigned_url_provider._get_pre_signed_info()

            mock_pre_signed_url_expiration_time.assert_called_with(fake_url)
            mock_file_handle_download.assert_called_with(
                file_handle_id=self.download_request.file_handle_id,
                synapse_id=self.download_request.object_id,
                entity_type=self.download_request.object_type,
                synapse_client=self.mock_synapse_client,
            )

    async def test_pre_signed_url_expiration_time(self) -> None:
        url = (
            "https://s3.amazonaws.com/examplebucket/test.txt"
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            "&X-Amz-Credential=your-access-key-id/20130721/us-east-1/s3/aws4_request"
            "&X-Amz-Date=20130721T201207Z"
            "&X-Amz-Expires=86400"
            "&X-Amz-SignedHeaders=host"
            "&X-Amz-Signature=signature-value"
        )

        expected = (
            datetime.datetime(year=2013, month=7, day=21, hour=20, minute=12, second=7)
            + datetime.timedelta(seconds=86400)
        ).replace(tzinfo=datetime.timezone.utc)
        assert expected == download_async._pre_signed_url_expiration_time(url)


async def test_generate_chunk_ranges() -> None:
    # test using smaller chunk size
    download_async.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE = 8

    result = [x for x in download_async._generate_chunk_ranges(18)]

    expected = [(0, 7), (8, 15), (16, 17)]

    assert expected == result
