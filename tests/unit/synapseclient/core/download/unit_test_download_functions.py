"""Unit tests for synapseclient.core.download.download_functions"""

import datetime
import os
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import File, Synapse
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.download import (
    PresignedUrlInfo,
    download_file_entity,
    download_functions,
    ensure_download_location_is_directory,
)
from synapseclient.core.download.download_async import (
    SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE,
)


def _presigned_url(seconds_until_expiry: int = 3600) -> str:
    """Build a syntactically valid AWS pre-signed URL that expires in the future."""
    made = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
        seconds=1
    )
    return (
        "https://s3.amazonaws.com/proddata/bucket/key.txt"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        f"&X-Amz-Date={made.strftime('%Y%m%dT%H%M%SZ')}"
        f"&X-Amz-Expires={seconds_until_expiry}"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=signature-value"
    )


def test_ensure_download_location_is_directory() -> None:
    download_location = "/foo/bar/baz"
    with patch("synapseclient.core.download.download_functions.os") as mock_os:
        mock_os.path.isfile.return_value = False
        ensure_download_location_is_directory(download_location)

        mock_os.path.isfile.return_value = True
        with pytest.raises(ValueError):
            ensure_download_location_is_directory(download_location)


async def test_download_file_entity_correct_local_state(syn: Synapse) -> None:
    mock_cache_path = utils.normalize_path("/i/will/show/you/the/path/yi.txt")
    file_entity = File(parentId="syn123")
    file_entity.dataFileHandleId = 123
    with patch.object(syn.cache, "get", return_value=mock_cache_path):
        await download_file_entity(
            download_location=None,
            entity=file_entity,
            if_collision="overwrite.local",
            submission=None,
            synapse_client=syn,
        )
        assert mock_cache_path == utils.normalize_path(file_entity.path)
        assert os.path.dirname(mock_cache_path) == file_entity.cacheDir
        assert 1 == len(file_entity.files)
        assert os.path.basename(mock_cache_path) == file_entity.files[0]


class TestMultiThreadedPresignedUrlReuse:
    """Tests that a multi-threaded download reuses the pre-signed URL already
    fetched in ``download_by_file_handle`` instead of requesting a second one,
    while still passing the file handle identifiers needed to refetch on expiry.
    """

    @pytest.fixture(autouse=True)
    def _large_s3_file_handle(self, monkeypatch: pytest.MonkeyPatch) -> dict:
        """A file handle for a large S3 file that routes to the multi-threaded path."""
        self.presigned_url = _presigned_url()
        self.file_handle_result = {
            "preSignedURL": self.presigned_url,
            "fileHandle": {
                "id": "9999",
                "fileName": "big_file.txt",
                "concreteType": concrete_types.S3_FILE_HANDLE,
                "contentSize": SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE + 1,
                "contentMd5": "abc123",
                "storageLocationId": 1,
            },
        }
        return self.file_handle_result

    async def test_multi_threaded_download_fetches_url_once_and_forwards_it(
        self, syn: Synapse, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """The metadata/pre-signed batch endpoint is hit exactly once and the
        resulting URL is forwarded to the multi-threaded downloader (so it does
        not request a second one)."""
        monkeypatch.setattr(syn, "multi_threaded", True)
        destination = str(tmp_path / "big_file.txt")

        with (
            patch.object(
                download_functions,
                "get_file_handle_for_download_async",
                new=AsyncMock(return_value=self.file_handle_result),
            ) as mock_get_handle,
            patch.object(
                download_functions,
                "download_from_url_multi_threaded",
                new=AsyncMock(return_value=destination),
            ) as mock_multi_threaded,
            patch.object(
                download_functions.sts_transfer,
                "is_boto_sts_transfer_enabled",
                return_value=False,
            ),
            patch.object(syn.cache, "add"),
        ):
            await download_functions.download_by_file_handle(
                file_handle_id="9999",
                synapse_id="syn123",
                entity_type="FileEntity",
                destination=destination,
                synapse_client=syn,
            )

        # The pre-signed URL / metadata batch call is made exactly once.
        assert mock_get_handle.call_count == 1

        # The already-fetched URL is forwarded so the downloader does not fetch again.
        assert mock_multi_threaded.call_count == 1
        forwarded = mock_multi_threaded.call_args.kwargs["presigned_url"]
        assert isinstance(forwarded, PresignedUrlInfo)
        assert forwarded.url == self.presigned_url
        assert forwarded.file_name == "big_file.txt"

        # File handle identifiers are still passed so a refetch is possible on expiry.
        assert mock_multi_threaded.call_args.kwargs["file_handle_id"] == "9999"
        assert mock_multi_threaded.call_args.kwargs["object_id"] == "syn123"
        assert mock_multi_threaded.call_args.kwargs["object_type"] == "FileEntity"


class TestDownloadFromUrlMultiThreadedRequest:
    """Tests for the DownloadRequest constructed by download_from_url_multi_threaded."""

    async def test_seeded_url_request_carries_file_handle_identifiers(
        self, syn: Synapse, tmp_path
    ) -> None:
        """When a pre-signed URL is supplied along with file handle identifiers,
        the DownloadRequest carries both so the downloader can refetch on expiry."""
        presigned = PresignedUrlInfo(
            file_name="big_file.txt",
            url=_presigned_url(),
            expiration_utc=datetime.datetime.now(tz=datetime.timezone.utc)
            + datetime.timedelta(hours=1),
        )
        captured = {}

        async def _capture(client, download_request) -> None:
            captured["request"] = download_request
            # Create the temp file so the post-download move succeeds.
            open(download_request.path, "wb").close()

        with patch.object(download_functions, "download_file", new=_capture):
            await download_functions.download_from_url_multi_threaded(
                destination=str(tmp_path / "big_file.txt"),
                file_handle_id="9999",
                object_id="syn123",
                object_type="FileEntity",
                presigned_url=presigned,
                synapse_client=syn,
            )

        request = captured["request"]
        assert request.presigned_url is presigned
        assert request.file_handle_id == 9999
        assert request.object_id == "syn123"
        assert request.object_type == "FileEntity"

    async def test_seeded_url_request_without_identifiers_preserves_wiki_behavior(
        self, syn: Synapse, tmp_path
    ) -> None:
        """Wiki callers pass a pre-signed URL without file handle identifiers.
        These must remain None (and int(None) must not be attempted)."""
        presigned = PresignedUrlInfo(
            file_name="attachment.txt",
            url=_presigned_url(),
            expiration_utc=datetime.datetime.now(tz=datetime.timezone.utc)
            + datetime.timedelta(hours=1),
        )
        captured = {}

        async def _capture(client, download_request) -> None:
            captured["request"] = download_request
            open(download_request.path, "wb").close()

        with patch.object(download_functions, "download_file", new=_capture):
            await download_functions.download_from_url_multi_threaded(
                destination=str(tmp_path),
                presigned_url=presigned,
                synapse_client=syn,
            )

        request = captured["request"]
        assert request.presigned_url is presigned
        assert request.file_handle_id is None
        assert request.object_id is None
        assert request.object_type is None
