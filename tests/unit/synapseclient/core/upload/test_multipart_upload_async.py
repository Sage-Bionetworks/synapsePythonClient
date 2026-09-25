from unittest import mock

import httpx
import pytest

from synapseclient.core.upload.multipart_upload_async import (
    HandlePartResult,
    UploadAttemptAsync,
)
from synapseclient.core.utils import md5_fn

PART_SIZE = 256
PART_NUMBER = 1


def _init_upload_attempt(syn):
    upload_request_payload = {
        "concreteType": "org.sagebionetworks.repo.model.file.MultipartUploadRequest",
        "contentMD5Hex": "abc",
        "contentType": "application/text",
        "fileName": "target.txt",
        "fileSizeBytes": 1024,
        "generatePreview": False,
        "storageLocationId": "1234",
        "partSizeBytes": PART_SIZE,
    }

    def part_request_body_provider_fn(part_number):
        return (f"{part_number}" * PART_SIZE).encode("utf-8")

    upload = UploadAttemptAsync(
        syn,
        "target.txt",
        upload_request_payload,
        part_request_body_provider_fn,
        md5_fn,
        False,
    )
    upload._upload_id = "123"
    upload._pre_signed_part_urls = {
        PART_NUMBER: ("https://foo.com/1", {"a": 1}),
    }
    return upload


class TestPutPartWithRetry:
    """Regression coverage for the retry paths inside
    UploadAttemptAsync._put_part_with_retry. multipart_upload_async.py previously
    passed requests.exceptions.ConnectionError as the retryable exception allowlist,
    but the session performing the PUT is an httpx.Client, which never raises that
    exception type -- so a transient httpx connection error skipped the retry
    window entirely and failed the part immediately.
    """

    @pytest.mark.parametrize(
        "exception",
        [
            httpx.ConnectError("connection refused"),
            httpx.ReadError("broken"),
            httpx.ReadTimeout("timed out"),
            httpx.ConnectTimeout("timed out"),
            httpx.RemoteProtocolError("disconnected"),
        ],
    )
    def test_handle_part__httpx_connection_error_then_success(self, syn, exception):
        upload = _init_upload_attempt(syn)
        mock_session = mock.Mock()
        mock_session.put.side_effect = [exception, mock.Mock(status_code=200)]

        with mock.patch.object(syn, "_requests_session_storage", mock_session):
            result = upload._handle_part(PART_NUMBER)

        assert mock_session.put.call_count == 2
        body = (f"{PART_NUMBER}" * PART_SIZE).encode("utf-8")
        assert result == HandlePartResult(PART_NUMBER, PART_SIZE, md5_fn(body, None))

    def test_handle_part__retryable_status_then_success(self, syn):
        upload = _init_upload_attempt(syn)
        mock_session = mock.Mock()
        mock_503 = mock.Mock(status_code=503, headers={}, text="")
        mock_session.put.side_effect = [mock_503, mock.Mock(status_code=200)]

        with mock.patch.object(syn, "_requests_session_storage", mock_session):
            upload._handle_part(PART_NUMBER)

        assert mock_session.put.call_count == 2

    def test_handle_part__expired_url_then_success(self, syn):
        upload = _init_upload_attempt(syn)
        mock_session = mock.Mock()
        mock_403 = mock.Mock(status_code=403, headers={}, text="")
        mock_session.put.side_effect = [mock_403, mock.Mock(status_code=200)]

        with (
            mock.patch.object(syn, "_requests_session_storage", mock_session),
            mock.patch.object(
                upload,
                "_refresh_pre_signed_part_urls",
                return_value=("https://bar.com/1", {"a": 2}),
            ) as refresh_urls,
        ):
            upload._handle_part(PART_NUMBER)

        refresh_urls.assert_called_once_with(PART_NUMBER, "https://foo.com/1")
        assert mock_session.put.call_count == 2

    def test_handle_part__non_retryable_exception_fails_immediately(self, syn):
        upload = _init_upload_attempt(syn)
        mock_session = mock.Mock()
        mock_session.put.side_effect = ValueError("boom")

        with mock.patch.object(syn, "_requests_session_storage", mock_session):
            with pytest.raises(ValueError):
                upload._handle_part(PART_NUMBER)

        assert mock_session.put.call_count == 1
