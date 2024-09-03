"""Unit tests for downloads."""

import datetime
import hashlib
import json
import os
import shutil
import tempfile
from typing import Dict
from unittest.mock import ANY, AsyncMock, MagicMock, call, mock_open, patch

import pytest
import requests

import synapseclient.core.constants.concrete_types as concrete_types
import synapseclient.core.download.download_async as download_async
from synapseclient import Synapse
from synapseclient.api import get_file_handle_for_download_async
from synapseclient.core import utils
from synapseclient.core.download import (
    download_by_file_handle,
    download_from_url,
    download_from_url_multi_threaded,
)
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseMd5MismatchError,
)

GET_FILE_HANDLE_FOR_DOWNLOAD = (
    "synapseclient.core.download.download_functions.get_file_handle_for_download_async"
)
DOWNLOAD_FROM_URL = "synapseclient.core.download.download_functions.download_from_url"

FILE_HANDLE_ID = "42"
OBJECT_ID = "syn789"
OBJECT_TYPE = "FileEntity"


# a callable that mocks the requests.get function
class MockRequestGetFunction(object):
    def __init__(self, responses):
        self.responses = responses
        self.i = 0

    def __call__(self, *args, **kwargs):
        response = self.responses[self.i]
        self.i += 1
        return response


# a class to iterate bogus content
class IterateContents(object):
    def __init__(self, contents, buffer_size, partial_start=0, partial_end=None):
        self.contents = contents
        self.buffer_size = buffer_size
        self.i = partial_start
        self.partial_end = partial_end
        self.bytes_iterated = 0

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.i >= len(self.contents):
            raise StopIteration()
        if self.partial_end and self.i >= self.partial_end:
            raise requests.exceptions.ChunkedEncodingError(
                "Simulated partial download! Connection reset by peer!"
            )
        start = self.i
        end = min(self.i + self.buffer_size, len(self.contents))
        if self.partial_end:
            end = min(end, self.partial_end)
        self.i = end
        data = self.contents[start:end].encode("utf-8")
        self.bytes_iterated += len(data)
        return data

    def total_bytes_iterated(self):
        return self.bytes_iterated


def create_mock_response(url, response_type, **kwargs):
    response = MagicMock()

    response.request.url = url
    response.request.method = kwargs.get("method", "GET")
    response.request.headers = {}
    response.request.body = None

    if response_type == "redirect":
        response.status_code = 301
        response.headers = {"location": kwargs["location"]}
    elif response_type == "error":
        response.status_code = kwargs.get("status_code", 500)
        response.reason = kwargs.get("reason", "fake reason")
        response.text = '{{"reason":"{}"}}'.format(kwargs.get("reason", "fake reason"))
        response.json = lambda: json.loads(response.text)
    elif response_type == "stream":
        response.status_code = kwargs.get("status_code", 200)
        response.headers = {
            "content-disposition": 'attachment; filename="fname.ext"',
            "content-type": "application/octet-stream",
            "content-length": len(response.text),
        }

        def _create_iterator(buffer_size):
            response._content_iterator = IterateContents(
                kwargs["contents"],
                kwargs["buffer_size"],
                kwargs.get("partial_start", 0),
                kwargs.get("partial_end", None),
            )
            return response._content_iterator

        response.iter_content = _create_iterator
        response.raw.tell = lambda: response._content_iterator.total_bytes_iterated()
    else:
        response.status_code = 200
        response.text = kwargs["text"]
        response.json = lambda: json.loads(response.text)
        response.headers = {
            "content-type": "application/json",
            "content-length": len(response.text),
        }

    return response


def mock_generate_headers(self, headers=None):
    return {}


async def test_mock_download(syn: Synapse) -> None:
    temp_dir = tempfile.gettempdir()

    # make bogus content
    contents = "\n".join(str(i) for i in range(1000))

    # compute MD5 of contents
    m = hashlib.md5()
    m.update(contents.encode("utf-8"))
    contents_md5 = m.hexdigest()

    url = "https://repo-prod.prod.sagebase.org/repo/v1/entity/syn6403467/file"

    # 1. No redirects
    mock_requests_get = MockRequestGetFunction(
        [create_mock_response(url, "stream", contents=contents, buffer_size=1024)]
    )

    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(Synapse, "_generate_headers", side_effect=mock_generate_headers),
    ):
        download_from_url(
            url=url,
            destination=temp_dir,
            entity_id=OBJECT_ID,
            file_handle_associate_type=OBJECT_TYPE,
            file_handle_id=12345,
            expected_md5=contents_md5,
            synapse_client=syn,
        )

    # 2. Multiple redirects
    mock_requests_get = MockRequestGetFunction(
        [
            create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
            create_mock_response(url, "redirect", location="https://fakeurl.com/qwer"),
            create_mock_response(url, "stream", contents=contents, buffer_size=1024),
        ]
    )

    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(Synapse, "_generate_headers", side_effect=mock_generate_headers),
    ):
        download_from_url(
            url=url,
            destination=temp_dir,
            entity_id=OBJECT_ID,
            file_handle_associate_type=OBJECT_TYPE,
            file_handle_id=12345,
            expected_md5=contents_md5,
            synapse_client=syn,
        )

    # 3. recover from partial download
    mock_requests_get = MockRequestGetFunction(
        [
            create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
            create_mock_response(
                url,
                "stream",
                contents=contents,
                buffer_size=1024,
                partial_end=len(contents) // 7 * 3,
                status_code=200,
            ),
            create_mock_response(
                url,
                "stream",
                contents=contents,
                buffer_size=1024,
                partial_start=len(contents) // 7 * 3,
                partial_end=len(contents) // 7 * 5,
                status_code=206,
            ),
            create_mock_response(
                url,
                "stream",
                contents=contents,
                buffer_size=1024,
                partial_start=len(contents) // 7 * 5,
                status_code=206,
            ),
        ]
    )

    _getFileHandleDownload_return_value = {
        "preSignedURL": url,
        "fileHandle": {
            "id": 12345,
            "contentMd5": contents_md5,
            "concreteType": concrete_types.S3_FILE_HANDLE,
        },
    }

    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(syn, "_generate_headers", side_effect=mock_generate_headers),
        patch(
            GET_FILE_HANDLE_FOR_DOWNLOAD,
            new_callable=AsyncMock,
            return_value=_getFileHandleDownload_return_value,
        ),
    ):
        await download_by_file_handle(
            file_handle_id=FILE_HANDLE_ID,
            synapse_id=OBJECT_ID,
            entity_type=OBJECT_TYPE,
            destination=temp_dir,
            synapse_client=syn,
        )

    # 4. as long as we're making progress, keep trying
    responses = [
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(
            url,
            "stream",
            contents=contents,
            buffer_size=1024,
            partial_start=0,
            partial_end=len(contents) // 11,
            status_code=200,
        ),
    ]
    for i in range(1, 12):
        responses.append(
            create_mock_response(
                url,
                "stream",
                contents=contents,
                buffer_size=1024,
                partial_start=len(contents) // 11 * i,
                partial_end=len(contents) // 11 * (i + 1),
                status_code=206,
            )
        )
    mock_requests_get = MockRequestGetFunction(responses)

    # TODO: When swapping out for the HTTPX client, we will need to update this test
    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    # with patch.object(
    #     syn, "rest_get_async", new_callable=AsyncMock, side_effect=mock_requests_get
    # )
    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(Synapse, "_generate_headers", side_effect=mock_generate_headers),
        patch(
            GET_FILE_HANDLE_FOR_DOWNLOAD,
            new_callable=AsyncMock,
            return_value=_getFileHandleDownload_return_value,
        ),
    ):
        await download_by_file_handle(
            file_handle_id=FILE_HANDLE_ID,
            synapse_id=OBJECT_ID,
            entity_type=OBJECT_TYPE,
            destination=temp_dir,
            synapse_client=syn,
        )

    # 5. don't recover, a partial download that never completes
    #    should eventually throw an exception
    responses = [
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(
            url,
            "stream",
            contents=contents,
            buffer_size=1024,
            partial_start=0,
            partial_end=len(contents) // 11,
            status_code=200,
        ),
    ]
    for i in range(1, 10):
        responses.append(
            create_mock_response(
                url,
                "stream",
                contents=contents,
                buffer_size=1024,
                partial_start=len(contents) // 11,
                partial_end=len(contents) // 11,
                status_code=200,
            )
        )
    mock_requests_get = MockRequestGetFunction(responses)

    # TODO: When swapping out for the HTTPX client, we will need to update this test
    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    # with patch.object(
    #     syn, "rest_get_async", new_callable=AsyncMock, side_effect=mock_requests_get
    # )
    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(Synapse, "_generate_headers", side_effect=mock_generate_headers),
        patch(
            GET_FILE_HANDLE_FOR_DOWNLOAD,
            new_callable=AsyncMock,
            return_value=_getFileHandleDownload_return_value,
        ),
    ):
        with pytest.raises(Exception):
            await download_by_file_handle(
                file_handle_id=FILE_HANDLE_ID,
                synapse_id=OBJECT_ID,
                entity_type=OBJECT_TYPE,
                destination=temp_dir,
                synapse_client=syn,
            )

    # 6. 206 Range header not supported, respond with 200 and full file
    mock_requests_get = MockRequestGetFunction(
        [
            create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
            create_mock_response(
                url,
                "stream",
                contents=contents,
                buffer_size=1024,
                partial=len(contents) // 7 * 3,
                status_code=200,
            ),
            create_mock_response(
                url, "stream", contents=contents, buffer_size=1024, status_code=200
            ),
        ]
    )

    # TODO: When swapping out for the HTTPX client, we will need to update this test
    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    # with patch.object(
    #     syn, "rest_get_async", new_callable=AsyncMock, side_effect=mock_requests_get
    # )
    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(Synapse, "_generate_headers", side_effect=mock_generate_headers),
        patch(
            GET_FILE_HANDLE_FOR_DOWNLOAD,
            new_callable=AsyncMock,
            return_value=_getFileHandleDownload_return_value,
        ),
    ):
        await download_by_file_handle(
            file_handle_id=FILE_HANDLE_ID,
            synapse_id=OBJECT_ID,
            entity_type=OBJECT_TYPE,
            destination=temp_dir,
            synapse_client=syn,
        )

    # 7. Too many redirects
    mock_requests_get = MockRequestGetFunction(
        [
            create_mock_response(url, "redirect", location="https://fakeurl.com/asdf")
            for i in range(100)
        ]
    )

    # TODO: When swapping out for the HTTPX client, we will need to update this test
    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    # with patch.object(
    #     syn, "rest_get_async", new_callable=AsyncMock, side_effect=mock_requests_get
    # )
    with (
        patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
        patch.object(Synapse, "_generate_headers", side_effect=mock_generate_headers),
        patch(
            GET_FILE_HANDLE_FOR_DOWNLOAD,
            new_callable=AsyncMock,
            return_value=_getFileHandleDownload_return_value,
        ),
    ):
        with pytest.raises(SynapseHTTPError):
            await download_by_file_handle(
                file_handle_id=FILE_HANDLE_ID,
                synapse_id=OBJECT_ID,
                entity_type=OBJECT_TYPE,
                destination=temp_dir,
                synapse_client=syn,
            )


class TestDownloadFileHandle:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def teardown_method(self) -> None:
        self.syn.multi_threaded = False

    async def test_multithread_true_s3_file_handle(self) -> None:
        with (
            patch.object(os, "makedirs"),
            patch(
                GET_FILE_HANDLE_FOR_DOWNLOAD,
                new_callable=AsyncMock,
            ) as mock_getFileHandleDownload,
            patch(
                "synapseclient.core.download.download_functions.download_from_url_multi_threaded",
                new_callable=AsyncMock,
            ) as mock_multi_thread_download,
            patch.object(self.syn, "cache"),
        ):
            mock_getFileHandleDownload.return_value = {
                "fileHandle": {
                    "id": "123",
                    "concreteType": concrete_types.S3_FILE_HANDLE,
                    "contentMd5": "someMD5",
                    "contentSize": download_async.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE
                    + 1,
                }
            }

            self.syn.multi_threaded = True
            await download_by_file_handle(
                file_handle_id=123,
                synapse_id=456,
                entity_type="FileEntity",
                destination="/myfakepath",
                synapse_client=self.syn,
            )

            mock_multi_thread_download.assert_called_once_with(
                file_handle_id=123,
                object_id=456,
                object_type="FileEntity",
                destination="/myfakepath",
                expected_md5="someMD5",
                synapse_client=self.syn,
            )

    async def _multithread_not_applicable(self, file_handle: Dict[str, str]) -> None:
        get_file_handle_for_download_return_value = {
            "fileHandle": file_handle,
            "preSignedURL": "asdf.com",
        }

        with (
            patch.object(os, "makedirs"),
            patch(
                GET_FILE_HANDLE_FOR_DOWNLOAD,
                new_callable=AsyncMock,
                return_value=get_file_handle_for_download_return_value,
            ),
            patch(
                DOWNLOAD_FROM_URL,
                new_callable=AsyncMock,
            ) as mock_download_from_URL,
            patch.object(self.syn, "cache"),
        ):
            # multi_threaded/max_threads will have effect
            self.syn.multi_threaded = True
            await download_by_file_handle(
                file_handle_id=123,
                synapse_id=456,
                entity_type="FileEntity",
                destination="/myfakepath",
                synapse_client=self.syn,
            )

            mock_download_from_URL.assert_called_once_with(
                url="asdf.com",
                destination="/myfakepath",
                entity_id=456,
                file_handle_associate_type="FileEntity",
                file_handle_id="123",
                expected_md5="someMD5",
                progress_bar=ANY,
                synapse_client=self.syn,
            )

    async def test_multithread_true_other_file_handle_type(self) -> None:
        """Verify that even if multithreaded is enabled we won't use it for unsupported file types"""
        file_handle = {
            "id": "123",
            "concreteType": "someFakeConcreteType",
            "contentMd5": "someMD5",
        }
        await self._multithread_not_applicable(file_handle)

    async def test_multithread_false_s3_file_handle_small_file(self) -> None:
        """Verify that even if multithreaded is enabled we still won't use a multithreaded
        download if the file is not large enough to make it worthwhile"""
        file_handle = {
            "id": "123",
            "concreteType": concrete_types.S3_FILE_HANDLE,
            "contentMd5": "someMD5",
            "contentSize": download_async.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE - 1,
        }
        await self._multithread_not_applicable(file_handle)

    async def test_multithread_false_s3_file_handle(self) -> None:
        with (
            patch.object(os, "makedirs"),
            patch(
                GET_FILE_HANDLE_FOR_DOWNLOAD,
                new_callable=AsyncMock,
            ) as mock_getFileHandleDownload,
            patch(
                DOWNLOAD_FROM_URL,
                new_callable=AsyncMock,
            ) as mock_download_from_URL,
            patch.object(self.syn, "cache"),
        ):
            mock_getFileHandleDownload.return_value = {
                "fileHandle": {
                    "id": "123",
                    "concreteType": concrete_types.S3_FILE_HANDLE,
                    "contentMd5": "someMD5",
                },
                "preSignedURL": "asdf.com",
            }

            self.syn.multi_threaded = False
            await download_by_file_handle(
                file_handle_id=123,
                synapse_id=456,
                entity_type="FileEntity",
                destination="/myfakepath",
                synapse_client=self.syn,
            )

            mock_download_from_URL.assert_called_once_with(
                url="asdf.com",
                destination="/myfakepath",
                entity_id=456,
                file_handle_associate_type="FileEntity",
                file_handle_id="123",
                expected_md5="someMD5",
                progress_bar=ANY,
                synapse_client=self.syn,
            )


class TestDownloadFromUrlMultiThreaded:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_md5_mismatch(self) -> None:
        with (
            patch("synapseclient.core.download.download_functions.download_file"),
            patch.object(utils, "md5_for_file") as mock_md5_for_file,
            patch.object(os, "remove") as mock_os_remove,
            patch.object(shutil, "move") as mock_move,
        ):
            path = os.path.abspath("/myfakepath")

            mock_md5_for_file.return_value.hexdigest.return_value = "unexpetedMd5"

            with pytest.raises(SynapseMd5MismatchError):
                await download_from_url_multi_threaded(
                    file_handle_id=123,
                    object_id=456,
                    object_type="FileEntity",
                    destination=path,
                    expected_md5="myExpectedMd5",
                    synapse_client=self.syn,
                )

            mock_os_remove.assert_called_once_with(
                utils.temp_download_filename(path, 123)
            )
            mock_move.assert_not_called()

    async def test_md5_match(self) -> None:
        expected_md5 = "myExpectedMd5"

        with (
            patch("synapseclient.core.download.download_functions.download_file"),
            patch.object(
                utils,
                "md5_for_file_hex",
                return_value=expected_md5,
            ),
            patch.object(os, "remove") as mock_os_remove,
            patch.object(shutil, "move") as mock_move,
        ):
            path = os.path.abspath("/myfakepath")

            await download_from_url_multi_threaded(
                file_handle_id=123,
                object_id=456,
                object_type="FileEntity",
                destination=path,
                expected_md5=expected_md5,
                synapse_client=self.syn,
            )

            mock_os_remove.assert_not_called()
            mock_move.assert_called_once_with(
                utils.temp_download_filename(path, 123), path
            )


class TestDownloadFromUrl:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_download_end_early_retry(self, syn: Synapse) -> None:
        """
        -------Test to ensure download retry even if connection ends early--------
        """

        url = "http://www.ayy.lmao/filerino.txt"
        contents = "\n".join(str(i) for i in range(1000))
        destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))
        temp_destination = os.path.normpath(
            os.path.expanduser("~/fake/path/filerino.txt.temp")
        )

        partial_content_break = len(contents) // 7 * 3
        mock_requests_get = MockRequestGetFunction(
            [
                create_mock_response(
                    url,
                    "stream",
                    contents=contents[:partial_content_break],
                    buffer_size=1024,
                    partial_end=len(contents),
                    status_code=200,
                ),
                create_mock_response(
                    url,
                    "stream",
                    contents=contents,
                    buffer_size=1024,
                    partial_start=partial_content_break,
                    status_code=206,
                ),
            ]
        )

        # make the first response's 'content-type' header say
        # it will transfer the full content even though it
        # is only partially doing so
        mock_requests_get.responses[0].headers["content-length"] = len(contents)
        mock_requests_get.responses[1].headers["content-length"] = len(
            contents[partial_content_break:]
        )

        # TODO: When swapping out for the HTTPX client, we will need to update this test
        # with patch.object(
        #     syn, "rest_get_async", new_callable=AsyncMock, side_effect=mock_requests_get
        # )
        with (
            patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
            patch.object(
                Synapse, "_generate_headers", side_effect=mock_generate_headers
            ),
            patch.object(
                utils, "temp_download_filename", return_value=temp_destination
            ) as mocked_temp_dest,
            patch(
                "synapseclient.core.download.download_functions.open",
                new_callable=mock_open(),
                create=True,
            ) as mocked_open,
            patch.object(os.path, "exists", side_effect=[False, True]) as mocked_exists,
            patch.object(
                os.path, "getsize", return_value=partial_content_break
            ) as mocked_getsize,
            patch.object(utils, "md5_for_file"),
            patch.object(shutil, "move") as mocked_move,
        ):
            # function under test
            download_from_url(
                url=url,
                destination=destination,
                entity_id=OBJECT_ID,
                file_handle_associate_type=OBJECT_TYPE,
                synapse_client=syn,
            )

            # assert temp_download_filename() called 2 times with same parameters
            assert [
                call(destination=destination, file_handle_id=None)
            ] * 2 == mocked_temp_dest.call_args_list

            # assert exists called 2 times
            assert [call(temp_destination)] * 2 == mocked_exists.call_args_list

            # assert open() called 2 times with different parameters
            assert [
                call(temp_destination, "wb"),
                call(temp_destination, "ab"),
            ] == mocked_open.call_args_list

            # assert getsize() called 2 times
            # once because exists()=True and another time because response status code = 206
            assert [
                call(filename=temp_destination)
            ] * 2 == mocked_getsize.call_args_list

            # assert shutil.move() called 1 time
            mocked_move.assert_called_once_with(temp_destination, destination)

    async def test_download_md5_mismatch__not_local_file(self, syn: Synapse) -> None:
        """
        --------Test to ensure file gets removed on md5 mismatch--------
        """
        url = "http://www.ayy.lmao/filerino.txt"
        contents = "\n".join(str(i) for i in range(1000))
        destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))
        temp_destination = os.path.normpath(
            os.path.expanduser("~/fake/path/filerino.txt.temp")
        )

        mock_requests_get = MockRequestGetFunction(
            [
                create_mock_response(
                    url,
                    "stream",
                    contents=contents,
                    buffer_size=1024,
                    partial_end=len(contents),
                    status_code=200,
                )
            ]
        )

        with (
            patch.object(syn._requests_session, "get", side_effect=mock_requests_get),
            patch.object(
                Synapse, "_generate_headers", side_effect=mock_generate_headers
            ),
            patch.object(
                utils, "temp_download_filename", return_value=temp_destination
            ) as mocked_temp_dest,
            patch(
                "synapseclient.core.download.download_functions.open",
                new_callable=mock_open(),
                create=True,
            ) as mocked_open,
            patch.object(os.path, "exists", side_effect=[False, True]) as mocked_exists,
            patch.object(shutil, "move") as mocked_move,
            patch.object(os, "remove") as mocked_remove,
        ):
            # function under test
            with pytest.raises(SynapseMd5MismatchError):
                await download_from_url(
                    url=url,
                    destination=destination,
                    entity_id=OBJECT_ID,
                    file_handle_associate_type=OBJECT_TYPE,
                    expected_md5="fake md5 is fake",
                    synapse_client=syn,
                )

            # assert temp_download_filename() called once
            mocked_temp_dest.assert_called_once_with(
                destination=destination, file_handle_id=None
            )

            # assert exists called 2 times
            assert [
                call(temp_destination),
                call(destination),
            ] == mocked_exists.call_args_list

            # assert open() called once
            mocked_open.assert_called_once_with(temp_destination, "wb")

            # assert shutil.move() called once
            mocked_move.assert_called_once_with(temp_destination, destination)

            # assert file was removed
            mocked_remove.assert_called_once_with(destination)

    async def test_download_md5_mismatch_local_file(self) -> None:
        """
        --------Test to ensure file gets removed on md5 mismatch--------
        """
        url = "file:///some/file/path.txt"
        destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))

        with (
            patch.object(
                utils, "file_url_to_path", return_value=destination
            ) as mocked_file_url_to_path,
            patch.object(
                utils,
                "md5_for_file_hex",
                return_value="Some other incorrect md5",
            ) as mocked_md5_for_file,
            patch("os.remove") as mocked_remove,
        ):
            # function under test
            with pytest.raises(SynapseMd5MismatchError):
                await download_from_url(
                    url=url,
                    destination=destination,
                    entity_id=OBJECT_ID,
                    file_handle_associate_type=OBJECT_TYPE,
                    expected_md5="fake md5 is fake",
                )

            mocked_file_url_to_path.assert_called_once_with(url, verify_exists=True)
            mocked_md5_for_file.assert_called_once()
            # assert file was NOT removed
            assert not mocked_remove.called

    async def test_download_expired_url(self, syn: Synapse) -> None:
        url = "http://www.ayy.lmao/filerino.txt"
        new_url = "http://www.ayy.lmao/new_url.txt"
        contents = "\n".join(str(i) for i in range(1000))
        temp_destination = os.path.normpath(
            os.path.expanduser("~/fake/path/filerino.txt.temp")
        )
        destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))

        partial_content_break = len(contents) // 7 * 3
        mock_requests_get = MockRequestGetFunction(
            [
                create_mock_response(
                    url,
                    "stream",
                    contents=contents[:partial_content_break],
                    buffer_size=1024,
                    partial_end=len(contents),
                    status_code=403,
                ),
                create_mock_response(
                    url,
                    "stream",
                    contents=contents,
                    buffer_size=1024,
                    partial_start=len(contents),
                    status_code=200,
                ),
            ]
        )
        with (
            patch.object(
                syn._requests_session, "get", side_effect=mock_requests_get
            ) as mocked_get,
            patch(
                "synapseclient.core.download.download_functions._pre_signed_url_expiration_time",
                return_value=datetime.datetime(
                    1900, 1, 1, tzinfo=datetime.timezone.utc
                ),
            ) as mocked_pre_signed_url_expiration_time,
            patch(
                "synapseclient.core.download.download_functions.get_file_handle_for_download",
                return_value={"preSignedURL": new_url},
            ) as mocked_get_file_handle_for_download,
            patch.object(
                Synapse, "_generate_headers", side_effect=mock_generate_headers
            ),
            patch.object(
                utils, "temp_download_filename", return_value=temp_destination
            ),
            patch(
                "synapseclient.core.download.download_functions.open",
                new_callable=mock_open(),
                create=True,
            ),
            patch.object(hashlib, "new") as mocked_hashlib_new,
            patch.object(shutil, "move"),
            patch.object(os, "remove"),
        ):
            mocked_hashlib_new.return_value.hexdigest.return_value = "fake md5 is fake"
            # WHEN I call download_from_url with an expired url
            download_from_url(
                url=url,
                destination=destination,
                entity_id=OBJECT_ID,
                file_handle_associate_type=OBJECT_TYPE,
                expected_md5="fake md5 is fake",
            )
            # I expect the expired url to be identified
            mocked_pre_signed_url_expiration_time.assert_called_once_with(url)
            # AND I expect the URL to be refreshed
            mocked_get_file_handle_for_download.assert_called_once()
            # AND I expect the download to be retried with the new URL
            mocked_get.assert_called_with(
                url=new_url,
                headers=mock_generate_headers(self),
                stream=True,
                allow_redirects=False,
                auth=None,
            )


async def test_get_file_handle_download__error_unauthorized(syn: Synapse) -> None:
    ret_val = {
        "requestedFiles": [
            {
                "failureCode": "UNAUTHORIZED",
            }
        ]
    }

    with patch.object(
        syn, "rest_post_async", new_callable=AsyncMock, return_value=ret_val
    ):
        with pytest.raises(SynapseError):
            await get_file_handle_for_download_async(
                file_handle_id="123", synapse_id="syn456", synapse_client=syn
            )


async def test_get_file_handle_download_error_not_found(syn: Synapse) -> None:
    ret_val = {
        "requestedFiles": [
            {
                "failureCode": "NOT_FOUND",
            }
        ]
    }
    with patch.object(
        syn, "rest_post_async", new_callable=AsyncMock, return_value=ret_val
    ):
        with pytest.raises(SynapseFileNotFoundError):
            await get_file_handle_for_download_async(
                file_handle_id="123", synapse_id="syn456", synapse_client=syn
            )
