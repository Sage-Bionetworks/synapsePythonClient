import concurrent.futures
import datetime
import os
import requests

from unittest import TestCase
import unittest.mock as mock
from nose.tools import assert_equals, assert_false, assert_raises, assert_true

import synapseclient.core.multithread_download.download_threads as download_threads
from synapseclient.core.multithread_download.download_threads import (
    _MultithreadedDownloader,
    download_file,
    DownloadRequest,
    PresignedUrlProvider,
    PresignedUrlInfo,
    TransferStatus,
)

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError


class TestPresignedUrlProvider(object):
    def setup(self):
        self.mock_synapse_client = mock.create_autospec(Synapse)
        self.download_request = DownloadRequest(123, '456', 'FileEntity', '/myFakepath')

    def test_get_info_not_expired(self):
        utc_now = datetime.datetime.utcnow()

        info = PresignedUrlInfo("myFile.txt", "https://synapse.org/somefile.txt",
                                expiration_utc=utc_now + datetime.timedelta(seconds=6))

        with mock.patch.object(PresignedUrlProvider, '_get_pre_signed_info',
                               return_value=info) as mock_get_presigned_info, \
                mock.patch.object(download_threads, "datetime", wraps=datetime) as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = utc_now

            presigned_url_provider = PresignedUrlProvider(self.mock_synapse_client,
                                                          self.download_request)
            assert_equals(info, presigned_url_provider.get_info())

            # only caled once in init
            mock_get_presigned_info.assert_called_once()
            mock_datetime.datetime.utcnow.assert_called_once()

    def test_get_info_expired(self):
        utc_now = datetime.datetime.utcnow()

        # expires in the past
        expired_info = PresignedUrlInfo("myFile.txt", "https://synapse.org/somefile.txt",
                                        expiration_utc=utc_now - datetime.timedelta(seconds=5))
        unexpired_info = PresignedUrlInfo("myFile.txt",
                                          "https://synapse.org/somefile.txt",
                                          expiration_utc=utc_now + datetime.timedelta(seconds=6))

        with mock.patch.object(PresignedUrlProvider, '_get_pre_signed_info',
                               side_effect=[expired_info, unexpired_info]) as mock_get_presigned_info, \
                mock.patch.object(download_threads, "datetime") as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = utc_now

            presigned_url_provider = PresignedUrlProvider(self.mock_synapse_client,
                                                          self.download_request)
            assert_equals(unexpired_info, presigned_url_provider.get_info())

            # only caled once in init and again in get_info
            assert_equals(2, mock_get_presigned_info.call_count)
            mock_datetime.datetime.utcnow.assert_called_once()

    def test_get_pre_signed_info(self):
        fake_exp_time = datetime.datetime.utcnow()
        fake_url = "https://synapse.org/foo.txt"
        fake_file_name = "foo.txt"

        with mock.patch.object(download_threads, "_pre_signed_url_expiration_time",
                               return_value=fake_exp_time) as mock_pre_signed_url_expiration_time:
            fake_file_handle_response = {
                "fileHandle": {"fileName": fake_file_name},
                "preSignedURL": fake_url
            }

            self.mock_synapse_client._getFileHandleDownload.return_value = fake_file_handle_response

            presigned_url_provider = PresignedUrlProvider(self.mock_synapse_client,
                                                          self.download_request)

            expected = PresignedUrlInfo(fake_file_name, fake_url, fake_exp_time)
            assert_equals(expected, presigned_url_provider._get_pre_signed_info())

            mock_pre_signed_url_expiration_time.assert_called_with(fake_url)
            self.mock_synapse_client._getFileHandleDownload.assert_called_with(
                self.download_request.file_handle_id,
                self.download_request.object_id,
                objectType=self.download_request.object_type,
            )


def test_generate_chunk_ranges():
    # test using smaller chunk size
    download_threads.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE = 8

    result = [x for x in download_threads._generate_chunk_ranges(18)]

    expected = [(0, 7), (8, 15), (16, 17)]

    assert_equals(expected, result)


def test_pre_signed_url_expiration_time():
    url = "https://s3.amazonaws.com/examplebucket/test.txt" \
          "?X-Amz-Algorithm=AWS4-HMAC-SHA256" \
          "&X-Amz-Credential=your-access-key-id/20130721/us-east-1/s3/aws4_request" \
          "&X-Amz-Date=20130721T201207Z" \
          "&X-Amz-Expires=86400" \
          "&X-Amz-SignedHeaders=host" \
          "&X-Amz-Signature=signature-value"

    expected = datetime.datetime(year=2013, month=7, day=21, hour=20, minute=12, second=7) + datetime.timedelta(
        seconds=86400)
    assert_equals(expected, download_threads._pre_signed_url_expiration_time(url))


@mock.patch.object(download_threads, '_MultithreadedDownloader')
def test_download_file(mock_multithreaded_downloader_init):
    """Verify that initiating a download instantiates a downloader and passes it the correct args.
    This test simulates a shared executor being set externally via the sharedexecutor context manager"""

    syn = mock.Mock()
    file_handle_id = 1234
    object_id = 'syn123'
    object_type = None
    path = '/tmp/foo'
    request = DownloadRequest(
        file_handle_id,
        object_id,
        object_type,
        path,
    )

    mock_executor = mock.Mock()
    mock_downloader = mock.Mock()
    mock_multithreaded_downloader_init.return_value = mock_downloader

    max_concurrent_parts = 5

    with download_threads.shared_executor(mock_executor):
        download_file(syn, request, max_concurrent_parts=max_concurrent_parts)

    mock_multithreaded_downloader_init.assert_called_once_with(syn, mock_executor, max_concurrent_parts)
    mock_downloader.download_file.assert_called_once_with(request)

    # executor was passed in from the outside, so it should be managed from the outside
    assert_false(mock_executor.shutdown.called)


@mock.patch.object(download_threads, 'get_executor')
@mock.patch.object(download_threads, '_MultithreadedDownloader')
def test_download_file__executor_shutdown(mock_multithreaded_downloader_init, mock_get_executor):
    """Verify that if no external executor is passed in the internally created one
    is shutdown once the download is done"""

    max_threads = 5
    syn = mock.Mock(max_threads=max_threads)

    file_handle_id = 1234
    object_id = 'syn123'
    object_type = None
    path = '/tmp/foo'
    request = DownloadRequest(
        file_handle_id,
        object_id,
        object_type,
        path,
    )

    mock_executor = mock.Mock()
    mock_get_executor.return_value = mock_executor

    mock_downloader = mock.Mock()
    mock_multithreaded_downloader_init.return_value = mock_downloader

    download_file(syn, request)

    # no max_concurrent_parts passed, should default to the number of client configured threads
    mock_multithreaded_downloader_init.assert_called_once_with(syn, mock_executor, max_threads)
    mock_downloader.download_file.assert_called_once_with(request)

    # internally created executor should be shutdown
    assert_true(mock_executor.shutdown.called)


class MultithreadedDownloaderTests(TestCase):

    def test_download_file(self):
        """Test downloading a file, succesfully, with multiple trips through the loop needed to complete the file"""

        file_handle_id = 1234
        object_id = 'syn123'
        path = '/tmp/foo'
        url = 'http://foo.com/bar'
        file_size = int(1.5 * (2 ** 20))
        request = DownloadRequest(file_handle_id, object_id, None, path)

        with mock.patch.object(download_threads, 'PresignedUrlProvider') as mock_url_provider_init, \
                mock.patch.object(download_threads, 'TransferStatus') as mock_transfer_status_init, \
                mock.patch.object(download_threads, '_get_file_size') as mock_get_file_size, \
                mock.patch.object(download_threads, '_generate_chunk_ranges') as mock_generate_chunk_ranges, \
                mock.patch.object(_MultithreadedDownloader, '_prep_file') as mock_prep_file, \
                mock.patch.object(_MultithreadedDownloader, '_submit_chunks') as mock_submit_chunks, \
                mock.patch.object(_MultithreadedDownloader, '_write_chunks') as mock_write_chunks, \
                mock.patch('concurrent.futures.wait') as mock_futures_wait, \
                mock.patch.object(_MultithreadedDownloader, '_check_for_errors') as mock_check_for_errors:

            mock_url_info = mock.create_autospec(PresignedUrlInfo, url=url)
            mock_url_provider = mock.create_autospec(PresignedUrlProvider)
            mock_url_provider.get_info.return_value = mock_url_info

            mock_url_provider_init.return_value = mock_url_provider
            mock_get_file_size.return_value = file_size
            chunk_generator = mock.Mock()
            mock_generate_chunk_ranges.return_value = chunk_generator

            transfer_status = TransferStatus(file_size)
            mock_transfer_status_init.return_value = transfer_status

            first_future = mock.Mock()
            second_future = mock.Mock()
            third_future = mock.Mock()

            # 3 parts total, submit 2, then 1, then no more the third time through the loop
            mock_submit_chunks.side_effect = [
                set([first_future, second_future]),
                set([third_future]),
                set(),
            ]

            # on first wait 1 part is done, one is pending,
            # on second wait last remaining part is completed

            mock_futures_wait.side_effect = [
                (set([first_future]), set([second_future])),
                (set([second_future, third_future]), set()),
            ]

            syn = mock.Mock()
            executor = mock.Mock()
            max_concurrent_parts = 5
            downloader = _MultithreadedDownloader(syn, executor, max_concurrent_parts)

            downloader.download_file(request)

            mock_prep_file.assert_called_once_with(request)

            expected_submit_chunks_calls = [
                mock.call(mock_url_provider, chunk_generator, set()),
                mock.call(mock_url_provider, chunk_generator, set([second_future])),
                mock.call(mock_url_provider, chunk_generator, set()),
            ]
            assert_equals(expected_submit_chunks_calls, mock_submit_chunks.call_args_list)

            expected_write_chunk_calls = [
                mock.call(request, set(), transfer_status),
                mock.call(request, set([first_future]), transfer_status),
                mock.call(request, set([second_future, third_future]), transfer_status),
            ]
            assert_equals(expected_write_chunk_calls, mock_write_chunks.call_args_list)

            expected_futures_wait_calls = [
                mock.call(set([first_future, second_future]), return_when=concurrent.futures.FIRST_COMPLETED),
                mock.call(set([second_future, third_future]), return_when=concurrent.futures.FIRST_COMPLETED),
            ]
            assert_equals(expected_futures_wait_calls, mock_futures_wait.call_args_list)

            expected_check_for_errors_calls = [
                mock.call(request, set([first_future])),
                mock.call(request, set([second_future, third_future])),
            ]
            assert_equals(expected_check_for_errors_calls, mock_check_for_errors.call_args_list)

    def test_download_file__error(self):
        """Test downloading a file when one of the file downloads generates an error.
        It should be surfaced raised in the entrant thread.
        """

        file_handle_id = 1234
        entity_id = 'syn123'
        path = '/tmp/foo'
        url = 'http://foo.com/bar'
        file_size = int(1.5 * (2 ** 20))
        request = DownloadRequest(file_handle_id, entity_id, None, path)

        with mock.patch.object(download_threads, 'PresignedUrlProvider') as mock_url_provider_init, \
                mock.patch.object(download_threads, 'TransferStatus') as mock_transfer_status_init, \
                mock.patch.object(download_threads, '_get_file_size') as mock_get_file_size, \
                mock.patch.object(download_threads, '_generate_chunk_ranges') as mock_generate_chunk_ranges, \
                mock.patch.object(download_threads, 'os') as mock_os, \
                mock.patch.object(_MultithreadedDownloader, '_prep_file'), \
                mock.patch.object(_MultithreadedDownloader, '_submit_chunks') as mock_submit_chunks, \
                mock.patch.object(_MultithreadedDownloader, '_write_chunks'), \
                mock.patch('concurrent.futures.wait') as mock_futures_wait:

            mock_url_info = mock.create_autospec(PresignedUrlInfo, url=url)
            mock_url_provider = mock.create_autospec(PresignedUrlProvider)
            mock_url_provider.get_info.return_value = mock_url_info

            mock_url_provider_init.return_value = mock_url_provider
            mock_get_file_size.return_value = file_size
            chunk_generator = mock.Mock()
            mock_generate_chunk_ranges.return_value = chunk_generator

            transfer_status = TransferStatus(file_size)
            mock_transfer_status_init.return_value = transfer_status

            exception = ValueError('failed!')
            part_future_1 = mock.create_autospec(concurrent.futures.Future)
            part_future_1.exception.return_value = exception
            part_future_2 = mock.create_autospec(concurrent.futures.Future)

            # future 1 completed with an error.
            # should atempt to cancel future 2 as a result
            mock_submit_chunks.return_value = set([part_future_1, part_future_2])
            mock_futures_wait.return_value = (set([part_future_1]), set([part_future_2]))

            syn = mock.Mock()
            executor = mock.Mock()
            max_concurrent_parts = 5
            downloader = _MultithreadedDownloader(syn, executor, max_concurrent_parts)

            with assert_raises(exception.__class__):
                downloader.download_file(request)

            # file should have been removed
            mock_os.remove.assert_called_once_with(path)

            # should have been an attempt to cancel the Future
            part_future_2.cancel.assert_called_once_with()

    @mock.patch.object(download_threads, 'open')
    def test_prep_file(self, mock_open):
        """Should open and close the file to create/truncate it"""
        path = '/tmp/foo'
        request = DownloadRequest(None, None, None, path)
        download_threads._MultithreadedDownloader._prep_file(request)
        mock_open.assert_called_once_with(path, 'wb')

        mock_open.return_value.close.assert_called_once_with()

    def test_submit_chunks(self):
        """Verify chunks are submitted to the executor as expected, not exceeding the available
        number of outstanding concurrent parts"""

        syn = mock.Mock()
        max_concurrent_parts = 3
        pending_futures = [mock.Mock()] * 1

        expected_submit_count = max_concurrent_parts - len(pending_futures)
        executor_submit_side_effect = [mock.Mock() for _ in range(expected_submit_count)]
        executor_submit = mock.Mock(side_effect=executor_submit_side_effect)
        executor = mock.Mock(submit=executor_submit)
        url_provider = mock.Mock()

        file_size = int(2.5 * download_threads.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE)
        chunk_range_generator = download_threads._generate_chunk_ranges(file_size)

        downloader = _MultithreadedDownloader(syn, executor, max_concurrent_parts)
        submitted_futures = downloader._submit_chunks(url_provider, chunk_range_generator, pending_futures)

        ranges = [r for r in download_threads._generate_chunk_ranges(file_size)][:expected_submit_count]
        expected_submits = [
            mock.call(
                downloader._get_response_with_retry,
                url_provider,
                start,
                end,
            ) for start, end in ranges
        ]
        assert_equals(expected_submits, executor_submit.call_args_list)
        assert_equals(set(executor_submit_side_effect), submitted_futures)

    @mock.patch.object(download_threads, 'open')
    def test_write_chunks__none_ready(self, mock_open):
        """Verify that if there are no parts ready that nothing is written out"""
        request = mock.Mock()
        transfer_status = mock.Mock()
        completed_futures = set()
        downloader = _MultithreadedDownloader(mock.Mock(), mock.Mock(), 5)
        downloader._write_chunks(request, completed_futures, transfer_status)
        assert_false(mock_open.called)

    @mock.patch.object(download_threads, 'printTransferProgress')
    @mock.patch.object(download_threads, 'open')
    def test_write_chunks(self, mock_open, mock_print_transfer_progress):
        """Verify expected behavior writing out chunks to disk"""
        request = mock.Mock(path='/tmp/foo')

        chunks = [b'foo', b'bar', b'baz']
        file_size = sum(len(d) for d in chunks)
        transfer_status = TransferStatus(file_size)

        completed_futures = []
        expected_seeks = []
        expected_writes = []
        expected_print_transfer_progresses = []

        byte_start = 0
        for chunk in chunks:
            future = mock.Mock(
                result=mock.Mock(
                    return_value=(
                        byte_start,
                        mock.Mock(content=chunk)
                    )
                )
            )
            completed_futures.append(future)
            expected_seeks.append(mock.call(byte_start))
            expected_writes.append(mock.call(chunk))

            byte_start += len(chunk)
            expected_print_transfer_progresses.append(
                mock.call(byte_start, file_size, 'Downloading ', os.path.basename(request.path), dt=mock.ANY)
            )

        downloader = _MultithreadedDownloader(mock.Mock(), mock.Mock(), 5)
        downloader._write_chunks(request, completed_futures, transfer_status)

        # with open (as a context manager)
        mock_write = mock_open.return_value.__enter__.return_value
        assert_equals(expected_seeks, mock_write.seek.call_args_list)
        assert_equals(expected_writes, mock_write.write.call_args_list)

        assert_equals(sum(len(c) for c in chunks), transfer_status.transferred)
        assert_equals(expected_print_transfer_progresses, mock_print_transfer_progress.call_args_list)

    def test_check_for_errors__no_errors(self):
        """Verify check_for_errors when there were no errors"""
        downloader = _MultithreadedDownloader(mock.Mock(), mock.Mock(), 5)

        request = mock.Mock()
        completed_futures = [mock.Mock(exception=mock.Mock(return_value=None))] * 3

        # does not raise error
        downloader._check_for_errors(request, completed_futures)

    def test_check_for_errors(self):
        """Verify check_for_errors when there were no errors"""
        downloader = _MultithreadedDownloader(mock.Mock(), mock.Mock(), 5)

        request = mock.Mock()
        exception = ValueError('failed')

        successful_future = mock.Mock(exception=mock.Mock(return_value=None))
        failed_future = mock.Mock(exception=mock.Mock(return_value=exception))
        completed_futures = ([successful_future] * 2) + [failed_future] + [successful_future]

        with assert_raises(exception.__class__):
            downloader._check_for_errors(request, completed_futures)

    @mock.patch.object(download_threads, "_get_thread_session")
    def test_get_response_with_retry__exceed_max_retries(self, mock_get_thread_session):
        mock_requests_response = mock.Mock(status_code=403)
        mock_requests_session = mock.create_autospec(requests.Session)
        mock_requests_session.get.return_value = mock_requests_response
        mock_get_thread_session.return_value = mock_requests_session

        mock_presigned_url_provider = mock.create_autospec(download_threads.PresignedUrlProvider)
        presigned_url_info = download_threads.PresignedUrlInfo(
            "foo.txt", "synapse.org/foo.txt",
            datetime.datetime.utcnow()
        )
        mock_presigned_url_provider.get_info.return_value = presigned_url_info

        start = 5
        end = 42

        downloader = _MultithreadedDownloader(mock.Mock(), mock.Mock(), 5)
        with assert_raises(SynapseError):
            downloader._get_response_with_retry(mock_presigned_url_provider, start, end)

        expected_call_list = [
            mock.call(
                presigned_url_info.url, headers={"Range": "bytes=5-42"}, stream=True
            )
        ] * download_threads.MAX_RETRIES
        assert_equals(expected_call_list, mock_requests_session.get.call_args_list)

    @mock.patch.object(download_threads, "_get_thread_session")
    def test_get_response_with_retry__partial_content_reponse(self, mock_get_thread_session):
        mock_requests_response = mock.Mock(status_code=206)
        mock_requests_session = mock.create_autospec(requests.Session)
        mock_requests_session.get.return_value = mock_requests_response
        mock_get_thread_session.return_value = mock_requests_session

        mock_presigned_url_provider = mock.create_autospec(download_threads.PresignedUrlProvider)
        presigned_url_info = download_threads.PresignedUrlInfo(
            "foo.txt", "synapse.org/foo.txt",
            datetime.datetime.utcnow()
        )

        mock_presigned_url_provider.get_info.return_value = presigned_url_info
        start = 5
        end = 42

        downloader = _MultithreadedDownloader(mock.Mock(), mock.Mock(), 5)
        assert_equals(
            (start, mock_requests_response),
            downloader._get_response_with_retry(mock_presigned_url_provider, start, end)
        )

        mock_requests_session.get.assert_called_once_with(
            presigned_url_info.url,
            headers={"Range": "bytes=5-42"},
            stream=True
        )


def test_shared_executor():
    """Test the shared_executor contextmanager which should set up thread_local Executor"""
    assert_false(hasattr(download_threads._thread_local, 'executor'))

    executor = mock.Mock()
    with download_threads.shared_executor(executor):
        assert_equals(executor, download_threads._thread_local.executor)

    assert_false(hasattr(download_threads._thread_local, 'executor'))
