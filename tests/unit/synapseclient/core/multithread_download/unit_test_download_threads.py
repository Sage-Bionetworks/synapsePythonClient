import datetime
import queue
import time
import threading
import requests

import pytest
import unittest.mock as mock

import synapseclient.core.multithread_download.download_threads as download_threads

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError


class TestCloseableQueue:
    def setup(self):
        self.queue = download_threads.CloseableQueue(maxsize=5)

    def test_send_sentinel(self):
        self.queue.send_sentinel(3)
        for _ in range(3):
            assert download_threads.CloseableQueue.SENTINEL == self.queue.get()

        pytest.raises(queue.Empty, self.queue.get_nowait)

    def test_iter(self):
        for i in range(3):
            self.queue.put(i)

        wait_for_sentinel_sec = 3

        def delay_send_sentinel(closable_queue):
            time.sleep(wait_for_sentinel_sec)
            closable_queue.send_sentinel()
            print("done")

        # delay sending the sentinel on another thread
        t = threading.Thread(target=delay_send_sentinel, args=(self.queue,), daemon=True)
        t.start()
        start_time = time.time()
        for i, queue_val in enumerate(self.queue):
            assert i == queue_val
        elapsed_time = time.time() - start_time
        # should have waited for some time before loop closed
        assert elapsed_time > wait_for_sentinel_sec - 1

    def test_close(self):
        for i in range(2):
            self.queue.put(i)

        self.queue.close()

        # get() should always return the sentinel even past the actual capacity of the queue
        for _ in range(8):
            assert download_threads.CloseableQueue.SENTINEL == self.queue.get()

        # put() should always throw a QueueClosed exception
        for i in range(8):
            pytest.raises(download_threads.QueueClosedException, self.queue.put, i)

    def test_unsupported_operations(self):
        pytest.raises(NotImplementedError, self.queue.join)
        pytest.raises(NotImplementedError, self.queue.task_done)


class TestPresignedUrlProvider(object):
    def setup(self):
        self.mock_synapse_client = mock.create_autospec(Synapse)
        self.download_request = download_threads.DownloadRequest(123, '456', 'FileEntity', '/myFakepath')

    def test_get_info_not_expired(self):
        utc_now = datetime.datetime.utcnow()

        info = download_threads.PresignedUrlInfo("myFile.txt", "https://synapse.org/somefile.txt",
                                                 expiration_utc=utc_now + datetime.timedelta(seconds=6))

        with mock.patch.object(download_threads.PresignedUrlProvider, '_get_pre_signed_info',
                               return_value=info) as mock_get_presigned_info, \
                mock.patch.object(download_threads, "datetime", wraps=datetime) as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = utc_now

            presigned_url_provider = download_threads.PresignedUrlProvider(self.mock_synapse_client,
                                                                           self.download_request)
            assert info == presigned_url_provider.get_info()

            # only caled once in init
            mock_get_presigned_info.assert_called_once()
            mock_datetime.datetime.utcnow.assert_called_once()

    def test_get_info_expired(self):
        utc_now = datetime.datetime.utcnow()

        # expires in the past
        expired_info = download_threads.PresignedUrlInfo("myFile.txt", "https://synapse.org/somefile.txt",
                                                         expiration_utc=utc_now - datetime.timedelta(seconds=5))
        unexpired_info = download_threads.PresignedUrlInfo("myFile.txt",
                                                           "https://synapse.org/somefile.txt",
                                                           expiration_utc=utc_now + datetime.timedelta(
                                                               seconds=6))

        with mock.patch.object(download_threads.PresignedUrlProvider, '_get_pre_signed_info',
                               side_effect=[expired_info, unexpired_info]) as mock_get_presigned_info, \
                mock.patch.object(download_threads, "datetime") as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = utc_now

            presigned_url_provider = download_threads.PresignedUrlProvider(self.mock_synapse_client,
                                                                           self.download_request)
            assert unexpired_info == presigned_url_provider.get_info()

            # only caled once in init and again in get_info
            assert 2 == mock_get_presigned_info.call_count
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

            presigned_url_provider = download_threads.PresignedUrlProvider(self.mock_synapse_client,
                                                                           self.download_request)

            expected = download_threads.PresignedUrlInfo(fake_file_name, fake_url, fake_exp_time)
            assert expected == presigned_url_provider._get_pre_signed_info()

            mock_pre_signed_url_expiration_time.assert_called_with(fake_url)


class TestDataChunkDownloadThread:

    def setup(self):
        self.mock_data_queue = mock.create_autospec(download_threads.CloseableQueue)
        self.mock_range_queue = mock.create_autospec(download_threads.CloseableQueue)
        self.mock_presigned_url_provider = mock.create_autospec(download_threads.PresignedUrlProvider)
        self.presigned_url_info = download_threads.PresignedUrlInfo("foo.txt", "synapse.org/foo.txt",
                                                                    datetime.datetime.utcnow())
        self.mock_presigned_url_provider.get_info.return_value = self.presigned_url_info
        self.mock_requests_session = mock.create_autospec(requests.Session)
        self.mock_requests_response = mock.create_autospec(requests.Response)
        self.mock_requests_session.get.return_value = self.mock_requests_response
        self.response_bytes = [b'some bytes', b'some more bytes']
        self.mock_requests_response.iter_content.return_value = self.response_bytes

        response_byte_len = sum(len(x) for x in self.response_bytes)
        self.mock_range_queue.__iter__.return_value = [(0, response_byte_len - 1),
                                                       (response_byte_len, response_byte_len * 2)]

    def test_get_response_with_retry__exceed_max_retries(self):
        self.mock_requests_response.status_code = 403
        start = 5
        end = 42

        with mock.patch.object(download_threads, "_get_new_session",
                               return_value=self.mock_requests_session):
            download_thread = download_threads.DataChunkDownloadThread(self.mock_presigned_url_provider,
                                                                       self.mock_range_queue,
                                                                       self.mock_data_queue)

            pytest.raises(SynapseError, download_thread._get_response_with_retry, start, end)

            expected_call_list = [mock.call(self.presigned_url_info.url, headers={"Range": "bytes=5-42"},
                                            stream=True)] * download_threads.MAX_RETRIES
            assert expected_call_list == self.mock_requests_session.get.call_args_list

    def test_get_response_with_retry__partial_content_reponse(self):
        self.mock_requests_response.status_code = 206
        start = 5
        end = 42

        with mock.patch.object(download_threads, "_get_new_session",
                               return_value=self.mock_requests_session):
            download_thread = download_threads.DataChunkDownloadThread(self.mock_presigned_url_provider,
                                                                       self.mock_range_queue,
                                                                       self.mock_data_queue)

            assert self.mock_requests_response == download_thread._get_response_with_retry(start, end)

            self.mock_requests_session.get \
                .assert_called_once_with(self.presigned_url_info.url, headers={"Range": "bytes=5-42"}, stream=True)

    def test_run(self):
        with mock.patch.object(download_threads, "_get_new_session",
                               return_value=self.mock_requests_session), \
             mock.patch.object(download_threads.DataChunkDownloadThread, "_get_response_with_retry",
                               return_value=self.mock_requests_response):
            t = download_threads.DataChunkDownloadThread(self.mock_presigned_url_provider,
                                                         self.mock_range_queue,
                                                         self.mock_data_queue)

            t.run()

            # should terminate early since queue is closed
            assert 4 == self.mock_data_queue.put.call_count
            expected_queue_put_calls = [mock.call((0, b'some bytes')), mock.call((10, b'some more bytes')),
                                        mock.call((25, b'some bytes')), mock.call((35, b'some more bytes'))]
            assert expected_queue_put_calls == self.mock_data_queue.put.call_args_list
            self.mock_requests_response.close.assert_not_called()

    def test_run__queue_closed(self):
        self.mock_data_queue.put.side_effect = download_threads.QueueClosedException('')

        with mock.patch.object(download_threads, "_get_new_session", return_value=self.mock_requests_session), \
            mock.patch.object(download_threads.DataChunkDownloadThread, "_get_response_with_retry",
                              return_value=self.mock_requests_response):
            t = download_threads.DataChunkDownloadThread(self.mock_presigned_url_provider,
                                                         self.mock_range_queue,
                                                         self.mock_data_queue)

            t.run()

            # should terminate early since queue is closed
            assert 1 == self.mock_data_queue.put.call_count
            self.mock_requests_response.close.assert_called_once()


class TestDataChunkWriteToFileThread:
    def setup(self):
        self.mock_data_queue = mock.create_autospec(download_threads.CloseableQueue)
        self.path = "/myfakepath/foo.txt"
        self.expected_file_size = 58
        self.bytes_a = b'some bytes'
        self.bytes_b = b'some more bytes'
        self.bytes_c = b'another chunk of bytes'
        self.offset_b = len(self.bytes_a)
        self.offset_c = self.offset_b + len(self.bytes_b)
        self.mock_data_queue.__iter__.return_value = [(0, self.bytes_a), (self.offset_b, self.bytes_b),
                                                      (self.offset_c, self.bytes_c)]

    def test_run(self):
        with mock.patch.object(download_threads, 'open', mock.mock_open()) as mock_open:
            t = download_threads.DataChunkWriteToFileThread(self.mock_data_queue, self.path, self.expected_file_size)

            t.run()

            self.mock_data_queue.close.assert_not_called()
            assert 3 == mock_open().write.call_count

            # make sure seek/write was called in order
            expected_calls = [mock.call.seek(0), mock.call.write(self.bytes_a),
                              mock.call.seek(self.offset_b), mock.call.write(self.bytes_b),
                              mock.call.seek(self.offset_c), mock.call.write(self.bytes_c)]
            print(mock_open().mock_calls)
            print(expected_calls)
            assert expected_calls in mock_open().mock_calls

    def test_run__write_error(self):
        with mock.patch.object(download_threads, 'open', mock.mock_open()) as mock_open:
            mock_open().write.side_effect = [len(self.bytes_a), OSError('fake error')]

            t = download_threads.DataChunkWriteToFileThread(self.mock_data_queue, self.path, self.expected_file_size)

            pytest.raises(OSError, t.run)

            self.mock_data_queue.close.assert_called_once()

            assert 2 == mock_open().write.call_count
            # make sure seek/write was called in order
            assert (
                [
                    mock.call.seek(0), mock.call.write(b'some bytes'),
                    mock.call.seek(self.offset_b), mock.call.write(b'some more bytes')
                ] in mock_open().mock_calls
            )


class TestDownloadThread:
    def setup(self):
        self.mock_data_queue = mock.create_autospec(download_threads.CloseableQueue)
        self.mock_range_queue = mock.create_autospec(download_threads.CloseableQueue)
        self.mock_write_file_thread = mock.create_autospec(download_threads.DataChunkWriteToFileThread)
        self.mock_write_file_thread.path = "/fake_path/foo.txt"
        self.mock_data_chunk_download_threads = [mock.create_autospec(download_threads.DataChunkDownloadThread)
                                                 for _ in range(4)]
        self.chunk_ranges = [(0, 7), (8, 15), (16, 18)]

    def test_download_file__exception_thrown(self):
        self.mock_range_queue.put.side_effect = KeyboardInterrupt("fake interrupt")

        with mock.patch.object(download_threads.os, "remove") as mock_os_remove:
            pytest.raises(KeyboardInterrupt, download_threads._download_file, self.mock_data_queue,
                          self.mock_range_queue,
                          self.mock_write_file_thread, self.mock_data_chunk_download_threads,
                          self.chunk_ranges)

            self.mock_data_queue.close.assert_called_once()
            self.mock_range_queue.close.assert_called_once()
            self.mock_write_file_thread.join.assert_called_once()
            mock_os_remove.assert_called_once()

            self.mock_range_queue.send_sentinel.assert_not_called()
            self.mock_data_queue.send_sentinel.assert_not_called()

    def test_download_file__no_exception(self):
        with mock.patch.object(download_threads.os, "remove") as mock_os_remove:
            download_threads._download_file(self.mock_data_queue, self.mock_range_queue,
                                            self.mock_write_file_thread, self.mock_data_chunk_download_threads,
                                            self.chunk_ranges)

            self.mock_data_queue.close.assert_not_called()
            self.mock_range_queue.close.assert_not_called()
            mock_os_remove.assert_not_called()

            self.mock_range_queue.send_sentinel.assert_called_once_with(len(self.mock_data_chunk_download_threads))
            self.mock_data_queue.send_sentinel.assert_called_once()

            self.mock_write_file_thread.join.assert_called_once()
            for mock_download_thread in self.mock_data_chunk_download_threads:
                mock_download_thread.join.assert_called_once()

            assert (
                [mock.call(chunk_range) for chunk_range in self.chunk_ranges] ==
                self.mock_range_queue.put.call_args_list
            )


def test_generate_chunk_ranges():
    # test using smaller chunk size
    download_threads.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE = 8

    result = [x for x in download_threads._generate_chunk_ranges(18)]

    expected = [(0, 7), (8, 15), (16, 17)]

    assert expected == result


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
    assert expected == download_threads._pre_signed_url_expiration_time(url)
