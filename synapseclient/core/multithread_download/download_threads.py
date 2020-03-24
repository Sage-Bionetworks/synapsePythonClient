import time

try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading
import datetime
import os
import queue

from typing import Generator, Sequence, NamedTuple, Tuple, Iterable
from urllib.parse import urlparse, parse_qs
from urllib3.util.retry import Retry
from synapseclient.core.utils import printTransferProgress
from synapseclient.core.exceptions import SynapseError
from requests import Session, Response
from requests.adapters import HTTPAdapter
from http import HTTPStatus

# constants
MAX_QUEUE_SIZE: int = 20
MAX_RETRIES: int = 20
MiB: int = 2 ** 20
SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE: int = 8 * MiB
MAX_CHUNK_WRITE_SIZE = 2 * MiB
ISO_AWS_STR_FORMAT: str = '%Y%m%dT%H%M%SZ'
CONNECT_FACTOR: int = 3
BACK_OFF_FACTOR: float = 0.5


class DownloadRequest(NamedTuple):
    """
    A request to download a file from Synapse

    ...

    Attributes
    ----------
    file_handle_id : int
        The file handle ID to download.
    object_id : str
        The Synapse object this file associated to.
    object_type : str
        the type of the associated Synapse object.
    path : str
        The local path to download the file to.
        This path can be either absolute path or relative path from where the code is executed to the download location.
    """
    file_handle_id: int
    object_id: str
    object_type: str
    path: str


class TransferStatus(object):
    """
    Transfer progress parameters. Lock should be acquired via `with trasfer_status:` before accessing attributes
    Attributes
    ----------
    total_bytes_to_be_transferred: int
    transferred: int
    """
    total_bytes_to_be_transferred: int
    transferred: int

    def __init__(self, total_bytes_to_be_transferred: int):
        self.total_bytes_to_be_transferred = total_bytes_to_be_transferred
        self.transferred = 0
        self._t0 = time.time()

    def elapsed_time(self) -> float:
        """
        :return: time since this object was created (assuming same time as transfer started)
        """
        return time.time() - self._t0


class CloseableQueue(queue.Queue):
    """
    A closeable queue used to signal when producers are finished producing so consumer threads know when to terminate.
    Adopted from Effective Python Item 39.

    !!!!!This SHOULD NOT be used as a drop in replacement for queue.Queue as some operations are not supported!!!
    """
    # Sentinel object to signal producer is done producing
    SENTINEL = object()

    def __init__(self, maxsize=0):
        self._closed_lock = _threading.Lock()
        self._closed = False
        super().__init__(maxsize=maxsize)

    def send_sentinel(self, num_sentinels=1):
        try:
            for _ in range(num_sentinels):
                self.put(CloseableQueue.SENTINEL)
        except QueueClosedException:
            pass

    def __iter__(self):
        while True:
            item = self.get()
            if item is CloseableQueue.SENTINEL:
                return  # stop iteration
            yield item

    def close(self):
        with self._closed_lock:
            self._closed = True

    def put(self, item, block=True, timeout=None):
        with self._closed_lock:
            if self._closed:
                raise QueueClosedException("queue is closed")
        super().put(item, block=block, timeout=timeout)

    def get(self, block=True, timeout=None):
        with self._closed_lock:
            if self._closed:
                return CloseableQueue.SENTINEL
        return super().get(block=block, timeout=timeout)

    """
        Once closed, the queue will always return the sentinel, even though nothing else was added to the queue.
        Therefore, calling task_done() can not be relied upon
        for tracking progress for a join() once the queue is closed.
    """

    def join(self):
        raise NotImplementedError("join() is not supported")

    def task_done(self):
        raise NotImplementedError("task_done() is not supported")


class QueueClosedException(Exception):
    pass


class PresignedUrlInfo(NamedTuple):
    """
    Information about a retrieved presigned-url

    ...

    Attributes
    ----------
    file_name: str
        name of the file for the presigned url
    url: str
        the actual presigned url
    expiration_utc: datetime.datetime
        datetime in UTC at which the url will expire
    """
    file_name: str
    url: str
    expiration_utc: datetime.datetime


class PresignedUrlProvider(object):
    """
    Provides an un-exipired pre-signed url to download a file
    """
    request: DownloadRequest
    _cached_info: PresignedUrlInfo

    # offset parameter used to buffer url expiration checks, time in seconds
    _TIME_BUFFER: datetime.timedelta = datetime.timedelta(seconds=5)

    def __init__(self, client, request: DownloadRequest):
        self.client = client
        self.request: DownloadRequest = request
        self._cached_info: PresignedUrlInfo = self._get_pre_signed_info()
        self._lock = _threading.Lock()

    def get_info(self) -> PresignedUrlInfo:
        with self._lock:
            if datetime.datetime.utcnow() + PresignedUrlProvider._TIME_BUFFER >= self._cached_info.expiration_utc:
                self._cached_info = self._get_pre_signed_info()

            return self._cached_info

    def _get_pre_signed_info(self) -> PresignedUrlInfo:
        """
        Returns the file_name and pre-signed url for download as specified in request

        :return: PresignedUrlInfo
        """
        # noinspection PyProtectedMember
        response = self.client._getFileHandleDownload(self.request.file_handle_id, self.request.object_id)
        file_name = response["fileHandle"]["fileName"]
        pre_signed_url = response["preSignedURL"]
        return PresignedUrlInfo(file_name, pre_signed_url, _pre_signed_url_expiration_time(pre_signed_url))


class DataChunkDownloadThread(_threading.Thread):
    """
    The producer threads that make the GET request and obtain the data for a download chunk
    """

    def __init__(self, presigned_url_provider: PresignedUrlProvider, range_queue: CloseableQueue,
                 data_queue: CloseableQueue):
        super().__init__()
        self.daemon = True
        self.presigned_url_provider = presigned_url_provider
        self.range_queue = range_queue
        self.data_queue = data_queue
        self.session = _get_new_session()

    def run(self):
        for byte_range in self.range_queue:
            start, end = byte_range

            response = self._get_response_with_retry(start, end)

            try:
                for data_chunk in response.iter_content(MAX_CHUNK_WRITE_SIZE):
                    self.data_queue.put((start, data_chunk))
                    start += len(data_chunk)
            except QueueClosedException:
                # the data_queue was closed so stop retrieving data chunks
                response.close()
                break

    def _get_response_with_retry(self, start: int, end: int) -> Response:
        range_header = {'Range': f'bytes={start}-{end}'}
        response = self.session.get(self.presigned_url_provider.get_info().url, headers=range_header, stream=True)
        # try request until successful or out of retries
        try_counter = 1
        while response.status_code != HTTPStatus.PARTIAL_CONTENT:
            if try_counter >= MAX_RETRIES:
                raise SynapseError(
                    f'Could not download the file: {self.presigned_url_provider.get_info().file_name},'
                    f' please try again.')
            response = self.session.get(self.presigned_url_provider.get_info().url, headers=range_header, stream=True)
            try_counter += 1
        return response


class DataChunkWriteToFileThread(_threading.Thread):
    """
    The worker threads that write download chunks to file
    """
    path: str

    def __init__(self, data_queue: CloseableQueue, path: str, expected_file_size: int):
        super().__init__()
        self.daemon = True
        self.data_queue = data_queue
        self.transfer_status = TransferStatus(expected_file_size)
        self.path = path

    def run(self):
        try:
            # write data to file
            with open(self.path, 'wb') as file_write:
                for start, data in self.data_queue:
                    file_write.seek(start)
                    file_write.write(data)
                    self.transfer_status.transferred += len(data)
                    printTransferProgress(self.transfer_status.transferred,
                                          self.transfer_status.total_bytes_to_be_transferred,
                                          'Downloading ', os.path.basename(self.path),
                                          dt=self.transfer_status.elapsed_time())
        except OSError:
            self.data_queue.close()
            raise


def download_file(client,
                  download_request: DownloadRequest,
                  num_threads: int):
    """
    Main driver for the multi-threaded download. Uses the producer-consumer with Queue design pattern as described
    in Effective Python Item 39.

    :param client: A synapseclient
    :param download_request: A batch of DownloadRequest objects specifying what Synapse files to download
    :param num_threads: The number of download threads
    :return: Map between each DownloadRequest in download_requests object and the corresponding DownloadResponse object
    """
    data_queue = CloseableQueue(MAX_QUEUE_SIZE)
    range_queue = CloseableQueue(MAX_QUEUE_SIZE)

    pre_signed_url_provider = PresignedUrlProvider(client, download_request)

    file_size = _get_file_size(pre_signed_url_provider.get_info().url)

    # use a single worker to write to the file
    write_to_file_thread = DataChunkWriteToFileThread(data_queue, download_request.path, file_size)
    data_chunk_download_threads = [DataChunkDownloadThread(pre_signed_url_provider, range_queue, data_queue)
                                   for _ in range(num_threads)]

    chunk_range_generator = _generate_chunk_ranges(file_size)

    return _download_file(data_queue, range_queue,
                          write_to_file_thread, data_chunk_download_threads,
                          chunk_range_generator)


def _download_file(data_queue: CloseableQueue,
                   range_queue: CloseableQueue,
                   write_to_file_thread: DataChunkWriteToFileThread,
                   data_chunk_download_threads: Sequence[DataChunkDownloadThread],
                   chunk_ranges: Iterable[Tuple[int, int]]):
    """
    helper for download_file() to make testing the thread management logic easier
    """
    try:
        write_to_file_thread.start()

        for data_chunk_download_worker in data_chunk_download_threads:
            data_chunk_download_worker.start()

        for chunk_range in chunk_ranges:
            # code in this main thread will usually block in this loop while download is progressing
            range_queue.put(chunk_range)

        # send signal for download threads to stop
        range_queue.send_sentinel(len(data_chunk_download_threads))
        # wait for download threads to complete
        for data_chunk_download_thread in data_chunk_download_threads:
            data_chunk_download_thread.join()

        # once download threads are done, send sentinel to the data queue to tell the file worker to stop
        data_queue.send_sentinel()
        # wait for the writer workers to shutdown
        write_to_file_thread.join()
    except BaseException:
        # on any exception (e.g. KeyboardInterrupt), ensure the started threads are killed

        # stop other threads early by closing the queues upon which they rely
        range_queue.close()
        data_queue.close()

        # release file lock and delete the partially downloaded file
        write_to_file_thread.join()
        try:
            os.remove(write_to_file_thread.path)
        except FileNotFoundError:
            pass

        raise


def _generate_chunk_ranges(file_size: int,
                           ) -> Generator:
    """
    Creates a generator which yields byte ranges and meta data required to make a range request download of url and
    write the data to file_name located at path. Download chunk sizes are 8MB by default.

    :param file_size: The size of the file
    :return: A generator of byte ranges and meta data needed to download the file in a multi-threaded manner
    """
    for start in range(0, file_size, SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE):
        # the start and end of a range in HTTP are both inclusive
        end = min(start + SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE, file_size) - 1
        yield start, end


def _pre_signed_url_expiration_time(url: str) -> datetime:
    """
    Returns time at which a presigned url will expire

    :param url: A pre-signed download url from AWS
    :return: datetime in UTC of when the url will expire
    """
    parsed_query: dict = parse_qs(urlparse(url).query)
    time_made: str = parsed_query['X-Amz-Date'][0]
    time_made_datetime: datetime.datetime = datetime.datetime.strptime(time_made, ISO_AWS_STR_FORMAT)
    expires: str = parsed_query['X-Amz-Expires'][0]
    return time_made_datetime + datetime.timedelta(seconds=int(expires))


def _get_new_session() -> Session:
    """
    Creates a new requests.Session object with retry defined by CONNECT_FACTOR and BACK_OFF_FACTOR
    :return: A new requests.Session object
    """
    session = Session()
    retry = Retry(connect=CONNECT_FACTOR, backoff_factor=BACK_OFF_FACTOR)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def _get_file_size(url: str) -> int:
    """
    Gets the size of the file located at url
    :param url: The pre-signed url of the file
    :return: The size of the file in bytes
    """
    session = _get_new_session()
    res_get = session.get(url, stream=True)
    return int(res_get.headers['Content-Length'])
