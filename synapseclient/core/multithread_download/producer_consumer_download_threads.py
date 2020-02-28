import time
import datetime
import os

from queue import Queue
from threading import Thread, Lock
from typing import Generator, Sequence, NamedTuple, Tuple
from math import ceil
from urllib.parse import urlparse, parse_qs
from urllib3.util.retry import Retry
from synapseclient.core.utils import printTransferProgress
from synapseclient.core.exceptions import SynapseError
from requests import Session, Response
from requests.adapters import HTTPAdapter
from http import HTTPStatus

# constants
MAX_QUEUE_SIZE: int = 20
MAX_RETRIES: int = 5
MiB: int = 2 ** 20
KiB: int = 2 ** 10
SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE: int = 8 * MiB
MAX_CHUNK_WRITE_SIZE = 16 * KiB
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
    t0: int
        initial time of transfer
    total_to_be_transferred: int
    transferred: int
    """
    total_bytes_to_be_transferred: int
    transferred: int

    def __init__(self, total_bytes_to_be_transferred: int):
        self.total_bytes_to_be_transferred = total_bytes_to_be_transferred
        self.transferred = 0
        self._t0 = time.time()
        self._lock = Lock()

    def elapsed_time(self) -> float :
        """
        Returns time since this object was created (assuming same time as transfer started)
        :return:
        """
        return time.time() - self._t0

    def __enter__(self) -> bool:
        """
        Blocks and waits to acquire lock on this TransferStatus
        :return: bool indicating whether lock was acquired
        """
        return self._lock.__enter__()


    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.__exit__(exc_type, exc_val, exc_tb)


class CloseableQueue(Queue):
    """
    A closeable queue used to signal when producers are finished producing so consumer threads know when to terminate.
    Adopted from Effective Python Item 39.
    """
    # Sentinel object to signal producer is done producing
    __SENTINEL = object()

    #TODO: prevent additonal work from being added done if an exception occurs elsewhere that would prevent a full completion

    def send_sentinel(self, num_sentinels = 1):
        for _ in range (num_sentinels):
            self.put(CloseableQueue.__SENTINEL)

    def __iter__(self):
        while True:
            item = self.get()
            try:
                if item is CloseableQueue.__SENTINEL:
                    return  # stop iteration
                yield item
            finally:
                # This is invoked after all processing has been done on the yielded item in the for loop
                self.task_done()

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
        self._cached_info: PresignedUrlInfo = self._get_pre_signed_batch_request_json()
        self._lock = Lock()

    def get_info(self) -> PresignedUrlInfo:
        with self._lock:
            if datetime.datetime.utcnow() + PresignedUrlProvider._TIME_BUFFER >= self._cached_info.expiration_utc:
                print("it's expired")
                self._cached_info = self._get_pre_signed_batch_request_json()

            return self._cached_info

    def _get_pre_signed_batch_request_json(self) -> PresignedUrlInfo:
        """
        Returns the file_name and pre-signed url for download as specified in request

        :return: PresignedUrlInfo
        """
        # noinspection PyProtectedMember
        response = self.client._getFileHandleDownload(self.request.file_handle_id, self.request.object_id)
        file_name = response["fileHandle"]["fileName"]
        pre_signed_url = response["preSignedURL"]
        return PresignedUrlInfo(file_name, pre_signed_url, _pre_signed_url_expiration_time(pre_signed_url))


class DataChunkDownloadThread(Thread):
    """
    The producer threads that make the GET request and obtain the data for a download chunk
    """
    def __init__(self, presigned_url_provider: PresignedUrlProvider, range_queue:CloseableQueue, data_queue:CloseableQueue):
        super().__init__()
        # self.daemon = True
        self.presigned_url_provider = presigned_url_provider
        self.range_queue = range_queue
        self.data_queue = data_queue
        self.session = _get_new_session()
    def run(self):
        for item in self.range_queue:
            start, end = item
            range_header = {'Range': f'bytes={start}-{end}'}
            response = self.session.get(self.presigned_url_provider.get_info().url, headers=range_header, stream=True)

            # try request until successful or out of retries
            try_counter = 0
            while response.status_code != HTTPStatus.PARTIAL_CONTENT and try_counter < MAX_RETRIES:
                try_counter += 1
                response = self.session.get(self.presigned_url_provider.get_info().url, headers=range_header, stream=True)

            if response.status_code == HTTPStatus.PARTIAL_CONTENT:
                for bytes in response.iter_content(MAX_CHUNK_WRITE_SIZE):
                    self.data_queue.put((start, bytes))
                    start += len(bytes)
            else:
                #remove
                print(response.status_code)
                raise SynapseError(f"Could not download the file: {self.presigned_url_provider.get_info().file_name}, please try again.")

class DataChunkWriteToFileThread(Thread):
    """
    The worker threads that write download chunks to file
    """
    def __init__(self, data_queue: CloseableQueue, path: str, transfer_status: TransferStatus):
        super().__init__()
        self.daemon = True
        self.data_queue = data_queue
        self.transfer_status = transfer_status
        self.path = path

    def run(self):
        # write data to file
        with open(self.path, "w+b") as file_write:
            for start, data in self.data_queue:
                file_write.seek(start)
                file_write.write(data)
                with self.transfer_status:
                    self.transfer_status.transferred += len(data)
                    printTransferProgress(self.transfer_status.transferred,
                                          self.transfer_status.total_bytes_to_be_transferred,
                                          'Downloading ', os.path.basename(self.path),
                                          dt=self.transfer_status.elapsed_time())


def download_files(client,
                   download_requests: Sequence[DownloadRequest],
                   num_threads: int):
    """
    Main driver for the multi-threaded download. Uses the producer-consumer with Queue design pattern as described
    in Effective Python Item 39.

    :param client: A synapseclient
    :param download_requests: A batch of DownloadRequest objects specifying what Synapse files to download
    :param num_threads: The number of download threads
    :return: Map between each DownloadRequest in download_requests object and the corresponding DownloadResponse object
    """

    data_queue = CloseableQueue(MAX_QUEUE_SIZE)
    range_queue = CloseableQueue(MAX_QUEUE_SIZE)

    for request in download_requests:
        pre_signed_url_provider = PresignedUrlProvider(client, request)

        file_size = _get_file_size(pre_signed_url_provider.get_info().url)

        # shared transfer status across all threads
        transfer_status = TransferStatus(file_size)

        data_chunk_download_threads = []
        for _ in range(num_threads):
            data_chunk_download_worker = DataChunkDownloadThread(pre_signed_url_provider, range_queue, data_queue)
            data_chunk_download_threads.append(data_chunk_download_worker)
            data_chunk_download_worker.start()

        #use a single worker to write to the file
        write_to_file_worker = DataChunkWriteToFileThread(data_queue, request.path, transfer_status)
        write_to_file_worker.start()

        chunk_range_generator = _generate_chunk_ranges(file_size)
        
        for chunk in chunk_range_generator:
            # This operation will block if the queue's max size has been reached
            range_queue.put(chunk)

        # send signal for download threads to stop
        range_queue.send_sentinel(num_threads)

        # wait for download threads to complete
        for data_chunk_download_thread in data_chunk_download_threads:
            data_chunk_download_thread.join()
        range_queue.join()

        # once download threads are done, send sentinel to the data queue to tell the file worker to stop
        data_queue.send_sentinel(1)

        # wait for the writer workers to shutdown
        write_to_file_worker.join()
        data_queue.join()





def _generate_chunk_ranges(file_size: int,
                           ) -> Generator:
    """
    Creates a generator which yields byte ranges and meta data required to make a range request download of url and
    write the data to file_name located at path. Download chunk sizes are 8MB by default.

    :param file_size: The size of the file
    :param file_name: The name of the file
    :param url: The pre-signed url to download the file from
    :param path: The local path describing where to download the file
    :return: A generator of byte ranges and meta data needed to download the file in a multi-threaded manner
    """
    num_chunks = ceil(file_size / SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE)
    for i in range(num_chunks):
        start = SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE * i
        end = start + SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE
        yield start, end

def _pre_signed_url_expiration_time(url: str) -> datetime:
    """
    Returns time at which a presigned url will expire

    :param url: A pre-signed download url from AWS
    :return: datetime in UTC of when the url will expire
    """
    if url is None:
        return datetime.datetime.utcfromtimestamp(0)

    parsed_query:dict = parse_qs(urlparse(url).query)
    time_made:str = parsed_query['X-Amz-Date'][0]
    time_made_datetime:datetime.datetime = datetime.datetime.strptime(time_made, ISO_AWS_STR_FORMAT)
    expires:str = parsed_query['X-Amz-Expires'][0]
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


def _get_response_with_refresh(presigned_url_provider: PresignedUrlProvider,
                               headers: dict, session: Session) -> Response:
    """
    Performs refresh on url if necessary and returns response for range request on url specified by headers
    :param url: A pre-signed url pointing to file to download
    :param client: The synapseclient being used to download
    :param request: The DownloadRequest specifying the file located at url
    :param headers: A dict specifying the byte range for the range request of url
    :param session: The current request.Session object to make the get call with
    :return: The requests.Response from calling get on url
    """
    url = presigned_url_provider.get_info().url
    return session.get(url, headers=headers, stream=True)
