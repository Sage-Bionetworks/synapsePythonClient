try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading
import datetime

import concurrent.futures
from contextlib import contextmanager
from http import HTTPStatus
import os
from requests import Session, Response
from requests.adapters import HTTPAdapter
from typing import Generator, NamedTuple
from urllib.parse import urlparse, parse_qs
from urllib3.util.retry import Retry
import time

from synapseclient.core.exceptions import SynapseError
from synapseclient.core.pool_provider import get_executor
from synapseclient.core.retry import (
    with_retry,
    RETRYABLE_CONNECTION_ERRORS,
    RETRYABLE_CONNECTION_EXCEPTIONS,
)

# constants
MAX_QUEUE_SIZE: int = 20
MiB: int = 2 ** 20
SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE: int = 8 * MiB
ISO_AWS_STR_FORMAT: str = '%Y%m%dT%H%M%SZ'
CONNECT_FACTOR: int = 3
BACK_OFF_FACTOR: float = 0.5


_thread_local = _threading.local()


@contextmanager
def shared_executor(executor):
    """An outside process that will eventually trigger a download through the this module
    can configure a shared Executor by running its code within this context manager."""
    _thread_local.executor = executor
    try:
        yield
    finally:
        del _thread_local.executor


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
        response = self.client._getFileHandleDownload(
            self.request.file_handle_id,
            self.request.object_id,
            objectType=self.request.object_type,
        )
        file_name = response["fileHandle"]["fileName"]
        pre_signed_url = response["preSignedURL"]
        return PresignedUrlInfo(file_name, pre_signed_url, _pre_signed_url_expiration_time(pre_signed_url))


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


def download_file(
    client,
    download_request: DownloadRequest,
    *,
    max_concurrent_parts: int = None,
):
    """
    Main driver for the multi-threaded download. Users an ExecutorService, either set externally onto a thread
    local by an outside process, or creating one as needed otherwise.

    :param client: A synapseclient
    :param download_request: A batch of DownloadRequest objects specifying what Synapse files to download
    :param max_concurrent_parts: The maximum concurrent number parts to download at once when downloading this file
    """

    # we obtain an executor from a thread local if we are in the context of a Synapse sync
    # and wan't to re-use the same threadpool as was created for that
    executor = getattr(_thread_local, 'executor', None)
    shutdown_after = False
    if not executor:
        shutdown_after = True
        executor = get_executor(client.max_threads)

    max_concurrent_parts = max_concurrent_parts or client.max_threads
    try:
        downloader = _MultithreadedDownloader(client, executor, max_concurrent_parts)
        downloader.download_file(download_request)
    finally:
        # if we created the Executor for the purposes of processing this download we also
        # shut it down. if it was passed in from the outside then it's managed by the caller
        if shutdown_after:
            executor.shutdown()


def _get_thread_session():
    # get a lazily initialized requests.Session from the thread.
    # we want to share a requests.Session over the course of a thread
    # to take advantage of persistent http connection. we put it on a
    # thread local since Sessions are not thread safe so we need one per
    # active thread and since we're allowing the use of an externally provided
    # ExecutorService we don't can't really allocate a pool of Sessions ourselves
    session = getattr(_thread_local, 'session', None)
    if not session:
        session = _thread_local.session = _get_new_session()
    return session


class _MultithreadedDownloader:
    """
    An object to manage the downloading of a Synapse file in concurrent chunks from a URL
    that supports range headers.
    """

    def __init__(self, syn, executor, max_concurrent_parts):
        """
        :param syn:                     A synapseclient
        :param executor:                An ExecutorService that will be used to run part downloads in separate threads
        :param max_concurrent_parts:    An integer to specify the maximum number of concurrent parts that can be
                                        downloaded at once. If there are more parts than can be run concurrently
                                        they will be scheduled in the executor when previously running part downloads
                                        complete.
        """
        self._syn = syn
        self._executor = executor
        self._max_concurrent_parts = max_concurrent_parts

    def download_file(self, request):
        url_provider = PresignedUrlProvider(self._syn, request)

        url_info = url_provider.get_info()
        file_size = _get_file_size(url_info.url)
        chunk_range_generator = _generate_chunk_ranges(file_size)

        self._prep_file(request)

        transfer_status = TransferStatus(file_size)

        # the entrant thread runs in a loop doing the following:
        # 1. scheduling any additional part downloads as previous parts are completed
        # 2. writing any completed parts out to the local disk
        # 3. waiting for additional parts to complete
        pending_futures = set()
        completed_futures = set()
        try:
            while True:
                submitted_futures = self._submit_chunks(
                    url_provider,
                    chunk_range_generator,
                    pending_futures,
                )

                self._write_chunks(request, completed_futures, transfer_status)

                # once there is nothing else pending we are done with the file download
                pending_futures = pending_futures.union(submitted_futures)
                if not pending_futures:
                    break

                completed_futures, pending_futures = concurrent.futures.wait(
                    pending_futures,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                self._check_for_errors(request, completed_futures)

        except BaseException:
            # on any exception (e.g. KeyboardInterrupt), attempt to cancel any pending futures.
            # if they are already running this won't have any effect though
            for future in pending_futures:
                future.cancel()

            try:
                os.remove(request.path)
            except FileNotFoundError:
                pass

            raise

    @staticmethod
    def _get_response_with_retry(presigned_url_provider, start: int, end: int) -> Response:
        session = _get_thread_session()
        range_header = {'Range': f'bytes={start}-{end}'}

        def session_get():
            return session.get(presigned_url_provider.get_info().url, headers=range_header)

        response = None
        cause = None
        try:
            # currently when doing a range request to AWS we retry on anything other than a 206.
            # this seems a bit excessive (i.e. some 400 statuses would suggest a non-retryable condition)
            # but for now matching previous behavior.
            response = with_retry(
                session_get,
                expected_status_codes=(HTTPStatus.PARTIAL_CONTENT,),
                retry_errors=RETRYABLE_CONNECTION_ERRORS,
                retry_exceptions=RETRYABLE_CONNECTION_EXCEPTIONS,
            )
        except Exception as ex:
            cause = ex

        if not response or response.status_code != HTTPStatus.PARTIAL_CONTENT:
            raise SynapseError(
                f'Could not download the file: {presigned_url_provider.get_info().file_name},'
                f' please try again.'
            ) from cause

        return start, response

    @staticmethod
    def _prep_file(request):
        # upon receiving the parts of the file we'll open the file
        # and write the specific byte ranges, but to open it in
        # r+ mode we need to to exist and be empty
        open(request.path, 'wb').close()

    def _submit_chunks(self, url_provider, chunk_range_generator, pending_futures):
        submit_count = self._max_concurrent_parts - len(pending_futures)
        submitted_futures = set()

        for chunk_range in chunk_range_generator:
            start, end = chunk_range
            chunk_future = self._executor.submit(
                self._get_response_with_retry,
                url_provider,
                start,
                end,
            )
            submitted_futures.add(chunk_future)

            if len(submitted_futures) == submit_count:
                break

        return submitted_futures

    def _write_chunks(self, request, completed_futures, transfer_status):
        if completed_futures:
            with open(request.path, 'rb+') as file_write:
                for chunk_future in completed_futures:
                    start, chunk_response = chunk_future.result()
                    chunk_data = chunk_response.content
                    file_write.seek(start)
                    file_write.write(chunk_response.content)

                    transfer_status.transferred += len(chunk_data)
                    self._syn._print_transfer_progress(transfer_status.transferred,
                                                       transfer_status.total_bytes_to_be_transferred,
                                                       'Downloading ', os.path.basename(request.path),
                                                       dt=transfer_status.elapsed_time())

    @staticmethod
    def _check_for_errors(request, completed_futures):
        # if any submitted part download failed we abort the download.
        # any retry/recovery should be attempted within the download method
        # submitted to the Executor, if an Exception was flagged on the Future
        # we consider it unrecoverable
        for completed_future in completed_futures:
            exception = completed_future.exception()
            if exception:
                raise ValueError(f"Failed downloading {request.object_id} to {request.path}") from exception
