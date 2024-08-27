"""Logic required for the actual transferring of files."""

try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading

import asyncio
import datetime
import gc
import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Generator, NamedTuple, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, urlparse

import httpx
from opentelemetry import context
from opentelemetry.context import Context

from synapseclient.api.file_services import (
    get_file_handle_for_download,
    get_file_handle_for_download_async,
)
from synapseclient.core.exceptions import (
    SynapseDownloadAbortedException,
    _raise_for_status_httpx,
)
from synapseclient.core.retry import (
    DEFAULT_MAX_BACK_OFF_ASYNC,
    RETRYABLE_CONNECTION_ERRORS,
    RETRYABLE_CONNECTION_EXCEPTIONS,
    with_retry_time_based,
)
from synapseclient.core.transfer_bar import get_or_create_download_progress_bar

if TYPE_CHECKING:
    from synapseclient import Synapse

# constants
MiB: int = 2**20
SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE: int = 8 * MiB
ISO_AWS_STR_FORMAT: str = "%Y%m%dT%H%M%SZ"


class DownloadRequest(NamedTuple):
    """
    A request to download a file from Synapse

    Attributes:
        file_handle_id : The file handle ID to download.
        object_id : The Synapse object this file associated to.
        object_type : The type of the associated Synapse object. Any of
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandleAssociateType.html>
        path : The local path to download the file to.
            This path can be either an absolute path or
            a relative path from where the code is executed to the download location.
        debug: A boolean to specify if debug mode is on.
    """

    file_handle_id: int
    object_id: str
    object_type: str
    path: str
    debug: bool = False


async def download_file(
    client: "Synapse",
    download_request: DownloadRequest,
) -> None:
    """
    Main driver for the multi-threaded download. Users an ExecutorService,
    either set externally onto a thread local by an outside process,
    or creating one as needed otherwise.

    Arguments:
        client: A synapseclient
        download_request: A batch of DownloadRequest objects specifying what
                            Synapse files to download
    """
    downloader = _MultithreadedDownloader(syn=client, download_request=download_request)
    await downloader.download_file()


class PresignedUrlInfo(NamedTuple):
    """
    Information about a retrieved presigned-url

    Attributes:
        file_name: Name of the file for the presigned url
        url: The actual presigned url
        expiration_utc: datetime in UTC at which the url will expire
    """

    file_name: str
    url: str
    expiration_utc: datetime.datetime


@dataclass
class PresignedUrlProvider:
    """
    Provides an un-exipired pre-signed url to download a file
    """

    client: "Synapse"
    request: DownloadRequest
    _lock: _threading.Lock = _threading.Lock()
    _cached_info: Optional[PresignedUrlInfo] = None

    # offset parameter used to buffer url expiration checks, time in seconds
    _TIME_BUFFER: datetime.timedelta = datetime.timedelta(seconds=5)

    async def get_info_async(self) -> PresignedUrlInfo:
        """
        Using async, returns the cached info if it's not expired, otherwise
        retrieves a new pre-signed url and returns that.

        Returns:
            Information about a retrieved presigned-url from either the cache or a
            new request
        """
        if not self._cached_info or (
            datetime.datetime.now(tz=datetime.timezone.utc)
            + PresignedUrlProvider._TIME_BUFFER
            >= self._cached_info.expiration_utc
        ):
            self._cached_info = await self._get_pre_signed_info_async()

        return self._cached_info

    def get_info(self) -> PresignedUrlInfo:
        """
        Using a thread lock, returns the cached info if it's not expired, otherwise
        retrieves a new pre-signed url and returns that.

        Returns:
            Information about a retrieved presigned-url from either the cache or a
            new request
        """
        with self._lock:
            if not self._cached_info or (
                datetime.datetime.now(tz=datetime.timezone.utc)
                + PresignedUrlProvider._TIME_BUFFER
                >= self._cached_info.expiration_utc
            ):
                self._cached_info = self._get_pre_signed_info()

            return self._cached_info

    def _get_pre_signed_info(self) -> PresignedUrlInfo:
        """
        Make an HTTP request to get a pre-signed url to download a file.

        Returns:
            Information about a retrieved presigned-url from a new request.
        """
        response = get_file_handle_for_download(
            file_handle_id=self.request.file_handle_id,
            synapse_id=self.request.object_id,
            entity_type=self.request.object_type,
            synapse_client=self.client,
        )
        file_name = response["fileHandle"]["fileName"]
        pre_signed_url = response["preSignedURL"]
        return PresignedUrlInfo(
            file_name=file_name,
            url=pre_signed_url,
            expiration_utc=_pre_signed_url_expiration_time(pre_signed_url),
        )

    async def _get_pre_signed_info_async(self) -> PresignedUrlInfo:
        """
        Make an HTTP request to get a pre-signed url to download a file.

        Returns:
            Information about a retrieved presigned-url from a new request.
        """
        response = await get_file_handle_for_download_async(
            file_handle_id=self.request.file_handle_id,
            synapse_id=self.request.object_id,
            entity_type=self.request.object_type,
            synapse_client=self.client,
        )
        file_name = response["fileHandle"]["fileName"]
        pre_signed_url = response["preSignedURL"]
        return PresignedUrlInfo(
            file_name=file_name,
            url=pre_signed_url,
            expiration_utc=_pre_signed_url_expiration_time(pre_signed_url),
        )


def _generate_chunk_ranges(
    file_size: int,
) -> Generator[Tuple[int, int], None, None]:
    """
    Creates a generator which yields byte ranges and meta data required
    to make a range request download of url and write the data to file_name
    located at path. Download chunk sizes are 8MB by default.

    Arguments:
        file_size: The size of the file

    Yields:
        A generator of byte ranges and meta data needed to download
        the file in a multi-threaded manner
    """
    for start in range(0, file_size, SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE):
        # the start and end of a range in HTTP are both inclusive
        end = min(start + SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE, file_size) - 1
        yield start, end


def _pre_signed_url_expiration_time(url: str) -> datetime:
    """
    Returns time at which a presigned url will expire

    Arguments:
        url: A pre-signed download url from AWS

    Returns:
        A datetime in UTC of when the url will expire
    """
    parsed_query = parse_qs(urlparse(url).query)
    time_made = parsed_query["X-Amz-Date"][0]
    time_made_datetime = datetime.datetime.strptime(time_made, ISO_AWS_STR_FORMAT)
    expires = parsed_query["X-Amz-Expires"][0]
    return_data = (
        time_made_datetime + datetime.timedelta(seconds=int(expires))
    ).replace(tzinfo=datetime.timezone.utc)
    return return_data


async def _get_file_size_wrapper(syn: "Synapse", url: str, debug: bool) -> int:
    """
    Gets the size of the file located at url

    Arguments:
        syn: The synapseclient
        url: The pre-signed url of the file
        debug: A boolean to specify if debug mode is on

    Returns:
        The size of the file in bytes
    """
    loop = asyncio.get_running_loop()
    otel_context = context.get_current()
    return await loop.run_in_executor(
        syn._get_thread_pool_executor(asyncio_event_loop=loop),
        _get_file_size,
        syn,
        url,
        debug,
        otel_context,
    )


def _get_file_size(
    syn: "Synapse", url: str, debug: bool, otel_context: context.Context = None
) -> int:
    """
    Gets the size of the file located at url

    Arguments:
        url: The pre-signed url of the file
        debug: A boolean to specify if debug mode is on
        otel_context: The OpenTelemetry context

    Returns:
        The size of the file in bytes
    """
    if otel_context:
        context.attach(otel_context)
    with syn._requests_session_storage.stream("GET", url) as response:
        _raise_for_status_httpx(
            response=response,
            logger=syn.logger,
            verbose=debug,
            read_response_content=False,
        )
        return int(response.headers["Content-Length"])


class _MultithreadedDownloader:
    """
    An object to manage the downloading of a Synapse file in concurrent chunks
    from a URL that supports range headers.
    """

    def __init__(
        self,
        syn: "Synapse",
        download_request: DownloadRequest,
    ) -> None:
        """
        Initializes the class

        Arguments:
            syn: A synapseclient
            executor: An ExecutorService that will be used to run part downloads
                         in separate threads
        """
        self._syn = syn
        self._thread_lock = _threading.Lock()
        self._aborted = False
        self._download_request = download_request
        self._progress_bar = None

    async def download_file(self) -> None:
        """
        Splits up and downloads a file in chunks from a URL.
        """
        url_provider = PresignedUrlProvider(self._syn, request=self._download_request)

        url_info = await url_provider.get_info_async()
        file_size = await _get_file_size_wrapper(
            syn=self._syn, url=url_info.url, debug=self._download_request.debug
        )
        self._progress_bar = get_or_create_download_progress_bar(
            file_size=file_size,
            postfix=self._download_request.object_id,
            synapse_client=self._syn,
        )
        self._prep_file()

        # Create AsyncIO tasks to download each of the parts according to chunk size
        download_tasks = self._generate_stream_and_write_chunk_tasks(
            url_provider=url_provider,
            chunk_range_generator=_generate_chunk_ranges(file_size),
        )
        await self._execute_download_tasks(download_tasks=download_tasks)

    async def _execute_download_tasks(self, download_tasks: Set[asyncio.Task]) -> None:
        """Handle the execution of the download tasks.

        Arguments:
            request: A DownloadRequest object specifying what Synapse file to download.
            download_tasks: A set of asyncio tasks to download the file in chunks.

        Returns:
            None
        """
        cause = None
        while download_tasks:
            done_tasks, pending_tasks = await asyncio.wait(
                download_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            download_tasks = pending_tasks
            for completed_task in done_tasks:
                try:
                    start_bytes, end_bytes = completed_task.result()
                    del completed_task
                    self._syn._parts_transfered_counter += 1

                    # Garbage collect every 100 iterations
                    if self._syn._parts_transfered_counter % 100 == 0:
                        gc.collect()

                    # self._syn.logger.debug(
                    #     f"Downloaded bytes {start_bytes}-{end_bytes} to {self._download_request.path}"
                    # )
                except BaseException as ex:
                    # on any exception (e.g. KeyboardInterrupt), attempt to cancel any pending futures.
                    # if they are already running this won't have any effect though
                    cause = ex

                    with self._thread_lock:
                        if self._aborted:
                            # we've already aborted, no need to execute this logic again
                            continue
                        self._aborted = True

                    try:
                        self._syn.logger.exception(
                            f"Failed downloading {self._download_request.object_id} to {self._download_request.path}"
                        )
                        os.remove(self._download_request.path)
                    except FileNotFoundError:
                        pass

        if cause:
            raise cause

    def _update_progress_bar(self, part_size: int) -> None:
        """Update the progress bar with the given part size."""
        if self._syn.silent or not self._progress_bar:
            return
        self._progress_bar.update(part_size)

    def _generate_stream_and_write_chunk_tasks(
        self,
        url_provider: PresignedUrlProvider,
        chunk_range_generator: Generator[Tuple[int, int], None, None],
    ) -> Set[asyncio.Task]:
        download_tasks = set()
        session = self._syn._requests_session_storage
        for chunk_range in chunk_range_generator:
            start, end = chunk_range

            download_tasks.add(
                asyncio.create_task(
                    self._stream_and_write_chunk_wrapper(
                        session=session,
                        url_provider=url_provider,
                        start=start,
                        end=end,
                    )
                )
            )

        return download_tasks

    async def _stream_and_write_chunk_wrapper(
        self,
        session: httpx.Client,
        url_provider: PresignedUrlProvider,
        start: int,
        end: int,
    ) -> Tuple[int, int]:
        loop = asyncio.get_running_loop()
        otel_context = context.get_current()
        return await loop.run_in_executor(
            self._syn._get_thread_pool_executor(asyncio_event_loop=loop),
            self._stream_and_write_chunk,
            session,
            url_provider,
            start,
            end,
            otel_context,
        )

    def _check_for_abort(self, start: int, end: int) -> None:
        """Check if the download has been aborted and raise an exception if so."""
        with self._thread_lock:
            if self._aborted:
                raise SynapseDownloadAbortedException(
                    f"Download aborted, skipping bytes {start}-{end}"
                )

    def _stream_and_write_chunk(
        self,
        session: httpx.Client,
        presigned_url_provider: PresignedUrlProvider,
        start: int,
        end: int,
        otel_context: Union[Context, None],
    ) -> Tuple[int, int]:
        """
        Wrapper around the actual download logic to handle retries and range requests.

        Arguments:
            session: An httpx.Client
            presigned_url_provider: A URL provider for the presigned urls
            start: The start byte of the range to download
            end: The end byte of the range to download
            otel_context: The OpenTelemetry context if known, else None

        Returns:
            The start and end bytes of the range downloaded
        """
        if otel_context:
            context.attach(otel_context)
        self._check_for_abort(start=start, end=end)
        range_header = {"Range": f"bytes={start}-{end}"}

        # currently when doing a range request to AWS we retry on anything other than a 206.
        # this seems a bit excessive (i.e. some 400 statuses would suggest a non-retryable condition)
        # but for now matching previous behavior.
        end = with_retry_time_based(
            lambda: _execute_stream_and_write_chunk(
                session=session,
                request=self,
                presigned_url_provider=presigned_url_provider,
                range_header=range_header,
                start=start,
            ),
            expected_status_codes=(HTTPStatus.PARTIAL_CONTENT,),
            retry_errors=RETRYABLE_CONNECTION_ERRORS,
            retry_exceptions=RETRYABLE_CONNECTION_EXCEPTIONS,
            retry_max_back_off=DEFAULT_MAX_BACK_OFF_ASYNC,
            read_response_content=False,
        )

        return start, end

    def _prep_file(self) -> None:
        """
        Upon receiving the parts of the file we'll open the file and write the specific
        byte ranges, but to open it in r+ mode we need it to exist and be empty.
        """
        open(self._download_request.path, "wb").close()

    def _write_chunk(
        self, request: DownloadRequest, chunk: bytes, start: int, length: int
    ) -> None:
        """Open the file and write the chunk to the specified byte range. Also update
        the progress bar.

        Arguments:
            request: A DownloadRequest object specifying what Synapse file to download
            chunk: The chunk of data to write to the file
            start: The start byte of the range to download
            length: The length of the chunk

        Returns:
            None
        """
        with self._thread_lock:
            with open(request.path, "rb+") as file_write:
                file_write.seek(start)
                file_write.write(chunk)
                self._update_progress_bar(part_size=length)


def _execute_stream_and_write_chunk(
    session: httpx.Client,
    request: _MultithreadedDownloader,
    presigned_url_provider: PresignedUrlProvider,
    range_header: httpx.Headers,
    start: int,
) -> int:
    """
    Coordinates the streaming of a chunk of data from a presigned url and writing it to
    a file.

    Arguments:
        session: An httpx.Client
        request: The request object for the download
        presigned_url_provider: A URL provider for the presigned urls
        range_header: The range of bytes to download
        start: The start byte of the range to download

    Returns:
        The end byte of the range downloaded
    """
    additional_offset = 0
    with session.stream(
        method="GET", url=presigned_url_provider.get_info().url, headers=range_header
    ) as response:
        _raise_for_status_httpx(
            response=response, logger=request._syn.logger, read_response_content=False
        )
        data = response.read()
        data_length = len(data)
        request._write_chunk(
            request=request._download_request,
            chunk=data,
            start=start + additional_offset,
            length=data_length,
        )
        additional_offset = data_length
    del data
    return start + additional_offset
