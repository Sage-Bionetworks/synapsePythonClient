"""Implements the client side of Synapse's
[Multipart File Upload API](https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.file.controller.UploadController),
which provides a robust means of uploading large files (into the 10s of GiB). End users
should not need to call any of the methods under
[UploadAttempt][synapseclient.core.upload.multipart_upload.UploadAttempt] directly.


This mermaid flowchart illustrates the process of uploading a file to Synapse using
the multipart upload API.

```mermaid
flowchart  TD
    upload_file_handle --> before-upload
    subgraph before-upload
        subgraph Disk I/O & CPU
            subgraph Multi-Processing
                md5["Calculate MD5"]
            end
            mime["Guess mime type"]
            file_size["Get file size"]
            file_name["Get file name"]
        end

        subgraph HTTP
            upload_destination["Find where to Upload \n GET /entity/{entity_id}/uploadDestination"]
            start_upload["Start upload with Synapse \n POST /file/multipart"]
            presigned_urls["Get URLs to upload to \n POST /file/multipart/{upload_id}/presigned/url/batch"]
        end
    end

    before-upload --> during-upload

    subgraph during-upload
        subgraph multi-threaded["multi-threaded for each part"]
            read_part["Read part to upload into Memory"]
            read_part --> put_part["HTTP PUT to storage provider"]

            subgraph thread_locked1["Lock thread"]
                refresh_check{"New URl available?"}
                refresh_check --> |no|refresh
                refresh["Refresh remaining URLs to upload to \n POST /file/multipart/{upload_id}/presigned/url/batch"]
            end


            put_part --> |URL Expired|refresh_check
            refresh_check --> |yes|put_part
            refresh --> put_part
            put_part --> |Finished|md5_part["Calculate MD5 of part"]
        end
        complete_part["PUT /file/multipart/{upload_id}/add/{part_number}?partMD5Hex={md5_hex}"]
        multi-threaded -->|Upload finished| complete_part
    end

    during-upload --> post-upload

    subgraph post-upload
        post_upload_compelete["PUT /file/multipart/{upload_id}/complete"]
        get_file_handle["GET /fileHandle/{file_handle_id}"]
    end

    post-upload --> entity["Create/Update Synapse entity"]
```
"""

import asyncio
import gc
import mimetypes
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import httpx
import psutil
import requests
from opentelemetry import context, trace
from opentelemetry.context import Context
from tqdm import tqdm

from synapseclient.api import (
    AddPartResponse,
    post_file_multipart,
    post_file_multipart_presigned_urls,
    put_file_multipart_add,
    put_file_multipart_complete,
)
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import (
    SynapseHTTPError,
    SynapseUploadAbortedException,
    SynapseUploadFailedException,
    _raise_for_status_httpx,
)
from synapseclient.core.retry import with_retry_time_based
from synapseclient.core.upload.upload_utils import (
    copy_md5_fn,
    copy_part_request_body_provider_fn,
    get_data_chunk,
    get_file_chunk,
    get_part_size,
)
from synapseclient.core.utils import MB
from synapseclient.core.utils import md5_fn as md5_fn_util
from synapseclient.core.utils import md5_for_file_hex

if TYPE_CHECKING:
    from synapseclient import Synapse

# AWS limits
MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 5 * MB

# ancient tribal knowledge
DEFAULT_PART_SIZE = 8 * MB
MAX_RETRIES = 7

_thread_local = threading.local()


@contextmanager
def shared_progress_bar(progress_bar):
    """An outside process that will eventually trigger an upload through this module
    can configure a shared Progress Bar by running its code within this context manager.
    """
    _thread_local.progress_bar = progress_bar
    try:
        yield
    finally:
        _thread_local.progress_bar.close()
        _thread_local.progress_bar.refresh()
        del _thread_local.progress_bar


@dataclass
class HandlePartResult:
    """Result of a part upload.

    Attributes:
        part_number: The part number that was uploaded.
        part_size: The size of the part that was uploaded.
        md5_hex: The MD5 hash of the part that was uploaded.
    """

    part_number: int
    part_size: int
    md5_hex: str


class UploadAttemptAsync:
    """
    Used to handle multi-threaded operations for uploading one or parts of a file.
    """

    def __init__(
        self,
        syn: "Synapse",
        dest_file_name: str,
        upload_request_payload: Dict[str, Any],
        part_request_body_provider_fn: Union[None, Callable[[int], bytes]],
        md5_fn: Callable[[bytes, httpx.Response], str],
        force_restart: bool,
        storage_str: str = None,
    ) -> None:
        self._syn = syn
        self._dest_file_name = dest_file_name
        self._part_size = upload_request_payload["partSizeBytes"]

        self._upload_request_payload = upload_request_payload

        self._part_request_body_provider_fn = part_request_body_provider_fn
        self._md5_fn = md5_fn

        self._force_restart = force_restart

        self._lock = asyncio.Lock()
        self._thread_lock = threading.Lock()
        self._aborted = False
        self._storage_str = storage_str

        self._close_progress_bar = getattr(_thread_local, "progress_bar", None) is None
        # populated later
        self._upload_id: Optional[str] = None
        self._pre_signed_part_urls: Optional[Mapping[int, str]] = None
        self._progress_bar = None

    async def __call__(self) -> Dict[str, str]:
        """Orchestrate the upload of a file to Synapse."""
        upload_status_response = await post_file_multipart(
            upload_request_payload=self._upload_request_payload,
            force_restart=self._force_restart,
            endpoint=self._syn.fileHandleEndpoint,
            synapse_client=self._syn,
        )
        upload_state = upload_status_response.get("state")

        if upload_state != "COMPLETED":
            self._upload_id = upload_status_response["uploadId"]
            part_count, remaining_part_numbers = self._get_remaining_part_numbers(
                upload_status_response
            )

            # if no remaining part numbers then all the parts have been
            # uploaded but the upload has not been marked complete.
            if remaining_part_numbers:
                await self._upload_parts(part_count, remaining_part_numbers)
            upload_status_response = await self._complete_upload()

        return upload_status_response

    @classmethod
    def _get_remaining_part_numbers(
        cls, upload_status: Dict[str, str]
    ) -> Tuple[int, List[int]]:
        part_numbers = []
        parts_state = upload_status["partsState"]

        # parts are 1-based
        for i, part_status in enumerate(parts_state, 1):
            if part_status == "0":
                part_numbers.append(i)

        return len(parts_state), part_numbers

    def _is_copy(self) -> bool:
        # is this a copy or upload request
        return (
            self._upload_request_payload.get("concreteType")
            == concrete_types.MULTIPART_UPLOAD_COPY_REQUEST
        )

    async def _fetch_pre_signed_part_urls_async(
        self,
        upload_id: str,
        part_numbers: List[int],
    ) -> Mapping[int, str]:
        trace.get_current_span().set_attributes({"synapse.upload_id": upload_id})
        response = await post_file_multipart_presigned_urls(
            upload_id=upload_id,
            part_numbers=part_numbers,
            synapse_client=self._syn,
        )

        part_urls = {}
        for part in response["partPresignedUrls"]:
            part_urls[part["partNumber"]] = (
                part["uploadPresignedUrl"],
                part.get("signedHeaders", {}),
            )

        return part_urls

    def _refresh_pre_signed_part_urls(
        self,
        part_number: int,
        expired_url: str,
    ) -> Tuple[str, Dict[str, str]]:
        """Refresh all unfetched presigned urls, and return the refreshed
        url for the given part number. If an existing expired_url is passed
        and the url for the given part has already changed that new url
        will be returned without a refresh (i.e. it is assumed that another
        thread has already refreshed the url since the passed url expired).

        Arguments:
            part_number: the part number whose refreshed url should
                         be returned
            expired_url: the url that was detected as expired triggering
                         this refresh
        Returns:
            refreshed URL

        """
        with self._thread_lock:
            current_url, headers = self._pre_signed_part_urls[part_number]
            if current_url != expired_url:
                # if the url has already changed since the given url
                # was detected as expired we can assume that another
                # thread already refreshed the url and can avoid the extra
                # fetch.
                refreshed_url = current_url, headers
            else:
                self._pre_signed_part_urls = wrap_async_to_sync(
                    self._fetch_pre_signed_part_urls_async(
                        self._upload_id,
                        list(self._pre_signed_part_urls.keys()),
                    ),
                    syn=self._syn,
                )

                refreshed_url = self._pre_signed_part_urls[part_number]

        return refreshed_url

    async def _handle_part_wrapper(self, part_number: int) -> HandlePartResult:
        loop = asyncio.get_running_loop()
        otel_context = context.get_current()

        mem_info = psutil.virtual_memory()

        if mem_info.available <= self._part_size * 2:
            gc.collect()

        return await loop.run_in_executor(
            self._syn._get_thread_pool_executor(asyncio_event_loop=loop),
            self._handle_part,
            part_number,
            otel_context,
        )

    async def _upload_parts(
        self, part_count: int, remaining_part_numbers: List[int]
    ) -> None:
        """Take a list of part numbers and upload them to the pre-signed URLs.

        Arguments:
            part_count: The total number of parts in the upload.
            remaining_part_numbers: The parts that still need to be uploaded.
        """
        completed_part_count = part_count - len(remaining_part_numbers)
        file_size = self._upload_request_payload.get("fileSizeBytes")

        self._pre_signed_part_urls = await self._fetch_pre_signed_part_urls_async(
            upload_id=self._upload_id,
            part_numbers=remaining_part_numbers,
        )

        async_tasks = []

        for part_number in remaining_part_numbers:
            async_tasks.append(
                asyncio.create_task(self._handle_part_wrapper(part_number=part_number))
            )

        if not self._syn.silent and not self._progress_bar:
            if self._is_copy():
                # we won't have bytes to measure during a copy so the byte oriented
                # progress bar is not useful
                self._progress_bar = getattr(
                    _thread_local, "progress_bar", None
                ) or tqdm(
                    total=part_count,
                    desc=self._storage_str or "Copying",
                    unit_scale=True,
                    postfix=self._dest_file_name,
                    smoothing=0,
                )
                self._progress_bar.update(completed_part_count)
            else:
                previously_transferred = min(
                    completed_part_count * self._part_size,
                    file_size,
                )

                self._progress_bar = getattr(
                    _thread_local, "progress_bar", None
                ) or tqdm(
                    total=file_size,
                    desc=self._storage_str or "Uploading",
                    unit="B",
                    unit_scale=True,
                    postfix=self._dest_file_name,
                    smoothing=0,
                )
                self._progress_bar.update(previously_transferred)

        raised_exception = await self._orchestrate_upload_part_tasks(async_tasks)

        if raised_exception is not None:
            if isinstance(raised_exception, KeyboardInterrupt):
                raise SynapseUploadAbortedException(
                    "User interrupted upload"
                ) from raised_exception
            raise SynapseUploadFailedException(
                "Part upload failed"
            ) from raised_exception

    def _update_progress_bar(self, part_size: int) -> None:
        """Update the progress bar with the given part size."""
        if self._syn.silent or not self._progress_bar:
            return
        self._progress_bar.update(1 if self._is_copy() else part_size)

    async def _orchestrate_upload_part_tasks(
        self, async_tasks: List[asyncio.Task]
    ) -> Union[Exception, KeyboardInterrupt, None]:
        """
        Orchestrate the result of the upload part tasks. If successful, send a
        request to the server to add the part to the upload.

        Arguments:
            async_tasks: A set of tasks to orchestrate.

        Returns:
            An exception if one was raised, otherwise None.
        """
        raised_exception = None

        while async_tasks:
            done_tasks, pending_tasks = await asyncio.wait(
                async_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            async_tasks = pending_tasks
            for completed_task in done_tasks:
                try:
                    task_result = completed_task.result()

                    if isinstance(task_result, HandlePartResult):
                        part_number = task_result.part_number
                        part_size = task_result.part_size
                        part_md5_hex = task_result.md5_hex
                    elif (
                        isinstance(task_result, AddPartResponse)
                        and task_result.add_part_state != "ADD_SUCCESS"
                    ):
                        # Restart the file upload process resuming where this left off.
                        # Rest docs state:
                        # "If add part fails for any reason, the client must re-upload
                        # the part and then re-attempt to add the part to the upload."
                        raise SynapseUploadFailedException(
                            (
                                "Adding individual part failed with unexpected state: "
                                f"{task_result.add_part_state}, for upload "
                                f"{task_result.upload_id} and part "
                                f"{task_result.part_number} with message: "
                                f"{task_result.error_message}"
                            )
                        )
                    else:
                        continue

                    async_tasks.add(
                        asyncio.create_task(
                            put_file_multipart_add(
                                upload_id=self._upload_id,
                                part_number=part_number,
                                md5_hex=part_md5_hex,
                                synapse_client=self._syn,
                            )
                        )
                    )

                    self._update_progress_bar(part_size=part_size)

                except (Exception, KeyboardInterrupt) as cause:
                    with self._thread_lock:
                        if self._aborted:
                            # we've already aborted, no need to raise
                            # another exception
                            continue
                        self._aborted = True
                    raised_exception = cause
                    continue
        return raised_exception

    async def _complete_upload(self) -> Dict[str, str]:
        """Close the upload and mark it as complete.

        Returns:
            The response from the server for the completed upload.
        """
        if not self._syn.silent and self._progress_bar and self._close_progress_bar:
            self._progress_bar.close()
        upload_status_response = await put_file_multipart_complete(
            upload_id=self._upload_id,
            endpoint=self._syn.fileHandleEndpoint,
            synapse_client=self._syn,
        )

        upload_state = upload_status_response.get("state")
        if upload_state != "COMPLETED":
            # at this point we think successfully uploaded all the parts
            # but the upload status isn't complete, we'll throw an error
            # and let a subsequent attempt try to reconcile
            raise SynapseUploadFailedException(
                f"Upload status has an unexpected state {upload_state}"
            )

        return upload_status_response

    def _handle_part(
        self, part_number: int, otel_context: Union[Context, None]
    ) -> HandlePartResult:
        """Take an individual part number and upload it to the pre-signed URL.

        Arguments:
            part_number: The part number to upload.
            otel_context: The OpenTelemetry context to use for tracing.

        Returns:
            The result of the part upload.

        Raises:
            SynapseUploadAbortedException: If the upload has been aborted.
            ValueError: If the part body is None.
        """
        if otel_context:
            context.attach(otel_context)
        with self._thread_lock:
            if self._aborted:
                # this upload attempt has already been aborted
                # so we short circuit the attempt to upload this part
                raise SynapseUploadAbortedException(
                    f"Upload aborted, skipping part {part_number}"
                )

            part_url, signed_headers = self._pre_signed_part_urls.get(part_number)

        session: httpx.Client = self._syn._requests_session_storage

        # obtain the body (i.e. the upload bytes) for the given part number.
        body = (
            self._part_request_body_provider_fn(part_number)
            if self._part_request_body_provider_fn
            else None
        )
        part_size = len(body) if body else 0
        self._syn.logger.debug(f"Uploading part {part_number} of size {part_size}")
        if not self._is_copy() and body is None:
            raise ValueError(f"No body for part {part_number}")

        response = self._put_part_with_retry(
            session=session,
            body=body,
            part_url=part_url,
            signed_headers=signed_headers,
            part_number=part_number,
        )

        md5_hex = self._md5_fn(body, response)
        del response
        del body

        # # remove so future batch pre_signed url fetches will exclude this part
        with self._thread_lock:
            del self._pre_signed_part_urls[part_number]

        return HandlePartResult(part_number, part_size, md5_hex)

    def _put_part_with_retry(
        self,
        session: httpx.Client,
        body: bytes,
        part_url: str,
        signed_headers: Dict[str, str],
        part_number: int,
    ) -> Union[httpx.Response, None]:
        """Put a part to the storage provider with retries.

        Arguments:
            session: The requests session to use for the put.
            body: The body of the part to put.
            part_url: The URL to put the part to.
            signed_headers: The signed headers to use for the put.
            part_number: The part number being put.

        Returns:
            The response from the put.

        Raises:
            SynapseHTTPError: If the put fails.
        """
        response = None
        for retry in range(2):
            try:
                # use our backoff mechanism here, we have encountered 500s on puts to AWS signed urls

                response = with_retry_time_based(
                    lambda part_url=part_url, signed_headers=signed_headers: session.put(
                        url=part_url,
                        content=body,  # noqa: F821
                        headers=signed_headers,
                    ),
                    retry_exceptions=[requests.exceptions.ConnectionError],
                )

                _raise_for_status_httpx(response=response, logger=self._syn.logger)

                # completed upload part to s3 successfully
                break

            except SynapseHTTPError as ex:
                if ex.response.status_code == 403 and retry < 1:
                    # we interpret this to mean our pre_signed url expired.
                    self._syn.logger.debug(
                        f"The pre-signed upload URL for part {part_number} has expired. "
                        "Refreshing urls and retrying.\n"
                    )

                    # we refresh all the urls and obtain this part's
                    # specific url for the retry
                    (
                        part_url,
                        signed_headers,
                    ) = self._refresh_pre_signed_part_urls(
                        part_number,
                        part_url,
                    )

                else:
                    raise
        return response


async def multipart_upload_file_async(
    syn: "Synapse",
    file_path: str,
    dest_file_name: str = None,
    content_type: str = None,
    part_size: int = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
    md5: str = None,
    storage_str: str = None,
) -> str:
    """Upload a file to a Synapse upload destination in chunks.

    Arguments:
        syn: a Synapse object
        file_path: the file to upload
        dest_file_name: upload as a different filename
        content_type: Refers to the Content-Type of the API request.
        part_size: Number of bytes per part. Minimum is 5MiB (5 * 1024 * 1024 bytes).
        storage_location_id: an id indicating where the file should be
                             stored. Retrieved from Synapse's UploadDestination
        preview: True to generate a preview
        force_restart: True to restart a previously initiated upload
                       from scratch, False to try to resume
        md5: The MD5 of the file. If not provided, it will be calculated.
        storage_str: Optional string to append to the upload message

    Returns:
        a File Handle ID

    Keyword arguments are passed down to
    [_multipart_upload()][synapseclient.core.upload.multipart_upload._multipart_upload].

    """
    trace.get_current_span().set_attributes(
        {
            "synapse.storage_location_id": (
                storage_location_id if storage_location_id is not None else ""
            )
        }
    )

    if not os.path.exists(file_path):
        raise IOError(f'File "{file_path}" not found.')
    if os.path.isdir(file_path):
        raise IOError(f'File "{file_path}" is a directory.')

    file_size = os.path.getsize(file_path)
    if not dest_file_name:
        dest_file_name = os.path.basename(file_path)

    if content_type is None:
        mime_type, _ = mimetypes.guess_type(file_path, strict=False)
        content_type = mime_type or "application/octet-stream"

    md5_hex = md5 or md5_for_file_hex(filename=file_path)

    part_size = get_part_size(
        part_size or DEFAULT_PART_SIZE,
        file_size,
        MIN_PART_SIZE,
        MAX_NUMBER_OF_PARTS,
    )

    upload_request = {
        "concreteType": concrete_types.MULTIPART_UPLOAD_REQUEST,
        "contentType": content_type,
        "contentMD5Hex": md5_hex,
        "fileName": dest_file_name,
        "fileSizeBytes": file_size,
        "generatePreview": preview,
        "partSizeBytes": part_size,
        "storageLocationId": storage_location_id,
    }

    def part_fn(part_number: int) -> bytes:
        """Return the nth chunk of a file."""
        return get_file_chunk(file_path, part_number, part_size)

    return await _multipart_upload_async(
        syn,
        dest_file_name,
        upload_request,
        part_fn,
        md5_fn_util,
        force_restart=force_restart,
        storage_str=storage_str,
    )


async def _multipart_upload_async(
    syn: "Synapse",
    dest_file_name: str,
    upload_request: Dict[str, Any],
    part_fn: Callable[[int], bytes],
    md5_fn: Callable[[bytes, httpx.Response], str],
    force_restart: bool = False,
    storage_str: str = None,
) -> str:
    """Calls upon an [UploadAttempt][synapseclient.core.upload.multipart_upload.UploadAttempt]
    object to initiate and/or retry a multipart file upload or copy. This function is wrapped by
    [multipart_upload_file][synapseclient.core.upload.multipart_upload.multipart_upload_file],
    [multipart_upload_string][synapseclient.core.upload.multipart_upload.multipart_upload_string], and
    [multipart_copy][synapseclient.core.upload.multipart_upload.multipart_copy].
    Retries cannot exceed 7 retries per call.

    Arguments:
        syn: A Synapse object
        dest_file_name: upload as a different filename
        upload_request: A dictionary object with the user-fed logistical
                        details of the upload/copy request.
        part_fn: Function to calculate the partSize of each part
        md5_fn: Function to calculate the MD5 of the file-like object
        storage_str: Optional string to append to the upload message

    Returns:
        A File Handle ID

    """
    retry = 0
    while True:
        try:
            upload_status_response = await UploadAttemptAsync(
                syn,
                dest_file_name,
                upload_request,
                part_fn,
                md5_fn,
                # only force_restart the first time through (if requested).
                # a retry after a caught exception will not restart the upload
                # from scratch.
                force_restart and retry == 0,
                storage_str=storage_str,
            )()

            # success
            return upload_status_response["resultFileHandleId"]

        except SynapseUploadFailedException:
            if retry < MAX_RETRIES:
                retry += 1
            else:
                raise


async def multipart_upload_string_async(
    syn: "Synapse",
    text: str,
    dest_file_name: str = None,
    part_size: int = None,
    content_type: str = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
) -> str:
    """Upload a file to a Synapse upload destination in chunks.

    Arguments:
        syn: a Synapse object
        text: a string to upload as a file.
        dest_file_name: upload as a different filename
        content_type: Refers to the Content-Type of the API request.
        part_size: number of bytes per part. Minimum 5MB.
        storage_location_id: an id indicating where the file should be
                             stored. Retrieved from Synapse's UploadDestination
        preview: True to generate a preview
        force_restart: True to restart a previously initiated upload
                       from scratch, False to try to resume

    Returns:
        a File Handle ID

    Keyword arguments are passed down to
    [_multipart_upload()][synapseclient.core.upload.multipart_upload._multipart_upload].

    """
    data = text.encode("utf-8")
    file_size = len(data)
    md5_hex = md5_fn_util(data, None)

    if not dest_file_name:
        dest_file_name = "message.txt"

    if not content_type:
        content_type = "text/plain; charset=utf-8"

    part_size = get_part_size(
        part_size or DEFAULT_PART_SIZE, file_size, MIN_PART_SIZE, MAX_NUMBER_OF_PARTS
    )

    upload_request = {
        "concreteType": concrete_types.MULTIPART_UPLOAD_REQUEST,
        "contentType": content_type,
        "contentMD5Hex": md5_hex,
        "fileName": dest_file_name,
        "fileSizeBytes": file_size,
        "generatePreview": preview,
        "partSizeBytes": part_size,
        "storageLocationId": storage_location_id,
    }

    def part_fn(part_number: int) -> bytes:
        """Get the nth chunk of a buffer."""
        return get_data_chunk(data, part_number, part_size)

    part_size = get_part_size(
        part_size or DEFAULT_PART_SIZE, file_size, MIN_PART_SIZE, MAX_NUMBER_OF_PARTS
    )
    return await _multipart_upload_async(
        syn,
        dest_file_name,
        upload_request,
        part_fn,
        md5_fn_util,
        force_restart=force_restart,
    )


async def multipart_copy_async(
    syn: "Synapse",
    source_file_handle_association: Dict[str, str],
    dest_file_name: str = None,
    part_size: int = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
) -> str:
    """Makes a
    [Multipart Upload Copy Request](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadCopyRequest.html).
    This request performs a copy of an existing file handle without data transfer from the client.

    Arguments:
        syn: A Synapse object
        source_file_handle_association: Describes an association of a FileHandle with another object.
        dest_file_name: The name of the file to be uploaded.
        part_size: The size that each part will be (in bytes).
        storage_location_id: The identifier of the storage location where this file should be copied to.
                             The user must be the owner of the storage location.
        preview: True to generate a preview of the data.
        force_restart: True to restart a previously initiated upload from scratch, False to try to resume.

    Returns:
        a File Handle ID

    Keyword arguments are passed down to
    [_multipart_upload()][synapseclient.core.upload.multipart_upload._multipart_upload].

    """
    part_size = part_size or DEFAULT_PART_SIZE

    upload_request = {
        "concreteType": concrete_types.MULTIPART_UPLOAD_COPY_REQUEST,
        "fileName": dest_file_name,
        "generatePreview": preview,
        "partSizeBytes": part_size,
        "sourceFileHandleAssociation": source_file_handle_association,
        "storageLocationId": storage_location_id,
    }

    return await _multipart_upload_async(
        syn,
        dest_file_name,
        upload_request,
        copy_part_request_body_provider_fn,
        copy_md5_fn,
        force_restart=force_restart,
    )
