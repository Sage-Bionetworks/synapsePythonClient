"""Implements the client side of
Synapse's [Multipart File Upload API](https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.file.controller.UploadController), which provides a
robust means of uploading large files (into the 10s of GiB). End users should not need to call any of the methods under
[UploadAttempt][synapseclient.core.upload.multipart_upload.UploadAttempt] directly.

"""

import asyncio
import concurrent.futures
import math
import mimetypes
import os
import re
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, List, Mapping, Union

import httpx
import requests
from opentelemetry import context, trace
from opentelemetry.context import Context

from synapseclient.api import (
    post_file_multipart,
    post_file_multipart_presigned_urls,
    put_file_multipart_add,
    put_file_multipart_complete,
)
from synapseclient.core import pool_provider
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import (
    SynapseHTTPError,
    SynapseUploadAbortedException,
    SynapseUploadFailedException,
    _raise_for_status,
)
from synapseclient.core.retry import with_retry_non_async
from synapseclient.core.utils import MB, Spinner, md5_fn, md5_for_file

if TYPE_CHECKING:
    from synapseclient import Synapse

# AWS limits
MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 5 * MB

# ancient tribal knowledge
DEFAULT_PART_SIZE = 8 * MB
MAX_RETRIES = 30


tracer = trace.get_tracer("synapseclient")

_thread_local = threading.local()


@contextmanager
def _executor(max_threads, shutdown_wait):
    """Yields an executor for running some asynchronous code, either obtaining the executor
    from the shared_executor or otherwise creating one.

    Arguments:
        max_threads: the maxmimum number of threads a created executor should use
        shutdown_wait: whether a created executor should shutdown after running the yielded to code
    """
    executor = getattr(_thread_local, "executor", None)
    shutdown_after = False
    if not executor:
        shutdown_after = True
        executor = pool_provider.get_executor(thread_count=max_threads)

    _thread_local.executor = executor
    try:
        yield executor
    finally:
        if shutdown_after:
            executor.shutdown(wait=shutdown_wait)


class UploadAttemptAsync:
    """
    Used to handle multi-threaded operations for uploading one or parts of a file.
    """

    def __init__(
        self,
        syn: "Synapse",
        dest_file_name: str,
        upload_request_payload,
        part_request_body_provider_fn,
        md5_fn,
        force_restart: bool,
        storage_str: str = None,
    ):
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

        # populated later
        self._upload_id: str = None
        self._pre_signed_part_urls: Mapping[int, str] = None

    @classmethod
    def _get_remaining_part_numbers(cls, upload_status):
        part_numbers = []
        parts_state = upload_status["partsState"]

        # parts are 1-based
        for i, part_status in enumerate(parts_state, 1):
            if part_status == "0":
                part_numbers.append(i)

        return len(parts_state), part_numbers

    def _is_copy(self):
        # is this a copy or upload request
        return (
            self._upload_request_payload.get("concreteType")
            == concrete_types.MULTIPART_UPLOAD_COPY_REQUEST
        )

    def _fetch_pre_signed_part_urls(
        self,
        upload_id: str,
        part_numbers: List[int],
    ) -> Mapping[int, str]:
        with tracer.start_as_current_span(
            "UploadAttemptAsync::_fetch_pre_signed_part_urls"
        ):
            response = post_file_multipart_presigned_urls(
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
    ):
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
        with tracer.start_as_current_span(
            "UploadAttemptAsync::_refresh_pre_signed_part_urls"
        ):
            with self._thread_lock:
                current_url = self._pre_signed_part_urls[part_number]
                if current_url != expired_url:
                    # if the url has already changed since the given url
                    # was detected as expired we can assume that another
                    # thread already refreshed the url and can avoid the extra
                    # fetch.
                    refreshed_url = current_url
                else:
                    self._pre_signed_part_urls = self._fetch_pre_signed_part_urls(
                        self._upload_id,
                        list(self._pre_signed_part_urls.keys()),
                    )

                    refreshed_url = self._pre_signed_part_urls[part_number]

            return refreshed_url

    def _handle_part(self, part_number, otel_context: Union[Context, None]):
        if otel_context:
            context.attach(otel_context)
        with tracer.start_as_current_span("UploadAttempt::_handle_part"):
            # part_url, signed_headers = self._pre_signed_part_urls.get(part_number)

            # TODO: Should the storage be non-async?
            session: httpx.Client = self._syn._requests_session_storage

            # with _executor(self._max_threads, False) as executor:

            # obtain the body (i.e. the upload bytes) for the given part number.
            body = (
                self._part_request_body_provider_fn(part_number)
                if self._part_request_body_provider_fn
                else None
            )
            part_size = len(body) if body else 0
            self._syn.logger.debug(f"Uploading part {part_number} of size {part_size}")
            if body is None:
                raise ValueError(f"No body for part {part_number}")
            with self._thread_lock:
                if self._aborted:
                    # this upload attempt has already been aborted
                    # so we short circuit the attempt to upload this part
                    raise SynapseUploadAbortedException(
                        "Upload aborted, skipping part {}".format(part_number)
                    )

                part_url, signed_headers = self._pre_signed_part_urls.get(part_number)

            for retry in range(2):
                try:
                    # use our backoff mechanism here, we have encountered 500s on puts to AWS signed urls
                    with tracer.start_as_current_span(
                        "UploadAttempt::put_on_storage_provider"
                    ):
                        trace.get_current_span().set_attributes({"url.path": part_url})
                        response = with_retry_non_async(
                            lambda: session.put(
                                url=part_url,
                                content=body,  # noqa: F821
                                headers=signed_headers,
                            ),
                            retry_exceptions=[requests.exceptions.ConnectionError],
                        )
                    try:
                        _raise_for_status(response)
                    except Exception as ex:
                        raise ex

                    # completed upload part to s3 successfully
                    break

                except SynapseHTTPError as ex:
                    if ex.response.status_code == 403 and retry < 1:
                        # we interpret this to mean our pre_signed url expired.
                        self._syn.logger.debug(
                            f"The pre-signed upload URL for part {part_number} has expired."
                            "Refreshing urls and retrying.\n"
                        )

                        # we refresh all the urls and obtain this part's
                        # specific url for the retry
                        with tracer.start_as_current_span(
                            "UploadAttempt::refresh_pre_signed_part_urls"
                        ):
                            (
                                part_url,
                                signed_headers,
                            ) = self._refresh_pre_signed_part_urls(
                                part_number,
                                part_url,
                            )

                    else:
                        raise

            md5_hex = self._md5_fn(body, response)
            del response
            del body

            # now tell synapse that we uploaded that part successfully
            put_file_multipart_add(
                upload_id=self._upload_id,
                part_number=part_number,
                md5_hex=md5_hex,
                endpoint=self._syn.fileHandleEndpoint,
                synapse_client=self._syn,
            )

            # # remove so future batch pre_signed url fetches will exclude this part
            with self._thread_lock:
                del self._pre_signed_part_urls[part_number]

            return part_number, part_size

    async def _upload_parts(self, part_count, remaining_part_numbers):
        with tracer.start_as_current_span("UploadAttempt::_upload_parts"):
            time_upload_started = time.time()
            completed_part_count = part_count - len(remaining_part_numbers)
            file_size = self._upload_request_payload.get("fileSizeBytes")

            # TODO: This likely needs to go into the executor
            self._pre_signed_part_urls = self._fetch_pre_signed_part_urls(
                self._upload_id,
                remaining_part_numbers,
            )

            futures = []

            with _executor(20, False) as executor:
                # we don't wait on the shutdown since we do so ourselves below

                for part_number in remaining_part_numbers:
                    futures.append(
                        executor.submit(
                            self._handle_part,
                            part_number,
                            context.get_current(),
                        )
                    )

            # for part_number in remaining_part_numbers:
            #     futures.append(
            #         asyncio.create_task(self._handle_part(part_number=part_number))
            #     )

            if not self._is_copy():
                # we won't have bytes to measure during a copy so the byte oriented progress bar is not useful
                progress = previously_transferred = min(
                    completed_part_count * self._part_size,
                    file_size,
                )

                self._syn._print_transfer_progress(
                    progress,
                    file_size,
                    prefix=self._storage_str if self._storage_str else "Uploading",
                    postfix=self._dest_file_name,
                    previouslyTransferred=previously_transferred,
                )

            for result in concurrent.futures.as_completed(futures):
                # for result in asyncio.as_completed(futures):
                try:
                    # _, part_size = await result
                    _, part_size = result.result()

                    if part_size and not self._is_copy():
                        progress += part_size
                        self._syn._print_transfer_progress(
                            min(progress, file_size),
                            file_size,
                            prefix=(
                                self._storage_str if self._storage_str else "Uploading"
                            ),
                            postfix=self._dest_file_name,
                            dt=time.time() - time_upload_started,
                            previouslyTransferred=previously_transferred,
                        )
                    del result
                    del _
                    del part_size
                except (Exception, KeyboardInterrupt) as cause:
                    # wait for all threads to complete before
                    # raising the exception, we don't want to return
                    # control while there are still threads from this
                    # upload attempt running
                    # await asyncio.wait(futures)
                    # for future in futures:
                    #     future.cancel()
                    # await asyncio.gather(*futures)
                    with self._lock:
                        self._aborted = True

                    # wait for all threads to complete before
                    # raising the exception, we don't want to return
                    # control while there are still threads from this
                    # upload attempt running
                    concurrent.futures.wait(futures)

                    if isinstance(cause, KeyboardInterrupt):
                        raise SynapseUploadAbortedException(
                            "User interrupted upload"
                        ) from cause
                    raise SynapseUploadFailedException("Part upload failed") from cause

    async def _complete_upload(self):
        with tracer.start_as_current_span("UploadAttempt::_complete_upload"):
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

    async def __call__(self):
        with tracer.start_as_current_span("UploadAttempt::__call__"):
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


async def multipart_upload_file_async(
    syn,
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
    with tracer.start_as_current_span("multipart_upload::multipart_upload_file"):
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

        callback_func = Spinner().print_tick if not syn.silent else None
        md5_hex = md5 or md5_for_file(file_path, callback=callback_func).hexdigest()

        part_size = _get_part_size(part_size, file_size)

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

        def part_fn(part_number):
            return _get_file_chunk(file_path, part_number, part_size)

        return await _multipart_upload_async(
            syn,
            dest_file_name,
            upload_request,
            part_fn,
            md5_fn,
            force_restart=force_restart,
            storage_str=storage_str,
        )


async def _multipart_upload_async(
    syn,
    dest_file_name,
    upload_request,
    part_fn,
    md5_fn,
    force_restart: bool = False,
    storage_str: str = None,
):
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
    with tracer.start_as_current_span("multipart_upload::_multipart_upload_async"):
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


def _get_file_chunk(file_path, part_number, chunk_size):
    """Read the nth chunk from the file."""
    with open(file_path, "rb") as f:
        f.seek((part_number - 1) * chunk_size)
        return f.read(chunk_size)


def _get_data_chunk(data, part_number, chunk_size):
    """Return the nth chunk of a buffer."""
    return data[((part_number - 1) * chunk_size) : part_number * chunk_size]


def _get_part_size(part_size, file_size):
    part_size = part_size or DEFAULT_PART_SIZE

    # can't exceed the maximum allowed num parts
    part_size = max(
        part_size, MIN_PART_SIZE, int(math.ceil(file_size / MAX_NUMBER_OF_PARTS))
    )
    return part_size


@tracer.start_as_current_span("multipart_upload::multipart_upload_string")
async def multipart_upload_string_async(
    syn,
    text: str,
    dest_file_name: str = None,
    part_size: int = None,
    content_type: str = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
):
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
    md5_hex = md5_fn(data, None)

    if not dest_file_name:
        dest_file_name = "message.txt"

    if not content_type:
        content_type = "text/plain; charset=utf-8"

    part_size = _get_part_size(part_size, file_size)

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

    def part_fn(part_number):
        return _get_data_chunk(data, part_number, part_size)

    part_size = _get_part_size(part_size, file_size)
    return await _multipart_upload_async(
        syn,
        dest_file_name,
        upload_request,
        part_fn,
        md5_fn,
        force_restart=force_restart,
    )


@tracer.start_as_current_span("multipart_upload::multipart_copy")
async def multipart_copy_async(
    syn,
    source_file_handle_association,
    dest_file_name: str = None,
    part_size: int = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
):
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

    def part_request_body_provider_fn(_) -> None:
        # for an upload copy there are no bytes
        return None

    def md5_fn(_, response) -> str:
        # for a multipart copy we use the md5 returned by the UploadPartCopy command
        # when we add the part to the Synapse upload

        # we extract the md5 from the <ETag> element in the response.
        # use lookahead and lookbehind to find the opening and closing ETag elements but
        # do not include those in the match, thus the entire matched string (group 0) will be
        # what was between those elements.
        md5_hex = re.search(
            "(?<=<ETag>).*?(?=<\\/ETag>)", (response.content.decode("utf-8"))
        ).group(0)

        # remove quotes found in the ETag to get at the normalized ETag
        return md5_hex.replace("&quot;", "").replace('"', "")

    return _multipart_upload_async(
        syn,
        dest_file_name,
        upload_request,
        part_request_body_provider_fn,
        md5_fn,
        force_restart=force_restart,
    )
