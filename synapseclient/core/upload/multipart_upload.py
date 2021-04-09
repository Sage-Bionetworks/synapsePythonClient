"""
************************
Synapse Multipart Upload
************************

Implements the client side of `Synapse multipart upload`_, which provides a
robust means of uploading large files (into the 10s of GB). End users should
not need to call any of these functions directly.

.. _Synapse multipart upload:
 http://docs.synapse.org/rest/index.html#org.sagebionetworks.file.controller.UploadController

"""

import concurrent.futures
from contextlib import contextmanager
import hashlib
import json
import math
import mimetypes
import os
import re
import requests
import threading
import time
from typing import List, Mapping

from synapseclient.core.retry import with_retry
from synapseclient.core import pool_provider
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import (
    _raise_for_status,  # why is is this a single underscore
    SynapseHTTPError,
    SynapseUploadAbortedException,
    SynapseUploadFailedException,
)
from synapseclient.core.utils import md5_for_file, MB, Spinner

# AWS limits
MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 5 * MB

# ancient tribal knowledge
DEFAULT_PART_SIZE = 8 * MB
MAX_RETRIES = 7


_thread_local = threading.local()


@contextmanager
def shared_executor(executor):
    """An outside process that will eventually trigger an upload through the this module
    can configure a shared Executor by running its code within this context manager."""
    _thread_local.executor = executor
    try:
        yield
    finally:
        del _thread_local.executor


@contextmanager
def _executor(max_threads, shutdown_wait):
    """Yields an executor for running some asynchronous code, either obtaining the executor
    from the shared_executor or otherwise creating one.

    :param max_threads: the maxmimum number of threads a created executor should use
    :param shutdown_wait: whether a created executor should shutdown after running the yielded to code
    """
    executor = getattr(_thread_local, 'executor', None)
    shutdown_after = False
    if not executor:
        shutdown_after = True
        executor = pool_provider.get_executor(thread_count=max_threads)

    try:
        yield executor
    finally:
        if shutdown_after:
            executor.shutdown(wait=shutdown_wait)


class UploadAttempt:

    def __init__(
        self,
        syn,
        dest_file_name,

        upload_request_payload,
        part_request_body_provider_fn,
        md5_fn,

        max_threads: int,
        force_restart: bool,
    ):
        self._syn = syn
        self._dest_file_name = dest_file_name
        self._part_size = upload_request_payload['partSizeBytes']

        self._upload_request_payload = upload_request_payload

        self._part_request_body_provider_fn = part_request_body_provider_fn
        self._md5_fn = md5_fn

        self._max_threads = max_threads
        self._force_restart = force_restart

        self._lock = threading.Lock()
        self._aborted = False

        # populated later
        self._upload_id: str = None
        self._pre_signed_part_urls: Mapping[int, str] = None

    @classmethod
    def _get_remaining_part_numbers(cls, upload_status):
        part_numbers = []
        parts_state = upload_status['partsState']

        # parts are 1-based
        for i, part_status in enumerate(parts_state, 1):
            if part_status == '0':
                part_numbers.append(i)

        return len(parts_state), part_numbers

    @classmethod
    def _get_thread_session(cls):
        # get a lazily initialized requests.Session from the thread.
        # we want to share a requests.Session over the course of a thread
        # to take advantage of persistent http connection. we put it on a
        # thread local rather that in the task closure since a connection can
        # be reused across separate part uploads so no reason to restrict it
        # per worker task.
        session = getattr(_thread_local, 'session', None)
        if not session:
            session = _thread_local.session = requests.Session()
        return session

    def _is_copy(self):
        # is this a copy or upload request
        return self._upload_request_payload.get('concreteType') == concrete_types.MULTIPART_UPLOAD_COPY_REQUEST

    def _create_synapse_upload(self):
        return self._syn.restPOST(
            "/file/multipart?forceRestart={}".format(
                str(self._force_restart).lower()
            ),
            json.dumps(self._upload_request_payload),
            endpoint=self._syn.fileHandleEndpoint,
        )

    def _fetch_pre_signed_part_urls(
        self,
        upload_id: str,
        part_numbers: List[int],
        requests_session: requests.Session = None,
    ) -> Mapping[int, str]:

        uri = "/file/multipart/{upload_id}/presigned/url/batch".format(
            upload_id=upload_id
        )
        body = {
            'uploadId': upload_id,
            'partNumbers': part_numbers,
        }

        response = self._syn.restPOST(
            uri,
            json.dumps(body),
            requests_session=requests_session,
            endpoint=self._syn.fileHandleEndpoint,
        )

        part_urls = {}
        for part in response['partPresignedUrls']:
            part_urls[part['partNumber']] = (
                part['uploadPresignedUrl'],
                part.get('signedHeaders', {}),
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

        :param part_number: the part number whose refreshed url should
            be returned
        :param expired_url: the url that was detected as expired triggering
            this refresh

        """
        with self._lock:
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

    def _handle_part(self, part_number):
        with self._lock:
            if self._aborted:
                # this upload attempt has already been aborted
                # so we short circuit the attempt to upload this part
                raise SynapseUploadAbortedException(
                    "Upload aborted, skipping part {}".format(part_number)
                )

            part_url, signed_headers = self._pre_signed_part_urls.get(part_number)

        session = self._get_thread_session()

        # obtain the body (i.e. the upload bytes) for the given part number.
        body = self._part_request_body_provider_fn(part_number) if self._part_request_body_provider_fn else None
        part_size = len(body) if body else 0
        for retry in range(2):
            def put_fn():
                return session.put(part_url, body, headers=signed_headers)
            try:
                # use our backoff mechanism here, we have encountered 500s on puts to AWS signed urls
                response = with_retry(put_fn, retry_exceptions=[requests.exceptions.ConnectionError])
                _raise_for_status(response)

                # completed upload part to s3 successfully
                break

            except SynapseHTTPError as ex:
                if ex.response.status_code == 403 and retry < 1:
                    # we interpret this to mean our pre_signed url expired.
                    self._syn.logger.debug(
                        "The pre-signed upload URL for part {} has expired."
                        "Refreshing urls and retrying.\n".format(part_number)
                    )

                    # we refresh all the urls and obtain this part's
                    # specific url for the retry
                    part_url, signed_headers = self._refresh_pre_signed_part_urls(
                        part_number,
                        part_url,
                    )

                else:
                    raise

        md5_hex = self._md5_fn(body, response)

        # now tell synapse that we uploaded that part successfully
        self._syn.restPUT(
            "/file/multipart/{upload_id}/add/{part_number}?partMD5Hex={md5}"
            .format(
                upload_id=self._upload_id,
                part_number=part_number,
                md5=md5_hex,
            ),
            requests_session=session,
            endpoint=self._syn.fileHandleEndpoint
        )

        # remove so future batch pre_signed url fetches will exclude this part
        with self._lock:
            del self._pre_signed_part_urls[part_number]

        return part_number, part_size

    def _upload_parts(self, part_count, remaining_part_numbers):
        time_upload_started = time.time()
        completed_part_count = part_count - len(remaining_part_numbers)
        file_size = self._upload_request_payload.get('fileSizeBytes')

        if not self._is_copy():
            # we won't have bytes to measure during a copy so the byte oriented progress bar is not useful
            progress = previously_transferred = min(
                completed_part_count * self._part_size,
                file_size,
            )

            self._syn._print_transfer_progress(
                progress,
                file_size,
                prefix='Uploading',
                postfix=self._dest_file_name,
                previouslyTransferred=previously_transferred,
            )

        self._pre_signed_part_urls = self._fetch_pre_signed_part_urls(
            self._upload_id,
            remaining_part_numbers,
        )

        futures = []
        with _executor(self._max_threads, False) as executor:
            # we don't wait on the shutdown since we do so ourselves below

            for part_number in remaining_part_numbers:
                futures.append(
                    executor.submit(
                        self._handle_part,
                        part_number,
                    )
                )

        for result in concurrent.futures.as_completed(futures):
            try:
                _, part_size = result.result()

                if part_size and not self._is_copy():
                    progress += part_size
                    self._syn._print_transfer_progress(
                        min(progress, file_size),
                        file_size,
                        prefix='Uploading',
                        postfix=self._dest_file_name,
                        dt=time.time() - time_upload_started,
                        previouslyTransferred=previously_transferred,
                    )
            except (Exception, KeyboardInterrupt) as cause:
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
                    )

                raise SynapseUploadFailedException(
                    "Part upload failed"
                ) from cause

    def _complete_upload(self):
        upload_status_response = self._syn.restPUT(
            "/file/multipart/{upload_id}/complete".format(
                upload_id=self._upload_id,
            ),
            requests_session=self._get_thread_session(),
            endpoint=self._syn.fileHandleEndpoint,
        )

        upload_state = upload_status_response.get('state')
        if upload_state != 'COMPLETED':
            # at this point we think successfully uploaded all the parts
            # but the upload status isn't complete, we'll throw an error
            # and let a subsequent attempt try to reconcile
            raise SynapseUploadFailedException(
                "Upload status has an unexpected state {}".format(upload_state)
            )

        return upload_status_response

    def __call__(self):
        upload_status_response = self._create_synapse_upload()
        upload_state = upload_status_response.get('state')

        if upload_state != 'COMPLETED':
            self._upload_id = upload_status_response['uploadId']
            part_count, remaining_part_numbers =\
                self._get_remaining_part_numbers(
                    upload_status_response
                )

            # if no remaining part numbers then all the parts have been
            # uploaded but the upload has not been marked complete.
            if remaining_part_numbers:
                self._upload_parts(part_count, remaining_part_numbers)
            upload_status_response = self._complete_upload()

        return upload_status_response


def _get_file_chunk(file_path, part_number, chunk_size):
    """
    Read the nth chunk from the file.
    """
    with open(file_path, 'rb') as f:
        f.seek((part_number - 1) * chunk_size)
        return f.read(chunk_size)


def _get_data_chunk(data, part_number, chunk_size):
    """
    Return the nth chunk of a buffer.
    """
    return data[(part_number - 1) * chunk_size: part_number * chunk_size]


def _get_part_size(part_size, file_size):
    part_size = part_size or DEFAULT_PART_SIZE

    # can't exceed the maximum allowed num parts
    part_size = max(
        part_size,
        MIN_PART_SIZE,
        int(math.ceil(file_size / MAX_NUMBER_OF_PARTS))
    )
    return part_size


def multipart_upload_file(
    syn,
    file_path: str,
    dest_file_name: str = None,
    content_type: str = None,
    part_size: int = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
    max_threads: int = None,
) -> str:
    """
    Upload a file to a Synapse upload destination in chunks.

    :param syn:                 a Synapse object
    :param file_path:           the file to upload
    :param dest_file_name:      upload as a different filename
    :param content_type:        contentType`_
    :param part_size:           number of bytes per part. Minimum 5MB.
    :param storage_location_id: an id indicating where the file should be
                                stored. Retrieved from Synapse's
                                UploadDestination
    :param preview:             True to generate a preview
    :param force_restart        True to restart a previously initiated upload
                                from scratch, False to try to resume
    :param max_threads          number of concurrent threads to devote
                                to upload

    :return: a File Handle ID

    Keyword arguments are passed down to
    :py:func:`_multipart_upload` and :py:func:`_start_multipart_upload`.

    .. _contentType:
     https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """

    if not os.path.exists(file_path):
        raise IOError('File "{}" not found.'.format(file_path))
    if os.path.isdir(file_path):

        raise IOError('File "{}" is a directory.'.format(file_path))

    file_size = os.path.getsize(file_path)
    if not dest_file_name:
        dest_file_name = os.path.basename(file_path)

    if content_type is None:
        mime_type, _ = mimetypes.guess_type(file_path, strict=False)
        content_type = mime_type or 'application/octet-stream'

    callback_func = Spinner().print_tick if not syn.silent else None
    md5_hex = md5_for_file(file_path, callback=callback_func).hexdigest()

    part_size = _get_part_size(part_size, file_size)

    upload_request = {
        'concreteType': concrete_types.MULTIPART_UPLOAD_REQUEST,
        'contentType': content_type,
        'contentMD5Hex': md5_hex,
        'fileName': dest_file_name,
        'fileSizeBytes': file_size,
        'generatePreview': preview,
        'partSizeBytes': part_size,
        'storageLocationId': storage_location_id,
    }

    def part_fn(part_number):
        return _get_file_chunk(file_path, part_number, part_size)

    def md5_fn(part, _):
        md5 = hashlib.md5()
        md5.update(part)
        return md5.hexdigest()

    return _multipart_upload(
        syn,
        dest_file_name,

        upload_request,
        part_fn,
        md5_fn,

        force_restart=force_restart,
        max_threads=max_threads,
    )


def multipart_upload_string(
    syn,
    text: str,
    dest_file_name: str = None,
    part_size: int = None,
    content_type: str = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
    max_threads: int = None,
):
    """
    Upload a file to a Synapse upload destination in chunks.

    :param syn:                 a Synapse object
    :param text:                a string to upload as a file.
    :param dest_file_name:      upload as a different filename
    :param content_type:        contentType`_
    :param part_size:           number of bytes per part. Minimum 5MB.
    :param storage_location_id: an id indicating where the file should be
                                stored. Retrieved from Synapse's
                                UploadDestination
    :param preview:             True to generate a preview
    :param force_restart        True to restart a previously initiated upload
                                from scratch, False to try to resume
    :param max_threads          number of concurrent threads to devote
                                to upload

    :return: a File Handle ID

    Keyword arguments are passed down to
    :py:func:`_multipart_upload` and :py:func:`_start_multipart_upload`.

    .. _contentType:
     https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """

    data = text.encode('utf-8')
    file_size = len(data)
    md5_hex = hashlib.md5(data).hexdigest()

    if not dest_file_name:
        dest_file_name = 'message.txt'

    if not content_type:
        content_type = "text/plain; charset=utf-8"

    part_size = _get_part_size(part_size, file_size)

    upload_request = {
        'concreteType': concrete_types.MULTIPART_UPLOAD_REQUEST,
        'contentType': content_type,
        'contentMD5Hex': md5_hex,
        'fileName': dest_file_name,
        'fileSizeBytes': file_size,
        'generatePreview': preview,
        'partSizeBytes': part_size,
        'storageLocationId': storage_location_id,
    }

    def part_fn(part_number):
        return _get_data_chunk(data, part_number, part_size)

    def md5_fn(part, _):
        md5 = hashlib.md5()
        md5.update(part)
        return md5.hexdigest()

    part_size = _get_part_size(part_size, file_size)
    return _multipart_upload(
        syn,
        dest_file_name,

        upload_request,
        part_fn,
        md5_fn,

        force_restart=force_restart,
        max_threads=max_threads,
    )


def multipart_copy(
    syn,
    source_file_handle_association,
    dest_file_name: str = None,
    part_size: int = None,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
    max_threads: int = None,
):

    part_size = part_size or DEFAULT_PART_SIZE

    upload_request = {
        'concreteType': concrete_types.MULTIPART_UPLOAD_COPY_REQUEST,
        'fileName': dest_file_name,
        'generatePreview': preview,
        'partSizeBytes': part_size,
        'sourceFileHandleAssociation': source_file_handle_association,
        'storageLocationId': storage_location_id,
    }

    def part_request_body_provider_fn(part_num):
        # for an upload copy there are no bytes
        return None

    def md5_fn(_, response):
        # for a multipart copy we use the md5 returned by the UploadPartCopy command
        # when we add the part to the Synapse upload

        # we extract the md5 from the <ETag> element in the response.
        # use lookahead and lookbehind to find the opening and closing ETag elements but
        # do not include those in the match, thus the entire matched string (group 0) will be
        # what was between those elements.
        md5_hex = re.search('(?<=<ETag>).*?(?=<\\/ETag>)', (response.content.decode('utf-8'))).group(0)

        # remove quotes found in the ETag to get at the normalized ETag
        return md5_hex.replace('&quot;', '').replace('"', '')

    return _multipart_upload(
        syn,
        dest_file_name,

        upload_request,
        part_request_body_provider_fn,
        md5_fn,

        force_restart=force_restart,
        max_threads=max_threads,
    )


def _multipart_upload(
    syn,
    dest_file_name,

    upload_request,
    part_fn,
    md5_fn,

    force_restart: bool = False,
    max_threads: int = None,
):

    if max_threads is None:
        max_threads = pool_provider.DEFAULT_NUM_THREADS

    max_threads = max(max_threads, 1)

    retry = 0
    while True:
        try:
            upload_status_response = UploadAttempt(
                syn,
                dest_file_name,

                upload_request,
                part_fn,
                md5_fn,

                max_threads,

                # only force_restart the first time through (if requested).
                # a retry after a caught exception will not restart the upload
                # from scratch.
                force_restart and retry == 0,
            )()

            # success
            return upload_status_response['resultFileHandleId']

        except SynapseUploadFailedException:
            if retry < MAX_RETRIES:
                retry += 1
            else:
                raise
