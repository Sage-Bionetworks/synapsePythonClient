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
import hashlib
import json
import math
import mimetypes
import os
import requests
import threading
import time
from typing import List, Mapping

from synapseclient.core import pool_provider
from synapseclient.core.exceptions import (
    _raise_for_status,  # why is is this a single underscore
    SynapseHTTPError,
    SynapseUploadAbortedException,
    SynapseUploadFailedException,
)
from synapseclient.core.utils import printTransferProgress, md5_for_file, MB

# AWS limits
MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 5 * MB

# ancient tribal knowledge
DEFAULT_PART_SIZE = 8 * MB
MAX_RETRIES = 7


thread_local = threading.local()


class UploadAttempt:

    def __init__(
        self,
        syn,
        chunk_fn,
        dest_file_name: str,
        file_size: int,
        part_size: int,
        md5_hex: str,
        content_type: str,
        preview: bool,
        storage_location_id: str,
        max_threads: int,
        force_restart: bool,
    ):
        self._syn = syn
        self._chunk_fn = chunk_fn
        self._dest_file_name = dest_file_name
        self._file_size = file_size
        self._part_size = part_size
        self._md5_hex = md5_hex
        self._content_type = content_type
        self._preview = preview
        self._storage_location_id = storage_location_id
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
        session = getattr(thread_local, 'session', None)
        if not session:
            session = thread_local.session = requests.Session()
        return session

    def _create_synapse_upload(self):
        upload_request = {
            'contentMD5Hex': self._md5_hex,
            'fileName': self._dest_file_name,
            'generatePreview': self._preview,
            'contentType': self._content_type,
            'partSizeBytes': self._part_size,
            'fileSizeBytes': self._file_size,
            'storageLocationId': self._storage_location_id,
        }

        return self._syn.restPOST(
            "/file/multipart?forceRestart={}".format(
                str(self._force_restart).lower()
            ),
            json.dumps(upload_request),
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
            part_urls[part['partNumber']] = part['uploadPresignedUrl']

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

            pre_signed_part_url = self._pre_signed_part_urls.get(part_number)

        session = self._get_thread_session()
        chunk = self._chunk_fn(part_number, self._part_size)
        part_size = len(chunk)

        md5 = hashlib.md5()
        md5.update(chunk)
        md5_hex = md5.hexdigest()

        for retry in range(2):
            try:
                response = session.put(
                    pre_signed_part_url,
                    chunk,
                )
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
                    pre_signed_part_url = self._refresh_pre_signed_part_urls(
                        part_number,
                        pre_signed_part_url,
                    )

                else:
                    raise

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

        # note this is an estimate, may not be exact since the final part
        # may be smaller and might be included in the completed parts.
        # it's good enough though.
        progress = previously_transferred = min(
            completed_part_count * self._part_size,
            self._file_size
        )
        printTransferProgress(
            progress,
            self._file_size,
            prefix='Uploading',
            postfix=self._dest_file_name,
            previouslyTransferred=previously_transferred,
        )

        self._pre_signed_part_urls = self._fetch_pre_signed_part_urls(
            self._upload_id,
            remaining_part_numbers,
        )

        futures = []
        executor = pool_provider.get_executor(thread_count=self._max_threads)
        for part_number in remaining_part_numbers:
            futures.append(
                executor.submit(
                    self._handle_part,
                    part_number,
                )
            )
        executor.shutdown(wait=False)

        for result in concurrent.futures.as_completed(futures):
            try:
                _, part_size = result.result()
                progress += part_size
                printTransferProgress(
                    min(progress, self._file_size),
                    self._file_size,
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
    md5_hex = md5_for_file(file_path).hexdigest()

    if content_type is None:
        mime_type, _ = mimetypes.guess_type(file_path, strict=False)
        content_type = mime_type or 'application/octet-stream'

    return _multipart_upload(
        syn,
        lambda n, c: _get_file_chunk(file_path, n, c),
        file_size,
        part_size,
        dest_file_name,
        md5_hex,
        content_type,
        storage_location_id,
        preview,
        force_restart,
        max_threads,
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

    return _multipart_upload(
        syn,
        lambda n, c: _get_data_chunk(data, n, c),
        file_size,
        part_size,
        dest_file_name,
        md5_hex,
        content_type,
        storage_location_id,
        preview,
        force_restart,
        max_threads,
    )


def _multipart_upload(
    syn,
    chunk_fn,
    file_size: int,
    part_size: int,
    dest_file_name: str,
    md5_hex: str,
    content_type,
    storage_location_id: str = None,
    preview: bool = True,
    force_restart: bool = False,
    max_threads: int = None,
):

    part_size = part_size or DEFAULT_PART_SIZE
    part_size = max(
        part_size,
        MIN_PART_SIZE,
        int(math.ceil(file_size / MAX_NUMBER_OF_PARTS))
    )

    if max_threads is None:
        # default to a number of threads based on the cpu count,
        # but no point in exceeding the number of parts
        part_count = math.ceil(file_size / part_size)
        max_threads = min(
            part_count,
            pool_provider.DEFAULT_NUM_THREADS,
        )
    else:
        max_threads = max(max_threads, 1)

    retry = 0
    while True:
        try:
            upload_status_response = UploadAttempt(
                syn,
                chunk_fn,
                dest_file_name,
                file_size,
                part_size,
                md5_hex,
                content_type,
                preview,
                storage_location_id,
                max_threads,

                # only force_restart the first time through (if requested).
                # a retry after a caught exception will not restart the upload
                # from scratch.
                force_restart=force_restart and retry == 0,
            )()

            # success
            return upload_status_response['resultFileHandleId']

        except SynapseUploadFailedException:
            if retry < MAX_RETRIES:
                retry += 1
            else:
                raise
