"""
************************
Synapse Multipart Upload
************************

Implements the client side of `Synapse multipart upload`_, which provides a robust means of uploading large files (into
the 10s of GB). End users should not need to call any of these functions directly.

.. _Synapse multipart upload:
 http://docs.synapse.org/rest/index.html#org.sagebionetworks.file.controller.UploadController

"""

import hashlib
import json
import math
import mimetypes
import os
import requests
import time
import warnings
import ctypes

from synapseclient.core import pool_provider, exceptions
from synapseclient.core.utils import printTransferProgress, md5_for_file, MB
from synapseclient.core.models.dict_object import DictObject
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.utils import threadsafe_generator

MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 8*MB
MAX_RETRIES = 7


def find_parts_to_upload(part_status):
    """
    Given a string of the form "1001110", where 1 and 0 indicate a status of completed or not, return the part numbers
    that aren't completed.
    """
    return [i+1 for i, c in enumerate(part_status) if c == '0']


def count_completed_parts(part_status):
    """
    Given a string of the form "1001110", where 1 and 0 indicate a status of completed or not, return the count of
    parts already completed.
    """
    return len([c for c in part_status if c == '1'])


def calculate_part_size(fileSize, partSize=None, min_part_size=MIN_PART_SIZE, max_parts=MAX_NUMBER_OF_PARTS):
    """
    Parts for multipart upload must be at least 5 MB and there must be at most 10,000 parts
    """
    if partSize is None:
        partSize = max(min_part_size, int(math.ceil(fileSize/float(max_parts))))
    if partSize < min_part_size:
        raise ValueError('Minimum part size is %d MB.' % (min_part_size/MB))
    if int(math.ceil(float(fileSize) / partSize)) > max_parts:
        raise ValueError('A part size of %0.1f MB results in too many parts (%d).'
                         % (float(partSize)/MB, int(math.ceil(fileSize / partSize))))
    return partSize


def get_file_chunk(filepath, n, chunksize=8*MB):
    """
    Read the nth chunk from the file.
    """
    with open(filepath, 'rb') as f:
        f.seek((n-1)*chunksize)
        return f.read(chunksize)


def get_data_chunk(data, n, chunksize=8*MB):
    """
    Return the nth chunk of a buffer.
    """
    return data[(n-1)*chunksize: n*chunksize]


def _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType, preview=True, storageLocationId=None,
                            forceRestart=False):
    """
    :returns: A `MultipartUploadStatus`_

    .. _MultipartUploadStatus:
     http://docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
    """
    upload_request = {
        'contentMD5Hex': md5,
        'fileName': filename,
        'generatePreview': preview,
        'contentType': contentType,
        'partSizeBytes': partSize,
        'fileSizeBytes': fileSize,
        'storageLocationId': storageLocationId
    }

    return DictObject(**syn.restPOST(uri='/file/multipart?forceRestart=%s' % forceRestart,
                                     body=json.dumps(upload_request),
                                     endpoint=syn.fileHandleEndpoint))


@threadsafe_generator
def _get_presigned_urls(syn, uploadId, parts_to_upload):
    """Returns list of urls to upload parts to.

    :param syn:             a Synapse object
    :param uploadId:        The id of the multipart upload
    :param parts_to_upload: A list of integers corresponding to the parts that need to be uploaded


    :returns: A BatchPresignedUploadUrlResponse_.
    .. BatchPresignedUploadUrlResponse:
     http://docs.synapse.org/rest/POST/file/multipart/uploadId/presigned/url/batch.html
    """
    if len(parts_to_upload) == 0:
        return 
    presigned_url_request = {'uploadId': uploadId}
    uri = '/file/multipart/{uploadId}/presigned/url/batch'.format(uploadId=uploadId)

    presigned_url_request['partNumbers'] = parts_to_upload
    presigned_url_batch = syn.restPOST(uri, body=json.dumps(presigned_url_request),
                                       endpoint=syn.fileHandleEndpoint)
    for part in presigned_url_batch['partPresignedUrls']:
        yield part


def _add_part(syn, uploadId, partNumber, partMD5Hex):
    """
    :returns: An AddPartResponse_ with fields for an errorMessage and addPartState containing either 'ADD_SUCCESS' or
              'ADD_FAILED'.

    .. AddPartResponse: http://docs.synapse.org/rest/org/sagebionetworks/repo/model/file/AddPartResponse.html
    """
    uri = '/file/multipart/{uploadId}/add/{partNumber}?partMD5Hex={partMD5Hex}'.format(**locals())
    return DictObject(**syn.restPUT(uri, endpoint=syn.fileHandleEndpoint))


def _complete_multipart_upload(syn, uploadId):
    """
    :returns: A MultipartUploadStatus_.

    .. MultipartUploadStatus:
     http://docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
    """
    uri = '/file/multipart/{uploadId}/complete'.format(uploadId=uploadId)
    return DictObject(**syn.restPUT(uri, endpoint=syn.fileHandleEndpoint))


def _put_chunk(url, chunk, verbose=False):
    response = requests.put(url, data=chunk)
    try:
        # Make sure requests closes response stream?:
        # see: http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
        if response is not None:
            response.content
    except Exception as ex:
        warnings.warn('error reading response: '+str(ex))
    exceptions._raise_for_status(response, verbose=verbose)


def multipart_upload_file(syn, filepath, filename=None, contentType=None, storageLocationId=None, **kwargs):
    """
    Upload a file to a Synapse upload destination in chunks.

    :param syn:                 a Synapse object
    :param filepath:            the file to upload
    :param filename:            upload as a different filename
    :param contentType:         `contentType`_
    :param partSize:            number of bytes per part. Minimum 5MB.
    :param storageLocationId:   a id indicating where the file should be stored.
                                Retrieved from Synapse's UploadDestination

    :return: a File Handle ID

    Keyword arguments are passed down to :py:func:`_multipart_upload` and :py:func:`_start_multipart_upload`.

    .. _contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """
    if not os.path.exists(filepath):
        raise IOError('File "%s" not found.' % filepath)
    if os.path.isdir(filepath):
        raise IOError('File "%s" is a directory.' % filepath)

    fileSize = os.path.getsize(filepath)
    if not filename:
        filename = os.path.basename(filepath)
    md5 = md5_for_file(filepath).hexdigest()

    if contentType is None:
        (mimetype, enc) = mimetypes.guess_type(filepath, strict=False)
        if not mimetype:
            mimetype = "application/octet-stream"
        contentType = mimetype
    syn.logger.debug("Initiating multi-part upload for file: [{path}] size={size} md5={md5}, contentType={contentType}"
                     .format(path=filepath, size=fileSize, md5=md5, contentType=contentType))

    def get_chunk_function(n, partSize): return get_file_chunk(filepath, n, partSize)

    status = _multipart_upload(syn, filename, contentType,
                               get_chunk_function=get_chunk_function,
                               md5=md5,
                               fileSize=fileSize,
                               storageLocationId=storageLocationId,
                               **kwargs)
    syn.logger.debug("Completed multi-part upload. Result:%s" % status)
    return status["resultFileHandleId"]


def multipart_upload_string(syn, text, filename=None, contentType=None, storageLocationId=None, **kwargs):
    """
    Upload a string using the multipart file upload.

    :param syn:                 a Synapse object
    :param text:                a string to upload as a file.
    :param filename:            a string containing the base filename
    :param contentType:         `contentType`_
    :param partSize:            number of bytes per part. Minimum 5MB.
    :param storageLocationId:   a id indicating where the text should be stored.
                                Retrieved from Synapse's UploadDestination

    :return: a File Handle ID

    Keyword arguments are passed down to :py:func:`_multipart_upload` and :py:func:`_start_multipart_upload`.

    .. _contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """

    data = text.encode('utf-8')
    fileSize = len(data)
    md5 = hashlib.md5(data).hexdigest()

    if not filename:
        filename = 'message.txt'

    if not contentType:
        contentType = "text/plain; charset=utf-8"

    def get_chunk_function(n, partSize): return get_data_chunk(data, n, partSize)

    status = _multipart_upload(syn, filename, contentType,
                               get_chunk_function=get_chunk_function,
                               md5=md5,
                               fileSize=fileSize,
                               storageLocationId=storageLocationId,
                               **kwargs)

    return status["resultFileHandleId"]


def _upload_chunk(part, completed, status, syn, filename, get_chunk_function,
                  fileSize, partSize, t0, expired, bytes_already_uploaded=0):
    partNumber = part["partNumber"]
    url = part["uploadPresignedUrl"]

    syn.logger.debug("uploading this part of the upload: %s" % part)
    # if the upload url for another worker has expired, assume that this one also expired and return early
    with expired.get_lock():
        if expired.value:
            syn.logger.debug("part %s is returning early because other parts have already expired" % partNumber)
            return

    try:
        chunk = get_chunk_function(partNumber, partSize)
        syn.logger.debug("start upload part %s" % partNumber)
        _put_chunk(url, chunk, syn.debug)
        syn.logger.debug("PUT upload of part %s complete" % partNumber)
        # compute the MD5 for the chunk
        md5 = hashlib.md5()
        md5.update(chunk)

        # confirm that part got uploaded
        syn.logger.debug("contacting Synapse to complete part %s" % partNumber)
        add_part_response = _add_part(syn, uploadId=status.uploadId,
                                      partNumber=partNumber, partMD5Hex=md5.hexdigest())
        # if part was successfully uploaded, increment progress
        if add_part_response["addPartState"] == "ADD_SUCCESS":
            syn.logger.debug("finished contacting Synapse about adding part %s" % partNumber)
            with completed.get_lock():
                completed.value += len(chunk)
            printTransferProgress(completed.value, fileSize, prefix='Uploading', postfix=filename, dt=time.time()-t0,
                                  previouslyTransferred=bytes_already_uploaded)
        else:
            syn.logger.debug("did not successfully add part %s" % partNumber)
    except Exception as ex1:
        if isinstance(ex1, SynapseHTTPError) and ex1.response.status_code == 403:
            syn.logger.debug("The pre-signed upload URL for part %s has expired. Restarting upload...\n" % partNumber)
            with expired.get_lock():
                if not expired.value:
                    warnings.warn("The pre-signed upload URL has expired. Restarting upload...\n")
                    expired.value = True
            return
        # If we are not in verbose debug mode we will swallow the error and retry.
        else:
            syn.logger.debug("Encountered an exception: %s. Retrying...\n" % str(type(ex1)), exc_info=True)


def _multipart_upload(syn, filename, contentType, get_chunk_function, md5, fileSize, 
                      partSize=None, storageLocationId=None, **kwargs):
    """
    Multipart Upload.

    :param syn:                 a Synapse object
    :param filename:            a string containing the base filename
    :param contentType:         contentType_
    :param get_chunk_function:  a function that takes a part number and size and returns the bytes of that chunk of the
                                file
    :param md5:                 the part's MD5 as hex.
    :param fileSize:            total number of bytes
    :param partSize:            number of bytes per part. Minimum 5MB.
    :param storageLocationId:   a id indicating where the file should be stored. retrieved from Synapse's
                                UploadDestination

    :return: a MultipartUploadStatus_ object

    Keyword arguments are passed down to :py:func:`_start_multipart_upload`.

    .. MultipartUploadStatus:
     http://docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
    .. contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """
    partSize = calculate_part_size(fileSize, partSize, MIN_PART_SIZE, MAX_NUMBER_OF_PARTS)
    status = _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType,
                                     storageLocationId=storageLocationId, **kwargs)

    # only force restart once
    kwargs['forceRestart'] = False

    completedParts = count_completed_parts(status.partsState)
    # bytes that were previously uploaded before the current upload began. This variable is set only once
    previously_completed_bytes = min(completedParts * partSize, fileSize)
    syn.logger.debug("file partitioned into size: %s" % partSize)
    syn.logger.debug("current multipart-upload status: %s" % status)
    syn.logger.debug("previously completed %d parts, estimated %d bytes" % (completedParts, previously_completed_bytes))
    time_upload_started = time.time()
    retries = 0
    mp = pool_provider.get_pool()
    try:
        while retries < MAX_RETRIES:
            syn.logger.debug("Started retry loop for multipart_upload. Currently %d/%d retries"
                             % (retries, MAX_RETRIES))
            # keep track of the number of bytes uploaded so far
            completed = pool_provider.get_value('d', min(completedParts * partSize, fileSize))
            expired = pool_provider.get_value(ctypes.c_bool, False)

            printTransferProgress(completed.value, fileSize, prefix='Uploading', postfix=filename)

            def chunk_upload(part): return _upload_chunk(part, completed=completed, status=status,
                                                         syn=syn, filename=filename,
                                                         get_chunk_function=get_chunk_function,
                                                         fileSize=fileSize, partSize=partSize, t0=time_upload_started,
                                                         expired=expired,
                                                         bytes_already_uploaded=previously_completed_bytes)

            syn.logger.debug("fetching pre-signed urls and mapping to Pool")
            url_generator = _get_presigned_urls(syn, status.uploadId, find_parts_to_upload(status.partsState))
            mp.map(chunk_upload, url_generator)
            syn.logger.debug("completed pooled upload")

            # Check if there are still parts
            status = _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType,
                                             storageLocationId=storageLocationId, **kwargs)
            oldCompletedParts, completedParts = completedParts, count_completed_parts(status.partsState)
            progress = (completedParts > oldCompletedParts)
            retries = retries+1 if not progress else retries
            syn.logger.debug("progress made in this loop? %s" % progress)

            # Are we done, yet?
            if completed.value >= fileSize:
                try:
                    syn.logger.debug("attempting to finalize multipart upload because completed.value >= filesize"
                                     " ({completed} >= {size})".format(completed=completed.value, size=fileSize))
                    status = _complete_multipart_upload(syn, status.uploadId)
                    if status.state == "COMPLETED":
                        break
                except Exception as ex1:
                    syn.logger.error("Attempt to complete the multipart upload failed with exception %s %s"
                                     % (type(ex1), ex1))
                    syn.logger.debug("multipart upload failed:", exc_info=True)
    finally:
        mp.terminate()
    if status["state"] != "COMPLETED":
        raise SynapseError("Upload {id} did not complete. Try again.".format(id=status["uploadId"]))

    return status
