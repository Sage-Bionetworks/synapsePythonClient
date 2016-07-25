"""
************************
Synapse Multipart Upload
************************

Implements the client side of `Synapse multipart upload`_, which provides
a robust means of uploading large files (into the 10s of GB). End users
should not need to call any of these functions directly.

.. _Synapse multipart upload: http://rest.synapse.org/index.html#org.sagebionetworks.file.controller.UploadController

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import hashlib
import json
import math
import mimetypes
import os
import requests
import sys
import time
import warnings
from functools import partial
from multiprocessing import Value
from multiprocessing.dummy import Pool

try:
    from urllib.parse import urlparse
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import urlparse
    from urlparse import parse_qs

import synapseclient.exceptions as exceptions
from .utils import printTransferProgress, md5_for_file, MB, GB
from .dict_object import DictObject
from .exceptions import SynapseError
from .utils import threadsafe_generator

MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 8*MB
MAX_RETRIES  = 7


def find_parts_to_upload(part_status):
    """
    Given a string of the form "1001110", where 1 and 0 indicate a status of
    completed or not, return the part numbers that aren't completed.
    """
    return [i+1 for i,c in enumerate(part_status) if c=='0']


def count_completed_parts(part_status):
    """
    Given a string of the form "1001110", where 1 and 0 indicate a status of
    completed or not, return the count of parts already completed.
    """
    return len([c for c in part_status if c=='1'])


def calculate_part_size(fileSize, partSize=None, min_part_size=MIN_PART_SIZE, max_parts=MAX_NUMBER_OF_PARTS):
    """
    Parts for mutipart upload must be at least 5 MB and there must
    be at most 10,000 parts
    """
    if partSize is None:
        partSize = max(min_part_size, int(math.ceil(fileSize/float(max_parts))))
    if partSize < min_part_size:
        raise ValueError('Minimum part size is %d MB.' % (min_part_size/MB))
    if int(math.ceil(float(fileSize) / partSize)) > max_parts:
        raise ValueError('A part size of %0.1f MB results in too many parts (%d).' % (float(partSize)/MB, int(math.ceil(fileSize / partSize))))
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
    return data[ (n-1)*chunksize : n*chunksize ]


def _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType, preview=True, storageLocationId=None, forceRestart=False):
    """
    :returns: A `MultipartUploadStatus`_

    .. _MultipartUploadStatus: http://rest.synapse.org/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
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

    :param syn: a Synapse object
    :param uploadId: The id of the multipart upload
    :param parts_to_upload: A list of integers corresponding to the parts that need to be uploaded


    :returns: A BatchPresignedUploadUrlResponse_.
    .. BatchPresignedUploadUrlResponse: http://rest.synapse.org/POST/file/multipart/uploadId/presigned/url/batch.html
    """
    if len(parts_to_upload)==0:
        return 
    presigned_url_request = { 'uploadId':uploadId }
    uri = '/file/multipart/{uploadId}/presigned/url/batch'.format(uploadId=uploadId)

    presigned_url_request['partNumbers'] = parts_to_upload
    presigned_url_batch = syn.restPOST(uri, body=json.dumps(presigned_url_request),
                                       endpoint=syn.fileHandleEndpoint)
    for part in presigned_url_batch['partPresignedUrls']:
        yield part


def _add_part(syn, uploadId, partNumber, partMD5Hex):
    """
    :returns: An AddPartResponse_ with fields for an errorMessage and addPartState containing
              either 'ADD_SUCCESS' or 'ADD_FAILED'.

    .. AddPartResponse: http://rest.synapse.org/org/sagebionetworks/repo/model/file/AddPartResponse.html
    """
    uri = '/file/multipart/{uploadId}/add/{partNumber}?partMD5Hex={partMD5Hex}'.format(**locals())
    return DictObject(**syn.restPUT(uri, endpoint=syn.fileHandleEndpoint))


def _complete_multipart_upload(syn, uploadId):
    """
    :returns: A MultipartUploadStatus_.

    .. MultipartUploadStatus: http://rest.synapse.org/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
    """
    uri = '/file/multipart/{uploadId}/complete'.format(uploadId=uploadId)
    return DictObject(**syn.restPUT(uri, endpoint=syn.fileHandleEndpoint))


def _put_chunk(url, chunk, verbose=False):
    response = requests.put(url, data=chunk)
    try:
        # Make sure requests closes response stream?:
        # see: http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
        if response is not None:
            throw_away = response.content
    except Exception as ex:
        warnings.warn('error reading response: '+str(ex))
    exceptions._raise_for_status(response, verbose=verbose)


def multipart_upload(syn, filepath, filename=None, contentType=None, **kwargs):
    """
    Upload a file to a Synapse upload destination in chunks.

    :param syn: a Synapse object
    :param filepath: the file to upload
    :param filename: upload as a different filename
    :param contentType: `contentType`_
    :param partSize: number of bytes per part. Minimum 5MB.

    :return: a File Handle ID

    Keyword arguments are passed down to :py:func:`_multipart_upload` and
    :py:func:`_start_multipart_upload`.

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

    get_chunk_function = lambda n,partSize: get_file_chunk(filepath, n, partSize)

    status = _multipart_upload(syn, filename, contentType,
                               get_chunk_function=get_chunk_function,
                               md5=md5,
                               fileSize=fileSize,
                               **kwargs)

    return status["resultFileHandleId"]


def multipart_upload_string(syn, text, filename=None, contentType=None, **kwargs):
    """
    Upload a string using the multipart file upload.

    :param syn: a Synapse object
    :param text: a string to upload as a file.
    :param filename: a string containing the base filename
    :param contentType: `contentType`_
    :param partSize: number of bytes per part. Minimum 5MB.

    :return: a File Handle ID

    Keyword arguments are passed down to :py:func:`_multipart_upload` and
    :py:func:`_start_multipart_upload`.

    .. _contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """

    data = text.encode('utf-8')
    fileSize = len(data)
    md5 = hashlib.md5(data).hexdigest()

    if not filename:
        filename = 'message.txt'

    if not contentType:
        contentType = "text/plain; charset=utf-8"

    get_chunk_function = lambda n,partSize: get_data_chunk(data, n, partSize)

    status = _multipart_upload(syn, filename, contentType,
                               get_chunk_function=get_chunk_function,
                               md5=md5,
                               fileSize=fileSize,
                               **kwargs)

    return status["resultFileHandleId"]


def _upload_chunk(part, completed, status, syn, filename, get_chunk_function,
                  fileSize, partSize, t0):
    partNumber=part["partNumber"]
    url=part["uploadPresignedUrl"]
    parsed = urlparse(url)
    expires = float(parse_qs(parsed.query)['Expires'][0])
    if expires<time.time(): return
    try:
        chunk = get_chunk_function(partNumber, partSize)
        _put_chunk(url, chunk, syn.debug)
        ## compute the MD5 for the chunk
        md5 = hashlib.md5()
        md5.update(chunk)

        ## confirm that part got uploaded
        add_part_response = _add_part(syn, uploadId=status.uploadId,
                                      partNumber=partNumber, partMD5Hex=md5.hexdigest())
        ## if part was successfully uploaded, increment progress
        if add_part_response["addPartState"] == "ADD_SUCCESS":
            with completed.get_lock():
                completed.value += len(chunk)
            printTransferProgress(completed.value, fileSize, prefix='Uploading', postfix=filename, dt=time.time()-t0)
    except Exception as ex1:
        #If we are not in verbose debug mode we will swallow the error and retry.
        if syn.debug:
            sys.stderr.write(str(ex1))
            sys.stderr.write("Encountered an exception: %s. Retrying...\n" % str(type(ex1)))


def _multipart_upload(syn, filename, contentType, get_chunk_function, md5, fileSize, 
                      partSize=None, **kwargs):
    """
    Multipart Upload.

    :param syn: a Synapse object
    :param filename: a string containing the base filename
    :param contentType: contentType_
    :param get_chunk_function: a function that takes a part number and size
                               and returns the bytes of that chunk of the file
    :param md5: the part's MD5 as hex.
    :param fileSize: total number of bytes
    :param partSize: number of bytes per part. Minimum 5MB.

    :return: a MultipartUploadStatus_ object

    Keyword arguments are passed down to :py:func:`_start_multipart_upload`.

    .. MultipartUploadStatus: http://rest.synapse.org/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
    .. contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """
    partSize = calculate_part_size(fileSize, partSize, MIN_PART_SIZE, MAX_NUMBER_OF_PARTS)
    status = _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType, **kwargs)

    ## only force restart once
    kwargs['forceRestart'] = False

    completedParts = count_completed_parts(status.partsState)
    progress=True
    retries=0
    mp = Pool(8)
    try:
        while retries<MAX_RETRIES:
            ## keep track of the number of bytes uploaded so far
            completed = Value('d', min(completedParts * partSize, fileSize))
            printTransferProgress(completed.value, fileSize, prefix='Uploading', postfix=filename)
            chunk_upload = lambda part: _upload_chunk(part, completed=completed, status=status, 
                                                      syn=syn, filename=filename,
                                                      get_chunk_function=get_chunk_function,
                                                      fileSize=fileSize, partSize=partSize, t0=time.time())

            url_generator = _get_presigned_urls(syn, status.uploadId, find_parts_to_upload(status.partsState))
            mp.map(chunk_upload, url_generator)

            #Check if there are still parts
            status = _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType, **kwargs)
            oldCompletedParts, completedParts = completedParts, count_completed_parts(status.partsState)
            progress = (completedParts>oldCompletedParts)
            retries = retries+1 if not progress else retries

            ## Are we done, yet?
            if completed.value >= fileSize:
                try:
                    status = _complete_multipart_upload(syn, status.uploadId)
                    if status.state == "COMPLETED":
                        break
                except Exception as ex1:
                    sys.stderr.write(str(ex1)+"\n")
    finally:
        mp.terminate()
    if status["state"] != "COMPLETED":
        raise SynapseError("Upoad {id} did not complete. Try again.".format(id=status["uploadId"]))

    return status

