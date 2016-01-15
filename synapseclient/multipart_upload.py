"""
************************
Synapse Multipart Upload
************************

see: http://rest.synapse.org/index.html#org.sagebionetworks.file.controller.UploadController

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
from functools import partial
import synapseclient.exceptions as exceptions
from .utils import printTransferProgress, md5_for_file, MB, GB
from .dict_object import DictObject
from .exceptions import SynapseError

MAX_NUMBER_OF_PARTS = 10000
MIN_PART_SIZE = 5*MB



def find_parts_to_download(part_status):
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


def partition(n, seq):
    """
    Split the input list into partitions of size n.
    """
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def calculate_part_size(fileSize, max_parts):
    """
    Parts for mutipart upload must be at least 5 MB and there must
    be at most 10,000 parts
    """
    return max(5*MB, math.ceil(fileSize/float(max_parts)))


def get_file_chunk(filepath, n, chunksize=5*MB):
    """
    Read the nth chunk from the file.
    """
    with open(filepath, 'rb') as f:
        f.seek((n-1)*chunksize)
        return f.read(chunksize)


def get_data_chunk(data, n, chunksize=5*MB):
    """
    Return the nth chunk of a buffer.
    """
    return data[ (n-1)*chunksize : n*chunksize ]


def _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType, preview=True, storageLocationId=None, forceRestart=False):
    """
    :returns: A MultipartUploadStatus_

    .. MultipartUploadStatus: http://rest.synapse.org/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
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


def _get_presigned_urls(syn, uploadId, partNumbers):
    """
    :returns: A BatchPresignedUploadUrlResponse_.

    .. BatchPresignedUploadUrlResponse: http://rest.synapse.org/POST/file/multipart/uploadId/presigned/url/batch.html
    """
    presigned_url_request = {
        'uploadId':uploadId,
        'partNumbers':partNumbers
    }

    uri = '/file/multipart/{uploadId}/presigned/url/batch'.format(uploadId=uploadId)
    return DictObject(**syn.restPOST(uri,
                                     body=json.dumps(presigned_url_request),
                                     endpoint=syn.fileHandleEndpoint))


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
    # Make sure requests closes response stream?:
    # see: http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
    try:
        if response is not None:
            throw_away = response.content
    except Exception as ex:
        warnings.warn('error reading response: '+str(ex))
    exceptions._raise_for_status(response, verbose=verbose)


def multipart_upload(syn, filepath, filename=None, partSize=None, contentType=None, retries=7, url_batch_size=6):
    """
    Upload a file to a Synapse upload destination in chunks.

    :param syn: a Synapse object
    :param filepath: the file to upload
    :param filename: upload as a different filename
    :param contentType: contentType_
    :param partSize: number of bytes per part. Minimum 5MB.
    :param retries: number of times to retry upload
    :param url_batch_size: number of signed URLs to request at once

    :return: a File Handle ID

    .. contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
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
                               partSize=partSize,
                               retries=retries,
                               url_batch_size=url_batch_size)

    return status["resultFileHandleId"]


def multipart_upload_string(syn, text, filename=None, partSize=None, contentType=None, retries=7, url_batch_size=6):
    """
    Upload a string using the multipart file upload.

    :param syn: a Synapse object
    :param text: a string to upload as a file.
    :param filename: a string containing the base filename
    :param contentType: contentType_
    :param partSize: number of bytes per part. Minimum 5MB.
    :param retries: number of times to retry upload
    :param url_batch_size: number of signed URLs to request at once

    :return: a File Handle ID

    .. contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
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
                               partSize=partSize,
                               retries=retries,
                               url_batch_size=url_batch_size)

    return status["resultFileHandleId"]


def _upload_chunk(partNumber, url, completed, status, syn, filename, get_chunk_function, fileSize, partSize):
    try:
        chunk = get_chunk_function(partNumber, partSize)
        _put_chunk(url, chunk, syn.debug)

        ## compute the MD5 for the chunk
        md5 = hashlib.md5()
        md5.update(chunk)

        ## confirm that part got uploaded
        add_part_response = _add_part(syn,
                                      uploadId=status.uploadId,
                                      partNumber=partNumber,
                                      partMD5Hex=md5.hexdigest())

        ## if part was successfully uploaded, increment progress
        if add_part_response["addPartState"] == "ADD_SUCCESS":
            with completed.get_lock():
                completed.value += len(chunk)
            printTransferProgress(completed.value, fileSize, prefix='Uploading', postfix=filename)
    except IOError as ex1:
        print(ex1)
        print("Encountered an exception: %s. Retrying..." % type(ex1))


def _multipart_upload(syn, filename, contentType, get_chunk_function, md5, fileSize, partSize, retries=7, url_batch_size=6):
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
    :param retries: number of times to retry upload
    :param url_batch_size: number of signed URLs to request at once

    :return: a MultipartUploadStatus_ object

    .. MultipartUploadStatus: http://rest.synapse.org/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html
    .. contentType: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    """
    from multiprocessing import Value

    if partSize is None:
        partSize = calculate_part_size(fileSize, MAX_NUMBER_OF_PARTS)
    if partSize < MIN_PART_SIZE:
        raise ValueError('Minimum part size is %d MB.' % (MIN_PART_SIZE/MB))

    ## make upload_chunk a function of 4 parameters:
    ##    partNumber, url, completed, status
    upload_chunk = partial(_upload_chunk, syn=syn,
                                          filename=filename,
                                          get_chunk_function=get_chunk_function,
                                          fileSize=fileSize,
                                          partSize=partSize)

    for i in range(retries):
        status = _start_multipart_upload(syn, filename, md5, fileSize, partSize, contentType)

        completed = Value('d', min(count_completed_parts(status.partsState) * partSize, fileSize))
        printTransferProgress(completed.value, fileSize, prefix='Uploading', postfix=filename)

        ## for each batch of parts to upload, get presigned URLs and attempt upload
        for parts_to_upload in partition(url_batch_size, find_parts_to_download(status.partsState)):
            presigned_url_batch = _get_presigned_urls(syn, status.uploadId, parts_to_upload)
            for part in presigned_url_batch.partPresignedUrls:
                upload_chunk(partNumber=part["partNumber"],
                             url=part["uploadPresignedUrl"],
                             completed=completed,
                             status=status)

        ## Are we done, yet?
        if completed.value >= fileSize:
            status = _complete_multipart_upload(syn, status.uploadId)
            if status.state == "COMPLETED":
                break

    if status["state"] != "COMPLETED":
        raise SynapseError("Upoad {id} did not complete. Try again.".format(id=status["uploadId"]))

    return status

