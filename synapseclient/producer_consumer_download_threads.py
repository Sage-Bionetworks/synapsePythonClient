from queue import Queue
from threading import Thread
import typing
import time
from typing import Generator
from re import split
from datetime import datetime
from math import ceil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from synapseclient.core.utils import printTransferProgress
import json
import os
import requests

# constants
BUF_SIZE = 20
MAX_RETRIES = 5
MB = 2**20
SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE = 8 * MB
SYNAPSE_DEFAULT_FILE_ENDPOINT = "https://repo-prod.prod.sagebase.org/file/v1"
SYNAPSE_URL_PATH_DOWNLOAD_GET_BATCH_PRE_SIGNED_URL = '/fileHandle/batch'
ISO_AWS_STR_FORMAT = '%Y%m%dT%H%M%SZ'
PARTIAL_CONTENT_CODE = 206
CONNECT_FACTOR = 3
BACK_OFF_FACTOR = 0.5

# transfer progress parameters
transferred = 0
t_first_write = 0
to_be_transferred = 0
previously_transferred = 0


class DownloadRequest:
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

    def __init__(self, file_handle_id: int, object_id: str, object_type: str, path: str):
        """

        :param file_handle_id:
        :param object_id:
        :param object_type:
        :param path:
        """

        self.file_handle_id = file_handle_id
        self.object_id = object_id
        self.object_type = object_type
        self.path = path


class ProducerDownloadThread(Thread):
    """
    The producer threads that make the GET request and obtain the data for a download chunk
    """
    def __init__(self, client, request, range_queue, data_queue):
        Thread.__init__(self)
        self.setDaemon(True)
        self.client = client
        self.request = request
        self.range_queue = range_queue
        self.data_queue = data_queue
        self.session = requests.Session()
        retry = Retry(connect=MAX_RETRIES, backoff_factor=BACK_OFF_FACTOR)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def run(self):
        while True:
            # Get the work from the chunk generator
            start, end, file_name, url, path = self.range_queue.get()
            try:
                # download chunk
                # specify the start and end of this chunk
                headers = {'Range': 'bytes=%d-%d' % (start, end)}

                if not _url_is_valid(url):
                    _, pre_signed_url_new = _get_pre_signed_batch_request_json(self.client, self.request)
                    url = pre_signed_url_new
                # request the chunk and capture the response
                response = self.session.get(url, headers=headers, stream=True)
                # try request until successful or out of retries
                try_counter = 0
                while response.status_code != PARTIAL_CONTENT_CODE and try_counter < MAX_RETRIES:
                    try_counter += 1
                    if not _url_is_valid(url):
                        _, pre_signed_url_new = _get_pre_signed_batch_request_json(self.client, self.request)
                        url = pre_signed_url_new
                    response = self.session.get(url, headers=headers, stream=True)

                if response.status_code == PARTIAL_CONTENT_CODE:
                    self.data_queue.put((start, file_name, path, response.content))
            finally:
                self.range_queue.task_done()


class ConsumerDownloadThread(Thread):
    """
    The worker threads that write download chunks to file
    """
    def __init__(self, data_queue):
        Thread.__init__(self)
        self.setDaemon(True)
        self.data_queue = data_queue

    def run(self):
        while True:
            # Get the chunk data from the data queue
            start, file_name, path, data = self.data_queue.get()
            try:
                # write data to file
                with open(path, "r+b") as file_write:
                    global transferred
                    global t_first_write
                    global previously_transferred
                    if transferred == 0:
                        t_first_write = time.time()
                    file_write.seek(start)
                    file_write.write(data)

                    transferred = len(data) + previously_transferred
                    previously_transferred += len(data)
                    printTransferProgress(transferred, to_be_transferred, 'Downloading ',
                                          os.path.basename(path), dt=time.time()-t_first_write)
            finally:
                self.data_queue.task_done()


def download_files(client, download_requests: typing.Sequence[DownloadRequest], num_threads):
    """
    Main driver for the multi-threaded download. Uses the producer-consumer with Queue design pattern as described
    in Effective Python Item 39.

    :param client: A SynapseBaseClient
    :param download_requests: A batch of DownloadRequest objects specifying what Synapse files to download
    :param num_threads: The number of download threads
    :return: Map between each DownloadRequest in download_requests object and the corresponding DownloadResponse object
    """

    session = requests.Session()
    retry = Retry(connect=CONNECT_FACTOR, backoff_factor=BACK_OFF_FACTOR)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    data_queue = Queue(BUF_SIZE)
    range_queue = Queue(BUF_SIZE)
    producer_threads = []
    consumer_threads = []

    for request in download_requests:
        producer_threads_local = []
        consumer_threads_local = []
        for _ in range(num_threads):
            producer_worker = ProducerDownloadThread(client, request, range_queue, data_queue)
            consumer_worker = ConsumerDownloadThread(data_queue)
            producer_threads_local.append(producer_worker)
            consumer_threads_local.append(consumer_worker)

        for producer_thread, consumer_thread in zip(producer_threads_local, consumer_threads_local):
            producer_thread.start()
            consumer_thread.start()

        for producer_thread, consumer_thread in zip(producer_threads_local, consumer_threads_local):
            producer_threads.append(producer_thread)
            consumer_threads.append(consumer_thread)

        global transferred
        global t_first_write
        global previously_transferred
        transferred = t_first_write = previously_transferred = 0

        file_name, pre_signed_url = _get_pre_signed_batch_request_json(client, request)
        res_get = session.get(pre_signed_url, stream=True)
        file_size = int(res_get.headers['Content-Length'])
        pre_signed_url_chunk_generator = _get_chunk_pre_signed_url(file_size,
                                                                   file_name,
                                                                   pre_signed_url,
                                                                   request.path)
        global to_be_transferred
        to_be_transferred = file_size
        _create_empty_file(file_size, request.path)
        for chunk in pre_signed_url_chunk_generator:
            range_queue.put(chunk)
    range_queue.join()
    data_queue.join()


def _get_pre_signed_batch_request_json(client, request: DownloadRequest) -> tuple:
    """
    Returns the file_name and pre-signed url for download as specified in request

    :param client: A SynapseBaseClient
    :param request: An individual entry in the form of a DownloadRequest
    :return: A tuple containing the file_name and pre-signed url
    """
    pre_signed_url_request = {
                                "requestedFiles": [
                                    {
                                        "fileHandleId": request.file_handle_id,
                                        "associateObjectId": request.object_id,
                                        "associateObjectType": request.object_type
                                    }
                                ],
                                "includePreSignedURLs": True,
                                "includeFileHandles": True,
                                "includePreviewPreSignedURLs": False
                            }
    pre_signed_url_batch = client.restPOST(uri=SYNAPSE_URL_PATH_DOWNLOAD_GET_BATCH_PRE_SIGNED_URL,
                                           body=json.dumps(pre_signed_url_request),
                                           endpoint=SYNAPSE_DEFAULT_FILE_ENDPOINT)
    file_name = pre_signed_url_batch["requestedFiles"][0]["fileHandle"]["fileName"]
    pre_signed_url = pre_signed_url_batch["requestedFiles"][0]["preSignedURL"]
    return file_name, pre_signed_url


def _get_chunk_pre_signed_url(file_size: int,
                              file_name: str,
                              url: str,
                              path: str
                              ) -> Generator:
    """
    Creates a generator which yields byte ranges and meta data required to make a range request download of url and
    write the data to file_name located at path. Download chunk sizes are 8MB by default.

    :param file_size: Tbe size of the file
    :param file_name: The name of the file
    :param url: The pre-signed url to download the file from
    :param path: The local path describing where to download the file
    :return: A generator of byte ranges and meta data needed to download the file in a multi-threaded manner
    """
    num_chunks = ceil(file_size / SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE)
    for i in range(num_chunks):
        start = SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE * i
        end = start + SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE
        yield start, end, file_name, url, path


def _url_is_valid(url: str) -> bool:
    """
    Checks if url is expired

    :param url: A pre-signed download url from AWS
    :return: True if url is not expired (i.e. valid), False otherwise
    """
    parsed_url_list = split('[&|=]', url)
    time_made = parsed_url_list[parsed_url_list.index('X-Amz-Date') + 1]
    time_made_datetime = datetime.strptime(time_made, ISO_AWS_STR_FORMAT)
    expires = parsed_url_list[parsed_url_list.index('X-Amz-Expires') + 1]
    time_delta_seconds = (datetime.utcnow() - time_made_datetime).total_seconds()
    return time_delta_seconds < int(expires)


def _create_empty_file(file_size, path) -> None:
    """
    Creates an empty file named file_name at location path and of size file_size

    :param file_size: The size of the file (in Bytes)
    :param path: The local path describing where to download the file
    :return: None
    """
    with open(path, "wb") as file:
        file.seek(file_size - 1)
        file.write(b'\0')
