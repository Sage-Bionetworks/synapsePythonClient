from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import botocore
from six import add_metaclass

from abc import ABCMeta, abstractmethod
import os
import boto3
import time
from .utils import printTransferProgress

#TODO:
#TODO: acept aws profile names
class ClientS3Connection:

    @staticmethod
    def download_file(bucket, endpoint_url, remote_file_key, download_file_path, profile_name = None, show_progress=True):
        # boto does not check for file existence and will overwrite the file if it already exists
        if os.path.exists(download_file_path):
            raise ValueError("The file: [%s] already exists", download_file_path)

        boto_session = boto3.session.Session(profile_name=profile_name)
        s3 = boto_session.resource('s3', endpoint_url=endpoint_url)

        try:
            s3_obj = s3.Object(bucket, remote_file_key)

            progress_callback = None
            if show_progress:
                s3_obj.load()
                file_size = s3_obj.content_length
                # TODO: does the lambda only resolve the time.time() once or multiple times????
                t0 = time.time()
                filename = os.path.basename(download_file_path)
                print(filename)
                progress_callback = lambda bytes_downloaded: printTransferProgress(bytes_downloaded, file_size,
                                                                                   prefix='Downloading', postfix=filename,
                                                                                   dt=time.time() - t0,
                                                                                   previouslyTransferred=0)
            s3_obj.download_file(download_file_path, Callback=progress_callback)
            return download_file_path
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise ValueError("The object does not exist.")
            else:
                raise

    @staticmethod
    def upload_file(bucket, endpoint_url, remote_file_key, upload_file_path, profile_name = None, show_progress=True):
        if not os.path.isfile(upload_file_path):
            raise ValueError("The path: [%s] does not exist or is not a file", upload_file_path)

        boto_session = boto3.session.Session(profile_name=profile_name)
        s3 = boto_session.resource('s3', endpoint_url=endpoint_url)

        progress_callback = None
        if show_progress:
            file_size = os.stat(upload_file_path).st_size
            filename = os.path.basename(upload_file_path)
            t0 = time.time()
            #TODO: does the lambda only resolve the time.time() once or multiple times????
            progress_callback = lambda bytes_uploaded:  printTransferProgress(bytes_uploaded, file_size, prefix='Uploading', postfix=filename, dt=time.time() - t0, previouslyTransferred=0)

        s3.Bucket(bucket).upload_file(upload_file_path, remote_file_key, Callback=progress_callback) #automatically determines whether to perform multi-part upload
        return upload_file_path
