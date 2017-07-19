from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import botocore
from six import add_metaclass

from abc import ABCMeta, abstractmethod
import os
import boto3

#TODO: RENAME
@add_metaclass(ABCMeta)
class RemoteFileConnection():

    @abstractmethod
    def downloadFile(self, remotePath, downloadFilePath):
        pass

    @abstractmethod
    def uploadFile(self, remotePath, uploadFilePath):
        pass

    # @classmethod
    # def createConnection(self, storageLocationSetting):
    #     class_constructor = _storageLocationSetting_type_to_Connection.get(storageLocationSetting['concreteType'], None)
    #     if class_constructor is None:
    #         raise NotImplementedError("The client delegated connection for concreteType [%s] is not currently supported." % storageLocationSetting['concreteType'])
    #     return class_constructor(storageLocationSetting)


#TODO: RENAME
class ClientS3Connection(RemoteFileConnection):

    #TODO: perhaps use a @meomoize __new__() instead of init so we dont create the same connection multiple times

    #TODO: Implement the basic lower level download/upload first but then look at hwo to integrate into python client and change object model accordingly

    @staticmethod
    def downloadFile(bucket, endpoint_url, remote_file_key, download_file_path, access_key_id = None, secret_access_key = None):
        # boto does not check for file existence and will overwrite the file if it already exists
        if os.path.exists(download_file_path):
            raise ValueError("The file: [%s] already exists", download_file_path)

        s3 = boto3.resource('s3', endpoint_url=endpoint_url, aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_access_key)
        try:
            s3.Bucket(bucket).download_file(remote_file_key, download_file_path)
            return download_file_path
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise ValueError("The object does not exist.")
            else:
                raise

    @staticmethod
    def uploadFile(bucket, endpoint_url, remote_file_key, upload_file_path, access_key_id = None, secret_access_key = None):
        if not os.path.isfile(upload_file_path):
            raise ValueError("The path: [%s] does not exist or is not a file", upload_file_path)

        s3 = boto3.resource('s3', endpoint_url=endpoint_url, aws_access_key_id=access_key_id,
                            aws_secret_access_key=secret_access_key)
        s3.Bucket(bucket).upload_file(upload_file_path, remote_file_key) #automatically determines whether to perform multi-part upload
        return upload_file_path

