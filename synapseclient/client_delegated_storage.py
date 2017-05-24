from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from six import add_metaclass

from abc import ABCMeta, abstractmethod
from synapseclient.utils import itersubclasses
from synapseclient import exceptions
import boto
import os

@add_metaclass(ABCMeta)
class ClientDelegatedStorageConnection():

    @abstractmethod
    def downloadFile(self, remotePath, downloadFilePath):
        pass

    @abstractmethod
    def uploadFile(self, remotePath, uploadFilePath):
        pass

    @classmethod
    def createConnection(self, storageLocationSetting):
        class_constructor = _storageLocationSetting_type_to_Connection.get(storageLocationSetting['concreteType'], None)
        if class_constructor is None:
            raise NotImplementedError("The client delegated connection for concreteType [%s] is not currently supported." % storageLocationSetting['concreteType'])
        return class_constructor(storageLocationSetting)



class ClientS3Connection(ClientDelegatedStorageConnection):
    #TODO: replace with actual concreteType
    _synapse_storageLocation_type = "org.sagebionetworks.placeholder.idk""

    #TODO: perhaps use a @meomoize __new__() instead of init so we dont create the same connection multiple times

    def __init__(self,storageLocation, access_key_id = None, secret_access_key = None):
        self._bucket = storageLocation['bucket']
        self._endpoint = storageLocation['endpointUrl'] # TODO: probably don't need to store this

        #Note: boto will automatically look inside the ~/.aws folder for AWS credentials if aws_access_key_id and aws_secret_access_key are None
        conn = boto.connect_s3(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, host=self.endpoint,
                        calling_format=boto.s3.connection.OrdinaryCallingFormat())

        #TODO: should bucket be created if not exist?
        self._boto_bucket = conn.get_bucket(storageLocation['bucket'])

    def downloadFile(self, remotePath, downloadFilePath):
        # boto does not check for file existence and will overwrite the file if it already exists
        if os.path.exists(downloadFilePath):
            raise ValueError("The file: [%s] already exists", downloadFilePath)

        key = self._boto_bucket.get_key(remotePath)
        if not key:
            raise ValueError("The given remote key [%s] does not exist" % remotePath)

        key.get_contents_to_filename(downloadFilePath)
        return downloadFilePath

    def uploadFile(self, remotePath, uploadFilePath):
        #TODO: use s3put for parallel multipart upload
        return uploadFilePath



_storageLocationSetting_type_to_Connection = {}
for cls in itersubclasses(ClientDelegatedStorageConnection):
    _storageLocationSetting_type_to_Connection[cls._synapse_storageLocation_type] = cls