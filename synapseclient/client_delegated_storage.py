from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from six import add_metaclass
from builtins import str

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

    def __init__(self,storageLocation, access_key_id = None, secret_access_key = None):
        self._bucket = storageLocation['bucket']
        self._endpoint = storageLocation['endpointUrl'] # TODO: probably don't need to store this

        # TODO: grab config file auth
        key_id = access_key_id
        secret_key = secret_access_key

        conn = boto.connect_s3(aws_access_key_id=key_id, aws_secret_access_key=secret_key, host=self.endpoint,
                        calling_format=boto.s3.connection.OrdinaryCallingFormat())

        #TODO: should bucket be created if not exist?
        self._boto_bucket = conn.get_bucket(storageLocation['bucket'])

    def downloadFile(self, remotePath, downloadFilePath):
        #TODO: do filepath existance check if boto does not do this already
        self._boto_bucket.get_contents_to_filename(remotePath, downloadFilePath)
        #TODO: error checking?
        return downloadFilePath

    def uploadFile(self, remotePath, uploadFilePath):
        #TODO: use s3put for parallel multipart upload
        return uploadFilePath



_storageLocationSetting_type_to_Connection = {}
for cls in itersubclasses(ClientDelegatedStorageConnection):
    _storageLocationSetting_type_to_Connection[cls._synapse_storageLocation_type] = cls