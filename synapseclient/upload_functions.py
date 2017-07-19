from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from six import add_metaclass

from abc import ABCMeta, abstractmethod
import os
import mimetypes
from .utils import is_url, md5_for_file
from . import concrete_types
import sys
from .remote_file_connection import ClientS3Connection
from .multipart_upload import  multipart_upload
try:
    from urllib.parse import urlparse
    from urllib.parse import urlunparse
    from urllib.parse import quote
    from urllib.parse import unquote
    from urllib.request import urlretrieve
except ImportError:
    from urlparse import urlparse
    from urlparse import urlunparse
    from urllib import quote
    from urllib import unquote
    from urllib import urlretrieve


# TODO: replace uploadExternallyStoringProjects make this a factory elsewhere
def upload_file(syn, entity_parent_id, local_state):
    if '_file_handle' not in local_state:
        local_state['_file_handle'] = {}

    local_state_file_handle = local_state['_file_handle']

    #TODO: what to do with localstate?? just use entity instead idk

    # if doing a external file handle with no actual upload
    if not local_state['synapseStore']:
        if local_state_file_handle.get('externalURL', None):
            return create_external_file_handle(syn, local_state_file_handle['externalUrl'], local_state['contentType'], )
        elif is_url(local_state['path']):
            local_state_file_handle['externalURL'] = local_state['path']
            # If the url is a local path compute the md5
            url = urlparse(local_state['path'])
            if os.path.isfile(url.path) and url.scheme == 'file':
                local_state_file_handle['contentMd5'] = md5_for_file(url.path).hexdigest()
            return create_external_file_handle()

    location = syn._getDefaultUploadDestination(entity_parent_id)
    upload_destination_type = location['concreteType']
    # synapse managed S3
    if upload_destination_type == concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION or \
                    upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION:
        storageString = 'Synapse' if upload_destination_type == concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION else 'your external S3'
        sys.stdout.write('\n' + '#' * 50 + '\n Uploading file to ' + storageString + ' storage \n' + '#' * 50 + '\n')

        return upload_synapse_s3(syn, local_state['path'], location['storageLocationId'])
    #external file handle (sftp)
    elif upload_destination_type == concrete_types.EXTERNAL_UPLOAD_DESTINATION:
        if location['uploadType'] == 'SFTP':
            sys.stdout.write('\n%s\n%s\nUploading to: %s\n%s\n' % ('#' * 50,
                                                                   location.get('banner', ''),
                                                                   urlparse(location['url']).netloc,
                                                                   '#' * 50))
            return upload_external_file_handle_sftp(syn, local_state['path'], location['url'])
        else:
            raise NotImplementedError('Can only handle SFTP upload locations.')
    #client authenticated S3
    elif upload_destination_type == concrete_types.EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION:
        return upload_client_auth_s3(syn, local_state['path'], location['bucket'], location['endpointUrl'], location['keyPrefixUUID'], location['storageLocationId'])

def create_external_file_handle(syn, file_path_or_url, mimetype, md5, file_size):
    #does nothing on purpose because there is nothing to upload
    return syn._create_ExternalFileHandle(file_path_or_url, mimetype=mimetype, md5=md5, fileSize=file_size)


def upload_external_file_handle_sftp(syn, file_path, sftp_url, md5, file_size):
    uploaded_url = syn._sftpUploadFile(file_path, unquote(sftp_url))

    return syn._create_ExternalFileHandle(uploaded_url, md5=md5_for_file(file_path).hexdigest(), fileSize=os.stat(file_path).st_size)

def upload_synapse_s3(syn, file_path, storageLocationId, mimetype=None):

    file_handle_id = multipart_upload(syn, file_path, contentType=mimetype, storageLocationId=storageLocationId)
    syn.cache.add(file_handle_id, file_path)
    return syn._getFileHandle(file_handle_id)

def upload_client_auth_s3(syn, file_path, bucket, endpoint_url, key_prefix, storage_location_id):
    file_key = key_prefix + '/' + os.path.basename(file_path)
    ClientS3Connection.uploadFile(bucket, endpoint_url, file_key, file_path)

    #TODO: move into helper function?
    mimetype, enc = mimetypes.guess_type(file_path, strict=False)
    file_handle = {'concreteType': 'org.sagebionetworks.repo.model.file.ExternalObjectStoreFileHandle',
                  'fileName': os.path.basename(file_path),
                  'contentMd5': md5_for_file(file_path).hexdigest(),
                  'contentSize': os.stat(file_path).st_size,
                  'storageLocationId': storage_location_id,
                  'contentType': mimetype}

    file_handle = syn._POST_ExternalFileHandleInterface(file_handle)
    syn.cache.add(file_handle['id'], file_path)
    return file_handle
