from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
from .utils import is_url, md5_for_file
from . import concrete_types
import sys
from .remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper
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

#TODO: documentation
def upload_file(syn, entity_parent_id, local_state):
    """Uploads the file set in the local_state['path'] (if necessary) to a storage location based on project settings. 
    Returns a new FileHandle as a dict to represent the stored file. The local_state's '_file_handle' dict is not modified at all.

    :param entity_parent_id: parent id of the entity to which we upload.
    :param local_state: local state of the entity

    :returns: a dict of a new FileHandle as a dict that represents the uploaded file 
    """
    if '_file_handle' not in local_state:
        local_state['_file_handle'] = {}

    local_state_file_handle = local_state['_file_handle']

    # if doing a external file handle with no actual upload
    if not local_state['synapseStore']:
        externalUrl = local_state_file_handle.get('externalURL', None)
        path = local_state.get('path', None)

        if externalUrl is None:
            if path is not None:
                externalUrl = os.path.expandvars(os.path.expanduser(path))
            else: # path is None
                raise ValueError("Both 'externalUrl' and 'path' values are none. When synapseStore=False. Please set either one of the values ('externalURL' will be preferred over 'path' if both are set) to continue uploading a ExternalFileHandle")

        md5 = None
        file_size = None
        if is_url(externalUrl):
            url = urlparse(externalUrl)
            if url.scheme == 'file' and os.path.isfile(url.path):
                md5 = md5_for_file(url.path).hexdigest()
                file_size = os.stat(url.path).st_size

        return create_external_file_handle(syn, externalUrl, mimetype=local_state_file_handle.get('contentType'), md5=md5, file_size=file_size)

    #expand the path because past this point an upload is required and some upload functions require an absolute path
    expanded_upload_path = os.path.expandvars(os.path.expanduser(local_state['path']))

    #determine the upload function based on the UploadDestination
    location = syn._getDefaultUploadDestination(entity_parent_id)
    upload_destination_type = location['concreteType']
    # synapse managed S3
    if upload_destination_type == concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION or \
                    upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION:
        storageString = 'Synapse' if upload_destination_type == concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION else 'your external S3'
        sys.stdout.write('\n' + '#' * 50 + '\n Uploading file to ' + storageString + ' storage \n' + '#' * 50 + '\n')

        return upload_synapse_s3(syn, expanded_upload_path, location['storageLocationId'], mimetype=local_state_file_handle.get('contentType'))
    #external file handle (sftp)
    elif upload_destination_type == concrete_types.EXTERNAL_UPLOAD_DESTINATION:
        if location['uploadType'] == 'SFTP':
            sys.stdout.write('\n%s\n%s\nUploading to: %s\n%s\n' % ('#' * 50,
                                                                   location.get('banner', ''),
                                                                   urlparse(location['url']).netloc,
                                                                   '#' * 50))
            return upload_external_file_handle_sftp(syn, expanded_upload_path, location['url'], mimetype=local_state_file_handle.get('contentType'), md5=local_state_file_handle.get('contentMd5'))
        else:
            raise NotImplementedError('Can only handle SFTP upload locations.')
    #client authenticated S3
    elif upload_destination_type == concrete_types.EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION:
        sys.stdout.write('\n%s\n%s\nUploading to endpoint: [%s] bucket: [%s]\n%s\n' % ('#' * 50,
                                                               location.get('banner', ''),
                                                               location.get('endpointUrl'),
                                                               location.get('bucket'),
                                                               '#' * 50))
        return upload_client_auth_s3(syn, expanded_upload_path, location['bucket'], location['endpointUrl'], location['keyPrefixUUID'], location['storageLocationId'], mimetype=local_state_file_handle.get("contentType"))


def create_external_file_handle(syn, file_path_or_url, mimetype=None, md5=None, file_size=None):
    #just creates the file handle because there is nothing to upload
    return syn._create_ExternalFileHandle(file_path_or_url, mimetype=mimetype, md5=md5, fileSize=file_size)


def upload_external_file_handle_sftp(syn, file_path, sftp_url, mimetype=None):
    username, password = syn._get_sftp_credentials(sftp_url, )
    uploaded_url = SFTPWrapper._sftpUploadFile(file_path, unquote(sftp_url), username, password)

    return syn._create_ExternalFileHandle(uploaded_url, mimetype=mimetype, md5=md5_for_file(file_path).hexdigest(), fileSize=os.stat(file_path).st_size)


def upload_synapse_s3(syn, file_path, storageLocationId=None, mimetype=None):
    file_handle_id = multipart_upload(syn, file_path, contentType=mimetype, storageLocationId=storageLocationId)
    syn.cache.add(file_handle_id, file_path)

    return syn._getFileHandle(file_handle_id)


def upload_client_auth_s3(syn, file_path, bucket, endpoint_url, key_prefix, storage_location_id, mimetype=None):
    profile = syn._get_client_authenticated_s3_profile(endpoint_url, bucket)
    file_key = key_prefix + '/' + os.path.basename(file_path)

    S3ClientWrapper.upload_file(bucket, endpoint_url, file_key, file_path, profile_name=profile)

    file_handle = syn._create_ExternalObjectStoreFileHandle(file_key, file_path,storage_location_id, mimetype=mimetype)
    syn.cache.add(file_handle['id'], file_path)

    return file_handle
