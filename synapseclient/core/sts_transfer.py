import collections.abc
import importlib
import json
import os
import platform

from synapseclient.core.utils import snake_case

try:
    boto3 = importlib.import_module('boto3')
except ImportError:
    # boto is not a requirement to load this module,
    # we are able to optionally use functionality if it's available
    boto3 = None


def enable_sts(syn, folder_id):
    destination = {
        'uploadType': 'S3',
        'stsEnabled': True,
        'concreteType': 'org.sagebionetworks.repo.model.project.S3StorageLocationSetting',
    }

    destination = syn.restPOST('/storageLocation', body=json.dumps(destination))

    project_destination = {
        'concreteType': 'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
        'settingsType': 'upload',
        'locations': [destination['storageLocationId']],
        'projectId': folder_id
    }

    return syn.restPOST('/projectSettings', body=json.dumps(project_destination))


def get_sts_credentials(syn, entity_id, write=False, output_format=None):
    permission = 'read_write' if write else 'read_only'

    # return value is initially a dictionary as returned to us by the API.
    # if an output format is specified we'll translate appropriately.
    value = syn.restGET(f'/entity/{entity_id}/sts?permission={permission}')

    if output_format == 'boto':
        # the Synapse STS API returns camel cased keys that we need to convert to use with boto.
        # prefix with "aws_", convert to snake case, and exclude any other key/value pairs in the value
        # e.g. expiration
        value = {"aws_{}".format(snake_case(k)): value[k] for k in (
            'accessKeyId', 'secretAccessKey', 'sessionToken'
        )}

    elif output_format == 'shell':
        # make output in the form of commands that will set the credentials into the user's
        # environment such that they can e.g. run awscli commands

        if platform.system() == 'Windows' and 'bash' not in os.environ.get('SHELL'):
            # if we're running on windows and we can't detect we're running a bash shell
            # then we make the output compatible for a windows cmd prompt environment.
            value = f"""\
setx AWS_ACCESS_KEY_ID {value['accessKeyId']}
setx AWS_SECRET_ACCESS_KEY {value['secretAccessKey']}
setx AWS_SESSION_TOKEN {value['sessionToken']}
"""
        else:
            # assume bourne shell compatible (i.e. bash, zsh, etc)
            value = f"""\
export AWS_ACCESS_KEY_ID={value['accessKeyId']}
export AWS_SECRET_ACCESS_KEY={value['secretAccessKey']}
export AWS_SESSION_TOKEN={value['sessionToken']}
"""

    return value


def is_boto_sts_transfer_enabled(syn):
    """
    Check if the boto/STS transfers are enabled in the Synapse configuration

    :param syn:         A Synapse client

    :returns: True if STS if enabled, False otherwise
    """

    use_boto_sts = syn._get_config_section_dict('transfer').get('use_boto_sts', '')
    return boto3 and 'true' == use_boto_sts.lower()


def is_storage_location_sts_enabled(syn, entity_id, location):
    """
    Returns whether the given storage location is enabled for STS.

    :param syn:         A Synapse client
    :param entity_id:   id of synapse entity whose storage location we want to check for sts access
    :param location:    a storage location id or an dictionary representing the location UploadDestination
                                these)
    :returns: True if STS if enabled for the location, False otherwise
    """
    if isinstance(location, collections.abc.Mapping):
        # looks like this is already an upload destination dict
        destination = location

    else:
        # otherwise treat it as a storage location id,
        destination = syn.restGET(
            f'/entity/{entity_id}/uploadDestination/{location}',
            endpoint=syn.fileHandleEndpoint
        )

    return destination.get('stsEnabled', False)
