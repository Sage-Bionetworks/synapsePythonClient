import collections.abc
import importlib

from synapseclient.core.utils import snake_case

try:
    boto3 = importlib.import_module('boto3')
except ImportError:
    # boto is not a requirement to load this module,
    # we are able to optionally use functionality if it's available
    boto3 = None


def get_sts_credentials(syn, entity_id, read_only: bool):
    permission = "read_only" if read_only else "read_write"
    response = syn.restGET(f'/entity/{entity_id}/sts?permission={permission}')

    # the Synapse STS API returns camel cased keys that we need to convert to use with boto.
    # prefix with "aws_", convert to snake case, and exclude any other key/value pairs in the response
    # e.g. expiration
    return {"aws_{}".format(snake_case(k)): response[k] for k in (
        'accessKeyId', 'secretAccessKey', 'sessionToken'
    )}


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
    Creates a new Entity or updates an existing Entity, uploading any files in the process.

    :param syn:         A Synapse client
    :param entity_id:   id of synapse entity whose storage location we want to check for sts access
    :param location:    a storage location id or an dictionary representing the location UploadDestination
                                these)
    :returns: True if STS if enabled for the location, False otherwise
    """
    destination = location
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
