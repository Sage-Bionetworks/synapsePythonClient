import collections
import collections.abc
import datetime
from enum import Enum, auto
import importlib
import json
import os
import threading
import typing
import platform

from synapseclient.core.utils import iso_to_datetime, snake_case

try:
    boto3 = importlib.import_module('boto3')
except ImportError:
    # boto is not a requirement to load this module,
    # we are able to optionally use functionality if it's available
    boto3 = None

STS_PERMISSIONS = set(['read_only', 'read_write'])

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


class _TokenCache(collections.OrderedDict):

    def __init__(self, min_life_delta, max_size):
        super().__init__()
        self.min_life_delta = min_life_delta
        self.max_size = max_size

    def _check_retrieved_token(self, key, token):
        if token and iso_to_datetime(token['expiration']) < (datetime.datetime.utcnow() + self.min_life_delta):
            # the token is too old to return
            del self[key]
            return None
        return token

    def __getitem__(self, key):
        token = super().__getitem__(key)
        return self._check_retrieved_token(key, token)

    def get(self, key, default=None):
        token = super().get(key, default)
        return self._check_retrieved_token(key, token)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._prune()

    def _prune(self):
        while len(self) > self.max_size:
            self.popitem(last=False)

        to_delete = []
        before_timestamp = (datetime.datetime.utcnow() + self.min_life_delta).timestamp()
        for entity_id, token in self.items():
            expiration_iso_str = token['expiration']

            # our "iso_to_datetime" util naively assumes UTC ("Z") times which in practice STS tokens are
            if iso_to_datetime(expiration_iso_str).timestamp() < before_timestamp:
                to_delete.append(entity_id)
            else:
                break

        for entity_id in to_delete:
            del self[entity_id]


class _StsTokenStore:
    """
    Cache STS tokens in memory for observed entity ids.
    An optimization for long lived Synapse objects that will interact with the same
    Synapse storage locations over and over again so they don't have to do a remote call
    to fetch a new token for every entity, which for e.g. small files can amount to
    non trivial overhead.
    """

    # each token is < 1k but given we don't know how long Python process will be running
    # (could be very long in a programmatic environment) we impose a limit on the maximum
    # number of tokens we will store in memory to prevent this optimization from becoming
    # a memory leak.
    DEFAULT_TOKEN_CACHE_SIZE = 5000

    # we won't hand out previously retrieved tokens that have less than this amount of
    # time left on them. we don't know exactly when they'll be used so we don't want to
    # hand out an about-to-expire cached token.
    DEFAULT_MIN_LIFE=datetime.timedelta(hours=1)

    def __init__(self, min_life_delta=DEFAULT_MIN_LIFE, max_token_cache_size=DEFAULT_TOKEN_CACHE_SIZE):
        self._tokens = {p: _TokenCache(min_life_delta, max_token_cache_size) for p in STS_PERMISSIONS}
        self._lock = threading.Lock()

    def get_token(self, syn, entity_id, permission):
        with self._lock:
            token_cache = self._tokens.get(permission)
            if token_cache is None:
                raise ValueError(f"Invalid STS permission {permission}")

            token = token_cache.get(entity_id)
            if not token:
                token = token_cache[entity_id] = self._fetch_token(syn, entity_id, permission)

        return token

    @staticmethod
    def _fetch_token(syn, entity_id, permission):
        return syn.restGET(f'/entity/{entity_id}/sts?permission={permission}')


_TOKEN_STORE = _StsTokenStore()


def get_sts_credentials(syn, entity_id, permission, output_format=None, ):
    value = _TOKEN_STORE.get_token(syn, entity_id, permission)

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
