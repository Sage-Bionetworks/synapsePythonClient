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

    #use_boto_sts = syn._get_config_section_dict('transfer').get('use_boto_sts', '')
    #return boto3 and 'true' == use_boto_sts.lower()
    return True


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

    def __init__(self, max_size):
        super().__init__()
        self.max_size = max_size

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._prune()

    def _prune(self):
        while len(self) > self.max_size:
            self.popitem(last=False)

        to_delete = []
        before_timestamp = datetime.datetime.utcnow().timestamp()
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

    def __init__(self, max_token_cache_size=DEFAULT_TOKEN_CACHE_SIZE):
        self._tokens = {p: _TokenCache(max_token_cache_size) for p in STS_PERMISSIONS}
        self._lock = threading.Lock()

    def get_token(self, syn, entity_id, permission, min_remaining_life: datetime.timedelta=None):
        min_remaining_life = min_remaining_life if min_remaining_life is not None else self.DEFAULT_MIN_LIFE

        utcnow = datetime.datetime.utcnow()
        with self._lock:
            token_cache = self._tokens.get(permission)
            if token_cache is None:
                raise ValueError(f"Invalid STS permission {permission}")

            token = token_cache.get(entity_id)
            if not token or (iso_to_datetime(token['expiration']) - utcnow) < min_remaining_life:
                # either there is no cached token or the remaining life on the token isn't enough so fetch new
                token = token_cache[entity_id] = self._fetch_token(syn, entity_id, permission)

        return token

    @staticmethod
    def _fetch_token(syn, entity_id, permission):
        return syn.restGET(f'/entity/{entity_id}/sts?permission={permission}')


_TOKEN_STORE = _StsTokenStore()


def get_sts_credentials(syn, entity_id, permission, output_format=None, **kwargs):
    value = _TOKEN_STORE.get_token(syn, entity_id, permission, **kwargs)
    value['secretAccessKey'] = value['secretAccessKey']

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


def with_boto_sts_credentials(fn, *args, **kwargs):
    """A wrapper around a function that will get sts credentials and try to use them on the given
    # function which should take aws_access_key_id, aws_secret_access_key, and aws_session_token as
    kwarg parameters. If the given function returns a boto error that looks like the token has expired
    it will retry once after fetching fresh credentials.

    The purpose is to be able to use potentially cached credentials in long running tasks while reducing
    worry that they will expire in the middle of running and cause an unrecoverable error.
    The alternative of fetching a fresh STS token for every request might be okay for a few large files
    but would greatly slow down transferring many small files.
    """

    # the passed fn takes boto style credentials
    token_kwargs = dict(kwargs)
    token_kwargs['output_format'] = 'boto'

    for attempt in range(2):
        credentials = get_sts_credentials(*args, **token_kwargs)
        try:
            response = fn(**credentials)
        except boto3.exceptions.Boto3Error as ex:
            if 'ExpiredToken' in str(ex) and attempt == 0:
                continue
            else:
                raise

        return response
