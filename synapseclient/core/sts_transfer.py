import collections
import collections.abc
import datetime
import importlib
import os
import threading
import platform

from synapseclient.core.utils import iso_to_datetime, snake_case

try:
    boto3 = importlib.import_module('boto3')
except ImportError:
    # boto is not a requirement to load this module,
    # we are able to optionally use functionality if it's available
    boto3 = None

STS_PERMISSIONS = set(['read_only', 'read_write'])

# default minimum life left on a cached token that we'll hand out.
DEFAULT_MIN_LIFE = datetime.timedelta(hours=1)


class _TokenCache(collections.OrderedDict):
    """A self pruning dictionary of STS tokens.
    It will prune itself as new keys are added, removing the oldest if the
    max_size is exceeded, and always removing all tokens that have expired
    on each additional insert."""

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


class StsTokenStore:
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

    def __init__(self, max_token_cache_size=DEFAULT_TOKEN_CACHE_SIZE):
        self._tokens = {p: _TokenCache(max_token_cache_size) for p in STS_PERMISSIONS}
        self._lock = threading.Lock()

    def get_token(self, syn, entity_id, permission, min_remaining_life: datetime.timedelta):
        with self._lock:
            utcnow = datetime.datetime.utcnow()
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


EXPORT_TEMPLATE_STRINGS = {
    'bash': """\
export SYNAPSE_STS_S3_LOCATION="s3://{bucket}/{baseKey}"
export AWS_ACCESS_KEY_ID="{accessKeyId}"
export AWS_SECRET_ACCESS_KEY="{secretAccessKey}"
export AWS_SESSION_TOKEN="{sessionToken}"
""",
    'cmd': """\
set SYNAPSE_STS_S3_LOCATION="s3://{bucket}/{baseKey}"
set AWS_ACCESS_KEY_ID="{accessKeyId}"
set AWS_SECRET_ACCESS_KEY="{secretAccessKey}"
set AWS_SESSION_TOKEN="{sessionToken}"
""",
    'powershell': """\
$Env:SYNAPSE_STS_S3_LOCATION="s3://{bucket}/{baseKey}"
$Env:AWS_ACCESS_KEY_ID="{accessKeyId}"
$Env:AWS_SECRET_ACCESS_KEY="{secretAccessKey}"
$Env:AWS_SESSION_TOKEN="{sessionToken}"
"""
}


def _format_export_template_string(syn, entity_id, credentials, template_string):
    # as of http://sagebionetworks.jira.com/browse/PLFM-6226
    # the initial call to the back end should return the bucket info as well as the STS token.
    # in the event that it doesn't we make a separate call to get the info from the upload destination.
    # at some point we can probably remove this extra check/call, but depending on when the fix
    # for the above is deployed and when the initial STS python client is released, we include
    # the fall back to fetch the keys separately, but only if necessary.
    bucket_keys = {'bucket', 'baseKey'}
    if all(k in credentials for k in bucket_keys):
        subs = credentials
    else:
        upload_destination = syn._getDefaultUploadDestination(entity_id)
        subs = {**upload_destination, **credentials}

    # if for some reason we still don't have the bucket info, we just don't include
    # the path in the output
    if any(k not in subs for k in bucket_keys):
        template_string = template_string[template_string.find('\n') + 1:]

    return template_string.format(**subs)


def get_sts_credentials(syn, entity_id, permission, *, output_format='json', min_remaining_life=None):
    """See Synapse.get_sts_storage_token"""
    min_remaining_life = min_remaining_life or DEFAULT_MIN_LIFE

    value = syn._sts_token_store.get_token(syn, entity_id, permission, min_remaining_life)

    if output_format == 'boto':
        # the Synapse STS API returns camel cased keys that we need to convert to use with boto.
        # prefix with "aws_", convert to snake case, and exclude any other key/value pairs in the value
        # e.g. expiration
        return {"aws_{}".format(snake_case(k)): value[k] for k in (
            'accessKeyId', 'secretAccessKey', 'sessionToken'
        )}
    elif output_format == 'json':
        # pass through what server sent
        return value

    elif output_format == 'shell':
        # for "shell" we try to detect what is best for the system
        # assume bourne compatible output outside of windows
        if platform.system() == 'Windows' and 'bash' not in os.environ.get('SHELL', ''):
            if len(os.getenv('PSModulePath', '').split(os.pathsep)) >= 3:
                # https://stackoverflow.com/a/55598796
                output_format = 'powershell'
            else:
                output_format = 'cmd'
        else:
            output_format = 'bash'

    template_string = EXPORT_TEMPLATE_STRINGS.get(output_format)
    if not template_string:
        raise ValueError(f'Unrecognized output_format {output_format}')

    return _format_export_template_string(syn, entity_id, value, template_string)


def with_boto_sts_credentials(fn, syn, entity_id, permission):
    """A wrapper around a function that will get sts credentials and try to use them on the given
    function which should take a dictionary with the aws_access_key_id, aws_secret_access_key, and aws_session_token
    as keys. If the given function returns a boto error that looks like the token has expired
    it will retry once after fetching fresh credentials.

    The purpose is to be able to use potentially cached credentials in long running tasks while reducing
    worry that they will expire in the middle of running and cause an unrecoverable error.
    The alternative of fetching a fresh STS token for every request might be okay for a few large files
    but would greatly slow down transferring many small files.
    """

    for attempt in range(2):
        credentials = get_sts_credentials(syn, entity_id, permission, output_format='boto')
        try:
            response = fn(credentials)
        except boto3.exceptions.Boto3Error as ex:
            if 'ExpiredToken' in str(ex) and attempt == 0:
                continue
            else:
                raise

        return response


def is_boto_sts_transfer_enabled(syn):
    """
    Check if the boto/STS transfers are enabled in the Synapse configuration.
    If enabled then synapseclient will attempt to automatically use boto to upload
    and download from supported storage locations that are sts enabled.

    :param syn:         A Synapse client

    :returns: True if STS if enabled, False otherwise
    """
    return bool(boto3 and syn.use_boto_sts_transfers)


def is_storage_location_sts_enabled(syn, entity_id, location):
    """
    Returns whether the given storage location is enabled for STS.

    :param syn:         A Synapse client
    :param entity_id:   id of synapse entity whose storage location we want to check for sts access
    :param location:    a storage location id or an dictionary representing the location UploadDestination
                                these)
    :returns: True if STS if enabled for the location, False otherwise
    """
    if not location:
        return False

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
