import keyring
import os
import json
import keyring.errors as keyring_errors
from synapseclient.core.cache import CACHE_ROOT_DIR
from synapseclient.core.utils import equal_paths

SYNAPSE_CACHED_SESSION_APLICATION_NAME = "SYNAPSE.ORG_CLIENT"
SESSION_CACHE_FILEPATH = os.path.expanduser("~/.synapseSession")


def get_api_key(username):
    """
    Retrieves the user's API key
    :param str username:
    :return: API key for the specified username
    :rtype: str
    """
    if username is not None:
            return keyring.get_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username)
    return None


def remove_api_key(username):
        try:
            keyring.delete_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username)
        except keyring_errors.PasswordDeleteError:
            # The API key does not exist, but that is fine
            pass


def set_api_key(username, api_key):
    keyring.set_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username, api_key)


def get_most_recent_user():
    session_cache = _read_session_cache(SESSION_CACHE_FILEPATH)
    return session_cache.get("<mostRecent>")


def set_most_recent_user(username):
    cachedSessions = {"<mostRecent>": username}
    _write_session_cache(SESSION_CACHE_FILEPATH, cachedSessions)


def _read_session_cache(filepath):
    """Returns the JSON contents of CACHE_DIR/SESSION_FILENAME."""
    try:
        file = open(filepath, 'r')
        result = json.load(file)
        if isinstance(result, dict):
            return result
    except:
        # If we cannot parse the cache for any reason, treat it as if the cache is empty
        pass
    return {}


def _write_session_cache(filepath, data):
    """Dumps the JSON data into CACHE_DIR/SESSION_FILENAME."""
    with open(filepath, 'w') as file:
        json.dump(data, file)
        file.write('\n')  # For compatibility with R's JSON parser


def migrate_old_session_file_credentials_if_necessary(syn):
    old_session_file_path = os.path.join(syn.cache.cache_root_dir, '.session')

    # only migrate if the download cache is in the default location (i.e. user did not set its location)
    # we don't want to migrate credentials if they were a part of a cache shared by multiple people
    if equal_paths(syn.cache.cache_root_dir, os.path.expanduser(CACHE_ROOT_DIR)):
        # iterate through the old file and place in new credential storage
        old_session_dict = _read_session_cache(old_session_file_path)
        for key, value in old_session_dict.items():
            if key == "<mostRecent>":
                set_most_recent_user(value)
            else:
                set_api_key(key, value)
    # always attempt to remove the old session file
    try:
        os.remove(old_session_file_path)
    except OSError:
        # file already removed.
        pass
