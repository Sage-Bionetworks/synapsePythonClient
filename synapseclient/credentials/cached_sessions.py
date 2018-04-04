import keyring
import os
import json
import warnings
from keyring.errors import PasswordDeleteError
from keyrings.alt.file import PlaintextKeyring


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
        except PasswordDeleteError:
            #The API key does not exist, but that is fine
            pass


def set_api_key(username, api_key):
    keyring.set_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username, api_key)




def get_most_recent_user():
    session_cache = _read_session_cache()
    return session_cache.get("<mostRecent>")


def set_most_recent_user(username):
    cachedSessions = {"<mostRecent>": username}
    _write_session_cache(cachedSessions)


def _read_session_cache():
    """Returns the JSON contents of CACHE_DIR/SESSION_FILENAME."""
    try:
        file = open(SESSION_CACHE_FILEPATH, 'r')
        result = json.load(file)
        if isinstance(result, dict):
            return result
    except: pass
    return {}


def _write_session_cache(data):
    """Dumps the JSON data into CACHE_DIR/SESSION_FILENAME."""
    with open(SESSION_CACHE_FILEPATH, 'w') as file:
        json.dump(data, file)
        file.write('\n') # For compatibility with R's JSON parser

