import keyring
import os
import json
import warnings
from keyring.errors import PasswordDeleteError
from keyring.backends.fail import Keyring as FailKeyring


SYNAPSE_CACHED_SESSION_APLICATION_NAME = "<SYNAPSE.ORG CLIENT>"
SESSION_CAHCE_FILEPATH = os.path.expanduser("~/.synapseSession")

# In the case where no key store backend is available (most likely in Linux), a fail.Keyring is returned which will throw errors when any of its functions are called
# However, the errors thrown are of the generic type RuntimeError (not a subclass of RuntimeError).
# It didn't feel safe to try/except and ignore RuntimeError since there are very many other RuntimeErrors that could occur.
# Instead, we use this boolean to keep track of whether we should call the keyring methods
_keyring_is_available = not isinstance(keyring.get_keyring(), FailKeyring)


def get_api_key(username):
    """
    Retrieves the user's API key
    :param str username:
    :return: API key for the specified username
    :rtype: str
    """
    if _keyring_is_available:
        return keyring.get_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username)
    return None


def remove_api_key(username):
    if _keyring_is_available:
        try:
            keyring.delete_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username)
        except PasswordDeleteError:
            #The API key does not exist, but that is fine
            pass


def set_api_key(username, api_key):
    if _keyring_is_available:
        keyring.set_password(SYNAPSE_CACHED_SESSION_APLICATION_NAME, username, api_key)
    else:
        warnings.warn("\nUnable to save user credentials as you do not have a keyring available.\n"
                      "'GNOME keyring' is necessary to be installed for credential store to work on Linux systems:\n"
                      "\tsudo apt-get install gnome-keyring\n\n"
                      "\tsudo apt-get install python-dbus  #(for Python 2 installed via apt-get)\n"
                      "\tOR\n"
                      "\tsudo apt-get install python3-dbus #(for Python 3 installed via apt-get)\n"
                      "\tOR\n"
                      "\tsudo apt-get install libdbus-glib-1-dev #(for custom installation of Python)\n"
                      "\tsudo pip install dbus-python #(may take a while to compile C code)\n"

                        
        
                      "\nIf you are on a headless Linux session (e.g. connecting via SSH), please run the following commands before running your Python session:\n"
                      "\tdbus-run-session -- bash #(replace 'bash' with 'sh' if bash is unavailable)\n"
                      "\techo REPLACE_WITH_YOUR_KEYRING_PASSWORD|gnome-keyring-daemon -- unlock\n")



def get_most_recent_user():
    session_cache = _read_session_cache()
    return session_cache.get("<mostRecent>")


def set_most_recent_user(username):
    cachedSessions = {"<mostRecent>": username}
    _write_session_cache(cachedSessions)


def _read_session_cache():
    """Returns the JSON contents of CACHE_DIR/SESSION_FILENAME."""
    try:
        file = open(SESSION_CAHCE_FILEPATH, 'r')
        result = json.load(file)
        if isinstance(result, dict):
            return result
    except: pass
    return {}


def _write_session_cache(data):
    """Dumps the JSON data into CACHE_DIR/SESSION_FILENAME."""
    with open(SESSION_CAHCE_FILEPATH, 'w') as file:
        json.dump(data, file)
        file.write('\n') # For compatibility with R's JSON parser