import os
import json

SESSION_CACHE_FILEPATH = os.path.expanduser("~/.synapseSession")


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
    except:  # noqa
        # If we cannot parse the cache for any reason, treat it as if the cache is empty
        pass
    return {}


def _write_session_cache(filepath, data):
    """Dumps the JSON data into CACHE_DIR/SESSION_FILENAME."""
    with open(filepath, 'w') as file:
        json.dump(data, file)
        file.write('\n')  # For compatibility with R's JSON parser
