# Note: Even though this has Sphinx format, this is not meant to be part of the public docs

"""
************
File Caching
************

.. automethod:: synapseclient.cache.local_file_has_changed
.. automethod:: synapseclient.cache.add_local_file_to_cache
.. automethod:: synapseclient.cache.remove_local_file_from_cache
.. automethod:: synapseclient.cache.retrieve_local_file_info
.. automethod:: synapseclient.cache.get_alternate_file_name

~~~~~~~
Helpers
~~~~~~~

.. automethod:: synapseclient.cache.obtain_lock_and_read_cache
.. automethod:: synapseclient.cache.write_cache_then_release_lock
.. automethod:: synapseclient.cache.iterator_over_cache_map
.. automethod:: synapseclient.cache.is_lock_valid
.. automethod:: synapseclient.cache.determine_cache_directory
.. automethod:: synapseclient.cache.determine_local_file_location
.. automethod:: synapseclient.cache.parse_cache_entry_into_seconds
.. automethod:: synapseclient.cache.get_modification_time

"""

import os
import datetime
import json
import synapseclient.utils as utils
from synapseclient.lock import Lock
from synapseclient.exceptions import *


CACHE_ROOT_DIR = os.path.join('~', '.synapseCache')


def epoch_time_to_iso(epoch_time):
    """
    Convert seconds since unix epoch to a string in ISO format
    """
    return utils.datetime_to_iso(utils.from_unix_epoch_time_secs(epoch_time))


def iso_time_to_epoch(iso_time):
    """
    Convert an ISO formatted time into seconds since unix epoch
    """
    return utils.to_unix_epoch_time_secs(utils.iso_to_datetime(iso_time))


def _get_modified_time(path):
    if os.path.exists(path):
        return os.path.getmtime(path)
    return None


class Cache():
    """
    Represent a cache in which files are accessed by file handle ID.
    """

    def __init__(self, cache_root_dir=CACHE_ROOT_DIR, fanout=1000):

        ## set root dir of cache in which meta data will be stored and files
        ## will be stored here by default, but other locations can be specified
        cache_root_dir = os.path.expanduser(cache_root_dir)
        if not os.path.exists(cache_root_dir):
            os.makedirs(cache_root_dir)
        self.cache_root_dir = cache_root_dir
        self.fanout = fanout
        self.cache_map_file_name = ".cacheMap"


    def get_cache_dir(self, file_handle_id):
        return os.path.join(self.cache_root_dir, str(int(file_handle_id) % self.fanout), str(file_handle_id))


    def _read_cache_map(self, cache_dir):
        cache_map_file = os.path.join(cache_dir, self.cache_map_file_name)

        if not os.path.exists(cache_map_file):
            return {}

        with open(cache_map_file, 'r') as f:
            cache_map = json.load(f)
        return cache_map


    def _write_cache_map(self, cache_dir, cache_map):
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        cache_map_file = os.path.join(cache_dir, self.cache_map_file_name)

        with open(cache_map_file, 'w') as f:
            json.dump(cache_map, f)
            f.write('\n') # For compatibility with R's JSON parser


    def get(self, file_handle_id, path=None):
        """
        Retrieve a file with the given file handle from the cache.

        :param file_handle_id:
        :param path: If the given path is None, look for a cached copy of the
                     file in the cache directory. If the path is a directory,
                     look there for a cached copy. If a full file-path is
                     given, only check whether that exact file exists and is
                     unmodified since it was cached.

        :returns: Either a file path, if an unmodified cached copy of the file
                  exists in the specified location or None if it does not
        """
        cache_dir = self.get_cache_dir(file_handle_id)
        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)

            # if file_path is None and file_name is None:
            #     ## find most recently cached version of the file
            #     most_recent_cached_file_path = None
            #     most_recent_cached_time = float("-inf")
            #     for cached_file_path, cached_time in cache_map.iteritems():
            #         cached_time = iso_time_to_epoch(cached_time)
            #         if _get_modified_time(cached_file_path) == cached_time and cached_time > most_recent_cached_time:
            #             most_recent_cached_file_path = cached_file_path
            #             most_recent_cached_time = cached_time
            #     return most_recent_cached_file_path

            if path is None:
                path = self.get_cache_dir(file_handle_id)

            if os.path.isdir(path):
                for cached_file_path, cached_time in cache_map.iteritems():
                    if path == os.path.dirname(cached_file_path) and _get_modified_time(cached_file_path) == iso_time_to_epoch(cached_time):
                        return cached_file_path
            else:
                for cached_file_path, cached_time in cache_map.iteritems():
                    if path == cached_file_path and _get_modified_time(cached_file_path) == iso_time_to_epoch(cached_time):
                        return cached_file_path

            return None


    def add(self, file_handle_id, path):
        """
        Add a file to the cache
        """
        if not path or not os.path.exists(path):
            raise ValueError("Can't cache file \"%s\"" % path)

        cache_dir = self.get_cache_dir(file_handle_id)
        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)
            cache_map[path] = epoch_time_to_iso(_get_modified_time(path))
            self._write_cache_map(cache_dir, cache_map)


