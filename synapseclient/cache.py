# Note: Even though this has Sphinx format, this is not meant to be part of the public docs

"""
************
File Caching
************

Implements a cache on local disk for Synapse file entities and other objects
with a `FileHandle <https://rest.synapse.org/org/sagebionetworks/repo/model/file/FileHandle.html>`_.
This is part of the internal implementation of the client and should not be
accessed directly by users of the client.
"""

import os
import datetime
import json
from math import floor
import synapseclient.utils as utils
from synapseclient.lock import Lock
from synapseclient.exceptions import *


CACHE_ROOT_DIR = os.path.join('~', '.synapseCache')


def epoch_time_to_iso(epoch_time):
    """
    Convert seconds since unix epoch to a string in ISO format
    """
    return None if epoch_time is None else utils.datetime_to_iso(utils.from_unix_epoch_time_secs(epoch_time))


def iso_time_to_epoch(iso_time):
    """
    Convert an ISO formatted time into seconds since unix epoch
    """
    return None if iso_time is None else utils.to_unix_epoch_time_secs(utils.iso_to_datetime(iso_time))


def compare_timestamps(modified_time, cached_time):
    """
    Compare a file's modified timestamp with the timestamp from a .cacheMap file.

    The R client always writes .000 for milliseconds, for compatibility,
    we should match a cached time ending in .000Z, meaning zero milliseconds
    with a modified time with any number of milliseconds.

    :param modified_time: float representing seconds since unix epoch
    :param cached_time: string holding a ISO formatted time
    """
    if cached_time is None or modified_time is None:
        return False
    if cached_time.endswith(".000Z"):
        return cached_time == epoch_time_to_iso(floor(modified_time))
    else:
        return cached_time == epoch_time_to_iso(modified_time)


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

            ## Note that on some file systems, you get greater than millisecond
            ## resolution on a file's modified timestamp. So, it's important to
            ## compare the ISO timestamp strings for equality rather than their
            ## unix epoch time representations.

            if os.path.isdir(path):
                for cached_file_path, cached_time in cache_map.iteritems():
                    if path == os.path.dirname(cached_file_path) and compare_timestamps(_get_modified_time(cached_file_path), cached_time):
                        return cached_file_path
            else:
                for cached_file_path, cached_time in cache_map.iteritems():
                    if path == cached_file_path and compare_timestamps(_get_modified_time(cached_file_path), cached_time):
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

        return cache_map


