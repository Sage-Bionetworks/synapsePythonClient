# Note: Even though this has Sphinx format, this is not meant to be part of the public docs

"""
************
File Caching
************

Implements a cache on local disk for Synapse file entities and other objects with a
`FileHandle <https://docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandle.html>`_.
This is part of the internal implementation of the client and should not be accessed directly by users of the client.
"""

import collections
import datetime
import json
import operator
import os
import re
import shutil
import math

from synapseclient.core.lock import Lock
from synapseclient.core.exceptions import *


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
    Compare two ISO formatted timestamps, with a special case when cached_time ends in .000Z.

    For backward compatibility, we always write .000 for milliseconds into the cache.
    We then match a cached time ending in .000Z, meaning zero milliseconds with a modified time with any number of
    milliseconds.

    :param modified_time: float representing seconds since unix epoch
    :param cached_time: string holding a ISO formatted time
    """
    if cached_time is None or modified_time is None:
        return False
    if cached_time.endswith(".000Z"):
        return cached_time == epoch_time_to_iso(math.floor(modified_time))
    else:
        return cached_time == epoch_time_to_iso(modified_time)


def _get_modified_time(path):
    if os.path.exists(path):
        return os.path.getmtime(path)
    return None


class Cache:
    """
    Represent a cache in which files are accessed by file handle ID.
    """

    def __setattr__(self, key, value):
        # expand out home shortcut ('~') and environment variables when setting cache_root_dir
        if key == "cache_root_dir":
            value = os.path.expandvars(os.path.expanduser(value))
            # create the cache_root_dir if it does not already exist
            if not os.path.exists(value):
                os.makedirs(value)
        self.__dict__[key] = value

    def __init__(self, cache_root_dir=CACHE_ROOT_DIR, fanout=1000):
        # set root dir of cache in which meta data will be stored and files
        # will be stored here by default, but other locations can be specified
        self.cache_root_dir = cache_root_dir
        self.fanout = fanout
        self.cache_map_file_name = ".cacheMap"

    def get_cache_dir(self, file_handle_id):
        if isinstance(file_handle_id, collections.Mapping):
            if 'dataFileHandleId' in file_handle_id:
                file_handle_id = file_handle_id['dataFileHandleId']
            elif 'concreteType' in file_handle_id \
                    and 'id' in file_handle_id \
                    and file_handle_id['concreteType'].startswith('org.sagebionetworks.repo.model.file'):
                file_handle_id = file_handle_id['id']
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
            f.write('\n')  # For compatibility with R's JSON parser

    def contains(self, file_handle_id, path):
        """
        Given a file and file_handle_id, return True if an unmodified cached
        copy of the file exists at the exact path given or False otherwise.
        :param file_handle_id:
        :param path: file path at which to look for a cached copy
        """
        cache_dir = self.get_cache_dir(file_handle_id)
        if not os.path.exists(cache_dir):
            return False

        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)

            cached_time = cache_map.get(path, None)
            if cached_time:
                return compare_timestamps(_get_modified_time(path), cached_time)
        return False

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
        if not os.path.exists(cache_dir):
            return None

        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)

            # If the caller specifies a path and that path exists in the cache
            # but has been modified, we need to indicate no match by returning
            # None. The logic for updating a synapse entity depends on this to
            # determine the need to upload a new file.

            if path is not None:
                # If we're given a path to a directory, look for a cached file in that directory
                if os.path.isdir(path):
                    matching_unmodified_directory = None
                    removed_entry_from_cache = False  # determines if cache_map needs to be rewritten to disk

                    # iterate a copy of cache_map to allow modifying original cache_map
                    for cached_file_path, cached_time in dict(cache_map).items():
                        if path == os.path.dirname(cached_file_path):
                            # compare_timestamps has an implicit check for whether the path exists
                            if compare_timestamps(_get_modified_time(cached_file_path), cached_time):
                                # "break" instead of "return" to write removed invalid entries to disk if necessary
                                matching_unmodified_directory = cached_file_path
                                break
                            else:
                                # remove invalid cache entries pointing to files that that no longer exist
                                # or have been modified
                                del cache_map[cached_file_path]
                                removed_entry_from_cache = True
                                
                    if removed_entry_from_cache:
                        # write cache_map with non-existent entries removed
                        self._write_cache_map(cache_dir, cache_map)

                    if matching_unmodified_directory is not None:
                        return matching_unmodified_directory

                # if we're given a full file path, look up a matching file in the cache
                else:
                    cached_time = cache_map.get(path, None)
                    if cached_time:
                        return path if compare_timestamps(_get_modified_time(path), cached_time) else None

            # return most recently cached and unmodified file OR
            # None if there are no unmodified files
            for cached_file_path, cached_time in sorted(cache_map.items(), key=operator.itemgetter(1), reverse=True):
                if compare_timestamps(_get_modified_time(cached_file_path), cached_time):
                    return cached_file_path
            return None

    def add(self, file_handle_id, path):
        """
        Add a file to the cache
        """
        if not path or not os.path.exists(path):
            raise ValueError("Can't find file \"%s\"" % path)

        cache_dir = self.get_cache_dir(file_handle_id)
        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)
            # write .000 milliseconds for backward compatibility
            cache_map[path] = epoch_time_to_iso(math.floor(_get_modified_time(path)))
            self._write_cache_map(cache_dir, cache_map)

        return cache_map

    def remove(self, file_handle_id, path=None, delete=None):
        """
        Remove a file from the cache.

        :param file_handle_id: Will also extract file handle id from either a File or file handle
        :param path: If the given path is None, remove (and potentially delete)
                     all cached copies. If the path is that of a file in the
                     .cacheMap file, remove it.

        :returns: A list of files removed
        """
        removed = []
        cache_dir = self.get_cache_dir(file_handle_id)

        # if we've passed an entity and not a path, get path from entity
        if path is None and isinstance(file_handle_id, collections.Mapping) and 'path' in file_handle_id:
            path = file_handle_id['path']

        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            if path is None:
                for path in cache_map:
                    if delete is True and os.path.exists(path):
                        os.remove(path)
                    removed.append(path)
                cache_map = {}
            else:
                path = utils.normalize_path(path)
                if path in cache_map:
                    if delete is True and os.path.exists(path):
                        os.remove(path)
                    del cache_map[path]
                    removed.append(path)

            self._write_cache_map(cache_dir, cache_map)

        return removed

    def _cache_dirs(self):
        """
        Generate a list of all cache dirs, directories of the form:
        [cache.cache_root_dir]/949/59949
        """
        for item1 in os.listdir(self.cache_root_dir):
            path1 = os.path.join(self.cache_root_dir, item1)
            if os.path.isdir(path1) and re.match('\d+', item1):
                for item2 in os.listdir(path1):
                    path2 = os.path.join(path1, item2)
                    if os.path.isdir(path2) and re.match('\d+', item2):
                        yield path2

    def purge(self, before_date, dry_run=False):
        """
        Purge the cache. Use with caution. Delete files whose cache maps were last updated prior to the given date.

        Deletes .cacheMap files and files stored in the cache.cache_root_dir, but does not delete files stored outside
        the cache.
        """
        if isinstance(before_date, datetime.datetime):
            before_date = utils.to_unix_epoch_time_secs(before_date)
        count = 0
        for cache_dir in self._cache_dirs():
            # _get_modified_time returns None if the cache map file doesn't
            # exist and n > None evaluates to True in python 2.7(wtf?). I'm guessing it's
            # OK to purge directories in the cache that have no .cacheMap file
            last_modified_time = _get_modified_time(os.path.join(cache_dir, self.cache_map_file_name))
            if last_modified_time is None or before_date > last_modified_time:
                if dry_run:
                    print(cache_dir)
                else:
                    shutil.rmtree(cache_dir)
                count += 1
        return count

