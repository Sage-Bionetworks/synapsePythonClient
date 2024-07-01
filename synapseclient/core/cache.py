# Note: Even though this has Sphinx format, this is not meant to be part of the public docs

"""
************
File Caching
************

Implements a cache on local disk for Synapse file entities and other objects with a
[FileHandle](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandle.html).
This is part of the internal implementation of the client and should not be accessed directly by users of the client.
"""

import collections.abc
import datetime
import json
import math
import os
import re
import shutil
import typing

from opentelemetry import trace

from synapseclient.core import utils
from synapseclient.core.lock import Lock

tracer = trace.get_tracer("synapseclient")

CACHE_ROOT_DIR = os.path.join("~", ".synapseCache")


def epoch_time_to_iso(epoch_time):
    """
    Convert seconds since unix epoch to a string in ISO format
    """
    return (
        None
        if epoch_time is None
        else utils.datetime_to_iso(utils.from_unix_epoch_time_secs(epoch_time))
    )


def iso_time_to_epoch(iso_time):
    """
    Convert an ISO formatted time into seconds since unix epoch
    """
    return (
        None
        if iso_time is None
        else utils.to_unix_epoch_time_secs(utils.iso_to_datetime(iso_time))
    )


def compare_timestamps(modified_time, cached_time):
    """
    Compare two ISO formatted timestamps, with a special case when cached_time ends in .000Z.

    For backward compatibility, we always write .000 for milliseconds into the cache.
    We then match a cached time ending in .000Z, meaning zero milliseconds with a modified time with any number of
    milliseconds.

    Arguments:
        modified_time: The float representing seconds since unix epoch
        cached_time:   The string holding a ISO formatted time
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
                os.makedirs(value, exist_ok=True)
        self.__dict__[key] = value

    def __init__(self, cache_root_dir=CACHE_ROOT_DIR, fanout=1000):
        # set root dir of cache in which meta data will be stored and files
        # will be stored here by default, but other locations can be specified
        self.cache_root_dir = cache_root_dir
        self.fanout = fanout
        self.cache_map_file_name = ".cacheMap"

    def get_cache_dir(
        self, file_handle_id: typing.Union[collections.abc.Mapping, str]
    ) -> str:
        if isinstance(file_handle_id, collections.abc.Mapping):
            if "dataFileHandleId" in file_handle_id:
                file_handle_id = file_handle_id["dataFileHandleId"]
            elif (
                "concreteType" in file_handle_id
                and "id" in file_handle_id
                and file_handle_id["concreteType"].startswith(
                    "org.sagebionetworks.repo.model.file"
                )
            ):
                file_handle_id = file_handle_id["id"]
        return os.path.join(
            self.cache_root_dir,
            str(int(file_handle_id) % self.fanout),
            str(file_handle_id),
        )

    def _read_cache_map(self, cache_dir: str) -> dict:
        cache_map_file = os.path.join(cache_dir, self.cache_map_file_name)

        if not os.path.exists(cache_map_file):
            return {}

        with open(cache_map_file, "r") as f:
            try:
                cache_map = json.load(f)
            except json.decoder.JSONDecodeError:
                # a corrupt cache map file that is not parseable as JSON is treated
                # as if it does not exist at all (will be overwritten).
                return {}

        return cache_map

    def _write_cache_map(self, cache_dir: str, cache_map: dict) -> None:
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        cache_map_file = os.path.join(cache_dir, self.cache_map_file_name)

        with open(cache_map_file, "w") as f:
            json.dump(cache_map, f)
            f.write("\n")  # For compatibility with R's JSON parser

    def _get_cache_modified_time(
        self, cache_map_entry: typing.Union[str, dict, None]
    ) -> typing.Union[str, None]:
        """
        Retrieve the `modified_time` from the `cache_map_entry`. This needed to be
        backwards-compatible any cache entries that do not have the new JSON structure
        for data. That means that if the cache_map_entry has a `modified_time` key,
        then it is a new entry and we can return the value. If it does not, then it
        is an old entry and we should return the `cache_map_entry` itself.

        The caveat is that `cache_map_entry` needs to be a string to return the value
        otherwise it will return None.

        Arguments:
            cache_map_entry: The entry from the cache map

        Returns:
            The modified time if it exists, otherwise the cache_map_entry
        """
        if cache_map_entry is not None and "modified_time" in cache_map_entry:
            return cache_map_entry.get("modified_time", None)
        elif cache_map_entry is not None and isinstance(cache_map_entry, str):
            return cache_map_entry
        return None

    def _get_cache_content_md5(
        self, cache_map_entry: typing.Union[str, dict, None]
    ) -> typing.Union[str, None]:
        """
        Retrieve the `content_md5` from the cache_map_entry.

        Arguments:
            cache_map_entry: The entry from the cache map

        Returns:
            The content md5 if it exists, otherwise None
        """
        if cache_map_entry is not None and "content_md5" in cache_map_entry:
            return cache_map_entry.get("content_md5", None)
        else:
            return None

    def _cache_item_unmodified(
        self, cache_map_entry: typing.Union[str, dict], path: str
    ) -> bool:
        """
        Determine if the cache_map_entry is unmodified by comparing the modified_time
        and content_md5 to the file at the given path.

        Arguments:
            cache_map_entry: The entry from the cache map
            path:            The path to the file to compare to

        Returns:
            True if the cache_map_entry is unmodified, otherwise False
        """
        cached_time = self._get_cache_modified_time(cache_map_entry)
        cached_md5 = self._get_cache_content_md5(cache_map_entry)

        # compare_timestamps has an implicit check for whether the path exists
        return compare_timestamps(_get_modified_time(path), cached_time) and (
            cached_md5 is None or cached_md5 == utils.md5_for_file(path).hexdigest()
        )

    def contains(
        self, file_handle_id: typing.Union[collections.abc.Mapping, str], path: str
    ) -> bool:
        """
        Given a file and file_handle_id, return True if an unmodified cached
        copy of the file exists at the exact path given or False otherwise.

        Arguments:
            file_handle_id: The ID of the fileHandle
            path:           The file path at which to look for a cached copy
        """
        cache_dir = self.get_cache_dir(file_handle_id)
        if not os.path.exists(cache_dir):
            return False

        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)

            cached_time = self._get_cache_modified_time(cache_map.get(path, None))

            if cached_time:
                return compare_timestamps(_get_modified_time(path), cached_time)
        return False

    @tracer.start_as_current_span("cache::get")
    def get(
        self,
        file_handle_id: typing.Union[collections.abc.Mapping, str],
        path: str = None,
    ) -> typing.Union[str, None]:
        """
        Retrieve a file with the given file handle from the cache.

        Arguments:
            file_handle_id: The ID of the fileHandle
            path:           If the given path is None, look for a cached copy of the
                            file in the cache directory. If the path is a directory,
                            look there for a cached copy. If a full file-path is
                            given, only check whether that exact file exists and is
                            unmodified since it was cached.

        Returns:
            Either a file path, if an unmodified cached copy of the file
            exists in the specified location or None if it does not
        """
        cache_dir = self.get_cache_dir(file_handle_id)
        trace.get_current_span().set_attributes(
            {
                "synapse.cache.dir": cache_dir,
                "synapse.cache.file_handle_id": file_handle_id,
            }
        )
        if not os.path.exists(cache_dir):
            trace.get_current_span().set_attributes({"synapse.cache.hit": False})
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
                    removed_entry_from_cache = (
                        False  # determines if cache_map needs to be rewritten to disk
                    )

                    # iterate a copy of cache_map to allow modifying original cache_map
                    for cached_file_path, cache_map_entry in dict(cache_map).items():
                        if path == os.path.dirname(cached_file_path):
                            if self._cache_item_unmodified(
                                cache_map_entry, cached_file_path
                            ):
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
                        trace.get_current_span().set_attributes(
                            {"synapse.cache.hit": True}
                        )
                        return matching_unmodified_directory

                # if we're given a full file path, look up a matching file in the cache
                else:
                    cache_map_entry = cache_map.get(path, None)
                    if cache_map_entry:
                        matching_file_path = (
                            path
                            if self._cache_item_unmodified(cache_map_entry, path)
                            else None
                        )
                        trace.get_current_span().set_attributes(
                            {"synapse.cache.hit": matching_file_path is not None}
                        )
                        return matching_file_path

            # return most recently cached and unmodified file OR
            # None if there are no unmodified files
            for cached_file_path, cache_map_entry in sorted(
                cache_map.items(),
                key=lambda item: (
                    item[1]["modified_time"] if isinstance(item[1], dict) else item[1]
                ),
                reverse=True,
            ):
                if self._cache_item_unmodified(cache_map_entry, cached_file_path):
                    trace.get_current_span().set_attributes({"synapse.cache.hit": True})
                    return cached_file_path

            trace.get_current_span().set_attributes({"synapse.cache.hit": False})
            return None

    def add(
        self,
        file_handle_id: typing.Union[collections.abc.Mapping, str],
        path: str,
        md5: str = None,
    ) -> dict:
        """
        Add a file to the cache
        """
        if not path or not os.path.exists(path):
            raise ValueError('Can\'t find file "%s"' % path)

        cache_dir = self.get_cache_dir(file_handle_id)
        content_md5 = md5 or utils.md5_for_file(path).hexdigest()
        with Lock(self.cache_map_file_name, dir=cache_dir):
            cache_map = self._read_cache_map(cache_dir)

            path = utils.normalize_path(path)
            # write .000 milliseconds for backward compatibility
            cache_map[path] = {
                "modified_time": epoch_time_to_iso(
                    math.floor(_get_modified_time(path))
                ),
                "content_md5": content_md5,
            }
            self._write_cache_map(cache_dir, cache_map)

        return cache_map

    def remove(
        self,
        file_handle_id: typing.Union[collections.abc.Mapping, str],
        path: str = None,
        delete: bool = None,
    ) -> typing.List[str]:
        """
        Remove a file from the cache.

        Arguments:
            file_handle_id: Will also extract file handle id from either a File or file handle
            path:           If the given path is None, remove (and potentially delete)
                            all cached copies. If the path is that of a file in the
                            .cacheMap file, remove it.
            delete:         If True, delete the file from disk as well as removing it from the cache

        Returns:
            A list of files removed
        """
        removed = []
        cache_dir = self.get_cache_dir(file_handle_id)

        # if we've passed an entity and not a path, get path from entity
        if (
            path is None
            and isinstance(file_handle_id, collections.abc.Mapping)
            and "path" in file_handle_id
        ):
            path = file_handle_id["path"]

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
            if os.path.isdir(path1) and re.match("\\d+", item1):
                for item2 in os.listdir(path1):
                    path2 = os.path.join(path1, item2)
                    if os.path.isdir(path2) and re.match("\\d+", item2):
                        yield path2

    def purge(
        self,
        before_date: typing.Union[datetime.datetime, int] = None,
        after_date: typing.Union[datetime.datetime, int] = None,
        dry_run: bool = False,
    ) -> int:
        """
        Purge the cache. Use with caution. Deletes files whose cache maps were last updated in a specified period.

        Deletes .cacheMap files and files stored in the cache.cache_root_dir, but does not delete files stored outside
        the cache.

        Arguments:
            before_date: If specified, all files before this date will be removed
            after_date:  If specified, all files after this date will be removed
            dry_run:     If dry_run is True, then the selected files are printed rather than removed

        Returns:
            The number of files selected for removal

        Example: Using this function
            Either the before_date or after_date must be specified. If both are passed, files between the two dates are
            selected for removal. Dates must either be a timezone naive Python datetime.datetime instance or the number
            of seconds since the unix epoch. For example to delete all the files modified in January 2021, either of the
            following can be used::

            using offset naive datetime objects

                cache.purge(after_date=datetime.datetime(2021, 1, 1), before_date=datetime.datetime(2021, 2, 1))

            using seconds since the unix epoch

                cache.purge(after_date=1609459200, before_date=1612137600)
        """
        if before_date is None and after_date is None:
            raise ValueError("Either before date or after date should be provided")

        if isinstance(before_date, datetime.datetime):
            before_date = utils.to_unix_epoch_time_secs(before_date)
        if isinstance(after_date, datetime.datetime):
            after_date = utils.to_unix_epoch_time_secs(after_date)

        if before_date and after_date and before_date < after_date:
            raise ValueError("Before date should be larger than after date")

        count = 0
        for cache_dir in self._cache_dirs():
            # _get_modified_time returns None if the cache map file doesn't
            # exist and n > None evaluates to True in python 2.7(wtf?). I'm guessing it's
            # OK to purge directories in the cache that have no .cacheMap file

            last_modified_time = _get_modified_time(
                os.path.join(cache_dir, self.cache_map_file_name)
            )
            if last_modified_time is None or (
                (not before_date or before_date > last_modified_time)
                and (not after_date or after_date < last_modified_time)
            ):
                if dry_run:
                    print(cache_dir)
                else:
                    shutil.rmtree(cache_dir)
                count += 1
        return count
