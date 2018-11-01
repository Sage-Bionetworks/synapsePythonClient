from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import six

import re
import os
import tempfile
import time
import random
from mock import patch
from nose.tools import assert_equal, assert_is_none, assert_is_not_none, assert_in, assert_false, assert_true,\
    assert_less
from collections import OrderedDict
from multiprocessing import Process

import synapseclient.cache as cache
import synapseclient.utils as utils


def add_file_to_cache(i, cache_root_dir):
    """
    Helper function for use in test_cache_concurrent_access
    """
    my_cache = cache.Cache(cache_root_dir=cache_root_dir)
    file_handle_ids = [1001, 1002, 1003, 1004, 1005]
    random.shuffle(file_handle_ids)
    for file_handle_id in file_handle_ids:
        cache_dir = my_cache.get_cache_dir(file_handle_id)
        file_path = os.path.join(cache_dir, "file_handle_%d_process_%02d.junk" % (file_handle_id, i))
        utils.touch(file_path)
        my_cache.add(file_handle_id, file_path)


def test_cache_concurrent_access():
    cache_root_dir = tempfile.mkdtemp()
    processes = [Process(target=add_file_to_cache, args=(i, cache_root_dir)) for i in range(20)]

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    my_cache = cache.Cache(cache_root_dir=cache_root_dir)
    file_handle_ids = [1001, 1002, 1003, 1004, 1005]
    for file_handle_id in file_handle_ids:
        cache_map = my_cache._read_cache_map(my_cache.get_cache_dir(file_handle_id))
        process_ids = set()
        for path, iso_time in six.iteritems(cache_map):
            m = re.match("file_handle_%d_process_(\d+).junk" % file_handle_id, os.path.basename(path))
            if m:
                process_ids.add(int(m.group(1)))
        assert_equal(process_ids, set(range(20)))


def test_get_cache_dir():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)
    cache_dir = my_cache.get_cache_dir(1234567)
    assert_true(cache_dir.startswith(tmp_dir))
    assert_true(cache_dir.endswith("1234567"))


def test_parse_cache_entry_into_seconds():
    # Values derived from http://www.epochconverter.com/
    timestamps = OrderedDict()
    timestamps["1970-01-01T00:00:00.000Z"] = 0
    timestamps["1970-04-26T17:46:40.375Z"] = 10000000.375
    timestamps["2001-09-09T01:46:40.000Z"] = 1000000000
    timestamps["2286-11-20T17:46:40.375Z"] = 10000000000.375
    timestamps["2286-11-20T17:46:40.999Z"] = 10000000000.999
    for stamp in timestamps.keys():
        assert_equal(cache.iso_time_to_epoch(stamp), timestamps[stamp])
        assert_equal(cache.epoch_time_to_iso(cache.iso_time_to_epoch(stamp)), stamp)


def test_get_modification_time():
    ALLOWABLE_TIME_ERROR = 0.01  # seconds

    # Non existent files return None
    assert_is_none(cache._get_modified_time("A:/I/h0pe/th1s/k0mput3r/haz/n0/fl0ppy.disk"))

    # File creation should result in a correct modification time
    _, path = tempfile.mkstemp()
    assert_less(cache._get_modified_time(path) - time.time(), ALLOWABLE_TIME_ERROR)

    # Directory creation should result in a correct modification time
    path = tempfile.mkdtemp()
    assert_less(cache._get_modified_time(path) - time.time(), ALLOWABLE_TIME_ERROR)


def test_cache_timestamps():
    # test conversion to epoch time to ISO with proper rounding to millisecond
    assert_equal(cache.epoch_time_to_iso(1433544108.080841), '2015-06-05T22:41:48.081Z')


def test_compare_timestamps():
    assert not cache.compare_timestamps(10000000.375, None)
    assert not cache.compare_timestamps(None, "1970-04-26T17:46:40.375Z")
    assert cache.compare_timestamps(10000000.375, "1970-04-26T17:46:40.375Z")
    assert cache.compare_timestamps(10000000.375, "1970-04-26T17:46:40.000Z")
    assert cache.compare_timestamps(1430861695.001111, "2015-05-05T21:34:55.001Z")
    assert cache.compare_timestamps(1430861695.001111, "2015-05-05T21:34:55.000Z")
    assert cache.compare_timestamps(1430861695.999999, "2015-05-05T21:34:55.000Z")


def test_subsecond_timestamps():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    path = utils.touch(os.path.join(tmp_dir, "some_crazy_location", "file1.ext"))

    my_cache.add(file_handle_id=1234, path=path)

    with patch("synapseclient.cache._get_modified_time") as _get_modified_time_mock, \
         patch("synapseclient.cache.Cache._read_cache_map") as _read_cache_map_mock:

        # this should be a match, 'cause we round microseconds to milliseconds
        _read_cache_map_mock.return_value = {path: "2015-05-05T21:34:55.001Z"}
        _get_modified_time_mock.return_value = 1430861695.001111

        assert_equal(path, my_cache.get(file_handle_id=1234, path=path))

        # The R client always writes .000 for milliseconds, for compatibility,
        # we should match .000 with any number of milliseconds
        _read_cache_map_mock.return_value = {path: "2015-05-05T21:34:55.000Z"}

        assert_equal(path, my_cache.get(file_handle_id=1234, path=path))


def test_cache_store_get():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    path1 = utils.touch(os.path.join(my_cache.get_cache_dir(101201), "file1.ext"))
    my_cache.add(file_handle_id=101201, path=path1)

    path2 = utils.touch(os.path.join(my_cache.get_cache_dir(101202), "file2.ext"))
    my_cache.add(file_handle_id=101202, path=path2)

    # set path3's mtime to be later than path2's
    new_time_stamp = cache._get_modified_time(path2)+2

    path3 = utils.touch(os.path.join(tmp_dir, "foo", "file2.ext"), (new_time_stamp, new_time_stamp))
    my_cache.add(file_handle_id=101202, path=path3)

    a_file = my_cache.get(file_handle_id=101201)
    assert utils.equal_paths(a_file, path1)

    a_file = my_cache.get(file_handle_id=101201, path=path1)
    assert utils.equal_paths(a_file, path1)

    a_file = my_cache.get(file_handle_id=101201, path=my_cache.get_cache_dir(101201))
    assert utils.equal_paths(a_file, path1)

    b_file = my_cache.get(file_handle_id=101202, path=os.path.dirname(path2))
    assert utils.equal_paths(b_file, path2)

    b_file = my_cache.get(file_handle_id=101202, path=os.path.dirname(path3))
    assert utils.equal_paths(b_file, path3)

    not_in_cache_file = my_cache.get(file_handle_id=101203, path=tmp_dir)
    assert_is_none(not_in_cache_file)

    removed = my_cache.remove(file_handle_id=101201, path=path1, delete=True)
    assert utils.normalize_path(path1) in removed
    assert len(removed) == 1
    assert_is_none(my_cache.get(file_handle_id=101201))

    removed = my_cache.remove(file_handle_id=101202, path=path3, delete=True)
    b_file = my_cache.get(file_handle_id=101202)
    assert utils.normalize_path(path3) in removed
    assert len(removed) == 1
    assert utils.equal_paths(b_file, path2)

    removed = my_cache.remove(file_handle_id=101202, delete=True)
    assert utils.normalize_path(path2) in removed
    assert len(removed) == 1
    assert_is_none(my_cache.get(file_handle_id=101202))


def test_cache_modified_time():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    path1 = utils.touch(os.path.join(my_cache.get_cache_dir(101201), "file1.ext"))
    my_cache.add(file_handle_id=101201, path=path1)

    new_time_stamp = cache._get_modified_time(path1)+1
    utils.touch(path1, (new_time_stamp, new_time_stamp))

    a_file = my_cache.get(file_handle_id=101201, path=path1)
    assert_is_none(a_file)


def test_cache_remove():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    path1 = utils.touch(os.path.join(my_cache.get_cache_dir(101201), "file1.ext"))
    my_cache.add(file_handle_id=101201, path=path1)

    alt_dir = tempfile.mkdtemp()
    path2 = utils.touch(os.path.join(alt_dir, "file2.ext"))
    my_cache.add(file_handle_id=101201, path=path2)

    # remove the cached copy at path1
    rp = my_cache.remove({'dataFileHandleId': 101201, 'path': path1})

    assert len(rp) == 1
    assert utils.equal_paths(rp[0], path1)
    assert utils.equal_paths(my_cache.get(101201), path2)

    my_cache.remove(101201)
    assert_is_none(my_cache.get(101201))


def test_cache_rules():
    # Cache should (in order of preference):
    #
    # 1. DownloadLocation specified:
    #   a. return exact match (unmodified file at the same path)
    #   b. return an unmodified file at another location,
    #      copy to downloadLocation subject to ifcollision
    #   c. download file to downloadLocation subject to ifcollision
    #
    # 2. DownloadLocation *not* specified:
    #   a. return an unmodified file at another location
    #   b. download file to cache_dir overwritting any existing file
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    # put file in cache dir
    path1 = utils.touch(os.path.join(my_cache.get_cache_dir(101201), "file1.ext"))
    my_cache.add(file_handle_id=101201, path=path1)

    new_time_stamp = cache._get_modified_time(path1)+1
    path2 = utils.touch(os.path.join(tmp_dir, "not_in_cache", "file1.ext"), (new_time_stamp, new_time_stamp))
    my_cache.add(file_handle_id=101201, path=path2)

    new_time_stamp = cache._get_modified_time(path2)+1
    path3 = utils.touch(os.path.join(tmp_dir, "also_not_in_cache", "file1.ext"), (new_time_stamp, new_time_stamp))
    my_cache.add(file_handle_id=101201, path=path3)

    # DownloadLocation specified, found exact match
    assert utils.equal_paths(my_cache.get(file_handle_id=101201, path=path2), path2)

    # DownloadLocation specified, no match, get most recent
    path = my_cache.get(file_handle_id=101201, path=os.path.join(tmp_dir, "file_is_not_here", "file1.ext"))
    assert utils.equal_paths(path, path3)

    # DownloadLocation specified as a directory, not in cache, get most recent
    empty_dir = os.path.join(tmp_dir, "empty_directory")
    os.makedirs(empty_dir)
    path = my_cache.get(file_handle_id=101201, path=empty_dir)
    assert utils.equal_paths(path, path3)

    # path2 is now modified
    new_time_stamp = cache._get_modified_time(path2)+2
    utils.touch(path2, (new_time_stamp, new_time_stamp))

    # test cache.contains
    assert_false(my_cache.contains(file_handle_id=101201, path=empty_dir))
    assert_false(my_cache.contains(file_handle_id=101201, path=path2))
    assert_false(my_cache.contains(file_handle_id=101999, path=path2))
    assert_true(my_cache.contains(file_handle_id=101201, path=path1))
    assert_true(my_cache.contains(file_handle_id=101201, path=path3))

    # Get file from alternate location. Do we care which file we get?
    assert_is_none(my_cache.get(file_handle_id=101201, path=path2))
    assert_in(my_cache.get(file_handle_id=101201), [utils.normalize_path(path1), utils.normalize_path(path3)])

    # Download uncached file to a specified download location
    assert_is_none(my_cache.get(file_handle_id=101202, path=os.path.join(tmp_dir, "not_in_cache")))

    # No downloadLocation specified, get file from alternate location. Do we care which file we get?
    assert_is_not_none(my_cache.get(file_handle_id=101201))
    assert_in(my_cache.get(file_handle_id=101201), [utils.normalize_path(path1), utils.normalize_path(path3)])

    # test case 2b.
    assert_is_none(my_cache.get(file_handle_id=101202))


def test_set_cache_root_dir():
    # set up an environment variable for the path
    enviornment_variable_name = "_SYNAPSE_PYTHON_CLIENT_TEST_ENV"
    enviornment_variable_value = "asdf/qwerty"
    os.environ[enviornment_variable_name] = enviornment_variable_value

    path_suffix = "GrayFaceNoSpace"

    expanded_path = os.path.expandvars(os.path.expanduser(os.path.join("~", enviornment_variable_value, path_suffix)))
    non_expanded_path = os.path.join("~", '$' + enviornment_variable_name, path_suffix)

    # test that the constructor correctly expands the path
    my_cache = cache.Cache(cache_root_dir=non_expanded_path)
    assert_equal(expanded_path, my_cache.cache_root_dir)

    # test that manually assigning cache_root_dir expands the path
    my_cache.cache_root_dir = non_expanded_path + "2"
    assert_equal(expanded_path + "2", my_cache.cache_root_dir)
