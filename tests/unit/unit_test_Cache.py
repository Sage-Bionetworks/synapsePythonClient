import re, os, tempfile, json
import time, datetime, random
from mock import MagicMock, patch
from nose.tools import assert_raises
from collections import OrderedDict
from multiprocessing import Process

import synapseclient
import synapseclient.cache as cache
import synapseclient.utils as utils


def setup():
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60


## delete a file

## modify a file


## ---- fixed below here ------

def test_cache_concurrent_access():

    def add_file_to_cache(i, cache_root_dir):
        # print ("Starting process %d" % i)
        my_cache = cache.Cache(cache_root_dir=cache_root_dir)
        file_handle_ids = [1001, 1002, 1003, 1004, 1005]
        random.shuffle(file_handle_ids)
        for file_handle_id in file_handle_ids:
            cache_dir = my_cache.get_cache_dir(file_handle_id)
            file_path = os.path.join(cache_dir, "file_handle_%d_process_%02d.junk" % (file_handle_id, i))
            utils.touch(file_path)
            my_cache.add(file_handle_id, file_path)
        # print ("Completed process %d" % i)

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
        for path, iso_time in cache_map.iteritems():
            m = re.match("file_handle_%d_process_(\d+).junk" % file_handle_id, os.path.basename(path))
            if m:
                process_ids.add(int(m.group(1)))
        assert process_ids == set(range(20)), process_ids


def test_get_cache_dir():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)
    cache_dir = my_cache.get_cache_dir(1234567)
    assert cache_dir.startswith(tmp_dir)
    assert cache_dir.endswith("1234567")


def test_parse_cache_entry_into_seconds():
    # Values derived from http://www.epochconverter.com/
    timestamps = OrderedDict()
    timestamps["1970-01-01T00:00:00.000Z"] = 0
    timestamps["1970-04-26T17:46:40.375Z"] = 10000000.375
    timestamps["2001-09-09T01:46:40.000Z"] = 1000000000
    timestamps["2286-11-20T17:46:40.375Z"] = 10000000000.375
    print "\n\n"
    for stamp in timestamps.keys():
        print "Input = %s | Parsed = %f" % (stamp, cache.iso_time_to_epoch(stamp))
        assert cache.iso_time_to_epoch(stamp) == timestamps[stamp]
        assert cache.epoch_time_to_iso(cache.iso_time_to_epoch(stamp)) == stamp


def test_get_modification_time():
    ALLOWABLE_TIME_ERROR = 0.01 # seconds

    # Non existent files return None
    assert cache._get_modified_time("A:/I/h0pe/th1s/k0mput3r/haz/n0/fl0ppy.disk") == None

    # File creation should result in a correct modification time
    _, path = tempfile.mkstemp()
    # print "Now = %f | File = %f" % (time.gmtime(), cache.get_modification_time(path))
    assert cache._get_modified_time(path) - time.time() < ALLOWABLE_TIME_ERROR

    # Directory creation should result in a correct modification time
    path = tempfile.mkdtemp()
    # print "Now = %f | File = %f" % (calendar.timegm(time.gmtime()), cache.get_modification_time(path))
    assert cache._get_modified_time(path) - time.time() < ALLOWABLE_TIME_ERROR


def test_cache_store_get():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    path1 = utils.touch(os.path.join(my_cache.get_cache_dir(101201), "file1.ext"))
    my_cache.add(file_handle_id=101201, path=path1)

    path2 = utils.touch(os.path.join(my_cache.get_cache_dir(101202), "file2.ext"))
    my_cache.add(file_handle_id=101202, path=path2)

    ## set path3's mtime to be later than path2's
    new_time_stamp = cache._get_modified_time(path2)+2

    path3 = utils.touch(os.path.join(tmp_dir, "foo", "file2.ext"), (new_time_stamp, new_time_stamp))
    my_cache.add(file_handle_id=101202, path=path3)

    a_file = my_cache.get(file_handle_id=101201)
    assert a_file == path1

    a_file = my_cache.get(file_handle_id=101201, path=path1)
    assert a_file == path1

    a_file = my_cache.get(file_handle_id=101201, path=my_cache.get_cache_dir(101201))
    assert a_file == path1

    b_file = my_cache.get(file_handle_id=101202, path=os.path.dirname(path2))
    assert b_file == path2

    b_file = my_cache.get(file_handle_id=101202, path=os.path.dirname(path3))
    assert b_file == path3

    not_in_cache_file = my_cache.get(file_handle_id=101203, path=tmp_dir)
    assert not_in_cache_file is None

    wrong_name_file = my_cache.get(file_handle_id=101201, path=os.path.join(my_cache.get_cache_dir(101202), "wrong_file1.ext"))
    assert wrong_name_file is None

    wrong_location_file = my_cache.get(file_handle_id=101202, path=os.path.join(tmp_dir, "wrong"))
    assert wrong_location_file is None


def test_cache_modified_time():
    tmp_dir = tempfile.mkdtemp()
    my_cache = cache.Cache(cache_root_dir=tmp_dir)

    path1 = utils.touch(os.path.join(my_cache.get_cache_dir(101201), "file1.ext"))
    my_cache.add(file_handle_id=101201, path=path1)

    new_time_stamp = cache._get_modified_time(path1)+1
    utils.touch(path1, (new_time_stamp, new_time_stamp))
    a_file = my_cache.get(file_handle_id=101201, path=path1)
    assert a_file is None


