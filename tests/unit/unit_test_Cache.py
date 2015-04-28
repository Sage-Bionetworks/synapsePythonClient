import re, os, tempfile, json
import time, calendar
from mock import MagicMock, patch
from nose.tools import assert_raises

import synapseclient
import synapseclient.cache as cache


def setup():
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60


@patch('time.time')
@patch('synapseclient.cache.is_lock_valid')
def test_obtain_lock_and_read_cache(lock_valid_mock, time_mock):
    cacheDir = tempfile.mkdtemp()
    cacheLock = os.path.join(cacheDir, '.cacheMap.lock')
    os.makedirs(cacheLock)
    oldUnlockWaitTime = cache.CACHE_UNLOCK_WAIT_TIME

    try:
        cache.CACHE_UNLOCK_WAIT_TIME = 0

        # -- Make sure the method retries appropriately --
        # Have the method retry locking four times, succeeding on the last one
        waitStack = [False, True, True, True]
        time_mock.side_effect = lambda: 0
        lock_valid_mock.side_effect = lambda x: waitStack.pop()
        assert cache.obtain_lock_and_read_cache(cacheDir) == {}
        assert len(waitStack) == 0

        # -- Make sure the method times out --
        # time.time is called two times within the loop and once outside the loop
        timeStack = [cache.CACHE_MAX_LOCK_TRY_TIME, cache.CACHE_MAX_LOCK_TRY_TIME, 0]
        time_mock.side_effect = lambda: timeStack.pop()

        assert_raises(Exception, cache.obtain_lock_and_read_cache, cacheDir)
        assert len(timeStack) == 0
    finally:
        cache.CACHE_UNLOCK_WAIT_TIME = oldUnlockWaitTime


@patch('synapseclient.cache.obtain_lock_and_read_cache')
@patch('synapseclient.cache.is_lock_valid')
def test_write_cache_then_release_lock(lock_valid_mock, read_mock):
    cacheDir = tempfile.mkdtemp()
    cacheLock = os.path.join(cacheDir, '.cacheMap.lock')
    # -- Make sure the .lock is removed along with any junk inside --
    os.makedirs(os.path.join(cacheLock, 'random folder'))
    os.makedirs(os.path.join(cacheLock, 'OtherExtraneousFolder'))
    os.makedirs(os.path.join(cacheLock, 'more_stuff_that_should_not_be_here'))
    f, _ = tempfile.mkstemp(dir=cacheLock)
    os.close(f)

    cache.write_cache_then_release_lock(cacheDir)
    assert not os.path.exists(cacheLock)
    assert not lock_valid_mock.called

    # -- Make sure the .cacheMap is written to correctly --
    # Pretend the lock has expired
    lock_valid_mock.return_value = False

    # Pretend the .cacheMap contains some JSON
    read_mock.return_value = {"a": "b", "c": "d", "overwrite": "me"}

    # Make the lock and remove it
    os.makedirs(cacheLock)
    cache.write_cache_then_release_lock(cacheDir, {"overwrite": "I've lost my identity"})
    assert not os.path.exists(cacheLock)

    lock_valid_mock.assert_called_once_with(cacheLock)
    with open(os.path.join(cacheDir, '.cacheMap'), 'r') as f:
        written = json.load(f)
        assert written == {"a": "b", "c": "d", "overwrite": "I've lost my identity"}


@patch('synapseclient.cache.get_modification_time')
@patch('os.path.exists')
@patch('synapseclient.cache.write_cache_then_release_lock') # Ignore the fact that the .lock is not made
@patch('synapseclient.cache.obtain_lock_and_read_cache')
def test_iterator_over_cache_map(*mocks):
    mocks = [mock for mock in mocks]
    mod_time_mock = mocks.pop()
    exist_mock    = mocks.pop()
    write_mock    = mocks.pop()
    read_mock     = mocks.pop()

    # Replace the CacheMap with a dictionary with timestamps (0, 1, 2, 3)
    ret_dict = {"0": "1970-01-01T00:00:00.000Z",
                "1": "1970-01-01T00:00:01.000Z",
                "2": "1970-01-01T00:00:02.000Z",
                "3": "1970-01-01T00:00:03.000Z"}
    dict_mock = MagicMock()
    dict_mock.keys.side_effect = lambda: sorted(ret_dict.keys())
    dict_mock.__getitem__.side_effect = ret_dict.__getitem__
    read_mock.return_value = dict_mock

    # No files are made, so return some bogus modification time
    mod_time_mock.return_value = 1337

    # The iterator should return as long as the file exists
    exist_mock.return_value = True

    iter = cache.iterator_over_cache_map(None) # Helper methods are mocked out
    file, cacheTime, fileMTime = iter.next()
    assert file == "0"
    assert cacheTime == 0
    assert fileMTime == 1337

    _, cacheTime, _ = iter.next()
    assert cacheTime == 1

    _, cacheTime, _ = iter.next()
    assert cacheTime == 2

    # Make sure the iterator ignores non-existent files
    exist_mock.return_value = False
    assert_raises(StopIteration, iter.next)

    # Lock should only ever be gotten once
    read_mock.assert_called_once_with(None)
    write_mock.assert_called_once_with(None)


def test_is_lock_valid():
    # Lock should be valid right after creation
    cacheDir = tempfile.mkdtemp()
    cache.obtain_lock_and_read_cache(cacheDir)
    assert cache.is_lock_valid(os.path.join(cacheDir, '.cacheMap.lock'))
    cache.write_cache_then_release_lock(cacheDir)


def test_determine_cache_directory():
    oldCacheDir = cache.CACHE_DIR
    try:
        cache.CACHE_DIR = '/'
        entityInfo = {'id'              : 'foo',
                      'versionNumber'   : 'bar',
                      'dataFileHandleId': '1337'}

        res = cache.determine_cache_directory(entityInfo)
        assert re.sub(r'\\', '/', res) == '/337/1337'
    finally:
        cache.CACHE_DIR = oldCacheDir


def test_parse_cache_entry_into_seconds():
    # Values derived from http://www.epochconverter.com/
    samples = {"1970-01-01T00:00:00.000Z": 0,
               "1970-04-26T17:46:40.000Z": 10000000,
               "2001-09-09T01:46:40.000Z": 1000000000,
               "2286-11-20T17:46:40.000Z": 10000000000}
    for stamp in samples.keys():
        # print "Input = %s | Parsed = %d" % (samples[stamp], cache.parse_cache_entry_into_seconds(stamp))
        assert cache.parse_cache_entry_into_seconds(stamp) == samples[stamp]


def test_get_modification_time():
    ALLOWABLE_TIME_ERROR = 0.01 # seconds

    # Non existent files return None
    assert cache.get_modification_time("A:/I/h0pe/th1s/k0mput3r/haz/n0/fl0ppy.disk") == None

    # File creation should result in a correct modification time
    _, path = tempfile.mkstemp()
    # print "Now = %f | File = %f" % (calendar.timegm(time.gmtime()), cache.get_modification_time(path))
    assert cache.get_modification_time(path) - calendar.timegm(time.gmtime()) < ALLOWABLE_TIME_ERROR

    # Directory creation should result in a correct modification time
    path = tempfile.mkdtemp()
    # print "Now = %f | File = %f" % (calendar.timegm(time.gmtime()), cache.get_modification_time(path))
    assert cache.get_modification_time(path) - calendar.timegm(time.gmtime()) < ALLOWABLE_TIME_ERROR
