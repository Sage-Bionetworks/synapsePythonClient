import os, tempfile
import time, calendar
import synapseclient.cache as cache


def setup():
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60

    
def test_lock_is_valid():
    # Lock should be valid right after creation
    cacheDir = tempfile.mkdtemp()
    cache.obtain_lock_and_read_cache(cacheDir)
    assert cache.is_lock_valid(os.path.join(cacheDir, '.lock'))
    cache.write_cache_then_release_lock(cacheDir)


def test_time_parsing():
    # Values derived from http://www.epochconverter.com/
    samples = {"1970-01-01T00:00:00.000Z": 0, 
               "1970-04-26T17:46:40.000Z": 10000000, 
               "2001-09-09T01:46:40.000Z": 1000000000, 
               "2286-11-20T17:46:40.000Z": 10000000000}
    for stamp in samples.keys():
        # print "Input = %s | Parsed = %d" % (samples[stamp], cache.parse_cache_entry_into_seconds(stamp))
        assert cache.parse_cache_entry_into_seconds(stamp) == samples[stamp]
    
    
def test_modification_time():
    # Non existent files return None
    assert cache.get_modification_time("A:/I/h0pe/th1s/k0mput3r/haz/n0/fl0ppy.disk") == None
    
    # File creation should result in a correct modification time
    _, path = tempfile.mkstemp()
    # print "Now = %f | File = %f" % (calendar.timegm(time.gmtime()), cache.get_modification_time(path))
    assert cache.get_modification_time(path) - calendar.timegm(time.gmtime()) < 0.01
    
    # Directory creation should result in a correct modification time
    path = tempfile.mkdtemp()
    # print "Now = %f | File = %f" % (calendar.timegm(time.gmtime()), cache.get_modification_time(path))
    assert cache.get_modification_time(path) - calendar.timegm(time.gmtime()) < 0.01