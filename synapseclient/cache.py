# Note: Even though this has Sphinx format, this is not meant to be part of the public docs

"""
************
File Caching
************

.. automethod:: synapseclient.cache.local_file_has_changed
.. automethod:: synapseclient.cache.add_local_file_to_cache

~~~~~~~
Helpers
~~~~~~~

.. automethod:: synapseclient.cache.obtain_lock
.. automethod:: synapseclient.cache.normalize_path
.. automethod:: synapseclient.cache.release_lock

"""

import os, sys, re, json, time, errno, shutil

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.synapseCache')
CACHE_LOCK_TIME = 10
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


def local_file_has_changed(path, entityBundle):
    """
    Checks the local cache to see if the given file has been modified.
    
    :param path:         Path to the local file.  May be in any format.  
    :param entityBundle: A dictionary with 'fileHandles' and 'entity'.
                         Typically created via::

        syn._getEntityBundle()
    
    """
    
    # Find the directory of the '.cacheMap' for the file
    cacheDir = None
    for handle in entityBundle['fileHandles']:
        if handle['id'] == entityBundle['entity']['dataFileHandleId']:
            cacheDir = os.path.join(CACHE_DIR, handle['id'])
            break
    
    if cacheDir is None:
        raise Exception("Invalid parameters: the entityBundle does not contain matching file handle IDs")
    
    cache = obtain_lock(cacheDir)
    release_lock(cacheDir)
    
    path = normalize_path(path)
    if path in cache and os.path.isfile(path):
        # Due to the way Python parses time via strptime()
        #   it may randomly append an incorrect Daylight Savings Time 
        #   to the resulting time_struct, which must be corrected
        cacheTime = time.strptime(cache[path], ISO_FORMAT)
        cacheTime = time.mktime(cacheTime) - 3600 * cacheTime.tm_isdst
        fileMTime = time.mktime(time.gmtime(os.path.getmtime(path)))
        return not fileMTime == cacheTime
            
    # The file is not cached or has been changed
    return True
        
        
def add_local_file_to_cache(path, fileHandle):
    """
    Makes a '.cacheMap' entry in the cache.  
    The 'path' should not collide with any existing entries.
    
    :param path:       Path to the local file.  May be in any format.  
    :param fileHandle: An S3 file handle used to identify the file.
    
    """
    
    cacheDir = os.path.join(CACHE_DIR, fileHandle)
    path = normalize_path(path)
    
    cache = obtain_lock(cacheDir)
    
    # Cached files should never be overwritten by the cacher
    if path in cache:
        raise Exception("Invalid parameters: %s is already in the cache" % path)
    
    cache[path] = time.strftime(ISO_FORMAT, time.gmtime(os.path.getmtime(path)))
        
    release_lock(cacheDir, cache)
    
    
def obtain_lock(cacheDir):
    """
    Blocks until a '.lock' folder can be made in the given directory.
    See `Cache Map Design <https://sagebionetworks.jira.com/wiki/pages/viewpage.action?pageId=34373660#CommonClientCommandsetandCache%28%22C4%22%29-CacheMapDesign>`_.
    
    Also creates the necessary directories for the cache.
    
    :returns: A dictionary with the JSON contents of the locked '.cacheMap'
    """
    
    cacheLock = os.path.join(cacheDir, '.lock')
    cacheMap = os.path.join(cacheDir, '.cacheMap')
    
    # Make and thereby obtain the '.lock'
    while True:
        try:
            os.makedirs(cacheLock)
            break
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise err # Something bad happened
        
        sys.stdout.write("Waiting for cache to unlock")
        while os.path.isdir(cacheLock):
            lockAge = time.time() - os.path.getmtime(cacheLock)
            
            # Sleep for a bit and check again
            if lockAge < CACHE_LOCK_TIME and lockAge > 0:
                sys.stdout.write(".")
                time.sleep((CACHE_LOCK_TIME - lockAge) / CACHE_LOCK_TIME)
                continue
                
            # Lock expired, so delete and try to lock again
            release_lock(cacheDir)
        sys.stdout.write("\n")
        
    # Make sure the '.cacheMap' exists
    if not os.path.isfile(cacheMap):
        empty = open(cacheMap, 'w')
        empty.write("{}")
        empty.close()
        
    # Read and parse the '.cacheMap'
    cacheMap = open(cacheMap, 'r')
    cache = json.load(cacheMap)
    cacheMap.close()
    
    return cache
    
    
def release_lock(cacheDir, cacheMapBody=None):
    """
    Removes the '.lock' folder in the given directory.
    
    :param cacheMapBody: JSON object to write in the '.cacheMap' before releasing the lock.
    """
    
    # Update the '.cacheMap'
    if cacheMapBody is not None:
        cacheMap = os.path.join(cacheDir, '.cacheMap')
        json.dump(cacheMapBody, open(cacheMap, 'w'))
        
    # Delete the '.lock'
    cacheLock = os.path.join(cacheDir, '.lock')
    try:
        os.rmdir(cacheLock)
    except OSError as err:
        if err.errno == errno.ENOTEMPTY:
            raise Exception("Invalid lock state: %s is not empty" % cacheLock)
        else: 
            raise err
    
    
def normalize_path(path):
    """Transforms a path into an absolute path with forward slashes only."""
    
    return re.sub(r'\\', '/', os.path.abspath(path))