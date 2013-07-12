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

.. automethod:: synapseclient.cache.obtain_lock
.. automethod:: synapseclient.cache.release_lock
.. automethod:: synapseclient.cache.normalize_path
.. automethod:: synapseclient.cache.determine_cache_directory
.. automethod:: synapseclient.cache.determine_local_file_location

"""

import os, sys, re, json, time, errno, shutil
import synapseclient.utils as utils
from threading import Lock

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.synapseCache')
CACHE_FANOUT = 1000
CACHE_LOCK_TIME = 10
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


def local_file_has_changed(entityBundle, path=None):
    """
    Checks the local cache to see if the given file has been modified.
    
    :param entityBundle: A dictionary with 'fileHandles' and 'entity'.
                         Typically created via::

        syn._getEntityBundle()
        
    :param path:         Path to the local file.  May be in any format.  
                         If not given, the information from the 'entityBundle'
                         is used to derive a cached location for the file.  
    
    :returns: True if the file has been modified.
    
    """
    
    # Find the directory of the '.cacheMap' for the file
    cacheDir, filepath = determine_local_file_location(entityBundle)
    if path is None:
        path = filepath
        
    # External URLs will be ignored
    if utils.is_url(path):
        return True
        
    # Read the '.cacheMap'
    cache = obtain_lock(cacheDir)
    release_lock(cacheDir)
    
    # Compare the modification times
    path = normalize_path(path)
    fileMTime = time.mktime(time.gmtime(os.path.getmtime(path))) if os.path.exists(path) else None
    unmodifiedFileExists = False
    for file in cache.keys():
        cacheTime = read_cache_entry(cache[file])
        
        # When there is a direct match, return if it is modified
        if path == file and os.path.exists(path):
            return not fileMTime == cacheTime
            
        # If there is no direct match, but a pristine copy exists, return False (after checking all entries)
        if fileMTime == cacheTime and os.path.exists(file):
            unmodifiedFileExists = True
            
    # The file is not cached or has been changed
    return not unmodifiedFileExists
        
        
def add_local_file_to_cache(path, fileHandle):
    """
    Makes a '.cacheMap' entry in the cache.  
    
    :param path:       Path to the local file.  May be in any format.  
    :param fileHandle: An S3 file handle used to identify the file.
    
    """
        
    # External URLs will be ignored
    if utils.is_url(path):
        return True
    
    # Get the '.cacheMap'
    cacheDir = determine_cache_directory(fileHandle)
    path = normalize_path(path)
    cache = obtain_lock(cacheDir)
    
    # If the file to-be-added does not exist, search the cache for a pristine copy
    if not os.path.exists(path):
        for file in cache.keys():
            fileMTime = time.mktime(time.gmtime(os.path.getmtime(file)))
            cacheTime = read_cache_entry(cache[file])
            if fileMTime == cacheTime and os.path.exists(file):
                shutil.copyfile(file, path)
                break
                
    # Update the cache
    if os.path.exists(path):
        cache[path] = time.strftime(ISO_FORMAT, time.gmtime(os.path.getmtime(path)))
    release_lock(cacheDir, cache)
    

def remove_local_file_from_cache(path, fileHandle):
    raise NotImplementedError("Behavior or usage of this method has not been decided yet")
    
    
def retrieve_local_file_info(entityBundle, path=None):
    """
    Returns a JSON dictionary with 'path', 'files', and 'cacheDir'
    that can be used to update the local state of a FileEntity.
    """
    cacheDir, filepath = determine_local_file_location(entityBundle)
    if path is None:
        path = filepath
    
    return {
        'path': path,
        'files': [os.path.basename(path)],
        'cacheDir': os.path.dirname(path) }

def get_alternate_file_name(path):
    """Returns a non-colliding path by appending (%d) to the end of the file name."""
    
    base, ext = os.path.splitext(path)
    counter = 1
    while os.path.exists(path):
        path = base + ("(%d)" % counter) + ext
        counter += 1
        
    return path
    
    
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
            # Still locked...
            if err.errno != errno.EEXIST:
                raise err
        
        print "Waiting for cache to unlock"
        try:
            lockAge = time.time() - os.path.getmtime(cacheLock)
            
            # Sleep for a bit and check again
            if lockAge < CACHE_LOCK_TIME and lockAge > 0:
                time.sleep((CACHE_LOCK_TIME - lockAge) / CACHE_LOCK_TIME)
                continue
                
            # Lock expired, so delete and try to lock again
            release_lock(cacheDir)
        except OSError as err:
            # Something else deleted the lock first
            if err.errno != errno.ENOENT:
                raise err
        
    # Make sure the '.cacheMap' exists
    if not os.path.exists(cacheMap):
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

    
def determine_cache_directory(fileHandle):
    """Uses a file handle to determine the cache folder."""
    
    return os.path.join(CACHE_DIR, str(int(fileHandle) % CACHE_FANOUT), fileHandle)

    
def determine_local_file_location(entityBundle):
    """
    Uses information from an Entity bundle to derive the cache directory and cached file location
    
    :param entityBundle: A dictionary with 'fileHandles' and 'entity'.
                         Typically created via::

        syn._getEntityBundle()
    
    :returns: The cache directory, the file location
    
    """
    
    for handle in entityBundle['fileHandles']:
        if handle['id'] == entityBundle['entity']['dataFileHandleId']:
            cacheDir = determine_cache_directory(handle['id'])
            path = os.path.join(cacheDir, handle['fileName'])
            return cacheDir, path
                
    raise Exception("Invalid parameters: the entityBundle does not contain matching file handle IDs")
    

strptimeLock = Lock()
def read_cache_entry(isoTime):
    """
    Note: Due to the way Python parses time via strptime()
        it may randomly append an incorrect Daylight Savings Time 
        to the resulting time_struct, which must be corrected
    Note: The `strptime() method is not thread-safe <http://bugs.python.org/issue7980>`_
    """
    strptimeLock.acquire()
    cacheTime = time.strptime(isoTime, ISO_FORMAT)
    strptimeLock.release()
    return time.mktime(cacheTime) - 3600 * cacheTime.tm_isdst
    