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
.. automethod:: synapseclient.cache.normalize_path
.. automethod:: synapseclient.cache.determine_cache_directory
.. automethod:: synapseclient.cache.determine_local_file_location
.. automethod:: synapseclient.cache.parse_cache_entry_into_seconds
.. automethod:: synapseclient.cache.get_modification_time

"""

import os, sys, re
import time, calendar
import errno, shutil
import json, urlparse
import synapseclient.utils as utils
from synapseclient.entity import is_locationable
from synapseclient.exceptions import *
from threading import Lock

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.synapseCache')
CACHE_FANOUT = 1000
CACHE_MAX_LOCK_TRY_TIME = 70
CACHE_LOCK_TIME = 60
CACHE_UNLOCK_WAIT_TIME = 0.5


def local_file_has_changed(entityBundle, checkIndirect, path=None):
    """
    Checks the local cache to see if the given file has been modified.
    
    :param entityBundle : A dictionary with 'fileHandles' and 'entity'.
                          Typically created via::

        syn._getEntityBundle()
        
    :param checkIndirect: Whether or not the cache should be checked for unmodified files.
                          Should be True when getting, False when storing.  
    :param path:          Path to the local file.  May be in any format.  
                          If not given, the information from the 'entityBundle'
                          is used to derive a cached location for the file.  
    
    :returns: True if the file has been modified.
    
    """
    
    # Find the directory of the '.cacheMap' for the file
    cacheDir, filepath, _ = determine_local_file_location(entityBundle)
    if path is None:
        path = filepath
        
    # If there is no file path, there is nothing to download
    if path is None:
        return False
        
    # External URLs will be ignored
    if utils.is_url(path):
        return True
    
    # Compare the modification times
    path = normalize_path(path)
    fileMTime = get_modification_time(path)
    unmodifiedFileExists = False
    for file, cacheTime, cachedFileMTime in iterator_over_cache_map(cacheDir):
        # When there is a direct match, return if it is modified
        if path == file and os.path.exists(path):
            return not fileMTime == cacheTime
            
        # If there is no direct match, but a pristine copy exists, return False (after checking all entries)
        if checkIndirect and cachedFileMTime == cacheTime:
            unmodifiedFileExists = True
            
    # The file is not cached or has been changed
    return not unmodifiedFileExists
        
        
def add_local_file_to_cache(**entity):
    """
    Makes a '.cacheMap' entry in the cache.  
    
    :param entity: A Synapse Entity object or dictionary with a 'path'.
                   FileEntities require a 'dataFileHandleID'.
                   Locationables require a 'id' and 'versionNumber'.
                   
    Example::
        
        foo = File('/path/to/file/xyz.txt')
        cache.add_local_file_to_cache(bar="Something to include in dict", **foo)
        
    Note: Since neither FileEntities nor Locationables have a 'path' in their properties,
          calls to this method should look like::
          
        cache.add_local_file_to_cache(path=entity['path'], **entity)
    """
        
    # External URLs will be ignored
    if utils.is_url(entity['path']):
        return True
    
    # Get the '.cacheMap'
    cacheDir = determine_cache_directory(entity)
    entity['path'] = normalize_path(entity['path'])
    
    # If the file to-be-added does not exist, search the cache for a pristine copy
    if not os.path.exists(entity['path']):
        for file, cacheTime, fileMTime in iterator_over_cache_map(cacheDir):
            if fileMTime == cacheTime:
                shutil.copyfile(file, entity['path'])
                break
                
    # Update the cache
    if os.path.exists(entity['path']):
        cache = obtain_lock_and_read_cache(cacheDir)
        cache[entity['path']] = time.strftime(utils.ISO_FORMAT, time.gmtime(os.path.getmtime(entity['path'])))
        write_cache_then_release_lock(cacheDir, cache)
    

def remove_local_file_from_cache(path, fileHandle):
    """TODO_Sphinx"""
    
    raise NotImplementedError("Behavior or usage of this method has not been decided yet")
    
    
def retrieve_local_file_info(entityBundle, path=None):
    """
    Returns a JSON dictionary with 'path', 'files', and 'cacheDir'
    that can be used to update the local state of a FileEntity.
    """
    cacheDir, filepath, unmodifiedFile = determine_local_file_location(entityBundle)        
    if path is None:
    
        # When an unmodified file exists while the default cached file does not, use the unmodified file
        if file is not None and unmodifiedFile is not None \
                and not os.path.exists(filepath) and os.path.exists(unmodifiedFile):
            path = unmodifiedFile
        else:
            path = filepath
        
    # No file info to retrieve
    if path is None:
        return {}
    
    return {
        'path': path,
        'files': [os.path.basename(path)],
        'cacheDir': os.path.dirname(path) }

    
def determine_local_file_location(entityBundle):
    """
    Uses information from an Entity bundle to derive the cache directory and cached file location.
    Also returns the first unmodified file in the cache (or None)
    
    :param entityBundle: A dictionary with 'fileHandles' and 'entity'.
                         Typically created via::

        syn._getEntityBundle()
    
    :returns: A 3-tuple (cache directory, default file location, first pristine cached file location)
              The file locations may be None if there is no file associated with the Entity or cache
    """
    
    cacheDir = determine_cache_directory(entityBundle['entity'])
    
    # Find the first unmodified file if any
    unmodifiedFile = None
    for file, cacheTime, fileMTime in iterator_over_cache_map(cacheDir):
        if fileMTime == cacheTime:
            unmodifiedFile = file
            break
    
    # Generate and return the default location of the cached file
    if is_locationable(entityBundle['entity']):
        if 'locations' not in entityBundle['entity']:
            # This Locationable does not have an associated file
            return cacheDir, None, unmodifiedFile
            
        url = entityBundle['entity']['locations'][0]['path']
        filename = urlparse.urlparse(url).path.split('/')[-1]
        path = os.path.join(cacheDir, filename)
        return cacheDir, path, unmodifiedFile
        
    else:
        for handle in entityBundle['fileHandles']:
            if handle['id'] == entityBundle['entity']['dataFileHandleId']:
                path = os.path.join(cacheDir, handle['fileName'])
                return cacheDir, path, unmodifiedFile
                    
        raise SynapseMalformedEntityError("Invalid parameters: the entityBundle does not contain matching file handle IDs")
    

def get_alternate_file_name(path):
    """Returns a non-colliding path by appending (%d) to the end of the file name."""
    
    base, ext = os.path.splitext(path)
    counter = 1
    while os.path.exists(path):
        path = base + ("(%d)" % counter) + ext
        counter += 1
        
    return path
    
######################
## Helper functions ##
######################
    
def obtain_lock_and_read_cache(cacheDir):
    """
    Blocks until a '.lock' folder can be made in the given directory.
    See `Cache Map Design <https://sagebionetworks.jira.com/wiki/pages/viewpage.action?pageId=34373660#CommonClientCommandsetandCache%28%22C4%22%29-CacheMapDesign>`_.
    
    Also creates the necessary directories for the cache.
    
    :returns: A dictionary with the JSON contents of the locked '.cacheMap'
    """
    
    cacheLock = os.path.join(cacheDir, '.lock')
    cacheMap = os.path.join(cacheDir, '.cacheMap')
    
    # Make and thereby obtain the '.lock'
    tryLockStartTime = time.time()
    while time.time() - tryLockStartTime < CACHE_MAX_LOCK_TRY_TIME:
        try:
            os.makedirs(cacheLock)
            
            # Make sure the modification times are correct
            # On some machines, the modification time could be seconds off
            os.utime(cacheLock, (0, time.time()))
            break
        except OSError as err:
            # Still locked...
            if err.errno != errno.EEXIST:
                raise
        
        print "Waiting for cache to unlock"
        if is_lock_valid(cacheLock):
            time.sleep(CACHE_UNLOCK_WAIT_TIME)
            continue
                
        # Lock expired, so delete and try to lock again (in the next loop)
        write_cache_then_release_lock(cacheDir)
    
    # Did it time out?
    if time.time() - tryLockStartTime >= CACHE_MAX_LOCK_TRY_TIME:
        raise SynapseFileCacheError("Could not obtain a lock on the file cache within %d seconds.  Please try again later" % CACHE_MAX_LOCK_TRY_TIME)
        
    # Make sure the '.cacheMap' exists, otherwise just return a blank dictionary
    if not os.path.exists(cacheMap):
        return {}
        
    # Read and parse the '.cacheMap'
    cacheMap = open(cacheMap, 'r')
    cache = json.load(cacheMap)
    cacheMap.close()
    
    return cache
    
    
def write_cache_then_release_lock(cacheDir, cacheMapBody=None):
    """
    Removes the '.lock' folder in the given directory.
    
    :param cacheMapBody: JSON object to write in the '.cacheMap' before releasing the lock.
    """
    
    cacheLock = os.path.join(cacheDir, '.lock')
    
    # Update the '.cacheMap'
    if cacheMapBody is not None:
        # Make sure the lock is still valid
        if not is_lock_valid(cacheLock):
            print "Lock has expired, reaquiring..."
            relockedCacheMap = obtain_lock_and_read_cache(cacheDir)
            # We assume that the rest of this operation can be completed within CACHE_LOCK_TIME seconds
            relockedCacheMap.update(cacheMapBody)
            cacheMapBody = relockedCacheMap
        
        cacheMap = os.path.join(cacheDir, '.cacheMap')
        with open(cacheMap, 'w') as f:
            json.dump(cacheMapBody, f)
        
    # Delete the '.lock' (and anything that might have been put into it)
    try:
        shutil.rmtree(cacheLock)
    except OSError as err:
        if err.errno != errno.ENOENT:
            raise
            
            
def iterator_over_cache_map(cacheDir):
    """
    Returns an iterator over the paths, timestamps, and modified times of the cache map.
    Values are only returned for paths that exist.
    """
    
    # Read the '.cacheMap'
    cache = obtain_lock_and_read_cache(cacheDir)
    write_cache_then_release_lock(cacheDir)
    
    for file in cache.keys():
        cacheTime = parse_cache_entry_into_seconds(cache[file])
        if os.path.exists(file):
            fileMTime = get_modification_time(file)
            yield file, cacheTime, fileMTime


def is_lock_valid(cacheLock):
    """Returns True if the lock has not expired yet."""
    
    try:
        # The lock may sometimes have a slightly negative age (> -1 ms)
        lockAge = time.time() - os.path.getmtime(cacheLock)
        return abs(lockAge) < CACHE_LOCK_TIME
    except OSError as err:
        if err.errno == errno.ENOENT:
            # Something else deleted the lock first, so lock is not valid
            return False
        raise
    
    
def normalize_path(path):
    """Transforms a path into an absolute path with forward slashes only."""
    
    return re.sub(r'\\', '/', os.path.abspath(path))

    
def determine_cache_directory(entity):
    """Uses the properties of the Entity to determine where it would be cached by default."""
    
    if is_locationable(entity):
        return os.path.join(CACHE_DIR, entity['id'], str(entity['versionNumber']))
        
    fileHandle = entity['dataFileHandleId']
    return os.path.join(CACHE_DIR, str(int(fileHandle) % CACHE_FANOUT), fileHandle)
        
        
strptimeLock = Lock()
def parse_cache_entry_into_seconds(isoTime):
    """
    Returns the number of seconds from the UNIX epoch.
    See :py:attribute:`synapseclient.utils.ISO_FORMAT` for the parameter's expected format.
    """
    
    # Note: The `strptime() method is not thread-safe (http://bugs.python.org/issue7980)
    strptimeLock.acquire()
    cacheTime = time.strptime(isoTime, utils.ISO_FORMAT)
    strptimeLock.release()
    return calendar.timegm(cacheTime)
    

def get_modification_time(path):
    """
    Returns the modification time of the path in the number of seconds from the UNIX epoch.
    May return None if the path does not exist.
    """
    
    if not os.path.exists(path): return None
    return calendar.timegm(time.gmtime(os.path.getmtime(path)))
    