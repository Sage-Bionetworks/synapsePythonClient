# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import filecmp, os, sys, traceback, logging, requests, uuid
import time, random
from threading import Lock
import six

if six.PY2:
    import thread
    from Queue import Queue
else:
    import _thread as thread
    from queue import Queue

import synapseclient
import synapseclient.utils as utils
import synapseclient.cache as cache
from synapseclient.exceptions import *
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File

import integration
from integration import schedule_for_cleanup

def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project
    
    # Use the module-level syn object to communicate between main and child threads
    # - Read-only objects (for the children)
    module.syn.test_parent = module.syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(module.syn.test_parent)
    module.syn.test_keepRunning = True
    
    # - Child writeable objects
    module.syn.test_errors = Queue()
    module.syn.test_runCountMutex = Lock()
    module.syn.test_threadsRunning = 0
    
def teardown(module):
    del module.syn.test_parent
    del module.syn.test_keepRunning
    del module.syn.test_errors
    del module.syn.test_runCountMutex
    del module.syn.test_threadsRunning

def test_threaded_access():
    """Starts multiple threads to perform store and get calls randomly."""
    ## Doesn't this test look like a DOS attack on Synapse?
    ## Maybe it should be called explicity...
    
    # Suppress most of the output from the many REST calls
    #   Otherwise, it flood the screen with irrelevant data upon error
    requests_log = logging.getLogger("requests")
    requests_originalLevel = requests_log.getEffectiveLevel()
    requests_log.setLevel(logging.WARNING)
    
    print("Starting threads")
    store_thread = wrap_function_as_child_thread(thread_keep_storing_one_File)
    get_thread = wrap_function_as_child_thread(thread_get_files_from_Project)
    update_thread = wrap_function_as_child_thread(thread_get_and_update_file_from_Project)
    thread.start_new_thread(store_thread, ())
    thread.start_new_thread(store_thread, ())
    thread.start_new_thread(store_thread, ())
    thread.start_new_thread(store_thread, ())
    thread.start_new_thread(get_thread, ())
    thread.start_new_thread(get_thread, ())
    thread.start_new_thread(get_thread, ())
    thread.start_new_thread(update_thread, ())
    thread.start_new_thread(update_thread, ())
    thread.start_new_thread(update_thread, ())
    
    # Give the threads some time to wreak havoc on the cache
    time.sleep(20)
    
    print("Terminating threads")
    syn.test_keepRunning = False
    while syn.test_threadsRunning > 0:
        time.sleep(1)

    # Reset the requests logging level
    requests_log.setLevel(requests_originalLevel)
        
    collect_errors_and_fail()
  
#############
## Helpers ##
#############

def wrap_function_as_child_thread(function):
    """Wraps the given function so that it ties into the main thread."""
    
    def child_thread():
        syn.test_runCountMutex.acquire()
        syn.test_threadsRunning += 1
        syn.test_runCountMutex.release()
        
        try:
            function()
        except Exception:
            syn.test_errors.put(traceback.format_exc())
            
        syn.test_runCountMutex.acquire()
        syn.test_threadsRunning -= 1
        syn.test_runCountMutex.release()
        
    return child_thread
    
def collect_errors_and_fail():
    """Pulls error traces from the error queue and fails if the queue is not empty."""
    failures = []
    for i in range(syn.test_errors.qsize()):
        failures.append(syn.test_errors.get())
    if len(failures) > 0:
        raise SynapseError('\n' + '\n'.join(failures))
    
######################
## Thread Behaviors ##    
######################

def thread_keep_storing_one_File():
    """Makes one file and stores it over and over again."""
    
    # Make a local file to continuously store
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    myPrecious = File(path, parent=syn.test_parent, description='This bogus file is MINE', mwa="hahahah")
    
    while syn.test_keepRunning:
        stored = store_catch_412_HTTPError(myPrecious)
        if stored is not None:
            myPrecious = stored
            # print("I've stored %s" % myPrecious.id)
        else: 
            myPrecious = syn.get(myPrecious)
            # print("Grrr... Someone modified my %s" % myPrecious.id)
                
        sleep_for_a_bit()

        
def thread_get_files_from_Project():
    """Continually polls and fetches items from the Project."""
    
    while syn.test_keepRunning:
        for id in get_all_ids_from_Project():
            # print("I got %s" % id)
            pass
            
        sleep_for_a_bit()
        
def thread_get_and_update_file_from_Project():
    """Fetches one item from the Project and updates it with a new file."""
    
    while syn.test_keepRunning:
        id = get_all_ids_from_Project()
        if len(id) <= 0:
            continue
            
        id = id[random.randrange(len(id))]
        entity = syn.get(id)
        
        # Replace the file and re-store
        path = utils.make_bogus_data_file()
        schedule_for_cleanup(path)
        entity.path = path
        entity = store_catch_412_HTTPError(entity)
        if entity is not None:
            # print("I updated %s" % entity.id)
            assert os.stat(entity.path) == os.stat(path)
            
        sleep_for_a_bit()
    
####################
## Thread Helpers ##
####################
    
def sleep_for_a_bit():
    """Sleeps for a random amount of seconds between 1 and 5 inclusive."""
    
    time.sleep(random.randint(1, 5))

def get_all_ids_from_Project():
    """Fetches all currently available Synapse IDs from the parent Project."""
    
    others = syn.chunkedQuery('select id from entity where parentId=="%s"' % syn.test_parent.id)
    return [result['entity.id'] for result in others]
    
def store_catch_412_HTTPError(entity):
    """Returns the stored Entity if the function succeeds or None if the 412 is caught."""
    try:
        return syn.store(entity)
    except SynapseHTTPError as err:
        # Some other thread modified the Entity, so try again
        if err.response.status_code == 412:
            return None
        raise

