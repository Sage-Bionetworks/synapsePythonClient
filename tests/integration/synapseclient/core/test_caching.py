import os
import traceback
import logging
import uuid
import time
import random
from threading import Lock

import _thread as thread
from queue import Queue

import pytest
import synapseclient

import synapseclient.core.utils as utils
from synapseclient.core.exceptions import SynapseError, SynapseHTTPError
from synapseclient import File, Project
from func_timeout import FunctionTimedOut, func_set_timeout


@pytest.fixture(scope="module")
def project(syn, schedule_for_cleanup):
    project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project)
    return project


@pytest.fixture(scope="module", autouse=True)
def syn_state(syn):
    syn.test_keepRunning = True

    # - Child writeable objects
    syn.test_errors = Queue()
    syn.test_runCountMutex = Lock()
    syn.test_threadsRunning = 0

    yield

    del syn.test_keepRunning
    del syn.test_errors
    del syn.test_runCountMutex
    del syn.test_threadsRunning


@pytest.mark.flaky(reruns=6)
def test_threaded_access(
    syn: synapseclient.Synapse, project: Project, schedule_for_cleanup
):
    try:
        execute_test_threaded_access(syn, project, schedule_for_cleanup)
    except FunctionTimedOut:
        syn.logger.warning("test_threaded_access timed out")
        pytest.fail("test_threaded_access timed out")


@func_set_timeout(120)
def execute_test_threaded_access(syn, project, schedule_for_cleanup):
    """Starts multiple threads to perform store and get calls randomly."""
    # Doesn't this test look like a DOS attack on Synapse?
    # Maybe it should be called explicity...

    # Suppress most of the output from the many REST calls
    #   Otherwise, it flood the screen with irrelevant data upon error
    requests_log = logging.getLogger("requests")
    requests_originalLevel = requests_log.getEffectiveLevel()
    requests_log.setLevel(logging.WARNING)

    store_thread = wrap_function_as_child_thread(
        syn, thread_keep_storing_one_File, syn, project, schedule_for_cleanup
    )
    get_thread = wrap_function_as_child_thread(
        syn, thread_get_files_from_Project, syn, project
    )
    update_thread = wrap_function_as_child_thread(
        syn, thread_get_and_update_file_from_Project, syn, project, schedule_for_cleanup
    )
    # thread.start_new_thread(store_thread, ())
    # thread.start_new_thread(store_thread, ())
    thread.start_new_thread(store_thread, ())
    thread.start_new_thread(store_thread, ())
    thread.start_new_thread(get_thread, ())
    thread.start_new_thread(get_thread, ())
    # thread.start_new_thread(get_thread, ())
    thread.start_new_thread(update_thread, ())
    thread.start_new_thread(update_thread, ())
    # thread.start_new_thread(update_thread, ())

    syn.logger.warning(f"execute_test_threaded_access Starting sleep for 20 seconds")
    # Give the threads some time to wreak havoc on the cache
    time.sleep(20)
    syn.logger.warning(f"execute_test_threaded_access Slept for 20 seconds")

    syn.test_keepRunning = False
    while syn.test_threadsRunning > 0:
        syn.logger.warning(
            f"Waiting on test_threaded_access() to finish ({syn.test_threadsRunning} threads remaining)"
        )
        time.sleep(1)

    # Reset the requests logging level
    requests_log.setLevel(requests_originalLevel)

    collect_errors_and_fail(syn)


#############
#  Helpers  #
#############


def wrap_function_as_child_thread(syn, function, *args, **kwargs):
    """Wraps the given function so that it ties into the main thread."""

    def child_thread():
        syn.test_runCountMutex.acquire()
        syn.test_threadsRunning += 1
        syn.test_runCountMutex.release()
        unique_uuid = str(uuid.uuid4())

        try:
            syn.logger.warning(f"Starting thread uuid: {unique_uuid}")
            function(*args, **kwargs, unique_uuid=unique_uuid)
        except Exception:
            syn.logger.warning(f"Exception in thread uuid: {unique_uuid}")
            syn.test_errors.put(traceback.format_exc())

        syn.logger.warning(f"Finished thread uuid: {unique_uuid}")
        syn.test_runCountMutex.acquire()
        syn.test_threadsRunning -= 1
        syn.test_runCountMutex.release()

    return child_thread


def collect_errors_and_fail(syn):
    """Pulls error traces from the error queue and fails if the queue is not empty."""
    failures = []
    for i in range(syn.test_errors.qsize()):
        failures.append(syn.test_errors.get())
    if len(failures) > 0:
        raise SynapseError("\n" + "\n".join(failures))


######################
#  Thread Behaviors  #
######################


def thread_keep_storing_one_File(syn, project, schedule_for_cleanup, unique_uuid):
    """Makes one file and stores it over and over again."""

    # Make a local file to continuously store
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    myPrecious = File(
        path, parent=project, description="This bogus file is MINE", mwa="hahahah"
    )

    while syn.test_keepRunning:
        syn.logger.warning(
            f"thread_keep_storing_one_File(): [storing {myPrecious.path}, uuid: {unique_uuid}]"
        )
        stored = store_catch_412_HTTPError(syn, myPrecious)
        if stored is not None:
            myPrecious = stored
        elif "id" in myPrecious:
            # only attempt to retrieve if the entity was initially saved above without encountering a 412 error
            # and thus has a retrievable synapse id
            myPrecious = syn.get(myPrecious)

        syn.logger.warning(
            f"Starting sleep - thread_keep_storing_one_File(), uuid: {unique_uuid}"
        )
        time_slept = sleep_for_a_bit()
        syn.logger.warning(
            f"Slept {time_slept}s - thread_keep_storing_one_File(), uuid: {unique_uuid}"
        )


def thread_get_files_from_Project(syn, project, unique_uuid):
    """Continually polls and fetches items from the Project."""

    while syn.test_keepRunning:
        syn.logger.warning(
            f"thread_get_files_from_Project(), Project: {project.id}, uuid: {unique_uuid}"
        )
        for id in get_all_ids_from_Project(syn, project):
            syn.logger.warning(
                f"thread_get_files_from_Project(), retrieved id: [Project: {project.id}, id: {id}, uuid: {unique_uuid}]"
            )
            pass

        syn.logger.warning(
            f"Starting sleep thread_get_files_from_Project(), uuid: {unique_uuid}"
        )
        time_slept = sleep_for_a_bit()
        syn.logger.warning(
            f"Slept {time_slept} thread_get_files_from_Project(), uuid: {unique_uuid}"
        )


def thread_get_and_update_file_from_Project(
    syn, project, schedule_for_cleanup, unique_uuid
):
    """Fetches one item from the Project and updates it with a new file."""

    while syn.test_keepRunning:
        syn.logger.warning(
            f"Running thread_get_and_update_file_from_Project(), uuid: {unique_uuid}"
        )
        id = get_all_ids_from_Project(syn, project)
        if len(id) <= 0:
            continue

        id = id[random.randrange(len(id))]
        entity = syn.get(id)

        # Replace the file and re-store
        path = utils.make_bogus_data_file()
        schedule_for_cleanup(path)
        syn.logger.warning(
            f"thread_get_and_update_file_from_Project(), Updating: [project: {project.id}, entity: {entity.id}, path: {path}, uuid: {unique_uuid}]]"
        )
        entity.path = path
        entity = store_catch_412_HTTPError(syn, entity)
        if entity is not None:
            assert os.stat(entity.path) == os.stat(path)

        syn.logger.warning(
            f"Starting sleep thread_get_and_update_file_from_Project(), uuid: {unique_uuid}"
        )
        time_slept = sleep_for_a_bit()
        syn.logger.warning(
            f"Slept {time_slept} thread_get_and_update_file_from_Project(), uuid: {unique_uuid}"
        )


####################
#  Thread Helpers  #
####################


def sleep_for_a_bit() -> int:
    """Sleeps for a random amount of seconds between 1 and 5 inclusive."""
    time_to_sleep = random.randint(1, 5)
    time.sleep(time_to_sleep)
    return time_to_sleep


def get_all_ids_from_Project(syn, project):
    """Fetches all currently available Synapse IDs from the parent Project."""
    return [result["id"] for result in syn.getChildren(project.id)]


def store_catch_412_HTTPError(syn, entity):
    """Returns the stored Entity if the function succeeds or None if the 412 is caught."""
    try:
        return syn.store(entity)
    except SynapseHTTPError as err:
        # Some other thread modified the Entity, so try again
        if err.response.status_code == 412:
            return None
        raise
