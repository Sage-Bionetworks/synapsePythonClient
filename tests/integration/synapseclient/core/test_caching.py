"""Integration testing for multiple threads working on the Synapse cache with stores
getsand ."""

import asyncio
import logging
import os
import random
import time
import traceback
import uuid
from queue import Queue
from typing import Callable

import pytest

import synapseclient.core.utils as utils
from synapseclient import Entity, File, Project, Synapse
from synapseclient.core.exceptions import SynapseError, SynapseHTTPError


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

    yield


async def sleep_and_end_test(syn: Synapse) -> None:
    """Exit the test after sleeping"""
    await asyncio.sleep(20)
    syn.test_keepRunning = False


@pytest.mark.asyncio
async def test_threaded_access(
    syn: Synapse, project: Project, schedule_for_cleanup: Callable[..., None]
) -> None:
    """Starts multiple asyncio Tasks to perform store and get calls randomly. This runs
    on the executor pool to avoid blocking the main thread."""

    # Suppress most of the output from the many REST calls
    #   Otherwise, it flood the screen with irrelevant data upon error
    requests_log = logging.getLogger("requests")
    requests_original_level = requests_log.getEffectiveLevel()
    requests_log.setLevel(logging.WARNING)
    syn.max_threads = 17

    tasks = []
    try:
        tasks.append(asyncio.create_task(sleep_and_end_test(syn)))
        tasks.extend(
            [
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_keep_storing_one_File,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_keep_storing_one_File,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_keep_storing_one_File,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_keep_storing_one_File,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
            ]
        )
        tasks.extend(
            [
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn, thread_get_files_from_Project, syn, project
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn, thread_get_files_from_Project, syn, project
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn, thread_get_files_from_Project, syn, project
                    )
                ),
            ]
        )
        tasks.extend(
            [
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_get_and_update_file_from_Project,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_get_and_update_file_from_Project,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
                asyncio.create_task(
                    wrap_function_as_child_thread(
                        syn,
                        thread_get_and_update_file_from_Project,
                        syn,
                        project,
                        schedule_for_cleanup,
                    )
                ),
            ]
        )

        await asyncio.gather(*tasks)
    finally:
        syn.test_keepRunning = False

    # Reset the requests logging level
    requests_log.setLevel(requests_original_level)

    collect_errors_and_fail(syn)


#############
#  Helpers  #
#############


async def wrap_function_as_child_thread(syn: Synapse, function, *args, **kwargs):
    """Wraps the given function and reports back test errors."""

    loop = asyncio.get_running_loop()

    try:
        loop.run_in_executor(
            syn._get_thread_pool_executor(asyncio_event_loop=loop),
            lambda: function(*args, **kwargs),
        )
    except Exception:
        syn.test_errors.put(traceback.format_exc())


def collect_errors_and_fail(syn: Synapse):
    """Pulls error traces from the error queue and fails if the queue is not empty."""
    failures = []
    for i in range(syn.test_errors.qsize()):
        failures.append(syn.test_errors.get())
    if len(failures) > 0:
        raise SynapseError("\n" + "\n".join(failures))


######################
#  Thread Behaviors  #
######################


def thread_keep_storing_one_File(syn: Synapse, project: Project, schedule_for_cleanup):
    """Makes one file and stores it over and over again."""

    # Make a local file to continuously store
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    myPrecious = File(
        path, parent=project, description="This bogus file is MINE", mwa="hahahah"
    )

    while syn.test_keepRunning:
        stored = None
        stored = store_catch_412_HTTPError(syn, myPrecious)

        if stored is not None:
            myPrecious = stored
        elif "id" in myPrecious:
            # only attempt to retrieve if the entity was initially saved above without encountering a 412 error
            # and thus has a retrievable synapse id
            myPrecious = syn.get(myPrecious)

        sleep_for_a_bit()


def thread_get_files_from_Project(syn: Synapse, project: Project):
    """Continually polls and fetches items from the Project."""

    while syn.test_keepRunning:
        get_all_ids_from_Project(syn, project)

        sleep_for_a_bit()


def thread_get_and_update_file_from_Project(
    syn: Synapse, project: Project, schedule_for_cleanup
):
    """Fetches one item from the Project and updates it with a new file."""

    while syn.test_keepRunning:
        id = get_all_ids_from_Project(syn, project)

        if len(id) == 0:
            sleep_for_a_bit()
            continue

        id = id[random.randrange(len(id))]
        entity = syn.get(id)

        # Replace the file and re-store
        path = utils.make_bogus_data_file()
        schedule_for_cleanup(path)
        entity.path = path
        entity = store_catch_412_HTTPError(syn, entity)

        if entity is not None:
            assert os.stat(entity.path) == os.stat(path)

        sleep_for_a_bit()


####################
#  Thread Helpers  #
####################


def sleep_for_a_bit() -> int:
    """Sleeps for a random amount of seconds between 1 and 5 inclusive."""
    time_to_sleep = random.randint(1, 5)
    time.sleep(time_to_sleep)
    return time_to_sleep


def get_all_ids_from_Project(syn: Synapse, project: Project):
    """Fetches all currently available Synapse IDs from the parent Project."""
    return [result["id"] for result in syn.getChildren(project.id)]


def store_catch_412_HTTPError(syn: Synapse, entity: Entity):
    """Returns the stored Entity if the function succeeds or None if the 412 is caught."""
    try:
        return syn.store(entity)
    except SynapseHTTPError as err:
        # Some other thread modified the Entity, so try again
        if err.response.status_code == 412:
            return None
        raise
