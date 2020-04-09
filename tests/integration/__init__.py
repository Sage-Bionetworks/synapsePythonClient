"""
Integration tests for the Synapse Client for Python

To run all the tests      : nosetests -vs tests
To run a single test suite: nosetests -vs tests/integration
To run a single test set  : nosetests -vs tests/integration/integration_test_Entity.py
To run a single test      : nosetests -vs tests/integration/integration_test_Entity.py:test_Entity
"""

import logging
import uuid
import os
import sys
import shutil
import tempfile


from synapseclient import *
from synapseclient.core import utils
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

QUERY_TIMEOUT_SEC = 25

# when running integration tests using the nose multiprocess plugin
# the fixtures in this module should be invoked once in the parent process
# prior to the fork.
_multiprocess_shared_ = True

_parent_pid = os.getpid()
_child_syn = None


def _get_syn():
    # return a Synapse instance for safely running tests.
    # if we're in the parent process we can use the instance
    # initialized during shared Setup, otherwise in a child
    # process we lazily initialize a new Synapse.
    # we need separate Synapse instances otherwise we may
    # have http session issues using forked copies of the same
    # Synapse instance and its underlying http connection pool.

    if os.getpid() == _parent_pid:
        return syn

    global _child_syn
    if not _child_syn:
        _child_syn = _init_syn()
    return _child_syn


def setup_module(module):
    print("Python version:", sys.version)

    syn = _init_syn()
    print("Testing against endpoints:")
    print("  " + syn.repoEndpoint)
    print("  " + syn.authEndpoint)
    print("  " + syn.fileHandleEndpoint)
    print("  " + syn.portalEndpoint + "\n")

    module.syn = syn
    _init_cleanup(module)

    # Make one project for all the tests to use
    project = syn.store(Project(name="integration_test_project"+str(uuid.uuid4())))
    schedule_for_cleanup(project)
    module.project = project

    # set the working directory to a temp directory
    module._old_working_directory = os.getcwd()
    working_directory = tempfile.mkdtemp(prefix="someTestFolder")
    schedule_for_cleanup(working_directory)
    os.chdir(working_directory)


def _init_syn():
    syn = Synapse(debug=False, skip_checks=True)
    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    syn.login()
    return syn


def _init_cleanup(module):
    to_cleanup = []
    module._to_cleanup = to_cleanup
    module.schedule_for_cleanup = lambda item: to_cleanup.append(item)


def init_module(module, teardown=None):
    """Instrument the given module with integration test facilities.
    Adds a logged in Synapse object, a project that can be used for
    with testing, a schedule_for_cleanup function, and a teardown
    that will automatically invoke the cleanup in when the module
    is torn down.

    :param module: the module being instrumented
    :param teardown: a teardown function if there is additional behavior
        that needs to be invoked on module teardown
    """

    module.syn = _get_syn()
    module.project = project

    _init_cleanup(module)

    def _teardown(module):
        if teardown:
            teardown(module)
        cleanup(module._to_cleanup, module.syn)

    module.teardown = _teardown


def teardown_module(module):
    os.chdir(module._old_working_directory)
    cleanup(module._to_cleanup, module.syn)


def cleanup(items, syn):
    """cleanup junk created during testing"""

    for item in reversed(items):
        if isinstance(item, Entity) or utils.is_synapse_id(item) or hasattr(item, 'deleteURI'):
            try:
                syn.delete(item)
            except Exception as ex:
                if hasattr(ex, 'response') and ex.response.status_code in [404, 403]:
                    pass
                else:
                    print("Error cleaning up entity: " + str(ex))
        elif isinstance(item, str):
            if os.path.exists(item):
                try:
                    if os.path.isdir(item):
                        shutil.rmtree(item)
                    else:  # Assume that remove will work on anything besides folders
                        os.remove(item)
                except Exception as ex:
                    print(ex)
        else:
            sys.stderr.write('Don\'t know how to clean: %s' % str(item))
