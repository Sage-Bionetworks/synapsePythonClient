import logging
import uuid
import os
import sys
import shutil
import tempfile

import pytest

from synapseclient import Entity, Synapse, Project
from synapseclient.core import utils
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

"""
pytest session level fixtures shared by all integration tests.
"""


@pytest.fixture(scope="session")
def syn():
    """
    Create a logged in Synapse instance that can be shared by all tests in the session.
    """
    print("Python version:", sys.version)

    syn = Synapse(debug=False, skip_checks=True)
    print("Testing against endpoints:")
    print("  " + syn.repoEndpoint)
    print("  " + syn.authEndpoint)
    print("  " + syn.fileHandleEndpoint)
    print("  " + syn.portalEndpoint + "\n")

    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    syn.login()
    return syn


@pytest.fixture(scope="session")
def project(request, syn):
    """
    Create a project to be shared by all tests in the session.
    """

    # Make one project for all the tests to use
    proj = syn.store(Project(name="integration_test_project" + str(uuid.uuid4())))

    # set the working directory to a temp directory
    _old_working_directory = os.getcwd()
    working_directory = tempfile.mkdtemp(prefix="someTestFolder")
    os.chdir(working_directory)

    def project_teardown():
        _cleanup(syn, [working_directory, proj])
        os.chdir(_old_working_directory)
    request.addfinalizer(project_teardown)

    return proj


@pytest.fixture(scope="module")
def schedule_for_cleanup(request, syn):
    """Returns a closure that takes an item that should be scheduled for cleanup.
    The cleanup will occur after the module tests finish to limit the residue left behind
    if a test session should be prematurely aborted for any reason."""

    items = []

    def _append_cleanup(item):
        items.append(item)

    def cleanup_scheduled_items():
        _cleanup(syn, items)

    request.addfinalizer(cleanup_scheduled_items)

    return _append_cleanup


def _cleanup(syn, items):
    """cleanup junk created during testing"""
    for item in reversed(items):
        if isinstance(item, Entity) or utils.is_synapse_id_str(item) or hasattr(item, 'deleteURI'):
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
