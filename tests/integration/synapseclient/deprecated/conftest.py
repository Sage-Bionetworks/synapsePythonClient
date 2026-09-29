"""Fixtures for the deprecated integration tests.

These tests exercise the legacy (pre-OOP) classes in the root synapseclient
package. The fixtures here rely on the deprecated synapseclient.Project and
synapseclient.Entity classes and are intentionally scoped to this directory so the
rest of the integration suite does not depend on deprecated code.
"""

import os
import shutil
import sys
import uuid

import pytest_asyncio

from synapseclient import Entity, Project, Synapse
from synapseclient.core import utils
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.models import (
    CurationTask,
    Grid,
    SubmissionView,
    Team,
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)


@pytest_asyncio.fixture(loop_scope="session", scope="session")
def project(syn: Synapse, schedule_for_cleanup) -> Project:
    """
    Create a project to be shared by all deprecated tests in the session. If xdist is
    being used a project is created for each worker node.
    """
    proj = syn.store(Project(name="integration_test_project" + str(uuid.uuid4())))
    schedule_for_cleanup(proj)
    return proj


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def schedule_for_cleanup(request, syn: Synapse):
    """Returns a closure that takes an item that should be scheduled for cleanup.
    The cleanup will occur after the session finish to allow the deletes to take
    advantage of any connection pooling."""

    items = []

    def _append_cleanup(item):
        items.append(item)

    def cleanup_scheduled_items():
        wrap_async_to_sync(_cleanup(syn, items))

    request.addfinalizer(cleanup_scheduled_items)

    return _append_cleanup


async def _cleanup(syn: Synapse, items):
    """cleanup junk created during testing"""
    for item in reversed(items):
        if (
            isinstance(item, Entity)
            or utils.is_synapse_id_str(item)
            or hasattr(item, "deleteURI")
        ):
            try:
                syn.delete(item)
            except Exception as ex:
                if hasattr(ex, "response") and ex.response.status_code in [404, 403]:
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
                sys.stderr.write(
                    "Don't know how to clean: %s (type: %s)"
                    % (str(item), type(item).__name__)
                )
        elif isinstance(
            item,
            (
                Team,
                SubmissionView,
                WikiPage,
                WikiHistorySnapshot,
                WikiHeader,
                WikiOrderHint,
                CurationTask,
                Grid,
            ),
        ):
            try:
                await item.delete_async(synapse_client=syn)
            except Exception as ex:
                if hasattr(ex, "response") and ex.response.status_code in [404, 403]:
                    pass
                else:
                    print("Error cleaning up entity: " + str(ex))
        else:
            sys.stderr.write(
                "Don't know how to clean: %s (type: %s)"
                % (str(item), type(item).__name__)
            )
