"""Set up for integration tests."""

import asyncio
import logging
import os
import platform
import shutil
import sys
import tempfile
import time
import uuid

import pytest
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import OS_DESCRIPTION, OS_TYPE, SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF

from synapseclient import Entity, Project, Synapse
from synapseclient.core import utils
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME
from synapseclient.models import Project as Project_Model
from synapseclient.models import Team

tracer = trace.get_tracer("synapseclient")

"""
pytest session level fixtures shared by all integration tests.
"""


@pytest.fixture(autouse=True)
def event_loop(request):
    """
    Redefine the event loop to support session/module-scoped fixtures;
    see https://github.com/pytest-dev/pytest-asyncio/issues/371
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()

    try:
        yield loop
    finally:
        loop.close()


@pytest.fixture(scope="session")
@tracer.start_as_current_span("conftest::syn")
def syn() -> Synapse:
    """
    Create a logged in Synapse instance that can be shared by all tests in the session.
    If xdist is being used a syn is created for each worker node.
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
@tracer.start_as_current_span("conftest::project")
@pytest.mark.asyncio
def project_model(request, syn: Synapse) -> Project_Model:
    """
    Create a project to be shared by all tests in the session. If xdist is being used
    a project is created for each worker node.
    """

    # Make one project for all the tests to use
    proj = asyncio.run(
        Project_Model(name="integration_test_project" + str(uuid.uuid4())).store_async()
    )

    # set the working directory to a temp directory
    _old_working_directory = os.getcwd()
    working_directory = tempfile.mkdtemp(prefix="someTestFolder_models")
    os.chdir(working_directory)

    def project_teardown() -> None:
        _cleanup(syn, [working_directory, proj.id])
        os.chdir(_old_working_directory)

    request.addfinalizer(project_teardown)

    return proj


@pytest.fixture(scope="session")
@tracer.start_as_current_span("conftest::project")
def project(request, syn: Synapse) -> Project:
    """
    Create a project to be shared by all tests in the session. If xdist is being used
    a project is created for each worker node.
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


@pytest.fixture(scope="function", autouse=True)
def clear_cache() -> None:
    """
    Clear all LRU caches before each test to avoid any side effects.
    """
    from synapseclient.api.entity_services import get_upload_destination

    # Clear the cache before each test
    get_upload_destination.cache_clear()

    yield

    # Clear the cache after each test
    get_upload_destination.cache_clear()


@pytest.fixture(scope="module")
def schedule_for_cleanup(request, syn: Synapse):
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


@tracer.start_as_current_span("conftest::_cleanup")
def _cleanup(syn: Synapse, items):
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
        elif isinstance(item, Team):
            try:
                item.delete()
            except Exception as ex:
                if hasattr(ex, "response") and ex.response.status_code in [404, 403]:
                    pass
                else:
                    print("Error cleaning up entity: " + str(ex))
        else:
            sys.stderr.write("Don't know how to clean: %s" % str(item))


@pytest.fixture(scope="session", autouse=True)
def setup_otel():
    """
    Handles setting up the OpenTelemetry tracer provider for integration tests.
    Depending on the environment variables set, the provider will be configured
    to export to the console, a file, or to an OTLP endpoint.
    """
    # Setup
    exporter_type = os.environ.get("SYNAPSE_OTEL_INTEGRATION_TEST_EXPORTER", None)
    if exporter_type:
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource(
                    attributes={
                        SERVICE_NAME: "syn_int_tests",
                        OS_DESCRIPTION: platform.release(),
                        OS_TYPE: platform.system(),
                    }
                ),
            )
        )
        if exporter_type == "otlp":
            trace.get_tracer_provider().add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter())
            )
        elif exporter_type == "console":
            trace.get_tracer_provider().add_span_processor(
                BatchSpanProcessor(ConsoleSpanExporter())
            )
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


@pytest.fixture(autouse=True)
def set_timezone():
    os.environ["TZ"] = "UTC"
    if platform.system() != "Windows":
        time.tzset()


def alternative_uuid_generation() -> str:
    """
    Alternative UUID generation function that includes the system timestamp.
    """
    timestamp = str(time.time())
    return str(f"{uuid.UUID(bytes=os.urandom(16), version=4)}-{timestamp}").replace(
        ".", ""
    )


@pytest.fixture(scope="session", autouse=True)
def replace_uuid_generation() -> None:
    """
    This is an attempt to make UUID generation collisions never happen during
    integration testing. The issue being solved is that for some reason when running
    the tests with pytest-xdist, the UUID generation would collide and cause tests to
    fail.

    Here are 5 cases I found with recent integration tests:

    synapseclient.core.exceptions.SynapseHTTPError: 409 Client Error: An entity with the name: 78ef8002-fcaf-41c4-914f-1313771b82f4 already exists with a parentId: syn123
    Name My Uniquely Named Team 49025ca1-5bc7-490d-bbcc-84804b1b3b8e is already used.
    An entity with the name: PartialRowTestViews42735d66-ebe5-40e0-9b04-0f88a80b3cf2 already exists with a parentId: syn123
    Name My Uniquely Named Team 42eb7452-dcdf-4f30-b962-715a4369a67e is already used.
    An Organization with the name: 'a10dc9a5e64264414958bb979b2fa1852' already exists
    """
    uuid.uuid4 = alternative_uuid_generation
