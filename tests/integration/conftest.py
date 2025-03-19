"""Set up for integration tests."""

import logging
import os
import platform
import shutil
import sys
import tempfile
import time
import uuid

import pytest
import pytest_asyncio
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import OS_DESCRIPTION, OS_TYPE, SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from pytest_asyncio import is_async_test

from synapseclient import Entity, Project, Synapse
from synapseclient.core import utils
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME
from synapseclient.models import Project as Project_Model
from synapseclient.models import Team

tracer = trace.get_tracer("synapseclient")
working_directory = tempfile.mkdtemp(prefix="someTestFolder")
Synapse.allow_client_caching = False


def pytest_collection_modifyitems(items) -> None:
    """Taken from docs at:
    https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/run_session_tests_in_same_loop.html

    I want to run all tests, even if they are not explictly async, within the same event
    loop. This will allow our async_to_sync wrapper logic use the same event loop
    for all tests. This implictly allows us to re-use the HTTP connection pooling for
    all tests.
    """
    pytest_asyncio_tests = (item for item in items if is_async_test(item))
    session_scope_marker = pytest.mark.asyncio(scope="session")
    for async_test in pytest_asyncio_tests:
        async_test.add_marker(session_scope_marker, append=False)


@pytest_asyncio.fixture(loop_scope="session", scope="session")
def syn(request) -> Synapse:
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

    # set the working directory to a temp directory
    _old_working_directory = os.getcwd()
    os.chdir(working_directory)

    def teardown() -> None:
        os.chdir(_old_working_directory)

    request.addfinalizer(teardown)
    return syn


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def project_model(request, syn: Synapse) -> Project_Model:
    """
    Create a project to be shared by all tests in the session. If xdist is being used
    a project is created for each worker node.
    """

    # Make one project for all the tests to use
    proj = await Project_Model(
        name="integration_test_project" + str(uuid.uuid4())
    ).store_async()

    def project_teardown() -> None:
        wrap_async_to_sync(_cleanup(syn, [working_directory, proj.id]), syn)

    request.addfinalizer(project_teardown)

    return proj


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def project(request, syn: Synapse) -> Project:
    """
    Create a project to be shared by all tests in the session. If xdist is being used
    a project is created for each worker node.
    """

    # Make one project for all the tests to use
    proj = syn.store(Project(name="integration_test_project" + str(uuid.uuid4())))

    def project_teardown():
        wrap_async_to_sync(_cleanup(syn, [working_directory, proj]), syn)

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


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def schedule_for_cleanup(request, syn: Synapse):
    """Returns a closure that takes an item that should be scheduled for cleanup.
    The cleanup will occur after the session finish to allow the deletes to take
    advantage of any connection pooling."""

    items = []

    def _append_cleanup(item):
        items.append(item)

    def cleanup_scheduled_items():
        wrap_async_to_sync(_cleanup(syn, items), syn)

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


class FileSpanExporter(ConsoleSpanExporter):
    """Create an exporter for OTEL data to a file."""

    def __init__(self, file_path) -> None:
        """Init with a path."""
        self.file_path = file_path

    def export(self, spans) -> None:
        """Export the spans to the file."""
        with open(self.file_path, "a", encoding="utf-8") as f:
            for span in spans:
                span_json_one_line = span.to_json().replace("\n", "") + "\n"
                f.write(span_json_one_line)


active_span_processors = []


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
        Synapse.enable_open_telemetry(True)
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
            processor = BatchSpanProcessor(OTLPSpanExporter())
            active_span_processors.append(processor)
            trace.get_tracer_provider().add_span_processor(processor)
        elif exporter_type == "console":
            processor = BatchSpanProcessor(ConsoleSpanExporter())
            active_span_processors.append(processor)
            trace.get_tracer_provider().add_span_processor(processor)
        elif exporter_type == "file":
            timestamp_millis = int(time.time() * 1000)
            file_name = f"otel_spans_integration_testing_{timestamp_millis}.ndjson"
            file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), file_name
            )
            processor = SimpleSpanProcessor(FileSpanExporter(file_path))
            active_span_processors.append(processor)
            trace.get_tracer_provider().add_span_processor(processor)
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


@pytest.fixture(autouse=True)
def set_timezone():
    os.environ["TZ"] = "UTC"
    if platform.system() != "Windows":
        time.tzset()


@pytest.fixture(autouse=True, scope="function")
def wrap_with_otel(request):
    """Start a new OTEL Span for each test function."""
    with tracer.start_as_current_span(request.node.name) as span:
        try:
            yield
        finally:
            for processor in active_span_processors:
                processor.force_flush()
            span.end()
