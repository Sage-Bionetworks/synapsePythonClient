import logging
import platform
import uuid
import os, time
import sys
import shutil
import tempfile

import pytest

from synapseclient import Entity, Synapse, Project
from synapseclient.core import utils
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, OS_TYPE, OS_DESCRIPTION, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF

tracer = trace.get_tracer("synapseclient")

"""
pytest session level fixtures shared by all integration tests.
"""


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
