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
from opentelemetry import metrics, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from pytest_asyncio import is_async_test

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.logging_setup import DEFAULT_LOGGER_NAME, SILENT_LOGGER_NAME
from synapseclient.models import CurationTask, Evaluation, Grid
from synapseclient.models import Project as Project_Model
from synapseclient.models import (
    SubmissionView,
    Team,
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)
from synapseclient.operations import delete_async
from tests.integration.helpers import (
    OTLP_EXPORTER_LOGGER,
    ExportFailureRecorder,
    export_failure_summary,
    telemetry_enabled,
    worker_telemetry_env,
)

tracer = trace.get_tracer("synapseclient")
working_directory = tempfile.mkdtemp(prefix="someTestFolder")
Synapse.allow_client_caching(False)


def pytest_collection_modifyitems(items) -> None:
    """Taken from docs at:
    https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/run_session_tests_in_same_loop.html

    I want to run all tests, even if they are not explictly async, within the same event
    loop. This will allow our async_to_sync wrapper logic use the same event loop
    for all tests. This implicitly allows us to re-use the HTTP connection pooling for
    all tests.
    """
    pytest_asyncio_tests = (item for item in items if is_async_test(item))
    session_scope_marker = pytest.mark.asyncio(loop_scope="session")
    for async_test in pytest_asyncio_tests:
        async_test.add_marker(session_scope_marker, append=False)


@pytest_asyncio.fixture(loop_scope="session", scope="session")
def syn(request) -> Synapse:
    """
    Create a logged in Synapse instance that can be shared by all tests in the session.
    If xdist is being used a syn is created for each worker node.
    """
    print("Python version:", sys.version)

    syn = Synapse(debug=False, skip_checks=True, cache_client=False)
    print("Testing against endpoints:")
    print("  " + syn.repoEndpoint)
    print("  " + syn.authEndpoint)
    print("  " + syn.fileHandleEndpoint)
    print("  " + syn.portalEndpoint + "\n")

    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    syn.login(profile=os.getenv("SYNAPSE_PROFILE", "default"))

    # set the working directory to a temp directory
    _old_working_directory = os.getcwd()
    os.chdir(working_directory)

    def teardown() -> None:
        os.chdir(_old_working_directory)

    request.addfinalizer(teardown)
    return syn


@pytest_asyncio.fixture(loop_scope="session", scope="session")
def syn_with_logger(request) -> Synapse:
    """
    Create a logged in Synapse instance that can be shared by all tests in the session.
    If xdist is being used a syn is created for each worker node.
    """
    print("Python version:", sys.version)

    syn = Synapse(debug=False, skip_checks=True, cache_client=False)
    print("Testing against endpoints:")
    print("  " + syn.repoEndpoint)
    print("  " + syn.authEndpoint)
    print("  " + syn.fileHandleEndpoint)
    print("  " + syn.portalEndpoint + "\n")

    syn.logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    syn.login(profile=os.getenv("SYNAPSE_PROFILE", "default"))

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
    ).store_async(synapse_client=syn)

    def project_teardown() -> None:
        wrap_async_to_sync(_cleanup(syn, [working_directory, proj.id]))

    request.addfinalizer(project_teardown)

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
        if utils.is_synapse_id_str(item) or hasattr(item, "deleteURI"):
            try:
                await delete_async(item, synapse_client=syn)
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
                Evaluation,
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


@pytest.fixture(scope="session", autouse=True)
def setup_otel(request):
    """
    Handles setting up the OpenTelemetry tracer provider for integration tests.

    When telemetry is enabled, also attaches an `ExportFailureRecorder` to the
    OTLP exporters' logger so a rejected export (e.g. a 401 from a malformed
    `OTEL_EXPORTER_OTLP_HEADERS`) is captured. `pytest_sessionfinish` below turns
    a captured failure into a non-zero exit code, since the exporters otherwise
    just log and swallow the error, leaving pytest itself green (feedback 0010).
    """
    if telemetry_enabled(os.environ):
        os.environ.update(worker_telemetry_env(os.environ))
        Synapse.enable_open_telemetry(enable_open_telemetry_metrics=True)

        recorder = ExportFailureRecorder()
        exporter_logger = logging.getLogger(OTLP_EXPORTER_LOGGER)
        exporter_logger.addHandler(recorder)

        yield

        tracer_provider = trace.get_tracer_provider()
        if hasattr(tracer_provider, "force_flush"):
            tracer_provider.force_flush(timeout_millis=30_000)
        meter_provider = metrics.get_meter_provider()
        if hasattr(meter_provider, "force_flush"):
            meter_provider.force_flush(timeout_millis=30_000)

        exporter_logger.removeHandler(recorder)
        request.session.config._otel_export_failures = recorder.messages
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))
        yield


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    """A rejected OTLP export must fail the run even though pytest itself exits
    0 for it (feedback 0010).

    On an xdist worker, forward the captured failures to the controller via
    `workeroutput` (picked up by `pytest_testnodedown` below). On the
    controller or in a serial run, fail the session if any failures were
    captured directly or forwarded from a worker.
    """
    messages = getattr(session.config, "_otel_export_failures", [])
    workeroutput = getattr(session.config, "workeroutput", None)
    if workeroutput is not None:
        workeroutput["otel_export_failures"] = messages
        return
    if messages:
        session.exitstatus = 1


def pytest_testnodedown(node, error):
    """Collect a worker's forwarded OTLP export failures back on the controller."""
    messages = (getattr(node, "workeroutput", None) or {}).get("otel_export_failures")
    if messages:
        node.config._otel_export_failures = (
            getattr(node.config, "_otel_export_failures", []) + messages
        )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print a summary line naming any captured OTLP export failures."""
    summary = export_failure_summary(getattr(config, "_otel_export_failures", []))
    if summary:
        terminalreporter.write_line(f"OTEL export rejected: {summary}", red=True)


@pytest.fixture(autouse=True)
def set_timezone():
    os.environ["TZ"] = "UTC"
    if platform.system() != "Windows":
        time.tzset()


@pytest.fixture(autouse=True, scope="function")
def wrap_with_otel(request):
    """Start a new OTEL Span for each test function."""
    with tracer.start_as_current_span(request.node.nodeid):
        yield
