"""Shared test helpers for integration tests."""

import asyncio
import logging
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

_TELEMETRY_TRUTHY_VALUES = ("1", "true", "yes", "on")

# Logger name used by the OTLP exporters to report rejected exports (e.g. a 401
# from a malformed `OTEL_EXPORTER_OTLP_HEADERS`).
OTLP_EXPORTER_LOGGER = "opentelemetry.exporter.otlp.proto.http"


class ExportFailureRecorder(logging.Handler):
    """Captures ERROR-level log records emitted by the OTLP exporters, so a
    rejected export can fail the run instead of leaving it silently green.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def export_failure_summary(messages: List[str]) -> Optional[str]:
    """Build a one-line summary of captured OTLP export failures.

    Args:
        messages: The messages captured by `ExportFailureRecorder`.

    Returns:
        None if messages is empty, otherwise a string naming the count and the
        first message.
    """
    if not messages:
        return None
    return f"{len(messages)} OTLP export failure(s); first: {messages[0]}"


def telemetry_enabled(env: Mapping[str, str]) -> bool:
    """Whether integration-test OpenTelemetry export is enabled.

    Args:
        env: The environment to check, typically `os.environ`.

    Returns:
        True if `SYNAPSE_INTEGRATION_TEST_OTEL_ENABLED` is `1`/`true`/`yes`/`on`
        (case-insensitive), False otherwise.
    """
    value = env.get("SYNAPSE_INTEGRATION_TEST_OTEL_ENABLED", "")
    return value.strip().lower() in _TELEMETRY_TRUTHY_VALUES


def worker_telemetry_env(env: Mapping[str, str]) -> Dict[str, str]:
    """Build the env-var deltas that give each pytest-xdist worker a distinct
    OpenTelemetry resource identity.

    Args:
        env: The environment to derive the deltas from, typically `os.environ`.

    Returns:
        A dict of env vars to apply via `os.environ.update(...)`:
        `OTEL_SERVICE_INSTANCE_ID` (only present if there is a worker id or an
        existing base value to preserve) and `OTEL_RESOURCE_ATTRIBUTES` (always
        present, appended to any existing value rather than overwriting it).
    """
    worker_id = env.get("PYTEST_XDIST_WORKER")
    base_instance_id = env.get("OTEL_SERVICE_INSTANCE_ID")
    if worker_id:
        service_instance_id = (
            f"{base_instance_id}-{worker_id}" if base_instance_id else worker_id
        )
    else:
        service_instance_id = base_instance_id

    resource_attribute_parts = [
        f"run.label={env.get('SYNAPSE_TEST_RUN_LABEL') or 'unlabeled'}"
    ]
    git_sha = env.get("GITHUB_SHA")
    if git_sha:
        resource_attribute_parts.append(f"git.sha={git_sha}")
    worker_count = env.get("PYTEST_XDIST_WORKER_COUNT")
    if worker_count:
        resource_attribute_parts.append(f"xdist.workers={worker_count}")

    new_resource_attributes = ",".join(resource_attribute_parts)
    existing_resource_attributes = env.get("OTEL_RESOURCE_ATTRIBUTES")
    resource_attributes = (
        f"{existing_resource_attributes},{new_resource_attributes}"
        if existing_resource_attributes
        else new_resource_attributes
    )

    result = {"OTEL_RESOURCE_ATTRIBUTES": resource_attributes}
    if service_instance_id:
        result["OTEL_SERVICE_INSTANCE_ID"] = service_instance_id
    return result


async def wait_for_condition(
    condition_fn: Callable[[], Union[Awaitable[T], T]],
    timeout_seconds: float = 60,
    poll_interval_seconds: float = 2,
    backoff_factor: float = 1.5,
    max_interval_seconds: float = 15,
    description: str = "condition",
) -> T:
    """Poll until condition_fn returns a truthy value, with exponential backoff.

    Args:
        condition_fn: A callable (sync or async) that returns a truthy value when
            the condition is met, or a falsy value to keep polling. If it raises
            an exception, polling continues until timeout.
        timeout_seconds: Maximum time to wait before raising TimeoutError.
        poll_interval_seconds: Initial interval between polls.
        backoff_factor: Multiplier applied to the interval after each poll.
        max_interval_seconds: Cap on the poll interval.
        description: Human-readable description for error messages.

    Returns:
        The truthy value returned by condition_fn.

    Raises:
        TimeoutError: If the condition is not met within timeout_seconds.
    """
    elapsed = 0.0
    interval = poll_interval_seconds
    last_exception: Optional[Exception] = None

    while elapsed < timeout_seconds:
        try:
            result = condition_fn()
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                result = await result
            if result:
                return result
        except Exception as ex:
            last_exception = ex
            logger.debug(f"Polling for {description}: caught {type(ex).__name__}: {ex}")

        wait_time = min(interval, timeout_seconds - elapsed)
        if wait_time <= 0:
            break
        await asyncio.sleep(wait_time)
        elapsed += wait_time
        interval = min(interval * backoff_factor, max_interval_seconds)

    msg = f"Timed out waiting for {description} after {timeout_seconds}s"
    if last_exception:
        msg += f" (last error: {type(last_exception).__name__}: {last_exception})"
    raise TimeoutError(msg)
