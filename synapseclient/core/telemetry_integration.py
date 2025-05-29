"""
Telemetry integration for the Synapse Python Client.

This module provides a central place to access all telemetry decorators and utilities.
It uses trace data (detailed logs) for analysis via spans, attributes, and events.
"""

import os
import threading
from contextlib import contextmanager
from typing import Dict, Any, Optional

from opentelemetry.trace import Span

from synapseclient.core.otel_config import get_tracer


# Create tracer for transfer operations
tracer = get_tracer("synapseclient.transfer")


class TransferMonitor:
    """
    Helper class to track progress during file transfers.
    Uses traces, attributes, and events for telemetry.
    Thread-safe for multi-threaded environments.
    """

    def __init__(
        self,
        span: Span,
    ):
        """
        Initialize the transfer monitor.

        Args:
            span: OpenTelemetry span for the transfer
            total_size: Total size of the file being transferred in bytes
            operation: The operation type (upload/download/sync)
        """
        self.span = span
        self.retry_count = 0
        self._events = []

        # Thread lock for safe updates in multi-threaded environments
        self._lock = threading.RLock()

    def record_retry(self, error: Optional[Exception] = None):
        """
        Record a retry attempt during transfer.

        Args:
            error: Optional exception that caused the retry
        """
        with self._lock:
            self.retry_count += 1

            # Create event attributes
            event_attrs = {"retry_number": self.retry_count}
            if error:
                event_attrs["error"] = str(error)
                event_attrs["error_type"] = type(error).__name__

            # Use an event since the retry timestamp is meaningful
            self._add_event("transfer_retry", event_attrs)

    def record_cache_hit(self, hit: bool = True):
        """
        Record whether a cache hit occurred.

        Args:
            hit: Whether the cache was hit (True) or missed (False)
        """
        with self._lock:
            # Add an event with all relevant information
            self._add_event("cache_access", {"hit": hit})

    def _add_event(self, name: str, attributes: Dict[str, Any]):
        """
        Add a timestamped event to the span.

        Args:
            name: Event name
            attributes: Event attributes dictionary
        """
        # Store events for potential later analysis
        self._events.append((name, attributes))

        # Add to trace
        self.span.add_event(name, attributes)


@contextmanager
def monitored_transfer(
    operation: str,
    file_path: str,
    file_size: int,
    storage_provider: Optional[str] = None,
    **attributes
):
    """
    Context manager to monitor and track file transfers with OpenTelemetry.
    Uses trace spans, attributes, and events to collect telemetry data.

    Args:
        operation: The operation type (upload/download/sync)
        file_path: Path of the file being transferred
        file_size: Size of the file in bytes
        storage_provider: Storage provider (S3, SFTP, Azure, GCS, Local)
        parent_span: Optional parent span to use for creating child spans
        **attributes: Additional span attributes

    Yields:
        TransferMonitor instance that can be updated during transfer
    """
    with tracer.start_as_current_span(
        f"synapse.transfer.{operation.lower()}"
    ) as span:

        # TODO: Since this could contain sensitive information assume we do not use this by
        # default, but allow it to be enabled by the user
        file_name = os.path.basename(file_path)
        span_attributes = {
            "synapse.file.name": file_name,
            "synapse.file.size_bytes": file_size,
            "synapse.operation.category": "file_transfer",
        }    # Add transfer direction attribute based on operation
        if operation.lower().startswith("upload"):
            span_attributes["synapse.transfer.direction"] = "upload"
        elif operation.lower().startswith("download"):
            span_attributes["synapse.transfer.direction"] = "download"
        elif operation.lower().startswith("sync"):
            span_attributes["synapse.transfer.direction"] = "sync"    # Add storage provider if specified
        if storage_provider:
            span_attributes["synapse.storage.provider"] = storage_provider    # Add additional attributes
        span_attributes.update(attributes)

        # Create and yield monitor
        monitor = TransferMonitor(
            span=span,
        )

        # Record transfer start event
        span.add_event("transfer_start", {"operation": operation})

        try:
            for key, value in span_attributes.items():
                span.set_attribute(key, value)
            yield monitor
            # Record successful completion in span
            span.add_event("transfer_complete", {"status": "success"})
        except Exception as e:
            # Record error in span - use events as error timing is meaningful
            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "error": True  # Flag for error filtering
            }
            span.add_event("transfer_error", error_details)
            span.add_event("transfer_complete", {"status": "error"})
            # Re-raise the exception after recording to span
            raise
