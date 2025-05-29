"""
Telemetry integration for the Synapse Python Client.

This module provides a central place to access all telemetry decorators and utilities.
It uses trace data (detailed logs) for analysis via spans, attributes, and events.
"""

import time
import os
import threading
from contextlib import contextmanager
from typing import Dict, Any, Optional

from opentelemetry import trace
from opentelemetry.trace import Span
from opentelemetry import context as context_api
from tqdm import tqdm

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
        progress_bar: Optional[tqdm] = None,
        total_size: Optional[int] = None,
        operation: Optional[str] = None
    ):
        """
        Initialize the transfer monitor.

        Args:
            span: OpenTelemetry span for the transfer
            progress_bar: Optional tqdm progress bar
            total_size: Total size of the file being transferred in bytes
            operation: The operation type (upload/download/sync)
        """
        self.span = span
        self.progress_bar = progress_bar
        self.transferred_bytes = 0
        self.chunks_total = 0
        self.chunks_completed = 0
        self.retry_count = 0
        self.total_size = total_size
        self.operation = operation
        self._events = []

        # Thread lock for safe updates in multi-threaded environments
        self._lock = threading.RLock()
        # Dictionary to track per-thread byte progress
        self._thread_bytes = {}
        # Dictionary to track chunk start times
        self._chunk_start_times = {}

        self._last_update_time = time.time()
        self._last_bytes = 0

    def record_retry(self, error: Optional[Exception] = None):
        """
        Record a retry attempt during transfer.

        Args:
            error: Optional exception that caused the retry
        """
        with self._lock:
            self.retry_count += 1
            # Update trace attributes - retry count is metadata about the overall transfer
            self.span.set_attribute("synapse.operation.retry_count", self.retry_count)

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
    destination: str,
    syn_id: Optional[str] = None,
    with_progress_bar: bool = True,
    mime_type: Optional[str] = None,
    file_version: Optional[int] = None,
    multipart: bool = False,
    chunk_size: Optional[int] = None,
    storage_provider: Optional[str] = None,
    attempts: Optional[int] = None,
    concrete_type: Optional[str] = None,
    pre_signed_url: bool = False,
    parent_span: Optional[Span] = None,
    reuse_monitor: Optional[TransferMonitor] = None,
    **attributes
):
    """
    Context manager to monitor and track file transfers with OpenTelemetry.
    Uses trace spans, attributes, and events to collect telemetry data.

    Args:
        operation: The operation type (upload/download/sync)
        file_path: Path of the file being transferred
        file_size: Size of the file in bytes
        destination: Destination description (e.g. "synapse:{id}", "S3", etc.)
        syn_id: Optional Synapse ID
        with_progress_bar: Whether to display a progress bar
        mime_type: Content type of the file
        file_version: Version number of the file
        multipart: Whether multipart transfer is used
        chunk_size: Size of chunks for multipart transfers
        storage_provider: Storage provider (S3, SFTP, Azure, GCS, Local)
        attempts: Number of attempts made so far
        concrete_type: The concrete type of the file handle
        pre_signed_url: Whether using pre-signed URL
        parent_span: Optional parent span to use for creating child spans
        reuse_monitor: Optional existing monitor to reuse instead of creating a new one
        **attributes: Additional span attributes

    Yields:
        TransferMonitor instance that can be updated during transfer
    """
    # TODO: Since this could contain sensitive information assume we do not use this by
    # default, but allow it to be enabled by the user
    file_name = os.path.basename(file_path)
    span_name = f"{operation}:{file_name}"

    # Use standardized attribute names
    span_attributes = {
        "synapse.transfer.method": operation,
        "synapse.file.name": file_name,
        "synapse.file.size_bytes": file_size,
        "synapse.transfer.destination": destination,
        "synapse.transfer.start_time": time.time(),
        "synapse.transfer.multipart": multipart,
        "synapse.operation.category": "file_transfer",
    }

    # Add destination type info to span attributes
    if destination.startswith("synapse:"):
        span_attributes["synapse.transfer.destination_type"] = "synapse"
    elif concrete_type:
        # Extract provider from concrete type
        provider = concrete_type.split(".")[-1].lower()
        if "s3" in provider:
            span_attributes["synapse.transfer.destination_type"] = "s3"
        elif "external" in provider:
            span_attributes["synapse.transfer.destination_type"] = "external"
        else:
            span_attributes["synapse.transfer.destination_type"] = provider

    # Add transfer direction attribute based on operation
    if operation.lower().startswith("upload"):
        span_attributes["synapse.transfer.direction"] = "upload"
        # Enhance transfer method with more specificity
        if multipart:
            span_attributes["synapse.transfer.method"] = "multipart_upload"
        else:
            span_attributes["synapse.transfer.method"] = "upload"
    elif operation.lower().startswith("download"):
        span_attributes["synapse.transfer.direction"] = "download"
        # Enhance transfer method with more specificity
        if "cache" in operation.lower():
            span_attributes["synapse.transfer.method"] = "cached_copy"
        else:
            span_attributes["synapse.transfer.method"] = "download"
    elif operation.lower().startswith("sync"):
        span_attributes["synapse.transfer.direction"] = "sync"

    # Add entity ID if provided
    if syn_id:
        span_attributes["synapse.entity.id"] = syn_id

    # Add storage provider if specified
    if storage_provider:
        span_attributes["synapse.storage.provider"] = storage_provider

    # Add optional file metadata for traces
    if mime_type:
        span_attributes["synapse.file.mime_type"] = mime_type
    if file_version is not None:
        span_attributes["synapse.file.version"] = file_version

    # Add multipart transfer information if applicable
    if multipart:
        if chunk_size:
            span_attributes["synapse.transfer.chunk_size"] = chunk_size

    # Track attempt number if provided
    if attempts is not None:
        span_attributes["synapse.operation.attempt"] = attempts
        # Calculate retry count as attempts minus 1 (first attempt isn't a retry)
        span_attributes["synapse.operation.retry_count"] = max(0, attempts - 1)

    # Track concrete type and pre-signed URL status
    if concrete_type:
        span_attributes["synapse.file.concrete_type"] = concrete_type
    if pre_signed_url:
        span_attributes["synapse.transfer.pre_signed_url"] = True

    # Add additional attributes
    span_attributes.update(attributes)

    # If we're reusing an existing monitor, just yield that
    if reuse_monitor:
        # Update the monitor with any new attributes if needed
        for key, value in span_attributes.items():
            reuse_monitor.span.set_attribute(key, value)

        # Record transfer start event
        reuse_monitor.span.add_event("transfer_start", {"operation": operation})

        try:
            yield reuse_monitor
            # Record successful completion in span
            if operation != reuse_monitor.operation:
                reuse_monitor.span.add_event("transfer_complete", {"status": "success"})
        except Exception as e:
            # Record error in span
            error_details = {"error_type": type(e).__name__, "error_message": str(e)}
            reuse_monitor.span.add_event("transfer_error", error_details)
            if operation != reuse_monitor.operation:
                reuse_monitor.span.add_event("transfer_complete", {"status": "error"})
            # Re-raise the exception after recording to span
            raise
        return

    # Start transfer span using context API
    if parent_span:
        # Create a child span if a parent was provided
        span = tracer.start_span(span_name, context=trace.set_span_in_context(parent_span), attributes=span_attributes)
    else:
        # Create a new root span
        span = tracer.start_span(span_name, attributes=span_attributes)

    # Set the span as the current span
    token = context_api.attach(trace.set_span_in_context(span))

    try:
        # Create progress bar if requested
        # TODO: Should the progress bar be getting created here??
        progress_bar = None
        if with_progress_bar:
            progress_bar = tqdm(
                total=file_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f"{operation} {file_name}"
            )

        # Create and yield monitor
        monitor = TransferMonitor(
            span=span,
            progress_bar=progress_bar,
            total_size=file_size,
            operation=operation
        )

        # Record transfer start event
        span.add_event("transfer_start", {"operation": operation})

        start_time = time.time()

        try:
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
        finally:
            elapsed = time.time() - start_time

            # Calculate throughput if there was any transfer and time elapsed
            throughput = 0
            if elapsed > 0 and monitor.transferred_bytes > 0:
                throughput = monitor.transferred_bytes / elapsed  # bytes per second

            # Add transfer complete details as an event (timestamp is meaningful)
            span.add_event("transfer_details", {
                "duration_seconds": elapsed,
                "transferred_bytes": monitor.transferred_bytes,
                "throughput_bytes_per_second": throughput
            })

            # Close progress bar if created
            if progress_bar:
                progress_bar.close()
    finally:
        # End the span and detach the context
        span.end()
        context_api.detach(token)
