"""
Telemetry integration for the Synapse Python Client.

This module provides a central place to access all telemetry decorators and utilities.
It separates metrics (statistical aggregation) from trace data (detailed logs) for better analysis.
"""

import time
import statistics
import os
import psutil
import threading
from contextlib import contextmanager
from typing import Dict, Any, Optional
from collections import deque

from opentelemetry import trace
from opentelemetry.trace import Span
from tqdm import tqdm

from synapseclient.core.otel_config import get_tracer, get_meter


# Create tracer and meter for transfer operations
tracer = get_tracer("synapseclient.transfer")
meter = get_meter("synapseclient.transfer")

# Create metrics instruments - byte counters
transfer_bytes_counter = meter.create_counter(
    name="synapse.transfer.bytes",
    description="Number of bytes transferred",
    unit="bytes"
)

transfer_start_counter = meter.create_counter(
    name="synapse.transfer.start",
    description="Number of transfers started"
)

transfer_complete_counter = meter.create_counter(
    name="synapse.transfer.complete",
    description="Number of transfers completed"
)

# Time related metrics
transfer_duration_histogram = meter.create_histogram(
    name="synapse.transfer.duration",
    description="Duration of file transfers",
    unit="s"
)

# Performance metrics
transfer_bandwidth_histogram = meter.create_histogram(
    name="synapse.transfer.bandwidth",
    description="Bandwidth of file transfers",
    unit="MBps"
)

transfer_effectiveness_histogram = meter.create_histogram(
    name="synapse.transfer.effectiveness",
    description="Transfer effectiveness (actual/theoretical bandwidth ratio)",
    unit="ratio"
)

# Retry and error metrics
retry_counter = meter.create_counter(
    name="synapse.transfer.retries",
    description="Number of transfer retries"
)

error_counter = meter.create_counter(
    name="synapse.transfer.errors",
    description="Number of transfer errors"
)

# Multipart transfer metrics
chunk_counter = meter.create_counter(
    name="synapse.transfer.chunks",
    description="Number of chunks in multipart transfers"
)

chunk_retry_counter = meter.create_counter(
    name="synapse.transfer.chunk_retries",
    description="Number of chunk retries in multipart transfers"
)

# Cache metrics
cache_counter = meter.create_counter(
    name="synapse.cache.access",
    description="Number of cache accesses"
)

# System metrics
cpu_usage_histogram = meter.create_histogram(
    name="synapse.system.cpu_usage",
    description="CPU usage during transfers",
    unit="percent"
)

memory_usage_histogram = meter.create_histogram(
    name="synapse.system.memory_usage",
    description="Memory usage during transfers",
    unit="mb"
)

disk_io_histogram = meter.create_histogram(
    name="synapse.system.disk_io",
    description="Disk I/O during transfers",
    unit="mbps"
)

disk_iops_histogram = meter.create_histogram(
    name="synapse.system.disk_iops",
    description="Disk IOPS during transfers",
    unit="iops"
)

disk_read_histogram = meter.create_histogram(
    name="synapse.system.disk_read",
    description="Disk read throughput during transfers",
    unit="mbps"
)

disk_write_histogram = meter.create_histogram(
    name="synapse.system.disk_write",
    description="Disk write throughput during transfers",
    unit="mbps"
)


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
    collect_host_metrics: bool = True,
    **attributes
):
    """
    Context manager to monitor and track file transfers with OpenTelemetry.
    Separates metrics from trace data for better analysis.

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
        collect_host_metrics: Whether to collect host system metrics
        **attributes: Additional span attributes

    Yields:
        TransferMonitor instance that can be updated during transfer
    """
    file_name = os.path.basename(file_path)
    span_name = f"{operation}:{file_name}"

    # Use standardized attribute names
    span_attributes = {
        "synapse.transfer.method": operation,
        "synapse.file.name": file_name,
        "synapse.file.size_bytes": file_size,
        "synapse.transfer.destination": destination,
    }

    # Common metric attributes for all instruments
    metric_attributes = {
        "operation": operation,
        "file_size_category": _categorize_file_size(file_size),
        "multipart": multipart,
    }

    # Add destination type to metric attributes
    if destination.startswith("synapse:"):
        metric_attributes["destination_type"] = "synapse"
    elif concrete_type:
        # Extract provider from concrete type
        provider = concrete_type.split(".")[-1].lower()
        if "s3" in provider:
            metric_attributes["destination_type"] = "s3"
        elif "external" in provider:
            metric_attributes["destination_type"] = "external"
        else:
            metric_attributes["destination_type"] = provider

    # Add transfer direction attribute based on operation
    if operation.lower().startswith("upload"):
        span_attributes["synapse.transfer.direction"] = "upload"
        metric_attributes["direction"] = "upload"
        # Enhance transfer method with more specificity
        if multipart:
            span_attributes["synapse.transfer.method"] = "multipart_upload"
        else:
            span_attributes["synapse.transfer.method"] = "upload"
    elif operation.lower().startswith("download"):
        span_attributes["synapse.transfer.direction"] = "download"
        metric_attributes["direction"] = "download"
        # Enhance transfer method with more specificity
        if multipart:
            span_attributes["synapse.transfer.method"] = "multipart_download"
        elif "cache" in operation.lower():
            span_attributes["synapse.transfer.method"] = "cached_copy"
        else:
            span_attributes["synapse.transfer.method"] = "download"
    elif operation.lower().startswith("sync"):
        span_attributes["synapse.transfer.direction"] = "sync"
        metric_attributes["direction"] = "sync"

    # Add operation category for dashboard filtering
    span_attributes["synapse.operation.category"] = "file_transfer"

    # Add entity ID if provided for trace but not metrics (to avoid cardinality explosion)
    if syn_id:
        span_attributes["synapse.entity.id"] = syn_id

    # Add storage provider if specified
    if storage_provider:
        span_attributes["synapse.storage.provider"] = storage_provider
        metric_attributes["storage_provider"] = storage_provider

    # Add optional file metadata for traces
    if mime_type:
        span_attributes["synapse.file.mime_type"] = mime_type
    if file_version is not None:
        span_attributes["synapse.file.version"] = file_version

    # Add multipart transfer information if applicable
    if multipart:
        span_attributes["synapse.transfer.multipart"] = True
        if chunk_size:
            span_attributes["synapse.transfer.chunk_size"] = chunk_size

    # Track attempt number if provided
    if attempts is not None:
        span_attributes["synapse.operation.attempt"] = attempts
        span_attributes["synapse.operation.retry_count"] = max(0, attempts - 1)

    # Track concrete type and pre-signed URL status
    if concrete_type:
        span_attributes["synapse.file.concrete_type"] = concrete_type
    if pre_signed_url:
        span_attributes["synapse.transfer.pre_signed_url"] = True

    # Add additional attributes
    span_attributes.update(attributes)

    # Increment metrics for transfer start
    transfer_start_counter.add(1, metric_attributes)

    # Create process monitor for host metrics if requested
    process = None
    host_metrics_thread = None
    host_metrics_stop_event = None

    if collect_host_metrics:
        try:
            print("Collecting host metrics")
            process = psutil.Process()
            host_metrics_stop_event = threading.Event()
            host_metrics_thread = threading.Thread(
                target=_collect_host_metrics,
                args=(process, host_metrics_stop_event),
                daemon=True
            )
            host_metrics_thread.start()
        except Exception:
            # If we can't get the process for some reason, continue without host metrics
            pass

    # Start transfer span
    with tracer.start_as_current_span(span_name, attributes=span_attributes) as span:
        # Create progress bar if requested
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
            metric_attributes=metric_attributes
        )
        start_time = time.time()

        try:
            yield monitor
            # Mark as successful completion in metrics
            transfer_complete_counter.add(1, {**metric_attributes, "status": "success"})
        except Exception as e:
            # Record error in metrics
            error_attributes = {**metric_attributes, "error_type": type(e).__name__}
            error_counter.add(1, error_attributes)
            transfer_complete_counter.add(1, {**metric_attributes, "status": "error"})
            # Re-raise the exception after recording metrics
            raise
        finally:
            # Stop host metrics collection if it's running
            if host_metrics_stop_event:
                host_metrics_stop_event.set()
                if host_metrics_thread and host_metrics_thread.is_alive():
                    host_metrics_thread.join(timeout=1.0)

                # Capture final host metrics if process is available
                if process:
                    try:
                        # CPU usage
                        cpu_percent = process.cpu_percent(interval=0.1)
                        if cpu_percent > 0:
                            span.set_attribute("synapse.system.cpu_percent", cpu_percent)
                            cpu_usage_histogram.record(cpu_percent, {"resource": "cpu"})

                        # Memory usage
                        memory_info = process.memory_info()
                        memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
                        span.set_attribute("synapse.system.memory_mb", memory_mb)
                        memory_usage_histogram.record(memory_mb, {"resource": "memory"})
                    except Exception:
                        # Ignore any errors in final metrics collection
                        pass

            elapsed = time.time() - start_time

            # Calculate bandwidth in MB/s if any data was transferred
            bandwidth_mbps = 0
            if monitor.transferred_bytes > 0 and elapsed > 0:
                bandwidth_mbps = (monitor.transferred_bytes / 1_000_000) / elapsed

            # Record metrics
            transfer_duration_histogram.record(elapsed, metric_attributes)
            transfer_bytes_counter.add(monitor.transferred_bytes, metric_attributes)
            if bandwidth_mbps > 0:
                transfer_bandwidth_histogram.record(bandwidth_mbps, metric_attributes)

            # Record final trace data
            span.set_attribute("synapse.transfer.duration_seconds", elapsed)
            span.set_attribute("synapse.transfer.bandwidth_mbps", bandwidth_mbps)
            span.set_attribute("synapse.file.transfer_size_bytes", monitor.transferred_bytes)

            # Calculate and record transfer effectiveness (actual/theoretical) if we can determine it
            if bandwidth_mbps > 0 and "theoretical_mbps" in attributes:
                effectiveness = bandwidth_mbps / attributes["theoretical_mbps"]
                span.set_attribute("synapse.transfer.effectiveness", effectiveness)
                transfer_effectiveness_histogram.record(effectiveness, metric_attributes)

            # Ensure final progress percentage is set
            if monitor.total_size and monitor.total_size > 0:
                final_progress = min(100.0, (monitor.transferred_bytes / monitor.total_size) * 100)
                span.set_attribute("synapse.transfer.progress_percent", final_progress)

            # Record throughput statistics if available
            monitor.record_final_statistics()

            # Close progress bar if created
            if progress_bar:
                progress_bar.close()


def _collect_host_metrics(process: psutil.Process, stop_event: threading.Event, interval: float = 1.0):
    """
    Collect host system metrics in a background thread.

    Args:
        process: The current process to monitor
        stop_event: Event to signal when to stop collection
        interval: How often to collect metrics in seconds
    """
    # Get the disk where the process is running - handle cross-platform paths
    try:
        print("Initializing host metrics collection")
        process_cwd = process.cwd()
        disk = None

        # Only attempt to get disk information on Windows
        # On Unix systems, disk path information isn't as straightforward
        if os.name == 'nt' and ':' in process_cwd:  # Windows path format
            disk = process_cwd.split(':')[0]
        print(f"Process CWD: {process_cwd}, Disk: {disk}")
    except (psutil.Error, IndexError, OSError):
        disk = None

    # Initial IO counters - these work cross-platform with psutil
    last_io_counters = process.io_counters() if hasattr(process, 'io_counters') else None

    # Get disk IO counters if available
    # This might not be available on all platforms, so we handle exceptions
    try:
        last_disk_io = psutil.disk_io_counters(perdisk=False) if disk else None
    except (AttributeError, OSError, RuntimeError):
        last_disk_io = None

    last_time = time.time()

    # Get span to report metrics to
    current_span = trace.get_current_span()
    if not current_span.is_recording():
        # If no span is recording, create a fake span to avoid errors
        class NoOpSpan:
            def set_attribute(self, *args): pass
        current_span = NoOpSpan()

    while not stop_event.is_set():
        try:
            # CPU usage - works cross-platform
            cpu_percent = process.cpu_percent(interval=None)
            if cpu_percent > 0:
                current_span.set_attribute("synapse.system.cpu_percent", cpu_percent)
                cpu_usage_histogram.record(cpu_percent, {"resource": "cpu"})

            # Memory usage - works cross-platform
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
            current_span.set_attribute("synapse.system.memory_mb", memory_mb)
            memory_usage_histogram.record(memory_mb, {"resource": "memory"})

            # Process IO - handle potential platform differences
            current_time = time.time()
            elapsed = current_time - last_time

            if last_io_counters and hasattr(process, 'io_counters') and elapsed > 0:
                try:
                    io_counters = process.io_counters()

                    # Calculate read/write rates in MB/s
                    read_bytes = io_counters.read_bytes - last_io_counters.read_bytes
                    write_bytes = io_counters.write_bytes - last_io_counters.write_bytes

                    read_mbps = (read_bytes / (1024 * 1024)) / elapsed
                    write_mbps = (write_bytes / (1024 * 1024)) / elapsed

                    # Record disk IO metrics
                    current_span.set_attribute("synapse.system.disk_read_mbps", read_mbps)
                    current_span.set_attribute("synapse.system.disk_write_mbps", write_mbps)

                    # Record combined IO
                    total_io_mbps = read_mbps + write_mbps
                    current_span.set_attribute("synapse.system.disk_io_mbps", total_io_mbps)

                    # Record to histograms
                    disk_read_histogram.record(read_mbps, {"io_type": "read"})
                    disk_write_histogram.record(write_mbps, {"io_type": "write"})
                    disk_io_histogram.record(total_io_mbps, {"io_type": "total"})

                    # Update last values
                    last_io_counters = io_counters
                except (AttributeError, OSError):
                    # Handle case where attributes might change between platforms
                    pass

            # System disk IO (IOPS) - might not be available on all platforms
            if last_disk_io and elapsed > 0:
                try:
                    disk_io = psutil.disk_io_counters(perdisk=False)

                    # Calculate IOPS (I/O operations per second)
                    read_count = disk_io.read_count - last_disk_io.read_count
                    write_count = disk_io.write_count - last_disk_io.write_count
                    total_iops = (read_count + write_count) / elapsed

                    if total_iops > 0:
                        current_span.set_attribute("synapse.system.disk_iops", total_iops)
                        disk_iops_histogram.record(total_iops, {"resource": "disk"})

                    # Update last values
                    last_disk_io = disk_io
                except (AttributeError, OSError):
                    # Handle case where these metrics might not be available
                    pass

            last_time = current_time

            # Sleep until next collection, but check for stop event frequently
            for _ in range(int(interval * 10)):
                if stop_event.is_set():
                    break
                time.sleep(0.1)
        except Exception:
            # Ignore errors in metrics collection to avoid impacting the main operation
            pass


def _categorize_file_size(size_bytes: int) -> str:
    """
    Categorize file size for better metrics aggregation.

    Returns a string category based on file size to reduce cardinality in metrics.
    """
    if size_bytes < 1024 * 1024:  # Less than 1MB
        return "small"
    elif size_bytes < 100 * 1024 * 1024:  # Less than 100MB
        return "medium"
    elif size_bytes < 1024 * 1024 * 1024:  # Less than 1GB
        return "large"
    else:
        return "very_large"


class TransferMonitor:
    """
    Helper class to track progress during file transfers.
    Separates metrics collection from trace data.
    Thread-safe for multi-threaded environments.
    """

    def __init__(
        self,
        span: Span,
        progress_bar: Optional[tqdm] = None,
        total_size: Optional[int] = None,
        metric_attributes: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the transfer monitor.

        Args:
            span: OpenTelemetry span for the transfer
            progress_bar: Optional tqdm progress bar
            total_size: Total size of the file being transferred in bytes
            metric_attributes: Attributes to add to metrics
        """
        self.span = span
        self.progress_bar = progress_bar
        self.transferred_bytes = 0
        self.chunks_total = 0
        self.chunks_completed = 0
        self.retry_count = 0
        self.total_size = total_size
        self.metric_attributes = metric_attributes or {}
        self._events = []

        # Thread lock for safe updates in multi-threaded environments
        self._lock = threading.RLock()
        # Dictionary to track per-thread byte progress
        self._thread_bytes = {}

        # For tracking throughput statistics
        self._throughput_window_size = 10  # Number of recent updates to track
        self._throughput_samples = deque(maxlen=self._throughput_window_size)
        self._last_update_time = time.time()
        self._last_bytes = 0

    def update(self, bytes_count: int):
        """
        Update the transfer progress.

        Args:
            bytes_count: Number of bytes transferred in this update
        """
        # Skip if no bytes were transferred
        if bytes_count <= 0:
            return

        with self._lock:
            # Update internal counter
            self.transferred_bytes += bytes_count

            # Update progress bar if available
            if self.progress_bar:
                self.progress_bar.update(bytes_count)

            # Calculate throughput for this update
            current_time = time.time()
            time_delta = current_time - self._last_update_time

            if time_delta > 0:
                # Only record throughput if time has passed
                throughput_mbps = (bytes_count / 1_000_000) / time_delta
                self._throughput_samples.append(throughput_mbps)

                # Update metrics in real-time
                transfer_bytes_counter.add(bytes_count, self.metric_attributes)

                # Record trace data
                self._record_trace_progress(bytes_count)

            # Update state for next calculation
            self._last_update_time = current_time
            self._last_bytes = bytes_count

    def _record_trace_progress(self, bytes_count: int):
        """
        Record progress in the trace span.

        Args:
            bytes_count: Number of bytes transferred in this update
        """
        # Update progress percentage if total size is known
        self.update_progress_percent()

        # Add progress event to span
        self._add_event("progress_update", {
            "transferred_bytes": self.transferred_bytes,
            "bytes_delta": bytes_count
        })

    def update_progress_percent(self):
        """
        Update the progress percentage attribute in the span.
        """
        with self._lock:
            if self.total_size and self.total_size > 0:
                progress_percent = min(100.0, (self.transferred_bytes / self.total_size) * 100)
                self.span.set_attribute("synapse.transfer.progress_percent", progress_percent)

    def set_chunks_info(self, total_chunks: int):
        """
        Set information about the number of chunks in a multipart transfer.

        Args:
            total_chunks: Total number of chunks to transfer
        """
        with self._lock:
            self.chunks_total = total_chunks
            self.span.set_attribute("synapse.transfer.chunks_total", total_chunks)

    def chunk_completed(self):
        """Mark a chunk as completed in a multipart transfer."""
        with self._lock:
            self.chunks_completed += 1

            # Update metrics
            chunk_counter.add(1, self.metric_attributes)

            # Update trace attributes
            self.span.set_attribute("synapse.transfer.chunks_completed", self.chunks_completed)

            # Update progress percentage based on chunks if available
            if self.chunks_total > 0:
                chunk_progress = (self.chunks_completed / self.chunks_total) * 100
                self.span.set_attribute("synapse.transfer.chunks_progress_percent", chunk_progress)

            self._add_event("chunk_completed", {
                "chunk_number": self.chunks_completed,
                "chunks_total": self.chunks_total
            })

    def chunk_retry(self, chunk_number: int, error: Optional[Exception] = None):
        """
        Record a chunk retry during multipart transfer.

        Args:
            chunk_number: The chunk number that failed
            error: Optional exception that caused the retry
        """
        with self._lock:
            # Update metrics
            retry_attributes = dict(self.metric_attributes)
            if error:
                retry_attributes["error_type"] = type(error).__name__
            chunk_retry_counter.add(1, retry_attributes)

            event_attrs = {"chunk_number": chunk_number}
            if error:
                event_attrs["error"] = str(error)
                event_attrs["error_type"] = type(error).__name__

            self._add_event("chunk_retry", event_attrs)

    def record_retry(self, error: Optional[Exception] = None):
        """
        Record a retry attempt during transfer.

        Args:
            error: Optional exception that caused the retry
        """
        with self._lock:
            self.retry_count += 1

            # Update metrics
            retry_attributes = dict(self.metric_attributes)
            if error:
                retry_attributes["error_type"] = type(error).__name__
            retry_counter.add(1, retry_attributes)

            # Update trace attributes
            self.span.set_attribute("synapse.operation.retry_count", self.retry_count)

            event_attrs = {"retry_number": self.retry_count}
            if error:
                event_attrs["error"] = str(error)
                event_attrs["error_type"] = type(error).__name__

            self._add_event("transfer_retry", event_attrs)

    def record_cache_hit(self, hit: bool = True):
        """
        Record whether a cache hit occurred.

        Args:
            hit: Whether the cache was hit (True) or missed (False)
        """
        with self._lock:
            self.span.set_attribute("synapse.cache.hit", hit)

            # Record in metrics
            cache_attributes = dict(self.metric_attributes)
            cache_attributes["cache_hit"] = hit
            cache_counter.add(1, cache_attributes)

            self._add_event("cache_access", {"hit": hit})

    def record_final_statistics(self):
        """
        Record final throughput statistics at the end of the transfer.
        """
        with self._lock:
            if not self._throughput_samples:
                return

            # Calculate statistics only if we have samples
            try:
                avg_throughput = sum(self._throughput_samples) / len(self._throughput_samples)
                max_throughput = max(self._throughput_samples)
                min_throughput = min(self._throughput_samples)

                # Record in trace for detailed analysis
                self.span.set_attribute("synapse.transfer.throughput.avg_mbps", avg_throughput)
                self.span.set_attribute("synapse.transfer.throughput.max_mbps", max_throughput)
                self.span.set_attribute("synapse.transfer.throughput.min_mbps", min_throughput)

                if len(self._throughput_samples) >= 2:
                    # Only calculate standard deviation if we have multiple samples
                    stdev = statistics.stdev(self._throughput_samples)
                    self.span.set_attribute("synapse.transfer.throughput.stdev_mbps", stdev)

                    # Calculate jitter (variability)
                    jitter = stdev / avg_throughput if avg_throughput > 0 else 0
                    self.span.set_attribute("synapse.transfer.throughput.jitter", jitter)

                    # Calculate and record percentiles
                    sorted_samples = sorted(self._throughput_samples)
                    median = sorted_samples[len(sorted_samples) // 2]
                    self.span.set_attribute("synapse.transfer.throughput.median_mbps", median)

                    # Record 95th percentile if we have enough samples
                    if len(sorted_samples) >= 5:
                        p95_index = int(len(sorted_samples) * 0.95)
                        p95 = sorted_samples[p95_index]
                        self.span.set_attribute("synapse.transfer.throughput.p95_mbps", p95)
            except Exception:
                # Don't fail the transfer if statistics calculation fails
                pass

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
