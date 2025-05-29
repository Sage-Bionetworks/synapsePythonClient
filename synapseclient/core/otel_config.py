"""OpenTelemetry configuration for Synapse Python Client."""
import os
import platform
import sys
import socket
from typing import Dict, Optional, Any

from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter

# Default service name for the client
DEFAULT_SERVICE_NAME = "synapseclient"
CLIENT_VERSION = "unspecified"


def configure_telemetry(
    service_name: Optional[str] = None,
    endpoint: Optional[str] = None,
    export_console: bool = False,
    headers: Optional[Dict[str, str]] = None,
    resource_attributes: Optional[Dict[str, Any]] = None,
    include_context: bool = True,
) -> TracerProvider:
    """
    Configure OpenTelemetry tracing for the Synapse Python Client.

    Args:
        service_name: Name of the service for telemetry identification
        endpoint: OTLP endpoint URL (if None, uses OTEL_EXPORTER_OTLP_ENDPOINT env var)
        export_console: Whether to also export spans to console for debugging
        headers: Optional headers for the OTLP exporter
        resource_attributes: Additional resource attributes to include
        include_context: Whether to include contextual information about the runtime environment

    Returns:
        The configured TracerProvider
    """
    # Get configuration from environment variables if not provided
    service_name = service_name or os.environ.get("OTEL_SERVICE_NAME", DEFAULT_SERVICE_NAME)
    endpoint = endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    # Create resource with service information
    resource_attrs = {
        "service.name": service_name,
        "service.version": CLIENT_VERSION,
    }

    # Include context information
    if include_context:
        # Add Python version information
        resource_attrs["python.version"] = ".".join(str(v) for v in sys.version_info[:3])

        # Add OS information
        resource_attrs["os.type"] = os.name

        # Add client identification
        try:
            from synapseclient import __version__ as client_version
            resource_attrs["service.version"] = client_version
        except ImportError:
            # Use default version
            pass

    # Add any user-provided resource attributes
    if resource_attributes:
        resource_attrs.update(resource_attributes)

    resource = Resource.create(resource_attrs)

    # Configure tracer provider
    provider = TracerProvider(resource=resource)

    # Add exporters based on configuration
    if endpoint:
        # Configure OTLP HTTP exporter
        otlp_exporter = OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces", headers=headers)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    if export_console or os.environ.get("OTEL_DEBUG_CONSOLE", "").lower() in ("true", "1"):
        # Add console exporter for debugging
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # Set as global tracer provider
    trace.set_tracer_provider(provider)

    return provider


def configure_metrics(
    service_name: Optional[str] = None,
    endpoint: Optional[str] = None,
    export_console: bool = False,
    headers: Optional[Dict[str, str]] = None,
    resource_attributes: Optional[Dict[str, Any]] = None,
    include_context: bool = True,
) -> MeterProvider:
    """
    Configure OpenTelemetry metrics for the Synapse Python Client.

    Args:
        service_name: Name of the service for telemetry identification
        endpoint: OTLP endpoint URL (if None, uses OTEL_EXPORTER_OTLP_ENDPOINT env var)
        export_console: Whether to also export metrics to console for debugging
        headers: Optional headers for the OTLP exporter
        resource_attributes: Additional resource attributes to include
        include_context: Whether to include contextual information about the runtime environment

    Returns:
        The configured MeterProvider
    """
    # Get configuration from environment variables if not provided
    service_name = service_name or os.environ.get("OTEL_SERVICE_NAME", DEFAULT_SERVICE_NAME)
    endpoint = endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    # Create resource with service information
    resource_attrs = {
        "service.name": service_name,
        "service.version": CLIENT_VERSION,
    }

    # Include context information
    if include_context:
        # Add Python version information
        resource_attrs["python.version"] = ".".join(str(v) for v in sys.version_info[:3])

        # Add OS information
        resource_attrs["os.type"] = os.name

        # Add client identification
        try:
            from synapseclient import __version__ as client_version
            resource_attrs["service.version"] = client_version
        except ImportError:
            # Use default version
            pass

    # Add any user-provided resource attributes
    if resource_attributes:
        resource_attrs.update(resource_attributes)

    resource = Resource.create(resource_attrs)

    # Set up metric readers
    readers = []

    # Configure OTLP HTTP exporter if endpoint is provided
    if endpoint:
        otlp_metric_exporter = OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics", headers=headers)
        readers.append(PeriodicExportingMetricReader(otlp_metric_exporter))

    # Add console exporter for debugging if requested
    if export_console or os.environ.get("OTEL_DEBUG_CONSOLE", "").lower() in ("true", "1"):
        console_metric_exporter = ConsoleMetricExporter()
        readers.append(PeriodicExportingMetricReader(console_metric_exporter))

    # Create and configure the meter provider
    provider = MeterProvider(resource=resource, metric_readers=readers)

    # Set as global meter provider
    metrics.set_meter_provider(provider)

    return provider


def get_tracer(name: Optional[str] = None, version: Optional[str] = None) -> trace.Tracer:
    """
    Get a tracer with the specified name or default to the service name.

    Args:
        name: Optional tracer name
        version: Optional version identifier

    Returns:
        An OpenTelemetry tracer
    """
    # Determine the version to use (passed in, from client, or default)
    if version is None:
        try:
            from synapseclient import __version__ as client_version
            version = client_version
        except ImportError:
            version = CLIENT_VERSION

    return trace.get_tracer(name or DEFAULT_SERVICE_NAME, version)


def get_meter(name: Optional[str] = None, version: Optional[str] = None):
    """
    Get a meter with the specified name or default to the service name.

    Args:
        name: Optional meter name
        version: Optional version identifier

    Returns:
        An OpenTelemetry meter
    """
    # Determine the version to use (passed in, from client, or default)
    if version is None:
        try:
            from synapseclient import __version__ as client_version
            version = client_version
        except ImportError:
            version = CLIENT_VERSION

    return metrics.get_meter(name or DEFAULT_SERVICE_NAME, version)
