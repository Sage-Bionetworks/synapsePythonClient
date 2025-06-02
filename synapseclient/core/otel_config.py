"""OpenTelemetry configuration for Synapse Python Client."""
import os
import sys
from typing import Any, Dict, List, Optional

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span, SpanContext, get_current_span

# Default service name for the client
DEFAULT_SERVICE_NAME = "synapseclient"
CLIENT_VERSION = "unspecified"


class AttributePropagatingSpanProcessor(SpanProcessor):
    """A custom span processor that propagates specific attributes from the parent span
    to the child span when the child span is started.
    It also propagates the attributes to the parent span when the child span ends.

    Args:
        SpanProcessor (opentelemetry.sdk.trace.SpanProcessor): The base class that provides hooks for processing spans during their lifecycle
    """

    def __init__(
        self,
        attributes_to_propagate_to_child: List[str] = None,
        attributes_to_propagate_to_parent: List[str] = None,
    ) -> None:
        self.attributes_to_propagate_to_child = attributes_to_propagate_to_child or []
        self.attributes_to_propagate_to_parent = attributes_to_propagate_to_parent or []

    def on_start(self, span: Span, parent_context: SpanContext) -> None:
        """Propagates attributes from the parent span to the child span.
        Arguments:
            span: The child span to which the attributes should be propagated.
            parent_context: The context of the parent span.
        Returns:
            None
        """
        parent_span = get_current_span()
        if parent_span is not None and parent_span.is_recording():
            for attribute in self.attributes_to_propagate_to_child:
                # Check if the attribute exists in the parent span's attributes
                attribute_val = parent_span.attributes.get(attribute)
                if attribute_val:
                    # Propagate the attribute to the current span
                    span.set_attribute(attribute, attribute_val)

    def on_end(self, span: Span) -> None:
        """Propagates attributes from the child span back to the parent span"""
        parent_span = get_current_span()
        if parent_span is not None and parent_span.is_recording():
            for attribute in self.attributes_to_propagate_to_parent:
                child_val = span.attributes.get(attribute)
                parent_val = parent_span.attributes.get(attribute)
                if child_val and not parent_val:
                    # Propagate the attribute back to parent span
                    parent_span.set_attribute(attribute, child_val)

    def shutdown(self) -> None:
        """No-op method that does nothing when the span processor is shut down."""
        pass

    def force_flush(self, timeout_millis: int = 30000) -> None:
        """No-op method that does nothing when the span processor is forced to flush."""
        pass


def configure_traces(
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
    service_name = service_name or os.environ.get(
        "OTEL_SERVICE_NAME", DEFAULT_SERVICE_NAME
    )
    endpoint = endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    # Create resource with service information
    resource_attrs = {
        "service.name": service_name,
        "service.version": CLIENT_VERSION,
    }

    # Include context information
    if include_context:
        # Add Python version information
        resource_attrs["python.version"] = ".".join(
            str(v) for v in sys.version_info[:3]
        )

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

    attribute_propagator = AttributePropagatingSpanProcessor(
        attributes_to_propagate_to_child=[
            "synapse.transfer.direction",
            "synapse.operation.category",
        ]
    )
    provider.add_span_processor(attribute_propagator)

    # Add exporters based on configuration
    if endpoint:
        # Configure OTLP HTTP exporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=f"{endpoint}/v1/traces", headers=headers
        )
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    if export_console or os.environ.get("OTEL_DEBUG_CONSOLE", "").lower() in (
        "true",
        "1",
    ):
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
    service_name = service_name or os.environ.get(
        "OTEL_SERVICE_NAME", DEFAULT_SERVICE_NAME
    )
    endpoint = endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    # Create resource with service information
    resource_attrs = {
        "service.name": service_name,
        "service.version": CLIENT_VERSION,
    }

    # Include context information
    if include_context:
        # Add Python version information
        resource_attrs["python.version"] = ".".join(
            str(v) for v in sys.version_info[:3]
        )

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
        otlp_metric_exporter = OTLPMetricExporter(
            endpoint=f"{endpoint}/v1/metrics", headers=headers
        )
        readers.append(PeriodicExportingMetricReader(otlp_metric_exporter))

    # Add console exporter for debugging if requested
    if export_console or os.environ.get("OTEL_DEBUG_CONSOLE", "").lower() in (
        "true",
        "1",
    ):
        console_metric_exporter = ConsoleMetricExporter()
        readers.append(PeriodicExportingMetricReader(console_metric_exporter))

    # Create and configure the meter provider
    provider = MeterProvider(resource=resource, metric_readers=readers)

    # Set as global meter provider
    metrics.set_meter_provider(provider)

    return provider


def get_tracer(name: Optional[str] = None) -> trace.Tracer:
    """
    Get a tracer with the specified name or default to the service name.

    Args:
        name: Optional tracer name

    Returns:
        An OpenTelemetry tracer
    """
    return trace.get_tracer(name or DEFAULT_SERVICE_NAME)


def get_meter(name: Optional[str] = None):
    """
    Get a meter with the specified name or default to the service name.

    Args:
        name: Optional meter name

    Returns:
        An OpenTelemetry meter
    """
    return metrics.get_meter(name or DEFAULT_SERVICE_NAME)
