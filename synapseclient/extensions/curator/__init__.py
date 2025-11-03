"""
Synapse Curator Extensions

This module provides library functions for metadata curation tasks in Synapse.
"""

from .file_based_metadata_task import create_file_based_metadata_task
from .record_based_metadata_task import create_record_based_metadata_task
from .schema_generation import generate_jsonld, generate_jsonschema
from .schema_registry import query_schema_registry

__all__ = [
    "create_file_based_metadata_task",
    "create_record_based_metadata_task",
    "query_schema_registry",
    "generate_jsonld",
    "generate_jsonschema",
]
