"""
Synapse Curator Extensions

This module provides library functions for metadata curation tasks in Synapse.
"""

from .file_based_metadata_task import create_file_based_metadata_task
from .record_based_metadata_task import create_record_based_metadata_task
from .schema_registry import query_schema_registry
from .schematic_code import convert, generate_jsonschema

__all__ = [
    "create_file_based_metadata_task",
    "create_record_based_metadata_task",
    "query_schema_registry",
    # TODO: Rename these
    "convert",
    "generate_jsonschema",
]
