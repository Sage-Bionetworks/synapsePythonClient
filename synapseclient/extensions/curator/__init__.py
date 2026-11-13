"""
Synapse Curator Extensions

This module provides library functions for metadata curation tasks in Synapse.
"""

from .curation_task_types import (
    ArchiveEntry,
    ArchiveResult,
    CurationManifestError,
    CurationProblem,
    CurationTaskManifest,
    CurationTaskRow,
    ProblemKind,
    RowChanges,
    RowDifference,
    TaskAction,
    TaskOutcome,
    TaskPlan,
    TaskResult,
)
from .curation_tasks import (
    apply_curation_tasks,
    apply_curation_tasks_async,
    plan_curation_tasks,
    plan_curation_tasks_async,
)
from .file_based_metadata_task import create_file_based_metadata_task
from .get_or_create_grid import (
    get_or_create_curator_grid,
    get_or_create_curator_grid_async,
)
from .record_based_metadata_task import (
    create_record_based_metadata_task,
    create_record_based_metadata_task_async,
)
from .schema_generation import generate_jsonld, generate_jsonschema
from .schema_management import (
    bind_jsonschema,
    bind_jsonschema_async,
    register_jsonschema,
    register_jsonschema_async,
)
from .schema_registry import query_schema_registry

__all__ = [
    "get_or_create_curator_grid",
    "get_or_create_curator_grid_async",
    "create_file_based_metadata_task",
    "create_record_based_metadata_task",
    "create_record_based_metadata_task_async",
    "query_schema_registry",
    "generate_jsonld",
    "generate_jsonschema",
    "register_jsonschema",
    "register_jsonschema_async",
    "bind_jsonschema",
    "bind_jsonschema_async",
    "CurationTaskManifest",
    "CurationTaskRow",
    "CurationManifestError",
    "plan_curation_tasks",
    "plan_curation_tasks_async",
    "apply_curation_tasks",
    "apply_curation_tasks_async",
    "TaskPlan",
    "TaskAction",
    "RowChanges",
    "RowDifference",
    "ArchiveEntry",
    "ArchiveResult",
    "CurationProblem",
    "ProblemKind",
    "TaskOutcome",
    "TaskResult",
]
