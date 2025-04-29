# These are all of the models that are used by the Synapse client.
from synapseclient.models.activity import Activity, UsedEntity, UsedURL
from synapseclient.models.agent import (
    Agent,
    AgentPrompt,
    AgentSession,
    AgentSessionAccessLevel,
)
from synapseclient.models.annotations import Annotations
from synapseclient.models.dataset import Dataset, DatasetCollection, EntityRef
from synapseclient.models.entityview import EntityView, ViewTypeMask
from synapseclient.models.file import File, FileHandle
from synapseclient.models.folder import Folder
from synapseclient.models.materializedview import MaterializedView
from synapseclient.models.mixins.table_components import QueryMixin
from synapseclient.models.project import Project
from synapseclient.models.services import FailureStrategy
from synapseclient.models.submissionview import SubmissionView
from synapseclient.models.table import Table
from synapseclient.models.table_components import (
    AppendableRowSetRequest,
    Column,
    ColumnChange,
    ColumnExpansionStrategy,
    ColumnType,
    CsvTableDescriptor,
    FacetType,
    JsonSubColumn,
    PartialRow,
    PartialRowSet,
    QueryResultBundle,
    SchemaStorageStrategy,
    TableSchemaChangeRequest,
    TableUpdateTransaction,
    UploadToTableRequest,
)
from synapseclient.models.team import Team, TeamMember
from synapseclient.models.user import UserPreference, UserProfile
from synapseclient.models.virtualtable import VirtualTable

__all__ = [
    "Activity",
    "UsedURL",
    "UsedEntity",
    "FailureStrategy",
    "File",
    "FileHandle",
    "Folder",
    "Project",
    "Annotations",
    "Team",
    "TeamMember",
    "UserProfile",
    "UserPreference",
    "Agent",
    "AgentSession",
    "AgentSessionAccessLevel",
    "AgentPrompt",
    # EntityView models
    "EntityView",
    "ViewTypeMask",
    # Table models
    "SchemaStorageStrategy",
    "ColumnExpansionStrategy",
    "Table",
    "Column",
    "ColumnType",
    "FacetType",
    "JsonSubColumn",
    "QueryResultBundle",
    "query_async",
    "query",
    "query_part_mask_async",
    "query_part_mask",
    "ColumnChange",
    "PartialRow",
    "PartialRowSet",
    "TableSchemaChangeRequest",
    "AppendableRowSetRequest",
    "UploadToTableRequest",
    "TableUpdateTransaction",
    "CsvTableDescriptor",
    "MaterializedView",
    "VirtualTable",
    # Dataset models
    "Dataset",
    "EntityRef",
    "DatasetCollection",
    # Submission models
    "SubmissionView",
]

# Static methods to expose as functions
query_async = QueryMixin.query_async
query = QueryMixin.query
query_part_mask_async = QueryMixin.query_part_mask_async
query_part_mask = QueryMixin.query_part_mask
