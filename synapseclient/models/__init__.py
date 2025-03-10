# These are all of the models that are used by the Synapse client.
from synapseclient.models.activity import Activity, UsedEntity, UsedURL
from synapseclient.models.agent import (
    Agent,
    AgentPrompt,
    AgentSession,
    AgentSessionAccessLevel,
)
from synapseclient.models.annotations import Annotations
from synapseclient.models.dataset import Dataset
from synapseclient.models.file import File, FileHandle
from synapseclient.models.folder import Folder
from synapseclient.models.mixins.table_components import QueryMixin
from synapseclient.models.project import Project
from synapseclient.models.services import FailureStrategy
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
    # Dataset model
    "Dataset",
]

# Static methods to expose as functions
query_async = QueryMixin.query_async
# TODO: Static typing on this might cause issues since it's created dynamically
query = QueryMixin.query
query_part_mask_async = QueryMixin.query_part_mask_async
query_part_mask = QueryMixin.query_part_mask
