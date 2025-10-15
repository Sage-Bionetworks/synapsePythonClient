# These are all of the models that are used by the Synapse client.
from synapseclient.models.activity import Activity, UsedEntity, UsedURL
from synapseclient.models.agent import (
    Agent,
    AgentPrompt,
    AgentSession,
    AgentSessionAccessLevel,
)
from synapseclient.models.annotations import Annotations
from synapseclient.models.curation import (
    CurationTask,
    FileBasedMetadataTaskProperties,
    Grid,
    RecordBasedMetadataTaskProperties,
)
from synapseclient.models.dataset import Dataset, DatasetCollection, EntityRef
from synapseclient.models.entityview import EntityView, ViewTypeMask
from synapseclient.models.file import File, FileHandle
from synapseclient.models.folder import Folder
from synapseclient.models.link import Link
from synapseclient.models.materializedview import MaterializedView
from synapseclient.models.mixins.table_components import QueryMixin
from synapseclient.models.project import Project
from synapseclient.models.recordset import RecordSet
from synapseclient.models.services import FailureStrategy
from synapseclient.models.submissionview import SubmissionView
from synapseclient.models.table import Table
from synapseclient.models.table_components import (
    ActionRequiredCount,
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
    Query,
    QueryBundleRequest,
    QueryJob,
    QueryNextPageToken,
    QueryResult,
    QueryResultBundle,
    QueryResultOutput,
    Row,
    RowSet,
    SchemaStorageStrategy,
    SelectColumn,
    SumFileSizes,
    TableSchemaChangeRequest,
    TableUpdateTransaction,
    UploadToTableRequest,
)
from synapseclient.models.team import Team, TeamMember, TeamMembershipStatus
from synapseclient.models.user import UserGroupHeader, UserPreference, UserProfile
from synapseclient.models.virtualtable import VirtualTable

__all__ = [
    "Activity",
    "UsedURL",
    "UsedEntity",
    "FailureStrategy",
    "File",
    "FileHandle",
    "Folder",
    "Link",
    "Project",
    "RecordSet",
    "Annotations",
    "Team",
    "TeamMember",
    "TeamMembershipStatus",
    "CurationTask",
    "FileBasedMetadataTaskProperties",
    "RecordBasedMetadataTaskProperties",
    "Grid",
    "UserProfile",
    "UserPreference",
    "UserGroupHeader",
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
    "SumFileSizes",
    "FacetType",
    "JsonSubColumn",
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
    "ActionRequiredCount",
    "QueryBundleRequest",
    "QueryNextPageToken",
    "QueryResult",
    "QueryResultBundle",
    "QueryResultOutput",
    "QueryJob",
    "Query",
    "Row",
    "RowSet",
    "SelectColumn",
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
