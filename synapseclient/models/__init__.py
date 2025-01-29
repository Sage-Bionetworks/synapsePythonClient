# These are all of the models that are used by the Synapse client.
from synapseclient.models.activity import Activity, UsedEntity, UsedURL
from synapseclient.models.agent import (
    Agent,
    AgentPrompt,
    AgentSession,
    AgentSessionAccessLevel,
)
from synapseclient.models.annotations import Annotations
from synapseclient.models.file import File, FileHandle
from synapseclient.models.folder import Folder
from synapseclient.models.project import Project
from synapseclient.models.services import FailureStrategy
from synapseclient.models.table import (
    Column,
    ColumnExpansionStrategy,
    ColumnType,
    FacetType,
    RowsetResultFormat,
    SchemaStorageStrategy,
    Table,
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
    "FacetType",
    "CsvResultFormat",
    "RowsetResultFormat",
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
    "query_async",
    "query",
]

# Static methods to expose as functions
query_async = Table.query_async
query = Table.query
