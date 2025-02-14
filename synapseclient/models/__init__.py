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
from synapseclient.models.mixins.table_operator import (
    Column,
    ColumnExpansionStrategy,
    ColumnType,
    FacetType,
    SchemaStorageStrategy,
)
from synapseclient.models.project import Project
from synapseclient.models.services import FailureStrategy
from synapseclient.models.table import Table
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
    # Dataset model
    "Dataset",
]

# Static methods to expose as functions
query_async = Table.query_async
query = Table.query
