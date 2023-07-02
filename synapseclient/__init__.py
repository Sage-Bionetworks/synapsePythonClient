import importlib.resources

import json
import requests  # ensure user-agent is set to track Synapse Python client usage

from .activity import Activity
from .annotations import Annotations
from .client import PUBLIC, AUTHENTICATED_USERS

# public APIs
from .client import Synapse, login
from .core.version_check import check_for_updates, release_notes
from .entity import Entity, Project, Folder, File, Link, DockerRepository
from .evaluation import Evaluation, Submission, SubmissionStatus
from .table import (
    Schema,
    EntityViewSchema,
    Column,
    RowSet,
    Row,
    as_table_columns,
    Table,
    PartialRowset,
    EntityViewType,
    build_table,
    SubmissionViewSchema,
    MaterializedViewSchema,
    Dataset,
)
from .team import Team, UserProfile, UserGroupHeader, TeamMember
from .wiki import Wiki

# ref = importlib.resources.files(__name__).joinpath("synapsePythonClient")
# with ref.open("r") as fp:
#     __version__ = json.load(fp)["latestVersion"]
# TODO: switch to the above after python 3.8 is deprecated
with importlib.resources.path(__name__, "synapsePythonClient") as ref:
    __version__ = json.load(open(ref))["latestVersion"]

__all__ = [
    # objects
    "Synapse",
    "Activity",
    "Entity",
    "Project",
    "Folder",
    "File",
    "Link",
    "DockerRepository",
    "Evaluation",
    "Submission",
    "SubmissionStatus",
    "Schema",
    "EntityViewSchema",
    "Column",
    "Row",
    "RowSet",
    "Table",
    "PartialRowset",
    "Team",
    "UserProfile",
    "UserGroupHeader",
    "TeamMember",
    "Wiki",
    "Annotations",
    "SubmissionViewSchema",
    "MaterializedViewSchema",
    "Dataset",
    # functions
    "login",
    "build_table",
    "as_table_columns",
    "check_for_updates",
    "release_notes",
    # enum
    "EntityViewType",
    # constants
    "PUBLIC",
    "AUTHENTICATED_USERS",
]

USER_AGENT = {
    "User-Agent": "synapseclient/%s %s"
    % (__version__, requests.utils.default_user_agent())
}

# patch json
from .core.models import custom_json  # noqa

# patch logging
from .core import logging_setup  # noqa
