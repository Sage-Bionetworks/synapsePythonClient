"""SearchIndex entity model.

A SearchIndex is a Synapse entity whose content is defined by a Synapse SQL query
(`defining_sql`). An OpenSearch index is built from the query results, supporting
full-text search, faceted search, and autocomplete.
"""

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.activity import Activity
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import (
    DeleteMixin,
    GetMixin,
    QueryMixin,
    TableBase,
    TableStoreMixin,
)
from synapseclient.models.table_components import Column


class SearchIndexSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SearchIndex operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store metadata about a SearchIndex including the annotations."""
        return self

    def get(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the SearchIndex from Synapse."""
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the SearchIndex from Synapse."""
        return None


@dataclass
@async_to_sync
class SearchIndex(
    SearchIndexSynchronousProtocol,
    AccessControllable,
    TableBase,
    TableStoreMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
):
    """
    A SearchIndex is a Synapse entity whose content is defined by a Synapse SQL
    query (`defining_sql`). An OpenSearch index is built from the query results,
    supporting full-text search, faceted search, and autocomplete.

    The `defining_sql` must reference exactly one table-like entity. Multi-entity
    JOIN/UNION queries are not supported. Optionally, a `search_configuration_id`
    may be supplied to control the analyzer/synonym settings used when building
    the index. If not specified, the configuration is resolved by walking up
    the entity hierarchy.

    REST API model: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchIndex.html>

    Attributes:
        id: The unique immutable ID for this entity.
        name: The name of this entity.
        description: The description of this entity.
        etag: Synapse OCC etag.
        created_on: Date this entity was created.
        modified_on: Date this entity was last modified.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        parent_id: The ID of the parent entity.
        version_number: The version number issued to this version on the object.
        version_label: The version label for this entity.
        version_comment: The version comment for this entity.
        is_latest_version: If this is the latest version of the object.
        columns: (Read Only) Columns derived from `defining_sql`.
        defining_sql: The Synapse SQL statement that defines which columns and
            rows are indexed.
        search_configuration_id: ID of the SearchConfiguration to apply when
            building this index. Optional.
        annotations: Additional metadata associated with the entity.
        activity: Provenance for this entity.

    Example: Create a new SearchIndex.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SearchIndex

        syn = Synapse()
        syn.login()

        index = SearchIndex(
            name="My Search Index",
            parent_id="syn12345",
            defining_sql="SELECT * FROM syn67890",
        )
        index = index.store()
        print(f"Created SearchIndex: {index.id}")
        ```
    """

    id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    etag: Optional[str] = field(default=None, compare=False)
    created_on: Optional[str] = field(default=None, compare=False)
    modified_on: Optional[str] = field(default=None, compare=False)
    created_by: Optional[str] = field(default=None, compare=False)
    modified_by: Optional[str] = field(default=None, compare=False)
    parent_id: Optional[str] = None
    version_number: Optional[int] = field(default=None, compare=False)
    version_label: Optional[str] = None
    version_comment: Optional[str] = None
    is_latest_version: Optional[bool] = field(default=None, compare=False)

    columns: Optional[OrderedDict[str, Column]] = field(
        default_factory=OrderedDict, compare=False
    )
    """(Read Only) Columns of a SearchIndex are derived from the defining SQL."""

    defining_sql: Optional[str] = None
    """The Synapse SQL statement that defines which columns and rows are indexed.
    Must reference exactly one entity."""

    search_configuration_id: Optional[str] = None
    """The ID of the SearchConfiguration to apply when building this search
    index. If not provided, the system will check for a search configuration
    binding on the parent project/folder hierarchy, or use platform defaults."""

    _last_persistent_instance: Optional["SearchIndex"] = field(
        default=None, repr=False, compare=False
    )

    annotations: Optional[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ]
    ] = field(default_factory=dict, compare=False)

    activity: Optional[Activity] = field(default=None, compare=False)

    @property
    def has_changed(self) -> bool:
        """Checks if the object has changed since the last persistent instance."""
        return self._last_persistent_instance != self

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = replace(self)
        self._last_persistent_instance.activity = (
            replace(self.activity) if self.activity and self.activity.id else None
        )
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else {}
        )

    def fill_from_dict(
        self, entity: Dict[str, Any], set_annotations: bool = True
    ) -> "SearchIndex":
        """Populate this dataclass from a Synapse REST API entity dict."""
        self.id = entity.get("id", None)
        self.name = entity.get("name", None)
        self.description = entity.get("description", None)
        self.parent_id = entity.get("parentId", None)
        self.etag = entity.get("etag", None)
        self.created_on = entity.get("createdOn", None)
        self.created_by = entity.get("createdBy", None)
        self.modified_on = entity.get("modifiedOn", None)
        self.modified_by = entity.get("modifiedBy", None)
        self.version_number = entity.get("versionNumber", None)
        self.version_label = entity.get("versionLabel", None)
        self.version_comment = entity.get("versionComment", None)
        self.is_latest_version = entity.get("isLatestVersion", None)
        self.defining_sql = entity.get("definingSQL", None)
        self.search_configuration_id = entity.get("searchConfigurationId", None)

        if set_annotations:
            self.annotations = entity.get("annotations", {})

        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """Convert the request to the body expected by the Synapse REST API."""
        entity = {
            "name": self.name,
            "description": self.description,
            "id": self.id,
            "etag": self.etag,
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "concreteType": concrete_types.SEARCH_INDEX_ENTITY,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "definingSQL": self.defining_sql,
            "searchConfigurationId": self.search_configuration_id,
        }
        delete_none_keys(entity)
        result = {"entity": entity}
        delete_none_keys(result)
        return result

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Asynchronously store the SearchIndex entity."""
        if not self.defining_sql:
            raise ValueError(
                "The defining_sql attribute must be set for a SearchIndex."
            )
        return await super().store_async(
            dry_run=dry_run, job_timeout=job_timeout, synapse_client=synapse_client
        )

    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Asynchronously fetch the SearchIndex metadata."""
        return await super().get_async(
            include_columns=include_columns,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Asynchronously delete this SearchIndex from Synapse."""
        await super().delete_async(synapse_client=synapse_client)
