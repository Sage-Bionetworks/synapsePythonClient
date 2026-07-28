"""SearchIndex entity model.

A SearchIndex is a Synapse entity whose content is defined by a Synapse SQL query
(`defining_sql`). An OpenSearch index is built from the query results, supporting
full-text search, faceted search, and autocomplete.
"""

from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import (
    delete_none_keys,
    log_dataclass_diff,
    merge_dataclass_entities,
)
from synapseclient.models.activity import Activity
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import DeleteMixin, GetMixin
from synapseclient.models.protocols.search_index_protocol import (
    SearchIndexSynchronousProtocol,
)
from synapseclient.models.search_dsl import Query, SourceFilter
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity import store_entity
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)

if TYPE_CHECKING:
    from synapseclient.models.search_management import (
        SearchHit,
        SearchIndexQuery,
        SearchQuery,
        SearchQueryPart,
    )


@dataclass
@async_to_sync
class SearchIndex(
    SearchIndexSynchronousProtocol,
    AccessControllable,
    DeleteMixin,
    GetMixin,
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
        defining_sql: The Synapse SQL statement that defines which columns and
            rows are indexed.
        search_configuration_id: ID of the SearchConfiguration to apply when
            building this index. Optional.
        annotations: Additional metadata associated with the entity.
        activity: Provenance for this entity.

    Example: Create a new SearchIndex.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SearchIndex

        syn = Synapse()
        syn.login()

        index = SearchIndex(
            name="My Search Index",
            parent_id="syn12345",
            # syn67890 must be a table or a view; multi-entity JOINs are not supported
            defining_sql="SELECT * FROM syn67890",
        )
        index = index.store()
        print(f"Created SearchIndex: {index.id}")
        ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for
    new Entities. Once issued, this ID is guaranteed to never change or be
    re-issued."""

    name: Optional[str] = None
    """The name of this entity. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus
    signs, apostrophes, and parentheses."""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = field(default=None, compare=False)
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is
    updated it is used to detect when a client's current representation of an
    entity is out-of-date."""

    created_on: Optional[str] = field(default=None, compare=False)
    """The date this entity was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The date this entity was last modified."""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this entity."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this Entity."""

    version_number: Optional[int] = field(default=None, compare=False)
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this entity."""

    version_comment: Optional[str] = None
    """The version comment for this entity."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """If this is the latest version of the object."""

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
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

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
        """Convert this dataclass into the entity body expected by the Synapse
        REST API."""
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
        return entity

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"SearchIndex_Store: {self.name}"
    )
    async def store_async(
        self,
        dry_run: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Asynchronously store the SearchIndex entity. Creates a new SearchIndex
        if `id` is not set, or updates the existing one otherwise. `defining_sql`
        must be set before calling this.

        Arguments:
            dry_run: If True, will not actually store the SearchIndex but will log
                to the console what would be created or updated.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself.

        Raises:
            ValueError: If `defining_sql` is not set.

        Example: Create a new SearchIndex.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            async def main():
                syn = Synapse()
                syn.login()

                index = SearchIndex(
                    name="My Search Index",
                    parent_id="syn12345",
                    # syn67890 must be a table or a view;
                    defining_sql="SELECT * FROM syn67890",
                )
                index = await index.store_async()
                print(f"Created SearchIndex: {index.id}")

            asyncio.run(main())
            ```
        """
        if not self.defining_sql:
            raise ValueError(
                "The defining_sql attribute must be set for a SearchIndex."
            )
        client = Synapse.get_client(synapse_client=synapse_client)

        if (
            (not self._last_persistent_instance)
            and (
                existing_id := await get_id(
                    entity=self, failure_strategy=None, synapse_client=synapse_client
                )
            )
            and (
                existing_index := await SearchIndex(id=existing_id).get_async(
                    synapse_client=synapse_client
                )
            )
        ):
            merge_dataclass_entities(
                source=existing_index, destination=self, logger=client.logger
            )

        if dry_run:
            client.logger.info(
                f"[{self.id}:{self.name}]: Dry run enabled. No changes will be made."
            )
            if self.has_changed:
                log_dataclass_diff(
                    logger=client.logger,
                    prefix=f"[{self.id}:{self.name}]: ",
                    obj1=self._last_persistent_instance or SearchIndex(),
                    obj2=self,
                    fields_to_ignore=["_last_persistent_instance"],
                )
            return self

        if self.has_changed:
            entity = await store_entity(
                resource=self,
                entity=self.to_synapse_request(),
                synapse_client=synapse_client,
            )
            self.fill_from_dict(entity=entity, set_annotations=False)

        re_read_required = await store_entity_components(
            root_resource=self,
            failure_strategy=FailureStrategy.RAISE_EXCEPTION,
            synapse_client=synapse_client,
        )
        if re_read_required:
            await self.get_async(synapse_client=synapse_client)
        self._set_last_persistent_instance()

        return self

    async def get_async(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Asynchronously fetch the SearchIndex metadata. Either `id`, or `name`
        and `parent_id`, must be set before calling this.

        Arguments:
            include_activity: If True, will include the provenance activity on
                the returned SearchIndex.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated from the Synapse response.

        Example: Get a SearchIndex by ID.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            async def main():
                syn = Synapse()
                await syn.login_async()

                index = await SearchIndex(id="syn12345").get_async()
                print(index.name, index.defining_sql)

            asyncio.run(main())
            ```
        """
        return await super().get_async(
            include_columns=False,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Asynchronously delete this SearchIndex from Synapse. `id` must be set
        before calling this.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a SearchIndex by ID.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            async def main():
                syn = Synapse()
                await syn.login_async()

                await SearchIndex(id="syn12345").delete_async()

            asyncio.run(main())
            ```
        """
        await super().delete_async(synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"SearchIndex_Query: {self.id}"
    )
    async def query_async(
        self,
        search_query: "SearchQuery",
        response_parts: Optional[List["SearchQueryPart"]] = None,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "SearchIndexQuery":
        """Asynchronously query this search index. Unlike a SQL-backed Table, a
        SearchIndex is queried with the
        [OpenSearch Query DSL](https://docs.opensearch.org/latest/query-dsl/)
        carried by a [SearchQuery][synapseclient.models.SearchQuery] — not with
        Synapse SQL. See [Query][https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/dsl/Query.html] for the
        supported clause kinds.

        Arguments:
            search_query: The OpenSearch
                [`_search`](https://docs.opensearch.org/latest/api-reference/search-apis/search/)
                body to execute against this index.
            response_parts: Additional response parts to request beyond the
                default hits, such as the total hit count or the select columns.
            job_timeout: The maximum amount of time to wait for the query job to
                complete before raising a `SynapseTimeoutError`.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The completed [SearchIndexQuery][synapseclient.models.SearchIndexQuery], carrying the `hits` and any requested response parts.

        Raises:
            ValueError: If the `id` attribute has not been set.

        Example: Query an index for documents mentioning "alzheimer".
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex, SearchQuery, SearchQueryPart

            async def main():
                syn = Synapse()
                await syn.login_async()

                results = await SearchIndex(id="syn12345").query_async(
                    search_query=SearchQuery(
                        query={"match": {"title": {"query": "alzheimer"}}},
                        size=10,
                    ),
                    response_parts=[SearchQueryPart.TOTAL_HITS],
                )
                print(results.total_hits)
                for hit in results.hits:
                    print(hit.row_id, hit.fields)

            asyncio.run(main())
            ```
        """
        from synapseclient.models.search_management import SearchIndexQuery

        if not self.id:
            raise ValueError("The id attribute must be set to query a SearchIndex.")
        return await SearchIndexQuery(
            search_index_id=self.id,
            search_query=search_query,
            response_parts=response_parts or [],
        ).send_job_and_wait_async(timeout=job_timeout, synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"SearchIndex_Autocomplete: {self.id}"
    )
    async def autocomplete_async(
        self,
        query: Query,
        source: Optional[SourceFilter] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SearchHit"]:
        """Run a synchronous autocomplete search against this index. The
        autocomplete endpoint allow lists only prefix-style queries
        ([`prefix`](https://docs.opensearch.org/latest/query-dsl/term/prefix/),
        [`match_phrase_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase-prefix/),
        or [`match_bool_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-bool-prefix/))
        and caps results at 8.

        Arguments:
            query: The top-level [OpenSearch Query DSL](https://docs.opensearch.org/latest/query-dsl/)
                clause -- see [Query][synapseclient.models.search_dsl.Query];
                restricted server-side to `prefix`, `match_phrase_prefix`, or
                `match_bool_prefix`.
            source: Optional [source filter](https://docs.opensearch.org/latest/search-plugins/searching-data/retrieve-specific-fields/)
                selecting which columns are returned on each hit.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The matching SearchHits, capped at 8.

        Raises:
            ValueError: If the ``id`` attribute has not been set.

        Example: Autocomplete titles beginning with "alz".
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            async def main():
                syn = Synapse()
                await syn.login_async()

                index = SearchIndex(id="syn12345")
                hits = await index.autocomplete_async(
                    query={"match_phrase_prefix": {"title": {"query": "alz"}}},
                )
                for hit in hits:
                    print(hit.row_id, hit.fields)

            asyncio.run(main())
            ```
        """
        from synapseclient.api import autocomplete_search
        from synapseclient.models.search_management import (
            SearchAutocompleteRequest,
            SearchHit,
        )

        if not self.id:
            raise ValueError("The id attribute must be set to call autocomplete.")
        request = SearchAutocompleteRequest(
            search_index_id=self.id,
            query=query,
            source=source,
        )
        response = await autocomplete_search(
            request.to_synapse_request(), synapse_client=synapse_client
        )
        return [SearchHit().fill_from_dict(h) for h in response.get("hits", []) or []]
