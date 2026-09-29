"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, List, Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.models.search_dsl import Query, SourceFilter

if TYPE_CHECKING:
    from synapseclient.models.search_management import (
        SearchHit,
        SearchIndexQuery,
        SearchQuery,
        SearchQueryPart,
    )


class SearchIndexSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SearchIndex operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store metadata about a SearchIndex including the annotations. Creates
        a new SearchIndex if `id` is not set, or updates the existing one
        otherwise. `defining_sql` must be set before calling this.

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
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            syn = Synapse()
            syn.login()

            index = SearchIndex(
                name="My Search Index",
                parent_id="syn12345",
                # syn67890 must be a table or a view;
                defining_sql="SELECT * FROM syn67890",
            )
            index = index.store()
            print(f"Created SearchIndex: {index.id}")
            ```
        """
        return self

    def get(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the SearchIndex from Synapse. Either `id`, or
        `name` and `parent_id`, must be set before calling this.

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
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            syn = Synapse()
            syn.login()

            index = SearchIndex(id="syn12345").get()
            print(index.name, index.defining_sql)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the SearchIndex from Synapse. `id` must be set before calling
        this.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a SearchIndex by ID.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex

            syn = Synapse()
            syn.login()

            SearchIndex(id="syn12345").delete()
            ```
        """
        return None

    def query(
        self,
        search_query: "SearchQuery",
        response_parts: Optional[List["SearchQueryPart"]] = None,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "SearchIndexQuery":
        """Query this search index. Unlike a SQL-backed Table, a SearchIndex is
        queried with the
        [OpenSearch Query DSL](https://docs.opensearch.org/latest/query-dsl/)
        carried by a [SearchQuery][synapseclient.models.SearchQuery] — not with
        Synapse SQL. See [Query][synapseclient.models.search_dsl.Query] for the
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
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex, SearchQuery, SearchQueryPart
            from synapseclient.models.search_dsl import Query

            syn = Synapse()
            syn.login()

            results = SearchIndex(id="syn12345").query(
                search_query=SearchQuery(
                    query=Query(match={"title": {"query": "alzheimer"}}),
                    size=10,
                ),
                response_parts=[SearchQueryPart.TOTAL_HITS],
            )
            print(results.total_hits)
            for hit in results.hits:
                print(hit.row_id, hit.fields)
            ```
        """
        from synapseclient.models.search_management import SearchIndexQuery

        return SearchIndexQuery()

    def autocomplete(
        self,
        query: Query,
        source: Optional[SourceFilter] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SearchHit"]:
        """Run a synchronous autocomplete search against this index. The
        autocomplete endpoint allowlists only prefix-style queries
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
            from synapseclient import Synapse
            from synapseclient.models import SearchIndex
            from synapseclient.models.search_dsl import Query

            syn = Synapse()
            syn.login()

            index = SearchIndex(id="syn12345")
            hits = index.autocomplete(
                query=Query(match_phrase_prefix={"title": {"query": "alz"}}),
            )
            for hit in hits:
                print(hit.row_id, hit.fields)
            ```
        """
        return []
