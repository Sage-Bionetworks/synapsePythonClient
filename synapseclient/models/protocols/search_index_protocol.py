"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models.search_management import SearchHit


class SearchIndexSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SearchIndex operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store metadata about a SearchIndex including the annotations. Creates
        a new SearchIndex if `id` is not set, or updates the existing one
        otherwise. `defining_sql` must be set before calling this.

        Arguments:
            dry_run: If True, will not actually store the SearchIndex but will log
                to the console what would be created or updated.
            job_timeout: The maximum amount of time to wait for the index-build job
                to complete before raising an error.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated with the server-assigned ID, etag, and columns.

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
                # syn67890 must be a table or a view; multi-entity JOINs are not supported
                defining_sql="SELECT * FROM syn67890",
            )
            index = index.store()
            print(f"Created SearchIndex: {index.id}")
            ```
        """
        return self

    def get(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the SearchIndex from Synapse. Either `id`, or
        `name` and `parent_id`, must be set before calling this.

        Arguments:
            include_columns: If True, will include the columns derived from
                `defining_sql` on the returned SearchIndex.
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

    def autocomplete(
        self,
        query: Dict[str, Any],
        source: Optional[Dict[str, Any]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SearchHit"]:
        """Run a synchronous autocomplete search against this index. The
        autocomplete endpoint allowlists only prefix-style queries (`prefix`,
        `match_phrase_prefix`, or `match_bool_prefix`) and caps results at 8.

        Arguments:
            query: The top-level OpenSearch Query DSL clause; restricted
                server-side to `prefix`, `match_phrase_prefix`, or
                `match_bool_prefix`.
            source: Optional source filter selecting which columns are returned
                on each hit.
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

            syn = Synapse()
            syn.login()

            index = SearchIndex(id="syn12345")
            hits = index.autocomplete(
                query={"match_phrase_prefix": {"title": {"query": "alz"}}},
            )
            for hit in hits:
                print(hit.row_id, hit.fields)
            ```
        """
        return []
