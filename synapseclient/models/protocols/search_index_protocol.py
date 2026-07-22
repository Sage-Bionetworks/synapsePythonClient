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

    def autocomplete(
        self,
        query: Dict[str, Any],
        source: Optional[Dict[str, Any]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SearchHit"]:
        """Run a synchronous autocomplete search against this index."""
        return []
