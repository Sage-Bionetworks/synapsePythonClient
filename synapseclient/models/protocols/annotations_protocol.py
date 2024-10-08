"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Annotations


class AnnotationsSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Annotations":
        """Storing annotations to synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The stored annotations.

        Raises:
            ValueError: If the id or etag are not set.
        """
        return self
