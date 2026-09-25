"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from typing_extensions import Self

if TYPE_CHECKING:
    from synapseclient import Synapse


class SearchConfigBindingSynchronousProtocol(Protocol):
    """Synchronous interface for SearchConfigBinding operations."""

    def store(self, *, synapse_client: Optional["Synapse"] = None) -> "Self":
        """Bind ``search_configuration_id`` to the entity ``object_id``. Replaces
        any existing binding on that entity.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated from the created SearchConfigBinding.

        Raises:
            ValueError: If ``object_id`` or ``search_configuration_id`` is not set.

        Example: Bind a SearchConfiguration to a Project.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchConfigBinding

            syn = Synapse()
            syn.login()

            binding = SearchConfigBinding(
                object_id="syn12345",
                search_configuration_id="6789",
            )
            binding = binding.store()
            print(f"Bound SearchConfiguration {binding.search_configuration_id} "
                  f"to {binding.object_id}")
            ```
        """
        return self

    def get(self, *, synapse_client: Optional["Synapse"] = None) -> "Self":
        """Get the effective binding for the entity ``object_id``, resolved by
        walking up the entity hierarchy.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated from the resolved SearchConfigBinding.

        Raises:
            ValueError: If ``object_id`` is not set.

        Example: Get the effective binding for a Project.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchConfigBinding

            syn = Synapse()
            syn.login()

            binding = SearchConfigBinding(object_id="syn12345").get()
            print(binding.search_configuration_id)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """Clear the binding on the entity ``object_id``.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If ``object_id`` is not set.

        Example: Clear the binding on a Project.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchConfigBinding

            syn = Synapse()
            syn.login()

            SearchConfigBinding(object_id="syn12345").delete()
            ```
        """
        return None
