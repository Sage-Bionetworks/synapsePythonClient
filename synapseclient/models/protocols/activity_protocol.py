"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Activity, File, Table


class ActivitySynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        parent: Optional[Union["Table", "File"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Activity":
        """
        Store the Activity in Synapse.

        Arguments:
            parent: The parent entity to associate this activity with.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The activity object.

        Raises:
            ValueError: Raised if both of the following are true:

                - If the parent does not have an ID.
                - If the Activity does not have an ID and ETag.
        """
        return self

    @classmethod
    def from_parent(
        cls,
        parent: Union["Table", "File"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union["Activity", None]:
        """
        Get the Activity from Synapse based on the parent entity.

        Arguments:
            parent: The parent entity this activity is associated with. The parent may
                also have a version_number. Gets the most recent version if version is
                omitted.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The activity object or None if it does not exist.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        from synapseclient.models import Activity

        return Activity()

    @classmethod
    def delete(
        cls,
        parent: Union["Table", "File"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete the Activity from Synapse. The Activity must be disassociated from
        all entities before it can be deleted. The first step of this delete call
        is to disassociate the Activity from the parent entity. If you have other
        entities that are associated with this Activity you must disassociate them
        by calling this method on them as well. You'll receive an error for all entities
        until the last one which will delete the Activity.

        Arguments:
            parent: The parent entity this activity is associated with.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        return None

    @classmethod
    async def disassociate_from_entity(
        cls,
        parent: Union["Table", "File"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Disassociate the Activity from the parent entity. This is the first step in
        deleting the Activity. If you have other entities that are associated with this
        Activity you must disassociate them by calling this method on them as well.

        Arguments:
            parent: The parent entity this activity is associated with.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        return None
