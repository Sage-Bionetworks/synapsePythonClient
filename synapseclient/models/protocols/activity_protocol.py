"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Activity, Dataset, EntityView, File, Table


class ActivitySynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        parent: Optional[Union["Table", "File", "EntityView", "Dataset", str]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Activity":
        """
        Store the Activity in Synapse.

        Arguments:
            parent: The parent entity to associate this activity with. Can be an entity
                object or a string ID (e.g., "syn123").
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
        parent: Union["Table", "File", "EntityView", "Dataset", str],
        parent_version_number: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union["Activity", None]:
        """
        Get the Activity from Synapse based on the parent entity.

        Arguments:
            parent: The parent entity this activity is associated with. The parent may
                also have a version_number. Gets the most recent version if version is
                omitted.
            parent_version_number: The version number of the parent entity. When parent
                is a string with version (e.g., "syn123.4"), the version in the string
                takes precedence. When parent is an object, this parameter takes precedence
                over parent.version_number. Gets the most recent version if omitted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
        parent: Union["Table", "File", str],
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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        return None

    @classmethod
    def disassociate_from_entity(
        cls,
        parent: Union["Table", "File", str],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Disassociate the Activity from the parent entity. This is the first step in
        deleting the Activity. If you have other entities that are associated with this
        Activity you must disassociate them by calling this method on them as well.

        Arguments:
            parent: The parent entity this activity is associated with.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        return None

    @classmethod
    def get(
        cls,
        activity_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        parent_version_number: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union["Activity", None]:
        """
        Get an Activity from Synapse by either activity ID or parent entity ID.

        Arguments:
            activity_id: The ID of the activity to retrieve. If provided, this takes
                precedence over parent_id.
            parent_id: The ID of the parent entity to get the activity for.
                Only used if activity_id is not provided (ignored when activity_id is provided).
            parent_version_number: The version number of the parent entity. Only used when
                parent_id is provided (ignored when activity_id is provided). Gets the
                most recent version if omitted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The activity object or None if it does not exist.

        Raises:
            ValueError: If neither activity_id nor parent_id is provided.
        """
        from synapseclient.models import Activity

        return Activity()
