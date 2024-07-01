"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import UserProfile


class UserProfileSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "UserProfile":
        """
        Gets a UserProfile object using its id or username in that order. If an id
        and username is not specified this will retrieve the current user's profile.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The UserProfile object.

        """
        return self

    @classmethod
    def from_id(
        cls, user_id: int, *, synapse_client: Optional[Synapse] = None
    ) -> "UserProfile":
        """Gets UserProfile object using its integer id. Wrapper for the
        [get][synapseclient.models.UserProfile.get] method.

        Arguments:
            user_id: The id of the user.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The UserProfile object.
        """
        from synapseclient.models import UserProfile

        return UserProfile()

    @classmethod
    def from_username(
        cls, username: str, *, synapse_client: Optional[Synapse] = None
    ) -> "UserProfile":
        """
        Gets UserProfile object using its string name. Wrapper for the
        [get][synapseclient.models.UserProfile.get] method.

        Arguments:
            username: A name chosen by the user that uniquely identifies them.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The UserProfile object.
        """
        from synapseclient.models import UserProfile

        return UserProfile()

    def is_certified(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "bool":
        """
        Determine whether a user is certified.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            True if the user is certified, False otherwise.

        Raises:
            ValueError: If id nor username is specified.
        """
        return bool()
