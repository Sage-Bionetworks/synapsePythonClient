"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Dict, List, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Team, TeamMember


class TeamSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def create(self, *, synapse_client: Optional[Synapse] = None) -> "Team":
        """Creates a new team on Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            Team: The Team object.
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Deletes a team from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            None
        """
        return None

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "Team":
        """
        Gets a Team from Synapse by ID or Name. If both are added to the Team instance
        it will use the ID.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Raises:
            ValueError: If the Team object has neither an id nor a name.

        Returns:
            Team: The Team object.
        """
        return self

    @classmethod
    def from_id(cls, id: int, *, synapse_client: Optional[Synapse] = None) -> "Team":
        """Gets Team object using its integer id.

        Arguments:
            id: The id of the team.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            Team: The Team object.
        """
        from synapseclient.models.team import Team

        return Team()

    @classmethod
    def from_name(
        cls, name: str, *, synapse_client: Optional[Synapse] = None
    ) -> "Team":
        """Gets Team object using its string name.

        *** You will be unable to retrieve a team by name immediately after its
        creation because the fragment service is eventually consistent. If you need
        to retrieve a team immediately following creation you should use the
        [from_id][synapseclient.models.Team.from_id] method. ***

        Arguments:
            name: The name of the team.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            Team: The Team object.
        """
        from synapseclient.models.team import Team

        return Team()

    def members(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> List["TeamMember"]:
        """
        Gets the TeamMembers associated with a team given the ID field on the
        Team instance.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            List[TeamMember]: A List of TeamMember objects.
        """
        from synapseclient.models.team import TeamMember

        return [TeamMember()]

    def invite(
        self,
        user: str,
        message: str,
        force: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, str]:
        """Invites a user to a team given the ID field on the Team instance.

        Arguments:
            user: The username of the user to invite.
            message: The message to send.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            dict: The invite response.
        """
        return {}

    def open_invitations(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> List[Dict[str, str]]:
        """Gets all open invitations for a team given the ID field on the Team instance.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            List[dict]: A list of invitations.
        """
        return list({})
