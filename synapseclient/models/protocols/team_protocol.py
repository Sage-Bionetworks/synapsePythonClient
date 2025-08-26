"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Team, TeamMember, TeamMembershipStatus


class TeamSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def create(self, *, synapse_client: Optional[Synapse] = None) -> "Team":
        """Creates a new team on Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Team: The Team object.
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Deletes a team from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None
        """
        return None

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "Team":
        """
        Gets a Team from Synapse by ID or Name. If both are added to the Team instance
        it will use the ID.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
    ) -> Union[Dict[str, str], None]:
        """Invites a user to a team given the ID field on the Team instance.

        Arguments:
            user: The username or ID of the user to invite.
            message: The message to send.
            force: If True, will send the invite even if the user is already a member
                or has an open invitation. If False, will not send the invite if the user
                is already a member or has an open invitation.
                Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The invite response or None if an invite was not sent.
        """
        return {}

    def open_invitations(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> List[Dict[str, str]]:
        """Gets all open invitations for a team given the ID field on the Team instance.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            List[dict]: A list of invitations.
        """
        return list({})

    def get_user_membership_status(
        self,
        user_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "TeamMembershipStatus":
        """Retrieve a user's Team Membership Status bundle.

        <https://rest-docs.synapse.org/rest/GET/team/id/member/principalId/membershipStatus.html>

        Arguments:
            user_id: The ID of the user whose membership status is being queried.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            TeamMembershipStatus object

        Example:
          Check if a user is a member of a team
            This example shows how to check a user's membership status in a team.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Team

            syn = Synapse()
            syn.login()

            # Get a team by ID
            team = Team.from_id(123456)

            # Check membership status for a specific user
            user_id = "3350396"  # Replace with actual user ID
            status = team.get_user_membership_status(user_id)

            print(f"User ID: {status.user_id}")
            print(f"Is member: {status.is_member}")
            print(f"Can join: {status.can_join}")
            print(f"Has open invitation: {status.has_open_invitation}")
            print(f"Has open request: {status.has_open_request}")
            print(f"Membership approval required: {status.membership_approval_required}")
            ```
        """
        from synapseclient.models.team import TeamMembershipStatus

        return TeamMembershipStatus().fill_from_dict({})
