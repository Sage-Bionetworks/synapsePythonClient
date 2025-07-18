from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import (
    create_team,
    delete_team,
    get_team,
    get_team_members,
    get_team_open_invitations,
    invite_to_team,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.models.protocols.team_protocol import TeamSynchronousProtocol
from synapseclient.models.user import UserGroupHeader
from synapseclient.team import Team as Synapse_Team
from synapseclient.team import TeamMember as Synapse_TeamMember


@dataclass
class TeamMember:
    """
    Contains information about a user's membership in a Team.
    In practice the constructor is not called directly by the client.

    Attributes:
        team_id: The ID of the team
        member: An object of type [org.sagebionetworks.repo.model.UserGroupHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeader.html)
                describing the member
        is_admin: Whether the given member is an administrator of the team
    """

    team_id: Optional[int] = None
    """The ID of the team"""

    member: Optional[UserGroupHeader] = None
    """An object of type [org.sagebionetworks.repo.model.UserGroupHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeader.html)"""

    is_admin: Optional[bool] = None
    """Whether the given member is an administrator of the team"""

    def fill_from_dict(
        self, synapse_team_member: Union[Synapse_TeamMember, Dict[str, str]]
    ) -> "TeamMember":
        self.team_id = (
            int(synapse_team_member.get("teamId", None))
            if synapse_team_member.get("teamId", None)
            else None
        )
        self.member = UserGroupHeader().fill_from_dict(
            synapse_team_member.get("member", None)
        )
        self.is_admin = synapse_team_member.get("isAdmin", None)
        return self


@dataclass
@async_to_sync
class Team(TeamSynchronousProtocol):
    """
    Represents a [Synapse Team](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Team.html).
    User definable fields are:

    Attributes:
        id: The ID of the team
        name: The name of the team
        description: A short description of the team
        icon: A file handle ID for the icon image of the team
        can_public_join: True if members can join without an invitation or approval
        can_request_membership: True if users can create a membership request to join
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity
            is out-of-date.
        created_on: The date this team was created
        modified_on: The date this team was last modified
        created_by: The ID of the user that created this team
        modified_by: The ID of the user that last modified this team
    """

    id: Optional[int] = None
    """The ID of the team"""

    name: Optional[str] = None
    """The name of the team"""

    description: Optional[str] = None
    """A short description of the team"""

    icon: Optional[str] = None
    """A file handle ID for the icon image of the team"""

    can_public_join: Optional[bool] = False
    """True if members can join without an invitation or approval"""

    can_request_membership: Optional[bool] = True
    """True if users can create a membership request to join"""

    etag: Optional[str] = None
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = None
    """The date this team was created"""

    modified_on: Optional[str] = None
    """The date this team was last modified"""

    created_by: Optional[str] = None
    """The ID of the user that created this team"""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this team"""

    def fill_from_dict(
        self, synapse_team: Union[Synapse_Team, Dict[str, str]]
    ) -> "Team":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_team: The response from the REST API.

        Returns:
            The Team object.
        """
        self.id = (
            int(synapse_team.get("id", None)) if synapse_team.get("id", None) else None
        )
        self.name = synapse_team.get("name", None)
        self.description = synapse_team.get("description", None)
        self.icon = synapse_team.get("icon", None)
        self.can_public_join = synapse_team.get("canPublicJoin", False)
        self.can_request_membership = synapse_team.get("canRequestMembership", True)
        self.etag = synapse_team.get("etag", None)
        self.created_on = synapse_team.get("createdOn", None)
        self.modified_on = synapse_team.get("modifiedOn", None)
        self.created_by = synapse_team.get("createdBy", None)
        self.modified_by = synapse_team.get("modifiedBy", None)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Create: {self.name}"
    )
    async def create_async(self, *, synapse_client: Optional[Synapse] = None) -> "Team":
        """Creates a new team on Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Team: The Team object.
        """
        trace.get_current_span().set_attributes(
            {
                "synapse.name": self.name or "",
                "synapse.id": self.id or "",
            }
        )
        team = await create_team(
            name=self.name,
            description=self.description,
            icon=self.icon,
            can_public_join=self.can_public_join,
            can_request_membership=self.can_request_membership,
            synapse_client=synapse_client,
        )
        self.fill_from_dict(synapse_team=team)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Delete: {self.id}"
    )
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Deletes a team from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None
        """
        await delete_team(id=self.id, synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Get: {self.id if self.id else self.name}"
    )
    async def get_async(self, *, synapse_client: Optional[Synapse] = None) -> "Team":
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
        if self.id:
            api_team = await get_team(id=self.id, synapse_client=synapse_client)
            return self.fill_from_dict(api_team)
        elif self.name:
            api_team = await get_team(id=self.name, synapse_client=synapse_client)
            return self.fill_from_dict(api_team)
        raise ValueError("Team must have either an id or a name")

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda cls, id, **kwargs: f"Team_From_Id: {id}"
    )
    async def from_id_async(
        cls, id: int, *, synapse_client: Optional[Synapse] = None
    ) -> "Team":
        """Gets Team object using its integer id.

        Arguments:
            id: The id of the team.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Team: The Team object.
        """

        return await cls(id=id).get_async(synapse_client=synapse_client)

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda cls, name, **kwargs: f"Team_From_Name: {name}"
    )
    async def from_name_async(
        cls, name: str, *, synapse_client: Optional[Synapse] = None
    ) -> "Team":
        """Gets Team object using its string name.

        *** You will be unable to retrieve a team by name immediately after its
        creation because the fragment service is eventually consistent. If you need to
        retrieve a team immediately following creation you should use the
        [from_id][synapseclient.models.Team.from_id] method. ***

        Arguments:
            name: The name of the team.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Team: The Team object.
        """
        return await cls(name=name).get_async(synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Members: {self.name}"
    )
    async def members_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> List[TeamMember]:
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
        team_members = await get_team_members(
            team=self.id, synapse_client=synapse_client
        )
        team_member_list = [
            TeamMember().fill_from_dict(synapse_team_member=member)
            for member in team_members
        ]
        return team_member_list

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Invite: {self.name}"
    )
    async def invite_async(
        self,
        user: Union[str, int],
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
        invite = await invite_to_team(
            team=self.id,
            user=user,
            message=message,
            force=force,
            synapse_client=synapse_client,
        )

        if invite:
            from synapseclient import Synapse

            client = Synapse.get_client(synapse_client=synapse_client)
            client.logger.info(
                f"Invited user {invite['inviteeId']} to team {invite['teamId']}"
            )

        return invite

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Open_Invitations: {self.name}"
    )
    async def open_invitations_async(
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
        invitations = await get_team_open_invitations(
            team=self.id, synapse_client=synapse_client
        )
        return list(invitations)
