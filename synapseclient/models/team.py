import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, List
from opentelemetry import trace, context

from synapseclient import Synapse
from synapseclient.team import (
    Team as Synapse_Team,
    TeamMember as Synapse_TeamMember,
)
from synapseclient.models.user import UserGroupHeader
from synapseclient.core.async_utils import otel_trace_method


tracer = trace.get_tracer("synapseclient")


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

    def fill_from_dict(self, synapse_team_member: Synapse_TeamMember) -> "TeamMember":
        self.team_id = synapse_team_member.get("teamId", None)
        self.member = UserGroupHeader().fill_from_dict(
            synapse_team_member.get("member", None)
        )
        self.is_admin = synapse_team_member.get("isAdmin", None)
        return self


@dataclass
class Team:
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
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates
                Since the E-Tag changes every time an entity is updated it is used to detect when
                a client's current representation of an entity is out-of-date.
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

    can_public_join: Optional[bool] = None
    """True if members can join without an invitation or approval"""

    can_request_membership: Optional[bool] = None
    """True if users can create a membership request to join"""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates
        Since the E-Tag changes every time an entity is updated it is used to detect when
        a client's current representation of an entity is out-of-date."""

    created_on: Optional[str] = None
    """The date this team was created"""

    modified_on: Optional[str] = None
    """The date this team was last modified"""

    created_by: Optional[str] = None
    """The ID of the user that created this team"""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this team"""

    def fill_from_dict(self, synapse_team: Synapse_Team) -> "Team":
        self.id = (
            int(synapse_team.get("id", None)) if synapse_team.get("id", None) else None
        )
        self.name = synapse_team.get("name", None)
        self.description = synapse_team.get("description", None)
        self.icon = synapse_team.get("icon", None)
        self.can_public_join = synapse_team.get("canPublicJoin", None)
        self.can_request_membership = synapse_team.get("canRequestMembership", None)
        self.etag = synapse_team.get("etag", None)
        self.created_on = synapse_team.get("createdOn", None)
        self.modified_on = synapse_team.get("modifiedOn", None)
        self.created_by = synapse_team.get("createdBy", None)
        self.modified_by = synapse_team.get("modifiedBy", None)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Create: {self.name}"
    )
    async def create(self, synapse_client: Optional[Synapse] = None) -> "Team":
        """Creates a new team on Synapse.

        Args:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            Team: The Team object.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        team = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).create_team(
                name=self.name,
                description=self.description if self.description else None,
                icon=self.icon if self.icon else None,
                opentelemetry_context=current_context,
            ),
        )
        self.fill_from_dict(team)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Delete: {self.id}"
    )
    async def delete(self, synapse_client: Optional[Synapse] = None) -> None:
        """Deletes a team from Synapse.

        Args:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            None
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        delete = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).delete_team(
                id=self.id, opentelemetry_context=current_context
            ),
        )
        return delete

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_From_Id: {self.id}"
    )
    async def from_id(
        self, id: int, synapse_client: Optional[Synapse] = None
    ) -> "Team":
        """Gets Team object using its integer id.

        Args:
            id: The id of the team.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            Team: The Team object.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        team = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).getTeam(
                id=id, opentelemetry_context=current_context
            ),
        )
        self.fill_from_dict(team)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_From_Name: {self.name}"
    )
    async def from_name(
        self, name: str, synapse_client: Optional[Synapse] = None
    ) -> "Team":
        """Gets Team object using its string name.

        Args:
            name: The name of the team.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            Team: The Team object.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        team = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).getTeam(
                id=name, opentelemetry_context=current_context
            ),
        )
        self.fill_from_dict(team)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Members: {self.name}"
    )
    async def members(
        self, synapse_client: Optional[Synapse] = None
    ) -> List[TeamMember]:
        """Gets the TeamMembers associated with a team.

        Args:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            List[TeamMember]: A List of TeamMember objects.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        team_members = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).getTeamMembers(
                team=self, opentelemetry_context=current_context
            ),
        )
        team_member_list = [
            TeamMember().fill_from_dict(synapse_team_member=member)
            for member in team_members
        ]
        return team_member_list

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Invite: {self.name}"
    )
    async def invite(
        self, user: str, message: str, synapse_client: Optional[Synapse] = None
    ) -> Dict[str, str]:
        """Invites a user to a team.

        Args:
            user: The user to invite.
            message: The message to send.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            dict: The invite response.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        invite = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).invite_to_team(
                team=self,
                user=user,
                message=message,
                force=True,
                opentelemetry_context=current_context,
            ),
        )
        return invite

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Team_Open_Invitations: {self.name}"
    )
    async def open_invitations(
        self, synapse_client: Optional[Synapse] = None
    ) -> List[dict[str, str]]:
        """Gets all open invitations for a team.

        Args:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            List[dict]: A list of invitations.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        invitations = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(
                synapse_client=synapse_client
            ).get_team_open_invitations(
                team=self, opentelemetry_context=current_context
            ),
        )
        return list(invitations)
