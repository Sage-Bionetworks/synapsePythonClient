import asyncio
from dataclasses import dataclass
from typing import Optional, Generator
from opentelemetry import trace, context

from synapseclient import Synapse
from synapseclient.team import (
    Team as Synapse_Team,
    TeamMember as Synapse_TeamMember,
)
from synapseclient.models.user import UserGroupHeader

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

    team_id: int
    """The ID of the team"""

    member: UserGroupHeader
    """An object of type [org.sagebionetworks.repo.model.UserGroupHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeader.html)"""

    is_admin: bool
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
        description (optional): A short description of the team
        icon (optional): A file handle ID for the icon image of the team
        can_public_join: True if members can join without an invitation or approval
        can_request_membership (optional): True if users can create a membership request to join
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates
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
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates"""

    created_on: Optional[str] = None
    """The date this team was created"""

    modified_on: Optional[str] = None
    """The date this team was last modified"""

    created_by: Optional[str] = None
    """The ID of the user that created this team"""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this team"""

    def fill_from_dict(self, synapse_team: Synapse_Team) -> "Team":
        self.id = synapse_team.get("id", None)
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

    # def create_team(self):
    #     ...

    # def delete_team(self):
    #     ...

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
        with tracer.start_as_current_span(f"Team_From_Id: {id}"):
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

    # def from_name(self, name: str, synapse_client: Optional[Synapse] = None) -> "Team":
    #     """Gets Team object using its string name.

    #     Args:
    #         name: The name of the team.
    #         synapse_client: If not passed in or None this will use the last client from the `.login()` method.

    #     Returns:
    #         Team: The Team object.
    #     """
    #     team = Synapse.get_client(synapse_client=synapse_client).getTeam(id=name)
    #     self.fill_from_dict(team)
    #     return self

    # def team_members(
    #     self, synapse_client: Optional[Synapse] = None
    # ) -> Generator[TeamMember, None, None]:
    #     """Gets the TeamMembers associated with a team.

    #     Args:
    #         synapse_client: If not passed in or None this will use the last client from the `.login()` method.

    #     Returns:
    #         Generator[TeamMember]: A generator of TeamMember objects.
    #     """
    #     return Synapse.get_client(synapse_client=synapse_client).getTeamMembers(
    #         team=self
    #     )

    # def invite(
    #     self, user: str, message: str, synapse_client: Optional[Synapse] = None
    # ) -> dict[str, str]:
    #     return Synapse.get_client(synapse_client=synapse_client).invite_to_team(
    #         team=self, user=user, message=message, force=True
    #     )

    # def open_invitiations(
    #     self, synapse_client: Optional[Synapse] = None
    # ) -> Generator[dict, None, None]:
    #     return Synapse.get_client(
    #         synapse_client=synapse_client
    #     ).get_team_open_invitations(team=self)
