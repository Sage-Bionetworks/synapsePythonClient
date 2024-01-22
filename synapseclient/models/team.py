from dataclasses import dataclass
from typing import Optional

from synapseclient.team import (
    Team as Synapse_Team,
    UserGroupHeader as Synapse_UserGroupHeader,
    TeamMember as Synapse_TeamMember,
)


@dataclass
class UserGroupHeader:
    """
    Select metadata about a Synapse principal.
    In practice the constructor is not called directly by the client.

    Attributes:
        owner_id: A foreign key to the ID of the 'principal' object for the user.
        first_name: First Name
        last_name: Last Name
        user_name: A name chosen by the user that uniquely identifies them.
        email: User's current email address
        is_individual: True if this is a user, false if it is a group
    """

    owner_id: int
    """A foreign key to the ID of the 'principal' object for the user."""

    first_name: str
    """First Name"""

    last_name: str
    """Last Name"""

    user_name: str
    """A name chosen by the user that uniquely identifies them."""

    is_individual: bool
    """True if this is a user, false if it is a group"""

    email: Optional[str] = None
    """User's current email address"""

    def fill_from_dict(
        self, synapse_user_group_header: Synapse_UserGroupHeader
    ) -> "UserGroupHeader":
        self.owner_id = synapse_user_group_header.get("ownerId", None)
        self.first_name = synapse_user_group_header.get("firstName", None)
        self.last_name = synapse_user_group_header.get("lastName", None)
        self.user_name = synapse_user_group_header.get("userName", None)
        self.email = synapse_user_group_header.get("email", None)
        self.is_individual = synapse_user_group_header.get("isIndividual", None)
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

    @classmethod
    def get_uri(cls, id: int):
        """Get the URI for the team with the given id"""
        return f"/team/{id}"

    def post_uri(self):
        """Post URI for the team"""
        return "/team"

    def put_uri(self):
        """Put URI for team"""
        return "/team"

    def delete_uri(self):
        """Delete URI for team"""
        return f"/team/{self.id}"

    def get_acl_uri(self):
        """Get ACL URI for team"""
        return f"/team/{self.id}/acl"

    def put_acl_uri(self):
        """Put ACL URI for team"""
        return "/team/acl"


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
