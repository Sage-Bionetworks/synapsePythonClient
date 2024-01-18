from dataclasses import dataclass
from typing import Optional


@dataclass
class UserGroupHeader:
    """
    Select metadata about a Synapse principal.
    In practice the constructor is not called directly by the client.

    Attributes:
        ownerId A foreign key to the ID of the 'principal' object for the user.
        firstName: First Name
        lastName: Last Name
        userName: A name chosen by the user that uniquely identifies them.
        email: User's current email address
        isIndividual: True if this is a user, false if it is a group
    """

    ownerId: int
    """A foreign key to the ID of the 'principal' object for the user."""

    firstName: str
    """First Name"""

    lastName: str
    """Last Name"""

    userName: str
    """A name chosen by the user that uniquely identifies them."""

    isIndividual: bool
    """True if this is a user, false if it is a group"""

    email: Optional[str] = None
    """User's current email address"""


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
        canPublicJoin: True if members can join without an invitation or approval
        canRequestMembership (optional): True if users can create a membership request to join
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates
        createdOn: The date this team was created
        modifiedOn: The date this team was last modified
        createdBy: The ID of the user that created this team
        modifiedBy: The ID of the user that last modified this team
    """

    id: int
    """The ID of the team"""

    name: str
    """The name of the team"""

    canPublicJoin: bool
    """True if members can join without an invitation or approval"""

    etag: str
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates"""

    createdOn: str
    """The date this team was created"""

    modifiedOn: str
    """The date this team was last modified"""

    createdBy: str
    """The ID of the user that created this team"""

    modifiedBy: str
    """The ID of the user that last modified this team"""

    description: Optional[str] = None
    """A short description of the team"""

    icon: Optional[str] = None
    """A file handle ID for the icon image of the team"""

    canRequestMembership: Optional[bool] = None
    """True if users can create a membership request to join"""

    @classmethod
    def getURI(cls, id: int):
        """Get the URI for the team with the given id"""
        return f"/team/{id}"

    def postURI(self):
        """Post URI for the team"""
        return "/team"

    def putURI(self):
        """Put URI for team"""
        return "/team"

    def deleteURI(self):
        """Delete URI for team"""
        return f"/team/{self.id}"

    def getACLURI(self):
        """Get ACL URI for team"""
        return f"/team/{self.id}/acl"

    def putACLURI(self):
        """Put ACL URI for team"""
        return "/team/acl"


@dataclass
class TeamMember:
    """
    Contains information about a user's membership in a Team.
    In practice the constructor is not called directly by the client.

    Attributes:
        teamId: The ID of the team
        member: An object of type [org.sagebionetworks.repo.model.UserGroupHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeader.html)
                describing the member
        isAdmin: Whether the given member is an administrator of the team
    """

    teamId: int
    """The ID of the team"""

    member: UserGroupHeader
    """An object of type [org.sagebionetworks.repo.model.UserGroupHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeader.html)"""

    isAdmin: bool
    """Whether the given member is an administrator of the team"""
