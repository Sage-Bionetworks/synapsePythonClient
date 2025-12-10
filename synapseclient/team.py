"""
Functions that interact with Synapse Teams
"""

from synapseclient.core.models.dict_object import DictObject
from synapseclient.core.utils import deprecated


@deprecated(
    version="4.9.0",
    reason="To be removed in 5.0.0. "
    "Moved to the `from synapseclient.models import UserProfile` class. "
    "Check the docstring for the replacement function example.",
)
class UserProfile(DictObject):
    """
    **Deprecated with replacement.** This class will be removed in 5.0.0.
    Use `from synapseclient.models import UserProfile` instead.

    Information about a Synapse user.  In practice the constructor is not called directly by the client.

    Example: Migration to new method
        &nbsp;

        ```python
        # Old approach (DEPRECATED)
        # from synapseclient.team import UserProfile

        # New approach (RECOMMENDED)
        from synapseclient import Synapse
        from synapseclient.models import UserProfile

        syn = Synapse()
        syn.login()

        # Get your own profile
        my_profile = UserProfile().get()
        print(f"My profile: {my_profile.username}")

        # Get another user's profile by username
        profile_by_username = UserProfile.from_username(username='synapse-service-dpe-team')
        print(f"Profile by username: {profile_by_username.username}")

        # Get another user's profile by ID
        profile_by_id = UserProfile.from_id(user_id=3485485)
        print(f"Profile by id: {profile_by_id.username}")
        ```

    Attributes:
        ownerId: A foreign key to the ID of the 'principal' object for the user.
        uri: The Uniform Resource Identifier (URI) for this entity.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates.
        Since the E-Tag changes every time an entity is updated it is
        used to detect when a client's current representation
        of an entity is out-of-date.
        firstName: This person's given name (forename)
        lastName: This person's family name (surname)
        emails: The list of user email addresses registered to this user.
        userName: A name chosen by the user that uniquely identifies them.
        summary: A summary description about this person
        position: This person's current position title
        location: This person's location
        industry: The industry/discipline that this person is associated with
        company: This person's current affiliation
        profilePicureFileHandleId: The File Handle ID of the user's profile picture.
        url: A link to more information about this person
        notificationSettings: An object of type [org.sagebionetworks.repo.model.message.Settings](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/message/Settings.html)
        containing the user's preferences regarding when email notifications should be sent
    """

    def __init__(self, **kwargs):
        super(UserProfile, self).__init__(kwargs)


@deprecated(
    version="4.9.0",
    reason="To be removed in 5.0.0. "
    "Moved to the `from synapseclient.models import UserGroupHeader` class. "
    "Check the docstring for the replacement function example.",
)
class UserGroupHeader(DictObject):
    """
    **Deprecated with replacement.** This class will be removed in 5.0.0.
    Use `from synapseclient.models import UserGroupHeader` instead.

    Select metadata about a Synapse principal.
    In practice the constructor is not called directly by the client.

    Example: Migration to new method
        ```python
        # Old approach (DEPRECATED)
        # from synapseclient.team import UserGroupHeader

        # New approach (RECOMMENDED)
        from synapseclient.models import UserGroupHeader
        ```

    Attributes:
        ownerId A foreign key to the ID of the 'principal' object for the user.
        firstName: First Name
        lastName: Last Name
        userName: A name chosen by the user that uniquely identifies them.
        email: User's current email address
        isIndividual: True if this is a user, false if it is a group
    """

    def __init__(self, **kwargs):
        super(UserGroupHeader, self).__init__(kwargs)


@deprecated(
    version="4.9.0",
    reason="To be removed in 5.0.0. "
    "Moved to the `from synapseclient.models import Team` class. "
    "Check the docstring for the replacement function example.",
)
class Team(DictObject):
    """
    **Deprecated with replacement.** This class will be removed in 5.0.0.
    Use `from synapseclient.models import Team` instead.

    Represents a [Synapse Team](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Team.html).
    User definable fields are:

    Example: Migration to new method
        &nbsp;

        ```python
        # Old approach (DEPRECATED)
        # from synapseclient.team import Team

        # New approach (RECOMMENDED)
        from synapseclient import Synapse
        from synapseclient.models import Team

        syn = Synapse()
        syn.login()

        # Create a new team
        new_team = Team(name="My Team", description="A sample team")
        created_team = new_team.create()
        print(f"Created team: {created_team.name}")

        # Get a team by ID
        team_by_id = Team.from_id(id=12345)
        print(f"Team by ID: {team_by_id.name}")

        # Get a team by name
        team_by_name = Team.from_name(name="My Team")
        print(f"Team by name: {team_by_name.name}")

        # Get team members
        members = team_by_id.members()
        print(f"Team has {len(members)} members")
        ```

    Attributes:
        icon: The fileHandleId for icon image of the Team
        description: A short description of this Team.
        name: The name of the Team.
        canPublicJoin: true for teams which members can join without an invitation or approval
    """

    def __init__(self, **kwargs):
        super(Team, self).__init__(kwargs)

    @classmethod
    def getURI(cls, id):
        return "/team/%s" % id

    def postURI(self):
        return "/team"

    def putURI(self):
        return "/team"

    def deleteURI(self):
        return "/team/%s" % self.id

    def getACLURI(self):
        return "/team/%s/acl" % self.id

    def putACLURI(self):
        return "/team/acl"


@deprecated(
    version="4.9.0",
    reason="To be removed in 5.0.0. "
    "Moved to the `from synapseclient.models import TeamMember` class. "
    "Check the docstring for the replacement function example.",
)
class TeamMember(DictObject):
    """
    **Deprecated with replacement.** This class will be removed in 5.0.0.
    Use `from synapseclient.models import TeamMember` instead.

    Contains information about a user's membership in a Team.
    In practice the constructor is not called directly by the client.

    Example: Migration to new method
        ```python
        # Old approach (DEPRECATED)
        # from synapseclient.team import TeamMember

        # New approach (RECOMMENDED)
        from synapseclient import Synapse
        from synapseclient.models import Team, TeamMember

        syn = Synapse()
        syn.login()

        # Get team members using the new Team model
        team = Team.from_id(id=12345)
        members = team.members()
        for member in members:
            print(f"Member: {member.member.user_name}")
        ```

    Attributes:
        teamId: The ID of the team
        member: An object of type [org.sagebionetworks.repo.model.UserGroupHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeader.html)
                describing the member
        isAdmin: Whether the given member is an administrator of the team

    """

    def __init__(self, **kwargs):
        if "member" in kwargs:
            kwargs["member"] = UserGroupHeader(**kwargs["member"])
        super(TeamMember, self).__init__(kwargs)
