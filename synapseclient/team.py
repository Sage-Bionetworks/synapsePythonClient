"""
Functions that interact with Synapse Teams
"""


from synapseclient.core.models.dict_object import DictObject


class UserProfile(DictObject):
    """
    Information about a Synapse user.  In practice the constructor is not called directly by the client.

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


class UserGroupHeader(DictObject):
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

    def __init__(self, **kwargs):
        super(UserGroupHeader, self).__init__(kwargs)


class Team(DictObject):
    """
    Represents a [Synapse Team](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Team.html).
    User definable fields are:

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


class TeamMember(DictObject):
    """
    Contains information about a user's membership in a Team.
    In practice the constructor is not called directly by the client.

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
