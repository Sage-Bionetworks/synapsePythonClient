from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .dict_object import DictObject

class UserProfile(DictObject):
    def __init__(self, **kwargs):
        super(UserProfile, self).__init__(kwargs)


class UserGroupHeader(DictObject):
    def __init__(self, **kwargs):
        super(UserGroupHeader, self).__init__(kwargs)

class Team(DictObject):
    """
    Represent a `Synapse Team <http://rest.synapse.org/org/sagebionetworks/repo/model/Team.html>`_. User definable fields are:
    :param icon:          fileHandleId for icon image of the Team
    :param description:   A short description of this Team.
    :param name:          The name of the Team.
    :param canPublicJoin: true for teams which members can join without an invitation or approval
    """
    def __init__(self, **kwargs):
        super(Team, self).__init__(kwargs)

    @classmethod
    def getURI(cls, id):
        return '/team/%s' %id

    def postURI(self):
        return '/team'

    def putURI(self):
        return '/team'

    def deleteURI(self):
        return '/team/%s' %self.id

    def getACLURI(self):
        return '/team/%s/acl' %self.id

    def putACLURI(self):
        return '/team/acl'


class TeamMember(DictObject):
    def __init__(self, **kwargs):
        if 'member' in kwargs:
            kwargs['member'] = UserGroupHeader(**kwargs['member'])
        super(TeamMember, self).__init__(kwargs)

