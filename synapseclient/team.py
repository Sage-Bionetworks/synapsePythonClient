from dict_object import DictObject

class Team(DictObject):
    """
    Represent a Synapse Team. User definable fields are:
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

