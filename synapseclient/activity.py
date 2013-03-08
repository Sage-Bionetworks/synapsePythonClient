##
## Provenance related functionality
############################################################


def makeUsed(target, targetVersion=None, wasExecuted=False):
    targetId = target['id'] if 'id' in target else str(target)
    reference = {'targetId':targetId}
    if targetVersion:
        reference['targetVersion'] = str(targetVersion)
    used = {'reference':reference, 'wasExecuted':wasExecuted}
    return used


class Activity(dict):
    """Represents the provenance of a Synapse entity.
    See: https://sagebionetworks.jira.com/wiki/display/PLFM/Analysis+Provenance+in+Synapse
    """
    def __init__(self, name=None, description=None, used=[], data=None):
        ## initialize from a dictionary, as in Activity(data=response.json())
        if data:
            super(Activity, self).__init__(data)
        if name:
            self['name'] = name
        if description:
            self['description'] = description
        if used:
            self['used'] = used

    def used(self, target, targetVersion=None, wasExecuted=False):
        self.setdefault('used', []).append(makeUsed(target, targetVersion=targetVersion, wasExecuted=wasExecuted))

    def executed(self, target, targetVersion=None):
        self.setdefault('used', []).append(makeUsed(target, targetVersion=targetVersion, wasExecuted=True))
