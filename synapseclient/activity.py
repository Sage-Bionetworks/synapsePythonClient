##
## Provenance related functionality
############################################################

import collections
from utils import is_url


def makeUsed(target, targetVersion=None, wasExecuted=False, **kwargs):
    """
    Returns a UsedEntity or UsedURL, trying to be very liberal about what input will be accepted
    target: can be either a synapse entity or a string holding a synapse ID (eg 'syn123456') or
       a UsedEntity object (see schema UsedEntity.json)
    targetVersion: an integer specifying the versionNumber of the target entity, or None
    wasExcecuted: a boolean indicating that the target is a code entity whose output is being described
    """

    ## if we're passed a correctly formed UsedEntity object, just return it
    if isinstance(target, collections.Mapping):
        ## update wasExecuted, if necessary
        target['wasExecuted'] = wasExecuted or target.pop('wasExecuted', False)
        ## looks like a UsedEntity
        if all([key in target.keys() for key in ['reference', 'wasExecuted']]):
            ## TODO: we could check if reference is properly formed, too
            if 'concreteType' not in target: target['concreteType'] = 'org.sagebionetworks.repo.model.provenance.UsedEntity'
            return target
        ## looks like a UsedURL
        if 'url' in target:
            if 'name' not in target: target['name'] = target['url']
            if 'concreteType' not in target: target['concreteType'] = 'org.sagebionetworks.repo.model.provenance.UsedURL'
            return target

    ## if we're passed a URL in kwargs
    if 'url' in kwargs:
        used = {'url':kwargs['url'],
                'name': kwargs['name'] if 'name' in kwargs else str(target),
                'concreteType':'org.sagebionetworks.repo.model.provenance.UsedURL'}

    ## if target looks like a URL
    elif is_url(target):
        used = {'url':target, 'name':target, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedURL'}

    ## otherwise, it's a UsedEntity
    else:
        ## if we're passed an entity or entity id, make a UsedEntity object
        targetId = target['id'] if 'id' in target else str(target)
        reference = {'targetId':targetId}
        if targetVersion:
            reference['targetVersionNumber'] = int(targetVersion)
        else:
            try:
                ## if we have an entity, get it's version number
                reference['targetVersionNumber'] = target['versionNumber']
            except (KeyError, TypeError):
                ## count on platform to get the current version of the entity from synapse
                pass
        used = {'reference':reference, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedEntity'}

    used['wasExecuted'] = wasExecuted

    return used


class Activity(dict):
    """Represents the provenance of a Synapse entity.
    See: https://sagebionetworks.jira.com/wiki/display/PLFM/Analysis+Provenance+in+Synapse
    See: The W3C's provenance ontology, http://www.w3.org/TR/prov-o/
    """
    def __init__(self, name=None, description=None, used=None, executed=None, data=None):
        """Activity constructor

        Args:
            name (str):
           
            description (str):

            used: either a list of reference objects (eg [{'targetId':'syn123456', 'targetVersionNumber':1}])
                or a list of synapse entities or entity ids

            data: a dictionary representation of an Activity, with fields 'name', 'description'
                and 'used' (a list of reference objects)
        """
        ## initialize from a dictionary, as in Activity(data=response.json())
        if data:
            super(Activity, self).__init__(data)
        if name:
            self['name'] = name
        if description:
            self['description'] = description
        if 'used' not in self:
            self['used'] = []
        if used:
            self.used(used)
        if executed:
            self.executed(executed)

    def used(self, target, targetVersion=None, wasExecuted=False):
        if isinstance(target, list):
            for t in target: self.used(t, targetVersion=targetVersion, wasExecuted=wasExecuted)
        self['used'].append(makeUsed(target, targetVersion=targetVersion, wasExecuted=wasExecuted))

    def executed(self, target, targetVersion=None):
        self.used(target=target, targetVersion=targetVersion, wasExecuted=True)
