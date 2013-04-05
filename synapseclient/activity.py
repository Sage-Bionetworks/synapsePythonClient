##
## Provenance related functionality
############################################################

import collections
from utils import is_url, id_of, is_synapse_entity


def is_used_entity(x):
    if not isinstance(x, collections.Mapping):
        return False
    if 'reference' not in x:
        return False
    for k in x:
        if k not in ('reference', 'wasExecuted', 'concreteType',):
            return False
    if 'targetId' not in x['reference']:
        return False
    for k in x['reference']:
        if k not in ('targetId', 'targetVersionNumber',):
            return False
    return True


def is_used_url(x):
    if not isinstance(x, collections.Mapping):
        return False
    if 'url' not in x:
        return False
    for k in x:
        if k not in ('url', 'name', 'wasExecuted', 'concreteType',):
            return False
    return True


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


    def usedEntity(self, target, targetVersion=None, wasExcecuted=False):
        """
        target: either a synapse entity or entity id (as a string)
        targetVersion: optionally specify the version of the entity
        wasExecuted: boolean indicating whether the entity represents code that was executed to produce the result
        """
        reference = {'targetId':id_of(target)}
        if targetVersion:
            reference['targetVersionNumber'] = int(targetVersion)
        else:
            try:
                ## if we have an entity, get it's version number
                reference['targetVersionNumber'] = target['versionNumber']
            except (KeyError, TypeError):
                ## count on platform to get the current version of the entity from synapse
                pass
        self['used'].append({'reference':reference, 'wasExecuted':wasExecuted, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedEntity'})


    def usedURL(self, url, name=None, wasExcecuted=False):
        """
        url: resource's URL as a string
        name: optionally name the indicated resource, defaults to the URL
        wasExecuted: boolean indicating whether the entity represents code that was executed to produce the result
        """
        if name is None:
            name = url
        self['used'].append({'url':url, 'name':name, 'wasExecuted':wasExecuted, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedURL'})


    def used(self, target=None, targetVersion=None, wasExecuted=None, url=None, name=None):
        """
        Add a resource used by the activity.

        This method tries to be as permissive as possible. It accepts a string which might
        be a synapse ID or a URL, a synapse entity, a UsedEntity or UsedURL dictionary or
        a list containing any combination of these.

        In addition, named parameters can be used to specify the fields of either a
        UsedEntity or a UsedURL. If target and optionally targetVersion are specified,
        create a UsedEntity. If url and optionally name are specified, create a UsedURL.

        It is an error to specify both target/targetVersion parameters and url/name
        parameters in the same call. To add multiple UsedEntities and UsedURLs, make a
        separate call for each or pass in a list.

        In case of conflicting settings for wasExecuted both inside an object and with a
        parameter, the parameter wins. For example, this UsedURL will have wasExecuted set
        to False:
        activity.used({'url':'http://google.com', 'name':'Goog', 'wasExecuted':True}, wasExecuted=False)

        Entity examples:
        used('syn12345')
        used(entity)
        used(target=entity, targetVersion=2)
        used(codeEntity, wasExecuted=True)
        used({'reference':{'target':'syn12345', 'targetVersion':1}, 'wasExecuted':False})

        URL examples:
        used('http://mydomain.com/my/awesome/data.RData')
        used(url='http://mydomain.com/my/awesome/data.RData', name='Awesome Data')
        used(url='https://github.com/joe_hacker/code_repo', name='Gnarly hacks', wasExecuted=True)
        used({'url':'https://github.com/joe_hacker/code_repo', 'name':'Gnarly hacks'}, wasExecuted=True)

        List example:
        used( [ 'syn12345', 'syn23456'
                entity,
                {'reference':{'target':'syn100009', 'targetVersion':2}, 'wasExecuted':True},
                'http://mydomain.com/my/awesome/data.RData' ] )
        """

        ## list
        if isinstance(target, list):
            ## check for invalid parameters
            for param in ('targetVersion', 'url', 'name',):
                if locals()[param] is not None:
                    raise Exception('Error in call to Activity.used(): It is an error to specify the \'%s\' parameter in combination with a list of Used resources.' % param)
            for item in target: self.used(item, wasExecuted=wasExecuted)
            return

        ## UsedEntity
        elif is_used_entity(target):
            for param in ('targetVersion', 'url', 'name',):
                if locals()[param] is not None:
                    raise Exception('Error in call to Activity.used(): It is an error to specify the \'%s\' parameter in combination with a dictionary representing a used resource.' % param)
            resource = target
            if 'concreteType' not in resource:
                resource['concreteType'] = 'org.sagebionetworks.repo.model.provenance.UsedEntity'

        ## UsedURL
        elif is_used_url(target):
            for param in ('targetVersion', 'url', 'name',):
                if locals()[param] is not None:
                    raise Exception('Error in call to Activity.used(): It is an error to specify the \'%s\' parameter in combination with a dictionary representing a used resource.' % param)
            resource = target
            if 'concreteType' not in resource:
                resource['concreteType'] = 'org.sagebionetworks.repo.model.provenance.UsedURL'

        ## synapse entity
        elif is_synapse_entity(target):
            reference = {'targetId':target['id']}
            if 'versionNumber' in target:
                reference['targetVersionNumber'] = target['versionNumber']
            ## if targetVersion is specified as a parameter, it overrides the version in the object
            if targetVersion:
                reference['targetVersionNumber'] = int(targetVersion)
            resource = {'reference':reference, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedEntity'}

        ## url parameter
        elif url:
            for param in ('target', 'targetVersion',):
                if locals()[param] is not None:
                    raise Exception('Error in call to Activity.used(): It is an error to specify the \'%s\' parameter in combination with a URL.' % param)
            resource = {'url':url, 'name':name if name else target, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedURL'}

        ## URL as a string
        elif is_url(target):
            for param in ('targetVersion',):
                if locals()[param] is not None:
                    raise Exception('Error in call to Activity.used(): It is an error to specify the \'%s\' parameter in combination with a URL.' % param)
            resource = {'url':target, 'name':name if name else target, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedURL'}

        ## if it's a string and isn't a URL, assume it's a synapse entity id
        elif isinstance(target, basestring):
            for param in ('url', 'name',):
                if locals()[param] is not None:
                    raise Exception('Error in call to Activity.used(): It is an error to specify the \'%s\' parameter in combination with a Synapse entity.' % param)
            reference = {'targetId':target}
            if targetVersion:
                reference['targetVersionNumber'] = int(targetVersion)
            resource = {'reference':reference, 'concreteType':'org.sagebionetworks.repo.model.provenance.UsedEntity'}

        else:
            raise Exception('Unexpected parameters in call to Activity.used().')

        ## set wasExecuted
        if wasExecuted is None:
            ## default to False
            if 'wasExecuted' not in resource:
                resource['wasExecuted'] = False
        else:
            ## wasExecuted parameter overrides setting in an object
            resource['wasExecuted'] = wasExecuted

        ## add the used resource to the activity
        self['used'].append(resource)


    def executed(self, target=None, targetVersion=None, url=None, name=None):
        self.used(target=target, targetVersion=targetVersion, url=url, name=name, wasExecuted=True)
