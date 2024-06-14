"""
# Provenance

The Activity object represents the source of a data set or the data processing steps used to produce it. Using
[W3C provenance ontology](http://www.w3.org/2011/prov/wiki/Main_Page) terms, a result is **generated by** a
combination of data and code which are either **used** or **executed**.

## Imports

    from synapseclient import Activity


## Creating an activity object

    act = Activity(name='clustering',
                   description='whizzy clustering',
                   used=['syn1234','syn1235'],
                   executed='syn4567')

Here, syn1234 and syn1235 might be two types of measurements on a common set of samples. Some whizzy clustering code
might be referred to by syn4567.  The used and executed can reference entities in Synapse or URLs.

Alternatively, you can build an activity up piecemeal:

    act = Activity(name='clustering', description='whizzy clustering')
    act.used(['syn12345', 'syn12346'])
    act.executed(
        'https://raw.githubusercontent.com/Sage-Bionetworks/synapsePythonClient/develop/tests/unit/unit_test_client.py')


## Storing entities with provenance


The activity can be passed in when storing an Entity to set the Entity's provenance:

    clustered_samples = syn.store(clustered_samples, activity=act)

We've now recorded that `clustered_samples` is the output of our whizzy clustering algorithm applied to the data stored
in syn1234 and syn1235.


## Recording data source


The [synapseclient.Synapse.store][] has shortcuts for specifying the used and executed lists directly.
For example, when storing a data entity, it's a good idea to record its source:

    excellent_data = syn.store(excellent_data,
                               activityName='data-r-us'
                               activityDescription='downloaded from data-r-us',
                               used='http://data-r-us.com/excellent/data.xyz')

"""

import collections.abc

from synapseclient.core.exceptions import SynapseError, SynapseMalformedEntityError
from synapseclient.core.utils import get_synid_and_version, is_synapse_id_str, is_url
from synapseclient.entity import is_synapse_entity


def is_used_entity(x) -> bool:
    """
    Returns:
        True if the given object represents a UsedEntity.
    """

    # A UsedEntity must be a dictionary with a 'reference' field, with a 'targetId' field
    if (
        not isinstance(x, collections.abc.Mapping)
        or "reference" not in x
        or "targetId" not in x["reference"]
    ):
        return False

    # Must only have three keys
    if not all(key in ("reference", "wasExecuted", "concreteType") for key in x.keys()):
        return False

    # 'reference' field can only have two keys
    if not all(
        key in ("targetId", "targetVersionNumber") for key in x["reference"].keys()
    ):
        return False

    return True


def is_used_url(x) -> bool:
    """
    Returns:
        True if the given object represents a UsedURL.
    """

    # A UsedURL must be a dictionary with a 'url' field
    if not isinstance(x, collections.abc.Mapping) or "url" not in x:
        return False

    # Must only have four keys
    if not all(
        key in ("url", "name", "wasExecuted", "concreteType") for key in x.keys()
    ):
        return False

    return True


def _get_any_bad_args(badargs, dictionary):
    """Returns the intersection of 'badargs' and the non-Null keys of 'dictionary'."""

    return list(
        illegal
        for illegal in badargs
        if illegal in dictionary and dictionary[illegal] is not None
    )


def _raise_incorrect_used_usage(badargs, message):
    """Raises an informative exception about Activity.used()."""

    if any(badargs):
        raise SynapseMalformedEntityError(
            "The parameter%s '%s' %s not allowed in combination with a %s."
            % (
                "s" if len(badargs) > 1 else "",
                badargs,
                "are" if len(badargs) > 1 else "is",
                message,
            )
        )


class Activity(dict):
    """
    Represents the provenance of a Synapse Entity.

    Parameters:
        name: Name of the Activity
        description: A short text description of the Activity
        used: Either a list of:

            - [reference objects](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Reference.html) (e.g. [{'targetId':'syn123456', 'targetVersionNumber':1}])
            - a list of Synapse Entities or Entity IDs
            - a list of URL's
        executed: A code resource that was executed to generate the Entity.
        data: A dictionary representation of an Activity, with fields 'name', 'description' and 'used' (a list of reference objects)

    See also: The [W3C's provenance ontology](http://www.w3.org/TR/prov-o/)
    """

    # TODO: make constructors from JSON consistent across objects
    def __init__(self, name=None, description=None, used=None, executed=None, data={}):
        super(Activity, self).__init__(data)
        if "used" not in self:
            self["used"] = []

        if name is not None:
            self["name"] = name
        if description is not None:
            self["description"] = description
        if used is not None:
            self.used(used)
        if executed is not None:
            self.executed(executed)

    def used(
        self, target=None, targetVersion=None, wasExecuted=None, url=None, name=None
    ):
        """
        Add a resource used by the activity.

        This method tries to be as permissive as possible. It accepts a string which might be a synapse ID or a URL,
        a synapse entity, a UsedEntity or UsedURL dictionary or a list containing any combination of these.

        In addition, named parameters can be used to specify the fields of either a UsedEntity or a UsedURL.
        If target and optionally targetVersion are specified, create a UsedEntity.
        If url and optionally name are specified, create a UsedURL.

        It is an error to specify both target/targetVersion parameters and url/name parameters in the same call.
        To add multiple UsedEntities and UsedURLs, make a separate call for each or pass in a list.

        In case of conflicting settings for wasExecuted both inside an object and with a parameter, the parameter wins.
        For example, this UsedURL will have wasExecuted set to False:

            activity.used({'url':'http://google.com', 'name':'Goog', 'wasExecuted':True}, wasExecuted=False)

        Entity examples:

            activity.used('syn12345')
            activity.used(entity)
            activity.used(target=entity, targetVersion=2)
            activity.used(codeEntity, wasExecuted=True)
            activity.used({'reference':{'target':'syn12345', 'targetVersion':1}, 'wasExecuted':False})

        URL examples:

            activity.used('http://mydomain.com/my/awesome/data.RData')
            activity.used(url='http://mydomain.com/my/awesome/data.RData', name='Awesome Data')
            activity.used(url='https://github.com/joe_hacker/code_repo', name='Gnarly hacks', wasExecuted=True)
            activity.used({'url':'https://github.com/joe_hacker/code_repo', 'name':'Gnarly hacks'}, wasExecuted=True)

        List example:

            activity.used(['syn12345', 'syn23456', entity, \
                          {'reference':{'target':'syn100009', 'targetVersion':2}, 'wasExecuted':True}, \
                          'http://mydomain.com/my/awesome/data.RData'])
        """
        # -- A list of targets
        if isinstance(target, list):
            badargs = _get_any_bad_args(["targetVersion", "url", "name"], locals())
            _raise_incorrect_used_usage(badargs, "list of used resources")

            for item in target:
                self.used(item, wasExecuted=wasExecuted)
            return

        # -- UsedEntity
        elif is_used_entity(target):
            badargs = _get_any_bad_args(["targetVersion", "url", "name"], locals())
            _raise_incorrect_used_usage(
                badargs, "dictionary representing a used resource"
            )

            resource = target
            if "concreteType" not in resource:
                resource[
                    "concreteType"
                ] = "org.sagebionetworks.repo.model.provenance.UsedEntity"

        # -- Used URL
        elif is_used_url(target):
            badargs = _get_any_bad_args(["targetVersion", "url", "name"], locals())
            _raise_incorrect_used_usage(badargs, "URL")

            resource = target
            if "concreteType" not in resource:
                resource[
                    "concreteType"
                ] = "org.sagebionetworks.repo.model.provenance.UsedURL"

        # -- Synapse Entity
        elif is_synapse_entity(target):
            badargs = _get_any_bad_args(["url", "name"], locals())
            _raise_incorrect_used_usage(badargs, "Synapse entity")

            reference = {"targetId": target["id"]}
            if "versionNumber" in target:
                reference["targetVersionNumber"] = target["versionNumber"]
            if targetVersion:
                reference["targetVersionNumber"] = int(targetVersion)
            resource = {
                "reference": reference,
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedEntity",
            }
        # -- URL parameter
        elif url:
            badargs = _get_any_bad_args(["target", "targetVersion"], locals())
            _raise_incorrect_used_usage(badargs, "URL")

            resource = {
                "url": url,
                "name": name if name else target,
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
            }

        # -- URL as a string
        elif is_url(target):
            badargs = _get_any_bad_args(["targetVersion"], locals())
            _raise_incorrect_used_usage(badargs, "URL")
            resource = {
                "url": target,
                "name": name if name else target,
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
            }

        # -- Synapse Entity ID (assuming the string is an ID)
        elif isinstance(target, str):
            badargs = _get_any_bad_args(["url", "name"], locals())
            _raise_incorrect_used_usage(badargs, "Synapse entity")
            if not is_synapse_id_str(target):
                raise ValueError("%s is not a valid Synapse id" % target)
            synid, version = get_synid_and_version(
                target
            )  # Handle synapseIds of from syn234.4
            if version:
                if targetVersion and int(targetVersion) != int(version):
                    raise ValueError(
                        "Two conflicting versions for %s were specified" % target
                    )
                targetVersion = int(version)
            reference = {"targetId": synid}
            if targetVersion:
                reference["targetVersionNumber"] = int(targetVersion)
            resource = {
                "reference": reference,
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedEntity",
            }
        else:
            raise SynapseError("Unexpected parameters in call to Activity.used().")

        # Set wasExecuted
        if wasExecuted is None:
            # Default to False
            if "wasExecuted" not in resource:
                resource["wasExecuted"] = False
        else:
            # wasExecuted parameter overrides setting in an object
            resource["wasExecuted"] = wasExecuted

        # Add the used resource to the activity
        self["used"].append(resource)

    def executed(self, target=None, targetVersion=None, url=None, name=None):
        """
        Add a code resource that was executed during the activity.
        See [synapseclient.activity.Activity.used][]
        """
        self.used(
            target=target,
            targetVersion=targetVersion,
            url=url,
            name=name,
            wasExecuted=True,
        )

    def _getStringList(self, wasExecuted=True):
        usedList = []
        for source in [
            source
            for source in self["used"]
            if source.get("wasExecuted", False) == wasExecuted
        ]:
            if source["concreteType"].endswith("UsedURL"):
                if source.get("name"):
                    usedList.append(source.get("name"))
                else:
                    usedList.append(source.get("url"))
            else:  # It is an entity for now
                tmpstr = source["reference"]["targetId"]
                if "targetVersionNumber" in source["reference"]:
                    tmpstr += ".%i" % source["reference"]["targetVersionNumber"]
                usedList.append(tmpstr)
        return usedList

    def _getExecutedStringList(self):
        return self._getStringList(wasExecuted=True)

    def _getUsedStringList(self):
        return self._getStringList(wasExecuted=False)

    def __str__(self):
        str = "%s\n  Executed:\n" % self.get("name", "")
        str += "\n".join(self._getExecutedStringList())
        str += "  Used:\n"
        str += "\n".join(self._getUsedStringList())
        return str
