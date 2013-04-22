##
## Represent a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
from dict_object import DictObject
from utils import id_of, entity_type, itersubclasses
import os



## File, Locationable and Summary are Versionable
class Versionable(object):
    synapse_class = 'org.sagebionetworks.repo.model.Versionable'
    _property_keys = ['versionNumber', 'versionLabel', 'versionComment', 'versionUrl', 'versions']


## compromises:
##   give on dot notation
##   properties and annotations share a namespace.


class Entity(dict):
    synapse_class = 'org.sagebionetworks.repo.model.Entity'
    _property_keys = ['id', 'name', 'description', 'parentId',
                     'entityType', 'concreteType',
                     'uri', 'etag', 'annotations', 'accessControlList',
                     'createdOn', 'createdBy', 'modifiedOn', 'modifiedBy']

    @classmethod
    def create(cls, properties=None, annotations=None):
        """
        Create an Entity or a subclass given dictionaries of properties
        and annotations, as might be received from the Synapse Repository.

        If entityType is defined in properties, we create the proper subclass
        of Entity. If not, give back the type asked for.
        """
        if cls==Entity and 'entityType' in properties and properties['entityType'] in _entity_type_to_class:
            cls = _entity_type_to_class[properties['entityType']]
        return cls(properties=properties, annotations=annotations)

    def __init__(self, properties=None, annotations=None, **kwargs):
        if annotations:
            if isinstance(annotations, collections.Mapping):
                for key, value in annotations.items():
                    self.__setitem__(key, value)

        ## properties will destructively overwrite annotations
        if properties:
            if isinstance(properties, collections.Mapping):
                for key, value in properties.items():
                    self.__setitem__(key, value)

        for key, value in kwargs.items():
            self.__setitem__(key, value)

    def properties(self):
        for key in self._property_keys:
            if key in self:
                yield (key, self[key],)

    def annotations(self):
        for key in self:
            if key not in self._property_keys:
                yield (key, self[key],)



class Project(Entity):
    _synapse_class = 'org.sagebionetworks.repo.model.Project'

    def __init__(self, name=None, properties=None, annotations=None, **kwargs):
        if name: kwargs['name'] = name
        super(Project, self).__init__(entityType=Project._synapse_class, properties=properties, annotations=annotations, **kwargs)


class Folder(Entity):
    _synapse_class = 'org.sagebionetworks.repo.model.Folder'

    def __init__(self, name=None, parent=None, properties=None, annotations=None, **kwargs):
        if name: kwargs['name'] = name
        if parent: kwargs['parentId'] = id_of(parent)
        super(Folder, self).__init__(entityType=Folder._synapse_class, properties=properties, annotations=annotations, **kwargs)


class File(Entity, Versionable):
    _property_keys = Entity._property_keys + Versionable._property_keys + ['dataFileHandleId']
    _synapse_class = 'org.sagebionetworks.repo.model.FileEntity'

    ## File(path="/path/to/file", synapseStore=True, parentId="syn101")
    def __init__(self, path=None, parent=None, synapseStore=True, properties=None, annotations=None, **kwargs):
        if path and 'name' not in kwargs:
            kwargs['name'] = os.path.basename(path)
        if parent: kwargs['parentId'] = id_of(parent)
        super(File, self).__init__(entityType=File._synapse_class, properties=properties, annotations=annotations, **kwargs)
        if path:
            self.path = path



## Deprecated, but kept around for compatibility with
## old-style Data, Code, Study, etc. entities
class Locationable(Versionable):
    _synapse_class = 'org.sagebionetworks.repo.model.Locationable'
    _property_keys = Versionable._property_keys + ['locations', 'md5', 'contentType', 's3Token']


class Analysis(Entity):
    _synapse_class = 'org.sagebionetworks.repo.model.Analysis'


class Code(Entity, Locationable):
    _synapse_class = 'org.sagebionetworks.repo.model.Code'
    _property_keys = Entity._property_keys + Locationable._property_keys


class Data(Entity, Locationable):
    _synapse_class = 'org.sagebionetworks.repo.model.Data'
    _property_keys = Entity._property_keys + Locationable._property_keys


class Study(Entity, Locationable):
    _synapse_class = 'org.sagebionetworks.repo.model.Study'
    _property_keys = Entity._property_keys + Locationable._property_keys


class Summary(Entity, Versionable):
    _synapse_class = 'org.sagebionetworks.repo.model.Summary'
    _property_keys = Entity._property_keys + Versionable._property_keys



## Create a mapping from Synapse class (as a string) to the equivalent
## Python class.
_entity_type_to_class = {}
for cls in itersubclasses(Entity):
    _entity_type_to_class[cls._synapse_class] = cls

