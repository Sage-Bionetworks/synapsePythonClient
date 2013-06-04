##
## Represent a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
import itertools

from synapseclient.dict_object import DictObject
from synapseclient.utils import id_of, class_of, itersubclasses
import os


## File, Locationable and Summary are Versionable
class Versionable(object):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Versionable'
    _property_keys = ['versionNumber', 'versionLabel', 'versionComment', 'versionUrl', 'versions']


## The Entity class is the base class for all entities. It has a few
## special characteristics. It is a dictionary-like object in which
## either object or dictionary notation (entity.foo or entity['foo'])
## can be used interchangeably.

## In Synapse, entities have both properties and annotations. This has
## come to be viewed as awkward, so we try to hide it. Furthermore,
## because we're getting tricky with the dot notation, there are three
## distinct namespaces to consider when accessing variables that are
## part of the entity: the members of the object, properties defined by
## Synapse, and Synapse annotations, which are open-ended and user-
## defined.

## The rule, for either getting or setting is: first look in the object
## then look in properties, then look in annotations. If the key is not
## found in any of these three, a get results in a KeyError and a set
## results in a new annotation being created. Thus, the following results
## in a new annotation that will be persisted in Synapse:
##   entity.foo = 'bar'

## To create an object member variable, which will *not* be persisted in
## Synapse, this unfortunate notation is required:
##   entity.__dict__['foo'] = 'bar'

## Between the three namespaces, name collisions are entirely possible,
## and already present in at least one instance - the 'annoations'
## property and the 'annotations' member variable that refers to the
## annotations dictionary. Keys in the three namespaces can be referred
## to unambiguously like so:
##   entity.__dict__['key']
##   entity.properties.key / entity.properties['key']
##   entity.annotations.key / entity.annotations['key']

## Alternate implementations include:
##  * a naming convention to tag object members
##  * keeping a list of 'transient' variables (the object members)
##  * giving up on the dot notation (implemented in Entity2.py in commit e441fcf5a6963118bcf2b5286c67fc66c004f2b5 in the entity_object branch)
##  * giving up on hiding the difference between properties and annotations

#TODO inherit from UserDict.DictMixin?
# http://docs.python.org/2/library/userdict.html#UserDict.DictMixin
class Entity(collections.MutableMapping):
    """
    A Synapse entity is an object that has metadata, access control, and
    potentially a file. It can represent data, source code, or a folder
    that contains other entities.
    """

    _synapse_entity_type = 'org.sagebionetworks.repo.model.Entity'
    _property_keys = ['id', 'name', 'description', 'parentId',
                     'entityType', 'concreteType',
                     'uri', 'etag', 'annotations', 'accessControlList',
                     'createdOn', 'createdBy', 'modifiedOn', 'modifiedBy']
    _local_keys = []

    @classmethod
    def create(cls, properties=None, annotations=None, local_state=None):
        """
        Create an Entity or a subclass given dictionaries of properties
        and annotations, as might be received from the Synapse Repository.

        Optionally, allow local state (not persisted in Synapse) to be
        given as well.

        If entityType is defined in properties, we create the proper subclass
        of Entity. If not, give back the type whose constructor was called:

        If passed an Entity as input, create a new Entity using the input
        entity as a prototype.
        """
        ## create a new Entity using an existing entity as a prototype?
        if isinstance(properties, Entity):
            annotations = properties.annotations + (annotations if annotations else {})
            local_state = properties.local_state() + (local_state if local_state else {})
            properties = properties.properties
            del properties['id']
        if cls==Entity and 'entityType' in properties and properties['entityType'] in _entity_type_to_class:
            cls = _entity_type_to_class[properties['entityType']]
        return cls(properties=properties, annotations=annotations, local_state=local_state)

    @classmethod
    def getURI(self, id):
        return '/entity/%s' %id


    def __new__(typ, *args, **kwargs):
        obj = object.__new__(typ, *args, **kwargs)
        ## make really sure that properties and annotations exist before
        ## any object methods get invoked. This is important because the
        ## dot operator magic methods have been over-ridden and depend on
        ## properties and annotations existing.
        obj.__dict__['properties'] = DictObject()
        obj.__dict__['annotations'] = DictObject()
        return obj


    def __init__(self, properties=None, annotations=None, local_state=None, parent=None, **kwargs):

        if properties:
            if isinstance(properties, collections.Mapping):
                if 'annotations' in properties and isinstance(properties['annotations'], collections.Mapping):
                    annotations.update(properties['annotations'])
                    del properties['annotations']
                self.__dict__['properties'].update(properties)
            else:
                raise Exception('Unknown argument type: properties is a %s' % str(type(properties)))

        if annotations:
            if isinstance(annotations, collections.Mapping):
                self.__dict__['annotations'].update(annotations)
            elif isinstance(annotations, basestring):
                self.properties['annotations'] = annotations
            else:
                raise Exception('Unknown argument type: annotations is a %s' % str(type(annotations)))

        if local_state:
            if isinstance(local_state, collections.Mapping):
                self.local_state(local_state)
            else:
                raise Exception('Unknown argument type: local_state is a %s' % str(type(local_state)))

        for key in self.__class__._local_keys:
            if key not in self.__dict__:
                self.__dict__[key] = None

        ## extract parentId from parent
        if 'parentId' not in kwargs:
            try:
                if parent: kwargs['parentId'] = id_of(parent)
            except Exception:
                if parent and isinstance(parent, Entity) and 'id' not in parent:
                    raise Exception('Couldn\'t find \'id\' of parent. Has it been stored in Synapse?')
                else:
                    raise Exception('Couldn\'t find \'id\' of parent.')

        ## note that this will work properly if derived classes declare their
        ## internal state variable *before* invoking super(...).__init__(...)
        for key, value in kwargs.items():
            self.__setitem__(key, value)

        if 'entityType' not in self:
            self['entityType'] = self.__class__._synapse_entity_type


    def postURI(self):
        return '/entity'

    def putURI(self):
        return '/entity/%s' %self.id

    def deleteURI(self):
        return '/entity/%s' %self.id


    def local_state(self, state=None):
        """
        Set or get the object's internal state, excluding properties or annotations.
        state: a dictionary
        """
        if state:
            for key,value in state.items():
                if key not in ['annotations','properties']:
                    self.__dict__[key] = value
        result = {}
        for key,value in self.__dict__.items():
            if key not in ['annotations','properties'] and not key.startswith('__'):
                result[key] = value
        return result


    def __setattr__(self, key, value):
        return self.__setitem__(key, value)


    def __setitem__(self, key, value):
        if key in self.__dict__:
            ## if we assign like so:
            ##   entity.annotations = {'foo';123, 'bar':'bat'}
            ## wrap the dictionary in a DictObject so we can
            ## later do:
            ##   entity.annotations.foo = 'bar'
            if (key=='annotations' or key=='properties') and not isinstance(value, DictObject):
                value = DictObject(value)
            self.__dict__[key] = value
        elif key in self.__class__._property_keys:
            self.properties[key] = value
        else:
            self.annotations[key] = value

    #TODO def __delattr__

    def __getattr__(self, key):
        ## note that __getattr__ is only called after an attempt to
        ## look the key up in the object's dictionary has failed.
        return self.__getitem__(key)


    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        elif key in self.properties:
            return self.properties[key]
        elif key in self.annotations:
            return self.annotations[key]
        else:
            raise KeyError(key)

    def __delitem__(self, key):
        if key in self.properties:
            del self.properties[key]
        elif key in self.annotations:
            del self.annotations[key]


    def __iter__(self):
        return iter(self.keys())


    def __len__(self):
        return len(self.keys())


    def keys(self):
        """return a set of property and annotation keys"""
        return set(self.properties.keys() + self.annotations.keys())

    def has_key(self, key):
        """Is the given key a property or annotation?"""
        return key in self.properties or key in self.annotations

    def __str__(self):            
        from cStringIO import StringIO
        f = StringIO()

        f.write('%s: %s (%s)\n' % (self.__class__.__name__, self.properties.get('name', 'None'), self['id'] if 'id' in self else '-',))

        def write_kvps(dictionary, key_filter=None):
            for key in sorted(dictionary.keys()):
                if (not key_filter) or key_filter(key):
                    f.write('  ')
                    f.write(key)
                    f.write('=')
                    f.write(str(dictionary[key]))
                    f.write('\n')

        write_kvps(self.__dict__, lambda key: not (key in ['properties', 'annotations'] or key.startswith('__')))

        f.write('properties:\n')
        write_kvps(self.properties)

        f.write('annotations:\n')
        write_kvps(self.annotations)

        return f.getvalue()

    def __repr__(self):
        """Returns an eval-able representation of the entity"""
        from cStringIO import StringIO
        f = StringIO()
        f.write(self.__class__.__name__)
        f.write("(")
        f.write(", ".join(
            {"%s=%s" % (str(key), value.__repr__(),) for key, value in 
                itertools.chain(
                    filter(lambda (k,v): not (k in ['properties', 'annotations'] or k.startswith('__')), 
                           self.__dict__.items()),
                    self.properties.items(),
                    self.annotations.items())}))
        f.write(")")
        return f.getvalue()



class Project(Entity):
    """
    Represent a project in Synapse.

    Projects in Synapse must be uniquely named. Trying to create a project with
    a name that's already taken, say 'My project', will result in an error.

        project = Project('Foobarbat project')
        project = syn.store(project)
    """
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Project'

    def __init__(self, name=None, properties=None, annotations=None, local_state=None, **kwargs):
        if name: kwargs['name'] = name
        super(Project, self).__init__(entityType=Project._synapse_entity_type, properties=properties, 
                                      annotations=annotations, local_state=local_state, **kwargs)


class Folder(Entity):
    """
    Represent a folder in Synapse.

    Folders must have a name and a parent and can optionally have annotations.

        folder = Folder('my data', parent=project)
        folder = syn.store(Folder)
    """
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Folder'

    def __init__(self, name=None, parent=None, properties=None, annotations=None, local_state=None, **kwargs):
        if name: kwargs['name'] = name
        super(Folder, self).__init__(entityType=Folder._synapse_entity_type, properties=properties, 
                                     annotations=annotations, local_state=local_state, parent=parent, **kwargs)


class File(Entity, Versionable):
    """
    Represent a file in Synapse.

    When a File object is stored, the associated local file or its URL will be
    stored in Synapse. A File must have a path (or URL) and a parent.

        data = File('/path/to/file/data.xyz', parent=folder)
        data = syn.store(data)
    """
    _property_keys = Entity._property_keys + Versionable._property_keys + ['dataFileHandleId']
    _local_keys = Entity._local_keys + ['path', 'cacheDir', 'files', 'synapseStore', 'externalURL', 'md5', 'fileSize']
    _synapse_entity_type = 'org.sagebionetworks.repo.model.FileEntity'

    #TODO: File(path="/path/to/file", synapseStore=True, parentId="syn101")
    def __init__(self, path=None, parent=None, synapseStore=True, properties=None, 
                 annotations=None, local_state=None, **kwargs):
        if path and 'name' not in kwargs:
            kwargs['name'] = os.path.basename(path)
        self.__dict__['path'] = path
        if path:
            cacheDir, basename = os.path.split(path)
            self.__dict__['cacheDir'] = cacheDir
            self.__dict__['files'] = [basename]
        else:
            self.__dict__['cacheDir'] = None
            self.__dict__['files'] = []
        self.__dict__['synapseStore'] = synapseStore
        super(File, self).__init__(entityType=File._synapse_entity_type, properties=properties, 
                                   annotations=annotations, local_state=local_state, parent=parent, **kwargs)



## Deprecated, but kept around for compatibility with
## old-style Data, Code, Study, etc. entities
class Locationable(Versionable):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Locationable'
    _local_keys = Entity._local_keys + ['cacheDir', 'files', 'path']
    _property_keys = Versionable._property_keys + ['locations', 'md5', 'contentType', 's3Token']


class Analysis(Entity):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Analysis'


class Code(Entity, Locationable):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Code'
    _property_keys = Entity._property_keys + Locationable._property_keys


class Data(Entity, Locationable):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Data'
    _property_keys = Entity._property_keys + Locationable._property_keys


class Study(Entity, Locationable):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Study'
    _property_keys = Entity._property_keys + Locationable._property_keys


class Summary(Entity, Versionable):
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Summary'
    _property_keys = Entity._property_keys + Versionable._property_keys



## Create a mapping from Synapse class (as a string) to the equivalent
## Python class.
_entity_type_to_class = {}
for cls in itersubclasses(Entity):
    _entity_type_to_class[cls._synapse_entity_type] = cls


def split_entity_namespaces(entity):
    """
    Given a plain dictionary or an Entity object,
    split into properties, annotations and local state.
    A dictionary will be processed as a specific type of Entity if its
    entityType field is recognized and otherwise as a generic Entity.
    Returns a 3-item tuple: (properties, annotations, local_state).
    """
    if isinstance(entity, Entity):
        ## defensive programming: return copies
        return (entity.properties.copy(), entity.annotations.copy(), entity.local_state())

    if not isinstance(entity, collections.Mapping):
        raise Exception("Can't call split_entity_namespaces on objects of type: %s" % class_of(entity))

    if 'entityType' in entity and entity['entityType'] in _entity_type_to_class:
        entity_class = _entity_type_to_class[entity['entityType']]
    else:
        entity_class = Entity

    properties = DictObject()
    annotations = DictObject()
    local_state = DictObject()

    property_keys = entity_class._property_keys
    local_keys = entity_class._local_keys
    for key, value in entity.items():
        if key in property_keys:
            properties[key] = value
        elif key in local_keys:
            local_state[key] = value
        else:
            annotations[key] = value

    return (properties, annotations, local_state)


def is_versionable(entity):
    """Return True if the given entity's entityType is one that is Versionable"""
    if 'entityType' in entity and entity['entityType'] in _entity_type_to_class:
        entity_class = _entity_type_to_class[entity['entityType']]
    else:
        entity_class = Entity
    return issubclass(entity_class, Versionable)


def is_locationable(entity):
    """Return True if the given entity is Locationable"""
    if isinstance(entity, collections.Mapping):
        if 'entityType' in entity:
            return entity['entityType'] in ['org.sagebionetworks.repo.model.Data',
                                            'org.sagebionetworks.repo.model.Code',
                                            'org.sagebionetworks.repo.model.ExpressionData',
                                            'org.sagebionetworks.repo.model.GenericData',
                                            'org.sagebionetworks.repo.model.GenomicData',
                                            'org.sagebionetworks.repo.model.GenotypeData',
                                            'org.sagebionetworks.repo.model.Media',
                                            'org.sagebionetworks.repo.model.PhenotypeData',
                                            'org.sagebionetworks.repo.model.RObject',
                                            'org.sagebionetworks.repo.model.Study',
                                            'org.sagebionetworks.repo.model.ExampleEntity']
        else:
            return 'locations' in entity
    else:
        raise Exception('Can\'t determine if %s is Locationable' % str(entity))

