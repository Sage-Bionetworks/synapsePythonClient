##
## Represent a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
from dict_object import DictObject
from utils import id_of, entity_type
import os


class Entity(collections.MutableMapping):
    """
    A Synapse entity is an object that has metadata, access control, and
    potentially a file. It can represent data, source code, or a folder
    that contains other entities.
    """

    _property_keys = ['id', 'name', 'description', 'parentId',
                     'entityType', 'concreteType',
                     'uri', 'etag', 'annotations', 'accessControlList',
                     'createdOn', 'createdBy', 'modifiedOn', 'modifiedBy',
                     'versionNumber', 'versionLabel', 'versionComment', 'versionUrl', 'versions']


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


    def __new__(typ, *args, **kwargs):
        obj = object.__new__(typ, *args, **kwargs)
        obj.__dict__['properties'] = DictObject()
        obj.__dict__['annotations'] = DictObject()
        return obj


    def __init__(self, properties=None, annotations=None, **kwargs):

        if properties:
            if isinstance(properties, collections.Mapping):
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

        for key, value in kwargs.items():
            self.__setitem__(key, value)


    def __setattr__(self, key, value):
        if key in self.__dict__:
            ## if we assign like so:
            ##   entity.annotations = {'foo';123, 'bar':'bat'}
            ## wrap the dictionary in a DictObject
            if key=='annotations' and not isinstance(value, DictObject):
                value = DictObject(value)
            object.__setattr__(self, key, value)
        else:
            self.__setitem__(key, value)

    #TODO def __delattr__

    def __getattr__(self, key):
        ## note that __getattr__ is only called after an attempt to
        ## look the key up in the object's dictionary has failed
        return self.__getitem__(key)

    def keys(self):
        return set(self.properties.keys() + self.annotations.keys())


    def __getitem__(self, key):
        if key in self.properties:
            return self.properties[key]
        elif key in self.annotations:
            return self.annotations[key]
        else:
            raise KeyError(key)


    def __setitem__(self, key, value):
        if key in self.__class__._property_keys:
            self.properties[key] = value
        else:
            self.annotations[key] = value


    def __delitem__(self, key):
        if key in self.properties:
            del self.properties[key]
        elif key in self.annotations:
            del self.annotations[key]


    def __iter__(self):
        return iter(self.keys())


    def __len__(self):
        return len(self.keys())


    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        from cStringIO import StringIO
        f = StringIO()
        f.write('Entity: %s %s\n' % (self.properties.get('name', 'None'), entity_type(self),))
        f.write('properties:\n')
        for key in sorted(self.properties.keys()):
            f.write('  ')
            f.write(key)
            f.write('=')
            f.write(str(self.properties[key]))
            f.write('\n')
        f.write('annotations:\n')
        for key in sorted(self.annotations.keys()):
            f.write('  ')
            f.write(key)
            f.write('=')
            f.write(str(self.annotations[key]))
            f.write('\n')
        return f.getvalue()



class Project(Entity):
    def __init__(self, name=None, properties=None, annotations=None, **kwargs):
        if name: kwargs['name'] = name
        super(Project, self).__init__(entityType='org.sagebionetworks.repo.model.Project', properties=properties, annotations=annotations, **kwargs)


class Folder(Entity):
    def __init__(self, name=None, parent=None, properties=None, annotations=None, **kwargs):
        if name: kwargs['name'] = name
        if parent: kwargs['parentId'] = id_of(parent)
        super(Folder, self).__init__(entityType='org.sagebionetworks.repo.model.Folder', properties=properties, annotations=annotations, **kwargs)


class File(Entity):
    _property_keys = Entity._property_keys + ['dataFileHandleId']

    ## File(path="/path/to/file", synapseStore=True, parentId="syn101")
    def __init__(self, path=None, parent=None, synapseStore=True, properties=None, annotations=None, **kwargs):
        name = os.path.basename(path)
        if name: kwargs['name'] = name
        if parent: kwargs['parentId'] = id_of(parent)
        super(File, self).__init__(entityType='org.sagebionetworks.repo.model.File', properties=properties, annotations=annotations, **kwargs)
        self.__dict__['path'] = path



_entity_type_to_class = {'org.sagebionetworks.repo.model.Project':Project,
                        'org.sagebionetworks.repo.model.Folder':Folder,
                        'org.sagebionetworks.repo.model.FileEntity':File}

_class_to_entity_type = {v:k for k,v in _entity_type_to_class.items()}

# org.sagebionetworks.repo.model.Analysis
# org.sagebionetworks.repo.model.Code
# org.sagebionetworks.repo.model.Data
# org.sagebionetworks.repo.model.Study
# org.sagebionetworks.repo.model.Summary
