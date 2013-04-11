##
## Represent a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
from dict_object import DictObject


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

    def __init__(self, properties=None, annotations=None, **kwargs):

        ## properties
        self.__dict__['properties'] = DictObject()
        if properties:
            if isinstance(properties, collections.Mapping):
                self.__dict__['properties'].update(properties)
            else:
                raise Exception('Unknown argument type: properties is a %s' % str(type(properties)))

        ## annotations
        self.__dict__['annotations'] = DictObject()
        if annotations:
            if isinstance(annotations, collections.Mapping):
                self.__dict__['annotations'].update(annotations)
            elif isinstance(annotations, basestring):
                self.properties['annotations'] = annotations
            else:
                raise Exception('Unknown argument type: annotations is a %s' % str(type(annotations)))

        for key in kwargs.keys():
            if key in self.__class__._property_keys:
                self.properties[key] = kwargs[key]
            else:
                self.annotations[key] = kwargs[key]


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


    def __getattr__(self, key):
        return self.__getitem__(key)


    def keys(self):
        return set(self.properties.keys() + self.annotations.keys())


    def __getitem__(self, key):
        if key in self.properties:
            return self.properties[key]
        elif key in self.annotations:
            return self.annotations[key]


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
        f.write('properties:\n')
        for key in sorted(self.properties.keys()):
            f.write('  ')
            f.write(key)
            f.write('=')
            f.write(str(self.properties[key]))
            f.write('\n')
        f.write('anotations:\n')
        for key in sorted(self.annotations.keys()):
            f.write('  ')
            f.write(key)
            f.write('=')
            f.write(str(self.annotations[key]))
            f.write('\n')
        return f.getvalue()
