import collections
from annotations import Annotations


class DictObject(dict):
    def __init__(self, *args, **kwargs):
        self.__dict__ = self
        for arg in args:
            if isinstance(arg, collections.Mapping):
                self.__dict__.update(arg)
        self.__dict__.update(kwargs)

# class DictObject(collections.MutableMapping):
#     def __init__(self, *args, **kwargs):
#         for arg in args:
#             if isinstance(arg, collections.Mapping):
#                 self.__dict__.update(arg)
#         self.__dict__.update(kwargs)

#     def __setattr__(self, key, value):
#         self.__dict__[key] = value

#     def __getattr__(self, key):
#         return self.__dict__[key]

#     def __getitem__(self, key):
#         return self.__dict__[key]

#     def __setitem__(self, key, value):
#         self.__dict__[key] = value

#     def __delitem__(self, key):
#         del self.__dict__[key]

#     def __iter__(self):
#         return iter(self.__dict__.keys())

#     def __len__(self):
#         return len(self.__dict__.keys())

#     def __str__(self):
#         return str(self.__dict__)


d = DictObject({'args_working?':'yes'},a=123, b='foobar', nerds=['chris','jen','janey'])
assert d.a==123
assert d['a']==123
assert d.b=='foobar'
assert d['b']=='foobar'
assert d.nerds==['chris','jen','janey']
assert d['nerds']==['chris','jen','janey']
print d.keys()
print d
d.new_key = 'new value!'
assert d['new_key'] == 'new value!'
print d.keys()
print d



class Entity(collections.MutableMapping):
    """
    A Synapse entity is an object that has metadata, access control, and
    potentially a file. It can represent data, source code, or a folder
    that contains other entities.
    """

    property_keys = ['name', 'description', 'annotations', 'parentId', 'entityType']

    def __init__(self, properties=None, annotations=None, **kwargs):

        self.__dict__['properties'] = DictObject()
        if properties:
            if isinstance(properties, collections.Mapping):
                self.__dict__['properties'].update(properties)
            else:
                raise Exception('Unknown argument type: properties is a %s' % str(type(properties)))

        ## annotations might be the url that should be part of properties
        ## or it might be the dictionary of annotations
        self.__dict__['annotations'] = DictObject()
        if annotations:
            if isinstance(annotations, collections.Mapping):
                self.__dict__['annotations'].update(annotations)
            elif isinstance(annotations, basestring):
                self.properties['annotations'] = annotations
            else:
                raise Exception('Unknown argument type: annotations is a %s' % str(type(annotations)))

        for key in kwargs.keys():
            if key in Entity.property_keys:
                self.properties[key] = kwargs[key]
            else:
                self.annotations[key] = kwargs[key]


    def __setattr__(self, key, value):
        if key in self.__dict__:
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
        if key in Entity.property_keys:
            self.properties[key] = value
        elif key in self.annotations:
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


def test(e):
    assert e.name == 'Test object'
    assert e.properties.name == 'Test object'

    assert e.parentId == 'syn1234'
    assert e['parentId'] == 'syn1234'
    assert e.properties['parentId'] == 'syn1234'
    assert e.properties.parentId =='syn1234'

    assert e.foo == 123
    assert e['foo'] == 123
    assert e.annotations['foo'] == 123
    assert e.annotations.foo == 123

    ## annotations is a bit funny, if we want e.annotations to point to a
    ## Mapping of annotations and e['annotations'] to be the property 'annotations'
    assert isinstance(e.annotations, collections.Mapping)
    assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'

    assert e.nerds == ['chris','jen','janey']
    assert e.md5 == 'cdef636522577fc8fb2de4d95875b27c'

    print "keys = " + str(e.keys())
    assert all([ k in e for k in ['name', 'description', 'foo', 'nerds', 'annotations', 'md5', 'parentId']])
    assert len(e) == 8

    e.annotations = {'splat':'a totally new set of annotations', 'foo':456}
    assert e.foo == 456
    assert e['foo'] == 456
    assert e.annotations.foo == 456
    assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'

    print 'ok'


e1 = Entity(name='Test object', description='I hope this works',
            annotations = dict(foo=123, nerds=['chris','jen','janey']),
            properties  = dict(annotations='/repo/v1/entity/syn1234/annotations',
                               md5='cdef636522577fc8fb2de4d95875b27c',
                               parentId='syn1234'),
            entityType='org.sagebionetworks.repo.model.Data')
test(e1)
print e1

e2 = Entity(name='Test object', description='I hope this works',
            foo=123,
            nerds=['chris','jen','janey'],
            annotations='/repo/v1/entity/syn1234/annotations',
            md5='cdef636522577fc8fb2de4d95875b27c',
            parentId='syn1234',
            entityType='org.sagebionetworks.repo.model.Data')
test(e2)
print e2


import synapseclient
syn = synapseclient.Synapse()
syn.login()

try:
    f = syn.getEntity('syn1658649')

    e2.parentId = f['id']

    import uuid
    e2.name = 'Test Project ' + str(uuid.uuid4())

    e = syn.createEntity(e2.properties)
    a = syn.setAnnotations(e, e2.annotations)
except Exception as ex:
    print ex
    print ex.response.text
# finally:
#     if e:
#         syn.deleteEntity(e)

