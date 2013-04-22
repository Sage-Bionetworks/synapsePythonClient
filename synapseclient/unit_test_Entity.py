from nose.tools import *
import collections
from entity import Entity, Project, Folder, File, Data


def test_Entity():
    """Test the basics of creating and accessing properties on an entity"""
    e =  Entity(name='Test object', description='I hope this works',
                annotations = dict(foo=123, nerds=['chris','jen','janey'], annotations='How confusing!'),
                properties  = dict(annotations='/repo/v1/entity/syn1234/annotations',
                               md5='cdef636522577fc8fb2de4d95875b27c',
                               parentId='syn1234'),
                entityType='org.sagebionetworks.repo.model.Data')

    assert e.parentId == 'syn1234'
    assert e['parentId'] == 'syn1234'
    assert e.properties['parentId'] == 'syn1234'
    assert e.properties.parentId =='syn1234'

    assert e.foo == 123
    assert e['foo'] == 123
    assert e.annotations['foo'] == 123
    assert e.annotations.foo == 123

    ## annotations is a bit funny, because there is a property call
    ## 'annotations', which will be masked by a member of the object
    ## called 'annotations'. Because annotations are open-ended, we
    ## might even have an annotations called 'annotations', which gets
    ## really confusing.
    assert isinstance(e.annotations, collections.Mapping)
    assert isinstance(e['annotations'], collections.Mapping)
    assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'
    assert e.annotations.annotations == 'How confusing!'
    assert e.annotations['annotations'] == 'How confusing!'

    assert e.nerds == ['chris','jen','janey']

    #print "keys = " + str(e.keys())
    assert all([ k in e for k in ['name', 'description', 'foo', 'nerds', 'annotations', 'md5', 'parentId']])

    ## test modifying props
    e.description = 'Working, so far'
    assert e['description'] == 'Working, so far'
    e['description'] = 'Wiz-bang flapdoodle'
    assert e.description == 'Wiz-bang flapdoodle'

    ## test modifying annos
    e.foo = 999
    assert e.annotations['foo'] == 999
    e['foo'] = 12345
    assert e.annotations.foo == 12345

    ## test creating a new annotation
    e['bar'] = 888

    assert e.annotations['bar'] == 888
    e['bat'] = 7788
    assert e.annotations['bat'] == 7788

    ## test replacing annotations object
    e.annotations = {'splat':'a totally new set of annotations', 'foo':456}
    assert e.foo == 456
    assert e['foo'] == 456
    assert isinstance(e.annotations, collections.Mapping)
    assert isinstance(e['annotations'], collections.Mapping)
    assert e.annotations.foo == 456
    assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'


def test_subclassing():
    """Test ability to subclass and add a member variable"""
    
    ## define a subclass of Entity to make sure subclassing and creating
    ## a new member variable works
    class FoobarEntity(Entity):
        def __init__(self, x):
            self.__dict__['x'] = x

    foobar = FoobarEntity(123)
    assert foobar.x == 123
    assert 'x' in foobar.__dict__
    assert foobar.__dict__['x'] == 123
    foobar.id = 'syn999'
    assert foobar.properties['id'] == 'syn999'
    foobar.n00b = 'henry'
    assert foobar.annotations['n00b'] == 'henry'

    print foobar


def test_entity_creation():
    props = {
        "id": "syn123456",
        "entityType": "org.sagebionetworks.repo.model.Folder",
        "parentId": "syn445566",
        "name": "Testing123"
    }
    annos = {'testing':123}
    folder = Entity.create(props, annos)

    assert folder.entityType == 'org.sagebionetworks.repo.model.Folder'
    assert folder.__class__ == Folder
    assert folder.name == 'Testing123'
    assert folder.testing == 123

    props = {
        "id": "syn123456",
        "entityType": "org.sagebionetworks.repo.model.DoesntExist",
        "name": "Whatsits"
    }
    whatsits = Entity.create(props)

    assert whatsits.entityType == 'org.sagebionetworks.repo.model.DoesntExist'
    assert whatsits.__class__ == Entity


def test_entity_constructors():
    project = Project(name=str(uuid.uuid4()), description='Testing 123')

    folder = Folder(name='Musicians', parent=project, genre='Jazz', datatype='personnel')

    personnel_file = File(fname, parentId=folder.id, group='Miles Davis Quintet', album='Stockholm 1960 Complete')


def test_entity_constructors():
    project = Project('TestProject', id='syn1001', foo='bar')
    assert project.name == 'TestProject'
    assert project['foo'] == 'bar'

    folder = Folder('MyFolder', parent=project, foo='bat', id='syn1002')
    assert folder.name == 'MyFolder'
    assert folder.foo == 'bat'
    assert folder.parentId == 'syn1001'

    a_file = File('/path/to/fabulous_things.zzz', parent=folder, foo='biz')
    #assert a_file.name == 'fabulous_things.zzz'
    assert a_file.entityType == 'org.sagebionetworks.repo.model.FileEntity'
    assert a_file.path == '/path/to/fabulous_things.zzz'
    assert a_file.foo == 'biz'
    assert a_file.parentId == 'syn1002'


def test_property_keys():
    assert 'parentId' in File._property_keys
    assert 'versionNumber' in File._property_keys
    assert 'dataFileHandleId' in File._property_keys


def test_asdf():
    f = File('foo.xyz', parent='syn1234', foo='bar')
    print f.keys()

    iter_keys = []
    for key in f:
        iter_keys.append(key)
    assert 'parentId' in iter_keys
    assert 'name' in iter_keys
    assert 'foo' in iter_keys
    assert 'entityType' in iter_keys


