# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str, ascii

import collections
import os, sys
from synapseclient.entity import Entity, Project, Folder, File, split_entity_namespaces, is_container
from synapseclient.exceptions import *
from nose.tools import assert_raises, assert_true, assert_false


def setup():
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)


def test_Entity():
    # Test the basics of creating and accessing properties on an entity
    for i in range(2):
        e = Entity(name='Test object', description='I hope this works',
                   annotations = dict(foo=123, nerds=['chris','jen','janey'], annotations='How confusing!'),
                   properties  = dict(annotations='/repo/v1/entity/syn1234/annotations',
                                      md5='cdef636522577fc8fb2de4d95875b27c',
                                      parentId='syn1234'),
                   concreteType='org.sagebionetworks.repo.model.Data')

        # Should be able to create an Entity from an Entity
        if i == 1:
            e = Entity.create(e)
            
        assert e.parentId == 'syn1234'
        assert e['parentId'] == 'syn1234'
        assert e.properties['parentId'] == 'syn1234'
        assert e.properties.parentId =='syn1234'

        assert e.foo == 123
        assert e['foo'] == 123
        assert e.annotations['foo'] == 123
        assert e.annotations.foo == 123

        assert hasattr(e, 'parentId')
        assert hasattr(e, 'foo')
        assert not hasattr(e, 'qwerqwer')

        # Annotations is a bit funny, because there is a property call
        # 'annotations', which will be masked by a member of the object
        # called 'annotations'. Because annotations are open-ended, we
        # might even have an annotations called 'annotations', which gets
        # really confusing.
        assert isinstance(e.annotations, collections.Mapping)
        assert isinstance(e['annotations'], collections.Mapping)
        assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
        assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'
        assert e.annotations.annotations == 'How confusing!'
        assert e.annotations['annotations'] == 'How confusing!'
        assert e.nerds == ['chris','jen','janey']
        assert all([ k in e for k in ['name', 'description', 'foo', 'nerds', 'annotations', 'md5', 'parentId']])

        # Test modifying properties
        e.description = 'Working, so far'
        assert e['description'] == 'Working, so far'
        e['description'] = 'Wiz-bang flapdoodle'
        assert e.description == 'Wiz-bang flapdoodle'

        # Test modifying annotations
        e.foo = 999
        assert e.annotations['foo'] == 999
        e['foo'] = 12345
        assert e.annotations.foo == 12345

        # Test creating a new annotation
        e['bar'] = 888
        assert e.annotations['bar'] == 888
        e['bat'] = 7788
        assert e.annotations['bat'] == 7788

        # Test replacing annotations object
        e.annotations = {'splat':'a totally new set of annotations', 'foo':456}
        assert e.foo == 456
        assert e['foo'] == 456
        assert isinstance(e.annotations, collections.Mapping)
        assert isinstance(e['annotations'], collections.Mapping)
        assert e.annotations.foo == 456
        assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
        assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'

        ## test unicode properties
        e.train = '時刻表には記載されない　月への列車が来ると聞いて'
        e.band = "Motörhead"
        e.lunch = "すし"

        if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding and sys.stdout.encoding.lower() == 'utf-8':
            print(e)
        else:
            print(ascii(e))


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


def test_entity_creation():
    props = {
        "id": "syn123456",
        "concreteType": "org.sagebionetworks.repo.model.Folder",
        "parentId": "syn445566",
        "name": "Testing123"
    }
    annos = {'testing':123}
    folder = Entity.create(props, annos)

    assert folder.concreteType == 'org.sagebionetworks.repo.model.Folder'
    assert folder.__class__ == Folder
    assert folder.name == 'Testing123'
    assert folder.testing == 123

    ## In case of unknown concreteType, fall back on generic Entity object
    props = {
        "id": "syn123456",
        "concreteType": "org.sagebionetworks.repo.model.DoesntExist",
        "parentId": "syn445566",
        "name": "Whatsits"
    }
    whatsits = Entity.create(props)

    assert whatsits.concreteType == 'org.sagebionetworks.repo.model.DoesntExist'
    assert whatsits.__class__ == Entity


def test_parent_id_required():
    xkcd1 = File('http://xkcd.com/1343/', name='XKCD: Manuals', parent='syn1000001', synapseStore=False)
    assert xkcd1.parentId == 'syn1000001'

    xkcd2 = File('http://xkcd.com/1343/', name='XKCD: Manuals', parentId='syn1000002', synapseStore=False)
    assert xkcd2.parentId == 'syn1000002'

    assert_raises(SynapseMalformedEntityError, File, 'http://xkcd.com/1343/', name='XKCD: Manuals', synapseStore=False)


def test_entity_constructors():
    project = Project('TestProject', id='syn1001', foo='bar')
    assert project.name == 'TestProject'
    assert project['foo'] == 'bar'

    folder = Folder('MyFolder', parent=project, foo='bat', id='syn1002')
    assert folder.name == 'MyFolder'
    assert folder.foo == 'bat'
    assert folder.parentId == 'syn1001'

    a_file = File('/path/to/fabulous_things.zzz', parent=folder, foo='biz', contentType='application/cattywampus')
    #assert a_file.name == 'fabulous_things.zzz'
    assert a_file.concreteType == 'org.sagebionetworks.repo.model.FileEntity'
    assert a_file.path == '/path/to/fabulous_things.zzz'
    assert a_file.foo == 'biz'
    assert a_file.parentId == 'syn1002'
    assert a_file.contentType == 'application/cattywampus'
    assert 'contentType' in a_file._file_handle


def test_property_keys():
    assert 'parentId' in File._property_keys
    assert 'versionNumber' in File._property_keys
    assert 'dataFileHandleId' in File._property_keys


def test_keys():
    f = File('foo.xyz', parent='syn1234', foo='bar')

    iter_keys = []
    for key in f:
        iter_keys.append(key)
    assert 'parentId' in iter_keys
    assert 'name' in iter_keys
    assert 'foo' in iter_keys
    assert 'concreteType' in iter_keys


def test_attrs():
    f = File('foo.xyz', parent='syn1234', foo='bar')
    assert hasattr(f, 'parentId')
    assert hasattr(f, 'foo')
    assert hasattr(f, 'path')


def test_split_entity_namespaces():
    """Test split_entity_namespaces"""

    e = {'concreteType':'org.sagebionetworks.repo.model.Folder',
         'name':'Henry',
         'color':'blue',
         'foo':1234,
         'parentId':'syn1234'}
    (properties,annotations,local_state) = split_entity_namespaces(e)

    assert set(properties.keys()) == set(['concreteType', 'name', 'parentId'])
    assert properties['name'] == 'Henry'
    assert set(annotations.keys()) == set(['color', 'foo'])
    assert annotations['foo'] == 1234
    assert len(local_state) == 0

    e = {'concreteType':'org.sagebionetworks.repo.model.FileEntity',
         'name':'Henry',
         'color':'blue',
         'foo':1234,
         'parentId':'syn1234',
         'dataFileHandleId':54321,
         'cacheDir':'/foo/bar/bat',
         'files':['foo.xyz'],
         'path':'/foo/bar/bat/foo.xyz'}
    (properties,annotations,local_state) = split_entity_namespaces(e)

    assert set(properties.keys()) == set(['concreteType', 'name', 'parentId', 'dataFileHandleId'])
    assert properties['name'] == 'Henry'
    assert properties['dataFileHandleId'] == 54321
    assert set(annotations.keys()) == set(['color', 'foo'])
    assert annotations['foo'] == 1234
    assert set(local_state.keys()) == set(['cacheDir', 'files', 'path'])
    assert local_state['cacheDir'] == '/foo/bar/bat'

    f = Entity.create(properties,annotations,local_state)
    assert f.properties.dataFileHandleId == 54321
    assert f.properties.name == 'Henry'
    assert f.annotations.foo == 1234
    assert f.__dict__['cacheDir'] == '/foo/bar/bat'
    assert f.__dict__['path'] == '/foo/bar/bat/foo.xyz'


def test_concrete_type():
    f1 = File('http://en.wikipedia.org/wiki/File:Nettlebed_cave.jpg', name='Nettlebed Cave', parent='syn1234567', synapseStore=False)
    assert f1.concreteType=='org.sagebionetworks.repo.model.FileEntity'


def test_is_container():
    ## result from a Synapse entity annotation query
    ## Note: prefix may be capitalized or not, depending on the from clause of the query
    result = {'entity.versionNumber': 1,
              'entity.nodeType': 'project',
              'entity.concreteType': ['org.sagebionetworks.repo.model.Project'],
              'entity.createdOn': 1451512703905,
              'entity.id': 'syn5570912',
              'entity.name': 'blah'}
    assert is_container(result)

    result = {'Entity.nodeType': 'project',
              'Entity.id': 'syn5570912',
              'Entity.name': 'blah'}
    assert is_container(result)

    result = {'entity.concreteType': ['org.sagebionetworks.repo.model.Folder'],
              'entity.id': 'syn5570914',
              'entity.name': 'flapdoodle'}
    assert is_container(result)

    result = {'File.concreteType': ['org.sagebionetworks.repo.model.FileEntity'],
              'File.id': 'syn5570914',
              'File.name': 'flapdoodle'}
    assert not is_container(result)

    assert is_container(Folder("Stuff", parentId="syn12345"))
    assert is_container(Project("My Project", parentId="syn12345"))
    assert not is_container(File("asdf.png", parentId="syn12345"))


def test_is_container__getChildren_results():
    file_result = {'versionLabel': '1',
                   'name': 'firstPageResult',
                   'versionNumber': 1,
                   'benefactorId': 987,
                   'type': 'org.sagebionetworks.repo.model.FileEntity',
                   'id': 'syn123'}
    assert not is_container(file_result)
    folder_result = {'versionLabel': '1',
                    'name': 'secondPageResult',
                    'versionNumber': 1,
                    'benefactorId': 654,
                    'type': 'org.sagebionetworks.repo.model.Folder',
                    'id': 'syn456'}
    assert is_container(folder_result)


def test_File_update_file_handle__External_sftp():
    sftp_file_handle = { 'concreteType': 'org.sagebionetworks.repo.model.file.ExternalFileHandle',
                         'externalURL' : "sftp://some.website"}
    f = File(parent="idk")
    assert_true(f.synapseStore)
    f._update_file_handle(sftp_file_handle)
    assert_true(f.synapseStore)


def test_File_update_file_handle__External_non_sftp():
        external_file_handle = {'concreteType': 'org.sagebionetworks.repo.model.file.ExternalFileHandle',
                            'externalURL': "https://some.website"}
        f = File(parent="idk")
        assert_true(f.synapseStore)
        f._update_file_handle(external_file_handle)
        assert_false(f.synapseStore)

