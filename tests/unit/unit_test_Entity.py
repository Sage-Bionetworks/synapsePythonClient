# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str, ascii

import collections
import os, sys
from synapseclient.entity import Entity, Project, Folder, File, DockerRepository, split_entity_namespaces, is_container
from synapseclient.exceptions import *
from nose.tools import assert_raises, assert_true, assert_false, assert_equals, raises

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
            
        assert_equals(e.parentId, 'syn1234')
        assert_equals(e['parentId'], 'syn1234')
        assert_equals(e.properties['parentId'], 'syn1234')
        assert_equals(e.properties.parentId, 'syn1234')

        assert_equals(e.foo, 123)
        assert_equals(e['foo'], 123)
        assert_equals(e.annotations['foo'], 123)
        assert_equals(e.annotations.foo, 123)

        assert_true(hasattr(e, 'parentId'))
        assert_true(hasattr(e, 'foo'))
        assert_false(hasattr(e, 'qwerqwer'))

        # Annotations is a bit funny, because there is a property call
        # 'annotations', which will be masked by a member of the object
        # called 'annotations'. Because annotations are open-ended, we
        # might even have an annotations called 'annotations', which gets
        # really confusing.
        assert_true(isinstance(e.annotations, collections.Mapping))
        assert_true(isinstance(e['annotations'], collections.Mapping))
        assert_equals(e.properties['annotations'], '/repo/v1/entity/syn1234/annotations')
        assert_equals(e.properties.annotations, '/repo/v1/entity/syn1234/annotations')
        assert_equals(e.annotations.annotations, 'How confusing!')
        assert_equals(e.annotations['annotations'], 'How confusing!')
        assert_equals(e.nerds, ['chris','jen','janey'])
        assert_true(all([ k in e for k in ['name', 'description', 'foo', 'nerds', 'annotations', 'md5', 'parentId']]))

        # Test modifying properties
        e.description = 'Working, so far'
        assert_equals(e['description'], 'Working, so far')
        e['description'] = 'Wiz-bang flapdoodle'
        assert_equals(e.description, 'Wiz-bang flapdoodle')

        # Test modifying annotations
        e.foo = 999
        assert_equals(e.annotations['foo'], 999)
        e['foo'] = 12345
        assert_equals(e.annotations.foo, 12345)

        # Test creating a new annotation
        e['bar'] = 888
        assert_equals(e.annotations['bar'], 888)
        e['bat'] = 7788
        assert_equals(e.annotations['bat'], 7788)

        # Test replacing annotations object
        e.annotations = {'splat':'a totally new set of annotations', 'foo':456}
        assert_equals(e.foo, 456)
        assert_equals(e['foo'], 456)
        assert_true(isinstance(e.annotations, collections.Mapping))
        assert_true(isinstance(e['annotations'], collections.Mapping))
        assert_equals(e.annotations.foo, 456)
        assert_equals(e.properties['annotations'], '/repo/v1/entity/syn1234/annotations')
        assert_equals(e.properties.annotations, '/repo/v1/entity/syn1234/annotations')

        ## test unicode properties
        e.train = '時刻表には記載されない　月への列車が来ると聞いて'
        e.band = "Motörhead"
        e.lunch = "すし"


def test_subclassing():
    """Test ability to subclass and add a member variable"""
    
    ## define a subclass of Entity to make sure subclassing and creating
    ## a new member variable works
    class FoobarEntity(Entity):
        def __init__(self, x):
            self.__dict__['x'] = x

    foobar = FoobarEntity(123)
    assert_equals(foobar.x, 123)
    assert_true('x' in foobar.__dict__)
    assert_equals(foobar.__dict__['x'], 123)
    foobar.id = 'syn999'
    assert_equals(foobar.properties['id'], 'syn999')
    foobar.n00b = 'henry'
    assert_equals(foobar.annotations['n00b'], 'henry')


def test_entity_creation():
    props = {
        "id": "syn123456",
        "concreteType": "org.sagebionetworks.repo.model.Folder",
        "parentId": "syn445566",
        "name": "Testing123"
    }
    annos = {'testing':123}
    folder = Entity.create(props, annos)

    assert_equals(folder.concreteType, 'org.sagebionetworks.repo.model.Folder')
    assert_equals(folder.__class__, Folder)
    assert_equals(folder.name, 'Testing123')
    assert_equals(folder.testing, 123)

    ## In case of unknown concreteType, fall back on generic Entity object
    props = {
        "id": "syn123456",
        "concreteType": "org.sagebionetworks.repo.model.DoesntExist",
        "parentId": "syn445566",
        "name": "Whatsits"
    }
    whatsits = Entity.create(props)

    assert_equals(whatsits.concreteType, 'org.sagebionetworks.repo.model.DoesntExist')
    assert_equals(whatsits.__class__, Entity)


def test_parent_id_required():
    xkcd1 = File('http://xkcd.com/1343/', name='XKCD: Manuals', parent='syn1000001', synapseStore=False)
    assert_equals(xkcd1.parentId, 'syn1000001')

    xkcd2 = File('http://xkcd.com/1343/', name='XKCD: Manuals', parentId='syn1000002', synapseStore=False)
    assert_equals(xkcd2.parentId, 'syn1000002')

    assert_raises(SynapseMalformedEntityError, File, 'http://xkcd.com/1343/', name='XKCD: Manuals', synapseStore=False)


def test_entity_constructors():
    project = Project('TestProject', id='syn1001', foo='bar')
    assert_equals(project.name, 'TestProject')
    assert_equals(project['foo'], 'bar')

    folder = Folder('MyFolder', parent=project, foo='bat', id='syn1002')
    assert_equals(folder.name, 'MyFolder')
    assert_equals(folder.foo, 'bat')
    assert_equals(folder.parentId, 'syn1001')

    a_file = File('/path/to/fabulous_things.zzz', parent=folder, foo='biz', contentType='application/cattywampus')
    assert_equals(a_file.concreteType, 'org.sagebionetworks.repo.model.FileEntity')
    assert_equals(a_file.path, '/path/to/fabulous_things.zzz')
    assert_equals(a_file.foo, 'biz')
    assert_equals(a_file.parentId, 'syn1002')
    assert_equals(a_file.contentType, 'application/cattywampus')
    assert_true('contentType' in a_file._file_handle)


def test_property_keys():
    assert_equals('parentId' in File._property_keys)
    assert_equals('versionNumber' in File._property_keys)
    assert_equals('dataFileHandleId' in File._property_keys)


def test_keys():
    f = File('foo.xyz', parent='syn1234', foo='bar')

    iter_keys = []
    for key in f:
        iter_keys.append(key)
    assert_true('parentId' in iter_keys)
    assert_true('name' in iter_keys)
    assert_true('foo' in iter_keys)
    assert_true('concreteType' in iter_keys)


def test_attrs():
    f = File('foo.xyz', parent='syn1234', foo='bar')
    assert_equals(hasattr(f, 'parentId'))
    assert_equals(hasattr(f, 'foo'))
    assert_equals(hasattr(f, 'path'))


def test_split_entity_namespaces():
    """Test split_entity_namespaces"""

    e = {'concreteType':'org.sagebionetworks.repo.model.Folder',
         'name':'Henry',
         'color':'blue',
         'foo':1234,
         'parentId':'syn1234'}
    (properties,annotations,local_state) = split_entity_namespaces(e)

    assert_equals(set(properties.keys()), {'concreteType', 'name', 'parentId'})
    assert_equals(properties['name'], 'Henry')
    assert_equals(set(annotations.keys()), {'color', 'foo'})
    assert_equals(annotations['foo'], 1234)
    assert_equals(len(local_state), 0)

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

    assert_equals(set(properties.keys()), {'concreteType', 'name', 'parentId', 'dataFileHandleId'})
    assert_equals(properties['name'], 'Henry')
    assert_equals(properties['dataFileHandleId'], 54321)
    assert_equals(set(annotations.keys()), {'color', 'foo'})
    assert_equals(annotations['foo'], 1234)
    assert_equals(set(local_state.keys()), {'cacheDir', 'files', 'path'})
    assert_equals(local_state['cacheDir'], '/foo/bar/bat')

    f = Entity.create(properties,annotations,local_state)
    assert_equals(f.properties.dataFileHandleId, 54321)
    assert_equals(f.properties.name, 'Henry')
    assert_equals(f.annotations.foo, 1234)
    assert_equals(f.__dict__['cacheDir'], '/foo/bar/bat')
    assert_equals(f.__dict__['path'], '/foo/bar/bat/foo.xyz')


def test_concrete_type():
    f1 = File('http://en.wikipedia.org/wiki/File:Nettlebed_cave.jpg', name='Nettlebed Cave', parent='syn1234567', synapseStore=False)
    assert_equals(f1.concreteType, 'org.sagebionetworks.repo.model.FileEntity')


def test_is_container():
    ## result from a Synapse entity annotation query
    ## Note: prefix may be capitalized or not, depending on the from clause of the query
    result = {'entity.versionNumber': 1,
              'entity.nodeType': 'project',
              'entity.concreteType': ['org.sagebionetworks.repo.model.Project'],
              'entity.createdOn': 1451512703905,
              'entity.id': 'syn5570912',
              'entity.name': 'blah'}
    assert_true(is_container(result))

    result = {'Entity.nodeType': 'project',
              'Entity.id': 'syn5570912',
              'Entity.name': 'blah'}
    assert_true(is_container(result))

    result = {'entity.concreteType': ['org.sagebionetworks.repo.model.Folder'],
              'entity.id': 'syn5570914',
              'entity.name': 'flapdoodle'}
    assert_true(is_container(result))

    result = {'File.concreteType': ['org.sagebionetworks.repo.model.FileEntity'],
              'File.id': 'syn5570914',
              'File.name': 'flapdoodle'}
    assert_false(is_container(result))

    assert_true(is_container(Folder("Stuff", parentId="syn12345")))
    assert_true(is_container(Project("My Project", parentId="syn12345")))
    assert_false(is_container(File("asdf.png", parentId="syn12345")))


@raises(SynapseMalformedEntityError)
def test_DockerRepository__no_repositoryName():
    DockerRepository(parentId="syn123")


def test_is_container__getChildren_results():
    file_result = {'versionLabel': '1',
                   'name': 'firstPageResult',
                   'versionNumber': 1,
                   'benefactorId': 987,
                   'type': 'org.sagebionetworks.repo.model.FileEntity',
                   'id': 'syn123'}
    assert_false(is_container(file_result))
    folder_result = {'versionLabel': '1',
                    'name': 'secondPageResult',
                    'versionNumber': 1,
                    'benefactorId': 654,
                    'type': 'org.sagebionetworks.repo.model.Folder',
                    'id': 'syn456'}
    assert_true(is_container(folder_result))


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

