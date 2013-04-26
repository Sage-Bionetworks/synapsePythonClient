## integration tests for the Entity class
############################################################

## to run tests: nosetests -vs synapseclient/integration_test_Entity.py
## to run single test: nosetests -vs synapseclient/integration_test_Entity.py:test_Entity

from nose.tools import *
import synapseclient
from entity import Entity, Project, Folder, File
from entity import Data
import utils
import uuid
import tempfile
import filecmp
import os
from datetime import datetime as Datetime


def setup_module(module):
    print '~' * 60
    print 'testing Entity'

    ## if testing endpoints are set in the config file, use them
    ## this was created 'cause nosetests doesn't have a good means of
    ## passing parameters to the tests
    if os.path.exists(synapseclient.client.CONFIG_FILE):
        try:
            import ConfigParser
            config = ConfigParser.ConfigParser()
            config.read(synapseclient.client.CONFIG_FILE)
            if config.has_section('testEndpoints'):
                repoEndpoint=config.get('testEndpoints', 'repo')
                authEndpoint=config.get('testEndpoints', 'auth')
                fileHandleEndpoint=config.get('testEndpoints', 'file')
                print "Testing against endpoint:"
                print "  " + repoEndpoint
                print "  " + authEndpoint
                print "  " + fileHandleEndpoint                    
        except Exception as e:
            print e

    syn = synapseclient.Synapse()
    syn.login()
    module.syn = syn
    module._to_cleanup = []

def teardown_module(module):
    cleanup(module._to_cleanup)


def get_cached_synapse_instance():
    """return a cached synapse instance, so we don't have to keep logging in"""
    return globals()['syn']

def create_project(name=None):
    """return a newly created project that will be cleaned up during teardown"""
    if name is None:
        name = str(uuid.uuid4())
    project = {'entityType':'org.sagebionetworks.repo.model.Project', 'name':name}
    project = syn.createEntity(project)
    schedule_for_cleanup(project)
    return project

def schedule_for_cleanup(item):
    """schedule a file of Synapse Entity to be deleted during teardown"""
    globals()['_to_cleanup'].append(item)

def cleanup(items):
    """cleanup junk created during testing"""
    for item in items:
        if isinstance(item, Entity):
            try:
                syn.deleteEntity(item)
            except Exception as ex:
                print "Error cleaning up entity: " + str(ex)
        elif isinstance(item, basestring) and os.path.exists(item):
            try:
                os.remove(item)
            except Exception as ex:
                print ex
        else:
            sys.stderr.write('Don\'t know how to clean: %s' % str(item))


def test_Entity():
    """test CRUD on Entity objects, Project, Folder, File with createEntity/getEntity/updateEntity"""

    syn = get_cached_synapse_instance()

    project_name = str(uuid.uuid4())
    project = Project(project_name, description='Bogus testing project')
    project = syn.createEntity(project)
    schedule_for_cleanup(project)

    folder = Folder('Test Folder', parent=project, description='A place to put my junk', foo=1000)
    folder = syn.createEntity(folder)

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    a_file = File(path, parent=folder, description='Random data for testing', foo='An arbitrary value', bar=[33,44,55], bday=Datetime(2013,3,15))
    a_file = syn._createFileEntity(a_file)

    ## local object state should be preserved
    assert a_file.path == path

    ## check the project entity
    project = syn.getEntity(project)
    assert project.name == project_name

    ## check the folder entity
    folder = syn.getEntity(folder.id)
    assert folder.name == 'Test Folder'
    assert folder.parentId == project.id
    assert folder.foo[0] == 1000

    ## check the file entity
    a_file = syn.getEntity(a_file)
    assert a_file['foo'][0] == 'An arbitrary value'
    assert a_file['bar'] == [33,44,55]
    assert a_file['bday'][0] == Datetime(2013,3,15)

    ## make sure file comes back intact
    a_file = syn.downloadEntity(a_file)
    assert filecmp.cmp(path, a_file.path)

    #TODO We're forgotten the local file path
    a_file.path = path

    ## update the file entity
    a_file['foo'] = 'Another arbitrary chunk of text data'
    a_file['new_key'] = 'A newly created value'
    a_file = syn.updateEntity(a_file)
    assert a_file['foo'][0] == 'Another arbitrary chunk of text data'
    assert a_file['bar'] == [33,44,55]
    assert a_file['bday'][0] == Datetime(2013,3,15)
    assert a_file.new_key[0] == 'A newly created value'
    assert a_file.path == path

    ## upload a new file
    new_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_path)

    a_file = syn.uploadFile(a_file, new_path)

    ## make sure file comes back intact
    a_file = syn.downloadEntity(a_file)
    assert filecmp.cmp(new_path, a_file.path)


def test_deprecated_entity_types():
    """Test Data Entity object"""
    syn = get_cached_synapse_instance()

    project = create_project()

    data = Data(parent=project)

    data = syn.createEntity(data)

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    data = syn.uploadFile(data, path)

    ## make sure file comes back intact
    data = syn.downloadEntity(data)
    assert filecmp.cmp(path, os.path.join(data.cacheDir, data.files[0]))


def test_get_and_store():
    """Test synapse.get and synapse.store in Project, Folder and File"""
    syn = get_cached_synapse_instance()
 
    ## create project
    project = Project(name=str(uuid.uuid4()), description='A bogus test project')
    project = syn.store(project)

    ## create folder
    folder = Folder('Bad stuff', parent=project, description='The rejects from the other fauxldurr', pi=3)
    folder = syn.store(folder)

    ## get folder
    folder = syn.get(folder.id)
    assert folder.name == 'Bad stuff'
    assert folder.parentId == project.id
    assert folder.description == 'The rejects from the other fauxldurr'
    assert folder.pi[0] == 3

    ## update folder
    folder.pi = 3.14159265359
    folder.description = 'The rejects from the other folder'
    syn.store(folder)

    ## verify that the updates stuck
    folder = syn.get(folder.id)
    assert folder.name == 'Bad stuff'
    assert folder.parentId == project.id
    assert folder.description == 'The rejects from the other folder'
    assert folder.pi[0] == 3.14159265359

    ## upload a File
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    random_data = File(path, parent=folder, description='Random data', foo=9844)
    random_data = syn.store(random_data)

    ## make sure file comes back intact
    random_data_2 = syn.downloadEntity(random_data)
    assert filecmp.cmp(path, random_data_2.path)
    assert random_data.foo[0] == 9844

    ## update with a new File
    new_file_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_file_path)
    random_data.path = new_file_path
    random_data.foo = 1266
    random_data = syn.store(random_data)

    ## make sure the updates stuck
    random_data_2 = syn.get(random_data)
    assert random_data_2.path is not None
    assert filecmp.cmp(new_file_path, random_data_2.path)
    assert random_data.foo[0] == 1266


