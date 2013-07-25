## integration tests for the Entity class
############################################################

## to run tests: nosetests -vs tests/integration/integration_test_Entity.py
## to run single test: nosetests -vs tests/integration/integration_test_Entity.py:test_Entity
import uuid
import filecmp
import os
import sys
import requests
import time
from datetime import datetime as Datetime

import synapseclient
import synapseclient.utils as utils
from synapseclient import Activity, Entity, Project, Folder, File, Data

import integration
from integration import create_project, schedule_for_cleanup

from nose.tools import assert_raises


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn


def test_Entity():
    # Test CRUD on Entity objects, Project, Folder, File with createEntity/getEntity/updateEntity
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
    project = create_project()

    data = Data(parent=project)

    data = syn.createEntity(data)

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    data = syn.uploadFile(data, path)

    ## make sure file comes back intact
    data = syn.downloadEntity(data)

    assert filecmp.cmp(path, os.path.join(data['cacheDir'], data['files'][0]))


def test_get_and_store():
    """Test synapse.get and synapse.store in Project, Folder and File"""
    ## create project
    project = Project(name=str(uuid.uuid4()), description='A bogus test project')
    project = syn.store(project)
    schedule_for_cleanup(project)

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
    folder = syn.get(folder)
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

    ## should be version 2
    assert random_data.versionNumber == 2

    ## make sure the updates stuck
    random_data_2 = syn.get(random_data)
    assert random_data_2.path is not None
    assert filecmp.cmp(new_file_path, random_data_2.path)
    assert random_data_2.foo[0] == 1266
    assert random_data_2.versionNumber == 2

    ## make sure we can still get the older version of file
    old_random_data = syn.get(random_data.id, version=1)
    assert filecmp.cmp(old_random_data.path, path)

    
def test_store_redundantly_named_projects():
    p1 = create_project()

    # If we store a project with the same name, and createOrUpdate==True,
    # it should become an update
    p2 = Project(p1.name)
    p2.updatedThing = 'Yep, sho\'nuf it\'s updated!'
    p2 = syn.store(p2, createOrUpdate=True)

    assert p1.id == p2.id
    assert p2.updatedThing == ['Yep, sho\'nuf it\'s updated!']


def test_store_with_create_or_update_flag():
    project = create_project()

    filepath = utils.make_bogus_binary_file()
    bogus1 = File(filepath, name='Bogus Test File', parent=project)

    bogus1 = syn.store(bogus1, createOrUpdate=True)

    # Create a different file with the same name and parent
    new_filepath = utils.make_bogus_binary_file()
    bogus1.path = new_filepath

    # Expected behavior is that a new version of the first File will be created
    bogus2 = syn.store(bogus1, createOrUpdate=True)

    assert bogus2.id == bogus1.id
    assert bogus2.versionNumber == 2
    assert not filecmp.cmp(bogus2.path, filepath)

    bogus2a = syn.get(bogus2.id)
    assert bogus2a.id == bogus1.id
    assert bogus2a.versionNumber == 2
    assert filecmp.cmp(bogus2.path, bogus2a.path)

    # Create yet another file with the same name and parent
    newer_filepath = utils.make_bogus_binary_file()
    bogus3 = File(newer_filepath, name='Bogus Test File', parent=project)

    # Expected behavior is raising an exception with a 409 error
    assert_raises(requests.exceptions.HTTPError, syn.store, bogus3, createOrUpdate=False)


def test_store_with_force_version_flag():
    project = create_project()

    filepath = utils.make_bogus_binary_file()
    bogus1 = File(filepath, name='Bogus Test File', parent=project)

    # Expect to get version 1 back
    bogus1 = syn.store(bogus1, forceVersion=False)
    assert bogus1.versionNumber == 1

    # Re-store the same thing and don't up the version
    bogus2 = syn.store(bogus1, forceVersion=False)
    assert bogus1.versionNumber == 1
    
    # Create a different file with the same name and parent
    new_filepath = utils.make_bogus_binary_file()
    bogus2.path = new_filepath

    # Expected behavior is that a new version of the first File will be created
    bogus2 = syn.store(bogus2, forceVersion=False)
    assert bogus2.id == bogus1.id
    assert bogus2.versionNumber == 2
    assert not filecmp.cmp(bogus2.path, filepath)


def test_get_with_downloadLocation_and_ifcollision():
    project = create_project()

    # Make the file to get then delete it
    filepath = utils.make_bogus_binary_file()
    bogus = File(filepath, name='Bogus Test File', parent=project)
    bogus = syn.store(bogus)
    os.remove(filepath)

    # Compare stuff to this one
    normalBogus = syn.get(bogus)
    print normalBogus.path
    
    # Download to the temp folder, should be the same
    otherBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath))
    print otherBogus.path
    assert otherBogus.id == normalBogus.id
    assert filecmp.cmp(otherBogus.path, normalBogus.path)
    
    # Invalidate the downloaded file's timestamps
    os.utime(otherBogus.path, (0, 0))
    badtimestamps = os.path.getmtime(otherBogus.path)
    
    # Download again, should change the modification time
    overwriteBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="overwrite.local")
    overwriteModTime = os.path.getmtime(overwriteBogus.path)
    assert badtimestamps != overwriteModTime
    
    # Download again, should not change the modification time
    otherBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.local")
    assert overwriteModTime == os.path.getmtime(otherBogus.path)
    
    # Invalidate the timestamps again
    os.utime(otherBogus.path, (0, 0))
    badtimestamps = os.path.getmtime(otherBogus.path)
    
    # Download once more, but rename
    renamedBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.both")
    assert otherBogus.path != renamedBogus.path
    assert filecmp.cmp(otherBogus.path, renamedBogus.path)
    
    os.remove(otherBogus.path)
    os.remove(renamedBogus.path)


def test_store_dictionary():
    project = { 'entityType': 'org.sagebionetworks.repo.model.Project',
                'name':str(uuid.uuid4()),
                'description':'Project from dictionary'}
    project = syn.store(project)
    schedule_for_cleanup(project)

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    entity = {  'entityType': 'org.sagebionetworks.repo.model.Data',
                'name':'foo',
                'parentId':project['id'],
                'foo':334455,
                'path':path }

    data = syn.store(entity)

    data = syn.get(data)

    assert data.parentId == project.id
    assert data.foo[0] == 334455
    assert filecmp.cmp(path, os.path.join(data['cacheDir'], data['files'][0]))

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    entity = {}
    entity.update(data.annotations)
    entity.update(data.properties)
    entity.update(data.local_state())
    entity['path'] = path
    entity['description'] = 'Updating with a plain dictionary should be rare.'

    data = syn.store(entity)
    assert data.description == entity['description']
    assert data.name == 'foo'

    data = syn.get(data['id'])
    assert filecmp.cmp(path, os.path.join(data['cacheDir'], data['files'][0]))


def test_get_store_download_file_equals_false():
    """
    Test for SYNR-474:
    Python client crashes when storing syn.store(syn.get(..., downloadFile=False))
    """
    project = create_project()

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    f = File(path, name='Foobarbat', parent=project)
    f = syn.store(f)

    f1 = syn.get(f.id, downloadFile=False)
    f1.description = 'Snorklewacker'
    f1.shoe_size = 11.5
    f1 = syn.store(f1)

    f2 = syn.get(f.id, downloadFile=False)
    assert f2.description == f1.description
    assert f2.shoe_size == [11.5]


def test_get_and_store_by_name_and_parent_id():
    project = create_project()

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    f = File(path, name='Foobarbat', parent=project)
    f2 = syn.store(f)
    f = syn.get(f)

    assert f.id == f2.id
    assert f.name == f2.name
    assert f.parentId == f2.parentId

    ## new file
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    ## should create a new version of the previous File entity
    f3 = File(path, name='Foobarbat', parent=project, description='banana', junk=1234)
    f3 = syn.store(f3)

    ## should be an update of the existing entity with the same name and parent
    assert f3.id == f.id
    assert f3.description == 'banana'
    assert f3.junk == [1234]
    assert filecmp.cmp(path, f3.path)


def test_update_and_increment_version():
    project = create_project()

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    f = File(path, parent=project)
    f = syn.store(f)

    newversion = {
        'id':f.id,
        'name':f.name,
        'description':'This is a totally new description',
        'entityType':File._synapse_entity_type,
        'dataFileHandleId':f.dataFileHandleId,
        'etag':f.etag,
        'parentId':f.parentId}

    updated_f = syn.store(newversion)

    assert updated_f.id == f.id
    assert updated_f.description == 'This is a totally new description'
    assert updated_f.versionNumber == 2


def test_store_activity():
    """Test storing entities with Activities"""
    project = create_project()

    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    f = File(path, name='Hinkle horn honking holes', parent=project)

    honking = Activity(name='Hinkle horn honking', description='Nettlebed Cave is a limestone cave located on the South Island of New Zealand.')
    honking.used('http://www.flickr.com/photos/bevanbfree/3482259379/')
    honking.used('http://www.flickr.com/photos/bevanbfree/3482185673/')

    ## doesn't set the ID of the activity
    f = syn.store(f, activity=honking)

    honking = syn.getProvenance(f.id)
    ## now, we have an activity ID

    assert honking['name'] == 'Hinkle horn honking'
    assert len(honking['used']) == 2
    assert honking['used'][0]['concreteType'] == 'org.sagebionetworks.repo.model.provenance.UsedURL'
    assert honking['used'][0]['wasExecuted'] == False
    assert honking['used'][0]['url'].startswith('http://www.flickr.com/photos/bevanbfree/3482')
    assert honking['used'][1]['concreteType'] == 'org.sagebionetworks.repo.model.provenance.UsedURL'
    assert honking['used'][1]['wasExecuted'] == False

    ## store another entity with the same activity
    f2 = File('http://en.wikipedia.org/wiki/File:Nettlebed_cave.jpg', name='Nettlebed Cave', parent=project, synapseStore=False)
    f2 = syn.store(f2, activity=honking)

    honking2 = syn.getProvenance(f2)

    assert honking['id'] == honking2['id']


def test_ExternalFileHandle():
    project = create_project()

    ## Tests shouldn't have external dependencies, but this is a pretty picture of Singapore
    singapore_url = 'http://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg'

    singapore = File(singapore_url, parent=project, synapseStore=False)
    singapore = syn.store(singapore)

    fileHandle = syn._getFileHandle(singapore.dataFileHandleId)

    assert fileHandle['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'
    assert fileHandle['externalURL']  == singapore_url

    singapore = syn.get(singapore, downloadFile=True)
    assert singapore.path is not None
    assert singapore.externalURL == singapore_url
    assert os.path.exists(singapore.path)


def test_synapseStore_flag():
    """Test storing entities while setting the synapseStore flag to False"""
    project = create_project()

    ## store a path to a local file (synapseStore=False)
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    f1 = File(path, name='Totally bogus data', parent=project, synapseStore=False)

    f1 = syn.store(f1)

    f1a = syn.get(f1.id, downloadFile=False)

    assert f1a.name == 'Totally bogus data'
    assert f1a.path == path, 'path='+str(f1a.path)+'; expected='+path
    assert f1a.synapseStore == False

    ## make sure the test runs on Windows and other OS's
    if path[0].isalpha() and path[1]==':':
        ## a windows file URL looks like this: file:///c:/foo/bar/bat.txt
        expected_url = 'file:///' + path
    else:
        expected_url = 'file://' + path

    assert f1a.externalURL==expected_url, 'unexpected externalURL: ' + f1a.externalURL

    ## a file path that doesn't exist should still work
    f2 = File('/path/to/local/file1.xyz', parentId=project.id, synapseStore=False)
    f2 = syn.store(f2)
    try:
        syn.get(f2)
        assert False
    except Exception as err:
        assert err.message.startswith("Could not download non-existent file")
    assert f1a.synapseStore == False

    ## Try a URL
    f3 = File('http://dev-versions.synapse.sagebase.org/synapsePythonClient', parent=project, synapseStore=False)
    f3 = syn.store(f3)
    f3a = syn.get(f3)
    assert f1a.synapseStore == False

