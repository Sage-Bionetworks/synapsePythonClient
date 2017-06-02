# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import tempfile, os, sys, filecmp, shutil, json, time
import uuid, base64
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

from datetime import datetime
from nose.tools import assert_raises, assert_equals, assert_not_equal
from nose.plugins.skip import SkipTest
from mock import MagicMock, patch, call

import synapseclient
import synapseclient.client as client
from synapseclient.exceptions import *
from synapseclient.activity import Activity
from synapseclient.version_check import version_check
from synapseclient.entity import Project, File, Folder
from synapseclient.team import Team

import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project

def test_login():
    try:
        # Test that we fail gracefully with wrong user
        assert_raises(SynapseAuthenticationError, syn.login, str(uuid.uuid1()), 'notarealpassword')

        config = configparser.ConfigParser()
        config.read(client.CONFIG_FILE)
        username = config.get('authentication', 'username')
        password = config.get('authentication', 'password')
        sessionToken = syn._getSessionToken(username, password)
        
        # Simple login with ID + PW
        syn.login(username, password, silent=True)
        
        # Login with ID + API key
        syn.login(email=username, apiKey=base64.b64encode(syn.apiKey), silent=True)
        syn.logout(forgetMe=True)
        
        # Config file is read-only for the client, so it must be mocked!
        if (sys.version < '3'):
            configparser_package_name = 'ConfigParser'
        else:
            configparser_package_name = 'configparser'
        with patch("%s.ConfigParser.items" % configparser_package_name) as config_items_mock, patch("synapseclient.Synapse._readSessionCache") as read_session_mock:

            config_items_mock.return_value = []
            read_session_mock.return_value = {}
            
            # Login with given bad session token, 
            # It should REST PUT the token and fail
            # Then keep going and, due to mocking, fail to read any credentials
            assert_raises(SynapseAuthenticationError, syn.login, sessionToken="Wheeeeeeee")
            assert config_items_mock.called
            
            # Login with no credentials 
            assert_raises(SynapseAuthenticationError, syn.login)
            
            config_items_mock.reset_mock()

            # Login with a session token from the config file
            config_items_mock.return_value = [('sessiontoken', sessionToken)]
            syn.login(silent=True)

            # Login with a bad session token from the config file
            config_items_mock.return_value = [('sessiontoken', "derp-dee-derp")]
            assert_raises(SynapseAuthenticationError, syn.login)
        
        # Login with session token
        syn.login(sessionToken=sessionToken, rememberMe=True, silent=True)
        
        # Login as the most recent user
        with patch('synapseclient.Synapse._readSessionCache') as read_session_mock:
            dict_mock = MagicMock()
            read_session_mock.return_value = dict_mock

            #first call is for <mostRecent> next call is the api key of the username in <mostRecent>
            dict_mock.get.side_effect = [syn.username, base64.b64encode(syn.apiKey)]

            syn.login(silent=True)
            dict_mock.assert_has_calls([call.get('<mostRecent>', None),call.get(syn.username, None)])
        
        # Login with ID only
        syn.login(username, silent=True)
        syn.logout(forgetMe=True)
    except configparser.Error:
        print("To fully test the login method, please supply a username and password in the configuration file")

    finally:
        # Login with config file
        syn.login(rememberMe=True, silent=True)


def testCustomConfigFile():
    if os.path.isfile(client.CONFIG_FILE):
        configPath='./CONFIGFILE'
        shutil.copyfile(client.CONFIG_FILE, configPath)
        schedule_for_cleanup(configPath)

        syn2 = synapseclient.Synapse(configPath=configPath)
        syn2.login()
    else:
        print("To fully test the login method a configuration file is required")


def test_entity_version():
    # Make an Entity and make sure the version is one
    entity = File(parent=project['id'])
    entity['path'] = utils.make_bogus_data_file()
    schedule_for_cleanup(entity['path'])
    entity = syn.createEntity(entity)
    
    syn.setAnnotations(entity, {'fizzbuzz':111222})
    entity = syn.getEntity(entity)
    assert entity.versionNumber == 1

    # Update the Entity and make sure the version is incremented
    entity.foo = 998877
    entity['name'] = 'foobarbat'
    entity['description'] = 'This is a test entity...'
    entity = syn.updateEntity(entity, incrementVersion=True, versionLabel="Prada remix")
    assert entity.versionNumber == 2

    # Get the older data and verify the random stuff is still there
    annotations = syn.getAnnotations(entity, version=1)
    assert annotations['fizzbuzz'][0] == 111222
    returnEntity = syn.getEntity(entity, version=1)
    assert returnEntity.versionNumber == 1
    assert returnEntity['fizzbuzz'][0] == 111222
    assert 'foo' not in returnEntity

    # Try the newer Entity
    returnEntity = syn.getEntity(entity)
    assert returnEntity.versionNumber == 2
    assert returnEntity['foo'][0] == 998877
    assert returnEntity['name'] == 'foobarbat'
    assert returnEntity['description'] == 'This is a test entity...'
    assert returnEntity['versionLabel'] == 'Prada remix'

    # Try the older Entity again
    returnEntity = syn.downloadEntity(entity, version=1)
    assert returnEntity.versionNumber == 1
    assert returnEntity['fizzbuzz'][0] == 111222
    assert 'foo' not in returnEntity
    
    # Delete version 2 
    syn.delete(entity, version=2)
    returnEntity = syn.getEntity(entity)
    assert returnEntity.versionNumber == 1

def test_md5_query():
    # Add the same Entity several times
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    repeated = File(path, parent=project['id'], description='Same data over and over again')
    
    # Retrieve the data via MD5
    num = 5
    stored = []
    for i in range(num):
        repeated.name = 'Repeated data %d.dat' % i
        stored.append(syn.store(repeated).id)
    
    # Although we expect num results, it is possible for the MD5 to be non-unique
    results = syn.md5Query(utils.md5_for_file(path).hexdigest())
    assert str(sorted(stored)) == str(sorted([res['id'] for res in results]))
    assert len(results) == num    


def test_uploadFile_given_dictionary():
    # Make a Folder Entity the old fashioned way
    folder = {'concreteType': Folder._synapse_entity_type, 
            'parentId'  : project['id'], 
            'name'      : 'fooDictionary',
            'foo'       : 334455}
    entity = syn.store(folder)
    
    # Download and verify that it is the same file
    entity = syn.get(entity)
    assert entity.parentId == project.id
    assert entity.foo[0] == 334455

    # Update via a dictionary
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    rareCase = {}
    rareCase.update(entity.annotations)
    rareCase.update(entity.properties)
    rareCase.update(entity.local_state())
    rareCase['description'] = 'Updating with a plain dictionary should be rare.'

    # Verify it works
    entity = syn.store(rareCase)
    assert entity.description == rareCase['description']
    assert entity.name == 'fooDictionary'
    entity = syn.get(entity['id'])
    

def test_uploadFileEntity():
    # Create a FileEntity
    # Dictionaries default to FileEntity as a type
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    entity = {'name'        : 'fooUploadFileEntity', \
              'description' : 'A test file entity', \
              'parentId'    : project['id']}
    entity = syn.uploadFile(entity, fname)

    # Download and verify
    entity = syn.downloadEntity(entity)

    print(entity['files'])
    assert entity['files'][0] == os.path.basename(fname)
    assert filecmp.cmp(fname, entity['path'])

    # Check if we upload the wrong type of file handle
    fh = syn.restGET('/entity/%s/filehandles' % entity.id)['list'][0]
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'

    # Create a different temporary file
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)

    # Update existing FileEntity
    entity = syn.uploadFile(entity, fname)

    # Download and verify that it is the same file
    entity = syn.downloadEntity(entity)
    print(entity['files'])
    assert_equals(entity['files'][0], os.path.basename(fname))
    assert filecmp.cmp(fname, entity['path'])


def test_downloadFile():
    # See if the a "wget" works
    filename = utils.download_file("http://dev-versions.synapse.sagebase.org/sage_bionetworks_logo_274x128.png")
    schedule_for_cleanup(filename)
    assert os.path.exists(filename)


def test_version_check():
    # Check current version against dev-synapsePythonClient version file
    version_check(version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    # Should be higher than current version and return true
    assert version_check(current_version="999.999.999", version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    # Test out of date version
    assert not version_check(current_version="0.0.1", version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    # Test blacklisted version
    assert_raises(SystemExit, version_check, current_version="0.0.0", version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    # Test bad URL
    assert not version_check(current_version="999.999.999", version_url="http://dev-versions.synapse.sagebase.org/bad_filename_doesnt_exist")


def test_provenance():
    # Create a File Entity
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    data_entity = syn.store(File(fname, parent=project['id']))

    # Create a File Entity of Code
    fd, path = tempfile.mkstemp(suffix=".py")
    with os.fdopen(fd, 'w') as f:
        f.write(utils.normalize_lines("""
            ## Chris's fabulous random data generator
            ############################################################
            import random
            random.seed(12345)
            data = [random.gauss(mu=0.0, sigma=1.0) for i in range(100)]
            """))
    schedule_for_cleanup(path)
    code_entity = syn.store(File(path, parent=project['id']))
    
    # Create a new Activity asserting that the Code Entity was 'used'
    activity = Activity(name='random.gauss', description='Generate some random numbers')
    activity.used(code_entity, wasExecuted=True)
    activity.used({'name':'Superhack', 'url':'https://github.com/joe_coder/Superhack'}, wasExecuted=True)
    activity = syn.setProvenance(data_entity, activity)
    
    # Retrieve and verify the saved Provenance record
    retrieved_activity = syn.getProvenance(data_entity)
    assert retrieved_activity == activity

    # Test Activity update
    new_description = 'Generate random numbers like a gangsta'
    retrieved_activity['description'] = new_description
    updated_activity = syn.updateActivity(retrieved_activity)
    assert updated_activity['name'] == retrieved_activity['name']
    assert updated_activity['description'] == new_description

    # Test delete
    syn.deleteProvenance(data_entity)
    assert_raises(SynapseHTTPError, syn.getProvenance, data_entity['id'])


def test_annotations():
    # Get the annotations of an Entity
    entity = syn.store(Folder(parent=project['id']))
    anno = syn.getAnnotations(entity)
    assert hasattr(anno, 'id')
    assert hasattr(anno, 'etag')
    assert anno.id == entity.id
    assert anno.etag == entity.etag

    # Set the annotations, with keywords too
    anno['bogosity'] = 'total'
    syn.setAnnotations(entity, anno, wazoo='Frank', label='Barking Pumpkin', shark=16776960)

    # Check the update
    annote = syn.getAnnotations(entity)
    assert annote['bogosity'] == ['total']
    assert annote['wazoo'] == ['Frank']
    assert annote['label'] == ['Barking Pumpkin']
    assert annote['shark'] == [16776960]

    # More annotation setting
    annote['primes'] = [2,3,5,7,11,13,17,19,23,29]
    annote['phat_numbers'] = [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    annote['goobers'] = ['chris', 'jen', 'jane']
    annote['present_time'] = datetime.now()
    syn.setAnnotations(entity, annote)
    
    # Check it again
    annotation = syn.getAnnotations(entity)
    assert annotation['primes'] == [2,3,5,7,11,13,17,19,23,29]
    assert annotation['phat_numbers'] == [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    assert annotation['goobers'] == ['chris', 'jen', 'jane']
    assert annotation['present_time'][0].strftime('%Y-%m-%d %H:%M:%S') == annote['present_time'].strftime('%Y-%m-%d %H:%M:%S')


def test_get_user_profile():
    p1 = syn.getUserProfile()

    ## get by name
    p2 = syn.getUserProfile(p1.userName)
    assert p2.userName == p1.userName

    ## get by user ID
    p2 = syn.getUserProfile(p1.ownerId)
    assert p2.userName == p1.userName


def test_teams():
    unique_name = "Team Gnarly Rad " + str(uuid.uuid4())
    team = Team(name=unique_name, description="A gnarly rad team", canPublicJoin=True)
    team = syn.store(team)

    team2 = syn.getTeam(team.id)
    assert team == team2

    ## Asynchronously populates index, so wait 'til it's there
    retry = 0
    backoff = 0.2
    while retry < 10:
        retry += 1
        time.sleep(backoff)
        backoff *= 2
        found_teams = list(syn._findTeam(team.name))
        if len(found_teams) > 0:
            break
    else:
        print("Failed to create team. May not be a real error.")

    syn.delete(team)

    assert team == found_teams[0]

def _set_up_external_s3_project():
    """
    creates a project and links it to an external s3 storage
    :return: synapse id of the created  project, and storageLocationId of the project
    """
    EXTERNAL_S3_BUCKET = 'python-client-integration-test.sagebase.org'
    project_ext_s3 = syn.store(Project(name=str(uuid.uuid4())))

    destination = {'uploadType': 'S3',
                   'concreteType': 'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting',
                   'bucket': EXTERNAL_S3_BUCKET}
    destination = syn.restPOST('/storageLocation', body=json.dumps(destination))

    project_destination = {'concreteType': 'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
                           'settingsType': 'upload',
                           'locations': [destination['storageLocationId']],
                           'projectId' : project_ext_s3.id}

    project_destination = syn.restPOST('/projectSettings', body=json.dumps(project_destination))
    schedule_for_cleanup(project_ext_s3)
    return project_ext_s3.id, destination['storageLocationId']


def test_external_s3_upload():
    #skip if not on the synapse-test user
    if syn.username != 'synapse-test':
        raise SkipTest("This test is configured to work on synapse's TravisCI. If you wish to run this locally, please create an external S3 bucket that your Synapse username can access (http://docs.synapse.org/articles/custom_storage_location.html) and modify the EXTERNAL_S3_BUCKET variable")

    #setup
    project_id, storage_location_id = _set_up_external_s3_project()

    # create a temporary file for upload
    temp_file_path = utils.make_bogus_data_file()
    expected_md5 = utils.md5_for_file(temp_file_path).hexdigest()
    schedule_for_cleanup(temp_file_path)

    #upload the file
    uploaded_syn_file = syn.store(File(path=temp_file_path, parent=project_id))

    #get file_handle of the uploaded file
    file_handle = syn.restGET('/entity/%s/filehandles' % uploaded_syn_file.id)['list'][0]

    #Verify correct file handle type
    assert_equals(file_handle['concreteType'], 'org.sagebionetworks.repo.model.file.S3FileHandle')

    # Verify storage location id to make sure it's using external S3
    assert_equals(storage_location_id, file_handle['storageLocationId'])

    #Verify md5 of upload
    assert_equals(expected_md5, file_handle['contentMd5'])

    # clear the cache and download the file
    syn.cache.purge(time.time())
    downloaded_syn_file = syn.get(uploaded_syn_file.id)

    #verify the correct file was downloaded
    assert_equals(os.path.basename(downloaded_syn_file['path']), os.path.basename(temp_file_path))
    assert_not_equal(os.path.normpath(temp_file_path), os.path.normpath(downloaded_syn_file['path']))
    assert filecmp.cmp(temp_file_path, downloaded_syn_file['path'])


def test_findEntityIdByNameAndParent():
    project_name = str(uuid.uuid1())
    project_id = syn.store(Project(name=project_name))['id']
    assert_equals(project_id, syn._findEntityIdByNameAndParent(project_name))
