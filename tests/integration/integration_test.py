# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import tempfile
import os
import filecmp
import shutil
import json
import time
import uuid
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

from datetime import datetime
from nose.tools import assert_raises, assert_equals, assert_not_equal, assert_is_none, assert_true, assert_false, \
    assert_not_in
from nose.plugins.skip import SkipTest
from mock import patch

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
    module.syn = integration.syn
    module.project = integration.project


def test_login():
    try:
        config = configparser.ConfigParser()
        config.read(client.CONFIG_FILE)
        username = config.get('authentication', 'username')
        password = config.get('authentication', 'password')
        sessionToken = syn._getSessionToken(username, password)

        syn.logout(forgetMe=True)

        # Simple login with ID + PW
        syn.login(username, password, silent=True)

        api_key = syn.credentials.api_key

        # Login with ID + API key
        syn.login(email=username, apiKey=api_key, silent=True)

        # login with session token
        syn.login(sessionToken=sessionToken)

        # login with config file no username
        syn.login(silent=True)

        # Login with ID only from config file
        syn.login(username, silent=True)

        # Login with ID not matching username
        assert_raises(SynapseNoCredentialsError, syn.login, "fakeusername")

        # login using cache
        # mock to make the config file empty
        with patch.object(syn, "_get_config_authentication", return_value={}):

            # Login with no credentials 
            assert_raises(SynapseNoCredentialsError, syn.login)

            # remember login info in cache
            syn.login(username, password, rememberMe=True, silent=True)

            # login using cached info
            syn.login(username, silent=True)
            syn.login(silent=True)

    except configparser.Error:
        raise SkipTest("To fully test the login method,"
                       " please supply a username and password in the configuration file")

    finally:
        # Login with config file
        syn.login(rememberMe=True, silent=True)


def test_login__bad_credentials():
    # nonexistant username and password
    assert_raises(SynapseAuthenticationError, synapseclient.login, email=str(uuid.uuid4()),
                  password="In the end, it doens't even matter")
    # existing username and bad password
    assert_raises(SynapseAuthenticationError, synapseclient.login, email=syn.username, password=str(uuid.uuid4()))


def testCustomConfigFile():
    if os.path.isfile(client.CONFIG_FILE):
        configPath = './CONFIGFILE'
        shutil.copyfile(client.CONFIG_FILE, configPath)
        schedule_for_cleanup(configPath)

        syn2 = synapseclient.Synapse(configPath=configPath)
        syn2.login()
    else:
        raise SkipTest("To fully test the login method a configuration file is required")


def test_entity_version():
    # Make an Entity and make sure the version is one
    entity = File(parent=project['id'])
    entity['path'] = utils.make_bogus_data_file()
    schedule_for_cleanup(entity['path'])
    entity = syn.createEntity(entity)
    
    syn.setAnnotations(entity, {'fizzbuzz': 111222})
    entity = syn.getEntity(entity)
    assert_equals(entity.versionNumber, 1)

    # Update the Entity and make sure the version is incremented
    entity.foo = 998877
    entity['name'] = 'foobarbat'
    entity['description'] = 'This is a test entity...'
    entity = syn.updateEntity(entity, incrementVersion=True, versionLabel="Prada remix")
    assert_equals(entity.versionNumber, 2)

    # Get the older data and verify the random stuff is still there
    annotations = syn.getAnnotations(entity, version=1)
    assert_equals(annotations['fizzbuzz'][0], 111222)
    returnEntity = syn.getEntity(entity, version=1)
    assert_equals(returnEntity.versionNumber, 1)
    assert_equals(returnEntity['fizzbuzz'][0], 111222)
    assert_not_in('foo', returnEntity)

    # Try the newer Entity
    returnEntity = syn.getEntity(entity)
    assert_equals(returnEntity.versionNumber, 2)
    assert_equals(returnEntity['foo'][0], 998877)
    assert_equals(returnEntity['name'], 'foobarbat')
    assert_equals(returnEntity['description'], 'This is a test entity...')
    assert_equals(returnEntity['versionLabel'], 'Prada remix')

    # Try the older Entity again
    returnEntity = syn.downloadEntity(entity, version=1)
    assert_equals(returnEntity.versionNumber, 1)
    assert_equals(returnEntity['fizzbuzz'][0], 111222)
    assert_not_in('foo', returnEntity)
    
    # Delete version 2 
    syn.delete(entity, version=2)
    returnEntity = syn.getEntity(entity)
    assert_equals(returnEntity.versionNumber, 1)


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
    assert_equals(str(sorted(stored)), str(sorted([res['id'] for res in results])))
    assert_equals(len(results), num)


def test_uploadFile_given_dictionary():
    # Make a Folder Entity the old fashioned way
    folder = {'concreteType': Folder._synapse_entity_type, 
              'parentId': project['id'],
              'name': 'fooDictionary',
              'foo': 334455}
    entity = syn.store(folder)
    
    # Download and verify that it is the same file
    entity = syn.get(entity)
    assert_equals(entity.parentId, project.id)
    assert_equals(entity.foo[0], 334455)

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
    assert_equals(entity.description, rareCase['description'])
    assert_equals(entity.name, 'fooDictionary')
    syn.get(entity['id'])


def test_uploadFileEntity():
    # Create a FileEntity
    # Dictionaries default to FileEntity as a type
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    entity = {'name': 'fooUploadFileEntity', 'description': 'A test file entity', 'parentId': project['id']}
    entity = syn.uploadFile(entity, fname)

    # Download and verify
    entity = syn.downloadEntity(entity)

    assert_equals(entity['files'][0], os.path.basename(fname))
    assert_true(filecmp.cmp(fname, entity['path']))

    # Check if we upload the wrong type of file handle
    fh = syn.restGET('/entity/%s/filehandles' % entity.id)['list'][0]
    assert_equals(fh['concreteType'], 'org.sagebionetworks.repo.model.file.S3FileHandle')

    # Create a different temporary file
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)

    # Update existing FileEntity
    entity = syn.uploadFile(entity, fname)

    # Download and verify that it is the same file
    entity = syn.downloadEntity(entity)
    assert_equals(entity['files'][0], os.path.basename(fname))
    assert_true(filecmp.cmp(fname, entity['path']))


def test_downloadFile():
    # See if the a "wget" works
    filename = utils.download_file("http://dev-versions.synapse.sagebase.org/sage_bionetworks_logo_274x128.png")
    schedule_for_cleanup(filename)
    assert_true(os.path.exists(filename))


def test_version_check():
    # Check current version against dev-synapsePythonClient version file
    version_check(version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    # Should be higher than current version and return true
    assert_true(version_check(current_version="999.999.999",
                              version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient"))

    # Test out of date version
    assert_false(version_check(current_version="0.0.1",
                               version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient"))

    # Test blacklisted version
    assert_raises(SystemExit, version_check, current_version="0.0.0",
                  version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    # Test bad URL
    assert_false(version_check(current_version="999.999.999",
                               version_url="http://dev-versions.synapse.sagebase.org/bad_filename_doesnt_exist"))


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
    activity.used({'name': 'Superhack', 'url': 'https://github.com/joe_coder/Superhack'}, wasExecuted=True)
    activity = syn.setProvenance(data_entity, activity)
    
    # Retrieve and verify the saved Provenance record
    retrieved_activity = syn.getProvenance(data_entity)
    assert_equals(retrieved_activity, activity)

    # Test Activity update
    new_description = 'Generate random numbers like a gangsta'
    retrieved_activity['description'] = new_description
    updated_activity = syn.updateActivity(retrieved_activity)
    assert_equals(updated_activity['name'], retrieved_activity['name'])
    assert_equals(updated_activity['description'], new_description)

    # Test delete
    syn.deleteProvenance(data_entity)
    assert_raises(SynapseHTTPError, syn.getProvenance, data_entity['id'])


def test_annotations():
    # Get the annotations of an Entity
    entity = syn.store(Folder(parent=project['id']))
    anno = syn.getAnnotations(entity)
    assert_true(hasattr(anno, 'id'))
    assert_true(hasattr(anno, 'etag'))
    assert_equals(anno.id, entity.id)
    assert_equals(anno.etag, entity.etag)

    # Set the annotations, with keywords too
    anno['bogosity'] = 'total'
    syn.setAnnotations(entity, anno, wazoo='Frank', label='Barking Pumpkin', shark=16776960)

    # Check the update
    annote = syn.getAnnotations(entity)
    assert_equals(annote['bogosity'], ['total'])
    assert_equals(annote['wazoo'], ['Frank'])
    assert_equals(annote['label'], ['Barking Pumpkin'])
    assert_equals(annote['shark'], [16776960])

    # More annotation setting
    annote['primes'] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    annote['phat_numbers'] = [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    annote['goobers'] = ['chris', 'jen', 'jane']
    annote['present_time'] = datetime.now()
    syn.setAnnotations(entity, annote)
    
    # Check it again
    annotation = syn.getAnnotations(entity)
    assert_equals(annotation['primes'], [2, 3, 5, 7, 11, 13, 17, 19, 23, 29])
    assert_equals(annotation['phat_numbers'], [1234.5678, 8888.3333, 1212.3434, 6677.8899])
    assert_equals(annotation['goobers'], ['chris', 'jen', 'jane'])
    assert_equals(annotation['present_time'][0].strftime('%Y-%m-%d %H:%M:%S'), \
                  annote['present_time'].strftime('%Y-%m-%d %H:%M:%S'))


def test_get_user_profile():
    p1 = syn.getUserProfile()

    # skip this test. See SYNPY-685
    # get by name
    # p2 = syn.getUserProfile(p1.userName)
    # assert_equals(p2.userName, p1.userName)

    # get by user ID
    p2 = syn.getUserProfile(p1.ownerId)
    assert_equals(p2.userName, p1.userName)


def test_teams():
    unique_name = "Team Gnarly Rad " + str(uuid.uuid4())
    team = Team(name=unique_name, description="A gnarly rad team", canPublicJoin=True)
    team = syn.store(team)

    team2 = syn.getTeam(team.id)
    assert_equals(team, team2)

    # Asynchronously populates index, so wait 'til it's there
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

    assert_equals(team, found_teams[0])


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
                           'projectId': project_ext_s3.id}

    syn.restPOST('/projectSettings', body=json.dumps(project_destination))
    schedule_for_cleanup(project_ext_s3)
    return project_ext_s3.id, destination['storageLocationId']


# TODO: this test should be rewritten as unit test
def test_external_s3_upload():
    # setup
    project_id, storage_location_id = _set_up_external_s3_project()

    # create a temporary file for upload
    temp_file_path = utils.make_bogus_data_file()
    expected_md5 = utils.md5_for_file(temp_file_path).hexdigest()
    schedule_for_cleanup(temp_file_path)

    # upload the file
    uploaded_syn_file = syn.store(File(path=temp_file_path, parent=project_id))

    # get file_handle of the uploaded file
    file_handle = syn.restGET('/entity/%s/filehandles' % uploaded_syn_file.id)['list'][0]

    # Verify correct file handle type
    assert_equals(file_handle['concreteType'], 'org.sagebionetworks.repo.model.file.S3FileHandle')

    # Verify storage location id to make sure it's using external S3
    assert_equals(storage_location_id, file_handle['storageLocationId'])

    # Verify md5 of upload
    assert_equals(expected_md5, file_handle['contentMd5'])

    # clear the cache and download the file
    syn.cache.purge(time.time())
    downloaded_syn_file = syn.get(uploaded_syn_file.id)

    # verify the correct file was downloaded
    assert_equals(os.path.basename(downloaded_syn_file['path']), os.path.basename(temp_file_path))
    assert_not_equal(os.path.normpath(temp_file_path), os.path.normpath(downloaded_syn_file['path']))
    assert_true(filecmp.cmp(temp_file_path, downloaded_syn_file['path']))


def test_findEntityIdByNameAndParent():
    project_name = str(uuid.uuid1())
    project_id = syn.store(Project(name=project_name))['id']
    assert_equals(project_id, syn.findEntityId(project_name))


def test_getChildren():
    # setup a hierarchy for folders
    # PROJECT
    # |     \
    # File   Folder
    #           |
    #         File
    project_name = str(uuid.uuid1())
    test_project = syn.store(Project(name=project_name))
    folder = syn.store(Folder(name="firstFolder", parent=test_project))
    syn.store(File(path="~/doesntMatter.txt", name="file inside folders", parent=folder, synapseStore=False))
    project_file = syn.store(File(path="~/doesntMatterAgain.txt", name="file inside project", parent=test_project,
                                  synapseStore=False))
    schedule_for_cleanup(test_project)

    expected_id_set = {project_file.id, folder.id}
    children_id_set = {x['id'] for x in syn.getChildren(test_project.id)}
    assert_equals(expected_id_set, children_id_set)


def test_ExternalObjectStore_roundtrip():
    endpoint = "https://s3.amazonaws.com"
    bucket = "test-client-auth-s3"
    profile_name = syn._get_client_authenticated_s3_profile(endpoint, bucket)

    if profile_name != 'client-auth-s3-test':
        raise SkipTest("This test only works on travis because it requires AWS credentials to a specific S3 bucket")

    proj = syn.store(Project(name=str(uuid.uuid4()) + "ExternalObjStoreProject"))
    schedule_for_cleanup(proj)

    storage_location = syn.createStorageLocationSetting("ExternalObjectStorage", endpointUrl=endpoint, bucket=bucket)
    syn.setStorageLocation(proj, storage_location['storageLocationId'])

    file_path = utils.make_bogus_data_file()

    file_entity = File(file_path, name="TestName", parent=proj)
    file_entity = syn.store(file_entity)

    syn.cache.purge(time.time())
    assert_is_none(syn.cache.get(file_entity['dataFileHandleId']))

    # verify key is in s3
    import boto3
    boto_session = boto3.session.Session(profile_name=profile_name)
    s3 = boto_session.resource('s3', endpoint_url=endpoint)
    try:
        s3_file = s3.Object(file_entity._file_handle.bucket, file_entity._file_handle.fileKey)
        s3_file.load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            raise Exception("The file was not uploaded to S3")

    file_entity_downloaded = syn.get(file_entity['id'])
    file_handle = file_entity_downloaded['_file_handle']

    # verify file_handle metadata
    assert_equals(endpoint, file_handle['endpointUrl'])
    assert_equals(bucket, file_handle['bucket'])
    assert_equals(utils.md5_for_file(file_path).hexdigest(), file_handle['contentMd5'])
    assert_equals(os.stat(file_path).st_size, file_handle['contentSize'])
    assert_equals('text/plain', file_handle['contentType'])
    assert_not_equal(utils.normalize_path(file_path), utils.normalize_path(file_entity_downloaded['path']))
    assert_true(filecmp.cmp(file_path, file_entity_downloaded['path']))

    # clean up
    s3_file.delete()


def testSetStorageLocation__existing_storage_location():
    proj = syn.store(Project(name=str(uuid.uuid4()) + "testSetStorageLocation__existing_storage_location"))
    schedule_for_cleanup(proj)

    endpoint = "https://url.doesnt.matter.com"
    bucket = "fake-bucket-name"
    storage_location = syn.createStorageLocationSetting("ExternalObjectStorage", endpointUrl=endpoint, bucket=bucket)
    storage_setting = syn.setStorageLocation(proj, storage_location['storageLocationId'])
    retrieved_setting = syn.getProjectSetting(proj, 'upload')
    assert_equals(storage_setting, retrieved_setting)

    new_endpoint = "https://some.other.url.com"
    new_bucket = "some_other_bucket"
    new_storage_location = syn.createStorageLocationSetting("ExternalObjectStorage", endpointUrl=new_endpoint,
                                                            bucket=new_bucket)
    new_storage_setting = syn.setStorageLocation(proj, new_storage_location['storageLocationId'])
    new_retrieved_setting = syn.getProjectSetting(proj, 'upload')
    assert_equals(new_storage_setting, new_retrieved_setting)


def testMoveProject():
    proj1 = syn.store(Project(name=str(uuid.uuid4()) + "testMoveProject-child"))
    proj2 = syn.store(Project(name=str(uuid.uuid4()) + "testMoveProject-newParent"))
    assert_raises(SynapseHTTPError, syn.move, proj1, proj2)
    schedule_for_cleanup(proj1)
    schedule_for_cleanup(proj2)
