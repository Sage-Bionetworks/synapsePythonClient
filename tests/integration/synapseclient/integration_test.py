
import tempfile
import os
import filecmp
import shutil
import time
import uuid
import configparser

from datetime import datetime
from nose.tools import assert_raises, assert_equals, assert_true, assert_false, assert_not_in
from unittest.mock import patch
from synapseclient import client

from synapseclient import *
from synapseclient.core.exceptions import *
from synapseclient.core.version_check import version_check
from tests import integration
from tests.integration import schedule_for_cleanup


def setup(module):
    module.syn = integration.syn
    module.project = integration.project


def test_login():
    try:
        config = configparser.RawConfigParser()
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
        raise ValueError("Please supply a username and password in the configuration file.")

    finally:
        # Login with config file
        syn.login(rememberMe=True, silent=True)


def test_login__bad_credentials():
    # nonexistant username and password
    assert_raises(SynapseAuthenticationError, login, email=str(uuid.uuid4()),
                  password="In the end, it doens't even matter")
    # existing username and bad password
    assert_raises(SynapseAuthenticationError, login, email=syn.username, password=str(uuid.uuid4()))


def testCustomConfigFile():
    if os.path.isfile(client.CONFIG_FILE):
        configPath = './CONFIGFILE'
        shutil.copyfile(client.CONFIG_FILE, configPath)
        schedule_for_cleanup(configPath)

        syn2 = Synapse(configPath=configPath)
        syn2.login()
    else:
        raise ValueError("Please supply a username and password in the configuration file.")


def test_entity_version():
    # Make an Entity and make sure the version is one
    entity = File(parent=project['id'])
    entity['path'] = utils.make_bogus_data_file()
    schedule_for_cleanup(entity['path'])
    entity = syn.store(entity)

    syn.set_annotations(Annotations(entity, entity.etag, {'fizzbuzz': 111222}))
    entity = syn.get(entity)
    assert_equals(entity.versionNumber, 1)

    # Update the Entity and make sure the version is incremented
    entity.foo = 998877
    entity['name'] = 'foobarbat'
    entity['description'] = 'This is a test entity...'
    entity = syn.store(entity, forceVersion=True, versionLabel="Prada remix")
    assert_equals(entity.versionNumber, 2)

    # Get the older data and verify the random stuff is still there
    annotations = syn.get_annotations(entity, version=1)
    assert_equals(annotations['fizzbuzz'][0], 111222)
    returnEntity = syn.get(entity, version=1)
    assert_equals(returnEntity.versionNumber, 1)
    assert_equals(returnEntity['fizzbuzz'][0], 111222)
    assert_not_in('foo', returnEntity)

    # Try the newer Entity
    returnEntity = syn.get(entity)
    assert_equals(returnEntity.versionNumber, 2)
    assert_equals(returnEntity['foo'][0], 998877)
    assert_equals(returnEntity['name'], 'foobarbat')
    assert_equals(returnEntity['description'], 'This is a test entity...')
    assert_equals(returnEntity['versionLabel'], 'Prada remix')

    # Try the older Entity again
    returnEntity = syn.get(entity, version=1)
    assert_equals(returnEntity.versionNumber, 1)
    assert_equals(returnEntity['fizzbuzz'][0], 111222)
    assert_not_in('foo', returnEntity)

    # Delete version 2
    syn.delete(entity, version=2)
    returnEntity = syn.get(entity)
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
    entity = File(name='fooUploadFileEntity', path=fname, parentId=project['id'], description='A test file entity')
    entity = syn.store(entity)

    # Download and verify
    entity = syn.get(entity)

    assert_equals(entity['files'][0], os.path.basename(fname))
    assert_true(filecmp.cmp(fname, entity['path']))

    # Check if we upload the wrong type of file handle
    fh = syn.restGET('/entity/%s/filehandles' % entity.id)['list'][0]
    assert_equals(fh['concreteType'], 'org.sagebionetworks.repo.model.file.S3FileHandle')

    # Create a different temporary file
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)

    # Update existing FileEntity
    entity.path = fname
    entity = syn.store(entity)

    # Download and verify that it is the same file
    entity = syn.get(entity)
    assert_equals(entity['files'][0], os.path.basename(fname))
    assert_true(filecmp.cmp(fname, entity['path']))


def test_download_multithreaded():
    # Create a FileEntity
    # Dictionaries default to FileEntity as a type
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    entity = File(name='testMultiThreadDownload' + str(uuid.uuid4()), path=fname, parentId=project['id'])
    entity = syn.store(entity)

    # Download and verify
    syn.multi_threaded = True
    entity = syn.get(entity)

    assert_equals(entity['files'][0], os.path.basename(fname))
    assert_true(filecmp.cmp(fname, entity['path']))
    syn.multi_threaded = False


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
    anno = syn.get_annotations(entity)
    assert_true(hasattr(anno, 'id'))
    assert_true(hasattr(anno, 'etag'))
    assert_equals(anno.id, entity.id)
    assert_equals(anno.etag, entity.etag)

    # Set the annotations, with keywords too
    anno['bogosity'] = 'total'
    syn.set_annotations(Annotations(entity, entity.etag, anno, wazoo='Frank', label='Barking Pumpkin', shark=16776960))

    # Check the update
    annote = syn.get_annotations(entity)
    assert_equals(annote['bogosity'], ['total'])
    assert_equals(annote['wazoo'], ['Frank'])
    assert_equals(annote['label'], ['Barking Pumpkin'])
    assert_equals(annote['shark'], [16776960])

    # More annotation setting
    annote['primes'] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    annote['phat_numbers'] = [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    annote['goobers'] = ['chris', 'jen', 'jane']
    annote['present_time'] = datetime.now()
    syn.set_annotations(annote)

    # Check it again
    annotation = syn.get_annotations(entity)
    assert_equals(annotation['primes'], [2, 3, 5, 7, 11, 13, 17, 19, 23, 29])
    assert_equals(annotation['phat_numbers'], [1234.5678, 8888.3333, 1212.3434, 6677.8899])
    assert_equals(annotation['goobers'], ['chris', 'jen', 'jane'])
    assert_equals(annotation['present_time'][0].strftime('%Y-%m-%d %H:%M:%S'),
                  annote['present_time'].strftime('%Y-%m-%d %H:%M:%S'))


def test_get_user_profile():
    p1 = syn.getUserProfile()

    # get by name
    p2 = syn.getUserProfile(p1.userName)
    assert_equals(p2.userName, p1.userName)

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


def testSetStorageLocation():
    proj = syn.store(Project(name=str(uuid.uuid4()) + "testSetStorageLocation__existing_storage_location"))
    schedule_for_cleanup(proj)

    endpoint = "https://url.doesnt.matter.com"
    bucket = "fake-bucket-name"
    storage_location = syn.createStorageLocationSetting("ExternalObjectStorage", endpointUrl=endpoint, bucket=bucket)
    storage_setting = syn.setStorageLocation(proj, storage_location['storageLocationId'])
    retrieved_setting = syn.getProjectSetting(proj, 'upload')
    assert_equals(storage_setting, retrieved_setting)


def testMoveProject():
    proj1 = syn.store(Project(name=str(uuid.uuid4()) + "testMoveProject-child"))
    proj2 = syn.store(Project(name=str(uuid.uuid4()) + "testMoveProject-newParent"))
    assert_raises(SynapseHTTPError, syn.move, proj1, proj2)
    schedule_for_cleanup(proj1)
    schedule_for_cleanup(proj2)
