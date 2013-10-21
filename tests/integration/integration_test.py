import tempfile, os, sys, filecmp, shutil, requests, json
import uuid, random, base64
import ConfigParser
from datetime import datetime
from nose.tools import assert_raises
from mock import MagicMock, patch

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient.evaluation import Evaluation
from synapseclient.activity import Activity
from synapseclient.version_check import version_check
from synapseclient.entity import Project, File, Data, Code
from synapseclient.wiki import Wiki

import integration
from integration import schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.project = integration.project


def test_login():
    # Test that we fail gracefully with wrong user
    assert_raises(SynapseAuthenticationError, syn.login, 'asdf', 'notarealpassword')

    try:
        config = ConfigParser.ConfigParser()
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
        with patch("ConfigParser.ConfigParser.has_option") as config_has_mock:
            config_has_mock.return_value = False
            
            # Login with given bad session token, 
            # It should REST PUT the token and fail
            # Then keep going and, due to mocking, fail to read any credentials
            assert_raises(SynapseAuthenticationError, syn.login, sessionToken="Wheeeeeeee")
            assert config_has_mock.called
            
            # Login with no credentials 
            assert_raises(SynapseAuthenticationError, syn.login)
            
            config_has_mock.reset_mock()
            config_has_mock.side_effect = lambda section, option: section == "authentication" and option == "sessiontoken"
            with patch("ConfigParser.ConfigParser.get") as config_get_mock:
                # Login with a session token from the config file
                config_get_mock.return_value = sessionToken
                syn.login(silent=True)
                
                # Login with a bad session token from the config file
                config_get_mock.return_value = "derp-dee-derp"
                assert_raises(SynapseAuthenticationError, syn.login)
        
        # Login with session token
        syn.login(sessionToken=sessionToken, rememberMe=True, silent=True)
        
        # Login as the most recent user
        with patch('synapseclient.Synapse._readSessionCache') as read_session_mock:
            dict_mock = MagicMock()
            read_session_mock.return_value = dict_mock
            dict_mock.__contains__.side_effect = lambda x: x == '<mostRecent>'
            dict_mock.__getitem__.return_value = syn.username
            syn.login(silent=True)
            dict_mock.__getitem__.assert_called_once_with('<mostRecent>')
        
        # Login with ID only
        syn.login(username, silent=True)
        syn.logout(forgetMe=True)
    except ConfigParser.Error:
        print "To fully test the login method, please supply a username and password in the configuration file"

    # Login with config file
    syn.login(rememberMe=True, silent=True)


def test_entity_version():
    # Make an Entity and make sure the version is one
    entity = Data(parent=project['id'])
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


def test_createEntity_with_provenance():
    # Create an Entity with a Provenance record
    entity = syn.createEntity(Data(parent=project['id']), used="syn123")

    # Verify the Provenance
    activity = syn.getProvenance(entity)
    assert activity['used'][0]['reference']['targetId'] == 'syn123'

    # test getting a data entity with no locations
    d1 = syn.get(entity['id'])
    assert d1.name==entity['name']
    

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
    # Make a Data Entity the old fashioned way
    data = {'concreteType': Data._synapse_entity_type, 
            'parentId'  : project['id'], 
            'name'      : 'fooDictionary',
            'foo'       : 334455}
    entity = syn.createEntity(data)
    
    # Create and upload a temporary file
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    syn.uploadFile(entity, fname)

    # Download and verify that it is the same file
    entity = syn.downloadEntity(entity)
    assert entity['files'][0] == os.path.basename(fname)
    assert filecmp.cmp(fname, os.path.join(entity['cacheDir'],entity['files'][0]))
    assert entity.parentId == project.id
    assert entity.foo[0] == 334455

    # Update via a dictionary
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    rareCase = {}
    rareCase.update(entity.annotations)
    rareCase.update(entity.properties)
    rareCase.update(entity.local_state())
    rareCase['path'] = path
    rareCase['description'] = 'Updating with a plain dictionary should be rare.'

    # Verify it works
    entity = syn.store(rareCase)
    assert entity.description == rareCase['description']
    assert entity.name == 'fooDictionary'
    entity = syn.get(entity['id'])
    assert filecmp.cmp(path, os.path.join(entity['cacheDir'], entity['files'][0]))
    

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
    assert entity['files'][0] == os.path.basename(fname)
    assert filecmp.cmp(fname, entity['path'])


def test_downloadFile():
    # See if the a "wget" works
    result = utils.download_file("http://dev-versions.synapse.sagebase.org/sage_bionetworks_logo_274x128.png")
    filename = result[0]
    
    # print "status: %s" % str(result[1].status))
    # print "filename: %s" % filename
    schedule_for_cleanup(filename)
    assert result, "Failed to download file: %s" % filename
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
    # Create a Data Entity
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    data_entity = syn.createEntity(Data(parent=project['id']))
    data_entity = syn.uploadFile(data_entity, fname)

    # Create a Code Entity
    fd, path = tempfile.mkstemp(suffix=".py")
    os.write(fd, """
                 ## Chris's fabulous random data generator
                 ############################################################
                 import random
                 random.seed(12345)
                 data = [random.gauss(mu=0.0, sigma=1.0) for i in range(100)]
                 """)
    os.close(fd)
    schedule_for_cleanup(path)
    code_entity = syn.createEntity(Code(parent=project['id']))
    code_entity = syn.uploadFile(code_entity, path)
    
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
    entity = syn.createEntity(Data(parent=project['id']))
    entity = syn.uploadFile(entity)
    anno = syn.getAnnotations(entity)
    assert 'etag' in anno

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
    syn.setAnnotations(entity, annote, )
    
    # Check it again
    annotation = syn.getAnnotations(entity)
    assert annotation['primes'] == [2,3,5,7,11,13,17,19,23,29]
    assert annotation['phat_numbers'] == [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    assert annotation['goobers'] == ['chris', 'jen', 'jane']
    assert annotation['present_time'][0].strftime('%Y-%m-%d %H:%M:%S') == annote['present_time'].strftime('%Y-%m-%d %H:%M:%S')


def test_wikiAttachment():
    # Upload a file to be attached to a Wiki
    filename = utils.make_bogus_data_file()
    attachname = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    schedule_for_cleanup(attachname)
    fileHandle = syn._uploadFileToFileHandleService(filename)

    # Create and store a Wiki 
    # The constructor should accept both file handles and file paths
    md = """
    This is a test wiki
    =======================

    Blabber jabber blah blah boo.
    """
    wiki = Wiki(owner=project, title='A Test Wiki', markdown=md, 
                fileHandles=[fileHandle['id']], 
                attachments=[attachname])
    wiki = syn.store(wiki)
    
    # Create a Wiki sub-page
    subwiki = Wiki(owner=project, title='A sub-wiki', 
                   markdown='nothing', parentWikiId=wiki.id)
    subwiki = syn.store(subwiki)
    
    # Retrieve the root Wiki from Synapse
    wiki2 = syn.getWiki(project)
    assert wiki == wiki2

    # Retrieve the sub Wiki from Synapse
    wiki2 = syn.getWiki(project, subpageId=subwiki.id)
    assert subwiki == wiki2

    # Try making an update
    wiki['title'] = 'A New Title'
    wiki['markdown'] = wiki['markdown'] + "\nNew stuff here!!!\n"
    wiki = syn.store(wiki)
    assert wiki['title'] == 'A New Title'
    assert wiki['markdown'].endswith("\nNew stuff here!!!\n")

    # Check the Wiki's metadata
    headers = syn.getWikiHeaders(project)
    assert headers['totalNumberOfResults'] == 2
    assert headers['results'][0]['title'] in (wiki['title'], subwiki['title'])

    # # Retrieve the file attachment
    # tmpdir = tempfile.mkdtemp()
    # file_props = syn._downloadWikiAttachment(project, wiki, 
    #                         os.path.basename(filename), dest_dir=tmpdir)
    # path = file_props['path']
    # assert os.path.exists(path)
    # assert filecmp.cmp(original_path, path)

    # Clean up
    # syn._deleteFileHandle(fileHandle)
    syn.delete(wiki)
    syn.delete(subwiki)
    assert_raises(SynapseHTTPError, syn.getWiki, project)


def test_evaluations():
    # Create an Evaluation
    name = 'Test Evaluation %s' % str(uuid.uuid4())
    ev = Evaluation(name=name, description='Evaluation for testing', 
                    contentSource=project['id'], status='CLOSED')
    ev = syn.store(ev)
    
    # -- Get the Evaluation by name
    evalNamed = syn.getEvaluationByName(name)
    assert ev['contentSource'] == evalNamed['contentSource']
    assert ev['createdOn'] == evalNamed['createdOn']
    assert ev['description'] == evalNamed['description']
    assert ev['etag'] == evalNamed['etag']
    assert ev['id'] == evalNamed['id']
    assert ev['name'] == evalNamed['name']
    assert ev['ownerId'] == evalNamed['ownerId']
    assert ev['status'] == evalNamed['status']
    
    # -- Get the Evaluation by project
    evalProj = syn.getEvaluationByContentSource(project)
    evalProj = evalProj.next()
    assert ev['contentSource'] == evalProj['contentSource']
    assert ev['createdOn'] == evalProj['createdOn']
    assert ev['description'] == evalProj['description']
    assert ev['etag'] == evalProj['etag']
    assert ev['id'] == evalProj['id']
    assert ev['name'] == evalProj['name']
    assert ev['ownerId'] == evalProj['ownerId']
    assert ev['status'] == evalProj['status']
    
    # Update the Evaluation
    ev['status'] = 'OPEN'
    ev = syn.store(ev, createOrUpdate=True)
    assert ev.status == 'OPEN'

    # Add the current user as a participant
    syn.joinEvaluation(ev)
        
    # Find this user in the participant list
    foundMe = False
    myOwnerId = int(syn.getUserProfile()['ownerId'])
    for item in syn.getParticipants(ev):
        if int(item['userId']) == myOwnerId:
            foundMe = True
            break
    assert foundMe

    # Test getSubmissions with no Submissions (SYNR-453)
    submissions = syn.getSubmissions(ev)
    assert len(list(submissions)) == 0
        
    # -- Get a Submission attachment belonging to another user (SYNR-541) --
    # See if the configuration contains test authentication
    try:
        config = ConfigParser.ConfigParser()
        config.read(client.CONFIG_FILE)
        other_user = {}
        other_user['email'] = config.get('test-authentication', 'username')
        other_user['password'] = config.get('test-authentication', 'password')
        print "Testing SYNR-541"
        
        # Login as the test user
        testSyn = client.Synapse(skip_checks=True)
        testSyn.login(email=other_user['email'], password=other_user['password'])
        testOwnerId = int(testSyn.getUserProfile()['ownerId'])
        
        # Make a project
        other_project = Project(name=str(uuid.uuid4()))
        other_project = testSyn.createEntity(other_project)
        
        # Give the test user permission to read and join the evaluation
        syn._allowParticipation(ev, testOwnerId)
        syn._allowParticipation(ev, "AUTHENTICATED_USERS")
        syn._allowParticipation(ev, "PUBLIC")
        
        # Have the test user join the evaluation
        testSyn.joinEvaluation(ev)
        
        # Find the test user in the participants list
        foundMe = False
        for item in syn.getParticipants(ev):
            if int(item['userId']) == testOwnerId:
                foundMe = True
                break
        assert foundMe
        
        # Make a file to submit
        fd, filename = tempfile.mkstemp()
        os.write(fd, str(random.gauss(0,1)) + '\n')
        os.close(fd)
        f = File(filename, parentId=other_project.id, 
                 name='Different from file name', 
                 description ="Haha!  I'm inaccessible...")
        entity = testSyn.store(f)
        submission = testSyn.submit(ev, entity)
        
        # Clean up, since the current user can't access this project
        # This also removes references to the submitted object :)
        testSyn.delete(other_project)
        
        # Mess up the cached file so that syn._getWithEntityBundle must download again
        os.utime(filename, (0, 0))
        
        # Grab the Submission as the original user
        fetched = syn.getSubmission(submission['id'])
        assert os.path.exists(fetched['filePath'])
        
    except ConfigParser.Error:
        print 'Skipping test for SYNR-541: No [test-authentication] in %s' % client.CONFIG_FILE

    # Create a bunch of Entities and submit them for scoring
    num_of_submissions = 3 # Increase this to fully test paging by getEvaluationSubmissions
    print "Creating Submissions"
    for i in range(num_of_submissions):
        fd, filename = tempfile.mkstemp()
        os.write(fd, str(random.gauss(0,1)) + '\n')
        os.close(fd)
        
        f = File(filename, parentId=project.id, name='entry-%02d' % i,
                 description='An entry for testing evaluation')
        entity=syn.store(f)
        syn.submit(ev, entity, name='Different from file name', teamName='My Team')

    # Score the submissions
    submissions = syn.getSubmissions(ev)
    print "Scoring Submissions"
    for submission in submissions:
        assert submission['name'] == 'Different from file name'
        status = syn.getSubmissionStatus(submission)
        status.score = random.random()
        status.status = 'SCORED'
        status.report = 'a fabulous effort!'
        syn.store(status)

    # Clean up
    syn.delete(ev)
    assert_raises(SynapseHTTPError, syn.getEvaluation, ev)

