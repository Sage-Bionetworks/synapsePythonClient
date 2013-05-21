"""
Integration tests for Synapse Client for Python
"""
## to run tests: nosetests -vs
## to run single test: nosetests -vs tests/integration/integration_test.py:test_foo

from nose.tools import assert_raises
import tempfile
import os
import sys
from datetime import datetime
import filecmp
import shutil
import uuid
import random
import ConfigParser

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.evaluation import Evaluation
from synapseclient.client import Activity
from synapseclient.version_check import version_check
from synapseclient.entity import File
from synapseclient.wiki import Wiki

import integration
from integration import create_project, create_data_entity, schedule_for_cleanup


PROJECT_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Project', u'name': ''}
DATA_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Data', u'parentId': ''}
CODE_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Code', u'parentId': ''}


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn


def test_printEntity():
    syn.printEntity({'hello':'world', 'alist':[1,2,3,4]}) 
    #Nothing really to test


def test_login():
    #Test that we fail gracefully with wrong user
    assert_raises(Exception, syn.login, 'asdf', 'notarealpassword')

    #Test that it work with assumed existing config file
    syn.login()

    #Test that it works with username and password
    config = ConfigParser.ConfigParser()
    config.read(client.CONFIG_FILE)
    syn.login(config.get('authentication', 'username'), config.get('authentication', 'password'))

    #Test that it works with session token (created in previous passed step)
    old_config_file = client.CONFIG_FILE

    try:
        fname = utils.make_bogus_data_file()
        client.CONFIG_FILE = fname

        #Test that we fail with the wrong config file and no session token
        os.remove(os.path.join(client.CACHE_DIR, '.session'))
        assert_raises(Exception, syn.login)
    finally:
        client.CONFIG_FILE = old_config_file
        os.remove(fname)


def test_getEntity():
    #Create a new project
    entity = create_project()

    #Get new entity and check that it is same
    returnEntity = syn.getEntity(entity)
    assert entity.properties == returnEntity.properties

    #Get entity by id
    returnEntity = syn.getEntity(entity['id'])
    assert entity.properties == returnEntity.properties


def test_entity_version():
    """Test the ability to get specific versions of Synapse Entities"""
    #Create a new project
    project = create_project()

    entity = create_data_entity(project['id'])
    syn.setAnnotations(entity, {'fizzbuzz':111222})

    #Get new entity and check that it is same
    entity = syn.getEntity(entity)
    assert entity.versionNumber == 1

    entity.description = 'Changed something'
    entity.foo = 998877
    entity = syn.updateEntity(entity, incrementVersion=True)
    assert entity.versionNumber == 2

    annotations = syn.getAnnotations(entity, version=1)
    assert annotations['fizzbuzz'][0] == 111222

    returnEntity = syn.getEntity(entity, version=1)
    assert returnEntity.versionNumber == 1
    assert returnEntity['fizzbuzz'][0] == 111222
    assert 'foo' not in returnEntity

    returnEntity = syn.getEntity(entity)
    assert returnEntity.versionNumber == 2
    assert returnEntity['description'] == 'Changed something'
    assert returnEntity['foo'][0] == 998877

    returnEntity = syn.get(entity, version=1)
    assert returnEntity.versionNumber == 1
    assert returnEntity['fizzbuzz'][0] == 111222
    assert 'foo' not in returnEntity



def test_loadEntity():
    #loadEntity does the same thing as downloadEntity so nothing new to test
    pass


def test_createEntity():
    #Create a project
    project = create_project()

    #Add a data entity to project
    entity = DATA_JSON.copy()
    entity['parentId']= project['id']
    entity['name'] = 'foo'
    entity['description'] = 'description of an entity'
    entity = syn.createEntity(entity)

    #Get the data entity and assert that it is unchanged
    returnEntity = syn.getEntity(entity['id'])

    assert entity.properties == returnEntity.properties

    syn.deleteEntity(returnEntity['id'])


def test_createEntity_with_provenance():
    #Create a project
    entity = create_project()

    #Add a data entity to project
    data = DATA_JSON.copy()
    data['parentId']= entity['id']

    #Create entity with provenance record
    entity = syn.createEntity(data, used='syn123')

    activity = syn.getProvenance(entity)
    assert activity['used'][0]['reference']['targetId'] == 'syn123'


def test_updateEntity():
    project = create_project()
    entity = create_data_entity(project['id'])
    entity[u'tissueType']= 'yuuupp'

    entity = syn.updateEntity(entity)
    returnEntity = syn.getEntity(entity['id'])
    assert entity == returnEntity


def test_updateEntity_version():
    project = create_project()
    entity = create_data_entity(project['id'])
    entity['name'] = 'foobarbat'
    entity['description'] = 'This is a test entity...'
    entity = syn.updateEntity(entity, incrementVersion=True, versionLabel="Prada remix")
    returnEntity = syn.getEntity(entity)
    #syn.printEntity(returnEntity)
    assert returnEntity['name'] == 'foobarbat'
    assert returnEntity['description'] == 'This is a test entity...'
    assert returnEntity['versionNumber'] == 2
    assert returnEntity['versionLabel'] == 'Prada remix'


def test_putEntity():
    #Does the same thing as updateEntity
    pass


def test_query():
    #Create a project then add entities and verify that I can find them with a query
    project = create_project()
    for i in range(2):
        try:
            entity = create_data_entity(project['id'])
        except Exception as ex:
            print ex
            print ex.response.text
        qry= syn.query("select id, name from entity where entity.parentId=='%s'" % project['id'])
        assert qry['totalNumberOfResults']==(i+1)


def test_deleteEntity():
    project = create_project()
    entity = create_data_entity(project['id'])
    
    #Check that we can delete an entity by dictionary
    syn.deleteEntity(entity)
    assert_raises(Exception, syn.getEntity, entity)

    #Check that we can delete an entity by synapse ID
    entity = create_data_entity(project['id'])
    syn.deleteEntity(entity['id'])
    assert_raises(Exception, syn.getEntity, entity)


def test_createSnapshotSummary():
    pass


def test_download_empty_entity():
    project = create_project()
    entity = create_data_entity(project['id'])
    entity = syn.downloadEntity(entity)


def test_uploadFile():
    project = create_project()

    ## here, entity has been saved to Synapse and entity is an Entity object
    entity = create_data_entity(project['id'])

    #create a temporary file
    fname = utils.make_bogus_data_file()
    syn.uploadFile(entity, fname)

    #Download and verify that it is the same filename
    entity = syn.downloadEntity(entity)
    assert entity['files'][0]==os.path.basename(fname)
    assert filecmp.cmp(fname, os.path.join(entity['cacheDir'],entity['files'][0]))
    os.remove(fname)


def test_uploadFile_given_dictionary():
    project = create_project()

    data = DATA_JSON.copy()
    data['parentId']= project.id

    filename = utils.make_bogus_data_file()
    data = syn.uploadFile(data, filename)

    entity = syn.downloadEntity(data['id'])
    assert entity['files'][0]==os.path.basename(filename)
    assert filecmp.cmp(filename, os.path.join(entity['cacheDir'],entity['files'][0]))
    os.remove(filename)


def test_uploadFileEntity():
    projectEntity = create_project()

    ## entityType will default to FileEntity
    entity = {'name':'foo', 'description':'A test file entity', 'parentId':projectEntity['id']}

    #create a temporary file
    fname = utils.make_bogus_data_file()

    ## create new FileEntity
    entity = syn.uploadFile(entity, fname)

    #Download and verify
    entity = syn.downloadEntity(entity)
    assert entity['files'][0]==os.path.basename(fname)
    assert filecmp.cmp(fname, entity['path'])

    ## check if we upload the wrong type of file handle
    fh = syn.restGET('/entity/%s/filehandles' % entity.id)['list'][0]
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'
    os.remove(fname)

    #create a different temporary file
    fname = utils.make_bogus_data_file()

    ## update existing FileEntity
    entity = syn.uploadFile(entity, fname)

    #Download and verify that it is the same filename
    entity = syn.downloadEntity(entity)
    assert entity['files'][0]==os.path.basename(fname)
    assert filecmp.cmp(fname, entity['path'])
    os.remove(fname)


def test_downloadFile():
    "test download file function in utils.py"
    result = utils.download_file("http://dev-versions.synapse.sagebase.org/sage_bionetworks_logo_274x128.png")
    if (result):
        # print("status: \"%s\"" % str(result[1].status))
        # print("filename: \"%s\"" % str(result[0]))
        filename = result[0]
        assert os.path.exists(filename)

        ## cleanup
        try:
            os.remove(filename)
        except Exception:
            print("warning: couldn't delete file: \"%s\"\n" % filename)
    else:
        print("failed to download file: \"%s\"" % filename)
        assert False


def test_version_check():
    """
    test the version checking and blacklisting functionality
    """

    ## current version against dev synapsePythonClient version file
    version_check(version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    ## should be higher than current version and return true
    assert version_check(current_version="999.999.999", version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    ## test out of date version
    assert not version_check(current_version="0.0.1", version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    ## test blacklisted version
    assert_raises(SystemExit, version_check, current_version="0.0.0", version_url="http://dev-versions.synapse.sagebase.org/synapsePythonClient")

    ## test bad url
    assert not version_check(current_version="999.999.999", version_url="http://dev-versions.synapse.sagebase.org/bad_filename_doesnt_exist")


def test_provenance():
    """
    test provenance features
    """

    ## create a new project
    project = create_project()

    ## create a data entity
    try:
        filename = utils.make_bogus_data_file()

        data_entity = create_data_entity(project['id'])

        data_entity = syn.uploadFile(data_entity, filename)
    finally:
        os.remove(filename)

    ## create a code entity, source of the data above
    code = """
    ## Chris's fabulous random data generator
    ############################################################
    import random
    random.seed(12345)
    data = [random.gauss(mu=0.0, sigma=1.0) for i in range(100)]
    """
    try:
        try:
            f = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
            f.write(code)
            f.write("\n")
        finally:
            f.close()
        CODE_JSON['parentId']= project['id']
        code_entity = syn.createEntity(CODE_JSON)
        code_entity = syn.uploadFile(code_entity, f.name)
    finally:
        os.remove(f.name)

    ## create a new activity asserting that the code entity was used to
    ## make the data entity
    activity = Activity(name='random.gauss', description='Generate some random numbers')
    activity.used(code_entity, wasExecuted=True)
    activity.used({'name':'Superhack', 'url':'https://github.com/joe_coder/Superhack'}, wasExecuted=True)

    activity = syn.setProvenance(data_entity, activity)
    ## retrieve the saved provenance record
    retrieved_activity = syn.getProvenance(data_entity)

    ## does it match what we expect?
    assert retrieved_activity == activity

    ## test update
    new_description = 'Generate random numbers like a gangsta'
    retrieved_activity['description'] = new_description
    updated_activity = syn.updateActivity(retrieved_activity)

    ## did the update stick?
    assert updated_activity['name'] == retrieved_activity['name']
    assert updated_activity['description'] == new_description

    ## test delete
    syn.deleteProvenance(data_entity)

    try:
        ## provenance should be gone now
        ## decided this should throw exception with a 404
        deleted_activity = syn.getProvenance(data_entity['id'])
    except Exception as ex:
        assert ex.response.status_code == 404
    else:
        assert False, 'Should throw 404 exception'


def test_annotations():
    ## create a new project
    project = create_project()

    entity = create_data_entity(project['id'])

    a = syn.getAnnotations(entity)
    assert 'etag' in a

    print a

    a['bogosity'] = 'total'
    print a
    syn.setAnnotations(entity, a)

    a2 = syn.getAnnotations(entity)
    assert a2['bogosity'] == ['total']

    a2['primes'] = [2,3,5,7,11,13,17,19,23,29]
    a2['phat_numbers'] = [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    a2['goobers'] = ['chris', 'jen', 'jane']
    a2['present_time'] = datetime.now()

    syn.setAnnotations(entity, a2)
    a3 = syn.getAnnotations(entity)
    assert a3['primes'] == [2,3,5,7,11,13,17,19,23,29]
    assert a3['phat_numbers'] == [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    assert a3['goobers'] == ['chris', 'jen', 'jane']
    ## only accurate to within a second 'cause synapse strips off the fractional part
    assert a3['present_time'][0].strftime('%Y-%m-%d %H:%M:%S') == a2['present_time'].strftime('%Y-%m-%d %H:%M:%S')

def test_keyword_annotations():
    """
    Test setting annotations using keyword arguments
    """
    ## create a new project and data entity
    project = create_project()
    entity = create_data_entity(project['id'])

    annos = syn.setAnnotations(entity, wazoo='Frank', label='Barking Pumpkin', shark=16776960)
    assert annos['wazoo'] == ['Frank']
    assert annos['label'] == ['Barking Pumpkin']
    assert annos['shark'] == [16776960]


def test_ACL():
    ## get the user's principalId, which is called ownerId and is
    ## returned as a string, while in the ACL, it's an integer
    current_user_id = int(syn.getUserProfile()['ownerId'])

    ## other user: using chris's principalId, should be a test user
    other_user_id = 1421212

    ## verify the validity of the other user
    try:
        profile = syn.getUserProfile(other_user_id)
    except Exception as ex:
        if hasattr(ex, 'response') and ex.response.status_code == 404:
            raise Exception('Test invalid, test user doesn\'t exist.', ex)
        raise

    ## create a new project
    project = create_project()

    acl = syn._getACL(project)
    #syn.printEntity(acl)
    assert('resourceAccess' in acl)
    assert current_user_id in [access['principalId'] for access in acl['resourceAccess']]

    acl['resourceAccess'].append({u'accessType': [u'READ', u'CREATE', u'UPDATE'], u'principalId': other_user_id})

    acl = syn._storeACL(project, acl)

    acl = syn._getACL(project)
    #syn.printEntity(acl)
    
    permissions = [access for access in acl['resourceAccess'] if access['principalId'] == current_user_id]
    assert len(permissions) == 1
    assert u'DELETE' in permissions[0]['accessType']
    assert u'CHANGE_PERMISSIONS' in permissions[0]['accessType']
    assert u'READ' in permissions[0]['accessType']
    assert u'CREATE' in permissions[0]['accessType']
    assert u'UPDATE' in permissions[0]['accessType']

    permissions = [access for access in acl['resourceAccess'] if access['principalId'] == other_user_id]
    assert len(permissions) == 1
    assert u'READ' in permissions[0]['accessType']
    assert u'CREATE' in permissions[0]['accessType']
    assert u'UPDATE' in permissions[0]['accessType']


def test_fileHandle():
    ## file the setup.py file to upload
    path = os.path.join(os.path.dirname(client.__file__), '..', 'setup.py')

    ## upload a file to the file handle service
    fileHandle = syn._uploadFileToFileHandleService(path)

    fileHandle2 = syn._getFileHandle(fileHandle)

    # print fileHandle
    # print fileHandle2
    assert fileHandle==fileHandle2

    syn._deleteFileHandle(fileHandle)


def test_fileEntity_round_trip():
    ## create a new project
    project = create_project()

    ## file the setup.py file to upload
    original_path = os.path.join(os.path.dirname(client.__file__), '..', 'setup.py')

    entity = {'name':'Foobar', 'description':'A test file entity...', 'parentId':project['id']}
    entity = syn._createFileEntity(entity, original_path)

    entity_downloaded = syn.downloadEntity(entity['id'])

    path = os.path.join(entity_downloaded['cacheDir'], entity_downloaded['files'][0])

    assert os.path.exists(path)
    assert filecmp.cmp(original_path, path)
    shutil.rmtree(entity_downloaded['cacheDir'])


def test_wikiAttachment():
    md = """
    This is a test wiki
    =======================

    Blabber jabber blah blah boo.
    """

    ## create a new project
    project = create_project()

    ## file the setup.py file to upload
    original_path = os.path.join(os.path.dirname(client.__file__), '..', 'setup.py')

    ## upload a file to the file handle service
    fileHandle = syn._uploadFileToFileHandleService(original_path)

    #Create and store the wiki 
    wiki = Wiki(owner=project, title='A Test Wiki', markdown=md, 
                attachmentFileHandleIds=[fileHandle['id']])
    wiki=syn.store(wiki)
    
    #Create a wiki subpage
    subwiki = Wiki(owner=project, title='A sub-wiki', 
                   markdown='nothing', parentWikiId=wiki.id)
    subwiki=syn.store(subwiki)
    
    ## retrieve the root wiki from Synapse
    wiki2 = syn.getWiki(project)
    assert wiki==wiki2

    ## retrieve the sub wiki from Synapse
    wiki2 = syn.getWiki(project, subpageId=subwiki.id)
    assert subwiki==wiki2

    ## try making an update
    wiki['title'] = 'A New Title'
    wiki['markdown'] = wiki['markdown'] + "\nNew stuff here!!!\n"
    wiki = syn.store(wiki)

    assert wiki['title'] == 'A New Title'
    assert wiki['markdown'].endswith("\nNew stuff here!!!\n")

    headers = syn.getWikiHeaders(project)
    assert headers['totalNumberOfResults']==2
    assert headers['results'][0]['title'] in (wiki['title'], subwiki['title'])

    ## retrieve the file we just uploaded
    #tmpdir = tempfile.mkdtemp()
    # file_props = syn._downloadWikiAttachment(project, wiki, 
    #            os.path.basename(original_path), dest_dir=tmpdir)
    # ## we get back a dictionary with path, files and cacheDir
    # path = file_props['path']
    # ## check and delete it
    # assert os.path.exists(path)
    # assert filecmp.cmp(original_path, path)
    # shutil.rmtree(tmpdir)


    ## cleanup
    syn._deleteFileHandle(fileHandle)
    syn.delete(wiki)
    syn.delete(subwiki)

    ## test that delete worked
    try:
        deleted_wiki = syn.getWiki(project)
    except Exception as ex:
        assert ex.response.status_code == 404
    else:
        assert False, 'Should raise 404 exception'


def test_get_and_store_other_objects():
    #Store something new
    #ev= syn.store(evaluation, createOrUpdate=True) 

    #Update it OK
    #ev['status']='OPEN'
    #ev= syn.store(ev, createOrUpdate=True)

    #Update it with createOrUpdate==False  ERROR
    #ev = Evaluation(name='foobar2', description='bar', status='OPEN')
    #print syn.store(ev, createOrUpdate=False)  #Check that we fail with a HTTPError Exception

    #Update evaluation without supplying all parameters - OK
    #ev= syn.store(Evaluation(description='Testing update'), createOrUpdate=True)
    pass

def test_evaluations():
    ## create a new project
    project = create_project()
    name = 'Test Evaluation %s' % (str(uuid.uuid4()),)
    try:
    #Create evaluation
        ev = Evaluation(name=name, description='Evaluation for testing', 
                        contentSource=project['id'], status='CLOSED')
        ev = syn.store(ev)
        #Update evaluation
        ev['status']='OPEN'
        ev= syn.store(ev, createOrUpdate=True)
        assert ev.status == 'OPEN'

        ## add the current user as a participant
        user = syn.getUserProfile()
        syn.addEvaluationParticipant(ev, user['ownerId'])

        ## increase this to fully test paging by getEvaluationSubmissions
        num_of_submissions = 3
        ## create a bunch of entities and submit them for evaluation
        sys.stdout.write('\ncreating evaluation submissions')
        for i in range(num_of_submissions):
            try:
                (fd, filename) = tempfile.mkstemp()
                with os.fdopen(fd, 'w') as f:
                    f.write(str(random.gauss(0,1)))
                    f.write('\n')
                f=File(filename, parentId=project.id, name='entry-%02d'%i,
                       description ='An entry for testing evaluation')
                entity=syn.store(f)
                syn.submit(ev, entity)
            finally:
                os.remove(filename)
            sys.stdout.write('.')
            sys.stdout.flush()

        ## score the submissions
        submissions = syn.getSubmissions(ev)
        sys.stdout.write('\nscoring submissions')
        for submission in submissions:
            status=syn.getSubmissionStatus(submission)
            status.score = random.random()
            status.status = 'SCORED'
            status.report = 'a fabulous effort!'
            syn.store(status)
            sys.stdout.write('.')
            sys.stdout.flush()
        sys.stdout.write('\n')

    finally:
        syn.delete(ev)

    ## make sure it's deleted
    try:
        ev = syn.getEvaluation(ev)
    except Exception as e:
        print e
        assert e.response.status_code == 404



