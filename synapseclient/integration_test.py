## integration tests for Python client
## to run tests: nosetests -vs
## to run single test: nosetests -vs synapseclient/integration_test.py:TestClient.test_downloadFile

import client, utils
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

from synapseclient.evaluation import Evaluation
from synapseclient.wiki import Wiki
from synapseclient.client import Activity
from synapseclient.version_check import version_check
from synapseclient.entity import File

def setup_module(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60


PROJECT_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Project', u'name': ''}
DATA_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Data', u'parentId': ''}
CODE_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Code', u'parentId': ''}

## a place to cache the synapse client so we don't have to create it
## for every test
synapse = None


class TestClient:
    """
    Integration tests against a repository service
    """

    def __init__(self, repoEndpoint='https://repo-prod.prod.sagebase.org/repo/v1',
                 authEndpoint='https://auth-prod.prod.sagebase.org/auth/v1',
                 fileHandleEndpoint='https://file-prod.prod.sagebase.org/file/v1'):
        """
        Arguments:
        - `repoEndpoint`:
        """

        if synapse:
            sys.stderr.write(',')
            self.syn = synapse
        else:
            ## if testing endpoints are set in the config file, use them
            ## this was created 'cause nosetests doesn't have a good means of
            ## passing parameters to the tests
            if os.path.exists(client.CONFIG_FILE):
                try:
                    import ConfigParser
                    config = ConfigParser.ConfigParser()
                    config.read(client.CONFIG_FILE)
                    if config.has_section('testEndpoints'):
                        repoEndpoint=config.get('testEndpoints', 'repo')
                        authEndpoint=config.get('testEndpoints', 'auth')
                        fileHandleEndpoint=config.get('testEndpoints', 'file')
                except Exception as e:
                    print e

            self.syn = client.Synapse(repoEndpoint=repoEndpoint, authEndpoint=authEndpoint, fileHandleEndpoint=fileHandleEndpoint, debug=False)
            self.syn._skip_version_check = True
            # self.syn.debug = True

            ## Assumes that a configuration file exists in the home directory with login information
            self.syn.login()
            ## cache the synapse client, so we don't have to keep creating it
            globals()['synapse'] = self.syn
            self.toRemove=[]


    def setUp(self):
        self.toRemove=[]


    def tearDown(self):
        print 'tearDown %i entities' %len(self.toRemove)
        for item in self.toRemove:
            self.syn.deleteEntity(item)


    def createProject(self):
        PROJECT_JSON['name'] = str(uuid.uuid4())
        entity = self.syn.createEntity(PROJECT_JSON)
        self.toRemove.append(entity)
        return entity


    def createDataEntity(self, parentId):
        data = DATA_JSON.copy()
        data['parentId']= parentId
        return self.syn.createEntity(data)


    def test_printEntity(self):
        self.syn.printEntity({'hello':'world', 'alist':[1,2,3,4]}) 
        #Nothing really to test

    
    def test_login(self):
        #Test that we fail gracefully with wrong user
        assert_raises(Exception, self.syn.login, 'asdf', 'notarealpassword')

        #Test that it work with assumed existing config file
        self.syn.login()

        #Test that it works with username and password
        config = ConfigParser.ConfigParser()
        config.read(client.CONFIG_FILE)
        self.syn.login(config.get('authentication', 'username'), config.get('authentication', 'password'))

        #Test that it works with session token (created in previous passed step)
        old_config_file = client.CONFIG_FILE

        try:
            fname = utils.make_bogus_data_file()
            client.CONFIG_FILE = fname

            #Test that we fail with the wrong config file and no session token
            os.remove(os.path.join(client.CACHE_DIR, '.session'))
            assert_raises(Exception, self.syn.login)
        finally:
            client.CONFIG_FILE = old_config_file
            os.remove(fname)


    def test_getEntity(self):
        #Create a new project
        entity = self.createProject()

        #Get new entity and check that it is same
        returnEntity = self.syn.getEntity(entity)
        assert entity.properties == returnEntity.properties

        #Get entity by id
        returnEntity = self.syn.getEntity(entity['id'])
        assert entity.properties == returnEntity.properties


    def test_entity_version(self):
        """Test the ability to get specific versions of Synapse Entities"""
        #Create a new project
        project = self.createProject()

        entity = self.createDataEntity(project['id'])
        self.syn.setAnnotations(entity, {'fizzbuzz':111222})

        #Get new entity and check that it is same
        entity = self.syn.getEntity(entity)
        assert entity.versionNumber == 1

        entity.description = 'Changed something'
        entity.foo = 998877
        entity = self.syn.updateEntity(entity, incrementVersion=True)
        assert entity.versionNumber == 2

        annotations = self.syn.getAnnotations(entity, version=1)
        assert annotations['fizzbuzz'][0] == 111222

        returnEntity = self.syn.getEntity(entity, version=1)
        assert returnEntity.versionNumber == 1
        assert returnEntity['fizzbuzz'][0] == 111222
        assert 'foo' not in returnEntity

        returnEntity = self.syn.getEntity(entity)
        assert returnEntity.versionNumber == 2
        assert returnEntity['description'] == 'Changed something'
        assert returnEntity['foo'][0] == 998877

        returnEntity = self.syn.get(entity, version=1)
        assert returnEntity.versionNumber == 1
        assert returnEntity['fizzbuzz'][0] == 111222
        assert 'foo' not in returnEntity



    def test_loadEntity(self):
        #loadEntity does the same thing as downloadEntity so nothing new to test
        pass


    def test_createEntity(self):
        #Create a project
        project = self.createProject()

        #Add a data entity to project
        entity = DATA_JSON.copy()
        entity['parentId']= project['id']
        entity['name'] = 'foo'
        entity['description'] = 'description of an entity'
        entity = self.syn.createEntity(entity)

        #Get the data entity and assert that it is unchanged
        returnEntity = self.syn.getEntity(entity['id'])

        assert entity.properties == returnEntity.properties

        self.syn.deleteEntity(returnEntity['id'])


    def test_createEntity_with_provenance(self):
        #Create a project
        entity = self.createProject()

        #Add a data entity to project
        data = DATA_JSON.copy()
        data['parentId']= entity['id']

        #Create entity with provenance record
        entity = self.syn.createEntity(data, used='syn123')

        activity = self.syn.getProvenance(entity)
        assert activity['used'][0]['reference']['targetId'] == 'syn123'


    def test_updateEntity(self):
        project = self.createProject()
        entity = self.createDataEntity(project['id'])
        entity[u'tissueType']= 'yuuupp'

        entity = self.syn.updateEntity(entity)
        returnEntity = self.syn.getEntity(entity['id'])
        assert entity == returnEntity


    def test_updateEntity_version(self):
        project = self.createProject()
        entity = self.createDataEntity(project['id'])
        entity['name'] = 'foobarbat'
        entity['description'] = 'This is a test entity...'
        entity = self.syn.updateEntity(entity, incrementVersion=True, versionLabel="Prada remix")
        returnEntity = self.syn.getEntity(entity)
        #self.syn.printEntity(returnEntity)
        assert returnEntity['name'] == 'foobarbat'
        assert returnEntity['description'] == 'This is a test entity...'
        assert returnEntity['versionNumber'] == 2
        assert returnEntity['versionLabel'] == 'Prada remix'


    def test_putEntity(self):
        #Does the same thing as updateEntity
        pass


    def test_query(self):
        #Create a project then add entities and verify that I can find them with a query
        project = self.createProject()
        for i in range(2):
            try:
                entity = self.createDataEntity(project['id'])
            except Exception as ex:
                print ex
                print ex.response.text
            qry= self.syn.query("select id, name from entity where entity.parentId=='%s'" % project['id'])
            assert qry['totalNumberOfResults']==(i+1)

    
    def test_deleteEntity(self):
        project = self.createProject()
        entity = self.createDataEntity(project['id'])
        
        #Check that we can delete an entity by dictionary
        self.syn.deleteEntity(entity)
        assert_raises(Exception, self.syn.getEntity, entity)

        #Check that we can delete an entity by synapse ID
        entity = self.createDataEntity(project['id'])
        self.syn.deleteEntity(entity['id'])
        assert_raises(Exception, self.syn.getEntity, entity)


    def test_createSnapshotSummary(self):
        pass


    def test_download_empty_entity(self):
        project = self.createProject()
        entity = self.createDataEntity(project['id'])
        entity = self.syn.downloadEntity(entity)


    def test_uploadFile(self):
        project = self.createProject()

        ## here, entity has been saved to Synapse and entity is an Entity object
        entity = self.createDataEntity(project['id'])

        #create a temporary file
        fname = utils.make_bogus_data_file()
        self.syn.uploadFile(entity, fname)

        #Download and verify that it is the same filename
        entity = self.syn.downloadEntity(entity)
        assert entity['files'][0]==os.path.basename(fname)
        assert filecmp.cmp(fname, os.path.join(entity['cacheDir'],entity['files'][0]))
        os.remove(fname)


    def test_uploadFile_given_dictionary(self):
        project = self.createProject()

        data = DATA_JSON.copy()
        data['parentId']= project.id

        filename = utils.make_bogus_data_file()
        data = self.syn.uploadFile(data, filename)

        entity = self.syn.downloadEntity(data['id'])
        assert entity['files'][0]==os.path.basename(filename)
        assert filecmp.cmp(filename, os.path.join(entity['cacheDir'],entity['files'][0]))
        os.remove(filename)


    def test_uploadFileEntity(self):
        projectEntity = self.createProject()

        ## entityType will default to FileEntity
        entity = {'name':'foo', 'description':'A test file entity', 'parentId':projectEntity['id']}

        #create a temporary file
        fname = utils.make_bogus_data_file()

        ## create new FileEntity
        entity = self.syn.uploadFile(entity, fname)

        #Download and verify that it is the same filename
        entity = self.syn.downloadEntity(entity)
        assert entity['files'][0]==os.path.basename(fname)
        assert filecmp.cmp(fname, entity['path'])
        os.remove(fname)

        #create a different temporary file
        fname = utils.make_bogus_data_file()

        ## update existing FileEntity
        entity = self.syn.uploadFile(entity, fname)

        #Download and verify that it is the same filename
        entity = self.syn.downloadEntity(entity)
        assert entity['files'][0]==os.path.basename(fname)
        assert filecmp.cmp(fname, entity['path'])
        os.remove(fname)


    def test_downloadFile(self):
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


    def test_version_check(self):
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


    def test_provenance(self):
        """
        test provenance features
        """

        ## create a new project
        project = self.createProject()

        ## create a data entity
        try:
            filename = utils.make_bogus_data_file()

            data_entity = self.createDataEntity(project['id'])

            data_entity = self.syn.uploadFile(data_entity, filename)
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
            code_entity = self.syn.createEntity(CODE_JSON)
            code_entity = self.syn.uploadFile(code_entity, f.name)
        finally:
            os.remove(f.name)

        ## create a new activity asserting that the code entity was used to
        ## make the data entity
        activity = Activity(name='random.gauss', description='Generate some random numbers')
        activity.used(code_entity, wasExecuted=True)
        activity.used({'name':'Superhack', 'url':'https://github.com/joe_coder/Superhack'}, wasExecuted=True)

        activity = self.syn.setProvenance(data_entity, activity)
        ## retrieve the saved provenance record
        retrieved_activity = self.syn.getProvenance(data_entity)

        ## does it match what we expect?
        assert retrieved_activity == activity

        ## test update
        new_description = 'Generate random numbers like a gangsta'
        retrieved_activity['description'] = new_description
        updated_activity = self.syn.updateActivity(retrieved_activity)

        ## did the update stick?
        assert updated_activity['name'] == retrieved_activity['name']
        assert updated_activity['description'] == new_description

        ## test delete
        self.syn.deleteProvenance(data_entity)

        try:
            ## provenance should be gone now
            ## decided this should throw exception with a 404
            deleted_activity = self.syn.getProvenance(data_entity['id'])
        except Exception as ex:
            assert ex.response.status_code == 404
        else:
            assert False, 'Should throw 404 exception'


    def test_annotations(self):
        ## create a new project
        project = self.createProject()

        entity = self.createDataEntity(project['id'])

        a = self.syn.getAnnotations(entity)
        assert 'etag' in a

        print a

        a['bogosity'] = 'total'
        print a
        self.syn.setAnnotations(entity, a)

        a2 = self.syn.getAnnotations(entity)
        assert a2['bogosity'] == ['total']

        a2['primes'] = [2,3,5,7,11,13,17,19,23,29]
        a2['phat_numbers'] = [1234.5678, 8888.3333, 1212.3434, 6677.8899]
        a2['goobers'] = ['chris', 'jen', 'jane']
        a2['present_time'] = datetime.now()

        self.syn.setAnnotations(entity, a2)
        a3 = self.syn.getAnnotations(entity)
        assert a3['primes'] == [2,3,5,7,11,13,17,19,23,29]
        assert a3['phat_numbers'] == [1234.5678, 8888.3333, 1212.3434, 6677.8899]
        assert a3['goobers'] == ['chris', 'jen', 'jane']
        ## only accurate to within a second 'cause synapse strips off the fractional part
        assert a3['present_time'][0].strftime('%Y-%m-%d %H:%M:%S') == a2['present_time'].strftime('%Y-%m-%d %H:%M:%S')

    def test_keyword_annotations(self):
        """
        Test setting annotations using keyword arguments
        """
        ## create a new project and data entity
        project = self.createProject()
        entity = self.createDataEntity(project['id'])

        annos = self.syn.setAnnotations(entity, wazoo='Frank', label='Barking Pumpkin', shark=16776960)
        assert annos['wazoo'] == ['Frank']
        assert annos['label'] == ['Barking Pumpkin']
        assert annos['shark'] == [16776960]


    def test_ACL(self):
        ## get the user's principalId, which is called ownerId and is
        ## returned as a string, while in the ACL, it's an integer
        current_user_id = int(self.syn.getUserProfile()['ownerId'])

        ## other user: using chris's principalId, should be a test user
        other_user_id = 1421212

        ## create a new project
        project = self.createProject()

        acl = self.syn._getACL(project)
        #self.syn.printEntity(acl)
        assert('resourceAccess' in acl)
        current_user_id in [access['principalId'] for access in acl['resourceAccess']]

        acl['resourceAccess'].append({u'accessType': [u'READ', u'CREATE', u'UPDATE'], u'principalId': other_user_id})

        acl = self.syn._storeACL(project, acl)

        acl = self.syn._getACL(project)
        #self.syn.printEntity(acl)
        
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


    def test_fileHandle(self):
        ## file the setup.py file to upload
        path = os.path.join(os.path.dirname(client.__file__), '..', 'setup.py')

        ## upload a file to the file handle service
        fileHandle = self.syn._uploadFileToFileHandleService(path)

        fileHandle2 = self.syn._getFileHandle(fileHandle)

        # print fileHandle
        # print fileHandle2
        assert fileHandle==fileHandle2

        self.syn._deleteFileHandle(fileHandle)


    def test_fileEntity_round_trip(self):
        ## create a new project
        project = self.createProject()

        ## file the setup.py file to upload
        original_path = os.path.join(os.path.dirname(client.__file__), '..', 'setup.py')

        entity = {'name':'Foobar', 'description':'A test file entity...', 'parentId':project['id']}
        entity = self.syn._createFileEntity(entity, original_path)

        entity_downloaded = self.syn.downloadEntity(entity['id'])

        path = os.path.join(entity_downloaded['cacheDir'], entity_downloaded['files'][0])

        assert os.path.exists(path)
        assert filecmp.cmp(original_path, path)
        shutil.rmtree(entity_downloaded['cacheDir'])


    def test_wikiAttachment(self):
        md = """
        This is a test wiki
        =======================

        Blabber jabber blah blah boo.
        """

        ## create a new project
        project = self.createProject()

        ## file the setup.py file to upload
        original_path = os.path.join(os.path.dirname(client.__file__), '..', 'setup.py')

        ## upload a file to the file handle service
        fileHandle = self.syn._uploadFileToFileHandleService(original_path)

        #Create and store the wiki 
        wiki = Wiki(owner=project, title='A Test Wiki', markdown=md, 
                    attachmentFileHandleIds=[fileHandle['id']])
        wiki=self.syn.store(wiki)
        
        #Create a wiki subpage
        subwiki = Wiki(owner=project, title='A sub-wiki', 
                       markdown='nothing', parentWikiId=wiki.id)
        subwiki=self.syn.store(subwiki)
        
        ## retrieve the root wiki from Synapse
        wiki2 = self.syn.getWiki(project)
        assert wiki==wiki2

        ## retrieve the sub wiki from Synapse
        wiki2 = self.syn.getWiki(project, subpageId=subwiki.id)
        assert subwiki==wiki2

        ## try making an update
        wiki['title'] = 'A New Title'
        wiki['markdown'] = wiki['markdown'] + "\nNew stuff here!!!\n"
        wiki = self.syn.store(wiki)

        assert wiki['title'] == 'A New Title'
        assert wiki['markdown'].endswith("\nNew stuff here!!!\n")

        headers = self.syn.getWikiHeaders(project)
        assert headers['totalNumberOfResults']==2
        assert headers['results'][0]['title'] in (wiki['title'], subwiki['title'])

        ## retrieve the file we just uploaded
        #tmpdir = tempfile.mkdtemp()
        # file_props = self.syn._downloadWikiAttachment(project, wiki, 
        #            os.path.basename(original_path), dest_dir=tmpdir)
        # ## we get back a dictionary with path, files and cacheDir
        # path = file_props['path']
        # ## check and delete it
        # assert os.path.exists(path)
        # assert filecmp.cmp(original_path, path)
        # shutil.rmtree(tmpdir)


        ## cleanup
        self.syn._deleteFileHandle(fileHandle)
        self.syn.delete(wiki)
        self.syn.delete(subwiki)

        # ## test that delete worked
        try:
            deleted_wiki = self.syn.getWiki(project)
        except Exception as ex:
            assert ex.response.status_code == 404
        else:
            assert False, 'Should raise 404 exception'

            
    def test_get_and_store_other_objects(self):
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

    def test_evaluations(self):
        ## create a new project
        project = self.createProject()
        name = 'Test Evaluation %s' % (str(uuid.uuid4()),)
        try:
        #Create evaluation
            ev = Evaluation(name=name, description='Evaluation for testing', 
                            contentSource=project['id'], status='CLOSED')
            ev = self.syn.store(ev)
            #Update evaluation
            ev['status']='OPEN'
            ev= self.syn.store(ev, createOrUpdate=True)
            assert ev.status == 'OPEN'

            ## add the current user as a participant
            user = self.syn.getUserProfile()
            self.syn.addEvaluationParticipant(ev, user['ownerId'])

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
                    entity=self.syn.store(f)
                    self.syn.submit(ev, entity)
                finally:
                    os.remove(filename)
                sys.stdout.write('.')
                sys.stdout.flush()

            ## score the submissions
            submissions = self.syn.getSubmissions(ev)
            sys.stdout.write('\nscoring submissions')
            for submission in submissions:
                status=self.syn.getSubmissionStatus(submission)
                status.score = random.random()
                status.status = 'SCORED'
                status.report = 'a fabulous effort!'
                self.syn.store(status)
                sys.stdout.write('.')
                sys.stdout.flush()
            sys.stdout.write('\n')

        finally:
            self.syn.delete(ev)

        ## make sure it's deleted
        try:
            ev = self.syn.getEvaluation(ev)
        except Exception as e:
            print e
            assert e.response.status_code == 404




if __name__ == '__main__':
    test = TestClient()
    test.test_printEntity()
    test.test_login()
    test.test_getEntity()
    test.test_createEntity()
    test.test_updateEntity()
    test.test_deleteEntity()
    test.test_query()
    test.test_uploadFile()
    test.test_version_check()
    test.test_provenance()
    test.test_annotations()
    test.test_fileHandle()
    test.test_fileEntity_round_trip()
    test.test_Evaluations()
    test.test_wikiAttachment()



