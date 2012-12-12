import client
import ConfigParser
from nose.tools import *
import tempfile
import os

PROJECT_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Project', u'name': ''}
DATA_JSON={ u'entityType': u'org.sagebionetworks.repo.model.Data', u'parentId': ''}


def test_b():
    assert 'b' == 'b'

class TestClient:
    """
    Integration tests against a repository service
    """

    def __init__(self, repoEndpoint='https://repo-prod.sagebase.org/repo/v1',
                 authEndpoint='https://auth-prod.sagebase.org/auth/v1'):
        """
        Arguments:
        - `repoEndpoint`:
        """
        config = ConfigParser.ConfigParser()
        config.read('config')
        self.syn = client.Synapse(repoEndpoint=repoEndpoint, authEndpoint=authEndpoint, debug=False)
        self.syn.login(config.get('authentication', 'username'), config.get('authentication', 'password'))
        self.toRemove=[]

    def setUp(self):
        self.toRemove=[]


    def tearDown(self):
        print 'tearDown %i entities' %len(self.toRemove)
        for item in self.toRemove:
            self.syn.deleteEntity(item)


    def createProject(self):
        import uuid
        PROJECT_JSON['name'] = str(uuid.uuid4())
        entity = self.syn.createEntity(PROJECT_JSON)
        self.toRemove.append(entity)
        return entity

    def test__connect(self):
        import httplib
        #Test https protocol
        self.syn.repoEndpoint['protocol']='https'
        conn = self.syn._connect(self.syn.repoEndpoint)
        assert isinstance(conn, httplib.HTTPSConnection)

        #Test http protocol
        self.syn.repoEndpoint['protocol']='http'
        conn = self.syn._connect(self.syn.repoEndpoint)
        assert isinstance(conn, httplib.HTTPConnection)


    def test_printEntity(self):
        self.syn.printEntity({'hello':'world', 'alist':[1,2,3,4]}) 
        #Nothing really to test

    
    def test_login(self):
        #Has already been tested for correct username and password.
        #Test that we fail gracefully with wrong user
        assert_raises(Exception, self.syn.login, 'asdf', 'notarealpassword')
        

    def test_getEntity(self):
        #Create a new project
        entity = self.createProject()

        #Get new entity and check that it is same
        returnEntity = self.syn.getEntity(entity)
        assert entity == returnEntity
        #Get entity by id
        returnEntity = self.syn.getEntity(entity['id'])
        assert entity == returnEntity


    def test_loadEntity(self):
        #loadEntity does the same thing as downloadEntity so nothing new to test
        pass

    def test__createUniveralEntity(self):
        #Tested by testing of login and createEntity
        pass

    def test_createEntity(self):
        #Create a project
        entity = self.createProject()

        #Add a data entity to project
        DATA_JSON['parentId']= entity['id']
        entity = self.syn.createEntity(DATA_JSON)

        #Get the data entity and assert that it is unchanged
        returnEntity = self.syn.getEntity(entity['id'])
        assert entity == returnEntity


    def test_updateEntitity(self):
        entity = self.createProject()
        DATA_JSON['parentId']= entity['id']
        entity = self.syn.createEntity(DATA_JSON)
        entity[u'tissueType']= u'yuuupp',
        entity = self.syn.updateEntity(entity)
        returnEntity = self.syn.getEntity(entity['id'])
        assert entity == returnEntity


    def test_putEntity(self):
        #Does the same thing as updateEntity
        pass


    def test_query(self):
        #Create a project then add entities and verify that I can find them with a query
        projectEntity = self.createProject()
        DATA_JSON['parentId'] = projectEntity['id']
        for i in range(2):
            entity = self.syn.createEntity(DATA_JSON)
            qry= self.syn.query("select id, name from entity where entity.parentId=='%s'" %projectEntity['id'])
            assert qry['totalNumberOfResults']==(i+1)

    
    def test_deleteEntity(self):
        projectEntity = self.createProject()
        DATA_JSON['parentId'] = projectEntity['id']
        
        #Check that we can delete an entity by dictionary
        entity = self.syn.createEntity(DATA_JSON)
        self.syn.deleteEntity(entity)
        assert_raises(Exception, self.syn.getEntity, entity)

        #Check that we can delete an entity by synapse ID
        entity = self.syn.createEntity(DATA_JSON)
        self.syn.deleteEntity(entity['id'])
        assert_raises(Exception, self.syn.getEntity, entity)


    def test_createSnapshotSummary(self):
        pass


    def test_uploadFile(self):
        projectEntity = self.createProject()
        DATA_JSON['parentId'] = projectEntity['id']
        entity = self.syn.createEntity(DATA_JSON)       

        #create a temporary file
        (fd, fname) = tempfile.mkstemp()
        self.syn.uploadFile(entity, fname)

        #Download and verify that it is the same filename
        entity = self.syn.downloadEntity(entity)
        assert entity['files'][0]==os.path.split(fname)[-1]




if __name__ == '__main__':
    test = TestClient()
    test.test__connect()
    test.test_printEntity()
    test.test_login()
    test.test_getEntity()
    test.test_createEntity()
    test.test_updateEntity()
    test.test_query()
    test.test_uploadFile()
