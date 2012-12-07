import client
import ConfigParser
from nose.tools import *


PROJECTSTR={ u'entityType': u'org.sagebionetworks.repo.model.Project', u'name': 'NoseTestParent'}
DATASTR={ u'entityType': u'org.sagebionetworks.repo.model.Data', u'name': 'test entity', u'parentId': ''}


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

    def setUp(self):
        self.toRemove=[]
        print "setUp"


    def tearDown(self):
        print 'tearDown %i entities' %len(self.toRemove)
        for item in self.toRemove:
            self.syn.deleteEntity(item)


    def test__connect(self):
        import httplib
        print '_connect()'
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
        entity = self.syn.createEntity(PROJECTSTR)
        self.toRemove.append(entity)
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
        #Tested by login testing and createEntity
        pass

    def test_createEntity(self):
        #Create a project
        entity = self.syn.createEntity(PROJECTSTR)
        self.toRemove.append(entity)
        #Add a data entity to project
        DATASTR['parentId']= entity['id']
        entity = self.syn.createEntity(DATASTR)

        #Get the data entity and assert that it is unchanged
        returnEntity = self.syn.getEntity(entity['id'])
        assert entity == returnEntity


    def test_updateEntitity(self):
        pass


    def test_query(self):
        pass
    
    def test_deleteEntity(self):
        pass


    def test_putEntity(self):
        pass


    def test_createSnapshotSummary(self):
        pass


    def test_uploadFile(self):
        pass



if __name__ == '__main__':
    test = TestClient()
    test.test__connect()
    test.test_printEntity()
    test.test_login()
    test.test_getEntity()
    test.test_createEntity()
    

        
        
#         # Project creation should work from authenticated conn
#         project = self.authenticatedConn.createProject(projectSpec)
#         self.assertNotEqual(project, None)
#         self.assertEqual(project["name"], projectSpec["name"])
#         projectId = project["id"]
        
#         # Dataset 1: inherits ACL from parent project
#         datasetSpec = {"name":"testDataset1","description":"Test dataset 1 inherits from project 1", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
#         dataset = self.authenticatedConn.createDataset(datasetSpec)
#         self.assertIsNotNone(dataset)
#         self.assertEqual(dataset["name"], datasetSpec["name"])
#         datasetId1 = dataset["id"]
        
#         # Dataset 2: overrides ACL
# #        datasetSpec = {"name":"testDataset2","description":"Test dataset 2 overrides ACL", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
# #        dataset = self.authenticatedConn.createDataset(datasetSpec)
# #        self.assertIsNotNone(dataset)
# #        self.assertEqual(dataset["name"], datasetSpec["name"])
# #        datasetId2 = dataset["id"]
# #        existingAcl = self.authenticatedConn.getRepoEntity("/dataset/"+str(datasetId2)+"/acl")
# #        self.assertIsNotNone(existingAcl)
# #        resourceAccessList = existingAcl["resourceAccess"]
# #        resourceAccessList.append({"groupName":p["name"], "accessType":["READ", "UPDATE"]})
# #        resourceAccessList.append({"groupName":"PUBLIC", "accessType":["READ"]})
# #        accessList = {"modifiedBy":"dataLoader", "modifiedOn":"2011-06-06T00:00:00.000-07:00", "resourceAccess":resourceAccessList}
# #        updatedAcl = self.authenticatedConn.updateRepoEntity(dataset["uri"]+"/acl", accessList)
# #        self.assertIsNotNone(updatedAcl)

            
            
