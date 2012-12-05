import client
import ConfigParser
from nose.tools import *

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

        print 'running init'
        config = ConfigParser.ConfigParser()
        config.read('config')
        self.syn = client.Synapse(repoEndpoint=repoEndpoint, authEndpoint=authEndpoint, debug=False)
        self.syn.login(config.get('authentication', 'username'), config.get('authentication', 'password'))

    def setUp(self):
       print "setUp"
       pass

    def tearDown(self):
       pass


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
        pass


if __name__ == '__main__':
    test = TestClient()
    test.test__connect()
    test.test_printEntity()
    test.test_login()
    test.test_getEntity()

        
#         # Should not be able to create a project from anon conn
#         projectSpec = {"name":"testProj1","description":"Test project","createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org"}
#         project = None
#         with self.assertRaises(Exception) as cm:
#             project = self.anonConn.createProject(projectSpec)
#             self.assertIsNone(project)
        
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

#         # Should be able to see created project from authenticated but not from anonymous
#         pr = self.authenticatedConn.getRepoEntity("/entity/"+str(projectId))
#         self.assertIsNotNone(pr)
#         with self.assertRaises(Exception) as cm:
#             pr = None
#             pr = self.anonConn.getRepoEntity("/entity/"+str(projectId))
#             self.assertIsNone(pr)
#         # Should be able to see dataset1 from authenticated but not from anonymous conn
#         ds = self.authenticatedConn.getRepoEntity("/entity/"+str(datasetId1))
#         self.assertIsNotNone(ds)
#         with self.assertRaises(Exception) as cm:
#             ds = None
#             ds = self.anonConn.getRepoEntity("/entity/"+str(datasetId1))
#             self.assertIsNone(ds)
#         # Should be able to see dataset1 from authenticated and anonymous conn
# #        ds = self.authenticatedConn.getRepoEntity("/dataset/"+str(datasetId2))
# #        self.assertIsNotNone(ds)
# #        ds = None
# #        ds = self.anonConn.getRepoEntity("/dataset/"+str(datasetId2))
# #        self.assertIsNotNone(ds)
                    
#         # Cleanup
#         self.authenticatedConn.deleteRepoEntity("/project/" + str(projectId))
        
#         print "test_integrated done"


#------- INTEGRATION TESTS -----------------
#if __name__ == '__main__':

            
    #     def test_admin(self):
    #         print "test_admin"
    #         list = self.adminClient.getRepoEntity('/project')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         for p in self.adminClient.getPrincipals():
    #             if p["name"] == "AUTHENTICATED_USERS":
    #                 break
    #         self.assertEqual(p["name"], "AUTHENTICATED_USERS")
            
    #         # Project setup
    #         projectSpec = {"name":"testProj1","description":"Test project","createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org"}
    #         project = self.adminClient.createProject(projectSpec)
    #         self.assertNotEqual(project, None)
    #         self.assertEqual(project["name"], projectSpec["name"])
    #         projectId = project["id"]
            
    #         resourceAccessList = [{"groupName":p["name"], "accessType":["READ"]}]
    #         accessList = {"modifiedBy":"dataLoader", "modifiedOn":"2011-06-06T00:00:00.000-07:00", "resourceAccess":resourceAccessList}
    #         updatedProject = self.adminClient.updateRepoEntity(project["uri"]+"/acl", accessList)
    #         self.assertNotEqual(updatedProject, None)
            
    #         # Dataset 1: inherits ACL from parent project
    #         datasetSpec = {"name":"testDataset1","description":"Test dataset 1 inherits from project 1", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
    #         dataset = self.adminClient.createDataset(datasetSpec)
    #         self.assertNotEqual(dataset, None)
    #         self.assertEqual(dataset["name"], datasetSpec["name"])
            
    #         # Dataset 2: overrides ACL
    #         datasetSpec = {"name":"testDataset2","description":"Test dataset 2 overrides ACL", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
    #         dataset = self.adminClient.createDataset(datasetSpec)
    #         self.assertEqual(dataset["name"], datasetSpec["name"])
    #         resourceAccessList = [{"groupName":p["name"], "accessType":["READ", "UPDATE"]}]
    #         accessList = {"modifiedBy":"dataLoader", "modifiedOn":"2011-06-06T00:00:00.000-07:00", "resourceAccess":resourceAccessList}
    #         updatedDataset = self.adminClient.updateRepoEntity(dataset["uri"]+"/acl", accessList)            
    #         # TODO: Add asserts
                        
    #         # Dataset 3: top-level (no parent)
    #         datasetSpec = {"name":"testDataset3","description":"Test dataset 3 top level ACL", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
    #         dataset = self.adminClient.createDataset(datasetSpec)
    #         self.assertEqual(dataset["name"], datasetSpec["name"])
    #         resourceAccessList = [{"groupName":p["name"], "accessType":["READ", "UPDATE"]}]
    #         accessList = {"modifiedBy":"dataLoader", "modifiedOn":"2011-06-06T00:00:00.000-07:00", "resourceAccess":resourceAccessList}
    #         updatedDataset = self.adminClient.updateRepoEntity(dataset["uri"]+"/acl", accessList)
            
    #         # Actual admin tests
    #         # Create project, dataset child, top level dataset covered above
    #         # Read
    #         list = self.adminClient.getRepoEntity('/project')
    #         self.assertEqual(list["totalNumberOfResults"], 1)
    #         list = self.adminClient.getRepoEntity('/dataset')
    #         self.assertEqual(list["totalNumberOfResults"], 3)
    #         # TODO: Change when adding entities
    #         list = self.adminClient.getRepoEntity('/layer')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         list = self.adminClient.getRepoEntity('/preview')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         list = self.adminClient.getRepoEntity('/location')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         # Can read user/userGroup until changed
    #         list = self.adminClient.getRepoEntity('/user')
    #         self.assertNotEqual(len(list), 0)
    #         list = self.adminClient.getRepoEntity('/userGroup')
    #         self.assertNotEqual(len(list), 0)
            
        
    #     #@unittest.skip("Skip")
    #     def test_anonymous(self):
    #         print "test_anonymous"
    #         # Read
    #         list = self.anonClient.getRepoEntity('/project')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         list = self.anonClient.getRepoEntity('/dataset')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         list = self.anonClient.getRepoEntity('/layer')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         list = self.anonClient.getRepoEntity('/preview')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         list = self.anonClient.getRepoEntity('/location')
    #         self.assertEqual(list["totalNumberOfResults"], 0)
    #         # Can read user/userGroup until changed
    #         list = self.anonClient.getRepoEntity('/user')
    #         self.assertNotEqual(len(list), 0)
    #         list = self.anonClient.getRepoEntity('/userGroup')
    #         self.assertNotEqual(len(list), 0)
            
    #         # Query
            
    #         # Create by anon
    #         projectSpec = {"name":"testProj1","description":"Test project","createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org"}
    #         project = self.anonClient.createProject(projectSpec)
    #         self.assertEqual(None, project)
    #         # TODO: Add tests for other types of entities
            
    #         # Create some entities by admin account
    #         projectSpec = {"name":"testProj1","description":"Test project","createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org"}
    #         project = self.adminClient.createProject(projectSpec)
    #         self.assertIsNotNone(project)
    #         datasetSpec = {"name":"testDataset1","description":"Test dataset 1", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(project["id"])}
    #         dataset = self.anonClient.createDataset(datasetSpec)
    #         self.assertEqual(None, dataset)
              
    #         # Update
    #         attributeChangeList = {"createdBy":"demouser@sagebase.org"}
    #         updatedProject = self.anonClient.updateRepoEntity(project["uri"], attributeChangeList)
    #         self.assertIsNone(updatedProject)
    #         project["createdBy"] = "demouser@sagebase.org"
    #         updateProject = self.anonClient.putRepoEntity(project["uri"], project)
    #         self.assertIsNone(updatedProject)
    #         # TODO: Add tests for other types of entities
            
    #         # Delete
    #         #project = self.anonClient.getRepoEntity("/project")
    #         self.anonClient.deleteRepoEntity(project["uri"])
    #         self.assertIsNotNone(self.adminClient.getRepoEntity(project["uri"]))
    #         # TODO: Add tests for other types of entities
            
