#!/usr/bin/env python
import unittest
import client
import utils
import argparse

class IntegrationTestSynapse(unittest.TestCase):
    '''
    Integration tests against a repository service

    To run them locally,
    (1) edit platform/trunk/integration-test/pom.xml set this to true
    <org.sagebionetworks.integration.debug>true</org.sagebionetworks.integration.debug>
    (2) run the integration build to start the servlets
    /platform/trunk/integration-test>mvn clean verify 
    '''
    
    #-------------------[ Constants ]----------------------
    
    def __init__(self, testname, repo_endpoint, auth_endpoint, user, password):
        super(IntegrationTestSynapse, self).__init__(testname)
        self.repo_endpoint = repo_endpoint
        self.auth_endpoint = auth_endpoint
        self.user = user
        self.password = password
    
    def setUp(self):
        print "setUp"
        
        # Anonymous connection
        self.anonConn = client.Synapse(self.repo_endpoint, self.auth_endpoint, 30, False)
#        self.anonConn = client.Synapse('http://140.107.149.29:8080/services-repository-0.6-SNAPSHOT/repo/v1', 'http://140.107.149.29:8080/services-authentication-0.6-SNAPSHOT/auth/v1', 30, False)
        self.assertTrue(self.anonConn.sessionToken == None)
        self.assertFalse("sessionToken" in self.anonConn.headers)
        # Authenticated connection
        #self.authenticatedConn = Synapse('http://localhost:8080/services-repository-0.6-SNAPSHOT/repo/v1', 'http://localhost:8080/services-authentication-0.6-SNAPSHOT/auth/v1', 30, False)
        #self.authenticatedConn = client.Synapse('http://140.107.149.29:8080/services-repository-0.6-SNAPSHOT/repo/v1', 'http://140.107.149.29:8080/services-authentication-0.6-SNAPSHOT/auth/v1', 30, False)
        self.authenticatedConn = client.Synapse(self.repo_endpoint, self.auth_endpoint, 30, False)
        self.authenticatedConn.login(user, password)
        self.assertFalse(self.authenticatedConn.sessionToken == None)
        self.assertTrue("sessionToken" in self.authenticatedConn.headers)
        if "sessionToken" in self.authenticatedConn.headers:
            self.assertTrue(self.authenticatedConn.sessionToken == self.authenticatedConn.headers["sessionToken"])
            
    def tearDown(self):
        # Can't do this in integration test as I can't ensure there's no leftover entity
        # that I don't have access to.
        #allProjects = self.authenticatedConn.getRepoEntity("/project?limit=100");
        #for project in allProjects["results"]:
        #    print "About to nuke: " + project["uri"]
        #    self.authenticatedConn.deleteRepoEntity(project["uri"])
        #    
        #allDatasets = self.authenticatedConn.getRepoEntity("/dataset?limit=500");
        #for dataset in allDatasets["results"]:
        #    print "About to nuke: " + dataset["uri"]
        #    self.authenticatedConn.deleteRepoEntity(dataset["uri"])
        #    
        #allLayers = self.authenticatedConn.getRepoEntity("/layer?limit=500");
        #for layer in allLayers["results"]:
        #    print "About to nuke: " + layer["uri"]
        #    self.authenticatedConn.deleteRepoEntity(layer["uri"])
        pass
        
    #def test_stub(self):
    #    print "test_stub"
    #    print self.repo_endpoint
    #    print self.auth_endpoint
    #    

    def test_integrated(self):
        print "test_integrated"
        
        list = self.authenticatedConn.getRepoEntity('/project')
        self.assertIsNotNone(list)

        for p in self.authenticatedConn.getPrincipals():
            if p["name"] == "AUTHENTICATED_USERS":
                break
        self.assertEqual(p["name"], "AUTHENTICATED_USERS")
        
        # Should not be able to create a project from anon conn
        projectSpec = {"name":"testProj1","description":"Test project","createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org"}
        project = None
        with self.assertRaises(Exception) as cm:
            project = self.anonConn.createProject(projectSpec)
            self.assertIsNone(project)
        
        # Project creation should work from authenticated conn
        project = self.authenticatedConn.createProject(projectSpec)
        self.assertNotEqual(project, None)
        self.assertEqual(project["name"], projectSpec["name"])
        projectId = project["id"]
        
        # Dataset 1: inherits ACL from parent project
        datasetSpec = {"name":"testDataset1","description":"Test dataset 1 inherits from project 1", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
        dataset = self.authenticatedConn.createDataset(datasetSpec)
        self.assertIsNotNone(dataset)
        self.assertEqual(dataset["name"], datasetSpec["name"])
        datasetId1 = dataset["id"]
        
        # Dataset 2: overrides ACL
#        datasetSpec = {"name":"testDataset2","description":"Test dataset 2 overrides ACL", "status":"pending", "createdOn":"2011-06-06T00:00:00.000-07:00", "createdBy":"test@sagebase.org", "parentId":str(projectId)}
#        dataset = self.authenticatedConn.createDataset(datasetSpec)
#        self.assertIsNotNone(dataset)
#        self.assertEqual(dataset["name"], datasetSpec["name"])
#        datasetId2 = dataset["id"]
#        existingAcl = self.authenticatedConn.getRepoEntity("/dataset/"+str(datasetId2)+"/acl")
#        self.assertIsNotNone(existingAcl)
#        resourceAccessList = existingAcl["resourceAccess"]
#        resourceAccessList.append({"groupName":p["name"], "accessType":["READ", "UPDATE"]})
#        resourceAccessList.append({"groupName":"PUBLIC", "accessType":["READ"]})
#        accessList = {"modifiedBy":"dataLoader", "modifiedOn":"2011-06-06T00:00:00.000-07:00", "resourceAccess":resourceAccessList}
#        updatedAcl = self.authenticatedConn.updateRepoEntity(dataset["uri"]+"/acl", accessList)
#        self.assertIsNotNone(updatedAcl)

        # Should be able to see created project from authenticated but not from anonymous
        pr = self.authenticatedConn.getRepoEntity("/project/"+str(projectId))
        self.assertIsNotNone(pr)
        with self.assertRaises(Exception) as cm:
            pr = None
            pr = self.anonConn.getRepoEntity("/project/"+str(projectId))
            self.assertIsNone(pr)
        # Should be able to see dataset1 from authenticated but not from anonymous conn
        ds = self.authenticatedConn.getRepoEntity("/dataset/"+str(datasetId1))
        self.assertIsNotNone(ds)
        with self.assertRaises(Exception) as cm:
            ds = None
            ds = self.anonConn.getRepoEntity("/dataset/"+str(datasetId1))
            self.assertIsNone(ds)
        # Should be able to see dataset1 from authenticated and anonymous conn
#        ds = self.authenticatedConn.getRepoEntity("/dataset/"+str(datasetId2))
#        self.assertIsNotNone(ds)
#        ds = None
#        ds = self.anonConn.getRepoEntity("/dataset/"+str(datasetId2))
#        self.assertIsNotNone(ds)
                    
        # Cleanup
        self.authenticatedConn.deleteRepoEntity("/project/" + str(projectId))
        
        print "test_integrated done"

# Main
parser = argparse.ArgumentParser()
parser.add_argument("--repoEndpoint")
parser.add_argument("--authEndpoint")
parser.add_argument("--user")
parser.add_argument("--password")
args = parser.parse_args()
repo_endpoint = args.repoEndpoint
auth_endpoint = args.authEndpoint
user = args.user
password = args.password

suite = unittest.TestSuite()
suite.addTest(IntegrationTestSynapse("test_integrated", repo_endpoint, auth_endpoint, user, password))
unittest.TextTestRunner().run(suite)

