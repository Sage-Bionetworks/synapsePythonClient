import client
#import utils
#import argparse

def test_b():
    assert 'b' == 'b'

class TestClient:
    '''
    Integration tests against a repository service
    '''

    def __init__(self, repoEndpoint='https://repo-prod.sagebase.org/repo/v1'):
        """
        
        Arguments:
        - `self`:
        - `repoEndpoint`:
        """
        
   def setUp(self):
       print "setUp"
       pass

   def tearDown(self):
       pass

    def test_printEntity():
        assert 'c' == 'b'

    def test_getEntity():
        pass

    #...
    #...
    #...


    
#     #-------------------[ Constants ]----------------------
    
    
#     def test_integrated(self):
#         print "test_integrated"
        
#         list = self.authenticatedConn.getRepoEntity('/query?query=''select+id+from+project''')
#         self.assertIsNotNone(list)

#         for p in self.authenticatedConn.getPrincipals():
#             if p["name"] == "AUTHENTICATED_USERS":
#                 break
#         self.assertEqual(p["name"], "AUTHENTICATED_USERS")
        
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
if __name__ == '__main__':
    pass
    
    # import unittest, utils

    # class IntegrationTestSynapse(unittest.TestCase):
    #     """TODO!  THESE TESTS ARE NO LONGER TRUE TO THE IMPLEMENTATION OF SYNAPSE!
    #        THESE TESTS SHOULD BE COMPLETELEY REWRITTEN.

    #     Integration tests against a repository service

    #     To run them locally,
    #     (1) edit platform/trunk/integration-test/pom.xml set this to true
    #     <org.sagebionetworks.integration.debug>true</org.sagebionetworks.integration.debug>
    #     (2) run the integration build to start the servlets
    #     /platform/trunk/integration-test>mvn clean verify """
    #     #-------------------[ Constants ]----------------------
    #     DATASET = '{"status": "pending", "description": "Genetic and epigenetic alterations have been identified that lead to transcriptional Annotation of prostate cancer genomes provides a foundation for discoveries that can impact disease understanding and treatment. Concordant assessment of DNA copy number, mRNA expression, and focused exon resequencing in the 218 prostate cancer tumors represented in this dataset haveidentified the nuclear receptor coactivator NCOA2 as an oncogene in approximately 11% of tumors. Additionally, the androgen-driven TMPRSS2-ERG fusion was associated with a previously unrecognized, prostate-specific deletion at chromosome 3p14 that implicates FOXP1, RYBP, and SHQ1 as potential cooperative tumor suppressors. DNA copy-number data from primary tumors revealed that copy-number alterations robustly define clusters of low- and high-risk disease beyond that achieved by Gleason score.  ", "createdBy": "Charles Sawyers", "releaseDate": "2008-09-14T00:00:00.000-07:00", "version": "1.0.0", "name": "MSKCC Prostate Cancer"}' 
        
    #     def setUp(self):
    #         print "setUp"
    #         # Anonymous connection
    #         self.anonClient = Synapse('http://localhost:8080/services-repository-0.6-SNAPSHOT/repo/v1', 'http://localhost:8080/services-authentication-0.6-SNAPSHOT/auth/v1', 30, False)
    #         self.assertTrue(self.anonClient.sessionToken == None)
    #         self.assertFalse("sessionToken" in self.anonClient.headers)
    #         # Admin connection
    #         self.adminClient = Synapse('http://localhost:8080/services-repository-0.6-SNAPSHOT/repo/v1', 'http://localhost:8080/services-authentication-0.6-SNAPSHOT/auth/v1', 30, False)
    #         self.adminClient.login("admin", "admin")
    #         self.assertFalse(self.adminClient.sessionToken == None)
    #         self.assertTrue("sessionToken" in self.adminClient.headers)
    #         if "sessionToken" in self.adminClient.headers:
    #             self.assertTrue(self.adminClient.sessionToken == self.adminClient.headers["sessionToken"])
                
    #     def tearDown(self):
    #         allProjects = self.adminClient.getRepoEntity("/project?limit=100");
    #         for project in allProjects["results"]:
    #             print "About to nuke: " + project["uri"]
    #             self.adminClient.deleteRepoEntity(project["uri"])
                
    #         allDatasets = self.adminClient.getRepoEntity("/dataset?limit=500");
    #         for dataset in allDatasets["results"]:
    #             print "About to nuke: " + dataset["uri"]
    #             self.adminClient.deleteRepoEntity(dataset["uri"])
                
    #         allLayers = self.adminClient.getRepoEntity("/layer?limit=500");
    #         for layer in allLayers["results"]:
    #             print "About to nuke: " + layer["uri"]
    #             self.adminClient.deleteRepoEntity(layer["uri"])
            
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
            
    # unittest.main()
