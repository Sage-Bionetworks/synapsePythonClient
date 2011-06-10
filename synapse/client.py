#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, sys, string, traceback, json, urllib, urlparse, httplib

def addArguments(parser):
    '''
    Synapse command line argument helper
    '''
    parser.add_argument('--repoEndpoint', '-e',
                        help='the url to which to send the metadata '
                        + '(e.g. https://staging-repoService.elasticbeanstalk.com/repo/v1)',
                        required=True)
    
    parser.add_argument('--authEndpoint', '-a',
                        help='the url against which to authenticate '
                        + '(e.g. http://staging-auth.elasticbeanstalk.com/auth/v1)',
                        required=True)

    parser.add_argument('--serviceTimeoutSeconds', '-t',
                        help='the socket timeout for blocking operations to the service (e.g., connect, send, receive), defaults to 30 seconds',
                        default=30)
    
    parser.add_argument('--user', '-u', help='user (email name)', required=True)
    
    parser.add_argument('--password', '-p', help='password', required=True)


    
def factory(args):
    '''
    Factory method to create a Synapse instance from command line args
    '''
    return Synapse(args.repoEndpoint,
                   args.authEndpoint,
                   args.serviceTimeoutSeconds,
                   args.debug)
    
class Synapse:
    '''
    Python implementation for Synapse repository service client
    '''
    #-------------------[ Constants ]----------------------
    #HEADERS = {
    #    'Content-type': 'application/json',
    #    'Accept': 'application/json',
    #    }
    
    def __init__(self, repoEndpoint, authEndpoint, serviceTimeoutSeconds, debug):
        '''
        Constructor
        '''
        
        self.headers = {'content-type': 'application/json', 'Accept': 'application/json'}

        self.serviceTimeoutSeconds = serviceTimeoutSeconds 
        self.debug = debug
        self.sessionToken = None

        self.repoEndpoint = {}
        self.authEndpoint = {}
        #self.serviceEndpoint = serviceEndpoint
        #self.authEndpoint = authEndpoint

        parseResult = urlparse.urlparse(repoEndpoint)
        self.repoEndpoint["location"] = parseResult.netloc
        self.repoEndpoint["prefix"] = parseResult.path
        self.repoEndpoint["protocol"] = parseResult.scheme
        #self.serviceLocation = parseResult.netloc
        #self.servicePrefix = parseResult.path
        #self.serviceProtocol = parseResult.scheme
        
        parseResult = urlparse.urlparse(authEndpoint)
        self.authEndpoint["location"] = parseResult.netloc
        self.authEndpoint["prefix"] = parseResult.path
        self.authEndpoint["protocol"] = parseResult.scheme
        #self.authLocation = parseResult.netloc
        #self.authPrefix = parseResult.path
        #self.authProtocol = parseResult.scheme


    def authenticate(self, email, password):
        """
        Authenticate and get session token
        """
        if (None == email or None == password):
            raise Exception("invalid parameters")
            
        uri = "/session"
        #req = "{\"email\":\"" + email + "\",\"password\":\"" + password + "\"}"
        req = {"email":email, "password":password}
        
        if(0 != string.find(uri, self.authEndpoint["prefix"])):
            uri = self.authEndpoint["prefix"] + uri
        
        storedEntity = self.createAuthEntity(uri, req)
        self.sessionToken = storedEntity["sessionToken"]
        self.headers["sessionToken"] = self.sessionToken
        
    def createEntity(self, endpoint, uri, entity):
        '''
        Create a new entity on either service
        '''
        if(None == uri or None == entity or None == endpoint
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")
            
        if(0 != string.find(uri, endpoint["prefix"])):
                uri = endpoint["prefix"] + uri

        conn = {}
        if('https' == endpoint["protocol"]):
            conn = httplib.HTTPSConnection(endpoint["location"],
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(endpoint["location"],
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to create %s with %s' % (uri, json.dumps(entity))

        storedEntity = None
        try:
            conn.request('POST', uri, json.dumps(entity), self.headers)
            resp = conn.getresponse()
            output = resp.read()
            if resp.status == 201:
                if self.debug:
                    print output
                storedEntity = json.loads(output)
            else:
                print resp.status, resp.reason
                print json.dumps(entity)
                print output
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return storedEntity
        
    def createAuthEntity(self, uri, entity):
        """
        Create a new user, session etc. on authentication service
        """
        return self.createEntity(self.authEndpoint, uri, entity)
        
    def createRepoEntity(self, uri, entity):
        """
        Create a new dataset, layer etc. on repository service
        """
            
        return self.createEntity(self.repoEndpoint, uri, entity)
        
    def getEntity(self, endpoint, uri):
        '''
        Get a dataset, layer, preview, annotations, etc..
        '''
      
        if endpoint == None:
            raise Exception("invalid parameters")
            
        if uri == None:
            return
            
        if(0 != string.find(uri, endpoint["prefix"])):
                uri = endpoint["prefix"] + uri
    
        conn = {}
        if('https' == endpoint["protocol"]):
            conn = httplib.HTTPSConnection(endpoint["location"],
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(endpoint["location"],
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to get %s' % (uri)
    
        entity = None
    
        try:
            conn.request('GET', uri, None, self.headers)
            resp = conn.getresponse()
            output = resp.read()
            if resp.status == 200:
                if self.debug:
                    print output
                entity = json.loads(output)
            else:
                print resp.status, resp.reason, output
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return entity
        
    def getAuthEntity(self, uri):
        """
        Get an entity from auth service
        """
        return self.getEntity(self.authEndpoint, uri)
        
    def getRepoEntity(self, uri):
        """
        Get an entity (dataset, layer, ...) from repository service
        """
        return self.getEntity(self.repoEndpoint, uri)
        
    def updateEntity(self, endpoint, uri, entity):
        '''
        Update a dataset, layer, preview, annotations, etc...

        This convenience method first grabs a copy of the currently
        stored entity, then overwrites fields from the entity passed
        in on top of the stored entity we retrieved and then PUTs the
        entity. This essentially does a partial update from the point
        of view of the user of this API.
        
        Note that users of this API may want to inspect what they are
        overwriting before they do so. Another approach would be to do
        a GET, display the field to the user, allow them to edit the
        fields, and then do a PUT.
        '''
        if(None == uri or None == entity or None == endpoint
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")
            
        oldEntity = self.getEntity(endpoint, uri)
        if(oldEntity == None):
            return None

        # Overwrite our stored fields with our updated fields
        keys = entity.keys()
        for key in keys:
            if(-1 != string.find(uri, "annotations")):
                # annotations need special handling
                for annotKey in entity[key].keys():
                    oldEntity[key][annotKey] = entity[key][annotKey]
            else:
                oldEntity[key] = entity[key]

        return self.putEntity(endpoint, uri, oldEntity)
        
    def updateAuthEntity(self, uri, entity):
        return self.updateEntity(self.authEndpoint, uri, entity)
        
    def updateRepoEntity(self, uri, entity):
        return self.updateEntity(self.repoEndpoint, uri, entity)

    def putEntity(self, endpoint, uri, entity):
        '''
        Update an entity on given endpoint
        '''
        if(None == uri or None == entity or None == endpoint
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")
          
        if(0 != string.find(uri, endpoint["prefix"])):
                uri = endpoint["prefix"] + uri
    
        conn = {}
        if('https' == endpoint["protocol"]):
            conn = httplib.HTTPSConnection(endpoint["location"],
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(endpoint["location"],
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(2);
            print 'About to update %s with %s' % (uri, json.dumps(entity))
    
        putHeaders = self.headers
        if "etag" in entity:
            putHeaders['ETag'] = entity['etag']
    
        storedEntity = None
        try:
            conn.request('PUT', uri, json.dumps(entity), putHeaders)
            resp = conn.getresponse()
            output = resp.read()
            # Handle both 200 and 204 as auth returns 204 for success
            if resp.status == 200 or resp.status == 204:
                if self.debug:
                    print output
                storedEntity = json.loads(output)
            else:
                print resp.status, resp.reason, output
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return storedEntity
        
    def putAuthEntity(self, uri, entity):
        """
        Updates an entity (user, ...) in the auth service
        """
        return self.putEntity(self.authEndpoint, uri, entity)
        
    def putRepoEntity(self, uri, entity):
        """
        Update an entity (dataset, layer, ...) in the repository service
        """
        return self.putEntity(self.repoEndpoint, uri, entity)

    def deleteEntity(self, endpoint, uri):
        '''
        Delete an entity (dataset, layer, user, ...) on either service
        '''

        if(None == uri):
            return
            
        if(0 != string.find(uri, endpoint["prefix"])):
                uri = endpoint["prefix"] + uri
    
        conn = {}
        if('https' == endpoint["protocol"]):
            conn = httplib.HTTPSConnection(endpoint["location"],
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(endpoint["location"],
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to delete %s' % (uri)

        try:
            conn.request('DELETE', uri, None, self.headers)
            resp = conn.getresponse()
            output = resp.read()
            if resp.status != 204:
                print resp.status, resp.reason, output
            elif self.debug:
                print output

            return None;
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        conn.close()
        
    #def deleteAuthEntity(self, uri):
    #    pass
        
    def deleteRepoEntity(self, uri):
        """
        Delete an entity (dataset, layer, ...) on the repository service
        """
            
        return self.deleteEntity(self.repoEndpoint, uri)

    def query(self, endpoint, query):
        '''
        Query for datasets, layers, etc..
        '''
        
        uri = endpoint["prefix"] + '/query?query=' + urllib.quote(query)
    
        conn = {}
        if('https' == endpoint["protocol"]):
            conn = httplib.HTTPSConnection(endpoint["location"],
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(endpoint["location"],
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to query %s' % (query)
    
        results = None
    
        try:
            conn.request('GET', uri, None, self.headers)
            resp = conn.getresponse()
            output = resp.read()
            if resp.status == 200:
                if self.debug:
                    print output
                results = json.loads(output)
            else:
                print resp.status, resp.reason, output
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return results
        
    def repoQuery(self, query):
        """
        Query for datasets, layers etc.
        """
        return self.query(self.repoEndpoint, query)
            
    def createDataset(self, dataset):
        '''
        We have a helper method to create a dataset since it is a top level url
        '''
        return self.createRepoEntity('/dataset', dataset)

    def getDataset(self, datasetId):
        '''
        We have a helper method to get a dataset since it is a top level url
        '''
        return self.getRepoEntity('/dataset/' + str(datasetId))
        
    def createProject(self, project):
        """
        Helper method to create project
        """
        return self.createRepoEntity('/project', project)
        
    def getProject(self, projectId):
        """
        Helper method to get a project
        """
        return self.getRepoEntity('/project/' + str(projectId))
        
    def getPrincipals(self):
        """
        Helper method to get list of principals
        """
        l = []
        l.extend(self.getRepoEntity('/userGroup'))
        l.extend(self.getRepoEntity('/user'))
        return l
        
    
#------- INTEGRATION TESTS -----------------
if __name__ == '__main__':
    import unittest, utils
    
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
        DATASET = '{"status": "pending", "description": "Genetic and epigenetic alterations have been identified that lead to transcriptional Annotation of prostate cancer genomes provides a foundation for discoveries that can impact disease understanding and treatment. Concordant assessment of DNA copy number, mRNA expression, and focused exon resequencing in the 218 prostate cancer tumors represented in this dataset haveidentified the nuclear receptor coactivator NCOA2 as an oncogene in approximately 11% of tumors. Additionally, the androgen-driven TMPRSS2-ERG fusion was associated with a previously unrecognized, prostate-specific deletion at chromosome 3p14 that implicates FOXP1, RYBP, and SHQ1 as potential cooperative tumor suppressors. DNA copy-number data from primary tumors revealed that copy-number alterations robustly define clusters of low- and high-risk disease beyond that achieved by Gleason score.  ", "creator": "Charles Sawyers", "releaseDate": "2008-09-14", "version": "1.0.0", "name": "MSKCC Prostate Cancer"}' 
        
        def setUp(self):
            pass
            #self.Client = Synapse('http://140.107.149.29:8080/repo/v1',
            #                      'http://staging-auth.elasticbeanstalk.com/auth/v1',
            #                      30,
            #                      True)

        def test_constructor(self):
            testClient = Synapse('http://140.107.149.29:8080/repo/v1', 'http://staging-auth.elasticbeanstalk.com/auth/v1', 30, False)
            self.assertEqual(testClient.repoEndpoint["location"], '140.107.149.29:8080')
            self.assertEqual(testClient.repoEndpoint["prefix"], '/repo/v1')
            self.assertEqual(testClient.repoEndpoint["protocol"], 'http')
            self.assertEqual(testClient.authEndpoint["location"], 'staging-auth.elasticbeanstalk.com')
            self.assertEqual(testClient.authEndpoint["prefix"], '/auth/v1')
            self.assertEqual(testClient.authEndpoint["protocol"], 'http')
            
        def test_anonymous(self):
            ## Create some entities to test update/delete
            idClient = Synapse('http://140.107.149.29:8080/repo/v1', 'http://staging-auth.elasticbeanstalk.com/auth/v1', 30, False)
            idClient.authenticate("demouser@sagebase.org", "demouser-pw")
            self.assertFalse(idClient.sessionToken == None)
            self.assertTrue("sessionToken" in idClient.headers)
            p = idClient.getPrincipals()[0]
            self.assertEqual(p["name"], "Identified Users")
            projectSpec = {"name":"testProj1","description":"Test project","creationDate":"2011-06-06", "creator":"test@sagebase.org"}
            project = idClient.createProject(projectSpec)
            self.assertEqual(project["name"], projectSpec["name"])
            projectId = project["id"]
            datasetSpec = {"name":"testDataset1","description":"Test dataset 1", "status":"pending", "creationDate":"2011-06-06", "creator":"test@sagebase.org", "parentId":str(projectId)}
            dataset = idClient.createDataset(datasetSpec)
            self.assertEqual(dataset["name"], datasetSpec["name"])
            datasetSpec = {"name":"testDataset2","description":"Test dataset 2", "status":"pending", "creationDate":"2011-06-06", "creator":"test@sagebase.org", "parentId":str(projectId)}
            dataset = idClient.createDataset(datasetSpec)
            self.assertEqual(dataset["name"], datasetSpec["name"])
            datasetSpec = {"name":"testDataset3","description":"Test dataset 3", "status":"pending", "creationDate":"2011-06-06", "creator":"test@sagebase.org"}
            dataset = idClient.createDataset(datasetSpec)
            self.assertEqual(dataset["name"], datasetSpec["name"])

            anonClient = Synapse('http://140.107.149.29:8080/repo/v1', 'http://staging-auth.elasticbeanstalk.com/auth/v1', 30, False)
            self.assertTrue(anonClient.sessionToken == None)
                        
            # Create
            projectSpec = {"name":"testProj1","description":"Test project","creationDate":"2011-06-06", "creator":"test@sagebase.org"}
            project = anonClient.createProject(projectSpec)
            self.assertEqual(None, project)
            # TODO: Add tests for other types of entities
            
            # Read
            list = anonClient.getRepoEntity('/project')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.getRepoEntity('/dataset')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.getRepoEntity('/layer')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.getRepoEntity('/preview')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.getRepoEntity('/location')
            self.assertEqual(list["totalNumberOfResults"], 0)
            # Can read user/userGroup until changed
            list = anonClient.getRepoEntity('/user')
            self.assertNotEqual(len(list), 0)
            list = anonClient.getRepoEntity('/userGroup')
            self.assertNotEqual(len(list), 0)
            
            # Query
            list = anonClient.repoQuery('select * from project')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.repoQuery('select * from dataset')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.repoQuery('select * from layer')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.repoQuery('select * from layerpreview')
            self.assertEqual(list["totalNumberOfResults"], 0)
            list = anonClient.repoQuery('select * from layerlocation')
            self.assertEqual(list["totalNumberOfResults"], 0)
            
            # Update
            # TODO: Recode to remove explicit reference to entity ids
            attributeChangeList = {"creator":"demouser@sagebase.org"}
            project = idClient.getRepoEntity("/project/0")
            updatedProject = anonClient.updateRepoEntity(project["uri"], attributeChangeList)
            self.assertIsNone(updatedProject)
            project["creator"] = "demouser@sagebase.org"
            updateProject = anonClient.putRepoEntity(project["uri"], project)
            self.assertIsNone(updatedProject)
            # TODO: Add tests for other types of entities
            
            # Delete
            dataset = idClient.getRepoEntity("/dataset/1")
            anonClient.deleteRepoEntity(dataset["uri"])
            self.assertIsNotNone(idClient.getRepoEntity(dataset["uri"]))
            # TODO: Add tests for other types of entities
            
        #def test_admin(self):
        #    idClient = Synapse('http://140.107.149.29:8080/repo/v1', 'http://staging-auth.elasticbeanstalk.com/auth/v1', 30, False)
        #    idClient.authenticate("demouser@sagebase.org", "demouser-pw")
        #    self.assertFalse(idClient.sessionToken == None)
        #    self.assertTrue("sessionToken" in idClient.headers)
        #
        #    # Create already tested in setup for anonymous
        #    # This is not not good, figure out setup/teardown for suites and redo the whole thing using that
        #    
        #    # Read
        #    list = idClient.getRepoEntity('/project')
        #    self.assertEqual(list["totalNumberOfResults"], 1)
        #    list = idClient.getRepoEntity('/dataset')
        #    self.assertEqual(list["totalNumberOfResults"], 3)
        #    list = idClient.getRepoEntity('/layer')
        #    self.assertEqual(list["totalNumberOfResults"], 0)
        #    list = idClient.getRepoEntity('/preview')
        #    self.assertEqual(list["totalNumberOfResults"], 0)
        #    list = idClient.getRepoEntity('/location')
        #    self.assertEqual(list["totalNumberOfResults"], 0)
        #    # Can read user/userGroup until changed
        #    list = idClient.getRepoEntity('/user')
        #    self.assertNotEqual(len(list), 0)
        #    list = idClient.getRepoEntity('/userGroup')
        #    self.assertNotEqual(len(list), 0)
        #    
        #    # Query
        #    list = idClient.repoQuery('select * from project')
        #    self.assertEqual(list["totalNumberOfResults"], 1)
        #    list = idClient.repoQuery('select * from dataset')
        #    self.assertEqual(list["totalNumberOfResults"], 3)
        #    list = idClient.repoQuery('select * from layer')
        #    self.assertEqual(list["totalNumberOfResults"], 0)
        #    list = idClient.repoQuery('select * from layerpreview')
        #    self.assertEqual(list["totalNumberOfResults"], 0)
        #    list = idClient.repoQuery('select * from layerlocation')
        #    self.assertEqual(list["totalNumberOfResults"], 0)
            
        def test_create_parent_and_acl(self):
            pass
            

        #def test_datasetCRUD(self):
        #    dataset = json.loads(IntegrationTestSynapse.DATASET)
        #
        #    # create
        #    createdDataset = self.client.createDataset(dataset)
        #    self.assertEqual(dataset['name'], createdDataset['name'])
        #
        #    # read
        #    storedDataset = self.client.getRepoEntity(createdDataset['uri'])
        #    self.assertTrue(None != storedDataset)
        #
        #    # update
        #    storedDataset['status'] = 'current'
        #    updatedDataset = self.client.updateRepoEntity(storedDataset['uri'], storedDataset)
        #    self.assertEqual('current', updatedDataset['status'])
        #    
        #    # query
        #    datasets = self.client.RepoQuery('select * from dataset')
        #    self.assertTrue(None != datasets)
        #    
        #    # delete
        #    self.client.deleteRepoEntity(createdDataset['uri']);
        #    goneDataset = self.client.getRepoEntity(createdDataset['uri'])
        #    self.assertTrue(None == goneDataset)
    
    unittest.main()
