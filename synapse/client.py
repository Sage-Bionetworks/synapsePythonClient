#!/usr/bin/env python2.7
# To debug this, python -m pdb myscript.py
import os
import string
import json
import base64
import urllib
import urlparse
import httplib
import time
import zipfile
import requests
import os.path
import mimetypes

import utils

CACHE_DIR='~/.synapseCache/python'  #TODO: this needs to be handled in a transparent way!

class Synapse:
    """
    Python implementation for Synapse repository service client
    """    
    def __init__(self, repoEndpoint='https://repo-prod.sagebase.org/repo/v1', 
                 authEndpoint='https://auth-prod.sagebase.org/auth/v1', 
                 serviceTimeoutSeconds=30, debug=False):
        '''Constructor of Synapse client
        params:
        - repoEndpoint: location of synapse repository
        - authEndpoint: location of authentication service
        - serviceTimeoutSeconds : wait time before timeout
        - debug: Boolean weather to print debugging messages.
        '''

        self.cacheDir = os.path.expanduser(CACHE_DIR)
        self.headers = {'content-type': 'application/json', 'Accept': 'application/json'}

        self.serviceTimeoutSeconds = serviceTimeoutSeconds 
        self.debug = debug
        self.sessionToken = None

        self.repoEndpoint = {}
        self.authEndpoint = {}

        parseResult = urlparse.urlparse(repoEndpoint)
        self.repoEndpoint["location"] = parseResult.netloc
        self.repoEndpoint["prefix"] = parseResult.path
        self.repoEndpoint["protocol"] = parseResult.scheme
        
        parseResult = urlparse.urlparse(authEndpoint)
        self.authEndpoint["location"] = parseResult.netloc
        self.authEndpoint["prefix"] = parseResult.path
        self.authEndpoint["protocol"] = parseResult.scheme
        
        self.request_profile = False
        self.profile_data = None


    def _connect(self, endpoint):
        """Creates a http connection to the desired endpoint

        Arguments:
        - `endpoint`: dictionary of endpoint details, like self.repoEndpoint
        Returns:
        - `conn`: Either a httplib.HTTPConnection or httplib.HTTPSConnection
        """
        conn = {}
        if('https' == endpoint["protocol"]):
            conn = httplib.HTTPSConnection(endpoint["location"],
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(endpoint["location"],
                                          timeout=self.serviceTimeoutSeconds)
        if (self.debug):
            conn.set_debuglevel(10);
        return conn


    def printEntity(self, entity):
        """Pretty prints an entity."""
        print json.dumps(entity, sort_keys=True, indent=2)


    def login(self, email, password):
        """
        Authenticate and get session token
        """
        if (None == email or None == password):
            raise Exception("invalid parameters")

        # Disable profiling during login
        # TODO: Check what happens if enabled
        req_profile = None
        if self.request_profile != None:
            req_profile = self.request_profile
            self.request_profile = False
        
        uri = self.authEndpoint["prefix"] + "/session"
        req = {"email":email, "password":password}
        
        storedEntity = self._createUniversalEntity(uri, req, self.authEndpoint)
        self.sessionToken = storedEntity["sessionToken"]
        self.headers["sessionToken"] = self.sessionToken
    
        if req_profile != None:
            self.request_profile = req_profile


    def getEntity(self, entity, endpoint=None):
        """Retrieves metainformation about an entity from a synapse Repository
        Arguments:
        - `entity`: A synapse ID of entity (i.e dictionary describing an entity)
        Returns:
        - A dictionary representing an entitity
        """
        #process input arguments
        if endpoint == None:
            endpoint = self.repoEndpoint
        if not (isinstance(entity, basestring) or (isinstance(entity, dict) and entity.has_key('id'))):
            raise Exception("invalid parameters")
        if isinstance(entity, dict):
            entity = entity['id']

        if not (endpoint['prefix'] in entity):
            uri = endpoint["prefix"] + '/entity/' + entity
        else:
            uri=entity
        conn = self._connect(endpoint)

        if(self.debug):  print 'About to get %s' % (uri)

        entity = None
        self.profile_data = None
        try:
            headers = self.headers
            if self.request_profile:
                headers["profile_request"] = "True"
            conn.request('GET', uri, None, headers)
            resp = conn.getresponse()
            if self.request_profile:
                profile_data = None
                for k,v in resp.getheaders():
                    if k == "profile_response_object":
                        profile_data = v
                        break
                self.profile_data = json.loads(base64.b64decode(profile_data))
            output = resp.read()
            if resp.status == 200:
                if self.debug:
                    print output
                entity = json.loads(output)
            else:
                raise Exception('GET %s failed: %d %s %s' % (uri, resp.status, resp.reason, output))
        finally:
            conn.close()
        return entity


    def downloadEntity(self, entity):
        """Download an entity and files associated with an entity to local cache
        TODO: Add storing of files in cache
        TODO: Add unpacking of files.
        
        Arguments:
        - `entity`: A synapse ID of entity (i.e dictionary describing an entity)
        Returns:
        - A dictionary representing an entitity
        """
        entity = self.getEntity(entity)
        if not entity.has_key('locations'):
            return entity
        locations = entity['locations']

        for location in locations:  #TODO: verify, can a entity have more than 1 location?
            url = location['path']
            parseResult = urlparse.urlparse(url)
            pathComponents = string.split(parseResult.path, '/')

            filename = os.path.join(self.cacheDir,entity['id'] ,pathComponents[-1])
            print filename, 'downloaded and unpacked'
            utils.downloadFile(url, filename)

            ## Unpack file
            filepath=os.path.join(os.path.dirname(filename), os.path.basename(filename)+'_unpacked')
            #TODO!!!FIX THIS TO BE PATH SAFE!  DON'T ALLOW ARBITRARY UNZIPING
            z = zipfile.ZipFile(filename, 'r')
            z.extractall(filepath) #WARNING!!!NOT SAFE

            #figure out files stored in there and return references to them
            entity['cacheDir'] = filepath
            entity['files'] = z.namelist()
        return entity

    def loadEntity(self, entity):
        """Downloads and attempts to load the contents of an entity into memory
        TODO: Currently only performs downlaod.
        Arguments:
        - `entity`: Either a string or dict representing and entity
        """
        #TODO: Try to load the entity into memory as well.
        #This will be depenendent on the type of entity.
        print 'WARNING!: THIS ONLY DOWNLOADS ENTITIES!'
        return self.downloadEntity(entity)


    def _createUniversalEntity(self, uri, entity, endpoint):
        """Creates an entity on either auth or repo"""

        if(None == uri or None == entity or None == endpoint
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")
 
        conn = self._connect(endpoint)

        if(self.debug): print 'About to create %s with %s' % (uri, json.dumps(entity))

        storedEntity = None
        self.profile_data = None
        
        try:
            headers = self.headers
            if self.request_profile:
                headers["profile_request"] = "True"
            conn.request('POST', uri, json.dumps(entity), headers)
            resp = conn.getresponse()
            if self.request_profile:
                profile_data = None
                for k,v in resp.getheaders():
                    if k == "profile_response_object":
                        profile_data = v
                        break
                self.profile_data = json.loads(base64.b64decode(profile_data))              
            output = resp.read()
            if resp.status == 201:
                if self.debug:
                    print output
                storedEntity = json.loads(output)
            else:
                raise Exception('POST %s failed: %d %s %s %s' % (uri, resp.status, resp.reason, json.dumps(entity), output))
        finally:
            conn.close()
        return storedEntity


    def createEntity(self, entity):
        """Create a new entity in the synapse Repository according to entity json object"""
        endpoint=self.repoEndpoint
        uri = endpoint["prefix"] + '/entity'
        return self._createUniversalEntity(uri, entity, endpoint)
        
        
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

        TODO: Verify functionality
        '''
        if(None == uri or None == entity or None == endpoint
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")

        oldEntity = self.getEntity(uri, endpoint)
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
        


    def query(self, queryStr):
        '''
        Query for datasets, layers, etc..

        Example:
        query("select id, name from entity where entity.parentId=='syn449742'")
        '''
        uri = self.repoEndpoint["prefix"] + '/query?query=' + urllib.quote(queryStr)
        conn = self._connect(self.repoEndpoint)

        if(self.debug): print 'About to query %s' % (queryStr)

        results = None
        self.profile_data = None

        try:
            headers = self.headers
            if self.request_profile:
                headers["profile_request"] = "True"
            conn.request('GET', uri, None, headers)
            resp = conn.getresponse()
            if self.request_profile:
                profile_data = None
                for k,v in resp.getheaders():
                    if k == "profile_response_object":
                        profile_data = v
                        break
                self.profile_data = json.loads(base64.b64decode(profile_data))
            output = resp.read()
            if resp.status == 200:
                if self.debug:
                    print output
                results = json.loads(output)
            else:
                raise Exception('Query %s failed: %d %s %s' % (queryStr, resp.status, resp.reason, output))
        finally:
            conn.close()
        return results


    def deleteEntity(self, entity):
        """Deletes an entity (dataset, layer, user, ...) on either service"""
        endpoint = self.repoEndpoint
        uri = endpoint["prefix"] + '/entity/' + entity
        conn = self._connect(endpoint)

        if (self.debug): print 'About to delete %s' % (uri)

        self.profile_data = None

        try:
            headers = self.headers
            if self.request_profile:
                headers["profile_request"] = "True"
            conn.request('DELETE', uri, None, headers)
            resp = conn.getresponse()
            if self.request_profile:
                profile_data = None
                for k,v in resp.getheaders():
                    if k == "profile_response_object":
                        profile_data = v
                        break
                self.profile_data = json.loads(base64.b64decode(profile_data))
            output = resp.read()
            if resp.status != 204:
                raise Exception('DELETE %s failed: %d %s %s' % (uri, resp.status, resp.reason, output))
            elif self.debug:
                print output

            return None;
        finally:
            conn.close()


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

        conn = self._connect(endpoint)

        if(self.debug): print 'About to update %s with %s' % (uri, json.dumps(entity))

        putHeaders = self.headers
        if "etag" in entity:
            putHeaders['ETag'] = entity['etag']

        storedEntity = None
        self.profile_data = None

        try:
            conn.request('PUT', uri, json.dumps(entity), putHeaders)
            resp = conn.getresponse()
            if self.request_profile:
                profile_data = None
                for k,v in resp.getheaders():
                    if k == "profile_response_object":
                        profile_data = v
                        break
                self.profile_data = json.loads(base64.b64decode(profile_data))
            output = resp.read()
            # Handle both 200 and 204 as auth returns 204 for success
            if resp.status == 200 or resp.status == 204:
                if self.debug:
                    print output
                storedEntity = json.loads(output)
            else:
                raise Exception('PUT %s failed: %d %s %s %s' % (uri, resp.status, resp.reason, json.dumps(entity), output))
        finally:
            conn.close()
        return storedEntity


    def _traverseTree(self, id, name=None, version=None):
        """Creates a tree of all 
        
        Arguments:
        - `id`:
        """
        children =  self.query("select id, versionNumber, name from entity where entity.parentId=='%s'" %id)
        count=children['totalNumberOfResults']
        children=children['results']
        output=[]
        if count>0:
            output.append({'name':name, 'targetVersionNumber':version, 'targetId':id, 'records':[]})
            for ent in children:
                output[-1]['records'].extend(self._traverseTree(ent['entity.id'], ent['entity.name'], ent['entity.versionNumber']))
        else:
            output.append({'targetId':id, 'targetVersionNumber':version})
        return output

    def _flattenTree2Groups(self,tree, level=0, out=[]):
        """Converts a complete tree to 2 levels corresponding to json schema of summary
        
        Arguments:
        - `tree`: json object representing entity organizion as output from _traverseTree
        """
        if level==0:  #Move direct entities to subgroup "Content"
            #I am so sorry!  This is incredibly inefficient but I had no time to think through it.
            contents = [group for group in tree if not group.has_key('records')]
            tree.append({'name':'Content', 'records':contents, 'targetId':'', 'targetVersionNumber':''})
            for i in sorted([i for i, group in enumerate(tree) if not group.has_key('records')], reverse=True):
                tree.pop(i)

            #tree=[group for i, group in enumerate(tree) if i not in contents]
            self.printEntity(tree)
            print "============================================"
        
        for i, group in enumerate(tree):
            if group.has_key('records'): #Means that it has subrecords
                self._flattenTree2Groups(group['records'], level+1, out)
            else:
                out.append({'entityReference':group})
            if level==0:
                del group['targetId']
                del group['targetVersionNumber']
                group['records']=out
                out=list()

    def createSnapshotSummary(self, id, name='summary', description=None, ):
        """Given the id of an entity traverses all subentities and creates a summary object within
        same entity as id.
        
        Arguments:
        - `id`:  Id of entity to traverse to create entity 
        - `name`: Name of created summary entity
        - `description`: Description of created entity.
        """
        print "hello"
        tree=self._traverseTree(id)[0]['records']
        print self.printEntity(tree)
        
        self._flattenTree2Groups(tree)
        self.printEntity(tree)
        self.createEntity({'name': name,
                           "description": description,
                           "entityType": "org.sagebionetworks.repo.model.Summary", 
                           "groups": tree,
                           "name": "Test_summary", 
                           "parentId": id})

    def uploadFile(self, entity, filename, endpoint=None):
        """Given an entity or the id of an entity, upload a filename as the location of that entity.
        
        Arguments:
        - `entity`:  an entity (dictionary) or Id of entity whose location you want to set 
        - `filename`: Name of file to upload
        """

        print(entity)

        # check parameters
        if entity is None or not (isinstance(entity, basestring) or (isinstance(entity, dict) and entity.has_key('id'))):
           raise Exception("invalid entity parameter")
        if isinstance(entity, basestring):
            entity = self.getEntity(entity)
        if endpoint == None:
            endpoint = self.repoEndpoint


        # compute hash of file to be uploaded
        md5 = utils.computeMd5ForFile(filename)

        print("computed md5: %s, or in base64: %s" % (md5.hexdigest(), base64.b64encode(md5.digest())) )

        # ask synapse for a signed URL for S3 upload
        headers = { "sessionToken": self.sessionToken,
                    "Content-Type": "application/json",
                    "Accept": "application/json" }

        url = "%s://%s%s/entity/%s/s3Token" % (
            self.repoEndpoint['protocol'],
            self.repoEndpoint['location'],
            self.repoEndpoint['prefix'],
            entity['id'])

        (_, base_filename) = os.path.split(filename)
        data = {"md5":md5.hexdigest(), "path":base_filename}

        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()

        location_path = response.json['path']

        #print("got signed URL for S3 upload: " + location_path)

        (mimetype, enc) = mimetypes.guess_type(filename)

        # PUT file to S3
        headers = { "Content-MD5": base64.b64encode(md5.digest()),
                  "Content-Type" : mimetype,
                  "x-amz-acl" : "bucket-owner-full-control" }

        response = requests.put(response.json['presignedUrl'], headers=headers, data=open(filename))
        #print(response)
        response.raise_for_status()

        # todo: error checking?

        # todo: how to update a location that already exists? handle versioning?
        #       for now, we just overwrite the existing location. The old thing
        #       will still be in an S3 bucket. Should we delete it??

        # add location to entity
        entity['locations'] = [{
        'path': location_path,
        'type': 'awss3'
        }]
        entity['md5'] = md5.hexdigest()

        return self.putEntity(self.repoEndpoint, entity['uri'], entity)


    # def monitorDaemonStatus(self, id):
    #     '''
    #     Continously monitor daemon status until it completes
    #     '''
    #     status = None
    #     complete = False
    #     pbar = None
    #     while (not complete):            
    #         status = self.checkDaemonStatus(id)
    #         complete = status["status"] != "STARTED"
    #         if (not complete):
    #             time.sleep(15) #in seconds  
    #     if (pbar):
    #         pbar.finish()  
    #     return status

    # def checkDaemonStatus(self, id):
    #     '''
    #     Make a single call to check daemon status
    #     '''
        
    #     conn = self._connect(self.repoEndpoint)

            
    #     uri = self.repoEndpoint["prefix"] + "/daemonStatus/" + str(id)
    #     results = None
        
    #     try:
    #         headers = self.headers            
    #         if self.request_profile:
    #             headers["profile_request"] = "True"
    #         conn.request('GET', uri, None, headers)
    #         resp = conn.getresponse()
    #         if self.request_profile:
    #             profile_data = None
    #             for k,v in resp.getheaders():
    #                 if k == "profile_response_object":
    #                     profile_data = v
    #                     break
    #             self.profile_data = json.loads(base64.b64decode(profile_data))
    #         output = resp.read()
    #         if resp.status == 200:
    #             if self.debug:
    #                 print output
    #             results = json.loads(output)
    #         else:
    #             raise Exception('Call failed: %d %s %s' % (resp.status, resp.reason, output))
    #     finally:
    #         conn.close()
    #     return results   
        
    # def startBackup(self):
    #     conn = self._connect(self.repoEndpoint)
    #     if (self.debug): print 'Starting backup of repository'
        
    #     uri = self.repoEndpoint["prefix"] + "/startBackupDaemon"
    #     results = None
        
    #     try:
    #         headers = self.headers            
    #         if self.request_profile:
    #             headers["profile_request"] = "True"
    #         conn.request('POST', uri, "{}", headers)
    #         resp = conn.getresponse()
    #         if self.request_profile:
    #             profile_data = None
    #             for k,v in resp.getheaders():
    #                 if k == "profile_response_object":
    #                     profile_data = v
    #                     break
    #             self.profile_data = json.loads(base64.b64decode(profile_data))
    #         output = resp.read()
    #         if resp.status == 201:
    #             if self.debug:
    #                 print output
    #             results = json.loads(output)
    #         else:
    #             raise Exception('Call failed: %d %s %s' % (resp.status, resp.reason, output))
    #     finally:
    #         conn.close()
    #     return results    

    # def startRestore(self, backupFile):
    #     conn = self._connect(self.repoEndpoint)

    #     if (self.debug): print 'Starting restore of repository'
        
    #     uri = self.repoEndpoint["prefix"] + "/startRestoreDaemon"
    #     results = None
        
    #     try:
    #         headers = self.headers            
    #         if self.request_profile:
    #             headers["profile_request"] = "True"
    #         body = "{\"url\": \""+backupFile+"\"}"
    #         conn.request('POST', uri, body, headers)
    #         resp = conn.getresponse()
    #         if self.request_profile:
    #             profile_data = None
    #             for k,v in resp.getheaders():
    #                 if k == "profile_response_object":
    #                     profile_data = v
    #                     break
    #             self.profile_data = json.loads(base64.b64decode(profile_data))
    #         output = resp.read()
    #         if resp.status == 201:
    #             if self.debug:
    #                 print output
    #             results = json.loads(output)
    #         else:
    #             raise Exception('Call failed: %d %s %s' % (resp.status, resp.reason, output))
    #     finally:
    #         conn.close()
    #     return results    


    # def getRepoEntityByName(self, kind, name, parentId=None):
    #     """
    #     Get an entity (dataset, layer, ...) from repository service using its name and optionally parentId
    #     """
    #     return self.getRepoEntityByProperty(kind, "name", name, parentId)


        



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
