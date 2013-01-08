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
import stat
from version_check import version_check
import pkg_resources

import utils


__version__=json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']

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
        ## check version before logging in
        version_check()

        session_file = os.path.join(self.cacheDir, ".session")
        if (None==email and None==password and os.path.exists(session_file)):
            with open(session_file) as f:
                sessionToken = f.read().strip()
            self.sessionToken = sessionToken
            self.headers["sessionToken"] = sessionToken
            return sessionToken

        if (None == email or None == password):
            raise Exception("missing Synapse username or password")

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

        ## cache session token
        with open(session_file, "w") as f:
            f.write(self.sessionToken)
        os.chmod(session_file, stat.S_IRUSR | stat.S_IWUSR)

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
        location = entity['locations'][0]  #TODO verify that this doesn't fail for unattached files
        url = location['path']
        parseResult = urlparse.urlparse(url)
        pathComponents = string.split(parseResult.path, '/')

        filename = os.path.join(self.cacheDir,entity['id'] ,pathComponents[-1])
        if os.path.exists(filename):
            print filename, "cached"
            md5 = utils.computeMd5ForFile(filename)
            if md5.hexdigest() != entity['md5']:
                print filename, "changed, redownloading"
                utils.downloadFile(url, filename)
        else:
            print filename, 'downloading...',
            utils.downloadFile(url, filename)

        if entity['contentType']=='application/zip':
            print 'unpacking...'
            ## Unpack file
            filepath=os.path.join(os.path.dirname(filename), os.path.basename(filename)+'_unpacked')
            #TODO!!!FIX THIS TO BE PATH SAFE!  DON'T ALLOW ARBITRARY UNZIPING
            z = zipfile.ZipFile(filename, 'r')
            z.extractall(filepath) #WARNING!!!NOT SAFE
            entity['cacheDir'] = filepath
            entity['files'] = z.namelist()
        else:
            entity['cacheDir'] = os.path.dirname(filename)
            entity['files'] = [os.path.basename(filename)]
        return entity

    def loadEntity(self, entity):
        """Downloads and attempts to load the contents of an entity into memory
        TODO: Currently only performs downlaod.
        Arguments:
r        - `entity`: Either a string or dict representing and entity
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
                raise Exception('POST %s failed: %d %s %s %s' % (uri, resp.status, resp.reason, json.dumps(entity), output), resp.status, resp.reason)
        finally:
            conn.close()
        return storedEntity


    def createEntity(self, entity):
        """Create a new entity in the synapse Repository according to entity json object"""
        endpoint=self.repoEndpoint
        uri = endpoint["prefix"] + '/entity'
        return self._createUniversalEntity(uri, entity, endpoint)
        
        
    def updateEntity(self, entity):
        """
        Update an entity stored in synapse with the properties in entity

        This convenience method first grabs a copy of the currently
        stored entity, then overwrites fields from the entity passed
        in on top of the stored entity we retrieved and then PUTs the
        entity. This essentially does a partial update from the point
        of view of the user of this API.
        
        """
        return self.putEntity(self.repoEndpoint, entity['uri'], entity)


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


    def deleteEntity(self, entity, endpoint=None):
        """Deletes an entity (dataset, layer, user, ...) on either service"""
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
        print id, count, name
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

    def createSnapshotSummary(self, id, name='summary', description=None, groupBy=None ):
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
        
        #TODO: Insteaad of doing a flatten just by the default heirarchy structure I should be 
        #using an external groupby parameter that determines weather by what property of structure
        # to group by.
        self._flattenTree2Groups(tree)
        self.printEntity(tree)
        self.createEntity({'name': name,
                           "description": description,
                           "entityType": "org.sagebionetworks.repo.model.Summary", 
                           "groups": tree,
                           "parentId": id})



    def uploadFile(self, entity, filename, endpoint=None):
        """Given an entity or the id of an entity, upload a filename as the location of that entity.
        
        Arguments:
        - `entity`:  an entity (dictionary) or Id of entity whose location you want to set 
        - `filename`: Name of file to upload
        """

        # check parameters
        if entity is None or not (isinstance(entity, basestring) or (isinstance(entity, dict) and entity.has_key('id'))):
           raise Exception('invalid entity parameter')
        if isinstance(entity, basestring):
            entity = self.getEntity(entity)
        if endpoint == None:
            endpoint = self.repoEndpoint

        # compute hash of file to be uploaded
        md5 = utils.computeMd5ForFile(filename)

        # guess mime-type - important for confirmation of MD5 sum by receiver
        (mimetype, enc) = mimetypes.guess_type(filename)
        if (mimetype is None):
            mimetype = "application/octet-stream"

        # ask synapse for a signed URL for S3 upload
        headers = { 'sessionToken': self.sessionToken,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json' }

        url = '%s://%s%s/entity/%s/s3Token' % (
            self.repoEndpoint['protocol'],
            self.repoEndpoint['location'],
            self.repoEndpoint['prefix'],
            entity['id'])

        (_, base_filename) = os.path.split(filename)
        data = {'md5':md5.hexdigest(), 'path':base_filename, 'contentType':mimetype}

        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()

        response_json = response.json()
        location_path = response_json['path']
        # PUT file to S3
        headers = { 'Content-MD5': base64.b64encode(md5.digest()),
                  'Content-Type' : mimetype,
                  'x-amz-acl' : 'bucket-owner-full-control' }
        response = requests.put(response_json['presignedUrl'], headers=headers, data=open(filename))
        response.raise_for_status()

        # add location to entity
        entity['locations'] = [{
        'path': location_path,
        'type': 'awss3'
        }]
        entity['md5'] = md5.hexdigest()

        return self.putEntity(self.repoEndpoint, entity['uri'], entity)





        



