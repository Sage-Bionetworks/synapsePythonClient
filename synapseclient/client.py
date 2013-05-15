import os
import json
import base64
import urllib
import urlparse
import zipfile
import requests
import os.path
import mimetypes
import stat
import pkg_resources
import webbrowser
import sys

import utils
from synapseclient.version_check import version_check
from synapseclient.utils import id_of, get_properties
from synapseclient.annotations import from_synapse_annotations, to_synapse_annotations
from synapseclient.activity import Activity
from synapseclient.entity import Entity, File, split_entity_namespaces, is_versionable, is_locationable
from synapseclient.evaluation import Evaluation, Submission, SubmissionStatus


__version__=json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']
CACHE_DIR=os.path.join(os.path.expanduser('~'), '.synapseCache', 'python')  #TODO change to /data when storing files as md5
CONFIG_FILE=os.path.join(os.path.expanduser('~'), '.synapseConfig')
FILE_BUFFER_SIZE = 1024


class Synapse:
    """
    Python implementation for Synapse repository service client
    """

    ## set this flag to true to skip version checking on login
    _skip_version_check = False

    def __init__(self, repoEndpoint='https://repo-prod.prod.sagebase.org/repo/v1', 
                 authEndpoint='https://auth-prod.prod.sagebase.org/auth/v1',
                 fileHandleEndpoint='https://file-prod.prod.sagebase.org/file/v1/',
                 serviceTimeoutSeconds=30, debug=False):
        '''Constructor of Synapse client
        params:
        - repoEndpoint: location of synapse repository
        - authEndpoint: location of authentication service
        - serviceTimeoutSeconds : wait time before timeout
        - debug: Boolean weather to print debugging messages.
        '''

        self.cacheDir = os.path.expanduser(CACHE_DIR)
        #create cacheDir if it does not exist
        try:
            os.makedirs(self.cacheDir)
        except OSError as exception:
            if exception.errno != os.errno.EEXIST:
                raise

        ## update endpoints if we get redirected
        resp=requests.get(repoEndpoint, allow_redirects=False)
        if resp.status_code==301:
            repoEndpoint=resp.headers['location']
        resp=requests.get(authEndpoint, allow_redirects=False)
        if resp.status_code==301:
            authEndpoint=resp.headers['location']
        resp=requests.get(fileHandleEndpoint, allow_redirects=False)
        if resp.status_code==301:
            fileHandleEndpoint=resp.headers['location']

        self.headers = {'content-type': 'application/json', 'Accept': 'application/json', 'request_profile':'False'}

        ## TODO serviceTimeoutSeconds is never used. Either use it or delete it.
        self.serviceTimeoutSeconds = serviceTimeoutSeconds 
        self.debug = debug
        self.sessionToken = None

        self.repoEndpoint = repoEndpoint
        self.authEndpoint = authEndpoint
        self.fileHandleEndpoint = fileHandleEndpoint


    def _storeTimingProfile(self, resp):
        """Stores timing information for the last call if request_profile was set."""
        if self.headers.get('request_profile', 'False')=='True':
            profile_data = None
            for k,v in resp.getheaders():
                if k == "profile_response_object":
                    profile_data = v
                    break
            self.profile_data = json.loads(base64.b64decode(profile_data))


    def login(self, email=None, password=None, sessionToken=None):
        """
        Authenticate and get session token by using (in order of preference):
        1) supplied email and password
        2) check for configuraton file
        3) Use already existing session token
        """
        ## check version before logging in
        if not Synapse._skip_version_check: version_check()

        session_file = os.path.join(self.cacheDir, ".session")

        if (email==None or password==None): 
            if sessionToken is not None:
                self.headers["sessionToken"] = sessionToken
                self.sessionToken = sessionToken
                return sessionToken
            else:
                #Try to use config then session token
                try:
                    import ConfigParser
                    config = ConfigParser.ConfigParser()
                    config.read(CONFIG_FILE)
                    email=config.get('authentication', 'username')
                    password=config.get('authentication', 'password')
                except ConfigParser.NoSectionError:
                    #Authentication not defined in config
                    raise Exception("LOGIN FAILED: no username/password provided or found in config file (%s)" % (CONFIG_FILE,))

        # Disable profiling during login and proceed with authentication
        self.headers['request_profile'], orig_request_profile='False', self.headers['request_profile']

        req = {"email":email, "password":password}
        session = self.restPOST('/session', body=json.dumps(req), endpoint=self.authEndpoint)

        self.sessionToken = session["sessionToken"]
        self.headers["sessionToken"] = self.sessionToken

        ## cache session token
        with open(session_file, "w") as f:
            f.write(self.sessionToken)
        os.chmod(session_file, stat.S_IRUSR | stat.S_IWUSR)
        self.headers['request_profile'] = orig_request_profile


    def _loggedIn(self):
        """Test whether the user is logged in to Synapse"""
        if self.sessionToken is None:
            return False
        url = '%s/userProfile' % (self.repoEndpoint,)
        response = requests.get(url, headers=self.headers)
        if response.status_code==401:
            ## bad or expired session token
            return False
        response.raise_for_status()
        user = response.json()
        if 'displayName' in user:
            if user['displayName']=='Anonymous':
                ## no session token, not logged in
                return False
            return user['displayName']
        return False


    def getUserProfile(self, id=None):
        """Get the details about a Synapse user"""
        uri = '/userProfile/%s' % '' if id is None else str(id)
        return self.restGET(uri)


    def onweb(self, entity):
        """Opens up a webbrowser window on the entity page.
        
        Arguments:
        - `entity`: Either an entity or a synapse id
        """
        webbrowser.open("https://synapse.sagebase.org/#Synapse:%s" % id_of(entity))


    def printEntity(self, entity):
        """Pretty prints an entity."""
        if utils.is_synapse_id(entity):
            entity = self._getEntity(entity)
        try:
            print json.dumps(entity, sort_keys=True, indent=2)
        except TypeError:
            print str(entity)



    ############################################################
    # get / store methods
    ############################################################

    def get(self, entity, **kwargs):
        """
        entity: Synapse ID, a Synapse Entity object or a plain dictionary in which 'id' maps to a Synapse ID
        returns: A new Synapse Entity object of the appropriate type

        synapse.get(id, version, downloadFile=True, downloadLocation=None, ifcollision="keep.both", load=False)
        """
        ## optional parameters
        version = kwargs.get('version', None)
        downloadFile = kwargs.get('downloadFile', True)

        local_state = entity.local_state() if isinstance(entity, Entity) else None
        properties = self._getEntity(entity, version=version)
        annotations = self.getAnnotations(properties, version=version)

        ## return a fresh copy of the entity
        entity = Entity.create(properties, annotations, local_state)

        ## for external URLs, we want to retrieve the URL from the fileHandle
        #TODO version, here
        if isinstance(entity, File):
            fh = self._getFileHandle(entity['dataFileHandleId'])
            if fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle':
                entity['externalURL'] = fh['externalURL']
                entity['synapseStore'] = False

                ## if it's a file URL, fill in the path whether downloadFile is True or not,
                if fh['externalURL'].startswith('file:'):
                    entity.update(utils.file_url_to_path(fh['externalURL']))

                ## by default, we download external URLs
                elif downloadFile:
                    entity.update(self._downloadFileEntity(entity))

            ## by default, we download files stored in Synapse
            elif downloadFile:
                entity.update(self._downloadFileEntity(entity))

        #TODO version, here
        if downloadFile and is_locationable(entity):
            entity = self._downloadLocations(entity)

        return entity


    #TODO implement createOrUpdate flag - new entity w/ same name as an existing entity turns into an update
    def store(self, obj, **kwargs):
        """
        create new entity or update an existing entity, uploading any files in the process
        entity: Synapse ID, a Synapse Entity object or a plain dictionary in which 'id' maps to a Synapse ID
        returns: A Synapse Entity object of the appropriate type

        store(entity, used, executed, activityName=None, 
                      activityDescription=None, createOrUpdate=T, forceVersion=T, isRestricted=F)
        store(entity, activity, createOrUpdate=T, forceVersion=T, isRestricted=F)
        """
        createOrUpdate = kwargs.get('createOrUpdate', True)

        #Handle all non entity objects
        if not (isinstance(obj, Entity) or type(obj)==dict):
            classType=obj.__class__
            if 'id' in obj: #If Id present update 
                return classType(**self.restPUT(obj.putURI(), json.dumps(obj)))
            try: #if no id attempt to post the object
                return classType(**self.restPOST(obj.postURI(), json.dumps(obj))) 
            except requests.exceptions.HTTPError as err:
                 #If already present and we want to update attempt to get the object content
                if err.response.status_code==500 and  createOrUpdate: 
                    newObj=self.restGET(obj.getByNameURI(obj.name))
                    newObj.update(obj)
                    obj=classType(**newObj)
                    return classType(**self.restPUT(obj.putURI(), json.dumps(obj)))
                raise

        #If input object is an Entity...
        entity = obj
        properties,annotations,local_state = split_entity_namespaces(entity)

        #TODO: can this be factored out?
        ## need to upload a FileEntity?
        ##   create FileHandle first, then create or update entity
        if entity['entityType'] == File._synapse_entity_type:
            if 'dataFileHandleId' not in properties or True: #TODO: file_cache.local_file_has_changed(entity.path):
                synapseStore = entity.get('synapseStore', None)
                fileHandle = self._uploadToFileHandleService(entity.path, synapseStore=synapseStore)
                properties['dataFileHandleId'] = fileHandle['id']

        ## need to upload a Locationable?
        ##   create the entity first, then upload file, then update entity, later
        if is_locationable(entity):
            ## for Locationables, entity must exist before upload
            if not 'id' in entity:
                properties = self._createEntity(properties)
            ## TODO is this a bug??
            ## TODO is this necessary?
            if 'path' in entity:
                path = annotations.pop('path')
                properties.update(self._uploadFileAsLocation(properties, path))
        #--end--TODO: can this be factored out?

        #TODO deal with access restrictions

        ## create or update entity in synapse
        if 'id' in properties:
            properties = self._updateEntity(properties)
        else:
            properties = self._createEntity(properties)

        annotations['etag'] = properties['etag']

        ## update annotations
        annotations = self.setAnnotations(properties, annotations)
        properties['etag'] = annotations['etag']


        ## if used or executed given, create an Activity object
        activity = kwargs.get('activity', None)
        used = kwargs.get('used', None)
        executed = kwargs.get('executed', None)
        if used or executed:
            if activity is not None:
                raise Exception('Provenance can be specified as an Activity object or as used/executed item(s), but not both.')
            activityName = kwargs.get('activityName', None)
            activityDescription = kwargs.get('activityDescription', None)
            activity = Activity(name=activityName, description=activityDescription, used=used, executed=executed)

        ## if we have an activity, set it as the entity's provenance record
        if activity:
            activity = self.setProvenance(properties, activity)
            ## etag has changed, so get new entity
            properties = self._getEntity(properties)

        ## return a new Entity object
        return Entity.create(properties, annotations, local_state)


    def delete(self, obj):
        """Removes a existing object from Synapse
        
        Arguments:
        - `obj`: An existing object stored on Synapse such as evaluation, File, Project, WikiPage etc.
        """
        if isinstance(obj, basestring): #Handle all strings as entity id for backward compatibility
            self.restDELETE(uri='/entity/'+id_of(obj))
        else:
            self.restDELETE(obj.deleteURI())


    ############################################################
    # older high level methods
    ############################################################

    def getEntity(self, entity, version=None):
        """Retrieves metainformation about an entity from a synapse Repository
        Arguments:
        - `entity`: A synapse ID or dictionary describing an entity
        Returns:
        - A new :class:`synapseclient.entity.Entity` object
        """
        return self.get(entity, version=version, downloadFile=False)


    def loadEntity(self, entity):
        """Downloads and attempts to load the contents of an entity into memory
        TODO: Currently only performs downlaod.
        Arguments:
        r - `entity`: Either a string or dict representing an entity
        """
        #TODO: Try to load the entity into memory as well.
        #This will be depenendent on the type of entity.
        print 'WARNING!: THIS ONLY DOWNLOADS ENTITIES!'
        return self.downloadEntity(entity)


    #TODO factor out common code with store? delegate to store? delegate store to createEntity?
    #TODO should this method upload files?
    def createEntity(self, entity, used=None, executed=None, **kwargs):
        """
        Create a new entity in the synapse Repository according to entity json object.

        entity: an Entity object or dictionary
        used: an entity, a synapse ID, a URL or a Used object or a List containing these
        executed: an entity, a synapse ID, a URL or a Used object or a List containing these        
        """
        ## make sure we're creating a new entity
        if 'id' in entity:
            raise Exception('Called createEntity on an entity with an ID (%s)' % str(id_of(entity)))

        # TODO: delegate to store?
        # return self.store(entity, used=used, executed=executed)
        properties,annotations,local_state = split_entity_namespaces(entity)

        properties = self._createEntity(properties)
        annotations['etag'] = properties['etag']

        ## set annotations
        annotations = self.setAnnotations(properties, annotations)
        properties['etag'] = annotations['etag']

        ## if used or executed given, create an Activity object
        activity = kwargs['activity'] if 'activity' in kwargs else None
        if used or executed:
            if activity is not None:
                raise Exception('Provenance can be specified as an Activity object or as used/executed item(s), but not both.')
            activityName = kwargs['activityName'] if 'activityName' in kwargs else None
            activityDescription = kwargs['activityDescription'] if 'activityDescription' in kwargs else None
            activity = Activity(name=activityName, description=activityDescription, used=used, executed=executed)

        ## if we have an activity, set it as the entity's provenance record
        if activity:
            activity = self.setProvenance(properties, activity)
            ## etag has changed, so get new entity
            properties = self._getEntity(properties)

        ## return a new Entity object
        return Entity.create(properties, annotations, local_state)


    #TODO remove?
    #TODO delegate to store? createEntity?
    def _createFileEntity(self, entity, filename=None, used=None, executed=None):
        #Determine if we want to upload or store the url
        #TODO this should be determined by a parameter not based on magic
        #TODO _createFileEntity and uploadFile are kinda redundant - pick one or fold into createEntity
        if filename is None:
            if 'path' in entity:
                filename = entity.path
            else:
                raise Exception('can\'t create a File entity without a file path or URL')
        if utils.is_url(filename):
            fileHandle = self._addURLtoFileHandleService(filename)
            entity['dataFileHandleId'] = fileHandle['id']
        else:
            fileHandle = self._uploadFileToFileHandleService(filename)
            entity['dataFileHandleId'] = fileHandle['id']
        if 'entityType' not in entity:
            entity['entityType'] = 'org.sagebionetworks.repo.model.FileEntity'
        return self.createEntity(entity, used=used, executed=executed)


    #TODO factor out common code with store? delegate to store? delegate store to updateEntity?
    #TODO should this method upload files?
    def updateEntity(self, entity, used=None, executed=None, incrementVersion=False, versionLabel=None, **kwargs):
        """
        Update an entity stored in synapse with the properties in entity
        """

        if not entity:
            raise Exception("entity cannot be empty")
        if 'id' not in entity:
            raise Exception("A entity without an 'id' can't be updated")

        properties,annotations,local_state = split_entity_namespaces(entity)

        properties = self._updateEntity(properties, incrementVersion, versionLabel)
        annotations['etag'] = properties['etag']

        annotations = self.setAnnotations(properties, annotations)
        properties['etag'] = annotations['etag']

        ## if used or executed given, create an Activity object
        activity = kwargs['activity'] if 'activity' in kwargs else None
        if used or executed:
            if activity is not None:
                raise Exception('Provenance can be specified as an Activity object or as used/executed item(s), but not both.')
            activityName = kwargs['activityName'] if 'activityName' in kwargs else None
            activityDescription = kwargs['activityDescription'] if 'activityDescription' in kwargs else None
            activity = Activity(name=activityName, description=activityDescription, used=used, executed=executed)

        ## if we have an activity, set it as the entity's provenance record
        if activity:
            activity = self.setProvenance(properties, activity)
            ## etag has changed, so get new entity
            properties = self._getEntity(properties)

        ## return a new Entity object
        return Entity.create(properties, annotations, local_state)


    def deleteEntity(self, entity):
        """Deletes a synapse entity"""
        self.delete(entity)


    #TODO: delegate to store?
    def uploadFile(self, entity, filename=None, used=None, executed=None):
        """Upload a file to Synapse"""
        ## if we got a synapse ID as a string, get the entity from synapse
        if isinstance(entity, basestring):
            if not filename:
                raise Exception('No filename specified in call to uploadFile')
            entity = self._getEntity(entity)

        if filename is None:
            if 'path' in entity:
                filename = entity['path']
            else:
                raise Exception('can\'t upload a file without a file path or URL')

        ## default name of entity to be the name of the file
        if 'name' not in entity or entity['name'] is None:
            entity['name'] = os.path.basename(filename)

        ## if we have an old location-able object use the deprecated file upload method
        if is_locationable(entity):
            if not 'id' in entity:
                entity = self._createEntity(entity)
            entity.update(self._uploadFileAsLocation(entity, filename))

        else:
            ## if we haven't specified the entity type, make it a FileEntity
            if 'entityType' not in entity:
                entity['entityType'] = 'org.sagebionetworks.repo.model.FileEntity'

            if entity['entityType'] != 'org.sagebionetworks.repo.model.FileEntity':
                raise Exception('Files can only be uploaded to FileEntity entities')

            synapseStore = entity['synapseStore'] if 'synapseStore' in entity else None
            fileHandle = self._uploadToFileHandleService(filename, synapseStore=synapseStore)

            ## for some reason, posting
            entity['dataFileHandleId'] = fileHandle['id']

        if 'id' in entity:
            return self.updateEntity(entity, used=used, executed=executed)
        else:
            return self.createEntity(entity, used=used, executed=executed)


    def downloadEntity(self, entity, version=None):
        """Download an entity and file(s) associated with it to local cache.
        
        Arguments:
        - `entity`: A synapse ID of entity (i.e dictionary describing an entity)
        Returns:
        - A dictionary representing an entity
        """
        return self.get(entity, version=version, downloadFile=True)



    ############################################################
    # get/set Annotations
    ############################################################

    def getAnnotations(self, entity, version=None):
        """
        Retrieve the annotations stored for an entity in the Synapse Repository
        """
        ## only use versioned URLs on request.
        ## note that using the versioned URL results in a zero-ed out etag,
        ## even if the version is the most recent. See: PLFM-1874
        if version:
            uri = '/entity/%s/version/%s/annotations' % (id_of(entity), str(version),)
        else:
            uri = '/entity/%s/annotations' % id_of(entity)
        return from_synapse_annotations(self.restGET(uri))        


    def setAnnotations(self, entity, annotations={}, **kwargs):
        """
        Store Annotations on an entity in the Synapse Repository.

        Accepts a dictionary, either in the Synapse format or a plain
        dictionary or key/value pairs.
        """
        uri = '/entity/%s/annotations' % id_of(entity)

        ## update annotations with keyword args
        annotations.update(kwargs)

        synapseAnnos = to_synapse_annotations(annotations)
        synapseAnnos['id'] = id_of(entity)
        if 'etag' in entity and 'etag' not in synapseAnnos:
            synapseAnnos['etag'] = entity['etag']

        return from_synapse_annotations(self.restPUT(uri, json.dumps(synapseAnnos) ))



    ############################################################
    # Query
    ############################################################

    def query(self, queryStr):
        '''
        Query for datasets, layers, etc..

        Example:
        query("select id, name from entity where entity.parentId=='syn449742'")
        '''
        if(self.debug): print 'About to query %s' % (queryStr)
        return self.restGET('/query?query=' + urllib.quote(queryStr))



    ############################################################
    # ACL manipulation
    ############################################################

    def _getACL(self, entity):
        ## get benefactor. (An entity gets its ACL from its benefactor.)
        uri = '/entity/%s/benefactor' % id_of(entity)
        benefactor = self.restGET(uri)

        ## get the ACL from the benefactor (which may be the entity itself)
        uri = '/entity/%s/acl' % (benefactor['id'],)        
        return self.restGET(uri)


    def _storeACL(self, entity, acl):
        entity_id = id_of(entity)

        ## get benefactor. (An entity gets its ACL from its benefactor.)
        uri = '/entity/%s/benefactor' % entity_id
        benefactor = self.restGET(uri)

        ## update or create new ACL
        uri = '/entity/%s/acl' % entity_id
        if benefactor['id']==entity_id:
            return self.restPUT(uri, json.dumps(acl))
        else:
            return self.restPOST(uri,json.dumps(acl))


    def getPermissions(self, entity, user=None, group=None):
        """get permissions that a user or group has on an entity"""
        pass


    def setPermissions(self, entity, user=None, group=None):
        """set permission that a user or groups has on an entity"""
        pass



    ############################################################
    # Provenance
    ############################################################

    ## TODO rename these to Activity
    def getProvenance(self, entity, version=None):
        """Retrieve provenance information for a synapse entity. Entity may be
        either an Entity object or a string holding a Synapse ID. Returns
        an Activity object or raises exception if no provenance record exists.

        Note that provenance applies to a specific version of an entity. The
        returned Activity will represent the provenance of the entity version
        supplied with the versionNumber parameter OR if versionNumber is None,
        the versonNumber property of the given entity.
        """

        ## get versionNumber from entity, if it's there
        if version is None and 'versionNumber' in entity:
            version = entity['versionNumber']

        if version:
            uri = '/entity/%s/version/%d/generatedBy' % (id_of(entity), version)
        else:
            uri = '/entity/%s/generatedBy' % id_of(entity)
        return Activity(data=self.restGET(uri))


    def setProvenance(self, entity, activity):
        """assert that the entity was generated by a given activity"""

        if 'id' in activity:
            ## we're updating provenance
            uri = '/activity/%s' % activity['id']
            activity = Activity(data=self.restPUT(uri, json.dumps(activity)))
        else:
            activity = self.restPOST('/activity', body=json.dumps(activity))

        # assert that an entity is generated by an activity
        uri = '/entity/%s/generatedBy?generatedBy=%s' % (id_of(entity), activity['id'])
        activity = Activity(data=self.restPUT(uri))

        return activity


    def deleteProvenance(self, entity):
        """remove provenance information from an entity and delete the activity"""

        activity = self.getProvenance(entity)
        if not activity: return

        uri = '/entity/%s/generatedBy' % id_of(entity)
        self.restDELETE(uri)

        #TODO: what happens if the activity is shared by more than one entity?

        # delete /activity/{activityId}
        uri = '/activity/%s' % activity['id']
        self.restDELETE(uri)


    def updateActivity(self, activity):
        """modify an existing activity"""
        uri = '/activity/%s' % activity['id']
        return Activity(data=self.restPUT(uri, json.dumps(activity)))



    ############################################################
    # locationable upload / download
    ############################################################

    def _uploadFileAsLocation(self, entity, filename):
        """Given an entity or the id of an entity, upload a filename as the location of that entity.

        (deprecated in favor of FileEntities)
        
        Arguments:
        - `entity`:  an entity (dictionary) or Id of entity whose location you want to set 
        - `filename`: Name of file to upload

        Returns:
        A dictionary with locations (a list of length 1) and the md5 of the file.
        """
        # compute hash of file to be uploaded
        md5 = utils.md5_for_file(filename)

        # guess mime-type - important for confirmation of MD5 sum by receiver
        (mimetype, enc) = mimetypes.guess_type(filename)
        if (mimetype is None):
            mimetype = "application/octet-stream"

        # ask synapse for a signed URL for S3 upload
        headers = { 'sessionToken': self.sessionToken,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json' }

        (_, base_filename) = os.path.split(filename)
        data = {'md5':md5.hexdigest(), 'path':base_filename, 'contentType':mimetype}
        uri = '/entity/%s/s3Token' % id_of(entity)
        response_json = self.restPOST(uri, body=json.dumps(data))
        location_path = response_json['path']

        # PUT file to S3
        headers = { 'Content-MD5' : base64.b64encode(md5.digest()),
                    'Content-Type' : mimetype,
                    'x-amz-acl' : 'bucket-owner-full-control' }
        response = requests.put(response_json['presignedUrl'], headers=headers, data=open(filename))
        response.raise_for_status()

        # Add location to entity. Path will get converted to a signed S3 URL.
        locations = [{'path': location_path, 'type': 'awss3'}]

        return {'locations':locations, 'md5':md5.hexdigest()}


    def _downloadLocations(self, entity):
        """
        Download files from Locationable entities.
        Locationable entities contain a signed S3 URL. These URLs expire after a time, so
        the entity object passed to this method must have been recently acquired from Synapse.
        (**deprecated** in favor of FileEntities)
        """
        if 'locations' not in entity or len(entity['locations'])==0:
            return entity
        location = entity['locations'][0]  #TODO verify that this doesn't fail for unattached files
        url = location['path']
        parseResult = urlparse.urlparse(url)
        pathComponents = parseResult.path.split('/')

        filename = os.path.join(self.cacheDir, entity['id'] ,pathComponents[-1])
        if os.path.exists(filename):
            #print filename, "cached"
            md5str = None
            #see if the MD5 has previously been computed, and that the cached MD5 was done after the file was created
            if os.path.exists( filename + ".md5" ) and os.path.getmtime(filename + ".md5") > os.path.getmtime(filename):
                if self.debug: print "Using Cached MD5"
                handle = open(filename + ".md5")
                md5str = handle.readline().rstrip()
                handle.close()
            else:
                md5str = utils.md5_for_file(filename).hexdigest()
                handle = open(filename + ".md5", "w")
                handle.write(md5str)
                handle.close()

            if md5str != entity.get('md5', ''):
                if self.debug: print filename, "changed, redownloading"
                utils.download_file(url, filename)
        else:
            if self.debug: print filename, 'downloading...',
            utils.download_file(url, filename)

        if entity['contentType']=='application/zip':
            ## Unpack file
            filepath=os.path.join(os.path.dirname(filename), os.path.basename(filename)+'_unpacked')
            #TODO!!!FIX THIS TO BE PATH SAFE!  DON'T ALLOW ARBITRARY UNZIPING
            z = zipfile.ZipFile(filename, 'r')
            z.extractall(filepath) #WARNING!!!NOT SAFE
            ## TODO fix - adding entries for 'files' and 'cacheDir' into entities causes an error in updateEntity
            entity['cacheDir'] = filepath
            entity['files'] = z.namelist()
        else:
            ## TODO fix - adding entries for 'files' and 'cacheDir' into entities causes an error in updateEntity
            entity['cacheDir'] = os.path.dirname(filename)
            entity['files'] = [os.path.basename(filename)]
        return entity



    ############################################################
    # File handle service calls
    ############################################################

    def _downloadFileEntity(self, entity):
        """Download the file associated with a FileEntity"""
        url = '%s/entity/%s/file' % (self.repoEndpoint, id_of(entity),)

        destDir = os.path.join(self.cacheDir, id_of(entity))

        #create destDir if it does not exist
        try:
            os.makedirs(destDir, mode=0700)
        except OSError as exception:
            if exception.errno != os.errno.EEXIST:
                raise

        return self._downloadFile(url, destDir)


    def _downloadFile(self, url, destDir):
        """Download a file from a URL to a local destination directory"""

        ## we expect to be redirected to a signed S3 URL
        response = requests.get(url, headers=self.headers, allow_redirects=False)
        if response.status_code in [301,302,303,307,308]:
            url = response.headers['location']

            if self.debug:
                print "_downloadFile: redirect url=", url

            ## if it's a file URL, turn it into a path and return it
            if url.startswith('file:'):
                return utils.file_url_to_path(url)

            headers = {'sessionToken':self.sessionToken}
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
        else:
            response.raise_for_status()

        ##Extract filename from url or header, if it is a Signed S3 URL do...
        if url.startswith('https://s3.amazonaws.com/proddata.sagebase.org'):
            filename = utils.extract_filename(response.headers['content-disposition'])
        else:
            filename = url.split('/')[-1]

        ## stream file to disk
        path = os.path.abspath(os.path.join(destDir, filename))
        with open(path, "wb") as f:
            data = response.raw.read(FILE_BUFFER_SIZE)
            while data:
                f.write(data)
                data = response.raw.read(FILE_BUFFER_SIZE)

        return {
            'path': path,
            'files': [os.path.basename(path)],
            'cacheDir': destDir }




    def _uploadToFileHandleService(self, filename, synapseStore=None):
        """
        Create and return a fileHandle, by either uploading a local file or
        linking to an external URL.

        synapseStore: store file in Synapse or just a URL
        """
        if utils.is_url(filename):
            # if synapseStore==True:
            #     ## download the file locally, then upload it and cache it?
            #     raise Exception('not implemented, yet!')
            # else:
            #     return self._addURLtoFileHandleService(filename)
            return self._addURLtoFileHandleService(filename)
        elif synapseStore==False:
            return self._addURLtoFileHandleService(filename)
        else:
            return self._uploadFileToFileHandleService(filename)

    def _uploadFileToFileHandleService(self, filepath):
        """Upload a file to the new fileHandle service (experimental)
           returns a fileHandle which can be used to create a FileEntity or attach to a wiki"""
        #print "_uploadFileToFileHandleService - filepath = " + str(filepath)
        url = "%s/fileHandle" % (self.fileHandleEndpoint,)
        headers = {'Accept': 'application/json', 'sessionToken': self.sessionToken}
        with open(filepath, 'r') as file:
            response = requests.post(url, files={os.path.basename(filepath): file}, headers=headers)
        response.raise_for_status()

        ## we expect a list of FileHandles of length 1
        fileHandleList = response.json()
        return fileHandleList['list'][0]

    def _addURLtoFileHandleService(self, externalURL):
        """Create a new FileHandle representing an external URL"""
        fileName = externalURL.split('/')[-1]
        externalURL = utils.as_url(externalURL)
        fileHandle={'concreteType':'org.sagebionetworks.repo.model.file.ExternalFileHandle',
                    'fileName': fileName,
                    'externalURL':externalURL}
        (mimetype, enc) = mimetypes.guess_type(externalURL)
        if mimetype:
            fileHandle['contentType'] = mimetype
        return self.restPOST('/externalFileHandle', json.dumps(fileHandle), self.fileHandleEndpoint)

    def _getFileHandle(self, fileHandle):
        """Retrieve a fileHandle from the fileHandle service (experimental)"""
        url = url = "%s/fileHandle/%s" % (self.fileHandleEndpoint, id_of(fileHandle),)
        headers = {'Accept': 'multipart/form-data, application/json', 'sessionToken': self.sessionToken}
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        return response.json()

    def _deleteFileHandle(self, fileHandle):
        url = url = "%s/fileHandle/%s" % (self.fileHandleEndpoint, id_of(fileHandle),)
        headers = {'Accept': 'application/json', 'sessionToken': self.sessionToken}
        response = requests.delete(url, headers=headers, stream=True)
        response.raise_for_status()
        return fileHandle



    ############################################################
    # Summary methods
    ############################################################

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
        
        #TODO: Instead of doing a flatten just by the default hierarchy structure I should be 
        #using an external group-by parameter that determines weather by what property of structure
        # to group by.
        self._flattenTree2Groups(tree)
        self.printEntity(tree)
        self.createEntity({'name': name,
                           "description": description,
                           "entityType": "org.sagebionetworks.repo.model.Summary", 
                           "groups": tree,
                           "parentId": id})



    ############################################################
    # CRUD for Evaluations
    ############################################################

    def getEvaluation(self, id):
        "Returns the evaluation object from synapse"
        evaluation_id = id_of(id)
        uri=Evaluation.getURI(id)
        return Evaluation(**self.restGET(uri))


    def submit(self, evaluation, entity, name=''):
        """submit an Entity for evaluation by evaluator
        
        Arguments:
        - `entity`: The entity containing the submission
        - `evaluation`: Evaluation board to submit to.
        """

        evaluation_id = id_of(evaluation)
        if not 'versionNumber' in entity:
            entity = self.get(entity)    
        entity_version = entity['versionNumber']
        entity_id = entity['id']

        name = entity['name'] if (name == '' and 'name' in entity) else ''
        submission =  {'evaluationId':evaluation_id, 'entityId':entity_id, 'name':name, 
                       'versionNumber': entity_version}
        return Submission(**self.restPOST('/evaluation/submission?etag=%s' %entity.etag, 
                                        json.dumps(submission)))


    def addEvaluationParticipant(self, evaluation, userId=None):
        """Adds a participant to to a evaluation

        Arguments:
        - `evaluation`: an evaluation object or evaluation id
        - `userId`: The prinicipal id of the participant, if not supplied uses your own
        """
        evaluation_id = id_of(evaluation)
        userId=self.getUserProfile()['ownerId'] if userId==None else userId
        self.restPOST('/evaluation/%s/participant/%s'  %(evaluation_id, userId), {})

  

    def getSubmissions(self, evaluation, status=None):
        """Return a generator over all submissions for a evaluation, or optionally all
           submissions with a specified status.

        Arguments:
        - `evaluation`: Evaluation board to get submissions from.
        - `status` :   Get submissions that have specific status one of {OPEN, CLOSED, SCORED, INVALID}

        Example:
        for submission in syn.getEvaluationSubmissions(1234567):
          print submission['entityId']
        """
        evaluation_id = evaluation['id'] if 'id' in evaluation else str(evaluation)

        result_count = 0
        limit = 20
        offset = 0 - limit
        max_results = 1000 ## gets updated later
        submissions = []

        while result_count < max_results:
            ## if we're out of results, do a(nother) REST call
            if result_count >= offset + len(submissions):
                offset += limit
                if status != None:
                    if status not in ['OPEN', 'CLOSED', 'SCORED', 'INVALID']:
                        raise Exception('status may be one of {OPEN, CLOSED, SCORED, INVALID}')
                    uri = "/evaluation/%s/submission/all?status=%s&limit=%d&offset=%d" %(evaluation_id, 
                                                                                         status, limit, 
                                                                                         offset)
                else:
                    uri = "/evaluation/%s/submission/all?limit=%d&offset=%d" %(evaluation_id, limit, 
                                                                               offset)
                page_of_submissions = self.restGET(uri)
                max_results = page_of_submissions['totalNumberOfResults']
                submissions = page_of_submissions['results']

            i = result_count - offset
            result_count += 1
            yield Submission(**submissions[i])


    def getOwnSubmissions(self, evaluation):
        #TODO implement this if this is really usefull?
        pass


    def getSubmission(self, id):
        submission_id = id_of(id)
        uri=SubmissionStatus.getURI(submission_id)
        return Submission(**self.restGET(uri))


    def getSubmissionStatus(self, id):
        """Get the status of a submission
        Arguments:
        - `submission`: Downloads the status of a evaluations submission
        
        """
        submission_id = id_of(id)
        uri=SubmissionStatus.getURI(submission_id)
        val=self.restGET(uri)
        return SubmissionStatus(**val)


            

    ############################################################
    # CRUD for Wikis
    ############################################################

    def _createWiki(self, owner, title, markdown, attachmentFileHandleIds=None, owner_type=None):
        """Create a new wiki page for an Entity (experimental).

        parameters:
        owner -- the owner object (entity, competition, evaluation) with which the new wiki page will be associated
        markdown -- the contents of the wiki page in markdown
        attachmentFileHandleIds -- a list of file handles or file handle IDs
        owner_type -- entity, competition, evaluation, can usually be automatically inferred from the owner object
        """
        if not owner_type:
            owner_type = utils.guess_object_type(owner)
        uri = '/%s/%s/wiki' % (owner_type, id_of(owner),)
        wiki = {'title':title, 'markdown':markdown}
        if attachmentFileHandleIds:
            wiki['attachmentFileHandleIds'] = attachmentFileHandleIds
        return self.restPOST(uri, body=json.dumps(wiki))


    def _getWiki(self, owner, wiki=None, owner_type=None):
        """Get the specified wiki page
        owner -- the owner object (entity, competition, evaluation) or its ID
        wiki -- (optional) the Wiki object or its ID
        owner_type -- (optional) entity, competition, evaluation, can usually be automatically inferred from the owner object
        """
        if not owner_type:
            owner_type = utils.guess_object_type(owner)
        if wiki:
            uri = '/%s/%s/wiki/%s' % (owner_type, id_of(owner), id_of(wiki),)
        else:
            uri = '/%s/%s/wiki' % (owner_type, id_of(owner),)
        return self.restGET(uri)


    def _updateWiki(self, owner, wiki, owner_type=None):
        """Update the specified wiki page
        owner -- the owner object (entity, competition, evaluation) or its ID
        wiki -- the Wiki object or its ID
        owner_type -- entity, competition, evaluation, can usually be automatically inferred from the owner object
        """
        if not owner_type:
            owner_type = utils.guess_object_type(owner)
        uri = '/%s/%s/wiki/%s' % (owner_type, id_of(owner), id_of(wiki),)
        return self.restPUT(uri, json.dumps(wiki))


    def _deleteWiki(self, owner, wiki, owner_type=None):
        """Delete the specified wiki page
        owner -- the owner object (entity, competition, evaluation) or its ID
        wiki -- the Wiki object or its ID
        owner_type -- entity, competition, evaluation, can usually be automatically inferred from the owner object
        """
        if not owner_type:
            owner_type = utils.guess_object_type(owner)
        uri = '/%s/%s/wiki/%s' % (owner_type, id_of(owner), id_of(wiki),)
        self.restDELETE(uri)


    def _getWikiHeaderTree(self, owner, owner_type=None):
        """Get the tree of wiki pages owned by the given object"""
        if not owner_type:
            owner_type = utils.guess_object_type(owner)
        uri = '/%s/%s/wikiheadertree' % (owner_type, id_of(owner),)
        return self.restGET(uri)


    def _downloadWikiAttachment(self, owner, wiki, filename, dest_dir=None, owner_type=None):
        """Download a file attached to a wiki page"""
        if not owner_type:
            owner_type = utils.guess_object_type(owner)
        url = "%s/%s/%s/wiki/%s/attachment?fileName=%s" % (self.repoEndpoint, owner_type, id_of(owner), id_of(wiki), filename,)
        return self._downloadFile(url, dest_dir)



    ############################################################
    # CRUD for Entities (properties)
    ############################################################

    def _getEntity(self, entity, version=None):
        """
        Get an entity from Synapse.
        entity: a Synapse ID, a dictionary representing an entity or a Synapse Entity object
        returns a dictionary representing an entity, specifically it's properties
        """
        uri = '/entity/'+id_of(entity)
        if version:
            uri += '/version/%d' % version
        return self.restGET(uri=uri)

    def _createEntity(self, entity):
        """
        Create a new entity in Synapse.
        entity: a dictionary representing an entity or a Synapse Entity object
        returns a dictionary representing an entity, specifically it's properties
        """
        if self.debug: print "\n\n~~~ creating ~~~\n" + json.dumps(get_properties(entity), indent=2)
        return self.restPOST(uri='/entity', body=json.dumps(get_properties(entity)))

    def _updateEntity(self, entity, incrementVersion=True, versionLabel=None):
        """
        Update an existing entity in Synapse.
        entity: a dictionary representing an entity or a Synapse Entity object
        returns a dictionary representing an entity, specifically it's properties
        """
        uri = '/entity/%s' % id_of(entity)

        if is_versionable(entity):
            if incrementVersion:
                entity['versionNumber'] += 1
                uri += '/version'
                entity['versionLabel'] = str(entity['versionNumber'])
            if versionLabel:
                entity['versionLabel'] = str(versionLabel)

        if self.debug: print "\n\n~~~ updating ~~~\n" + json.dumps(get_properties(entity), indent=2)
        return self.restPUT(uri=uri, body=json.dumps(get_properties(entity)))



    ############################################################
    # Low level Rest calls
    ############################################################
    def restGET(self, uri, endpoint=None, **kwargs):
        """Performs a REST GET operation to the Synapse server.
        
        Arguments:
        - `uri`: URI on which get is performed
        - `endpoint`: Server endpoint, defaults to self.repoEndpoint

        Returns: json encoding of response
        """
        if endpoint==None:
            endpoint=self.repoEndpoint    
        response = requests.get(endpoint+uri, headers=self.headers)
        self._storeTimingProfile(response)
        if self.debug:
            utils.debug_response(response)
        try:
            response.raise_for_status()
        except:
            sys.stderr.write(response.content)
            raise 
        return response.json()
     
    def restPOST(self, uri, body, endpoint=None):
        """Performs a POST request toward the synapse repo
        
        Arguments:
        - `uri`: URI on which get is performed
        - `endpoint`: Server endpoint, defaults to self.repoEndpoint
        - `body`: The payload to be delivered 

        Returns: json encoding of response

        """
        if endpoint==None:
            endpoint=self.repoEndpoint    
        response = requests.post(endpoint + uri, data=body, headers=self.headers)
        if self.debug:
            utils.debug_response(response)
        try:
            response.raise_for_status()
        except:
            sys.stderr.write(response.content)
            raise 
        return response.json()


    def restPUT(self, uri, body=None, endpoint=None):
        """Performs a POST request toward the synapse repo
        
        Arguments:
        - `uri`: URI on which get is performed
        - `endpoint`: Server endpoint, defaults to self.repoEndpoint
        - `body`: The payload to be delivered 

        Returns: json encoding of response 

        """
        if endpoint==None:
            endpoint=self.repoEndpoint    
        response = requests.put(endpoint + uri, data=body, headers=self.headers)
        if self.debug:
            utils.debug_response(response)
        try:
            response.raise_for_status()
        except:
            sys.stderr.write(response.content)
            raise 

        return response.json()


    def restDELETE(self, uri, endpoint=None):
        """Performs a REST DELETE operation to the Synapse server.
        
        Arguments:
        - `uri`: URI of resource to be deleted
        - `endpoint`: Server endpoint, defaults to self.repoEndpoint

        """
        if endpoint==None:
            endpoint=self.repoEndpoint    
        response = requests.delete(endpoint+uri, headers=self.headers)
        if self.debug:
            utils.debug_response(response)
        try:
            response.raise_for_status()
        except:
            sys.stderr.write(response.content)
            raise 

