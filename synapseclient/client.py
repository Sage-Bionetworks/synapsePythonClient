"""
******
Client
******

.. autoclass:: synapseclient.Synapse
   :members:
   
~~~~~~~~~~~~~~~~
More information
~~~~~~~~~~~~~~~~

`Synapse API's <https://sagebionetworks.jira.com/wiki/display/PLFM/Synapse+REST+APIs>`_

"""

import ConfigParser
import collections
import os, sys, stat, re, json, time
import os.path
import base64, hashlib, hmac
import urllib, urlparse, requests, webbrowser
import zipfile
import mimetypes
import pkg_resources

import synapseclient.utils as utils
import synapseclient.cache as cache
import synapseclient.exceptions as exceptions
from synapseclient.exceptions import *
from synapseclient.version_check import version_check
from synapseclient.utils import id_of, get_properties, KB, MB
from synapseclient.annotations import from_synapse_annotations, to_synapse_annotations
from synapseclient.activity import Activity
from synapseclient.entity import Entity, File, Project, split_entity_namespaces, is_versionable, is_locationable
from synapseclient.evaluation import Evaluation, Submission, SubmissionStatus
from synapseclient.retry import RetryRequest
from synapseclient.wiki import Wiki


PRODUCTION_ENDPOINTS = {'repoEndpoint':'https://repo-prod.prod.sagebase.org/repo/v1',
                        'authEndpoint':'https://auth-prod.prod.sagebase.org/auth/v1',
                        'fileHandleEndpoint':'https://file-prod.prod.sagebase.org/file/v1', 
                        'portalEndpoint':'https://synapse.org/'}

STAGING_ENDPOINTS    = {'repoEndpoint':'https://repo-staging.prod.sagebase.org/repo/v1',
                        'authEndpoint':'https://auth-staging.prod.sagebase.org/auth/v1',
                        'fileHandleEndpoint':'https://file-staging.prod.sagebase.org/file/v1', 
                        'portalEndpoint':'https://staging.synapse.org/'}

__version__ = json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']
CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.synapseConfig')
SESSION_FILENAME = '.session'
FILE_BUFFER_SIZE = 4*KB
CHUNK_SIZE = 5*MB
QUERY_LIMIT = 5000
CHUNK_UPLOAD_POLL_INTERVAL = 1 # second
ROOT_ENTITY = 'syn4489'
DEBUG_DEFAULT = False


## defines the standard retry policy applied to the rest methods
STANDARD_RETRY_REQUEST = RetryRequest(retry_status_codes=[502,503],
                                      retry_errors=[],
                                      retry_exceptions=['Timeout', 'timeout'],
                                      retries=3, wait=1, back_off=2, verbose=False)

## add additional mimetypes
mimetypes.add_type('text/x-r', '.R', strict=False)
mimetypes.add_type('text/x-r', '.r', strict=False)


class Synapse:
    """
    Constructs a Python client object for the Synapse repository service
    
    :param repoEndpoint:          Location of Synapse repository
    :param authEndpoint:          Location of authentication service
    :param fileHandleEndpoint:    Location of file service
    :param portalEndpoint:        Location of the website
    :param serviceTimeoutSeconds: Wait time before timeout (currently unused) 
    :param debug:                 Print debugging messages if True
    :param skip_checks:           Skip version and endpoint checks
    
    Typically, no parameters are needed::
    
        import synapseclient
        syn = synapseclient.Synapse()
        
    See: 
    
    - :py:func:`synapseclient.Synapse.login`
    - :py:func:`synapseclient.Synapse.setEndpoints`
    """

    def __init__(self, repoEndpoint=None, authEndpoint=None, fileHandleEndpoint=None, portalEndpoint=None, 
                 debug=DEBUG_DEFAULT, skip_checks=False):
        # Check for a config file
        if os.path.isfile(CONFIG_FILE):
            config = self.getConfigFile()
            
            if config.has_option('cache', 'location'):
                cache.CACHE_DIR = config.get('cache', 'location')
        else: 
            # Alert the user if no config is found
            print "Could not find a config file (%s).  Using defaults." % os.path.abspath(CONFIG_FILE)
            
            
        # Create the cache directory if it does not exist
        self.cacheDir = cache.CACHE_DIR
        try:
            os.makedirs(self.cacheDir)
        except OSError as exception:
            if exception.errno != os.errno.EEXIST:
                raise

                
        self.setEndpoints(repoEndpoint, authEndpoint, fileHandleEndpoint, portalEndpoint, skip_checks)
        
        ## TODO: rename to defaultHeaders ?
        self.headers = {'content-type': 'application/json', 'Accept': 'application/json', 'request_profile':'False'}
        self.username = None
        self.apiKey = None
        self.debug = debug
        self.skip_checks = skip_checks
        
    
    def getConfigFile(self):
        """Returns a ConfigParser populated with properties from the user's configuration file."""
        
        try:
            config = ConfigParser.ConfigParser()
            config.read(CONFIG_FILE) # Does not fail if the file does not exist
            return config
        except ConfigParser.Error:
            sys.stderr.write('Error parsing Synapse config file: %s' % CONFIG_FILE)
            raise
        
    
    def setEndpoints(self, repoEndpoint=None, authEndpoint=None, fileHandleEndpoint=None, portalEndpoint=None, skip_checks=False):
        """
        Sets the locations for each of the Synapse services (mostly useful for testing).

        :param repoEndpoint:          Location of synapse repository
        :param authEndpoint:          Location of authentication service
        :param fileHandleEndpoint:    Location of file service
        :param portalEndpoint:        Location of the website
        :param skip_checks:           Skip version and endpoint checks
        
        To switch between staging and production endpoints::
            
            syn.setEndpoints(**synapseclient.client.STAGING_ENDPOINTS)
            syn.setEndpoints(**synapseclient.client.PRODUCTION_ENDPOINTS)
            
        """
        
        endpoints = {'repoEndpoint'       : repoEndpoint, 
                     'authEndpoint'       : authEndpoint, 
                     'fileHandleEndpoint' : fileHandleEndpoint, 
                     'portalEndpoint'     : portalEndpoint}
        
        # For unspecified endpoints, first look in the config file
        config = self.getConfigFile()
        for point in endpoints.keys():
            if endpoints[point] is None and config.has_option('endpoints', point):
                endpoints[point] = config.get('endpoints', point)

        # Endpoints default to production
        for point in endpoints.keys():
            if endpoints[point] is None:
                endpoints[point] = PRODUCTION_ENDPOINTS[point]

            # Update endpoints if we get redirected
            if not skip_checks:
                response = requests.get(endpoints[point], allow_redirects=False)
                if response.status_code == 301:
                    endpoints[point] = response.headers['location']

        self.repoEndpoint       = endpoints['repoEndpoint']
        self.authEndpoint       = endpoints['authEndpoint']
        self.fileHandleEndpoint = endpoints['fileHandleEndpoint']
        self.portalEndpoint     = endpoints['portalEndpoint']


    def _storeTimingProfile(self, resp):
        """Stores timing information for the last call if request_profile was set."""
        
        ## TODO: Only used in .restGET().  Should the other rest methods use this too?
        if self.headers.get('request_profile', 'False') == 'True':
            profile_data = None
            for k,v in resp.getheaders():
                if k == "profile_response_object":
                    profile_data = v
                    break
            self.profile_data = json.loads(base64.b64decode(profile_data))


    def login(self, email=None, password=None, apiKey=None, sessionToken=None, rememberMe=False, silent=False):
        """
        Authenticates the user using the given credentials, in order of preference:
        
        1) supplied email and password
        2) supplied email and API key (base 64 encoded)
        3) supplied session token
        4) supplied email and cached API key
        5) email in the configuration file and cached API key
        6) email and API key in the configuration file
        7) email and password in the configuraton file
        8) session token in the configuration file
        
        :param apiKey:     Base64 encoded
        :param rememberMe: Whether the authentication information should be cached locally
                           for usage across sessions and clients.
        :param silent:     Defaults to False.  Suppresses the "Welcome ...!" message.
        """
        # Note: the order of the logic below reflects the ordering in the docstring above.

        # Check version before logging in
        if not self.skip_checks: version_check()
        
        # Make sure to invalidate the existing session
        self.logout(local=True, clearCache=False)

        if email is not None and password is not None:
            self.username = email
            sessionToken = self._getSessionToken(email=self.username, password=password)
            self.apiKey = self._getAPIKey(sessionToken)
            
        elif email is not None and apiKey is not None:
            self.username = email
            self.apiKey = base64.b64decode(apiKey)
        
        elif sessionToken is not None:
            try:
                self._getSessionToken(sessionToken=sessionToken)
                self.username = self.getUserProfile(sessionToken=sessionToken)['userName']
                self.apiKey = self._getAPIKey(sessionToken)
            except SynapseAuthenticationError: 
                # Session token is invalid
                pass
            
        # If supplied arguments are not enough
        # Try fetching the information from the API key cache
        if self.apiKey is None:
            cachedSessions = self._readSessionCache()
            if email is not None and email in cachedSessions:
                self.username = email
                self.apiKey = base64.b64decode(cachedSessions[email])
        
            # Resort to reading the configuration file
            if self.apiKey is None:
                # Resort to checking the config file
                config = ConfigParser.ConfigParser()
                try:
                    config.read(CONFIG_FILE)
                except ConfigParser.Error:
                    sys.stderr.write('Error parsing Synapse config file: %s' % CONFIG_FILE)
                    raise
                    
                if config.has_option('authentication', 'username'):
                    self.username = config.has_option('authentication', 'username')
                    if self.username in cachedSessions:
                        self.apiKey = base64.b64decode(cachedSessions[self.username])
                
                # Just use the configuration file
                if self.apiKey is None:
                    if config.has_option('authentication', 'username') and config.has_option('authentication', 'apikey'):
                        self.username = config.get('authentication', 'username')
                        self.apiKey = base64.b64decode(config.get('authentication', 'apikey'))
                        
                    elif config.has_option('authentication', 'username') and config.has_option('authentication', 'password'):
                        self.username = config.get('authentication', 'username')
                        password = config.get('authentication', 'password')
                        token = self._getSessionToken(email=self.username, password=password)
                        self.apiKey = self._getAPIKey(token)
                        
                    elif config.has_option('authentication', 'sessiontoken'):
                        sessionToken = config.get('authentication', 'sessiontoken')
                        try:
                            self._getSessionToken(sessionToken=sessionToken)
                            self.username = self.getUserProfile(sessionToken=sessionToken)['userName']
                            self.apiKey = self._getAPIKey(sessionToken)
                        except SynapseAuthenticationError:
                            raise SynapseAuthenticationError("No credentials provided.  Note: the session token within your configuration file has expired.")
        
        # Final check on login success
        if self.username is not None and self.apiKey is None:
            raise SynapseAuthenticationError("No credentials provided.")
            
        # Save the API key in the cache
        if rememberMe:
            cachedSessions = self._readSessionCache()
            cachedSessions[self.username] = base64.b64encode(self.apiKey)
            self._writeSessionCache(cachedSessions)
            
        if not silent:
            profile = self.getUserProfile()
            print "Welcome, %s!" % (profile['displayName'] if 'displayName' in profile else self.username)
        
        
    def _getSessionToken(self, email=None, password=None, sessionToken=None):
        """Returns a validated session token."""
        if email is not None and password is not None:
            # Login normally
            try:
                req = {'email' : email, 'password' : password}
                session = self.restPOST('/session', body=json.dumps(req), endpoint=self.authEndpoint, headers=self.headers)
                return session['sessionToken']
            except SynapseHTTPError as err:
                if err.response.status_code == 400:
                    raise SynapseAuthenticationError("Invalid username or password.")
                raise
                    
        elif sessionToken is not None:
            # Validate the session token
            try:
                token = {'sessionToken' : sessionToken}
                response = self.restPUT('/session', body=json.dumps(token), endpoint=self.authEndpoint, headers=self.headers)
                
                # Success!
                return sessionToken
                
            except SynapseHTTPError as err:
                if err.response.status_code == 404:
                    raise SynapseAuthenticationError("Supplied session token (%s) is invalid." % sessionToken)
                raise
        else:
            raise SynapseAuthenticationError("No credentials provided.")
            
    def _getAPIKey(self, sessionToken):
        """Uses a session token to fetch an API key."""
        
        headers = {'sessionToken' : sessionToken, 'Accept': 'application/json'}
        secret = self.restGET('/secretKey', endpoint=self.authEndpoint, headers=headers)
        return base64.b64decode(secret['secretKey'])
        
    
    def _readSessionCache(self):
        """Returns the JSON contents of CACHE_DIR/SESSION_FILENAME."""
        
        sessionFile = os.path.join(cache.CACHE_DIR, SESSION_FILENAME)
        if os.path.isfile(sessionFile):
            try:
                file = open(sessionFile, 'r')
                return json.load(file)
            except: pass
        return {}
        
        
    def _writeSessionCache(self, data):
        """Dumps the JSON data into CACHE_DIR/SESSION_FILENAME."""
        
        sessionFile = os.path.join(cache.CACHE_DIR, SESSION_FILENAME)
        with open(sessionFile, 'w') as file:
            json.dump(data, file)
    

    def _loggedIn(self):
        """Test whether the user is logged in to Synapse."""
        
        if self.apiKey is None:
            return False
            
        try:
            user = restGET('/userProfile')
            if 'displayName' in user:
                if user['displayName'] == 'Anonymous':
                    # No session token, not logged in
                    return False
                return user['displayName']
        except SynapseHTTPError as err:
            if err.response.status_code == 401:
                return False
            raise
        
        
    def logout(self, local=False, clearCache=False):
        """
        Invalidates authentication.
        
        :param local:      Set as True to logout locally, otherwise all sessions are logged out.
        :param clearCache: Set as True to clear any local storage of authentication information.
                           See the flag "rememberMe" in :py:func:`synapseclient.Synapse.login`.
        """

        # Logout globally
        if not local: self.restDELETE('/secretKey', endpoint=self.authEndpoint)
        
        # Delete the user's API key from the cache
        if not local or clearCache:
            cachedSessions = self._readSessionCache()
            if self.username in cachedSessions:
                del cachedSessions[self.username]
                self._writeSessionCache(cachedSessions)
            
        # Remove the authentication information from memory
        self.username = None
        self.apiKey = None


    def getUserProfile(self, id=None, sessionToken=None):
        """
        Get the details about a Synapse user.  
        Retrieves information on the current user if 'id' is omitted.
        
        :param id:           The 'ownerId' of a user
        :param sessionToken: The session token to use to find the user profile
        
        :returns: JSON-object
        """
        
        uri = '/userProfile/%s' % ('' if id is None else str(id))
        if sessionToken is None:
            return self.restGET(uri)
        return self.restGET(uri, headers={'sessionToken' : sessionToken})


    def onweb(self, entity, subpageId=None):
        """
        Opens up a browser window to the entity page or wiki-subpage.
        
        :param entity:    Either an Entity or a Synapse ID
        :param subpageId: (Optional) ID of one of the wiki's sub-pages
        """
        
        if subpageId is None:
            webbrowser.open("%s#!Synapse:%s" % (self.portalEndpoint, id_of(entity)))
        else: 
            webbrowser.open("%s#!Wiki:%s/ENTITY/%s" % (self.portalEndpoint, id_of(entity), subpageId))


    def printEntity(self, entity):
        """Pretty prints an Entity."""
        
        if utils.is_synapse_id(entity):
            entity = self._getEntity(entity)
        try:
            print json.dumps(entity, sort_keys=True, indent=2)
        except TypeError:
            print str(entity)



    ############################################################
    ##                  Get / Store methods                   ##
    ############################################################

    def get(self, entity, **kwargs):
        """
        Gets a Synapse entity from the repository service.
        
        :param entity:           A Synapse ID, a Synapse Entity object, 
                                 or a plain dictionary in which 'id' maps to a Synapse ID
        :param version:          The specific version to get.
                                 Defaults to the most recent version.
        :param downloadFile:     Whether associated files(s) should be downloaded.  
                                 Defaults to True
        :param downloadLocation: Directory where to download the Synapse File Entity.
                                 Defaults to the local cache.
        :param ifcollision:      Determines how to handle file collisions.
                                 May be "overwrite.local", "keep.local", or "keep.both".
                                 Defaults to "keep.both".

        :returns: A new Synapse Entity object of the appropriate type
        """
        
        version = kwargs.get('version', None)
        return self._getWithEntityBundle(entity, entityBundle=self._getEntityBundle(entity, version), **kwargs)
        
    def _getWithEntityBundle(self, entity, **kwargs):
        """
        Gets a Synapse entity from the repository service.
        See :py:func:`synapseclient.Synapse.get`.
        
        :param entityBundle: Uses the given dictionary as the meta information of the Entity to get
        :param submission:   Makes the method treats the entityBundle like it came from a Submission 
                             and thereby needs a different URL to download
        """
        
        # Note: This version overrides the version of 'entity' (if the object is Mappable)
        version = kwargs.get('version', None)
        downloadFile = kwargs.get('downloadFile', True)
        downloadLocation = kwargs.get('downloadLocation', None)
        ifcollision = kwargs.get('ifcollision', 'keep.both')
        submission = kwargs.get('submission', None)
        
        # Make sure the download location is fully resolved
        downloadLocation = None if downloadLocation is None else os.path.expanduser(downloadLocation)
        if downloadLocation is not None and os.path.isfile(downloadLocation):
            raise ValueError("Parameter 'downloadLocation' should be a directory, not a file.")

        # Retrieve metadata
        bundle = kwargs.get('entityBundle', None)
        if bundle is None:
            raise SynapseMalformedEntityError("Could not determine the Synapse ID to fetch")
            
        # Make a fresh copy of the Entity
        local_state = entity.local_state() if isinstance(entity, Entity) else None
        properties = bundle['entity']
        annotations = from_synapse_annotations(bundle['annotations'])
        entity = Entity.create(properties, annotations, local_state)

        # Handle both FileEntities and Locationables
        isLocationable = is_locationable(entity)
        if isinstance(entity, File) or isLocationable:
            fileName = entity['name']
            
            if not isLocationable:
                # Fill in some information about the file
                # Note: fileHandles will be empty if there are unmet access requirements
                for handle in bundle['fileHandles']:
                    if handle['id'] == bundle['entity']['dataFileHandleId']:
                        entity.md5 = handle.get('contentMd5', '')
                        entity.fileSize = handle.get('contentSize', None)
                        fileName = handle['fileName']
                        if handle['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle':
                            entity['externalURL'] = handle['externalURL']
                            entity['synapseStore'] = False
        
            # Determine if the file should be downloaded
            downloadPath = None if downloadLocation is None else os.path.join(downloadLocation, fileName)
            if downloadFile: 
                downloadFile = cache.local_file_has_changed(bundle, True, downloadPath)
                
            # Determine where the file should be downloaded to
            if downloadFile:
                _, localPath, _ = cache.determine_local_file_location(bundle)
                
                # By default, download to the local cache
                if downloadPath is None:
                    downloadPath = localPath
                    
                # If there's no file to download, don't download
                if downloadPath is None:
                    downloadFile = False
                    
                # If the file already exists...
                elif os.path.exists(downloadPath):
                    if ifcollision == "overwrite.local":
                        pass
                        
                    elif ifcollision == "keep.local":
                        downloadFile = False
                        
                    elif ifcollision == "keep.both":
                        downloadPath = cache.get_alternate_file_name(downloadPath)
                        
                    else:
                        raise ValueError('Invalid parameter: "%s" is not a valid value for "ifcollision"' % ifcollision)

            if downloadFile:
                if isLocationable:
                    ## TODO: version, here
                    entity = self._downloadLocations(entity, downloadPath)
                else:
                    entity.update(self._downloadFileEntity(entity, downloadPath, submission))
            else:
                # The local state of the Entity is normally updated by the _downloadFileEntity method
                # Use the EntityBundle to fill in the path information
                entity.update(cache.retrieve_local_file_info(bundle, downloadPath))
                
                # If the file was not downloaded and does not exist, set the synapseStore flag appropriately
                if not isLocationable and 'path' in entity and not os.path.exists(entity['path']):
                    entity['synapseStore'] = False
                    
            # Send the Entity's dictionary to the update the file cache
            if 'path' in entity.keys():
                cache.add_local_file_to_cache(**entity)
            elif 'path' in entity:
                cache.add_local_file_to_cache(path=entity['path'], **entity)
            elif downloadPath is not None:
                cache.add_local_file_to_cache(path=downloadPath, **entity)

        return entity


    def store(self, obj, **kwargs):
        """
        Creates a new Entity or updates an existing Entity, 
        uploading any files in the process.

        :param obj:                 A Synapse Entity, Evaluation, or Wiki
        :param used:                The Entity, Synapse ID, or URL 
                                    used to create the object
        :param executed:            The Entity, Synapse ID, or URL 
                                    representing code executed to create the object
        :param activity:            Activity object specifying the user's provenance
        :param activityName:        TODO_Sphinx. 
        :param activityDescription: TODO_Sphinx. 
        :param createOrUpdate:      Indicates whether the method should automatically perform an update if the 'obj' conflicts with an existing Synapse object.  Defaults to True. 
        :param forceVersion:        Indicates whether the method should increment the version of the object even if nothing has changed.  Defaults to True.
        :param versionLabel:        Arbitrary string used to label the version.  

        :returns: A Synapse Entity, Evaluation, or Wiki
        """
        
        createOrUpdate = kwargs.get('createOrUpdate', True)
        forceVersion = kwargs.get('forceVersion', True)
        versionLabel = kwargs.get('versionLabel', None)
        isRestricted = kwargs.get('isRestricted', False)

        # Handle all non-Entity objects
        if not (isinstance(obj, Entity) or type(obj) == dict):
            if 'id' in obj: # If ID is present, update 
                obj.update(self.restPUT(obj.putURI(), obj.json()))
                return obj
                
            try: # If no ID is present, attempt to POST the object
                obj.update(self.restPOST(obj.postURI(), obj.json()))
                return obj
                
            except SynapseHTTPError as err:
                # If already present and we want to update attempt to get the object content
                if createOrUpdate and err.response.status_code == 409:
                    newObj = self.restGET(obj.getByNameURI(obj.name))
                    newObj.update(obj)
                    obj = obj.__class__(**newObj)
                    obj.update(self.restPUT(obj.putURI(), obj.json()))
                    return obj
                raise
        
        # If the input object is an Entity or a dictionary
        entity = obj
        properties, annotations, local_state = split_entity_namespaces(entity)
        isLocationable = is_locationable(properties)

        # Anything with a path is treated as a cache-able item (FileEntity or Locationable)
        if entity.get('path', False):
            if 'concreteType' not in properties:
                properties['concreteType'] = File._synapse_entity_type
                
            # Make sure the path is fully resolved
            entity['path'] = os.path.expanduser(entity['path'])
            
            # Check if the File already exists in Synapse by fetching metadata on it
            bundle = self._getEntityBundle(entity)
                    
            # Check if the file should be uploaded
            if bundle is None or cache.local_file_has_changed(bundle, False, entity['path']):
                if isLocationable:
                    # Entity must exist before upload for Locationables
                    if 'id' not in properties: 
                        properties = self._createEntity(properties)
                    annotations.pop('path')
                    properties.update(self._uploadFileAsLocation(properties, entity['path']))
                
                    # A file has been uploaded, so version should not be incremented if possible
                    forceVersion = False
                    
                else:
                    fileHandle = self._uploadToFileHandleService(entity['path'], \
                                            synapseStore=entity.get('synapseStore', True))
                    properties['dataFileHandleId'] = fileHandle['id']
                
                    # A file has been uploaded, so version must be updated
                    forceVersion = True
                    
                # The cache expects a path, but FileEntities and Locationables do not have the path in their properties
                cache.add_local_file_to_cache(path=entity['path'], **properties)
                    
            elif 'dataFileHandleId' not in properties:
                # Handle the case where the Entity lacks an ID 
                # But becomes an update() due to conflict
                properties['dataFileHandleId'] = bundle['entity']['dataFileHandleId']

        ## TODO: deal with access restrictions

        # Create or update Entity in Synapse
        if 'id' in properties:
            properties = self._updateEntity(properties, forceVersion, versionLabel)
        else:
            try:
                properties = self._createEntity(properties)
            except SynapseHTTPError as ex:
                if createOrUpdate and ex.response.status_code == 409:
                    existing_entity_id = None
                    
                    ## TODO: no longer necessary?
                    # The conflict is between projects
                    if properties['concreteType'] == Project._synapse_entity_type:
                        properties['parentId'] = ROOT_ENTITY
                        
                        # Below is a roundabout, but not-hard-coded way to find the ROOT_ENTITY
                        # errText = ex.response.json()['reason']
                        # properties['parentId'] = re.findall('syn\d+', errText)[-1]
                        
                    # Get the existing Entity's ID via the name and parent
                    existing_entity_id = self._findEntityIdByNameAndParent(properties['name'], properties.get('parentId', None))
                    if existing_entity_id is None: raise

                    # Update the conflicting Entity
                    existing_entity = self._getEntity(existing_entity_id)
                    # Need some fields from the existing entity: id, etag, and version info.
                    existing_entity.update(properties)
                    properties = self._updateEntity(existing_entity, forceVersion, versionLabel)
                else:
                    raise


        # Update annotations
        annotations['etag'] = properties['etag']
        annotations = self.setAnnotations(properties, annotations)
        properties['etag'] = annotations['etag']

        # If the parameters 'used' or 'executed' are given, create an Activity object
        activity = kwargs.get('activity', None)
        used = kwargs.get('used', None)
        executed = kwargs.get('executed', None)
        
        if used or executed:
            if activity is not None:
                ## TODO: move this argument check closer to the front of the method
                raise SynapseProvenanceError('Provenance can be specified as an Activity object or as used/executed item(s), but not both.')
            activityName = kwargs.get('activityName', None)
            activityDescription = kwargs.get('activityDescription', None)
            activity = Activity(name=activityName, description=activityDescription, used=used, executed=executed)

        # If we have an Activity, set it as the Entity's provenance record
        if activity:
            activity = self.setProvenance(properties, activity)
            
            # 'etag' has changed, so get the new Entity
            properties = self._getEntity(properties)

        # Return the updated Entity object
        return Entity.create(properties, annotations, local_state)

    
    ## TODO: add bitFlag parameter?
    def _getEntityBundle(self, entity, version=None):
        """
        Gets some information about the Entity.
        
        :returns: An EntityBundle with the Entity header, annotations, unmet access requirements, and file handles
        """
        
        # EntityBundle bit-flags (see the Java class org.sagebionetworks.repo.model.EntityBundle)
        # ENTITY                    = 0x1
        # ANNOTATIONS               = 0x2
        # PERMISSIONS               = 0x4
        # ENTITY_PATH               = 0x8
        # ENTITY_REFERENCEDBY       = 0x10
        # HAS_CHILDREN              = 0x20
        # ACL                       = 0x40
        # ACCESS_REQUIREMENTS       = 0x200
        # UNMET_ACCESS_REQUIREMENTS = 0x400
        # FILE_HANDLES              = 0x800
        bitFlags = 0x800 | 0x400 | 0x2 | 0x1;

        # If 'entity' is given without an ID, try to find it by 'parentId' and 'name'.
        # Use case:
        #     If the user forgets to catch the return value of a syn.store(e)
        #     this allows them to recover by doing: e = syn.get(e)
        if isinstance(entity, collections.Mapping) and 'id' not in entity and 'name' in entity:
            entity = self._findEntityIdByNameAndParent(entity['name'], entity.get('parentId',ROOT_ENTITY))
        
        # Avoid an exception from finding an ID from a NoneType
        try: id_of(entity) 
        except SynapseMalformedEntityError:
            return None
        
        if version is not None:
            uri = '/entity/%s/version/%d/bundle?mask=%d' %(id_of(entity), version, bitFlags)
        else:
            uri = '/entity/%s/bundle?mask=%d' %(id_of(entity), bitFlags)
        bundle = self.restGET(uri)
        
        # Check and warn for unmet access requirements
        if len(bundle['unmetAccessRequirements']) > 0:
            sys.stderr.write("\nWARNING: This entity has access restrictions. Please visit the web page for this entity (syn.onweb(\"%s\")). Click the downward pointing arrow next to the file's name to review and fulfill its download requirement(s).\n" % id_of(entity))
        
        return bundle

    def delete(self, obj):
        """
        Removes an object from Synapse.
        
        :param obj: An existing object stored on Synapse 
                    such as Evaluation, File, Project, WikiPage etc
        """
        
        # Handle all strings as the Entity ID for backward compatibility
        if isinstance(obj, basestring):
            self.restDELETE(uri='/entity/%s' % id_of(obj))
        else:
            self.restDELETE(obj.deleteURI())


            
    ############################################################
    ##                   Deprecated methods                   ##
    ############################################################

    def getEntity(self, entity, version=None):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.get`
        """

        return self.get(entity, version=version, downloadFile=False)


    def loadEntity(self, entity):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.get`
        """

        print 'WARNING!: THIS ONLY DOWNLOADS ENTITIES!'
        return self.downloadEntity(entity)


    def createEntity(self, entity, used=None, executed=None, **kwargs):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.store`
        """

        return self.store(entity, used=used, executed=executed, **kwargs)


    ## TODO: This code is never used (except in a test). Remove?
    def _createFileEntity(self, entity, filename=None, used=None, executed=None):
        """Determine if we want to upload or store the URL."""
        ## TODO: this should be determined by a parameter not based on magic
        ## TODO: _createFileEntity() and uploadFile() are kinda redundant - pick one or fold into store()
        
        if filename is None:
            if 'path' in entity:
                filename = entity.path
            else:
                raise SynapseMalformedEntityError('can\'t create a File entity without a file path or URL')
        if utils.is_url(filename):
            fileHandle = self._addURLtoFileHandleService(filename)
            entity['dataFileHandleId'] = fileHandle['id']
        else:
            fileHandle = self._chunkedUploadFile(filename)
            entity['dataFileHandleId'] = fileHandle['id']
        if 'concreteType' not in entity:
            entity['concreteType'] = 'org.sagebionetworks.repo.model.FileEntity'
        return self.createEntity(entity, used=used, executed=executed)


    def updateEntity(self, entity, used=None, executed=None, incrementVersion=False, versionLabel=None, **kwargs):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.store`
        """

        return self.store(entity, used=used, executed=executed, forceVersion=incrementVersion, versionLabel=versionLabel, **kwargs)


    def deleteEntity(self, entity):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.delete`
        """
        self.delete(entity)


    def uploadFile(self, entity, filename=None, used=None, executed=None):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.store`
        """
        
        if filename is not None:
            entity['path'] = filename
        if 'name' not in entity or entity['name'] is None:
            entity['name'] = os.path.basename(filename)
            
        return self.store(entity, used=used, executed=executed)


    def downloadEntity(self, entity, version=None):
        """
        **Deprecated**
        
        Use :py:func:`synapseclient.Synapse.get`
        """
        
        return self.get(entity, version=version, downloadFile=True)


        
    ############################################################
    ##                 Get / Set Annotations                  ##
    ############################################################

    def getAnnotations(self, entity, version=None):
        """
        Retrieve annotations for an Entity in the Synapse Repository.
        
        :param entity:  An Entity or Synapse ID to lookup
        :param version: The version of the Entity to retrieve.  
        
        :returns: A dictionary
        """
        
        # Note: Specifying the version results in a zero-ed out etag, 
        # even if the version is the most recent. 
        # See `PLFM-1874 <https://sagebionetworks.jira.com/browse/PLFM-1874>`_ for more details.
        if version:
            uri = '/entity/%s/version/%s/annotations' % (id_of(entity), str(version))
        else:
            uri = '/entity/%s/annotations' % id_of(entity)
        return from_synapse_annotations(self.restGET(uri))


    def setAnnotations(self, entity, annotations={}, **kwargs):
        """
        Store annotations for an Entity in the Synapse Repository.

        :param entity:      An Entity or Synapse ID to update annotations of
        :param annotations: A dictionary in Synapse format or a Python format
        :param kwargs:      Any additional entries to be added to the annotations dictionary
        
        :returns: A dictionary
        """
        
        uri = '/entity/%s/annotations' % id_of(entity)

        annotations.update(kwargs)
        synapseAnnos = to_synapse_annotations(annotations)
        synapseAnnos['id'] = id_of(entity)
        if 'etag' in entity and 'etag' not in synapseAnnos:
            synapseAnnos['etag'] = entity['etag']

        return from_synapse_annotations(self.restPUT(uri, body=json.dumps(synapseAnnos)))


        
    ############################################################
    ##                        Querying                        ##
    ############################################################

    def query(self, queryStr):
        """
        Query for Synapse entities.  
        **To be replaced** with :py:func:`synapseclient.Synapse.chunkedQuery` in the future.
        See `in-depth documentation <https://sagebionetworks.jira.com/wiki/display/PLFM/Repository+Service+API#RepositoryServiceAPI-QueryAPI>`_.
        
        :returns: A JSON object containing an array of query results

        Example::
        
            syn.query("select id, name from entity where entity.parentId=='syn449742'")
        """
        
        if(self.debug): print 'About to query %s' % (queryStr)
        return self.restGET('/query?query=' + urllib.quote(queryStr))
        
        
    def chunkedQuery(self, queryStr):
        """
        Query for Synapse Entities.  
        More robust than :py:func:`synapseclient.Synapse.query`.
        See `in-depth documentation <https://sagebionetworks.jira.com/wiki/display/PLFM/Repository+Service+API#RepositoryServiceAPI-QueryAPI>`_.
        
        :returns: An iterator that will break up large queries into managable pieces.  
        
        Example::
        
            results = syn.query("select id, name from entity where entity.parentId=='syn449742'")
            for res in results:
                print res['entity.id']
        
        """
        
        # The query terms LIMIT and OFFSET are managed by this method
        # So any user specified limits and offsets must be removed first
        #   Note: The limit and offset terms are always placed at the end of a query
        #   Note: The server does not parse the limit and offset terms if the offset occurs first.
        #         This parsing enforces the correct order so the user does not have to consider it.  
        
        # Regex a lower-case string to simplify matching
        tempQueryStr = queryStr.lower() 
        regex = '\A(.*\s)(offset|limit)\s*(\d*\s*)\Z'
        
        # Continue to strip off and save the last limit/offset
        match = re.search(regex, tempQueryStr)
        options = {'limit':None, 'offset':None}
        while match is not None:
            options[match.group(2)] = match.group(3)
            tempQueryStr = match.group(1);
            match = re.search(regex, tempQueryStr)
            
        # Parse the stripped off values or default them to no limit and no offset
        options['limit'] = int(options['limit']) if options['limit'] is not None else float('inf')
        options['offset'] = int(options['offset']) if options['offset'] is not None else 1
        
        # Get a truncated version of the original query string (not in lower-case)
        queryStr = queryStr[:len(tempQueryStr)]
            
        # Continue querying until the entire query has been fetched (or crash out)
        limit = options['limit'] if options['limit'] < QUERY_LIMIT else QUERY_LIMIT
        offset = options['offset']
        while True:
            # Build the sub-query
            remaining = options['limit'] + options['offset'] - offset + 1
            subqueryStr = "%s limit %d offset %d" %(queryStr, limit if limit < remaining else remaining, offset)
            if(self.debug): print 'About to query: %s' % (subqueryStr)
            try: 
                response = self.restGET('/query?query=' + urllib.quote(subqueryStr))
                for res in response['results']:
                    yield res
                    
                # Increase the size of the limit slowly 
                if limit < QUERY_LIMIT / 2: 
                    limit = int(limit * 1.5 + 1)
                    
                # Exit when no more results can be pulled
                if len(response['results']) > 0:
                    offset += len(response['results'])
                else:
                    break
                    
                # Exit when all requests results have been pulled
                if offset > options['offset'] + options['limit']:
                    break
            except SynapseHTTPError as err:
                # Shrink the query size when appropriate
                ## TODO: Change the error check when PLFM-1990 is resolved
                if err.response.status_code == 400 and err.response.json()['reason'].startswith('java.lang.IllegalArgumentException: The results of this query exceeded the maximum'):
                    if (limit == 1):
                        raise SynapseMalformedEntityError("A single row (offset %s) of this query exceeds the maximum size.  Consider limiting the columns returned in the select clause." % offset)
                    limit /= 2
                else:
                    raise
                  
                  
    def md5Query(self, md5):
        """
        Find the Entities with attached file(s) with the given MD5 hash.
        
        :param md5: The MD5 to query for (hexadecimal string)
        
        :returns: A list of Entity headers
        """
        
        return self.restGET('/entity/md5/%s' % md5)['results']
        


    ############################################################
    ##                    ACL manipulation                    ##
    ############################################################

    def _getBenefactor(self, entity):
        """An Entity gets its ACL from its benefactor."""
        
        return self.restGET('/entity/%s/benefactor' % id_of(entity))

    def _getACL(self, entity):
        """TODO_Sphinx."""
        
        # Get the ACL from the benefactor (which may be the entity itself)
        benefactor = self._getBenefactor(entity)
        uri = '/entity/%s/acl' % (benefactor['id'])
        return self.restGET(uri)


    def _storeACL(self, entity, acl):
        """TODO_Sphinx."""
        
        # Get benefactor. (An entity gets its ACL from its benefactor.)
        entity_id = id_of(entity)
        uri = '/entity/%s/benefactor' % entity_id
        benefactor = self.restGET(uri)

        # Update or create new ACL
        uri = '/entity/%s/acl' % entity_id
        if benefactor['id']==entity_id:
            return self.restPUT(uri, json.dumps(acl))
        else:
            return self.restPOST(uri,json.dumps(acl))


    def getPermissions(self, entity, principalId):
        """
        Get the permissions that a user or group has on an Entity.

        :param entity:      An Entity or Synapse ID to lookup
        :param principalId: TODO_Sphinx
        
        :returns: An array containing some combination of 
                  ['READ', 'CREATE', 'UPDATE', 'DELETE', 'CHANGE_PERMISSIONS']
                  or an empty array
        """

        ## TODO: look up user by email?
        ## TODO: what if user has permissions by membership in a group?
        acl = self._getACL(entity)
        for permissions in acl['resourceAccess']:
            if 'principalId' in permissions and permissions['principalId'] == int(principalId):
                return permissions['accessType']
        return []


    def setPermissions(self, entity, principalId, accessType=['READ'], modify_benefactor=False, warn_if_inherits=True):
        """
        Sets permission that a user or group has on an Entity.
        An Entity may have its own ACL or inherit its ACL from a benefactor.  

        :param entity:            An Entity or Synapse ID to modify
        :param principalId:       TODO_Sphinx
        :param accessType:        TODO_Sphinx
        :param modify_benefactor: Set as True when modifying a benefactor's ACL
        :param warn_if_inherits:  Set as False, when creating a new ACL. 
                                  Trying to modify the ACL of an Entity that 
                                  inherits its ACL will result in a warning
        
        :returns: TODO_Sphinx
        """

        benefactor = self._getBenefactor(entity)

        if benefactor['id'] != id_of(entity):
            if modify_benefactor:
                entity = benefactor
            elif warn_if_inherits:
                sys.stderr.write(utils.normalize_whitespace(
                    '''Warning: Creating an ACL for entity %s, which formerly inherited
                       access control from a benefactor entity, "%s" (%s).''' % 
                       (id_of(entity), benefactor['name'], benefactor['id'],))+'\n')

        principalId = int(principalId)

        acl = self._getACL(entity)

        # Find existing permissions
        existing_permissions = None
        for permissions in acl['resourceAccess']:
            if 'principalId' in permissions and permissions['principalId'] == principalId:
                existing_permissions = permissions
        if existing_permissions:
            existing_permissions['accessType'] = accessType
        else:
            acl['resourceAccess'].append({u'accessType': accessType, u'principalId': principalId})
        acl = self._storeACL(entity, acl)

        return acl



    ############################################################
    ##                       Provenance                       ##
    ############################################################

    ## TODO: rename these to Activity
    def getProvenance(self, entity, version=None):
        """
        Retrieve provenance information for a Synapse Entity.
        
        :param entity:  An Entity or Synapse ID to lookup
        :param version: The version of the Entity to retrieve.  
                        Gets the most recent version if omitted
        
        :returns: An Activity object or 
                  raises exception if no provenance record exists
        """

        # Get versionNumber from Entity
        if version is None and 'versionNumber' in entity:
            version = entity['versionNumber']

        if version:
            uri = '/entity/%s/version/%d/generatedBy' % (id_of(entity), version)
        else:
            uri = '/entity/%s/generatedBy' % id_of(entity)
        return Activity(data=self.restGET(uri))


    def setProvenance(self, entity, activity):
        """
        TODO_Sphinx
        
        :param entity:   An Entity or Synapse ID to modify
        :param activity: TODO_Sphinx
        
        :returns: An updated Activity object
        """
        
        # Assert that the entity was generated by a given Activity.
        if 'id' in activity:
            # We're updating provenance
            uri = '/activity/%s' % activity['id']
            activity = Activity(data=self.restPUT(uri, json.dumps(activity)))
        else:
            activity = self.restPOST('/activity', body=json.dumps(activity))

        # assert that an entity is generated by an activity
        uri = '/entity/%s/generatedBy?generatedBy=%s' % (id_of(entity), activity['id'])
        activity = Activity(data=self.restPUT(uri))

        return activity


    def deleteProvenance(self, entity):
        """
        Removes provenance information from an Entity 
        and deletes the associated Activity.
        
        :param entity: An Entity or Synapse ID to modify
        """

        activity = self.getProvenance(entity)
        if not activity: return

        uri = '/entity/%s/generatedBy' % id_of(entity)
        self.restDELETE(uri)

        ## TODO: what happens if the activity is shared by more than one entity?
        uri = '/activity/%s' % activity['id']
        self.restDELETE(uri)


    def updateActivity(self, activity):
        """
        Modifies an existing Activity.
        
        :returns: An updated Activity object
        """
        
        uri = '/activity/%s' % activity['id']
        return Activity(data=self.restPUT(uri, json.dumps(activity)))



    ############################################################
    ##             Locationable upload / download             ##
    ############################################################

    def _uploadFileAsLocation(self, entity, filename):
        """
        Uploads a filename as the location of an Entity.
        
        **Deprecated** in favor of FileEntities, but still supported.
        
        :param entity:   An Entity, dictionary, or SynapseID to set the location of
        :param filename: Name of the file to upload
        
        :returns: A list of length one containing a dictionary with 'locations' and 'md5' of the file
        """
        
        md5 = utils.md5_for_file(filename)

        # Guess mime-type - important for confirmation of MD5 sum by receiver
        (mimetype, enc) = mimetypes.guess_type(filename, strict=False)
        if (mimetype is None):
            mimetype = "application/octet-stream"

        # Ask synapse for a signed URL for S3 upload
        (_, base_filename) = os.path.split(filename)
        data = {'md5':md5.hexdigest(), 'path':base_filename, 'contentType':mimetype}
        uri = '/entity/%s/s3Token' % id_of(entity)
        headers = self._generateSignedHeaders(uri, 
                            {'Content-Type': 'application/json',
                             'Accept': 'application/json'})
        response_json = self.restPOST(uri, body=json.dumps(data))
        location_path = response_json['path']

        # PUT file to S3
        headers = { 'Content-MD5' : base64.b64encode(md5.digest()),
                    'Content-Type' : mimetype,
                    'x-amz-acl' : 'bucket-owner-full-control' }
        with open(filename, 'rb') as f:
            response = requests.put(response_json['presignedUrl'], headers=headers, data=f)
        exceptions._raise_for_status(response)

        # Add location to entity. Path will get converted to a signed S3 URL.
        locations = [{'path': location_path, 'type': 'awss3'}]

        return {'locations':locations, 'md5':md5.hexdigest()}


    def _downloadLocations(self, entity, filename):
        """
        Download files from Locationables.
        Locationables contain a signed S3 URL, which expire after a time, 
            so the Entity object passed to this method must have been recently acquired from Synapse.
            
        **Deprecated** in favor of FileEntities, but still supported.
        
        :returns: An updated Entity dictionary
        """
        
        if 'locations' not in entity or len(entity['locations']) == 0:
            return entity
            
        location = entity['locations'][0]  ## TODO: verify that this doesn't fail for unattached files
        url = location['path']
        utils.download_file(url, filename)

        entity.path = filename
        if entity['contentType'] == 'application/zip':
            # Unpack file
            filepath = os.path.join(os.path.dirname(filename), os.path.basename(filename) + '_unpacked')
            
            ## TODO: !!!FIX THIS TO BE PATH SAFE!  DON'T ALLOW ARBITRARY UNZIPING
            z = zipfile.ZipFile(filename, 'r')
            z.extractall(filepath) #WARNING!!!NOT SAFE
            
            ## TODO: fix - adding entries for 'files' and 'cacheDir' into entities causes an error in updateEntity
            entity['cacheDir'] = filepath
            entity['files'] = z.namelist()
        else:
            ## TODO: fix - adding entries for 'files' and 'cacheDir' into entities causes an error in updateEntity
            entity['cacheDir'] = os.path.dirname(filename)
            entity['files'] = [os.path.basename(filename)]
        return entity


        
    ############################################################
    ##               File handle service calls                ##
    ############################################################

    def _downloadFileEntity(self, entity, destination, submission=None):
        """Downloads the file associated with a FileEntity to the given file path."""
        
        if submission is not None:
            url = '%s/evaluation/submission/%s/file/%s' % (self.repoEndpoint, id_of(submission), entity['dataFileHandleId'])
        elif 'versionNumber' in entity:
            url = '%s/entity/%s/version/%s/file' % (self.repoEndpoint, id_of(entity), entity['versionNumber'])
        else:
            url = '%s/entity/%s/file' % (self.repoEndpoint, id_of(entity))

        # Create the necessary directories
        try:
            os.makedirs(os.path.dirname(destination))
        except OSError as exception:
            if exception.errno != os.errno.EEXIST:
                raise
        return self._downloadFile(url, destination)


    def _downloadFile(self, url, destination):
        """Download a file from a URL to a the given file path."""

        # We expect to be redirected to a signed S3 URL
        response = requests.get(url, headers=self._generateSignedHeaders(url), allow_redirects=False)
        if response.status_code in [301,302,303,307,308]:
            url = response.headers['location']

            if self.debug:
                print "_downloadFile: redirect url=", url

            # If it's a file URL, turn it into a path and return it
            if url.startswith('file:'):
                pathinfo = utils.file_url_to_path(url, verify_exists=True)
                if 'path' not in pathinfo:
                    raise IOError("Could not download non-existent file (%s)." % url)
                else:
                    raise NotImplementedError("File can already be accessed.  Consider setting downloadFile to False")

            response = requests.get(url, headers=self._generateSignedHeaders(url, {}), stream=True)
        
        try:
            response.raise_for_status()
        except SynapseHTTPError as err:
            if err.response.status_code == 404:
                raise SynapseError("Could not download the file at %s" % url)
            raise

        # Stream the file to disk
        with open(destination, "wb") as f:
            data = response.raw.read(FILE_BUFFER_SIZE)
            while data:
                f.write(data)
                data = response.raw.read(FILE_BUFFER_SIZE)

        destination = os.path.abspath(destination)
        return {
            'path': destination,
            'files': [os.path.basename(destination)],
            'cacheDir': os.path.dirname(destination) }


    def _uploadToFileHandleService(self, filename, synapseStore=True):
        """
        Create and return a fileHandle, by either uploading a local file or
        linking to an external URL.
        
        :param synapseStore: Indicates whether the file should be stored or just the URL.  
                             Defaults to True.
        """
        
        if filename is None:
            raise ValueError('No filename given')

        elif utils.is_url(filename):
            if synapseStore:
                raise NotImplementedError('Automatic downloading and storing of external files is not supported.  Please try downloading the file locally first before storing it.')
            return self._addURLtoFileHandleService(filename)

        # For local files, we default to uploading the file unless explicitly instructed otherwise
        else:
            if synapseStore:
                return self._chunkedUploadFile(filename)
            else:
                return self._addURLtoFileHandleService(filename)

                
    def _uploadFileToFileHandleService(self, filepath):
        """
        Upload a file to the new fileHandle service (experimental)
        
        :returns: A fileHandle which can be used to create a FileEntity or attach to a Wiki
        """
           
        # print "_uploadFileToFileHandleService - filepath = " + str(filepath)
        url = "%s/fileHandle" % (self.fileHandleEndpoint,)
        headers = self._generateSignedHeaders(url, {'Accept': 'application/json'})
        with open(filepath, 'rb') as f:
            response = requests.post(url, files={os.path.basename(filepath): f}, headers=headers)
        exceptions._raise_for_status(response)

        # We expect a list of FileHandles of length one
        fileHandleList = response.json()
        return fileHandleList['list'][0]

        
    def _addURLtoFileHandleService(self, externalURL):
        """Create a new FileHandle representing an external URL."""
        
        fileName = externalURL.split('/')[-1]
        externalURL = utils.as_url(externalURL)
        fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.ExternalFileHandle',
                      'fileName'    : fileName,
                      'externalURL' : externalURL}
        (mimetype, enc) = mimetypes.guess_type(externalURL, strict=False)
        if mimetype:
            fileHandle['contentType'] = mimetype
        return self.restPOST('/externalFileHandle', json.dumps(fileHandle), self.fileHandleEndpoint)

        
    def _getFileHandle(self, fileHandle):
        """Retrieve a fileHandle from the fileHandle service (experimental)."""
        
        uri = "/fileHandle/%s" % (id_of(fileHandle),)
        return self.restGET(uri, endpoint=self.fileHandleEndpoint)

        
    def _deleteFileHandle(self, fileHandle):
        """TODO_Sphinx."""
        
        uri = "/fileHandle/%s" % (id_of(fileHandle),)
        self.restDELETE(uri, endpoint=self.fileHandleEndpoint)
        return fileHandle

        
    def _createChunkedFileUploadToken(self, filepath, mimetype):
        """TODO_Sphinx."""
    
        md5 = utils.md5_for_file(filepath)
        chunkedFileTokenRequest = \
            {'fileName'    : os.path.basename(filepath), \
             'contentType' : mimetype, \
             'contentMD5'  : md5.hexdigest()}
        return self.restPOST('/createChunkedFileUploadToken', json.dumps(chunkedFileTokenRequest), endpoint=self.fileHandleEndpoint)

        
    def _createChunkedFileUploadChunkURL(self, chunkNumber, chunkedFileToken):
        """TODO_Sphinx."""
    
        chunkRequest = {'chunkNumber':chunkNumber, 'chunkedFileToken':chunkedFileToken}
        return self.restPOST('/createChunkedFileUploadChunkURL', json.dumps(chunkRequest), endpoint=self.fileHandleEndpoint)

        
    def _startCompleteUploadDaemon(self, chunkedFileToken, chunkNumbers):
        """TODO_Sphinx."""
    
        completeAllChunksRequest = {'chunkNumbers': chunkNumbers,
                                    'chunkedFileToken': chunkedFileToken}
        return self.restPOST('/startCompleteUploadDaemon', json.dumps(completeAllChunksRequest), endpoint=self.fileHandleEndpoint)

        
    def _completeUploadDaemonStatus(self, status):
        """TODO_Sphinx."""
    
        return self.restGET('/completeUploadDaemonStatus/%s' % status['daemonId'], endpoint=self.fileHandleEndpoint)

        
    def _chunkedUploadFile(self, filepath, chunksize=CHUNK_SIZE, verbose=False, progress=True):
        """
        Upload a file to be stored in Synapse, dividing large files into chunks.
        
        :param filepath: The file to be uploaded
        :param chunksize: Chop the file into chunks of this many bytes. 
                          The default value is 5MB, which is also the minimum value.
        
        :returns: An S3 FileHandle
        """
        
        if chunksize < 5*MB:
            raise ValueError('Minimum chunksize is 5 MB.')
        if filepath is None or not os.path.exists(filepath):
            raise ValueError('File not found: ' + str(filepath))
    
        old_debug = self.debug
        if verbose=='debug':
            self.debug = True

        # Start timing
        start_time = time.time()

        try:
            i = 0

            # Guess mime-type - important for confirmation of MD5 sum by receiver
            (mimetype, enc) = mimetypes.guess_type(filepath, strict=False)
            if (mimetype is None):
                mimetype = "application/octet-stream"

            # S3 wants 'content-type' and 'content-length' headers. S3 doesn't like
            # 'transfer-encoding': 'chunked', which requests will add for you, if it
            # can't figure out content length. The errors given by S3 are not very
            # informative:
            # If a request mistakenly contains both 'content-length' and
            # 'transfer-encoding':'chunked', you get [Errno 32] Broken pipe.
            # If you give S3 'transfer-encoding' and no 'content-length', you get:
            #   501 Server Error: Not Implemented
            #   A header you provided implies functionality that is not implemented
            headers = { 'Content-Type' : mimetype}

            # Get token
            token = self._createChunkedFileUploadToken(filepath, mimetype)
            if verbose: sys.stderr.write('\n\ntoken= ' + str(token) + '\n')

            if progress:
                sys.stdout.write('.')
                sys.stdout.flush()

            # Define the retry policy for uploading chunks
            with_retry = RetryRequest(
                retry_status_codes=[502,503],
                retry_errors=['We encountered an internal error. Please try again.'],
                retries=4, wait=1, back_off=2, verbose=verbose,
                tag='S3 put RetryRequest')

            with open(filepath, 'rb') as f:
                for chunk in utils.chunks(f, chunksize):
                    i += 1
                    if verbose: sys.stderr.write('\nChunk %d\n' % i)

                    # Get the signed S3 URL
                    url = self._createChunkedFileUploadChunkURL(i, token)
                    if verbose: sys.stderr.write('url= ' + str(url) + '\n')
                    if progress:
                        sys.stdout.write('.')
                        sys.stdout.flush()

                    # PUT the chunk to S3
                    response = with_retry(requests.put)(url, data=chunk, headers=self._generateSignedHeaders(url, headers))
                    exceptions._raise_for_status(response)
                    if progress:
                        sys.stdout.write(',')
                        sys.stdout.flush()

                    # Is requests closing response stream? Let's make sure:
                    # "Note that connections are only released back to
                    #  the pool for reuse once all body data has been
                    #  read; be sure to either set stream to False or
                    #  read the content property of the Response object."
                    # see: http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
                    try:
                        if response:
                            throw_away = response.content
                    except Exception as ex:
                        sys.stderr.write('error reading response: '+str(ex))

            status = self._startCompleteUploadDaemon(chunkedFileToken=token, chunkNumbers=[a+1 for a in range(i)])

            # Poll until concatenating chunks is complete
            while (status['state']=='PROCESSING'):
                if progress:
                    sys.stdout.write('!')
                    sys.stdout.flush()
                time.sleep(CHUNK_UPLOAD_POLL_INTERVAL)
                status = self._completeUploadDaemonStatus(status)
                if verbose: sys.stderr.write('status= ' + str(status) + '\n')
                #sys.stderr.write(str(status['runTimeMS']) + '\t' + str(status['percentComplete']) + '\n')

            if progress:
                sys.stdout.write('!\n')
                sys.stdout.flush()

            if status['state'] == 'FAILED':
                raise SynapseError(status['errorMessage'])

            # Return a fileHandle
            fileHandle = self._getFileHandle(status['fileHandleId'])

            # Print timing information
            if progress: sys.stdout.write("Upload completed in %s.\n" % utils.format_time_interval(time.time()-start_time))

            return fileHandle
        finally:
            self.debug = old_debug



    ############################################################
    ##                    Summary methods                     ##
    ############################################################

    def _traverseTree(self, id, name=None, version=None):
        """
        Creates a tree of IDs, versions, and names contained by the given Entity of ID
        
        :param id:      Entity to query for
        :param name:    TODO_Sphinx
        :param version: TODO_Sphinx
        
        :returns: TODO_Sphinx
        """
        
        children = self.chunkedQuery("select id, versionNumber, name from entity where entity.parentId=='%s'" % id)
        output = []
        output.append({               'name' : name, \
                       'targetVersionNumber' : version, \
                                  'targetId' : id, \
                                   'records' : [] })
        count = 0
        for entity in children:
            count += 1
            output[-1]['records'].extend( \
                    self._traverseTree(entity['entity.id'], \
                                       entity['entity.name'], \
                                       entity['entity.versionNumber']))
        print id, count, name
        if count == 0:
            del output[-1]['records']
            del output[-1]['name'] 
        return output

        
    def _flattenTree2Groups(self,tree, level=0, out=[]):
        """
        Converts a complete tree to a 2 level tree corresponding to a JSON schema of summary.
        
        :param tree:  JSON object representing entity organizion.  
                      Generally retrieved from :py:func:`synapseclient.Synapse._traverseTree`.
        :param level: TOSO_Sphinx
        :param out:   TODO_Sphinx
        
        :returns: TODO_Sphinx
        """
        
        # Move direct entities to subgroup "Content"
        if level == 0: 
            ## TODO: I am so sorry!  This is incredibly inefficient but I had no time to think through it.
            contents = [group for group in tree if not group.has_key('records')]
            tree.append({'name':'Content', 'records':contents, 'targetId':'', 'targetVersionNumber':''})
            for i in sorted([i for i, group in enumerate(tree) if not group.has_key('records')], reverse=True):
                tree.pop(i)

            # tree=[group for i, group in enumerate(tree) if i not in contents]
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
        """
        Traverses all sub-Entities of the given Entity 
        and creates a summary object within the given Entity.
        
        :param id:          Id of Entity to traverse to create Entity 
        :param name:        Name of created summary Entity
        :param description: Description of created Entity
        """
        print "hello"
        tree=self._traverseTree(id)[0]['records']
        print self.printEntity(tree)
        
        ## TODO: Instead of doing a flatten just by the default hierarchy structure, 
        ##       I should be using an external group-by parameter that determines whether 
        ##       and by what property of structure to group by.
        self._flattenTree2Groups(tree)
        self.printEntity(tree)
        self.createEntity({'name': name,
                           "description": description,
                           "concreteType": "org.sagebionetworks.repo.model.Summary", 
                           "groups": tree,
                           "parentId": id})



    ############################################################
    ##                  CRUD for Evaluations                  ##
    ############################################################

    def getEvaluation(self, id):
        """Gets an Evaluation object from Synapse."""
        
        evaluation_id = id_of(id)
        uri = Evaluation.getURI(evaluation_id)
        return Evaluation(**self.restGET(uri))
        
        
    ## TODO: Should this be combined with getEvaluation?
    def getEvaluationByName(self, name):
        """Gets an Evaluation object from Synapse."""
        
        uri = Evaluation.getByNameURI(urllib.quote(name))
        return Evaluation(**self.restGET(uri))


    def submit(self, evaluation, entity, name=None, teamName=None):
        """
        Submit an Entity for evaluation by an evaluator.
        
        :param evaluation: Evaluation board to submit to
        :param entity:     The Entity containing the Submission
        :param name:       A name for this submission
        :param teamName:   Team name to be publicly displayed
        
        :returns: A :py:class:`synapseclient.Submission` object

        Example::
            evaluation = syn.getEvaluation(12345)
            entity = syn.get('syn12345')
            submission = syn.submit(evaluation, entity, name='Our Final Answer', teamName='Blue Team')

        Set team name to user name::
            profile = syn.getUserProfile()
            submission = syn.submit(evaluation, entity, name='My Data', teamName=profile['displayName'])
        """

        evaluation_id = id_of(evaluation)
        
        # Check for access rights
        unmetRights = self.restGET('/evaluation/%s/accessRequirementUnfulfilled' % evaluation_id)
        if unmetRights['totalNumberOfResults'] > 0:
            accessTerms = ["%s - %s" % (rights['accessType'], rights['termsOfUse']) for rights in unmetRights['results']]
            raise SynapseAuthenticationError('You have unmet access requirements: \n%s' % '\n'.join(accessTerms))
        
        if not 'versionNumber' in entity:
            entity = self.get(entity)
        entity_version = entity['versionNumber']
        entity_id = entity['id']

        name = entity['name'] if (name is None and 'name' in entity) else None
        submission = {'evaluationId'  : evaluation_id, 
                      'entityId'      : entity_id, 
                      'name'          : name, 
                      'submitterAlias': teamName, 
                      'versionNumber' : entity_version}
        submitted = Submission(**self.restPOST('/evaluation/submission?etag=%s' % entity['etag'], 
                                               json.dumps(submission)))
        
        if 'submissionReceiptMessage' in evaluation:
            print evaluation['submissionReceiptMessage']
        return submitted


    def addEvaluationParticipant(self, evaluation, userId=None):
        """
        Adds a participant to an Evaluation.

        :param evaluation: An Evaluation object or Evaluation ID
        :param userId:     The prinicipal ID of the participant.
                           If not supplied, uses your ID
        """
        
        evaluation_id = id_of(evaluation)
        userId = self.getUserProfile()['ownerId'] if userId == None else userId
        self.restPOST('/evaluation/%s/participant/%s' % (evaluation_id, userId), {})


    def getSubmissions(self, evaluation, status=None):
        """
        TODO_Sphinx
        
        :param evaluation: Evaluation board to get submissions from.
        :param status:     Get submissions that have specific status 
                           one of {OPEN, CLOSED, SCORED, INVALID}
                           
        :returns: A generator over all Submissions for an Evaluation, 
                  optionally filters Submissions for a specified status.
                  
        Example::
        
            for submission in syn.getEvaluationSubmissions(1234567):
                print submission['entityId']
        """
        
        evaluation_id = evaluation['id'] if 'id' in evaluation else str(evaluation)

        result_count = 0
        limit = 20
        offset = 0 - limit
        max_results = 1000 # Gets updated later
        submissions = []

        while result_count < max_results:
            # If we're out of results, do a(nother) REST call
            if result_count >= offset + len(submissions):
                offset += limit
                if status != None:
                    if status not in ['OPEN', 'CLOSED', 'SCORED', 'INVALID']:
                        raise SynapseError('status may be one of {OPEN, CLOSED, SCORED, INVALID}')
                    uri = "/evaluation/%s/submission/all?status=%s&limit=%d&offset=%d" %(evaluation_id, 
                                                                                         status, limit, 
                                                                                         offset)
                else:
                    uri = "/evaluation/%s/submission/all?limit=%d&offset=%d" %(evaluation_id, limit, 
                                                                               offset)
                page_of_submissions = self.restGET(uri)
                max_results = page_of_submissions['totalNumberOfResults']
                submissions = page_of_submissions['results']
                if len(submissions)==0:
                    return

            i = result_count - offset
            result_count += 1
            yield Submission(**submissions[i])


    def getOwnSubmissions(self, evaluation):
        """
        TODO_Sphinx - Not implemented yet
        """
        
        ## TODO: implement this if this is really usefull?
        pass


    def getSubmission(self, id, **kwargs):
        """
        Gets a Submission object.
        
        See: :py:func:`synapseclient.Synapse.get` for information 
             on the *downloadFile*, *downloadLocation*, and *ifcollision* parameters
        """
        
        submission_id = id_of(id)
        uri = Submission.getURI(submission_id)
        submission = Submission(**self.restGET(uri))
        
        # Pre-fetch the Entity tied to the Submission, if there is one
        if 'entityId' in submission and submission['entityId'] is not None:
            related = self._getWithEntityBundle(submission['entityId'], \
                                entityBundle=json.loads(submission['entityBundleJSON']), 
                                submission=submission_id, **kwargs)
            submission['filePath'] = related['path']
            
        return submission


    def getSubmissionStatus(self, submission):
        """
        Downloads the status of a Submission.
        
        :param submission: The Submission to lookup
        
        :returns: A SubmissionStatus object
        """
        
        submission_id = id_of(submission)
        uri = SubmissionStatus.getURI(submission_id)
        val = self.restGET(uri)
        return SubmissionStatus(**val)



    ############################################################
    ##                     CRUD for Wikis                     ##
    ############################################################

    def getWiki(self, owner, subpageId=None):
        """Gets a Wiki object from Synapse."""
        
        owner_type = utils.guess_object_type(owner)
        if subpageId:
            uri = '/%s/%s/wiki/%s' % (owner_type, id_of(owner), id_of(subpageId))
        else:
            uri = '/%s/%s/wiki' % (owner_type, id_of(owner))
        wiki = self.restGET(uri)
        wiki['owner'] = owner
        return Wiki(**wiki)
        

    def getWikiHeaders(self, owner):
        """Retrieves the the header of all Wiki's belonging to the owner."
        
        :param owner: An Evaluation or Entity
        
        :returns: TODO_Sphinx
        """
        
        owner_type = utils.guess_object_type(owner)
        uri = '/%s/%s/wikiheadertree' % (owner_type, id_of(owner),)
        return self.restGET(uri)

        
    # # Need to test functionality of this
    # def _downloadWikiAttachment(self, owner, wiki, filename, destination=None, owner_type=None):
    #     # Download a file attached to a wiki page
    #     if not owner_type:
    #         owner_type = utils.guess_object_type(owner)
    #     url = "%s/%s/%s/wiki/%s/attachment?fileName=%s" % (self.repoEndpoint, owner_type, id_of(owner), id_of(wiki), filename,)
    #     return self._downloadFile(url, destination)

    
    # # Superseded by getWiki
    # def _createWiki(self, owner, title, markdown, attachmentFileHandleIds=None, owner_type=None):
    #     """
    #     Create a new wiki page for an Entity (experimental).
    #     
    #     :param owner:                   The owner object (Entity, Competition, or Evaluation) 
    #                                     with which the new Wiki page will be associated.
    #     :param markdown:                The markdown contents of the Wiki page
    #     :param attachmentFileHandleIds: A list of file handles or file handle IDs
    #     :param owner_type:              Entity, Competition, or Evaluation can usually be 
    #                                     automatically inferred from the owner object
    #     """
    # 
    #     if not owner_type:
    #         owner_type = utils.guess_object_type(owner)
    #     uri = '/%s/%s/wiki' % (owner_type, id_of(owner),)
    #     wiki = {'title':title, 'markdown':markdown}
    #     if attachmentFileHandleIds:
    #         wiki['attachmentFileHandleIds'] = attachmentFileHandleIds
    #     return self.restPOST(uri, body=json.dumps(wiki))


    
    ############################################################
    ##             CRUD for Entities (properties)             ##
    ############################################################

    def _getEntity(self, entity, version=None):
        """
        Get an entity from Synapse.
        
        :param entity:  A Synapse ID, a dictionary representing an Entity, or a Synapse Entity object
        :param version: The version number to fetch
        
        :returns: A dictionary containing an Entity's properties
        """
        
        uri = '/entity/'+id_of(entity)
        if version:
            uri += '/version/%d' % version
        return self.restGET(uri)

        
    def _createEntity(self, entity):
        """
        Create a new entity in Synapse.
        
        :param entity: A dictionary representing an Entity or a Synapse Entity object
        
        :returns: A dictionary containing an Entity's properties
        """
        
        if self.debug: print "\n\n~~~ creating ~~~\n" + json.dumps(get_properties(entity), indent=2)
        return self.restPOST(uri='/entity', body=json.dumps(get_properties(entity)))

        
    def _updateEntity(self, entity, incrementVersion=True, versionLabel=None):
        """
        Update an existing entity in Synapse.

        :param entity: A dictionary representing an Entity or a Synapse Entity object
        
        :returns: A dictionary containing an Entity's properties
        """
        
        uri = '/entity/%s' % id_of(entity)

        if is_versionable(entity):
            if incrementVersion:
                uri += '/version'
                if 'versionNumber' in entity:
                    entity['versionNumber'] += 1
                    if 'versionLabel' in entity:
                        entity['versionLabel'] = str(entity['versionNumber'])

        if versionLabel:
            entity['versionLabel'] = str(versionLabel)

        if self.debug: print "\n\n~~~ updating ~~~\n" + json.dumps(get_properties(entity), indent=2)
        return self.restPUT(uri, body=json.dumps(get_properties(entity)))

        
    def _findEntityIdByNameAndParent(self, name, parent=None):
        """
        Find an Entity given its name and parent ID.
        
        :returns: the Entity ID or None if not found
        """
        
        if parent is None:
            parent = ROOT_ENTITY
        qr = self.query('select id from entity where name=="%s" and parentId=="%s"' % (name, id_of(parent)))
        if qr.get('totalNumberOfResults', 0) == 1:
            return qr['results'][0]['entity.id']
        else:
            return None


            
    ############################################################
    ##                  Low level Rest calls                  ##
    ############################################################
    
    def _generateSignedHeaders(self, url, headers=None):
        """Generate headers signed with the API key."""
        
        if self.username is None or self.apiKey is None:
            raise SynapseAuthenticationError("Please login")
            
        if headers is None:
            headers = dict(self.headers)
            
        sig_timestamp = time.strftime(utils.ISO_FORMAT, time.gmtime())
        url = urlparse.urlparse(url).path
        sig_data = self.username + url + sig_timestamp
        signature = base64.b64encode(hmac.new(self.apiKey, sig_data, hashlib.sha1).digest())

        sig_header = {'userId'             : self.username,
                      'signatureTimestamp' : sig_timestamp,
                      'signature'          : signature}

        headers.update(sig_header)
        return headers
    
    
    @STANDARD_RETRY_REQUEST
    def restGET(self, uri, endpoint=None, headers=None, **kwargs):
        """
        Performs a REST GET operation to the Synapse server.
        
        :param uri:      URI on which get is performed
        :param endpoint: Server endpoint, defaults to self.repoEndpoint
        :param headers:  Dictionary of headers to use rather than the API-key-signed default set of headers
        :param kwargs:   Any other arguments taken by a `requests <http://docs.python-requests.org/en/latest/>`_ method

        :returns: JSON encoding of response
        """
        
        # Build up the HTTP request info
        if endpoint == None:
            endpoint = self.repoEndpoint    
        uri = endpoint + uri
        if headers is None:
            headers = self._generateSignedHeaders(uri)
            
        response = requests.get(uri, headers=headers, **kwargs)
        self._storeTimingProfile(response)
        if self.debug:
            utils.debug_response(response)
        exceptions._raise_for_status(response)
        return response.json()
     
     
    @STANDARD_RETRY_REQUEST
    def restPOST(self, uri, body, endpoint=None, headers=None, **kwargs):
        """
        Performs a REST POST operation to the Synapse server.
        
        :param uri:      URI on which get is performed
        :param endpoint: Server endpoint, defaults to self.repoEndpoint
        :param body:     The payload to be delivered 
        :param headers:  Dictionary of headers to use rather than the API-key-signed default set of headers
        :param kwargs:   Any other arguments taken by a `requests <http://docs.python-requests.org/en/latest/>`_ method

        :returns: JSON encoding of response
        """
        
        # Build up the HTTP request info
        if endpoint == None:
            endpoint = self.repoEndpoint    
        uri = endpoint + uri
        if headers is None:
            headers = self._generateSignedHeaders(uri)
            
        response = requests.post(uri, data=body, headers=headers, **kwargs)
        if self.debug:
            utils.debug_response(response)
        exceptions._raise_for_status(response)

        if response.headers.get('content-type',None) == 'application/json':
            return response.json()
        # 'content-type': 'text/plain;charset=ISO-8859-1'
        return response.text

        
    @STANDARD_RETRY_REQUEST
    def restPUT(self, uri, body=None, endpoint=None, headers=None, **kwargs):
        """
        Performs a REST PUT operation to the Synapse server.
        
        :param uri:      URI on which get is performed
        :param endpoint: Server endpoint, defaults to self.repoEndpoint
        :param body:     The payload to be delivered 
        :param headers:  Dictionary of headers to use rather than the API-key-signed default set of headers
        :param kwargs:   Any other arguments taken by a `requests <http://docs.python-requests.org/en/latest/>`_ method

        :returns: JSON encoding of response
        """
        
        # Build up the HTTP request info
        if endpoint == None:
            endpoint = self.repoEndpoint    
        uri = endpoint + uri
        if headers is None:
            headers = self._generateSignedHeaders(uri)
            
        response = requests.put(uri, data=body, headers=headers, **kwargs)
        if self.debug:
            utils.debug_response(response)
        exceptions._raise_for_status(response)
            
        if response.headers.get('content-type',None) == 'application/json':
            return response.json()
        return response.text

        
    @STANDARD_RETRY_REQUEST
    def restDELETE(self, uri, endpoint=None, headers=None, **kwargs):
        """
        Performs a REST DELETE operation to the Synapse server.
        
        :param uri:      URI of resource to be deleted
        :param endpoint: Server endpoint, defaults to self.repoEndpoint
        :param headers:  Dictionary of headers to use rather than the API-key-signed default set of headers
        :param kwargs:   Any other arguments taken by a `requests <http://docs.python-requests.org/en/latest/>`_ method
        """
        
        # Build up the HTTP request info
        if endpoint == None:
            endpoint = self.repoEndpoint    
        uri = endpoint + uri
        if headers is None:
            headers = self._generateSignedHeaders(uri)
            
        response = requests.delete(uri, headers=headers, **kwargs)
        if self.debug:
            utils.debug_response(response)
        exceptions._raise_for_status(response)
