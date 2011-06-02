#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, sys, string, traceback, json, urllib, urlparse, httplib

def addArguments(parser):
    '''
    Synapse command line argument helper
    '''
    parser.add_argument('--serviceEndpoint', '-e',
                        help='the url to which to send the metadata '
                        + '(e.g. https://repositoryservice.sagebase.org/repo/v1)',
                        required=True)

    parser.add_argument('--serviceTimeoutSeconds', '-t',
                        help='the socket timeout for blocking operations to the service (e.g., connect, send, receive), defaults to 30 seconds',
                        default=30)

    
def factory(args):
    '''
    Factory method to create a Synapse instance from command line args
    '''
    return Synapse(args.serviceEndpoint,
                   args.serviceTimeoutSeconds,
                   args.debug)
    
class Synapse:
    '''
    Python implementation for Synapse repository service client
    '''
    #-------------------[ Constants ]----------------------
    HEADERS = {
        'Content-type': 'application/json',
        'Accept': 'application/json',
        }
    
    def __init__(self, serviceEndpoint, serviceTimeoutSeconds, debug):
        '''
        Constructor
        '''
        self.serviceEndpoint = serviceEndpoint
        self.serviceTimeoutSeconds = serviceTimeoutSeconds 
        self.debug = debug

        parseResult = urlparse.urlparse(serviceEndpoint)
        self.serviceLocation = parseResult.netloc
        self.servicePrefix = parseResult.path
        self.serviceProtocol = parseResult.scheme
        
    def createEntity(self, uri, entity):
        '''
        Create a new dataset, layer, etc ...
        '''
        if(None == uri or None == entity
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")

        if(0 != string.find(uri, self.servicePrefix)):
                uri = self.servicePrefix + uri

        conn = {}
        if('https' == self.serviceProtocol):
            conn = httplib.HTTPSConnection(self.serviceLocation,
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(self.serviceLocation,
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to create %s with %s' % (uri, json.dumps(entity))

        storedEntity = None
        try:
            conn.request('POST', uri, json.dumps(entity), Synapse.HEADERS)
            resp = conn.getresponse()
            output = resp.read()
            if self.debug:
                print output
            if resp.status == 201:
                storedEntity = json.loads(output)
            else:
                print resp.status, resp.reason
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return storedEntity
        
    def getEntity(self, uri):
        '''
        Get a dataset, layer, preview, annotations, etc..
        '''
        if(uri == None):
            return
        if(0 != string.find(uri, self.servicePrefix)):
                uri = self.servicePrefix + uri
    
        conn = {}
        if('https' == self.serviceProtocol):
            conn = httplib.HTTPSConnection(self.serviceLocation,
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(self.serviceLocation,
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to get %s' % (uri)
    
        entity = None
    
        try:
            conn.request('GET', uri, None, Synapse.HEADERS)
            resp = conn.getresponse()
            output = resp.read()
            if self.debug:
                print output
            if resp.status == 200:
                entity = json.loads(output)
            else:
                print resp.status, resp.reason
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return entity
            
    def updateEntity(self, uri, entity):
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
        if(None == uri or None == entity
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")

        oldEntity = self.getEntity(uri)
        if(oldEntity == None):
            return None

        # Overwrite our stored fields with our updated fields
        keys = entity.keys()
        for key in keys:
            if(0 != string.find(uri, "annotations")):
                # annotations need special handling
                for annotKey in entity[key].keys():
                    oldEntity[key][annotKey] = entity[key][annotKey]
            else:
                oldEntity[key] = entity[key]

        return self.putEntity(uri, oldEntity)

    def putEntity(self, uri, entity):
        '''
        Update a dataset, layer, preview, annotations, etc..
        '''
        if(None == uri or None == entity
           or not (isinstance(entity, dict)
                   and (isinstance(uri, str) or isinstance(uri, unicode)))):
            raise Exception("invalid parameters")

        if(0 != string.find(uri, self.servicePrefix)):
                uri = self.servicePrefix + uri
    
        conn = {}
        if('https' == self.serviceProtocol):
            conn = httplib.HTTPSConnection(self.serviceLocation,
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(self.serviceLocation,
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(2);
            print 'About to update %s with %s' % (uri, json.dumps(entity))
    
        putHeaders = Synapse.HEADERS
        putHeaders['ETag'] = entity['etag']
    
        storedEntity = None
        try:
            conn.request('PUT', uri, json.dumps(entity), putHeaders)
            resp = conn.getresponse()
            output = resp.read()
            if self.debug:
                print output
            if resp.status == 200:
                storedEntity = json.loads(output)
            else:
                print resp.status, resp.reason
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return storedEntity

    def deleteEntity(self, uri):
        '''
        Delete a dataset, layer, etc..
        '''
        if(None == uri):
            return
        if(0 != string.find(uri, self.servicePrefix)):
                uri = self.servicePrefix + uri
    
        conn = {}
        if('https' == self.serviceProtocol):
            conn = httplib.HTTPSConnection(self.serviceLocation,
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(self.serviceLocation,
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to delete %s' % (uri)

        try:
            conn.request('DELETE', uri, None, Synapse.HEADERS)
            resp = conn.getresponse()
            output = resp.read()
            if self.debug:
                print output
            if resp.status != 204:
                print resp.status, resp.reason
            return None;
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        conn.close()

    def query(self, query):
        '''
        Query for datasets, layers, etc..
        '''
        uri = self.servicePrefix + '/query?query=' + urllib.quote(query)
    
        conn = {}
        if('https' == self.serviceProtocol):
            conn = httplib.HTTPSConnection(self.serviceLocation,
                                           timeout=self.serviceTimeoutSeconds)
        else:
            conn = httplib.HTTPConnection(self.serviceLocation,
                                          timeout=self.serviceTimeoutSeconds)

        if(self.debug):
            conn.set_debuglevel(10);
            print 'About to query %s' % (query)
    
        results = None
    
        try:
            conn.request('GET', uri, None, Synapse.HEADERS)
            resp = conn.getresponse()
            output = resp.read()
            if self.debug:
                print output
            if resp.status == 200:
                results = json.loads(output)
            else:
                print resp.status, resp.reason
        except Exception, err:
            traceback.print_exc(file=sys.stderr)
            # re-throw the exception
            raise
        finally:
            conn.close()
        return results
            
    def createDataset(self, dataset):
        '''
        We have a helper method to create a dataset since it is a top level url
        '''
        return self.createEntity('/dataset', dataset)

    def getDataset(self, datasetId):
        '''
        We have a helper method to get a dataset since it is a top level url
        '''
        return self.getEntity('/dataset/' + str(datasetId))


    
#------- UNIT TESTS -----------------
if __name__ == '__main__':
    import unittest

    class TestSynapse(unittest.TestCase):
        '''
        Unit tests against a local service, read/write operations are okay here
        '''
        def setUp(self):
            self.client = Synapse('localhost:8080', '/repo/v1', False, True)

        def test_getDataset(self):
            self.assertRaises(Exception, self.client.getDataset, list())
    
    class IntegrationTestSynapse(unittest.TestCase):
        '''
        Integration tests against a remote service, do only read-only operations here
        '''
        def setUp(self):
            self.client = Synapse('repositoryservicea.elasticbeanstalk.com',
                                  '/repo/v1', False, True)

        def test_getDataset(self):
            dataset = self.client.getDataset(0)
            self.assertTrue(None != dataset)
    
        def test_query(self):
            dataset = self.client.query('select * from dataset')
            self.assertTrue(None != dataset)
    
    unittest.main()
