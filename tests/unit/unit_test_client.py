import os, json, tempfile, filecmp
from nose.tools import assert_raises
from mock import MagicMock, patch
import unit
import synapseclient
from synapseclient.exceptions import *
from synapseclient import Evaluation


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = unit.syn


@patch('synapseclient.Synapse._loggedIn')
@patch('synapseclient.Synapse.restDELETE')
@patch('synapseclient.Synapse._readSessionCache')
@patch('synapseclient.Synapse._writeSessionCache')
def test_logout(*mocks):
    mocks = [item for item in mocks]
    logged_in_mock     = mocks.pop()
    delete_mock        = mocks.pop()
    read_session_mock  = mocks.pop()
    write_session_mock = mocks.pop()
    
    # -- Logging out while not logged in shouldn't do anything --
    logged_in_mock.return_value = False
    syn.username = None
    syn.logout()
    syn.logout()
    
    assert not delete_mock.called
    assert not write_session_mock.called


@patch('synapseclient.client.is_locationable')
@patch('synapseclient.cache.determine_local_file_location')
@patch('synapseclient.Synapse._downloadFileEntity')
def test_getWithEntityBundle(*mocks):
    mocks = [item for item in mocks]
    is_loco_mock              = mocks.pop()
    cache_location_guess_mock = mocks.pop()
    download_file_mock        = mocks.pop()
    
    # -- Change downloadLocation but do not download more than once --
    is_loco_mock.return_value = False
    
    bundle = {"entity"     : {"name": "anonymous", 
                              "dataFileHandleId": "-1337", 
                              "concreteType": "org.sagebionetworks.repo.model.FileEntity",
                              "parentId": "syn12345"},
              "fileHandles": [], 
              "annotations": {}}
    
    # Make the cache point to some temporary location
    cacheDir = synapseclient.cache.determine_cache_directory(bundle['entity'])
    
    # Pretend that the file is downloaded by the first call to syn._downloadFileEntity
    # The temp file should be added to the cache by the first syn._getWithEntityBundle() call
    f, cachedFile = tempfile.mkstemp()
    os.close(f)
    defaultLocation = os.path.join(cacheDir, bundle['entity']['name'])
    cache_location_guess_mock.return_value = (cacheDir, defaultLocation, cachedFile)
    
    # Make sure the Entity is updated with the cached file path
    def _downloadFileEntity(entity, path, submission):
        # We're disabling the download, but the given path should be within the cache
        assert path == defaultLocation
        return {"path": cachedFile}
    download_file_mock.side_effect = _downloadFileEntity

    # Make sure the cache does not already exist
    cacheMap = os.path.join(cacheDir, '.cacheMap')
    if os.path.exists(cacheMap):
        os.remove(cacheMap)
    
    syn._getWithEntityBundle(None, entityBundle=bundle, downloadLocation=cacheDir, ifcollision="overwrite.local")
    syn._getWithEntityBundle(None, entityBundle=bundle, ifcollision="overwrite.local")
    syn._getWithEntityBundle(None, entityBundle=bundle, downloadLocation=cacheDir, ifcollision="overwrite.local")
    assert download_file_mock.call_count == 1
    
    ## TODO: add more test cases for flag combination of this method


@patch('synapseclient.Synapse.restGET')
@patch('synapseclient.Synapse.restPOST')
@patch('synapseclient.Synapse.getEvaluation')
def test_submit(*mocks):
    mocks = [item for item in mocks]
    GET_mock        = mocks.pop()
    POST_mock       = mocks.pop()
    getEvaluation_mock = mocks.pop()
    
    # -- Unmet access rights --
    GET_mock.return_value = {'totalNumberOfResults': 2, 
                             'results': [
                                {'accessType': 'Foo', 
                                 'termsOfUse': 'Bar'}, 
                                {'accessType': 'bat', 
                                 'termsOfUse': 'baz'}]}
    getEvaluation_mock.return_value = Evaluation(**{u'contentSource': u'syn1001',
                                                    u'createdOn': u'2013-11-06T06:04:26.789Z',
                                                    u'etag': u'86485ea1-8c89-4f24-a0a4-2f63bc011091',
                                                    u'id': u'9090',
                                                    u'name': u'test evaluation',
                                                    u'ownerId': u'1560252',
                                                    u'status': u'OPEN',
                                                    u'submissionReceiptMessage': u'mmmm yummy!'})

    assert_raises(SynapseAuthenticationError, syn.submit, "9090", "syn1001")
    GET_mock.assert_called_once_with('/evaluation/9090/accessRequirementUnfulfilled')
    
    # -- Normal submission --
    # Pretend the user has access rights 
    GET_mock.return_value = {'totalNumberOfResults': 0, 'results': []}
    
    # insert a shim that returns the dictionary it was passed after adding a bogus id
    def shim(*args):
        assert args[0] == '/evaluation/submission?etag=Fake eTag'
        submission = json.loads(args[1])
        submission['id'] = 1234
        return submission
    POST_mock.side_effect = shim
    
    submission = syn.submit('9090', {'versionNumber': 1337, 'id': "Whee...", 'etag': 'Fake eTag'}, name='George', teamName='Team X')
    assert GET_mock.call_count == 2

    assert submission.id == 1234
    assert submission.evaluationId == '9090'
    assert submission.name == 'George'
    assert submission.submitterAlias == 'Team X'

    print submission
