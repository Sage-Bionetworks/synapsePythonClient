import os, json, tempfile
from nose.tools import assert_raises
from mock import MagicMock, patch
import unit
import synapseclient
from synapseclient.exceptions import *


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = unit.syn


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
                              "entityType": "org.sagebionetworks.repo.model.FileEntity"},
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


@patch('synapseclient.client.id_of')
@patch('synapseclient.Synapse.restGET')
@patch('synapseclient.Synapse.get')
@patch('synapseclient.Synapse.restPOST')
@patch('json.dumps')
@patch('synapseclient.client.Submission')
def test_submit(*mocks):
    mocks = [item for item in mocks]
    id_mock         = mocks.pop()
    GET_mock        = mocks.pop()
    get_mock        = mocks.pop()
    POST_mock       = mocks.pop()
    json_mock       = mocks.pop()
    submission_mock = mocks.pop()
    
    # -- Unmet access rights --
    id_mock.return_value = 1337
    GET_mock.return_value = {'totalNumberOfResults': 2, 
                             'results': [
                                {'accessType': 'Foo', 
                                 'termsOfUse': 'Bar'}, 
                                {'accessType': 'bat', 
                                 'termsOfUse': 'baz'}]}
                                 
    assert_raises(SynapseAuthenticationError, syn.submit, "Evaluation", "Entity")
    id_mock.assert_called_once_with("Evaluation")
    GET_mock.assert_called_once_with('/evaluation/1337/accessRequirementUnfulfilled')
    
    # -- Normal submission --
    # Pretend the user has access rights 
    GET_mock.return_value = {'totalNumberOfResults': 0, 'results': []}
    
    # Ignore whatever is POST-ed
    json_mock.return_value = None
    POST_mock.return_value = {}
    
    syn.submit("EvalTwo", {'versionNumber': 1337, 'id': "Whee...", 'etag': 'Fake eTag'})
    id_mock.assert_called_with("EvalTwo")
    assert GET_mock.call_count == 2
    assert not get_mock.called
    POST_mock.assert_called_once_with('/evaluation/submission?etag=Fake eTag', None)
    assert submission_mock.called
    