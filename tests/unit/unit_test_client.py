import os, json, tempfile, filecmp
from nose.tools import assert_raises
from mock import MagicMock, patch
import unit
import synapseclient
from synapseclient import File
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


@patch('synapseclient.Synapse._downloadFileEntity')
def test_getWithEntityBundle(download_file_mock):

    ## Note: one thing that remains unexplained is why the previous version of
    ## this test worked if you had a .cacheMap file of the form:
    ## {"/Users/chris/.synapseCache/663/-1337/anonymous": "2014-09-15T22:54:57.000Z",
    ##  "/var/folders/ym/p7cr7rrx4z7fw36sxv04pqh00000gq/T/tmpJ4nz8U": "2014-09-15T23:27:25.000Z"}
    ## ...but failed if you didn't.

    ## TODO: Uncomment failing asserts after SYNR-790 and SYNR-697 are fixed

    bundle = {
        'entity': {
            'id': 'syn10101',
            'name': 'anonymous',
            'dataFileHandleId': '-1337',
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'parentId': 'syn12345'},
        'fileHandles': [{
            'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            'fileName': 'anonymous',
            'contentType': 'application/flapdoodle',
            'contentMd5': '1698d26000d60816caab15169efcd23a',
            'id': '-1337'}],
        'annotations': {}}

    fileHandle = bundle['fileHandles'][0]['id']
    cacheDir = synapseclient.cache.determine_cache_directory(fileHandle)
    print "cacheDir=", cacheDir

    # Make sure the .cacheMap file does not already exist
    cacheMap = os.path.join(cacheDir, '.cacheMap')
    if os.path.exists(cacheMap):
        print "removing cacheMap file: ", cacheMap
        os.remove(cacheMap)

    def _downloadFileEntity(entity, path, submission):
        print "mock downloading file to:", path
        ## touch file at path
        with open(path, 'a'):
            os.utime(path, None)
        dest_dir, filename = os.path.split(path)
        return {"path": path,
                "files": [filename],
                "cacheDir": dest_dir}
    download_file_mock.side_effect = _downloadFileEntity

    # 1. ----------------------------------------------------------------------
    # download file to an alternate location

    temp_dir1 = tempfile.mkdtemp()
    print "temp_dir1=", temp_dir1

    e = syn._getWithEntityBundle(entityBundle=bundle,
                                 downloadLocation=temp_dir1,
                                 ifcollision="overwrite.local")
    print e

    assert e.name == bundle["entity"]["name"]
    assert e.parentId == bundle["entity"]["parentId"]
    assert e.cacheDir == temp_dir1
    assert bundle["fileHandles"][0]["fileName"] in e.files
    assert e.path == os.path.join(temp_dir1, bundle["fileHandles"][0]["fileName"])

    # 2. ----------------------------------------------------------------------
    # download to cache
    e = syn._getWithEntityBundle(entityBundle=bundle, ifcollision="overwrite.local")

    print e

    assert e.name == bundle["entity"]["name"]
    assert e.parentId == bundle["entity"]["parentId"]
    assert bundle["fileHandles"][0]["fileName"] in e.files

    # should this put the file in the cache?
    assert e.cacheDir == cacheDir
    assert e.path == os.path.join(cacheDir, bundle["entity"]["name"])

    # 3. ----------------------------------------------------------------------
    # download to another location
    temp_dir2 = tempfile.mkdtemp()
    assert temp_dir2 != temp_dir1
    e = syn._getWithEntityBundle(entityBundle=bundle,
                                 downloadLocation=temp_dir2,
                                 ifcollision="overwrite.local")
    print "temp_dir2=", temp_dir2
    print e

    assert bundle["fileHandles"][0]["fileName"] in e.files
    assert e.path is not None
    assert os.path.dirname(e.path) == temp_dir2

    # 4. ----------------------------------------------------------------------
    ## test preservation of local state
    url = 'http://foo.com/secretstuff.txt'
    e = File(name='anonymous', parentId="syn12345", synapseStore=False, externalURL=url)
    e.local_state({'zap':'pow'})
    e = syn._getWithEntityBundle(entityBundle=bundle, entity=e)
    assert e.local_state()['zap'] == 'pow'
    assert e.synapseStore == False
    assert e.externalURL == url

    ## TODO: add more test cases for flag combination of this method
    ## TODO: separate into another test?


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
