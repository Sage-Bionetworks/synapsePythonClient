import os, json
from nose.tools import assert_raises
from mock import MagicMock, patch
import unit


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = unit.syn
    

@patch('synapseclient.client.Submission')
@patch('json.dumps')
@patch('synapseclient.Synapse.restPOST')
@patch('synapseclient.Synapse.get')
@patch('synapseclient.Synapse.restGET')
@patch('synapseclient.client.id_of')
def test_submit(id_mock, GET_mock, get_mock, POST_mock, json_mock, submission_mock):
    # -- Unmet access rights --
    id_mock.return_value = 1337
    GET_mock.return_value = {'totalNumberOfResults': 2, 
                             'results': [
                                {'accessType': 'Foo', 
                                 'termsOfUse': 'Bar'}, 
                                {'accessType': 'bat', 
                                 'termsOfUse': 'baz'}]}
                                 
    assert_raises(Exception, syn.submit, "Evaluation", "Entity")
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
    