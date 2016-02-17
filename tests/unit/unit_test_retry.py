import os, json, tempfile, filecmp
from nose.tools import assert_raises
from mock import MagicMock, patch
import unit
import synapseclient
from synapseclient.retry import _with_retry
from synapseclient.exceptions import *


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = unit.syn


def test_with_retry():
    retryParams = {"retries": 3, "wait": 0}
    response = MagicMock()
    function = MagicMock()
    function.return_value = response
    
    # -- No failures -- 
    response.status_code.__eq__.side_effect = lambda x: x == 250
    _with_retry(function, verbose=True, **retryParams)
    assert function.call_count == 1
    
    # -- Always fail -- 
    response.status_code.__eq__.side_effect = lambda x: x == 503
    _with_retry(function, verbose=True, **retryParams)
    assert function.call_count == 1 + 4
    
    # -- Fail then succeed -- 
    thirdTimes = [3, 2, 1]
    def theCharm(x):
        if x == 503:
            count = thirdTimes.pop()
            return count != 3
        return x == 503
    response.status_code.__eq__.side_effect = theCharm
    _with_retry(function, verbose=True, **retryParams)
    assert function.call_count == 1 + 4 + 3
    
    # -- Retry with an error message --
    retryErrorMessages = ["Foo"]
    retryParams["retry_errors"] = retryErrorMessages
    response.status_code.__eq__.side_effect = lambda x: x == 500
    response.headers.__contains__.reset_mock()
    response.headers.__contains__.side_effect = lambda x: x == 'content-type'
    response.headers.get.side_effect = lambda x,default_value: "application/json" if x == 'content-type' else None
    response.json.return_value = {"reason": retryErrorMessages[0]}
    _with_retry(function, **retryParams)
    assert response.headers.get.called
    assert function.call_count == 1 + 4 + 3 + 4
    
    # -- Propagate an error up --
    print("Expect a SynapseError: Bar")
    def foo(): raise SynapseError("Bar")
    function.side_effect = foo
    assert_raises(SynapseError, _with_retry, function, **retryParams)
    assert function.call_count == 1 + 4 + 3 + 4 + 1

