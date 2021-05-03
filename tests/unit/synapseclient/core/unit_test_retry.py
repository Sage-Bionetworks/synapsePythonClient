import pytest
from unittest.mock import MagicMock

from synapseclient.core.retry import with_retry, with_retry_network
from synapseclient.core.exceptions import SynapseError


def test_with_retry():
    retryParams = {"retries": 3, "wait": 0}
    response = MagicMock()
    function = MagicMock()
    function.return_value = response

    # -- No failures --
    response.status_code.__eq__.side_effect = lambda x: x == 250
    with_retry_network(function, verbose=True, **retryParams)
    assert function.call_count == 1

    # -- Always fail --
    response.status_code.__eq__.side_effect = lambda x: x == 503
    with_retry_network(function, verbose=True, **retryParams)
    assert function.call_count == 1 + 4

    # -- Fail then succeed --
    thirdTimes = [3, 2, 1]

    def theCharm(x):
        if x == 503:
            count = thirdTimes.pop()
            return count != 3
        return x == 503
    response.status_code.__eq__.side_effect = theCharm
    with_retry_network(function, verbose=True, **retryParams)
    assert function.call_count == 1 + 4 + 3

    # -- Retry with an error message --
    retryErrorMessages = ["Foo"]
    retryParams["retry_errors"] = retryErrorMessages
    response.status_code.__eq__.side_effect = lambda x: x == 500
    response.headers.__contains__.reset_mock()
    response.headers.__contains__.side_effect = lambda x: x == 'content-type'
    response.headers.get.side_effect = lambda x, default_value: "application/json" if x == 'content-type' else None
    response.json.return_value = {"reason": retryErrorMessages[0]}
    with_retry_network(function, **retryParams)
    assert response.headers.get.called
    assert function.call_count == 1 + 4 + 3 + 4

    # -- Propagate an error up --
    print("Expect a SynapseError: Bar")

    def foo():
        raise SynapseError("Bar")
    function.side_effect = foo
    pytest.raises(SynapseError, with_retry, function, **retryParams)
    assert function.call_count == 1 + 4 + 3 + 4 + 1


def test_with_retry__no_status_code():
    """Verify that with_retry can also be used on any function
    even whose return values don't have status_codes.
    In that case just for its exception retrying
    and back off capabiliies."""

    x = 0

    def fn():
        nonlocal x
        x += 1
        if x < 2:
            raise ValueError('not yet')
        return x

    response = with_retry(fn, retry_exceptions=[ValueError])
    assert 2 == response
