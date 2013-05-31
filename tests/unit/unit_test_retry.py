import filecmp
import tempfile
import os
from nose.tools import assert_raises

from synapseclient.retry import RetryRequest
from synapseclient.dict_object import DictObject


class MyException(Exception):
    """Mock HTTP Exceptions"""
    def __init__(self, message, **kwargs):
        self.message = message
        self.__dict__.update(kwargs)
    def __str__(self):
        return 'MyException: ' + str(self.__dict__)

class MockResponse(DictObject):
    def __init__(self, *args, **kwargs):
        super(MockResponse, self).__init__(*args, **kwargs)
        self.headers={'content-type':'application/json'}
    def json(self):
        if self.status_code >= 500:
            return {'reason':self.get('reason', 'Darnit!')}
        else:
            return {'ok':'ok'}

class Failer(object):
    """A class to generate failure for testing the RetryRequest"""
    def __init__(self):
        self.counter = 0

    def fail_n_times_exception(self, n, status_code, message):
        if self.counter < n:
            self.counter += 1
            response = MockResponse(reason=message, status_code=status_code)
            raise MyException('Fail n times exception: ' + str(self.counter), response=response)
        self.reset()
        return MockResponse(status_code=200, text='it worked!')

    @RetryRequest(retries=3, wait=0, verbose=True, tag='fail_n_times_decorated')
    def fail_n_times_decorated(self, n, status_code, message):
        if self.counter < n:
            self.counter += 1
            response =  MockResponse(status_code=status_code, reason=message)
            raise MyException('Fail n times exception: ' + str(self.counter), response=response)
        self.reset()
        return MockResponse(status_code=200, text=message)

    def fail_n_times(self, n, result):
        if self.counter < n:
            self.counter += 1
            return MockResponse(status_code=503, text=result)
        self.reset()
        return MockResponse(status_code=200, text=result)

    def dont_fail(self, result):
        return MockResponse(status_code=200, text=result)

    def always_fail(self, result):
        return MockResponse(status_code=503, text=result)

    def reset(self):
        self.counter = 0

def test_retry_request():

    failer = Failer()

    with_retry = RetryRequest(retries=3, wait=0, verbose=True)
    
    print '\n\ndon\'t fail', '-' * 60

    ## test something that doesn't fail
    response = with_retry(failer.dont_fail)('didn\'t fail!')
    assert response.status_code == 200

    print 'always fail', '-' * 60

    ## test something that totally borks
    response = with_retry(failer.always_fail)('failed!')
    assert response.status_code == 503

    print 'fail 2 times', '-' * 60

    ## fail n times then succeed
    response = with_retry(failer.fail_n_times)(2, 'fail 2 times')
    assert response.status_code == 200

    print 'fail 2 times', '-' * 60

    response = failer.fail_n_times_decorated(2, 503, 'fail 2 times')

    print 'fail 2 times', '-' * 60

    response = with_retry(failer.fail_n_times_exception)(2, 502, 'fail 2 times')

    print 'fail 10 times', '-' * 60

    assert_raises(Exception, with_retry(failer.fail_n_times_exception), 10, 502, 'fail 10 times')

    print 'errmsg', '-' * 60
    failer.reset()
    with_retry = RetryRequest(retries=3, wait=0, retryable_errors=['The specified key does not exist.'], verbose=True)
    response = with_retry(failer.fail_n_times_exception)(2, 500, 'The specified key does not exist.')
    assert response.status_code==200

def test_as_decorator():

    @RetryRequest(retries=3, verbose=True)
    def foo(x,y):
        """docstring of foo"""
        if x + y < 0:
            raise Exception('Foobar exception!')
        return DictObject(status_code=200, text=(x + y))

    assert foo.__name__ == 'foo'
    assert foo.__doc__ == "docstring of foo"

    assert foo(8,3).text == 11
    assert_raises(Exception, foo, -8, 3)


def test_double_wrapped():

    print '\n', '^v' * 30
    failer = Failer()

    with_retry = RetryRequest(retries=3, wait=0,
                              retry_status_codes=[],
                              retryable_errors=['The specified key does not exist.'],
                              verbose=True, tag='key does not exist')

    response = with_retry(failer.fail_n_times_decorated)(2, 500, 'The specified key does not exist.')



