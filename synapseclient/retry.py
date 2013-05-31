import functools
import sys
import time

from synapseclient.utils import _to_iterable


class RetryRequest(object):
    """
    A decorator that wraps calls to HTTP methods in the requests library in a
    retry function, with various settings.
    """
    ## This class is a decorator factory, described here:
    ## Python Class Based Decorator with parameters that can decorate a method or a function
    ## http://stackoverflow.com/a/9417088/199166
    def __init__(self, retry_status_codes=[502,503], retryable_errors=[], retryable_exceptions=[], retries=3, wait=1, back_off=2, verbose=False, tag='RetryRequest'):
        self.retry_status_codes = _to_iterable(retry_status_codes)
        self.retries = retries
        self.wait = wait
        self.back_off = back_off
        self.verbose = verbose
        self.retryable_errors = retryable_errors
        self.retryable_exceptions = retryable_exceptions
        self.tag = tag

    def __call__(self, fn):
        @functools.wraps(fn)
        def with_retry(*args, **kwargs):
            ## make local copies of these variables, so we can modify them safely
            retries = self.retries
            wait = self.wait

            if self.verbose=='debug':
                tags = []
                a = with_retry
                while a:
                    if hasattr(a, 'tag'):
                        tags.append(a.tag)
                    if hasattr(a, '__wrapped__'):
                        a = a.__wrapped__
                    else:
                        a = None
                print 'tag chain=',tags

            ## retry 'til we succeed or run out of tries
            while True:
                ## start with a clean slate
                exc_info = None
                retry = False
                response = None

                ## try making the call
                try:
                    response = fn(*args, **kwargs)
                except Exception as ex:
                    exc_info = sys.exc_info()
                    if self.verbose=='debug':
                        print '[%s] exception=' % with_retry.tag, str(exc_info[1])
                    if hasattr(ex,'response'):
                        response = ex.response

                ## check if we got a retryable error
                if response is not None:
                    if self.verbose=='debug':
                        print '[%s] response=' % with_retry.tag, response
                        if hasattr(response, 'reason'):
                            print '[%s] reason=' % with_retry.tag, response.reason
                        if hasattr(response, 'content'):
                            print '[%s] response.content=' % with_retry.tag, response.content
                    if hasattr(response, 'status_code') and response.status_code not in range(200,299):
                        if response.status_code in self.retry_status_codes:
                            retry = True
                        elif hasattr(response, 'headers') and response.headers['content-type'].lower().startswith('application/json'):
                            try:
                                json = response.json()
                            except (AttributeError, ValueError) as ex:
                                pass
                            else:
                                if 'reason' in json and json['reason'] in self.retryable_errors:
                                    retry = True
                        else:
                            if hasattr(response, 'content'):
                                if any([msg in response.content for msg in self.retryable_errors]):
                                    retry = True
                else:
                    if any( [isinstance(exc_info, retryable_exception) for retryable_exception in self.retryable_exceptions] ):
                        if self.verbose=='debug':
                            print '[%s] exception=' % with_retry.tag, exc_info.__class__.__name__
                        retry = True

                ## wait then retry
                retries -= 1
                if retries > 0 and retry:
                    if self.verbose:
                        sys.stdout.write('\n...retrying in %d seconds...\n' % wait)
                    time.sleep(wait)
                    wait *= self.back_off
                    continue

                ## out of retries, reraise the exception or return the response
                if exc_info:
                    ## re-raise exception, preserving original stack trace
                    raise exc_info[0], exc_info[1], exc_info[2]
                return response

        ## Provide a hook to get back the wrapped function
        ## functools.wraps does this in Python 3.x
        with_retry.__wrapped__ = fn
        with_retry.tag = self.tag

        ## return the wrapper function
        return with_retry

