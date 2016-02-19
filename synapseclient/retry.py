from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import random
import sys
import time

from synapseclient.utils import _is_json


def _with_retry(function, verbose=False, \
        retry_status_codes=[429, 502, 503, 504], retry_errors=[], retry_exceptions=[], \
        retries=3, wait=1, back_off=2, max_wait=30):
    """
    Retries the given function under certain conditions.
    
    :param function:           A function with no arguments.  If arguments are needed, use a lambda (see example).  
    :param retry_status_codes: What status codes to retry upon in the case of a SynapseHTTPError.
    :param retry_errors:       What reasons to retry upon, if function().response.json()['reason'] exists.
    :param retry_exceptions:   What types of exceptions, specified as strings, to retry upon.
    :param retries:            How many times to retry maximum.
    :param wait:               How many seconds to wait between retries.  
    :param back_off:           Exponential constant to increase wait for between progressive failures.  
    
    :returns: function()
    
    Example::
        
        def foo(a, b, c): return [a, b, c]
        result = self._with_retry(lambda: foo("1", "2", "3"), **STANDARD_RETRY_PARAMS)
    """
    
    # Retry until we succeed or run out of tries
    while True:
        # Start with a clean slate
        exc_info = None
        retry = False
        response = None

        # Try making the call
        try:
            response = function()
        except Exception as ex:
            exc_info = sys.exc_info()
            if verbose:
                sys.stderr.write(str(ex.message)+'\n')
            if hasattr(ex, 'response'):
                response = ex.response

        # Check if we got a retry-able error
        if response is not None:
            if response.status_code in retry_status_codes:
                if verbose:
                    sys.stderr.write("retrying on status code: \n" + str(response.status_code))
                retry = True

            elif response.status_code not in range(200,299):
                ## if the response is JSON, look for retryable errors in the 'reason' field
                if _is_json(response.headers.get('content-type', None)):
                    try:
                        json = response.json()
                        ## special case for message throttling
                        if 'Please slow down.  You may send a maximum of 10 message' in json.get('reason', None):
                            if verbose: sys.stderr.write("retrying"+ json.get('reason', None)+'\n')
                            retry = True
                            wait = 16
                        elif any([msg.lower() in json.get('reason', None).lower() for msg in retry_errors]):
                            if verbose: sys.stderr.write("retrying"+ json.get('reason', None)+'\n')
                            retry = True
                    except (AttributeError, ValueError) as ex:
                        pass

                ## if the response is not JSON, look for retryable errors in its text content
                elif any([msg.lower() in response.text.lower() for msg in retry_errors]):
                    if verbose: sys.stderr.write("retrying"+ response.text+'\n')
                    retry = True

        # Check if we got a retry-able exception
        if exc_info is not None:
            if exc_info[1].__class__.__name__ in retry_exceptions or any([msg.lower() in str(exc_info[1]).lower() for msg in retry_errors]):
                if verbose: sys.stderr.write("retrying exception: "+ exc_info[1].__class__.__name__ + str(exc_info[1])+'\n')
                retry = True

        # Wait then retry
        retries -= 1
        if retries >= 0 and retry:
            randomized_wait = wait*random.uniform(0.5,1.5)
            if verbose:
                sys.stderr.write('\n... Retrying in {wait} seconds...\n'.format(wait=randomized_wait))
            time.sleep(randomized_wait)
            wait = min(max_wait, wait*back_off)
            continue

        # Out of retries, re-raise the exception or return the response
        if exc_info is not None and exc_info[0] is not None:
            #import traceback
            #traceback.print_exc()
            print(exc_info[0])
            print(exc_info[1])
            print(exc_info[2])
            # Re-raise exception, preserving original stack trace
            raise exc_info[0](exc_info[1])
            #raise
        return response
