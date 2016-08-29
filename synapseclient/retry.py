from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import random
import sys
import time

from synapseclient.utils import _is_json, log_error


def _with_retry(function, verbose=False,
                retry_status_codes=[429, 500, 502, 503, 504], retry_errors=[], retry_exceptions=[],
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
    total_wait = 0
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
            log_error(str(ex), verbose)
            if hasattr(ex, 'response'):
                response = ex.response

        # Check if we got a retry-able error
        if response is not None:
            if response.status_code in retry_status_codes:
                response_message = _get_message(response)
                retry = True
                log_error("retrying on status code: %s" % str(response.status_code), verbose)
                log_error(str(response_message))
                if (response.status_code == 429) and (wait>10):
                    sys.stderr.write('%s...\n' % response_message)
                    sys.stderr.write('Retrying in %i seconds' %wait)
                
            elif response.status_code not in range(200,299):
                ## For all other non 200 messages look for retryable errors in the body or reason field
                response_message = _get_message(response)
                if any([msg.lower() in response_message.lower() for msg in retry_errors]):
                    retry = True
                    log_error('retrying %s' %response_message, verbose)
                ## special case for message throttling
                elif 'Please slow down.  You may send a maximum of 10 message' in response:
                    retry = True
                    wait = 16
                    log_error("retrying "+ response_message,  verbose)

        # Check if we got a retry-able exception
        if exc_info is not None:
            if (exc_info[1].__class__.__name__ in retry_exceptions or
                any([msg.lower() in str(exc_info[1]).lower() for msg in retry_errors])):
                retry = True
                log_error("retrying exception: "+ exc_info[1].__class__.__name__ + str(exc_info[1]), verbose)

        # Wait then retry
        retries -= 1
        if retries >= 0 and retry:
            randomized_wait = wait*random.uniform(0.5,1.5)
            log_error(('total wait time {total_wait:5.0f} seconds\n'
                       '... Retrying in {wait:5.1f} seconds...'.format(total_wait=total_wait, wait=randomized_wait)),
                       verbose)
            total_wait +=randomized_wait
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
        return response


def _get_message(response):
    """
    Extracts the message body or a response object by checking for a
    json response and returning the reason otherwise getting body.
    """
    if _is_json(response.headers.get('content-type', None)):
        try:
            json = response.json()
            return json.get('reason', None)
        except (AttributeError, ValueError) as ex:
            pass
    else:
        # if the response is not JSON, return the text content
        return response.text


