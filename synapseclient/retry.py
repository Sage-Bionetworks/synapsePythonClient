import sys
import time

from synapseclient.utils import _is_json


def _with_retry(function, verbose=False, \
        retry_status_codes=[502,503], retry_errors=[], retry_exceptions=[], \
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
                sys.stderr.write(ex.message+'\n') # This message will contain lots of info
            if hasattr(ex, 'response'):
                response = ex.response

        # Check if we got a retry-able error
        if response is not None:
            if response.status_code in retry_status_codes:
                retry = True

            elif response.status_code not in range(200,299):
                if _is_json(response.headers.get('content-type', None)):
                    try:
                        json = response.json()
                        if json.get('reason', None) in retry_errors:
                            retry = True
                    except (AttributeError, ValueError) as ex:
                        pass
                        
                elif any([msg in response.content for msg in retry_errors]):
                    retry = True

        # Check if we got a retry-able exception
        if exc_info is not None and exc_info[1].__class__.__name__ in retry_exceptions:
            retry = True

        # Wait then retry
        retries -= 1
        if retries >= 0 and retry:
            if verbose:
                sys.stderr.write('\n... Retrying in %d seconds...\n' % wait)
            time.sleep(wait)
            wait = min(max_wait, wait*back_off)
            continue

        # Out of retries, re-raise the exception or return the response
        if exc_info:
            # Re-raise exception, preserving original stack trace
            raise exc_info[0], exc_info[1], exc_info[2]
        return response
