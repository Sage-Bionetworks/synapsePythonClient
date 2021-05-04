import random
import sys
import logging

from synapseclient.core.logging_setup import DEBUG_LOGGER_NAME, DEFAULT_LOGGER_NAME
from synapseclient.core.utils import is_json
from synapseclient.core.dozer import doze


# def _handle_status_code(response, retry_status_codes, retry_info, logger):
#     retry = False
#     if response is None or not hasattr(response, 'status_code'):
#         return retry
#     if response.status_code in retry_status_codes:
#         response_message = _get_message(response)
#         retry = True
#         logger.debug("retrying on status code: %s" % str(response.status_code))
#         # TODO: this was originally printed regardless of 'verbose' was that behavior correct?
#         logger.debug(str(response_message))
#         if (response.status_code == 429) and (retry_info['wait'] > 10):
#             logger.warning('%s...\n' % response_message)
#             logger.warning('Retrying in %i seconds' % retry_info['wait'])
#
#     elif response.status_code not in range(200, 299):
#         # For all other non 200 messages look for retryable errors in the body or reason field
#         response_message = _get_message(response)
#         if any([msg.lower() in response_message.lower() for msg in retry_info['retry_errors']]):
#             retry = True
#             logger.debug('retrying %s' % response_message)
#         # special case for message throttling
#         elif 'Please slow down.  You may send a maximum of 10 message' in response:
#             retry = True
#             retry_info['wait'] = 16
#             logger.debug("retrying " + response_message)
#     # retry_info['retry'] = retry
#     return retry


def with_retry_network(function, verbose=False,
                       retry_status_codes=[429, 500, 502, 503, 504], retry_errors=[], retry_exceptions=[],
                       retries=3, wait=1, back_off=2, max_wait=30):
    logger = _get_logger(verbose)

    def _handle_status_code(response):
        retry = False
        if response is None or not hasattr(response, 'status_code'):
            return retry
        if response.status_code in retry_status_codes:
            response_message = _get_message(response)
            retry = True
            logger.debug("retrying on status code: %s" % str(response.status_code))
            # TODO: this was originally printed regardless of 'verbose' was that behavior correct?
            logger.debug(str(response_message))
            # if (response.status_code == 429) and (wait > 10):
            #     logger.warning('%s...\n' % response_message)
            #     logger.warning('Retrying in %i seconds' % wait)

        elif response.status_code not in range(200, 299):
            # For all other non 200 messages look for retryable errors in the body or reason field
            response_message = _get_message(response)
            if any([msg.lower() in response_message.lower() for msg in retry_errors]):
                retry = True
                logger.debug('retrying %s' % response_message)
            # special case for message throttling
            elif 'Please slow down.  You may send a maximum of 10 message' in response:
                logger.debug("retrying " + response_message)
                raise SynapseThrottling(response_message, wait=16)
        return retry
    return with_retry(function, _handle_status_code, verbose=verbose,
                      retry_errors=retry_errors, retry_exceptions=retry_exceptions, retries=retries, wait=wait,
                      back_off=back_off, max_wait=max_wait)


def with_retry(function, retry_evaluator=None, verbose=False, retry_errors=[], retry_exceptions=[],
               retries=3, wait=1, back_off=2, max_wait=30):
    """
    Retries the given function under certain conditions.

    :param function:           A function with no arguments.  If arguments are needed, use a lambda (see example).
    :param retry_evaluator:    Determined whether to retry again.
    # :param retry_status_codes: What status codes to retry upon in the case of a SynapseHTTPError.
    :param retry_errors:       What reasons to retry upon, if function().response.json()['reason'] exists.
    :param retry_exceptions:   What types of exceptions, specified as strings, to retry upon.
    :param retries:            How many times to retry maximum.
    :param wait:               How many seconds to wait between retries.
    :param back_off:           Exponential constant to increase wait for between progressive failures.
    :param max_wait:           How long for the max wait time, default is 30 secs.

    :returns: function()

    Example::

        def foo(a, b, c): return [a, b, c]
        result = self._with_retry(lambda: foo("1", "2", "3"), **STANDARD_RETRY_PARAMS)
    """

    logger = _get_logger(verbose)

    # Retry until we succeed or run out of tries
    total_wait = 0
    while True:
        # Start with a clean slate
        exc = None
        exc_info = None
        retry = False
        response = None

        # Try making the call
        try:
            response = function()
            if retry_evaluator:
                retry = retry_evaluator(response)
        except SynapseThrottling as throttling_ex:
            retry = True
            wait = throttling_ex.wait
        except Exception as ex:
            exc = ex
            exc_info = sys.exc_info()
            logger.debug("calling %s resulted in an Exception" % function)
            if hasattr(ex, 'response'):
                response = ex.response

        # Check if we got a retry-able exception
        if exc is not None:
            if (exc.__class__.__name__ in retry_exceptions or
                    exc.__class__ in retry_exceptions or
                    not retry_errors or
                    any([msg.lower() in str(exc_info[1]).lower() for msg in retry_errors])):
                retry = True
                logger.debug("retrying exception: " + str(exc))

        # Wait then retry
        retries -= 1
        if retries >= 0 and retry:
            randomized_wait = wait*random.uniform(0.5, 1.5)
            logger.debug(('total wait time {total_wait:5.0f} seconds\n '
                          '... Retrying in {wait:5.1f} seconds...'.format(total_wait=total_wait, wait=randomized_wait)))
            total_wait += randomized_wait
            doze(randomized_wait)
            wait = min(max_wait, wait*back_off)
            continue

        # Out of retries, re-raise the exception or return the response
        if exc_info is not None and exc_info[0] is not None:
            logger.debug("retries have run out. re-raising the exception", exc_info=True)
            raise exc
        return response


def _get_logger(verbose):
    if verbose:
        logger = logging.getLogger(DEBUG_LOGGER_NAME)
    else:
        logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    return logger


def _get_message(response):
    """
    Extracts the message body or a response object by checking for a json response and returning the reason otherwise
    getting body.
    """
    try:
        if is_json(response.headers.get('content-type', None)):
            json = response.json()
            return json.get('reason', None)
        else:
            # if the response is not JSON, return the text content
            return response.text
    except (AttributeError, ValueError):
        # The response can be truncated. In which case, the message cannot be retrieved.
        return None


class SynapseThrottling(Exception):
    def __init__(self, message, wait):
        super().__init__(message)
        self.wait = wait
