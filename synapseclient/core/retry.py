"""A helper tool that allows the Python client to
make more than one attempt at connecting to the server if initially met
with an error. These retry attempts can be made under certain conditions,
i.e. for certain status codes, connection errors, and/or connection exceptions.


"""

import asyncio
import logging
import random
import sys
import time
from logging import Logger
from typing import Any, Coroutine, List, Tuple, Type, Union

import httpx
from opentelemetry import trace

from synapseclient.core.dozer import doze
from synapseclient.core.logging_setup import DEBUG_LOGGER_NAME, DEFAULT_LOGGER_NAME
from synapseclient.core.utils import is_json

tracer = trace.get_tracer("synapseclient")

# All of these constants are in seconds
DEFAULT_RETRIES = 3
DEFAULT_WAIT = 1
DEFAULT_BACK_OFF = 2
DEFAULT_MAX_WAIT = 30


DEFAULT_BASE_WAIT_ASYNC = 0.001

DEFAULT_WAIT_RANDOM_LOWER_ASYNC = 0.01
DEFAULT_WAIT_RANDOM_UPPER_ASYNC = 0.1

DEFAULT_BACK_OFF_FACTOR_ASYNC = 2
DEFAULT_MAX_BACK_OFF_ASYNC = 10
DEFAULT_MAX_WAIT_BEFORE_FAIL_ASYNC = 20 * 60

DEFAULT_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

# strings that may appear in responses that suggest a retryable condition
RETRYABLE_CONNECTION_ERRORS = [
    "proxy error",
    "slow down",
    "timeout",
    "timed out",
    "connection reset by peer",
    "unknown ssl protocol error",
    "couldn't connect to host",
    "slowdown",
    "try again",
]

# Exceptions that may be retryable. These are socket level exceptions
# not associated with an HTTP response
RETRYABLE_CONNECTION_EXCEPTIONS = [
    "ChunkedEncodingError",
    "ConnectionError",
    "ConnectionResetError",
    "Timeout",
    "timeout",
    "ReadError",
    "ReadTimeout",
    # HTTPX Specific connection exceptions:
    "RemoteProtocolError",
    "TimeoutException",
    "ConnectError",
    "ConnectTimeout",
    # SSL Specific exceptions:
    "SSLZeroReturnError",
]

DEBUG_EXCEPTION = "calling %s resulted in an Exception"


def with_retry(
    function,
    verbose=False,
    retry_status_codes=[429, 500, 502, 503, 504],
    expected_status_codes=[],
    retry_errors=[],
    retry_exceptions=[],
    retries=DEFAULT_RETRIES,
    wait=DEFAULT_WAIT,
    back_off=DEFAULT_BACK_OFF,
    max_wait=DEFAULT_MAX_WAIT,
):
    """
    Retries the given function under certain conditions.

    Arguments:
        function: A function with no arguments.  If arguments are needed, use a lambda (see example).
        retry_status_codes: What status codes to retry upon in the case of a SynapseHTTPError.
        expected_status_codes: If specified responses with any other status codes result in a retry.
        retry_errors: What reasons to retry upon, if function().response.json()['reason'] exists.
        retry_exceptions: What types of exceptions, specified as strings or Exception classes, to retry upon.
        retries: How many times to retry maximum.
        wait: How many seconds to wait between retries.
        back_off: Exponential constant to increase wait for between progressive failures.
        max_wait: back_off between requests will not exceed this value

    Returns:
        function()

    Example: Using with_retry
        Using ``with_retry`` to consolidate inputs into a list.

            from synapseclient.core.retry import with_retry

            def foo(a, b, c): return [a, b, c]
            result = with_retry(lambda: foo("1", "2", "3"), **STANDARD_RETRY_PARAMS)
    """

    if verbose:
        logger = logging.getLogger(DEBUG_LOGGER_NAME)
    else:
        logger = logging.getLogger(DEFAULT_LOGGER_NAME)

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
        except Exception as ex:
            exc = ex
            exc_info = sys.exc_info()
            logger.debug(DEBUG_EXCEPTION, function, exc_info=True)
            if hasattr(ex, "response"):
                response = ex.response

        # Check if we got a retry-able error
        if response is not None and hasattr(response, "status_code"):
            if (
                expected_status_codes
                and response.status_code not in expected_status_codes
            ) or (retry_status_codes and response.status_code in retry_status_codes):
                response_message = _get_message(response)
                retry = True
                logger.debug("retrying on status code: %s" % str(response.status_code))
                # TODO: this was originally printed regardless of 'verbose' was that behavior correct?
                logger.debug(str(response_message))
                if (response.status_code == 429) and (wait > 10):
                    logger.warning("%s...\n" % response_message)
                    logger.warning("Retrying in %i seconds" % wait)

            elif response.status_code not in range(200, 299):
                # For all other non 200 messages look for retryable errors in the body or reason field
                response_message = _get_message(response)
                if any(
                    [msg.lower() in response_message.lower() for msg in retry_errors]
                ):
                    retry = True
                    logger.debug("retrying %s" % response_message)
                # special case for message throttling
                elif (
                    "Please slow down.  You may send a maximum of 10 message"
                    in response
                ):
                    retry = True
                    wait = 16
                    logger.debug("retrying " + response_message)

        # Check if we got a retry-able exception
        if exc is not None:
            if (
                exc.__class__.__name__ in retry_exceptions
                or exc.__class__ in retry_exceptions
                or any(
                    [msg.lower() in str(exc_info[1]).lower() for msg in retry_errors]
                )
            ):
                retry = True
                logger.debug("retrying exception: " + str(exc))

        # Wait then retry
        retries -= 1
        if retries >= 0 and retry:
            randomized_wait = wait * random.uniform(0.5, 1.5)
            logger.debug(
                "total wait time {total_wait:5.0f} seconds\n "
                "... Retrying in {wait:5.1f} seconds...".format(
                    total_wait=total_wait, wait=randomized_wait
                )
            )
            total_wait += randomized_wait
            doze(randomized_wait)
            wait = min(max_wait, wait * back_off)
            continue

        # Out of retries, re-raise the exception or return the response
        if exc_info is not None and exc_info[0] is not None:
            logger.debug(
                "retries have run out. re-raising the exception", exc_info=True
            )
            raise exc
        return response


def calculate_exponential_backoff(
    retries: int,
    base_wait: float,
    wait_random_lower: float,
    wait_random_upper: float,
    back_off_factor: float,
    max_back_off: float,
) -> float:
    """
    Handle calculating the exponential backoff.

    Arguments:
        retries: The number of retries that have been attempted
        base_wait: The base wait time
        wait_random_lower: The lower bound of the random wait time
        wait_random_upper: The upper bound of the random wait time
        back_off_factor: The factor to increase the wait time by for each retry
        max_back_off: The maximum wait time

    Returns:
        The total wait time
    """
    random_jitter = random.uniform(wait_random_lower, wait_random_upper)
    time_to_wait = min(
        (base_wait * (back_off_factor**retries)) + random_jitter,
        max_back_off,
    )
    return time_to_wait


def _assign_default_values(
    retry_status_codes: List[int] = None,
    expected_status_codes: List[int] = None,
    retry_errors: List[str] = None,
    retry_exceptions: List[Union[Exception, str]] = None,
    verbose: bool = False,
) -> Tuple[List[int], List[int], List[str], List[Union[Exception, str]], Logger]:
    """Assigns default values to the retry parameters."""
    if not retry_status_codes:
        retry_status_codes = [429, 500, 502, 503, 504]
    if not expected_status_codes:
        expected_status_codes = []
    if not retry_errors:
        retry_errors = []
    if not retry_exceptions:
        retry_exceptions = []

    if verbose:
        logger = logging.getLogger(DEBUG_LOGGER_NAME)
    else:
        logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    return (
        retry_status_codes,
        expected_status_codes,
        retry_errors,
        retry_exceptions,
        logger,
    )


async def with_retry_time_based_async(
    function: Coroutine[Any, Any, Any],
    verbose: bool = False,
    retry_status_codes: List[int] = None,
    expected_status_codes: List[int] = None,
    retry_errors: List[str] = None,
    retry_exceptions: List[Union[Exception, str]] = None,
    retry_base_wait: float = DEFAULT_BASE_WAIT_ASYNC,
    retry_wait_random_lower: float = DEFAULT_WAIT_RANDOM_LOWER_ASYNC,
    retry_wait_random_upper: float = DEFAULT_WAIT_RANDOM_UPPER_ASYNC,
    retry_back_off_factor: float = DEFAULT_BACK_OFF_FACTOR_ASYNC,
    retry_max_back_off: float = DEFAULT_MAX_BACK_OFF_ASYNC,
    retry_max_wait_before_failure: float = DEFAULT_MAX_WAIT_BEFORE_FAIL_ASYNC,
    read_response_content: bool = True,
) -> Union[Exception, httpx.Response, Any, None]:
    """
    Retries the given function under certain conditions. This is created such that it
    will retry an unbounded number of times until the maximum wait time is reached. The
    backoff is calculated using an exponential backoff algorithm with a random jitter.
    The maximum backoff inbetween retries is capped at `retry_max_back_off`.

    Arguments:
        verbose: Whether to log debug messages
        function: A function with no arguments. If arguments are needed, use a lambda
            (see example).
        retry_status_codes: What status codes to retry upon in the case of a
            SynapseHTTPError.
        expected_status_codes: If specified responses with any other status codes result
            in a retry.
        retry_errors: What reasons to retry upon, if
            `function().response.json()['reason']` exists.
        retry_exceptions: What types of exceptions, specified as strings or Exception
            classes, to retry upon.
        retry_base_wait: The base wait time inbetween retries.
        retry_wait_random_lower: The lower bound of the random wait time.
        retry_wait_random_upper: The upper bound of the random wait time.
        retry_back_off_factor: The factor to increase the wait time by for each retry.
        retry_max_back_off: The maximum wait time.
        retry_max_wait_before_failure: The maximum wait time before failure.
        read_response_content: Whether to read the response content for HTTP requests.

    Example: Using with_retry
        Using ``with_retry_time_based_async`` to consolidate inputs into a list.

            from synapseclient.core.retry import with_retry_time_based_async

            async def foo(a, b, c): return [a, b, c]
            result = await with_retry_time_based_async(lambda: foo("1", "2", "3"))
    """
    (
        retry_status_codes,
        expected_status_codes,
        retry_errors,
        retry_exceptions,
        logger,
    ) = _assign_default_values(
        retry_status_codes=retry_status_codes,
        expected_status_codes=expected_status_codes,
        retry_errors=retry_errors,
        retry_exceptions=retry_exceptions,
        verbose=verbose,
    )

    # Retry until we succeed or run past the maximum wait time
    total_wait = 0
    retries = -1
    while True:
        caught_exception = None
        caught_exception_info = None
        response = None
        current_span = trace.get_current_span()
        current_span.set_attribute("synapse.retries", str(retries + 1))

        try:
            response = await function()
        except Exception as ex:
            caught_exception = ex
            caught_exception_info = sys.exc_info()
            logger.debug(DEBUG_EXCEPTION, function, exc_info=True)
            if hasattr(ex, "response"):
                response = ex.response

        retry = _is_retryable(
            response=response,
            caught_exception=caught_exception,
            caught_exception_info=caught_exception_info,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            retry_exceptions=retry_exceptions,
            retry_errors=retry_errors,
        )

        # Wait then retry
        retries += 1
        if total_wait < retry_max_wait_before_failure and retry:
            _log_for_retry(
                logger=logger,
                response=response,
                caught_exception=caught_exception,
                read_response_content=read_response_content,
            )

            backoff_wait = calculate_exponential_backoff(
                retries=retries,
                base_wait=retry_base_wait,
                wait_random_lower=retry_wait_random_lower,
                wait_random_upper=retry_wait_random_upper,
                back_off_factor=retry_back_off_factor,
                max_back_off=retry_max_back_off,
            )
            total_wait += backoff_wait
            await asyncio.sleep(backoff_wait)
            continue

        # Out of retries, re-raise the exception or return the response
        if caught_exception_info is not None and caught_exception_info[0] is not None:
            logger.debug(
                (
                    "Retries have run out. re-raising the exception: %s"
                    if retry
                    else "Raising the exception: %s"
                ),
                str(caught_exception_info[0]),
            )
            raise caught_exception
        return response


def with_retry_time_based(
    function,
    verbose: bool = False,
    retry_status_codes: List[int] = None,
    expected_status_codes: List[int] = None,
    retry_errors: List[str] = None,
    retry_exceptions: List[Union[Exception, str]] = None,
    retry_base_wait: float = DEFAULT_BASE_WAIT_ASYNC,
    retry_wait_random_lower: float = DEFAULT_WAIT_RANDOM_LOWER_ASYNC,
    retry_wait_random_upper: float = DEFAULT_WAIT_RANDOM_UPPER_ASYNC,
    retry_back_off_factor: float = DEFAULT_BACK_OFF_FACTOR_ASYNC,
    retry_max_back_off: float = DEFAULT_MAX_BACK_OFF_ASYNC,
    retry_max_wait_before_failure: float = DEFAULT_MAX_WAIT_BEFORE_FAIL_ASYNC,
    read_response_content: bool = True,
) -> Union[Exception, httpx.Response, Any, None]:
    """
    Retries the given function under certain conditions. This is created such that it
    will retry an unbounded number of times until the maximum wait time is reached. The
    backoff is calculated using an exponential backoff algorithm with a random jitter.
    The maximum backoff inbetween retries is capped at `retry_max_back_off`.

    Arguments:
        verbose: Whether to log debug messages
        function: A function with no arguments. If arguments are needed, use a lambda
            (see example).
        retry_status_codes: What status codes to retry upon in the case of a
            SynapseHTTPError.
        expected_status_codes: If specified responses with any other status codes result
            in a retry.
        retry_errors: What reasons to retry upon, if
            `function().response.json()['reason']` exists.
        retry_exceptions: What types of exceptions, specified as strings or Exception
            classes, to retry upon.
        retry_base_wait: The base wait time inbetween retries.
        retry_wait_random_lower: The lower bound of the random wait time.
        retry_wait_random_upper: The upper bound of the random wait time.
        retry_back_off_factor: The factor to increase the wait time by for each retry.
        retry_max_back_off: The maximum wait time.
        retry_max_wait_before_failure: The maximum wait time before failure.
        read_response_content: Whether to read the response content for HTTP requests.

    Example: Using with_retry
        Using ``with_retry_time_based`` to consolidate inputs into a list.

            from synapseclient.core.retry import with_retry_time_based

            async def foo(a, b, c): return [a, b, c]
            result = with_retry_time_based(lambda: foo("1", "2", "3"))
    """
    (
        retry_status_codes,
        expected_status_codes,
        retry_errors,
        retry_exceptions,
        logger,
    ) = _assign_default_values(
        retry_status_codes=retry_status_codes,
        expected_status_codes=expected_status_codes,
        retry_errors=retry_errors,
        retry_exceptions=retry_exceptions,
        verbose=verbose,
    )

    # Retry until we succeed or run past the maximum wait time
    total_wait = 0
    retries = -1
    while True:
        caught_exception = None
        caught_exception_info = None
        response = None
        current_span = trace.get_current_span()
        current_span.set_attribute("synapse.retries", str(retries + 1))

        try:
            response = function()
        except Exception as ex:
            caught_exception = ex
            caught_exception_info = sys.exc_info()
            logger.debug(DEBUG_EXCEPTION, function, exc_info=True)
            if hasattr(ex, "response"):
                response = ex.response

        retry = _is_retryable(
            response=response,
            caught_exception=caught_exception,
            caught_exception_info=caught_exception_info,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            retry_exceptions=retry_exceptions,
            retry_errors=retry_errors,
        )

        # Wait then retry
        retries += 1
        if total_wait < retry_max_wait_before_failure and retry:
            _log_for_retry(
                logger=logger,
                response=response,
                caught_exception=caught_exception,
                read_response_content=read_response_content,
            )

            backoff_wait = calculate_exponential_backoff(
                retries=retries,
                base_wait=retry_base_wait,
                wait_random_lower=retry_wait_random_lower,
                wait_random_upper=retry_wait_random_upper,
                back_off_factor=retry_back_off_factor,
                max_back_off=retry_max_back_off,
            )
            total_wait += backoff_wait
            time.sleep(backoff_wait)
            continue

        # Out of retries, re-raise the exception or return the response
        if caught_exception_info is not None and caught_exception_info[0] is not None:
            logger.debug(
                (
                    "Retries have run out. re-raising the exception: %s"
                    if retry
                    else "Raising the exception: %s"
                ),
                str(caught_exception_info[0]),
            )
            raise caught_exception
        return response


def _is_retryable(
    response: httpx.Response,
    caught_exception: Exception,
    caught_exception_info: Tuple[Type, Exception, Any],
    expected_status_codes: List[int],
    retry_status_codes: List[int],
    retry_exceptions: List[Union[Exception, str]],
    retry_errors: List[str],
) -> bool:
    """Determines if a request should be retried based on the response and caught
    exception.

    Arguments:
        response: The response object from the request.
        caught_exception: The exception caught from the request.
        caught_exception_info: The exception info caught from the request.
        expected_status_codes: The expected status codes for the request.
        retry_status_codes: The status codes that should be retried.
        retry_exceptions: The exceptions that should be retried.
        retry_errors: The errors that should be retried.

    Returns:
        True if the request should be retried, False otherwise.
    """
    # Check if we got a retry-able HTTP error
    if response is not None and hasattr(response, "status_code"):
        if (
            expected_status_codes and response.status_code not in expected_status_codes
        ) or (response.status_code in retry_status_codes):
            return True

        elif response.status_code not in range(200, 299):
            # For all other non 200 messages look for retryable errors in the body or reason field
            response_message = _get_message(response)
            if (
                any([msg.lower() in response_message.lower() for msg in retry_errors])
                # special case for message throttling
                or response_message
                and (
                    "Please slow down.  You may send a maximum of 10 message"
                    in response_message
                )
            ):
                return True

    # Check if we got a retry-able exception
    if caught_exception is not None and (
        caught_exception.__class__.__name__ in retry_exceptions
        or caught_exception.__class__ in retry_exceptions
        or any(
            [
                msg.lower() in str(caught_exception_info[1]).lower()
                for msg in retry_errors
            ]
        )
    ):
        return True
    return False


def _log_for_retry(
    logger: logging.Logger,
    response: httpx.Response = None,
    caught_exception: Exception = None,
    read_response_content: bool = True,
) -> None:
    """Logs the retry message to debug.

    Arguments:
        logger: The logger to use for logging the retry message.
        response: The response object from the request.
        caught_exception: The exception caught from the request.
        read_response_content: Whether to read the response content for HTTP requests.
    """
    if response is not None:
        response_message = _get_message(response) if read_response_content else ""
        url_message_part = ""

        if hasattr(response, "request") and hasattr(response.request, "url"):
            url_param_part = (
                f"?{response.request.url.params}" if response.request.url.params else ""
            )
            url_message_part = f"{response.request.url.host}{response.request.url.path}{url_param_part}"
        logger.debug(
            "retrying on status code: %s - %s - %s",
            str(response.status_code),
            url_message_part,
            response_message,
        )

    elif caught_exception is not None:
        logger.debug("retrying exception: %s", str(caught_exception))


def _get_message(response):
    """Extracts the message body or a response object by checking for a json response and returning the reason otherwise
    getting body.
    """
    try:
        if is_json(response.headers.get("content-type", None)):
            json = response.json()
            return json.get("reason", None)
        else:
            # if the response is not JSON, return the text content
            return response.text
    except (AttributeError, ValueError):
        # The response can be truncated. In which case, the message cannot be retrieved.
        return None
