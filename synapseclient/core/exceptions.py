"""Contains all of the exceptions that can be thrown within this Python client as well
as handling error cases for HTTP requests."""

import logging
from typing import Union

import httpx
import requests

from synapseclient.core import utils


class SynapseError(Exception):
    """Generic exception thrown by the client."""


class SynapseMd5MismatchError(SynapseError, IOError):
    """Error raised when MD5 computed for a download file fails to match the MD5 of its file handle."""


class SynapseFileNotFoundError(SynapseError):
    """Error thrown when a local file is not found in Synapse."""


class SynapseNotFoundError(SynapseError):
    """Error thrown when a requested resource is not found in Synapse."""


class SynapseTimeoutError(SynapseError):
    """Timed out waiting for response from Synapse."""


class SynapseAuthenticationError(SynapseError):
    """Authentication errors."""


class SynapseAuthorizationError(SynapseError):
    """Authorization errors."""


class SynapseNoCredentialsError(SynapseAuthenticationError):
    """No credentials for authentication"""


class SynapseFileCacheError(SynapseError):
    """Error related to local file storage."""


class SynapseMalformedEntityError(SynapseError):
    """Unexpected structure of Entities."""


class SynapseUnmetAccessRestrictions(SynapseError):
    """Request cannot be completed due to unmet access restrictions."""


class SynapseProvenanceError(SynapseError):
    """Incorrect usage of provenance objects."""


class SynapseHTTPError(SynapseError, requests.exceptions.HTTPError):
    """Wraps recognized HTTP errors.  See
    `HTTPError <http://docs.python-requests.org/en/latest/api/?highlight=exceptions#requests.exceptions.HTTPError>`_
    """


class SynapseUploadAbortedException(SynapseError):
    """Raised when a worker thread detects the upload was
    aborted and stops further processing."""


class SynapseDownloadAbortedException(SynapseError):
    """Raised when a worker thread detects the download was
    aborted and stops further processing."""


class SynapseUploadFailedException(SynapseError):
    """Raised when an upload failed. Should be chained to a cause Exception"""


def _get_message(response: httpx.Response, logger: logging.Logger) -> Union[str, None]:
    """Extracts the message body or a response object by checking for a json response
    and returning the reason otherwise getting body.
    """
    if utils.is_json(response.headers.get("content-type", None)):
        json = response.json()
        return json.get("reason", None)
    else:
        # if the response is not JSON, return the text content
        return response.text


CLIENT_ERROR = "Client Error:"
SERVER_ERROR = "Server Error:"
RESPONSE_PREFIX = ">>>>>> Response <<<<<<"
REQUEST_PREFIX = ">>>>>> Request <<<<<<"
HEADERS_PREFIX = ">>> Headers: "
BODY_PREFIX = ">>> Body: "
UNABLE_TO_APPEND_REQUEST = "Could not append all request info"
UNABLE_TO_APPEND_RESPONSE = "Could not append all response info"


def _raise_for_status(response, verbose=False):
    """
    Replacement for requests.response.raise_for_status().
    Catches and wraps any Synapse-specific HTTP errors with appropriate text.
    """

    message = None

    # TODO: Add more meaningful Synapse-specific messages to each error code
    # TODO: For some status codes, throw other types of exceptions
    if 400 <= response.status_code < 500:
        # TODOs:
        # 400: 'bad_request'
        # 401: 'unauthorized'
        # 402: 'payment_required'
        # 403: 'forbidden'
        # 404: 'not_found'
        # 405: 'method_not_allowed'
        # 406: 'not_acceptable'
        # 407: 'proxy_authentication_required'
        # 408: 'request_timeout'
        # 409: 'conflict'
        # 410: 'gone'
        # 411: 'length_required'
        # 412: 'precondition_failed'
        # 413: 'request_entity_too_large'
        # 414: 'request_uri_too_large'
        # 415: 'unsupported_media_type'
        # 416: 'requested_range_not_satisfiable'
        # 417: 'expectation_failed'
        # 418: 'im_a_teapot'
        # 422: 'unprocessable_entity'
        # 423: 'locked'
        # 424: 'failed_dependency'
        # 425: 'unordered_collection'
        # 426: 'upgrade_required'
        # 428: 'precondition_required'
        # 429: 'too_many_requests'
        # 431: 'header_fields_too_large'
        # 444: 'no_response'
        # 449: 'retry_with'
        # 450: 'blocked_by_windows_parental_controls'
        # 451: 'unavailable_for_legal_reasons'
        # 499: 'client_closed_request'
        message = f"{response.status_code} {CLIENT_ERROR} {response.reason}"

    elif 500 <= response.status_code < 600:
        # TODOs:
        # 500: 'internal_server_error'
        # 501: 'not_implemented'
        # 502: 'bad_gateway'
        # 503: 'service_unavailable'
        # 504: 'gateway_timeout'
        # 505: 'http_version_not_supported'
        # 506: 'variant_also_negotiates'
        # 507: 'insufficient_storage'
        # 509: 'bandwidth_limit_exceeded'
        # 510: 'not_extended'
        message = f"{response.status_code} {SERVER_ERROR} {response.reason}"

    if message is not None:
        # Append the server's JSON error message
        if (
            utils.is_json(response.headers.get("content-type", None))
            and "reason" in response.json()
        ):
            message += f"\n{response.json()['reason']}"
        else:
            message += f"\n{response.text}"

        if verbose:
            try:
                # Append the request sent
                message += f"\n\n{REQUEST_PREFIX}\n{response.request.url} {response.request.method}"
                message += f"\n{HEADERS_PREFIX}{response.request.headers}"
                message += f"\n{BODY_PREFIX}{response.request.body}"
            except:  # noqa
                message += f"\n{UNABLE_TO_APPEND_REQUEST}"

            try:
                # Append the response received
                message += f"\n\n{RESPONSE_PREFIX}\n{str(response)}"
                message += f"\n{HEADERS_PREFIX}{response.headers}"
                message += f"\n{BODY_PREFIX}{response.text}\n\n"
            except:  # noqa
                message += f"\n{UNABLE_TO_APPEND_RESPONSE}"

        raise SynapseHTTPError(message, response=response)


def _raise_for_status_httpx(
    response: httpx.Response,
    logger: logging.Logger,
    verbose: bool = False,
    read_response_content: bool = True,
) -> None:
    """
    Replacement for requests.response.raise_for_status().
    Catches and wraps any Synapse-specific HTTP errors with appropriate text with the
    HTTPX library in mind.

    Arguments:
        response: The response object from the HTTPX request.
        logger: The logger object to log any exceptions.
        verbose: If True, the request and response information will be appended to the
            error message.
        read_response_content: If True, the response content will be read and appended
            to the error message. If False, the response content will not be read and
            appended to the error message.
    """

    message = None

    # TODO: Add more meaningful Synapse-specific messages to each error code
    # TODO: For some status codes, throw other types of exceptions
    if 400 <= response.status_code < 500:
        # TODOs:
        # 400: 'bad_request'
        # 401: 'unauthorized'
        # 402: 'payment_required'
        # 403: 'forbidden'
        # 404: 'not_found'
        # 405: 'method_not_allowed'
        # 406: 'not_acceptable'
        # 407: 'proxy_authentication_required'
        # 408: 'request_timeout'
        # 409: 'conflict'
        # 410: 'gone'
        # 411: 'length_required'
        # 412: 'precondition_failed'
        # 413: 'request_entity_too_large'
        # 414: 'request_uri_too_large'
        # 415: 'unsupported_media_type'
        # 416: 'requested_range_not_satisfiable'
        # 417: 'expectation_failed'
        # 418: 'im_a_teapot'
        # 422: 'unprocessable_entity'
        # 423: 'locked'
        # 424: 'failed_dependency'
        # 425: 'unordered_collection'
        # 426: 'upgrade_required'
        # 428: 'precondition_required'
        # 429: 'too_many_requests'
        # 431: 'header_fields_too_large'
        # 444: 'no_response'
        # 449: 'retry_with'
        # 450: 'blocked_by_windows_parental_controls'
        # 451: 'unavailable_for_legal_reasons'
        # 499: 'client_closed_request'
        message_body = _get_message(response, logger) if read_response_content else ""
        message = f"{response.status_code} {CLIENT_ERROR} {message_body}"

    elif 500 <= response.status_code < 600:
        # TODOs:
        # 500: 'internal_server_error'
        # 501: 'not_implemented'
        # 502: 'bad_gateway'
        # 503: 'service_unavailable'
        # 504: 'gateway_timeout'
        # 505: 'http_version_not_supported'
        # 506: 'variant_also_negotiates'
        # 507: 'insufficient_storage'
        # 509: 'bandwidth_limit_exceeded'
        # 510: 'not_extended'
        message_body = _get_message(response, logger) if read_response_content else ""
        message = f"{response.status_code} {SERVER_ERROR} {message_body}"

    if message is not None:
        if verbose:
            try:
                # Append the request sent
                message += f"\n\n{REQUEST_PREFIX}\n{response.request.url} {response.request.method}"
                message += f"\n{HEADERS_PREFIX}{response.request.headers}"
                message += f"\n{BODY_PREFIX}{response.request.content}"
            except Exception:  # noqa
                logger.exception(UNABLE_TO_APPEND_REQUEST)
                message += f"\n{UNABLE_TO_APPEND_REQUEST}"

            try:
                # Append the response received
                message += f"\n\n{RESPONSE_PREFIX}\n{str(response)}"
                message += f"\n{HEADERS_PREFIX}{response.headers}"
                if read_response_content:
                    message += f"\n{BODY_PREFIX}{message_body}\n\n"
            except Exception:  # noqa
                logger.exception(UNABLE_TO_APPEND_RESPONSE)
                message += f"\n{UNABLE_TO_APPEND_RESPONSE}"

        raise SynapseHTTPError(message, response=response)
