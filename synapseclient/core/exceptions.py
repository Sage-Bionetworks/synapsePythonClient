"""
**********
Exceptions
**********

.. autoclass:: synapseclient.exceptions.SynapseError
.. autoclass:: synapseclient.exceptions.SynapseAuthenticationError
.. autoclass:: synapseclient.exceptions.SynapseFileCacheError
.. autoclass:: synapseclient.exceptions.SynapseMalformedEntityError
.. autoclass:: synapseclient.exceptions.SynapseProvenanceError
.. autoclass:: synapseclient.exceptions.SynapseHTTPError

"""

import requests

from synapseclient.core import utils


class SynapseError(Exception):
    """Generic exception thrown by the client."""


class SynapseMd5MismatchError(SynapseError, IOError):
    """Error raised when MD5 computed for a download file fails to match the MD5 of its file handle."""


class SynapseFileNotFoundError(SynapseError):
    """Error thrown when a local file is not found in Synapse."""


class SynapseTimeoutError(SynapseError):
    """Timed out waiting for response from Synapse."""


class SynapseAuthenticationError(SynapseError):
    """Unauthorized access."""


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
    `HTTPError <http://docs.python-requests.org/en/latest/api/?highlight=exceptions#requests.exceptions.HTTPError>`_"""


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
        message = '%s Client Error: %s' % (response.status_code, response.reason)

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
        message = '%s Server Error: %s' % (response.status_code, response.reason)

    if message is not None:
        # Append the server's JSON error message
        if utils.is_json(response.headers.get('content-type', None)) and 'reason' in response.json():
            message += "\n%s" % response.json()['reason']
        else:
            message += "\n%s" % response.text

        if verbose:
            try:
                # Append the request sent
                message += "\n\n>>>>>> Request <<<<<<\n%s %s" % (response.request.url, response.request.method)
                message += "\n>>> Headers: %s" % response.request.headers
                message += "\n>>> Body: %s" % response.request.body
            except:
                message += "\nCould not append all request info"

            try:
                # Append the response received
                message += "\n\n>>>>>> Response <<<<<<\n%s" % str(response)
                message += "\n>>> Headers: %s" % response.headers
                message += "\n>>> Body: %s\n\n" % response.text
            except:
                message += "\nCould not append all response info"

        raise SynapseHTTPError(message, response=response)
