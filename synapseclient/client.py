"""
The `Synapse` object encapsulates a connection to the Synapse service and is used for building projects, uploading and
retrieving data, and recording provenance of data analysis.
"""
import collections
import collections.abc
import configparser
import csv
import deprecated
import errno
import functools
import getpass
import hashlib
import json
import logging
import mimetypes
import os
import requests
import shutil
import sys
import tempfile
import time
import typing
import urllib.parse as urllib_urlparse
import urllib.request as urllib_request
import warnings
import webbrowser
import zipfile

import synapseclient
from .annotations import (
    from_synapse_annotations,
    to_synapse_annotations,
    Annotations,
    convert_old_annotation_json,
    check_annotations_changed,
)
from .activity import Activity
import synapseclient.core.multithread_download as multithread_download
from .entity import (
    Entity,
    File,
    Folder,
    Versionable,
    split_entity_namespaces,
    is_versionable,
    is_container,
    is_synapse_entity,
)
from synapseclient.core.models.dict_object import DictObject
from .evaluation import Evaluation, Submission, SubmissionStatus
from .table import (
    Schema,
    SchemaBase,
    Column,
    TableQueryResult,
    CsvFileTable,
    EntityViewSchema,
    SubmissionViewSchema,
    Dataset,
)
from .team import UserProfile, Team, TeamMember, UserGroupHeader
from .wiki import Wiki, WikiAttachment
from synapseclient.core import cache, exceptions, utils
from synapseclient.core.constants import config_file_constants
from synapseclient.core.constants import concrete_types
from synapseclient.core import cumulative_transfer_progress
from synapseclient.core.credentials import (
    cached_sessions,
    delete_stored_credentials,
    get_default_credential_chain,
    UserLoginArgs,
)
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseMd5MismatchError,
    SynapseNoCredentialsError,
    SynapseProvenanceError,
    SynapseTimeoutError,
    SynapseUnmetAccessRestrictions,
)
from synapseclient.core.logging_setup import (
    DEFAULT_LOGGER_NAME,
    DEBUG_LOGGER_NAME,
    SILENT_LOGGER_NAME,
)
from synapseclient.core.version_check import version_check
from synapseclient.core.pool_provider import DEFAULT_NUM_THREADS
from synapseclient.core.utils import (
    id_of,
    get_properties,
    MB,
    is_json,
    extract_synapse_id_from_query,
    find_data_file_handle,
    extract_zip_file_to_directory,
    is_integer,
    require_param,
)
from synapseclient.core.retry import (
    with_retry,
    DEFAULT_RETRY_STATUS_CODES,
    RETRYABLE_CONNECTION_ERRORS,
    RETRYABLE_CONNECTION_EXCEPTIONS,
)
from synapseclient.core import sts_transfer
from synapseclient.core.upload.multipart_upload import (
    multipart_upload_file,
    multipart_upload_string,
)
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper
from synapseclient.core.upload.upload_functions import (
    upload_file_handle,
    upload_synapse_s3,
)
from synapseclient.core.dozer import doze
from typing import Union

PRODUCTION_ENDPOINTS = {
    "repoEndpoint": "https://repo-prod.prod.sagebase.org/repo/v1",
    "authEndpoint": "https://auth-prod.prod.sagebase.org/auth/v1",
    "fileHandleEndpoint": "https://file-prod.prod.sagebase.org/file/v1",
    "portalEndpoint": "https://www.synapse.org/",
}

STAGING_ENDPOINTS = {
    "repoEndpoint": "https://repo-staging.prod.sagebase.org/repo/v1",
    "authEndpoint": "https://auth-staging.prod.sagebase.org/auth/v1",
    "fileHandleEndpoint": "https://file-staging.prod.sagebase.org/file/v1",
    "portalEndpoint": "https://staging.synapse.org/",
}

CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".synapseConfig")
SESSION_FILENAME = ".session"
FILE_BUFFER_SIZE = 2 * MB
CHUNK_SIZE = 5 * MB
QUERY_LIMIT = 1000
CHUNK_UPLOAD_POLL_INTERVAL = 1  # second
ROOT_ENTITY = "syn4489"
PUBLIC = 273949  # PrincipalId of public "user"
AUTHENTICATED_USERS = 273948
DEBUG_DEFAULT = False
REDIRECT_LIMIT = 5
MAX_THREADS_CAP = 128

# Defines the standard retry policy applied to the rest methods
# The retry period needs to span a minute because sending messages is limited to 10 per 60 seconds.
STANDARD_RETRY_PARAMS = {
    "retry_status_codes": DEFAULT_RETRY_STATUS_CODES,
    "retry_errors": RETRYABLE_CONNECTION_ERRORS,
    "retry_exceptions": RETRYABLE_CONNECTION_EXCEPTIONS,
    "retries": 60,  # Retries for up to about 30 minutes
    "wait": 1,
    "max_wait": 30,
    "back_off": 2,
}

# Add additional mimetypes
mimetypes.add_type("text/x-r", ".R", strict=False)
mimetypes.add_type("text/x-r", ".r", strict=False)
mimetypes.add_type("text/tab-separated-values", ".maf", strict=False)
mimetypes.add_type("text/tab-separated-values", ".bed5", strict=False)
mimetypes.add_type("text/tab-separated-values", ".bed", strict=False)
mimetypes.add_type("text/tab-separated-values", ".vcf", strict=False)
mimetypes.add_type("text/tab-separated-values", ".sam", strict=False)
mimetypes.add_type("text/yaml", ".yaml", strict=False)
mimetypes.add_type("text/x-markdown", ".md", strict=False)
mimetypes.add_type("text/x-markdown", ".markdown", strict=False)

DEFAULT_STORAGE_LOCATION_ID = 1


def login(*args, **kwargs):
    """
    Convenience method to create a Synapse object and login.

    See :py:func:`synapseclient.Synapse.login` for arguments and usage.

    Example::

        import synapseclient
        syn = synapseclient.login()
    """

    syn = Synapse()
    syn.login(*args, **kwargs)
    return syn


class Synapse(object):
    """
    Constructs a Python client object for the Synapse repository service

    :param repoEndpoint:          Location of Synapse repository
    :param authEndpoint:          Location of authentication service
    :param fileHandleEndpoint:    Location of file service
    :param portalEndpoint:        Location of the website
    :param serviceTimeoutSeconds: Wait time before timeout (currently unused)
    :param debug:                 Print debugging messages if True
    :param skip_checks:           Skip version and endpoint checks
    :param configPath:            Path to config File with setting for Synapse
                                  defaults to ~/.synapseConfig
    :param requests_session:      a custom requests.Session object that this Synapse instance will use
                                  when making http requests

    Typically, no parameters are needed::

        import synapseclient
        syn = synapseclient.Synapse()

    See:

    - :py:func:`synapseclient.Synapse.login`
    - :py:func:`synapseclient.Synapse.setEndpoints`

    """

    # TODO: add additional boolean for write to disk?
    def __init__(
        self,
        repoEndpoint: str = None,
        authEndpoint: str = None,
        fileHandleEndpoint: str = None,
        portalEndpoint: str = None,
        debug: bool = None,
        skip_checks: bool = False,
        configPath: str = CONFIG_FILE,
        requests_session: requests.Session = None,
        cache_root_dir: str = None,
        silent: bool = None,
    ):
        self._requests_session = requests_session or requests.Session()

        cache_root_dir = (
            cache.CACHE_ROOT_DIR if cache_root_dir is None else cache_root_dir
        )

        config_debug = None
        # Check for a config file
        self.configPath = configPath
        if os.path.isfile(configPath):
            config = self.getConfigFile(configPath)
            if config.has_option("cache", "location"):
                cache_root_dir = config.get("cache", "location")
            if config.has_section("debug"):
                config_debug = True

        if debug is None:
            debug = config_debug if config_debug is not None else DEBUG_DEFAULT

        self.cache = cache.Cache(cache_root_dir)
        self._sts_token_store = sts_transfer.StsTokenStore()

        self.setEndpoints(
            repoEndpoint, authEndpoint, fileHandleEndpoint, portalEndpoint, skip_checks
        )

        self.default_headers = {
            "content-type": "application/json; charset=UTF-8",
            "Accept": "application/json; charset=UTF-8",
        }
        self.credentials = None

        if not isinstance(debug, bool):
            raise ValueError("debug must be set to a bool (either True or False)")
        self.debug = debug

        self.silent = silent
        self._init_logger()  # initializes self.logger

        self.skip_checks = skip_checks

        self.table_query_sleep = 2
        self.table_query_backoff = 1.1
        self.table_query_max_sleep = 20
        self.table_query_timeout = 600  # in seconds
        self.multi_threaded = True  # if set to True, multi threaded download will be used for http and https URLs

        transfer_config = self._get_transfer_config()
        self.max_threads = transfer_config["max_threads"]
        self.use_boto_sts_transfers = transfer_config["use_boto_sts"]

    # initialize logging
    def _init_logger(self):
        logger_name = (
            SILENT_LOGGER_NAME
            if self.silent
            else DEBUG_LOGGER_NAME
            if self.debug
            else DEFAULT_LOGGER_NAME
        )
        self.logger = logging.getLogger(logger_name)
        logging.getLogger("py.warnings").handlers = self.logger.handlers

    @property
    def max_threads(self) -> int:
        return self._max_threads

    @max_threads.setter
    def max_threads(self, value: int):
        self._max_threads = min(max(value, 1), MAX_THREADS_CAP)

    @property
    def username(self) -> Union[str, None]:
        # for backwards compatability when username was a part of the Synapse object and not in credentials
        return self.credentials.username if self.credentials is not None else None

    @functools.lru_cache()
    def getConfigFile(self, configPath: str) -> configparser.RawConfigParser:
        """
        Retrieves the client configuration information.

        :param configPath:  Path to configuration file on local file system
        :return: a RawConfigParser populated with properties from the user's configuration file.
        """

        try:
            config = configparser.RawConfigParser()
            config.read(configPath)  # Does not fail if the file does not exist
            return config
        except configparser.Error as ex:
            raise ValueError(
                "Error parsing Synapse config file: {}".format(configPath)
            ) from ex

    def setEndpoints(
        self,
        repoEndpoint: str = None,
        authEndpoint: str = None,
        fileHandleEndpoint: str = None,
        portalEndpoint: str = None,
        skip_checks: bool = False,
    ):
        """
        Sets the locations for each of the Synapse services (mostly useful for testing).

        :param repoEndpoint:          Location of synapse repository
        :param authEndpoint:          Location of authentication service
        :param fileHandleEndpoint:    Location of file service
        :param portalEndpoint:        Location of the website
        :param skip_checks:           Skip version and endpoint checks

        To switch between staging and production endpoints::

            syn.setEndpoints(**synapseclient.client.STAGING_ENDPOINTS)
            syn.setEndpoints(**synapseclient.client.PRODUCTION_ENDPOINTS)

        """

        endpoints = {
            "repoEndpoint": repoEndpoint,
            "authEndpoint": authEndpoint,
            "fileHandleEndpoint": fileHandleEndpoint,
            "portalEndpoint": portalEndpoint,
        }

        # For unspecified endpoints, first look in the config file
        config = self.getConfigFile(self.configPath)
        for point in endpoints.keys():
            if endpoints[point] is None and config.has_option("endpoints", point):
                endpoints[point] = config.get("endpoints", point)

        # Endpoints default to production
        for point in endpoints.keys():
            if endpoints[point] is None:
                endpoints[point] = PRODUCTION_ENDPOINTS[point]

            # Update endpoints if we get redirected
            if not skip_checks:
                response = self._requests_session.get(
                    endpoints[point],
                    allow_redirects=False,
                    headers=synapseclient.USER_AGENT,
                )
                if response.status_code == 301:
                    endpoints[point] = response.headers["location"]

        self.repoEndpoint = endpoints["repoEndpoint"]
        self.authEndpoint = endpoints["authEndpoint"]
        self.fileHandleEndpoint = endpoints["fileHandleEndpoint"]
        self.portalEndpoint = endpoints["portalEndpoint"]

    def login(
        self,
        email: str = None,
        password: str = None,
        apiKey: str = None,
        sessionToken: str = None,
        rememberMe: bool = False,
        silent: bool = False,
        forced: bool = False,
        authToken: str = None,
    ):
        """
        Valid combinations of login() arguments:

            - email/username and password
            - email/username and apiKey (Base64 encoded string)
            - authToken
            - sessionToken (**DEPRECATED**)

        If no login arguments are provided or only username is provided, login() will attempt to log in using
         information from these sources (in order of preference):

        #. User's personal access token from environment the variable: SYNAPSE_AUTH_TOKEN

        #. .synapseConfig file (in user home folder unless configured otherwise)

        #. cached credentials from previous `login()` where `rememberMe=True` was passed as a parameter

        :param email:        Synapse user name (or an email address associated with a Synapse account)
        :param password:     **!!WILL BE DEPRECATED!!** password. Please use authToken (Synapse personal access token)
        :param apiKey:       **!!WILL BE DEPRECATED!!** Base64 encoded Synapse API key
        :param sessionToken: **!!DEPRECATED FIELD!!** User's current session token. Using this field will ignore the
                             following fields: email, password, apiKey
        :param rememberMe:   Whether the authentication information should be cached in your operating system's
                             credential storage.
        :param authToken:    A bearer authorization token, e.g. a personal access token, can be used in lieu of a
                             password or apiKey

        **GNOME Keyring** (recommended) or **KWallet** is recommended to be installed for credential storage on
        **Linux** systems.
        If it is not installed/setup, credentials will be stored as PLAIN-TEXT file with read and write permissions for
        the current user only (chmod 600).
        On Windows and Mac OS, a default credentials storage exists so it will be preferred over the plain-text file.
        To install GNOME Keyring on Ubuntu::

            sudo apt-get install gnome-keyring

            sudo apt-get install python-dbus  #(for Python 2 installed via apt-get)
            OR
            sudo apt-get install python3-dbus #(for Python 3 installed via apt-get)
            OR
            sudo apt-get install libdbus-glib-1-dev #(for custom installation of Python or vitualenv)
            sudo pip install dbus-python #(may take a while to compile C code)

        If you are on a headless Linux session (e.g. connecting via SSH), please run the following commands before
        running your Python session::

            dbus-run-session -- bash #(replace 'bash' with 'sh' if bash is unavailable)
            echo -n "REPLACE_WITH_YOUR_KEYRING_PASSWORD"|gnome-keyring-daemon -- unlock

        :param silent:     Defaults to False.  Suppresses the "Welcome ...!" message.
        :param forced:     Defaults to False.  Bypass the credential cache if set.

        Example::

            syn.login('my-username', 'secret-password', rememberMe=True)
            #> Welcome, Me!

        After logging in with the *rememberMe* flag set, an API key will be cached and
        used to authenticate for future logins::

            syn.login()
            #> Welcome, Me!

        """
        # Note: the order of the logic below reflects the ordering in the docstring above.

        # Check version before logging in
        if not self.skip_checks:
            version_check()

        # Make sure to invalidate the existing session
        self.logout()

        credential_provider_chain = get_default_credential_chain()
        # TODO: remove deprecated sessionToken when we move to a different solution
        self.credentials = credential_provider_chain.get_credentials(
            self,
            UserLoginArgs(
                email,
                password,
                apiKey,
                forced,
                sessionToken,
                authToken,
            ),
        )

        # Final check on login success
        if not self.credentials:
            raise SynapseNoCredentialsError("No credentials provided.")

        # Save the API key in the cache
        if rememberMe:
            message = (
                "The rememberMe parameter will be deprecated by early 2024. Please use the ~/.synapseConfig "
                "or SYNAPSE_AUTH_TOKEN environmental variable to set up your Synapse connection."
            )
            self.logger.warning(message)
            delete_stored_credentials(self.credentials.username)
            self.credentials.store_to_keyring()
            cached_sessions.set_most_recent_user(self.credentials.username)

        if not silent:
            profile = self.getUserProfile()
            # TODO-PY3: in Python2, do we need to ensure that this is encoded in utf-8
            self.logger.info(
                "Welcome, %s!\n"
                % (
                    profile["displayName"]
                    if "displayName" in profile
                    else self.credentials.username
                )
            )

    def _get_config_section_dict(self, section_name: str) -> dict:
        config = self.getConfigFile(self.configPath)
        try:
            return dict(config.items(section_name))
        except configparser.NoSectionError:
            # section not present
            return {}

    def _get_config_authentication(self) -> str:
        return self._get_config_section_dict(
            config_file_constants.AUTHENTICATION_SECTION_NAME
        )

    def _get_client_authenticated_s3_profile(self, endpoint: str, bucket: str) -> str:
        config_section = endpoint + "/" + bucket
        return self._get_config_section_dict(config_section).get(
            "profile_name", "default"
        )

    def _get_transfer_config(self) -> dict:
        # defaults
        transfer_config = {"max_threads": DEFAULT_NUM_THREADS, "use_boto_sts": False}

        for k, v in self._get_config_section_dict("transfer").items():
            if v:
                if k == "max_threads" and v:
                    try:
                        transfer_config["max_threads"] = int(v)
                    except ValueError as cause:
                        raise ValueError(
                            f"Invalid transfer.max_threads config setting {v}"
                        ) from cause

                elif k == "use_boto_sts":
                    lower_v = v.lower()
                    if lower_v not in ("true", "false"):
                        raise ValueError(
                            f"Invalid transfer.use_boto_sts config setting {v}"
                        )

                    transfer_config["use_boto_sts"] = "true" == lower_v

        return transfer_config

    def _getSessionToken(self, email: str, password: str) -> str:
        """Returns a validated session token."""
        try:
            req = {"email": email, "password": password}
            session = self.restPOST(
                "/session",
                body=json.dumps(req),
                endpoint=self.authEndpoint,
                headers=self.default_headers,
            )
            return session["sessionToken"]
        except SynapseHTTPError as err:
            if (
                err.response.status_code == 403
                or err.response.status_code == 404
                or err.response.status_code == 401
            ):
                raise SynapseAuthenticationError("Invalid username or password.")
            raise

    def _getAPIKey(self, sessionToken: str) -> str:
        """Uses a session token to fetch an API key."""

        headers = {"sessionToken": sessionToken, "Accept": "application/json"}
        secret = self.restGET("/secretKey", endpoint=self.authEndpoint, headers=headers)
        return secret["secretKey"]

    def _is_logged_in(self) -> bool:
        """Test whether the user is logged in to Synapse."""
        # This is a quick sanity check to see if credentials have been
        # configured on the client
        if self.credentials is None:
            return False
        # The public can query this command so there is no need to try catch.
        user = self.restGET("/userProfile")
        if user.get("userName") == "anonymous":
            return False
        return True

    def logout(self, forgetMe: bool = False):
        """
        Removes authentication information from the Synapse client.

        :param forgetMe: Set as True to clear any local storage of authentication information.
                         See the flag "rememberMe" in :py:func:`synapseclient.Synapse.login`.
        """
        # Delete the user's API key from the cache
        if forgetMe and self.credentials:
            self.credentials.delete_from_keyring()

        self.credentials = None

    def invalidateAPIKey(self):
        """Invalidates authentication across all clients."""

        # Logout globally
        if self._is_logged_in():
            self.restDELETE("/secretKey", endpoint=self.authEndpoint)

    @functools.lru_cache()
    def getUserProfile(
        self,
        id: Union[str, int, UserProfile, TeamMember] = None,
        sessionToken: str = None,
    ) -> UserProfile:
        """
        Get the details about a Synapse user.
        Retrieves information on the current user if 'id' is omitted.
        :param id:           The 'userId' (aka 'ownerId') of a user or the userName
        :param sessionToken: The session token to use to find the user profile
        :returns: The user profile for the user of interest.

        Example::
            my_profile = syn.getUserProfile()
            freds_profile = syn.getUserProfile('fredcommo')
        """
        try:
            # if id is unset or a userID, this will succeed
            id = "" if id is None else int(id)
        except (TypeError, ValueError):
            if isinstance(id, collections.abc.Mapping) and "ownerId" in id:
                id = id.ownerId
            elif isinstance(id, TeamMember):
                id = id.member.ownerId
            else:
                principals = self._findPrincipals(id)
                if len(principals) == 1:
                    id = principals[0]["ownerId"]
                else:
                    for principal in principals:
                        if principal.get("userName", None).lower() == id.lower():
                            id = principal["ownerId"]
                            break
                    else:  # no break
                        raise ValueError('Can\'t find user "%s": ' % id)
        uri = "/userProfile/%s" % id
        return UserProfile(
            **self.restGET(
                uri, headers={"sessionToken": sessionToken} if sessionToken else None
            )
        )

    def _findPrincipals(self, query_string: str) -> typing.List[UserGroupHeader]:
        """
        Find users or groups by name or email.

        :returns: A list of userGroupHeader objects with fields displayName, email, firstName, lastName, isIndividual,
                  ownerId

        Example::

            syn._findPrincipals('test')

            [{u'displayName': u'Synapse Test',
              u'email': u'syn...t@sagebase.org',
              u'firstName': u'Synapse',
              u'isIndividual': True,
              u'lastName': u'Test',
              u'ownerId': u'1560002'},
             {u'displayName': ... }]

        """
        uri = "/userGroupHeaders?prefix=%s" % urllib_urlparse.quote(query_string)
        return [UserGroupHeader(**result) for result in self._GET_paginated(uri)]

    def _get_certified_passing_record(self, userid: int) -> dict:
        """Retrieve the Passing Record on the User Certification test for the given user.

        :params userid: Synapse user Id

        :returns: Synapse Passing Record
            https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/quiz/PassingRecord.html
        """
        response = self.restGET(f"/user/{userid}/certifiedUserPassingRecord")
        return response

    def _get_user_bundle(self, userid: int, mask: int) -> dict:
        """Retrieve the user bundle for the given user.

        :params userid: Synapse user Id
        :params mask: Bit field indicating which components to include in the bundle.

        :returns: Synapse User Bundle
            https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserBundle.html
        """
        try:
            response = self.restGET(f"/user/{userid}/bundle?mask={mask}")
        except SynapseHTTPError as ex:
            if ex.response.status_code == 404:
                return None
        return response

    def is_certified(self, user: typing.Union[str, int]) -> bool:
        """Determines whether a Synapse user is a certified user.

        :params user: Synapse username or Id

        :returns: True if the Synapse user is certified
        """
        # Check if userid or username exists
        syn_user = self.getUserProfile(user)
        # Get passing record

        try:
            certification_status = self._get_certified_passing_record(
                syn_user["ownerId"]
            )
            return certification_status["passed"]
        except SynapseHTTPError as ex:
            if ex.response.status_code == 404:
                # user hasn't taken the quiz
                return False
            raise

    def is_synapse_id(self, syn_id: str) -> bool:
        """Checks if given synID is valid (attached to actual entity?)"""
        if isinstance(syn_id, str):
            try:
                self.get(syn_id, downloadFile=False)
            except SynapseFileNotFoundError:
                return False
            except (
                SynapseHTTPError,
                SynapseAuthenticationError,
            ) as err:
                status = (
                    err.__context__.response.status_code or err.response.status_code
                )
                if status in (400, 404):
                    return False
                # Valid ID but user lacks permission or is not logged in
                elif status == 403:
                    return True
            return True
        self.logger.warning("synID must be a string")
        return False

    def onweb(self, entity, subpageId=None):
        """Opens up a browser window to the entity page or wiki-subpage.

        :param entity:    Either an Entity or a Synapse ID
        :param subpageId: (Optional) ID of one of the wiki's sub-pages
        """
        if isinstance(entity, str) and os.path.isfile(entity):
            entity = self.get(entity, downloadFile=False)
        synId = id_of(entity)
        if subpageId is None:
            webbrowser.open("%s#!Synapse:%s" % (self.portalEndpoint, synId))
        else:
            webbrowser.open(
                "%s#!Wiki:%s/ENTITY/%s" % (self.portalEndpoint, synId, subpageId)
            )

    def printEntity(self, entity, ensure_ascii=True):
        """
        Pretty prints an Entity.

        :param entity:  The entity to be printed.
        :param ensure_ascii:  If True, escapes all non-ASCII characters
        """

        if utils.is_synapse_id_str(entity):
            entity = self._getEntity(entity)
        try:
            self.logger.info(
                json.dumps(entity, sort_keys=True, indent=2, ensure_ascii=ensure_ascii)
            )
        except TypeError:
            self.logger.info(str(entity))

    def _print_transfer_progress(self, *args, **kwargs):
        # Checking synapse if the mode is silent mode.
        # If self.silent is True, no need to print out transfer progress.
        if self.silent is not True:
            cumulative_transfer_progress.printTransferProgress(*args, **kwargs)

    ############################################################
    #                      Service methods                     #
    ############################################################

    _services = {
        "json_schema": "JsonSchemaService",
    }

    def get_available_services(self):
        """Get available Synapse services
        This is a beta feature and is subject to change"""
        services = self._services.keys()
        return list(services)

    def service(self, service_name: str):
        """Get available Synapse services
        This is a beta feature and is subject to change"""
        # This is to avoid circular imports
        # TODO: revisit the import order and method https://stackoverflow.com/a/37126790
        # To move this to the top
        import synapseclient.services

        assert isinstance(service_name, str)
        service_name = service_name.lower().replace(" ", "_")
        assert service_name in self._services, (
            f"Unrecognized service ({service_name}). Run the 'get_available_"
            "services()' method to get a list of available services."
        )
        service_attr = self._services[service_name]
        service_cls = getattr(synapseclient.services, service_attr)
        service = service_cls(self)
        return service

    ############################################################
    #                   Get / Store methods                    #
    ############################################################

    def get(self, entity, **kwargs):
        """
        Gets a Synapse entity from the repository service.

        :param entity:           A Synapse ID, a Synapse Entity object, a plain dictionary in which 'id' maps to a
                                 Synapse ID or a local file that is stored in Synapse (found by the file MD5)
        :param version:          The specific version to get.
                                 Defaults to the most recent version.
        :param downloadFile:     Whether associated files(s) should be downloaded.
                                 Defaults to True
        :param downloadLocation: Directory where to download the Synapse File Entity.
                                 Defaults to the local cache.
        :param followLink:       Whether the link returns the target Entity.
                                 Defaults to False
        :param ifcollision:      Determines how to handle file collisions.
                                 May be "overwrite.local", "keep.local", or "keep.both".
                                 Defaults to "keep.both".
        :param limitSearch:      a Synanpse ID used to limit the search in Synapse if entity is specified as a local
                                 file.  That is, if the file is stored in multiple locations in Synapse only the ones
                                 in the specified folder/project will be returned.

        :returns: A new Synapse Entity object of the appropriate type

        Example::

            # download file into cache
            entity = syn.get('syn1906479')
            print(entity.name)
            print(entity.path)

            # download file into current working directory
            entity = syn.get('syn1906479', downloadLocation='.')
            print(entity.name)
            print(entity.path)

           # Determine the provenance of a locally stored file as indicated in Synapse
           entity = syn.get('/path/to/file.txt', limitSearch='syn12312')
           print(syn.getProvenance(entity))

        """
        # If entity is a local file determine the corresponding synapse entity
        if isinstance(entity, str) and os.path.isfile(entity):
            bundle = self._getFromFile(entity, kwargs.pop("limitSearch", None))
            kwargs["downloadFile"] = False
            kwargs["path"] = entity

        elif isinstance(entity, str) and not utils.is_synapse_id_str(entity):
            raise SynapseFileNotFoundError(
                (
                    "The parameter %s is neither a local file path "
                    " or a valid entity id" % entity
                )
            )
        # have not been saved entities
        elif isinstance(entity, Entity) and not entity.get("id"):
            raise ValueError(
                "Cannot retrieve entity that has not been saved."
                " Please use syn.store() to save your entity and try again."
            )
        else:
            version = kwargs.get("version", None)
            bundle = self._getEntityBundle(entity, version)
        # Check and warn for unmet access requirements
        self._check_entity_restrictions(
            bundle, entity, kwargs.get("downloadFile", True)
        )

        return self._getWithEntityBundle(entityBundle=bundle, entity=entity, **kwargs)

    def _check_entity_restrictions(self, bundle, entity, downloadFile):
        restrictionInformation = bundle["restrictionInformation"]
        if restrictionInformation["hasUnmetAccessRequirement"]:
            warning_message = (
                "\nThis entity has access restrictions. Please visit the web page for this entity "
                f'(syn.onweb("{id_of(entity)}")). Look for the "Access" label and the lock icon underneath '
                'the file name. Click "Request Access", and then review and fulfill the file '
                "download requirement(s).\n"
            )
            if downloadFile and bundle.get("entityType") not in ("project", "folder"):
                raise SynapseUnmetAccessRestrictions(warning_message)
            warnings.warn(warning_message)

    def _getFromFile(self, filepath, limitSearch=None):
        """
        Gets a Synapse entityBundle based on the md5 of a local file
        See :py:func:`synapseclient.Synapse.get`.

        :param filepath:        path to local file
        :param limitSearch:     Limits the places in Synapse where the file is searched for.
        """
        results = self.restGET(
            "/entity/md5/%s" % utils.md5_for_file(filepath).hexdigest()
        )["results"]
        if limitSearch is not None:
            # Go through and find the path of every entity found
            paths = [self.restGET("/entity/%s/path" % ent["id"]) for ent in results]
            # Filter out all entities whose path does not contain limitSearch
            results = [
                ent
                for ent, path in zip(results, paths)
                if utils.is_in_path(limitSearch, path)
            ]
        if len(results) == 0:  # None found
            raise SynapseFileNotFoundError("File %s not found in Synapse" % (filepath,))
        elif len(results) > 1:
            id_txts = "\n".join(
                ["%s.%i" % (r["id"], r["versionNumber"]) for r in results]
            )
            self.logger.warning(
                "\nThe file %s is associated with many files in Synapse:\n%s\n"
                "You can limit to files in specific project or folder by setting the limitSearch to the"
                " synapse Id of the project or folder.\n"
                "Will use the first one returned: \n"
                "%s version %i\n"
                % (filepath, id_txts, results[0]["id"], results[0]["versionNumber"])
            )
        entity = results[0]

        bundle = self._getEntityBundle(entity, version=entity["versionNumber"])
        self.cache.add(bundle["entity"]["dataFileHandleId"], filepath)

        return bundle

    def move(self, entity, new_parent):
        """
        Move a Synapse entity to a new container.

        :param entity:           A Synapse ID, a Synapse Entity object, or a local file that is stored in Synapse
        :param new_parent:       The new parent container (Folder or Project) to which the entity should be moved.

        :returns: The Synapse Entity object that has been moved.

        Example::

            entity = syn.move('syn456', 'syn123')
        """

        entity = self.get(entity, downloadFile=False)
        entity.parentId = id_of(new_parent)
        entity = self.store(entity, forceVersion=False)

        return entity

    def _getWithEntityBundle(self, entityBundle, entity=None, **kwargs):
        """
        Creates a :py:mod:`synapseclient.Entity` from an entity bundle returned by Synapse.
        An existing Entity can be supplied in case we want to refresh a stale Entity.

        :param entityBundle: Uses the given dictionary as the meta information of the Entity to get
        :param entity:       Optional, entity whose local state will be copied into the returned entity
        :param submission:   Optional, access associated files through a submission rather than
                             through an entity.

        See :py:func:`synapseclient.Synapse.get`.
        See :py:func:`synapseclient.Synapse._getEntityBundle`.
        See :py:mod:`synapseclient.Entity`.
        """

        # Note: This version overrides the version of 'entity' (if the object is Mappable)
        kwargs.pop("version", None)
        downloadFile = kwargs.pop("downloadFile", True)
        downloadLocation = kwargs.pop("downloadLocation", None)
        ifcollision = kwargs.pop("ifcollision", None)
        submission = kwargs.pop("submission", None)
        followLink = kwargs.pop("followLink", False)
        path = kwargs.pop("path", None)

        # make sure user didn't accidentlaly pass a kwarg that we don't handle
        if kwargs:  # if there are remaining items in the kwargs
            raise TypeError("Unexpected **kwargs: %r" % kwargs)

        # If Link, get target ID entity bundle
        if (
            entityBundle["entity"]["concreteType"]
            == "org.sagebionetworks.repo.model.Link"
            and followLink
        ):
            targetId = entityBundle["entity"]["linksTo"]["targetId"]
            targetVersion = entityBundle["entity"]["linksTo"].get("targetVersionNumber")
            entityBundle = self._getEntityBundle(targetId, targetVersion)

        # TODO is it an error to specify both downloadFile=False and downloadLocation?
        # TODO this matters if we want to return already cached files when downloadFile=False

        # Make a fresh copy of the Entity
        local_state = (
            entity.local_state() if entity and isinstance(entity, Entity) else {}
        )
        if path is not None:
            local_state["path"] = path
        properties = entityBundle["entity"]
        annotations = from_synapse_annotations(entityBundle["annotations"])
        entity = Entity.create(properties, annotations, local_state)

        # Handle download of fileEntities
        if isinstance(entity, File):
            # update the entity with FileHandle metadata
            file_handle = next(
                (
                    handle
                    for handle in entityBundle["fileHandles"]
                    if handle["id"] == entity.dataFileHandleId
                ),
                None,
            )
            entity._update_file_handle(file_handle)

            if downloadFile:
                if file_handle:
                    self._download_file_entity(
                        downloadLocation,
                        entity,
                        ifcollision,
                        submission,
                        file_handle.get("contentMd5", None),
                    )
                else:  # no filehandle means that we do not have DOWNLOAD permission
                    warning_message = (
                        "WARNING: You have READ permission on this file entity but not DOWNLOAD "
                        "permission. The file has NOT been downloaded."
                    )
                    self.logger.warning(
                        "\n"
                        + "!" * len(warning_message)
                        + "\n"
                        + warning_message
                        + "\n"
                        + "!" * len(warning_message)
                        + "\n"
                    )
        return entity

    def _ensure_download_location_is_directory(self, downloadLocation):
        download_dir = os.path.expandvars(os.path.expanduser(downloadLocation))
        if os.path.isfile(download_dir):
            raise ValueError(
                "Parameter 'downloadLocation' should be a directory, not a file."
            )
        return download_dir

    def _download_file_entity(
        self,
        downloadLocation: str,
        entity: Entity,
        ifcollision: str,
        submission: str,
        expected_md5: typing.Union[str, None],
    ):
        # set the initial local state
        entity.path = None
        entity.files = []
        entity.cacheDir = None

        # check to see if an UNMODIFIED version of the file (since it was last downloaded) already exists
        # this location could be either in .synapseCache or a user specified location to which the user previously
        # downloaded the file
        cached_file_path = self.cache.get(entity.dataFileHandleId, downloadLocation)

        # This is to handle for cases where the names of the files in the cache and the
        # requested file name match - But there is a difference in content.
        if (
            expected_md5 is not None
            and cached_file_path is not None
            and utils.md5_for_file(cached_file_path).hexdigest() != expected_md5
        ):
            cached_file_path = None

        # location in .synapseCache where the file would be corresponding to its FileHandleId
        synapseCache_location = self.cache.get_cache_dir(entity.dataFileHandleId)

        file_name = (
            entity._file_handle.fileName
            if cached_file_path is None
            else os.path.basename(cached_file_path)
        )

        # Decide the best download location for the file
        if downloadLocation is not None:
            # Make sure the specified download location is a fully resolved directory
            downloadLocation = self._ensure_download_location_is_directory(
                downloadLocation
            )
        elif cached_file_path is not None:
            # file already cached so use that as the download location
            downloadLocation = os.path.dirname(cached_file_path)
        else:
            # file not cached and no user-specified location so default to .synapseCache
            downloadLocation = synapseCache_location

        # resolve file path collisions by either overwriting, renaming, or not downloading, depending on the
        # ifcollision value
        downloadPath = self._resolve_download_path_collisions(
            downloadLocation,
            file_name,
            ifcollision,
            synapseCache_location,
            cached_file_path,
        )
        if downloadPath is None:
            return

        if cached_file_path is not None:  # copy from cache
            if downloadPath != cached_file_path:
                # create the foider if it does not exist already
                if not os.path.exists(downloadLocation):
                    os.makedirs(downloadLocation)
                shutil.copy(cached_file_path, downloadPath)

        else:  # download the file from URL (could be a local file)
            objectType = "FileEntity" if submission is None else "SubmissionAttachment"
            objectId = entity["id"] if submission is None else submission

            # reassign downloadPath because if url points to local file (e.g. file://~/someLocalFile.txt)
            # it won't be "downloaded" and, instead, downloadPath will just point to '~/someLocalFile.txt'
            # _downloadFileHandle may also return None to indicate that the download failed
            downloadPath = self._downloadFileHandle(
                entity.dataFileHandleId, objectId, objectType, downloadPath
            )

            if downloadPath is None or not os.path.exists(downloadPath):
                return

        # converts the path format from forward slashes back to backward slashes on Windows
        entity.path = os.path.normpath(downloadPath)
        entity.files = [os.path.basename(downloadPath)]
        entity.cacheDir = os.path.dirname(downloadPath)

    def _resolve_download_path_collisions(
        self,
        downloadLocation,
        file_name,
        ifcollision,
        synapseCache_location,
        cached_file_path,
    ):
        # always overwrite if we are downloading to .synapseCache
        if utils.normalize_path(downloadLocation) == synapseCache_location:
            if ifcollision is not None and ifcollision != "overwrite.local":
                self.logger.warning(
                    "\n"
                    + "!" * 50
                    + f"\nifcollision={ifcollision} "
                    + "is being IGNORED because the download destination is synapse's cache."
                    ' Instead, the behavior is "overwrite.local". \n' + "!" * 50 + "\n"
                )
            ifcollision = "overwrite.local"
        # if ifcollision not specified, keep.local
        ifcollision = ifcollision or "keep.both"

        downloadPath = utils.normalize_path(os.path.join(downloadLocation, file_name))
        # resolve collison
        if os.path.exists(downloadPath):
            if ifcollision == "overwrite.local":
                pass
            elif ifcollision == "keep.local":
                # Don't want to overwrite the local file.
                return None
            elif ifcollision == "keep.both":
                if downloadPath != cached_file_path:
                    return utils.unique_filename(downloadPath)
            else:
                raise ValueError(
                    'Invalid parameter: "%s" is not a valid value '
                    'for "ifcollision"' % ifcollision
                )
        return downloadPath

    def store(
        self,
        obj,
        *,
        createOrUpdate=True,
        forceVersion=True,
        versionLabel=None,
        isRestricted=False,
        activity=None,
        used=None,
        executed=None,
        activityName=None,
        activityDescription=None,
    ):
        """
        Creates a new Entity or updates an existing Entity, uploading any files in the process.

        :param obj:                 A Synapse Entity, Evaluation, or Wiki
        :param used:                The Entity, Synapse ID, or URL used to create the object (can also be a list of
                                    these)
        :param executed:            The Entity, Synapse ID, or URL representing code executed to create the object
                                    (can also be a list of these)
        :param activity:            Activity object specifying the user's provenance.
        :param activityName:        Activity name to be used in conjunction with *used* and *executed*.
        :param activityDescription: Activity description to be used in conjunction with *used* and *executed*.
        :param createOrUpdate:      Indicates whether the method should automatically perform an update if the 'obj'
                                    conflicts with an existing Synapse object.  Defaults to True.
        :param forceVersion:        Indicates whether the method should increment the version of the object even if
                                    nothing has changed.  Defaults to True.
        :param versionLabel:        Arbitrary string used to label the version.
        :param isRestricted:        If set to true, an email will be sent to the Synapse access control team to start
                                    the process of adding terms-of-use or review board approval for this entity.
                                    You will be contacted with regards to the specific data being restricted and the
                                    requirements of access.

        :returns: A Synapse Entity, Evaluation, or Wiki

        Example::

            from synapseclient import Project

            project = Project('My uniquely named project')
            project = syn.store(project)

        Adding files with `provenance <Activity.html>`_::

            from synapseclient import File, Activity

            # A synapse entity *syn1906480* contains data
            # entity *syn1917825* contains code
            activity = Activity(
                'Fancy Processing',
                description='No seriously, really fancy processing',
                used=['syn1906480', 'http://data_r_us.com/fancy/data.txt'],
                executed='syn1917825')

            test_entity = File('/path/to/data/file.xyz', description='Fancy new data', parent=project)
            test_entity = syn.store(test_entity, activity=activity)

        """
        # SYNPY-1031: activity must be Activity object or code will fail later
        if activity:
            if not isinstance(activity, synapseclient.Activity):
                raise ValueError("activity should be synapseclient.Activity object")
        # _before_store hook
        # give objects a chance to do something before being stored
        if hasattr(obj, "_before_synapse_store"):
            obj._before_synapse_store(self)

        # _synapse_store hook
        # for objects that know how to store themselves
        if hasattr(obj, "_synapse_store"):
            return obj._synapse_store(self)

        # Handle all non-Entity objects
        if not (isinstance(obj, Entity) or type(obj) == dict):
            if isinstance(obj, Wiki):
                return self._storeWiki(obj, createOrUpdate)

            if "id" in obj:  # If ID is present, update
                return type(obj)(**self.restPUT(obj.putURI(), obj.json()))

            try:  # If no ID is present, attempt to POST the object
                return type(obj)(**self.restPOST(obj.postURI(), obj.json()))

            except SynapseHTTPError as err:
                # If already present and we want to update attempt to get the object content
                if createOrUpdate and err.response.status_code == 409:
                    newObj = self.restGET(obj.getByNameURI(obj.name))
                    newObj.update(obj)
                    obj = type(obj)(**newObj)
                    obj.update(self.restPUT(obj.putURI(), obj.json()))
                    return obj
                raise

        # If the input object is an Entity or a dictionary
        entity = obj
        properties, annotations, local_state = split_entity_namespaces(entity)
        bundle = None
        # Explicitly set an empty versionComment property if none is supplied,
        # otherwise an existing entity bundle's versionComment will be copied to the update.
        properties["versionComment"] = (
            properties["versionComment"] if "versionComment" in properties else None
        )

        # Anything with a path is treated as a cache-able item
        if entity.get("path", False):
            if "concreteType" not in properties:
                properties["concreteType"] = File._synapse_entity_type
            # Make sure the path is fully resolved
            entity["path"] = os.path.expanduser(entity["path"])

            # Check if the File already exists in Synapse by fetching metadata on it
            bundle = self._getEntityBundle(entity)

            if bundle:
                if createOrUpdate:
                    # update our properties from the existing bundle so that we have
                    # enough to process this as an entity update.
                    properties = {**bundle["entity"], **properties}

                # Check if the file should be uploaded
                fileHandle = find_data_file_handle(bundle)
                if (
                    fileHandle
                    and fileHandle["concreteType"]
                    == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
                ):
                    # switching away from ExternalFileHandle or the url was updated
                    needs_upload = entity["synapseStore"] or (
                        fileHandle["externalURL"] != entity["externalURL"]
                    )
                else:
                    # Check if we need to upload a new version of an existing
                    # file. If the file referred to by entity['path'] has been
                    # modified, we want to upload the new version.
                    # If synapeStore is false then we must upload a ExternalFileHandle
                    needs_upload = not entity[
                        "synapseStore"
                    ] or not self.cache.contains(
                        bundle["entity"]["dataFileHandleId"], entity["path"]
                    )
            elif entity.get("dataFileHandleId", None) is not None:
                needs_upload = False
            else:
                needs_upload = True

            if needs_upload:
                local_state_fh = local_state.get("_file_handle", {})
                synapseStore = local_state.get("synapseStore", True)
                fileHandle = upload_file_handle(
                    self,
                    entity["parentId"],
                    local_state["path"]
                    if (synapseStore or local_state_fh.get("externalURL") is None)
                    else local_state_fh.get("externalURL"),
                    synapseStore=synapseStore,
                    md5=local_state_fh.get("contentMd5"),
                    file_size=local_state_fh.get("contentSize"),
                    mimetype=local_state_fh.get("contentType"),
                    max_threads=self.max_threads,
                )
                properties["dataFileHandleId"] = fileHandle["id"]
                local_state["_file_handle"] = fileHandle

            elif "dataFileHandleId" not in properties:
                # Handle the case where the Entity lacks an ID
                # But becomes an update() due to conflict
                properties["dataFileHandleId"] = bundle["entity"]["dataFileHandleId"]

            # update the file_handle metadata if the FileEntity's FileHandle id has changed
            local_state_fh_id = local_state.get("_file_handle", {}).get("id")
            if (
                local_state_fh_id
                and properties["dataFileHandleId"] != local_state_fh_id
            ):
                local_state["_file_handle"] = find_data_file_handle(
                    self._getEntityBundle(
                        properties["id"],
                        requestedObjects={
                            "includeEntity": True,
                            "includeFileHandles": True,
                        },
                    )
                )

                # check if we already have the filehandleid cached somewhere
                cached_path = self.cache.get(properties["dataFileHandleId"])
                if cached_path is None:
                    local_state["path"] = None
                    local_state["cacheDir"] = None
                    local_state["files"] = []
                else:
                    local_state["path"] = cached_path
                    local_state["cacheDir"] = os.path.dirname(cached_path)
                    local_state["files"] = [os.path.basename(cached_path)]

        # Create or update Entity in Synapse
        if "id" in properties:
            properties = self._updateEntity(properties, forceVersion, versionLabel)
        else:
            # If Link, get the target name, version number and concrete type and store in link properties
            if properties["concreteType"] == "org.sagebionetworks.repo.model.Link":
                target_properties = self._getEntity(
                    properties["linksTo"]["targetId"],
                    version=properties["linksTo"].get("targetVersionNumber"),
                )
                if target_properties["parentId"] == properties["parentId"]:
                    raise ValueError(
                        "Cannot create a Link to an entity under the same parent."
                    )
                properties["linksToClassName"] = target_properties["concreteType"]
                if (
                    target_properties.get("versionNumber") is not None
                    and properties["linksTo"].get("targetVersionNumber") is not None
                ):
                    properties["linksTo"]["targetVersionNumber"] = target_properties[
                        "versionNumber"
                    ]
                properties["name"] = target_properties["name"]
            try:
                properties = self._createEntity(properties)
            except SynapseHTTPError as ex:
                if createOrUpdate and ex.response.status_code == 409:
                    # Get the existing Entity's ID via the name and parent
                    existing_entity_id = self.findEntityId(
                        properties["name"], properties.get("parentId", None)
                    )
                    if existing_entity_id is None:
                        raise

                    # get existing properties and annotations
                    if not bundle:
                        bundle = self._getEntityBundle(
                            existing_entity_id,
                            requestedObjects={
                                "includeEntity": True,
                                "includeAnnotations": True,
                            },
                        )

                    properties = {**bundle["entity"], **properties}

                    # we additionally merge the annotations under the assumption that a missing annotation
                    # from a resolved conflict represents an newer annotation that should be preserved
                    # rather than an intentionally deleted annotation.
                    annotations = {
                        **from_synapse_annotations(bundle["annotations"]),
                        **annotations,
                    }

                    properties = self._updateEntity(
                        properties, forceVersion, versionLabel
                    )

                else:
                    raise

        # Deal with access restrictions
        if isRestricted:
            self._createAccessRequirementIfNone(properties)

        # Update annotations
        if (not bundle and annotations) or (
            bundle and check_annotations_changed(bundle["annotations"], annotations)
        ):
            annotations = self.set_annotations(
                Annotations(properties["id"], properties["etag"], annotations)
            )
            properties["etag"] = annotations.etag

        # If the parameters 'used' or 'executed' are given, create an Activity object
        if used or executed:
            if activity is not None:
                raise SynapseProvenanceError(
                    "Provenance can be specified as an Activity object or as used/executed"
                    " item(s), but not both."
                )
            activity = Activity(
                name=activityName,
                description=activityDescription,
                used=used,
                executed=executed,
            )

        # If we have an Activity, set it as the Entity's provenance record
        if activity:
            self.setProvenance(properties, activity)

            # 'etag' has changed, so get the new Entity
            properties = self._getEntity(properties)

        # Return the updated Entity object
        entity = Entity.create(properties, annotations, local_state)
        return self.get(entity, downloadFile=False)

    def _createAccessRequirementIfNone(self, entity):
        """
        Checks to see if the given entity has access requirements.
        If not, then one is added
        """
        existingRestrictions = self.restGET(
            "/entity/%s/accessRequirement?offset=0&limit=1" % id_of(entity)
        )
        if len(existingRestrictions["results"]) <= 0:
            self.restPOST("/entity/%s/lockAccessRequirement" % id_of(entity), body="")

    def _getEntityBundle(self, entity, version=None, requestedObjects=None):
        """
        Gets some information about the Entity.

        :parameter entity:      a Synapse Entity or Synapse ID
        :parameter version:     the entity's version (defaults to None meaning most recent version)
        :parameter requestedObjects:    A dict indicating settings for what to include

        default value for requestedObjects is::

            requestedObjects = {'includeEntity': True,
                                'includeAnnotations': True,
                                'includeFileHandles': True,
                                'includeRestrictionInformation': True}

        Keys available for requestedObjects::

            includeEntity
            includeAnnotations
            includePermissions
            includeEntityPath
            includeHasChildren
            includeAccessControlList
            includeFileHandles
            includeTableBundle
            includeRootWikiId
            includeBenefactorACL
            includeDOIAssociation
            includeFileName
            includeThreadCount
            includeRestrictionInformation


        Keys with values set to False may simply be omitted.
        For example, we might ask for an entity bundle containing file handles, annotations, and properties::
            requested_objects = {'includeEntity':True
                                 'includeAnnotations':True,
                                 'includeFileHandles':True}
            bundle = syn._getEntityBundle('syn111111', )

        :returns: An EntityBundle with the requested fields or by default Entity header, annotations, unmet access
         requirements, and file handles
        """

        # If 'entity' is given without an ID, try to find it by 'parentId' and 'name'.
        # Use case:
        #     If the user forgets to catch the return value of a syn.store(e)
        #     this allows them to recover by doing: e = syn.get(e)
        if requestedObjects is None:
            requestedObjects = {
                "includeEntity": True,
                "includeAnnotations": True,
                "includeFileHandles": True,
                "includeRestrictionInformation": True,
            }
        if (
            isinstance(entity, collections.abc.Mapping)
            and "id" not in entity
            and "name" in entity
        ):
            entity = self.findEntityId(entity["name"], entity.get("parentId", None))

        # Avoid an exception from finding an ID from a NoneType
        try:
            id_of(entity)
        except ValueError:
            return None

        if version is not None:
            uri = f"/entity/{id_of(entity)}/version/{int(version):d}/bundle2"
        else:
            uri = f"/entity/{id_of(entity)}/bundle2"
        bundle = self.restPOST(uri, body=json.dumps(requestedObjects))

        return bundle

    def delete(self, obj, version=None):
        """
        Removes an object from Synapse.

        :param obj:         An existing object stored on Synapse such as
                            Evaluation, File, Project, or Wiki
        :param version:     For entities, specify a particular version to
                            delete.

        """
        # Handle all strings as the Entity ID for backward compatibility
        if isinstance(obj, str):
            if version:
                self.restDELETE(uri=f"/entity/{id_of(obj)}/version/{version}")
            else:
                self.restDELETE(uri=f"/entity/{id_of(obj)}")
        elif hasattr(obj, "_synapse_delete"):
            return obj._synapse_delete(self)
        else:
            try:
                if isinstance(obj, Versionable):
                    self.restDELETE(obj.deleteURI(versionNumber=version))
                else:
                    self.restDELETE(obj.deleteURI())
            except AttributeError:
                raise SynapseError(
                    f"Can't delete a {type(obj)}. Please specify a Synapse object or id"
                )

    _user_name_cache = {}

    def _get_user_name(self, user_id):
        if user_id not in self._user_name_cache:
            self._user_name_cache[user_id] = utils.extract_user_name(
                self.getUserProfile(user_id)
            )
        return self._user_name_cache[user_id]

    def _list(
        self,
        parent,
        recursive=False,
        long_format=False,
        show_modified=False,
        indent=0,
        out=sys.stdout,
    ):
        """
        List child objects of the given parent, recursively if requested.
        """
        fields = ["id", "name", "nodeType"]
        if long_format:
            fields.extend(["createdByPrincipalId", "createdOn", "versionNumber"])
        if show_modified:
            fields.extend(["modifiedByPrincipalId", "modifiedOn"])
        results = self.getChildren(parent)

        results_found = False
        for result in results:
            results_found = True

            fmt_fields = {
                "name": result["name"],
                "id": result["id"],
                "padding": " " * indent,
                "slash_or_not": "/" if is_container(result) else "",
            }
            fmt_string = "{id}"

            if long_format:
                fmt_fields["createdOn"] = utils.iso_to_datetime(
                    result["createdOn"]
                ).strftime("%Y-%m-%d %H:%M")
                fmt_fields["createdBy"] = self._get_user_name(result["createdBy"])[:18]
                fmt_fields["version"] = result["versionNumber"]
                fmt_string += " {version:3}  {createdBy:>18} {createdOn}"
            if show_modified:
                fmt_fields["modifiedOn"] = utils.iso_to_datetime(
                    result["modifiedOn"]
                ).strftime("%Y-%m-%d %H:%M")
                fmt_fields["modifiedBy"] = self._get_user_name(result["modifiedBy"])[
                    :18
                ]
                fmt_string += "  {modifiedBy:>18} {modifiedOn}"

            fmt_string += "  {padding}{name}{slash_or_not}\n"
            out.write(fmt_string.format(**fmt_fields))

            if (indent == 0 or recursive) and is_container(result):
                self._list(
                    result["id"],
                    recursive=recursive,
                    long_format=long_format,
                    show_modified=show_modified,
                    indent=indent + 2,
                    out=out,
                )

        if indent == 0 and not results_found:
            out.write(
                "No results visible to {username} found for id {id}\n".format(
                    username=self.credentials.username, id=id_of(parent)
                )
            )

    def uploadFileHandle(
        self, path, parent, synapseStore=True, mimetype=None, md5=None, file_size=None
    ):
        """Uploads the file in the provided path (if necessary) to a storage location based on project settings.
        Returns a new FileHandle as a dict to represent the stored file.

        :param parent:          parent of the entity to which we upload.
        :param path:            file path to the file being uploaded
        :param synapseStore:    If False, will not upload the file, but instead create an ExternalFileHandle that
                                references the file on the local machine.
                                If True, will upload the file based on StorageLocation determined by the
                                entity_parent_id
        :param mimetype:        The MIME type metadata for the uploaded file
        :param md5:             The MD5 checksum for the file, if known. Otherwise if the file is a local file, it will
                                be calculated automatically.
        :param file_size:       The size the file, if known. Otherwise if the file is a local file, it will be
                                calculated automatically.
        :param file_type:       The MIME type the file, if known. Otherwise if the file is a local file, it will be
                                calculated automatically.

        :returns: a dict of a new FileHandle as a dict that represents the uploaded file
        """
        return upload_file_handle(
            self, parent, path, synapseStore, md5, file_size, mimetype
        )

    ############################################################
    #                  Download List                           #
    ############################################################
    def clear_download_list(self):
        """Clear all files from download list"""
        self.restDELETE("/download/list")

    def remove_from_download_list(self, list_of_files: typing.List[typing.Dict]) -> int:
        """Remove a batch of files from download list

        :param: array of files in the format of a mapping
                {fileEntityId: synid, versionNumber: version}

        :returns: Number of files removed from download list
        """
        request_body = {"batchToRemove": list_of_files}
        num_files_removed = self.restPOST(
            "/download/list/remove", body=json.dumps(request_body)
        )
        return num_files_removed

    def _generate_manifest_from_download_list(
        self,
        quoteCharacter: str = '"',
        escapeCharacter: str = "\\",
        lineEnd: str = os.linesep,
        separator: str = ",",
        header: bool = True,
    ):
        """Creates a download list manifest generation request

        :param quoteCharacter:  The character to be used for quoted elements in the resulting file.
                                Defaults to '"'.
        :param escapeCharacter: The escape character to be used for escaping a separator or quote in the resulting
                                file. Defaults to "\".
        :param lineEnd:         The line feed terminator to be used for the resulting file. Defaults to os.linesep.
        :param separator:       The delimiter to be used for separating entries in the resulting file. Defaults to ",".
        :param header:          Is the first line a header? Defaults to True.

        :returns: Filehandle of download list manifest
        """
        request_body = {
            "concreteType": "org.sagebionetworks.repo.model.download.DownloadListManifestRequest",
            "csvTableDescriptor": {
                "separator": separator,
                "quoteCharacter": quoteCharacter,
                "escapeCharacter": escapeCharacter,
                "lineEnd": lineEnd,
                "isFirstLineHeader": header,
            },
        }
        return self._waitForAsync(
            uri="/download/list/manifest/async", request=request_body
        )

    def get_download_list_manifest(self):
        """Get the path of the download list manifest file

        :returns: path of download list manifest file
        """
        manifest = self._generate_manifest_from_download_list()
        # Get file handle download link
        file_result = self._getFileHandleDownload(
            fileHandleId=manifest["resultFileHandleId"],
            objectId=manifest["resultFileHandleId"],
            objectType="FileEntity",
        )
        # Download the manifest
        downloaded_path = self._download_from_URL(
            url=file_result["preSignedURL"],
            destination="./",
            fileHandleId=file_result["fileHandleId"],
            expected_md5=file_result["fileHandle"].get("contentMd5"),
        )
        return downloaded_path

    def get_download_list(self, downloadLocation: str = None) -> str:
        """Download all files from your Synapse download list

        :param downloadLocation: Directory to download files to.

        :returns: manifest file with file paths
        """
        dl_list_path = self.get_download_list_manifest()
        downloaded_files = []
        new_manifest_path = f"manifest_{time.time_ns()}.csv"
        with open(dl_list_path) as manifest_f, open(
            new_manifest_path, "w"
        ) as write_obj:
            reader = csv.DictReader(manifest_f)
            columns = reader.fieldnames
            columns.extend(["path", "error"])
            # Write the downloaded paths to a new manifest file
            writer = csv.DictWriter(write_obj, fieldnames=columns)
            writer.writeheader()

            for row in reader:
                # You can add things to the download list that you don't have access to
                # So there must be a try catch here
                try:
                    entity = self.get(row["ID"], downloadLocation=downloadLocation)
                    # Must include version number because you can have multiple versions of a
                    # file in the download list
                    downloaded_files.append(
                        {
                            "fileEntityId": row["ID"],
                            "versionNumber": row["versionNumber"],
                        }
                    )
                    row["path"] = entity.path
                    row["error"] = ""
                except Exception:
                    row["path"] = ""
                    row["error"] = "DOWNLOAD FAILED"
                    self.logger.error("Unable to download file")
                writer.writerow(row)

        # Don't want to clear all the download list because you can add things
        # to the download list after initiating this command.
        # Files that failed to download should not be removed from download list
        # Remove all files from download list after the entire download is complete.
        # This is because if download fails midway, we want to return the full manifest
        if downloaded_files:
            # Only want to invoke this if there is a list of files to remove
            # or the API call will error
            self.remove_from_download_list(list_of_files=downloaded_files)
        else:
            self.logger.warning("A manifest was created, but no files were downloaded")

        # Always remove original manifest file
        os.remove(dl_list_path)

        return new_manifest_path

    ############################################################
    #                  Get / Set Annotations                   #
    ############################################################

    def _getRawAnnotations(self, entity, version=None):
        """
        Retrieve annotations for an Entity returning them in the native Synapse format.
        """
        # Note: Specifying the version results in a zero-ed out etag,
        # even if the version is the most recent.
        # See `PLFM-1874 <https://sagebionetworks.jira.com/browse/PLFM-1874>`_ for more details.
        if version:
            uri = f"/entity/{id_of(entity)}/version/{str(version)}/annotations2"
        else:
            uri = f"/entity/{id_of(entity)}/annotations2"
        return self.restGET(uri)

    @deprecated.sphinx.deprecated(
        version="2.1.0",
        reason="deprecated and replaced with :py:meth:`get_annotations`",
    )
    def getAnnotations(self, entity, version=None):
        """deprecated and replaced with :py:meth:`get_annotations`"""
        return self.get_annotations(entity, version=version)

    def get_annotations(
        self, entity: typing.Union[str, Entity], version: typing.Union[str, int] = None
    ) -> Annotations:
        """
        Retrieve annotations for an Entity from the Synapse Repository as a Python dict.

        Note that collapsing annotations from the native Synapse format to a Python dict may involve some loss of
        information. See :py:func:`_getRawAnnotations` to get annotations in the native format.

        :param entity:  An Entity or Synapse ID to lookup
        :param version: The version of the Entity to retrieve.

        :returns: A :py:class:`synapseclient.annotations.Annotations` object, \
        a dict that also has id and etag attributes
        :rtype: :py:class:`synapseclient.annotations.Annotations`
        """
        return from_synapse_annotations(self._getRawAnnotations(entity, version))

    @deprecated.sphinx.deprecated(
        version="2.1.0",
        reason="deprecated and replaced with :py:meth:`set_annotations` "
        "This method is UNSAFE and may overwrite existing annotations"
        " without confirming that you have retrieved and"
        " updated the latest annotations",
    )
    def setAnnotations(self, entity, annotations=None, **kwargs):
        """
        Store annotations for an Entity in the Synapse Repository.

        :param entity:      The Entity or Synapse Entity ID whose annotations are to be updated
        :param annotations: A dictionary of annotation names and values
        :param kwargs:      annotation names and values
        :returns: the updated annotations for the entity

        """
        if not annotations:
            annotations = {}

        annotations.update(kwargs)

        id = id_of(entity)
        etag = (
            annotations.etag
            if hasattr(annotations, "etag")
            else annotations.get("etag")
        )

        if not etag:
            if "etag" in entity:
                etag = entity["etag"]
            else:
                uri = "/entity/%s/annotations2" % id_of(entity)
                old_annos = self.restGET(uri)
                etag = old_annos["etag"]

        return self.set_annotations(Annotations(id, etag, annotations))

    def set_annotations(self, annotations: Annotations):
        """
        Store annotations for an Entity in the Synapse Repository.

        :param annotations: A :py:class:`synapseclient.annotations.Annotations` of annotation names and values,
         with the id and etag attribute set

        :returns: the updated :py:class:`synapseclient.annotations.Annotations` for the entity


        Example::

            annos = syn.get_annotations('syn123')

            # annos will contain the id and etag associated with the entity upon retrieval
            print(annos.id)
            # syn123
            print(annos.etag)
            # 7bdb83e9-a50a-46e4-987a-4962559f090f   (Usually some UUID in the form of a string)

            # returned annos object from get_annotations() can be used as if it were a dict

            # set key 'foo' to have value of 'bar' and 'baz'
            annos['foo'] = ['bar', 'baz']

            # single values will automatically be wrapped in a list once stored
            annos['qwerty'] = 'asdf'

            # store the annotations
            annos = syn.set_annotations(annos)

            print(annos)
            # {'foo':['bar','baz], 'qwerty':['asdf']}
        """

        if not isinstance(annotations, Annotations):
            raise TypeError("Expected a synapseclient.Annotations object")

        synapseAnnos = to_synapse_annotations(annotations)

        return from_synapse_annotations(
            self.restPUT(
                f"/entity/{id_of(annotations)}/annotations2",
                body=json.dumps(synapseAnnos),
            )
        )

    ############################################################
    #                         Querying                         #
    ############################################################

    def getChildren(
        self,
        parent,
        includeTypes=[
            "folder",
            "file",
            "table",
            "link",
            "entityview",
            "dockerrepo",
            "submissionview",
            "dataset",
            "materializedview",
        ],
        sortBy="NAME",
        sortDirection="ASC",
    ):
        """
        Retrieves all of the entities stored within a parent such as folder or project.

        :param parent:          An id or an object of a Synapse container or None to retrieve all projects

        :param includeTypes:    Must be a list of entity types (ie. ["folder","file"]) which can be found here:
                                http://docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html

        :param sortBy:          How results should be sorted.  Can be NAME, or CREATED_ON

        :param sortDirection:   The direction of the result sort.  Can be ASC, or DESC

        :returns:                An iterator that shows all the children of the container.

        Also see:

        - :py:func:`synapseutils.walk`
        """
        parentId = id_of(parent) if parent is not None else None
        entityChildrenRequest = {
            "parentId": parentId,
            "includeTypes": includeTypes,
            "sortBy": sortBy,
            "sortDirection": sortDirection,
            "nextPageToken": None,
        }
        entityChildrenResponse = {"nextPageToken": "first"}
        while entityChildrenResponse.get("nextPageToken") is not None:
            entityChildrenResponse = self.restPOST(
                "/entity/children", body=json.dumps(entityChildrenRequest)
            )
            for child in entityChildrenResponse["page"]:
                yield child
            if entityChildrenResponse.get("nextPageToken") is not None:
                entityChildrenRequest["nextPageToken"] = entityChildrenResponse[
                    "nextPageToken"
                ]

    def md5Query(self, md5):
        """
        Find the Entities which have attached file(s) which have the given MD5 hash.

        :param md5: The MD5 to query for (hexadecimal string)

        :returns: A list of Entity headers
        """

        return self.restGET("/entity/md5/%s" % md5)["results"]

    ############################################################
    #                     ACL manipulation                     #
    ############################################################

    def _getBenefactor(self, entity):
        """An Entity gets its ACL from its benefactor."""

        if utils.is_synapse_id_str(entity) or is_synapse_entity(entity):
            return self.restGET("/entity/%s/benefactor" % id_of(entity))
        return entity

    def _getACL(self, entity):
        """Get the effective ACL for a Synapse Entity."""

        if hasattr(entity, "getACLURI"):
            uri = entity.getACLURI()
        else:
            # Get the ACL from the benefactor (which may be the entity itself)
            benefactor = self._getBenefactor(entity)
            uri = "/entity/%s/acl" % (benefactor["id"])
        return self.restGET(uri)

    def _storeACL(self, entity, acl):
        """
        Create or update the ACL for a Synapse Entity.

        :param entity:  An entity or Synapse ID
        :param acl:  An ACl as a dict

        :returns: the new or updated ACL

        .. code-block:: python

            {'resourceAccess': [
                {'accessType': ['READ'],
                 'principalId': 222222}
            ]}
        """
        if hasattr(entity, "putACLURI"):
            return self.restPUT(entity.putACLURI(), json.dumps(acl))
        else:
            # Get benefactor. (An entity gets its ACL from its benefactor.)
            entity_id = id_of(entity)
            uri = "/entity/%s/benefactor" % entity_id
            benefactor = self.restGET(uri)

            # Update or create new ACL
            uri = "/entity/%s/acl" % entity_id
            if benefactor["id"] == entity_id:
                return self.restPUT(uri, json.dumps(acl))
            else:
                return self.restPOST(uri, json.dumps(acl))

    def _getUserbyPrincipalIdOrName(self, principalId: str = None):
        """
        Given either a string, int or None finds the corresponding user where None implies PUBLIC

        :param principalId: Identifier of a user or group

        :returns: The integer ID of the user
        """
        if principalId is None or principalId == "PUBLIC":
            return PUBLIC
        try:
            return int(principalId)

        # If principalId is not a number assume it is a name or email
        except ValueError:
            userProfiles = self.restGET("/userGroupHeaders?prefix=%s" % principalId)
            totalResults = len(userProfiles["children"])
            if totalResults == 1:
                return int(userProfiles["children"][0]["ownerId"])
            elif totalResults > 1:
                for profile in userProfiles["children"]:
                    if profile["userName"] == principalId:
                        return int(profile["ownerId"])

            supplementalMessage = (
                "Please be more specific" if totalResults > 1 else "No matches"
            )
            raise SynapseError(
                "Unknown Synapse user (%s).  %s." % (principalId, supplementalMessage)
            )

    def getPermissions(
        self,
        entity: Union[Entity, Evaluation, str, collections.abc.Mapping],
        principalId: str = None,
    ):
        """Get the permissions that a user or group has on an Entity.

        :param entity:      An Entity or Synapse ID to lookup
        :param principalId: Identifier of a user or group (defaults to PUBLIC users)

        :returns: An array containing some combination of
                  ['READ', 'CREATE', 'UPDATE', 'DELETE', 'CHANGE_PERMISSIONS', 'DOWNLOAD']
                  or an empty array

        """
        principal_id = self._getUserbyPrincipalIdOrName(principalId)
        acl = self._getACL(entity)

        team_list = self._find_teams_for_principal(principal_id)
        team_ids = [int(team.id) for team in team_list]
        effective_permission_set = set()

        # This user_profile_bundle is being used to verify that the principal_id is a registered user of the system
        user_profile_bundle = self._get_user_bundle(principal_id, 1)

        # Loop over all permissions in the returned ACL and add it to the effective_permission_set
        # if the principalId in the ACL matches
        # 1) the one we are looking for,
        # 2) a team the entity is a member of,
        # 3) PUBLIC
        # 4) A user_profile_bundle exists for the principal_id
        for permissions in acl["resourceAccess"]:
            if "principalId" in permissions and (
                permissions["principalId"] == principal_id
                or permissions["principalId"] in team_ids
                or permissions["principalId"] == PUBLIC
                or (
                    permissions["principalId"] == AUTHENTICATED_USERS
                    and user_profile_bundle is not None
                )
            ):
                effective_permission_set = effective_permission_set.union(
                    permissions["accessType"]
                )
        return list(effective_permission_set)

    def setPermissions(
        self,
        entity,
        principalId=None,
        accessType=["READ", "DOWNLOAD"],
        modify_benefactor=False,
        warn_if_inherits=True,
        overwrite=True,
    ):
        """
        Sets permission that a user or group has on an Entity.
        An Entity may have its own ACL or inherit its ACL from a benefactor.

        :param entity:              An Entity or Synapse ID to modify
        :param principalId:         Identifier of a user or group. '273948' is for all registered Synapse users
                                    and '273949' is for public access.
        :param accessType:          Type of permission to be granted. One or more of CREATE, READ, DOWNLOAD, UPDATE,
                                    DELETE, CHANGE_PERMISSIONS
        :param modify_benefactor:   Set as True when modifying a benefactor's ACL
        :param warn_if_inherits:    Set as False, when creating a new ACL.
                                    Trying to modify the ACL of an Entity that inherits its ACL will result in a warning
        :param overwrite:           By default this function overwrites existing permissions for the specified user.
                                    Set this flag to False to add new permissions non-destructively.

        :returns: an Access Control List object

        Example::

            # Grant all registered users download access
            syn.setPermissions('syn1234','273948',['READ','DOWNLOAD'])
            # Grant the public view access
            syn.setPermissions('syn1234','273949',['READ'])
        """

        benefactor = self._getBenefactor(entity)
        if benefactor["id"] != id_of(entity):
            if modify_benefactor:
                entity = benefactor
            elif warn_if_inherits:
                self.logger.warning(
                    "Creating an ACL for entity %s, which formerly inherited access control from a"
                    ' benefactor entity, "%s" (%s).\n'
                    % (id_of(entity), benefactor["name"], benefactor["id"])
                )

        acl = self._getACL(entity)

        principalId = self._getUserbyPrincipalIdOrName(principalId)

        # Find existing permissions
        permissions_to_update = None
        for permissions in acl["resourceAccess"]:
            if (
                "principalId" in permissions
                and permissions["principalId"] == principalId
            ):
                permissions_to_update = permissions
                break

        if accessType is None or accessType == []:
            # remove permissions
            if permissions_to_update and overwrite:
                acl["resourceAccess"].remove(permissions_to_update)
        else:
            # add a 'resourceAccess' entry, if necessary
            if not permissions_to_update:
                permissions_to_update = {"accessType": [], "principalId": principalId}
                acl["resourceAccess"].append(permissions_to_update)
            if overwrite:
                permissions_to_update["accessType"] = accessType
            else:
                permissions_to_update["accessType"] = list(
                    set(permissions_to_update["accessType"]) | set(accessType)
                )
        return self._storeACL(entity, acl)

    ############################################################
    #                        Provenance                        #
    ############################################################

    # TODO: rename these to Activity
    def getProvenance(self, entity, version=None):
        """
        Retrieve provenance information for a Synapse Entity.

        :param entity:  An Entity or Synapse ID to lookup
        :param version: The version of the Entity to retrieve.
                        Gets the most recent version if omitted

        :returns: An Activity object or
                  raises exception if no provenance record exists
        """

        # Get versionNumber from Entity
        if version is None and "versionNumber" in entity:
            version = entity["versionNumber"]

        if version:
            uri = "/entity/%s/version/%d/generatedBy" % (id_of(entity), version)
        else:
            uri = "/entity/%s/generatedBy" % id_of(entity)
        return Activity(data=self.restGET(uri))

    def setProvenance(self, entity, activity):
        """
        Stores a record of the code and data used to derive a Synapse entity.

        :param entity:   An Entity or Synapse ID to modify
        :param activity: a :py:class:`synapseclient.activity.Activity`

        :returns: An updated :py:class:`synapseclient.activity.Activity` object
        """

        # Assert that the entity was generated by a given Activity.
        activity = self._saveActivity(activity)

        # assert that an entity is generated by an activity
        uri = "/entity/%s/generatedBy?generatedBy=%s" % (id_of(entity), activity["id"])
        activity = Activity(data=self.restPUT(uri))

        return activity

    def deleteProvenance(self, entity):
        """
        Removes provenance information from an Entity and deletes the associated Activity.

        :param entity: An Entity or Synapse ID to modify
        """

        activity = self.getProvenance(entity)
        if not activity:
            return

        uri = "/entity/%s/generatedBy" % id_of(entity)
        self.restDELETE(uri)

        # TODO: what happens if the activity is shared by more than one entity?
        uri = "/activity/%s" % activity["id"]
        self.restDELETE(uri)

    def _saveActivity(self, activity):
        if "id" in activity:
            # We're updating provenance
            uri = "/activity/%s" % activity["id"]
            activity = Activity(data=self.restPUT(uri, json.dumps(activity)))
        else:
            activity = self.restPOST("/activity", body=json.dumps(activity))
        return activity

    def updateActivity(self, activity):
        """
        Modifies an existing Activity.

        :param activity:  The Activity to be updated.

        :returns: An updated Activity object
        """
        if "id" not in activity:
            raise ValueError("The activity you want to update must exist on Synapse")
        return self._saveActivity(activity)

    def _convertProvenanceList(self, usedList, limitSearch=None):
        """Convert a list of synapse Ids, URLs and local files by replacing local files with Synapse Ids"""
        if usedList is None:
            return None
        usedList = [
            self.get(target, limitSearch=limitSearch)
            if (os.path.isfile(target) if isinstance(target, str) else False)
            else target
            for target in usedList
        ]
        return usedList

    ############################################################
    #                File handle service calls                 #
    ############################################################

    def _getFileHandleDownload(self, fileHandleId, objectId, objectType=None):
        """
        Gets the URL and the metadata as filehandle object for a filehandle or fileHandleId

        :param fileHandleId:   ID of fileHandle to download
        :param objectId:       The ID of the object associated with the file e.g. syn234
        :param objectType:     Type of object associated with a file e.g. FileEntity, TableEntity

        :returns: dictionary with keys: fileHandle, fileHandleId and preSignedURL
        """
        body = {
            "includeFileHandles": True,
            "includePreSignedURLs": True,
            "requestedFiles": [
                {
                    "fileHandleId": fileHandleId,
                    "associateObjectId": objectId,
                    "associateObjectType": objectType or "FileEntity",
                }
            ],
        }
        response = self.restPOST(
            "/fileHandle/batch", body=json.dumps(body), endpoint=self.fileHandleEndpoint
        )
        result = response["requestedFiles"][0]
        failure = result.get("failureCode")
        if failure == "NOT_FOUND":
            raise SynapseFileNotFoundError(
                "The fileHandleId %s could not be found" % fileHandleId
            )
        elif failure == "UNAUTHORIZED":
            raise SynapseError(
                "You are not authorized to access fileHandleId %s associated with the Synapse"
                " %s: %s" % (fileHandleId, objectType, objectId)
            )
        return result

    @staticmethod
    def _is_retryable_download_error(ex):
        # some exceptions caught during download indicate non-recoverable situations that
        # will not be remedied by a repeated download attempt.
        return not (
            (isinstance(ex, OSError) and ex.errno == errno.ENOSPC)
            or isinstance(ex, SynapseMd5MismatchError)  # out of disk space
        )

    def _downloadFileHandle(
        self, fileHandleId, objectId, objectType, destination, retries=5
    ):
        """
        Download a file from the given URL to the local file system.

        :param fileHandleId: id of the FileHandle to download
        :param objectId:     id of the Synapse object that uses the FileHandle e.g. "syn123"
        :param objectType:   type of the Synapse object that uses the FileHandle e.g. "FileEntity"
        :param destination:  destination on local file system
        :param retries:      (default=5) Number of download retries attempted before throwing an exception.

        :returns: path to downloaded file
        """
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        while retries > 0:
            try:
                fileResult = self._getFileHandleDownload(
                    fileHandleId, objectId, objectType
                )
                fileHandle = fileResult["fileHandle"]
                concreteType = fileHandle["concreteType"]
                storageLocationId = fileHandle.get("storageLocationId")

                if concreteType == concrete_types.EXTERNAL_OBJECT_STORE_FILE_HANDLE:
                    profile = self._get_client_authenticated_s3_profile(
                        fileHandle["endpointUrl"], fileHandle["bucket"]
                    )
                    downloaded_path = S3ClientWrapper.download_file(
                        fileHandle["bucket"],
                        fileHandle["endpointUrl"],
                        fileHandle["fileKey"],
                        destination,
                        profile_name=profile,
                        show_progress=not self.silent,
                    )

                elif (
                    sts_transfer.is_boto_sts_transfer_enabled(self)
                    and sts_transfer.is_storage_location_sts_enabled(
                        self, objectId, storageLocationId
                    )
                    and concreteType == concrete_types.S3_FILE_HANDLE
                ):

                    def download_fn(credentials):
                        return S3ClientWrapper.download_file(
                            fileHandle["bucketName"],
                            None,
                            fileHandle["key"],
                            destination,
                            credentials=credentials,
                            show_progress=not self.silent,
                            # pass through our synapse threading config to boto s3
                            transfer_config_kwargs={
                                "max_concurrency": self.max_threads
                            },
                        )

                    downloaded_path = sts_transfer.with_boto_sts_credentials(
                        download_fn,
                        self,
                        objectId,
                        "read_only",
                    )

                elif (
                    self.multi_threaded
                    and concreteType == concrete_types.S3_FILE_HANDLE
                    and fileHandle.get("contentSize", 0)
                    > multithread_download.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE
                ):
                    # run the download multi threaded if the file supports it, we're configured to do so,
                    # and the file is large enough that it would be broken into parts to take advantage of
                    # multiple downloading threads. otherwise it's more efficient to run the download as a simple
                    # single threaded URL download.
                    downloaded_path = self._download_from_url_multi_threaded(
                        fileHandleId,
                        objectId,
                        objectType,
                        destination,
                        expected_md5=fileHandle.get("contentMd5"),
                    )

                else:
                    downloaded_path = self._download_from_URL(
                        fileResult["preSignedURL"],
                        destination,
                        fileHandle["id"],
                        expected_md5=fileHandle.get("contentMd5"),
                    )
                self.cache.add(fileHandle["id"], downloaded_path)
                return downloaded_path

            except Exception as ex:
                if not self._is_retryable_download_error(ex):
                    raise

                exc_info = sys.exc_info()
                ex.progress = 0 if not hasattr(ex, "progress") else ex.progress
                self.logger.debug(
                    "\nRetrying download on error: [%s] after progressing %i bytes"
                    % (exc_info[0], ex.progress),
                    exc_info=True,
                )  # this will include stack trace
                if ex.progress == 0:  # No progress was made reduce remaining retries.
                    retries -= 1
                if retries <= 0:
                    # Re-raise exception
                    raise

        raise Exception("should not reach this line")

    def _download_from_url_multi_threaded(
        self, file_handle_id, object_id, object_type, destination, *, expected_md5=None
    ):
        destination = os.path.abspath(destination)
        temp_destination = utils.temp_download_filename(destination, file_handle_id)

        request = multithread_download.DownloadRequest(
            file_handle_id=int(file_handle_id),
            object_id=object_id,
            object_type=object_type,
            path=temp_destination,
        )

        multithread_download.download_file(self, request)

        if (
            expected_md5
        ):  # if md5 not set (should be the case for all except http download)
            actual_md5 = utils.md5_for_file(temp_destination).hexdigest()
            # check md5 if given
            if actual_md5 != expected_md5:
                try:
                    os.remove(temp_destination)
                except FileNotFoundError:
                    # file already does not exist. nothing to do
                    pass
                raise SynapseMd5MismatchError(
                    "Downloaded file {filename}'s md5 {md5} does not match expected MD5 of"
                    " {expected_md5}".format(
                        filename=temp_destination,
                        md5=actual_md5,
                        expected_md5=expected_md5,
                    )
                )
        # once download completed, rename to desired destination
        shutil.move(temp_destination, destination)

        return destination

    def _is_synapse_uri(self, uri):
        # check whether the given uri is hosted at the configured synapse repo endpoint
        uri_domain = urllib_urlparse.urlparse(uri).netloc
        synapse_repo_domain = urllib_urlparse.urlparse(self.repoEndpoint).netloc
        return uri_domain.lower() == synapse_repo_domain.lower()

    def _download_from_URL(
        self, url, destination, fileHandleId=None, expected_md5=None
    ):
        """
        Download a file from the given URL to the local file system.

        :param url:             source of download
        :param destination:     destination on local file system
        :param fileHandleId:    (optional) if given, the file will be given a temporary name that includes the file
                                handle id which allows resuming partial downloads of the same file from previous
                                sessions
        :param expected_md5:    (optional) if given, check that the MD5 of the downloaded file matched the expected MD5

        :returns: path to downloaded file
        """
        destination = os.path.abspath(destination)
        actual_md5 = None
        redirect_count = 0
        delete_on_md5_mismatch = True
        while redirect_count < REDIRECT_LIMIT:
            redirect_count += 1
            scheme = urllib_urlparse.urlparse(url).scheme
            if scheme == "file":
                delete_on_md5_mismatch = False
                destination = utils.file_url_to_path(url, verify_exists=True)
                if destination is None:
                    raise IOError("Local file (%s) does not exist." % url)
                break
            elif scheme == "sftp":
                username, password = self._getUserCredentials(url)
                destination = SFTPWrapper.download_file(
                    url, destination, username, password, show_progress=not self.silent
                )
                break
            elif scheme == "ftp":
                urllib_request.urlretrieve(url, destination)
                break
            elif scheme == "http" or scheme == "https":
                # if a partial download exists with the temporary name,
                temp_destination = utils.temp_download_filename(
                    destination, fileHandleId
                )
                range_header = (
                    {
                        "Range": "bytes={start}-".format(
                            start=os.path.getsize(temp_destination)
                        )
                    }
                    if os.path.exists(temp_destination)
                    else {}
                )

                # pass along synapse auth credentials only if downloading directly from synapse
                auth = self.credentials if self._is_synapse_uri(url) else None
                response = with_retry(
                    lambda: self._requests_session.get(
                        url,
                        headers=self._generate_headers(range_header),
                        stream=True,
                        allow_redirects=False,
                        auth=auth,
                    ),
                    verbose=self.debug,
                    **STANDARD_RETRY_PARAMS,
                )
                try:
                    exceptions._raise_for_status(response, verbose=self.debug)
                except SynapseHTTPError as err:
                    if err.response.status_code == 404:
                        raise SynapseError("Could not download the file at %s" % url)
                    elif (
                        err.response.status_code == 416
                    ):  # Requested Range Not Statisfiable
                        # this is a weird error when the client already finished downloading but the loop continues
                        # When this exception occurs, the range we request is guaranteed to be >= file size so we
                        # assume that the file has been fully downloaded, rename it to destination file
                        # and break out of the loop to perform the MD5 check.
                        # If it fails the user can retry with another download.
                        shutil.move(temp_destination, destination)
                        break
                    raise

                # handle redirects
                if response.status_code in [301, 302, 303, 307, 308]:
                    url = response.headers["location"]
                    # don't break, loop again
                else:
                    # get filename from content-disposition, if we don't have it already
                    if os.path.isdir(destination):
                        filename = utils.extract_filename(
                            content_disposition_header=response.headers.get(
                                "content-disposition", None
                            ),
                            default_filename=utils.guess_file_name(url),
                        )
                        destination = os.path.join(destination, filename)
                    # Stream the file to disk
                    if "content-length" in response.headers:
                        toBeTransferred = float(response.headers["content-length"])
                    else:
                        toBeTransferred = -1
                    transferred = 0

                    # Servers that respect the Range header return 206 Partial Content
                    if response.status_code == 206:
                        mode = "ab"
                        previouslyTransferred = os.path.getsize(temp_destination)
                        toBeTransferred += previouslyTransferred
                        transferred += previouslyTransferred
                        sig = utils.md5_for_file(temp_destination)
                    else:
                        mode = "wb"
                        previouslyTransferred = 0
                        sig = hashlib.new("md5", usedforsecurity=False)

                    try:
                        with open(temp_destination, mode) as fd:
                            t0 = time.time()
                            for nChunks, chunk in enumerate(
                                response.iter_content(FILE_BUFFER_SIZE)
                            ):
                                fd.write(chunk)
                                sig.update(chunk)

                                # the 'content-length' header gives the total number of bytes that will be transferred
                                # to us len(chunk) cannot be used to track progress because iter_content automatically
                                # decodes the chunks if the response body is encoded so the len(chunk) could be
                                # different from the total number of bytes we've read read from the response body
                                # response.raw.tell() is the total number of response body bytes transferred over the
                                # wire so far
                                transferred = (
                                    response.raw.tell() + previouslyTransferred
                                )
                                self._print_transfer_progress(
                                    transferred,
                                    toBeTransferred,
                                    "Downloading ",
                                    os.path.basename(destination),
                                    dt=time.time() - t0,
                                )
                    except (
                        Exception
                    ) as ex:  # We will add a progress parameter then push it back to retry.
                        ex.progress = transferred - previouslyTransferred
                        raise

                    # verify that the file was completely downloaded and retry if it is not complete
                    if toBeTransferred > 0 and transferred < toBeTransferred:
                        self.logger.warning(
                            "\nRetrying download because the connection ended early.\n"
                        )
                        continue

                    actual_md5 = sig.hexdigest()
                    # rename to final destination
                    shutil.move(temp_destination, destination)
                    break
            else:
                self.logger.error("Unable to download URLs of type %s" % scheme)
                return None

        else:  # didn't break out of loop
            raise SynapseHTTPError("Too many redirects")

        if (
            actual_md5 is None
        ):  # if md5 not set (should be the case for all except http download)
            actual_md5 = utils.md5_for_file(destination).hexdigest()

        # check md5 if given
        if expected_md5 and actual_md5 != expected_md5:
            if delete_on_md5_mismatch and os.path.exists(destination):
                os.remove(destination)
            raise SynapseMd5MismatchError(
                "Downloaded file {filename}'s md5 {md5} does not match expected MD5 of"
                " {expected_md5}".format(
                    filename=destination, md5=actual_md5, expected_md5=expected_md5
                )
            )

        return destination

    def _createExternalFileHandle(
        self, externalURL, mimetype=None, md5=None, fileSize=None
    ):
        """Create a new FileHandle representing an external URL."""
        fileName = externalURL.split("/")[-1]
        externalURL = utils.as_url(externalURL)
        fileHandle = {
            "concreteType": concrete_types.EXTERNAL_FILE_HANDLE,
            "fileName": fileName,
            "externalURL": externalURL,
            "contentMd5": md5,
            "contentSize": fileSize,
        }
        if mimetype is None:
            (mimetype, enc) = mimetypes.guess_type(externalURL, strict=False)
        if mimetype is not None:
            fileHandle["contentType"] = mimetype
        return self.restPOST(
            "/externalFileHandle", json.dumps(fileHandle), self.fileHandleEndpoint
        )

    def _createExternalObjectStoreFileHandle(
        self, s3_file_key, file_path, storage_location_id, mimetype=None
    ):
        if mimetype is None:
            mimetype, enc = mimetypes.guess_type(file_path, strict=False)
        file_handle = {
            "concreteType": concrete_types.EXTERNAL_OBJECT_STORE_FILE_HANDLE,
            "fileKey": s3_file_key,
            "fileName": os.path.basename(file_path),
            "contentMd5": utils.md5_for_file(file_path).hexdigest(),
            "contentSize": os.stat(file_path).st_size,
            "storageLocationId": storage_location_id,
            "contentType": mimetype,
        }

        return self.restPOST(
            "/externalFileHandle", json.dumps(file_handle), self.fileHandleEndpoint
        )

    def create_external_s3_file_handle(
        self,
        bucket_name,
        s3_file_key,
        file_path,
        *,
        parent=None,
        storage_location_id=None,
        mimetype=None,
    ):
        """
        Create an external S3 file handle for e.g. a file that has been uploaded directly to
        an external S3 storage location.

        :param bucket_name:             Name of the S3 bucket
        :param s3_file_key:             S3 key of the uploaded object
        :param file_path:               Local path of the uploaded file
        :param parent:                  Parent entity to create the file handle in, the file handle will be created
                                            in the default storage location of the parent. Mutually exclusive with
                                            storage_location_id
        :param storage_location_id:     Explicit storage location id to create the file handle in, mutually exclusive
                                            with parent
        :param mimetype:                Mimetype of the file, if known
        """

        if storage_location_id:
            if parent:
                raise ValueError("Pass parent or storage_location_id, not both")
        elif not parent:
            raise ValueError("One of parent or storage_location_id is required")
        else:
            upload_destination = self._getDefaultUploadDestination(parent)
            storage_location_id = upload_destination["storageLocationId"]

        if mimetype is None:
            mimetype, enc = mimetypes.guess_type(file_path, strict=False)

        file_handle = {
            "concreteType": concrete_types.S3_FILE_HANDLE,
            "key": s3_file_key,
            "bucketName": bucket_name,
            "fileName": os.path.basename(file_path),
            "contentMd5": utils.md5_for_file(file_path).hexdigest(),
            "contentSize": os.stat(file_path).st_size,
            "storageLocationId": storage_location_id,
            "contentType": mimetype,
        }

        return self.restPOST(
            "/externalFileHandle/s3",
            json.dumps(file_handle),
            endpoint=self.fileHandleEndpoint,
        )

    def _get_file_handle_as_creator(self, fileHandle):
        """Retrieve a fileHandle from the fileHandle service.
        You must be the creator of the filehandle to use this method. Otherwise, an 403-Forbidden error will be raised
        """

        uri = "/fileHandle/%s" % (id_of(fileHandle),)
        return self.restGET(uri, endpoint=self.fileHandleEndpoint)

    def _deleteFileHandle(self, fileHandle):
        """
        Delete the given file handle.

        Note: Only the user that created the FileHandle can delete it. Also, a FileHandle cannot be deleted if it is
        associated with a FileEntity or WikiPage
        """

        uri = "/fileHandle/%s" % (id_of(fileHandle),)
        self.restDELETE(uri, endpoint=self.fileHandleEndpoint)
        return fileHandle

    ############################################################
    #                    SFTP                                  #
    ############################################################

    def _getDefaultUploadDestination(self, parent_entity):
        return self.restGET(
            "/entity/%s/uploadDestination" % id_of(parent_entity),
            endpoint=self.fileHandleEndpoint,
        )

    def _getUserCredentials(self, url, username=None, password=None):
        """Get user credentials for a specified URL by either looking in the configFile or querying the user.

        :param username: username on server (optionally specified)
        :param password: password for authentication on the server (optionally specified)

        :returns: tuple of username, password
        """
        # Get authentication information from configFile

        parsedURL = urllib_urlparse.urlparse(url)
        baseURL = parsedURL.scheme + "://" + parsedURL.hostname

        config = self.getConfigFile(self.configPath)
        if username is None and config.has_option(baseURL, "username"):
            username = config.get(baseURL, "username")
        if password is None and config.has_option(baseURL, "password"):
            password = config.get(baseURL, "password")
        # If I still don't have a username and password prompt for it
        if username is None:
            username = getpass.getuser()  # Default to login name
            # Note that if we hit the following line from within nosetests in
            # Python 3, we get "TypeError: bad argument type for built-in operation".
            # Luckily, this case isn't covered in our test suite!
            user = input("Username for %s (%s):" % (baseURL, username))
            username = username if user == "" else user
        if password is None:
            password = getpass.getpass("Password for %s:" % baseURL)
        return username, password

    ############################################
    # Project/Folder storage location settings #
    ############################################

    def createStorageLocationSetting(self, storage_type, **kwargs):
        """
        Creates an IMMUTABLE storage location based on the specified type.

        For each storage_type, the following kwargs should be specified:

        ExternalObjectStorage: (S3-like (e.g. AWS S3 or Openstack) bucket not accessed by Synapse)
            - endpointUrl: endpoint URL of the S3 service (for example: 'https://s3.amazonaws.com')
            - bucket: the name of the bucket to use

        ExternalS3Storage: (Amazon S3 bucket accessed by Synapse)
            - bucket: the name of the bucket to use

        ExternalStorage: (SFTP or FTP storage location not accessed by Synapse)
            - url: the base URL for uploading to the external destination
            - supportsSubfolders(optional): does the destination support creating subfolders under the base url
              (default: false)

        ProxyStorage: (a proxy server that controls access to a storage)
            - secretKey: The encryption key used to sign all pre-signed URLs used to communicate with the proxy.
            - proxyUrl: The HTTPS URL of the proxy used for upload and download.

        Optional kwargs for ALL types:
            - banner: The optional banner to show every time a file is uploaded
            - description: The description to show the user when the user has to choose which upload destination to use

        :param storage_type:    the type of the StorageLocationSetting to create
        :param kwargs:          fields necessary for creation of the specified storage_type

        :return: a dict of the created StorageLocationSetting
        """
        upload_type_dict = {
            "ExternalObjectStorage": "S3",
            "ExternalS3Storage": "S3",
            "ExternalStorage": "SFTP",
            "ProxyStorage": "PROXYLOCAL",
        }

        if storage_type not in upload_type_dict:
            raise ValueError("Unknown storage_type: %s", storage_type)

        # ProxyStorageLocationSettings has an extra 's' at the end >:(
        kwargs["concreteType"] = (
            "org.sagebionetworks.repo.model.project."
            + storage_type
            + "LocationSetting"
            + ("s" if storage_type == "ProxyStorage" else "")
        )
        kwargs["uploadType"] = upload_type_dict[storage_type]

        return self.restPOST("/storageLocation", body=json.dumps(kwargs))

    def getMyStorageLocationSetting(self, storage_location_id):
        """
        Get a StorageLocationSetting by its id.

        :param storage_location_id: id of the StorageLocationSetting to retrieve.
                                    The corresponding StorageLocationSetting must have been created by this user.

        :return: a dict describing the StorageLocationSetting retrieved by its id
        """
        return self.restGET("/storageLocation/%s" % storage_location_id)

    def setStorageLocation(self, entity, storage_location_id):
        """
        Sets the storage location for a Project or Folder

        :param entity:              a Project or Folder to which the StorageLocationSetting is set
        :param storage_location_id: a StorageLocation id or a list of StorageLocation ids. Pass in None for the default
                                    Synapse storage.

        :return: The created or updated settings as a dict
        """
        if storage_location_id is None:
            storage_location_id = DEFAULT_STORAGE_LOCATION_ID
        locations = (
            storage_location_id
            if isinstance(storage_location_id, list)
            else [storage_location_id]
        )

        existing_setting = self.getProjectSetting(entity, "upload")
        if existing_setting is not None:
            existing_setting["locations"] = locations
            self.restPUT("/projectSettings", body=json.dumps(existing_setting))
            return self.getProjectSetting(entity, "upload")
        else:
            project_destination = {
                "concreteType": "org.sagebionetworks.repo.model.project.UploadDestinationListSetting",
                "settingsType": "upload",
                "locations": locations,
                "projectId": id_of(entity),
            }

            return self.restPOST(
                "/projectSettings", body=json.dumps(project_destination)
            )

    def getProjectSetting(self, project, setting_type):
        """
        Gets the ProjectSetting for a project.

        :param project:         Project entity or its id as a string
        :param setting_type:    type of setting. Choose from: {'upload', 'external_sync', 'requester_pays'}

        :return: The ProjectSetting as a dict or None if no settings of the specified type exist.
        """
        if setting_type not in {"upload", "external_sync", "requester_pays"}:
            raise ValueError("Invalid project_type: %s" % setting_type)

        response = self.restGET(
            "/projectSettings/{projectId}/type/{type}".format(
                projectId=id_of(project), type=setting_type
            )
        )
        return (
            response if response else None
        )  # if no project setting, a empty string is returned as the response

    def get_sts_storage_token(
        self, entity, permission, *, output_format="json", min_remaining_life=None
    ):
        """Get STS credentials for the given entity_id and permission, outputting it in the given format

        :param entity:          the entity or entity id whose credentials are being returned
        :param permission:      one of 'read_only' or 'read_write'
        :param output_format:   one of 'json', 'boto', 'shell', 'bash', 'cmd', 'powershell'
                                json: the dictionary returned from the Synapse STS API including expiration
                                boto: a dictionary compatible with a boto session (aws_access_key_id, etc)
                                shell: output commands for exporting credentials appropriate for the detected shell
                                bash: output commands for exporting credentials into a bash shell
                                cmd: output commands for exporting credentials into a windows cmd shell
                                powershell: output commands for exporting credentials into a windows powershell
        :param min_remaining_life: the minimum allowable remaining life on a cached token to return. if a cached token
            has left than this amount of time left a fresh token will be fetched
        """
        return sts_transfer.get_sts_credentials(
            self,
            id_of(entity),
            permission,
            output_format=output_format,
            min_remaining_life=min_remaining_life,
        )

    def create_s3_storage_location(
        self,
        *,
        parent=None,
        folder_name=None,
        folder=None,
        bucket_name=None,
        base_key=None,
        sts_enabled=False,
    ):
        """
        Create a storage location in the given parent, either in the given folder or by creating a new
        folder in that parent with the given name. This will both create a StorageLocationSetting,
        and a ProjectSetting together, optionally creating a new folder in which to locate it,
        and optionally enabling this storage location for access via STS. If enabling an existing folder for STS,
        it must be empty.

        :param parent:              The parent in which to locate the storage location (mutually exclusive with folder)
        :param folder_name:         The name of a new folder to create (mutually exclusive with folder)
        :param folder:              The existing folder in which to create the storage location
                                        (mutually exclusive with folder_name)
        :param bucket_name:         The name of an S3 bucket, if this is an external storage location,
                                        if None will use Synapse S3 storage
        :param base_key:            The base key of within the bucket, None to use the bucket root,
                                        only applicable if bucket_name is passed
        :param sts_enabled:         Whether this storage location should be STS enabled

        :return: a 3-tuple of the synapse Folder, a the storage location setting, and the project setting dictionaries
        """
        if folder_name and parent:
            if folder:
                raise ValueError(
                    "folder and  folder_name are mutually exclusive, only one should be passed"
                )

            folder = self.store(Folder(name=folder_name, parent=parent))

        elif not folder:
            raise ValueError("either folder or folder_name should be required")

        storage_location_kwargs = {
            "uploadType": "S3",
            "stsEnabled": sts_enabled,
        }

        if bucket_name:
            storage_location_kwargs[
                "concreteType"
            ] = concrete_types.EXTERNAL_S3_STORAGE_LOCATION_SETTING
            storage_location_kwargs["bucket"] = bucket_name
            if base_key:
                storage_location_kwargs["baseKey"] = base_key
        else:
            storage_location_kwargs[
                "concreteType"
            ] = concrete_types.SYNAPSE_S3_STORAGE_LOCATION_SETTING

        storage_location_setting = self.restPOST(
            "/storageLocation", json.dumps(storage_location_kwargs)
        )

        storage_location_id = storage_location_setting["storageLocationId"]
        project_setting = self.setStorageLocation(
            folder,
            storage_location_id,
        )

        return folder, storage_location_setting, project_setting

    ############################################################
    #                   CRUD for Evaluations                   #
    ############################################################

    def getEvaluation(self, id):
        """
        Gets an Evaluation object from Synapse.

        :param id:  The ID of the :py:class:`synapseclient.evaluation.Evaluation` to return.

        :return: an :py:class:`synapseclient.evaluation.Evaluation` object

        See: :py:mod:`synapseclient.evaluation`

        Example::

            evaluation = syn.getEvaluation(2005090)
        """

        evaluation_id = id_of(id)
        uri = Evaluation.getURI(evaluation_id)
        return Evaluation(**self.restGET(uri))

    # TODO: Should this be combined with getEvaluation?
    def getEvaluationByName(self, name):
        """
        Gets an Evaluation object from Synapse.

        :param name:  The name of the :py:class:`synapseclient.evaluation.Evaluation` to return.

        :return: an :py:class:`synapseclient.evaluation.Evaluation` object

        See: :py:mod:`synapseclient.evaluation`
        """
        uri = Evaluation.getByNameURI(name)
        return Evaluation(**self.restGET(uri))

    def getEvaluationByContentSource(self, entity):
        """
        Returns a generator over evaluations that derive their content from the given entity

        :param entity:  The :py:class:`synapseclient.entity.Project` whose Evaluations are to be fetched.

        :return: a Generator over the :py:class:`synapseclient.evaluation.Evaluation` objects for the given
         :py:class:`synapseclient.entity.Project`

        """

        entityId = id_of(entity)
        url = "/entity/%s/evaluation" % entityId

        for result in self._GET_paginated(url):
            yield Evaluation(**result)

    def _findTeam(self, name):
        """
        Retrieve a Teams matching the supplied name fragment
        """
        for result in self._GET_paginated("/teams?fragment=%s" % name):
            yield Team(**result)

    def _find_teams_for_principal(self, principal_id: str) -> typing.Iterator[Team]:
        """
        Retrieve a list of teams for the matching principal ID. If the principalId that is passed in is a team itself,
        or not found, this will return a generator that yields no results.

        :param principal_id: Identifier of a user or group.

        :return:  A generator that yields objects of type :py:class:`synapseclient.team.Team`
        """
        for result in self._GET_paginated(f"/user/{principal_id}/team"):
            yield Team(**result)

    def getTeam(self, id):
        """
        Finds a team with a given ID or name.

        :param id:  The ID or name of the team or a Team object to retrieve

        :return:  An object of type :py:class:`synapseclient.team.Team`
        """
        # Retrieves team id
        teamid = id_of(id)
        try:
            int(teamid)
        except (TypeError, ValueError):
            if isinstance(id, str):
                for team in self._findTeam(id):
                    if team.name == id:
                        teamid = team.id
                        break
                else:
                    raise ValueError('Can\'t find team "{}"'.format(teamid))
            else:
                raise ValueError('Can\'t find team "{}"'.format(teamid))
        return Team(**self.restGET("/team/%s" % teamid))

    def getTeamMembers(self, team):
        """
        Lists the members of the given team.

        :parameter team: A :py:class:`synapseclient.team.Team` object or a team's ID.
        :returns: a generator over :py:class:`synapseclient.team.TeamMember` objects.
        """
        for result in self._GET_paginated("/teamMembers/{id}".format(id=id_of(team))):
            yield TeamMember(**result)

    def _get_docker_digest(self, entity, docker_tag="latest"):
        """
        Get matching Docker sha-digest of a DockerRepository given a Docker tag

        :param entity:      Synapse id or entity of Docker repository
        :param docker_tag:  Docker tag
        :returns: Docker digest matching Docker tag
        """
        entityid = id_of(entity)
        uri = "/entity/{entityId}/dockerTag".format(entityId=entityid)

        docker_commits = self._GET_paginated(uri)
        docker_digest = None
        for commit in docker_commits:
            if docker_tag == commit["tag"]:
                docker_digest = commit["digest"]
        if docker_digest is None:
            raise ValueError(
                "Docker tag {docker_tag} not found.  Please specify a "
                "docker tag that exists. 'latest' is used as "
                "default.".format(docker_tag=docker_tag)
            )
        return docker_digest

    def get_team_open_invitations(self, team):
        """Retrieve the open requests submitted to a Team
        https://rest-docs.synapse.org/rest/GET/team/id/openInvitation.html

        :param team: A :py:class:`synapseclient.team.Team` object or a
                     team's ID.

        :returns: generator of MembershipRequest
        """
        teamid = id_of(team)
        request = "/team/{team}/openInvitation".format(team=teamid)
        open_requests = self._GET_paginated(request)
        return open_requests

    def get_membership_status(self, userid, team):
        """Retrieve a user's Team Membership Status bundle.
        https://rest-docs.synapse.org/rest/GET/team/id/member/principalId/membershipStatus.html

        :param user: Synapse user ID
        :param team: A :py:class:`synapseclient.team.Team` object or a
                     team's ID.

        :returns: dict of TeamMembershipStatus"""
        teamid = id_of(team)
        request = "/team/{team}/member/{user}/membershipStatus".format(
            team=teamid, user=userid
        )
        membership_status = self.restGET(request)
        return membership_status

    def _delete_membership_invitation(self, invitationid):
        """Delete open membership invitation

        :param invitationid: Open invitation id
        """
        self.restDELETE("/membershipInvitation/{id}".format(id=invitationid))

    def send_membership_invitation(
        self, teamId, inviteeId=None, inviteeEmail=None, message=None
    ):
        """Create a membership invitation and send an email notification
        to the invitee.

        :param teamId: Synapse teamId
        :param inviteeId: Synapse username or profile id of user
        :param inviteeEmail: Email of user
        :param message: Additional message for the user getting invited to the
                        team. Default to None.

        :returns: MembershipInvitation
        """

        invite_request = {"teamId": str(teamId), "message": message}
        if inviteeEmail is not None:
            invite_request["inviteeEmail"] = str(inviteeEmail)
        if inviteeId is not None:
            invite_request["inviteeId"] = str(inviteeId)

        response = self.restPOST(
            "/membershipInvitation", body=json.dumps(invite_request)
        )
        return response

    def invite_to_team(
        self, team, user=None, inviteeEmail=None, message=None, force=False
    ):
        """Invite user to a Synapse team via Synapse username or email
        (choose one or the other)

        :param syn: Synapse object
        :param team: A :py:class:`synapseclient.team.Team` object or a
                     team's ID.
        :param user: Synapse username or profile id of user
        :param inviteeEmail: Email of user
        :param message: Additional message for the user getting invited to the
                        team. Default to None.
        :param force: If an open invitation exists for the invitee,
                      the old invite will be cancelled. Default to False.

        :returns: MembershipInvitation or None if user is already a member
        """
        # Throw error if both user and email is specified and if both not
        # specified
        id_email_specified = inviteeEmail is not None and user is not None
        id_email_notspecified = inviteeEmail is None and user is None
        if id_email_specified or id_email_notspecified:
            raise ValueError("Must specify either 'user' or 'inviteeEmail'")

        teamid = id_of(team)
        is_member = False
        open_invitations = self.get_team_open_invitations(teamid)

        if user is not None:
            inviteeId = self.getUserProfile(user)["ownerId"]
            membership_status = self.get_membership_status(inviteeId, teamid)
            is_member = membership_status["isMember"]
            open_invites_to_user = [
                invitation
                for invitation in open_invitations
                if invitation.get("inviteeId") == inviteeId
            ]
        else:
            inviteeId = None
            open_invites_to_user = [
                invitation
                for invitation in open_invitations
                if invitation.get("inviteeEmail") == inviteeEmail
            ]
        # Only invite if the invitee is not a member and
        # if invitee doesn't have an open invitation unless force=True
        if not is_member and (not open_invites_to_user or force):
            # Delete all old invitations
            for invite in open_invites_to_user:
                self._delete_membership_invitation(invite["id"])
            return self.send_membership_invitation(
                teamid, inviteeId=inviteeId, inviteeEmail=inviteeEmail, message=message
            )
        if is_member:
            not_sent_reason = "invitee is already a member"
        else:
            not_sent_reason = (
                "invitee already has an open invitation "
                "Set force=True to send new invite."
            )

        self.logger.warning("No invitation sent: {}".format(not_sent_reason))
        # Return None if no invite is sent.
        return None

    def submit(
        self,
        evaluation,
        entity,
        name=None,
        team=None,
        silent=False,
        submitterAlias=None,
        teamName=None,
        dockerTag="latest",
    ):
        """
        Submit an Entity for `evaluation <Evaluation.html>`_.

        :param evaluation:      Evaluation queue to submit to
        :param entity:          The Entity containing the Submission
        :param name:            A name for this submission.
                                In the absent of this parameter, the entity name will be used.
        :param team:            (optional) A :py:class:`Team` object, ID or name of a Team that is registered for the
                                challenge
        :param silent:          Set to True to suppress output.
        :param submitterAlias:  (optional) A nickname, possibly for display in leaderboards in place of the submitter's
                                name
        :param teamName:        (deprecated) A synonym for submitterAlias
        :param dockerTag:       (optional) The Docker tag must be specified if the entity is a DockerRepository.
                                Defaults to "latest".


        :returns: A :py:class:`synapseclient.evaluation.Submission` object

        In the case of challenges, a team can optionally be provided to give credit to members of the team that
        contributed to the submission. The team must be registered for the challenge with which the given evaluation is
        associated. The caller must be a member of the submitting team.

        Example::

            evaluation = syn.getEvaluation(123)
            entity = syn.get('syn456')
            submission = syn.submit(evaluation, entity, name='Our Final Answer', team='Blue Team')
        """

        require_param(evaluation, "evaluation")
        require_param(entity, "entity")

        evaluation_id = id_of(evaluation)

        entity_id = id_of(entity)
        if isinstance(entity, synapseclient.DockerRepository):
            # Edge case if dockerTag is specified as None
            if dockerTag is None:
                raise ValueError(
                    "A dockerTag is required to submit a DockerEntity. Cannot be None"
                )
            docker_repository = entity["repositoryName"]
        else:
            docker_repository = None

        if "versionNumber" not in entity:
            entity = self.get(entity, downloadFile=False)
        # version defaults to 1 to hack around required version field and allow submission of files/folders
        entity_version = entity.get("versionNumber", 1)

        # default name of submission to name of entity
        if name is None and "name" in entity:
            name = entity["name"]

        team_id = None
        if team:
            team = self.getTeam(team)
            team_id = id_of(team)

        contributors, eligibility_hash = self._get_contributors(evaluation_id, team)

        # for backward compatible until we remove supports for teamName
        if not submitterAlias:
            if teamName:
                submitterAlias = teamName
            elif team and "name" in team:
                submitterAlias = team["name"]

        if isinstance(entity, synapseclient.DockerRepository):
            docker_digest = self._get_docker_digest(entity, dockerTag)
        else:
            docker_digest = None

        submission = {
            "evaluationId": evaluation_id,
            "name": name,
            "entityId": entity_id,
            "versionNumber": entity_version,
            "dockerDigest": docker_digest,
            "dockerRepositoryName": docker_repository,
            "teamId": team_id,
            "contributors": contributors,
            "submitterAlias": submitterAlias,
        }

        submitted = self._submit(submission, entity["etag"], eligibility_hash)

        # if we want to display the receipt message, we need the full object
        if not silent:
            if not (isinstance(evaluation, Evaluation)):
                evaluation = self.getEvaluation(evaluation_id)
            if "submissionReceiptMessage" in evaluation:
                self.logger.info(evaluation["submissionReceiptMessage"])

        return Submission(**submitted)

    def _submit(self, submission, entity_etag, eligibility_hash):
        require_param(submission, "submission")
        require_param(entity_etag, "entity_etag")
        # URI requires the etag of the entity and, in the case of a team submission, requires an eligibilityStateHash
        uri = "/evaluation/submission?etag=%s" % entity_etag
        if eligibility_hash:
            uri += "&submissionEligibilityHash={0}".format(eligibility_hash)
        submitted = self.restPOST(uri, json.dumps(submission))
        return submitted

    def _get_contributors(self, evaluation_id, team):
        if not evaluation_id or not team:
            return None, None

        team_id = id_of(team)
        # see https://rest-docs.synapse.org/rest/GET/evaluation/evalId/team/id/submissionEligibility.html
        eligibility = self.restGET(
            "/evaluation/{evalId}/team/{id}/submissionEligibility".format(
                evalId=evaluation_id, id=team_id
            )
        )

        if not eligibility["teamEligibility"]["isEligible"]:
            # Check team eligibility and raise an exception if not eligible
            if not eligibility["teamEligibility"]["isRegistered"]:
                raise SynapseError(
                    'Team "{team}" is not registered.'.format(team=team.name)
                )
            if eligibility["teamEligibility"]["isQuotaFilled"]:
                raise SynapseError(
                    'Team "{team}" has already submitted the full quota of submissions.'.format(
                        team=team.name
                    )
                )
            raise SynapseError('Team "{team}" is not eligible.'.format(team=team.name))

        # Include all team members who are eligible.
        contributors = [
            {"principalId": member["principalId"]}
            for member in eligibility["membersEligibility"]
            if member["isEligible"] and not member["hasConflictingSubmission"]
        ]
        return contributors, eligibility["eligibilityStateHash"]

    def _allowParticipation(
        self,
        evaluation,
        user,
        rights=["READ", "PARTICIPATE", "SUBMIT", "UPDATE_SUBMISSION"],
    ):
        """
        Grants the given user the minimal access rights to join and submit to an Evaluation.
        Note: The specification of this method has not been decided yet, so the method is likely to change in future.

        :param evaluation: An Evaluation object or Evaluation ID
        :param user:       Either a user group or the principal ID of a user to grant rights to.
                           To allow all users, use "PUBLIC".
                           To allow authenticated users, use "AUTHENTICATED_USERS".
        :param rights:     The access rights to give to the users.
                           Defaults to "READ", "PARTICIPATE", "SUBMIT", and "UPDATE_SUBMISSION".
        """

        # Check to see if the user is an ID or group
        userId = -1
        try:
            # TODO: is there a better way to differentiate between a userID and a group name?
            # What if a group is named with just numbers?
            userId = int(user)

            # Verify that the user exists
            try:
                self.getUserProfile(userId)
            except SynapseHTTPError as err:
                if err.response.status_code == 404:
                    raise SynapseError("The user (%s) does not exist" % str(userId))
                raise

        except ValueError:
            # Fetch the ID of the user group
            userId = self._getUserbyPrincipalIdOrName(user)

        if not isinstance(evaluation, Evaluation):
            evaluation = self.getEvaluation(id_of(evaluation))

        self.setPermissions(evaluation, userId, accessType=rights, overwrite=False)

    def getSubmissions(self, evaluation, status=None, myOwn=False, limit=20, offset=0):
        """
        :param evaluation: Evaluation to get submissions from.
        :param status:     Optionally filter submissions for a specific status.
                           One of {OPEN, CLOSED, SCORED,INVALID,VALIDATED,
                           EVALUATION_IN_PROGRESS,RECEIVED, REJECTED, ACCEPTED}
        :param myOwn:      Determines if only your Submissions should be fetched.
                           Defaults to False (all Submissions)
        :param limit:      Limits the number of submissions in a single response.
                           Because this method returns a generator and repeatedly
                           fetches submissions, this argument is limiting the
                           size of a single request and NOT the number of sub-
                           missions returned in total.
        :param offset:     Start iterating at a submission offset from the first
                           submission.

        :returns: A generator over :py:class:`synapseclient.evaluation.Submission` objects for an Evaluation

        Example::

            for submission in syn.getSubmissions(1234567):
                print(submission['entityId'])

        See: :py:mod:`synapseclient.evaluation`
        """

        evaluation_id = id_of(evaluation)
        uri = "/evaluation/%s/submission%s" % (evaluation_id, "" if myOwn else "/all")

        if status is not None:
            uri += "?status=%s" % status

        for result in self._GET_paginated(uri, limit=limit, offset=offset):
            yield Submission(**result)

    def _getSubmissionBundles(
        self, evaluation, status=None, myOwn=False, limit=20, offset=0
    ):
        """
        :param evaluation: Evaluation to get submissions from.
        :param status:     Optionally filter submissions for a specific status.
                           One of {OPEN, CLOSED, SCORED, INVALID}
        :param myOwn:      Determines if only your Submissions should be fetched.
                           Defaults to False (all Submissions)
        :param limit:      Limits the number of submissions coming back from the
                           service in a single response.
        :param offset:     Start iterating at a submission offset from the first
                           submission.

        :returns: A generator over dictionaries with keys 'submission' and 'submissionStatus'.

        Example::

            for sb in syn._getSubmissionBundles(1234567):
                print(sb['submission']['name'], \\
                      sb['submission']['submitterAlias'], \\
                      sb['submissionStatus']['status'], \\
                      sb['submissionStatus']['score'])

        This may later be changed to return objects, pending some thought on how submissions along with related status
        and annotations should be represented in the clients.

        See: :py:mod:`synapseclient.evaluation`
        """

        evaluation_id = id_of(evaluation)
        url = "/evaluation/%s/submission/bundle%s" % (
            evaluation_id,
            "" if myOwn else "/all",
        )
        if status is not None:
            url += "?status=%s" % status

        return self._GET_paginated(url, limit=limit, offset=offset)

    def getSubmissionBundles(
        self, evaluation, status=None, myOwn=False, limit=20, offset=0
    ):
        """
        Retrieve submission bundles (submission and submissions status) for an evaluation queue, optionally filtered by
        submission status and/or owner.

        :param evaluation: Evaluation to get submissions from.
        :param status:     Optionally filter submissions for a specific status.
                           One of {OPEN, CLOSED, SCORED, INVALID}
        :param myOwn:      Determines if only your Submissions should be fetched.
                           Defaults to False (all Submissions)
        :param limit:      Limits the number of submissions coming back from the
                           service in a single response.
        :param offset:     Start iterating at a submission offset from the first
                           submission.

        :returns: A generator over tuples containing a :py:class:`synapseclient.evaluation.Submission`
                  and a :py:class:`synapseclient.evaluation.SubmissionStatus`.

        Example::

            for submission, status in syn.getSubmissionBundles(evaluation):
                print(submission.name, \\
                      submission.submitterAlias, \\
                      status.status, \\
                      status.score)

        This may later be changed to return objects, pending some thought on how submissions along with related status
        and annotations should be represented in the clients.

        See: :py:mod:`synapseclient.evaluation`
        """
        for bundle in self._getSubmissionBundles(
            evaluation, status=status, myOwn=myOwn, limit=limit, offset=offset
        ):
            yield (
                Submission(**bundle["submission"]),
                SubmissionStatus(**bundle["submissionStatus"]),
            )

    def _GET_paginated(self, uri, limit=20, offset=0):
        """
        :param uri:     A URI that returns paginated results
        :param limit:   How many records should be returned per request
        :param offset:  At what record offset from the first should iteration start

        :returns: A generator over some paginated results

        The limit parameter is set at 20 by default. Using a larger limit results in fewer calls to the service, but if
        responses are large enough to be a burden on the service they may be truncated.
        """

        prev_num_results = sys.maxsize
        while prev_num_results > 0:
            uri = utils._limit_and_offset(uri, limit=limit, offset=offset)
            page = self.restGET(uri)
            results = page["results"] if "results" in page else page["children"]
            prev_num_results = len(results)

            for result in results:
                offset += 1
                yield result

    def _POST_paginated(self, uri, body, **kwargs):
        """
        :param uri:     A URI that returns paginated results
        :param body:    POST request payload

        :returns: A generator over some paginated results
        """

        next_page_token = None
        while True:
            body["nextPageToken"] = next_page_token
            response = self.restPOST(uri, body=json.dumps(body), **kwargs)
            next_page_token = response.get("nextPageToken")
            for item in response["page"]:
                yield item
            if next_page_token is None:
                break

    def getSubmission(self, id, **kwargs):
        """
        Gets a :py:class:`synapseclient.evaluation.Submission` object by its id.

        :param id:  The id of the submission to retrieve

        :return:  a :py:class:`synapseclient.evaluation.Submission` object

        See: :py:func:`synapseclient.Synapse.get` for information
             on the *downloadFile*, *downloadLocation*, and *ifcollision* parameters
        """

        submission_id = id_of(id)
        uri = Submission.getURI(submission_id)
        submission = Submission(**self.restGET(uri))

        # Pre-fetch the Entity tied to the Submission, if there is one
        if "entityId" in submission and submission["entityId"] is not None:
            entityBundleJSON = json.loads(submission["entityBundleJSON"])

            # getWithEntityBundle expects a bundle services v2 style
            # annotations dict, but the evaluations API may return
            # an older format annotations object in the encoded JSON
            # depending on when the original submission was made.
            annotations = entityBundleJSON.get("annotations")
            if annotations:
                entityBundleJSON["annotations"] = convert_old_annotation_json(
                    annotations
                )

            related = self._getWithEntityBundle(
                entityBundle=entityBundleJSON,
                entity=submission["entityId"],
                submission=submission_id,
                **kwargs,
            )
            submission.entity = related
            submission.filePath = related.get("path", None)

        return submission

    def getSubmissionStatus(self, submission):
        """
        Downloads the status of a Submission.

        :param submission: The Submission to lookup

        :returns: A :py:class:`synapseclient.evaluation.SubmissionStatus` object
        """

        submission_id = id_of(submission)
        uri = SubmissionStatus.getURI(submission_id)
        val = self.restGET(uri)
        return SubmissionStatus(**val)

    ############################################################
    #                      CRUD for Wikis                      #
    ############################################################

    def getWiki(self, owner, subpageId=None, version=None):
        """
        Get a :py:class:`synapseclient.wiki.Wiki` object from Synapse. Uses wiki2 API which supports versioning.

        :param owner:       The entity to which the Wiki is attached
        :param subpageId:   The id of the specific sub-page or None to get the root Wiki page
        :param version:     The version of the page to retrieve or None to retrieve the latest

        :return: a :py:class:`synapseclient.wiki.Wiki` object
        """
        uri = "/entity/{ownerId}/wiki2".format(ownerId=id_of(owner))
        if subpageId is not None:
            uri += "/{wikiId}".format(wikiId=subpageId)
        if version is not None:
            uri += "?wikiVersion={version}".format(version=version)

        wiki = self.restGET(uri)
        wiki["owner"] = owner
        wiki = Wiki(**wiki)

        path = self.cache.get(wiki.markdownFileHandleId)
        if not path:
            cache_dir = self.cache.get_cache_dir(wiki.markdownFileHandleId)
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
            path = self._downloadFileHandle(
                wiki["markdownFileHandleId"],
                wiki["id"],
                "WikiMarkdown",
                os.path.join(cache_dir, str(wiki.markdownFileHandleId) + ".md"),
            )
        try:
            import gzip

            with gzip.open(path) as f:
                markdown = f.read().decode("utf-8")
        except IOError:
            with open(path) as f:
                markdown = f.read().decode("utf-8")

        wiki.markdown = markdown
        wiki.markdown_path = path

        return wiki

    def getWikiHeaders(self, owner):
        """
        Retrieves the headers of all Wikis belonging to the owner (the entity to which the Wiki is attached).

        :param owner: An Entity

        :returns: A list of Objects with three fields: id, title and parentId.
        """

        uri = "/entity/%s/wikiheadertree" % id_of(owner)
        return [DictObject(**header) for header in self._GET_paginated(uri)]

    def _storeWiki(self, wiki, createOrUpdate):  # type: (Wiki, bool) -> Wiki
        """
        Stores or updates the given Wiki.

        :param wiki: A Wiki object

        :returns: An updated Wiki object
        """
        # Make sure the file handle field is a list
        if "attachmentFileHandleIds" not in wiki:
            wiki["attachmentFileHandleIds"] = []

        # Convert all attachments into file handles
        if wiki.get("attachments") is not None:
            for attachment in wiki["attachments"]:
                fileHandle = upload_synapse_s3(self, attachment)
                wiki["attachmentFileHandleIds"].append(fileHandle["id"])
            del wiki["attachments"]

        # Perform an update if the Wiki has an ID
        if "id" in wiki:
            updated_wiki = Wiki(
                owner=wiki.ownerId, **self.restPUT(wiki.putURI(), wiki.json())
            )

        # Perform a create if the Wiki has no ID
        else:
            try:
                updated_wiki = Wiki(
                    owner=wiki.ownerId, **self.restPOST(wiki.postURI(), wiki.json())
                )
            except SynapseHTTPError as err:
                # If already present we get an unhelpful SQL error
                if createOrUpdate and (
                    (
                        err.response.status_code == 400
                        and "DuplicateKeyException" in err.message
                    )
                    or err.response.status_code == 409
                ):
                    existing_wiki = self.getWiki(wiki.ownerId)

                    # overwrite everything except for the etag (this will keep unmodified fields in the existing wiki)
                    etag = existing_wiki["etag"]
                    existing_wiki.update(wiki)
                    existing_wiki.etag = etag

                    updated_wiki = Wiki(
                        owner=wiki.ownerId,
                        **self.restPUT(existing_wiki.putURI(), existing_wiki.json()),
                    )
                else:
                    raise
        return updated_wiki

    def getWikiAttachments(self, wiki):
        """
        Retrieve the attachments to a wiki page.

        :param wiki: the Wiki object for which the attachments are to be returned.

        :return: A list of file handles for the files attached to the Wiki.
        """
        uri = "/entity/%s/wiki/%s/attachmenthandles" % (wiki.ownerId, wiki.id)
        results = self.restGET(uri)
        file_handles = list(WikiAttachment(**fh) for fh in results["list"])
        return file_handles

    ############################################################
    #                      Tables                              #
    ############################################################

    def _waitForAsync(self, uri, request, endpoint=None):
        if endpoint is None:
            endpoint = self.repoEndpoint
        async_job_id = self.restPOST(
            uri + "/start", body=json.dumps(request), endpoint=endpoint
        )

        # https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html
        sleep = self.table_query_sleep
        start_time = time.time()
        lastMessage, lastProgress, lastTotal, progressed = "", 0, 1, False
        while time.time() - start_time < self.table_query_timeout:
            result = self.restGET(
                uri + "/get/%s" % async_job_id["token"], endpoint=endpoint
            )
            if result.get("jobState", None) == "PROCESSING":
                progressed = True
                message = result.get("progressMessage", lastMessage)
                progress = result.get("progressCurrent", lastProgress)
                total = result.get("progressTotal", lastTotal)
                if message != "":
                    self._print_transfer_progress(
                        progress, total, message, isBytes=False
                    )
                # Reset the time if we made progress (fix SYNPY-214)
                if message != lastMessage or lastProgress != progress:
                    start_time = time.time()
                    lastMessage, lastProgress, lastTotal = message, progress, total
                sleep = min(
                    self.table_query_max_sleep, sleep * self.table_query_backoff
                )
                doze(sleep)
            else:
                break
        else:
            raise SynapseTimeoutError(
                "Timeout waiting for query results: %0.1f seconds "
                % (time.time() - start_time)
            )
        if result.get("jobState", None) == "FAILED":
            raise SynapseError(
                result.get("errorMessage", None)
                + "\n"
                + result.get("errorDetails", None),
                asynchronousJobStatus=result,
            )
        if progressed:
            self._print_transfer_progress(total, total, message, isBytes=False)
        return result

    def getColumn(self, id):
        """
        Gets a Column object from Synapse by ID.

        See: :py:mod:`synapseclient.table.Column`

        :param id: the ID of the column to retrieve

        :return: an object of type :py:class:`synapseclient.table.Column`

        Example::

            column = syn.getColumn(123)
        """
        return Column(**self.restGET(Column.getURI(id)))

    def getColumns(self, x, limit=100, offset=0):
        """
        Get the columns defined in Synapse either (1) corresponding to a set of column headers, (2) those for a given
        schema, or (3) those whose names start with a given prefix.

        :param x:       a list of column headers, a Table Entity object (Schema/EntityViewSchema), a Table's Synapse ID,
                        or a string prefix
        :param limit:   maximum number of columns to return (pagination parameter)
        :param offset:  the index of the first column to return (pagination parameter)
        :return:        a generator of Column objects
        """
        if x is None:
            uri = "/column"
            for result in self._GET_paginated(uri, limit=limit, offset=offset):
                yield Column(**result)
        elif isinstance(x, (list, tuple)):
            for header in x:
                try:
                    # if header is an integer, it's a columnID, otherwise it's an aggregate column, like "AVG(Foo)"
                    int(header)
                    yield self.getColumn(header)
                except ValueError:
                    # ignore aggregate column
                    pass
        elif isinstance(x, SchemaBase) or utils.is_synapse_id_str(x):
            for col in self.getTableColumns(x):
                yield col
        elif isinstance(x, str):
            uri = "/column?prefix=" + x
            for result in self._GET_paginated(uri, limit=limit, offset=offset):
                yield Column(**result)
        else:
            ValueError("Can't get columns for a %s" % type(x))

    def create_snapshot_version(
        self,
        table: typing.Union[
            EntityViewSchema, Schema, str, SubmissionViewSchema, Dataset
        ],
        comment: str = None,
        label: str = None,
        activity: typing.Union[Activity, str] = None,
        wait: bool = True,
    ) -> int:
        """Create a new Table Version, new View version, or new Dataset version.

        :param table:  The schema of the Table/View, or its ID.
        :param comment:  Optional snapshot comment.
        :param label:  Optional snapshot label.
        :param activity:  Optional activity ID applied to snapshot version.
        :param wait: True if this method should return the snapshot version after waiting for any necessary
                        asynchronous table updates to complete. If False this method will return
                        as soon as any updates are initiated.
        :return: the snapshot version number if wait=True, None if wait=False
        """
        ent = self.get(id_of(table), downloadFile=False)
        if isinstance(ent, (EntityViewSchema, SubmissionViewSchema, Dataset)):
            result = self._async_table_update(
                table,
                create_snapshot=True,
                comment=comment,
                label=label,
                activity=activity,
                wait=wait,
            )
        elif isinstance(ent, Schema):
            result = self._create_table_snapshot(
                table,
                comment=comment,
                label=label,
                activity=activity,
            )
        else:
            raise ValueError(
                "This function only accepts Synapse ids of Tables or Views"
            )

        # for consistency we return nothing if wait=False since we can't
        # supply the snapshot version on an async table update without waiting
        return result["snapshotVersionNumber"] if wait else None

    def _create_table_snapshot(
        self,
        table: typing.Union[Schema, str],
        comment: str = None,
        label: str = None,
        activity: typing.Union[Activity, str] = None,
    ) -> dict:
        """Creates Table snapshot

        :param table:  The schema of the Table
        :param comment:  Optional snapshot comment.
        :param label:  Optional snapshot label.
        :param activity:  Optional activity ID or activity instance applied to snapshot version.

        :return:  Snapshot Response
        """

        # check the activity id or object is provided
        activity_id = None
        if isinstance(activity, collections.abc.Mapping):
            if "id" not in activity:
                activity = self._saveActivity(activity)
            activity_id = activity["id"]
        elif activity is not None:
            activity_id = str(activity)

        snapshot_body = {
            "snapshotComment": comment,
            "snapshotLabel": label,
            "snapshotActivityId": activity_id,
        }
        new_body = {
            key: value for key, value in snapshot_body.items() if value is not None
        }
        snapshot = self.restPOST(
            "/entity/{}/table/snapshot".format(id_of(table)), body=json.dumps(new_body)
        )
        return snapshot

    def _async_table_update(
        self,
        table: typing.Union[EntityViewSchema, Schema, str, SubmissionViewSchema],
        changes: typing.List[dict] = [],
        create_snapshot: bool = False,
        comment: str = None,
        label: str = None,
        activity: str = None,
        wait: bool = True,
    ) -> dict:
        """Creates view updates and snapshots

        :param table:  The schema of the EntityView or its ID.
        :param changes: Array of Table changes
        :param create_snapshot: Create snapshot
        :param comment:  Optional snapshot comment.
        :param label:  Optional snapshot label.
        :param activity:  Optional activity ID applied to snapshot version.
        :param wait: True to wait for async table update to complete

        :return:  Snapshot Response
        """
        snapshot_options = {
            "snapshotComment": comment,
            "snapshotLabel": label,
            "snapshotActivityId": activity,
        }
        new_snapshot = {
            key: value for key, value in snapshot_options.items() if value is not None
        }
        table_update_body = {
            "changes": changes,
            "createSnapshot": create_snapshot,
            "snapshotOptions": new_snapshot,
        }

        uri = "/entity/{}/table/transaction/async".format(id_of(table))

        if wait:
            result = self._waitForAsync(uri, table_update_body)

        else:
            result = self.restPOST(
                "{}/start".format(uri), body=json.dumps(table_update_body)
            )

        return result

    def getTableColumns(self, table):
        """
        Retrieve the column models used in the given table schema.

        :param table:  the schema of the Table whose columns are to be retrieved

        :return:  a Generator over the Table's columns
        """
        uri = "/entity/{id}/column".format(id=id_of(table))
        # The returned object type for this service, PaginatedColumnModels, is a misnomer.
        # This service always returns the full list of results so the pagination does not not actually matter.
        for result in self.restGET(uri)["results"]:
            yield Column(**result)

    def tableQuery(self, query, resultsAs="csv", **kwargs):
        """
        Query a Synapse Table.

        :param query: query string in a `SQL-like syntax \
         <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html>`_, for example
            "SELECT * from syn12345"

        :param resultsAs:   select whether results are returned as a CSV file ("csv") or incrementally downloaded as
                            sets of rows ("rowset").

        You can receive query results either as a generator over rows or as a CSV file. For smallish tables, either
        method will work equally well. Use of a "rowset" generator allows rows to be processed one at a time and
        processing may be stopped before downloading the entire table.

        Optional keyword arguments differ for the two return types. For the "rowset" option,

        :param  limit:          specify the maximum number of rows to be returned, defaults to None
        :param offset:          don't return the first n rows, defaults to None
        :param isConsistent:    (**DEPRECATED**)

        For CSV files, there are several parameters to control the format of the resulting file:

        :param quoteCharacter:   default double quote
        :param escapeCharacter:  default backslash
        :param lineEnd:          defaults to os.linesep
        :param separator:        defaults to comma
        :param header:           True by default
        :param includeRowIdAndRowVersion: True by default
        :param downloadLocation: directory path to download the CSV file to

        :return: A Table object that serves as a wrapper around a CSV file (or generator over Row objects if
                 resultsAs="rowset").

        NOTE: When performing queries on frequently updated tables, the table can be inaccessible for a period leading
              to a timeout of the query.  Since the results are guaranteed to eventually be returned you can change the
              max timeout by setting the table_query_timeout variable of the Synapse object::

                  # Sets the max timeout to 5 minutes.
                  syn.table_query_timeout = 300

        """
        if resultsAs.lower() == "rowset":
            return TableQueryResult(self, query, **kwargs)
        elif resultsAs.lower() == "csv":
            # TODO: remove isConsistent because it has now been deprecated
            # from the backend
            if kwargs.get("isConsistent") is not None:
                kwargs.pop("isConsistent")
            return CsvFileTable.from_table_query(self, query, **kwargs)
        else:
            raise ValueError(
                "Unknown return type requested from tableQuery: " + str(resultsAs)
            )

    def _queryTable(
        self, query, limit=None, offset=None, isConsistent=True, partMask=None
    ):
        """
        Query a table and return the first page of results as a `QueryResultBundle \
         <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryResultBundle.html>`_.
        If the result contains a *nextPageToken*, following pages a retrieved by calling :py:meth:`~._queryTableNext`.

        :param partMask: Optional, default all. The 'partsMask' is a bit field for requesting
                         different elements in the resulting JSON bundle.
                            Query Results (queryResults) = 0x1
                            Query Count (queryCount) = 0x2
                            Select Columns (selectColumns) = 0x4
                            Max Rows Per Page (maxRowsPerPage) = 0x8
        """

        # See: https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryBundleRequest.html
        query_bundle_request = {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryBundleRequest",
            "query": {
                "sql": query,
                "isConsistent": isConsistent,
                "includeEntityEtag": True,
            },
        }

        if partMask:
            query_bundle_request["partMask"] = partMask
        if limit is not None:
            query_bundle_request["query"]["limit"] = limit
        if offset is not None:
            query_bundle_request["query"]["offset"] = offset
        query_bundle_request["query"]["isConsistent"] = isConsistent

        uri = "/entity/{id}/table/query/async".format(
            id=extract_synapse_id_from_query(query)
        )

        return self._waitForAsync(uri=uri, request=query_bundle_request)

    def _queryTableNext(self, nextPageToken, tableId):
        uri = "/entity/{id}/table/query/nextPage/async".format(id=tableId)
        return self._waitForAsync(uri=uri, request=nextPageToken)

    def _uploadCsv(
        self,
        filepath,
        schema,
        updateEtag=None,
        quoteCharacter='"',
        escapeCharacter="\\",
        lineEnd=os.linesep,
        separator=",",
        header=True,
        linesToSkip=0,
    ):
        """
        Send an `UploadToTableRequest \
         <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/UploadToTableRequest.html>`_ to Synapse.

        :param filepath:    Path of a `CSV <https://en.wikipedia.org/wiki/Comma-separated_values>`_ file.
        :param schema:      A table entity or its Synapse ID.
        :param updateEtag:  Any RowSet returned from Synapse will contain the current etag of the change set.
                            To update any rows from a RowSet the etag must be provided with the POST.

        :returns: `UploadToTableResult \
         <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/UploadToTableResult.html>`_
        """

        fileHandleId = multipart_upload_file(self, filepath, content_type="text/csv")

        uploadRequest = {
            "concreteType": "org.sagebionetworks.repo.model.table.UploadToTableRequest",
            "csvTableDescriptor": {
                "isFirstLineHeader": header,
                "quoteCharacter": quoteCharacter,
                "escapeCharacter": escapeCharacter,
                "lineEnd": lineEnd,
                "separator": separator,
            },
            "linesToSkip": linesToSkip,
            "tableId": id_of(schema),
            "uploadFileHandleId": fileHandleId,
        }

        if updateEtag:
            uploadRequest["updateEtag"] = updateEtag

        response = self._async_table_update(schema, changes=[uploadRequest], wait=True)
        self._check_table_transaction_response(response)

        return response

    def _check_table_transaction_response(self, response):
        for result in response["results"]:
            result_type = result["concreteType"]

            if result_type in {
                concrete_types.ROW_REFERENCE_SET_RESULTS,
                concrete_types.TABLE_SCHEMA_CHANGE_RESPONSE,
                concrete_types.UPLOAD_TO_TABLE_RESULT,
            }:
                # if these fail, it we would have gotten an HttpError before the results came back
                pass
            elif result_type == concrete_types.ENTITY_UPDATE_RESULTS:
                # TODO: output full response to error file when the logging JIRA issue gets pulled in
                successful_updates = []
                failed_updates = []
                for update_result in result["updateResults"]:
                    failure_code = update_result.get("failureCode")
                    failure_message = update_result.get("failureMessage")
                    entity_id = update_result.get("entityId")
                    if failure_code or failure_message:
                        failed_updates.append(update_result)
                    else:
                        successful_updates.append(entity_id)

                if failed_updates:
                    raise SynapseError(
                        "Not all of the entities were updated."
                        " Successful updates: %s.  Failed updates: %s"
                        % (successful_updates, failed_updates)
                    )

            else:
                warnings.warn(
                    "Unexpected result from a table transaction of type [%s]."
                    " Please check the result to make sure it is correct. %s"
                    % (result_type, result)
                )

    def _queryTableCsv(
        self,
        query,
        quoteCharacter='"',
        escapeCharacter="\\",
        lineEnd=os.linesep,
        separator=",",
        header=True,
        includeRowIdAndRowVersion=True,
        downloadLocation=None,
    ):
        """
        Query a Synapse Table and download a CSV file containing the results.

        Sends a `DownloadFromTableRequest \
         <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/DownloadFromTableRequest.html>`_ to Synapse.

        :return: a tuple containing a `DownloadFromTableResult \
         <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/DownloadFromTableResult.html>`_

        The DownloadFromTableResult object contains these fields:
         * headers:             ARRAY<STRING>, The list of ColumnModel IDs that describes the rows of this set.
         * resultsFileHandleId: STRING, The resulting file handle ID can be used to download the CSV file created by
                                this query.
         * concreteType:        STRING
         * etag:                STRING, Any RowSet returned from Synapse will contain the current etag of the change
                                set.
                                To update any rows from a RowSet the etag must be provided with the POST.
         * tableId:             STRING, The ID of the table identified in the from clause of the table query.
        """

        download_from_table_request = {
            "concreteType": "org.sagebionetworks.repo.model.table.DownloadFromTableRequest",
            "csvTableDescriptor": {
                "isFirstLineHeader": header,
                "quoteCharacter": quoteCharacter,
                "escapeCharacter": escapeCharacter,
                "lineEnd": lineEnd,
                "separator": separator,
            },
            "sql": query,
            "writeHeader": header,
            "includeRowIdAndRowVersion": includeRowIdAndRowVersion,
            "includeEntityEtag": True,
        }

        uri = "/entity/{id}/table/download/csv/async".format(
            id=extract_synapse_id_from_query(query)
        )
        download_from_table_result = self._waitForAsync(
            uri=uri, request=download_from_table_request
        )
        file_handle_id = download_from_table_result["resultsFileHandleId"]
        cached_file_path = self.cache.get(
            file_handle_id=file_handle_id, path=downloadLocation
        )
        if cached_file_path is not None:
            return download_from_table_result, cached_file_path

        if downloadLocation:
            download_dir = self._ensure_download_location_is_directory(downloadLocation)
        else:
            download_dir = self.cache.get_cache_dir(file_handle_id)

        os.makedirs(download_dir, exist_ok=True)
        filename = f"SYNAPSE_TABLE_QUERY_{file_handle_id}.csv"
        path = self._downloadFileHandle(
            file_handle_id,
            extract_synapse_id_from_query(query),
            "TableEntity",
            os.path.join(download_dir, filename),
        )

        return download_from_table_result, path

    # This is redundant with syn.store(Column(...)) and will be removed unless people prefer this method.
    def createColumn(
        self, name, columnType, maximumSize=None, defaultValue=None, enumValues=None
    ):
        columnModel = Column(
            name=name,
            columnType=columnType,
            maximumSize=maximumSize,
            defaultValue=defaultValue,
            enumValue=enumValues,
        )
        return Column(**self.restPOST("/column", json.dumps(columnModel)))

    def createColumns(self, columns):
        """
        Creates a batch of :py:class:`synapseclient.table.Column` s within a single request.

        :param columns: a list of :py:class:`synapseclient.table.Column` objects

        :return: a list of :py:class:`synapseclient.table.Column` objects that have been created in Synapse
        """
        request_body = {
            "concreteType": "org.sagebionetworks.repo.model.ListWrapper",
            "list": list(columns),
        }
        response = self.restPOST("/column/batch", json.dumps(request_body))
        return [Column(**col) for col in response["list"]]

    def _getColumnByName(self, schema, column_name):
        """
        Given a schema and a column name, get the corresponding py:class:`Column` object.
        """
        for column in self.getColumns(schema):
            if column.name == column_name:
                return column
        return None

    def downloadTableColumns(self, table, columns, downloadLocation=None, **kwargs):
        """
        Bulk download of table-associated files.

        :param table:               table query result
        :param columns:             a list of column names as strings
        :param downloadLocation:    directory into which to download the files

        :returns: a dictionary from file handle ID to path in the local file system.

        For example, consider a Synapse table whose ID is "syn12345" with two columns of type FILEHANDLEID named 'foo'
        and 'bar'. The associated files are JSON encoded, so we might retrieve the files from Synapse and load for the
        second 100 of those rows as shown here::

            import json

            results = syn.tableQuery('SELECT * FROM syn12345 LIMIT 100 OFFSET 100')
            file_map = syn.downloadTableColumns(results, ['foo', 'bar'])

            for file_handle_id, path in file_map.items():
                with open(path) as f:
                    data[file_handle_id] = f.read()

        """

        RETRIABLE_FAILURE_CODES = ["EXCEEDS_SIZE_LIMIT"]
        MAX_DOWNLOAD_TRIES = 100
        max_files_per_request = kwargs.get("max_files_per_request", 2500)
        # Rowset tableQuery result not allowed
        if isinstance(table, TableQueryResult):
            raise ValueError(
                "downloadTableColumn doesn't work with rowsets. Please use default tableQuery settings."
            )
        if isinstance(columns, str):
            columns = [columns]
        if not isinstance(columns, collections.abc.Iterable):
            raise TypeError("Columns parameter requires a list of column names")

        (
            file_handle_associations,
            file_handle_to_path_map,
        ) = self._build_table_download_file_handle_list(
            table,
            columns,
            downloadLocation,
        )

        self.logger.info(
            "Downloading %d files, %d cached locally"
            % (len(file_handle_associations), len(file_handle_to_path_map))
        )

        permanent_failures = collections.OrderedDict()

        attempts = 0
        while len(file_handle_associations) > 0 and attempts < MAX_DOWNLOAD_TRIES:
            attempts += 1

            file_handle_associations_batch = file_handle_associations[
                :max_files_per_request
            ]

            # ------------------------------------------------------------
            # call async service to build zip file
            # ------------------------------------------------------------

            # returns a BulkFileDownloadResponse:
            #   https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/BulkFileDownloadResponse.html
            request = dict(
                concreteType="org.sagebionetworks.repo.model.file.BulkFileDownloadRequest",
                requestedFiles=file_handle_associations_batch,
            )
            response = self._waitForAsync(
                uri="/file/bulk/async",
                request=request,
                endpoint=self.fileHandleEndpoint,
            )

            # ------------------------------------------------------------
            # download zip file
            # ------------------------------------------------------------

            temp_dir = tempfile.mkdtemp()
            zipfilepath = os.path.join(temp_dir, "table_file_download.zip")
            try:
                zipfilepath = self._downloadFileHandle(
                    response["resultZipFileHandleId"],
                    table.tableId,
                    "TableEntity",
                    zipfilepath,
                )
                # TODO handle case when no zip file is returned
                # TODO test case when we give it partial or all bad file handles
                # TODO test case with deleted fileHandleID
                # TODO return null for permanent failures

                # ------------------------------------------------------------
                # unzip into cache
                # ------------------------------------------------------------

                if downloadLocation:
                    download_dir = self._ensure_download_location_is_directory(
                        downloadLocation
                    )

                with zipfile.ZipFile(zipfilepath) as zf:
                    # the directory structure within the zip follows that of the cache:
                    # {fileHandleId modulo 1000}/{fileHandleId}/{fileName}
                    for summary in response["fileSummary"]:
                        if summary["status"] == "SUCCESS":
                            if not downloadLocation:
                                download_dir = self.cache.get_cache_dir(
                                    summary["fileHandleId"]
                                )

                            filepath = extract_zip_file_to_directory(
                                zf, summary["zipEntryName"], download_dir
                            )
                            self.cache.add(summary["fileHandleId"], filepath)
                            file_handle_to_path_map[summary["fileHandleId"]] = filepath
                        elif summary["failureCode"] not in RETRIABLE_FAILURE_CODES:
                            permanent_failures[summary["fileHandleId"]] = summary
            finally:
                if os.path.exists(zipfilepath):
                    os.remove(zipfilepath)

            # Do we have remaining files to download?
            file_handle_associations = [
                fha
                for fha in file_handle_associations
                if fha["fileHandleId"] not in file_handle_to_path_map
                and fha["fileHandleId"] not in permanent_failures.keys()
            ]

        # TODO if there are files we still haven't downloaded

        return file_handle_to_path_map

    def _build_table_download_file_handle_list(self, table, columns, downloadLocation):
        # ------------------------------------------------------------
        # build list of file handles to download
        # ------------------------------------------------------------
        cols_not_found = [
            c for c in columns if c not in [h.name for h in table.headers]
        ]
        if len(cols_not_found) > 0:
            raise ValueError(
                "Columns not found: "
                + ", ".join('"' + col + '"' for col in cols_not_found)
            )
        col_indices = [i for i, h in enumerate(table.headers) if h.name in columns]
        # see: https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/BulkFileDownloadRequest.html
        file_handle_associations = []
        file_handle_to_path_map = collections.OrderedDict()
        seen_file_handle_ids = (
            set()
        )  # ensure not sending duplicate requests for the same FileHandle IDs
        for row in table:
            for col_index in col_indices:
                file_handle_id = row[col_index]
                if is_integer(file_handle_id):
                    path_to_cached_file = self.cache.get(
                        file_handle_id, path=downloadLocation
                    )
                    if path_to_cached_file:
                        file_handle_to_path_map[file_handle_id] = path_to_cached_file
                    elif file_handle_id not in seen_file_handle_ids:
                        file_handle_associations.append(
                            dict(
                                associateObjectType="TableEntity",
                                fileHandleId=file_handle_id,
                                associateObjectId=table.tableId,
                            )
                        )
                    seen_file_handle_ids.add(file_handle_id)
                else:
                    warnings.warn("Weird file handle: %s" % file_handle_id)
        return file_handle_associations, file_handle_to_path_map

    def _get_default_view_columns(self, view_type, view_type_mask=None):
        """Get default view columns"""
        uri = f"/column/tableview/defaults?viewEntityType={view_type}"
        if view_type_mask:
            uri += f"&viewTypeMask={view_type_mask}"
        return [Column(**col) for col in self.restGET(uri)["list"]]

    def _get_annotation_view_columns(
        self, scope_ids: list, view_type: str, view_type_mask: str = None
    ) -> list:
        """Get all the columns of a submission of entity view based on existing annotations

        :param scope_ids:  List of Evaluation Queue or Project/Folder Ids
        :param view_type: submissionview or entityview
        :param view_type_mask: Bit mask representing the types to include in the view.

        :returns: list of columns
        """
        columns = []
        next_page_token = None
        while True:
            view_scope = {
                "concreteType": "org.sagebionetworks.repo.model.table.ViewColumnModelRequest",
                "viewScope": {
                    "scope": scope_ids,
                    "viewEntityType": view_type,
                    "viewTypeMask": view_type_mask,
                },
            }
            if next_page_token:
                view_scope["nextPageToken"] = next_page_token
            response = self._waitForAsync(
                uri="/column/view/scope/async", request=view_scope
            )
            columns.extend(Column(**column) for column in response["results"])
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                break
        return columns

    ############################################################
    #              CRUD for Entities (properties)              #
    ############################################################

    def _getEntity(self, entity, version=None):
        """
        Get an entity from Synapse.

        :param entity:  A Synapse ID, a dictionary representing an Entity, or a Synapse Entity object
        :param version: The version number to fetch

        :returns: A dictionary containing an Entity's properties
        """

        uri = "/entity/" + id_of(entity)
        if version:
            uri += "/version/%d" % version
        return self.restGET(uri)

    def _createEntity(self, entity):
        """
        Create a new entity in Synapse.

        :param entity: A dictionary representing an Entity or a Synapse Entity object

        :returns: A dictionary containing an Entity's properties
        """

        return self.restPOST(uri="/entity", body=json.dumps(get_properties(entity)))

    def _updateEntity(self, entity, incrementVersion=True, versionLabel=None):
        """
        Update an existing entity in Synapse.

        :param entity: A dictionary representing an Entity or a Synapse Entity object
        :param incrementVersion: whether to increment the entity version (if Versionable)
        :param versionLabel: a label for the entity version (if Versionable)


        :returns: A dictionary containing an Entity's properties
        """

        uri = "/entity/%s" % id_of(entity)

        params = {}
        if is_versionable(entity):
            if versionLabel:
                # a versionLabel implicitly implies incrementing
                incrementVersion = True
            elif incrementVersion and "versionNumber" in entity:
                versionLabel = str(entity["versionNumber"] + 1)

            if incrementVersion:
                entity["versionLabel"] = versionLabel
                params["newVersion"] = "true"

        return self.restPUT(uri, body=json.dumps(get_properties(entity)), params=params)

    def findEntityId(self, name, parent=None):
        """
        Find an Entity given its name and parent.

        :param name:    name of the entity to find
        :param parent:  An Entity object or the Id of an entity as a string. Omit if searching for a Project by name

        :return: the Entity ID or None if not found
        """
        # when we want to search for a project by name. set parentId as None instead of ROOT_ENTITY
        entity_lookup_request = {
            "parentId": id_of(parent) if parent else None,
            "entityName": name,
        }
        try:
            return self.restPOST(
                "/entity/child", body=json.dumps(entity_lookup_request)
            ).get("id")
        except SynapseHTTPError as e:
            if (
                e.response.status_code == 404
            ):  # a 404 error is raised if the entity does not exist
                return None
            raise

    ############################################################
    #                       Send Message                       #
    ############################################################
    def sendMessage(
        self, userIds, messageSubject, messageBody, contentType="text/plain"
    ):
        """
        send a message via Synapse.

        :param userIds:         A list of user IDs to which the message is to be sent
        :param messageSubject:  The subject for the message
        :param messageBody:     The body of the message
        :param contentType:     optional contentType of message body (default="text/plain")
                                Should be one of "text/plain" or "text/html"

        :returns: The metadata of the created message
        """

        fileHandleId = multipart_upload_string(
            self, messageBody, content_type=contentType
        )
        message = dict(
            recipients=userIds, subject=messageSubject, fileHandleId=fileHandleId
        )
        return self.restPOST(uri="/message", body=json.dumps(message))

    ############################################################
    #                   Low level Rest calls                   #
    ############################################################

    def _generate_headers(self, headers=None):
        """Generate headers (auth headers produced separately by credentials object)"""

        if headers is None:
            headers = dict(self.default_headers)
        headers.update(synapseclient.USER_AGENT)

        return headers

    def _handle_synapse_http_error(self, response):
        """Raise errors as appropriate for returned Synapse http status codes"""

        try:
            exceptions._raise_for_status(response, verbose=self.debug)
        except exceptions.SynapseHTTPError as ex:
            # if we get a unauthenticated or forbidden error and the user is not logged in
            # then we raise it as an authentication error.
            # we can't know for certain that logging in to their particular account will grant them
            # access to this resource but more than likely it's the cause of this error.
            if response.status_code in (401, 403) and not self.credentials:
                raise SynapseAuthenticationError(
                    "You are not logged in and do not have access to a requested resource."
                ) from ex

            raise

    def _rest_call(
        self,
        method,
        uri,
        data,
        endpoint,
        headers,
        retryPolicy,
        requests_session,
        **kwargs,
    ):
        uri, headers = self._build_uri_and_headers(
            uri, endpoint=endpoint, headers=headers
        )

        retryPolicy = self._build_retry_policy(retryPolicy)
        requests_session = requests_session or self._requests_session

        auth = kwargs.pop("auth", self.credentials)
        requests_method_fn = getattr(requests_session, method)
        response = with_retry(
            lambda: requests_method_fn(
                uri,
                data=data,
                headers=headers,
                auth=auth,
                **kwargs,
            ),
            verbose=self.debug,
            **retryPolicy,
        )

        self._handle_synapse_http_error(response)
        return response

    def restGET(
        self,
        uri,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session=None,
        **kwargs,
    ):
        """
        Sends an HTTP GET request to the Synapse server.

        :param uri:                 URI on which get is performed
        :param endpoint:            Server endpoint, defaults to self.repoEndpoint
        :param headers:             Dictionary of headers to use rather than the API-key-signed default set of headers
        :param requests_session:    an external requests.Session object to use when making this specific call
        :param kwargs:              Any other arguments taken by a
                                    `requests <http://docs.python-requests.org/en/latest/>`_ method

        :returns: JSON encoding of response
        """
        response = self._rest_call(
            "get", uri, None, endpoint, headers, retryPolicy, requests_session, **kwargs
        )
        return self._return_rest_body(response)

    def restPOST(
        self,
        uri,
        body,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session=None,
        **kwargs,
    ):
        """
        Sends an HTTP POST request to the Synapse server.

        :param uri:                 URI on which get is performed
        :param endpoint:            Server endpoint, defaults to self.repoEndpoint
        :param body:                The payload to be delivered
        :param headers:             Dictionary of headers to use rather than the API-key-signed default set of headers
        :param requests_session:    an external requests.Session object to use when making this specific call
        :param kwargs:              Any other arguments taken by a
                                    `requests <http://docs.python-requests.org/en/latest/>`_ method

        :returns: JSON encoding of response
        """
        response = self._rest_call(
            "post",
            uri,
            body,
            endpoint,
            headers,
            retryPolicy,
            requests_session,
            **kwargs,
        )
        return self._return_rest_body(response)

    def restPUT(
        self,
        uri,
        body=None,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session=None,
        **kwargs,
    ):
        """
        Sends an HTTP PUT request to the Synapse server.

        :param uri:                 URI on which get is performed
        :param endpoint:            Server endpoint, defaults to self.repoEndpoint
        :param body:                The payload to be delivered
        :param headers:             Dictionary of headers to use rather than the API-key-signed default set of headers
        :param requests_session:    an external requests.session object to use when making this specific call
        :param kwargs:              Any other arguments taken by a
                                    `requests <http://docs.python-requests.org/en/latest/>`_ method

        :returns: JSON encoding of response
        """
        response = self._rest_call(
            "put", uri, body, endpoint, headers, retryPolicy, requests_session, **kwargs
        )
        return self._return_rest_body(response)

    def restDELETE(
        self,
        uri,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session=None,
        **kwargs,
    ):
        """
        Sends an HTTP DELETE request to the Synapse server.

        :param uri:                 URI of resource to be deleted
        :param endpoint:            Server endpoint, defaults to self.repoEndpoint
        :param headers:             Dictionary of headers to use rather than the API-key-signed default set of headers
        :param requests_session:    an external requests.session object to use when making this specific call
        :param kwargs:              Any other arguments taken by a
                                    `requests <http://docs.python-requests.org/en/latest/>`_ method
        """
        self._rest_call(
            "delete",
            uri,
            None,
            endpoint,
            headers,
            retryPolicy,
            requests_session,
            **kwargs,
        )

    def _build_uri_and_headers(self, uri, endpoint=None, headers=None):
        """Returns a tuple of the URI and headers to request with."""

        if endpoint is None:
            endpoint = self.repoEndpoint

        # Check to see if the URI is incomplete (i.e. a Synapse URL)
        # In that case, append a Synapse endpoint to the URI
        parsedURL = urllib_urlparse.urlparse(uri)
        if parsedURL.netloc == "":
            uri = endpoint + uri

        if headers is None:
            headers = self._generate_headers()
        return uri, headers

    def _build_retry_policy(self, retryPolicy={}):
        """Returns a retry policy to be passed onto _with_retry."""

        defaults = dict(STANDARD_RETRY_PARAMS)
        defaults.update(retryPolicy)
        return defaults

    def _return_rest_body(self, response):
        """Returns either a dictionary or a string depending on the 'content-type' of the response."""
        if is_json(response.headers.get("content-type", None)):
            return response.json()
        return response.text
