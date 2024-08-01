"""
This module contains classes that are responsible for retrieving synapse authentication
information (e.g. authToken) from a source (e.g. login args, config file).
"""

import abc
import os
from typing import TYPE_CHECKING, Dict, Tuple, Union

from synapseclient.api import get_config_authentication
from synapseclient.core.credentials.cred_data import (
    SynapseAuthTokenCredentials,
    SynapseCredentials,
)
from synapseclient.core.exceptions import SynapseAuthenticationError

if TYPE_CHECKING:
    from synapseclient import Synapse


class SynapseCredentialsProvider(metaclass=abc.ABCMeta):
    """
    A credential provider is responsible for retrieving synapse authentication
    information (e.g. authToken) from a source (e.g. login args, config file),
    and use them to return a
    [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials]
    instance.
    """

    @abc.abstractmethod
    def _get_auth_info(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Tuple[None, None]:
        """
        Subclasses must implement this to decide how to obtain an authentication token.
        For any of these values, return None if it is not possible to get that value.

        Not all implementations will need to make use of the user_login_args parameter
        or syn. These parameters provide context about the Synapse client's configuration
        and login() arguments.

        Arguments:
            syn: Synapse client instance
            user_login_args: subset of arguments passed during syn.login()

        Returns:
            Tuple of (username, bearer authentication token e.g. a personal access token),
            any of these values could None if it is not available.
        """
        return None, None

    def get_synapse_credentials(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Union[SynapseCredentials, None]:
        """
        Returns
        [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials]
        if this provider is able to get valid credentials, returns None otherwise.

        Arguments:
            syn: Synapse client instance
            user_login_args: subset of arguments passed during syn.login()

        Returns:
            [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials]
                if valid credentials can be found by this provider, None otherwise.
        """
        return self._create_synapse_credential(
            syn, *self._get_auth_info(syn=syn, user_login_args=user_login_args)
        )

    def _create_synapse_credential(
        self, syn: "Synapse", username: str, auth_token: str
    ) -> Union[SynapseCredentials, None]:
        if auth_token is not None:
            credentials = SynapseAuthTokenCredentials(auth_token)
            profile = syn.restGET("/userProfile", auth=credentials)
            profile_username = profile.get("userName")
            profile_emails = profile.get("emails", [])
            profile_displayname = profile.get("displayName")

            if username and (
                username != profile_username and username not in profile_emails
            ):
                # a username/email is not required when logging in with an auth token
                # however if both are provided raise an error if they do not correspond
                # to avoid any ambiguity about what profile was logged in
                raise SynapseAuthenticationError(
                    "username/email and auth_token both provided but username does not "
                    "match token profile"
                )
            credentials.username = profile_username
            credentials.displayname = profile_displayname

            return credentials

        return None


class UserArgsCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from user_login_args during a CLI session.
    """

    def _get_auth_info(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Tuple[str, str]:
        return (
            user_login_args.username,
            user_login_args.auth_token,
        )


class ConfigFileCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from the `~/.synapseConfig` file
    """

    def _get_auth_info(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Tuple[Union[str, None], Union[str, None]]:
        config_dict = get_config_authentication(config_path=syn.configPath)
        # check to make sure we didn't accidentally provide the wrong user

        username = config_dict.get("username")
        token = config_dict.get("authtoken")

        if user_login_args.username and username != user_login_args.username:
            # if the username is provided and there is a config file username but they
            # don't match then we don't use any of the values from the config
            # to prevent ambiguity
            username = None
            token = None
            syn.logger.warning(
                f"{user_login_args.username} was defined in the user login "
                "arguments, however, it is also defined in the `~/.synapseConfig` "
                "file. Becuase they do not match we will not use the `authtoken` "
                "in the `~/.synapseConfig` file.",
            )

        return username, token


class AWSParameterStoreCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves user's authentication token from AWS SSM Parameter store
    """

    ENVIRONMENT_VAR_NAME = "SYNAPSE_TOKEN_AWS_SSM_PARAMETER_NAME"

    def _get_auth_info(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Tuple[Union[str, None], Union[str, None]]:
        ssm_param_name = os.environ.get(self.ENVIRONMENT_VAR_NAME)
        token = None
        if ssm_param_name:
            try:
                import boto3
                import botocore

                ssm_client = boto3.client("ssm")
                result = ssm_client.get_parameter(
                    Name=ssm_param_name,
                    WithDecryption=True,
                )
                token = result["Parameter"]["Value"]
            except ImportError:
                syn.logger.warning(
                    f"{self.ENVIRONMENT_VAR_NAME} was defined as {ssm_param_name}, "
                    'but "boto3" could not be imported. The Synapse client uses "boto3" '
                    "in order to access Systems Manager Parameter Storage. Please ensure "
                    'that you have installed "boto3" to enable this feature.'
                )
            # this except block must be defined after the ImportError except block
            # otherwise, there's no guarantee "botocore" is already imported and defined
            except botocore.exceptions.ClientError:
                syn.logger.warning(
                    f"{self.ENVIRONMENT_VAR_NAME} was defined as {ssm_param_name}, "
                    "but the matching parameter name could not be found in AWS Parameter "
                    "Store. Caused by AWS error:\n",
                    exc_info=True,
                )

        # if username is included in user's arguments, return it so that
        # it may be validated against the username authenticated by the token
        return user_login_args.username, token


class EnvironmentVariableCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves the user's authentication token from an environment variable
    """

    ENVIRONMENT_VAR_NAME = "SYNAPSE_AUTH_TOKEN"

    def _get_auth_info(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Tuple[Union[str, None], Union[str, None]]:
        return (
            user_login_args.username,
            os.environ.get(self.ENVIRONMENT_VAR_NAME),
        )


class SynapseCredentialsProviderChain(object):
    """
    Class that has a list of
    [SynapseCredentialsProvider][synapseclient.core.credentials.credential_provider.SynapseCredentialsProvider]
    from which this class attempts to retrieve
    [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials].


    By default this class uses the following providers in this order:

    1. [UserArgsCredentialsProvider][synapseclient.core.credentials.credential_provider.UserArgsCredentialsProvider]
    2. [ConfigFileCredentialsProvider][synapseclient.core.credentials.credential_provider.ConfigFileCredentialsProvider]
    3. [EnvironmentVariableCredentialsProvider][synapseclient.core.credentials.credential_provider.EnvironmentVariableCredentialsProvider]
    4. [AWSParameterStoreCredentialsProvider][synapseclient.core.credentials.credential_provider.AWSParameterStoreCredentialsProvider]

    Attributes:
        cred_providers: list of
            ([SynapseCredentialsProvider][synapseclient.core.credentials.credential_provider.SynapseCredentialsProvider])
            credential providers
    """

    def __init__(self, cred_providers) -> None:
        self.cred_providers = list(cred_providers)

    def get_credentials(
        self, syn: "Synapse", user_login_args: Dict[str, str]
    ) -> Union[SynapseCredentials, None]:
        """
        Iterates its list of
        [SynapseCredentialsProvider][synapseclient.core.credentials.credential_provider.SynapseCredentialsProvider]
        and returns the first non-None
        [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials]
        returned by a provider. If no provider is able to provide a
        [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials],
        returns None.

        Arguments:
            syn: Synapse client instance
            user_login_args: subset of arguments passed during syn.login()

        Returns:
            [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials]
                returned by the first non-None provider in its list, None otherwise
        """
        for provider in self.cred_providers:
            creds = provider.get_synapse_credentials(syn, user_login_args)
            if creds is not None:
                return creds
        return None


# NOTE: If you change the order of this list, please also change the documentation
# in Synapse.login() that describes the order

DEFAULT_CREDENTIAL_PROVIDER_CHAIN = SynapseCredentialsProviderChain(
    cred_providers=[
        UserArgsCredentialsProvider(),
        ConfigFileCredentialsProvider(),
        EnvironmentVariableCredentialsProvider(),
        AWSParameterStoreCredentialsProvider(),  # see service catalog issue: SC-260
    ]
)


def get_default_credential_chain() -> SynapseCredentialsProviderChain:
    """
    Creates and uses a default credential chain to retrieve
    [SynapseCredentials][synapseclient.core.credentials.cred_data.SynapseCredentials].
    The order this is returned is the order in which the credential providers
    are attempted.

    Returns:
        credential chain
    """
    return DEFAULT_CREDENTIAL_PROVIDER_CHAIN
