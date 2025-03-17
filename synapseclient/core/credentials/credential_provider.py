"""
This module contains classes that are responsible for retrieving synapse authentication
information (e.g. authToken) from a source (e.g. login args, config file).
"""

import abc
import configparser
import os
from typing import TYPE_CHECKING, Dict, Tuple, Union

from opentelemetry import trace

from synapseclient.api import get_config_authentication
from synapseclient.core.credentials.cred_data import (
    SynapseAuthTokenCredentials,
    SynapseCredentials, UserLoginArgs,
)
from synapseclient.core.exceptions import SynapseAuthenticationError

from synapseclient.core.credentials.cred_data import get_config_authentication

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

        username, auth_token = self._get_auth_info(syn=syn, user_login_args=user_login_args)

        return self._create_synapse_credential(syn, username, auth_token)


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
                username != profile_username and username not in profile_emails):

            # a username/email is not required when logging in with an auth token
                # however if both are provided raise an error if they do not correspond
                # to avoid any ambiguity about what profile was logged in
                raise SynapseAuthenticationError(
                    "username/email and auth_token both provided but username does not "
                    "match token profile"
                )
            credentials.username = profile_username
            credentials.displayname = profile_displayname
            credentials.owner_id = profile.get("ownerId", None)

            current_span = trace.get_current_span()
            if current_span.is_recording():
                current_span.set_attribute("user.id", credentials.owner_id)

            return credentials

        return None


class UserArgsCredentialsProvider(SynapseCredentialsProvider):
    def _get_auth_info(self, syn, user_login_args):

        username = user_login_args.get("username")
        token = user_login_args.get("auth_token")

        if username and token:
            return username, token

        return None, None
    """
    Retrieves auth info from user_login_args during a CLI session.
    """

class ConfigFileCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from the `~/.synapseConfig` file
    """

    def _get_auth_info(self, syn, user_login_args):
        selected_profile = user_login_args.get("profile", "default")

        config_profiles = get_config_authentication(config_path=syn.configPath)

        # Retrieve credentials for the selected profile
        if selected_profile in config_profiles:
            username = config_profiles[selected_profile].get("username")
            token = config_profiles[selected_profile].get("auth_token")
            return username, token

        raise SynapseAuthenticationError(f"Profile '{selected_profile}' not found in {syn.configPath}")

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

    def get_credentials(self, syn: "Synapse", user_login_args: "UserLoginArgs") -> Union[SynapseCredentials, None]:
        selected_profile = user_login_args.get("profile") or os.getenv("SYNAPSE_PROFILE", "default")

        for provider in self.cred_providers:

            creds = provider.get_synapse_credentials(
                syn,
                UserLoginArgs(
                    username=user_login_args.username,
                    auth_token=user_login_args.auth_token,
                    profile=selected_profile,
                ),
            )

            if creds is not None:
                return creds
        return None



# NOTE: If you change the order of this list, please also change the documentation
# in Synapse.login() that describes the order

# TODO: change of order of list if needed

DEFAULT_CREDENTIAL_PROVIDER_CHAIN = SynapseCredentialsProviderChain(
    cred_providers=[
        UserArgsCredentialsProvider(),
        ConfigFileCredentialsProvider(),
        EnvironmentVariableCredentialsProvider(),
        AWSParameterStoreCredentialsProvider(),
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
