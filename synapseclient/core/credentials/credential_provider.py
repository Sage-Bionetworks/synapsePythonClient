"""
This module contains classes that are responsible for retrieving synapse authentication
information from various sources such as:

- User-provided login arguments
- Synapse configuration file (`~/.synapseConfig`)
- Environment variables
- AWS Parameter Store

The retrieved authentication credentials are used for secure login.
"""

import abc
import os
from typing import TYPE_CHECKING, Dict, Tuple, Union, Optional

from opentelemetry import trace

from synapseclient.api import get_config_authentication
from synapseclient.core.credentials.cred_data import (
    SynapseAuthTokenCredentials,
    SynapseCredentials, UserLoginArgs
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
    ) -> Tuple[Optional[str], Optional[str]]:
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
        self, syn: "Synapse", user_login_args: UserLoginArgs
    ) -> Union[SynapseCredentials, None]:

        if not user_login_args.auth_token and not user_login_args.username:
            auth_profiles = get_config_authentication(syn.configPath, user_login_args.profile)
            user_login_args.username = auth_profiles["username"]
            user_login_args.auth_token = auth_profiles["auth_token"]

        return self._create_synapse_credential(syn, user_login_args.username, user_login_args.auth_token)

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
    """Retrieves authentication information from user_login_args during a CLI session."""

    def _get_auth_info(self, syn: "Synapse", user_login_args: UserLoginArgs) -> Tuple[Optional[str], Optional[str]]:
        """
        Retrieves authentication information from user_login_args during a CLI session.

        Args:
            syn (Synapse): Synapse client instance.
            user_login_args (UserLoginArgs): Object containing login credentials.

        Returns:
            Tuple[Optional[str], Optional[str]]: A tuple containing username and token,
            where either or both values may be None.
        """

        token: Optional[str] = user_login_args.auth_token
        username: Optional[str] = user_login_args.username if user_login_args.username else None

        return username, token

class ConfigFileCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from the `~/.synapseConfig` file
    """

    def _get_auth_info(self, syn: "Synapse", user_login_args) -> Tuple[Optional[str], Optional[str]]:
        """
        Retrieves authentication credentials from the `~/.synapseConfig` file.

        This provider loads authentication details from the Synapse configuration file.
        It supports multi-profile authentication, allowing users to switch between
        different profiles dynamically.

        - If an auth token is explicitly provided, it is returned immediately.
        - If no auth token is provided, it attempts to retrieve credentials
          based on the specified profile.
        - If the profile does not exist, an authentication error is raised.

        Raises:
            SynapseAuthenticationError: If no authentication method is provided or
            if the specified profile does not exist in the configuration file.
        """
        # If authToken is explicitly provided, return it directly and skip profiles
        if user_login_args.auth_token:
            return user_login_args.username, user_login_args.auth_token

        # Otherwise, fall back to profile-based lookup
        if not user_login_args.profile:
            raise SynapseAuthenticationError("No authentication method provided (neither authToken nor profile).")

        # Fetch available profiles from the config file
        auth_profiles = get_config_authentication(syn.configPath)

        # If the profile exists, return its credentials
        if user_login_args.profile in auth_profiles:
            profile_data = auth_profiles[user_login_args.profile]
            return profile_data["username"], profile_data["auth_token"]

        # Otherwise, raise an error
        raise SynapseAuthenticationError(f"Profile '{user_login_args.profile}' not found in {syn.configPath}")


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
        selected_profile = user_login_args.profile or os.getenv("SYNAPSE_PROFILE")

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
