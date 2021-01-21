import abc
import deprecated.sphinx

from .cred_data import SynapseApiKeyCredentials, SynapseBearerTokenCredentials
from . import cached_sessions


class SynapseCredentialsProvider(metaclass=abc.ABCMeta):
    """
    A credential provider is responsible for retrieving synapse authentication information (e.g. username/password or
    username/api key) from a source(e.g. login args, config file, cached credentials in keyring), and use them to return
    a ``SynapseCredentials` instance.
    """
    @abc.abstractmethod
    def _get_auth_info(self, syn, user_login_args):
        """
        Subclasses must implement this to decide how to obtain username, password, and api_key.
        For any of these 3 values, return None if it is not possible to get that value.

        Not all implementations will need to make use of the user_login_args parameter or syn.
        These parameters provide context about the Synapse client's configuration and login() arguments.

        :param ``synapseclient.client.Synapse`` syn:        Synapse client instance
        :param ``cred_data.UserLoginArgs`` user_login_args: subset of arguments passed during syn.login()
        :return: tuple of (username, password, api_key, token), any of these values could None if it is not available.
        """
        return None, None, None, None

    def get_synapse_credentials(self, syn, user_login_args):
        """
        Returns `SynapseCredentials` if this provider is able to get valid credentials, returns None otherwise.
        :param ``synapseclient.client.Synapse`` syn:        Synapse client instance
        :param ``cred_data.UserLoginArgs`` user_login_args: subset of arguments passed during syn.login()
        :return: `SynapseCredentials` if valid credentials can be found by this provider, None otherwise
        """
        return self._create_synapse_credential(syn, *self._get_auth_info(syn, user_login_args))

    def _create_synapse_credential(self, syn, username, password, api_key, bearer_token):
        if username is not None:
            if password is not None:
                retrieved_session_token = syn._getSessionToken(email=username, password=password)
                return SynapseApiKeyCredentials(username, syn._getAPIKey(retrieved_session_token))
            elif api_key is not None:
                return SynapseApiKeyCredentials(username, api_key)
            elif bearer_token is not None:
                return SynapseBearerTokenCredentials(username, bearer_token)
        return None


class UserArgsCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from user_login_args
    """
    def _get_auth_info(self, syn, user_login_args):
        return (
            user_login_args.username,
            user_login_args.password,
            user_login_args.api_key,
            user_login_args.bearer_token,
        )


@deprecated.sphinx.deprecated(version='1.9.0', action='ignore',
                              reason="This will be removed in 2.0. Please use username and password or apiKey instead.")
class UserArgsSessionTokenCredentialsProvider(SynapseCredentialsProvider):
    """
    This is a special case where we are not given context as to what the username is. We are only given a session token
    and must retrieve the username and api key from Synapse
    """

    def _get_auth_info(self, syn, user_login_args):
        username = None
        password = None
        api_key = None
        bearer_token = None

        if user_login_args.session_token:
            username = syn.getUserProfile(sessionToken=user_login_args.session_token)['userName']
            api_key = syn._getAPIKey(user_login_args.session_token)

        return username, password, api_key, bearer_token


class ConfigFileCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from .synapseConfig file
    """
    def _get_auth_info(self, syn, user_login_args):
        config_dict = syn._get_config_authentication()
        # check to make sure we didn't accidentally provide the wrong user

        config_username = config_dict.get('username')

        username = None
        password = None
        api_key = None
        bearer_token = None

        if user_login_args.username is None or config_username == user_login_args.username:
            username = config_username
            password = config_dict.get('password')
            api_key = config_dict.get('apikey')
            bearer_token = config_dict.get('token')

        return username, password, api_key, bearer_token


class CachedCredentialsProvider(SynapseCredentialsProvider):
    """
    Retrieves auth info from cached_sessions
    """
    def _get_auth_info(self, syn, user_login_args):
        username = None
        password = None
        api_key = None
        bearer_token = None

        if not user_login_args.skip_cache:
            username = user_login_args.username or cached_sessions.get_most_recent_user()

            api_creds = SynapseApiKeyCredentials.get_from_keyring(username)
            bearer_token_creds = SynapseBearerTokenCredentials.get_from_keyring(username)

            api_key = api_creds.secret if api_creds else None
            bearer_token = bearer_token_creds.secret if bearer_token_creds else None

        return username, password, api_key, bearer_token


class SynapseCredentialsProviderChain(object):
    """
    Class that has a list of ``SynapseCredentialsProvider`` from which this class attempts to retrieve
    ``SynapseCredentials``.
    """
    def __init__(self, cred_providers):
        """
        :param list[``SynapseCredentialsProvider``] cred_providers: list of credential providers
        """
        self.cred_providers = list(cred_providers)

    def get_credentials(self, syn, user_login_args):
        """
        Iterates its list of ``SynapseCredentialsProvider`` and returns the first non-None ``SynapseCredential``
        returned by a provider. If no provider is able to provide a ``SynapseCredential``, returns None.
        :param ``synapseclient.client.Synapse`` syn:        Synapse client instance
        :param ``cred_data.UserLoginArgs`` user_login_args: subset of arguments passed during syn.login()
        :return: `SynapseCredentials` returned by the first non-None provider in its list, None otherwise
        """
        for provider in self.cred_providers:
            creds = provider.get_synapse_credentials(syn, user_login_args)
            if creds is not None:
                return creds
        return None


# NOTE: If you change the order of this list, please also change the documentation in Synapse.login() that describes the
# order

DEFAULT_CREDENTIAL_PROVIDER_CHAIN = SynapseCredentialsProviderChain([
    UserArgsSessionTokenCredentialsProvider(),  # This provider is DEPRECATED
    UserArgsCredentialsProvider(),
    ConfigFileCredentialsProvider(),
    CachedCredentialsProvider()])


def get_default_credential_chain():
    """
    :return: credential chain
    :rtype: ```SynapseCredentialsProviderChain``
    """
    return DEFAULT_CREDENTIAL_PROVIDER_CHAIN
