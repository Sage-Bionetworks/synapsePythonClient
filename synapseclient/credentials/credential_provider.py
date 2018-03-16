from abc import ABCMeta, abstractmethod
from six import with_metaclass
from cred_data import SynapseCredentials
import cached_sessions


class SynapseCredentialsProvider(with_metaclass(ABCMeta)):
    @abstractmethod
    def get_username_and_api_key(self, syn, user_login_args):
        """
        Sublcasses implement this to decide how to obtain username and password.
        Not all implementations will need to use the user_login_args parameter or syn.
        :param ``synapseclient.client.Synapse`` syn: synapse client
        :param ``cred_data.UserLoginArgs`` user_login_args: subset of arguments passed during syn.login()
        :return: (username, api_key)
        """
        pass


class UserArgsUsernamePasswordCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        return syn._get_login_credentials(username=user_login_args.username, password=user_login_args.password)


class UserArgsUsernameAPICredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        return user_login_args.username, user_login_args.api_key


class UserArgsSessionTokenCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        return syn._get_login_credentials(sessiontoken=user_login_args.session_token)


class ConfigUsernamePasswordCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        config_dict = syn._get_config_section_dict('authentication')
        config_username = config_dict.get('username')
        # check to make sure we didn't accidentally provide the wrong user
        if user_login_args.username is None or user_login_args.username == config_username:
            return syn._get_login_credentials(username=config_username, password=config_dict.get('password'))
        return None, None


class ConfigUsernameAPICredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        config_dict = syn._get_config_section_dict('authentication')
        config_username = config_dict.get('username')
        # check to make sure we didn't accidentally provide the wrong user
        if user_login_args.username is None or user_login_args.username == config_username:
            return config_username, config_dict.get('apikey')
        return None, None


class ConfigSessionTokenCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        config_dict = syn._get_config_section_dict('authentication')
        # check to make sure we didn't accidentally provide the wrong user
        username, api_key =  syn._get_login_credentials(sessiontoken=config_dict.get('sessiontoken'))
        if user_login_args.username is None or user_login_args.username == username:
            return username, api_key
        return None, None


class CachedUserNameCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        if not user_login_args.skip_cache:
            cached_api_key = cached_sessions.get_api_key(user_login_args.username)
            if cached_api_key is not None:
                return user_login_args.username, cached_api_key
        return None, None


class CachedRecentlyUsedUsernameCredentialsProvider(SynapseCredentialsProvider):
    """
    Only checks cache for username and
    """
    def get_username_and_api_key(self, syn, user_login_args):
        if not user_login_args.skip_cache and user_login_args.username is None: #don't use the cached recent used username, if a username was specified
            username = cached_sessions.get_most_recent_user()
            return username, cached_sessions.get_api_key(username)
        return None, None


class SynapseCredentialsProviderChain(object):
    def __init__(self, cred_providers):
        """
        Chain of ``SynapseCredentialsProvider`` that decides which source of Synapse credentials to use
        :param ``cred_data.UserLoginArgs`` user_login_args:
        :param list[``SynapseCredentialsProvider``] cred_providers:
        """
        self.cred_providers = list(cred_providers)

    def get_credentials(self, syn, user_login_args): #maybe use __call__ instead
        for provider in self.cred_providers:
            result_username, result_api_key = provider.get_username_and_api_key(syn, user_login_args)
            if result_username is not None and result_api_key is not None:
                return SynapseCredentials(result_username, result_api_key)
        return None


DEFAULT_CREDENTIAL_PROVIDERS = [UserArgsUsernamePasswordCredentialsProvider(),
                                UserArgsSessionTokenCredentialsProvider(),
                                UserArgsUsernameAPICredentialsProvider(),
                                ConfigUsernamePasswordCredentialsProvider(),
                                ConfigSessionTokenCredentialsProvider(),
                                ConfigUsernameAPICredentialsProvider(),
                                CachedUserNameCredentialsProvider(),
                                CachedRecentlyUsedUsernameCredentialsProvider()]

def get_default_credential_chain():
    return SynapseCredentialsProviderChain(DEFAULT_CREDENTIAL_PROVIDERS)



