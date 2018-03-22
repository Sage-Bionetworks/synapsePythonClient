from abc import ABCMeta, abstractmethod
from six import with_metaclass
from .cred_data import SynapseCredentials
from . import cached_sessions


class SynapseCredentialsProvider(with_metaclass(ABCMeta)):
    """
    A credential provider is responsible for retrieving synapse authentication information (e.g. username/password or username/api key)
    from a source and return th
    the
    """
    @abstractmethod
    def get_username_and_api_key(self, syn, user_login_args):
        """
        Sublcasses implement this to decide how to obtain username and password.
        Not all implementations will need to use the user_login_args parameter or syn.
        :param ``synapseclient.client.Synapse`` syn: synapse client
        :param ``cred_data.UserLoginArgs`` user_login_args: subset of arguments passed during syn.login()
        :return: (username, api_key) or (None,None) if unable to provide credentials
        """
        username = self.get_username(user_login_args)
        if user_login_args.username is None or user_login_args.username == username:
            self.get_api_key(username)


class UserArgsCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        return user_login_args.username, user_login_args.password, user_login_args.api_key



class ConfigUsernamePasswordCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        config_dict = syn._get_config_authenticaton()
        # check to make sure we didn't accidentally provide the wrong user

        return config_dict.get('username'), config_dict.get('password'), config_dict.get('apikey')



class CachedUserNameCredentialsProvider(SynapseCredentialsProvider):
    def get_username_and_api_key(self, syn, user_login_args):
        if not user_login_args.skip_cache:
            username = user_login_args.username or cached_sessions.get_most_recent_user()
            return username, None, cached_sessions.get_api_key(username)
        return None, None


class CachedRecentlyUsedUsernameCredentialsProvider(SynapseCredentialsProvider):
    """
    Only checks cache for username and
    """
    def get_username_and_api_key(self, syn, user_login_args):
        if not user_login_args.skip_cache:
            username = cached_sessions.get_most_recent_user()
            return username, cached_sessions.get_api_key(username)
        return None, None


class SynapseCredentialsProviderChain(object):
    def __init__(self, cred_providers):
        """
        Uses a list of ``SynapseCredentialsProvider`` that decides which source of Synapse credentials to use
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


#NOTE: If you change the order of this list, please also change the documentation in Synapse.login() that describes the order
DEFAULT_CREDENTIAL_PROVIDERS = [UserArgsUsernamePasswordCredentialsProvider(),
                                UserArgsUsernameAPICredentialsProvider(),
                                ConfigUsernamePasswordCredentialsProvider(),
                                ConfigUsernameAPICredentialsProvider(),
                                CachedUserNameCredentialsProvider(),
                                CachedRecentlyUsedUsernameCredentialsProvider()]

def get_default_credential_chain():
    return SynapseCredentialsProviderChain(DEFAULT_CREDENTIAL_PROVIDERS)



