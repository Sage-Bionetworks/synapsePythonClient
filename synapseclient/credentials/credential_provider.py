from abc import ABCMeta, abstractmethod




from six import with_metaclass
from .cred_data import SynapseCredentials
import cached_sessions
#TODO: either need to pass in a synapse client or reafactor out the code that gets the API key from URI's
#TODO: Either way, need to be able to swap endpoints easily.







class SynapseCredentialsProvider(with_metaclass(ABCMeta)):

    @abstractmethod
    def _get_username_and_api_key(self, user_login_args):
        """
        Sublcasses implement this to decide how to obtain username and password.
        Not all implementations will need to use the user_login_args parameter.
        :param user_login_args:
        :return:
        """
        pass

    def get_synapse_credentials(self, user_login_args):
        """

        :param ``UserLoginArgs`` user_login_args:
        :return:
        """

        result_username, result_api_key = self._get_username_and_api_key(user_login_args)

        if result_username is not None and result_api_key is not None:
            return SynapseCredentials(result_username, result_api_key)
        return None



#TODO: these classes are essentially callback functions at this point
class UserArgsUsernamePasswordCredentialsProvider(SynapseCredentialsProvider):
    def _get_username_and_api_key(self, user_login_args):
        return get_api_from_username_password(user_login_args.username, user_login_args.pasword)


class UserArgsUsernameAPICredentialsProvider(SynapseCredentialsProvider):
    def _get_username_and_api_key(self, user_login_args):
        return get_api_from_username_api(user_login_args.username, user_login_args.api_key)

class UserArgsSessionTokenCredentialsProvider(SynapseCredentialsProvider):
    def _get_username_and_api_key(self, user_login_args):
        return get_api_from_session_token(user_login_args.session_token)

class ConfigUsernamePasswordCredentialsProvider(SynapseCredentialsProvider):
    def __init__(self):
        pass
    def _get_username_and_api_key(self, user_login_args):
        return

class ConfigUsernameAPICredentialsProvider(SynapseCredentialsProvider):
    def __init__(self):
        pass
    def _get_username_and_api_key(self, user_login_args):
        return

class ConfigSessionTokenCredentialsProvider(SynapseCredentialsProvider):
    def __init__(self):
        pass
    def _get_username_and_api_key(self, user_login_args):
        return

class CachedUserNameCredentialsProvider(SynapseCredentialsProvider):
    def _get_username_and_api_key(self, user_login_args):
        assert user_login_args.username is not None and user_login_args.password is None and user_login_args.api_key is None and user_login_args.session_token is None #if we got here no argument should have been set
        if(user_login_args.use_cache

class CachedRecentlyUsedUsernameCredentialsProvider(SynapseCredentialsProvider):
    """
    Only checks cache for username and
    """
    def _get_username_and_api_key(self, user_login_args):
        assert user_login_args.username is None and user_login_args.password is None and user_login_args.api_key is None and user_login_args.session_token is None #if we got here no argument should have been set
        if(user_login_args.use_cache):
            username = cached_sessions.get_most_recent_user()
            return username, cached_sessions.get_API_key(username)

        return None, None

class SynapseCredentialsProviderChain(object):
    def __init__(self, user_login_args, cred_providers):
        """
        :param ``cred_data.UserLoginArgs`` user_login_args:
        :param list[``SynapseCredentialsProvider``] cred_providers:
        """
        self.cred_providers = list(cred_providers)

    def insert_cred_provider(self, index, cred_provider): #TODO: remove if not used
        self.cred_providers.insert(index, cred_provider)

    def get_credentials(self, syn, user_login_args): #maybe use __call__ instead
        for provider in self.cred_providers:
            credentials = provider.get_synapse_credentials(user_login_args)
            if credentials and user_login_args.username == credentials.username: # just in case a incorrect credential was used to log in
                return credentials
        return None


def get_default_credential_chain():

    credential_providers = [UserArgsUsernamePasswordCredentialsProvider,
                            UserArgsSessionTokenCredentialsProvider,
                            UserArgsUsernameAPICredentialsProvider,
                            ConfigUsernamePasswordCredentialsProvider,
                            ConfigSessionTokenCredentialsProvider,
                            ConfigUsernameAPICredentialsProvider,
                            CachedRecentlyUsedUsernameCredentialsProvider]

    return SynapseCredentialsProviderChain(credential_providers)



