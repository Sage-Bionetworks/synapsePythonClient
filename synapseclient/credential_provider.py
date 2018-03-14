from abc import ABCMeta, abstractmethod

try:
    from urllib.parse import urlparse
    from urllib.parse import urlunparse
    from urllib.parse import quote
    from urllib.parse import unquote
    from urllib.request import urlretrieve
except ImportError:
    from urlparse import urlparse
    from urlparse import urlunparse
    from urllib import quote
    from urllib import unquote
    from urllib import urlretrieve

import time
import base64, hmac, hashlib
from six import with_metaclass

import utils

#TODO: either need to pass in a synapse client or reafactor out the code that gets the API key from URI's
#TODO: Either way, need to be able to swap endpoints easily.



class SynapseCredentials(object): #TODO: if no additional functionality needed, perhaps use namedtuple?
    """
    Credentials used to make requests to Synapse.
    """
    def __init__(self, username, api_key):
        self.username = username
        self.api_key = api_key

    def get_signed_headers(self, url):
        """
        Generates signed HTTP headers for accessing Synapse urls
        :param url:
        :return:
        """
        sig_timestamp = time.strftime(utils.ISO_FORMAT, time.gmtime())
        url = urlparse(url).path
        sig_data = self.username + url + sig_timestamp
        signature = base64.b64encode(hmac.new(self.api_key,
                                              sig_data.encode('utf-8'),
                                              hashlib.sha1).digest())

        sig_header = {'userId': self.username,
                      'signatureTimestamp': sig_timestamp,
                      'signature': signature}
        return sig_header

class SynapseAuthInformationProvider(with_metaclass(ABCMeta)):
    def get_credentials(self):
        self.get_auth_info()

    @abstractmethod
    def get_auth_info(self):
        pass #TODO return info about what is the user password apikey or sesion_token

class UserArgAuthInformationProvider(SynapseAuthInformationProvider):
    def __init__(self, username, password, api_key, session_token):
        if username is None and (password is not None or api_key is not None):
                raise ValueError('Username must also be specified with a password or API key')
        self.username = username
        self.password = password
        self.api_key = api_key
        self.session_token = session_token
    def get_credentials(self):
        return {} #TODO

class CachedUsernameAuthInformationProvider(SynapseAuthInformationProvider):
    pass


class ConfigFileAuthInformationProvider(SynapseAuthInformationProvider):
    def get_credentials(self):
        pass


class SynapseAuthInformationProviderChain(object):
    def __init__(self, cred_providers):
        """

        :param list cred_providers: a list of ``SynapseCredentialProvider``
        """
        self._cred_providers = list(cred_providers)

    def insert_cred_provider(self, index, cred_provider):
        self._cred_providers.insert(index, cred_provider)

    def get_credentials(self):
        for provider in self._cred_providers:
            credentials = provider.get_credentials()
            if credentials:
                return credentials
        return None


def get_default_auth_information_chain(username, password, api_key, session_token, skip_cache = False):



    credential_providers = [UserArgAuthInformationProvider(username, password, api_key, session_token),
                            ]



