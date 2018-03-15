from collections import namedtuple

import synapseclient.utils
import time
import base64, hmac, hashlib

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class SynapseCredentials(object): #TODO: inherit requests.AuthBase so that this object can be simply passed to requests library
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
        sig_timestamp = time.strftime(synapseclient.utils.ISO_FORMAT, time.gmtime())
        url = urlparse(url).path
        sig_data = self.username + url + sig_timestamp
        signature = base64.b64encode(hmac.new(self.api_key,
                                              sig_data.encode('utf-8'),
                                              hashlib.sha1).digest())

        sig_header = {'userId': self.username,
                      'signatureTimestamp': sig_timestamp,
                      'signature': signature}
        return sig_header

#a class that just contains args passed form synapse client login
UserLoginArgs = namedtuple('UserLoginArgs', ['username','password','api_key','session_token','use_cache'])