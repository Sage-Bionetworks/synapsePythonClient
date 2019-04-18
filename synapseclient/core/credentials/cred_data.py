import time
import base64
import hmac
import hashlib
import collections
import urllib.parse as urllib_parse

import synapseclient.core.utils


# TODO: inherit requests.AuthBase so that this object can be simply passed to requests library
class SynapseCredentials(object):
    """
    Credentials used to make requests to Synapse.
    """

    # setting and getting api_key it will use the base64 encoded string representation
    @property
    def api_key(self):
        return base64.b64encode(self._api_key).decode()

    @api_key.setter
    def api_key(self, value):
        self._api_key = base64.b64decode(value)

    def __init__(self, username, api_key_string):
        self.username = username
        self.api_key = api_key_string

    def get_signed_headers(self, url):
        """
        Generates signed HTTP headers for accessing Synapse urls
        :param url:
        :return:
        """
        sig_timestamp = time.strftime(synapseclient.core.utils.ISO_FORMAT, time.gmtime())
        url = urllib_parse.urlparse(url).path
        sig_data = self.username + url + sig_timestamp
        signature = base64.b64encode(hmac.new(self._api_key,
                                              sig_data.encode('utf-8'),
                                              hashlib.sha1).digest())

        return {'userId': self.username,
                'signatureTimestamp': sig_timestamp,
                'signature': signature}

    def __repr__(self):
        return "SynapseCredentials(username='%s', api_key_string='%s')" % (self.username, self.api_key)


# a class that just contains args passed form synapse client login
# TODO remove deprecated sessionToken
UserLoginArgs = collections.namedtuple('UserLoginArgs',
                                       ['username', 'password', 'api_key', 'skip_cache', 'session_token'])
# make the namedtuple's arguments optional instead of positional. All values default to None
UserLoginArgs.__new__.__defaults__ = (None,) * len(UserLoginArgs._fields)
