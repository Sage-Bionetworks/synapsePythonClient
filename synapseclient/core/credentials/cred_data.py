import abc
import base64
import collections
import hashlib
import hmac
import keyring
import requests.auth
import time
import urllib.parse as urllib_parse

import synapseclient.core.utils


class SynapseCredentials(requests.auth.AuthBase, abc.ABC):

    @property
    @abc.abstractmethod
    def username(self):
        pass

    @property
    @abc.abstractmethod
    def secret(self):
        pass

    @classmethod
    @abc.abstractmethod
    def get_keyring_service_name(cls):
        pass

    @classmethod
    def get_from_keyring(cls, username: str) -> 'SynapseCredentials':
        secret = keyring.get_password(cls.get_keyring_service_name(), username)
        return cls(username, secret) if secret else None

    def delete_from_keyring(self):
        try:
            keyring.delete_password(self.get_keyring_service_name(), self.username)
        except keyring.errors.PasswordDeleteError:
            # The api key does not exist, but that is fine
            pass

    def store_to_keyring(self):
        keyring.set_password(self.get_keyring_service_name(), self.username, self.secret)


class SynapseApiKeyCredentials(SynapseCredentials):
    """
    Credentials used to make requests to Synapse.
    """

    @classmethod
    def get_keyring_service_name(cls):
        # cannot change without losing access to existing client's stored api keys
        return "SYNAPSE.ORG_CLIENT"

    def __init__(self, username, api_key_string):
        self._username = username
        self._api_key = base64.b64decode(api_key_string)

    @property
    def username(self):
        return self._username

    @property
    def secret(self):
        return base64.b64encode(self._api_key).decode()

    @property
    def api_key(self):
        # this is provided for backwards compatibility if any code is using it
        return self.secret

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

    def __call__(self, r):
        signed_headers = self.get_signed_headers(r.url)
        r.headers.update(signed_headers)
        return r

    def __repr__(self):
        return f"SynapseApiKeyCredentials(username='{self.username}', api_key_string='{self.secret}')"


class SynapseAuthTokenCredentials(SynapseCredentials):

    @classmethod
    def get_keyring_service_name(cls):
        return 'SYNAPSE.ORG_CLIENT_AUTH_TOKEN'

    def __init__(self, username, token):
        self._username = username
        self._token = token

    @property
    def username(self):
        return self._username

    @property
    def secret(self):
        return self._token

    def __call__(self, r):
        r.headers.update({'Authorization': f"Bearer {self.secret}"})
        return r

    def __repr__(self):
        return f"SynapseAuthTokenCredentials(username='{self.username}', token='{self.secret}')"


# a class that just contains args passed form synapse client login
# TODO remove deprecated sessionToken
UserLoginArgs = collections.namedtuple(
    'UserLoginArgs',
    [
        'username',
        'password',
        'api_key',
        'skip_cache',
        'session_token',
        'auth_token',
    ]
)

# make the namedtuple's arguments optional instead of positional. All values default to None
# when we require Python 3.6.1 we can use typing.NamedTuple's built-in default support
UserLoginArgs.__new__.__defaults__ = (None,) * len(UserLoginArgs._fields)
