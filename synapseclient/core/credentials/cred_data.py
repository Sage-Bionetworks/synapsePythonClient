import abc
import base64
import collections
import hashlib
import hmac
import json
import keyring
import requests.auth
import time
import urllib.parse as urllib_parse

from synapseclient.core.exceptions import SynapseAuthenticationError
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
        return cls(secret, username) if secret else None

    def delete_from_keyring(self):
        try:
            keyring.delete_password(self.get_keyring_service_name(), self.username)
        except keyring.errors.PasswordDeleteError:
            # key does not exist, but that is fine
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

    def __init__(self, api_key_string, username):
        self._api_key = base64.b64decode(api_key_string)
        self._username = username

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

    @classmethod
    def _validate_token(cls, token):
        # decode the token to ensure it minimally has view scope.
        # if it doesn't raise an error, the client will not be useful without it.

        # if for any reason we are not able to decode the token and check its scopes
        # we do NOT raise an error. this is to accommodate the possibility of a changed
        # token format some day that this version of the client may still be able to
        # pass as a bearer token.
        try:
            token_body = json.loads(
                str(
                    base64.urlsafe_b64decode(
                        # we add padding to ensure that lack of padding won't prevent a decode error.
                        # the python base64 implementation will truncate extra padding so we can overpad
                        # rather than compute exactly how much padding we might need.
                        # https://stackoverflow.com/a/49459036
                        token.split('.')[1] + '==='
                    ),
                    'utf-8'
                )
            )
            scopes = token_body.get('access', {}).get('scope')
            if scopes is not None and 'view' not in scopes:
                raise SynapseAuthenticationError('A view scoped token is required')

        except (IndexError, ValueError):
            # possible errors if token is not encoded as expected:
            # IndexError if the token is not a '.' delimited base64 string with a header and body
            # ValueError if the split string is not base64 encoded or if the decoded base64 is not json
            pass

    def __init__(self, token, username=None):
        self._validate_token(token)

        self._token = token
        self.username = username

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, username):
        self._username = username

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


def delete_stored_credentials(username):
    """
    Delete all credentials stored to the keyring.
    """
    for credential_cls in (SynapseApiKeyCredentials, SynapseAuthTokenCredentials):
        creds = credential_cls.get_from_keyring(username)
        if creds:
            creds.delete_from_keyring()
