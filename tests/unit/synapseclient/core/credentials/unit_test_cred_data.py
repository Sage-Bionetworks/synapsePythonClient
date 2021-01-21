import base64
import requests
import time

from unittest.mock import MagicMock, patch

from synapseclient.core.credentials.cred_data import (
    keyring,
    SynapseApiKeyCredentials,
    SynapseAuthTokenCredentials,
)


class TestSynapseApiKeyCredentials:

    def setup(self):
        self.api_key = b"I am api key"
        self.api_key_b64 = base64.b64encode(self.api_key).decode()
        self.username = "ahhhhhhhhhhhhhh"
        self.credentials = SynapseApiKeyCredentials(self.username, self.api_key_b64)

    def test_username(self):
        assert self.username == self.credentials.username

    def test_secret(self):
        # test exposed variable
        assert self.api_key_b64 == self.credentials.secret

        # test actual internal representation
        assert self.api_key == self.credentials._api_key

        # also provided via api_key property for backwards compatibility
        assert self.api_key == self.credentials._api_key

    def test_get_signed_headers(self):
        url = "https://www.synapse.org/fake_url"

        # mock the 'time' module so the result is always the same instead of dependent upon current time
        fake_time_string = "It is Wednesday, my dudes"
        with patch.object(time, "strftime", return_value=fake_time_string):
            headers = self.credentials.get_signed_headers(url)
            assert (
                {
                    'signatureTimestamp': fake_time_string,
                    'userId': self.username,
                    'signature': b'018ADVu2o2NUOxgO0gM9bo08Wcw='
                } == headers
            )

    def test_call(self):
        """Test the __call__ method used by requests.auth"""

        url = 'https://foobar.com/baz'
        initial_headers = {'existing': 'header'}
        signed_headers = {'signed': 'header'}

        with patch.object(self.credentials, 'get_signed_headers') as mock_get_signed_headers:
            mock_get_signed_headers.return_value = signed_headers

            request = MagicMock(spec=requests.Request)
            request.url = url
            request.headers = initial_headers

            self.credentials(request)

            assert request.headers == {**initial_headers, **signed_headers}
            mock_get_signed_headers.assert_called_once_with(url)

    def test_repr(self):
        assert (
            "SynapseApiKeyCredentials(username='ahhhhhhhhhhhhhh', api_key_string='SSBhbSBhcGkga2V5')" ==
            repr(self.credentials)
        )

    @patch.object(keyring, 'get_password')
    def test_get_from_keyring(self, mock_keyring_get_password):
        mock_keyring_get_password.return_value = self.api_key_b64
        credentials = SynapseApiKeyCredentials.get_from_keyring(self.username)
        mock_keyring_get_password.assert_called_once_with(
            SynapseApiKeyCredentials.get_keyring_service_name(),
            self.username,
        )
        assert credentials.username == self.username
        assert credentials.secret == self.api_key_b64

    @patch.object(keyring, 'delete_password')
    def test_delete_from_keyring(self, mock_keyring_delete_password):
        self.credentials.delete_from_keyring()
        mock_keyring_delete_password.assert_called_once_with(
            SynapseApiKeyCredentials.get_keyring_service_name(),
            self.username,
        )

    @patch.object(keyring, 'set_password')
    def test_store_to_keyring(self, mock_keyring_set_password):
        self.credentials.store_to_keyring()
        mock_keyring_set_password.assert_called_once_with(
            SynapseApiKeyCredentials.get_keyring_service_name(),
            self.username,
            self.api_key_b64,
        )


class TestSynapseAuthTokenCredentials:

    def setup(self):
        self.username = "ahhhhhhhhhhhhhh"
        self.auth_token = 'opensesame'
        self.credentials = SynapseAuthTokenCredentials(self.username, self.auth_token)

    def test_username(self):
        assert self.username == self.credentials.username

    def test_secret(self):
        assert self.credentials.secret == self.auth_token

    def test_call(self):
        """Test the __call__ method used by requests.auth"""

        initial_headers = {'existing': 'header'}
        auth_header = {'Authorization': f"Bearer {self.auth_token}"}

        request = MagicMock(spec=requests.Request)
        request.headers = initial_headers

        self.credentials(request)

        assert request.headers == {**initial_headers, **auth_header}

    def test_repr(self):
        assert (
            f"SynapseAuthTokenCredentials(username='{self.username}', token='{self.auth_token}')" ==
            repr(self.credentials)
        )

    @patch.object(keyring, 'get_password')
    def test_get_from_keyring(self, mock_keyring_get_password):
        mock_keyring_get_password.return_value = self.auth_token
        credentials = SynapseAuthTokenCredentials.get_from_keyring(self.username)
        mock_keyring_get_password.assert_called_once_with(
            SynapseAuthTokenCredentials.get_keyring_service_name(),
            self.username,
        )
        assert credentials.username == self.username
        assert credentials.secret == self.auth_token

    @patch.object(keyring, 'delete_password')
    def test_delete_from_keyring(self, mock_keyring_delete_password):
        self.credentials.delete_from_keyring()
        mock_keyring_delete_password.assert_called_once_with(
            SynapseAuthTokenCredentials.get_keyring_service_name(),
            self.username,
        )

    @patch.object(keyring, 'set_password')
    def test_store_to_keyring(self, mock_keyring_set_password):
        self.credentials.store_to_keyring()
        mock_keyring_set_password.assert_called_once_with(
            SynapseAuthTokenCredentials.get_keyring_service_name(),
            self.username,
            self.auth_token,
        )
