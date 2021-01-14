import base64
import requests
import time

from unittest.mock import MagicMock, patch

from synapseclient.core.credentials.cred_data import SynapseCredentials

#@patch.object(cached_sessions, "keyring", autospec=True)
#class TestCachedSessionsKeyring:
#    def setup(self):
#        self.username = "username"
#        self.api_key = "ecks dee"
#
#    def test_get_api_key__username_not_None(self, mocked_keyring):
#        key = "asdf"
#        mocked_keyring.get_password.return_value = key
#
#        # function under test
#        returned_key = cached_sessions.get_api_key(self.username)
#
#        assert key == returned_key
#        mocked_keyring.get_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME,
#                                                            self.username)
#
#    def test_get_api_key_username_is_None(self, mocked_keyring):
#        key = "asdf"
#        mocked_keyring.get_password.return_value = key
#
#        # function under test
#        returned_key = cached_sessions.get_api_key(None)
#
#        assert returned_key is None
#        mocked_keyring.get_password.assert_not_called()
#
#    def test_get_remove_api_key(self, mocked_keyring):
#        # function under test
#        cached_sessions.remove_api_key(self.username)
#
#        mocked_keyring.delete_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME,
#                                                               self.username)
#
#    def test_set_api_key(self, mocked_keyring):
#        # function under test
#        cached_sessions.set_api_key(self.username, self.api_key)
#
#        mocked_keyring.set_password.assert_called_with(
#            cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME,
#            self.username, self.api_key,
#        )


class TestSynapseCredentials:
    def setup(self):
        self.api_key = b"I am api key"
        self.api_key_b64 = base64.b64encode(self.api_key).decode()
        self.username = "ahhhhhhhhhhhhhh"
        self.credentials = SynapseCredentials(self.username, self.api_key_b64)

    def test_api_key_property(self):
        # test exposed variable
        assert self.api_key_b64 == self.credentials.api_key

        # test actual internal representation
        assert self.api_key == self.credentials._api_key

    def test_get_auth_headers(self):
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
            "SynapseCredentials(username='ahhhhhhhhhhhhhh', api_key_string='SSBhbSBhcGkga2V5')" ==
            repr(self.credentials)
        )
