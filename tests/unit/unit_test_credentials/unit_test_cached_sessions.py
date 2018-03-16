import synapseclient.credentials.cached_sessions as cached_sessions
from mock import patch
from nose.tools import assert_is_none, assert_equals

@patch.object(cached_sessions, "keyring", autospec=True)
class TestCachedSessions():
    def setup(self):
        self.username = "username"

    def test_get_api_key__keyring_not_available(self, mocked_keyring):
        cached_sessions._keyring_is_available = False

        #function under test
        returned_key = cached_sessions.get_api_key(self.username)

        assert_is_none(returned_key)
        mocked_keyring.get_password.assert_not_called()

    def test_get_api_key__keyring_availble(self, mocked_keyring):
        cached_sessions._keyring_is_available = True
        key = "asdf"
        mocked_keyring.get_password.return_value = key

        #functionu under test
        returned_key = cached_sessions.get_api_key(self.username)

        assert_equals(key, returned_key)
        mocked_keyring.get_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME, self.username)

    def test_get_remove_api_key__keyring_not_available(self, mocked_keyring):
        cached_sessions._keyring_is_available = False

        #functionu under test
        cached_sessions.remove_api_key(self.username)

        mocked_keyring.delete_password.assert_not_called()

    def test_get_remove_api_key__keyring_available(self, mocked_keyring):
        cached_sessions._keyring_is_available = True

        #function under test
        cached_sessions.remove_api_key(self.username)

        mocked_keyring.delete_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME, self.username)

    def test_set_api_key__keyring_not_available(self):
        cached_sessions._keyring_is_available = False

        #function under test
        cached_sessions.set_api_key()

    #TODO MORE TESTS
