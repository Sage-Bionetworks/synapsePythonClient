import json
from mock import patch, mock_open
from nose.tools import assert_false, assert_equals

import synapseclient.credentials.cached_sessions as cached_sessions


@patch.object(cached_sessions, "keyring", autospec=True)
class TestCachedSessionsKeyring():
    def setup(self):
        self.username = "username"
        self.api_key = "ecks dee"


    def test_get_api_key__username_not_None(self, mocked_keyring):
        key = "asdf"
        mocked_keyring.get_password.return_value = key

        #functionu under test
        returned_key = cached_sessions.get_api_key(self.username)

        assert_equals(key, returned_key)
        mocked_keyring.get_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME, self.username)


    def test_get_api_key_username_is_None(self, mocked_keyring):
        key = "asdf"
        mocked_keyring.get_password.return_value = key

        #functionu under test
        returned_key = cached_sessions.get_api_key(None)

        assert_equals(None, returned_key)
        mocked_keyring.get_password.assert_not_called()


    def test_get_remove_api_key(self, mocked_keyring):
        #function under test
        cached_sessions.remove_api_key(self.username)

        mocked_keyring.delete_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME, self.username)


    def test_set_api_key(self, mocked_keyring):
            #function under test
            cached_sessions.set_api_key(self.username, self.api_key)

            mocked_keyring.set_password.assert_called_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME, self.username, self.api_key)



class TestCachedSessionsMostRecentUserFile():
    def test_readSessionCache_bad_file_data(self):
        with patch("os.path.isfile", return_value=True), \
             patch("os.path.join"):
            bad_cache_file_data = [
                '[]\n',  # empty array
                '["dis"]\n',  # array w/ element
                '{"is"}\n',  # set with element ( '{}' defaults to empty map so no case for that)
                '[{}]\n',  # array with empty set inside.
                '[{"snek"}]\n',  # array with nonempty set inside
                'hissss\n'  # string
            ]
            expectedDict = {}  # empty map
            # read each bad input and makes sure an empty map is returned instead
            for bad_data in bad_cache_file_data:
                with patch.object(cached_sessions,"open", mock_open(read_data=bad_data), create=True):
                    assert_equals(expectedDict, cached_sessions._read_session_cache())


    def test_readSessionCache_good_file_data(self):
        with patch("os.path.isfile", return_value=True), \
             patch("os.path.join"):
            expectedDict = {'AzureDiamond': 'hunter2',
                            'ayy': 'lmao'}
            good_data = json.dumps(expectedDict)
            with patch.object(cached_sessions, "open", mock_open(read_data=good_data), create=True):
                assert_equals(expectedDict, cached_sessions._read_session_cache())


    def test_get_most_recent_user(self):
        with patch.object(cached_sessions, "_read_session_cache", return_value={"<mostRecent>":"asdf"}) as mocked_read_session_cache:
            assert_equals("asdf", cached_sessions.get_most_recent_user())
            mocked_read_session_cache.assert_called_once_with()


    def test_set_most_recent_user(self):
        with patch.object(cached_sessions, "_write_session_cache") as mocked_write_session_cache:
            cached_sessions.set_most_recent_user("asdf")
            mocked_write_session_cache.assert_called_once_with({"<mostRecent>":"asdf"})
