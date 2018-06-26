import json
from mock import patch, mock_open, create_autospec, call
from nose.tools import assert_equals

import synapseclient.credentials.cached_sessions as cached_sessions
from synapseclient import Synapse


@patch.object(cached_sessions, "keyring", autospec=True)
class TestCachedSessionsKeyring:
    def setup(self):
        self.username = "username"
        self.api_key = "ecks dee"

    def test_get_api_key__username_not_None(self, mocked_keyring):
        key = "asdf"
        mocked_keyring.get_password.return_value = key

        # function under test
        returned_key = cached_sessions.get_api_key(self.username)

        assert_equals(key, returned_key)
        mocked_keyring.get_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME,
                                                            self.username)

    def test_get_api_key_username_is_None(self, mocked_keyring):
        key = "asdf"
        mocked_keyring.get_password.return_value = key

        # function under test
        returned_key = cached_sessions.get_api_key(None)

        assert_equals(None, returned_key)
        mocked_keyring.get_password.assert_not_called()

    def test_get_remove_api_key(self, mocked_keyring):
        # function under test
        cached_sessions.remove_api_key(self.username)

        mocked_keyring.delete_password.assert_called_once_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME,
                                                               self.username)

    def test_set_api_key(self, mocked_keyring):
            # function under test
            cached_sessions.set_api_key(self.username, self.api_key)

            mocked_keyring.set_password.assert_called_with(cached_sessions.SYNAPSE_CACHED_SESSION_APLICATION_NAME,
                                                           self.username, self.api_key)


class TestCachedSessionsMostRecentUserFile:

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
                with patch.object(cached_sessions, "open", mock_open(read_data=bad_data), create=True):
                    assert_equals(expectedDict, cached_sessions._read_session_cache("mock so path doesn't matter"))

    def test_readSessionCache_good_file_data(self):
        with patch("os.path.isfile", return_value=True), \
             patch("os.path.join"):
            expectedDict = {'AzureDiamond': 'hunter2',
                            'ayy': 'lmao'}
            good_data = json.dumps(expectedDict)
            with patch.object(cached_sessions, "open", mock_open(read_data=good_data), create=True):
                assert_equals(expectedDict, cached_sessions._read_session_cache(cached_sessions.SESSION_CACHE_FILEPATH))

    def test_get_most_recent_user(self):
        with patch.object(cached_sessions, "_read_session_cache", return_value={"<mostRecent>": "asdf"})\
                as mocked_read_session_cache:
            assert_equals("asdf", cached_sessions.get_most_recent_user())
            mocked_read_session_cache.assert_called_once_with(cached_sessions.SESSION_CACHE_FILEPATH)

    def test_set_most_recent_user(self):
        with patch.object(cached_sessions, "_write_session_cache") as mocked_write_session_cache:
            cached_sessions.set_most_recent_user("asdf")
            mocked_write_session_cache.assert_called_once_with(cached_sessions.SESSION_CACHE_FILEPATH,
                                                               {"<mostRecent>": "asdf"})


class TestMigrateOldSessionFile(object):

    def setup(self):
        read_session_cache_patcher = patch.object(cached_sessions, "_read_session_cache")
        set_most_recent_user_patcher = patch.object(cached_sessions, "set_most_recent_user")
        set_api_key_patcher = patch.object(cached_sessions, "set_api_key")
        os_patcher = patch.object(cached_sessions, 'os')
        equals_path_patcher = patch.object(cached_sessions, 'equal_paths')
        self.patchers = [read_session_cache_patcher, set_most_recent_user_patcher, set_api_key_patcher, os_patcher,
                         equals_path_patcher]

        self.mock_syn = create_autospec(Synapse())

        self.mock_read_session_cache = read_session_cache_patcher.start()
        self.mock_set_most_recent_user = set_most_recent_user_patcher.start()
        self.mock_set_api_key = set_api_key_patcher.start()
        self.mock_os = os_patcher.start()
        self.mock_equals_path = equals_path_patcher.start()

        self.file_path = "/asdf/asdf/asdf/.session"
        self.mock_os.path.join.return_value = self.file_path

    def teardown(self):
        for patcher in self.patchers:
            patcher.stop()

    def test_no_migrate(self):
        self.mock_equals_path.return_value = False
        cached_sessions.migrate_old_session_file_credentials_if_necessary(self.mock_syn)

        self.mock_read_session_cache.assert_not_called()
        self.mock_set_most_recent_user.assert_not_called()
        self.mock_set_api_key.assert_not_called()
        self.mock_os.remove.assert_called_once_with(self.file_path)

    def test_migrate(self):
        self.mock_equals_path.return_value = True
        self.mock_read_session_cache.return_value = {'user': 'userapi', '<mostRecent>': 'asdf', 'user2': 'user2api'}

        cached_sessions.migrate_old_session_file_credentials_if_necessary(self.mock_syn)

        self.mock_read_session_cache.assert_called_once_with(self.file_path)
        self.mock_set_most_recent_user.assert_called_with('asdf')
        self.mock_set_api_key.assert_has_calls([call('user', 'userapi'), call('user2', 'user2api')], any_order=True)
        self.mock_os.remove.assert_called_once_with(self.file_path)
