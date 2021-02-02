import json

from unittest.mock import mock_open, patch

import synapseclient.core.credentials.cached_sessions as cached_sessions


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
                    assert expectedDict == cached_sessions._read_session_cache("mock so path doesn't matter")

    def test_readSessionCache_good_file_data(self):
        with patch("os.path.isfile", return_value=True), \
             patch("os.path.join"):
            expectedDict = {'AzureDiamond': 'hunter2',
                            'ayy': 'lmao'}
            good_data = json.dumps(expectedDict)
            with patch.object(cached_sessions, "open", mock_open(read_data=good_data), create=True):
                assert expectedDict == cached_sessions._read_session_cache(cached_sessions.SESSION_CACHE_FILEPATH)

    def test_get_most_recent_user(self):
        with patch.object(cached_sessions, "_read_session_cache", return_value={"<mostRecent>": "asdf"})\
                as mocked_read_session_cache:
            assert "asdf" == cached_sessions.get_most_recent_user()
            mocked_read_session_cache.assert_called_once_with(cached_sessions.SESSION_CACHE_FILEPATH)

    def test_set_most_recent_user(self):
        with patch.object(cached_sessions, "_write_session_cache") as mocked_write_session_cache:
            cached_sessions.set_most_recent_user("asdf")
            mocked_write_session_cache.assert_called_once_with(cached_sessions.SESSION_CACHE_FILEPATH,
                                                               {"<mostRecent>": "asdf"})
