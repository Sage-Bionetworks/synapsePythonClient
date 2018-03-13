import json

from mock import patch, mock_open
from nose.tools import assert_equal


def test_readSessionCache_bad_file_data():
    with patch("os.path.isfile", return_value=True), \
         patch("os.path.join"):

        bad_cache_file_data = [
                            '[]\n', # empty array
                            '["dis"]\n', # array w/ element
                            '{"is"}\n', # set with element ( '{}' defaults to empty map so no case for that)
                            '[{}]\n', # array with empty set inside.
                            '[{"snek"}]\n', # array with nonempty set inside
                            'hissss\n' # string
                            ]
        expectedDict = {} # empty map
        # read each bad input and makes sure an empty map is returned instead
        for bad_data in bad_cache_file_data:
            with patch("synapseclient.client.open", mock_open(read_data=bad_data), create=True):
                assert_equal(expectedDict, syn._readSessionCache())


def test_readSessionCache_good_file_data():
    with patch("os.path.isfile", return_value=True), \
         patch("os.path.join"):

        expectedDict = {'AzureDiamond': 'hunter2',
                        'ayy': 'lmao'}
        good_data = json.dumps(expectedDict)
        with patch("synapseclient.client.open", mock_open(read_data=good_data), create=True):
            assert_equal(expectedDict, syn._readSessionCache())