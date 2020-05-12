from collections import OrderedDict
import datetime


from synapseclient.core.sts_transfer import _StsTokenStore, _TokenCache

from synapseclient.core.utils import iso_to_datetime, datetime_to_iso

import mock
from nose.tools import assert_equal, assert_is, assert_is_none, assert_raises


class TestTokenCache:

    def test_max_size(self):
        """Verify a token cache will not exceed the specified number of keys, ejecting FIFO as needed"""
        max_size = 5
        ejections = 3

        token_cache = _TokenCache(datetime.timedelta(hours=1), max_size)
        token = {'expiration': datetime_to_iso(datetime.datetime.utcnow() + datetime.timedelta(days=1))}

        for i in range(max_size + ejections):
            token_cache[f"syn_{i}"] = token

        expected_keys = [f"syn_{i}" for i in range(ejections, max_size + ejections)]
        assert_equal(expected_keys, list(token_cache.keys()))

    @mock.patch('synapseclient.core.sts_transfer.datetime')
    def test_old_tokens_pruned(self, mock_datetime):
        """Verify that tokens that do not have the remaining min_life before their expiration are not returned
        and are pruned as new tokens are added."""

        # we mock datetime and drop subseconds to make results to the second deterministic
        # as they go back and forth bewteen parsing etc
        utc_now = datetime.datetime.utcnow().replace(microsecond=0)
        mock_datetime.datetime.utcnow = mock.Mock(
            return_value=utc_now
        )

        token_cache = _TokenCache(datetime.timedelta(minutes=60), 1000)

        # this token should be immediately pruned
        token_cache['syn_1'] = {'expiration': datetime_to_iso(utc_now)}
        assert_equal(0, len(token_cache))

        token_cache['syn_2'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=60))}
        token_cache['syn_3'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=60))}
        token_cache['syn_4'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=60))}
        token_cache['syn_5'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=60))}
        token_cache['syn_6'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=75))}
        token_cache['syn_7'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=90))}
        token_cache['syn_8'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=105))}

        # all the aditional keys should still be there
        expected_keys = [f"syn_{i}" for i in range(2, 9)]
        assert_equal(expected_keys, list(token_cache.keys()))

        token_cache.min_life_delta = datetime.timedelta(minutes=90)

        # the cache still thinks there are some stale entries, but attempting to retrieve them will
        # cause them to be removed
        assert_equal(expected_keys, list(token_cache.keys()))
        assert_is_none(token_cache['syn_3'])
        assert_is_none(token_cache['syn_4'])
        expected_keys = ['syn_2', 'syn_5', 'syn_6', 'syn_7', 'syn_8']
        assert_equal(expected_keys, list(token_cache.keys()))

        # setting another token should cause all remaining stale tokens to be removed
        expected_keys = ['syn_7', 'syn_8', 'syn_9']
        token_cache['syn_9'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=110))}
        assert_equal(expected_keys, list(token_cache.keys()))


class TestStsTokenStore:

    def test_invalid_permission(self):
        with assert_raises(ValueError):
            _StsTokenStore().get_token(mock.Mock(), 'syn_1', 'not_a_valid_permission')

    def test_fetch_and_cache_token(self):
        entity_id = 'syn_1'
        token_store = _StsTokenStore()

        expiration = datetime_to_iso(datetime.datetime.utcnow() + datetime.timedelta(hours=10))
        read_token = {'accessKeyId': '123', 'expiration': expiration}
        write_token = {'accessKeyId': '456', 'expiration': expiration}

        def synGET(uri):
            if 'read_write' in uri:
                return write_token
            return read_token
        syn = mock.Mock(restGET=mock.Mock(side_effect=synGET))

        token = token_store.get_token(syn, entity_id, 'read_only')
        assert_is(token, read_token)
        assert_equal(syn.restGET.call_count, 1)
        assert_equal(f"/entity/{entity_id}/sts?permission=read_only", syn.restGET.call_args[0][0])

        # getting the token again shouldn't cause it to be fetched again
        token = token_store.get_token(syn, entity_id, 'read_only')
        assert_is(token, read_token)
        assert_equal(syn.restGET.call_count, 1)

        # however fetching a read_write token should cause a separate fetch
        token = token_store.get_token(syn, entity_id, 'read_write')
        assert_is(token, write_token)
        assert_equal(syn.restGET.call_count, 2)
        assert_equal(f"/entity/{entity_id}/sts?permission=read_write", syn.restGET.call_args[0][0])

        # but that should also b cached now
        token = token_store.get_token(syn, entity_id, 'read_write')
        assert_is(token, write_token)
        assert_equal(syn.restGET.call_count, 2)
