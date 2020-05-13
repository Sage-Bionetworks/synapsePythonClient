from collections import OrderedDict
import boto3
import datetime


from synapseclient.core import sts_transfer
from synapseclient.core.sts_transfer import _StsTokenStore, _TokenCache, with_boto_sts_credentials

from synapseclient.core.utils import iso_to_datetime, datetime_to_iso

import mock
from nose.tools import assert_equal, assert_is, assert_is_none, assert_raises, assert_true


class TestTokenCache:

    def test_max_size(self):
        """Verify a token cache will not exceed the specified number of keys, ejecting FIFO as needed"""
        max_size = 5
        ejections = 3

        token_cache = _TokenCache(max_size)
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

        token_cache = _TokenCache(1000)

        # this token should be immediately pruned
        token_cache['syn_1'] = {'expiration': datetime_to_iso(utc_now - datetime.timedelta(seconds=1))}
        assert_equal(0, len(token_cache))

        token_cache['syn_2'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(seconds=1))}
        token_cache['syn_3'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(minutes=1))}
        token_cache['syn_4'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(hours=1))}

        # all the additional keys should still be there
        assert_equal(['syn_2', 'syn_3', 'syn_4'], list(token_cache.keys()))

        # if we set a new key in the future any keys that are expired at that time should be pruned
        mock_datetime.datetime.utcnow = mock.Mock(
            return_value=utc_now + datetime.timedelta(minutes=30)
        )

        token_cache['syn_5'] = {'expiration': datetime_to_iso(utc_now + datetime.timedelta(days=1))}
        assert_equal(['syn_4', 'syn_5'], list(token_cache.keys()))


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


@mock.patch.object(sts_transfer, 'get_sts_credentials')
class TestWithBotoStsCredentials:

    @staticmethod
    def _make_credentials():
        return {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_session_token': 'baz',
        }

    def test_successful_request(self, mock_get_sts_credentials):
        """Verify that a successful request with valid unexpired credentials
        passes through as expected and returns the proper value."""

        return_value = 'success!!!'

        def fn(**credentials):
            assert_equal(credentials, self._make_credentials())
            return return_value

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        # additional args/kwargs should be passed through to the get token function
        args = ['these', 'are', 'args']
        kwargs = {'and': 'these', 'are': 'kwargs'}

        result = with_boto_sts_credentials(fn, syn, entity_id, permission, *args, **kwargs)
        assert_equal(result, return_value)

        expected_get_sts_call = mock.call(
            *[syn, entity_id, permission, *args],
            output_format='boto',
            **kwargs,
        )

        assert_equal(expected_get_sts_call, mock_get_sts_credentials.call_args)

    def test_other_error(self, mock_get_sts_credentials):
        """Verify any error that isn't expired credentials is raised straight away"""

        ex_message = 'This is not a boto error'

        def fn(**credentials):
            raise ValueError(ex_message)

        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        with assert_raises(ValueError) as ex_cm:
            with_boto_sts_credentials(fn, entity_id, permission)
        assert_true(ex_message == str(ex_cm.exception))

    def test_expired_creds(self, mock_get_sts_credentials):
        """Verify that a request with expired creds retries once."""

        ex_message = 'This error is the result of an ExpiredToken'
        return_value = 'success!!!'

        call_count = 0

        def fn(**credentials):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise boto3.exceptions.Boto3Error(ex_message)

            return return_value

        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        result = with_boto_sts_credentials(fn, entity_id, permission)
        assert_equal(result, return_value)
        assert_equal(call_count, 2)

    def test_error_raised_if_multiple_errors(self, mock_get_sts_credentials):
        """Verify that if we end up with multiple consecutive expired error tokens
        somehow we just raise it and don't get stuck in an infinite retry loop"""
        ex_message = 'error is the result of an ExpiredToken'
        call_count = 0

        def fn(**credentials):
            nonlocal call_count
            call_count += 1
            raise boto3.exceptions.Boto3Error(ex_message)

        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        with assert_raises(boto3.exceptions.Boto3Error) as ex_cm:
            with_boto_sts_credentials(fn, entity_id, permission)
        assert_equal(ex_message, str(ex_cm.exception))
        assert_equal(2, call_count)
