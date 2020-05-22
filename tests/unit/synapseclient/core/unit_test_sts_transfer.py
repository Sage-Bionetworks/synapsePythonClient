import boto3
import datetime

from synapseclient import Synapse
from synapseclient.core import sts_transfer
from synapseclient.core.sts_transfer import _StsTokenStore, _TokenCache, with_boto_sts_credentials

from synapseclient.core.utils import datetime_to_iso

import mock
from nose.tools import assert_equal, assert_false, assert_is, assert_raises, assert_true


@mock.patch.object(sts_transfer._TOKEN_STORE, 'get_token')
class TestGetStsCredentials:

    @classmethod
    def setUpClass(cls):
        cls._utcnow = datetime.datetime.utcnow()

    @classmethod
    def _make_credentials(cls):
        return {
            'accessKeyId': 'foo',
            'secretAccessKey': 'bar',
            'sessionToken': 'baz',
            'expiration': datetime_to_iso(cls._utcnow),
        }

    def test_boto_format(self, mock_get_token):
        """Verify that tokens returned in boto format are as expected,
        i.e. a dictionary that can be passed to a boto session"""
        mock_get_token.return_value = self._make_credentials()

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'

        expected_output = {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_session_token': 'baz',
        }

        credentials = sts_transfer.get_sts_credentials(syn, entity_id, permission, output_format='boto')
        assert_equal(credentials, expected_output)
        mock_get_token.assert_called_once_with(
            syn,
            entity_id,
            permission,
            sts_transfer.DEFAULT_MIN_LIFE
        )

    @mock.patch.object(sts_transfer, 'platform')
    @mock.patch.object(sts_transfer, 'os')
    def test_shell_format__windows_cmd(self, mock_os, mock_platform, mock_get_token):
        """Verify Windows cmd compatible shell output"""
        mock_os.environ = {}
        mock_platform.system.return_value = 'Windows'
        mock_get_token.return_value = self._make_credentials()

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'

        expected_output = f"""\
setx AWS_ACCESS_KEY_ID "foo"
setx AWS_SECRET_ACCESS_KEY "bar"
setx AWS_SESSION_TOKEN "baz"
"""

        min_remaining_life = datetime.timedelta(minutes=30)
        credentials = sts_transfer.get_sts_credentials(
            syn,
            entity_id,
            permission,
            output_format='shell',
            min_remaining_life=datetime.timedelta(minutes=30),
        )
        assert_equal(credentials, expected_output)
        mock_get_token.assert_called_once_with(syn, entity_id, permission, min_remaining_life)

    def _bash_shell_test(self, mock_get_token):
        mock_get_token.return_value = self._make_credentials()

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read'

        expected_output = f"""\
export AWS_ACCESS_KEY_ID="foo"
export AWS_SECRET_ACCESS_KEY="bar"
export AWS_SESSION_TOKEN="baz"
"""

        credentials = sts_transfer.get_sts_credentials(syn, entity_id, permission, output_format='shell')
        assert_equal(credentials, expected_output)
        mock_get_token.assert_called_with(syn, entity_id, permission, sts_transfer.DEFAULT_MIN_LIFE)

    @mock.patch.object(sts_transfer, 'platform')
    @mock.patch.object(sts_transfer, 'os')
    def test_shell_format__windows_bash(self, mock_os, mock_platform, mock_get_token):
        """Verify that a bash shell on Windows is treated as bash on shell output"""
        mock_platform.system.return_value = 'Windows'
        mock_os.environ = {'SHELL': 'bash'}
        mock_get_token.return_value = self._make_credentials()

        self._bash_shell_test(mock_get_token)

    @mock.patch.object(sts_transfer, 'platform')
    def test_shell__other_oses(self, mock_platform, mock_get_token):
        """Verify we treat any other os as running in a bash compatible shell"""
        for platform in ('Linux', 'Darwin', 'AnythingElse'):
            mock_platform.system.return_value = platform
            mock_get_token.return_value = self._make_credentials()

            self._bash_shell_test(mock_get_token)

    @mock.patch.object(sts_transfer, 'platform')
    def test_json_format(self, mock_platform, mock_get_token):
        """Verify that any other output_format just results in the raw dictionary being passed through."""
        expected_credentials = mock_get_token.return_value = self._make_credentials()

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read'

        min_remaining_life = datetime.timedelta(hours=2)
        credentials = sts_transfer.get_sts_credentials(
            syn,
            entity_id,
            permission,
            output_format='json',
            min_remaining_life=min_remaining_life,
        )
        assert_equal(expected_credentials, credentials)
        mock_get_token.assert_called_with(syn, entity_id, permission, min_remaining_life)

    def test_other_formats_rejected(self, mock_get_token):
        mock_get_token.return_value = self._make_credentials()

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read'

        for output_format in ('', None, 'foobar'):
            with assert_raises(ValueError):
                sts_transfer.get_sts_credentials(syn, entity_id, permission, output_format=output_format)


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
            _StsTokenStore().get_token(mock.Mock(), 'syn_1', 'not_a_valid_permission', datetime.timedelta(hours=1))

    def test_fetch_and_cache_token(self):
        entity_id = 'syn_1'
        token_store = _StsTokenStore()
        min_remaining_life = datetime.timedelta(hours=1)

        expiration = datetime_to_iso(datetime.datetime.utcnow() + datetime.timedelta(hours=10))
        read_token = {'accessKeyId': '123', 'expiration': expiration}
        write_token = {'accessKeyId': '456', 'expiration': expiration}

        def synGET(uri):
            if 'read_write' in uri:
                return write_token
            return read_token
        syn = mock.Mock(restGET=mock.Mock(side_effect=synGET))

        token = token_store.get_token(syn, entity_id, 'read_only', min_remaining_life)
        assert_is(token, read_token)
        assert_equal(syn.restGET.call_count, 1)
        assert_equal(f"/entity/{entity_id}/sts?permission=read_only", syn.restGET.call_args[0][0])

        # getting the token again shouldn't cause it to be fetched again
        token = token_store.get_token(syn, entity_id, 'read_only', min_remaining_life)
        assert_is(token, read_token)
        assert_equal(syn.restGET.call_count, 1)

        # however fetching a read_write token should cause a separate fetch
        token = token_store.get_token(syn, entity_id, 'read_write', min_remaining_life)
        assert_is(token, write_token)
        assert_equal(syn.restGET.call_count, 2)
        assert_equal(f"/entity/{entity_id}/sts?permission=read_write", syn.restGET.call_args[0][0])

        # but that should also b cached now
        token = token_store.get_token(syn, entity_id, 'read_write', min_remaining_life)
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

        def fn(credentials):
            assert_equal(credentials, self._make_credentials())
            return return_value

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        # additional args/kwargs should be passed through to the get token function
        result = with_boto_sts_credentials(fn, syn, entity_id, permission)
        assert_equal(result, return_value)

        expected_get_sts_call = mock.call(syn, entity_id, permission, output_format='boto')
        assert_equal(expected_get_sts_call, mock_get_sts_credentials.call_args)

    def test_other_error(self, mock_get_sts_credentials):
        """Verify any error that isn't expired credentials is raised straight away"""

        ex_message = 'This is not a boto error'

        def fn(credentials):
            raise ValueError(ex_message)

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        with assert_raises(ValueError) as ex_cm:
            with_boto_sts_credentials(fn, syn, entity_id, permission)
        assert_true(ex_message == str(ex_cm.exception))

    def test_expired_creds(self, mock_get_sts_credentials):
        """Verify that a request with expired creds retries once."""

        ex_message = 'This error is the result of an ExpiredToken'
        return_value = 'success!!!'

        call_count = 0

        def fn(credentials):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise boto3.exceptions.Boto3Error(ex_message)

            return return_value

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        result = with_boto_sts_credentials(fn, syn, entity_id, permission)
        assert_equal(result, return_value)
        assert_equal(call_count, 2)

    def test_error_raised_if_multiple_errors(self, mock_get_sts_credentials):
        """Verify that if we end up with multiple consecutive expired error tokens
        somehow we just raise it and don't get stuck in an infinite retry loop"""
        ex_message = 'error is the result of an ExpiredToken'
        call_count = 0

        def fn(credentials):
            nonlocal call_count
            call_count += 1
            raise boto3.exceptions.Boto3Error(ex_message)

        syn = mock.Mock()
        entity_id = 'syn_1'
        permission = 'read_write'
        mock_get_sts_credentials.return_value = self._make_credentials()

        with assert_raises(boto3.exceptions.Boto3Error) as ex_cm:
            with_boto_sts_credentials(fn, syn, entity_id, permission)
        assert_equal(ex_message, str(ex_cm.exception))
        assert_equal(2, call_count)


class TestIsBotoStsTransferEnabled:

    @mock.patch.object(sts_transfer, 'boto3', new_callable=mock.PropertyMock)
    def test_config_enabled(self, mock_boto3):
        """Verify that so long as boto3 is importable we are enabled for boto transfers
        if the synapse object is configured for it"""

        syn = mock.Mock()
        for val in (True, False):
            syn.use_boto_sts_transfers = val
            assert_equal(val, sts_transfer.is_boto_sts_transfer_enabled(syn))

    @mock.patch.object(sts_transfer, 'boto3', new_callable=mock.PropertyMock(return_value=None))
    def test_boto_import_required(self, mock_boto3):
        """Verify that if boto3 is not importable that sts transfers are always
        disabled no matter what the config says."""

        syn = mock.Mock()
        syn._get_config_section_dict.return_value = {'use_boto_sts': 'true'}
        assert_false(sts_transfer.is_boto_sts_transfer_enabled(syn))


class TestIsStorageLocationStsEnabled:

    def test_none_location(self):
        """A None location is not enabled"""
        assert_false(sts_transfer.is_storage_location_sts_enabled(mock.Mock(), 'syn_1', None))

    def test_location_mapping(self):
        """Check a storage location dictionary as returned by e.g. """

        for sts_enabled in (True, False, None):
            location = {}
            if sts_enabled:
                location['stsEnabled'] = sts_enabled

            assert_equal(sts_transfer.is_storage_location_sts_enabled(
                mock.Mock(),
                'syn_1',
                location,
            ), bool(sts_enabled))

    def test_storage_location_id(self):
        """Test with determining if a storage_location_id is STS enabled by
        fetching the upload destination."""

        syn = mock.Mock()
        entity_id = 'syn_1'
        storage_location_id = 1234

        for sts_enabled in (True, False, None):
            location = {}
            if sts_enabled:
                location['stsEnabled'] = sts_enabled

            syn.restGET.return_value = location
            assert_equal(
                bool(sts_enabled),
                sts_transfer.is_storage_location_sts_enabled(syn, entity_id, storage_location_id)
            )

            syn.restGET.assert_called_with(
                f'/entity/{entity_id}/uploadDestination/{storage_location_id}',
                endpoint=syn.fileHandleEndpoint,
            )
