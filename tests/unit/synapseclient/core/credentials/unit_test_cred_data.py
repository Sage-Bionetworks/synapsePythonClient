import base64
import requests
import time

import pytest
from unittest.mock import MagicMock, patch

from synapseclient.core.credentials.cred_data import (
    delete_stored_credentials,
    keyring,
    SynapseApiKeyCredentials,
    SynapseAuthTokenCredentials,
)
from synapseclient.core.exceptions import SynapseAuthenticationError


class TestSynapseApiKeyCredentials:

    def setup(self):
        self.api_key = b"I am api key"
        self.api_key_b64 = base64.b64encode(self.api_key).decode()
        self.username = "ahhhhhhhhhhhhhh"
        self.credentials = SynapseApiKeyCredentials(self.api_key_b64, self.username)
        self.KEYRING_NAME = 'SYNAPSE.ORG_CLIENT'

    def test_username(self):
        assert self.username == self.credentials.username

    def test_secret(self):
        # test exposed variable
        assert self.api_key_b64 == self.credentials.secret

        # test actual internal representation
        assert self.api_key == self.credentials._api_key

        # secret also provided via api_key property for backwards compatibility
        assert self.api_key_b64 == self.credentials.api_key

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
            self.KEYRING_NAME,
            self.username,
        )
        assert credentials.username == self.username
        assert credentials.secret == self.api_key_b64

    @patch.object(keyring, 'delete_password')
    def test_delete_from_keyring(self, mock_keyring_delete_password):
        self.credentials.delete_from_keyring()
        mock_keyring_delete_password.assert_called_once_with(
            self.KEYRING_NAME,
            self.username,
        )

    @patch.object(keyring, 'set_password')
    def test_store_to_keyring(self, mock_keyring_set_password):
        self.credentials.store_to_keyring()
        mock_keyring_set_password.assert_called_once_with(
            self.KEYRING_NAME,
            self.username,
            self.api_key_b64,
        )


class TestSynapseAuthTokenCredentials:

    def setup(self):
        self.username = "ahhhhhhhhhhhhhh"
        self.auth_token = 'opensesame'
        self.credentials = SynapseAuthTokenCredentials(self.auth_token, username=self.username)
        self.KEYRING_NAME = 'SYNAPSE.ORG_CLIENT_AUTH_TOKEN'

    def test_username(self):
        assert self.username == self.credentials.username

    def test_username_setter(self):
        credentials = SynapseAuthTokenCredentials(self.auth_token)
        assert credentials.username is None
        credentials.username = self.username
        assert credentials.username is self.username

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
            self.KEYRING_NAME,
            self.username,
        )
        assert credentials.username == self.username
        assert credentials.secret == self.auth_token

    @patch.object(keyring, 'delete_password')
    def test_delete_from_keyring(self, mock_keyring_delete_password):
        self.credentials.delete_from_keyring()
        mock_keyring_delete_password.assert_called_once_with(
            self.KEYRING_NAME,
            self.username,
        )

    @patch.object(keyring, 'set_password')
    def test_store_to_keyring(self, mock_keyring_set_password):
        self.credentials.store_to_keyring()
        mock_keyring_set_password.assert_called_once_with(
            self.KEYRING_NAME,
            self.username,
            self.auth_token,
        )

    def test_tokens_validated(self, mocker):
        """Validate that tokens are validated when a credentials object is created"""
        token = 'foo'
        mock_validate_token = mocker.patch.object(SynapseAuthTokenCredentials, '_validate_token')
        SynapseAuthTokenCredentials(token)
        mock_validate_token.assert_called_once_with(token)

    @pytest.mark.parametrize(
        "token,valid",
        [
            # valid because not parseable at all.
            # we deem these valid to future-proof against a change to the token format that may not be parseable
            # in the same way (or at all)
            ('', True),
            ('thisisnotatoken', True),

            # invalid, parseable but do not contain view scope
            ('eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zNDQtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYxMzU4NTY5MywiaWF0IjoxNjEzNTg1NjkzLCJqdGkiOiI2MTMiLCJzdWIiOiIzNDA1MDk1In0.VFHau1pQJo1zCnK99R5QDY8zivwQg2S9K-aBKsYGpGwXlUuoQXAll9rjFo8ylz0Yy2qjVihCCxHZVqDOAnb_qjNYl2ZDO3C2QSACDDdITQM0lxVD1iuPoHtjM0Z6e1L4pTBOxpI2BqAlyXKV3se7E7Ix54E6JyVDTSACvOphwiM6Vkg5qmYHd8KWQXDXJRPG-ieQW4hXjbWElzaaQpUBGhesqVuZTgyAB1OIkWtJlirkLtxRXlXHsZ9jaNyrhtpscgu527kg2mIR_PePaEan3St-dMvRdggKrDGUmaxmLI68842eff__DRRJLiNdog4UJR5cbQP_9lFbv0l7ev5hEA', False),  # noqa
            ('eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zNDQtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYxMzU4NTY5MywiaWF0IjoxNjEzNTg1NjkzLCJqdGkiOiI2MTMiLCJzdWIiOiIzNDA1MDk1In0.VFHau1pQJo1zCnK99R5QDY8zivwQg2S9K - aBKsYGpGwXlUuoQXAll9rjFo8ylz0Yy2qjVihCCxHZVqDOAnb_qjNYl2ZDO3C2QSACDDdITQM0lxVD1iuPoHtjM0Z6e1L4pTBOxpI2BqAlyXKV3se7E7Ix54E6JyVDTSACvOphwiM6Vkg5qmYHd8KWQXDXJRPG - ieQW4hXjbWElzaaQpUBGhesqVuZTgyAB1OIkWtJlirkLtxRXlXHsZ9jaNyrhtpscgu527kg2mIR_PePaEan3St - dMvRdggKrDGUmaxmLI68842eff__DRRJLiNdog4UJR5cbQP_9lFbv0l7ev5hEA', False),  # noqa

            # valid, contain view scope
            ('eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyJdLCJvaWRjX2NsYWltcyI6e319LCJ0b2tlbl90eXBlIjoiUEVSU09OQUxfQUNDRVNTX1RPS0VOIiwiaXNzIjoiaHR0cHM6Ly9yZXBvLXByb2QtMzQ0LTAucHJvZC5zYWdlYmFzZS5vcmcvYXV0aC92MSIsImF1ZCI6IjAiLCJuYmYiOjE2MTM1ODUxNjIsImlhdCI6MTYxMzU4NTE2MiwianRpIjoiNjEyIiwic3ViIjoiMzQwNTA5NSJ9.rNm-SlmWMP4039fcSpnoDNbu9hnkCfoQ0D4O4Cvd0PPlods6ww8eIaCrzfADZ4Uk5vb58R4pW0ZcZmx3mnwVA3rNnLFrgj8BwSwTFiazGoSJ4GWu5bqEviRxP1FD5fKsQHa3EOjd9Zj9u4AvygywWAH97YflNdALH--4aSgeNVcDBldVw5oR_r09j9vXAioeoSW3Ty4QUtIH05cFbWKJmzmZy8K14JIWxj5Dpw6NvfkQbcNuDDEZ2If8hTVr3AyNrDtAZwdp_fNX26caXkWeHWCYUQKv_KUxzj34CZHOu4eeuTSlM0ozfUmrq0LpK7W05WtUEaIoVq7WeNon9yFjLQ', True),  # noqa
            ('eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zNDQtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYxMzU5Nzc0OSwiaWF0IjoxNjEzNTk3NzQ5LCJqdGkiOiI2MTQiLCJzdWIiOiIzNDA1MDk1In0.s_oB1PDOmZOQ43ALol6krcvs32QSR-sTbHd7wwFWgK9KActjpoqoSoypqYqMd4W5qIr0r633Pucc7KMZMK8jfZXSBAJsuBOXrJ5-4g2dwXib8TX_wWqXj6ten241_qOCVqWzEP9X3aIlAVTMExrIxaj3ReF_NKnVmgsk00L73UPezlG8OUBZBbG9_hvzgBObhqRhkYLA3-HwxuYtxOJfYz9iaJmDJ6xCG7VlEj2SZnBSt2tmScOo0FPCIZYFSvl9neNg9ITSD_B5AuigLHJDLQZD6goGCnB8StSa8rDGa8aCj_G9eM4bTIqdVKf3kctGtggbRQJ88JFVbsNCZNgvQ', True),  # noqa
        ]

    )
    def test_validate_token(self, token, valid):
        """Validate that parseable token must have view scope and that an unparseable token is considered valid"""
        if valid:
            SynapseAuthTokenCredentials._validate_token(token)
        else:
            pytest.raises(SynapseAuthenticationError, SynapseAuthTokenCredentials._validate_token, token)


def test_delete_stored_credentials__stored(mocker):
    """Verify deleting all credentials stored in the keyring."""

    username = 'foo'
    mock_token_credentials = MagicMock(spec=SynapseAuthTokenCredentials)
    mock_token_get = mocker.patch.object(SynapseAuthTokenCredentials, 'get_from_keyring')
    mock_token_get.return_value = mock_token_credentials

    mock_apikey_credentials = MagicMock(spec=SynapseApiKeyCredentials)
    mock_apikey_get = mocker.patch.object(SynapseApiKeyCredentials, 'get_from_keyring')
    mock_apikey_get.return_value = mock_apikey_credentials

    delete_stored_credentials(username)

    mock_token_get.assert_called_once_with(username)
    mock_token_credentials.delete_from_keyring.assert_called_once_with()

    mock_apikey_get.assert_called_once_with(username)
    mock_apikey_credentials.delete_from_keyring.assert_called_once_with()


def test_delete_stored_credentials__empty(mocker):
    """Verify the behavior of deleting all stored credentials when none are actually stored."""

    username = 'foo'
    mock_token_get = mocker.patch.object(SynapseAuthTokenCredentials, 'get_from_keyring')
    mock_token_delete = mocker.patch.object(SynapseAuthTokenCredentials, 'delete_from_keyring')
    mock_token_get.return_value = None

    mock_apikey_get = mocker.patch.object(SynapseApiKeyCredentials, 'get_from_keyring')
    mock_apikey_delete = mocker.patch.object(SynapseApiKeyCredentials, 'delete_from_keyring')
    mock_apikey_get.return_value = None

    delete_stored_credentials(username)

    mock_token_get.assert_called_once_with(username)
    mock_token_delete.assert_not_called()
    mock_apikey_get.assert_called_once_with(username)
    mock_apikey_delete.assert_not_called()
