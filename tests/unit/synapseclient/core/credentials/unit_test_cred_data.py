from unittest.mock import MagicMock

import pytest
import requests

from synapseclient.core.credentials.cred_data import SynapseAuthTokenCredentials
from synapseclient.core.exceptions import SynapseAuthenticationError


class TestSynapseAuthTokenCredentials:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.username = "ahhhhhhhhhhhhhh"
        self.auth_token = "opensesame"
        self.displayname = "hhhhhaaaa"
        self.credentials = SynapseAuthTokenCredentials(
            self.auth_token, username=self.username, displayname=self.displayname
        )
        self.KEYRING_NAME = "SYNAPSE.ORG_CLIENT_AUTH_TOKEN"

    def test_username(self):
        assert self.username == self.credentials.username

    def test_username_setter(self):
        credentials = SynapseAuthTokenCredentials(self.auth_token)
        assert credentials.username is None
        credentials.username = self.username
        assert credentials.username is self.username

    def test_displayname(self):
        assert self.displayname == self.credentials.displayname

    def test_displayname_setter(self):
        credentials = SynapseAuthTokenCredentials(self.auth_token)
        assert credentials.displayname is None
        credentials.displayname = self.displayname
        assert credentials.displayname is self.displayname

    def test_secret(self):
        assert self.credentials.secret == self.auth_token

    def test_call(self):
        """Test the __call__ method used by requests.auth"""

        initial_headers = {"existing": "header"}
        auth_header = {"Authorization": f"Bearer {self.auth_token}"}

        request = MagicMock(spec=requests.Request)
        request.headers = initial_headers

        self.credentials(request)

        assert request.headers == {**initial_headers, **auth_header}

    def test_repr(self):
        assert (
            f"SynapseAuthTokenCredentials("\
            f"username='{self.username}', "\
            f"displayname='{self.displayname}', "\
            f"token='{self.auth_token}')"
            == repr(self.credentials)
        )

    def test_tokens_validated(self, mocker):
        """Validate that tokens are validated when a credentials object is created"""
        token = "foo"
        mock_validate_token = mocker.patch.object(
            SynapseAuthTokenCredentials, "_validate_token"
        )
        SynapseAuthTokenCredentials(token)
        mock_validate_token.assert_called_once_with(token)

    @pytest.mark.parametrize(
        "token,valid",
        [
            # valid because not parseable at all.
            # we deem these valid to future-proof against a change to the token format that may not be parseable
            # in the same way (or at all)
            ("", True),
            ("thisisnotatoken", True),
            # invalid, parseable but do not contain view scope
            (
                "eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zNDQtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYxMzU4NTY5MywiaWF0IjoxNjEzNTg1NjkzLCJqdGkiOiI2MTMiLCJzdWIiOiIzNDA1MDk1In0.VFHau1pQJo1zCnK99R5QDY8zivwQg2S9K-aBKsYGpGwXlUuoQXAll9rjFo8ylz0Yy2qjVihCCxHZVqDOAnb_qjNYl2ZDO3C2QSACDDdITQM0lxVD1iuPoHtjM0Z6e1L4pTBOxpI2BqAlyXKV3se7E7Ix54E6JyVDTSACvOphwiM6Vkg5qmYHd8KWQXDXJRPG-ieQW4hXjbWElzaaQpUBGhesqVuZTgyAB1OIkWtJlirkLtxRXlXHsZ9jaNyrhtpscgu527kg2mIR_PePaEan3St-dMvRdggKrDGUmaxmLI68842eff__DRRJLiNdog4UJR5cbQP_9lFbv0l7ev5hEA",
                False,
            ),  # noqa
            (
                "eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zNDQtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYxMzU4NTY5MywiaWF0IjoxNjEzNTg1NjkzLCJqdGkiOiI2MTMiLCJzdWIiOiIzNDA1MDk1In0.VFHau1pQJo1zCnK99R5QDY8zivwQg2S9K - aBKsYGpGwXlUuoQXAll9rjFo8ylz0Yy2qjVihCCxHZVqDOAnb_qjNYl2ZDO3C2QSACDDdITQM0lxVD1iuPoHtjM0Z6e1L4pTBOxpI2BqAlyXKV3se7E7Ix54E6JyVDTSACvOphwiM6Vkg5qmYHd8KWQXDXJRPG - ieQW4hXjbWElzaaQpUBGhesqVuZTgyAB1OIkWtJlirkLtxRXlXHsZ9jaNyrhtpscgu527kg2mIR_PePaEan3St - dMvRdggKrDGUmaxmLI68842eff__DRRJLiNdog4UJR5cbQP_9lFbv0l7ev5hEA",
                False,
            ),  # noqa
            # valid, contain view scope
            (
                "eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyJdLCJvaWRjX2NsYWltcyI6e319LCJ0b2tlbl90eXBlIjoiUEVSU09OQUxfQUNDRVNTX1RPS0VOIiwiaXNzIjoiaHR0cHM6Ly9yZXBvLXByb2QtMzQ0LTAucHJvZC5zYWdlYmFzZS5vcmcvYXV0aC92MSIsImF1ZCI6IjAiLCJuYmYiOjE2MTM1ODUxNjIsImlhdCI6MTYxMzU4NTE2MiwianRpIjoiNjEyIiwic3ViIjoiMzQwNTA5NSJ9.rNm-SlmWMP4039fcSpnoDNbu9hnkCfoQ0D4O4Cvd0PPlods6ww8eIaCrzfADZ4Uk5vb58R4pW0ZcZmx3mnwVA3rNnLFrgj8BwSwTFiazGoSJ4GWu5bqEviRxP1FD5fKsQHa3EOjd9Zj9u4AvygywWAH97YflNdALH--4aSgeNVcDBldVw5oR_r09j9vXAioeoSW3Ty4QUtIH05cFbWKJmzmZy8K14JIWxj5Dpw6NvfkQbcNuDDEZ2If8hTVr3AyNrDtAZwdp_fNX26caXkWeHWCYUQKv_KUxzj34CZHOu4eeuTSlM0ozfUmrq0LpK7W05WtUEaIoVq7WeNon9yFjLQ",
                True,
            ),  # noqa
            (
                "eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zNDQtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYxMzU5Nzc0OSwiaWF0IjoxNjEzNTk3NzQ5LCJqdGkiOiI2MTQiLCJzdWIiOiIzNDA1MDk1In0.s_oB1PDOmZOQ43ALol6krcvs32QSR-sTbHd7wwFWgK9KActjpoqoSoypqYqMd4W5qIr0r633Pucc7KMZMK8jfZXSBAJsuBOXrJ5-4g2dwXib8TX_wWqXj6ten241_qOCVqWzEP9X3aIlAVTMExrIxaj3ReF_NKnVmgsk00L73UPezlG8OUBZBbG9_hvzgBObhqRhkYLA3-HwxuYtxOJfYz9iaJmDJ6xCG7VlEj2SZnBSt2tmScOo0FPCIZYFSvl9neNg9ITSD_B5AuigLHJDLQZD6goGCnB8StSa8rDGa8aCj_G9eM4bTIqdVKf3kctGtggbRQJ88JFVbsNCZNgvQ",
                True,
            ),  # noqa
        ],
    )
    def test_validate_token(self, token, valid):
        """Validate that parseable token must have view scope and that an unparseable token is considered valid"""
        if valid:
            SynapseAuthTokenCredentials._validate_token(token)
        else:
            pytest.raises(
                SynapseAuthenticationError,
                SynapseAuthTokenCredentials._validate_token,
                token,
            )
