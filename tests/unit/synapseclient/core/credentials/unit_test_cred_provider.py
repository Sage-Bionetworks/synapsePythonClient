import base64

import pytest
from unittest.mock import create_autospec, MagicMock, patch

from synapseclient.core.credentials import credential_provider
from synapseclient.core.credentials.credential_provider import (
    cached_sessions,
    CachedCredentialsProvider,
    ConfigFileCredentialsProvider,
    SynapseCredentialsProvider,
    SynapseCredentialsProviderChain,
    UserArgsCredentialsProvider,
    UserArgsSessionTokenCredentialsProvider,
)
from synapseclient.core.credentials.cred_data import (
    SynapseApiKeyCredentials,
    SynapseAuthTokenCredentials,
    UserLoginArgs,
)


class TestSynapseApiKeyCredentialsProviderChain(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.cred_provider = create_autospec(SynapseCredentialsProvider)
        self.user_login_args = UserLoginArgs(*(None,) * 4)  # user login args don't matter for these tests
        self.credential_provider_chain = SynapseCredentialsProviderChain([self.cred_provider])

    def test_get_credentials__provider_not_return_credentials(self):
        self.cred_provider.get_synapse_credentials.return_value = None

        creds = self.credential_provider_chain.get_credentials(self.syn, self.user_login_args)

        assert creds is None
        self.cred_provider.get_synapse_credentials.assert_called_once_with(self.syn, self.user_login_args)

    def test_get_credentials__provider_return_credentials(self):
        username = "synapse_user"
        api_key = base64.b64encode(b"api_key").decode()
        self.cred_provider.get_synapse_credentials.return_value = SynapseApiKeyCredentials(username, api_key)

        creds = self.credential_provider_chain.get_credentials(self.syn, self.user_login_args)

        assert isinstance(creds, SynapseApiKeyCredentials)
        assert username == creds.username
        assert api_key == creds.secret
        self.cred_provider.get_synapse_credentials.assert_called_once_with(self.syn, self.user_login_args)

    def test_get_credentials__multiple_providers(self):
        cred_provider2 = create_autospec(SynapseCredentialsProvider)
        cred_provider3 = create_autospec(SynapseCredentialsProvider)

        self.cred_provider.get_synapse_credentials.return_value = None
        cred_provider2.get_synapse_credentials.return_value = SynapseApiKeyCredentials(
            "asdf",
            base64.b64encode(b"api_key").decode(),
        )
        cred_provider3.get_synapse_credentials.return_value = None

        # change the credential providers
        self.credential_provider_chain.cred_providers = [self.cred_provider, cred_provider2, cred_provider3]

        creds = self.credential_provider_chain.get_credentials(self.syn, self.user_login_args)
        assert isinstance(creds, SynapseApiKeyCredentials)

        self.cred_provider.get_synapse_credentials.assert_called_once_with(self.syn, self.user_login_args)
        cred_provider2.get_synapse_credentials.assert_called_once_with(self.syn, self.user_login_args)
        cred_provider3.get_synapse_credentials.assert_not_called()


class TestSynapseCredentialProvider(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.username = "username"
        self.password = "password"
        self.api_key = base64.b64encode(b"api_key").decode()
        self.session_token = None
        self.auth_token = 'auth_token'
        self.user_login_args = UserLoginArgs(
            self.username,
            self.password,
            self.api_key,
            False,
            self.session_token,
            self.auth_token
        )
        # SynapseApiKeyCredentialsProvider has abstractmethod so we can't instantiate it unless we overwrite it

        class SynapseCredProviderTester(SynapseCredentialsProvider):
            def _get_auth_info(self, syn, user_login_args):
                pass
        self.provider = SynapseCredProviderTester()

    def test_get_synapse_credentials(self):
        auth_info = ("username", "password", "api_key")
        with patch.object(self.provider, "_get_auth_info", return_value=auth_info) as mock_get_auth_info, \
             patch.object(self.provider, "_create_synapse_credential") as mock_create_synapse_credentials:

            self.provider.get_synapse_credentials(self.syn, self.user_login_args)

            mock_get_auth_info.assert_called_once_with(self.syn, self.user_login_args)
            mock_create_synapse_credentials.assert_called_once_with(self.syn, *auth_info)

    def test_create_synapse_credential__username_is_None(self):
        # shouldn't matter what the other fields are if username is None
        cred = self.provider._create_synapse_credential(self.syn, None, self.password, self.api_key, self.auth_token)
        assert cred is None

    def test_create_synapse_credential__username_not_None_password_not_None(self):
        """Verify that the password is used to generate credentials if provided (and takes precedence
        over api key and auth bearer token)"""
        session_token = "37842837946"
        with patch.object(self.syn, "_getSessionToken", return_value=session_token) as mock_get_session_token, \
             patch.object(self.syn, "_getAPIKey", return_value=self.api_key) as mock_get_api_key:

            # even if api key and/or auth_token is provided, password applies first
            cred = self.provider._create_synapse_credential(
                self.syn,
                self.username,
                self.password,
                self.api_key,
                self.auth_token
            )

            assert self.username == cred.username
            assert self.api_key == cred.secret
            mock_get_session_token.assert_called_with(email=self.username, password=self.password)
            mock_get_api_key.assert_called_once_with(session_token)
            assert isinstance(cred, SynapseApiKeyCredentials)

    def test_create_synapse_credential__username_not_None_api_key_not_None(self):
        """Verify that the api key is used if the password is not provided (and takes precedence over auth token)"""

        cred = self.provider._create_synapse_credential(self.syn, self.username, None, self.api_key, self.auth_token)
        assert self.username == cred.username
        assert self.api_key == cred.secret
        assert isinstance(cred, SynapseApiKeyCredentials)

    def test_create_synapse_credential__username_not_None_api_key_is_None_auth_token_is_not_None(self):
        """Verify that the auth bearer token is used if provided (and password and api key are not)"""

        cred = self.provider._create_synapse_credential(self.syn, self.username, None, None, self.auth_token)
        assert self.username == cred.username
        assert self.auth_token == cred.secret
        assert isinstance(cred, SynapseAuthTokenCredentials)


class TestUserArgsSessionTokenCredentialsProvider(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.provider = UserArgsSessionTokenCredentialsProvider()
        self.get_user_profile__patcher = patch.object(self.syn, "getUserProfile")
        self.get_api_key__patcher = patch.object(self.syn, "_getAPIKey")

        self.mock_get_user_profile = self.get_user_profile__patcher.start()
        self.mock_get_api_key = self.get_api_key__patcher.start()

    def teardown(self):
        self.get_user_profile__patcher.stop()
        self.get_api_key__patcher.stop()

    def test_get_auth_info__session_token_not_None(self):
        username = 'asdf'
        api_key = 'qwerty'
        session_token = "my session token"
        user_login_args = UserLoginArgs(session_token=session_token)
        self.mock_get_user_profile.return_value = {'userName': username}
        self.mock_get_api_key.return_value = api_key

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert (username, None, api_key, None) == returned_tuple
        self.mock_get_user_profile.assert_called_once_with(sessionToken=session_token)
        self.mock_get_api_key.assert_called_once_with(session_token)

    def test_get_auth_info__session_token_is_None(self):
        user_login_args = UserLoginArgs(session_token=None)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert (None, None, None, None) == returned_tuple
        self.mock_get_user_profile.assert_not_called()
        self.mock_get_api_key.assert_not_called()


class TestUserArgsCredentialsProvider(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test_get_auth_info(self):
        user_login_args = UserLoginArgs(
            'username',
            'password',
            base64.b64encode(b"api_key"),
            False,
            None,
            'auth_token'
        )
        provider = UserArgsCredentialsProvider()
        returned_tuple = provider._get_auth_info(self.syn, user_login_args)

        assert (
            user_login_args.username,
            user_login_args.password,
            user_login_args.api_key,
            user_login_args.auth_token
        ) == returned_tuple


class TestConfigFileCredentialsProvider(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.username = "username"
        password = "password123"
        api_key = "TWFkZSB5b3UgbG9vaw=="
        token = 'token123'
        self.expected_return_tuple = (self.username, password, api_key, token)
        self.config_dict = {"username": self.username, "password": password, "apikey": api_key, 'authToken': token}
        self.get_config_authentication__patcher = patch.object(self.syn, "_get_config_authentication",
                                                               return_value=self.config_dict)
        self.mock_get_config_authentication = self.get_config_authentication__patcher.start()

        self.provider = ConfigFileCredentialsProvider()

    def teardown(self):
        self.get_config_authentication__patcher.stop()

    def test_get_auth_info__user_arg_username_is_None(self):
        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with()

    def test_get_auth_info__user_arg_username_matches_config(self):
        user_login_args = UserLoginArgs(username=self.username, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with()

    def test_get_auth_info__user_arg_username_does_not_match_config(self):
        user_login_args = UserLoginArgs(username="shrek", password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert (None, None, None, None) == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with()


class TestCachedCredentialsProvider(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.username = "username"
        api_key = base64.b64encode(b"my api kye")
        self.api_key_b64 = base64.b64encode(api_key).decode()
        auth_token = 'auth_token'
        self.provider = CachedCredentialsProvider()

        self.expected_return_tuple = (self.username, None, api_key, auth_token)

        self.get_most_recent_user__patcher = patch.object(cached_sessions, "get_most_recent_user",
                                                          return_value=self.username)

        api_key_credentials = MagicMock(spec=SynapseApiKeyCredentials)
        api_key_credentials.secret = api_key
        self.api_key_credentials_patcher = patch.object(
            credential_provider,
            'SynapseApiKeyCredentials',
            return_value=api_key_credentials
        )
        self.mock_api_key_credentials = self.api_key_credentials_patcher.start()
        self.mock_api_key_credentials.get_from_keyring.return_value = api_key_credentials

        auth_token_credentials = MagicMock(spec=SynapseAuthTokenCredentials)
        auth_token_credentials.secret = auth_token
        self.auth_token_credentials_patcher = patch.object(
            credential_provider,
            'SynapseAuthTokenCredentials',
            return_value=auth_token_credentials
        )
        self.mock_auth_token_credentials = self.auth_token_credentials_patcher.start()
        self.mock_auth_token_credentials.get_from_keyring.return_value = auth_token_credentials

        self.mock_get_most_recent_user = self.get_most_recent_user__patcher.start()

    def teardown(self):
        self.get_most_recent_user__patcher.stop()
        self.api_key_credentials_patcher.stop()

    def test_get_auth_info__skip_cache_is_True(self):
        user_login_args = UserLoginArgs(
            username=self.username, password=None, api_key=None, skip_cache=True, session_token=None, auth_token=None)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)
        assert (None, None, None, None) == returned_tuple
        self.mock_get_most_recent_user.assert_not_called()
        self.mock_api_key_credentials.assert_not_called()
        self.mock_auth_token_credentials.assert_not_called()

    def test_get_auth_info__user_arg_username_is_None(self):
        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False, auth_token=None)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)
        assert self.expected_return_tuple == returned_tuple
        self.mock_get_most_recent_user.assert_called_once_with()
        self.mock_api_key_credentials.get_from_keyring.assert_called_once_with(self.username)
        self.mock_auth_token_credentials.get_from_keyring.assert_called_once_with(self.username)

    def test_get_auth_info__user_arg_username_is_not_None(self):
        user_login_args = UserLoginArgs(username=self.username, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_most_recent_user.assert_not_called()
        self.mock_api_key_credentials.get_from_keyring.assert_called_once_with(self.username)
        self.mock_auth_token_credentials.get_from_keyring.assert_called_once_with(self.username)
