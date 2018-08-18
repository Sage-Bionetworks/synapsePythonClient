import base64

import unit
import mock
from mock import patch
from nose.tools import assert_equals, assert_is_none, assert_is_instance
from synapseclient.credentials.credential_provider import *
from synapseclient.credentials.cred_data import UserLoginArgs, SynapseCredentials


def setup_module(module):
    module.syn = unit.syn


class TestSynapseCredentialsProviderChain(object):

    def setup(self):
        self.cred_provider = mock.create_autospec(SynapseCredentialsProvider)
        self.user_login_args = UserLoginArgs(*(None,) * 4)  # user login args don't matter for these tests
        self.credential_provider_chain = SynapseCredentialsProviderChain([self.cred_provider])

    def test_get_credentials__provider_not_return_credentials(self):
        self.cred_provider.get_synapse_credentials.return_value = None

        creds = self.credential_provider_chain.get_credentials(syn, self.user_login_args)

        assert_is_none(creds)
        self.cred_provider.get_synapse_credentials.assert_called_once_with(syn, self.user_login_args)

    def test_get_credentials__provider_return_credentials(self):
        username = "synapse_user"
        api_key = base64.b64encode(b"api_key").decode()
        self.cred_provider.get_synapse_credentials.return_value = SynapseCredentials(username, api_key)

        creds = self.credential_provider_chain.get_credentials(syn, self.user_login_args)

        assert_is_instance(creds, SynapseCredentials)
        assert_equals(username, creds.username)
        assert_equals(api_key, creds.api_key)
        self.cred_provider.get_synapse_credentials.assert_called_once_with(syn, self.user_login_args)

    def test_get_credentials__multiple_providers(self):
        cred_provider2 = mock.create_autospec(SynapseCredentialsProvider)
        cred_provider3 = mock.create_autospec(SynapseCredentialsProvider)

        self.cred_provider.get_synapse_credentials.return_value = None
        cred_provider2.get_synapse_credentials.return_value = SynapseCredentials("asdf",
                                                                                 base64.b64encode(b"api_key").decode())
        cred_provider3.get_synapse_credentials.return_value = None

        # change the credential providers
        self.credential_provider_chain.cred_providers = [self.cred_provider, cred_provider2, cred_provider3]

        creds = self.credential_provider_chain.get_credentials(syn, self.user_login_args)
        assert_is_instance(creds, SynapseCredentials)

        self.cred_provider.get_synapse_credentials.assert_called_once_with(syn, self.user_login_args)
        cred_provider2.get_synapse_credentials.assert_called_once_with(syn, self.user_login_args)
        cred_provider3.get_synapse_credentials.assert_not_called()


class TestSynapseCredentialProvider(object):

    def setup(self):
        self.username = "username"
        self.password = "password"
        self.api_key = base64.b64encode(b"api_key").decode()
        self.user_login_args = UserLoginArgs(self.username, self.password, self.api_key, False)
        # SynapseCredentialsProvider has abstractmethod so we can't instantiate it unless we overwrite it

        class SynapseCredProviderTester(SynapseCredentialsProvider):
            def _get_auth_info(self, syn, user_login_args):
                pass
        self.provider = SynapseCredProviderTester()

    def test_get_synapse_credentials(self):
        auth_info = ("username", "password", "api_key")
        with patch.object(self.provider, "_get_auth_info", return_value=auth_info) as mock_get_auth_info, \
             patch.object(self.provider, "_create_synapse_credential") as mock_create_synapse_credentials:

            self.provider.get_synapse_credentials(syn, self.user_login_args)

            mock_get_auth_info.assert_called_once_with(syn, self.user_login_args)
            mock_create_synapse_credentials.assert_called_once_with(syn, *auth_info)

    def test_create_synapse_credential__username_is_None(self):
        # shouldn't matter what the other fields are if username is None
        cred = self.provider._create_synapse_credential(syn, None, self.password, self.api_key)
        assert_is_none(cred)

    def test_create_synapse_credential__username_not_None_password_not_None(self):
        session_token = "37842837946"
        with patch.object(syn, "_getSessionToken", return_value=session_token) as mock_get_session_token, \
             patch.object(syn, "_getAPIKey", return_value=self.api_key) as mock_get_api_key:

            cred = self.provider._create_synapse_credential(syn, self.username, self.password, None)

            assert_equals(self.username, cred.username)
            assert_equals(self.api_key, cred.api_key)
            mock_get_session_token.assert_called_with(email=self.username, password=self.password)
            mock_get_api_key.assert_called_once_with(session_token)

    def test_create_synapse_credential__username_not_None_api_key_not_None(self):
        cred = self.provider._create_synapse_credential(syn, self.username, None, self.api_key)
        assert_equals(self.username, cred.username)
        assert_equals(self.api_key, cred.api_key)


class TestUserArgsSessionTokenCredentialsProvider(object):

    def setup(self):
        self.provider = UserArgsSessionTokenCredentialsProvider()
        self.get_user_profile__patcher = patch.object(syn, "getUserProfile")
        self.get_api_key__patcher = patch.object(syn, "_getAPIKey")

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

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)

        assert_equals((username, None, api_key), returned_tuple)
        self.mock_get_user_profile.assert_called_once_with(sessionToken=session_token)
        self.mock_get_api_key.assert_called_once_with(session_token)

    def test_get_auth_info__session_token_is_None(self):
        user_login_args = UserLoginArgs(session_token=None)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)

        assert_equals((None, None, None), returned_tuple)
        self.mock_get_user_profile.assert_not_called()
        self.mock_get_api_key.assert_not_called()


class TestUserArgsCredentialsProvider(object):

    def test_get_auth_info(self):
        user_login_args = UserLoginArgs("username", "password", base64.b64encode(b"api_key"), False)
        provider = UserArgsCredentialsProvider()
        returned_tuple = provider._get_auth_info(syn, user_login_args)

        assert_equals((user_login_args.username, user_login_args.password, user_login_args.api_key), returned_tuple)


class TestConfigFileCredentialsProvider(object):

    def setup(self):
        self.username = "username"
        password = "password123"
        api_key = "TWFkZSB5b3UgbG9vaw=="
        self.expected_return_tuple = (self.username, password, api_key)
        self.config_dict = {"username": self.username, "password": password, "apikey": api_key}
        self.get_config_authentication__patcher = patch.object(syn, "_get_config_authentication",
                                                               return_value=self.config_dict)
        self.mock_get_config_authentication = self.get_config_authentication__patcher.start()

        self.provider = ConfigFileCredentialsProvider()

    def tearddown(self):
        self.get_config_authentication__patcher.stop()

    def test_get_auth_info__user_arg_username_is_None(self):
        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)

        assert_equals(self.expected_return_tuple, returned_tuple)
        self.mock_get_config_authentication.assert_called_once_with()

    def test_get_auth_info__user_arg_username_matches_config(self):
        user_login_args = UserLoginArgs(username=self.username, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)

        assert_equals(self.expected_return_tuple, returned_tuple)
        self.mock_get_config_authentication.assert_called_once_with()

    def test_get_auth_info__user_arg_username_does_not_match_config(self):
        user_login_args = UserLoginArgs(username="shrek", password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)

        assert_equals((None, None, None), returned_tuple)
        self.mock_get_config_authentication.assert_called_once_with()


class TestCachedCredentialsProvider(object):

    def setup(self):
        self.username = "username"
        api_key = base64.b64encode(b"my api kye")
        self.provider = CachedCredentialsProvider()

        self.expected_return_tuple = (self.username, None, api_key)

        self.get_most_recent_user__patcher = patch.object(cached_sessions, "get_most_recent_user",
                                                          return_value=self.username)
        self.get_api_key__patcher = patch.object(cached_sessions, "get_api_key", return_value=api_key)

        self.mock_get_most_recent_user = self.get_most_recent_user__patcher.start()
        self.mock_get_api_key = self.get_api_key__patcher.start()

    def teardown(self):
        self.get_most_recent_user__patcher.stop()
        self.get_api_key__patcher.stop()

    def test_get_auth_info__skip_cache_is_True(self):
        user_login_args = UserLoginArgs(username=self.username, password=None, api_key=None, skip_cache=True)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)
        assert_equals((None, None, None), returned_tuple)
        self.mock_get_most_recent_user.assert_not_called()
        self.mock_get_api_key.assert_not_called()

    def test_get_auth_info__user_arg_username_is_None(self):
        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)
        assert_equals(self.expected_return_tuple, returned_tuple)
        self.mock_get_most_recent_user.assert_called_once_with()
        self.mock_get_api_key.assert_called_once_with(self.username)

    def test_get_auth_info__user_arg_username_is_not_None(self):
        user_login_args = UserLoginArgs(username=self.username, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(syn, user_login_args)

        assert_equals(self.expected_return_tuple, returned_tuple)
        self.mock_get_most_recent_user.assert_not_called()
        self.mock_get_api_key.assert_called_once_with(self.username)
