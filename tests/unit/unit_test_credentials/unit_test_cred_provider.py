import base64

import unit
import mock
import synapseclient
from mock import patch, MagicMock
from nose.tools import assert_equals, assert_is_none, assert_is_instance
from synapseclient.credentials.credential_provider import *
from synapseclient.credentials.cred_data import UserLoginArgs, SynapseCredentials

def setup_module(module):
    module.syn = unit.syn


class TestSynapseCredentialsProviderChain(object):
    def setup(self):
        self.cred_provider = mock.create_autospec(SynapseCredentialsProvider)
        self.user_login_args = UserLoginArgs(*(None,) * 5) #user login args don't matter for these tests
        self.credential_provider_chain = SynapseCredentialsProviderChain([self.cred_provider])


    def test_get_credentials__provider_not_return_credentials(self):
        self.cred_provider.get_username_and_api_key.return_value = (None, None)

        creds = self.credential_provider_chain.get_credentials(syn, self.user_login_args)

        assert_is_none(creds)
        self.cred_provider.get_username_and_api_key.assert_called_once_with(syn, self.user_login_args)


    def test_get_credentials__provider_return_credentials(self):
        username = "synapse_user"
        api_key = base64.b64encode(b"api_key").decode()
        self.cred_provider.get_username_and_api_key.return_value = (username, api_key)

        creds = self.credential_provider_chain.get_credentials(syn, self.user_login_args)

        assert_is_instance(creds, SynapseCredentials)
        assert_equals(username, creds.username)
        assert_equals(api_key, creds.api_key)
        self.cred_provider.get_username_and_api_key.assert_called_once_with(syn, self.user_login_args)

    def test_get_credentials__provider_username_not_match_user_arg_username(self):
        username = "synapse_user"
        api_key = base64.b64encode(b"api_key")
        self.cred_provider.get_username_and_api_key.return_value = (username, api_key)
        user_args = UserLoginArgs("non_match_username", *(None,) * 4)

        creds = self.credential_provider_chain.get_credentials(syn, user_args)
        assert_is_none(creds)
        self.cred_provider.get_username_and_api_key.assert_called_once_with(syn, user_args)


    def test_get_credentials__multiple_providers(self):
        cred_provider2 = mock.create_autospec(SynapseCredentialsProvider)
        cred_provider3 = mock.create_autospec(SynapseCredentialsProvider)

        self.cred_provider.get_username_and_api_key.return_value = (None, None)
        cred_provider2.get_username_and_api_key.return_value = ("asdf", base64.b64encode(b"api_key").decode())
        cred_provider3.get_username_and_api_key.return_value = (None, None)

        #change the credential providers
        self.credential_provider_chain.cred_providers = [self.cred_provider, cred_provider2, cred_provider3]

        creds = self.credential_provider_chain.get_credentials(syn, self.user_login_args)
        assert_is_instance(creds, SynapseCredentials)

        self.cred_provider.get_username_and_api_key.assert_called_once_with(syn, self.user_login_args)
        cred_provider2.get_username_and_api_key.assert_called_once_with(syn, self.user_login_args)
        cred_provider3.get_username_and_api_key.assert_not_called()


class TestCredentialProviders(object):
    """
    Common setup/teardown for test involving config file
    """
    def setup(self):
        self.user_login_args = UserLoginArgs("username","password", base64.b64encode(b"api_key"),"session_token", False)
        self.config_auth_dict = {"username": "username",
                                 "password": "password",
                                 "apikey": "api_key",
                                 "sessiontoken": "session_token"}

        self.get_config_authentication_patcher = patch.object(syn,"_get_config_authenticaton", return_value=self.config_auth_dict)
        self.get_login_credentials_patcher = patch.object(syn,"_get_login_credentials")
        self.cached_sessions_patcher = patch.object(synapseclient.credentials.credential_provider, "cached_sessions")

        self.mock_get_config_authentication = self.get_config_authentication_patcher.start()
        self.mock_get_login_credentials = self.get_login_credentials_patcher.start()
        self.mock_cached_session = self.cached_sessions_patcher.start()



    def teardown(self):
        self.get_config_authentication_patcher.stop()
        self.get_login_credentials_patcher.stop()
        self.cached_sessions_patcher.stop()

    def test_UserArgsUsernamePasswordCredentialsProvider(self):
        provider = UserArgsUsernamePasswordCredentialsProvider()
        provider.get_username_and_api_key(syn, self.user_login_args)
        self.mock_get_login_credentials.assert_called_once_with(username=self.user_login_args.username, password=self.user_login_args.password)

    def test_UserArgsUsernameAPICredentialsProvider(self):
        provider = UserArgsUsernameAPICredentialsProvider()
        result = provider.get_username_and_api_key(syn, self.user_login_args)
        assert_equals((self.user_login_args.username, self.user_login_args.api_key), result)

    def test_UserArgsSessionTokenCredentialsProvider(self):
        provider = UserArgsSessionTokenCredentialsProvider()
        provider.get_username_and_api_key(syn, self.user_login_args)
        self.mock_get_login_credentials.assert_called_once_with(sessiontoken=self.user_login_args.session_token)

    def test_ConfigUsernamePasswordCredentialsProvider(self):
        provider = ConfigUsernamePasswordCredentialsProvider()
        provider.get_username_and_api_key(syn, self.user_login_args)
        self.mock_get_login_credentials.assert_called_once_with(username=self.config_auth_dict['username'],
                                                                password=self.config_auth_dict['password'])

    def test_ConfigUsernameAPICredentialsProvider(self):
        provider = ConfigUsernameAPICredentialsProvider()
        result = provider.get_username_and_api_key(syn, self.user_login_args)
        assert_equals((self.config_auth_dict['username'], self.config_auth_dict['apikey']), result)

    def test_ConfigSessionTokenCredentialsProvider(self):
        provider = ConfigSessionTokenCredentialsProvider()
        provider.get_username_and_api_key(syn, self.user_login_args)
        self.mock_get_login_credentials.assert_called_once_with(sessiontoken=self.config_auth_dict['sessiontoken'])

    def test_CachedUserNameCredentialsProvider(self):
        provider = CachedUserNameCredentialsProvider()
        provider.get_username_and_api_key(syn, self.user_login_args)
        self.mock_cached_session.get_api_key.assert_called_once_with(self.user_login_args.username)

    def test_CachedRecentlyUsedUsernameCredentialsProvider(self):
        provider = CachedRecentlyUsedUsernameCredentialsProvider()
        most_recent_user = "shrek"
        self.mock_cached_session.get_most_recent_user.return_value = most_recent_user

        provider.get_username_and_api_key(syn, self.user_login_args)

        self.mock_cached_session.get_most_recent_user.assert_called_once_with()
        self.mock_cached_session.get_api_key.assert_called_once_with(most_recent_user)
