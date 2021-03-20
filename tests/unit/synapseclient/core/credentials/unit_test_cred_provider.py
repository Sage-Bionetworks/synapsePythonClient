import base64
import os
from unittest.mock import create_autospec, MagicMock, patch

import boto3
import pytest
from botocore.stub import Stubber
from pytest_mock import MockerFixture

from synapseclient.core.credentials import credential_provider
from synapseclient.core.credentials.cred_data import (
    SynapseApiKeyCredentials,
    SynapseAuthTokenCredentials,
    UserLoginArgs,
)
from synapseclient.core.credentials.credential_provider import (
    cached_sessions,
    CachedCredentialsProvider,
    ConfigFileCredentialsProvider,
    SynapseCredentialsProvider,
    SynapseCredentialsProviderChain,
    UserArgsCredentialsProvider,
    UserArgsSessionTokenCredentialsProvider,
    AWSParameterStoreCredentialsProvider
)
from synapseclient.core.exceptions import SynapseAuthenticationError


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
        self.cred_provider.get_synapse_credentials.return_value = SynapseApiKeyCredentials(api_key, username)

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
            base64.b64encode(b"api_key").decode(),
            "asdf",
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

    def test_create_synapse_credential__username_is_None__auth_is_None(self):
        # username is required with credentials other than auth token.
        # if neither is provided it shouldn't matter what the other fields are
        cred = self.provider._create_synapse_credential(self.syn, None, self.password, self.api_key, None)
        assert cred is None

    def test_create_synapse_credential__username_is_None__auth_provided(self, mocker):
        mock_rest_get = mocker.patch.object(self.syn, 'restGET')
        mock_init_auth_creds = mocker.patch.object(credential_provider, 'SynapseAuthTokenCredentials')
        mock_creds = MagicMock(spec=SynapseAuthTokenCredentials)
        mock_init_auth_creds.return_value = mock_creds
        mock_creds.secret = self.auth_token
        mock_creds.username = self.username
        mock_rest_get.return_value = {'userName': self.username}
        creds = self.provider._create_synapse_credential(self.syn, None, self.password, self.api_key, self.auth_token)
        assert creds is mock_creds
        mock_rest_get.assert_called_once_with('/userProfile', auth=mock_creds)

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
        """Verify that the api key is used if the password is None and auth token is not provided"""

        cred = self.provider._create_synapse_credential(self.syn, self.username, None, self.api_key, None)
        assert self.username == cred.username
        assert self.api_key == cred.secret
        assert isinstance(cred, SynapseApiKeyCredentials)

    def test_create_synapse_credential__username_not_None__auth_token_is_not_None_api_key_not_None(self, mocker):
        """Verify that the auth bearer token is used if provided and takes precedence over the api key"""

        mock_rest_get = mocker.patch.object(self.syn, 'restGET')
        mock_rest_get.return_value = {'userName': self.username}
        mock_init_auth_creds = mocker.patch.object(credential_provider, 'SynapseAuthTokenCredentials')
        mock_creds = MagicMock(spec=SynapseAuthTokenCredentials)
        mock_init_auth_creds.return_value = mock_creds

        creds = self.provider._create_synapse_credential(self.syn, self.username, None, self.api_key, self.auth_token)
        assert creds is mock_creds

    def test_create_synapse_credential__username_auth_token_mismatch(self, mocker):
        """Verify that if both a username and a auth token are provided, and error is raised
        if they do not correspond"""

        mock_rest_get = mocker.patch.object(self.syn, 'restGET')
        mock_rest_get.return_value = {'userName': "otherUserName"}

        with pytest.raises(SynapseAuthenticationError) as ex:
            self.provider._create_synapse_credential(self.syn, self.username, None, self.api_key, self.auth_token)
            assert str(ex.value) == 'username and auth_token both provided but username does not match token profile'


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
        self.token = 'token123'
        self.expected_return_tuple = (self.username, password, api_key, self.token)
        self.config_dict = {"username": self.username, "password": password, "apikey": api_key, 'authtoken': self.token}
        self.get_config_authentication__patcher = patch.object(self.syn, "_get_config_authentication",
                                                               return_value=self.config_dict)
        self.mock_get_config_authentication = self.get_config_authentication__patcher.start()

        self.provider = ConfigFileCredentialsProvider()

    def teardown(self):
        self.get_config_authentication__patcher.stop()

    def test_get_auth_info__user_arg_username_is_None(self):
        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False, auth_token=None)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with()

    def test_get_auth_info__user_arg_username_matches_config(self):
        user_login_args = UserLoginArgs(
            username=self.username,
            password=None,
            api_key=None,
            skip_cache=False,
            auth_token=None,
        )

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with()

    def test_get_auth_info__user_arg_username_does_not_match_config(self):
        """Verify that if the username is provided via an arg and it doesn't
        match what's in the config then we don't read any other values from config"""
        user_login_args = UserLoginArgs(
            username="shrek",
            password=None,
            api_key=None,
            skip_cache=False,
            auth_token=None
        )

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

    @patch.object(credential_provider, 'cached_sessions')
    def test_get_auth_info__username_is_None(self, mock_cached_sessions):
        """Verify if there is no username provided and one isn't cached we return nothing
        (but also don't blow up)"""
        mock_cached_sessions.get_most_recent_user.return_value = None
        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False, auth_token=None)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)
        assert (None, None, None, None) == returned_tuple

    def test_get_auth_info__user_arg_username_is_not_None(self):
        user_login_args = UserLoginArgs(username=self.username, password=None, api_key=None, skip_cache=False)

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_most_recent_user.assert_not_called()
        self.mock_api_key_credentials.get_from_keyring.assert_called_once_with(self.username)
        self.mock_auth_token_credentials.get_from_keyring.assert_called_once_with(self.username)


class TestAWSParameterStoreCredentialsProvider(object):
    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    @pytest.fixture()
    def environ_with_param_name(self):
        return {'SYNAPSE_TOKEN_AWS_SSM_PARAMETER_NAME': '/synapse/cred/i-12134312'}

    def stub_ssm(self, mocker: MockerFixture):
        # use fake credentials otherwise boto will look for them via http calls
        ssm_client = boto3.client('ssm', aws_access_key_id='foo', aws_secret_access_key='bar', region_name='us-east-1')
        stubber = Stubber(ssm_client)
        mock_boto3_client = mocker.patch.object(boto3, 'client', return_value=ssm_client)
        return mock_boto3_client, stubber

    def setup(self):
        self.provider = AWSParameterStoreCredentialsProvider()

    def test_get_auth_info__no_environment_variable(self, mocker: MockerFixture, syn):
        empty_dict = {}
        mocker.patch.object(os, "environ", new=empty_dict)
        mock_boto3_client, stubber = self.stub_ssm(mocker)

        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False, auth_token=None)

        assert (None,) * 4 == self.provider._get_auth_info(syn, user_login_args)
        assert not mock_boto3_client.called
        stubber.assert_no_pending_responses()

    def test_get_auth_info__parameter_name_not_exist(self, mocker: MockerFixture, syn, environ_with_param_name):
        mocker.patch.object(os, 'environ', new=environ_with_param_name)

        mock_boto3_client, stubber = self.stub_ssm(mocker)
        stubber.add_client_error('get_parameter', 'ParameterNotFound')

        user_login_args = UserLoginArgs(username=None, password=None, api_key=None, skip_cache=False, auth_token=None)

        with stubber:
            assert (None,) * 4 == self.provider._get_auth_info(syn, user_login_args)
            mock_boto3_client.assert_called_once_with("ssm")
            stubber.assert_no_pending_responses()

    def test_get_auth_info__parameter_name_exists(self, mocker: MockerFixture, syn, environ_with_param_name):
        mocker.patch.object(os, 'environ', new=environ_with_param_name)

        mock_boto3_client, stubber = self.stub_ssm(mocker)
        token = 'KmhhY2tlciB2b2ljZSogIkknbSBpbiI='
        response = {
            'Parameter': {
                'Name': '/synapse/cred/i-12134312',
                'Type': 'SecureString',
                'Value': token,
                'Version': 502,
                'LastModifiedDate': 'Sun, 20 Apr 1969 16:20:00 GMT',
                'ARN':
                    'arn:aws:ssm:us-east-1:123123123:parameter/synapse/cred/i-12134312',
                'DataType': 'text'
            },
        }
        stubber.add_response('get_parameter', response)

        username = 'foobar'
        user_login_args = UserLoginArgs(username=username, password=None, api_key=None,
                                        skip_cache=False, auth_token=None)

        with stubber:
            assert (username, None, None, token) == self.provider._get_auth_info(syn, user_login_args)
            mock_boto3_client.assert_called_once_with("ssm")
            stubber.assert_no_pending_responses()
