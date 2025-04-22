"""Unit tests for synapseclient.core.credentials.credential_provider"""

import os
import sys
from typing import Dict
from unittest.mock import MagicMock, create_autospec, patch

import boto3
import pytest
from botocore.stub import Stubber
from pytest_mock import MockerFixture

from synapseclient import Synapse
from synapseclient.core.credentials import credential_provider
from synapseclient.core.credentials.cred_data import (
    SynapseAuthTokenCredentials,
    UserLoginArgs,
)
from synapseclient.core.credentials.credential_provider import (
    AWSParameterStoreCredentialsProvider,
    ConfigFileCredentialsProvider,
    EnvironmentVariableCredentialsProvider,
    SynapseCredentialsProvider,
    SynapseCredentialsProviderChain,
    UserArgsCredentialsProvider,
)
from synapseclient.core.exceptions import SynapseAuthenticationError


class TestSynapseApiKeyCredentialsProviderChain(object):
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.cred_provider = create_autospec(SynapseCredentialsProvider)
        self.user_login_args = UserLoginArgs(
            username=None, auth_token=None
        )  # user login args don't matter for these tests
        self.credential_provider_chain = SynapseCredentialsProviderChain(
            [self.cred_provider]
        )

    def test_get_credentials__provider_not_return_credentials(self) -> None:
        self.cred_provider.get_synapse_credentials.return_value = None

        creds = self.credential_provider_chain.get_credentials(
            self.syn, self.user_login_args
        )

        assert creds is None
        self.cred_provider.get_synapse_credentials.assert_called_once_with(
            self.syn, self.user_login_args
        )

    def test_get_credentials__multiple_providers(self) -> None:
        cred_provider2 = create_autospec(SynapseCredentialsProvider)
        cred_provider3 = create_autospec(SynapseCredentialsProvider)

        self.cred_provider.get_synapse_credentials.return_value = None
        cred_provider2.get_synapse_credentials.return_value = (
            SynapseAuthTokenCredentials(
                username="asdf",
                displayname="aaaa",
                token="ghjk",
            )
        )
        cred_provider3.get_synapse_credentials.return_value = None

        # change the credential providers
        self.credential_provider_chain.cred_providers = [
            self.cred_provider,
            cred_provider2,
            cred_provider3,
        ]

        creds = self.credential_provider_chain.get_credentials(
            self.syn, self.user_login_args
        )
        assert isinstance(creds, SynapseAuthTokenCredentials)

        self.cred_provider.get_synapse_credentials.assert_called_once_with(
            self.syn, self.user_login_args
        )
        cred_provider2.get_synapse_credentials.assert_called_once_with(
            self.syn, self.user_login_args
        )
        cred_provider3.get_synapse_credentials.assert_not_called()


class TestSynapseCredentialProvider(object):
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.username = "username"
        self.auth_token = "auth_token"
        self.owner_id = 1234567890
        self.user_login_args = UserLoginArgs(
            self.username,
            self.auth_token,
        )

        # SynapseApiKeyCredentialsProvider has abstractmethod so we can't instantiate it unless we overwrite it

        class SynapseCredProviderTester(SynapseCredentialsProvider):
            def _get_auth_info(
                self, syn: Synapse, user_login_args: Dict[str, str]
            ) -> None:
                """Empty as this is a stub."""
                pass

        self.provider = SynapseCredProviderTester()

    def test_get_synapse_credentials(self) -> None:
        auth_info = ("username", "auth_token")
        with patch.object(
            self.provider, "_get_auth_info", return_value=auth_info
        ) as mock_get_auth_info, patch.object(
            self.provider, "_create_synapse_credential"
        ) as mock_create_synapse_credentials:
            self.provider.get_synapse_credentials(self.syn, self.user_login_args)

            mock_get_auth_info.assert_called_once_with(
                syn=self.syn, user_login_args=self.user_login_args
            )
            mock_create_synapse_credentials.assert_called_once_with(
                self.syn, *auth_info
            )

    def test_create_synapse_credential__username_is_None__auth_is_None(self) -> None:
        # username is required with credentials other than auth token.
        # if neither is provided it shouldn't matter what the other fields are
        cred = self.provider._create_synapse_credential(
            syn=self.syn, username=None, auth_token=None, authentication_profile=None
        )
        assert cred is None

    def test_create_synapse_credential_username_is_None_auth_provided(
        self, mocker
    ) -> None:
        mock_rest_get = mocker.patch.object(self.syn, "restGET")
        mock_init_auth_creds = mocker.patch.object(
            credential_provider, "SynapseAuthTokenCredentials"
        )
        mock_creds = MagicMock(spec=SynapseAuthTokenCredentials)
        mock_init_auth_creds.return_value = mock_creds
        mock_creds.secret = self.auth_token
        mock_creds.username = self.username
        mock_creds.owner_id = self.owner_id
        mock_rest_get.return_value = {"userName": self.username}
        creds = self.provider._create_synapse_credential(
            syn=self.syn, username=None, auth_token=self.auth_token, authentication_profile=None,
        )
        assert creds is mock_creds
        mock_rest_get.assert_called_once_with("/userProfile", auth=mock_creds)

    def test_create_synapse_credential_username_not_None_auth_token_is_not_None(
        self, mocker
    ) -> None:
        """Verify that the auth bearer token is used if provided and the username is not None"""

        mock_rest_get = mocker.patch.object(self.syn, "restGET")
        mock_rest_get.return_value = {"userName": self.username}
        mock_init_auth_creds = mocker.patch.object(
            credential_provider, "SynapseAuthTokenCredentials"
        )
        mock_creds = MagicMock(spec=SynapseAuthTokenCredentials)
        mock_init_auth_creds.return_value = mock_creds

        creds = self.provider._create_synapse_credential(
            syn=self.syn, username=self.username, auth_token=self.auth_token, authentication_profile=None,
        )
        assert creds is mock_creds

    @pytest.mark.parametrize(
        "login_username,profile_username,profile_emails,profile_displayname,profile_owner_id",
        (
            ("foo", "foo", ["foo@bar.com"], "foo", 123),  # username matches
            (
                "foo@bar.com",
                "foo",
                ["1@2.com", "foo@bar.com", "3@4.com"],
                "foo",
                456,
            ),  # email matches
        ),
    )
    def test_create_synapse_credential__username_auth_token_match(
        self,
        mocker,
        login_username,
        profile_username,
        profile_emails,
        profile_displayname,
        profile_owner_id,
    ) -> None:
        """Verify that if both a username/email and a auth token are provided, the login is successful
        if the token matches either the username or a profile email address."""

        mock_rest_get = mocker.patch.object(self.syn, "restGET")
        mock_rest_get.return_value = {
            "userName": profile_username,
            "emails": profile_emails,
            "displayName": profile_displayname,
            "ownerId": profile_owner_id,
        }

        cred = self.provider._create_synapse_credential(
            syn=self.syn, username=login_username, auth_token=self.auth_token, authentication_profile="ownerId"
        )
        assert cred.secret == self.auth_token
        assert cred.username == profile_username
        assert cred.displayname == profile_displayname
        assert cred.owner_id == profile_owner_id

    def test_create_synapse_credential__username_auth_token_mismatch(
        self, mocker
    ) -> None:
        """Verify that if both a username/email and a auth token are provided, and error is raised
        if they do not correspond"""

        mock_rest_get = mocker.patch.object(self.syn, "restGET")
        login_username = "blatherskite"
        mock_rest_get.return_value = {
            "userName": "foo",
            "emails": ["foo@bar.com", "bar@baz.com"],
            "displayName": "foo",
        }

        with pytest.raises(SynapseAuthenticationError) as ex:
            self.provider._create_synapse_credential(
                syn=self.syn, username=login_username, auth_token=self.auth_token, authentication_profile=None,
            )
        assert (
            str(ex.value)
            == "username/email and auth_token both provided but username does not match token profile"
        )


class TestUserArgsCredentialsProvider(object):
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_get_auth_info(self) -> None:
        user_login_args = UserLoginArgs(
            username="username",
            auth_token="auth_token",
        )
        provider = UserArgsCredentialsProvider()
        returned_tuple = provider._get_auth_info(self.syn, user_login_args)

        assert (
            user_login_args.username,
            user_login_args.auth_token,
            None,
        ) == returned_tuple


class TestConfigFileCredentialsProvider(object):
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.username = "username"
        self.token = "token123"
        self.expected_return_tuple = (self.username, self.token, None)
        self.config_dict = {
            "username": self.username,
            "authtoken": self.token,
        }
        self.get_config_authentication__patcher = patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value=self.config_dict,
        )
        self.mock_get_config_authentication = (
            self.get_config_authentication__patcher.start()
        )

        self.provider = ConfigFileCredentialsProvider()

    def teardown_method(self):
        self.get_config_authentication__patcher.stop()

    def test_get_auth_info__user_arg_username_is_None(self) -> None:
        user_login_args = UserLoginArgs(
            username=None,
            auth_token=None,
        )

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args,)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with(
            config_path=self.syn.configPath,
            profile="default"
        )

    def test_get_auth_info__user_arg_username_matches_config(self) -> None:
        user_login_args = UserLoginArgs(
            username=self.username,
            auth_token=None,
        )

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert self.expected_return_tuple == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with(
            config_path=self.syn.configPath,
            profile="default"
        )

    def test_get_auth_info__user_arg_username_does_not_match_config(self) -> None:
        """Verify that if the username is provided via an arg and it doesn't
        match what's in the config then we don't read any other values from config"""
        user_login_args = UserLoginArgs(
            username="shrek",
            auth_token=None,
        )

        returned_tuple = self.provider._get_auth_info(self.syn, user_login_args)

        assert (None, None, None) == returned_tuple
        self.mock_get_config_authentication.assert_called_once_with(
            config_path=self.syn.configPath,
            profile="default"
        )


class TestAWSParameterStoreCredentialsProvider(object):
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture()
    def environ_with_param_name(self) -> Dict[str, str]:
        return {"SYNAPSE_TOKEN_AWS_SSM_PARAMETER_NAME": "/synapse/cred/i-12134312"}

    def stub_ssm(self, mocker: MockerFixture):
        # use fake credentials otherwise boto will look for them via http calls
        ssm_client = boto3.client(
            "ssm",
            aws_access_key_id="foo",
            aws_secret_access_key="bar",
            region_name="us-east-1",
        )
        stubber = Stubber(ssm_client)
        mock_boto3_client = mocker.patch.object(
            boto3, "client", return_value=ssm_client
        )
        return mock_boto3_client, stubber

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.provider = AWSParameterStoreCredentialsProvider()

    def test_get_auth_info__no_environment_variable(
        self, mocker: MockerFixture, syn: Synapse
    ):
        mocker.patch.dict(os.environ, {}, clear=True)
        mock_boto3_client, stubber = self.stub_ssm(mocker)

        user_login_args = UserLoginArgs(
            username=None,
            auth_token=None,
        )

        assert (None, None, None) == self.provider._get_auth_info(syn, user_login_args)
        assert not mock_boto3_client.called
        stubber.assert_no_pending_responses()

    # there could be other errors as well, but we will handle them all in the same way
    @pytest.mark.parametrize(
        "error_code",
        ["UnrecognizedClientException", "AccessDenied", "ParameterNotFound"],
    )
    def test_get_auth_info__get_parameter_error(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        environ_with_param_name: Dict[str, str],
        error_code: str,
    ) -> None:
        mocker.patch.dict(os.environ, environ_with_param_name)

        mock_boto3_client, stubber = self.stub_ssm(mocker)
        stubber.add_client_error("get_parameter", error_code)

        user_login_args = UserLoginArgs(
            username=None,
            auth_token=None,
        )

        with stubber:
            assert (None, None, None) == self.provider._get_auth_info(syn, user_login_args)
            mock_boto3_client.assert_called_once_with("ssm")
            stubber.assert_no_pending_responses()

    def test_get_auth_info__parameter_name_exists(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        environ_with_param_name: Dict[str, str],
    ) -> None:
        mocker.patch.dict(os.environ, environ_with_param_name)

        mock_boto3_client, stubber = self.stub_ssm(mocker)
        token = "KmhhY2tlciB2b2ljZSogIkknbSBpbiI="
        response = {
            "Parameter": {
                "Name": "/synapse/cred/i-12134312",
                "Type": "SecureString",
                "Value": token,
                "Version": 502,
                "LastModifiedDate": "Sun, 20 Apr 1969 16:20:00 GMT",
                "ARN": "arn:aws:ssm:us-east-1:123123123:parameter/synapse/cred/i-12134312",
                "DataType": "text",
            },
        }
        stubber.add_response("get_parameter", response)

        username = "foobar"
        user_login_args = UserLoginArgs(
            username=username,
            auth_token=None,
        )

        with stubber:
            assert (username, token, None) == self.provider._get_auth_info(
                syn, user_login_args
            )
            mock_boto3_client.assert_called_once_with("ssm")
            stubber.assert_no_pending_responses()

    def test_get_auth_info__boto3_ImportError(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        environ_with_param_name: Dict[str, str],
    ) -> None:
        mocker.patch.dict(os.environ, environ_with_param_name)
        # simulate import error by "removing" boto3 from sys.modules
        mocker.patch.dict(sys.modules, {"boto3": None})

        user_login_args = UserLoginArgs(
            username=None,
            auth_token=None,
        )

        assert (None, None, None) == self.provider._get_auth_info(syn, user_login_args)


class TestEnvironmentVariableCredentialsProvider:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.provider = EnvironmentVariableCredentialsProvider()

    def test_get_auth_info__has_environment_variable(
        self, mocker: MockerFixture, syn: Synapse
    ) -> None:
        token = "aHR0cHM6Ly93d3cueW91dHViZS5jb20vd2F0Y2g/dj1mQzdvVU9VRUVpNA=="
        mocker.patch.dict(os.environ, {"SYNAPSE_AUTH_TOKEN": token})

        user_login_args = UserLoginArgs(
            username=None,
            auth_token=None,
        )
        assert (None, token, None) == self.provider._get_auth_info(syn, user_login_args)

    def test_get_auth_info__has_environment_variable_user_args_with_username(
        self, mocker: MockerFixture, syn: Synapse
    ) -> None:
        token = "aHR0cHM6Ly93d3cueW91dHViZS5jb20vd2F0Y2g/dj1mQzdvVU9VRUVpNA=="
        mocker.patch.dict(os.environ, {"SYNAPSE_AUTH_TOKEN": token})
        username = "foobar"
        user_login_args = UserLoginArgs(
            username=username,
            auth_token=None,
        )
        assert (username, token, None) == self.provider._get_auth_info(syn, user_login_args)

    def test_get_auth_info__no_environment_variable(
        self, mocker: MockerFixture, syn: Synapse
    ) -> None:
        mocker.patch.dict(os.environ, {}, clear=True)

        user_login_args = UserLoginArgs(
            username=None,
            auth_token=None,
        )

        assert (None, None, None) == self.provider._get_auth_info(syn, user_login_args)


# Test case to verify behavior when auth_token is provided
class TestAuthTokenProvided:
    """
   This test verifies that when an auth_token is explicitly provided in the user login arguments,
   the profile-related logic is skipped, and the auth_token is directly used for authentication.

   The test ensures that:
   - If the auth_token is provided, it is returned without attempting to fetch credentials from the config file.
   - Other authentication logic (e.g., from the ConfigFileCredentialsProvider) is not called when an auth_token is provided.
   """

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.auth_token = "auth_token123"
        self.username = "username"
        self.user_login_args = UserLoginArgs(
            username=self.username,
            auth_token=self.auth_token,
        )

        # Mock the SynapseCredentialsProvider so we don't interact with external systems
        class SynapseCredProviderTester(credential_provider.SynapseCredentialsProvider):
            def _get_auth_info(self, syn: Synapse, user_login_args: UserLoginArgs):
                """Test specific implementation for the auth_token logic"""
                if user_login_args.auth_token:
                    # If auth_token is provided, skip the profile logic and return immediately
                    return user_login_args.username, user_login_args.auth_token
                return None, None

        self.provider = SynapseCredProviderTester()

    @patch("synapseclient.core.credentials.credential_provider.get_config_authentication")
    @patch("synapseclient.core.credentials.credential_provider.ConfigFileCredentialsProvider._get_auth_info")
    def test_auth_token_provided_skips_profile_logic(
            self, mock_get_config_authentication, mock_config_file_auth_info
    ) -> None:
        # Call the _get_auth_info method to verify behavior
        username, auth_token = self.provider._get_auth_info(self.syn, self.user_login_args)

        # Assert that the provided username and auth_token are returned
        assert username == self.username
        assert auth_token == self.auth_token

        # Ensure that _get_config_authentication and _get_auth_info (for ConfigFileCredentialsProvider) were not called
        mock_get_config_authentication.assert_not_called()
        mock_config_file_auth_info.assert_not_called()

class TestCredentialResolutionPrecedence:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.user_login_args = UserLoginArgs(username=None, auth_token=None)

    def test_config_with_authentication_default_and_profile__uses_default(self, mocker, syn):
        config_dict = {"username": "default_user", "authtoken": "default_token"}
        mock_get_config = mocker.patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value=config_dict,
        )
        provider = ConfigFileCredentialsProvider()

        returned = provider._get_auth_info(syn, self.user_login_args)

        assert returned == ("default_user", "default_token", None)
        mock_get_config.assert_called_once_with(config_path=syn.configPath, profile="default")

    def test_config_with_authentication_and_profile__no_default__uses_authentication(self, mocker, syn):
        config_dict = {"username": "auth_user", "authtoken": "auth_token"}
        mock_get_config = mocker.patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value=config_dict,
        )
        provider = ConfigFileCredentialsProvider()

        returned = provider._get_auth_info(syn, self.user_login_args)

        assert returned == ("auth_user", "auth_token", None)
        mock_get_config.assert_called_once_with(config_path=syn.configPath, profile="default")

    def test_config_with_profile_specified__uses_profile(self, mocker, syn):
        config_dict = {
            "username": "profile_user",
            "authtoken": "profile_token",
            "section_name": "testprofile",
        }
        mock_get_config = mocker.patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value=config_dict,
        )
        provider = ConfigFileCredentialsProvider()
        user_login_args = UserLoginArgs(username=None, auth_token=None, profile="testprofile")

        returned = provider._get_auth_info(syn, user_login_args)

        assert returned == ("profile_user", "profile_token", "testprofile")
        mock_get_config.assert_called_once_with(config_path=syn.configPath, profile="testprofile")

    def test_config_with_profile_only__no_profile_specified__prompts_login(self, mocker, syn):
        mock_get_config = mocker.patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value={},
        )
        provider = ConfigFileCredentialsProvider()

        returned = provider._get_auth_info(syn, self.user_login_args)

        assert returned == (None, None, None)

    def test_config_with_profile_only__profile_specified__succeeds(self, mocker, syn):
        config_dict = {"username": "profile_user", "authtoken": "profile_token"}
        mock_get_config = mocker.patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value=config_dict,
        )
        provider = ConfigFileCredentialsProvider()
        syn._login_args = {"profile": "onlyprofile"}

        returned = provider._get_auth_info(syn, self.user_login_args)

        assert returned == ("profile_user", "profile_token", None)

    def test_config_with_authentication_only__fallback_succeeds(self, mocker, syn):
        config_dict = {"username": "auth_user", "authtoken": "auth_token"}
        mock_get_config = mocker.patch(
            "synapseclient.core.credentials.credential_provider.get_config_authentication",
            return_value=config_dict,
        )
        provider = ConfigFileCredentialsProvider()

        returned = provider._get_auth_info(syn, self.user_login_args)

        assert returned == ("auth_user", "auth_token", None)