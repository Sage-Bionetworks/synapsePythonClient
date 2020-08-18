"""Test the Synapse command line client.

"""

import base64

import pytest
from unittest.mock import call, Mock, patch

import synapseclient.__main__ as cmdline
from synapseclient.core.exceptions import SynapseAuthenticationError, SynapseNoCredentialsError
import synapseutils


def test_command_sync(syn):
    """Test the sync function.

    Since this function only passes argparse arguments for the sync subcommand
    straight to `synapseutils.sync.syncToSynapse`, the only tests here are for
    the command line arguments provided and that the function is called once.

    """

    parser = cmdline.build_parser()
    args = parser.parse_args(['sync', '/tmp/foobarbaz.tsv'])

    assert args.manifestFile == '/tmp/foobarbaz.tsv'
    assert args.dryRun is False
    assert args.sendMessages is False
    assert args.retries == 4

    with patch.object(synapseutils, "syncToSynapse") as mockedSyncToSynapse:
        cmdline.sync(args, syn)
        mockedSyncToSynapse.assert_called_once_with(syn,
                                                    manifestFile=args.manifestFile,
                                                    dryRun=args.dryRun,
                                                    sendMessages=args.sendMessages,
                                                    retries=args.retries)


def test_get_multi_threaded_flag():
    """Test the multi threaded command line flag"""
    parser = cmdline.build_parser()
    args = parser.parse_args(['get', '--multiThreaded', 'syn123'])

    assert args.multiThreaded

    # defaults to True
    args = parser.parse_args(['get', 'syn123'])
    assert args.multiThreaded


@patch('builtins.print')
def test_get_sts_token(mock_print):
    """Test getting an STS token."""
    folder_id = 'syn_1'
    permission = 'read_write'
    syn = Mock()

    expected_output = 'export foo=bar'
    syn.get_sts_storage_token.return_value = expected_output

    parser = cmdline.build_parser()
    args = parser.parse_args(['get-sts-token', folder_id, permission, '-o', 'shell'])
    cmdline.get_sts_token(args, syn)
    syn.get_sts_storage_token.assert_called_with(folder_id, permission, output_format='shell')

    mock_print.assert_called_once_with(expected_output)


def test_authenticate_login__success(syn):
    """Verify happy path for _authenticate_login"""

    with patch.object(syn, 'login'):
        cmdline._authenticate_login(syn, 'foo', 'bar', rememberMe=True, silent=True)
        syn.login.assert_called_once_with('foo', 'bar', rememberMe=True, silent=True)


def test_authenticate_login__api_key(syn):
    """Verify attempting to authenticate when supplying an api key as the password.
    Should attempt to treat the password as an api key after the initial failure as a password."""

    username = 'foo'
    password = base64.b64encode(b'bar').decode('utf-8')
    login_kwargs = {'rememberMe': True}

    expected_login_calls = [
        call(username, password, **login_kwargs),
        call(username, apiKey=password, **login_kwargs)
    ]

    with patch.object(syn, 'login') as login:
        login.side_effect = SynapseAuthenticationError()

        # simulate failure both as password and as api key
        with pytest.raises(SynapseAuthenticationError):
            cmdline._authenticate_login(syn, username, password, **login_kwargs)

        assert expected_login_calls == login.call_args_list
        login.reset_mock()

        # now simulate success when used as an api key
        def login_side_effect(*args, **kwargs):
            if login.call_count == 1:
                raise SynapseAuthenticationError()
            return

        login.side_effect = login_side_effect

        cmdline._authenticate_login(syn, username, password, **login_kwargs)
        assert expected_login_calls == login.call_args_list


@patch.object(cmdline, '_authenticate_login')
def test_login_with_prompt(mock_authenticate_login, syn):
    """Verify logging in when username/pass supplied as args to the command"""

    user = 'foo'
    password = 'bar'
    login_kwargs = {
        'rememberMe': False,
        'silent': True,
        'forced': True,
    }

    cmdline.login_with_prompt(syn, user, password, **login_kwargs)
    mock_authenticate_login.assert_called_once_with(syn, user, password, **login_kwargs)


@patch.object(cmdline, 'getpass')
@patch.object(cmdline, 'input')
@patch.object(cmdline, '_authenticate_login')
def test_login_with_prompt__getpass(mock_authenticate_login, mock_input, mock_getpass, syn):
    """Verify logging in when entering username/pass from the console."""

    user = 'foo'
    password = 'bar'
    login_kwargs = {
        'rememberMe': False,
        'silent': True,
        'forced': True,
    }

    def authenticate_side_effect(*args, **kwargs):
        if mock_authenticate_login.call_count == 1:
            raise SynapseNoCredentialsError()
        return

    mock_authenticate_login.side_effect = authenticate_side_effect
    mock_input.return_value = user
    mock_getpass.getpass.return_value = password

    cmdline.login_with_prompt(syn, None, None, **login_kwargs)

    mock_input.assert_called_once_with("Synapse username: ")
    mock_getpass.getpass.assert_called_once_with(("Password or api key for " + user + ": ").encode('utf-8'))

    expected_authenticate_calls = [
        call(syn, None, None, **login_kwargs),
        call(syn, user, password, **{k: v for k, v in login_kwargs.items() if k != 'silent'})
    ]

    assert expected_authenticate_calls == mock_authenticate_login.call_args_list
