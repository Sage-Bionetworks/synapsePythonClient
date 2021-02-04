"""Test the Synapse command line client.

"""

import base64
import os

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


def test_migrate(syn):
    """Test that the command line arguments are successfully passed to the migrate function."""

    entity_id = 'syn12345'
    storage_location_id = '98766'
    db_path = '/tmp/foo/bar'

    parser = cmdline.build_parser()

    # test w/ default optional args
    args = parser.parse_args([
        'migrate',
        'syn12345',
        storage_location_id,
        db_path,
    ])

    assert args.id == entity_id
    assert args.storage_location_id == storage_location_id
    assert args.db_path == db_path
    assert args.file_version_strategy == 'new'
    assert args.include_table_files is False
    assert args.continue_on_error is False
    assert args.dryRun is False
    assert args.force is False
    assert args.csv_log_path is None

    # test w/ fully specified args
    args = parser.parse_args([
        'migrate',
        entity_id,
        storage_location_id,
        db_path,
        '--file_version_strategy', 'all',
        '--dryRun',
        '--include_table_files',
        '--continue_on_error',
        '--force',
        '--csv_log_path', '/tmp/foo/bar'
    ])

    assert args.id == entity_id
    assert args.storage_location_id == storage_location_id
    assert args.db_path == db_path
    assert args.file_version_strategy == 'all'
    assert args.include_table_files is True
    assert args.continue_on_error is True
    assert args.dryRun is True
    assert args.force is True
    assert args.csv_log_path == '/tmp/foo/bar'

    # verify args are passed through to the fn
    with patch.object(synapseutils, 'index_files_for_migration') as mock_index, \
            patch.object(synapseutils, 'migrate_indexed_files') as mock_migrate:

        cmdline.migrate(args, syn)

        mock_index.assert_called_once_with(
            syn,
            args.id,
            args.storage_location_id,
            args.db_path,
            file_version_strategy='all',
            include_table_files=True,
            continue_on_error=True,
        )

        # during a dryRun the actual migration should not occur
        assert mock_migrate.called is False

        # without dryRun then migrate should also be called
        args.dryRun = False
        cmdline.migrate(args, syn)

        mock_migrate.assert_called_once_with(
            syn,
            args.db_path,
            create_table_snapshots=True,
            continue_on_error=True,
            force=True
        )


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

    # mock_print.assert_called_once_with(expected_output)


def test_authenticate_login__success(syn):
    """Verify happy path for _authenticate_login"""

    with patch.object(syn, 'login'):
        cmdline._authenticate_login(syn, 'foo', 'bar', rememberMe=True, silent=True)
        syn.login.assert_called_once_with('foo', password='bar', rememberMe=True, silent=True)


def test_authenticate_login__api_key(syn):
    """Verify attempting to authenticate when supplying an api key as the password.
    Should attempt to treat the password as an api key after the initial failure as a password."""

    username = 'foo'
    password = base64.b64encode(b'bar').decode('utf-8')
    login_kwargs = {'rememberMe': True}

    expected_login_calls = [
        call(username, password=password, **login_kwargs),
        call(username, authToken=password, **login_kwargs),
        call(username, apiKey=password, **login_kwargs),
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
            api_key = kwargs.get('apiKey')
            if not api_key:
                raise SynapseAuthenticationError()

        login.side_effect = login_side_effect

        cmdline._authenticate_login(syn, username, password, **login_kwargs)
        assert expected_login_calls == login.call_args_list


def test_authenticate_login__auth_token(syn):
    """Verify attempting to authenticate when supplying an auth bearer token instead of an password (or api key).
    Should attempt to treat the password as an api key after the initial failure as a password and api key"""

    username = 'foo'
    auth_token = 'auth_bearer_token'
    login_kwargs = {'rememberMe': True}

    expected_login_calls = [
        call(username, password=auth_token, **login_kwargs),
        call(username, authToken=auth_token, **login_kwargs),
    ]

    with patch.object(syn, 'login') as login:
        login.side_effect = SynapseAuthenticationError()

        # simulate failure both as password and as auth token.
        # token is not a base 64 encoded string so we don't expect it to be
        # tried as an api key
        with pytest.raises(SynapseAuthenticationError):
            cmdline._authenticate_login(syn, username, auth_token, **login_kwargs)

        assert expected_login_calls == login.call_args_list
        login.reset_mock()

        def login_side_effect(*args, **kwargs):
            # simulate a failure when called with other than auth token
            passed_auth_token = kwargs.get('authToken')
            if not passed_auth_token:
                raise SynapseAuthenticationError()

        login.side_effect = login_side_effect

        cmdline._authenticate_login(syn, username, auth_token, **login_kwargs)
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


@patch.object(cmdline, 'sys')
@patch.object(cmdline, 'getpass')
@patch.object(cmdline, 'input')
@patch.object(cmdline, '_authenticate_login')
def test_login_with_prompt__getpass(mock_authenticate_login, mock_input, mock_getpass, mock_sys, syn):
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

    mock_sys.stdin.isatty.return_value = True

    mock_authenticate_login.side_effect = authenticate_side_effect
    mock_input.return_value = user
    mock_getpass.getpass.return_value = password

    cmdline.login_with_prompt(syn, None, None, **login_kwargs)

    mock_input.assert_called_once_with("Synapse username: ")
    mock_getpass.getpass.assert_called_once_with(("Password or api key for " + user + ": "))

    expected_authenticate_calls = [
        call(syn, None, None, **login_kwargs),
        call(syn, user, password, **{k: v for k, v in login_kwargs.items() if k != 'silent'})
    ]

    assert expected_authenticate_calls == mock_authenticate_login.call_args_list


def test_syn_commandline_silent_mode():
    """
    Test the silent argument from commandline
    """

    parser = cmdline.build_parser()
    args = parser.parse_args([])
    assert args.silent is False

    parser = cmdline.build_parser()
    args = parser.parse_args(['--silent'])
    assert args.silent is True


@patch("synapseclient.Synapse")
def test_commandline_main(mock_syn):
    """
    Test the main method
    """

    configPath = os.path.join(os.path.expanduser('~'), '.synapseConfig')
    args = cmdline.build_parser().parse_args(['-u', 'testUser', '--silent'])

    with patch.object(cmdline, 'build_parser') as mock_build_parser:
        mock_build_parser.return_value.parse_args.return_value = args
        cmdline.main()
        mock_syn.assert_called_once_with(debug=False, skip_checks=False,
                                         configPath=configPath, silent=True)


@patch.object(cmdline, 'sys')
@patch.object(cmdline, 'input')
@patch.object(cmdline, '_authenticate_login')
def test_login_with_prompt_no_tty(mock_authenticate_login, mock_input, mock_sys, syn):
    """
    Verify login_with_prompt when the terminal is not a tty,
    we are unable to read from standard input and throw a SynapseAuthenticationError
    """

    user = 'test_user'
    login_kwargs = {
        'rememberMe': False,
        'silent': True,
        'forced': True,
    }

    mock_authenticate_login.side_effect = SynapseNoCredentialsError()
    mock_sys.stdin.isatty.return_value = False
    mock_input.return_value = user
    with pytest.raises(SynapseAuthenticationError):
        cmdline.login_with_prompt(syn, None, None, **login_kwargs)


@patch.object(cmdline, 'build_parser')
def test_no_command_print_help(mock_build_parser, syn):
    """
    Verify command without any function,
    we are automatically print out help instructions.
    """

    args = cmdline.build_parser().parse_args(['-u', 'test_user'])
    mock_build_parser.assert_called_once_with()

    cmdline.perform_main(args, syn)
    mock_build_parser.call_count == 2

    mock_build_parser.return_value.print_help.assert_called_once_with()


@patch.object(cmdline, 'login_with_prompt')
def test_command_auto_login(mock_login_with_prompt, syn):
    """
    Verify command with the function but without login function,
    we are calling login_with_prompt automatically.
    """

    mock_login_with_prompt.assert_not_called()

    args = cmdline.build_parser().parse_args(['-u', 'test_user', 'get'])
    cmdline.perform_main(args, syn)

    mock_login_with_prompt.assert_called_once_with(syn, 'test_user', None, silent=True)
