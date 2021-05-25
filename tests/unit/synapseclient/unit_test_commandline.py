"""Test the Synapse command line client.

"""

import base64
import os

import pytest
import tempfile
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


def test_migrate__default_args(syn):
    """Test that the command line arguments are successfully passed to the migrate function
    when using the default options"""

    entity_id = 'syn12345'
    dest_storage_location_id = '98766'
    db_path = '/tmp/foo/bar'

    parser = cmdline.build_parser()

    # test w/ default optional args
    args = parser.parse_args([
        'migrate',
        'syn12345',
        dest_storage_location_id,
        db_path,
    ])

    assert args.id == entity_id
    assert args.dest_storage_location_id == dest_storage_location_id
    assert args.db_path == db_path
    assert args.file_version_strategy == 'new'
    assert args.include_table_files is False
    assert args.continue_on_error is False
    assert args.dryRun is False
    assert args.force is False
    assert args.csv_log_path is None


def test_migrate__fully_specified_args(mocker, syn):
    """Test that the command line arguments are successfully passed to the migrate function
    when the arguments are fully specified"""

    entity_id = 'syn12345'
    dest_storage_location_id = '98766'
    source_storage_location_ids = ['12345', '23456']
    db_path = '/tmp/foo/bar'

    parser = cmdline.build_parser()

    # test w/ fully specified args
    args = parser.parse_args([
        'migrate',
        entity_id,
        dest_storage_location_id,
        db_path,
        '--source_storage_location_ids', *source_storage_location_ids,
        '--file_version_strategy', 'all',
        '--dryRun',
        '--include_table_files',
        '--continue_on_error',
        '--force',
        '--csv_log_path', '/tmp/foo/bar'
    ])

    assert args.id == entity_id
    assert args.dest_storage_location_id == dest_storage_location_id
    assert args.source_storage_location_ids == source_storage_location_ids
    assert args.db_path == db_path
    assert args.file_version_strategy == 'all'
    assert args.include_table_files is True
    assert args.continue_on_error is True
    assert args.dryRun is True
    assert args.force is True
    assert args.csv_log_path == '/tmp/foo/bar'

    # verify args are passed through to the fn
    mock_index = mocker.patch.object(synapseutils, 'index_files_for_migration')
    mock_migrate = mocker.patch.object(synapseutils, 'migrate_indexed_files')

    cmdline.migrate(args, syn)

    mock_index.assert_called_once_with(
        syn,
        args.id,
        args.dest_storage_location_id,
        args.db_path,
        source_storage_location_ids=args.source_storage_location_ids,
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


def test_migrate__dont_continue(mocker, syn):
    """Verify we exit gracefully if migrate returns no result
    (e.g. the user declined to continue with the migration after reading the result of the index"""
    storage_location_id = '98766'
    db_path = '/tmp/foo/bar'

    parser = cmdline.build_parser()

    mocker.patch.object(synapseutils, 'index_files_for_migration')
    mock_migrate = mocker.patch.object(synapseutils, 'migrate_indexed_files')

    # a None simulates the user declining to continue
    mock_migrate.return_value = None

    args = parser.parse_args([
        'migrate',
        'syn12345',
        storage_location_id,
        db_path,
    ])

    cmdline.migrate(args, syn)


def test_get_multi_threaded_flag():
    """Test the multi threaded command line flag"""
    parser = cmdline.build_parser()
    args = parser.parse_args(['get', '--multiThreaded', 'syn123'])

    assert args.multiThreaded

    # defaults to True
    args = parser.parse_args(['get', 'syn123'])
    assert args.multiThreaded


def test_get_sts_token():
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
    syn.logger.info.assert_called_once_with(expected_output)


def test_authenticate_login__username_password(syn):
    """Verify happy path for _authenticate_login"""

    with patch.object(syn, 'login'):
        cmdline._authenticate_login(syn, 'foo', 'bar', rememberMe=True, silent=True)
        syn.login.assert_called_once_with('foo', password='bar', rememberMe=True, silent=True)


def test_authenticate_login__api_key(syn):
    """Verify attempting to authenticate when supplying an api key as the password.
    Should attempt to treat the password as an api key after the initial failures as a password and token"""

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
    Should attempt to treat the password as token after the initial failure as a password."""

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


def test_authenticate_login__no_input(mocker, syn):
    """Verify attempting to authenticate with a bare login command (i.e. expecting
    to derive credentials from config for cache)"""

    login_kwargs = {'rememberMe': True}

    call(**login_kwargs),

    mock_login = mocker.patch.object(syn, 'login')

    cmdline._authenticate_login(syn, None, None, **login_kwargs)
    mock_login.assert_called_once_with(None, **login_kwargs)


def test_authenticate_login__failure(mocker, syn):
    """Verify that a login with invalid credentials raises an error (the
    first error when multiple login methods were attempted."""

    login_kwargs = {'rememberMe': True}

    call(**login_kwargs),

    mock_login = mocker.patch.object(syn, 'login')

    def login_side_effect(*args, **kwargs):
        raise SynapseAuthenticationError("call{}".format(mock_login.call_count))
    mock_login.side_effect = login_side_effect

    with pytest.raises(SynapseAuthenticationError) as ex_cm:
        cmdline._authenticate_login(syn, None, None, **login_kwargs)
    assert str(ex_cm.value) == 'call1'


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


@pytest.mark.parametrize(
    'username,expected_pass_prompt',
    [
        ('foo', 'Password, api key, or auth token for user foo:'),
        ('', 'Auth token:'),
    ]
)
def test_login_with_prompt__getpass(mocker, username, expected_pass_prompt, syn):
    """
    Verify logging in when entering username and a secret from the console.
    The secret prompt should be customized depending on whether a username was entered
    or not (if not prompt for an auth token since username is not required for an auth token).
    """

    mock_sys = mocker.patch.object(cmdline, 'sys')
    mock_getpass = mocker.patch.object(cmdline, 'getpass')
    mock_input = mocker.patch.object(cmdline, 'input')
    mock_authenticate_login = mocker.patch.object(cmdline, '_authenticate_login')

    password = 'bar'
    login_kwargs = {
        'rememberMe': False,
        'silent': True,
        'forced': True,
    }

    def authenticate_side_effect(*args, **kwargs):
        if mock_authenticate_login.call_count == 1:
            # the first authenticate call doesn't take any input from console
            # (i.e. tries to use cache or config), when that returns no credentials
            # it prompts for username and a secret
            raise SynapseNoCredentialsError()
        return

    mock_sys.stdin.isatty.return_value = True

    mock_authenticate_login.side_effect = authenticate_side_effect
    mock_input.return_value = username
    mock_getpass.getpass.return_value = password

    cmdline.login_with_prompt(syn, None, None, **login_kwargs)

    mock_input.assert_called_once_with("Synapse username (leave blank if using an auth token): ")
    mock_getpass.getpass.assert_called_once_with(expected_pass_prompt)

    expected_authenticate_calls = [
        call(syn, None, None, **login_kwargs),
        call(syn, username, password, **{k: v for k, v in login_kwargs.items() if k != 'silent'})
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


def test_login_with_prompt__user_supplied(mocker, syn):
    """
    Verify that if we login_with_prompt and the username was supplied then we don't prompt the
    user for a username.
    """

    username = 'shrek'
    password = 'testpass'

    mock_sys = mocker.patch.object(cmdline, 'sys')
    mock_sys.isatty.return_value = True

    mock_getpass = mocker.patch.object(cmdline, 'getpass')
    mock_getpass.getpass.return_value = password

    mock_input = mocker.patch.object(cmdline, 'input')
    mock_authenticate_login = mocker.patch.object(cmdline, '_authenticate_login')
    mock_authenticate_login.side_effect = [SynapseNoCredentialsError(), None]

    cmdline.login_with_prompt(syn, username, None)
    assert not mock_input.called
    mock_authenticate_login.assert_called_with(
        syn,
        username,
        password,
        forced=False,
        rememberMe=False,
    )


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


def test__replace_existing_config__prepend(syn):
    """Replace adding authentication to .synapseConfig when there is no
    authentication section
    """
    f = tempfile.NamedTemporaryFile()
    auth_section = (
        '#[authentication]\n'
        "#username=foobar\n"
        "#password=testingtestingtesting\n\n"
    )
    with open(f.name, "w") as config_f:
        config_f.write(auth_section)

    new_auth_section = (
        '[authentication]\n'
        "username=foobar\n"
        "apikey=testingtesting\n\n"
    )
    new_config_text = cmdline._replace_existing_config(f.name, new_auth_section)

    expected_text = (
        '[authentication]\n'
        "username=foobar\n"
        "apikey=testingtesting\n\n\n\n"
        '#[authentication]\n'
        "#username=foobar\n"
        "#password=testingtestingtesting\n\n"
    )

    assert new_config_text == expected_text
    f.close()


def test__replace_existing_config__backup(syn):
    """Replace backup files are created"""
    f = tempfile.NamedTemporaryFile()
    auth_section = "foobar"
    with open(f.name, "w") as config_f:
        config_f.write(auth_section)
    new_auth_section = (
        '[authentication]\n'
        "username=foobar\n"
        "apikey=foobar\n\n"
    )
    cmdline._replace_existing_config(f.name, new_auth_section)
    # If command is run again, it will make sure to save existing
    # backup files
    cmdline._replace_existing_config(f.name, new_auth_section)
    assert os.path.exists(f.name + ".backup")
    assert os.path.exists(f.name + ".backup2")
    f.close()


def test__replace_existing_config__replace(syn):
    """Replace existing authentication to .synapseConfig
    """
    f = tempfile.NamedTemporaryFile()
    auth_section = (
        '[authentication]\n'
        "username=foobar\n"
        "password=testingtestingtesting\n\n"
    )
    with open(f.name, "w") as config_f:
        config_f.write(auth_section)

    new_auth_section = (
        '[authentication]\n'
        "username=foobar\n"
        "apikey=testingtesting\n\n"
    )
    new_config_text = cmdline._replace_existing_config(f.name, new_auth_section)

    expected_text = (
        '[authentication]\n'
        "username=foobar\n"
        "apikey=testingtesting\n\n"
    )
    assert new_config_text == expected_text
    f.close()


def test__generate_new_config(syn):
    """Generate new configuration file"""
    new_auth_section = (
        '[authentication]\n'
        "username=foobar\n"
        "apikey=testingtesting\n\n"
    )
    new_config_text = cmdline._generate_new_config(new_auth_section)
    expected_text = (
        "###########################\n# Login Credentials       "
        "#\n###########################\n\n"
        "## Used for logging in to Synapse\n## Alternatively, you can use "
        "rememberMe=True in synapseclient.login or login subcommand of the "
        "commandline client.\n[authentication]\nusername=foobar\n"
        "apikey=testingtesting\n\n\n\n\n## If you have projects with file "
        "stored on SFTP servers, you can specify your credentials here\n## "
        "You can specify multiple sftp credentials\n"
        "#[sftp://some.sftp.url.com]\n#username= <sftpuser>\n"
        "#password= <sftppwd>\n#[sftp://a.different.sftp.url.com]\n"
        "#username= <sftpuser>\n#password= <sftppwd>\n\n\n## If you have "
        "projects that need to be stored in an S3-like (e.g. AWS S3, "
        "Openstack) storage but cannot allow Synapse\n## to manage access "
        "your storage you may put your credentials here.\n## To avoid "
        "duplicating credentials with that used by the AWS Command Line "
        "Client,\n## simply put the profile name form your "
        "~/.aws/credentials file\n## more information about aws credentials "
        "can be found here http://docs.aws.amazon.com/cli/latest/userguide/"
        "cli-config-files.html\n#[https://s3.amazonaws.com/bucket_name] # "
        "this is the bucket's endpoint\n"
        "#profile_name=local_credential_profile_name\n\n\n"
        "###########################\n# Caching                 "
        "#\n###########################\n\n## your downloaded files are "
        "cached to avoid repeat downloads of the same file. change "
        "'location' to use a different folder on your computer as the "
        "cache location\n#[cache]\n#location = ~/.synapseCache\n\n\n"
        "###########################\n# Advanced Configurations #\n"
        "###########################\n\n## If this section is specified, "
        "then the synapseclient will print out debug information\n#[debug]"
        "\n\n\n## Configuring these will cause the Python client to use "
        "these as Synapse service endpoints instead of the default prod "
        "endpoints.\n#[endpoints]\n#repoEndpoint=<repoEndpoint>\n#"
        "authEndpoint=<authEndpoint>\n#fileHandleEndpoint="
        "<fileHandleEndpoint>\n#portalEndpoint=<portalEndpoint>\n\n## "
        "Settings to configure how Synapse uploads/downloads data\n"
        "#[transfer]\n\n# use this to configure the default for how "
        "many threads/connections Synapse will use to perform file "
        "transfers.\n# Currently this applies only to files whose "
        "underlying storage is AWS S3.\n# max_threads=16\n"
    )
    assert new_config_text == expected_text
