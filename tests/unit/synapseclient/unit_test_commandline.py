"""Test the Synapse command line client.

"""

import base64
import hashlib
import os

import pytest
from unittest.mock import call, Mock, patch, MagicMock

import synapseclient.__main__ as cmdline
from synapseclient.core.exceptions import (SynapseAuthenticationError, SynapseNoCredentialsError,
                                           SynapseProvenanceError, SynapseError)
from synapseclient.entity import File

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


@patch.object(cmdline, 'synapseutils')
def test_get_manifest_option(mock_synapseutils):
    """
    Verify the create manifest option works properly for three choices which are 'all', 'root', 'suppress'.
    """
    parser = cmdline.build_parser()
    syn = Mock()

    # createManifest defaults to all
    args = parser.parse_args(['get', '-r', 'syn123'])
    assert args.manifest == 'all'
    cmdline.get(args, syn)
    mock_synapseutils.syncFromSynapse.assert_called_with(syn, 'syn123', './', followLink=False, manifest="all")

    # creating the root manifest file only
    args = parser.parse_args(['get', '-r', 'syn123', '--manifest', 'root'])
    assert args.manifest == 'root'
    cmdline.get(args, syn)
    mock_synapseutils.syncFromSynapse.assert_called_with(syn, 'syn123', './', followLink=False, manifest="root")

    # suppress creating the manifest file
    args = parser.parse_args(['get', '-r', 'syn123', '--manifest', 'suppress'])
    assert args.manifest == 'suppress'
    cmdline.get(args, syn)
    mock_synapseutils.syncFromSynapse.assert_called_with(syn, 'syn123', './', followLink=False, manifest="suppress")


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


class TestGetFunction:
    @patch('synapseclient.client.Synapse')
    def setup(self, mock_syn):
        self.syn = mock_syn

    @patch.object(synapseutils, 'syncFromSynapse')
    def test_get__with_arg_recursive(self, mock_syncFromSynapse):
        parser = cmdline.build_parser()
        args = parser.parse_args(['get', '-r', 'syn123'])
        cmdline.get(args, self.syn)

        mock_syncFromSynapse.assert_called_once_with(self.syn, 'syn123', './', followLink=False, manifest='all')

    @patch.object(cmdline, '_getIdsFromQuery')
    def test_get__with_arg_queryString(self, mock_getIdsFromQuery):
        parser = cmdline.build_parser()
        args = parser.parse_args(['get', '-q', 'test_query'])
        mock_getIdsFromQuery.return_value = ['syn123', 'syn456']
        cmdline.get(args, self.syn)

        mock_getIdsFromQuery.assert_called_once_with('test_query', self.syn, './')
        assert self.syn.get.call_args_list == [call('syn123', downloadLocation='./'),
                                               call('syn456', downloadLocation='./')]

    @patch.object(cmdline, 'os')
    def test_get__with_id_path(self, mock_os):
        parser = cmdline.build_parser()
        args = parser.parse_args(['get', './temp/path'])
        mock_os.path.isfile.return_value = True
        self.syn.get.return_value = {}
        cmdline.get(args, self.syn)

        self.syn.get.assert_called_once_with('./temp/path', version=None, limitSearch=None, downloadFile=False)

    @patch.object(cmdline, 'os')
    def test_get__with_normal_id(self, mock_os):
        parser = cmdline.build_parser()
        args = parser.parse_args(['get', 'syn123'])
        mock_entity = MagicMock(id='syn123')
        mock_os.path.isfile.return_value = False
        self.syn.get.return_value = mock_entity
        cmdline.get(args, self.syn)

        self.syn.get.assert_called_once_with('syn123', version=None, followLink=False, downloadLocation='./')
        assert self.syn.logger.info.call_args_list == [call('WARNING: No files associated with entity %s\n', 'syn123'),
                                                       call(mock_entity)]

        mock_entity2 = File(path='./tmp_path', parent='syn123')

        self.syn.get.return_value = mock_entity2
        mock_os.path.exists.return_value = True
        mock_os.path.basename.return_value = "./base_tmp_path"
        cmdline.get(args, self.syn)
        assert self.syn.logger.info.call_args_list == [call('WARNING: No files associated with entity %s\n', 'syn123'),
                                                       call(mock_entity),
                                                       call('Downloaded file: %s', './base_tmp_path'),
                                                       call('Creating %s', './tmp_path')]


@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'os')
def test_get_provenance_function__with_syn_id(mock_os, mock_syn):
    parser = cmdline.build_parser()
    args = parser.parse_args(['get-provenance', '-id', 'syn123'])
    mock_os.path.isfile.return_value = False

    mock_syn.getProvenance.return_value = {'id': 'syn123', 'name': 'test_name', 'description': 'test_description'}
    cmdline.getProvenance(args, mock_syn)

    mock_syn.getProvenance.assert_called_with('syn123', None)
    mock_syn.logger.info.assert_called_with('{\n  "description": "test_description",\n  "id": "syn123",\n  '
                                            '"name": "test_name"\n}')


@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'os')
@patch.object(cmdline, 'utils')
def test_get_provenance_function__with_syn_id_md5__only_one_result__without_limitSeatch(mock_utils, mock_os, mock_syn):
    parser = cmdline.build_parser()
    args = parser.parse_args(['get-provenance', '-id', 'home/temp_path/temp_file'])
    mock_os.path.isfile.return_value = True

    mock_syn.restGET.return_value = {'results': [{'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False}]}

    mock_syn.getProvenance.return_value = {'id': 'syn123', 'name': 'test_name', 'description': 'test_description'}
    mock_utils.md5_for_file.return_value = hashlib.md5()

    cmdline.getProvenance(args, mock_syn)

    mock_syn.getProvenance.assert_called_with('syn12345', None)


@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'filter_id_by_limitSearch')
@patch.object(cmdline, 'os')
@patch.object(cmdline, 'utils')
def test_get_provenance_function__with_syn_id_md5__with_limitSeatch(mock_utils, mock_os, mock_filter_id_by_limitSearch,
                                                               mock_syn):
    parser = cmdline.build_parser()
    args = parser.parse_args(['get-provenance', '-id', 'home/temp_path/temp_file', '-limitSearch', 'syn123'])
    mock_os.path.isfile.return_value = True

    mock_syn.restGET.return_value = {'results': [{'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False},
                                                 {'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 2, 'versionLabel': '2', 'isLatestVersion': False}
                                                 ]}
    mock_filter_id_by_limitSearch.return_value = [{'name': 'test_file', 'id': 'syn12345',
                                                   'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                   'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False},
                                                  {'name': 'test_file', 'id': 'syn12345',
                                                   'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                   'versionNumber': 2, 'versionLabel': '2', 'isLatestVersion': False}
                                                  ]
    mock_syn.getProvenance.return_value = {'id': 'syn123', 'name': 'test_name', 'description': 'test_description'}
    mock_utils.md5_for_file.return_value = hashlib.md5()

    cmdline.getProvenance(args, mock_syn)

    mock_syn.getProvenance.assert_called_with('syn12345', None)
    mock_filter_id_by_limitSearch.assert_called_with(mock_syn, mock_syn.restGET()['results'], args.limitSearch)


@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'filter_id_by_limitSearch')
@patch.object(cmdline, 'os')
@patch.object(cmdline, 'utils')
def test_get_provenance_function__raise_exception(mock_utils, mock_os, mock_filter_id_by_limitSearch, mock_syn):
    parser = cmdline.build_parser()
    args = parser.parse_args(['get-provenance', '-id', 'home/temp_path/temp_file', '-limitSearch', 'syn123'])
    mock_os.path.isfile.return_value = True

    mock_syn.restGET.return_value = {'results': [{'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False},
                                                 {'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 2, 'versionLabel': '2', 'isLatestVersion': False}
                                                 ]}
    mock_filter_id_by_limitSearch.return_value = [{'name': 'test_file', 'id': 'syn12345',
                                                   'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                   'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False},
                                                  {'name': 'test_file', 'id': 'syn789',
                                                   'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                   'versionNumber': 2, 'versionLabel': '2', 'isLatestVersion': False}
                                                  ]
    mock_syn.getProvenance.return_value = {'id': 'syn123', 'name': 'test_name', 'description': 'test_description'}
    mock_utils.md5_for_file.return_value = hashlib.md5()

    assert len(cmdline.check_id_results(mock_filter_id_by_limitSearch())) == 2

    with pytest.raises(SynapseProvenanceError) as syn_ex:
        cmdline.getProvenance(args, mock_syn)

    str(syn_ex.value) == 'There are more than one identical content for this file in different locations on Synapse'
    mock_syn.getProvenance.assert_not_called()


@patch.object(cmdline, 'check_id_results')
@patch.object(cmdline, 'filter_id_by_limitSearch')
@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'os')
def test_set_provenance_function__with_syn_id(mock_os, mock_syn, mock_filter_id_by_limitSearch, mock_check_id_results):
    parser = cmdline.build_parser()
    args = parser.parse_args(['set-provenance', '-id', 'syn123', '-name', 'test_filename', '-description', 'test_desc',
                              '-used', 'syn456'])
    mock_os.path.isfile.return_value = False

    mock_syn._convertProvenanceList.return_value = ['syn456']
    cmdline.setProvenance(args, mock_syn)

    mock_syn.setProvenance.assert_called_with('syn123', {'used': [{'reference': {'targetId': 'syn456'},
                                                                   'concreteType':
                                                                       'org.sagebionetworks.repo.model.'
                                                                       'provenance.UsedEntity',
                                                                   'wasExecuted': False}],
                                                         'name': 'test_filename',
                                                         'description': 'test_desc'})
    mock_filter_id_by_limitSearch.assert_not_called()
    mock_check_id_results.assert_not_called()


@patch.object(cmdline, 'check_id_results')
@patch.object(cmdline, 'filter_id_by_limitSearch')
@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'os')
@patch.object(cmdline, 'utils')
def test_set_provenance_function__with_syn_id_md5__only_one_result__without_limitSeatch(mock_utils, mock_os, mock_syn,
                                                                                        mock_filter_id_by_limitSearch,
                                                                                        mock_check_id_results):
    parser = cmdline.build_parser()
    args = parser.parse_args(['set-provenance', '-id', 'home/temp_path/temp_file', '-name', 'test_filename',
                              '-description', 'test_desc', '-used', 'syn456'])
    mock_os.path.isfile.return_value = True

    mock_syn.restGET.return_value = {'results': [{'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False}]}

    mock_utils.md5_for_file.return_value = hashlib.md5()
    mock_syn._convertProvenanceList.return_value = ['syn456']

    cmdline.setProvenance(args, mock_syn)

    mock_syn.setProvenance.assert_called_with('syn12345', {'used': [{'reference': {'targetId': 'syn456'},
                                                                     'concreteType':
                                                                         'org.sagebionetworks.repo.model.'
                                                                         'provenance.UsedEntity',
                                                                     'wasExecuted': False}],
                                                           'name': 'test_filename',
                                                           'description': 'test_desc'})
    mock_filter_id_by_limitSearch.assert_not_called()
    mock_check_id_results.assert_called_with([{'name': 'test_file',
                                               'id': 'syn12345',
                                               'type': 'org.sagebionetworks.repo.model.FileEntity',
                                               'versionNumber': 1,
                                               'versionLabel': '1',
                                               'isLatestVersion': False}])


@patch.object(cmdline, 'check_id_results')
@patch.object(cmdline, 'filter_id_by_limitSearch')
@patch('synapseclient.client.Synapse')
@patch.object(cmdline, 'os')
@patch.object(cmdline, 'utils')
def test_set_provenance_function__with_syn_id_md5__with_limitSeatch(mock_utils, mock_os, mock_syn,
                                                                    mock_filter_id_by_limitSearch,
                                                                    mock_check_id_results):
    parser = cmdline.build_parser()
    args = parser.parse_args(['set-provenance', '-id', 'home/temp_path/temp_file', '-name', 'test_filename',
                              '-description', 'test_desc', '-used', 'syn456'])
    mock_os.path.isfile.return_value = True

    mock_syn.restGET.return_value = {'results': [{'name': 'test_file', 'id': 'syn12345',
                                                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                                                  'versionNumber': 1, 'versionLabel': '1', 'isLatestVersion': False}]}

    mock_utils.md5_for_file.return_value = hashlib.md5()
    mock_syn._convertProvenanceList.return_value = ['syn456']

    cmdline.setProvenance(args, mock_syn)

    mock_syn.setProvenance.assert_called_with('syn12345', {'used': [{'reference': {'targetId': 'syn456'},
                                                                     'concreteType':
                                                                         'org.sagebionetworks.repo.model.'
                                                                         'provenance.UsedEntity',
                                                                     'wasExecuted': False}],
                                                           'name': 'test_filename',
                                                           'description': 'test_desc'})
    mock_filter_id_by_limitSearch.assert_not_called()
    mock_check_id_results.assert_called_with([{'name': 'test_file',
                                               'id': 'syn12345',
                                               'type': 'org.sagebionetworks.repo.model.FileEntity',
                                               'versionNumber': 1,
                                               'versionLabel': '1',
                                               'isLatestVersion': False}])
