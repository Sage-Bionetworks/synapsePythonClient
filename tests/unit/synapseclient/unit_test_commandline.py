"""Test the Synapse command line client.

"""

import base64
import os
import tempfile
from unittest.mock import MagicMock, Mock, call, mock_open, patch

import pytest

import synapseclient.__main__ as cmdline
import synapseutils
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseNoCredentialsError,
)
from synapseclient.entity import File


def test_command_sync(syn):
    """Test the sync function.

    Since this function only passes argparse arguments for the sync subcommand
    straight to `synapseutils.sync.syncToSynapse`, the only tests here are for
    the command line arguments provided and that the function is called once.

    """
    mockFileOpener = MagicMock()
    with patch("argparse.FileType", return_value=mockFileOpener):
        parser = cmdline.build_parser()
        args = parser.parse_args(["sync", "/tmp/foobarbaz.tsv"])
        mockFileOpener.assert_called_once_with("/tmp/foobarbaz.tsv")

    assert args.manifestFile is mockFileOpener.return_value
    assert args.dryRun is False
    assert args.sendMessages is False
    assert args.retries == 4

    with patch.object(synapseutils, "syncToSynapse") as mockedSyncToSynapse:
        cmdline.sync(args, syn)
        mockedSyncToSynapse.assert_called_once_with(
            syn,
            manifestFile=args.manifestFile,
            dryRun=args.dryRun,
            sendMessages=args.sendMessages,
            retries=args.retries,
        )


def test_migrate__default_args(syn):
    """Test that the command line arguments are successfully passed to the migrate function
    when using the default options"""

    entity_id = "syn12345"
    dest_storage_location_id = "98766"
    db_path = "/tmp/foo/bar"

    parser = cmdline.build_parser()

    # test w/ default optional args
    args = parser.parse_args(
        [
            "migrate",
            "syn12345",
            dest_storage_location_id,
            db_path,
        ]
    )

    assert args.id == entity_id
    assert args.dest_storage_location_id == dest_storage_location_id
    assert args.db_path == db_path
    assert args.file_version_strategy == "new"
    assert args.include_table_files is False
    assert args.continue_on_error is False
    assert args.dryRun is False
    assert args.force is False
    assert args.csv_log_path is None


def test_migrate__fully_specified_args(mocker, syn):
    """Test that the command line arguments are successfully passed to the migrate function
    when the arguments are fully specified"""

    entity_id = "syn12345"
    dest_storage_location_id = "98766"
    source_storage_location_ids = ["12345", "23456"]
    db_path = "/tmp/foo/bar"

    parser = cmdline.build_parser()

    # test w/ fully specified args
    args = parser.parse_args(
        [
            "migrate",
            entity_id,
            dest_storage_location_id,
            db_path,
            "--source_storage_location_ids",
            *source_storage_location_ids,
            "--file_version_strategy",
            "all",
            "--dryRun",
            "--include_table_files",
            "--continue_on_error",
            "--force",
            "--csv_log_path",
            "/tmp/foo/bar",
        ]
    )

    assert args.id == entity_id
    assert args.dest_storage_location_id == dest_storage_location_id
    assert args.source_storage_location_ids == source_storage_location_ids
    assert args.db_path == db_path
    assert args.file_version_strategy == "all"
    assert args.include_table_files is True
    assert args.continue_on_error is True
    assert args.dryRun is True
    assert args.force is True
    assert args.csv_log_path == "/tmp/foo/bar"

    # verify args are passed through to the fn
    mock_index = mocker.patch.object(synapseutils, "index_files_for_migration")
    mock_migrate = mocker.patch.object(synapseutils, "migrate_indexed_files")

    cmdline.migrate(args, syn)

    mock_index.assert_called_once_with(
        syn,
        args.id,
        args.dest_storage_location_id,
        args.db_path,
        source_storage_location_ids=args.source_storage_location_ids,
        file_version_strategy="all",
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
        force=True,
    )


def test_migrate__dont_continue(mocker, syn):
    """Verify we exit gracefully if migrate returns no result
    (e.g. the user declined to continue with the migration after reading the result of the index
    """
    storage_location_id = "98766"
    db_path = "/tmp/foo/bar"

    parser = cmdline.build_parser()

    mocker.patch.object(synapseutils, "index_files_for_migration")
    mock_migrate = mocker.patch.object(synapseutils, "migrate_indexed_files")

    # a None simulates the user declining to continue
    mock_migrate.return_value = None

    args = parser.parse_args(
        [
            "migrate",
            "syn12345",
            storage_location_id,
            db_path,
        ]
    )

    cmdline.migrate(args, syn)


@patch.object(cmdline, "synapseutils")
def test_get_manifest_option(mock_synapseutils):
    """
    Verify the create manifest option works properly for three choices which are 'all', 'root', 'suppress'.
    """
    parser = cmdline.build_parser()
    syn = Mock()

    # createManifest defaults to all
    args = parser.parse_args(["get", "-r", "syn123"])
    assert args.manifest == "all"
    cmdline.get(args, syn)
    mock_synapseutils.syncFromSynapse.assert_called_with(
        syn, "syn123", "./", followLink=False, manifest="all"
    )

    # creating the root manifest file only
    args = parser.parse_args(["get", "-r", "syn123", "--manifest", "root"])
    assert args.manifest == "root"
    cmdline.get(args, syn)
    mock_synapseutils.syncFromSynapse.assert_called_with(
        syn, "syn123", "./", followLink=False, manifest="root"
    )

    # suppress creating the manifest file
    args = parser.parse_args(["get", "-r", "syn123", "--manifest", "suppress"])
    assert args.manifest == "suppress"
    cmdline.get(args, syn)
    mock_synapseutils.syncFromSynapse.assert_called_with(
        syn, "syn123", "./", followLink=False, manifest="suppress"
    )


def test_get_multi_threaded_flag():
    """Test the multi threaded command line flag"""
    parser = cmdline.build_parser()
    args = parser.parse_args(["get", "--multiThreaded", "syn123"])

    assert args.multiThreaded

    # defaults to True
    args = parser.parse_args(["get", "syn123"])
    assert args.multiThreaded


def test_get_sts_token():
    """Test getting an STS token."""
    folder_id = "syn_1"
    permission = "read_write"
    syn = Mock()

    expected_output = "export foo=bar"
    syn.get_sts_storage_token.return_value = expected_output

    parser = cmdline.build_parser()
    args = parser.parse_args(["get-sts-token", folder_id, permission, "-o", "shell"])
    cmdline.get_sts_token(args, syn)
    syn.get_sts_storage_token.assert_called_with(
        folder_id, permission, output_format="shell"
    )
    syn.logger.info.assert_called_once_with(expected_output)


def test_authenticate_login__auth_token(syn):
    """Verify attempting to authenticate when supplying an auth bearer token instead of an password (or api key).
    Should attempt to treat the password as token after the initial failure as a password.
    """

    username = "foo"
    auth_token = "auth_bearer_token"
    login_kwargs = {}

    expected_login_calls = [
        call(username, authToken=auth_token, **login_kwargs),
    ]

    with patch.object(syn, "login") as login:
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
            passed_auth_token = kwargs.get("authToken")
            if not passed_auth_token:
                raise SynapseAuthenticationError()

        login.side_effect = login_side_effect

        cmdline._authenticate_login(syn, username, auth_token, **login_kwargs)
        assert expected_login_calls == login.call_args_list


def test_authenticate_login__no_input(mocker, syn):
    """Verify attempting to authenticate with a bare login command (i.e. expecting
    to derive credentials from config for cache)"""

    login_kwargs = {}

    call(**login_kwargs),

    mock_login = mocker.patch.object(syn, "login")

    cmdline._authenticate_login(syn, None, None, **login_kwargs)
    mock_login.assert_called_once_with(None, **login_kwargs)


def test_authenticate_login__failure(mocker, syn):
    """Verify that a login with invalid credentials raises an error (the
    first error when multiple login methods were attempted."""

    login_kwargs = {}

    call(**login_kwargs),

    mock_login = mocker.patch.object(syn, "login")

    def login_side_effect(*args, **kwargs):
        raise SynapseAuthenticationError("call{}".format(mock_login.call_count))

    mock_login.side_effect = login_side_effect

    with pytest.raises(SynapseAuthenticationError) as ex_cm:
        cmdline._authenticate_login(syn, None, None, **login_kwargs)
    assert str(ex_cm.value) == "call1"


@pytest.mark.parametrize(
    "username,expected_pass_prompt",
    [
        ("foo", "Auth token for user foo:"),
        ("", "Auth token:"),
    ],
)
def test_login_with_prompt__getpass(mocker, username, expected_pass_prompt, syn):
    """
    Verify logging in when entering username and a secret from the console.
    The secret prompt should be customized depending on whether a username was entered
    or not (if not prompt for an auth token since username is not required for an auth token).
    """

    mock_sys = mocker.patch.object(cmdline, "sys")
    mock_getpass = mocker.patch.object(cmdline, "getpass")
    mock_input = mocker.patch.object(cmdline, "input")
    mock_authenticate_login = mocker.patch.object(cmdline, "_authenticate_login")

    password = "bar"

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

    cmdline.login_with_prompt(syn=syn, user=None, password=None, silent=True)

    assert mock_input.call_args_list[0] == call("Synapse username (Optional): ")
    mock_getpass.getpass.assert_called_once_with(expected_pass_prompt)

    profile_name = username or "default"

    expected_authenticate_calls = [
        call(syn=syn, user=None, secret=None, profile=None, silent=True),
        call(
            syn=syn, user=username, secret=password, profile=profile_name, silent=True
        ),
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
    args = parser.parse_args(["--silent"])
    assert args.silent is True


@patch("synapseclient.Synapse")
def test_commandline_main(mock_syn):
    """
    Test the main method
    """

    configPath = os.path.join(os.path.expanduser("~"), ".synapseConfig")
    args = cmdline.build_parser().parse_args(["-u", "testUser", "--silent"])

    with patch.object(cmdline, "build_parser") as mock_build_parser:
        mock_build_parser.return_value.parse_args.return_value = args
        cmdline.main()
        mock_syn.assert_called_once_with(
            debug=False, skip_checks=False, configPath=configPath, silent=True
        )


@patch.object(cmdline, "sys")
@patch.object(cmdline, "input")
@patch.object(cmdline, "_authenticate_login")
def test_login_with_prompt_no_tty(mock_authenticate_login, mock_input, mock_sys, syn):
    """
    Verify login_with_prompt when the terminal is not a tty,
    we are unable to read from standard input and throw a SynapseAuthenticationError
    """

    user = "test_user"
    login_kwargs = {
        "silent": True,
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

    username = "shrek"
    password = "testpass"

    mock_sys = mocker.patch.object(cmdline, "sys")
    mock_sys.isatty.return_value = True

    mock_getpass = mocker.patch.object(cmdline, "getpass")
    mock_getpass.getpass.return_value = password

    mock_input = mocker.patch.object(cmdline, "input")
    mock_input.return_value = username
    mock_authenticate_login = mocker.patch.object(cmdline, "_authenticate_login")
    mock_authenticate_login.side_effect = [SynapseNoCredentialsError(), None]

    cmdline.login_with_prompt(syn=syn, user=username, password=None)
    mock_input.assert_called_once_with(
        "Configuration profile name (Optional, 'default' used if not specified): "
    )
    mock_authenticate_login.assert_called_with(
        syn=syn,
        user=username,
        secret=password,
        profile=username,
        silent=False,
    )


@patch.object(cmdline, "build_parser")
def test_no_command_print_help(mock_build_parser, syn):
    """
    Verify command without any function,
    we are automatically print out help instructions.
    """

    args = cmdline.build_parser().parse_args(["-u", "test_user"])
    mock_build_parser.assert_called_once_with()

    cmdline.perform_main(args, syn)
    mock_build_parser.call_count == 2

    mock_build_parser.return_value.print_help.assert_called_once_with()


@patch.object(cmdline.sys, "exit")
@patch.object(cmdline, "login_with_prompt")
def test_command_auto_login(mock_login_with_prompt, mock_sys_exit, syn):
    """
    Verify command with the function but without login function,
    we are calling login_with_prompt automatically.
    """

    mock_login_with_prompt.assert_not_called()

    args = cmdline.build_parser().parse_args(["-u", "test_user", "get"])
    cmdline.perform_main(args, syn)

    mock_login_with_prompt.assert_called_once_with(
        syn=syn, user="test_user", password=None, silent=False, profile=None
    )
    mock_sys_exit.assert_called_once_with(1)


def test__replace_existing_config__prepend(syn):
    """Replace adding authentication to .synapseConfig when there is no
    authentication section
    """
    f = tempfile.NamedTemporaryFile(mode="w", delete=False)
    auth_section = (
        "#[authentication]\n" "#username=foobar\n" "#password=testingtestingtesting\n\n"
    )
    with open(f.name, "w") as config_f:
        config_f.write(auth_section)

    new_auth_section = (
        "[authentication]\n" "username=foobar\n" "authToken=testingtesting\n\n"
    )
    profile_name = "authentication"
    new_config_text = cmdline._replace_existing_config(
        f.name, new_auth_section, profile_name
    )

    expected_text = (
        "[authentication]\n"
        "username=foobar\n"
        "authToken=testingtesting\n\n\n\n"
        "#[authentication]\n"
        "#username=foobar\n"
        "#password=testingtestingtesting\n\n"
    )

    assert new_config_text == expected_text
    f.close()

    for suffix in ["", ".backup", ".backup2", ".backup3"]:
        try:
            os.remove(f.name + suffix)
        except FileNotFoundError:
            pass


def test__replace_existing_config__backup(syn):
    """Replace backup files are created"""
    f = tempfile.NamedTemporaryFile(mode="w", delete=False)
    auth_section = "foobar"
    with open(f.name, "w") as config_f:
        config_f.write(auth_section)
    new_auth_section = "[authentication]\n" "username=foobar\n" "authToken=foobar\n\n"
    profile_name = "authentication"
    cmdline._replace_existing_config(f.name, new_auth_section, profile_name)
    # If command is run again, it will make sure to save existing
    # backup files
    cmdline._replace_existing_config(f.name, new_auth_section, profile_name)
    assert os.path.exists(f.name + ".backup")
    assert os.path.exists(f.name + ".backup2")
    f.close()


def test__replace_existing_config__replace(syn):
    """Replace existing authentication to .synapseConfig"""
    f = tempfile.NamedTemporaryFile(mode="w", delete=False)
    auth_section = (
        "[authentication]\n" "username=foobar\n" "password=testingtestingtesting\n\n"
    )
    with open(f.name, "w") as config_f:
        config_f.write(auth_section)

    new_auth_section = (
        "[authentication]\n" "username=foobar\n" "authToken=testingtesting\n\n"
    )
    profile_name = "authentication"
    new_config_text = cmdline._replace_existing_config(
        f.name, new_auth_section, profile_name
    )

    expected_text = (
        "[authentication]\n" "username=foobar\n" "authToken=testingtesting\n\n"
    )
    assert new_config_text == expected_text
    f.close()


def test__generate_new_config(syn):
    """Generate new configuration file"""
    profile_name = "authentication"
    new_auth_section = "username=foobar\nauthToken=testingtesting"
    expected_section = f"[profile {profile_name}]\n{new_auth_section}"
    new_config_text = cmdline._generate_new_config(new_auth_section, profile_name)
    assert expected_section in new_config_text


@patch.object(cmdline, "_generate_new_config")
@patch.object(cmdline, "_authenticate_login")
@patch.object(cmdline, "_prompt_for_credentials")
def test_config_generate(
    mock__prompt_for_credentials,
    mock__authenticate_login,
    mock__generate_new_config,
    syn,
):
    """Config when generating new configuration"""
    mock__prompt_for_credentials.return_value = ("test", "wow", "authentication")
    mock__authenticate_login.return_value = "password"
    mock__generate_new_config.return_value = "test"

    expected_auth_section = "[profile authentication]\nusername=test\npassword=wow\n\n"
    args = Mock()
    args.configPath = "foo"
    args.profile = "authentication"
    cmdline.config(args, syn)
    os.unlink("foo")
    mock__generate_new_config.assert_called_once_with(
        expected_auth_section, "authentication"
    )


@patch.object(cmdline, "_replace_existing_config")
@patch.object(cmdline, "_authenticate_login")
@patch.object(cmdline, "_prompt_for_credentials")
def test_config_replace(
    mock__prompt_for_credentials,
    mock__authenticate_login,
    mock__replace_existing_config,
    syn,
):
    """Config when replacing configuration"""
    mock__prompt_for_credentials.return_value = ("test", "wow", "authentication")
    mock__authenticate_login.return_value = "password"
    mock__replace_existing_config.return_value = "test"

    expected_auth_section = "[profile authentication]\nusername=test\npassword=wow\n\n"
    temp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    args = Mock()
    args.configPath = temp.name
    args.profile = "authentication"
    cmdline.config(args, syn)
    mock__replace_existing_config.assert_called_once_with(
        temp.name, expected_auth_section, "authentication"
    )
    temp.close()


@patch("synapseclient.__main__.os.path.exists", return_value=False)
@patch("synapseclient.__main__.open", new_callable=mock_open)
@patch.object(cmdline, "_generate_new_config")
@patch.object(cmdline, "_authenticate_login")
@patch.object(cmdline, "_prompt_for_credentials")
@patch.object(cmdline, "_replace_existing_config")
def test_config_generate_named_profile(
    mock_replace_existing_config,
    mock_prompt_for_credentials,
    mock_authenticate_login,
    mock_generate_new_config,
    mock_exists,
    syn,
):
    """Test config command generates a new config with a named profile"""
    mock_prompt_for_credentials.return_value = (
        "testuser",
        "authtoken123",
        "devprofile",
    )
    mock_authenticate_login.return_value = "authtoken"
    mock_generate_new_config.return_value = "config text"

    args = Mock()
    args.configPath = "test_config_path"
    args.profile = "devprofile"

    expected_auth_section = (
        "[profile devprofile]\nusername=testuser\nauthtoken=authtoken123\n\n"
    )
    cmdline.config(args, syn)

    # Assertions for the calls
    mock_generate_new_config.assert_called_once_with(
        expected_auth_section, "devprofile"
    )

    # Ensure that _authenticate_login was called once to validate credentials
    mock_authenticate_login.assert_called_once()

    # Ensure that _replace_existing_config was not called
    mock_replace_existing_config.assert_not_called()

    # Check that the open function was called with the correct parameters
    mock_exists.assert_called_once_with(args.configPath, "w")


@patch("synapseclient.__main__.os.path.exists", return_value=True)
@patch("synapseclient.__main__.open", new_callable=mock_open)
@patch.object(cmdline, "_generate_new_config")
@patch.object(cmdline, "_replace_existing_config")
@patch.object(cmdline, "_authenticate_login")
@patch.object(cmdline, "_prompt_for_credentials")
def test_config_replace_named_profile(
    mock_prompt_for_credentials,
    mock_authenticate_login,
    mock_replace_existing_config,
    mock_generate_new_config,
    mock_open,
    mock_exists,
    syn,
):
    """Test config replacement logic for named profile"""
    mock_prompt_for_credentials.return_value = ("testuser", "authtoken123", "prod")
    mock_authenticate_login.return_value = "authtoken"
    mock_replace_existing_config.return_value = "new config text"

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        config_path = tmp.name

    args = Mock()
    args.configPath = config_path
    args.profile = "prod"

    expected_auth_section = (
        "[profile prod]\nusername=testuser\nauthtoken=authtoken123\n\n"
    )
    cmdline.config(args, syn)

    # Assertions for the calls
    mock_replace_existing_config.assert_called_once_with(
        config_path, expected_auth_section, "prod"
    )

    # Ensure that _authenticate_login was called once to validate credentials
    mock_authenticate_login.assert_called_once()

    # Ensure that _generate_new_config was NOT called, since we are replacing the config
    mock_generate_new_config.assert_not_called()

    # Check that open was called with the correct parameters
    mock_open.assert_called_once_with(config_path, "w")

    # Clean up backups if any were made
    for suffix in ["", ".backup", ".backup2", ".backup3"]:
        try:
            os.remove(config_path + suffix)
        except FileNotFoundError:
            pass


def test_login_from_named_profile(mocker, syn):
    """Test CLI login using a profile name"""
    mock_login = mocker.patch.object(syn, "login")

    args = Mock()
    args.username = None
    args.password = None
    args.profile = "testprofile"
    args.silent = False

    cmdline._authenticate_login(
        syn, args.username, args.password, profile=args.profile, silent=args.silent
    )
    mock_login.assert_called_once_with(None, profile="testprofile", silent=False)


class TestGetFunction:
    @pytest.fixture(scope="function", autouse=True)
    @patch("synapseclient.client.Synapse")
    def setup(self, mock_syn):
        self.syn = mock_syn

    @patch.object(synapseutils, "syncFromSynapse")
    def test_get__with_arg_recursive(self, mock_syncFromSynapse):
        parser = cmdline.build_parser()
        args = parser.parse_args(["get", "-r", "syn123"])
        cmdline.get(args, self.syn)

        mock_syncFromSynapse.assert_called_once_with(
            self.syn, "syn123", "./", followLink=False, manifest="all"
        )

    @patch.object(cmdline, "_getIdsFromQuery")
    def test_get__with_arg_queryString(self, mock_getIdsFromQuery):
        parser = cmdline.build_parser()
        args = parser.parse_args(["get", "-q", "test_query"])
        mock_getIdsFromQuery.return_value = ["syn123", "syn456"]
        cmdline.get(args, self.syn)

        mock_getIdsFromQuery.assert_called_once_with("test_query", self.syn, "./")
        assert self.syn.get.call_args_list == [
            call("syn123", downloadLocation="./"),
            call("syn456", downloadLocation="./"),
        ]

    @patch.object(cmdline, "os")
    def test_get__with_id_path(self, mock_os):
        parser = cmdline.build_parser()
        args = parser.parse_args(["get", "./temp/path"])
        mock_os.path.isfile.return_value = True
        self.syn.get.return_value = {}
        cmdline.get(args, self.syn)

        self.syn.get.assert_called_once_with(
            "./temp/path", version=None, limitSearch=None, downloadFile=False
        )

    @patch.object(cmdline, "os")
    def test_get__with_normal_id(self, mock_os):
        parser = cmdline.build_parser()
        args = parser.parse_args(["get", "syn123"])
        mock_entity = MagicMock(id="syn123")
        mock_os.path.isfile.return_value = False
        self.syn.get.return_value = mock_entity
        cmdline.get(args, self.syn)

        self.syn.get.assert_called_once_with(
            "syn123", version=None, followLink=False, downloadLocation="./"
        )
        assert self.syn.logger.info.call_args_list == [
            call("WARNING: No files associated with entity %s\n", "syn123"),
            call(mock_entity),
        ]

        mock_entity2 = File(path="./tmp_path", parent="syn123")

        self.syn.get.return_value = mock_entity2
        mock_os.path.exists.return_value = True
        mock_os.path.basename.return_value = "./base_tmp_path"
        cmdline.get(args, self.syn)
        assert self.syn.logger.info.call_args_list == [
            call("WARNING: No files associated with entity %s\n", "syn123"),
            call(mock_entity),
        ]

    def test_get__without_synapse_id(self):
        # test normal get command without synapse ID
        parser = cmdline.build_parser()
        with pytest.raises(ValueError) as ve:
            args = parser.parse_args(["get"])
            cmdline.get(args, self.syn)
        assert (
            str(ve.value) == "Missing expected id argument for use with the get command"
        )

        # test get command with -r but without synapse ID
        parser = cmdline.build_parser()
        with pytest.raises(ValueError) as ve:
            args = parser.parse_args(["get", "-r"])
            cmdline.get(args, self.syn)
        assert (
            str(ve.value) == "Missing expected id argument for use with the get command"
        )


class TestStoreFunction:
    @pytest.fixture(scope="function", autouse=True)
    @patch("synapseclient.client.Synapse")
    def setup(self, mock_syn):
        self.syn = mock_syn

    def test_get__without_file_args(self):
        parser = cmdline.build_parser()
        args = parser.parse_args(["store", "--parentid", "syn123", "--used", "syn456"])
        with pytest.raises(ValueError) as ve:
            cmdline.store(args, self.syn)
        assert str(ve.value) == "store missing required FILE argument"
