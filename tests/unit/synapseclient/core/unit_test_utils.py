# unit tests for utils.py

import base64
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from shutil import rmtree
from typing import Optional
from unittest.mock import MagicMock, Mock, call, mock_open, patch

import pytest

from synapseclient.core import constants, utils
from synapseclient.core.utils import coerce_enum_list


def test_is_url() -> None:
    """test the ability to determine whether a string is a URL"""
    assert utils.is_url("http://mydomain.com/foo/bar/bat?asdf=1234&qewr=ooo")
    assert utils.is_url("http://xkcd.com/1193/")
    assert not utils.is_url("syn123445")
    assert not utils.is_url("wasssuuuup???")
    assert utils.is_url("file://foo.com/path/to/file.xyz")
    assert utils.is_url("file:///path/to/file.xyz")
    assert utils.is_url("file:/path/to/file.xyz")
    assert utils.is_url("file:///c:/WINDOWS/clock.avi")
    assert utils.is_url("file:c:/WINDOWS/clock.avi")
    assert not utils.is_url("c:/WINDOWS/ugh/ugh.ugh")


def test_windows_file_urls() -> None:
    url = "file:///c:/WINDOWS/clock.avi"
    assert utils.is_url(url)
    assert (
        utils.file_url_to_path(url, verify_exists=False) == "c:/WINDOWS/clock.avi"
    ), utils.file_url_to_path(url)


def test_is_in_path() -> None:
    # Path as returned form syn.restGET('entity/{}/path')
    path = {
        "path": [
            {
                "id": "syn4489",
                "name": "root",
                "type": "org.sagebionetworks.repo.model.Folder",
            },
            {
                "id": "syn537704",
                "name": "my Test project",
                "type": "org.sagebionetworks.repo.model.Project",
            },
            {
                "id": "syn2385356",
                "name": ".emacs",
                "type": "org.sagebionetworks.repo.model.FileEntity",
            },
        ]
    }

    assert utils.is_in_path("syn537704", path)
    assert not utils.is_in_path("syn123", path)


def test_humanize_bytes() -> None:
    for input_bytes, expected_output in [
        (-1, "-1.0bytes"),
        (0, "0.0bytes"),
        (1, "1.0bytes"),
        (10, "10.0bytes"),
        ((2**10) - 1, "1023.0bytes"),
        ((2**10), "1.0kB"),
        ((2**20), "1.0MB"),
        ((2**20) * 1.5, "1.5MB"),
        ((2**70), "Oops larger than Exabytes"),
    ]:
        assert utils.humanizeBytes(input_bytes) == expected_output


def test_humanize_bytes_none() -> None:
    with pytest.raises(ValueError):
        utils.humanizeBytes(None)


def test_id_of() -> None:
    assert utils.id_of(1) == "1"
    assert utils.id_of("syn12345") == "syn12345"
    assert utils.id_of({"foo": 1, "id": 123}) == "123"
    pytest.raises(ValueError, utils.id_of, {"foo": 1, "idzz": 123})
    assert utils.id_of({"properties": {"id": 123}}) == "123"
    pytest.raises(ValueError, utils.id_of, {"properties": {"qq": 123}})
    pytest.raises(ValueError, utils.id_of, object())

    class Foo:
        def __init__(self, id_attr_name: str, id: str) -> None:
            self.properties = {id_attr_name: id}

    id_attr_names = ["id", "ownerId", "tableId"]

    for attr_name in id_attr_names:
        foo = Foo(attr_name, 123)
        assert utils.id_of(foo) == "123"


@pytest.mark.parametrize(
    "input_value, expected_output, expected_warning",
    [
        # Test 1: Valid inputs
        ("123", "123", None),
        (123, "123", None),
        ({"id": "222"}, "222", None),
        # Test 2: Invalid inputs that should be corrected
        (
            "123.0",
            "123",
            "Submission ID '123.0' contains decimals which are not supported",
        ),
        (
            123.0,
            "123",
            "Submission ID '123.0' contains decimals which are not supported",
        ),
        (
            {"id": "999.222"},
            "999",
            "Submission ID '999.222' contains decimals which are not supported",
        ),
    ],
)
def test_validate_submission_id(input_value, expected_output, expected_warning, caplog):
    with caplog.at_level(logging.WARNING):
        assert utils.validate_submission_id(input_value) == expected_output
        if expected_warning:
            assert expected_warning in caplog.text
        else:
            assert not caplog.text


def test_validate_submission_id_letters_input() -> None:
    letters_input = "syn123"
    expected_error = f"Submission ID '{letters_input}' is not a valid submission ID. Please use digits only."
    with pytest.raises(ValueError) as err:
        utils.validate_submission_id(letters_input)

    assert str(err.value) == expected_error


# TODO: Add a test for is_synapse_id_str(...)
# https://sagebionetworks.jira.com/browse/SYNPY-1425


def test_get_synid_and_version() -> None:
    # Test 1: Ensure that a synID string with no version works
    synid_no_version = "syn123"
    id, version = utils.get_synid_and_version(synid_no_version)
    assert id == synid_no_version
    assert version == None

    # Test 2: Ensure that a synID string with version syntax works
    synid_with_version = "syn123.2"
    id, version = utils.get_synid_and_version(synid_with_version)
    assert (id, str(version)) == tuple(synid_with_version.split("."))

    # Test 3: Ensure that a synID string with version syntax typo breaks
    synid_with_typo = "syn123.oops"
    error_msg = "The input string was not determined to be a syn ID."
    with pytest.raises(ValueError, match=error_msg):
        utils.get_synid_and_version(synid_with_typo)

    # Test 4: Ensure that a versionable Entity obj with version works
    _, version = utils.get_synid_and_version({"foo": 1, "id": 123, "versionNumber": 2})
    assert version == 2

    # Test 5: Ensure that an Entity obj with no version works
    _, version = utils.get_synid_and_version({"foo": 1, "id": 123})
    assert not version


def test_concrete_type_of() -> None:
    """Verify behavior of utils#concrete_type_of"""

    for invalid_obj in [
        "foo",  # not a Mapping
        {},  # doesn't have a concreteType or type,
        {"concreteType": object()},  # isn't a str
        {"type": object()},  # isn't a str
        {"concreteType": "foo"},  # doesn't appear to be of expected format
        {"type": "foo"},  # doesn't appear to be of expected format
    ]:
        with pytest.raises(ValueError) as ex:
            utils.concrete_type_of(invalid_obj)
        assert "Unable to determine concreteType" in str(ex)

    for value, expected_type in [
        (
            {"concreteType": constants.concrete_types.FILE_ENTITY},
            constants.concrete_types.FILE_ENTITY,
        ),
        (
            {"type": constants.concrete_types.FOLDER_ENTITY},
            constants.concrete_types.FOLDER_ENTITY,
        ),
    ]:
        assert expected_type == utils.concrete_type_of(value)


def test_guess_file_name() -> None:
    assert utils.guess_file_name("a/b") == "b"
    assert utils.guess_file_name("file:///a/b") == "b"
    assert utils.guess_file_name("A:/a/b") == "b"
    assert utils.guess_file_name("B:/a/b/") == "b"
    assert utils.guess_file_name("c:\\a\\b") == "b"
    assert utils.guess_file_name("d:\\a\\b\\") == "b"
    assert utils.guess_file_name("E:\\a/b") == "b"
    assert utils.guess_file_name("F:\\a/b/") == "b"
    assert utils.guess_file_name("/a/b") == "b"
    assert utils.guess_file_name("/a/b/") == "b"
    assert utils.guess_file_name("http://www.a.com/b") == "b"
    assert utils.guess_file_name("http://www.a.com/b/") == "b"
    assert utils.guess_file_name("http://www.a.com/b?foo=bar") == "b"
    assert utils.guess_file_name("http://www.a.com/b/?foo=bar") == "b"
    assert utils.guess_file_name("http://www.a.com/b?foo=bar&arga=barga") == "b"
    assert utils.guess_file_name("http://www.a.com/b/?foo=bar&arga=barga") == "b"


def test_extract_filename() -> None:
    assert utils.extract_filename('attachment; filename="fname.ext"') == "fname.ext"
    assert utils.extract_filename("attachment; filename=fname.ext") == "fname.ext"
    assert utils.extract_filename(None) is None
    assert utils.extract_filename(None, "fname.ext") == "fname.ext"


def test_normalize_path() -> None:
    # tests should pass on reasonable OSes and also on windows

    # resolves relative paths
    assert len(utils.normalize_path("asdf.txt")) > 8

    # doesn't resolve home directory references
    # assert '~' not in utils.normalize_path('~/asdf.txt')

    # converts back slashes to forward slashes
    assert utils.normalize_path("\\windows\\why\\why\\why.txt")

    # what's the right thing to do for None?
    assert utils.normalize_path(None) is None


def test_limit_and_offset() -> None:
    def query_params(uri):
        """Return the query params as a dict"""
        return dict([kvp.split("=") for kvp in uri.split("?")[1].split("&")])

    qp = query_params(utils._limit_and_offset("/asdf/1234", limit=10, offset=0))
    assert qp["limit"] == "10"
    assert qp["offset"] == "0"

    qp = query_params(
        utils._limit_and_offset("/asdf/1234?limit=5&offset=10", limit=25, offset=50)
    )
    assert qp["limit"] == "25"
    assert qp["offset"] == "50"
    assert len(qp) == 2

    qp = query_params(
        utils._limit_and_offset("/asdf/1234?foo=bar", limit=10, offset=30)
    )
    assert qp["limit"] == "10"
    assert qp["offset"] == "30"
    assert qp["foo"] == "bar"
    assert len(qp) == 3

    qp = query_params(utils._limit_and_offset("/asdf/1234?foo=bar&a=b", limit=10))
    assert qp["limit"] == "10"
    assert "offset" not in qp
    assert qp["foo"] == "bar"
    assert qp["a"] == "b"
    assert len(qp) == 3


def test_utils_extract_user_name() -> None:
    profile = {"firstName": "Madonna"}
    assert utils.extract_user_name(profile) == "Madonna"
    profile = {"firstName": "Oscar", "lastName": "the Grouch"}
    assert utils.extract_user_name(profile) == "Oscar the Grouch"
    profile["displayName"] = None
    assert utils.extract_user_name(profile) == "Oscar the Grouch"
    profile["displayName"] = ""
    assert utils.extract_user_name(profile) == "Oscar the Grouch"
    profile["displayName"] = "Assistant Professor Oscar the Grouch, PhD"
    assert (
        utils.extract_user_name(profile) == "Assistant Professor Oscar the Grouch, PhD"
    )
    profile["userName"] = "otg"
    assert utils.extract_user_name(profile) == "otg"


def test_is_json() -> None:
    assert utils.is_json("application/json")
    assert utils.is_json("application/json;charset=ISO-8859-1")
    assert not utils.is_json("application/flapdoodle;charset=ISO-8859-1")
    assert not utils.is_json(None)
    assert not utils.is_json("")


def test_normalize_whitespace() -> None:
    assert "zip tang pow a = 2" == utils.normalize_whitespace(
        "   zip\ttang   pow   \n    a = 2   "
    )
    result = utils.normalize_lines("   zip\ttang   pow   \n    a = 2   \n    b = 3   ")
    assert "zip tang pow\na = 2\nb = 3" == result


def test_query_limit_and_offset() -> None:
    query, limit, offset = utils.query_limit_and_offset(
        "select foo from bar where zap > 2 limit 123 offset 456"
    )
    assert query == "select foo from bar where zap > 2"
    assert limit == 123
    assert offset == 456

    query, limit, offset = utils.query_limit_and_offset(
        "select limit from offset where limit==2 limit 123 offset 456"
    )
    assert query == "select limit from offset where limit==2"
    assert limit == 123
    assert offset == 456

    query, limit, offset = utils.query_limit_and_offset(
        "select foo from bar where zap > 2 limit 123"
    )
    assert query == "select foo from bar where zap > 2"
    assert limit == 123
    assert offset == 1

    query, limit, offset = utils.query_limit_and_offset(
        "select foo from bar where zap > 2 limit 65535", hard_limit=1000
    )
    assert query == "select foo from bar where zap > 2"
    assert limit == 1000
    assert offset == 1


def test_as_urls() -> None:
    assert (
        utils.as_url("C:\\Users\\Administrator\\AppData\\Local\\Temp\\2\\tmpvixuld.txt")
        == "file:///C:/Users/Administrator/AppData/Local/Temp/2/tmpvixuld.txt"
    )
    assert utils.as_url("/foo/bar/bat/zoinks.txt") == "file:///foo/bar/bat/zoinks.txt"
    assert (
        utils.as_url("http://foo/bar/bat/zoinks.txt") == "http://foo/bar/bat/zoinks.txt"
    )
    assert (
        utils.as_url("ftp://foo/bar/bat/zoinks.txt") == "ftp://foo/bar/bat/zoinks.txt"
    )
    assert (
        utils.as_url("sftp://foo/bar/bat/zoinks.txt") == "sftp://foo/bar/bat/zoinks.txt"
    )


def test_time_manipulation() -> None:
    round_tripped_datetime = utils.datetime_to_iso(
        utils.from_unix_epoch_time_secs(
            utils.to_unix_epoch_time_secs(
                utils.iso_to_datetime("2014-12-10T19:09:34.000Z")
            )
        )
    )
    assert "2014-12-10T19:09:34.000Z" == round_tripped_datetime

    round_tripped_datetime = utils.datetime_to_iso(
        utils.from_unix_epoch_time_secs(
            utils.to_unix_epoch_time_secs(
                utils.iso_to_datetime("1969-04-28T23:48:34.123Z")
            )
        )
    )
    assert "1969-04-28T23:48:34.123Z" == round_tripped_datetime

    # check that rounding to milliseconds works
    round_tripped_datetime = utils.datetime_to_iso(
        utils.from_unix_epoch_time_secs(
            utils.to_unix_epoch_time_secs(
                utils.iso_to_datetime("1969-04-28T23:48:34.999499Z")
            )
        )
    )
    assert "1969-04-28T23:48:34.999Z" == round_tripped_datetime

    # check that rounding to milliseconds works
    round_tripped_datetime = utils.datetime_to_iso(
        utils.from_unix_epoch_time_secs(
            utils.to_unix_epoch_time_secs(
                utils.iso_to_datetime("1969-04-27T23:59:59.999999Z")
            )
        )
    )
    assert "1969-04-28T00:00:00.000Z" == round_tripped_datetime


def test_treadsafe_generator() -> None:
    @utils.threadsafe_generator
    def generate_letters():
        for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            yield c

    "".join(letter for letter in generate_letters()) == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def test_extract_synapse_id_from_query() -> None:
    assert (
        utils.extract_synapse_id_from_query("select * from syn1234567") == "syn1234567"
    )
    assert (
        utils.extract_synapse_id_from_query(
            "select * from syn1234567 where foo = 'bar'"
        )
        == "syn1234567"
    )
    assert utils.extract_synapse_id_from_query("select * from syn1") == "syn1"
    assert (
        utils.extract_synapse_id_from_query("select foo from syn99999999999")
        == "syn99999999999"
    )


def test_temp_download_filename() -> None:
    temp_destination = utils.temp_download_filename("/foo/bar/bat", 12345)
    assert temp_destination == "/foo/bar/bat.synapse_download_12345", temp_destination

    regex = r"/foo/bar/bat.synapse_download_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    assert re.match(regex, utils.temp_download_filename("/foo/bar/bat", None))


@patch("zipfile.ZipFile")
@patch("os.makedirs")
@patch("os.path.exists", return_value=False)
def test_extract_zip_file_to_directory(
    mocked_path_exists: MagicMock, mocked_makedir: MagicMock, mocked_zipfile: MagicMock
) -> None:
    file_base_name = "test.txt"
    file_dir = "some/folders/"
    target_dir = tempfile.mkdtemp()  # TODO rename
    expected_filepath = os.path.join(target_dir, file_base_name)

    try:
        # call the method and make sure correct values are being used
        with patch.object(utils, "open", mock_open(), create=True) as mocked_open:
            actual_filepath = utils.extract_zip_file_to_directory(
                mocked_zipfile, file_dir + file_base_name, target_dir
            )

            # make sure it returns the correct cache path
            assert expected_filepath == actual_filepath

            # make sure it created the cache folders
            mocked_makedir.assert_called_once_with(target_dir)

            # make sure zip was read and file was witten
            mocked_open.assert_called_once_with(expected_filepath, "wb")
            mocked_zipfile.read.assert_called_once_with(file_dir + file_base_name)
            mocked_open().write.assert_called_once_with(mocked_zipfile.read())
    finally:
        rmtree(target_dir, ignore_errors=True)


def test_snake_case():
    for input_word, expected_output in [
        ("", ""),
        ("A", "a"),
        ("a", "a"),
        ("123", "123"),
        ("PascalCase", "pascal_case"),
        ("camelCasedWord", "camel_cased_word"),
        ("camelCase_WithUnderscore", "camel_case__with_underscore"),
        ("camel123Abc", "camel123_abc"),
    ]:
        assert expected_output == utils.snake_case(input_word)


@pytest.mark.parametrize(
    "string,expected",
    [
        (None, False),
        ("", False),
        ("foo", False),
        ("foo江", False),
        # should be able to handle both byte strings and unicode strings
        (base64.b64encode(b"foo"), True),
        (base64.b64encode(b"foo").decode("utf-8"), True),
    ],
)
def test_is_base_64_encoded(string, expected):
    assert utils.is_base64_encoded(string) == expected


def test_deprecated_keyword_param():
    keywords = ["foo", "baz"]
    version = "2.1.1"
    reason = "keyword is no longer used"

    fn_return_val = "expected return"

    @utils.deprecated_keyword_param(keywords, version, reason)
    def test_fn(positional, foo=None, bar=None, baz=None):
        return fn_return_val

    with patch("warnings.warn") as mock_warn:
        return_val = test_fn("positional", foo="foo", bar="bar", baz="baz")

    assert fn_return_val == return_val
    mock_warn.assert_called_once_with(
        "Parameter(s) ['baz', 'foo'] deprecated since version 2.1.1; keyword is no longer used",
        category=DeprecationWarning,
        stacklevel=2,
    )


def test_synapse_error_msg():
    """Test the output of utils._synapse_error_message"""

    # single unchained exception
    expected = "\nValueError: test error\n\n"
    ex = ValueError("test error")
    assert expected == utils._synapse_error_msg(ex)

    # exception chain with multiple chained causes
    try:
        raise NotImplementedError("root error")
    except NotImplementedError as ex0:
        try:
            raise NameError("error 1") from ex0
        except NameError as ex1:
            try:
                raise ValueError("error 2") from ex1
            except ValueError as ex2:
                expected = """
ValueError: error 2
  caused by NameError: error 1
    caused by NotImplementedError: root error

"""  # noqa for outdenting
                assert expected == utils._synapse_error_msg(ex2)


@patch.object(utils, "hashlib")
def test_md5_for_file(mock_hashlib: MagicMock) -> None:
    """
    Verify the md5 calculation is correct, and call the callback func if it passed in as argument.
    """
    file_name = "/home/foo/bar/test.txt"
    mock_callback = Mock()
    mock_md5 = Mock()
    mock_hashlib.new.return_value = mock_md5
    with patch.object(utils, "open", mock_open(), create=True) as mocked_open:
        mocked_open.return_value.read.side_effect = ["data1", "data2", None]
        utils.md5_for_file(file_name, callback=mock_callback)

        mocked_open.assert_called_once_with(file_name, "rb")
        assert mock_md5.update.call_args_list == [call("data1"), call("data2")]
        mock_callback.call_count == 3


class TestSpinner:
    """
    Verify the Spinner object work correctly
    """

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self) -> None:
        self.msg = "test_msg"
        self.spinner = utils.Spinner(self.msg)

    @patch.object(utils, "sys")
    def test_print_tick_is_atty(self, mock_sys: MagicMock) -> None:
        """
        assume the sys.stdin.isatty is True, verify the sys.stdout.write will call once if print_tick is called.
        """
        mock_sys.stdin.isatty.return_value = True
        signs = ["|", "/", "-", "\\"]

        assert self.spinner._tick == 0
        self.spinner.print_tick()
        mock_sys.stdout.write.assert_called_once_with(f"\r {signs[0]} {self.msg}")

        assert self.spinner._tick == 1
        self.spinner.print_tick()
        mock_sys.stdout.write.assert_called_with(f"\r {signs[1]} {self.msg}")

        assert self.spinner._tick == 2
        self.spinner.print_tick()
        mock_sys.stdout.write.assert_called_with(f"\r {signs[2]} {self.msg}")

        assert self.spinner._tick == 3
        self.spinner.print_tick()
        mock_sys.stdout.write.assert_called_with(f"\r {signs[3]} {self.msg}")

        mock_sys.stdout.flush.call_count == 4

    @patch.object(utils, "sys")
    def test_print_tick_is_not_atty(self, mock_sys: MagicMock) -> None:
        """
        assume the sys.stdin.isatty is False,
        verify the sys.stdout won't be called.
        """
        mock_sys.stdin.isatty.return_value = False

        self.spinner.print_tick()
        mock_sys.stdout.write.assert_not_called()
        mock_sys.stdout.flush.assert_not_called()
        assert self.spinner._tick == 1


class _Color(Enum):
    RED = "RED"
    BLUE = "BLUE"


class TestCoerceEnumList:
    """Tests for coerce_enum_list."""

    @pytest.mark.parametrize(
        "input_filter,expected",
        [
            ([_Color.RED], ["RED"]),
            ([_Color.BLUE], ["BLUE"]),
            ([_Color.RED, _Color.BLUE], ["RED", "BLUE"]),
            (["RED"], ["RED"]),
            (["BLUE"], ["BLUE"]),
            ([_Color.RED, "BLUE"], ["RED", "BLUE"]),
            ([], []),
        ],
    )
    def test_valid_inputs(self, input_filter, expected) -> None:
        """Accepts enum members, matching strings, mixed lists, and empty lists."""
        assert coerce_enum_list(_Color, input_filter) == expected

    @pytest.mark.parametrize(
        "input_filter,match",
        [
            (["NOT_A_COLOR"], "Invalid value"),
            ([42], "Invalid value"),
            (["red"], "Invalid value"),
        ],
    )
    def test_invalid_inputs_raise_value_error(self, input_filter, match) -> None:
        """Raises ValueError for unrecognized strings and non-string, non-enum values."""
        with pytest.raises(ValueError, match=match):
            coerce_enum_list(_Color, input_filter)


@dataclass
class _SimpleEntity:
    """A minimal dataclass with scalar fields only."""

    id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None


@dataclass
class _AnnotatedEntity:
    """A dataclass with an 'annotations' dict field."""

    id: Optional[str] = None
    annotations: dict = field(default_factory=dict)


@dataclass
class _ColumnLike:
    """A column-like dataclass with scalar fields only."""

    id: Optional[str] = None
    name: Optional[str] = None
    column_type: Optional[str] = None


@dataclass
class _TableLike:
    """A dataclass with a 'columns' dict-of-dataclasses field (special-cased)."""

    id: Optional[str] = None
    columns: dict = field(default_factory=dict)


@dataclass
class _EntityRefLike:
    """An item-like dataclass exposing an 'id' attribute."""

    id: Optional[str] = None
    version: Optional[int] = None


@dataclass
class _CollectionLike:
    """A dataclass with an 'items' list-of-dataclasses field (special-cased)."""

    id: Optional[str] = None
    items: list = field(default_factory=list)


@dataclass
class _NestedProperties:
    """A nested dataclass, mirroring CurationTask's task_properties."""

    record_set_id: Optional[str] = None
    note: Optional[str] = None


@dataclass
class _DifferentNestedProperties:
    """A different nested dataclass type that shares the 'note' attribute with
    _NestedProperties but has its own distinct field."""

    other_id: Optional[str] = None
    note: Optional[str] = None


@dataclass
class _EntityWithProperties:
    """A dataclass whose 'properties' field holds another dataclass instance."""

    id: Optional[str] = None
    properties: Optional[_NestedProperties] = None


class TestMergeDataclassEntities:
    """Tests for utils.merge_dataclass_entities."""

    def test_returns_destination_instance(self) -> None:
        """The destination object itself is returned (merged in place)."""
        # GIVEN a source and destination entity with no missing values
        source: _SimpleEntity = _SimpleEntity(id="s")
        destination: _SimpleEntity = _SimpleEntity(name="d")
        # WHEN I merge them
        result: _SimpleEntity = utils.merge_dataclass_entities(
            source=source, destination=destination
        )
        # THEN the returned object is the same destination instance
        assert result is destination

    @pytest.mark.parametrize(
        "source, destination, kwargs, expected",
        [
            # Gap-fill: None destination fields are filled from source, while a
            # field set on both is a conflict and keeps the destination value.
            (
                _SimpleEntity(id="s-id", name="s-name", description="s-desc"),
                _SimpleEntity(id=None, name=None, description="d-desc"),
                {},
                {"id": "s-id", "name": "s-name", "description": "d-desc"},
            ),
            # Conflict: both have a non-None value, so the destination wins.
            (
                _SimpleEntity(id="s-id", name="s-name"),
                _SimpleEntity(id="d-id", name="d-name"),
                {},
                {"id": "d-id", "name": "d-name"},
            ),
        ],
        ids=["gap_fill", "conflict_keeps_destination"],
    )
    def test_scalar_merge_outcomes(
        self,
        source: _SimpleEntity,
        destination: _SimpleEntity,
        kwargs: dict,
        expected: dict,
    ) -> None:
        """Scalar fields gap-fill from source, keep destination on conflict, and
        respect fields_to_ignore."""
        # GIVEN source and destination entities and optional merge kwargs
        # WHEN I merge them
        result: _SimpleEntity = utils.merge_dataclass_entities(
            source=source, destination=destination, **kwargs
        )
        # THEN each field matches the expected outcome
        for attr, value in expected.items():
            assert getattr(result, attr) == value

    def test_annotations_merge_destination_wins_on_conflict(self) -> None:
        """Annotations dicts are merged with destination keys overriding source."""
        # GIVEN a source with some annotations and a destination with overlapping
        # and distinct annotation keys
        source = _AnnotatedEntity(annotations={"shared": ["src"], "only_source": [1]})
        destination = _AnnotatedEntity(
            annotations={"shared": ["dest"], "only_dest": [2]}
        )
        # WHEN I merge them
        result: _AnnotatedEntity = utils.merge_dataclass_entities(
            source=source, destination=destination
        )
        # THEN the destination key wins on conflict, source-only keys are added,
        # and destination-only keys are retained
        assert result.annotations == {
            "shared": ["dest"],
            "only_source": [1],
            "only_dest": [2],
        }

    def test_columns_merge_adds_new_and_merges_existing(self) -> None:
        """New source columns are added; existing columns recurse-merge with id kept."""
        # GIVEN a source with two columns and a destination with one overlapping column
        source = _TableLike(
            columns={
                "col1": _ColumnLike(id="s1", name="col1", column_type="STRING"),
                "col2": _ColumnLike(id="s2", name="col2", column_type="INTEGER"),
            }
        )
        destination = _TableLike(
            columns={
                "col1": _ColumnLike(id="d1", name="col1", column_type=None),
            }
        )
        # WHEN I merge them
        result: _TableLike = utils.merge_dataclass_entities(
            source=source, destination=destination
        )
        # THEN the new source column is added wholesale
        assert "col2" in result.columns
        assert result.columns["col2"].id == "s2"

        # AND the existing column is recurse-merged: destination id is kept and
        # the None column_type is gap-filled from source
        merged_col1: _ColumnLike = result.columns["col1"]
        assert merged_col1.id == "d1"
        assert merged_col1.column_type == "STRING"

    def test_items_merge_appends_only_new_ids(self) -> None:
        """Source items are appended only when their id is not already present."""
        # GIVEN a source with a duplicate item id and a new item id, and a
        # destination already containing the duplicate id
        source = _CollectionLike(
            items=[
                _EntityRefLike(id="syn1", version=2),
                _EntityRefLike(id="syn2", version=1),
            ]
        )
        destination = _CollectionLike(items=[_EntityRefLike(id="syn1", version=1)])
        # WHEN I merge them
        result: _CollectionLike = utils.merge_dataclass_entities(
            source=source, destination=destination
        )
        # THEN only the new id is appended and the pre-existing item is untouched
        ids = [item.id for item in result.items]
        assert ids == ["syn1", "syn2"]
        syn1: _EntityRefLike = next(item for item in result.items if item.id == "syn1")
        assert syn1.version == 1

    def test_fields_to_preserve_from_source(self) -> None:
        """A preserved field always takes the source value."""
        # GIVEN a source and destination whose 'id' differs based on the parameter,
        # and 'id' is listed in fields_to_preserve_from_source
        source = _SimpleEntity(id="server-id", name="server-name")
        destination = _SimpleEntity(id="xxx", name="user-name")
        # WHEN I merge with fields_to_preserve_from_source=["id"]
        result: _SimpleEntity = utils.merge_dataclass_entities(
            source=source,
            destination=destination,
            fields_to_preserve_from_source=["id"],
        )
        # THEN 'id' is forced to the source value regardless of destination
        assert result.id == "server-id"
        # AND 'name' is not preserved so the normal merge applies (destination wins)
        assert result.name == "user-name"

    @pytest.mark.parametrize(
        "destination_id, expected_id",
        [
            # Destination is None: would normally gap-fill from source, but
            # fields_to_ignore prevents the copy, so it stays None.
            (None, None),
            # Destination has a value: would normally keep it anyway (destination
            # wins on conflict), but fields_to_ignore is what enforces the skip.
            ("d-id", "d-id"),
        ],
        ids=["prevents_gap_fill", "prevents_source_copy_on_conflict"],
    )
    def test_fields_to_ignore(
        self,
        destination_id: Optional[str],
        expected_id: Optional[str],
    ) -> None:
        """An ignored field is never copied from source, regardless of whether the
        destination's value is None or already set."""
        # GIVEN a source with a value for 'id' and a destination whose 'id' is
        # either None or set, with 'id' listed in fields_to_ignore
        source = _SimpleEntity(id="s-id", name="s-name")
        destination = _SimpleEntity(id=destination_id, name="d-name")

        # WHEN I merge with fields_to_ignore=["id"]
        result: _SimpleEntity = utils.merge_dataclass_entities(
            source=source, destination=destination, fields_to_ignore=["id"]
        )

        # THEN 'id' is never touched regardless of its starting value
        assert result.id == expected_id
        # AND non-ignored fields merge normally (destination wins on conflict)
        assert result.name == "d-name"

    def test_nested_dataclass_field_kept_when_source_is_none(self) -> None:
        """When the source's nested dataclass field is None, the destination's
        existing dataclass value is preserved unchanged."""
        # GIVEN a source with a None nested dataclass field and a destination
        # with a populated nested dataclass
        source = _EntityWithProperties(id=None, properties=None)
        destination = _EntityWithProperties(
            id="syn1", properties=_NestedProperties(record_set_id="synKEEP")
        )

        # WHEN I merge them
        result: _EntityWithProperties = utils.merge_dataclass_entities(
            source=source, destination=destination
        )

        # THEN the destination's nested dataclass is preserved unchanged
        assert result.properties is not None
        assert result.properties.record_set_id == "synKEEP"

    def test_nested_dataclass_field_keeps_destination_on_conflict(self) -> None:
        """A nested dataclass set on both source and destination follows the same
        'destination wins on conflict' rule as scalar fields."""
        # GIVEN a source with a stale nested dataclass and a destination with a
        # newer value for the same field (the CurationTask.task_properties scenario)
        source = _EntityWithProperties(
            id=None, properties=_NestedProperties(record_set_id="synOLD")
        )
        destination = _EntityWithProperties(
            id="syn1", properties=_NestedProperties(record_set_id="synNEW")
        )

        # WHEN I merge them
        result: _EntityWithProperties = utils.merge_dataclass_entities(
            source=source, destination=destination
        )

        # THEN the destination's value is preserved (destination wins on conflict)
        assert result.properties.record_set_id == "synNEW"

    def test_nested_dataclass_field_recurse_merges_subfields(self) -> None:
        """When both source and destination hold a nested dataclass, each sub-field
        follows the normal rule: destination wins on conflict, None is gap-filled."""
        # GIVEN a source whose nested dataclass has a stale conflicting sub-field
        # and a non-None note, and a destination whose nested dataclass has a new
        # conflicting sub-field and a None note
        source = _EntityWithProperties(
            properties=_NestedProperties(record_set_id="synOLD", note="from-source"),
        )
        destination = _EntityWithProperties(
            id="syn1",
            properties=_NestedProperties(record_set_id="synNEW", note=None),
        )
        # WHEN I merge them
        result: _EntityWithProperties = utils.merge_dataclass_entities(
            source=source, destination=destination
        )
        # THEN the conflicting sub-field keeps the destination's value
        assert result.properties.record_set_id == "synNEW"
        # AND the None sub-field on the destination is gap-filled from the source
        assert result.properties.note == "from-source"

    def test_nested_dataclass_source_wins_when_destination_field_is_not_a_dataclass(
        self,
    ) -> None:
        """When the source field is a dataclass but the destination field holds a
        non-dataclass non-None value (e.g. a plain string from a type mismatch),
        the source value wins rather than raising TypeError inside merge_dataclass_entities.
        """
        # GIVEN a source whose 'properties' field is a proper dataclass instance and
        # a destination whose 'properties' field has been set to a plain string
        # (simulating a type-mismatch scenario)
        source = _EntityWithProperties(
            id="syn1", properties=_NestedProperties(record_set_id="synNEW")
        )
        destination = _EntityWithProperties(id="syn1", properties=None)
        # Force the destination field to a non-None, non-dataclass value directly so
        # the type annotation is bypassed
        object.__setattr__(destination, "properties", "not-a-dataclass")

        # WHEN I merge them
        result: _EntityWithProperties = utils.merge_dataclass_entities(
            source=source, destination=destination
        )

        # THEN the source dataclass value is used rather than crashing
        assert result.properties is source.properties

    def test_nested_dataclass_field_recurse_merges_across_different_types(
        self,
    ) -> None:
        """When source and destination hold different dataclass types that share
        overlapping field names, the merge still does NOT field-by-field: the
        destination instance is maintained."""
        # GIVEN a source and destination whose 'properties' fields are different
        # dataclass types that happen to share the 'note' attribute
        source = _EntityWithProperties(
            properties=_NestedProperties(record_set_id="synOLD", note="from-source"),
        )
        destination = _EntityWithProperties(
            id="syn1",
            properties=_DifferentNestedProperties(other_id="other", note=None),
        )

        # WHEN I merge them
        result: _EntityWithProperties = utils.merge_dataclass_entities(
            source=source, destination=destination
        )

        # THEN the destination instance (of its original type) is returned
        assert result.properties is destination.properties
        assert isinstance(result.properties, _DifferentNestedProperties)
        # AND the destination-only field is retained
        assert result.properties.other_id == "other"
        # AND the overlapping None field is NOT gap-filled from the source
        assert result.properties.note is None
        # AND the source-only field is NOT grafted onto the destination instance
        assert not hasattr(result.properties, "record_set_id")
