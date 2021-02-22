# unit tests for utils.py

import base64
import os
import re
from shutil import rmtree
import tempfile

import pytest
from unittest.mock import patch, mock_open, Mock, call

from synapseclient.core import constants, utils


def test_is_url():
    """test the ability to determine whether a string is a URL"""
    assert utils.is_url("http://mydomain.com/foo/bar/bat?asdf=1234&qewr=ooo")
    assert utils.is_url("http://xkcd.com/1193/")
    assert not utils.is_url("syn123445")
    assert not utils.is_url("wasssuuuup???")
    assert utils.is_url('file://foo.com/path/to/file.xyz')
    assert utils.is_url('file:///path/to/file.xyz')
    assert utils.is_url('file:/path/to/file.xyz')
    assert utils.is_url('file:///c:/WINDOWS/clock.avi')
    assert utils.is_url('file:c:/WINDOWS/clock.avi')
    assert not utils.is_url('c:/WINDOWS/ugh/ugh.ugh')


def test_windows_file_urls():
    url = 'file:///c:/WINDOWS/clock.avi'
    assert utils.is_url(url)
    assert utils.file_url_to_path(url, verify_exists=False) == 'c:/WINDOWS/clock.avi', utils.file_url_to_path(url)


def test_is_in_path():
    # Path as returned form syn.restGET('entity/{}/path')
    path = {u'path': [{u'id': u'syn4489',  u'name': u'root', u'type': u'org.sagebionetworks.repo.model.Folder'},
                      {u'id': u'syn537704', u'name': u'my Test project',
                       u'type': u'org.sagebionetworks.repo.model.Project'},
                      {u'id': u'syn2385356', u'name': u'.emacs',
                       u'type': u'org.sagebionetworks.repo.model.FileEntity'}]}

    assert utils.is_in_path('syn537704', path)
    assert not utils.is_in_path('syn123', path)


def test_humanizeBytes():
    for (input_bytes, expected_output) in [
        (-1, '-1.0bytes'),
        (0, '0.0bytes'),
        (1, '1.0bytes'),
        (10, '10.0bytes'),
        ((2 ** 10) - 1, '1023.0bytes'),
        ((2 ** 10), '1.0kB'),
        ((2 ** 20), '1.0MB'),
        ((2 ** 20) * 1.5, '1.5MB'),
        ((2 ** 70), 'Oops larger than Exabytes'),
    ]:
        assert utils.humanizeBytes(input_bytes) == expected_output


def test_humanizeBytes__None():
    with pytest.raises(ValueError):
        utils.humanizeBytes(None)


def test_id_of():
    assert utils.id_of(1) == '1'
    assert utils.id_of('syn12345') == 'syn12345'
    assert utils.id_of({'foo': 1, 'id': 123}) == '123'
    pytest.raises(ValueError, utils.id_of, {'foo': 1, 'idzz': 123})
    assert utils.id_of({'properties': {'id': 123}}) == '123'
    pytest.raises(ValueError, utils.id_of, {'properties': {'qq': 123}})
    pytest.raises(ValueError, utils.id_of, object())

    class Foo:
        def __init__(self, id_attr_name, id):
            self.properties = {id_attr_name: id}

    id_attr_names = ['id', 'ownerId', 'tableId']

    for attr_name in id_attr_names:
        foo = Foo(attr_name, 123)
        assert utils.id_of(foo) == '123'


def test_concrete_type_of():
    """Verify behavior of utils#concrete_type_of"""

    for invalid_obj in [
        'foo',  # not a Mapping
        {},  # doesn't have a concreteType or type,
        {'concreteType': object()},  # isn't a str
        {'type': object()},  # isn't a str
        {'concreteType': 'foo'},  # doesn't appear to be of expected format
        {'type': 'foo'},  # doesn't appear to be of expected format
    ]:
        with pytest.raises(ValueError) as ex:
            utils.concrete_type_of(invalid_obj)
        assert 'Unable to determine concreteType' in str(ex)

    for value, expected_type in [
        ({'concreteType': constants.concrete_types.FILE_ENTITY}, constants.concrete_types.FILE_ENTITY),
        ({'type': constants.concrete_types.FOLDER_ENTITY}, constants.concrete_types.FOLDER_ENTITY),
    ]:
        assert expected_type == utils.concrete_type_of(value)


def test_guess_file_name():
    assert utils.guess_file_name('a/b') == 'b'
    assert utils.guess_file_name('file:///a/b') == 'b'
    assert utils.guess_file_name('A:/a/b') == 'b'
    assert utils.guess_file_name('B:/a/b/') == 'b'
    assert utils.guess_file_name('c:\\a\\b') == 'b'
    assert utils.guess_file_name('d:\\a\\b\\') == 'b'
    assert utils.guess_file_name('E:\\a/b') == 'b'
    assert utils.guess_file_name('F:\\a/b/') == 'b'
    assert utils.guess_file_name('/a/b') == 'b'
    assert utils.guess_file_name('/a/b/') == 'b'
    assert utils.guess_file_name('http://www.a.com/b') == 'b'
    assert utils.guess_file_name('http://www.a.com/b/') == 'b'
    assert utils.guess_file_name('http://www.a.com/b?foo=bar') == 'b'
    assert utils.guess_file_name('http://www.a.com/b/?foo=bar') == 'b'
    assert utils.guess_file_name('http://www.a.com/b?foo=bar&arga=barga') == 'b'
    assert utils.guess_file_name('http://www.a.com/b/?foo=bar&arga=barga') == 'b'


def test_extract_filename():
    assert utils.extract_filename('attachment; filename="fname.ext"') == "fname.ext"
    assert utils.extract_filename('attachment; filename=fname.ext') == "fname.ext"
    assert utils.extract_filename(None) is None
    assert utils.extract_filename(None, "fname.ext") == "fname.ext"


def test_version_check():
    from synapseclient.core.version_check import _version_tuple
    assert _version_tuple('0.5.1.dev200', levels=2) == ('0', '5')
    assert _version_tuple('0.5.1.dev200', levels=3) == ('0', '5', '1')
    assert _version_tuple('1.6', levels=3) == ('1', '6', '0')


def test_normalize_path():
    # tests should pass on reasonable OSes and also on windows

    # resolves relative paths
    assert len(utils.normalize_path('asdf.txt')) > 8

    # doesn't resolve home directory references
    # assert '~' not in utils.normalize_path('~/asdf.txt')

    # converts back slashes to forward slashes
    assert utils.normalize_path('\\windows\\why\\why\\why.txt')

    # what's the right thing to do for None?
    assert utils.normalize_path(None) is None


def test_limit_and_offset():
    def query_params(uri):
        """Return the query params as a dict"""
        return dict([kvp.split('=') for kvp in uri.split('?')[1].split('&')])

    qp = query_params(utils._limit_and_offset('/asdf/1234', limit=10, offset=0))
    assert qp['limit'] == '10'
    assert qp['offset'] == '0'

    qp = query_params(utils._limit_and_offset('/asdf/1234?limit=5&offset=10', limit=25, offset=50))
    assert qp['limit'] == '25'
    assert qp['offset'] == '50'
    assert len(qp) == 2

    qp = query_params(utils._limit_and_offset('/asdf/1234?foo=bar', limit=10, offset=30))
    assert qp['limit'] == '10'
    assert qp['offset'] == '30'
    assert qp['foo'] == 'bar'
    assert len(qp) == 3

    qp = query_params(utils._limit_and_offset('/asdf/1234?foo=bar&a=b', limit=10))
    assert qp['limit'] == '10'
    assert 'offset' not in qp
    assert qp['foo'] == 'bar'
    assert qp['a'] == 'b'
    assert len(qp) == 3


def test_utils_extract_user_name():
    profile = {'firstName': 'Madonna'}
    assert utils.extract_user_name(profile) == 'Madonna'
    profile = {'firstName': 'Oscar', 'lastName': 'the Grouch'}
    assert utils.extract_user_name(profile) == 'Oscar the Grouch'
    profile['displayName'] = None
    assert utils.extract_user_name(profile) == 'Oscar the Grouch'
    profile['displayName'] = ''
    assert utils.extract_user_name(profile) == 'Oscar the Grouch'
    profile['displayName'] = 'Assistant Professor Oscar the Grouch, PhD'
    assert utils.extract_user_name(profile) == 'Assistant Professor Oscar the Grouch, PhD'
    profile['userName'] = 'otg'
    assert utils.extract_user_name(profile) == 'otg'


def test_is_json():
    assert utils.is_json('application/json')
    assert utils.is_json('application/json;charset=ISO-8859-1')
    assert not utils.is_json('application/flapdoodle;charset=ISO-8859-1')
    assert not utils.is_json(None)
    assert not utils.is_json('')


def test_normalize_whitespace():
    assert "zip tang pow a = 2" == utils.normalize_whitespace("   zip\ttang   pow   \n    a = 2   ")
    result = utils.normalize_lines("   zip\ttang   pow   \n    a = 2   \n    b = 3   ")
    assert "zip tang pow\na = 2\nb = 3" == result


def test_query_limit_and_offset():
    query, limit, offset = utils.query_limit_and_offset("select foo from bar where zap > 2 limit 123 offset 456")
    assert query == "select foo from bar where zap > 2"
    assert limit == 123
    assert offset == 456

    query, limit, offset = utils.query_limit_and_offset(
        "select limit from offset where limit==2 limit 123 offset 456")
    assert query == "select limit from offset where limit==2"
    assert limit == 123
    assert offset == 456

    query, limit, offset = utils.query_limit_and_offset("select foo from bar where zap > 2 limit 123")
    assert query == "select foo from bar where zap > 2"
    assert limit == 123
    assert offset == 1

    query, limit, offset = utils.query_limit_and_offset("select foo from bar where zap > 2 limit 65535",
                                                        hard_limit=1000)
    assert query == "select foo from bar where zap > 2"
    assert limit == 1000
    assert offset == 1


def test_as_urls():
    assert (utils.as_url("C:\\Users\\Administrator\\AppData\\Local\\Temp\\2\\tmpvixuld.txt") ==
            "file:///C:/Users/Administrator/AppData/Local/Temp/2/tmpvixuld.txt")
    assert utils.as_url("/foo/bar/bat/zoinks.txt") == "file:///foo/bar/bat/zoinks.txt"
    assert utils.as_url("http://foo/bar/bat/zoinks.txt") == "http://foo/bar/bat/zoinks.txt"
    assert utils.as_url("ftp://foo/bar/bat/zoinks.txt") == "ftp://foo/bar/bat/zoinks.txt"
    assert utils.as_url("sftp://foo/bar/bat/zoinks.txt") == "sftp://foo/bar/bat/zoinks.txt"


def test_time_manipulation():
    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("2014-12-10T19:09:34.000Z"))))
    assert "2014-12-10T19:09:34.000Z" == round_tripped_datetime

    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("1969-04-28T23:48:34.123Z"))))
    assert "1969-04-28T23:48:34.123Z" == round_tripped_datetime

    # check that rounding to milliseconds works
    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("1969-04-28T23:48:34.999499Z"))))
    assert "1969-04-28T23:48:34.999Z" == round_tripped_datetime

    # check that rounding to milliseconds works
    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("1969-04-27T23:59:59.999999Z"))))
    assert "1969-04-28T00:00:00.000Z" == round_tripped_datetime


def test_treadsafe_generator():
    @utils.threadsafe_generator
    def generate_letters():
        for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            yield c

    "".join(letter for letter in generate_letters()) == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def test_extract_synapse_id_from_query():
    assert utils.extract_synapse_id_from_query("select * from syn1234567") == "syn1234567"
    assert utils.extract_synapse_id_from_query("select * from syn1234567 where foo = 'bar'") == "syn1234567"
    assert utils.extract_synapse_id_from_query("select * from syn1") == "syn1"
    assert utils.extract_synapse_id_from_query("select foo from syn99999999999") == "syn99999999999"


def test_temp_download_filename():
    temp_destination = utils.temp_download_filename("/foo/bar/bat", 12345)
    assert temp_destination == "/foo/bar/bat.synapse_download_12345", temp_destination

    regex = r'/foo/bar/bat.synapse_download_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    assert re.match(regex, utils.temp_download_filename("/foo/bar/bat", None))


@patch('zipfile.ZipFile')
@patch('os.makedirs')
@patch('os.path.exists', return_value=False)
def test_extract_zip_file_to_directory(mocked_path_exists, mocked_makedir, mocked_zipfile):
    file_base_name = 'test.txt'
    file_dir = 'some/folders/'
    target_dir = tempfile.mkdtemp()  # TODO rename
    expected_filepath = os.path.join(target_dir, file_base_name)

    try:
        # call the method and make sure correct values are being used
        with patch.object(utils, 'open', mock_open(), create=True) as mocked_open:
            actual_filepath = utils.extract_zip_file_to_directory(mocked_zipfile, file_dir + file_base_name,
                                                                  target_dir)

            # make sure it returns the correct cache path
            assert expected_filepath == actual_filepath

            # make sure it created the cache folders
            mocked_makedir.assert_called_once_with(target_dir)

            # make sure zip was read and file was witten
            mocked_open.assert_called_once_with(expected_filepath, 'wb')
            mocked_zipfile.read.assert_called_once_with(file_dir + file_base_name)
            mocked_open().write.assert_called_once_with(mocked_zipfile.read())
    finally:
        rmtree(target_dir, ignore_errors=True)


def test_snake_case():
    for (input_word, expected_output) in [
        ('', ''),
        ('A', 'a'),
        ('a', 'a'),
        ('123', '123'),
        ('PascalCase', 'pascal_case'),
        ('camelCasedWord', 'camel_cased_word'),
        ('camelCase_WithUnderscore', 'camel_case__with_underscore'),
        ('camel123Abc', 'camel123_abc'),
    ]:
        assert expected_output == utils.snake_case(input_word)


@pytest.mark.parametrize(
    "string,expected",
    [
        (None, False),
        ('', False),

        ('foo', False),
        ('foo江', False),

        # should be able to handle both byte strings and unicode strings
        (base64.b64encode(b'foo'), True),
        (base64.b64encode(b'foo').decode('utf-8'), True),
    ]
)
def test_is_base_64_encoded(string, expected):
    assert utils.is_base64_encoded(string) == expected


def test_deprecated_keyword_param():

    keywords = ['foo', 'baz']
    version = '2.1.1'
    reason = "keyword is no longer used"

    fn_return_val = 'expected return'

    @utils.deprecated_keyword_param(keywords, version, reason)
    def test_fn(positional, foo=None, bar=None, baz=None):
        return fn_return_val

    with patch('warnings.warn') as mock_warn:
        return_val = test_fn('positional', foo='foo', bar='bar', baz='baz')

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
        raise NotImplementedError('root error')
    except NotImplementedError as ex0:
        try:
            raise NameError('error 1') from ex0
        except NameError as ex1:
            try:
                raise ValueError('error 2') from ex1
            except ValueError as ex2:
                expected = \
"""
ValueError: error 2
  caused by NameError: error 1
    caused by NotImplementedError: root error

"""  # noqa for outdenting
                assert expected == utils._synapse_error_msg(ex2)


@patch.object(utils, 'hashlib')
def test_md5_for_file(mock_hashlib):
    """
    Verify the md5 calculation is correct, and call the callback func if it passed in as argument.
    """
    file_name = '/home/foo/bar/test.txt'
    mock_callback = Mock()
    mock_md5 = Mock()
    mock_hashlib.md5.return_value = mock_md5
    with patch.object(utils, 'open', mock_open(), create=True) as mocked_open:
        mocked_open.return_value.read.side_effect = ['data1', 'data2', None]
        utils.md5_for_file(file_name, callback=mock_callback)

        mocked_open.assert_called_once_with(file_name, 'rb')
        assert mock_md5.update.call_args_list == [call('data1'), call('data2')]
        mock_callback.call_count == 3


class TestSpinner:
    """
    Verify the Spinner object work correctly
    """
    def setup(self):
        self.msg = "test_msg"
        self.spinner = utils.Spinner(self.msg)

    @patch.object(utils, 'sys')
    def test_print_tick_is_atty(self, mock_sys):
        """
        assume the sys.stdin.isatty is True, verify the sys.stdout.write will call once if print_tick is called.
        """
        mock_sys.stdin.isatty.return_value = True
        signs = ['|', '/', '-', '\\']

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

    @patch.object(utils, 'sys')
    def test_print_tick_is_not_atty(self, mock_sys):
        """
        assume the sys.stdin.isatty is False,
        verify the sys.stdout won't be called.
        """
        mock_sys.stdin.isatty.return_value = False

        self.spinner.print_tick()
        mock_sys.stdout.write.assert_not_called()
        mock_sys.stdout.flush.assert_not_called()
        assert self.spinner._tick == 1
