# unit tests for utils.py

import os
import re
from unittest.mock import patch, mock_open
import tempfile
from shutil import rmtree

from nose.tools import assert_raises, assert_equals, assert_false, assert_true, assert_is_none

from synapseclient.core import utils


def test_is_url():
    """test the ability to determine whether a string is a URL"""
    assert_true(utils.is_url("http://mydomain.com/foo/bar/bat?asdf=1234&qewr=ooo"))
    assert_true(utils.is_url("http://xkcd.com/1193/"))
    assert_false(utils.is_url("syn123445"))
    assert_false(utils.is_url("wasssuuuup???"))
    assert_true(utils.is_url('file://foo.com/path/to/file.xyz'))
    assert_true(utils.is_url('file:///path/to/file.xyz'))
    assert_true(utils.is_url('file:/path/to/file.xyz'))
    assert_true(utils.is_url('file:///c:/WINDOWS/clock.avi'))
    assert_true(utils.is_url('file:c:/WINDOWS/clock.avi'))
    assert_false(utils.is_url('c:/WINDOWS/ugh/ugh.ugh'))


def test_windows_file_urls():
    url = 'file:///c:/WINDOWS/clock.avi'
    assert_true(utils.is_url(url))
    assert_equals(utils.file_url_to_path(url, verify_exists=False), 'c:/WINDOWS/clock.avi', utils.file_url_to_path(url))


def test_is_in_path():
    # Path as returned form syn.restGET('entity/{}/path')
    path = {u'path': [{u'id': u'syn4489',  u'name': u'root', u'type': u'org.sagebionetworks.repo.model.Folder'},
                      {u'id': u'syn537704', u'name': u'my Test project',
                       u'type': u'org.sagebionetworks.repo.model.Project'},
                      {u'id': u'syn2385356', u'name': u'.emacs',
                       u'type': u'org.sagebionetworks.repo.model.FileEntity'}]}

    assert_true(utils.is_in_path('syn537704', path))
    assert_false(utils.is_in_path('syn123', path))


def test_id_of():
    assert_equals(utils.id_of(1), '1')
    assert_equals(utils.id_of('syn12345'), 'syn12345')
    assert_equals(utils.id_of({'foo': 1, 'id': 123}), '123')
    assert_raises(ValueError, utils.id_of, {'foo': 1, 'idzz': 123})
    assert_equals(utils.id_of({'properties': {'id': 123}}), '123')
    assert_raises(ValueError, utils.id_of, {'properties': {'qq': 123}})
    assert_raises(ValueError, utils.id_of, object())

    class Foo:
        def __init__(self, id_attr_name, id):
            self.properties = {id_attr_name: id}

    id_attr_names = ['id', 'ownerId', 'tableId']

    for attr_name in id_attr_names:
        foo = Foo(attr_name, 123)
        assert_equals(utils.id_of(foo), '123')


def test_guess_file_name():
    assert_equals(utils.guess_file_name('a/b'), 'b')
    assert_equals(utils.guess_file_name('file:///a/b'), 'b')
    assert_equals(utils.guess_file_name('A:/a/b'), 'b')
    assert_equals(utils.guess_file_name('B:/a/b/'), 'b')
    assert_equals(utils.guess_file_name('c:\\a\\b'), 'b')
    assert_equals(utils.guess_file_name('d:\\a\\b\\'), 'b')
    assert_equals(utils.guess_file_name('E:\\a/b'), 'b')
    assert_equals(utils.guess_file_name('F:\\a/b/'), 'b')
    assert_equals(utils.guess_file_name('/a/b'), 'b')
    assert_equals(utils.guess_file_name('/a/b/'), 'b')
    assert_equals(utils.guess_file_name('http://www.a.com/b'), 'b')
    assert_equals(utils.guess_file_name('http://www.a.com/b/'), 'b')
    assert_equals(utils.guess_file_name('http://www.a.com/b?foo=bar'), 'b')
    assert_equals(utils.guess_file_name('http://www.a.com/b/?foo=bar'), 'b')
    assert_equals(utils.guess_file_name('http://www.a.com/b?foo=bar&arga=barga'), 'b')
    assert_equals(utils.guess_file_name('http://www.a.com/b/?foo=bar&arga=barga'), 'b')


def test_extract_filename():
    assert_equals(utils.extract_filename('attachment; filename="fname.ext"'), "fname.ext")
    assert_equals(utils.extract_filename('attachment; filename=fname.ext'), "fname.ext")
    assert_is_none(utils.extract_filename(None))
    assert_equals(utils.extract_filename(None, "fname.ext"), "fname.ext")


def test_version_check():
    from synapseclient.core.version_check import _version_tuple
    assert_equals(_version_tuple('0.5.1.dev200', levels=2), ('0', '5'))
    assert_equals(_version_tuple('0.5.1.dev200', levels=3), ('0', '5', '1'))
    assert_equals(_version_tuple('1.6', levels=3), ('1', '6', '0'))


def test_normalize_path():
    # tests should pass on reasonable OSes and also on windows

    # resolves relative paths
    assert_true(len(utils.normalize_path('asdf.txt')) > 8)

    # doesn't resolve home directory references
    # assert '~' not in utils.normalize_path('~/asdf.txt')

    # converts back slashes to forward slashes
    assert_true(utils.normalize_path('\\windows\\why\\why\\why.txt'))

    # what's the right thing to do for None?
    assert_is_none(utils.normalize_path(None))


def test_limit_and_offset():
    def query_params(uri):
        """Return the query params as a dict"""
        return dict([kvp.split('=') for kvp in uri.split('?')[1].split('&')])

    qp = query_params(utils._limit_and_offset('/asdf/1234', limit=10, offset=0))
    assert_equals(qp['limit'], '10')
    assert_equals(qp['offset'], '0')

    qp = query_params(utils._limit_and_offset('/asdf/1234?limit=5&offset=10', limit=25, offset=50))
    assert_equals(qp['limit'], '25')
    assert_equals(qp['offset'], '50')
    assert_equals(len(qp), 2)

    qp = query_params(utils._limit_and_offset('/asdf/1234?foo=bar', limit=10, offset=30))
    assert_equals(qp['limit'], '10')
    assert_equals(qp['offset'], '30')
    assert_equals(qp['foo'], 'bar')
    assert_equals(len(qp), 3)

    qp = query_params(utils._limit_and_offset('/asdf/1234?foo=bar&a=b', limit=10))
    assert_equals(qp['limit'], '10')
    assert_false('offset' in qp)
    assert_equals(qp['foo'], 'bar')
    assert_equals(qp['a'], 'b')
    assert_equals(len(qp), 3)


def test_utils_extract_user_name():
    profile = {'firstName': 'Madonna'}
    assert_equals(utils.extract_user_name(profile), 'Madonna')
    profile = {'firstName': 'Oscar', 'lastName': 'the Grouch'}
    assert_equals(utils.extract_user_name(profile), 'Oscar the Grouch')
    profile['displayName'] = None
    assert_equals(utils.extract_user_name(profile), 'Oscar the Grouch')
    profile['displayName'] = ''
    assert_equals(utils.extract_user_name(profile), 'Oscar the Grouch')
    profile['displayName'] = 'Assistant Professor Oscar the Grouch, PhD'
    assert_equals(utils.extract_user_name(profile), 'Assistant Professor Oscar the Grouch, PhD')
    profile['userName'] = 'otg'
    assert_equals(utils.extract_user_name(profile), 'otg')


def test_is_json():
    assert_true(utils.is_json('application/json'))
    assert_true(utils.is_json('application/json;charset=ISO-8859-1'))
    assert_false(utils.is_json('application/flapdoodle;charset=ISO-8859-1'))
    assert_false(utils.is_json(None))
    assert_false(utils.is_json(''))


def test_normalize_whitespace():
    assert_equals("zip tang pow a = 2", utils.normalize_whitespace("   zip\ttang   pow   \n    a = 2   "))
    result = utils.normalize_lines("   zip\ttang   pow   \n    a = 2   \n    b = 3   ")
    assert_equals("zip tang pow\na = 2\nb = 3", result)


def test_query_limit_and_offset():
    query, limit, offset = utils.query_limit_and_offset("select foo from bar where zap > 2 limit 123 offset 456")
    assert_equals(query, "select foo from bar where zap > 2")
    assert_equals(limit, 123)
    assert_equals(offset, 456)

    query, limit, offset = utils.query_limit_and_offset(
        "select limit from offset where limit==2 limit 123 offset 456")
    assert_equals(query, "select limit from offset where limit==2")
    assert_equals(limit, 123)
    assert_equals(offset, 456)

    query, limit, offset = utils.query_limit_and_offset("select foo from bar where zap > 2 limit 123")
    assert_equals(query, "select foo from bar where zap > 2")
    assert_equals(limit, 123)
    assert_equals(offset, 1)

    query, limit, offset = utils.query_limit_and_offset("select foo from bar where zap > 2 limit 65535",
                                                        hard_limit=1000)
    assert_equals(query, "select foo from bar where zap > 2")
    assert_equals(limit, 1000)
    assert_equals(offset, 1)


def test_as_urls():
    assert_equals(utils.as_url("C:\\Users\\Administrator\\AppData\\Local\\Temp\\2\\tmpvixuld.txt"),
                  "file:///C:/Users/Administrator/AppData/Local/Temp/2/tmpvixuld.txt")
    assert_equals(utils.as_url("/foo/bar/bat/zoinks.txt"), "file:///foo/bar/bat/zoinks.txt")
    assert_equals(utils.as_url("http://foo/bar/bat/zoinks.txt"), "http://foo/bar/bat/zoinks.txt")
    assert_equals(utils.as_url("ftp://foo/bar/bat/zoinks.txt"), "ftp://foo/bar/bat/zoinks.txt")
    assert_equals(utils.as_url("sftp://foo/bar/bat/zoinks.txt"), "sftp://foo/bar/bat/zoinks.txt")


def test_time_manipulation():
    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("2014-12-10T19:09:34.000Z"))))
    assert_equals("2014-12-10T19:09:34.000Z", round_tripped_datetime)

    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("1969-04-28T23:48:34.123Z"))))
    assert_equals("1969-04-28T23:48:34.123Z", round_tripped_datetime)

    # check that rounding to milliseconds works
    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("1969-04-28T23:48:34.999499Z"))))
    assert_equals("1969-04-28T23:48:34.999Z", round_tripped_datetime)

    # check that rounding to milliseconds works
    round_tripped_datetime = utils.datetime_to_iso(
                                utils.from_unix_epoch_time_secs(
                                    utils.to_unix_epoch_time_secs(
                                        utils.iso_to_datetime("1969-04-27T23:59:59.999999Z"))))
    assert_equals("1969-04-28T00:00:00.000Z", round_tripped_datetime)


def test_treadsafe_generator():
    @utils.threadsafe_generator
    def generate_letters():
        for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            yield c

    "".join(letter for letter in generate_letters()) == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def test_extract_synapse_id_from_query():
    assert_equals(utils.extract_synapse_id_from_query("select * from syn1234567"), "syn1234567")
    assert_equals(utils.extract_synapse_id_from_query("select * from syn1234567 where foo = 'bar'"), "syn1234567")
    assert_equals(utils.extract_synapse_id_from_query("select * from syn1"), "syn1")
    assert_equals(utils.extract_synapse_id_from_query("select foo from syn99999999999"), "syn99999999999")


def test_temp_download_filename():
    temp_destination = utils.temp_download_filename("/foo/bar/bat", 12345)
    assert_equals(temp_destination, "/foo/bar/bat.synapse_download_12345", temp_destination)

    regex = r'/foo/bar/bat.synapse_download_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    assert_true(re.match(regex, utils.temp_download_filename("/foo/bar/bat", None)))


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
            assert_equals(expected_filepath, actual_filepath)

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
        assert_equals(expected_output, utils.snake_case(input_word))
