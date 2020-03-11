import shutil
import tempfile
import os
import hashlib
import unittest

from unittest.mock import MagicMock, patch, mock_open, call
from nose.tools import assert_raises, assert_equals, assert_false
from synapseclient import client

from synapseclient import *
from synapseclient.core.exceptions import SynapseHTTPError, SynapseMd5MismatchError, SynapseError, SynapseFileNotFoundError
import synapseclient.core.constants.concrete_types as concrete_types
import synapseclient.core.multithread_download as multithread_download
from synapseclient.core import utils
from tests import unit


def setup(module):
    module.syn = unit.syn


# a callable that mocks the requests.get function
class MockRequestGetFunction(object):
    def __init__(self, responses):
        self.responses = responses
        self.i = 0

    def __call__(self, *args, **kwargs):
        response = self.responses[self.i]
        self.i += 1
        return response


# a class to iterate bogus content
class IterateContents(object):
    def __init__(self, contents, buffer_size, partial_start=0, partial_end=None):
        self.contents = contents
        self.buffer_size = buffer_size
        self.i = partial_start
        self.partial_end = partial_end
        self.bytes_iterated = 0

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.i >= len(self.contents):
            raise StopIteration()
        if self.partial_end and self.i >= self.partial_end:
            raise requests.exceptions.ChunkedEncodingError("Simulated partial download! Connection reset by peer!")
        start = self.i
        end = min(self.i + self.buffer_size, len(self.contents))
        if self.partial_end:
            end = min(end, self.partial_end)
        self.i = end
        data = self.contents[start:end].encode('utf-8')
        self.bytes_iterated += len(data)
        return data

    def total_bytes_iterated(self):
        return self.bytes_iterated


def create_mock_response(url, response_type, **kwargs):
    response = MagicMock()

    response.request.url = url
    response.request.method = kwargs.get('method', 'GET')
    response.request.headers = {}
    response.request.body = None

    if response_type == "redirect":
        response.status_code = 301
        response.headers = {'location': kwargs['location']}
    elif response_type == "error":
        response.status_code = kwargs.get('status_code', 500)
        response.reason = kwargs.get('reason', 'fake reason')
        response.text = '{{"reason":"{}"}}'.format(kwargs.get('reason', 'fake reason'))
        response.json = lambda: json.loads(response.text)
    elif response_type == "stream":
        response.status_code = kwargs.get('status_code', 200)
        response.headers = {
            'content-disposition': 'attachment; filename="fname.ext"',
            'content-type': 'application/octet-stream',
            'content-length': len(response.text)
        }

        def _create_iterator(buffer_size):
            response._content_iterator = IterateContents(kwargs['contents'],
                                                         kwargs['buffer_size'],
                                                         kwargs.get('partial_start', 0),
                                                         kwargs.get('partial_end', None))
            return response._content_iterator

        response.iter_content = _create_iterator
        response.raw.tell = lambda: response._content_iterator.total_bytes_iterated()
    else:
        response.status_code = 200
        response.text = kwargs['text']
        response.json = lambda: json.loads(response.text)
        response.headers = {
            'content-type': 'application/json',
            'content-length': len(response.text)
        }

    return response


def mock_generateSignedHeaders(self, url, headers=None):
    return {}


def test_mock_download():
    temp_dir = tempfile.gettempdir()

    fileHandleId = "42"
    objectId = "syn789"
    objectType = "FileEntity"

    # make bogus content
    contents = "\n".join(str(i) for i in range(1000))

    # compute MD5 of contents
    m = hashlib.md5()
    m.update(contents.encode('utf-8'))
    contents_md5 = m.hexdigest()

    url = "https://repo-prod.prod.sagebase.org/repo/v1/entity/syn6403467/file"

    # 1. No redirects
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "stream", contents=contents, buffer_size=1024)
    ])

    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        syn._download_from_URL(url, destination=temp_dir, fileHandleId=12345, expected_md5=contents_md5)

    # 2. Multiple redirects
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "redirect", location="https://fakeurl.com/qwer"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024)
    ])

    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        syn._download_from_URL(url, destination=temp_dir, fileHandleId=12345, expected_md5=contents_md5)

    # 3. recover from partial download
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_end=len(contents) // 7 * 3,
                             status_code=200),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents) // 7 * 3,
                             partial_end=len(contents) // 7 * 5, status_code=206),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents) // 7 * 5,
                             status_code=206)
    ])

    _getFileHandleDownload_return_value = {'preSignedURL': url,
                                           'fileHandle': {'id': 12345, 'contentMd5': contents_md5,
                                                          'concreteType': concrete_types.S3_FILE_HANDLE}}
    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(Synapse, '_getFileHandleDownload',
                      return_value=_getFileHandleDownload_return_value):
        syn._downloadFileHandle(fileHandleId, objectId, objectType, destination=temp_dir)

    # 4. as long as we're making progress, keep trying
    responses = [
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=0,
                             partial_end=len(contents) // 11, status_code=200)
    ]
    for i in range(1, 12):
        responses.append(
            create_mock_response(url, "stream", contents=contents, buffer_size=1024,
                                 partial_start=len(contents) // 11 * i,
                                 partial_end=len(contents) // 11 * (i + 1), status_code=206))
    mock_requests_get = MockRequestGetFunction(responses)

    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(Synapse, '_getFileHandleDownload',
                      return_value=_getFileHandleDownload_return_value):

        syn._downloadFileHandle(fileHandleId, objectId, objectType, destination=temp_dir)

    # 5. don't recover, a partial download that never completes
    #    should eventually throw an exception
    responses = [
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=0,
                             partial_end=len(contents) // 11, status_code=200),
    ]
    for i in range(1, 10):
        responses.append(
            create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents) // 11,
                                 partial_end=len(contents) // 11, status_code=200))
    mock_requests_get = MockRequestGetFunction(responses)

    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(Synapse, '_getFileHandleDownload',
                      return_value=_getFileHandleDownload_return_value):

        assert_raises(Exception,
                      syn._downloadFileHandle, fileHandleId, objectId, objectType, destination=temp_dir)

    # 6. 206 Range header not supported, respond with 200 and full file
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial=len(contents) // 7 * 3,
                             status_code=200),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, status_code=200)
    ])

    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(Synapse, '_getFileHandleDownload',
                      return_value=_getFileHandleDownload_return_value):
        syn._downloadFileHandle(fileHandleId, objectId, objectType, destination=temp_dir)

    # 7. Too many redirects
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf") for i in range(100)])

    # patch requests.get and also the method that generates signed
    # headers (to avoid having to be logged in to Synapse)
    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(Synapse, '_getFileHandleDownload',
                      return_value=_getFileHandleDownload_return_value):
        assert_raises(SynapseHTTPError, syn._downloadFileHandle, fileHandleId, objectId, objectType,
                      destination=temp_dir)

class Test__downloadFileHandle(unittest.TestCase):

    def tearDown(self) -> None:
        syn.multi_threaded=False

    def test_multithread_true__S3_fileHandle(self):
        with patch.object(os, "makedirs") as mock_makedirs,\
            patch.object(syn, "_getFileHandleDownload") as mock_getFileHandleDownload,\
            patch.object(syn, "_download_from_url_multi_threaded") as mock_multi_thread_download,\
            patch.object(syn, "cache") as mock_cache:

            mock_getFileHandleDownload.return_value = {
                'fileHandle':{
                    'id':'123',
                    'concreteType':concrete_types.S3_FILE_HANDLE,
                    'contentMd5':'someMD5'
                }
            }

            syn.multi_threaded=True
            syn._downloadFileHandle(fileHandleId=123, objectId=456, objectType="FileEntity", destination="/myfakepath")

            mock_multi_thread_download.assert_called_once_with(123,456,"FileEntity","/myfakepath", expected_md5="someMD5")

    def test_multithread_True__other_file_handle_type(self):
        with patch.object(os, "makedirs") as mock_makedirs,\
            patch.object(syn, "_getFileHandleDownload") as mock_getFileHandleDownload,\
            patch.object(syn, "_download_from_URL") as mock_download_from_URL,\
            patch.object(syn, "cache") as mock_cache:

            mock_getFileHandleDownload.return_value = {
                'fileHandle':{
                    'id':'123',
                    'concreteType': "someFakeConcreteType",
                    'contentMd5':'someMD5'
                },
                'preSignedURL': 'asdf.com'
            }

            syn.multi_threaded=True
            syn._downloadFileHandle(fileHandleId=123, objectId=456, objectType="FileEntity", destination="/myfakepath")

            mock_download_from_URL.assert_called_once_with("asdf.com", "/myfakepath", "123", expected_md5="someMD5")

    def test_multithread_false__S3_fileHandle(self):
        with patch.object(os, "makedirs") as mock_makedirs,\
            patch.object(syn, "_getFileHandleDownload") as mock_getFileHandleDownload,\
            patch.object(syn, "_download_from_URL") as mock_download_from_URL,\
            patch.object(syn, "cache") as mock_cache:

            mock_getFileHandleDownload.return_value = {
                'fileHandle':{
                    'id':'123',
                    'concreteType':concrete_types.S3_FILE_HANDLE,
                    'contentMd5':'someMD5'
                },
                'preSignedURL': 'asdf.com'
            }

            syn.multi_threaded=False
            syn._downloadFileHandle(fileHandleId=123, objectId=456, objectType="FileEntity", destination="/myfakepath")

            mock_download_from_URL.assert_called_once_with("asdf.com", "/myfakepath", "123", expected_md5="someMD5")


class Test_download_from_url_multi_threaded:
    def test_md5_mis_match(self):
        with patch.object(multithread_download, "download_file"),\
            patch.object(utils, "md5_for_file") as mock_md5_for_file,\
            patch.object(os, "remove") as mock_os_remove,\
            patch.object(shutil, "move") as mock_move:

            path = os.path.abspath("/myfakepath")

            mock_md5_for_file.return_value.hexdigest.return_value = "unexpetedMd5"

            assert_raises(SynapseMd5MismatchError, syn._download_from_url_multi_threaded, file_handle_id=123,
                          object_id=456, object_type="FileEntity",
                          destination=path, expected_md5="myExpectedMd5")

            mock_os_remove.assert_called_once_with(utils.temp_download_filename(path, 123))
            mock_move.assert_not_called()



    def test_md5_mismatch(self):
        with patch.object(multithread_download, "download_file"), \
             patch.object(utils, "md5_for_file") as mock_md5_for_file, \
                patch.object(os, "remove") as mock_os_remove, \
                patch.object(shutil, "move") as mock_move:
            path = os.path.abspath("/myfakepath")

            expected_md5 = "myExpectedMd5"

            mock_md5_for_file.return_value.hexdigest.return_value = expected_md5

            syn._download_from_url_multi_threaded(file_handle_id=123,
                          object_id=456, object_type="FileEntity",
                          destination=path, expected_md5=expected_md5)

            mock_os_remove.assert_not_called()
            mock_move.assert_called_once_with(utils.temp_download_filename(path, 123), path)


def test_download_end_early_retry():
    """
    -------Test to ensure download retry even if connection ends early--------
    """

    url = "http://www.ayy.lmao/filerino.txt"
    contents = "\n".join(str(i) for i in range(1000))
    destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))
    temp_destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt.temp"))

    partial_content_break = len(contents) // 7 * 3
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "stream", contents=contents[:partial_content_break], buffer_size=1024,
                             partial_end=len(contents),
                             status_code=200),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=partial_content_break,
                             status_code=206)
    ])

    # make the first response's 'content-type' header say it will transfer the full content even though it
    # is only partially doing so
    mock_requests_get.responses[0].headers['content-length'] = len(contents)
    mock_requests_get.responses[1].headers['content-length'] = len(contents[partial_content_break:])

    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(utils, 'temp_download_filename', return_value=temp_destination) as mocked_temp_dest, \
         patch.object(client, 'open', new_callable=mock_open(), create=True) as mocked_open, \
         patch.object(os.path, 'exists', side_effect=[False, True]) as mocked_exists, \
         patch.object(os.path, 'getsize', return_value=partial_content_break) as mocked_getsize, \
         patch.object(utils, 'md5_for_file'), \
         patch.object(shutil, 'move') as mocked_move:
        # function under test
        syn._download_from_URL(url, destination)

        # assert temp_download_filename() called 2 times with same parameters
        assert_equals([call(destination, None)] * 2, mocked_temp_dest.call_args_list)

        # assert exists called 2 times
        assert_equals([call(temp_destination)] * 2, mocked_exists.call_args_list)

        # assert open() called 2 times with different parameters
        assert_equals([call(temp_destination, 'wb'), call(temp_destination, 'ab')], mocked_open.call_args_list)

        # assert getsize() called 2 times
        # once because exists()=True and another time because response status code = 206
        assert_equals([call(temp_destination)] * 2, mocked_getsize.call_args_list)

        # assert shutil.move() called 1 time
        mocked_move.assert_called_once_with(temp_destination, destination)


def test_download_md5_mismatch__not_local_file():
    """
    --------Test to ensure file gets removed on md5 mismatch--------
    """
    url = "http://www.ayy.lmao/filerino.txt"
    contents = "\n".join(str(i) for i in range(1000))
    destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))
    temp_destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt.temp"))

    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_end=len(contents),
                             status_code=200)
    ])

    with patch.object(syn._requests_session, 'get', side_effect=mock_requests_get), \
         patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(utils, 'temp_download_filename', return_value=temp_destination) as mocked_temp_dest, \
         patch.object(client, 'open', new_callable=mock_open(), create=True) as mocked_open, \
         patch.object(os.path, 'exists', side_effect=[False, True]) as mocked_exists, \
         patch.object(shutil, 'move') as mocked_move, \
         patch.object(os, 'remove') as mocked_remove:
        # function under test
        assert_raises(SynapseMd5MismatchError, syn._download_from_URL, url, destination,
                      expected_md5="fake md5 is fake")

        # assert temp_download_filename() called once
        mocked_temp_dest.assert_called_once_with(destination, None)

        # assert exists called 2 times
        assert_equals([call(temp_destination), call(destination)], mocked_exists.call_args_list)

        # assert open() called once
        mocked_open.assert_called_once_with(temp_destination, 'wb')

        # assert shutil.move() called once
        mocked_move.assert_called_once_with(temp_destination, destination)

        # assert file was removed
        mocked_remove.assert_called_once_with(destination)


def test_download_md5_mismatch_local_file():
    """
    --------Test to ensure file gets removed on md5 mismatch--------
    """
    url = "file:///some/file/path.txt"
    destination = os.path.normpath(os.path.expanduser("~/fake/path/filerino.txt"))

    with patch.object(Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders), \
         patch.object(utils, 'file_url_to_path', return_value=destination) as mocked_file_url_to_path, \
            patch.object(utils, 'md5_for_file', return_value=hashlib.md5()) as mocked_md5_for_file, \
            patch('os.remove') as mocked_remove:
        # function under test
        assert_raises(SynapseMd5MismatchError, syn._download_from_URL, url, destination,
                      expected_md5="fake md5 is fake")

        mocked_file_url_to_path.assert_called_once_with(url, verify_exists=True)
        mocked_md5_for_file.assert_called_once_with(destination)
        # assert file was NOT removed
        assert_false(mocked_remove.called)


def test_download_file_entity__correct_local_state():
    mock_cache_path = utils.normalize_path("/i/will/show/you/the/path/yi.txt")
    file_entity = File(parentId="syn123")
    file_entity.dataFileHandleId = 123
    with patch.object(syn.cache, 'get', return_value=mock_cache_path):
        syn._download_file_entity(downloadLocation=None, entity=file_entity, ifcollision="overwrite.local",
                                  submission=None)
        assert_equals(mock_cache_path, file_entity.path)
        assert_equals(os.path.dirname(mock_cache_path), file_entity.cacheDir)
        assert_equals(1, len(file_entity.files))
        assert_equals(os.path.basename(mock_cache_path), file_entity.files[0])


def test_getFileHandleDownload__error_UNAUTHORIZED():
    ret_val = {'requestedFiles': [{'failureCode': 'UNAUTHORIZED', }]}
    with patch.object(syn, "restPOST", return_value=ret_val):
        assert_raises(SynapseError, syn._getFileHandleDownload, '123', 'syn456')


def test_getFileHandleDownload__error_NOT_FOUND():
    ret_val = {'requestedFiles': [{'failureCode': 'NOT_FOUND', }]}
    with patch.object(syn, "restPOST", return_value=ret_val):
        assert_raises(SynapseFileNotFoundError, syn._getFileHandleDownload, '123', 'syn456')
