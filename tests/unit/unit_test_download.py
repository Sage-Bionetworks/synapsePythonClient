from __future__ import print_function
from __future__ import unicode_literals
from builtins import str, ascii

import requests
import synapseclient
import tempfile, os, hashlib
import unit
from mock import MagicMock, patch
from synapseclient.utils import MB, GB
from synapseclient.exceptions import SynapseDownloadError, SynapseHTTPError



def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = unit.syn


## a callable that mocks the requests.get function
class MockRequestGetFunction(object):
    def __init__(self, responses):
        self.responses = responses
        self.i = 0

    def __call__(self, *args, **kwargs):
        response = self.responses[self.i]
        self.i += 1
        return response


## a class to iterate bogus content
class IterateContents(object):
    def __init__(self, contents, buffer_size, partial_start=0, partial_end=None):
        self.contents = contents
        self.buffer_size = buffer_size
        self.i = partial_start
        self.partial_end = partial_end

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
        end   = min(self.i + self.buffer_size, len(self.contents))
        if self.partial_end:
            end = min(end, self.partial_end)
        self.i = end
        return self.contents[start:end].encode('utf-8')


def create_mock_response(url, response_type, **kwargs):
    response = MagicMock()

    response.request.url = url
    response.request.method = kwargs.get('method', 'GET')
    response.request.headers = {}
    response.request.body = None

    if response_type=="redirect":
        response.status_code = 301
        response.headers = {'location': kwargs['location']}
    elif response_type=="error":
        response.status_code = kwargs.get('status_code', 500)
        response.reason = kwargs.get('reason', 'fake reason')
        response.text = '{{"reason":"{}"}}'.format(kwargs.get('reason', 'fake reason'))
        response.json = lambda: json.loads(response.text)
    elif response_type=="stream":
        response.status_code = kwargs.get('status_code', 200)
        response.headers = {
            'content-disposition':'attachment; filename="fname.ext"',
            'content-type':'application/octet-stream',
            'content-length':len(response.text)
        }
        response.iter_content = lambda buffer_size: \
            IterateContents(kwargs['contents'],
                            kwargs['buffer_size'],
                            kwargs.get('partial_start', 0),
                            kwargs.get('partial_end', None))
    else:
        response.status_code = 200
        response.text = kwargs['text']
        response.json = lambda: json.loads(response.text)
        response.headers = {
            'content-type':'application/json',
            'content-length':len(response.text)
        }

    return response

def mock_generateSignedHeaders(self, url, headers=None):
    return {}


def test_mock_download():
    temp_dir = tempfile.gettempdir()

    ## make bogus content
    contents = "\n".join(str(i) for i in range(1000))

    ## compute MD5 of contents
    m = hashlib.md5()
    m.update(contents.encode('utf-8'))
    contents_md5 = m.hexdigest()

    url = "https://repo-prod.prod.sagebase.org/repo/v1/entity/syn6403467/file"

    ## 1. No redirects
    print("\n1. No redirects", "-"*60)
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "stream", contents=contents, buffer_size=1024)
    ])

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        path = syn._download(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)


    ## 2. Multiple redirects
    print("\n2. Multiple redirects", "-"*60)
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "redirect", location="https://fakeurl.com/qwer"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024)
    ])

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        path = syn._download(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)


    ## 3. recover from partial download
    print("\n3. recover from partial download", "-"*60)
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_end=len(contents)//7*3, status_code=200),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents)//7*3, partial_end=len(contents)//7*5, status_code=206),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents)//7*5, status_code=206)
    ])

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        path = syn._download_with_retries(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)


    ## 4. as long as we're making progress, keep trying
    print("\n4. as long as we're making progress, keep trying", "-"*60)
    caught_exception = None
    responses = [
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=0, partial_end=len(contents)//11, status_code=200)
    ]
    for i in range(1,12):
        responses.append(
            create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents)//11*i, partial_end=len(contents)//11*(i+1), status_code=206))
    mock_requests_get = MockRequestGetFunction(responses)

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        path = syn._download_with_retries(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)


    ## 5. don't recover, a partial download that never completes
    ##    should eventually throw an exception
    print("\n5. don't recover", "-"*60)
    caught_exception = None
    responses = [
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=0, partial_end=len(contents)//11, status_code=200),
    ]
    for i in range(1,10):
        responses.append(
            create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial_start=len(contents)//11, partial_end=len(contents)//11, status_code=200))
    mock_requests_get = MockRequestGetFunction(responses)

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        try:
            path = syn._download_with_retries(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)
        except SynapseDownloadError as ex:
            caught_exception = ex
            print("Caught expected exception: ", str(ex))

    assert caught_exception, "Expected a SynapseDownloadError"


    ## 6. 206 Range header not supported, respond with 200 and full file
    print("\n6. 206 Range header not supported", "-"*60)
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf"),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, partial=len(contents)//7*3, status_code=200),
        create_mock_response(url, "stream", contents=contents, buffer_size=1024, status_code=200)
    ])

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        path = syn._download_with_retries(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)


    ## 6. 206 Range header not supported, respond with 200 and full file
    print("\n7. Too many redirects", "-"*60)
    mock_requests_get = MockRequestGetFunction([
        create_mock_response(url, "redirect", location="https://fakeurl.com/asdf") for i in range(100)])

    ## patch requests.get and also the method that generates signed
    ## headers (to avoid having to be logged in to Synapse)
    with patch.object(requests, 'get', side_effect=mock_requests_get), \
         patch.object(synapseclient.client.Synapse, '_generateSignedHeaders', side_effect=mock_generateSignedHeaders):
        try:
            path = syn._download_with_retries(url, destination=temp_dir, file_handle_id=12345, expected_md5=contents_md5)
        except SynapseHTTPError as ex:
            caught_exception = ex
            print("Caught expected exception: ", str(ex))

    assert caught_exception, "Expected a SynapseDownloadError"

