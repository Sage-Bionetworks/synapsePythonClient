from concurrent.futures import Future
import hashlib
import json
import math

from unittest import mock
from nose.tools import assert_equal, assert_false, assert_raises, assert_true

from synapseclient.core.exceptions import (
    SynapseHTTPError,
    SynapseUploadAbortedException,
    SynapseUploadFailedException,
)
import synapseclient.core.upload.multipart_upload
from synapseclient.core.upload.multipart_upload import (
    DEFAULT_PART_SIZE,
    MAX_NUMBER_OF_PARTS,
    MIN_PART_SIZE,
    _multipart_upload,
    multipart_upload_file,
    multipart_upload_string,
    pool_provider,
    UploadAttempt,
)


class TestUploadAttempt:

    def _init_upload_attempt(self):
        syn = mock.Mock()
        file_path = "/foo/bar/baz"
        dest_file_name = "target.txt"
        file_size = 1024
        part_size = 256
        md5_hex = "abc"
        content_type = "application/text"
        preview = False
        storage_location_id = "1234"
        max_threads = 8
        force_restart = True

        return UploadAttempt(
            syn,
            file_path,
            dest_file_name,
            file_size,
            part_size,
            md5_hex,
            content_type,
            preview,
            storage_location_id,
            max_threads,
            force_restart=force_restart,
        )

    def test_create_synapse_upload(self):
        upload = self._init_upload_attempt()

        expected_upload_request = {
            'contentMD5Hex': upload._md5_hex,
            'fileName': upload._dest_file_name,
            'generatePreview': upload._preview,
            'contentType': upload._content_type,
            'partSizeBytes': upload._part_size,
            'fileSizeBytes': upload._file_size,
            'storageLocationId': upload._storage_location_id,
        }
        expected_uri = "/file/multipart?forceRestart={}".format(
            str(upload._force_restart).lower()
        )

        response = mock.Mock()
        upload._syn.restPOST.return_value = response

        assert_equal(upload._create_synapse_upload(), response)

        upload._syn.restPOST.assert_called_once_with(
            expected_uri,
            json.dumps(expected_upload_request),
            endpoint=upload._syn.fileHandleEndpoint,
        )

    def test_fetch_presigned_urls(self):
        upload = self._init_upload_attempt()

        upload_id = '1234'
        part_numbers = [1, 3]
        session = mock.Mock()

        response = {
            'partPresignedUrls': [
                {
                    'partNumber': 1,
                    'uploadPresignedUrl': 'https://foo.com/1',
                },
                {
                    'partNumber': 3,
                    'uploadPresignedUrl': 'https://foo.com/3',
                }
            ]

        }
        upload._syn.restPOST.return_value = response

        expected_return = {
            i['partNumber']: i['uploadPresignedUrl']
            for i in response['partPresignedUrls']
        }

        pre_signed_urls = upload._fetch_pre_signed_part_urls(
            upload_id,
            part_numbers,
            session=session,
        )

        assert_equal(pre_signed_urls, expected_return)

        expected_uri =\
            "/file/multipart/{upload_id}/presigned/url/batch".format(
                upload_id=upload_id
            )
        expected_body = {
            'uploadId': upload_id,
            'partNumbers': part_numbers,
        }

        upload._syn.restPOST.assert_called_once_with(
            expected_uri,
            json.dumps(expected_body),
            session=session,
            endpoint=upload._syn.fileHandleEndpoint,
        )

    def test_refresh_presigned_part_url__fetch_required(self):
        """Verify that when calling the refresh function that if the
        url that was passed as expired is that last known available
        url for the given part number then we know that no other
        thread has already refreshed and this thread needs to do so."""

        upload = self._init_upload_attempt()

        part_number = 2
        current_url = "http://bar.com{}".format(part_number)
        original_presigned_urls = {
           2: current_url,
        }
        upload._pre_signed_part_urls = original_presigned_urls

        pre_signed_url = "http://foo.com/{}".format(part_number)
        with mock.patch.object(
            upload,
            '_fetch_pre_signed_part_urls',
        ) as fetch_urls:
            fetch_urls.return_value = {
                part_number: pre_signed_url,
            }

            # the passed url that expired is the same that as the last
            # one available for this part number, so a refresh is required.
            refreshed_url = upload._refresh_pre_signed_part_urls(
                part_number,
                current_url,
            )

            assert_equal(refreshed_url, pre_signed_url)

            fetch_urls.assert_called_once_with(
                upload._upload_id,
                list(original_presigned_urls.keys())
            )

    def test_refresh_presigned_part_url__no_fetch_required(self):
        """Test that if another thread already refreshed all the
        signed urls after this thread's url was detected as expired
        then we don't need to fetch new urls from synapse."""

        upload = self._init_upload_attempt()

        part_number = 2

        current_url = "http://bar.com{}".format(part_number)
        original_presigned_urls = {
           2: current_url,
        }
        upload._pre_signed_part_urls = original_presigned_urls

        pre_signed_url = "http://foo.com/{}".format(part_number)
        with mock.patch.object(
            upload,
            '_fetch_pre_signed_part_urls',
        ) as fetch_urls:
            # the passed url that expired is the same that as the last
            # one available for this part number, so a refresh is required.
            refreshed_url = upload._refresh_pre_signed_part_urls(
                part_number,
                pre_signed_url,
            )

            # should return the new url already on file without having
            # to have made a remote call.
            assert_equal(refreshed_url, current_url)
            fetch_urls.assert_not_called()

    def test_handle_part_aborted(self):
        """Verify that handle part processing short circuits when
        the upload attempt has already been aborted."""

        upload = self._init_upload_attempt()
        upload._aborted = True

        with assert_raises(SynapseUploadAbortedException):
            upload._handle_part(5)

    def _handle_part_success_test(
        self,
        upload,
        part_number,
        expired_url,
        aws_calls,
        chunk,
        refresh_url_response,
    ):

        mock_session = mock.Mock()

        md5_hex = hashlib.md5(chunk).hexdigest()

        with mock.patch.object(upload, '_chunk_fn')\
                as chunk_fn,\
                mock.patch.object(upload, '_get_thread_session')\
                as get_session,\
                mock.patch.object(upload, '_refresh_pre_signed_part_urls')\
                as refresh_urls:

            get_session.return_value = mock_session
            chunk_fn.return_value = chunk
            refresh_urls.return_value = refresh_url_response

            mock_session.put.side_effect = [
                aws_call[1] for aws_call in aws_calls
            ]

            result = upload._handle_part(1)

        expected_put_calls = [
            aws_call[0] for aws_call in aws_calls
        ]
        assert_equal(
            mock_session.put.call_args_list,
            expected_put_calls
        )

        assert_equal(
            result,
            (part_number, len(chunk)),
        )

        if refresh_url_response:
            refresh_urls.assert_called_once_with(
                part_number,
                expired_url,
            )
        else:
            assert_false(refresh_urls.called)

        upload._syn.restPUT.assert_called_once_with(
            "/file/multipart/{upload_id}/add/{part_number}?partMD5Hex={md5}"
            .format(
                upload_id=upload._upload_id,
                part_number=part_number,
                md5=md5_hex,
            ),
            session=mock_session,
            endpoint=upload._syn.fileHandleEndpoint
        )

        assert_true(part_number not in upload._pre_signed_part_urls)

    def test_handle_part_success(self):
        """Verify behavior of a successful processing of a part.
        Part bytes should be uploaded to aws, and """

        upload = self._init_upload_attempt()
        upload._upload_id = '123'
        part_number = 1
        chunk = b'1234'
        pre_signed_url_1 = 'https://foo.com/1'

        upload._pre_signed_part_urls = {part_number: pre_signed_url_1}

        self._handle_part_success_test(
            upload,
            part_number,
            pre_signed_url_1,
            [(mock.call(pre_signed_url_1, chunk), mock.Mock(status_code=200))],
            chunk,
            None,
        )

    def test_handle_part_expired_url(self):
        """An initial 403 when invoking a presigned url indicates its
        expired, verify that we recovery by refreshing the urls and
        invoking the refreshed url."""

        upload = self._init_upload_attempt()
        upload._upload_id = '123'
        part_number = 1
        chunk = b'1234'

        pre_signed_url_1 = 'https://foo.com/1'
        pre_signed_url_2 = 'https://bar.com/1'

        upload._pre_signed_part_urls = {part_number: pre_signed_url_1}

        self._handle_part_success_test(
            upload,
            part_number,
            pre_signed_url_1,

            # initial call is expired and results in a 403
            # second call is successful
            [
                (
                    mock.call(pre_signed_url_1, chunk),
                    mock.Mock(
                        status_code=403,
                        headers={},
                        reason=''
                    )
                ),
                (
                    mock.call(pre_signed_url_2, chunk),
                    mock.Mock(status_code=200)
                ),
            ],
            chunk,
            pre_signed_url_2,
        )

    def test_handle_part__url_expired_twice(self):
        """Verify that consecutive attempts to upload a part resulting
        in a 403 from AWS results in the expected error."""

        upload = self._init_upload_attempt()
        upload._upload_id = '123'
        part_number = 1
        chunk = b'1234'

        pre_signed_url_1 = 'https://foo.com/1'
        pre_signed_url_2 = 'https://bar.com/1'

        upload._pre_signed_part_urls = {part_number: pre_signed_url_1}
        mock_session = mock.Mock()

        with mock.patch.object(upload, '_chunk_fn')\
                as chunk_fn,\
                mock.patch.object(upload, '_get_thread_session')\
                as get_session,\
                mock.patch.object(upload, '_refresh_pre_signed_part_urls')\
                as refresh_urls:

            get_session.return_value = mock_session
            chunk_fn.return_value = chunk
            refresh_urls.side_effect = [
                {
                    part_number: url for url in [
                        pre_signed_url_1,
                        pre_signed_url_2,
                    ]
                }
            ]

            mock_session.put.return_value = mock.Mock(
                status_code=403,
                headers={},
                reason=''
            )

            with assert_raises(SynapseHTTPError):
                upload._handle_part(1)

    def test_call_upload(self):
        """Verify the behavior of an upload call, it should trigger
        calls to handle each of the individual outstanding parts
        and then call Synapse indicating that the upload is complete"""

        upload = self._init_upload_attempt()

        upload_id = '1234'
        parts_state = '010'
        upload_status = {
            'uploadId': upload_id,
            'partsState': parts_state,
        }
        pre_signed_urls = {
            1: 'http://foo.com/1',
            3: 'http://foo.com/3',
        }

        futures = []
        for i in pre_signed_urls.keys():
            future = Future()
            future.set_result((i, upload._part_size))
            futures.append(future)

        with mock.patch.object(upload, '_create_synapse_upload')\
            as create_synapse_upload,\
            mock.patch.object(upload, '_fetch_pre_signed_part_urls')\
            as fetch_pre_signed_urls,\
            mock.patch.object(pool_provider, 'get_executor')\
            as get_executor,\
            mock.patch.object(upload, '_get_thread_session')\
                as get_session:

            mock_session = mock.Mock()
            get_session.return_value = mock_session

            create_synapse_upload.return_value = upload_status
            fetch_pre_signed_urls.return_value = pre_signed_urls

            get_executor.return_value.submit.side_effect = futures

            upload_response = {
                'state': 'COMPLETED'
            }

            upload._syn.restPUT.return_value = upload_response
            result = upload()
            assert_equal(result, upload_response)

            upload._syn.restPUT.assert_called_once_with(
                "/file/multipart/{upload_id}/complete".format(
                    upload_id=upload_id
                ),
                session=mock_session,
                endpoint=upload._syn.fileHandleEndpoint,
            )

    def test_call_upload__part_failure(self):
        """Verify that an error raised while processing one part
        results in an error on the upload."""
        upload = self._init_upload_attempt()

        upload_id = '1234'
        parts_state = '0'
        upload_status = {
            'uploadId': upload_id,
            'partsState': parts_state,
        }
        pre_signed_urls = {
            1: 'http://foo.com/1',
        }

        future = Future()
        future.set_exception(Exception())

        with mock.patch.object(upload, '_create_synapse_upload')\
            as create_synapse_upload,\
            mock.patch.object(upload, '_fetch_pre_signed_part_urls')\
            as fetch_pre_signed_urls,\
            mock.patch.object(pool_provider, 'get_executor')\
                as get_executor:

            create_synapse_upload.return_value = upload_status
            fetch_pre_signed_urls.return_value = pre_signed_urls

            get_executor.return_value.submit.return_value = future

            with assert_raises(SynapseUploadFailedException):
                upload()


class TestMultipartUpload:

    def test_multipart_upload_file(self):
        """Verify multipart_upload_file passes through its
        args, validating and supplying defaults as expected."""

        syn = mock.Mock()

        file_path = '/foo/bar/baz'
        file_size = 1234
        md5_hex = 'abc123'

        with mock.patch('os.path.exists') as os_path_exists,\
                mock.patch('os.path.isdir') as os_path_is_dir,\
                mock.patch('os.path.getsize') as os_path_getsize,\
                mock.patch.object(
                    synapseclient.core.upload.multipart_upload,
                    'md5_for_file',
                ) as md5_for_file,\
                mock.patch.object(
                    synapseclient.core.upload.multipart_upload,
                    '_multipart_upload',
                ) as mock_multipart_upload:

            os_path_getsize.return_value = file_size
            md5_for_file.return_value.hexdigest.return_value = md5_hex

            os_path_exists.return_value = False

            # bad file
            with assert_raises(IOError):
                multipart_upload_file(syn, file_path)

            os_path_exists.return_value = True
            os_path_is_dir.return_value = True

            with assert_raises(IOError):
                multipart_upload_file(syn, file_path)

            os_path_is_dir.return_value = False

            # call w/ defaults
            multipart_upload_file(syn, file_path)
            mock_multipart_upload.assert_called_once_with(
                syn,
                mock.ANY,  # lambda chunk function
                file_size,
                None,  # part_size
                'baz',
                md5_hex,
                'application/octet-stream',  # content_type
                None,  # storage_location_id
                True,  # preview
                False,  # force_restart
                None,  # max_threads
            )

            mock_multipart_upload.reset_mock()

            # call specifying all optional kwargs
            kwargs = {
                'dest_file_name': 'blort',
                'content_type': 'text/plain',
                'part_size': 9876,
                'storage_location_id': 5432,
                'preview': False,
                'force_restart': True,
                'max_threads': 8,
            }
            multipart_upload_file(
                syn,
                file_path,
                **kwargs
            )
            mock_multipart_upload.assert_called_once_with(
                syn,
                mock.ANY,  # lambda chunk function
                file_size,
                kwargs['part_size'],
                kwargs['dest_file_name'],
                md5_hex,
                kwargs['content_type'],
                kwargs['storage_location_id'],
                kwargs['preview'],
                kwargs['force_restart'],
                kwargs['max_threads'],
            )

    def test_multipart_upload_string(self):
        """Verify multipart_upload_string passes through its
        args, validating and supplying defaults as expected."""

        syn = mock.Mock()
        upload_text = 'foobarbaz'

        with mock.patch.object(
               synapseclient.core.upload.multipart_upload,
               '_multipart_upload',
           ) as mock_multipart_upload:

            encoded = upload_text.encode('utf-8')
            md5_hex = hashlib.md5(encoded).hexdigest()

            # call w/ default args
            multipart_upload_string(syn, upload_text)
            mock_multipart_upload.assert_called_once_with(
                syn,
                mock.ANY,  # lambda chunk function
                len(encoded),
                None,  # part_size
                'message.txt',
                md5_hex,
                'text/plain; charset=utf-8',
                None,  # storage_location_id
                True,  # preview
                False,  # force_restart
                None,  # max_threads
            )

            mock_multipart_upload.reset_mock()

            # call specifying all optional kwargs
            kwargs = {
                'dest_file_name': 'blort',
                'content_type': 'text/csv',
                'part_size': 9876,
                'storage_location_id': 5432,
                'preview': False,
                'force_restart': True,
                'max_threads': 8,
            }
            multipart_upload_string(syn, upload_text, **kwargs)
            mock_multipart_upload.assert_called_once_with(
                syn,
                mock.ANY,  # lambda chunk function
                len(encoded),
                kwargs['part_size'],
                kwargs['dest_file_name'],
                md5_hex,
                kwargs['content_type'],
                kwargs['storage_location_id'],
                kwargs['preview'],
                kwargs['force_restart'],
                kwargs['max_threads'],
            )

    def _multipart_upload_test(self, upload_side_effect, *args, **kwargs):
        with mock.patch.object(
            synapseclient.core.upload.multipart_upload,
            'UploadAttempt'
        ) as mock_upload_attempt,\
            mock.patch.object(
                 synapseclient.core.upload.multipart_upload,
                 'multiprocessing',
                ) as mock_multiprocessing:

            mock_upload_attempt.side_effect = upload_side_effect

            # predictable value so things don't vary by test environment
            mock_multiprocessing.cpu_count.return_value = 4

            return _multipart_upload(*args, **kwargs), mock_upload_attempt

    def test_multipart_upload(self):
        """"Verify the behavior of a successful call to multipart_upload
        with various parameterizations applied.  Verify that parameters
        are validated/adjusted as expected."""

        default_num_threads = 10
        syn = mock.Mock(NUM_THREADS=default_num_threads)
        chunk_fn = mock.Mock()
        md5_hex = 'ab123'
        dest_file_name = 'foo'
        content_type = 'text/plain'
        storage_location_id = 3210
        result_file_handle_id = 'foo'
        upload_side_effect = [
            mock.Mock(
                return_value={'resultFileHandleId': result_file_handle_id}
            )
        ]

        # (file_size, in_part_size, in_max_threads, in_force_restart)
        # (out_part_size, out_max_threads, out_force_restart)
        tests = [

            # part_size exceeds file size, so only 1 part expect 1 worker
            (
                (1234, None, None, False),
                (DEFAULT_PART_SIZE, 1, False)
            ),

            # multiple parts, but less parts than specified max workers
            # so we expect max workers to be num of parts
            (
               (pow(2, 24), pow(2, 23) - 1000, None, False),
               (pow(2, 23) - 1000, 3, False),
            ),

            # parts exceeds specified max_threads, so specified max_threads
            # passes through unchanged, also specify force_restart
            (
               (pow(2, 28), None, 8, True),
               (DEFAULT_PART_SIZE, 8, True),
            ),

            # many parts, no max_threads, specified, should use default
            (
                (pow(2, 28), None, default_num_threads, False),
                (DEFAULT_PART_SIZE, default_num_threads, False),
            ),

            # part size specified below min, should be raised
            (
                (1000, 1, None, False),
                (MIN_PART_SIZE, 1, False),
            ),

            # part size would exceed max number of parts,
            # should be adjusted accordingly
            (
                (pow(2, 36), MIN_PART_SIZE + 1, 32, True),
                (int(math.ceil(pow(2, 36) / MAX_NUMBER_OF_PARTS)), 32, True),
            )
        ]

        for (file_size, in_part_size, in_max_threads, in_force_restart),\
            (out_part_size, out_max_threads, out_force_restart)\
                in tests:

            result, upload_mock = self._multipart_upload_test(
                upload_side_effect,
                syn,
                chunk_fn,
                file_size,
                in_part_size,
                dest_file_name,
                md5_hex,
                content_type,
                storage_location_id,
                max_threads=in_max_threads,
                force_restart=in_force_restart,
            )

            upload_mock.assert_called_once_with(
                syn,
                chunk_fn,
                dest_file_name,
                file_size,
                out_part_size,
                md5_hex,
                content_type,
                True,
                storage_location_id,
                out_max_threads,
                force_restart=out_force_restart,
            )

    def test_multipart_upload__retry_success(self):
        """Verify we recover on a failed upload if a subsequent
        retry succeeds."""

        syn = mock.Mock(NUM_THREADS=12)
        chunk_fn = mock.Mock()
        md5_hex = 'ab123'
        file_size = 1234
        dest_file_name = 'foo'
        content_type = 'text/plain'
        storage_location_id = 3210
        result_file_handle_id = 'foo'
        upload_side_effect = [
            SynapseUploadFailedException(),
            SynapseUploadFailedException(),
            mock.Mock(
                return_value={'resultFileHandleId': result_file_handle_id}
            )
        ]

        result, upload_mock = self._multipart_upload_test(
            upload_side_effect,
            syn,
            chunk_fn,
            file_size,
            None,  # part_size
            dest_file_name,
            md5_hex,
            content_type,
            storage_location_id,
            None,  # max_threads
        )

        # should have been called multiple times but returned
        # the result in the end.
        assert_equal(result_file_handle_id, result)
        assert_equal(len(upload_side_effect), upload_mock.call_count)

    def test_multipart_upload__retry_failure(self):
        """Verify if we run out of upload attempts we give up
        and raise the failure."""

        syn = mock.Mock(NUM_THREADS=5)
        chunk_fn = mock.Mock()
        md5_hex = 'ab123'
        file_size = 1234
        dest_file_name = 'foo'
        content_type = 'text/plain'
        storage_location_id = 3210
        upload_side_effect = SynapseUploadFailedException()

        with assert_raises(SynapseUploadFailedException):
            self._multipart_upload_test(
                upload_side_effect,
                syn,
                chunk_fn,
                file_size,
                None,  # part_size
                dest_file_name,
                md5_hex,
                content_type,
                storage_location_id,
                None,  # max_threads
            )
