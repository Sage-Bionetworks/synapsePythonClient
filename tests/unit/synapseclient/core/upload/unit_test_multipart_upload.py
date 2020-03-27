from collections import OrderedDict
from concurrent.futures import Future
import hashlib
import json

from unittest import mock
from nose.tools import assert_equal, assert_false, assert_raises, assert_true

from synapseclient.core.upload.multipart_upload import (
    pool_provider,
    UploadAbortedException,
    UploadAttempt,
    UploadFailedException,
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
        max_workers = 8
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
            max_workers,
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

        expected_return = OrderedDict(
            {
                i['partNumber']: i['uploadPresignedUrl']
                for i in response['partPresignedUrls']
            }
        )

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

    def test_refresh_pre_signed_part_urls(self):
        upload = self._init_upload_attempt()

        original_presigned_urls = {
           2: "http://bar.com",
        }
        upload._pre_signed_part_urls = original_presigned_urls

        part_number = 2
        pre_signed_url = "http://foo.com/{}".format(part_number)
        with mock.patch.object(
            upload,
            '_fetch_pre_signed_part_urls',
        ) as fetch_urls:
            fetch_urls.return_value = {
                part_number: pre_signed_url,
            }

            refreshed_url = upload._refresh_pre_signed_part_urls(2)
            assert_equal(refreshed_url, pre_signed_url)

            fetch_urls.assert_called_once_with(
                upload._upload_id,
                list(original_presigned_urls.keys())
            )

    def test_handle_part_aborted(self):
        """Verify that handle part processing short circuits when
        the upload attempt has already been aborted."""

        upload = self._init_upload_attempt()
        upload._aborted = True

        with assert_raises(UploadAbortedException):
            upload._handle_part(5)

    def _handle_part_success_test(
        self,
        upload,
        part_number,
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
            refresh_urls.assert_called_once_with(part_number)
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
        """Verify behavior of a successul processing of a part.
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

            with assert_raises(UploadFailedException):
                upload()
