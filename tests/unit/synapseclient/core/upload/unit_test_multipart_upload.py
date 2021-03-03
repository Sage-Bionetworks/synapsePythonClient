from concurrent.futures import Future
import hashlib
import json

import pytest
from unittest import mock

from synapseclient import Synapse
from synapseclient.core.exceptions import (
    SynapseHTTPError,
    SynapseUploadAbortedException,
    SynapseUploadFailedException,
)
import synapseclient.core.upload.multipart_upload as multipart_upload
from synapseclient.core.upload.multipart_upload import (
    DEFAULT_PART_SIZE,
    MIN_PART_SIZE,
    _multipart_upload,
    multipart_copy,
    multipart_upload_file,
    multipart_upload_string,
    pool_provider,
    UploadAttempt,
)


class TestUploadAttempt:

    dest_file_name = 'target.txt'
    content_type = 'application/text'
    part_size = 256
    file_size = 1024
    md5_hex = 'abc'
    preview = False
    storage_location_id = '1234'

    def _init_upload_attempt(self, syn):
        upload_request_payload = {
            'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
            'contentMD5Hex': self.md5_hex,
            'contentType': self.content_type,
            'fileName': self.dest_file_name,
            'fileSizeBytes': self.file_size,
            'generatePreview': self.preview,
            'storageLocationId': self.storage_location_id,
            'partSizeBytes': self.part_size,
        }

        def part_request_body_provider_fn(part_number):
            return (f"{part_number}" * self.part_size).encode('utf-8')

        def md5_fn(part, _):
            md5 = hashlib.md5()
            md5.update(part)
            return md5.hexdigest()

        max_threads = 8
        force_restart = True

        return UploadAttempt(
            syn,
            self.dest_file_name,
            upload_request_payload,
            part_request_body_provider_fn,
            md5_fn,
            max_threads,
            force_restart=force_restart,
        )

    def test_create_synapse_upload(self, syn):
        upload = self._init_upload_attempt(syn)

        expected_upload_request = {
            'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
            'contentMD5Hex': self.md5_hex,
            'contentType': self.content_type,
            'fileName': self.dest_file_name,
            'fileSizeBytes': self.file_size,
            'generatePreview': self.preview,
            'storageLocationId': self.storage_location_id,
            'partSizeBytes': self.part_size,
        }
        expected_uri = "/file/multipart?forceRestart={}".format(
            str(upload._force_restart).lower()
        )

        response = mock.Mock()
        with mock.patch.object(syn, 'restPOST') as restPOST:
            restPOST.return_value = response

            assert upload._create_synapse_upload() == response

        restPOST.assert_called_once_with(
            expected_uri,
            json.dumps(expected_upload_request),
            endpoint=upload._syn.fileHandleEndpoint,
        )

    def test_fetch_presigned_urls(self, syn):
        upload = self._init_upload_attempt(syn)

        upload_id = '1234'
        part_numbers = [1, 3]
        session = mock.Mock()

        response = {
            'partPresignedUrls': [
                {
                    'partNumber': 1,
                    'uploadPresignedUrl': 'https://foo.com/1',
                    'signedHeaders': {'a': 'b'},
                },
                {
                    'partNumber': 3,
                    'uploadPresignedUrl': 'https://foo.com/3',
                    'signedHeaders': {'c': 'd'},
                }
            ]

        }

        with mock.patch.object(syn, 'restPOST') as restPOST:
            restPOST.return_value = response

            expected_return = {
                i['partNumber']: (i['uploadPresignedUrl'], i['signedHeaders'])
                for i in response['partPresignedUrls']
            }

            pre_signed_urls = upload._fetch_pre_signed_part_urls(
                upload_id,
                part_numbers,
                requests_session=session,
            )

        assert pre_signed_urls == expected_return

        expected_uri =\
            "/file/multipart/{upload_id}/presigned/url/batch".format(
                upload_id=upload_id
            )
        expected_body = {
            'uploadId': upload_id,
            'partNumbers': part_numbers,
        }

        restPOST.assert_called_once_with(
            expected_uri,
            json.dumps(expected_body),
            requests_session=session,
            endpoint=upload._syn.fileHandleEndpoint,
        )

    def test_refresh_presigned_part_url__fetch_required(self, syn):
        """Verify that when calling the refresh function that if the
        url that was passed as expired is that last known available
        url for the given part number then we know that no other
        thread has already refreshed and this thread needs to do so."""

        upload = self._init_upload_attempt(syn)

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

            assert refreshed_url == pre_signed_url

            fetch_urls.assert_called_once_with(
                upload._upload_id,
                list(original_presigned_urls.keys())
            )

    def test_refresh_presigned_part_url__no_fetch_required(self, syn):
        """Test that if another thread already refreshed all the
        signed urls after this thread's url was detected as expired
        then we don't need to fetch new urls from synapse."""

        upload = self._init_upload_attempt(syn)

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
            assert refreshed_url == current_url
            fetch_urls.assert_not_called()

    def test_handle_part_aborted(self, syn):
        """Verify that handle part processing short circuits when
        the upload attempt has already been aborted."""

        upload = self._init_upload_attempt(syn)
        upload._aborted = True

        with pytest.raises(SynapseUploadAbortedException):
            upload._handle_part(5)

    def _handle_part_success_test(
        self,
        syn,
        upload,
        part_number,
        expired_url,
        aws_calls,
        chunk,
        refresh_url_response,
    ):

        mock_session = mock.Mock()

        md5_hex = hashlib.md5(chunk).hexdigest()

        with mock.patch.object(multipart_upload, '_get_file_chunk') \
                as chunk_fn, \
                mock.patch.object(upload, '_get_thread_session') \
                as get_session, \
                mock.patch.object(upload, '_refresh_pre_signed_part_urls') \
                as refresh_urls, \
                mock.patch.object(syn, 'restPUT'):

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
            assert (
                mock_session.put.call_args_list ==
                expected_put_calls)

            assert result == (part_number, len(chunk))

            if refresh_url_response:
                refresh_urls.assert_called_once_with(
                    part_number,
                    expired_url,
                )
            else:
                assert not refresh_urls.called

            syn.restPUT.assert_called_once_with(
                "/file/multipart/{upload_id}/add/{part_number}?partMD5Hex={md5}"
                .format(
                    upload_id=upload._upload_id,
                    part_number=part_number,
                    md5=md5_hex,
                ),
                requests_session=mock_session,
                endpoint=upload._syn.fileHandleEndpoint
            )

            assert part_number not in upload._pre_signed_part_urls

    def test_handle_part_success(self, syn):
        """Verify behavior of a successful processing of a part.
        Part bytes should be uploaded to aws, and """

        upload = self._init_upload_attempt(syn)
        upload._upload_id = '123'
        part_number = 1
        chunk = b'1' * TestUploadAttempt.part_size
        pre_signed_url_1 = 'https://foo.com/1'
        signed_headers_1 = {'a': 'b'}

        upload._pre_signed_part_urls = {part_number: (pre_signed_url_1, signed_headers_1)}

        self._handle_part_success_test(
            syn,
            upload,
            part_number,
            pre_signed_url_1,
            [(mock.call(pre_signed_url_1, chunk, headers=signed_headers_1), mock.Mock(status_code=200))],
            chunk,
            None,
        )

    def test_handle_part_expired_url(self, syn):
        """An initial 403 when invoking a presigned url indicates its
        expired, verify that we recovery by refreshing the urls and
        invoking the refreshed url."""

        upload = self._init_upload_attempt(syn)
        upload._upload_id = '123'
        part_number = 1
        chunk = b'1' * TestUploadAttempt.part_size

        pre_signed_url_1 = 'https://foo.com/1'
        signed_headers_1 = {'a': 1}
        pre_signed_url_2 = 'https://bar.com/1'
        signed_headers_2 = {'a': 2}

        upload._pre_signed_part_urls = {part_number: (pre_signed_url_1, signed_headers_1)}

        self._handle_part_success_test(
            syn,
            upload,
            part_number,
            pre_signed_url_1,

            # initial call is expired and results in a 403
            # second call is successful
            [
                (
                    mock.call(pre_signed_url_1, chunk, headers=signed_headers_1),
                    mock.Mock(
                        status_code=403,
                        headers={},
                        reason=''
                    )
                ),
                (
                    mock.call(pre_signed_url_2, chunk, headers=signed_headers_2),
                    mock.Mock(status_code=200)
                ),
            ],
            chunk,
            (pre_signed_url_2, signed_headers_2)
        )

    def test_handle_part__url_expired_twice(self, syn):
        """Verify that consecutive attempts to upload a part resulting
        in a 403 from AWS results in the expected error."""

        upload = self._init_upload_attempt(syn)
        upload._upload_id = '123'
        part_number = 1
        chunk = b'1' * TestUploadAttempt.part_size

        pre_signed_url_1 = 'https://foo.com/1'
        pre_signed_url_2 = 'https://bar.com/1'
        signed_headers = {'a': 1}

        upload._pre_signed_part_urls = {part_number: (pre_signed_url_1, signed_headers)}
        mock_session = mock.Mock()

        with mock.patch.object(multipart_upload, '_get_file_chunk')\
                as chunk_fn,\
                mock.patch.object(upload, '_get_thread_session')\
                as get_session,\
                mock.patch.object(upload, '_refresh_pre_signed_part_urls')\
                as refresh_urls:

            get_session.return_value = mock_session
            chunk_fn.return_value = chunk
            refresh_urls.side_effect = [
                (url, signed_headers) for url in [
                    pre_signed_url_1,
                    pre_signed_url_2,
                ]
            ]

            mock_session.put.return_value = mock.Mock(
                status_code=403,
                headers={},
                reason=''
            )

            with pytest.raises(SynapseHTTPError):
                upload._handle_part(1)

    def test_call_upload(self, syn):
        """Verify the behavior of an upload call, it should trigger
        calls to handle each of the individual outstanding parts
        and then call Synapse indicating that the upload is complete"""

        upload = self._init_upload_attempt(syn)

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
            as get_session,\
            mock.patch.object(syn, 'restPUT')\
                as restPUT:

            mock_session = mock.Mock()
            get_session.return_value = mock_session

            create_synapse_upload.return_value = upload_status
            fetch_pre_signed_urls.return_value = pre_signed_urls

            get_executor.return_value.submit.side_effect = futures

            upload_response = {
                'state': 'COMPLETED'
            }

            restPUT.return_value = upload_response
            result = upload()
            assert result == upload_response

            restPUT.assert_called_once_with(
                "/file/multipart/{upload_id}/complete".format(
                    upload_id=upload_id
                ),
                requests_session=mock_session,
                endpoint=upload._syn.fileHandleEndpoint,
            )

    def _test_call_upload__part_exception(
            self,
            syn,
            part_exception,
            expected_raised_exception
    ):
        upload = self._init_upload_attempt(syn)

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
        future.set_exception(part_exception())

        with mock.patch.object(upload, '_create_synapse_upload')\
            as create_synapse_upload,\
            mock.patch.object(upload, '_fetch_pre_signed_part_urls')\
            as fetch_pre_signed_urls,\
            mock.patch.object(pool_provider, 'get_executor')\
                as get_executor:

            create_synapse_upload.return_value = upload_status
            fetch_pre_signed_urls.return_value = pre_signed_urls

            get_executor.return_value.submit.return_value = future

            with pytest.raises(expected_raised_exception):
                upload()

    def test_call_upload__part_failure(self, syn):
        """Verify that an error raised while processing one part
        results in an error on the upload."""
        self._test_call_upload__part_exception(
            syn,
            Exception,
            SynapseUploadFailedException,
        )

    def test_call_upload__interrupt(self, syn):
        """Verify that a KeyboardInterrupt raises an abort exception"""
        self._test_call_upload__part_exception(
            syn,
            KeyboardInterrupt,
            SynapseUploadAbortedException,
        )

    def test_already_completed(self, syn):
        """Verify that uploading a file that is already complete
        but that wasn't force restarted returns without attempting
        to reupload the file."""
        upload = self._init_upload_attempt(syn)

        upload_id = '1234'
        parts_state = '0'
        upload_status_response = {
            'uploadId': upload_id,
            'partsState': parts_state,
            'state': 'COMPLETED',
        }

        with mock.patch.object(upload, '_create_synapse_upload')\
            as create_synapse_upload,\
            mock.patch.object(upload, '_fetch_pre_signed_part_urls')\
            as fetch_pre_signed_urls,\
            mock.patch.object(pool_provider, 'get_executor')\
                as get_executor:
            create_synapse_upload.return_value = upload_status_response

            upload_result = upload()
            assert upload_status_response == upload_result

            # we should have been able to short circuit any further
            # upload work and have returned immediately
            assert not fetch_pre_signed_urls.called
            assert not get_executor.called

    def test_all_parts_completed(self, syn):
        """Verify that if all the parts are already complete but
        the upload itself hasn't been marked as complete then
        we mark it as such without re-uploading any of the parts."""

        upload = self._init_upload_attempt(syn)

        upload_id = '1234'
        parts_state = '11'

        create_status_response = {
            'uploadId': upload_id,
            'partsState': parts_state,
            'state': 'UPLOADING',
        }
        complete_status_response = {
            'uploadId': upload_id,
            'state': 'COMPLETED',
        }

        with mock.patch.object(upload, '_create_synapse_upload')\
            as create_synapse_upload,\
            mock.patch.object(upload, '_fetch_pre_signed_part_urls')\
            as fetch_pre_signed_urls,\
            mock.patch.object(pool_provider, 'get_executor')\
            as get_executor,\
                mock.patch.object(upload._syn, 'restPUT') as restPUT:

            create_synapse_upload.return_value = create_status_response
            restPUT.return_value = complete_status_response

            upload_result = upload()
            assert complete_status_response == upload_result

            restPUT.assert_called_once()
            assert f"/file/multipart/{upload_id}/complete" in restPUT.call_args_list[0][0]

            # we should have been able to short circuit any further
            # upload work and have returned immediately
            assert not fetch_pre_signed_urls.called
            assert not get_executor.called


class TestMultipartUpload:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test_multipart_upload_file(self):
        """Verify multipart_upload_file passes through its
        args, validating and supplying defaults as expected."""

        syn = mock.Mock()

        file_path = '/foo/bar/baz'
        file_size = 1234
        md5_hex = 'abc123'
        storage_location_id = 5432

        with mock.patch('os.path.exists') as os_path_exists,\
                mock.patch('os.path.isdir') as os_path_is_dir,\
                mock.patch('os.path.getsize') as os_path_getsize,\
                mock.patch.object(
                    multipart_upload,
                    'md5_for_file',
                ) as md5_for_file,\
                mock.patch.object(
                    multipart_upload,
                    '_multipart_upload',
                ) as mock_multipart_upload,\
                mock.patch.object(multipart_upload, 'Spinner') as mock_spinner:

            os_path_getsize.return_value = file_size
            md5_for_file.return_value.hexdigest.return_value = md5_hex

            os_path_exists.return_value = False

            # bad file
            with pytest.raises(IOError):
                multipart_upload_file(syn, file_path, storage_location_id=storage_location_id)

            os_path_exists.return_value = True
            os_path_is_dir.return_value = True

            with pytest.raises(IOError):
                multipart_upload_file(syn, file_path, storage_location_id=storage_location_id)

            os_path_is_dir.return_value = False

            expected_upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
                'contentType': 'application/octet-stream',
                'contentMD5Hex': md5_hex,
                'fileName': 'baz',
                'fileSizeBytes': file_size,
                'generatePreview': True,
                'storageLocationId': storage_location_id,
                'partSizeBytes': DEFAULT_PART_SIZE,
            }

            # call w/ defaults
            multipart_upload_file(
                syn,
                file_path,
                storage_location_id=storage_location_id,
            )

            mock_multipart_upload.assert_called_once_with(
                syn,
                'baz',

                expected_upload_request,
                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                force_restart=False,
                max_threads=None,
            )

            # Test when call the multipart_upload_file, md5_for_file pass in the correct callback function
            syn_with_silent_mode = Synapse(silent=True, skip_checks=True)
            multipart_upload_file(
                syn_with_silent_mode,
                file_path,
                storage_location_id=storage_location_id,
            )
            md5_for_file.assert_called_with(file_path, callback=None)

            syn_with_no_silent_mode = Synapse(debug=False, skip_checks=True)
            multipart_upload_file(
                syn_with_no_silent_mode,
                file_path,
                storage_location_id=storage_location_id,
            )
            md5_for_file.assert_called_with(file_path, callback=mock_spinner.return_value.print_tick)

            mock_multipart_upload.reset_mock()

            # call specifying all optional kwargs
            kwargs = {
                'dest_file_name': 'blort',
                'content_type': 'text/plain',
                'part_size': MIN_PART_SIZE * 2,
                'preview': False,
                'force_restart': True,
                'max_threads': 8,
            }
            expected_upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
                'contentType': kwargs['content_type'],
                'contentMD5Hex': md5_hex,
                'fileName': kwargs['dest_file_name'],
                'fileSizeBytes': file_size,
                'generatePreview': kwargs['preview'],
                'storageLocationId': storage_location_id,
                'partSizeBytes': kwargs['part_size'],
            }
            multipart_upload_file(
                syn,
                file_path,
                storage_location_id=storage_location_id,
                **kwargs
            )
            mock_multipart_upload.assert_called_once_with(
                syn,
                kwargs['dest_file_name'],

                expected_upload_request,
                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                force_restart=kwargs['force_restart'],
                max_threads=kwargs['max_threads'],
            )

    def test_multipart_upload_string(self):
        """Verify multipart_upload_string passes through its
        args, validating and supplying defaults as expected."""

        syn = mock.Mock()
        upload_text = 'foobarbaz'
        storage_location_id = 5432

        with mock.patch.object(
                multipart_upload,
                '_multipart_upload',
           ) as mock_multipart_upload:

            encoded = upload_text.encode('utf-8')
            md5_hex = hashlib.md5(encoded).hexdigest()

            # call w/ default args
            multipart_upload_string(syn, upload_text, storage_location_id=storage_location_id)

            expected_upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
                'contentType': 'text/plain; charset=utf-8',
                'contentMD5Hex': md5_hex,
                'fileName': 'message.txt',
                'fileSizeBytes': len(upload_text),
                'generatePreview': True,
                'storageLocationId': storage_location_id,
                'partSizeBytes': DEFAULT_PART_SIZE,
            }
            mock_multipart_upload.assert_called_once_with(
                syn,
                'message.txt',

                expected_upload_request,
                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                force_restart=False,
                max_threads=None,
            )

            mock_multipart_upload.reset_mock()

            # call specifying all optional kwargs
            kwargs = {
                'dest_file_name': 'blort',
                'content_type': 'text/csv',
                'part_size': MIN_PART_SIZE * 2,
                'storage_location_id': storage_location_id,
                'preview': False,
                'force_restart': True,
                'max_threads': 8,
            }
            multipart_upload_string(syn, upload_text, **kwargs)

            expected_upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
                'contentType': kwargs['content_type'],
                'contentMD5Hex': md5_hex,
                'fileName': kwargs['dest_file_name'],
                'fileSizeBytes': len(upload_text),
                'generatePreview': kwargs['preview'],
                'storageLocationId': storage_location_id,
                'partSizeBytes': kwargs['part_size'],
            }
            mock_multipart_upload.assert_called_once_with(
                syn,
                kwargs['dest_file_name'],

                expected_upload_request,
                mock.ANY,  # part_request_body_provider_fn
                mock.ANY,  # md5_fn,

                force_restart=kwargs['force_restart'],
                max_threads=kwargs['max_threads'],
            )

    def test_multipart_copy__default_args(self):
        """Test multipart copy using only the required positional args.
        Default settings should be used for unspecified params."""

        syn = mock.Mock()
        part_size_bytes = 9876
        file_handle_id = 1234
        associate_object_id = 'syn123456'
        associate_object_type = 'FileEntity'

        source_file_handle_association = {
            'fileHandleId': file_handle_id,
            'associateObjectId': associate_object_id,
            'associateObjectType': associate_object_type,
        }

        with mock.patch.object(multipart_upload, '_multipart_upload') as mock_multipart_upload:
            expected_upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadCopyRequest',
                'fileName': None,
                'generatePreview': True,
                'partSizeBytes': part_size_bytes,
                'sourceFileHandleAssociation': source_file_handle_association,
                'storageLocationId': None
            }

            # call w/ defaults
            multipart_copy(
                syn,
                source_file_handle_association,
                part_size=part_size_bytes,
            )
            mock_multipart_upload.assert_called_once_with(
                syn,
                None,

                expected_upload_request,
                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                force_restart=False,
                max_threads=None,
            )

            assert not syn._print_transfer_progress.called

    def test_multipart_copy__explicit_args(self):
        """Test multipart copy explicitly defining all args.
        The parameterization should be passed through as expected."""

        syn = mock.Mock()
        part_size_bytes = 9876
        file_handle_id = 1234
        associate_object_id = 'syn123456'
        associate_object_type = 'FileEntity'

        source_file_handle_association = {
            'fileHandleId': file_handle_id,
            'associateObjectId': associate_object_id,
            'associateObjectType': associate_object_type,
        }

        storage_location_id = 5432

        with mock.patch.object(multipart_upload, '_multipart_upload') as mock_multipart_upload:

            # call specifying all optional kwargs
            kwargs = {
                'dest_file_name': 'blort',
                'preview': False,
                'storage_location_id': storage_location_id,
                'force_restart': True,
                'max_threads': 8,
                'part_size': part_size_bytes,
            }
            expected_upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadCopyRequest',
                'fileName': kwargs['dest_file_name'],
                'generatePreview': kwargs['preview'],
                'sourceFileHandleAssociation': source_file_handle_association,
                'storageLocationId': kwargs['storage_location_id'],
                'partSizeBytes': kwargs['part_size'],
            }

            multipart_copy(
                syn,
                source_file_handle_association,
                **kwargs,
            )
            mock_multipart_upload.assert_called_once_with(
                syn,
                kwargs['dest_file_name'],

                expected_upload_request,
                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                force_restart=kwargs['force_restart'],
                max_threads=kwargs['max_threads'],
            )

            assert not syn._print_transfer_progress.called

    def _multipart_upload_test(self, upload_side_effect, syn, *args, **kwargs):
        with mock.patch.object(
            multipart_upload,
            'UploadAttempt'
        ) as mock_upload_attempt:
            mock_upload_attempt.side_effect = upload_side_effect

            return _multipart_upload(syn, *args, **kwargs), mock_upload_attempt

    def test_multipart_upload(self):
        """"Verify the behavior of a successful call to multipart_upload
        with various parameterizations applied.  Verify that parameters
        are validated/adjusted as expected."""

        syn = mock.Mock()
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
        # (out_max_threads, out_force_restart)
        tests = [

            # non-positive max threads corrected
            (
                (1234, DEFAULT_PART_SIZE, 0, False),
                (1, False)
            ),

            # specify force_restart
            (
                (pow(2, 28), DEFAULT_PART_SIZE, 8, True),
                (8, True),
            ),

            # no max_threads, specified, should use default
            (
                (pow(2, 28), 1000, None, False),
                (pool_provider.DEFAULT_NUM_THREADS, False),
            ),

            # part size specified below min, should be raised
            (
                (1000, 1, 5, False),
                (5, False),
            ),

            # part size would exceed max number of parts,
            # should be adjusted accordingly
            (
                (pow(2, 36), MIN_PART_SIZE + 1, 8, True),
                (8, True),
            )
        ]

        for (file_size, in_part_size, in_max_threads, in_force_restart),\
            (out_max_threads, out_force_restart)\
                in tests:

            upload_request = {
                'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
                'contentType': content_type,
                'contentMD5Hex': md5_hex,
                'fileName': dest_file_name,
                'fileSizeBytes': file_size,
                'generatePreview': True,
                'storageLocationId': storage_location_id,
                'partSizeBytes': in_part_size,
            }

            result, upload_mock = self._multipart_upload_test(
                upload_side_effect,
                syn,
                dest_file_name,

                upload_request,
                mock.ANY,
                mock.ANY,

                max_threads=in_max_threads,
                force_restart=in_force_restart,
            )

            upload_mock.assert_called_once_with(
                syn,
                dest_file_name,

                upload_request,

                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                out_max_threads,
                out_force_restart,
            )

    def test_multipart_upload__retry_success(self):
        """Verify we recover on a failed upload if a subsequent
        retry succeeds."""

        syn = mock.Mock()
        md5_hex = 'ab123'
        file_size = 1234
        part_size = 567
        dest_file_name = 'foo'
        content_type = 'text/plain'
        storage_location_id = 3210
        result_file_handle_id = 'foo'
        max_threads = 5
        upload_side_effect = [
            SynapseUploadFailedException(),
            SynapseUploadFailedException(),
            mock.Mock(
                return_value={'resultFileHandleId': result_file_handle_id}
            )
        ]

        expected_upload_request = {
            'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
            'contentType': content_type,
            'contentMD5Hex': md5_hex,
            'fileName': dest_file_name,
            'fileSizeBytes': file_size,
            'generatePreview': True,
            'storageLocationId': storage_location_id,
            'partSizeBytes': part_size,
        }

        result, upload_mock = self._multipart_upload_test(
            upload_side_effect,

            syn,
            dest_file_name,

            expected_upload_request,

            mock.ANY,  # part_fn
            mock.ANY,  # md5_fn,

            max_threads,
            False,
        )

        # should have been called multiple times but returned
        # the result in the end.
        assert result_file_handle_id == result
        assert len(upload_side_effect) == upload_mock.call_count

    def test_multipart_upload__retry_failure(self):
        """Verify if we run out of upload attempts we give up
        and raise the failure."""

        syn = mock.Mock()
        md5_hex = 'ab123'
        file_size = 1234
        part_size = 567
        dest_file_name = 'foo'
        content_type = 'text/plain'
        storage_location_id = 3210
        max_threads = 5
        upload_side_effect = SynapseUploadFailedException()

        expected_upload_request = {
            'concreteType': 'org.sagebionetworks.repo.model.file.MultipartUploadRequest',
            'contentType': content_type,
            'contentMD5Hex': md5_hex,
            'fileName': dest_file_name,
            'fileSizeBytes': file_size,
            'generatePreview': True,
            'storageLocationId': storage_location_id,
            'partSizeBytes': part_size
        }

        with pytest.raises(SynapseUploadFailedException):
            self._multipart_upload_test(
                upload_side_effect,

                syn,
                dest_file_name,

                expected_upload_request,

                mock.ANY,  # part_fn
                mock.ANY,  # md5_fn,

                max_threads,
                False,
            )
