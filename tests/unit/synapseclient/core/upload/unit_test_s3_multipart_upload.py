import json
import mock
from nose.tools import assert_equal

from synapseclient.core.upload.s3_multipart_upload import PresignedUrls


class TestPresignedUrls:

    def _mock_syn(self):
        return mock.Mock(
            restPOST=mock.Mock(
                return_value={
                    'partPresignedUrls': [
                        {
                            'partNumber': 1,
                            'uploadPresignedUrl': 'http://1.com'
                        },
                        {
                            'partNumber': 3,
                            'uploadPresignedUrl': 'http://3.com'
                        },
                    ]
                }
            ),
            fileHandleEndpoint='http://foo',
        )

    def test_fetch_urls(self):
        upload_id = '18'
        part_numbers = [1, 3]
        syn = self._mock_syn()
        session = mock.Mock()

        urls = PresignedUrls._fetch_urls(
            syn,
            upload_id,
            part_numbers,
            session=session,
        )

        assert_equal(
            {
                1: "http://1.com",
                3: "http://3.com",
            },
            urls
        )

        syn.restPOST.assert_called_once_with(
            PresignedUrls.PRESIGNED_URL_BATCH_PATH.format(upload_id=upload_id),
            json.dumps({
                'uploadId': upload_id,
                'partNumbers': part_numbers,
            }),
            session=session,
            endpoint=syn.fileHandleEndpoint,
        )

