from nose.tools import assert_raises, assert_equal
from mock import patch, call
import json
import synapseutils
from synapseutils.copy import *
from synapseutils.copy import _copy_file_handles_batch
from tests import unit


def setup(module):
    module.syn = unit.syn


def test_copyWiki_empty_Wiki():
    entity = {"id": "syn123"}
    with patch.object(syn, "getWikiHeaders", return_value=None), \
            patch.object(syn, "get", return_value=entity):
        synapseutils.copyWiki(syn, "syn123", "syn456", updateLinks=False)

def test_copyWiki_input_validation():
    to_copy=[{'id': '8688', 'title': 'A Test Wiki'},
             {'id': '8689', 'title': 'A sub-wiki', 'parentId': '8688'},
             {'id': '8690', 'title': 'A sub-sub-wiki', 'parentId': '8689'}]
    wiki={"id": "8786",
          "title": "A Test Wiki",
          "markdown": "some text"
         }
    entity={"id":"syn123"}
    expected_calls=[call({'id': 'syn123'}, '4'),
                    call({'id': 'syn123'}, '8688'),
                    call({'id': 'syn123'}, '8689'),
                    call({'id': 'syn123'}, '8690')]
    with patch.object(syn, "getWikiHeaders", return_value=to_copy),\
            patch.object(syn, "get", return_value=entity),\
            patch.object(syn, "getWiki", return_value=wiki) as mock_getWiki,\
            patch.object(syn, "store", return_value=wiki):
        synapseutils.copyWiki(syn, "syn123", "syn456", entitySubPageId="8688", destinationSubPageId="4",
                              updateLinks=False)
        mock_getWiki.assert_has_calls(expected_calls)

        synapseutils.copyWiki(syn, "syn123", "syn456", entitySubPageId=8688.0, destinationSubPageId=4.0,
                              updateLinks=False)
        mock_getWiki.assert_has_calls(expected_calls)

        assert_raises(ValueError, synapseutils.copyWiki, syn, "syn123", "syn456", entitySubPageId="some_string",
                              updateLinks=False)


class TestCopyFileHandles:

    def setup(self):
        self.patch_restPOST = patch.object(syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test_copy_invalid_input(self):
        file_handles = ["test"]
        obj_types = []
        obj_ids = [None]
        assert_raises(ValueError, synapseutils.copyFileHandles, syn, file_handles, obj_types, obj_ids)
        self.mock_restPOST.assert_not_called()

    def test_private_copy_batch(self):
        file_handles = ["40827299", "41414463"]
        obj_types = ["FileEntity", "FileEntity"]
        obj_ids = ["20181837", "20459234"]
        con_types = [None, "text/plain"]
        file_names = [None, "test"]
        expected_input = {
            "copyRequests": [
                {
                    "originalFile": {
                        "fileHandleId": "40827299",
                        "associateObjectId": "20181837",
                        "associateObjectType": "FileEntity"
                    },
                    "newContentType": None,
                    "newFileName": None
                },
                {
                    "originalFile": {
                        "fileHandleId": "41414463",
                        "associateObjectId": "20459234",
                        "associateObjectType": "FileEntity"
                    },
                    "newContentType": "text/plain",
                    "newFileName": "test"
                }
            ]
        }
        return_val = [
            {
                "newFileHandle": {
                    "contentMd5": "2d1f1bf95a4bcb0426c1b9f4682b64e5",
                    "bucketName": "proddata.sagebase.org",
                    "fileName": "HelloSynapse.txt",
                    "createdBy": "3391500",
                    "contentSize": 16,
                    "concreteType": "org.sagebionetworks.repo.model.file.S3FileHandle",
                    "etag": "90fc8c8f-e571-4bc4-a823-98b30054d32c",
                    "id": "41791438",
                    "storageLocationId": 1,
                    "createdOn": "2019-07-24T21:49:40.615Z",
                    "contentType": "text/plain",
                    "key": "3391500/202c03b0-0960-49f2-8039-8a90e14854e0/HelloSynapse.txt"
                },
                "originalFileHandleId": "40827299"
            },
            {
                "newFileHandle": {
                    "contentMd5": "d8e8fca2dc0f896fd7cb4cb0031ba249",
                    "bucketName": "proddata.sagebase.org",
                    "fileName": "test",
                    "createdBy": "3391500",
                    "contentSize": 5,
                    "concreteType": "org.sagebionetworks.repo.model.file.S3FileHandle",
                    "etag": "c9f04c33-6527-4dea-8601-64affe95e997",
                    "id": "41791439",
                    "storageLocationId": 1,
                    "createdOn": "2019-07-24T21:49:40.638Z",
                    "contentType": "wiki",
                    "key": "3391500/6436b18c-61de-4983-870f-45cda5e425e6/HelloSynapse2.txt"
                },
                "originalFileHandleId": "41414463"
            }
        ]
        post_return_val = {"copyResults": return_val}
        with patch.object(syn, "restPOST", return_value=post_return_val) as mocked_POST:
            result = _copy_file_handles_batch(syn, file_handles, obj_types, obj_ids, con_types, file_names)
            assert_equal(result, post_return_val)

    def test_large_copy(self):
        num_copies = 202
        max_copy_per_request = 100
        file_handles = [str(x) for x in range(num_copies)]
        obj_types = ["FileEntity"] * num_copies
        obj_ids = [str(x) for x in range(num_copies)]
        con_types = ["text/plain"] * num_copies
        file_names = ["test" + str(i) for i in range(num_copies)]
        synapseutils.copyFileHandles(syn, file_handles, obj_types, obj_ids, con_types, file_names)
        expected_input_1 = synapseutils.create_batch_file_handle_copy_request(file_handles[:max_copy_per_request],
                                                                             obj_types[:max_copy_per_request],
                                                                             obj_ids[:max_copy_per_request],
                                                                             con_types[:max_copy_per_request],
                                                                             file_names[:max_copy_per_request])
        expected_input_2 = synapseutils.create_batch_file_handle_copy_request(file_handles[max_copy_per_request
                                                                                          :max_copy_per_request * 2],
                                                                             obj_types[max_copy_per_request
                                                                                       :max_copy_per_request * 2],
                                                                             obj_ids[max_copy_per_request
                                                                                     :max_copy_per_request * 2],
                                                                             con_types[max_copy_per_request
                                                                                       :max_copy_per_request * 2],
                                                                             file_names[max_copy_per_request
                                                                                        :max_copy_per_request * 2])
        expected_input_3 = synapseutils.create_batch_file_handle_copy_request(file_handles[max_copy_per_request * 2
                                                                                          :max_copy_per_request * 3],
                                                                             obj_types[max_copy_per_request * 2
                                                                                       :max_copy_per_request * 3],
                                                                             obj_ids[max_copy_per_request * 2
                                                                                     :max_copy_per_request * 3],
                                                                             con_types[max_copy_per_request * 2
                                                                                       :max_copy_per_request * 3],
                                                                             file_names[max_copy_per_request * 2
                                                                                        :max_copy_per_request * 3])
        self.mock_restPOST.assert_any_call('/filehandles/copy', body=json.dumps(expected_input_1),
                                           endpoint=syn.fileHandleEndpoint)
        self.mock_restPOST.assert_any_call('/filehandles/copy', body=json.dumps(expected_input_2),
                                           endpoint=syn.fileHandleEndpoint)
        self.mock_restPOST.assert_any_call('/filehandles/copy', body=json.dumps(expected_input_3),
                                           endpoint=syn.fileHandleEndpoint)
        assert_equal(1, 1)
