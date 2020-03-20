import json
import uuid

from mock import patch, call
from nose.tools import assert_raises, assert_equal

import synapseutils
from synapseutils.copy_functions import _copy_file_handles_batch, _create_batch_file_handle_copy_request, \
    _batch_iterator_generator
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
        self.patch_private_copy = patch.object(synapseutils.copy_functions, "_copy_file_handles_batch")
        self.mock_private_copy = self.patch_private_copy.start()

    def teardown(self):
        self.patch_private_copy.stop()

    def test_copy_file_handles__invalid_input_params_branch1(self):
        file_handles = ["test"]
        obj_types = []
        obj_ids = ["123"]
        assert_raises(ValueError, synapseutils.copyFileHandles, syn, file_handles, obj_types, obj_ids)
        self.mock_private_copy.assert_not_called()

    def test_copy_file_handles__invalid_input_params_branch2(self):
        file_handles = ["test"]
        obj_types = ["FileEntity"]
        obj_ids = ["123"]
        new_con_type = []
        assert_raises(ValueError, synapseutils.copyFileHandles, syn, file_handles, obj_types, obj_ids, new_con_type)
        self.mock_private_copy.assert_not_called()

    def test_copy_file_handles__invalid_input_params_branch3(self):
        file_handles = ["test"]
        obj_types = ["FileEntity"]
        obj_ids = ["123"]
        new_con_type = ["text/plain"]
        new_file_name = []
        assert_raises(ValueError, synapseutils.copyFileHandles, syn, file_handles, obj_types, obj_ids, new_con_type,
                      new_file_name)
        self.mock_private_copy.assert_not_called()

    def test_copy_file_handles__multiple_batch_calls(self):
        synapseutils.copy_functions.MAX_FILE_HANDLE_PER_COPY_REQUEST = 1  # set batch size to 1
        file_handles = ["789", "NotAccessibleFile"]
        obj_types = ["FileEntity", "FileEntity"]
        obj_ids = ["0987", "2352"]
        con_types = [None, "text/plain"]
        file_names = [None, "testName"]

        expected_calls = [((syn, file_handles[0:1], obj_types[0:1], obj_ids[0:1], con_types[0:1], file_names[0:1]),),
                          ((syn, file_handles[1:2], obj_types[1:2], obj_ids[1:2], con_types[1:2], file_names[1:2]),)]

        return_val_1 = [{
                            "newFileHandle": {
                                "contentMd5": "alpha_num_1",
                                "bucketName": "bucket.sagebase.org",
                                "fileName": "Name1.txt",
                                "createdBy": "111",
                                "contentSize": 16,
                                "concreteType": "type1",
                                "etag": "etag1",
                                "id": "0987",
                                "storageLocationId": 1,
                                "createdOn": "2019-07-24T21:49:40.615Z",
                                "contentType": "text/plain",
                                "key": "key1"
                            },
                            "originalFileHandleId": "789"
                        }]

        return_val_2 = [{
                            "failureCode": "UNAUTHORIZED",
                            "originalFileHandleId": "NotAccessibleFile"
                        }]

        expected_return = return_val_1 + return_val_2

        self.mock_private_copy.side_effect = [return_val_1, return_val_2]  # define multiple returns
        result = synapseutils.copyFileHandles(syn, file_handles, obj_types, obj_ids, con_types, file_names)
        assert_equal(expected_calls, self.mock_private_copy.call_args_list)

        assert_equal(result, expected_return)
        assert_equal(self.mock_private_copy.call_count, 2)


class TestProtectedCopyFileHandlesBatch:

    def setup(self):
        self.patch_restPOST = patch.object(syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test__copy_file_handles_batch__two_file_handles(self):
        file_handles = ["123", "456"]
        obj_types = ["FileEntity", "FileEntity"]
        obj_ids = ["321", "645"]
        con_types = [None, "text/plain"]
        file_names = [None, "test"]
        expected_input = {
                            "copyRequests": [
                                {
                                    "originalFile": {
                                        "fileHandleId": "123",
                                        "associateObjectId": "321",
                                        "associateObjectType": "FileEntity"
                                    },
                                    "newContentType": None,
                                    "newFileName": None
                                },
                                {
                                    "originalFile": {
                                        "fileHandleId": "456",
                                        "associateObjectId": "645",
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
                                "contentMd5": "alpha_num_1",
                                "bucketName": "bucket.sagebase.org",
                                "fileName": "Name1.txt",
                                "createdBy": "111",
                                "contentSize": 16,
                                "concreteType": "type1",
                                "etag": "etag1",
                                "id": "123",
                                "storageLocationId": 1,
                                "createdOn": "2019-07-24T21:49:40.615Z",
                                "contentType": "text/plain",
                                "key": "key1"
                            },
                            "originalFileHandleId": "122"
                        },
                        {
                            "newFileHandle": {
                                "contentMd5": "alpha_num2",
                                "bucketName": "bucket.sagebase.org",
                                "fileName": "Name2.txt",
                                "createdBy": "111",
                                "contentSize": 5,
                                "concreteType": "type2",
                                "etag": "etag2",
                                "id": "456",
                                "storageLocationId": 1,
                                "createdOn": "2019-07-24T21:49:40.638Z",
                                "contentType": "text/plain",
                                "key": "key2"
                            },
                            "originalFileHandleId": "124"
                        }
                    ]
        post_return_val = {"copyResults": return_val}
        self.mock_restPOST.return_value = post_return_val
        result = _copy_file_handles_batch(syn, file_handles, obj_types, obj_ids, con_types, file_names)
        assert_equal(result, return_val)
        self.mock_restPOST.assert_called_once_with('/filehandles/copy', body=json.dumps(expected_input),
                                                   endpoint=syn.fileHandleEndpoint)


class TestProtectedCreateBatchFileHandleCopyRequest:

    def test__create_batch_file_handle_copy_request__no_optional_params(self):
        file_handle_ids = ["123"]
        obj_types = ["FileEntity"]
        obj_ids = ["321"]
        new_con_types = []
        new_file_names = []
        expected_result = {
                            "copyRequests": [
                                {
                                    "originalFile": {
                                        "fileHandleId": "123",
                                        "associateObjectId": "321",
                                        "associateObjectType": "FileEntity"
                                    },
                                    "newFileName": None,
                                    "newContentType": None
                                }
                            ]
                        }
        result = _create_batch_file_handle_copy_request(file_handle_ids, obj_types, obj_ids, new_con_types,
                                                        new_file_names)
        assert_equal(expected_result, result)

    def test__create_batch_file_handle_copy_request__two_file_request(self):
        file_handle_ids = ["345", "789"]
        obj_types = ["FileEntity", "FileEntity"]
        obj_ids = ["543", "987"]
        new_con_types = [None, "text/plain"]
        new_file_names = [None, "test"]
        expected_result = {
                            "copyRequests": [
                                {
                                    "originalFile": {
                                        "fileHandleId": "345",
                                        "associateObjectId": "543",
                                        "associateObjectType": "FileEntity"
                                    },
                                    "newFileName": None,
                                    "newContentType": None
                                },
                                {
                                    "originalFile": {
                                        "fileHandleId": "789",
                                        "associateObjectId": "987",
                                        "associateObjectType": "FileEntity"
                                    },
                                    "newFileName": "test",
                                    "newContentType": "text/plain"
                                }
                            ]
                        }
        result = _create_batch_file_handle_copy_request(file_handle_ids, obj_types, obj_ids, new_con_types,
                                                        new_file_names)
        assert_equal(expected_result, result)


class TestProtectedBatchIteratorGenerator:

    def test__batch_iterator_generator__empty_iterable(self):
        iterables = []
        batch_size = 2
        with assert_raises(ValueError):
            list(_batch_iterator_generator(iterables, batch_size))

    def test__batch_iterator_generator__single_iterable(self):
        iterables = ["ABCDEFG"]
        batch_size = 3
        expected_result_list = [["ABC"], ["DEF"], ["G"]]
        result_list = list(_batch_iterator_generator(iterables, batch_size))
        assert_equal(expected_result_list, result_list)

    def test__batch_iterator_generator__two_iterables(self):
        iterables = [[1, 2, 3], [4, 5, 6]]
        batch_size = 2
        expected_result_list = [[[1, 2], [4, 5]], [[3], [6]]]
        result_list = list(_batch_iterator_generator(iterables, batch_size))
        assert_equal(expected_result_list, result_list)


class TestCopyPermissions:
    """Test copy entities with different permissions"""
    def setup(self):
        self.project_entity = Project(name=str(uuid.uuid4()), id="syn1234")
        self.second_project = Project(name=str(uuid.uuid4()), id="syn2345")
        self.file_ent = File(name='File', parent=self.project_entity.id,
                             id="syn3456")

    def test_dont_copy_read_permissions(self):
        """Entities with READ permissions not copied"""
        permissions = {'canDownload': False}
        with patch.object(syn, "get",
                          return_value=self.file_ent) as patch_syn_get,\
             patch.object(syn, "restGET",
                          return_value=permissions) as patch_rest_get:
            copied_file = synapseutils.copy(syn, self.file_ent,
                                            destinationId=self.second_project.id,
                                            skipCopyWikiPage=True)
            assert_equals(copied_file, dict())
            patch_syn_get.assert_called_once_with(self.file_ent,
                                                  downloadFile=False)
            rest_call = "/entity/{}/permissions".format(self.file_ent.id)
            patch_rest_get.assert_called_once_with(rest_call)


class TestCopyAccessRestriction:
    """Test that entities with access restrictions aren't copied"""
    def setup(self):
        self.project_entity = Project(name=str(uuid.uuid4()), id="syn1234")
        self.second_project = Project(name=str(uuid.uuid4()), id="syn2345")
        self.file_ent = File(name='File', parent=self.project_entity.id)
        self.file_ent.id = "syn3456"

    def test_copy_entity_access_requirements(self):
        # TEST: Entity with access requirement not copied
        access_requirements = {'results': ["fee", "fi"]}
        permissions = {'canDownload': True}
        with patch.object(syn, "get",
                          return_value=self.file_ent) as patch_syn_get,\
             patch.object(syn, "restGET",
                          side_effects=[permissions,
                                        access_requirements]) as patch_rest_get:
            copied_file = synapseutils.copy(syn, self.file_ent,
                                            destinationId=self.second_project.id,
                                            skipCopyWikiPage=True)
            assert_equals(copied_file, dict())
            patch_syn_get.assert_called_once_with(self.file_ent,
                                                  downloadFile=False)
            calls = [call('/entity/{}/accessRequirement'.format(self.file_ent.id)),
                     call("/entity/{}/permissions".format(self.file_ent.id))]
            patch_rest_get.has_calls(calls)


class TestCopy:
    """Test that certain entities aren't copied"""
    def setup(self):
        self.project_entity = Project(name=str(uuid.uuid4()), id="syn1234")
        self.second_project = Project(name=str(uuid.uuid4()), id="syn2345")
        self.file_ent = File(name='File', parent=self.project_entity.id)
        self.file_ent.id = "syn3456"

    def test_no_copy_types(self):
        """Docker repositories and EntityViews aren't copied"""
        access_requirements = {'results': []}
        permissions = {'canDownload': True}
        with patch.object(syn, "get",
                          return_value=self.project_entity) as patch_syn_get,\
             patch.object(syn, "restGET",
                          side_effect=[permissions,
                                       access_requirements]) as patch_rest_get,\
             patch.object(syn, "getChildren") as patch_get_children:
            copied_file = synapseutils.copy(syn, self.project_entity,
                                             destinationId=self.second_project.id,
                                            skipCopyWikiPage=True)
            assert_equals(copied_file, {self.project_entity.id:
                                        self.second_project.id})
            calls = [call(self.project_entity, downloadFile=False),
                     call(self.second_project.id)]
            patch_syn_get.assert_has_calls(calls)
            calls = [call('/entity/{}/accessRequirement'.format(self.file_ent.id)),
                     call("/entity/{}/permissions".format(self.file_ent.id))]
            patch_rest_get.has_calls(calls)
            patch_get_children.assert_called_once_with(self.project_entity,
                                                       includeTypes=['folder', 'file',
                                                                     'table', 'link'])
