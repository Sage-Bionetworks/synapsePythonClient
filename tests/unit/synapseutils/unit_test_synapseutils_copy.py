from nose.tools import assert_raises
from mock import patch, call
import json
import synapseutils
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


def test_copyFileHandles_input_validation():
    file_handles = ["test"]
    obj_types = []
    obj_ids = [None]
    assert_raises(ValueError, synapseutils.copyFileHandles, syn, file_handles, obj_types, obj_ids)


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

    def test_copy_two_files(self):
        file_handles = ["40827299", "41414463"]
        obj_types = ["FileEntity", "FileEntity"]
        obj_ids = ["20181837", "20459234"]
        con_types = [None, "wiki"]
        file_names = [None, "test"]
        synapseutils.copyFileHandles(syn, file_handles, obj_types, obj_ids, con_types, file_names)
        expected = {
                        "copyRequests": [
                            {
                                "originalFile": {
                                    "fileHandleId": "40827299",
                                    "associateObjectId": "20181837",
                                    "associateObjectType": "FileEntity"
                                }
                            },
                            {
                                "originalFile": {
                                    "fileHandleId": "41414463",
                                    "associateObjectId": "20459234",
                                    "associateObjectType": "FileEntity"
                                },
                                "newContentType": "wiki",
                                "newFileName": "test"
                            }
                        ]
                    }
        self.mock_restPOST.assert_called_once_with('/filehandles/copy', body=json.dumps(expected), endpoint=syn.fileHandleEndpoint)