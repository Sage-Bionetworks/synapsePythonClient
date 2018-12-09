# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unit
from nose.tools import assert_raises, assert_equals
from mock import patch, call
from synapseclient import Project, File, Schema

import uuid
import synapseutils


def setup(module):
    module.syn = unit.syn

class test_copy:
    
    def setup(self):
        self.project_entity = Project(name=str(uuid.uuid4()))
        self.second_project = Project(name=str(uuid.uuid4()))
        self.project_entity.id = "syn1234"
        self.second_project.id = "syn2345"

    def test_copy_permissions(self):

        file_ent = File(name='File', parent=self.project_entity.id)
        file_ent.id = "syn3456"
        schema_ent = Schema(name='Testing', parent=self.project_entity.id)
        schema_ent.id = "syn2456"

        user_name = {'userName':'test'}
        permissions = ["READ"]
        access_requirements = {'totalNumberOfResults':0}

        #TEST: File entity READ permissions not copied
        with patch.object(syn, "get", return_value=file_ent) as patch_syn_get, \
             patch.object(syn, "getUserProfile", return_value=user_name) as patch_syn_get_profile, \
             patch.object(syn, "getPermissions", return_value=permissions) as patch_syn_permissions, \
             patch.object(syn, "restGET", return_value=access_requirements) as patch_syn_restget:
            copied_file = synapseutils.copy(syn, patch_syn_get.id, destinationId=self.second_project.id, skipCopyWikiPage=True)
            assert_equals(copied_file, dict())

        #TEST: Schema entity READ permissions not copied
        with patch.object(syn, "get", return_value=schema_ent) as patch_syn_get, \
             patch.object(syn, "getUserProfile", return_value=user_name) as patch_syn_get_profile, \
             patch.object(syn, "getPermissions", return_value=permissions) as patch_syn_permissions, \
             patch.object(syn, "restGET", return_value=access_requirements) as patch_syn_restget:
            copied_file = synapseutils.copy(syn, patch_syn_get.id, destinationId=self.second_project.id, skipCopyWikiPage=True)
            assert_equals(copied_file, dict())

        permissions = ["READ","DOWNLOAD"]
        access_requirements = {'totalNumberOfResults':1}
        #TEST: Entity access requirement > 0 not copied
        with patch.object(syn, "get", return_value=file_ent) as patch_syn_get, \
             patch.object(syn, "getUserProfile", return_value=user_name) as patch_syn_get_profile, \
             patch.object(syn, "getPermissions", return_value=permissions) as patch_syn_permissions, \
             patch.object(syn, "restGET", return_value=access_requirements) as patch_syn_restget:
            copied_file = synapseutils.copy(syn, patch_syn_get.id, destinationId=self.second_project.id, skipCopyWikiPage=True)
            assert_equals(copied_file, dict())


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