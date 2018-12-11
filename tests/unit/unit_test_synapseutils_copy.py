# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unit
from nose.tools import assert_raises, assert_equals
from mock import patch, call

import synapseutils
from synapseclient import Activity, Wiki, Project, Folder, File, Link, Column, Schema, RowSet, Row, EntityViewSchema

import uuid

def setup(module):
    module.syn = unit.syn
    
class test_copy_entityview:
    
    def setup(self):
        self.project_entity = Project(name=str(uuid.uuid4()))
        self.second_project = Project(name=str(uuid.uuid4()))
        self.project_entity.id = "syn1234"
        self.second_project.id = "syn2345"
        cols = [Column(name='n', columnType='DOUBLE', maximumSize=50),
                Column(name='c', columnType='STRING', maximumSize=50),
                Column(name='i', columnType='INTEGER')]
        self.entity_view_schema = EntityViewSchema(name='TestingEntityView', columns=cols, parent=self.project_entity.id, scopes=[self.project_entity.id])
        self.entity_view_schema.id = "syn3456"
        self.copied_view_schema = EntityViewSchema(name='TestingEntityView', columns=cols, parent=self.second_project.id, scopes=[self.project_entity.id])
        self.copied_view_schema.id = "syn1111"

    def test_call_copy_entityview(self):
        expected_view_schema = EntityViewSchema(self.entity_view_schema.name, columns = self.entity_view_schema.columnIds, parent = self.second_project.id, scopes = self.entity_view_schema.scopeIds, add_default_columns=False)
        #TEST: Correct EntityView is copied
        with patch.object(syn, "get", return_value=self.entity_view_schema) as patch_syn_get, \
             patch.object(syn, "findEntityId", return_value=None) as patch_syn_entity, \
             patch.object(syn, "store", return_value=self.copied_view_schema) as patch_syn_store:
            entity_view_schema_map = synapseutils.copy(syn, patch_syn_get.id, destinationId=self.second_project.id, skipCopyWikiPage=True)
            patch_syn_store.assert_called_once_with(expected_view_schema)
            assert_equals(entity_view_schema_map, {self.entity_view_schema.id:self.copied_view_schema.id})

    def test_copy_none_entityview(self):
        #TEST: Entity passed in is None
        assert_raises(TypeError,synapseutils.copy, syn,None, destinationId=self.second_project.id, skipCopyWikiPage=True)

    def test_copy_exists_entityview(self):
        #TEST: Entity exists at target location
        with patch.object(syn, "get", return_value=self.entity_view_schema) as patch_syn_get, \
             patch.object(syn, "findEntityId", return_value="foo") as patch_syn_entity:
             assert_raises(ValueError,synapseutils.copy, syn, patch_syn_get.id, destinationId=self.second_project.id, skipCopyWikiPage=True)
        

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