# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unit
from nose.tools import assert_raises
from mock import patch

import synapseutils


def setup(module):
    module.syn = unit.syn


def test_copyWiki_input_validation():
    to_copy=[{'id': '8688', 'title': 'A Test Wiki'},
             {'id': '8689', 'title': 'A sub-wiki', 'parentId': '8688'},
             {'id': '8690', 'title': 'A sub-sub-wiki', 'parentId': '8689'}]
    wiki={"id": "8786",
          "title": "A Test Wiki"
         }
    entity={"id":"syn123"}
    with patch.object(syn, "getWikiHeaders", return_value=to_copy),\
            patch.object(syn, "get", return_value=entity),\
            patch.object(syn, "getWiki", return_value=wiki) as mock_getWiki,\
            patch.object(syn, "store", return_value=wiki):
        synapseutils.copyWiki(syn, "syn123", "syn456", entitySubPageId="8688", destinationSubPageId="4",
                              updateLinks=False)
        mock_getWiki.assert_called_with({'id': 'syn123'}, "4")
        synapseutils.copyWiki(syn, "syn123", "syn456", entitySubPageId=8688.0, destinationSubPageId=4.0,
                              updateLinks=False)
        mock_getWiki.assert_called_with({'id': 'syn123'},  "4")
        assert_raises(ValueError, synapseutils.copyWiki, syn, "syn123", "syn456", entitySubPageId="some_string",
                              updateLinks=False)