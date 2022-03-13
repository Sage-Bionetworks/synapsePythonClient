import json
import uuid

import pytest
from unittest.mock import patch, call

import synapseclient
import synapseutils.walk_functions


def test_helpWalk_not_container(syn):
    entity = {"id": "syn123", "concreteType": "File"}
    with patch.object(syn, "get", return_value=entity):
        result = synapseutils.walk_functions._helpWalk(syn=syn, synId="syn123", includeTypes=["folder", "file"])
        # Execute generator
        gen_result = list(result)
    assert gen_result == []


def test_helpWalk_get_children(syn):
    entity = {"id": "syn123", "concreteType": "org.sagebionetworks.repo.model.Project", "name": "parent_folder"}
    child = [{"id": "syn2222", "conreteType": "File", "name": "test_file"}]
    expected = [(('parent_folder', 'syn123'), [], [('test_file', 'syn2222')])]
    with patch.object(syn, "get", return_value=entity),\
         patch.object(syn, "getChildren", return_value=child):
        result = synapseutils.walk_functions._helpWalk(syn=syn, synId="syn123", includeTypes=["folder", "file"])
        # Execute generator
        gen_result = list(result)
    assert gen_result == expected
# def test_helpWalk_not_container(syn):
#     entity = {"id": "syn123", "concreteType": "File"}
#     with patch.object(syn, "get", return_value=entity),\
#          patch.object(syn, "getChildren", return_value=None):
#         synapseutils.walk._helpWalk(syn, "syn123", "syn456", updateLinks=False)
