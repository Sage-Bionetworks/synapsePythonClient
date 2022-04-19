import os
from unittest.mock import patch

import pytest
import synapseutils.walk_functions


def test_helpWalk_not_container(syn):
    """Test if entry entity isn't a container"""
    entity = {"id": "syn123", "concreteType": "File"}
    with patch.object(syn, "get", return_value=entity):
        result = synapseutils.walk_functions._helpWalk(syn=syn, synId="syn123", includeTypes=["folder", "file"])
        # Execute generator
        gen_result = list(result)
    assert gen_result == []


@pytest.mark.parametrize("include_types", [["folder", "file"], ["file", "folder", "dockerrepo", "table"]])
def test_helpWalk_one_child_file(syn, include_types):
    """Test if there is one file in parent directory"""
    entity = {"id": "syn123", "concreteType": "org.sagebionetworks.repo.model.Project", "name": "parent_folder"}
    child = [{"id": "syn2222", "conreteType": "File", "name": "test_file"}]
    expected = [
        (('parent_folder', 'syn123'), [], [('test_file', 'syn2222')])
    ]
    with patch.object(syn, "get", return_value=entity) as mock_syn_get,\
         patch.object(syn, "getChildren", return_value=child) as mock_get_child:
        result = synapseutils.walk_functions._helpWalk(syn=syn, synId="syn123", includeTypes=include_types)
        # Execute generator
        gen_result = list(result)
        mock_syn_get.assert_called_once_with("syn123", downloadFile=False)
        # Make sure the correct include types is passed into the code
        mock_get_child.assert_called_once_with("syn123", include_types)
    assert gen_result == expected


def test_helpWalk_recursive(syn):
    """Test recursive functionality"""
    entity_list = [
        {"id": "syn123", "concreteType": "org.sagebionetworks.repo.model.Project", "name": "parent_folder"},
        {"id": "syn124", "concreteType": "org.sagebionetworks.repo.model.Folder", "name": "test_folder"}
    ]
    child_list = [
        [{"id": "syn2222", "concreteType": "File", "name": "test_file"},
         {"id": "syn124", "concreteType": "org.sagebionetworks.repo.model.Folder", "name": "test_folder"}],
        [{"id": "syn22223", "conreteType": "File", "name": "test_file_2"}]
    ]
    expected = [
        (('parent_folder', 'syn123'), [('test_folder', 'syn124')], [('test_file', 'syn2222')]),
        ((os.path.join('parent_folder', 'test_folder'), 'syn124'), [], [('test_file_2', 'syn22223')])
    ]
    with patch.object(syn, "get", side_effect=entity_list),\
         patch.object(syn, "getChildren", side_effect=child_list):
        result = synapseutils.walk_functions._helpWalk(syn=syn, synId="syn123", includeTypes=["folder", "file"])
        # Execute generator
        gen_result = list(result)
    assert gen_result == expected


def test_helpWalk_newpath(syn):
    """Test new path is utilized correctly"""
    entity = {"id": "syn123", "concreteType": "org.sagebionetworks.repo.model.Project", "name": "parent_folder"}
    child = [{"id": "syn2222", "conreteType": "File", "name": "test_file"}]
    expected = [
        (('testpathnow', 'syn123'), [], [('test_file', 'syn2222')])
    ]
    with patch.object(syn, "get", return_value=entity) as mock_syn_get,\
         patch.object(syn, "getChildren", return_value=child) as mock_get_child:
        result = synapseutils.walk_functions._helpWalk(
            syn=syn, synId="syn123", includeTypes=["folder", "file"], newpath="testpathnow"
        )
        # Execute generator
        gen_result = list(result)
        mock_syn_get.assert_called_once_with("syn123", downloadFile=False)
        mock_get_child.assert_called_once_with("syn123", ["folder", "file"])
    assert gen_result == expected


def test_walk_include_types(syn):
    """Test that "folder" is added to include types"""
    with patch.object(synapseutils.walk_functions, "_helpWalk", return_value="test") as mock_helpwalk:
        results = synapseutils.walk(syn=syn, synId="syn123", includeTypes=["file"])
        # Execute generator
        mock_helpwalk.assert_called_once_with(syn, "syn123", ["file", "folder"])
        # Make sure correct value is returned
        assert results == "test"
