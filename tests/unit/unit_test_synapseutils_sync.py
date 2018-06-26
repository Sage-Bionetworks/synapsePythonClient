# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

import unit
from mock import patch, create_autospec, Mock, call
from nose import SkipTest
from nose.tools import assert_dict_equal, assert_raises, assert_equals, assert_list_equal
from builtins import str

import synapseutils
from synapseclient import Project, Schema, File, Folder
from synapseclient.exceptions import SynapseHTTPError

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import pandas as pd
import pandas.util.testing as pdt


def setup(module):
    module.syn = unit.syn


def test_readManifest__sync_order_with_home_directory():
    """SYNPY-508"""

    # row1's file depends on row2's file but is listed first
    file_path1 = '~/file1.txt'
    file_path2 = '~/file2.txt'
    project_id = "syn123"
    header = 'path	parent	used	executed	activityName	synapseStore	foo\n'
    row1 = '%s\t%s\t%s\t""\tprovActivity1\tTrue\tsomeFooAnnotation1\n' % (file_path1, project_id, file_path2)
    row2 = '%s\t%s\t""\t""\tprovActivity2\tTrue\tsomeFooAnnotation2\n' % (file_path2, project_id)

    manifest = StringIO(header+row1+row2)
    # mock syn.get() to return a project because the final check is making sure parent is a container
    # mock isfile() to always return true to avoid having to create files in the home directory
    # side effect mocks values for: manfiest file, file1.txt, file2.txt, isfile(project.id) check in syn.get()
    with patch.object(syn, "get", return_value=Project()), \
         patch.object(os.path, "isfile", side_effect=[True, True, True, False]):
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        expected_order = pd.Series([os.path.normpath(os.path.expanduser(file_path2)),
                                    os.path.normpath(os.path.expanduser(file_path1))])
        pdt.assert_series_equal(expected_order, manifest_dataframe.path, check_names=False)


def test_readManifestFile__synapseStore_values_not_set():

    project_id = "syn123"
    header = 'path\tparent\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = 'http://www.synapse.org'
    row1 = '%s\t%s\n' % (path1, project_id)
    row2 = '%s\t%s\n' % (path2, project_id)

    expected_synapseStore = {
        str(path1): True,
        str(path2): False,
    }

    manifest = StringIO(header+row1+row2)
    with patch.object(syn, "get", return_value=Project()),\
         patch.object(os.path, "isfile", return_value=True):  # side effect mocks values for: file1.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        actual_synapseStore = (manifest_dataframe.set_index('path')['synapseStore'].to_dict())
        assert_dict_equal(expected_synapseStore, actual_synapseStore)


def test_readManifestFile__synapseStore_values_are_set():

    project_id = "syn123"
    header = 'path\tparent\tsynapseStore\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = 'http://www.synapse.org'
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))
    path4 = 'http://www.github.com'
    path5 = os.path.abspath(os.path.expanduser('~/file5.txt'))
    path6 = 'http://www.checkoutmymixtapefam.com/fire.mp3'

    row1 = '%s\t%s\tTrue\n' % (path1, project_id)
    row2 = '%s\t%s\tTrue\n' % (path2, project_id)
    row3 = '%s\t%s\tFalse\n' % (path3, project_id)
    row4 = '%s\t%s\tFalse\n' % (path4, project_id)
    row5 = '%s\t%s\t""\n' % (path5, project_id)
    row6 = '%s\t%s\t""\n' % (path6, project_id)

    expected_synapseStore = {
        str(path1): True,
        str(path2): False,
        str(path3): False,
        str(path4): False,
        str(path5): True,
        str(path6): False
    }

    manifest = StringIO(header+row1+row2+row3+row4+row5+row6)
    with patch.object(syn, "get", return_value=Project()),\
         patch.object(os.path, "isfile", return_value=True):  # mocks values for: file1.txt, file3.txt, file5.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)

        actual_synapseStore = (manifest_dataframe.set_index('path')['synapseStore'].to_dict())
        assert_dict_equal(expected_synapseStore, actual_synapseStore)


def test_syncFromSynapse__non_file_entity():
    table_schema = "syn12345"
    with patch.object(syn, "getChildren", return_value=[]),\
         patch.object(syn, "get", return_value=Schema(name="asssdfa", parent="whatever")):
        assert_raises(ValueError, synapseutils.syncFromSynapse, syn, table_schema)


def test_syncFromSynapse__empty_folder():
    folder = Folder(name="the folder", parent="whatever", id="syn123")
    with patch.object(syn, "getChildren", return_value=[]),\
         patch.object(syn, "get", return_value=Folder(name="asssdfa", parent="whatever")):
        assert_equals(list(), synapseutils.syncFromSynapse(syn, folder))


def test_syncFromSynapse__file_entity():
    file = File(name="a file", parent="some parent", id="syn456")
    with patch.object(syn, "getChildren", return_value=[file]) as patch_syn_get_children,\
         patch.object(syn, "get", return_value=file):
        assert_equals([file], synapseutils.syncFromSynapse(syn, file))
        patch_syn_get_children.assert_not_called()


def test_syncFromSynapse__folder_contains_one_file():
    folder = Folder(name="the folder", parent="whatever", id="syn123")
    file = File(name="a file", parent=folder, id="syn456")
    with patch.object(syn, "getChildren", return_value=[file]) as patch_syn_get_children,\
         patch.object(syn, "get", return_value=file):
        assert_equals([file], synapseutils.syncFromSynapse(syn, folder))
        patch_syn_get_children.called_with(folder['id'])


def test_syncFromSynapse__project_contains_empty_folder():
    project = Project(name="the project", parent="whatever", id="syn123")
    file = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")
    with patch.object(syn, "getChildren", side_effect=[[folder, file], []]) as patch_syn_get_children,\
         patch.object(syn, "get", side_effect=[folder, file]) as patch_syn_get:
        assert_equals([file], synapseutils.syncFromSynapse(syn, project))
        expected_get_children_agrs = [call(project['id']), call(folder['id'])]
        assert_list_equal(expected_get_children_agrs, patch_syn_get_children.call_args_list)
        expected_get_args = [
            call(folder['id'], downloadLocation=None, ifcollision='overwrite.local', followLink=False),
            call(file['id'], downloadLocation=None, ifcollision='overwrite.local', followLink=False)]
        assert_list_equal(expected_get_args, patch_syn_get.call_args_list)


def test_extract_file_entity_metadata__ensure_correct_row_metadata():
    # Test for SYNPY-692, where 'contentType' was incorrectly set on all rows except for the very first row.

    # create 2 file entities with different metadata
    entity1 = File(parent='syn123', id='syn456', contentType='text/json', path='path1', name='entity1',
                   synapseStore=True)
    entity2 = File(parent='syn789', id='syn890', contentType='text/html', path='path2', name='entity2',
                   synapseStore=False)
    files = [entity1, entity2]

    # we don't care about provenance metadata in this case
    patch.object(synapseutils.sync, "_get_file_entity_provenance_dict", return_value={}).start()

    # method under test
    keys, data = synapseutils.sync._extract_file_entity_metadata(syn, files)

    # compare source entity metadata gainst the extracted metadata
    for file_entity, file_row_data in zip(files, data):
        for key in keys:
            if key == 'parent':  # workaroundd for parent/parentId inconsistency. (SYNPY-697)
                assert_equals(file_entity.get('parentId'), file_row_data.get(key))
            else:
                assert_equals(file_entity.get(key), file_row_data.get(key))


class TestGetFileEntityProvenanceDict:
    """
    test synapseutils.sync._get_file_entity_provenance_dict
    """
    def setup(self):
        self.mock_syn = create_autospec(syn)

    def test_get_file_entity_provenance_dict__error_is_404(self):
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(response=Mock(status_code=404))

        result_dict = synapseutils.sync._get_file_entity_provenance_dict(self.mock_syn, "syn123")
        assert_dict_equal({}, result_dict)

    def test_get_file_entity_provenance_dict__error_not_404(self):
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(response=Mock(status_code=400))

        assert_raises(SynapseHTTPError, synapseutils.sync._get_file_entity_provenance_dict, self.mock_syn, "syn123")
