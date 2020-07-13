import os
import csv
import pandas as pd
import pandas.util.testing as pdt
from io import StringIO
import tempfile

import pytest
from unittest.mock import patch, create_autospec, Mock, call

import synapseutils
from synapseclient import Activity, File, Folder, Project, Schema
from synapseclient.core.exceptions import SynapseHTTPError
from tests.unit import syn


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
        assert expected_synapseStore == actual_synapseStore


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
        assert expected_synapseStore == actual_synapseStore


def test_syncFromSynapse__non_file_entity():
    table_schema = "syn12345"
    with patch.object(syn, "getChildren", return_value=[]),\
            patch.object(syn, "get", return_value=Schema(name="asssdfa", parent="whatever")):
        pytest.raises(ValueError, synapseutils.syncFromSynapse, syn, table_schema)


def test_syncFromSynapse__empty_folder():
    folder = Folder(name="the folder", parent="whatever", id="syn123")
    with patch.object(syn, "getChildren", return_value=[]),\
            patch.object(syn, "get", return_value=Folder(name="asssdfa", parent="whatever")):
        assert list() == synapseutils.syncFromSynapse(syn, folder)


def test_syncFromSynapse__file_entity():
    file = File(name="a file", parent="some parent", id="syn456")
    with patch.object(syn, "getChildren", return_value=[file]) as patch_syn_get_children,\
            patch.object(syn, "get", return_value=file):
        assert [file] == synapseutils.syncFromSynapse(syn, file)
        patch_syn_get_children.assert_not_called()


def test_syncFromSynapse__folder_contains_one_file():
    folder = Folder(name="the folder", parent="whatever", id="syn123")
    file = File(name="a file", parent=folder, id="syn456")
    with patch.object(syn, "getChildren", return_value=[file]) as patch_syn_get_children,\
            patch.object(syn, "get", return_value=file):
        assert [file] == synapseutils.syncFromSynapse(syn, folder)
        patch_syn_get_children.called_with(folder['id'])


def test_syncFromSynapse__project_contains_empty_folder():
    project = Project(name="the project", parent="whatever", id="syn123")
    file = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")
    with patch.object(syn, "getChildren", side_effect=[[folder, file], []]) as patch_syn_get_children,\
            patch.object(syn, "get", side_effect=[folder, file]) as patch_syn_get:
        assert [file] == synapseutils.syncFromSynapse(syn, project)
        expected_get_children_agrs = [call(project['id']), call(folder['id'])]
        assert expected_get_children_agrs == patch_syn_get_children.call_args_list
        expected_get_args = [
            call(folder['id'], downloadLocation=None, ifcollision='overwrite.local', followLink=False),
            call(file['id'], downloadLocation=None, ifcollision='overwrite.local', followLink=False)]
        assert expected_get_args == patch_syn_get.call_args_list


def _compareCsv(expected_csv_string, csv_path):
    # compare our expected csv with the one written to the given path.
    # compare parsed dictionaries vs just comparing strings to avoid newline differences across platforms
    expected = [r for r in csv.DictReader(StringIO(expected_csv_string), delimiter='\t')]
    with open(csv_path, 'r') as csv_file:
        actual = [r for r in csv.DictReader(csv_file, delimiter='\t')]
    assert expected == actual


def test_syncFromSynase__manifest():
    """Verify that we generate manifest files when syncing to a location outside of the cache."""

    project = Project(name="the project", parent="whatever", id="syn123")
    file1 = File(name="file1", parent=project, id="syn456")
    file2 = File(name="file2", parent=project, id="syn789", parentId='syn098')
    folder = Folder(name="a folder", parent=project, id="syn098")

    file_1_provenance = Activity(data={
        'used': '',
        'executed': '',
    })
    file_2_provenance = Activity(data={
        'used': '',
        'executed': '',
        'name': 'foo',
        'description': 'bar',
    })

    expected_project_manifest = \
        """path\tparent\tname\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
\tsyn098\tfile2\tTrue\t\t\t\tfoo\tbar
\tsyn123\tfile1\tTrue\t\t\t\t\t
"""

    expected_folder_manifest = \
        """path\tparent\tname\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
\tsyn098\tfile2\tTrue\t\t\t\tfoo\tbar
"""

    expected_synced_files = [file2, file1]

    with tempfile.TemporaryDirectory() as sync_dir:

        with patch.object(syn, "getChildren", side_effect=[[folder, file1], [file2]]),\
            patch.object(syn, "get", side_effect=[folder, file2, file1]),\
                patch.object(syn, "getProvenance") as patch_syn_get_provenance:

            patch_syn_get_provenance.side_effect = [file_2_provenance, file_1_provenance]

            synced_files = synapseutils.syncFromSynapse(syn, project, path=sync_dir)

            assert expected_synced_files == synced_files

            # we only expect two calls to provenance even though there are three rows of provenance data
            # in the manifests (two in the outer project, one in the folder)
            # since one of the files is repeated in both manifests we expect only the single get provenance call
            assert len(expected_synced_files) == patch_syn_get_provenance.call_count

            # we should have two manifest files, one rooted at the project and one rooted in the sub folder

            _compareCsv(
                expected_project_manifest,
                os.path.join(sync_dir, synapseutils.sync.MANIFEST_FILENAME)
            )
            _compareCsv(
                expected_folder_manifest,
                os.path.join(sync_dir, folder.name, synapseutils.sync.MANIFEST_FILENAME)
            )


def test_extract_file_entity_metadata__ensure_correct_row_metadata():
    # Test for SYNPY-692, where 'contentType' was incorrectly set on all rows except for the very first row.

    # create 2 file entities with different metadata
    entity1 = File(parent='syn123', id='syn456', contentType='text/json', path='path1', name='entity1',
                   synapseStore=True)
    entity2 = File(parent='syn789', id='syn890', contentType='text/html', path='path2', name='entity2',
                   synapseStore=False)
    files = [entity1, entity2]

    # we don't care about provenance metadata in this case
    with patch.object(synapseutils.sync, "_get_file_entity_provenance_dict", return_value={}):
        # method under test
        keys, data = synapseutils.sync._extract_file_entity_metadata(syn, files)

    # compare source entity metadata gainst the extracted metadata
    for file_entity, file_row_data in zip(files, data):
        for key in keys:
            if key == 'parent':  # workaroundd for parent/parentId inconsistency. (SYNPY-697)
                assert file_entity.get('parentId') == file_row_data.get(key)
            else:
                assert file_entity.get(key) == file_row_data.get(key)


class TestGetFileEntityProvenanceDict:
    """
    test synapseutils.sync._get_file_entity_provenance_dict
    """

    def setup(self):
        self.mock_syn = create_autospec(syn)

    def test_get_file_entity_provenance_dict__error_is_404(self):
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(response=Mock(status_code=404))

        result_dict = synapseutils.sync._get_file_entity_provenance_dict(self.mock_syn, "syn123")
        assert {} == result_dict

    def test_get_file_entity_provenance_dict__error_not_404(self):
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(response=Mock(status_code=400))

        pytest.raises(SynapseHTTPError, synapseutils.sync._get_file_entity_provenance_dict, self.mock_syn, "syn123")
