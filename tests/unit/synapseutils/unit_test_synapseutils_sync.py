import csv
from concurrent.futures import Future
import os
import pandas as pd
import pandas.testing as pdt
from io import StringIO
import random
import tempfile
import threading

import pytest
from unittest.mock import ANY, patch, create_autospec, Mock, call, MagicMock

import synapseutils
from synapseutils import sync
from synapseutils.sync import _FolderSync, _PendingProvenance, _SyncUploader, _SyncUploadItem
from synapseclient import Activity, File, Folder, Project, Schema, Synapse
from synapseclient.core.cumulative_transfer_progress import CumulativeTransferProgress
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.utils import id_of
from synapseclient.core.pool_provider import get_executor


def test_readManifest__sync_order_with_home_directory(syn):
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
            patch.object(os.path, "isfile", side_effect=[True, True, True, False]), \
            patch.object(sync, "_check_size_each_file", return_value=Mock()):
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        expected_order = pd.Series([os.path.normpath(os.path.expanduser(file_path2)),
                                    os.path.normpath(os.path.expanduser(file_path1))])
        pdt.assert_series_equal(expected_order, manifest_dataframe.path, check_names=False)


def test_readManifestFile__synapseStore_values_not_set(syn):

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
    with patch.object(syn, "get", return_value=Project()), \
            patch.object(os.path, "isfile", return_value=True), \
            patch.object(sync, "_check_size_each_file", return_value=Mock()):  # side effect mocks values for: file1.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        actual_synapseStore = (manifest_dataframe.set_index('path')['synapseStore'].to_dict())
        assert expected_synapseStore == actual_synapseStore


def test_readManifestFile__synapseStore_values_are_set(syn):

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
    with patch.object(syn, "get", return_value=Project()), \
            patch.object(sync, "_check_size_each_file", return_value=Mock()), \
            patch.object(os.path, "isfile", return_value=True):  # mocks values for: file1.txt, file3.txt, file5.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)

        actual_synapseStore = (manifest_dataframe.set_index('path')['synapseStore'].to_dict())
        assert expected_synapseStore == actual_synapseStore


def test_syncFromSynapse__non_file_entity(syn):
    table_schema = "syn12345"
    with patch.object(syn, "getChildren", return_value=[]),\
            patch.object(syn, "get", return_value=Schema(name="asssdfa", parent="whatever")):
        pytest.raises(ValueError, synapseutils.syncFromSynapse, syn, table_schema)


def test_syncFromSynapse__empty_folder(syn):
    folder = Folder(name="the folder", parent="whatever", id="syn123")
    with patch.object(syn, "getChildren", return_value=[]),\
            patch.object(syn, "get", return_value=Folder(name="asssdfa", parent="whatever")):
        assert list() == synapseutils.syncFromSynapse(syn, folder)


def test_syncFromSynapse__file_entity(syn):
    file = File(name="a file", parent="some parent", id="syn456")
    with patch.object(syn, "getChildren", return_value=[file]) as patch_syn_get_children,\
            patch.object(syn, "get", return_value=file):
        assert [file] == synapseutils.syncFromSynapse(syn, file)
        patch_syn_get_children.assert_not_called()


def test_syncFromSynapse__folder_contains_one_file(syn):
    folder = Folder(name="the folder", parent="whatever", id="syn123")
    file = File(name="a file", parent=folder, id="syn456")
    with patch.object(syn, "getChildren", return_value=[file]) as patch_syn_get_children,\
            patch.object(syn, "get", return_value=file):
        assert [file] == synapseutils.syncFromSynapse(syn, folder)
        patch_syn_get_children.called_with(folder['id'])


def test_syncFromSynapse__project_contains_empty_folder(syn):
    project = Project(name="the project", parent="whatever", id="syn123")
    file = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")

    entities = {
        file.id: file,
        folder.id: folder,
    }

    def syn_get_side_effect(entity, *args, **kwargs):
        return entities[id_of(entity)]

    with patch.object(syn, "getChildren", side_effect=[[folder, file], []]) as patch_syn_get_children,\
            patch.object(syn, "get", side_effect=syn_get_side_effect) as patch_syn_get:

        assert [file] == synapseutils.syncFromSynapse(syn, project)
        expected_get_children_agrs = [call(project['id']), call(folder['id'])]
        assert expected_get_children_agrs == patch_syn_get_children.call_args_list
        patch_syn_get.assert_called_once_with(
            file['id'],
            downloadLocation=None,
            ifcollision='overwrite.local',
            followLink=False,
            downloadFile=True,
        )


def test_syncFromSynapse__downloadFile_is_false(syn):
    """
    Verify when passing the argument downloadFile is equal to False,
    syncFromSynapse won't download the file to clients' local end.
    """

    project = Project(name="the project", parent="whatever", id="syn123")
    file = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")

    entities = {
        file.id: file,
        folder.id: folder,
    }

    def syn_get_side_effect(entity, *args, **kwargs):
        return entities[id_of(entity)]

    with patch.object(syn, "getChildren", side_effect=[[folder, file], []]),\
            patch.object(syn, "get", side_effect=syn_get_side_effect) as patch_syn_get:

        synapseutils.syncFromSynapse(syn, project, downloadFile=False)
        patch_syn_get.assert_called_once_with(
            file['id'],
            downloadLocation=None,
            ifcollision='overwrite.local',
            followLink=False,
            downloadFile=False,
        )


@patch.object(synapseutils.sync, 'generateManifest')
@patch.object(synapseutils.sync, '_get_file_entity_provenance_dict')
def test_syncFromSynapse__manifest_is_all(mock__get_file_entity_provenance_dict, mock_generateManifest, syn):
    """
    Verify manifest argument equal to "all" that pass in to syncFromSynapse, it will create root_manifest and all
    child_manifests for every layers.
    """

    project = Project(name="the project", parent="whatever", id="syn123")
    file1 = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")
    file2 = File(name="a file2", parent=folder, id="syn789123")

    # Structure of nested project
    # project
    #    |---> file1
    #    |---> folder
    #             |---> file2

    entities = {
        file1.id: file1,
        folder.id: folder,
        file2.id: file2,
    }

    def syn_get_side_effect(entity, *args, **kwargs):
        return entities[id_of(entity)]

    mock__get_file_entity_provenance_dict.return_value = {}

    with patch.object(syn, "getChildren", side_effect=[[folder, file1], [file2]]),\
            patch.object(syn, "get", side_effect=syn_get_side_effect) as patch_syn_get:

        synapseutils.syncFromSynapse(syn, project, path="./", downloadFile=False, manifest="all")
        assert patch_syn_get.call_args_list == [call(file1['id'], downloadLocation="./",
                                                ifcollision='overwrite.local', followLink=False, downloadFile=False,),
                                                call(file2['id'], downloadLocation="./a folder",
                                                ifcollision='overwrite.local', followLink=False, downloadFile=False, )]

        assert mock_generateManifest.call_count == 2

        # child_manifest in folder
        call_files = mock_generateManifest.call_args_list[0][0][1]
        assert len(call_files) == 1
        assert call_files[0].id == "syn789123"

        # root_manifest file
        call_files = mock_generateManifest.call_args_list[1][0][1]
        assert len(call_files) == 2
        assert call_files[0].id == "syn456"
        assert call_files[1].id == "syn789123"


@patch.object(synapseutils.sync, 'generateManifest')
@patch.object(synapseutils.sync, '_get_file_entity_provenance_dict')
def test_syncFromSynapse__manifest_is_root(mock__get_file_entity_provenance_dict, mock_generateManifest, syn):
    """
    Verify manifest argument equal to "root" that pass in to syncFromSynapse, it will create root_manifest file only.
    """

    project = Project(name="the project", parent="whatever", id="syn123")
    file1 = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")
    file2 = File(name="a file2", parent=folder, id="syn789123")

    # Structure of nested project
    # project
    #    |---> file1
    #    |---> folder
    #             |---> file2

    entities = {
        file1.id: file1,
        folder.id: folder,
        file2.id: file2,
    }

    def syn_get_side_effect(entity, *args, **kwargs):
        return entities[id_of(entity)]

    mock__get_file_entity_provenance_dict.return_value = {}

    with patch.object(syn, "getChildren", side_effect=[[folder, file1], [file2]]),\
            patch.object(syn, "get", side_effect=syn_get_side_effect) as patch_syn_get:

        synapseutils.syncFromSynapse(syn, project, path="./", downloadFile=False, manifest="root")
        assert patch_syn_get.call_args_list == [call(file1['id'], downloadLocation="./",
                                                ifcollision='overwrite.local', followLink=False, downloadFile=False,),
                                                call(file2['id'], downloadLocation="./a folder",
                                                ifcollision='overwrite.local', followLink=False, downloadFile=False, )]

        assert mock_generateManifest.call_count == 1

        call_files = mock_generateManifest.call_args_list[0][0][1]
        assert len(call_files) == 2
        assert call_files[0].id == "syn456"
        assert call_files[1].id == "syn789123"


@patch.object(synapseutils.sync, 'generateManifest')
@patch.object(synapseutils.sync, '_get_file_entity_provenance_dict')
def test_syncFromSynapse__manifest_is_suppress(mock__get_file_entity_provenance_dict, mock_generateManifest, syn):
    """
    Verify manifest argument equal to "suppress" that pass in to syncFromSynapse, it won't create any manifest file.
    """

    project = Project(name="the project", parent="whatever", id="syn123")
    file1 = File(name="a file", parent=project, id="syn456")
    folder = Folder(name="a folder", parent=project, id="syn789")
    file2 = File(name="a file2", parent=folder, id="syn789123")

    # Structure of nested project
    # project
    #    |---> file1
    #    |---> folder
    #             |---> file2

    entities = {
        file1.id: file1,
        folder.id: folder,
        file2.id: file2,
    }

    def syn_get_side_effect(entity, *args, **kwargs):
        return entities[id_of(entity)]

    mock__get_file_entity_provenance_dict.return_value = {}

    with patch.object(syn, "getChildren", side_effect=[[folder, file1], [file2]]),\
            patch.object(syn, "get", side_effect=syn_get_side_effect) as patch_syn_get:

        synapseutils.syncFromSynapse(syn, project, path="./", downloadFile=False, manifest="suppress")
        assert patch_syn_get.call_args_list == [call(file1['id'], downloadLocation="./",
                                                ifcollision='overwrite.local', followLink=False, downloadFile=False,),
                                                call(file2['id'], downloadLocation="./a folder",
                                                ifcollision='overwrite.local', followLink=False, downloadFile=False, )]

        assert mock_generateManifest.call_count == 0


def test_syncFromSynapse__manifest_value_is_invalid(syn):
    project = Project(name="the project", parent="whatever", id="syn123")
    with pytest.raises(ValueError) as ve:
        synapseutils.syncFromSynapse(syn, project, path="./", downloadFile=False, manifest="invalid_str")
    assert str(ve.value) == 'Value of manifest option should be one of the ("all", "root", "suppress")'


def _compareCsv(expected_csv_string, csv_path):
    # compare our expected csv with the one written to the given path.
    # compare parsed dictionaries vs just comparing strings to avoid newline differences across platforms
    expected = [r for r in csv.DictReader(StringIO(expected_csv_string), delimiter='\t')]
    with open(csv_path, 'r') as csv_file:
        actual = [r for r in csv.DictReader(csv_file, delimiter='\t')]
    assert expected == actual


def test_syncFromSynase__manifest(syn):
    """Verify that we generate manifest files when syncing to a location outside of the cache."""

    project = Project(name="the project", parent="whatever", id="syn123")
    path1 = '/tmp/foo'
    file1 = File(name="file1", parent=project, id="syn456", path=path1)
    path2 = '/tmp/afolder/bar'
    file2 = File(name="file2", parent=project, id="syn789", parentId='syn098', path=path2)
    folder = Folder(name="afolder", parent=project, id="syn098")
    entities = {
        file1.id: file1,
        file2.id: file2,
        folder.id: folder,
    }

    def syn_get_side_effect(entity, *args, **kwargs):
        return entities[id_of(entity)]

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

    provenance = {
        file1.id: file_1_provenance,
        file2.id: file_2_provenance,
    }

    def getProvenance_side_effect(entity, *args, **kwargs):
        return provenance[id_of(entity)]

    expected_project_manifest = \
        f"""path\tparent\tname\tid\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{path1}\tsyn123\tfile1\tsyn456\tTrue\t\t\t\t\t
{path2}\tsyn098\tfile2\tsyn789\tTrue\t\t\t\tfoo\tbar
"""

    expected_folder_manifest = \
        f"""path\tparent\tname\tid\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{path2}\tsyn098\tfile2\tsyn789\tTrue\t\t\t\tfoo\tbar
"""

    expected_synced_files = [file2, file1]

    with tempfile.TemporaryDirectory() as sync_dir:

        with patch.object(syn, "getChildren", side_effect=[[folder, file1], [file2]]),\
                patch.object(syn, "get", side_effect=syn_get_side_effect),\
                patch.object(syn, "getProvenance") as patch_syn_get_provenance:

            patch_syn_get_provenance.side_effect = getProvenance_side_effect

            synced_files = synapseutils.syncFromSynapse(syn, project, path=sync_dir)
            assert sorted([id_of(e) for e in expected_synced_files]) == sorted([id_of(e) for e in synced_files])

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


class TestFolderSync:

    def test_init(self):
        syn = Mock()
        entity_id = 'syn123'
        path = '/tmp/foo/bar'
        child_ids = ['syn456', 'syn789']

        parent = _FolderSync(syn, 'syn987', '/tmp/foo', [entity_id], None)
        child = _FolderSync(syn, entity_id, path, child_ids, parent, create_manifest=False)
        assert syn == child._syn
        assert entity_id == child._entity_id
        assert path == child._path
        assert set(child_ids) == child._pending_ids
        assert parent == child._parent
        assert parent._create_manifest
        assert not child._create_manifest

    def test_update(self):
        syn = Mock()
        entity_id = 'syn123'
        path = '/tmp/foo/bar'
        child_ids = ['syn456', 'syn789']
        folder_sync = _FolderSync(syn, entity_id, path, child_ids, None)

        file = Mock()
        provenance = {'syn456': {'foo': 'bar'}}

        folder_sync.update(finished_id='syn456', files=[file], provenance=provenance)
        assert set(['syn789']) == folder_sync._pending_ids
        assert [file] == folder_sync._files
        assert provenance == folder_sync._provenance

    def _finished_test(self, path, create_manifest=True):
        syn = Mock()
        entity_id = 'syn123'
        child_ids = ['syn456']
        file = Mock()

        parent = _FolderSync(syn, 'syn987', path, [entity_id], None, create_manifest=create_manifest)
        child = _FolderSync(syn, entity_id, (path + '/bar') if path else None, child_ids, parent,
                            create_manifest=create_manifest)

        child.update(finished_id='syn456', files=[file])
        assert child._is_finished()
        assert parent._is_finished()
        parent.wait_until_finished()
        return child

    def test_update__finished(self):
        self._finished_test(None)

    def test_update__finish__generate_manifest(self):
        with patch.object(synapseutils.sync, 'generateManifest') as mock_generateManifest:
            folder_sync = self._finished_test('/tmp/foo')

            manifest_filename = folder_sync._manifest_filename()
            parent_manifest_filename = folder_sync._parent._manifest_filename()

            mock_generateManifest.call_count == 2

            expected_manifest_calls = [
                call(folder_sync._syn, folder_sync._files, manifest_filename,
                     provenance_cache={}),
                call(folder_sync._parent._syn, folder_sync._parent._files, parent_manifest_filename,
                     provenance_cache={}),
            ]
            assert expected_manifest_calls == mock_generateManifest.call_args_list

    def test_update__finish__without_generating_manifest(self):
        """
        Verify the update method won't call generate_manifest if the create_manifest is False
        """
        with patch.object(synapseutils.sync, 'generateManifest') as mock_generateManifest:
            # create_manifest flag is False then won't call generateManifest
            self._finished_test('/tmp/foo', False)
            mock_generateManifest.assert_not_called()

    def test_set_exception(self):
        syn = Mock()
        path = '/tmp/foo'
        entity_id = 'syn123'
        child_ids = ['syn456']

        parent = _FolderSync(syn, 'syn987', path, [entity_id], None)
        child = _FolderSync(syn, entity_id, (path + '/bar') if path else None, child_ids, parent)

        exception = ValueError('failed!')
        child.set_exception(exception)
        assert exception is child.get_exception()
        assert exception is parent.get_exception()
        assert child._is_finished()
        assert parent._is_finished()


def test_extract_file_entity_metadata__ensure_correct_row_metadata(syn):
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


def test_manifest_upload(syn):
    """Verify behavior of synapseutils.sync._manifest_upload"""

    data = {
        'path': ['/tmp/foo', '/tmp/bar', '/tmp/baz'],
        'parent': ['syn123', 'syn456', 'syn789'],
        'name': ['foo', 'bar', 'baz'],
        'used': [None, '/tmp/foo', '/tmp/bar'],
        'executed': [None, None, '/tmp/foo'],
        'anno_1': ['', 'v1', 'v2'],
        'anno_2': ['v3', 'v4', ''],
    }

    # any empty annotations that result from any empty csv column should not
    # be included in the upload
    expected_anno_data = [
        {'anno_2': 'v3'},
        {'anno_1': 'v1', 'anno_2': 'v4'},
        {'anno_1': 'v2'}
    ]

    df = pd.DataFrame(data=data)

    with patch.object(synapseutils.sync, '_SyncUploadItem') as upload_item_init, \
            patch.object(synapseutils.sync, '_SyncUploader') as uploader_init:
        synapseutils.sync._manifest_upload(syn, df)

    upload_items = []
    for i in range(3):
        expected_path = data['path'][i]
        expected_parent = data['parent'][i]
        expected_name = data['name'][i]
        expected_used = data['used'][i]
        expected_executed = data['executed'][i]
        expected_annos = expected_anno_data[i]

        upload_items.append(upload_item_init.return_value)
        upload_item_init_args = upload_item_init.call_args_list[i]
        file, used, executed, store_fields = upload_item_init_args[0]
        assert file.path == expected_path
        assert file.parentId == expected_parent
        assert file.name == expected_name
        assert file.annotations == expected_annos
        assert used == expected_used
        assert executed == expected_executed

    uploader_init.return_value.upload.assert_called_once_with(upload_items)


class TestSyncUploader:

    @patch('os.path.isfile')
    def test_order_items(self, mock_isfile):
        """Verfy that items are properly ordered according to their provenance."""

        def isfile(path):
            return path.startswith('/tmp')

        mock_isfile.side_effect = isfile

        # dependencies flow down
        #
        #                       /tmp/6
        #                         |
        #                  _______|_______
        #                  |             |
        #                  V             |
        #  /tmp/4        /tmp/5          |
        #    |_____________|             |
        #           |                    |
        #           V                    |
        #         /tmp/3                 |
        #           |                    |
        #           V                    V
        #         /tmp/1               /tmp/2

        item_1 = _SyncUploadItem(
            File(path='/tmp/1', parentId='syn123'),
            [],  # used
            [],  # executed
            {},  # annotations
        )
        item_2 = _SyncUploadItem(
            File(path='/tmp/2', parentId='syn123'),
            [],  # used
            [],  # executed
            {},  # annotations
        )
        item_3 = _SyncUploadItem(
            File(path='/tmp/3', parentId='syn123'),
            ['/tmp/1'],  # used
            [],  # executed
            {},  # annotations
        )
        item_4 = _SyncUploadItem(
            File(path='/tmp/4', parentId='syn123'),
            [],  # used
            ['/tmp/3'],  # executed
            {},  # annotations
        )
        item_5 = _SyncUploadItem(
            File(path='/tmp/5', parentId='syn123'),
            ['/tmp/3'],  # used
            [],  # executed
            {},  # annotations
        )
        item_6 = _SyncUploadItem(
            File(path='/tmp/6', parentId='syn123'),
            ['/tmp/5'],  # used
            ['/tmp/2'],  # executed
            {},  # annotations
        )

        items = [
            item_5,
            item_6,
            item_2,
            item_3,
            item_1,
            item_4,
        ]

        random.shuffle(items)

        ordered = _SyncUploader._order_items(items)

        seen = set()
        for i in ordered:
            assert all(p in seen for p in (i.used + i.executed))
            seen.add(i.entity.path)

    @patch('os.path.isfile')
    def test_order_items__provenance_cycle(self, isfile):
        """Verify that if a provenance cycle is detected we raise an error"""

        isfile.return_value = True

        items = [
            _SyncUploadItem(
                File(path='/tmp/1', parentId='syn123'),
                ['/tmp/2'],  # used
                [],
                {},  # annotations
            ),
            _SyncUploadItem(
                File(path='/tmp/2', parentId='syn123'),
                [],  # used
                ['/tmp/1'],  # executed
                {},  # annotations
            ),
        ]

        with pytest.raises(RuntimeError) as cm_ex:
            _SyncUploader._order_items(items)
        assert 'cyclic' in str(cm_ex.value)

    @patch('os.path.isfile')
    def test_order_items__provenance_file_not_uploaded(self, isfile):
        """Verify that if one file depends on another for provenance but that file
        is not included in the upload we raise an error."""

        isfile.return_value = True

        items = [
            _SyncUploadItem(
                File(path='/tmp/1', parentId='syn123'),
                ['/tmp/2'],  # used
                [],
                {},  # annotations
            ),
        ]

        with pytest.raises(ValueError) as cm_ex:
            _SyncUploader._order_items(items)
        assert 'not being uploaded' in str(cm_ex.value)

    def test_upload_item_success(self, syn):
        """Test successfully uploading an item"""

        uploader = _SyncUploader(syn, Mock())

        used = ['foo']
        executed = ['bar']
        item = _SyncUploadItem(
            File(path='/tmp/file', parentId='syn123'),
            used,
            executed,
            {'forceVersion': True},
        )

        finished_items = {}
        mock_condition = create_autospec(threading.Condition())
        pending_provenance = _PendingProvenance()
        pending_provenance.update(set([item.entity.path]))
        abort_event = threading.Event()
        progress = CumulativeTransferProgress('Test Upload')

        mock_stored_entity = Mock()

        with patch.object(syn, 'store') as mock_store, \
                patch.object(uploader._file_semaphore, 'release') as mock_release:

            mock_store.return_value = mock_stored_entity

            uploader._upload_item(
                item,
                used,
                executed,
                finished_items,
                pending_provenance,
                mock_condition,
                abort_event,
                progress,
            )

        mock_store.assert_called_once_with(item.entity, used=used, executed=executed, **item.store_kwargs)

        # item should be finished and removed from pending provenance
        assert mock_stored_entity == finished_items[item.entity.path]
        assert len(pending_provenance._pending) == 0

        # should have notified the condition to let anything waiting on this provenance to continue
        mock_condition.notifyAll.assert_called_once_with()

        assert not abort_event.is_set()

        mock_release.assert_called_once_with()

    def test_upload_item__failure(self, syn):
        """Verify behavior if an item upload fails.
        Exception should be raised, and appropriate threading controls should be released/notified."""

        uploader = _SyncUploader(syn, Mock())

        item = _SyncUploadItem(
            File(path='/tmp/file', parentId='syn123'),
            [],
            [],
            {'forceVersion': True},
        )

        finished_items = {}
        mock_condition = create_autospec(threading.Condition())
        pending_provenance = set([item.entity.path])
        abort_event = threading.Event()
        progress = CumulativeTransferProgress('Test Upload')

        with pytest.raises(ValueError), \
                patch.object(syn, 'store') as mock_store, \
                patch.object(uploader._file_semaphore, 'release') as mock_release:

            mock_store.side_effect = ValueError('Falure during upload')

            uploader._upload_item(
                item,
                item.used,
                item.executed,
                finished_items,
                pending_provenance,
                mock_condition,
                abort_event,
                progress,
            )

        # abort event should have been raised and we shoudl have released threading locks
        assert abort_event.is_set()
        mock_release.assert_called_once_with()
        mock_condition.notifyAll.assert_called_once_with()

    def test_abort(self):
        """Verify abort behavior.
        Should raise an exception chained from the first Exception on a Future and cancel any unfinished Futures"""

        future_spec = Future()
        future_1 = create_autospec(future_spec)
        future_2 = create_autospec(future_spec)
        future_3 = create_autospec(future_spec)

        future_1.done.return_value = True
        future_1.exception.return_value = None

        ex = ValueError('boom')
        future_2.exception.return_value = ex

        future_3.done.return_value = False

        futures = [future_1, future_2, future_3]

        with pytest.raises(ValueError) as cm_ex:
            _SyncUploader._abort(futures)

        assert cm_ex.value.__cause__ == ex
        future_3.cancel.assert_called_once_with()

    def test_upload__error(self, syn):
        """Verify that if an item upload fails the error is raised in the main thread
        and any running Futures are cancelled"""

        item_1 = _SyncUploadItem(File(path='/tmp/foo', parentId='syn123'), [], [], {})
        item_2 = _SyncUploadItem(File(path='/tmp/bar', parentId='syn123'), [], [], {})
        items = [item_1, item_2]

        def syn_store_side_effect(entity, *args, **kwargs):
            if entity.path == entity.path:
                raise ValueError()
            return Mock()

        uploader = _SyncUploader(syn, get_executor())
        original_abort = uploader._abort

        def abort_side_effect(futures):
            return original_abort(futures)

        with patch.object(syn, 'store') as mock_syn_store, \
                patch.object(uploader, '_abort') as mock_abort:

            mock_syn_store.side_effect = syn_store_side_effect
            mock_abort.side_effect = abort_side_effect
            with pytest.raises(ValueError):
                uploader.upload(items)

            # it would be aborted with Futures
            mock_abort.assert_called_once_with([ANY])
            isinstance(mock_abort.call_args_list[0][0], Future)

    @patch('os.path.isfile')
    def test_upload(self, mock_os_isfile, syn):
        """Ensure that an upload including multiple items which depend on each other through
        provenance are all uploaded and in the expected order."""
        mock_os_isfile.return_value = True

        item_1 = _SyncUploadItem(
            File(path='/tmp/foo', parentId='syn123'),
            [],  # used
            [],  # executed
            {},  # annotations
        )
        item_2 = _SyncUploadItem(
            File(path='/tmp/bar', parentId='syn123'),
            ['/tmp/foo'],  # used
            [],  # executed
            {},  # annotations
        )
        item_3 = _SyncUploadItem(
            File(path='/tmp/baz', parentId='syn123'),
            ['/tmp/bar'],  # used
            [],  # executed
            {},  # annotations
        )

        items = [
            item_1,
            item_2,
            item_3,
        ]

        convert_provenance_calls = 2 * len(items)
        convert_provenance_condition = threading.Condition()

        mock_stored_entities = {
            item_1.entity.path: Mock(),
            item_2.entity.path: Mock(),
            item_3.entity.path: Mock(),
        }

        uploader = _SyncUploader(syn, get_executor())

        convert_provenance_original = uploader._convert_provenance

        def patched_convert_provenance(provenance, finished_items):
            # we hack the convert_provenance method as a way of ensuring that
            # the first item doesn't finish storing until the items that depend on it
            # have finished one trip through the wait loop. that way we ensure that
            # our locking logic is being exercised.
            nonlocal convert_provenance_calls
            with convert_provenance_condition:
                convert_provenance_calls -= 1

                if convert_provenance_calls == 0:
                    convert_provenance_condition.notifyAll()

            return convert_provenance_original(provenance, finished_items)

        def syn_store_side_effect(entity, *args, **kwargs):
            if entity.path == item_1.entity.path:
                with convert_provenance_condition:
                    if convert_provenance_calls > 0:
                        convert_provenance_condition.wait_for(lambda: convert_provenance_calls == 0)

            return mock_stored_entities[entity.path]

        with patch.object(uploader, '_convert_provenance') as mock_convert_provenance, \
                patch.object(syn, 'store') as mock_syn_store:

            mock_convert_provenance.side_effect = patched_convert_provenance
            mock_syn_store.side_effect = syn_store_side_effect

            uploader.upload(items)

        # all three of our items should have been stored
        stored = [args[0][0].path for args in mock_syn_store.call_args_list]
        assert [i.entity.path for i in items] == stored


class TestGetFileEntityProvenanceDict:
    """
    test synapseutils.sync._get_file_entity_provenance_dict
    """

    def setup(self):
        self.mock_syn = create_autospec(Synapse)

    def test_get_file_entity_provenance_dict__error_is_404(self):
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(response=Mock(status_code=404))

        result_dict = synapseutils.sync._get_file_entity_provenance_dict(self.mock_syn, "syn123")
        assert {} == result_dict

    def test_get_file_entity_provenance_dict__error_not_404(self):
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(response=Mock(status_code=400))

        pytest.raises(SynapseHTTPError, synapseutils.sync._get_file_entity_provenance_dict, self.mock_syn, "syn123")


@patch.object(sync, 'os')
def test_check_size_each_file(mock_os, syn):
    """
    Verify the check_size_each_file method works correctly
    """

    project_id = "syn123"
    header = 'path\tparent\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = 'http://www.synapse.org'
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))
    path4 = 'http://www.github.com'

    row1 = f'{path1}\t{project_id}\n'
    row2 = f'{path2}\t{project_id}\n'
    row3 = f'{path3}\t{project_id}\n'
    row4 = f'{path4}\t{project_id}\n'

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.basename.side_effect = [
        "file1.txt", "www.synapse.org", "file3.txt", "www.github.com",
        "file1.txt", "www.synapse.org", "file3.txt", "www.github.com",
        "file1.txt", "www.synapse.org", "file3.txt", "www.github.com"
    ]
    mock_os.path.isfile.side_effect = [True, True, True, False]
    mock_os.path.abspath.side_effect = [path1, path3]
    # mock_os.path.basename.return_value = 'file1.txt'
    mock_stat = MagicMock(spec='st_size')
    mock_os.stat.return_value = mock_stat
    mock_stat.st_size = 5

    # mock syn.get() to return a project because the final check is making sure parent is a container
    with patch.object(syn, "get", return_value=Project()):
        sync.readManifestFile(syn, manifest)
        mock_os.stat.call_count == 4


@patch.object(sync, 'os')
def test_check_size_each_file_raise_error(mock_os, syn):
    """
    Verify the check_size_each_file method raises the ValueError when the file is empty.
    """

    project_id = "syn123"
    header = 'path\tparent\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = 'http://www.synapse.org'
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))
    path4 = 'http://www.github.com'

    row1 = f'{path1}\t{project_id}\n'
    row2 = f'{path2}\t{project_id}\n'
    row3 = f'{path3}\t{project_id}\n'
    row4 = f'{path4}\t{project_id}\n'

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    # mock_os.path.basename.side_effect = ["file1.txt", "www.synapse.org", "file3.txt", "www.github.com"]
    mock_os.path.isfile.side_effect = [True, True, True, False]
    mock_os.path.abspath.side_effect = [path1, path3]
    mock_os.path.basename.return_value = 'file1.txt'
    mock_stat = MagicMock(spec='st_size')
    mock_os.stat.return_value = mock_stat
    mock_stat.st_size = 0
    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert str(ve.value) == "File {} is empty, empty files cannot be uploaded to Synapse".format("file1.txt")


@patch.object(sync, 'os')
def test_check_file_name(mock_os, syn):
    """
    Verify the check_file_name method works correctly
    """

    project_id = "syn123"
    header = 'path\tparent\tname\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = os.path.abspath(os.path.expanduser('~/file2.txt'))
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))

    row1 = f"{path1}\t{project_id}\tTest_file_name.txt\n"
    row2 = f"{path2}\t{project_id}\tTest_file-name`s(1).txt\n"
    row3 = f"{path3}\t{project_id}\t\n"

    manifest = StringIO(header + row1 + row2 + row3)
    # mock isfile() to always return true to avoid having to create files in the home directory
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3]
    mock_os.path.basename.return_value = 'file3.txt'

    # mock syn.get() to return a project because the final check is making sure parent is a container
    with patch.object(syn, "get", return_value=Project()):
        sync.readManifestFile(syn, manifest)


@patch.object(sync, 'os')
def test_check_file_name_with_illegal_char(mock_os, syn):
    """
    Verify the check_file_name method raises the ValueError when the file name contains illegal char
    """

    project_id = "syn123"
    header = 'path\tparent\tname\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = os.path.abspath(os.path.expanduser('~/file2.txt'))
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))
    path4 = os.path.abspath(os.path.expanduser('~/file4.txt'))

    row1 = f"{path1}\t{project_id}\tTest_file_name.txt\n"
    row2 = f"{path2}\t{project_id}\tTest_file-name`s(1).txt\n"
    row3 = f"{path3}\t{project_id}\t\n"
    illegal_name = "Test_file_name_with_#.txt"
    row4 = f"{path4}\t{project_id}\t{illegal_name}\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3, path4]
    mock_os.path.basename.return_value = 'file3.txt'

    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert str(ve.value) == "File name {} cannot be stored to Synapse. Names may contain letters, numbers, spaces, " \
                            "underscores, hyphens, periods, plus signs, apostrophes, " \
                            "and parentheses".format(illegal_name)


@patch.object(sync, 'os')
def test_check_file_name_with_too_long_filename(mock_os, syn):
    """
    Verify the check_file_name method raises the ValueError when the file name is too long
    """

    project_id = "syn123"
    header = 'path\tparent\tname\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = os.path.abspath(os.path.expanduser('~/file2.txt'))
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))
    path4 = os.path.abspath(os.path.expanduser('~/file4.txt'))

    long_file_name = 'test_filename_too_long_test_filename_too_long_test_filename_too_long_test_filename_too_long_' \
                     'test_filename_too_long_test_filename_too_long_test_filename_too_long_test_filename_too_long_' \
                     'test_filename_too_long_test_filename_too_long_test_filename_too_long_test_filename_too_long_'

    row1 = f"{path1}\t{project_id}\tTest_file_name.txt\n"
    row2 = f"{path2}\t{project_id}\tTest_file-name`s(1).txt\n"
    row3 = f"{path3}\t{project_id}\t\n"
    row4 = f"{path4}\t{project_id}\t{long_file_name}\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3, path4]
    mock_os.path.basename.return_value = 'file3.txt'

    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert str(ve.value) == "File name {} cannot be stored to Synapse. Names may contain letters, numbers, spaces, " \
                            "underscores, hyphens, periods, plus signs, apostrophes, " \
                            "and parentheses".format(long_file_name)


def test__create_folder(syn):
    folder_name = 'TestName'
    parent_id = 'syn123'
    with patch.object(syn, "store") as patch_syn_store:
        sync._create_folder(syn, folder_name, parent_id)
        patch_syn_store.assert_called_once_with({
            'name': folder_name,
            'concreteType': 'org.sagebionetworks.repo.model.Folder',
            'parentId': parent_id
        })
        sync._create_folder(syn, folder_name * 2, parent_id * 2)
        patch_syn_store.assert_called_with({
            'name': folder_name * 2,
            'concreteType': 'org.sagebionetworks.repo.model.Folder',
            'parentId': parent_id * 2
        })


@patch.object(sync, 'os')
def test__walk_directory_tree(mock_os, syn):
    folder_name = 'TestFolder'
    subfolder_name = 'TestSubfolder'
    parent_id = 'syn123'
    mock_os.walk.return_value = [
        (folder_name, [subfolder_name], ['TestFile.txt']),
        (os.path.join(folder_name, subfolder_name), [], ['TestSubfile.txt'])
    ]
    mock_os.stat.return_value.st_size = 1
    mock_os.path.join.side_effect = os.path.join
    with patch.object(sync, "_create_folder") as mock_create_folder:
        mock_create_folder.return_value = {"id": "syn456"}
        rows = sync._walk_directory_tree(syn, folder_name, parent_id)
        mock_os.walk.assert_called_once_with(folder_name)
        mock_create_folder.assert_called_once_with(
            syn, subfolder_name, parent_id
        )
        assert mock_os.stat.call_count == 2
        assert len(rows) == 2


def test_generate_sync_manifest(syn):
    folder_name = 'TestName'
    parent_id = 'syn123'
    manifest_path = "TestFolder"
    with patch.object(sync, "_walk_directory_tree") as patch_walk_directory_tree, \
         patch.object(sync, "_write_manifest_data") as patch_write_manifest_data:
        sync.generate_sync_manifest(syn, folder_name, parent_id, manifest_path)
        patch_walk_directory_tree.assert_called_once_with(
            syn, folder_name, parent_id
        )
        patch_write_manifest_data.assert_called_with(
            manifest_path, ["path", "parent"], patch_walk_directory_tree.return_value
        )
