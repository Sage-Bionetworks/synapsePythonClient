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
from unittest.mock import ANY, patch, create_autospec, Mock, call

import synapseutils
from synapseutils.sync import _FolderSync, _SyncUploader, _SyncUploadItem
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
            patch.object(os.path, "isfile", side_effect=[True, True, True, False]):
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
    with patch.object(syn, "get", return_value=Project()),\
            patch.object(os.path, "isfile", return_value=True):  # side effect mocks values for: file1.txt
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
    with patch.object(syn, "get", return_value=Project()),\
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
        )


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
        f"""path\tparent\tname\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{path1}\tsyn123\tfile1\tTrue\t\t\t\t\t
{path2}\tsyn098\tfile2\tTrue\t\t\t\tfoo\tbar
"""

    expected_folder_manifest = \
        f"""path\tparent\tname\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{path2}\tsyn098\tfile2\tTrue\t\t\t\tfoo\tbar
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
        child = _FolderSync(syn, entity_id, path, child_ids, parent)
        assert syn == child._syn
        assert entity_id == child._entity_id
        assert path == child._path
        assert set(child_ids) == child._pending_ids
        assert parent == child._parent

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

    def _finished_test(self, path):
        syn = Mock()
        entity_id = 'syn123'
        child_ids = ['syn456']
        file = Mock()

        parent = _FolderSync(syn, 'syn987', path, [entity_id], None)
        child = _FolderSync(syn, entity_id, (path + '/bar') if path else None, child_ids, parent)

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

            expected_manifest_calls = [
                call(folder_sync._syn, folder_sync._files, manifest_filename,
                     provenance_cache={}),
                call(folder_sync._parent._syn, folder_sync._parent._files, parent_manifest_filename,
                     provenance_cache={}),
            ]
            assert expected_manifest_calls == mock_generateManifest.call_args_list

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
        #  /tmp/4        /tmp/5          |
        #    |_____________|             |
        #           |                    |
        #         /tmp/3                 |
        #           |                    |
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
        assert 'not being uploaded' in cm_ex.value

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
        pending_provenance = set([item.entity.path])
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
        assert len(pending_provenance) == 0

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
