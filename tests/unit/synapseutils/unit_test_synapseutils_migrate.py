import pytest
import sqlite3
import tempfile
from unittest import mock

import synapseclient
from synapseclient.core.exceptions import SynapseHTTPError
import synapseclient.core.upload
from synapseclient.core import utils
import synapseutils
from synapseutils.migrate_functions import (
    _ensure_schema,
    _MigrationStatus,
    _MigrationType,
    _retrieve_index_settings,
    _verify_index_settings,
)


class TestMigrationResult:

    @pytest.fixture(scope="class", autouse=True)
    def db_path(self):
        values = [
            ('syn1', _MigrationType.PROJECT.value, None, None, None, None, _MigrationStatus.INDEXED.value, None, None, None, None),  # noqa
            ('syn2', _MigrationType.FOLDER.value, None, None, None, 'syn1', _MigrationStatus.INDEXED.value, None, None, None, None),  # noqa
            ('syn3', _MigrationType.FILE.value, 5, None, None, 'syn2', _MigrationStatus.MIGRATED.value, None, 8, 3, 30),  # noqa
            ('syn4', _MigrationType.TABLE_ATTACHED_FILE.value, 5, 1, 2, 'syn2', _MigrationStatus.MIGRATED.value, None, 8, 4, 40),  # noqa
            ('syn5', _MigrationType.TABLE_ATTACHED_FILE.value, 5, 1, 3, 'syn2', _MigrationStatus.ERRORED.value, 'boom', None, None, None),  # noqa
        ]

        db_file = tempfile.NamedTemporaryFile(delete=False)
        with sqlite3.connect(db_file.name) as conn:
            cursor = conn.cursor()
            _ensure_schema(cursor)

            cursor.executemany(
                """
                    insert into migrations (
                        id,
                        type,
                        version,
                        row_id,
                        col_id,
                        parent_id,
                        status,
                        exception,
                        from_storage_location_id,
                        from_file_handle_id,
                        to_file_handle_id
                    ) values (?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values
            )
            conn.commit()

            yield db_file.name

    def test_as_csv(self, db_path):
        syn = mock.MagicMock(synapseclient.Synapse)
        result = synapseutils.migrate_functions.MigrationResult(syn, db_path, 3, 2, 1)

        csv_path = tempfile.NamedTemporaryFile(delete=False)
        with mock.patch.object(syn, 'restGET') as mock_rest_get:

            mock_rest_get.side_effect = [
                {'name': 'col_2'},
                {'name': 'col_3'},
            ]

            result.as_csv(csv_path.name)

            with open(csv_path.name, 'r') as csv_read:
                csv_contents = csv_read.read()

            expected_csv = """id,type,version,row_id,col_name,from_storage_location_id,from_file_handle_id,to_file_handle_id,status,exception
syn3,file,5,,,8,3,30,MIGRATED,
syn4,table,5,1,col_2,8,4,40,MIGRATED,
syn5,table,5,1,col_3,,,,ERRORED,boom
"""  # noqa
            assert csv_contents == expected_csv
            assert result.indexed_total == 3
            assert result.migrated_total == 2
            assert result.error_total == 1

    def test_get_migrations(self, db_path):
        syn = mock.MagicMock(synapseclient.Synapse)
        result = synapseutils.migrate_functions.MigrationResult(syn, db_path, 3, 3, 0)

        with mock.patch.object(syn, 'restGET') as mock_rest_get:
            mock_rest_get.side_effect = [
                {'name': 'col_2'},
                {'name': 'col_3'},
            ]
            migrations = [m for m in result.get_migrations()]

        expected_migrations = [
            {
                'id': 'syn3',
                'type': 'file',
                'version': 5,
                'status': 'MIGRATED',
                'from_storage_location_id': 8,
                'from_file_handle_id': 3,
                'to_file_handle_id': 30,
            },
            {
                'id': 'syn4',
                'type': 'table',
                'version': 5,
                'row_id': 1,
                'col_name': 'col_2',
                'status': 'MIGRATED',
                'from_storage_location_id': 8,
                'from_file_handle_id': 4,
                'to_file_handle_id': 40,
            },
            {
                'id': 'syn5',
                'type': 'table',
                'version': 5,
                'row_id': 1,
                'col_name': 'col_3',
                'status': 'ERRORED',
                'exception': 'boom',
            }
        ]

        assert migrations == expected_migrations
        assert result.indexed_total == 3
        assert result.migrated_total == 3
        assert result.error_total == 0


class TestMigrate:
    """Test a project migration that involves multiple recursive entity migrations.
    All synapse calls are mocked but the local sqlite3 are not in these"""

    @pytest.fixture(scope='function')
    def db_path(self):
        # temp file context manager doesn't work on windows so we manually remove in fixture
        db_file = tempfile.NamedTemporaryFile(delete=False)
        yield db_file.name

    def _migrate_test(self, db_path, syn, continue_on_error):
        # project structure:
        # project (syn1)
        #  folder1 (syn2)
        #    file1 (syn3)
        #    table1 (syn4)
        #  folder2 (syn5)
        #    file2 (syn6)
        #  file3 (syn7)
        #  table2 (syn8)

        old_storage_location = '9876'
        new_storage_location_id = '1234'

        entities = []
        project = synapseclient.Project()
        project.id = 'syn1'
        entities.append(project)

        folder1 = synapseclient.Folder(id='syn2', parentId=project.id)
        folder1.id = 'syn2'
        entities.append(folder1)

        file1 = synapseclient.File(id='syn3', parentId=folder1.id)
        file1.dataFileHandleId = 3
        file1.versionNumber = 1
        file1._file_handle = {
            'id': file1.dataFileHandleId,
            'storageLocationId': old_storage_location
        }
        entities.append(file1)

        table1 = synapseclient.Schema(id='syn4', parentId=folder1.id)
        entities.append(table1)

        folder2 = synapseclient.Folder(id='syn5', parentId=project.id)
        folder2.id = 'syn5'
        entities.append(folder2)

        file2 = synapseclient.File(id='syn6', parentId=folder2.id)
        file2.dataFileHandleId = 6
        file2.versionNumber = 1
        file2._file_handle = {
            'id': file2.dataFileHandleId,
            'storageLocationId': old_storage_location
        }
        entities.append(file2)

        file3 = synapseclient.File(id='syn7', parentId=project.id)
        file3.dataFileHandleId = 7
        file3.versionNumber = 1
        file3._file_handle = {
            'id': file3.dataFileHandleId,
            'storageLocationId': old_storage_location
        }
        entities.append(file3)

        table2 = synapseclient.Schema(id='syn8', parentId=project.id)
        entities.append(table2)

        get_entities = {}
        for entity in entities:
            get_entities[entity.id] = entity

        def mock_syn_get_side_effect(entity, *args, **kwargs):
            entity_id = utils.id_of(entity)
            return get_entities[entity_id]

        def mock_syn_store_side_effect(entity):
            return entity

        def mock_get_file_handle_download_side_effect(fileHandleId, objectId, objectType):
            if fileHandleId == 4:
                return {
                    'fileHandle': {
                        'id': 4,
                        'storageLocationId': 1
                    }
                }

            raise ValueError("Unexpected file handle retrieval {}".format(fileHandleId))

        def mock_syn_get_children_side_effect(entity, includeTypes):
            if set(includeTypes) != {'folder', 'file', 'table'}:
                pytest.fail('Unexpected includeTypes')

            if entity is project.id:
                return [folder1, folder2, file3, table2]
            elif entity is folder1.id:
                return [file1, table1]
            elif entity is folder2.id:
                return [file2]
            else:
                pytest.fail("Shouldn't reach here")

        new_file_handle_mapping = {
            '3': 30,  # file1
            '4': 40,  # table1
            '6': 60,  # file2
            '7': 70,  # file3
        }

        def mock_syn_table_query_side_effect(query_string):
            if query_string == "select filecol from {}".format(table2.id):
                # simulate a failure querying syn8
                raise ValueError('boom')

            if query_string != "select filecol from {}".format(table1.id):
                pytest.fail("Unexpected table query")

            return [['1', '1', 4]]

        def mock_multipart_copy_side_effect(syn, source_file_handle_association, *args, **kwargs):
            # we simulate some failure with the copy for syn6
            if source_file_handle_association['associateObjectId'] == 'syn6':
                raise ValueError('boom')

            return new_file_handle_mapping[source_file_handle_association['fileHandleId']]

        def mock_rest_get_side_effect(uri):
            column_def = {
                'id': 5,
                'columnType': 'FILEHANDLEID',
                'name': 'filecol',
            }

            if uri.startswith('/entity'):
                return {
                    'results': [
                        {'columnType': 'STRING'},
                        column_def
                    ]
                }
            elif uri.startswith('/column'):
                return column_def

            elif uri.startswith('/storageLocation'):
                # just don't error
                return {}

            else:
                raise ValueError('Unexpected restGET call {}'.format(uri))

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(syn, 'store') as mock_syn_store, \
                mock.patch.object(syn, '_getFileHandleDownload') as mock_get_file_handle_download, \
                mock.patch.object(syn, 'getChildren') as mock_syn_get_children, \
                mock.patch.object(syn, 'restGET') as mock_syn_rest_get, \
                mock.patch.object(syn, 'create_snapshot_version') as mock_create_snapshot_version, \
                mock.patch.object(syn, 'tableQuery') as mock_syn_table_query, \
                mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy:

            mock_syn_get.side_effect = mock_syn_get_side_effect
            mock_syn_store.side_effect = mock_syn_store_side_effect
            mock_get_file_handle_download.side_effect = mock_get_file_handle_download_side_effect
            mock_syn_get_children.side_effect = mock_syn_get_children_side_effect
            mock_syn_rest_get.side_effect = mock_rest_get_side_effect
            mock_syn_table_query.side_effect = mock_syn_table_query_side_effect
            mock_multipart_copy.side_effect = mock_multipart_copy_side_effect

            result = synapseutils.migrate(
                syn,
                project,
                new_storage_location_id,
                db_path,
                dry_run=False,
                file_version_strategy='new',
                table_strategy='snapshot',
                continue_on_error=continue_on_error
            )

            file_versions_migrated = [m for m in result.get_migrations()]
            migrated = {}
            errored = {}
            for row in file_versions_migrated:
                entity_id = row['id']
                if row['status'] == 'MIGRATED':
                    migrated[entity_id] = row
                elif row['status'] == 'ERRORED':
                    errored[entity_id] = row

            assert 'syn3' in migrated
            assert 'syn4' in migrated
            assert 'syn7' in migrated
            assert migrated['syn3']['from_file_handle_id'] == 3
            assert migrated['syn3']['to_file_handle_id'] == 30
            assert migrated['syn4']['from_file_handle_id'] == 4
            assert migrated['syn4']['to_file_handle_id'] == 40
            assert migrated['syn7']['from_file_handle_id'] == 7
            assert migrated['syn7']['to_file_handle_id'] == 70
            assert 'syn6' in errored
            assert 'boom' in errored['syn6']['exception']

            mock_create_snapshot_version.assert_called_once_with('syn4')

            assert result.indexed_total == 4
            assert result.migrated_total == 3
            assert result.error_total == 1

    def test_continue_on_error__true(self, db_path, syn):
        """Test a migration of a project when an error is encountered while continuing on the error."""
        # we expect the migration to run to the finish, but a failed file entity will be marked with
        # a failed status and have an exception
        self._migrate_test(db_path, syn, True)

    def test_continue_on_error__false(self, db_path, syn):
        """Test a migration for a project when an error is encountered when aborting on errors"""
        # we expect the error to be surfaced
        with pytest.raises(ValueError) as ex:
            self._migrate_test(db_path, syn, False)
            assert 'boom' in str(ex)

    def test_migrate__dry_run(self, db_path, syn):
        entities = []
        project = synapseclient.Project()
        project.id = 'syn1'
        entities.append(project)

        folder1 = synapseclient.Folder(id='syn2', parentId=project.id)
        folder1.id = 'syn2'
        entities.append(folder1)

        file1 = synapseclient.File(id='syn3', parentId=folder1.id)
        file1.dataFileHandleId = 3
        file1.versionNumber = 1
        entities.append(file1)

        table1 = synapseclient.Schema(id='syn4', parentId=folder1.id)
        entities.append(table1)

        get_entities = {}
        for entity in entities:
            get_entities[entity.id] = entity

        def mock_syn_get_side_effect(entity, *args, **kwargs):
            entity_id = utils.id_of(entity)
            return get_entities[entity_id]

        def mock_syn_store_side_effect(entity):
            return entity

        def mock_get_file_handle_download_side_effect(fileHandleId, objectId, objectType):
            if fileHandleId == 4:
                return {
                    'fileHandle': {
                        'id': 4,
                        'storageLocationId': 1
                    }
                }

            raise ValueError("Unexpected file handle retrieval {}".format(fileHandleId))

        def mock_syn_get_children_side_effect(entity, includeTypes):
            if set(includeTypes) != {'folder', 'file', 'table'}:
                pytest.fail('Unexpected includeTypes')

            if entity is project.id:
                return [folder1]
            elif entity is folder1.id:
                return [file1, table1]
            else:
                pytest.fail("Shouldn't reach here")

        def mock_rest_get_side_effect(uri):
            column_def = {
                'id': 5,
                'columnType': 'FILEHANDLEID',
                'name': 'filecol',
            }

            if uri.startswith('/entity'):
                return {
                    'results': [
                        {'columnType': 'STRING'},
                        column_def
                    ]
                }
            elif uri.startswith('/column'):
                return column_def

            elif uri.startswith('/storageLocation'):
                # just don't error
                return {}

            else:
                raise ValueError('Unexpected restGET call {}'.format(uri))

        def mock_syn_table_query_side_effect(query_string):
            if query_string != "select filecol from {}".format(table1.id):
                pytest.fail("Unexpected table query")

            return [['1', '1', 4]]

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(syn, 'getChildren') as mock_syn_get_children, \
                mock.patch.object(syn, '_getFileHandleDownload') as mock_get_file_handle_download, \
                mock.patch.object(syn, 'restGET') as mock_syn_rest_get, \
                mock.patch.object(syn, 'tableQuery') as mock_syn_table_query, \
                mock.patch.object(syn, 'store') as mock_syn_store, \
                mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy:

            mock_syn_get.side_effect = mock_syn_get_side_effect
            mock_syn_store.side_effect = mock_syn_store_side_effect
            mock_get_file_handle_download.side_effect = mock_get_file_handle_download_side_effect
            mock_syn_get_children.side_effect = mock_syn_get_children_side_effect
            mock_syn_rest_get.side_effect = mock_rest_get_side_effect
            mock_syn_table_query.side_effect = mock_syn_table_query_side_effect

            result = synapseutils.migrate(
                syn,
                project,
                '1234',
                db_path,
                dry_run=True,
                file_version_strategy='new',
                table_strategy='snapshot',
            )

            assert result.indexed_total == 2
            assert result.migrated_total == 0
            assert result.error_total == 0

            migrations = {m['id']: m for m in result.get_migrations()}
            assert migrations.keys() == {'syn3', 'syn4'}
            for migration in result.get_migrations():
                assert migration['status'] == 'INDEXED'

            # should have been no changes
            assert not mock_syn_store.called
            assert not mock_multipart_copy.called


class TestArgValidation:

    def test_file_version_strategy_valid(self, syn, ):
        entity = mock.MagicMock(synapseclient.File)
        storage_location_id = '1234'
        db_path = '/tmo/foo'
        for arg in ('', 0, 'foo'):
            with pytest.raises(ValueError) as ex:
                synapseutils.migrate(syn, entity, storage_location_id, db_path, file_version_strategy=arg)
            assert 'invalid' in str(ex)

    def test_table_strategy_valid(self, syn):
        entity = mock.MagicMock(synapseclient.File)
        storage_location_id = '1234'
        db_path = '/tmo/foo'
        for arg in ('', 0, 'foo'):
            with pytest.raises(ValueError) as ex:
                synapseutils.migrate(syn, entity, storage_location_id, db_path, table_strategy=arg)
            assert 'invalid' in str(ex)

    def test_file_or_table_strategy_required(self, syn):
        entity = mock.MagicMock(synapseclient.File)
        storage_location_id = '1234'
        db_path = '/tmo/foo'

        with pytest.raises(ValueError) as ex:
            synapseutils.migrate(
                syn,
                entity,
                storage_location_id,
                db_path,
                file_version_strategy=None,
                table_strategy=None
            )
            assert 'either' in str(ex)

    def test_storage_location_owner(self, syn):
        def mock_rest_get_side_effect(uri):
            if uri.startswith('/storageLocation'):
                raise SynapseHTTPError(response={'status_code': 403})
            raise ValueError('Unexpected rest GET')

        with mock.patch.object(syn, 'restGET') as mock_rest_get:
            mock_rest_get.side_effect = mock_rest_get_side_effect

            with pytest.raises(ValueError) as ex:
                synapseutils.migrate(syn, mock.MagicMock(synapseclient.File), '123', '/tmp/foo')
            assert 'creator' in str(ex)


def _verify_schema(cursor):
    results = cursor.execute(
        """
            SELECT
              m.name as table_name,
              p.name as column_name
            FROM
              sqlite_master AS m
            JOIN
              pragma_table_info(m.name) AS p
            ORDER BY
              m.name,
              p.cid
        """
    )

    expected_table_columns = {
        'migration_settings': {
            'root_id',
            'storage_location_id',
            'file_version_strategy',
            'skip_table_files'
        },
        'migrations': {
            'id',
            'type',
            'version',
            'row_id',
            'col_id',
            'parent_id',
            'status',
            'exception',
            'from_storage_location_id',
            'from_file_handle_id',
            'to_file_handle_id'
        }
    }

    table_columns = {}
    for row in results:
        table = row[0]
        column = row[1]
        table_columns.setdefault(table, set()).add(column)

    assert table_columns == expected_table_columns


def test_ensure_schema():
    """Verify _ensure_schema bootstraps the necessary schema"""

    with tempfile.NamedTemporaryFile() as db_file, \
            sqlite3.connect(db_file.name) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        _verify_schema(cursor)

        # invoking a second time should be idempotent
        _ensure_schema(cursor)
        _verify_schema(cursor)


def test__verify_index_settings__retrieve_index_settings():
    """Verify the behavior saving index settings and re-retreiving them."""

    with tempfile.NamedTemporaryFile() as db_file, \
            sqlite3.connect(db_file.name) as conn:
        db_path = db_file.name
        cursor = conn.cursor()
        _ensure_schema(cursor)

        root_id = 'syn123'
        storage_location_id = '12345'
        file_version_strategy = 'latest'
        skip_table_files = True

        _verify_index_settings(
            cursor,
            db_path,
            root_id,
            storage_location_id,
            file_version_strategy,
            skip_table_files
        )

        settings = _retrieve_index_settings(cursor)
        assert settings['root_id'] == root_id
        assert settings['storage_location_id'] == storage_location_id
        assert settings['file_version_strategy'] == file_version_strategy
        assert settings['skip_table_files'] == skip_table_files

        # same settings, no error
        _verify_index_settings(
            cursor,
            db_path,
            root_id,
            storage_location_id,
            file_version_strategy,
            skip_table_files
        )

        # changed root_id
        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                'changed',
                storage_location_id,
                file_version_strategy,
                skip_table_files
            )
        assert 'changed' in str(ex.value)

        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                root_id,
                'changed',
                file_version_strategy,
                skip_table_files
            )
        assert 'changed' in str(ex.value)

        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                root_id,
                storage_location_id,
                'changed',
                skip_table_files
            )
        assert 'changed' in str(ex.value)

        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                root_id,
                storage_location_id,
                file_version_strategy,
                'changed'
            )
        assert 'changed' in str(ex.value)
