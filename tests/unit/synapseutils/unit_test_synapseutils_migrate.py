import json
import pytest
import sqlite3
import tempfile
import threading
from unittest import mock, skipIf

import synapseclient
from synapseclient.core.exceptions import SynapseHTTPError
import synapseclient.core.upload
from synapseclient.core.constants.concrete_types import FILE_ENTITY, FOLDER_ENTITY, PROJECT_ENTITY, TABLE_ENTITY
from synapseclient.core import utils
import synapseutils
from synapseutils import migrate_functions
from synapseutils.migrate_functions import (
    _check_indexed,
    _create_new_file_version,
    _confirm_migration,
    _ensure_schema,
    _get_row_dict,
    _index_container,
    _index_entity,
    _index_file_entity,
    _index_table_entity,
    _IndexingError,
    _migrate_file_version,
    _migrate_table_attached_file,
    _MigrationKey,
    _MigrationStatus,
    _MigrationType,
    _retrieve_index_settings,
    _verify_index_settings,
    _verify_storage_location_ownership,
    index_files_for_migration,
    migrate_indexed_files,
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
            ('syn6', _MigrationType.FILE.value, 6, None, None, 'syn2', _MigrationStatus.INDEXED.value, None, 3, 6, None),  # noqa
            ('syn7', _MigrationType.FILE.value, 7, None, None, 'syn2', _MigrationStatus.ALREADY_MIGRATED.value, None, 10, 7, None),  # noqa

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
        result = synapseutils.migrate_functions.MigrationResult(syn, db_path)

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
syn6,file,6,,,3,6,,INDEXED,
syn7,file,7,,,10,7,,ALREADY_MIGRATED,
"""  # noqa
            assert csv_contents == expected_csv

            counts_by_status = result.get_counts_by_status()
            assert counts_by_status['INDEXED'] == 1
            assert counts_by_status['MIGRATED'] == 2
            assert counts_by_status['ERRORED'] == 1
            assert counts_by_status['ALREADY_MIGRATED'] == 1

    def test_get_migrations(self, db_path):
        syn = mock.MagicMock(synapseclient.Synapse)
        result = synapseutils.migrate_functions.MigrationResult(syn, db_path)

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
            },
            {
                'id': 'syn6',
                'type': 'file',
                'version': 6,
                'status': 'INDEXED',
                'from_storage_location_id': 3,
                'from_file_handle_id': 6,
            },
            {
                'id': 'syn7',
                'type': 'file',
                'version': 7,
                'status': 'ALREADY_MIGRATED',
                'from_storage_location_id': 10,
                'from_file_handle_id': 7,
            },
        ]

        assert migrations == expected_migrations

        counts_by_status = result.get_counts_by_status()
        assert counts_by_status['MIGRATED'] == 2
        assert counts_by_status['ERRORED'] == 1
        assert counts_by_status['INDEXED'] == 1
        assert counts_by_status['ALREADY_MIGRATED'] == 1


class TestIndex:

    @pytest.fixture(scope='function')
    def conn(self):
        # temp file context manager doesn't work on windows so we manually remove in fixture
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile, \
                sqlite3.connect(tmpfile.name) as conn:
            yield conn

    def test_check_indexed(self, conn):
        entity_id = 'syn57'

        cursor = conn.cursor()
        _ensure_schema(cursor)

        cursor.execute(
            "insert into migrations (id, type, status) values (?, ?, ?)",
            (entity_id, _MigrationType.FILE.value, _MigrationStatus.INDEXED.value)
        )
        conn.commit()

        assert _check_indexed(cursor, entity_id)
        assert not _check_indexed(cursor, 'syn22')

    def _index_file_entity_version_test(self, conn, file_version_strategy):
        entity_id = 'syn123'
        parent_id = 'syn321'
        latest_version_number = 5
        from_storage_location_id = '1234'
        to_storage_location_id = '4321'
        data_file_handle_id = '5678'
        file_size = 9876

        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        syn = mock.MagicMock(synapseclient.Synapse)

        mock_file = mock.MagicMock(synapseclient.File)
        mock_file.versionNumber = latest_version_number
        mock_file.dataFileHandleId = data_file_handle_id
        mock_file._file_handle = {
            'contentSize': file_size,
            'storageLocationId': from_storage_location_id,
        }
        syn.get.return_value = mock_file

        _index_file_entity(cursor, syn, entity_id, parent_id, to_storage_location_id, file_version_strategy)

        row = cursor.execute(
            """
                select
                    id,
                    parent_id,
                    version,
                    from_storage_location_id,
                    from_file_handle_id,
                    file_size,
                    status
                from migrations
            """
        ).fetchone()

        row_dict = _get_row_dict(cursor, row, True)

        assert row_dict['id'] == entity_id
        assert row_dict['parent_id'] == parent_id
        assert row_dict['from_storage_location_id'] == from_storage_location_id
        assert row_dict['from_file_handle_id'] == data_file_handle_id
        assert row_dict['file_size'] == file_size
        assert row_dict['status'] == _MigrationStatus.INDEXED.value
        return mock_file, row_dict

    def test_index_file_entity__new(self, conn):
        """Verify indexing creating a record to migrate a new version of a file entity"""
        # record version should be None representing the need to create a new version
        _, row_dict = self._index_file_entity_version_test(conn, 'new')
        assert row_dict['version'] is None

    def test_index_file_entity__latest(self, conn):
        """Verify indexing creating a record to migrate the latest version of a file entity"""
        # record version should match the file version representing the need to migrate that version
        mock_file, row_dict = self._index_file_entity_version_test(conn, 'latest')
        assert mock_file.versionNumber == row_dict['version']

    def test_index_file_entity__all(self, conn):
        """Verify indexing all the file version of a FileEntity"""

        entity_id = 'syn123'
        parent_id = 'syn321'
        from_storage_location_id = '1234'
        to_storage_location_id = '4321'

        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        syn = mock.MagicMock(synapseclient.Synapse)

        mock_file_2 = mock.MagicMock(synapseclient.File)
        mock_file_2.dataFileHandleId = 2
        mock_file_2_size = 9876
        mock_file_2._file_handle = {
            'contentSize': mock_file_2_size,
            'storageLocationId': from_storage_location_id
        }
        mock_file_2.versionNumber = 2

        # already in the destination storage location
        mock_file_3 = mock.MagicMock(synapseclient.File)
        mock_file_3.dataFileHandleId = 3
        mock_file_3_size = 1234
        mock_file_3._file_handle = {
            'contentSize': mock_file_3_size,
            'storageLocationId': to_storage_location_id
        }
        mock_file_3.versionNumber = 3

        syn.get.side_effect = [
            mock_file_2,
            mock_file_3
        ]

        # simulate multiple versions
        syn._GET_paginated.return_value = [
            {'versionNumber': 2},
            {'versionNumber': 3},
        ]

        _index_file_entity(cursor, syn, entity_id, parent_id, to_storage_location_id, 'all')

        result = cursor.execute(
            """
                select
                    id,
                    parent_id,
                    version,
                    from_storage_location_id,
                    from_file_handle_id,
                    file_size,
                    status
                from migrations
            """
        ).fetchall()

        result_iter = iter(result)
        row_0 = next(result_iter)
        row_1 = next(result_iter)

        row_dict_0 = _get_row_dict(cursor, row_0, True)
        row_dict_1 = _get_row_dict(cursor, row_1, True)
        for version, row_dict in enumerate([row_dict_0, row_dict_1], 2):
            assert row_dict['id'] == entity_id
            assert row_dict['parent_id'] == parent_id
            assert row_dict['version'] == version
            assert row_dict['from_file_handle_id'] == str(version)

        assert row_dict_0['status'] == _MigrationStatus.INDEXED.value
        assert row_dict_0['from_storage_location_id'] == from_storage_location_id
        assert row_dict_0['file_size'] == mock_file_2_size
        assert row_dict_1['status'] == _MigrationStatus.ALREADY_MIGRATED.value
        assert row_dict_1['from_storage_location_id'] == to_storage_location_id
        assert row_dict_1['file_size'] == mock_file_3_size

    def test_index_table_entity(self, mocker, conn):
        mock_get_batch_size = mocker.patch.object(synapseutils.migrate_functions, '_get_batch_size')

        cursor = conn.cursor()
        _ensure_schema(cursor)

        # small batch size to test multiple batches
        mock_get_batch_size.return_value = 1

        table_id = 'syn123'
        parent_id = 'syn321'
        from_storage_location_id = 1
        to_storage_location_id = 2
        syn = mock.MagicMock(synapseclient.Synapse)

        # gets the columns of the table
        # 4 columns, 2 of the file handles
        syn.restGET.return_value = {
            'results': [
                {'columnType': 'STRING'},
                {'columnType': 'FILEHANDLEID', 'id': '1', 'name': 'col1'},
                {'columnType': 'FILEHANDLEID', 'id': '2', 'name': 'col2'},
                {'columnType': 'INTEGER'},
            ]
        }

        file_handle_id_1 = 'fh1'
        file_handle_id_2 = 'fh2'
        file_handle_id_3 = 'fh3'
        file_handle_id_4 = 'fh4'

        file_handle_size_1 = 100
        file_handle_size_2 = 200
        file_handle_size_3 = 300
        file_handle_size_4 = 400

        file_handle_1 = {
            'fileHandle': {
                'id': file_handle_id_1,
                'contentSize': file_handle_size_1,
                'storageLocationId': from_storage_location_id,
            }
        }
        file_handle_2 = {
            'fileHandle': {
                'id': file_handle_id_2,
                'contentSize': file_handle_size_2,
                'storageLocationId': from_storage_location_id,
            }
        }
        file_handle_3 = {
            'fileHandle': {
                'id': file_handle_id_3,
                'contentSize': file_handle_size_3,
                'storageLocationId': from_storage_location_id,
            }
        }

        # this one already in the destination storage location
        file_handle_4 = {
            'fileHandle': {
                'id': file_handle_id_4,
                'contentSize': file_handle_size_4,
                'storageLocationId': to_storage_location_id,
            }
        }

        syn.tableQuery.return_value = [
            [1, 1, file_handle_id_1, file_handle_id_2],
            [2, 1, file_handle_id_3, file_handle_id_4],
        ]

        syn._getFileHandleDownload.side_effect = [
            file_handle_1,
            file_handle_2,
            file_handle_3,
            file_handle_4,
        ]

        _index_table_entity(cursor, syn, table_id, parent_id, to_storage_location_id)

        result = cursor.execute(
            """
                select
                    id,
                    parent_id,
                    row_id,
                    col_id,
                    from_storage_location_id,
                    from_file_handle_id,
                    status
                from migrations
            """
        ).fetchall()

        row_iter = iter(result)
        row_0_dict = _get_row_dict(cursor, next(row_iter), True)
        row_1_dict = _get_row_dict(cursor, next(row_iter), True)
        row_2_dict = _get_row_dict(cursor, next(row_iter), True)
        row_3_dict = _get_row_dict(cursor, next(row_iter), True)
        row_dicts = [row_0_dict, row_1_dict, row_2_dict]

        for row_dict in row_dicts:
            assert row_dict['id'] == table_id
            assert row_dict['parent_id'] == parent_id
            assert row_dict['from_storage_location_id'] == from_storage_location_id
            assert row_dict['status'] == _MigrationStatus.INDEXED.value

        assert row_0_dict['row_id'] == 1
        assert row_0_dict['col_id'] == 1
        assert row_0_dict['from_file_handle_id'] == file_handle_id_1
        assert row_1_dict['row_id'] == 1
        assert row_1_dict['col_id'] == 2
        assert row_1_dict['from_file_handle_id'] == file_handle_id_2
        assert row_2_dict['row_id'] == 2
        assert row_2_dict['col_id'] == 1
        assert row_2_dict['from_file_handle_id'] == file_handle_id_3

        # already in destination storage location
        assert row_3_dict['id'] == table_id
        assert row_3_dict['parent_id'] == parent_id
        assert row_3_dict['row_id'] == 2
        assert row_3_dict['col_id'] == 2
        assert row_3_dict['from_file_handle_id'] == file_handle_id_4
        assert row_3_dict['from_storage_location_id'] == to_storage_location_id
        assert row_3_dict['status'] == _MigrationStatus.ALREADY_MIGRATED.value

    @mock.patch.object(synapseutils.migrate_functions, '_index_entity')
    def test_index_container__files(self, mock_index_entity, conn):
        """Test indexing a project container, including files but not tables, and with one sub folder"""

        cursor = conn.cursor()
        _ensure_schema(cursor)

        project_id = 'syn123'
        parent_id = 'syn321'
        file_id = 'syn456'
        sub_folder_id = 'syn654'
        storage_location_id = '1234'
        file_version_strategy = 'new'
        include_table_files = False
        continue_on_error = True

        project = mock.MagicMock(synapseclient.Project)
        project.id = project_id
        project.get.return_value = PROJECT_ENTITY

        file_dict = {
            'id': file_id,
            'concreteType': FILE_ENTITY
        }

        sub_folder_dict = {
            'id': sub_folder_id,
            'concreteType': FOLDER_ENTITY,
        }

        syn = mock.MagicMock(synapseclient.Synapse)
        syn.getChildren.return_value = [
            file_dict,
            sub_folder_dict,
        ]

        _index_container(
            conn,
            cursor,
            syn,
            project,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error,
        )

        syn.getChildren.assert_called_once_with(project_id, includeTypes=['folder', 'file'])

        expected_calls = []
        for child in (file_dict, sub_folder_dict):
            expected_calls.append(
                mock.call(
                    conn,
                    cursor,
                    syn,
                    child,
                    project_id,
                    storage_location_id,
                    file_version_strategy,
                    include_table_files,
                    continue_on_error
                )
            )

        assert mock_index_entity.call_args_list == expected_calls

        row = cursor.execute(
            """
                select
                    id,
                    type,
                    parent_id,
                    status
                from migrations
            """
        ).fetchone()

        row_dict = _get_row_dict(cursor, row, True)
        assert row_dict['id'] == project_id
        assert row_dict['type'] == _MigrationType.PROJECT.value
        assert row_dict['parent_id'] == parent_id
        assert row_dict['status'] == _MigrationStatus.INDEXED.value

    @mock.patch.object(synapseutils.migrate_functions, '_index_entity')
    def test_index_container__tables(self, mock_index_entity, conn):
        """Test indexing a folder container, including tables but not files"""

        cursor = conn.cursor()
        _ensure_schema(cursor)

        folder_id = 'syn123'
        parent_id = 'syn321'
        table_id = 'syn456'
        storage_location_id = '1234'
        file_version_strategy = 'skip'
        include_table_files = True
        continue_on_error = False

        folder = mock.MagicMock(synapseclient.Folder)
        folder.id = folder_id
        folder.get.return_value = FOLDER_ENTITY

        table_dict = {
            'id': table_id,
            'concreteType': TABLE_ENTITY
        }

        syn = mock.MagicMock(synapseclient.Synapse)
        syn.getChildren.return_value = [
            table_dict,
        ]

        _index_container(
            conn,
            cursor,
            syn,
            folder,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error,
        )

        syn.getChildren.assert_called_once_with(folder_id, includeTypes=['folder', 'table'])

        expected_calls = [
            mock.call(
                conn,
                cursor,
                syn,
                table_dict,
                folder_id,
                storage_location_id,
                file_version_strategy,
                include_table_files,
                continue_on_error
            )
        ]

        assert mock_index_entity.call_args_list == expected_calls

        row = cursor.execute(
            """
                select
                    id,
                    type,
                    parent_id,
                    status
                from migrations
            """
        ).fetchone()

        row_dict = _get_row_dict(cursor, row, True)
        assert row_dict['id'] == folder_id
        assert row_dict['type'] == _MigrationType.FOLDER.value
        assert row_dict['parent_id'] == parent_id
        assert row_dict['status'] == _MigrationStatus.INDEXED.value

    @mock.patch.object(synapseutils.migrate_functions, '_index_file_entity')
    @mock.patch.object(synapseutils.migrate_functions, '_check_indexed')
    def test_index_entity__already_indexed(self, mock_check_indexed, mock_index_file_entity, conn):
        cursor = conn.cursor()
        syn = mock.MagicMock(synapseclient.Synapse)
        parent_id = 'syn123'
        storage_location_id = '1234'
        file_version_strategy = 'new'
        include_table_files = True
        continue_on_error = True

        mock_check_indexed.return_value = True

        entity_id = 'syn123'
        entity = {
            'id': entity_id,
            'concreteType': FILE_ENTITY
        }

        _index_entity(
            conn,
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error
        )

        mock_check_indexed.assert_called_once_with(cursor, entity_id)
        assert mock_index_file_entity.called is False

    @mock.patch.object(synapseutils.migrate_functions, '_index_file_entity')
    @mock.patch.object(synapseutils.migrate_functions, '_check_indexed')
    def test_index_entity__file(self, mock_check_indexed, mock_index_file_entity):
        """Verify behavior of _index_file_entity"""

        conn = mock.Mock()
        cursor = mock.Mock()
        conn.cursor.return_value = cursor

        syn = mock.MagicMock(synapseclient.Synapse)
        parent_id = 'syn123'
        storage_location_id = '1234'
        file_version_strategy = 'new'
        include_table_files = False
        continue_on_error = True

        mock_check_indexed.return_value = False

        entity_id = 'syn123'
        entity = {
            'id': entity_id,
            'concreteType': FILE_ENTITY
        }

        _index_entity(
            conn,
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error
        )

        mock_check_indexed.assert_called_once_with(cursor, entity_id)
        mock_index_file_entity.assert_called_once_with(
            cursor,
            syn,
            entity_id,
            parent_id,
            storage_location_id,
            file_version_strategy
        )

        conn.commit.assert_called_once_with()

    @mock.patch.object(synapseutils.migrate_functions, '_index_table_entity')
    @mock.patch.object(synapseutils.migrate_functions, '_check_indexed')
    def test_index_entity__table(self, mock_check_indexed, mock_index_table_entity):
        """Verify behavior of _index_table_entity"""

        conn = mock.Mock()
        cursor = mock.Mock()
        conn.cursor.return_value = cursor

        syn = mock.MagicMock(synapseclient.Synapse)
        parent_id = 'syn123'
        storage_location_id = '1234'
        file_version_strategy = 'skip'
        include_table_files = True
        continue_on_error = True

        mock_check_indexed.return_value = False

        entity_id = 'syn123'
        entity = {
            'id': entity_id,
            'concreteType': TABLE_ENTITY
        }

        _index_entity(
            conn,
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error
        )

        mock_check_indexed.assert_called_once_with(cursor, entity_id)
        mock_index_table_entity.assert_called_once_with(
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id
        )

        conn.commit.assert_called_once_with()

    @mock.patch.object(synapseutils.migrate_functions, '_index_container')
    @mock.patch.object(synapseutils.migrate_functions, '_check_indexed')
    def test_index_entity__container(self, mock_check_indexed, mock_index_container):
        """Verify behavior of _index_container"""

        conn = mock.Mock()
        cursor = mock.Mock()
        conn.cursor.return_value = cursor

        syn = mock.MagicMock(synapseclient.Synapse)
        parent_id = 'syn123'
        storage_location_id = '1234'
        file_version_strategy = 'latest'
        include_table_files = False
        continue_on_error = True

        mock_check_indexed.return_value = False

        entity_id = 'syn123'
        entity = {
            'id': entity_id,
            'concreteType': FOLDER_ENTITY
        }

        _index_entity(
            conn,
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error
        )

        mock_check_indexed.assert_called_once_with(cursor, entity_id)
        mock_index_container.assert_called_once_with(
            conn,
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error,
        )

        conn.commit.assert_called_once_with()

    @mock.patch.object(synapseutils.migrate_functions, '_index_file_entity')
    @mock.patch.object(synapseutils.migrate_functions, '_check_indexed')
    def test_index_entity__error__continue(self, mock_check_indexed, mock_index_file_entity, conn):
        """Verify that if continue_on_error is True that the no error is raised but it is recorded"""

        cursor = conn.cursor()
        _ensure_schema(cursor)

        syn = mock.MagicMock(synapseclient.Synapse)
        parent_id = 'syn123'
        storage_location_id = '1234'
        file_version_strategy = 'new'
        include_table_files = False
        continue_on_error = True

        mock_check_indexed.return_value = False

        entity_id = 'syn123'
        entity = {
            'id': entity_id,
            'concreteType': FILE_ENTITY
        }

        mock_index_file_entity.side_effect = ValueError('boom')

        _index_entity(
            conn,
            cursor,
            syn,
            entity,
            parent_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error
        )

        mock_check_indexed.assert_called_once_with(cursor, entity_id)
        mock_index_file_entity.assert_called_once_with(
            cursor,
            syn,
            entity_id,
            parent_id,
            storage_location_id,
            file_version_strategy
        )

        row = cursor.execute(
            """
                select
                    id,
                    type,
                    status,
                    exception
                from migrations
            """
        ).fetchone()

        row_dict = _get_row_dict(cursor, row, True)
        assert row_dict['id'] == entity_id
        assert row_dict['type'] == _MigrationType.FILE.value
        assert row_dict['status'] == _MigrationStatus.ERRORED.value
        assert 'boom' in row_dict['exception']

    @mock.patch.object(synapseutils.migrate_functions, '_index_file_entity')
    @mock.patch.object(synapseutils.migrate_functions, '_check_indexed')
    def test_index_entity__error__no_continue(self, mock_check_indexed, mock_index_file_entity, conn):
        """Verify that if continue_on_error is False that the error is raised"""

        cursor = conn.cursor()
        _ensure_schema(cursor)

        syn = mock.MagicMock(synapseclient.Synapse)
        parent_id = 'syn123'
        storage_location_id = '1234'
        file_version_strategy = 'new'
        include_table_files = False
        continue_on_error = False

        mock_check_indexed.return_value = False

        entity_id = 'syn123'
        entity = {
            'id': entity_id,
            'concreteType': FILE_ENTITY
        }

        mock_index_file_entity.side_effect = ValueError('boom')

        with pytest.raises(_IndexingError) as indexing_ex:
            _index_entity(
                conn,
                cursor,
                syn,
                entity,
                parent_id,
                storage_location_id,
                file_version_strategy,
                include_table_files,
                continue_on_error
            )

        assert entity_id == indexing_ex.value.entity_id
        assert indexing_ex.value.concrete_type == FILE_ENTITY
        assert 'boom' in str(indexing_ex.value.__cause__)

    def test_index_files_for_migration__invalid_file_version_strategy(self):
        """Verify error if invalid file_version_strategy"""

        syn = mock.MagicMock(synapseclient.Synapse)
        entity_id = 'syn123'
        storage_location_id = '1234'
        db_path = '/tmp/foo'
        file_version_strategy = 'wizzle'  # invalid
        include_table_files = False
        continue_on_error = True

        with pytest.raises(ValueError) as ex:
            index_files_for_migration(
                syn,
                entity_id,
                storage_location_id,
                db_path,
                file_version_strategy,
                include_table_files,
                continue_on_error
            )
        assert 'Invalid file_version_strategy' in str(ex.value)

    def test_index_files_for_migration__nothing_selected(self):
        """Verify error is raised if not migrating either files or table attached files"""

        syn = mock.MagicMock(synapseclient.Synapse)
        entity_id = 'syn123'
        storage_location_id = '1234'
        db_path = '/tmp/foo'
        file_version_strategy = 'skip'
        include_table_files = False
        continue_on_error = True

        with pytest.raises(ValueError) as ex:
            index_files_for_migration(
                syn,
                entity_id,
                storage_location_id,
                db_path,
                file_version_strategy,
                include_table_files,
                continue_on_error
            )
        assert 'Skipping both' in str(ex.value)

    @mock.patch.object(synapseutils.migrate_functions, '_verify_index_settings')
    @mock.patch.object(synapseutils.migrate_functions, '_index_entity')
    @mock.patch('sqlite3.connect')
    def test_index_files_for_migration(self, mock_sqlite_connect, mock_index_entity, mock_verify_index_settings):
        syn = mock.MagicMock(synapseclient.Synapse)
        entity_id = 'syn123'
        storage_location_id = '1234'
        db_path = '/tmp/foo'
        file_version_strategy = 'all'
        include_table_files = False
        continue_on_error = True

        entity = mock.MagicMock(synapseclient.File)
        syn.get.return_value = entity

        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_sqlite_connect.return_value. __enter__.return_value = mock_conn

        result = index_files_for_migration(
            syn,
            entity_id,
            storage_location_id,
            db_path,
            file_version_strategy,
            include_table_files,
            continue_on_error
        )

        mock_verify_index_settings.assert_called_once_with(
            mock_cursor,
            db_path,
            entity_id,
            storage_location_id,
            file_version_strategy,
            include_table_files
        )

        mock_index_entity.assert_called_once_with(
            mock_conn,
            mock_cursor,
            syn,
            entity,
            None,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error,
        )

        assert result._syn == syn
        assert result.db_path == db_path

    @mock.patch.object(synapseutils.migrate_functions, '_verify_index_settings')
    @mock.patch.object(synapseutils.migrate_functions, '_index_entity')
    @mock.patch('sqlite3.connect')
    def test_index_files_for_migration__indexing_error(
        self,
        mock_sqlite_connect,
        mock_index_entity,
        mock_verify_index_settings
    ):
        syn = mock.MagicMock(synapseclient.Synapse)
        entity_id = 'syn123'
        storage_location_id = '1234'
        db_path = '/tmp/foo'
        file_version_strategy = 'all'
        include_table_files = False
        continue_on_error = True

        entity = mock.MagicMock(synapseclient.File)
        syn.get.return_value = entity

        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_sqlite_connect.return_value. __enter__.return_value = mock_conn

        indexing_error = _IndexingError(entity_id, FILE_ENTITY)
        indexing_error.__cause__ = ValueError('boom')
        mock_index_entity.side_effect = indexing_error

        with pytest.raises(ValueError) as ex:
            index_files_for_migration(
                syn,
                entity_id,
                storage_location_id,
                db_path,
                file_version_strategy,
                include_table_files,
                continue_on_error
            )
        assert 'boom' in str(ex.value)

        mock_verify_index_settings.assert_called_once_with(
            mock_cursor,
            db_path,
            entity_id,
            storage_location_id,
            file_version_strategy,
            include_table_files
        )

        mock_index_entity.assert_called_once_with(
            mock_conn,
            mock_cursor,
            syn,
            entity,
            None,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error,
        )


class TestMigrate:
    """Test a project migration that involves multiple recursive entity migrations.
    All synapse calls are mocked but the local sqlite3 are not in these"""

    @pytest.fixture(scope='function')
    def conn(self):
        # temp file context manager doesn't work on windows so we manually remove in fixture
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile, \
                sqlite3.connect(tmpfile.name) as conn:
            yield conn

    @pytest.fixture(scope='function')
    def db_path(self):
        # temp file context manager doesn't work on windows so we manually remove in fixture
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            yield tmpfile.name

    def _migrate_test(self, db_path, syn, continue_on_error):

        # project structure:
        # project (syn1)
        #  folder1 (syn2)
        #    file1 (syn3)
        #    table1 (syn4)
        #  folder2 (syn5)
        #    file2 (syn6)
        #  file3 (syn7)

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
        file1_file_size = 1234
        file1._file_handle = {
            'id': file1.dataFileHandleId,
            'storageLocationId': old_storage_location,
            'contentSize': file1_file_size,
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
        file2_file_size = 9876
        file2._file_handle = {
            'id': file2.dataFileHandleId,
            'storageLocationId': old_storage_location,
            'contentSizes': file2_file_size,
        }
        entities.append(file2)

        file3 = synapseclient.File(id='syn7', parentId=project.id)
        file3.dataFileHandleId = 7
        file3.versionNumber = 1
        file3_file_size = 10 * utils.MB
        file3._file_handle = {
            'id': file3.dataFileHandleId,
            'storageLocationId': old_storage_location,
            'contentSize': file3_file_size,
        }
        entities.append(file3)

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
                        'storageLocationId': 1,
                        'contentSize': 400
                    }
                }

            raise ValueError("Unexpected file handle retrieval {}".format(fileHandleId))

        new_file_handle_mapping = {
            '3': 30,  # file1
            '4': 40,  # table1
            '6': 60,  # file2
            '7': 70,  # file3
        }

        def mock_syn_table_query_side_effect(query_string):
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

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            _ensure_schema(cursor)

            cursor.execute(
                """
                    insert into migration_settings (
                        root_id,
                        storage_location_id,
                        file_version_strategy,
                        include_table_files
                    ) values (?, ?, ?, ?)
                """,
                (
                    project.id,
                    new_storage_location_id,
                    'new',
                    1
                )
            )

            migration_values = [
                (
                    project.id,
                    _MigrationType.PROJECT.value,
                    None,
                    None,
                    None,
                    None,
                    _MigrationStatus.INDEXED.value,
                    None,
                    None,
                    None,
                ),
                (
                    folder1.id,
                    _MigrationType.FOLDER.value,
                    None,
                    None,
                    None,
                    project.id,
                    _MigrationStatus.INDEXED.value,
                    None,
                    None,
                    None,
                ),
                (
                    file1.id,
                    _MigrationType.FILE.value,
                    None,
                    None,
                    None,
                    folder1.id,
                    _MigrationStatus.INDEXED.value,
                    old_storage_location,
                    file1.dataFileHandleId,
                    file1_file_size,
                ),
                (
                    table1.id,
                    _MigrationType.TABLE_ATTACHED_FILE.value,
                    None,
                    0,
                    5,
                    folder1.id,
                    _MigrationStatus.INDEXED.value,
                    old_storage_location,
                    '4',
                    400,
                ),
                (
                    folder2.id,
                    _MigrationType.FOLDER.value,
                    None,
                    None,
                    None,
                    project.id,
                    _MigrationStatus.INDEXED.value,
                    None,
                    None,
                    None,
                ),
                (
                    file2.id,
                    _MigrationType.FILE.value,
                    None,
                    None,
                    None,
                    folder2.id,
                    _MigrationStatus.INDEXED.value,
                    old_storage_location,
                    file2.dataFileHandleId,
                    file2_file_size,
                ),
                (
                    file3.id,
                    _MigrationType.FILE.value,
                    None,
                    None,
                    None,
                    project.id,
                    _MigrationStatus.INDEXED.value,
                    old_storage_location,
                    file3.dataFileHandleId,
                    file3_file_size,
                ),
            ]

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
                        from_storage_location_id,
                        from_file_handle_id,
                        file_size
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                migration_values
            )

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(syn, 'store') as mock_syn_store, \
                mock.patch.object(syn, '_getFileHandleDownload') as mock_get_file_handle_download, \
                mock.patch.object(syn, 'restGET') as mock_syn_rest_get, \
                mock.patch.object(syn, 'create_snapshot_version') as mock_create_snapshot_version, \
                mock.patch.object(syn, 'tableQuery') as mock_syn_table_query, \
                mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy:

            mock_syn_get.side_effect = mock_syn_get_side_effect
            mock_syn_store.side_effect = mock_syn_store_side_effect
            mock_get_file_handle_download.side_effect = mock_get_file_handle_download_side_effect
            mock_syn_rest_get.side_effect = mock_rest_get_side_effect
            mock_syn_table_query.side_effect = mock_syn_table_query_side_effect
            mock_multipart_copy.side_effect = mock_multipart_copy_side_effect

            result = migrate_indexed_files(
                syn,
                db_path,
                create_table_snapshots=True,
                continue_on_error=continue_on_error,
                force=True
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

            counts_by_status = result.get_counts_by_status()
            assert counts_by_status['MIGRATED'] == 3
            assert counts_by_status['ERRORED'] == 1

    def test_continue_on_error__true(self, syn, db_path):
        """Test a migration of a project when an error is encountered while continuing on the error."""
        # we expect the migration to run to the finish, but a failed file entity will be marked with
        # a failed status and have an exception
        self._migrate_test(db_path, syn, True)

    def test_continue_on_error__false(self, syn, db_path):
        """Test a migration for a project when an error is encountered when aborting on errors"""
        # we expect the error to be surfaced
        with pytest.raises(ValueError) as ex:
            self._migrate_test(db_path, syn, False)
            assert 'boom' in str(ex)


@skipIf(
    synapseclient.core.config.single_threaded,
    "This test verifies behavior in a multi threaded environment and cannot run/is not relevant under a single thread"
)
def test_migrate__shared_file_handles(mocker, syn):
    """Verify if we migrate file entities that share the same file handle
    the file handle migration code is invoked the expected number of times.
    If two file entities share the same file handle then we have to go through
    the query cycle multiple times since the first time through the loop
    the migration for the second file is deferred so that the copied file handle
    can be reused by both files."""

    # 2 file entities sharing a file handle id
    project = synapseclient.Project(id='syn1')
    old_storage_location_id = 1234
    new_storage_location_id = 5678
    from_file_handle_id = 4321
    to_file_handle_id = 9876
    file_size = 1000000
    file_handle = {
        'id': from_file_handle_id,
        'storageLocationId': old_storage_location_id,
        'contentSize': file_size,
    }

    file1 = synapseclient.File(id='syn2', parentId=project.id)
    file1.dataFileHandleId = from_file_handle_id
    file1.versionNumber = 1
    file1._file_handle = file_handle

    file2 = synapseclient.File(id='syn3', parentId=project.id)
    file2.dataFileHandleId = from_file_handle_id
    file2.versionNumber = 3
    file2._file_handle = file_handle

    migration_values = [
        (
            project.id,
            _MigrationType.PROJECT.value,
            None,
            None,
            None,
            None,
            _MigrationStatus.INDEXED.value,
            None,
            None,
            None,
        ),
        (
            file1.id,
            _MigrationType.FILE.value,
            file1.versionNumber,
            None,
            None,
            project.id,
            _MigrationStatus.INDEXED.value,
            old_storage_location_id,
            from_file_handle_id,
            file_size,
        ),
        (
            file2.id,
            _MigrationType.FILE.value,
            file2.versionNumber,
            None,
            None,
            project.id,
            _MigrationStatus.INDEXED.value,
            old_storage_location_id,
            from_file_handle_id,
            file_size,
        ),
    ]

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile, \
            sqlite3.connect(tmpfile.name) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)

        cursor.execute(
            """
                insert into migration_settings (
                    root_id,
                    storage_location_id,
                    file_version_strategy,
                    include_table_files
                ) values (?, ?, ?, ?)
            """,
            (
                project.id,
                new_storage_location_id,
                'all',
                0,
            )
        )

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
                    from_storage_location_id,
                    from_file_handle_id,
                    file_size
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            migration_values
        )
        conn.commit()

    mock_migrate_file_version = mocker.patch.object(migrate_functions, '_migrate_file_version')

    wait_event = threading.Event()
    wait_futures = migrate_functions._wait_futures
    wait_mock = mocker.patch.object(migrate_functions, '_wait_futures')

    # a spy that records that wait_futures was called so we can ensure that
    # a "migration" won't complete until after the main thread is waiting once
    def wait_mock_side_effect(*args, **kwargs):
        wait_event.set()
        return wait_futures(*args, **kwargs)
    wait_mock.side_effect = wait_mock_side_effect

    def mock_migrate_file_version_side_effect(*args, **kwargs):
        # wait until the driving thread is complete before we simulate
        # the completion of the migration
        wait_event.wait()
        return to_file_handle_id

    mock_migrate_file_version.side_effect = mock_migrate_file_version_side_effect
    result = migrate_indexed_files(
        syn,
        tmpfile.name,
        force=True
    )

    assert mock_migrate_file_version.call_count == 2
    # verify that it took to cycles to complete this migration
    # since one of the migrations was deferred because of the shared file handle
    assert wait_mock.call_count == 2
    for migration in result.get_migrations():
        assert to_file_handle_id == migration['to_file_handle_id']


def test_create_new_file_version__copy_file_handle(mocker, syn):
    """Verify the behavior of create_new_file_version when we need to copy the file handle"""

    key = _MigrationKey('syn123', _MigrationType.FILE, 1, None, None)
    from_file_handle_id = 1234
    to_file_handle_id = 4321
    storage_location_id = 9876
    file_size = 1000 * utils.MB

    mock_file = mock.MagicMock(spec=synapseclient.File)

    mock_syn_get = mocker.patch.object(syn, 'get')
    mock_syn_store = mocker.patch.object(syn, 'store')
    mock_multipart_copy = mocker.patch.object(migrate_functions, 'multipart_copy')
    mock_get_part_size = mocker.patch.object(migrate_functions, '_get_part_size')
    mock_syn_get.return_value = mock_file
    mock_multipart_copy.return_value = to_file_handle_id
    mock_get_part_size.return_value = 5 * utils.MB

    # first test without passing an existing destination file handle.
    # the from file handle should be copied
    assert to_file_handle_id == _create_new_file_version(
        syn,
        key,
        from_file_handle_id,
        None,
        file_size,
        storage_location_id
    )

    mock_syn_get.assert_called_once_with(key.id, downloadFile=False)
    mock_syn_store.assert_called_once_with(mock_file)
    assert mock_file.dataFileHandleId == to_file_handle_id

    mock_multipart_copy.assert_called_once_with(
        syn,
        {
            'fileHandleId': from_file_handle_id,
            'associateObjectId': key.id,
            'associateObjectType': 'FileEntity',
        },
        storage_location_id=storage_location_id,
        part_size=mock_get_part_size.return_value,
    )


def test_create_new_file_version__use_existing_file_handle(mocker, syn):
    """Verify the behavior of create_new_file_version when we can use an existing file handle"""

    key = _MigrationKey('syn123', _MigrationType.FILE, 1, None, None)
    from_file_handle_id = 1234
    to_file_handle_id = 4321
    storage_location_id = 9876
    file_size = 123456

    mock_file = mock.MagicMock(spec=synapseclient.File)

    mock_syn_get = mocker.patch.object(syn, 'get')
    mock_syn_store = mocker.patch.object(syn, 'store')
    mock_multipart_copy = mocker.patch.object(migrate_functions, 'multipart_copy')
    mock_syn_get.return_value = mock_file

    assert to_file_handle_id == _create_new_file_version(
        syn,
        key,
        from_file_handle_id,
        to_file_handle_id,
        file_size,
        storage_location_id
    )

    mock_syn_get.assert_called_once_with(key.id, downloadFile=False)
    mock_syn_store.assert_called_once_with(mock_file)
    assert mock_multipart_copy.called is False


def test_migrate_file_version__copy_file_handle(mocker, syn):
    """Verify the behavior of migrate_file_version when we need to copy the file handle"""

    key = _MigrationKey('syn123', _MigrationType.FILE, 5, None, None)
    from_file_handle_id = 1234
    to_file_handle_id = 4321
    storage_location_id = 9876
    file_size = 6543

    expected_file_version_update_put_uri = f"/entity/{key.id}/version/{key.version}/filehandle"
    expected_file_handle_update_request_body = json.dumps({
        'oldFileHandleId': from_file_handle_id,
        'newFileHandleId': to_file_handle_id,
    })

    mock_syn_rest_put = mocker.patch.object(syn, 'restPUT')
    mock_multipart_copy = mocker.patch.object(migrate_functions, 'multipart_copy')
    mock_get_part_size = mocker.patch.object(migrate_functions, '_get_part_size')
    mock_get_part_size.return_value = 321

    mock_multipart_copy.return_value = to_file_handle_id

    # first test without passing an existing destination file handle.
    # the from file handle should be copied
    assert to_file_handle_id == _migrate_file_version(
        syn,
        key,
        from_file_handle_id,
        None,
        file_size,
        storage_location_id
    )

    mock_multipart_copy.assert_called_once_with(
        syn,
        {
            'fileHandleId': from_file_handle_id,
            'associateObjectId': key.id,
            'associateObjectType': 'FileEntity',
        },
        storage_location_id=storage_location_id,
        part_size=mock_get_part_size.return_value,
    )

    mock_syn_rest_put.assert_called_once_with(
        expected_file_version_update_put_uri,
        expected_file_handle_update_request_body
    )

    mock_syn_rest_put.reset_mock()
    mock_multipart_copy.reset_mock()

    # test with an existing file handle id
    assert to_file_handle_id == _migrate_file_version(
        syn,
        key,
        from_file_handle_id,
        to_file_handle_id,
        file_size,
        storage_location_id,
    )

    # should NOT have copied the file
    assert mock_multipart_copy.called is False

    # but we still should have updated the entity version with the new file handle id
    mock_syn_rest_put.assert_called_once_with(
        expected_file_version_update_put_uri,
        expected_file_handle_update_request_body
    )


def test_migrate_file_version__use_existing_file_handle(mocker, syn):
    """Verify the behavior of migrate_file_version when the file handle has already been copied"""

    key = _MigrationKey('syn123', _MigrationType.FILE, 5, None, None)
    from_file_handle_id = 1234
    to_file_handle_id = 4321
    storage_location_id = 9876
    file_size = 123456789

    expected_file_version_update_put_uri = f"/entity/{key.id}/version/{key.version}/filehandle"
    expected_file_handle_update_request_body = json.dumps({
        'oldFileHandleId': from_file_handle_id,
        'newFileHandleId': to_file_handle_id,
    })

    mock_syn_rest_put = mocker.patch.object(syn, 'restPUT')
    mock_multipart_copy = mocker.patch.object(migrate_functions, 'multipart_copy')

    # test with an existing file handle id
    assert to_file_handle_id == _migrate_file_version(
        syn,
        key,
        from_file_handle_id,
        to_file_handle_id,
        file_size,
        storage_location_id
    )

    # should NOT have copied the file
    assert mock_multipart_copy.called is False

    # but we still should have updated the entity version with the new file handle id
    mock_syn_rest_put.assert_called_once_with(
        expected_file_version_update_put_uri,
        expected_file_handle_update_request_body
    )


def test_migrate_table_attached_file__copy_file_handle(mocker, syn):
    """"Verify the behavior of _migrate_table_attached_file when we need to copy the file"""

    key = _MigrationKey('syn123', _MigrationType.FILE, None, 5, 6)
    from_file_handle_id = 1234
    to_file_handle_id = 4321
    storage_location_id = 9876
    expected_row_mapping = {str(key.col_id): to_file_handle_id}
    file_size = 987654

    mock_syn_store = mocker.patch.object(syn, 'store')
    mock_table = mocker.patch('synapseclient.table')
    mock_get_part_size = mocker.patch.object(migrate_functions, '_get_part_size')
    mock_row_set = mocker.patch('synapseclient.PartialRowset')
    mock_multipart_copy = mocker.patch.object(migrate_functions, 'multipart_copy')
    mock_get_part_size.return_value = 1024 * 1024

    mock_multipart_copy.return_value = to_file_handle_id

    # calling without an existing to file handle should result in a copy
    assert to_file_handle_id == _migrate_table_attached_file(
        syn,
        key,
        from_file_handle_id,
        None,
        file_size,
        storage_location_id
    )

    mock_multipart_copy.assert_called_once_with(
        syn,
        {
            'fileHandleId': from_file_handle_id,
            'associateObjectId': key.id,
            'associateObjectType': 'TableEntity',
        },
        storage_location_id=storage_location_id,
        part_size=mock_get_part_size.return_value,
    )

    mock_syn_store.assert_called_once_with(
        mock_row_set.return_value
    )
    mock_table.PartialRow.assert_called_once_with(expected_row_mapping, key.row_id)
    mock_row_set.assert_called_once_with(key.id, [mock_table.PartialRow.return_value])


def test_migrate_table_attached_file__use_existing_file_handle(mocker, syn):
    """"Verify the behavior of _migrate_table_attached_file when the file handle has already been copied"""

    key = _MigrationKey('syn123', _MigrationType.FILE, None, 5, 6)
    from_file_handle_id = 1234
    to_file_handle_id = 4321
    storage_location_id = 9876
    file_size = 765432
    expected_row_mapping = {str(key.col_id): to_file_handle_id}

    mock_syn_store = mocker.patch.object(syn, 'store')
    mock_table = mocker.patch('synapseclient.table')
    mock_row_set = mocker.patch('synapseclient.PartialRowset')
    mock_multipart_copy = mocker.patch.object(migrate_functions, 'multipart_copy')

    # calling with an existing to file handle should not result in a copy
    assert to_file_handle_id == _migrate_table_attached_file(
        syn,
        key,
        from_file_handle_id,
        to_file_handle_id,
        file_size,
        storage_location_id,
    )

    mock_syn_store.assert_called_once_with(
        mock_row_set.return_value
    )
    mock_table.PartialRow.assert_called_once_with(expected_row_mapping, key.row_id)
    mock_row_set.assert_called_once_with(key.id, [mock_table.PartialRow.return_value])

    assert mock_multipart_copy.called is False


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
            'include_table_files'
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
            'file_size',
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

    with tempfile.NamedTemporaryFile(delete=False) as db_file, \
            sqlite3.connect(db_file.name) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        _verify_schema(cursor)

        # invoking a second time should be idempotent
        _ensure_schema(cursor)
        _verify_schema(cursor)


def test_verify_storage_location_ownership():
    storage_location_id = '1234'
    syn = mock.MagicMock(synapseclient.Synapse)

    # no error raised
    _verify_storage_location_ownership(syn, storage_location_id)

    syn.restGET.side_effect = SynapseHTTPError('boom')
    with pytest.raises(ValueError) as ex:
        _verify_storage_location_ownership(syn, storage_location_id)
    assert 'Error verifying' in str(ex.value)
    assert syn.restGET.called_with("/storageLocation/{}".format(storage_location_id))


def test__verify_index_settings__retrieve_index_settings():
    """Verify the behavior saving index settings and re-retreiving them."""

    with tempfile.NamedTemporaryFile(delete=False) as db_file, \
            sqlite3.connect(db_file.name) as conn:
        db_path = db_file.name
        cursor = conn.cursor()
        _ensure_schema(cursor)

        root_id = 'syn123'
        storage_location_id = '12345'
        file_version_strategy = 'latest'
        include_table_files = False

        _verify_index_settings(
            cursor,
            db_path,
            root_id,
            storage_location_id,
            file_version_strategy,
            include_table_files
        )

        settings = _retrieve_index_settings(cursor)
        assert settings['root_id'] == root_id
        assert settings['storage_location_id'] == storage_location_id
        assert settings['file_version_strategy'] == file_version_strategy
        assert settings['include_table_files'] == include_table_files

        # same settings, no error
        _verify_index_settings(
            cursor,
            db_path,
            root_id,
            storage_location_id,
            file_version_strategy,
            include_table_files
        )

        # changed root_id
        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                'changed',
                storage_location_id,
                file_version_strategy,
                include_table_files
            )
        assert 'changed' in str(ex.value)

        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                root_id,
                'changed',
                file_version_strategy,
                include_table_files
            )
        assert 'changed' in str(ex.value)

        with pytest.raises(ValueError) as ex:
            _verify_index_settings(
                cursor,
                db_path,
                root_id,
                storage_location_id,
                'changed',
                include_table_files
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


class TestConfirmMigration:

    def test_force(self):
        """Verify forcing always confirms"""
        cursor = mock.Mock()
        force = True
        storage_location_id = '1234'
        assert _confirm_migration(cursor, force, storage_location_id) is True

    def test_no_items(self):
        """Verify no migration if no items to migrate."""
        cursor = mock.Mock()
        cursor.execute.return_value.fetchone.return_value = [0]

        force = False
        storage_location_id = '1234'

        assert _confirm_migration(cursor, force, storage_location_id) is False

    @mock.patch('sys.stdout.isatty', mock.Mock(return_value=True))
    @mock.patch('builtins.input', lambda *args: 'y')
    def test_interactive_shell__y(self):
        """Verify confirming via interactive shell"""
        cursor = mock.Mock()
        cursor.execute.return_value.fetchone.return_value = [1]
        force = False
        storage_location_id = '1234'

        assert _confirm_migration(cursor, force, storage_location_id) is True

    @mock.patch('sys.stdout.isatty', mock.Mock(return_value=True))
    @mock.patch('builtins.input', lambda *args: 'n')
    def test_interactive_shell__n(self):
        """Verify aborting via interactive shell"""
        cursor = mock.Mock()
        cursor.execute.return_value.fetchone.return_value = [1]
        force = False
        storage_location_id = '1234'

        assert _confirm_migration(cursor, force, storage_location_id) is False

    @mock.patch('sys.stdout.isatty', mock.Mock(return_value=False))
    def test_non_interactive_shell(self):
        """Verify no migration if not forcing from a non-interactive shell"""
        cursor = mock.Mock()
        cursor.execute.return_value.fetchone.return_value = [1]
        force = False
        storage_location_id = '1234'

        assert _confirm_migration(cursor, force, storage_location_id) is False
