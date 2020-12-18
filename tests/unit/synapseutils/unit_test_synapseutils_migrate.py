import pytest
import sqlite3
import tempfile
from unittest import mock

import synapseclient
import synapseclient.core.upload
from synapseclient.core import utils
import synapseutils
from synapseutils.migrate_functions import _ensure_schema, _MigrationStatus, _MigrationType


class TestMigrationResult:

    @pytest.fixture(scope="class", autouse=True)
    def db_path(self):
        values = [
            ('syn1', _MigrationType.PROJECT.value, None, None, None, None, _MigrationStatus.INDEXED.value, None, None, None),  # noqa
            ('syn2', _MigrationType.FOLDER.value, None, None, None, 'syn1', _MigrationStatus.INDEXED.value, None, None, None),  # noqa
            ('syn3', _MigrationType.FILE.value, 5, None, None, 'syn2', _MigrationStatus.MIGRATED.value, None, 3, 30),  # noqa
            ('syn4', _MigrationType.TABLE_ATTACHED_FILE.value, 5, 1, 2, 'syn2', _MigrationStatus.MIGRATED.value, None, 4, 40),  # noqa
            ('syn5', _MigrationType.TABLE_ATTACHED_FILE.value, 5, 1, 3, 'syn2', _MigrationStatus.ERRORED.value, 'boom', None, None),  # noqa
        ]

        with tempfile.NamedTemporaryFile() as db_file, \
                sqlite3.connect(db_file.name) as conn:
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
                        from_file_handle_id,
                        to_file_handle_id
                    ) values (?, ? ,? ,?, ?, ?, ?, ?, ?, ?)
                """,
                values
            )
            conn.commit()

            yield db_file.name

    def test_as_csv(self, db_path):
        syn = mock.MagicMock(synapseclient.Synapse)
        result = synapseutils.migrate_functions.MigrationResult(syn, db_path)

        with tempfile.NamedTemporaryFile() as csv_path, \
                mock.patch.object(syn, 'restGET') as mock_rest_get:

            mock_rest_get.side_effect = [
                {'name': 'col_2'},
                {'name': 'col_3'},
            ]

            result.as_csv(csv_path.name)

            with open(csv_path.name, 'r') as csv_read:
                csv_contents = csv_read.read()

            expected_csv = """id,type,version,row_id,col_name,from_file_handle_id,to_file_handle_id,status,exception
syn3,file,5,,,3,30,MIGRATED,
syn4,table,5,1,col_2,4,40,MIGRATED,
syn5,table,5,1,col_3,,,ERRORED,boom
"""
        assert csv_contents == expected_csv

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


class TestMigrate:
    """Test a project migration that involves multiple recursive entity migrations.
    All synapse calls are mocked but the local sqlite3 are not in these"""

    def _migrate_test(self, syn, continue_on_error):
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
        file1._file_handle = {'storageLocationId': old_storage_location}
        entities.append(file1)

        table1 = synapseclient.Schema(id='syn4', parentId=folder1.id)
        entities.append(table1)

        folder2 = synapseclient.Folder(id='syn5', parentId=project.id)
        folder2.id = 'syn5'
        entities.append(folder2)

        file2 = synapseclient.File(id='syn6', parentId=folder2.id)
        file2.dataFileHandleId = 6
        file2.versionNumber = 1
        file2._file_handle = {'storageLocationId': old_storage_location}
        entities.append(file2)

        file3 = synapseclient.File(id='syn7', parentId=project.id)
        file3.dataFileHandleId = 7
        file3.versionNumber = 1
        file3._file_handle = {'storageLocationId': old_storage_location}
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

            else:
                raise ValueError('Unexpected restGET call {}'.format(uri))

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(syn, 'store') as mock_syn_store, \
                mock.patch.object(syn, 'getChildren') as mock_syn_get_children, \
                mock.patch.object(syn, 'restGET') as mock_syn_rest_get, \
                mock.patch.object(syn, 'create_snapshot_version'), \
                mock.patch.object(syn, 'tableQuery') as mock_syn_table_query, \
                mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy, \
                mock.patch.object(synapseclient.PartialRowset, 'from_mapping'):

            mock_syn_get.side_effect = mock_syn_get_side_effect
            mock_syn_store.side_effect = mock_syn_store_side_effect
            mock_syn_get_children.side_effect = mock_syn_get_children_side_effect
            mock_syn_rest_get.side_effect = mock_rest_get_side_effect
            mock_syn_table_query.side_effect = mock_syn_table_query_side_effect
            mock_multipart_copy.side_effect = mock_multipart_copy_side_effect

            # can't seem to use a normal temp file context manager here on windows.
            # https://stackoverflow.com/a/55081210
            db_path = tempfile.NamedTemporaryFile(delete=False).name
            result = synapseutils.migrate(
                syn,
                project,
                new_storage_location_id,
                db_path,
                file_version_strategy='new',
                create_table_snapshot=True,
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
            assert 'syn7' in migrated
            assert migrated['syn3']['from_file_handle_id'] == 3
            assert migrated['syn3']['to_file_handle_id'] == 30
            assert migrated['syn7']['from_file_handle_id'] == 7
            assert migrated['syn7']['to_file_handle_id'] == 70
            assert 'syn6' in errored
            assert 'boom' in errored['syn6']['exception']

    def test_continue_on_error__true(self, syn):
        """Test a migration of a project when an error is encountered while continuing on the error."""
        # we expect the migration to run to the finish, but a failed file entity will be marked with
        # a failed status and have an exception
        self._migrate_test(syn, True)

    def test_continue_on_error__false(self, syn):
        """Test a migration for a project when an error is encountered when aborting on errors"""
        # we expect the error to be surfaced
        with pytest.raises(ValueError) as ex:
            self._migrate_test(syn, False)
            assert 'boom' in str(ex)
