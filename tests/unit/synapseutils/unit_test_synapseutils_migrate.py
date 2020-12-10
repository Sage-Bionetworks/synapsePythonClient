import json
import pytest
import tempfile
from unittest import mock

import synapseclient
import synapseclient.core.upload
from synapseclient.core import utils
import synapseutils
import synapseutils.migrate_functions


class TestMigrateFile:

    def test_migrate_file__new(self, syn):
        """Verify that migrate_file creates a new version when version = 'new'"""

        entity = mock.MagicMock(spec=synapseclient.File)
        destination_storage_location = '1234'

        version = 5
        old_file_handle_id = '123'
        new_file_handle_id = '456'

        with mock.patch.object(syn, 'get') as syn_get, \
                mock.patch.object(synapseutils.migrate_functions, '_create_new_file_version') as mock_create_new:

            mock_create_new.return_value = (version, old_file_handle_id, new_file_handle_id)
            syn_get.return_value = entity

            mapping = synapseutils.migrate_file(syn, entity, destination_storage_location, version='new')

        expected_mapping = {version: (old_file_handle_id, new_file_handle_id)}
        assert mapping == expected_mapping

        mock_create_new.assert_called_once_with(syn, entity, destination_storage_location)

    def test_migrate_file__new__already_migrated(self, syn):
        """Verify that migrate_file creates a new version when version = 'new'"""

        entity = mock.MagicMock(spec=synapseclient.File)
        destination_storage_location = '1234'

        with mock.patch.object(syn, 'get') as syn_get, \
                mock.patch.object(synapseutils.migrate_functions, '_create_new_file_version') as mock_create_new:

            mock_create_new.side_effect = synapseutils.migrate_functions._AlreadyMigratedException()
            syn_get.return_value = entity

            mapping = synapseutils.migrate_file(syn, entity, destination_storage_location, version='new')

        assert mapping == {}
        mock_create_new.assert_called_once_with(syn, entity, destination_storage_location)

    def test_migrate_file__all(self, syn):
        """Verify that migrate_file invokes the proper internal functionality when all versions are copied"""

        entity = mock.MagicMock(spec=synapseclient.File)
        destination_storage_location = '1234'

        all_version_mapping = {
            4: ('123', '456'),
            5: ('012', '345'),
        }

        with mock.patch.object(syn, 'get') as syn_get, \
                mock.patch.object(synapseutils.migrate_functions, '_migrate_all_file_versions') as mock_all_versions:

            mock_all_versions.return_value = all_version_mapping
            syn_get.return_value = entity

            mapping = synapseutils.migrate_file(syn, entity, destination_storage_location, version='all')

        assert mapping == all_version_mapping

        mock_all_versions.assert_called_once_with(syn, entity, destination_storage_location)

    def test_migrate_file__latest(self, syn):
        """Verify that migrate_file invokes the proper internal functionality when the latest version is called"""

        entity = mock.MagicMock(spec=synapseclient.File)
        destination_storage_location = '1234'
        returned_version = 7
        old_file_handle_id = '123'
        new_file_handle_id = '456'

        with mock.patch.object(syn, 'get') as syn_get, \
                mock.patch.object(synapseutils.migrate_functions, '_migrate_file_version') as mock_migrate_version:

            mock_migrate_version.return_value = (7, old_file_handle_id, new_file_handle_id)
            syn_get.return_value = entity

            expected_mapping = {returned_version: (old_file_handle_id, new_file_handle_id)}
            mapping = synapseutils.migrate_file(syn, entity, destination_storage_location, version='latest')

        assert mapping == expected_mapping

        # latest becomes None when passed to internal handler which uses syn.get style None = latest
        mock_migrate_version.assert_called_once_with(syn, entity, destination_storage_location, None)

    def test_migrate_file__specific_version(self, syn):
        """Verify that migrate_file invokes the proper internal functionality when a specific version is copied"""

        entity = mock.MagicMock(spec=synapseclient.File)
        destination_storage_location = '1234'

        version = 5
        old_file_handle_id = '123'
        new_file_handle_id = '456'

        with mock.patch.object(syn, 'get') as syn_get, \
                mock.patch.object(synapseutils.migrate_functions, '_migrate_file_version') as mock_migrate_version:

            mock_migrate_version.return_value = (version, old_file_handle_id, new_file_handle_id)
            syn_get.return_value = entity

            mapping = synapseutils.migrate_file(syn, entity, destination_storage_location, version=version)

            expected_mapping = {version: (old_file_handle_id, new_file_handle_id)}
            assert mapping == expected_mapping

            mock_migrate_version.assert_called_once_with(
                syn,
                entity,
                destination_storage_location,
                version
            )

            mock_migrate_version.reset_mock()

    def test_create_new_file_version(self, syn):
        """Verify _create_new_file_version copies the file handle and creates a new entity version"""

        old_file_handle_id = '1234'
        old_file_handle = {
            'id': old_file_handle_id,
            'fileName': 'foo.txt',
            'storageLocationId': '5678'
        }
        new_file_handle_id = '5678'
        new_storage_location = '9876'

        entity = mock.MagicMock(spec=synapseclient.File)
        entity.dataFileHandleId = old_file_handle_id
        entity._file_handle = old_file_handle
        entity.id = 'syn123'

        store_entity = mock.MagicMock(spec=synapseclient.File)
        store_entity.versionNumber = 5

        with mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy, \
                mock.patch.object(syn, 'store') as mock_store:

            mock_multipart_copy.return_value = new_file_handle_id
            mock_store.return_value = store_entity

            result = synapseutils.migrate_functions._create_new_file_version(syn, entity, new_storage_location)
            assert result == (store_entity.versionNumber, old_file_handle_id, new_file_handle_id)

    def test_create_new_file_version__already_migrated(self, syn):
        """Verify an exception is raised internally if the entity is already at the desired storage location"""

        storage_location_id = '01234'
        old_file_handle_id = '1234'
        old_file_handle = {
            'id': old_file_handle_id,
            'fileName': 'foo.txt',
            'storageLocationId': storage_location_id
        }

        entity = mock.MagicMock(spec=synapseclient.File)
        entity.dataFileHandleId = old_file_handle_id
        entity._file_handle = old_file_handle
        entity.id = 'syn123'

        with pytest.raises(synapseutils.migrate_functions._AlreadyMigratedException):
            synapseutils.migrate_functions._create_new_file_version(syn, entity, storage_location_id)

    def test_migrate_all_file_versions(self, syn):
        """Verify behavior of _migrate_all_file_versions"""

        versions = [
            {'versionNumber': 1},
            {'versionNumber': 2},
            {'versionNumber': 3},
        ]

        storage_location_id = '01234'

        entity = mock.MagicMock(spec=synapseclient.File)
        entity.id = 'syn123'

        old_file_handle_id_1 = '1234'
        new_file_handle_id_1 = '5678'

        old_file_handle_id_3 = '0987'
        new_file_handle_id_3 = '6543'

        with mock.patch.object(syn, '_GET_paginated') as mock_get_paginated, \
                mock.patch.object(synapseutils.migrate_functions, '_migrate_file_version') as mock_migrate_version:

            # a version already in the destination location should be ignored from the mapping
            mock_get_paginated.return_value = versions
            mock_migrate_version.side_effect = [
                (1, old_file_handle_id_1, new_file_handle_id_1),
                synapseutils.migrate_functions._AlreadyMigratedException(),
                (3, old_file_handle_id_3, new_file_handle_id_3)
            ]

            mapping = synapseutils.migrate_functions._migrate_all_file_versions(syn, entity, storage_location_id)

            expected_mapping = {
                1: (old_file_handle_id_1, new_file_handle_id_1),
                3: (old_file_handle_id_3, new_file_handle_id_3),
            }
            assert mapping == expected_mapping

    def test_migrate_file_version(self, syn):
        """Test behavior when migrating a specific file version"""

        old_storage_location_id = '01234'
        old_file_handle_id = '1234'
        old_file_handle = {
            'id': old_file_handle_id,
            'fileName': 'foo.txt',
            'storageLocationId': old_storage_location_id
        }

        entity = mock.MagicMock(spec=synapseclient.File)
        entity.dataFileHandleId = old_file_handle_id
        entity._file_handle = old_file_handle
        entity.versionNumber = 5
        entity.id = 'syn123'

        new_file_handle_id = '9876'
        new_storage_location_id = '6543'

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy, \
                mock.patch.object(syn, 'restPUT') as mock_rest_put:

            mock_syn_get.return_value = entity
            mock_multipart_copy.return_value = new_file_handle_id

            result = synapseutils.migrate_functions._migrate_file_version(
                syn,
                entity,
                new_storage_location_id,
                entity.versionNumber
            )

            expected_result = entity.versionNumber, old_file_handle_id, new_file_handle_id
            assert result == expected_result

            mock_syn_get.assert_called_once_with(entity, downloadFile=False, version=entity.versionNumber)

            mock_rest_put.assert_called_once_with(
                "/entity/{id}/version/{versionNumber}/filehandle".format(
                    id=entity.id,
                    versionNumber=entity.versionNumber
                ),
                json.dumps(
                    {
                        'oldFileHandleId': old_file_handle_id,
                        'newFileHandleId': new_file_handle_id,
                    }
                )
            )

    def test_migrate_file_version__already_migrated(self, syn):
        """Verify _migrate_file_version raises an internal exception if the entity is already stored
        in the desired location"""

        old_storage_location_id = '01234'
        old_file_handle_id = '1234'
        old_file_handle = {
            'id': old_file_handle_id,
            'fileName': 'foo.txt',
            'storageLocationId': old_storage_location_id
        }

        entity = mock.MagicMock(spec=synapseclient.File)
        entity.dataFileHandleId = old_file_handle_id
        entity._file_handle = old_file_handle
        entity.versionNumber = 5
        entity.id = 'syn123'

        with mock.patch.object(syn, 'get') as mock_syn_get:
            mock_syn_get.return_value = entity

            with pytest.raises(synapseutils.migrate_functions._AlreadyMigratedException):
                synapseutils.migrate_functions._migrate_file_version(syn, entity, old_storage_location_id, None)


class TestMigrateTable:

    def test_non_table(self, syn):
        file = mock.MagicMock(spec=synapseclient.File)

        with pytest.raises(ValueError) as ex, \
                mock.patch.object(syn, 'get', return_value=file):
            synapseutils.migrate_table(syn, file, '1234')
            assert 'not a Table schema' in str(ex)

    def test_migrate_table__no_file_handle_cols(self, syn):
        """Verify that a migrating a table with no file columns results in no migration"""
        table = mock.MagicMock(spec=synapseclient.Schema)

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(syn, 'restGET') as mock_rest_get:
            mock_syn_get.return_value = table
            mock_rest_get.return_value = {
                'results': [
                    {'columnType': 'STRING'},
                    {'columnType': 'INTEGER'}
                ]
            }

            mapping = synapseutils.migrate_table(syn, table, '1234')
            assert mapping == {}

    def test_migrate_table(self, syn):
        storage_location_id = '12346789'

        table = mock.MagicMock(spec=synapseclient.Schema)
        table.id = 'syn1234'

        old_file_handle_id_1 = '0987'
        old_file_handle_id_2 = '7654'
        old_file_handle_id_3 = '8765'
        old_file_handle_id_4 = '6543'
        old_file_handle_id_5 = '5432'
        old_file_handle_id_6 = '5432'

        new_file_handle_id_1 = '1234'
        new_file_handle_id_2 = '2345'
        new_file_handle_id_3 = '3456'
        new_file_handle_id_4 = '4567'
        new_file_handle_id_5 = '5678'
        new_file_handle_id_6 = '5678'

        mock_results = [
            [1, None, old_file_handle_id_1, old_file_handle_id_2],
            [2, None, old_file_handle_id_3, old_file_handle_id_4],
            [3, None, old_file_handle_id_5, old_file_handle_id_6],
        ]

        with mock.patch.object(syn, 'get') as mock_syn_get, \
                mock.patch.object(syn, 'restGET') as mock_rest_get, \
                mock.patch.object(syn, 'tableQuery') as mock_table_query, \
                mock.patch.object(syn, 'create_snapshot_version') as mock_create_snapshot_version, \
                mock.patch.object(synapseutils.migrate_functions, 'multipart_copy') as mock_multipart_copy, \
                mock.patch.object(syn, 'store') as mock_syn_store, \
                mock.patch.object(synapseclient.PartialRowset, 'from_mapping') as mock_from_mapping, \
                mock.patch.object(synapseutils.migrate_functions, '_get_batch_size', return_value=2):

            mock_syn_get.return_value = table
            mock_rest_get.return_value = {
                'results': [
                    {'columnType': 'INTEGER'},
                    {
                        'name': 'file1',
                        'columnType': 'FILEHANDLEID'
                    },
                    {
                        'name': 'file2',
                        'columnType': 'FILEHANDLEID'
                    }
                ]
            }

            mock_table_query.return_value = mock_results
            mock_multipart_copy.side_effect = [
                new_file_handle_id_1,
                new_file_handle_id_2,
                new_file_handle_id_3,
                new_file_handle_id_4,
                new_file_handle_id_5,
                new_file_handle_id_6,
            ]

            mapping = synapseutils.migrate_table(syn, table, storage_location_id, create_snapshot=True)
            expected_mapping = {
                1: {
                    'file1': (old_file_handle_id_1, new_file_handle_id_1),
                    'file2': (old_file_handle_id_2, new_file_handle_id_2),
                },
                2: {
                    'file1': (old_file_handle_id_3, new_file_handle_id_3),
                    'file2': (old_file_handle_id_4, new_file_handle_id_4),
                },
                3: {
                    'file1': (old_file_handle_id_5, new_file_handle_id_5),
                    'file2': (old_file_handle_id_6, new_file_handle_id_6),
                }
            }

            assert mapping == expected_mapping
            mock_create_snapshot_version.assert_called_once_with(table)
            assert mock_syn_store.call_count == 2
            assert mock_from_mapping.call_count == 2


class TestMigrate:
    """Test a project migration that involves multiple recursive entity migrations.
    All synapse calls are mocked but the local sqlite3 care not in these dates."""

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
            # we simulate some failure migrating syn6,
            # in this case just a failure to look it up
            if entity_id in ('syn6', 'syn8'):
                raise ValueError('boom')
            return get_entities[utils.id_of(entity)]

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
            3: 30,  # file1
            4: 40,  # table1
            6: 60,  # file2
            7: 70,  # file3
        }

        def mock_syn_table_query_side_effect(query_string):
            if not query_string == "select filecol from {}".format(table1.id):
                pytest.fail("Unexpected table query")

            return [['1', '1', 4]]

        def mock_multipart_copy_side_effect(syn, source_file_handle_association, *args, **kwargs):
            return new_file_handle_mapping[source_file_handle_association['fileHandleId']]

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
            mock_syn_rest_get.return_value = {
                'results': [
                    {'columnType': 'STRING'},
                    {
                        'columnType': 'FILEHANDLEID',
                        'name': 'filecol',
                    }
                ]
            }
            mock_syn_table_query.side_effect = mock_syn_table_query_side_effect
            mock_multipart_copy.side_effect = mock_multipart_copy_side_effect

            # can't seem to use a normal temp file context manager here on windows.
            # https://stackoverflow.com/a/55081210
            db_path = tempfile.NamedTemporaryFile(delete=False).name
            result = synapseutils.migrate(
                syn,
                project,
                new_storage_location_id,
                db_path=db_path,
                continue_on_error=continue_on_error
            )

            expected_file_versions_migrated = [
                ('syn3', 1, '3', '30'),
                ('syn7', 1, '7', '70'),
            ]
            file_versions_migrated = [m for m in result.get_file_versions_migrated()]
            assert file_versions_migrated == expected_file_versions_migrated

            expected_table_files_migrated = [
                ('syn4', 1, 'filecol', '4', '40')
            ]
            table_files_migrated = [m for m in result.get_table_files_migrated()]
            assert table_files_migrated == expected_table_files_migrated

            file_errors = [e for e in result.get_file_migration_errors()]
            assert len(file_errors) == 1
            assert file_errors[0][0] == 'syn6'
            assert 'boom' in file_errors[0][1]

            table_errors = [e for e in result.get_table_migration_errors()]
            assert len(table_errors) == 1
            assert table_errors[0][0] == 'syn8'
            assert 'boom' in table_errors[0][1]

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
