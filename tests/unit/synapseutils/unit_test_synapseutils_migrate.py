import json
import pytest
from unittest import mock

import synapseclient
import synapseclient.core.upload
import synapseutils
import synapseutils.migrate_functions


@mock.patch('importlib.import_module')
def test_import_failure(mock_import_module, syn):
    """Verify behavior when sqlite3 module is not available. Should fail when function
    is called only"""
    # successful importing is implicitly tested everywhere else
    mock_import_module.side_effect = ImportError('boom')

    with pytest.raises(ImportError):
        synapseutils.migrate(syn, mock.Mock(), '1234')


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

    def test_migrate_file__specific_version(self, syn):
        """Verify that migrate_file invokes the proper internal functionality when a specific version is copied"""

        entity = mock.MagicMock(spec=synapseclient.File)
        destination_storage_location = '1234'

        returned_version = 5
        old_file_handle_id = '123'
        new_file_handle_id = '456'

        with mock.patch.object(syn, 'get') as syn_get, \
                mock.patch.object(synapseutils.migrate_functions, '_migrate_file_version') as mock_migrate_version:

            mock_migrate_version.return_value = (returned_version, old_file_handle_id, new_file_handle_id)
            syn_get.return_value = entity

            for version in (returned_version, None):
                mapping = synapseutils.migrate_file(syn, entity, destination_storage_location, version=version)

                expected_mapping = {returned_version: (old_file_handle_id, new_file_handle_id)}
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

    def test_continue_on_error__True(self):
        pass

    def test_continue_on_error__False(self):
        pass
