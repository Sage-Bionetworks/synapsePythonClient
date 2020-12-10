import json
import pytest
import sqlite3
import tempfile
import uuid

import synapseclient
from synapseclient.core.constants import concrete_types
import synapseclient.core.utils as utils
import synapseutils


@pytest.fixture(scope='module')
def storage_location_id(syn, project):
    storage_location_setting = syn.restPOST('/storageLocation', json.dumps({
        'concreteType': concrete_types.SYNAPSE_S3_STORAGE_LOCATION_SETTING
    }))

    storage_location_id = storage_location_setting['storageLocationId']
    yield storage_location_id


def _create_temp_file():
    randomizer = uuid.uuid4()
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
        temp.write("{} {}".format(__name__, randomizer))
        return temp.name


def _migrate_file_version_test_helper(request, syn, project, schedule_for_cleanup):
    entity_name = request.node.name
    temp_path_1 = _create_temp_file()
    schedule_for_cleanup(temp_path_1)
    file = synapseclient.File(name=entity_name, path=temp_path_1, parent=project)
    v1 = syn.store(file)

    # create another revision
    temp_path_2 = _create_temp_file()
    schedule_for_cleanup(temp_path_2)
    file = synapseclient.File(name=entity_name, path=temp_path_2, parent=project)
    v2 = syn.store(file)

    return v1, v2


def _assert_storage_location(file_entities, storage_location_id):
    for entity in file_entities:
        assert entity._file_handle['storageLocationId'] == storage_location_id


def test_migrate_project(request, syn, schedule_for_cleanup, storage_location_id):
    test_name = request.node.name
    project_name = "{}-{}".format(test_name, uuid.uuid4())
    project = synapseclient.Project(name=project_name)
    project_entity = syn.store(project)

    file_0_path = _create_temp_file()
    schedule_for_cleanup(file_0_path)
    file_0_name = "{}-{}".format(test_name, 1)
    file_0 = synapseclient.File(name=file_0_name, path=file_0_path, parent=project_entity)
    file_0_entity = syn.store(file_0)
    default_storage_location = file_0_entity._file_handle['storageLocationId']

    folder_1_name = "{}-{}-{}".format(test_name, 1, uuid.uuid4())
    folder_1 = synapseclient.Folder(parent=project_entity, name=folder_1_name)
    folder_1_entity = syn.store(folder_1)

    file_1_path = _create_temp_file()
    schedule_for_cleanup(file_1_path)
    file_1_name = "{}-{}".format(test_name, 1)
    file_1 = synapseclient.File(name=file_1_name, path=file_1_path, parent=folder_1_entity)
    file_1_entity = syn.store(file_1)

    file_2_path = _create_temp_file()
    schedule_for_cleanup(file_2_path)
    file_2_name = "{}-{}".format(test_name, 2)
    file_2 = synapseclient.File(name=file_2_name, path=file_2_path, parent=folder_1_entity)
    file_2_entity = syn.store(file_2)

    table_1_cols = [
        synapseclient.Column(name='num', columnType='INTEGER'),
        synapseclient.Column(name='file', columnType='FILEHANDLEID'),
    ]
    table_1 = syn.store(
        synapseclient.Schema(
            name=test_name, columns=table_1_cols, parent=folder_1_entity
        )
    )
    table_1_file_1 = _create_temp_file()
    table_1_file_handle_1 = syn.uploadFileHandle(table_1_file_1, table_1)
    table_1_file_2 = _create_temp_file()
    table_1_file_handle_2 = syn.uploadFileHandle(table_1_file_2, table_1)

    data = [
        [1, table_1_file_handle_1['id']],
        [2, table_1_file_handle_2['id']],
    ]

    syn.store(
        synapseclient.RowSet(
            schema=table_1,
            rows=[synapseclient.Row(r) for r in data]
        )
    )

    db_path = tempfile.NamedTemporaryFile(delete=False).name
    schedule_for_cleanup(db_path)

    synapseutils.migrate(
        syn,
        project_entity,
        storage_location_id,
        db_path=db_path,
        version='latest',
    )

    file_0_entity_updated = syn.get(utils.id_of(file_0_entity), downloadFile=False)
    file_1_entity_updated = syn.get(utils.id_of(file_1_entity), downloadFile=False)
    file_2_entity_updated = syn.get(utils.id_of(file_2_entity), downloadFile=False)
    _assert_storage_location(
        [file_0_entity_updated, file_1_entity_updated, file_2_entity_updated],
        storage_location_id
    )
    assert storage_location_id != default_storage_location

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        result = cursor.execute(
            "select status, count(*) from entities where type in ('file', 'table') group by status"
        ).fetchall()

        counts = {r[0]: r[1] for r in result}

        # should only be one status and they should all be migrated (enum 2)
        # 3 file entities and 1 table entity, 4 total migrated entities
        assert len(counts) == 1
        assert counts[2] == 4

        migrated_file_entity_count = cursor.execute('select count(*) from file_entity_versions').fetchone()[0]
        assert migrated_file_entity_count == 3

        # table entity had 2 files attached
        migrated_table_file_entity_count = cursor.execute('select count(*) from table_entity_files').fetchone()[0]
        assert migrated_table_file_entity_count == 2


def test_migrate_file__specific_version(request, syn, project, schedule_for_cleanup, storage_location_id):
    """Verify we copy the file handle for the specified revision at the destination storage location"""

    v1, v2 = _migrate_file_version_test_helper(request, syn, project, schedule_for_cleanup)
    entity_id = utils.id_of(v2)
    mapping = synapseutils.migrate_file(syn, entity_id, storage_location_id, version=1)

    v1_updated = syn.get(entity_id, version=1, downloadFile=False)

    expected_mapping = {1: (v1.dataFileHandleId, v1_updated.dataFileHandleId)}
    assert mapping == expected_mapping

    # the file content should be the the same but the updated entity should be at the new location
    assert v1._file_handle['contentMd5'] == v1_updated._file_handle['contentMd5']
    assert v1._file_handle['storageLocationId'] != v1_updated._file_handle['storageLocationId']


def test_migrate_file__latest_version(request, syn, project, schedule_for_cleanup, storage_location_id):
    """Verify we copy the file handle for the latest revision at the destination storage lcoation"""

    v1, v2 = _migrate_file_version_test_helper(request, syn, project, schedule_for_cleanup)
    entity_id = utils.id_of(v2)

    # not passing version, should update the latest
    mapping = synapseutils.migrate_file(syn, entity_id, storage_location_id, version='latest')

    v2_updated = syn.get(entity_id, downloadFile=False)

    expected_mapping = {2: (v2.dataFileHandleId, v2_updated.dataFileHandleId)}
    assert mapping == expected_mapping

    # the file content should be the the same but the updated entity should be at the new location
    assert v2._file_handle['contentMd5'] == v2_updated._file_handle['contentMd5']
    assert v2._file_handle['storageLocationId'] != v2_updated._file_handle['storageLocationId']


def test_migrate_file__all_versions(request, syn, project, schedule_for_cleanup, storage_location_id):
    """Verify we make migrated versions of all file entity versions"""

    v1, v2 = _migrate_file_version_test_helper(request, syn, project, schedule_for_cleanup)
    entity_id = utils.id_of(v1)

    mapping = synapseutils.migrate_file(syn, entity_id, storage_location_id, version='all')

    v1_updated = syn.get(entity_id, version=1, downloadFile=False)
    v2_updated = syn.get(entity_id, version=2, downloadFile=False)

    expected_mapping = {
        1: (v1.dataFileHandleId, v1_updated.dataFileHandleId),
        2: (v2.dataFileHandleId, v2_updated.dataFileHandleId),
    }
    assert mapping == expected_mapping

    # the file content should be the the same but the updated entity should be at the new location
    for original_entity, updated_entity in [
        (v1, v1_updated),
        (v2, v2_updated),
    ]:
        assert original_entity._file_handle['contentMd5'] == updated_entity._file_handle['contentMd5']


def test_migrate_file__new_version(request, syn, project, schedule_for_cleanup, storage_location_id):
    """Verify we create a new revision at the destination storage location copying the data from the most
    recent version"""

    temp_path = _create_temp_file()
    schedule_for_cleanup(temp_path)
    file = synapseclient.File(name=request.node.name, path=temp_path, parent=project)
    v1 = syn.store(file)
    entity_id = utils.id_of(v1)

    mapping = synapseutils.migrate_file(syn, entity_id, storage_location_id, version='new')

    v2 = syn.get(entity_id)
    assert v2.versionNumber == 2

    expected_mapping = {2: (v1.dataFileHandleId, v2.dataFileHandleId)}
    assert mapping == expected_mapping

    # the file content should be the the same but the updated entity should be at the new location
    assert v1._file_handle['storageLocationId'] != v2._file_handle['storageLocationId']
    assert v1._file_handle['contentMd5'] == v2._file_handle['contentMd5']
    with open(temp_path, mode='r') as source, \
            open(v2.path, mode='r') as v2_read:
        source_data = source.read()
        v2_data = v2_read.read()
        assert v2_data == source_data


def test_migrate_table_file_handles(request, syn, project, storage_location_id):
    """Verify that we migrate all the file handles attached to a TableEntity"""

    test_name = '{}-{}'.format(request.node.name, uuid.uuid4())

    # store the table's schema
    cols = [
        synapseclient.Column(name='num', columnType='INTEGER'),
        synapseclient.Column(name='file_a', columnType='FILEHANDLEID'),
        synapseclient.Column(name='file_b', columnType='FILEHANDLEID'),
    ]
    schema = syn.store(
        synapseclient.Schema(
            name=test_name, columns=cols, parent=project
        )
    )

    test_file_1a = _create_temp_file()
    test_file_1b = _create_temp_file()

    test_file_2a = _create_temp_file()
    test_file_2b = _create_temp_file()

    test_file_3a = _create_temp_file()
    test_file_3b = _create_temp_file()

    file_handle_id_1a = syn.uploadFileHandle(test_file_1a, schema)
    file_handle_id_1b = syn.uploadFileHandle(test_file_1b, schema)
    file_handle_id_2a = syn.uploadFileHandle(test_file_2a, schema)
    file_handle_id_2b = syn.uploadFileHandle(test_file_2b, schema)
    file_handle_id_3a = syn.uploadFileHandle(test_file_3a, schema)
    file_handle_id_3b = syn.uploadFileHandle(test_file_3b, schema)

    data = [
        [1, file_handle_id_1a['id'], file_handle_id_1b['id']],
        [2, file_handle_id_2a['id'], file_handle_id_2b['id']],
        [3, file_handle_id_3a['id'], file_handle_id_3b['id']],
    ]

    syn.store(
        synapseclient.RowSet(
            schema=schema,
            rows=[synapseclient.Row(r) for r in data]
        )
    )

    entity_id = utils.id_of(schema)
    mapping = synapseutils.migrate_table(syn, schema, storage_location_id)
    for row in data:
        row_id = str(row[0])
        old_file_handle_id_a, new_file_handle_id_a = mapping[row_id]['file_a']
        old_file_handle_id_b, new_file_handle_id_b = mapping[row_id]['file_b']

        for old_file_handle_id, new_file_handle_id in [
            (old_file_handle_id_a, new_file_handle_id_a),
            (old_file_handle_id_b, new_file_handle_id_b),
        ]:
            old_file_handle = syn._getFileHandleDownload(
                old_file_handle_id,
                entity_id,
                'TableEntity'
            )['fileHandle']
            new_file_handle = syn._getFileHandleDownload(
                new_file_handle_id,
                entity_id,
                'TableEntity'
            )['fileHandle']
            assert new_file_handle['storageLocationId'] != old_file_handle['storageLocationId']
            assert new_file_handle['storageLocationId'] == storage_location_id
            assert new_file_handle['contentMd5'] == old_file_handle['contentMd5']
