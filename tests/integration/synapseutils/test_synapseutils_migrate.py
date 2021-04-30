import json
import pytest
import sqlite3
import tempfile
import uuid

import synapseclient
from synapseclient.core.constants import concrete_types
import synapseclient.core.utils as utils
import synapseutils
from synapseutils.migrate_functions import _MigrationType, _MigrationStatus


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


def _assert_storage_location(file_handles, storage_location_id):
    for fh in file_handles:
        assert fh['storageLocationId'] == storage_location_id


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
    default_storage_location_id = file_0_entity._file_handle['storageLocationId']

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

    # file 3 shares the same file handle id as file 1
    file_3_path = file_1_path
    file_3_name = "{}-{}".format(test_name, 3)
    file_3 = synapseclient.File(name=file_3_name, path=file_3_path, parent=folder_1_entity)
    file_3.dataFileHandleId = file_1_entity.dataFileHandleId
    file_3_entity = syn.store(file_3)

    table_1_cols = [
        synapseclient.Column(name='file_col_1', columnType='FILEHANDLEID'),
        synapseclient.Column(name='num', columnType='INTEGER'),
        synapseclient.Column(name='file_col_2', columnType='FILEHANDLEID'),
    ]
    table_1 = syn.store(
        synapseclient.Schema(
            name=test_name, columns=table_1_cols, parent=folder_1_entity
        )
    )
    table_1_file_col_1_1 = _create_temp_file()
    table_1_file_handle_1 = syn.uploadFileHandle(table_1_file_col_1_1, table_1)
    table_1_file_col_1_2 = _create_temp_file()
    table_1_file_handle_2 = syn.uploadFileHandle(table_1_file_col_1_2, table_1)
    table_1_file_col_2_1 = _create_temp_file()
    table_1_file_handle_3 = syn.uploadFileHandle(table_1_file_col_2_1, table_1)
    table_1_file_col_2_2 = _create_temp_file()
    table_1_file_handle_4 = syn.uploadFileHandle(table_1_file_col_2_2, table_1)

    data = [
        [table_1_file_handle_1['id'], 1, table_1_file_handle_2['id']],
        [table_1_file_handle_3['id'], 2, table_1_file_handle_4['id']],
    ]

    table_1_entity = syn.store(
        synapseclient.RowSet(
            schema=table_1,
            rows=[synapseclient.Row(r) for r in data]
        )
    )

    db_path = tempfile.NamedTemporaryFile(delete=False).name
    schedule_for_cleanup(db_path)

    index_result = synapseutils.index_files_for_migration(
        syn,
        project_entity,
        storage_location_id,
        db_path,
        file_version_strategy='new',
        include_table_files=True,
    )

    counts_by_status = index_result.get_counts_by_status()
    assert counts_by_status['INDEXED'] == 8
    assert counts_by_status['ERRORED'] == 0

    migration_result = synapseutils.migrate_indexed_files(
        syn,
        db_path,
        force=True
    )

    file_0_entity_updated = syn.get(utils.id_of(file_0_entity), downloadFile=False)
    file_1_entity_updated = syn.get(utils.id_of(file_1_entity), downloadFile=False)
    file_2_entity_updated = syn.get(utils.id_of(file_2_entity), downloadFile=False)
    file_3_entity_updated = syn.get(utils.id_of(file_3_entity), downloadFile=False)
    file_handles = [
        f['_file_handle'] for f in (
            file_0_entity_updated,
            file_1_entity_updated,
            file_2_entity_updated,
            file_3_entity_updated,
        )
    ]

    table_1_id = utils.id_of(table_1_entity)
    results = syn.tableQuery("select file_col_1, file_col_2 from {}".format(utils.id_of(table_1_entity)))
    table_file_handles = []
    for row in results:
        for file_handle_id in row[2:]:
            file_handle = syn._getFileHandleDownload(file_handle_id, table_1_id, objectType='TableEntity')['fileHandle']
            table_file_handles.append(file_handle)
    file_handles.extend(table_file_handles)

    _assert_storage_location(
        file_handles,
        storage_location_id
    )
    assert storage_location_id != default_storage_location_id

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        query_result = cursor.execute(
            "select status, count(*) from migrations where type in (?, ?) group by status",
            (_MigrationType.FILE.value, _MigrationType.TABLE_ATTACHED_FILE.value)
        ).fetchall()

        counts = {r[0]: r[1] for r in query_result}

        # should only be one status and they should all be migrated
        # should be 3 migrated files entities + 4 migrated table attached files
        assert len(counts) == 1
        assert counts[_MigrationStatus.MIGRATED.value] == 8

    csv_file = tempfile.NamedTemporaryFile(delete=False)
    schedule_for_cleanup(csv_file.name)
    migration_result.as_csv(csv_file.name)
    with open(csv_file.name, 'r') as csv_file_in:
        csv_contents = csv_file_in.read()

    table_1_id = table_1_entity['tableId']

    # assert the content of the csv. we don't assert any particular order of the lines
    # but the presence of the expected lines and the correct # of lines
    csv_lines = csv_contents.split('\n')
    assert "id,type,version,row_id,col_name,from_storage_location_id,from_file_handle_id,to_file_handle_id,status,exception" in csv_lines  # noqa
    assert f"{file_0_entity.id},file,,,,{default_storage_location_id},{file_0_entity.dataFileHandleId},{file_0_entity_updated.dataFileHandleId},MIGRATED," in csv_lines  # noqa
    assert f"{file_1_entity.id},file,,,,{default_storage_location_id},{file_1_entity.dataFileHandleId},{file_1_entity_updated.dataFileHandleId},MIGRATED," in csv_lines  # noqa
    assert f"{file_2_entity.id},file,,,,{default_storage_location_id},{file_2_entity.dataFileHandleId},{file_2_entity_updated.dataFileHandleId},MIGRATED," in csv_lines  # noqa
    assert f"{file_3_entity.id},file,,,,{default_storage_location_id},{file_3_entity.dataFileHandleId},{file_3_entity_updated.dataFileHandleId},MIGRATED," in csv_lines  # noqa
    assert f"{table_1_id},table,1,1,file_col_1,{default_storage_location_id},{table_1_file_handle_1['id']},{table_file_handles[0]['id']},MIGRATED," in csv_lines  # noqa
    assert f"{table_1_id},table,1,1,file_col_2,{default_storage_location_id},{table_1_file_handle_2['id']},{table_file_handles[1]['id']},MIGRATED," in csv_lines  # noqa
    assert f"{table_1_id},table,1,2,file_col_1,{default_storage_location_id},{table_1_file_handle_3['id']},{table_file_handles[2]['id']},MIGRATED," in csv_lines  # noqa
    assert f"{table_1_id},table,1,2,file_col_2,{default_storage_location_id},{table_1_file_handle_4['id']},{table_file_handles[3]['id']},MIGRATED," in csv_lines  # noqa
    assert "" in csv_lines  # expect trailing newline in a csv
