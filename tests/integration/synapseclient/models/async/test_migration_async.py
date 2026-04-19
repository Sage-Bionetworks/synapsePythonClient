"""Integration tests for storage location migration using the StorageLocation model."""

import os
import tempfile
import uuid

import pandas as pd
import pytest
import pytest_asyncio

import synapseclient.core.utils as syn_utils
from synapseclient import Synapse
from synapseclient.api.file_services import get_file_handle_for_download_async
from synapseclient.core.upload import upload_file_handle
from synapseclient.models import (
    Column,
    File,
    FileHandle,
    Folder,
    Project,
    StorageLocation,
    StorageLocationType,
    Table,
)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="This test runs only locally, not in CI/CD environments.",
)
@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def migration_storage_location(syn: Synapse) -> StorageLocation:
    """Create a EXTERNAL_S3 storage location to migrate files into."""
    storage_location = await StorageLocation(
        storage_type=StorageLocationType.EXTERNAL_S3,
        bucket="test-storage-location-python-client-us-east-1",
    ).store_async(synapse_client=syn)
    return storage_location


def _assert_storage_location(file_handles, storage_location_id):
    for fh in file_handles:
        assert fh.storage_location_id == storage_location_id


class TestMigrateProjectWithStorageLocation:
    """Tests migrating a project's files to a storage location created via StorageLocation model."""

    @pytest.fixture(autouse=True)
    def setup_method(
        self,
        syn: Synapse,
        schedule_for_cleanup,
        migration_storage_location: StorageLocation,
        project_model: Project,
    ) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup
        self.dest_storage_location_id = migration_storage_location.storage_location_id
        self.project = project_model

    async def test_migrate_project(self) -> None:
        """Test migrating a project's files and table file handles to a new EXTERNAL_S3
        storage location created via the StorageLocation model."""
        # Create files to migrate
        file_0_path = syn_utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(file_0_path)
        file_0 = File(
            name=os.path.basename(file_0_path),
            path=file_0_path,
            parent_id=self.project.id,
        )
        file_0_entity = await file_0.store_async(synapse_client=self.syn)
        default_storage_location_id = file_0_entity.file_handle.storage_location_id

        folder_1 = await Folder(
            parent_id=self.project.id, name=str(uuid.uuid4())
        ).store_async(synapse_client=self.syn)

        file_1_path = syn_utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(file_1_path)
        file_1_entity = await File(
            name=os.path.basename(file_1_path), path=file_1_path, parent_id=folder_1.id
        ).store_async(synapse_client=self.syn)

        file_2_path = syn_utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(file_2_path)
        file_2_entity = await File(
            name=os.path.basename(file_2_path), path=file_2_path, parent_id=folder_1.id
        ).store_async(synapse_client=self.syn)

        # file_3 shares the same file handle as file_1
        file_3_entity = File(
            name=f"{os.path.basename(file_1_path)}_copy",
            path=file_1_path,
            parent_id=folder_1.id,
        )
        file_3_entity.data_file_handle_id = file_1_entity.data_file_handle_id
        file_3_entity = await file_3_entity.store_async(synapse_client=self.syn)

        # filehandles
        file_0_fh = file_0_entity.file_handle
        file_1_fh = file_1_entity.file_handle
        file_2_fh = file_2_entity.file_handle
        file_3_fh = file_3_entity.file_handle

        # Create a table with file handle columns
        table_cols = [
            Column(name="file_col_1", column_type="FILEHANDLEID"),
            Column(name="num", column_type="INTEGER"),
            Column(name="file_col_2", column_type="FILEHANDLEID"),
        ]
        table_entity = await Table(
            columns=table_cols, name=str(uuid.uuid4()), parent_id=self.project.id
        ).store_async(synapse_client=self.syn)
        fh_1_table = upload_file_handle(
            syn=self.syn,
            path=syn_utils.make_bogus_uuid_file(),
            parent_entity=table_entity.id,
        )
        fh_2_table = upload_file_handle(
            syn=self.syn,
            path=syn_utils.make_bogus_uuid_file(),
            parent_entity=table_entity.id,
        )
        fh_3_table = upload_file_handle(
            syn=self.syn,
            path=syn_utils.make_bogus_uuid_file(),
            parent_entity=table_entity.id,
        )
        fh_4_table = upload_file_handle(
            syn=self.syn,
            path=syn_utils.make_bogus_uuid_file(),
            parent_entity=table_entity.id,
        )
        df = pd.DataFrame(
            {
                "file_col_1": [fh_1_table["id"], fh_3_table["id"]],
                "num": [1, 2],
                "file_col_2": [fh_2_table["id"], fh_4_table["id"]],
            }
        )
        await table_entity.store_rows_async(values=df, synapse_client=self.syn)
        db_path = tempfile.NamedTemporaryFile(delete=False).name
        self.schedule_for_cleanup(db_path)
        # GIVEN files indexed for migration to the new storage location
        index_result = await self.project.index_files_for_migration_async(
            dest_storage_location_id=self.dest_storage_location_id,
            db_path=db_path,
            include_table_files=True,
            synapse_client=self.syn,
        )
        counts_by_status = await index_result.get_counts_by_status_async()
        assert counts_by_status["INDEXED"] == 8
        assert counts_by_status["ERRORED"] == 0
        assert counts_by_status["ALREADY_MIGRATED"] == 0
        # WHEN we migrate the indexed files
        migration_result = await self.project.migrate_indexed_files_async(
            db_path=db_path,
            force=True,
            synapse_client=self.syn,
        )
        # AND migration status should show all as MIGRATED
        counts_by_status = await migration_result.get_counts_by_status_async()
        assert counts_by_status["INDEXED"] == 0
        assert counts_by_status["ERRORED"] == 0
        assert counts_by_status["MIGRATED"] == 8

        file_0_updated = await File(id=file_0_entity.id).get_async(
            synapse_client=self.syn
        )
        file_1_updated = await File(id=file_1_entity.id).get_async(
            synapse_client=self.syn
        )
        file_2_updated = await File(id=file_2_entity.id).get_async(
            synapse_client=self.syn
        )
        file_3_updated = await File(id=file_3_entity.id).get_async(
            synapse_client=self.syn
        )
        file_handles = [
            file_0_updated.file_handle,
            file_1_updated.file_handle,
            file_2_updated.file_handle,
            file_3_updated.file_handle,
        ]

        # file handles for files should be updated
        assert file_0_updated.file_handle != file_0_fh
        assert file_1_updated.file_handle != file_1_fh
        assert file_2_updated.file_handle != file_2_fh
        assert file_3_updated.file_handle != file_3_fh

        table_id = table_entity.id
        results = await table_entity.query_async(
            "select file_col_1, file_col_2 from {}".format(table_id),
            synapse_client=self.syn,
        )
        # assert that the table file handles are updated
        assert results.iloc[0]["file_col_1"] != fh_1_table["id"]
        assert results.iloc[0]["file_col_2"] != fh_2_table["id"]
        assert results.iloc[1]["file_col_1"] != fh_3_table["id"]
        assert results.iloc[1]["file_col_2"] != fh_4_table["id"]

        table_file_handles = []
        for _, row in results.iterrows():
            for file_handle_id in row[2:]:
                response = await get_file_handle_for_download_async(
                    file_handle_id=file_handle_id,
                    synapse_id=table_id,
                    entity_type="TableEntity",
                    synapse_client=self.syn,
                )
                file_handle = FileHandle().fill_from_dict(response["fileHandle"])
                table_file_handles.append(file_handle)
        file_handles.extend(table_file_handles)
        # THEN all file handles should be migrated to the new storage location
        _assert_storage_location(file_handles, self.dest_storage_location_id)
