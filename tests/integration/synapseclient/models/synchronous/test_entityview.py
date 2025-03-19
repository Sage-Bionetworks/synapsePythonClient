import asyncio
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import synapseclient.models.mixins.table_components as table_module
from synapseclient import Synapse
from synapseclient.api import get_default_columns
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    EntityView,
    File,
    Folder,
    Project,
    UsedURL,
    ViewTypeMask,
    query,
    query_part_mask,
)


class TestEntityViewCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_entityview_with_default_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a entityview with no columns
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            view_type_mask=ViewTypeMask.FILE,
        )

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # THEN the entityview should be created
        assert entityview.id is not None

        # AND I can retrieve that entityview from Synapse
        new_entityview_instance = EntityView(id=entityview.id).get(
            synapse_client=self.syn
        )
        assert new_entityview_instance is not None
        assert new_entityview_instance.name == entityview_name
        assert new_entityview_instance.id == entityview.id
        assert new_entityview_instance.description == entityview_description

        # AND the columns on the view match the default columns
        default_columns = await get_default_columns(
            view_type_mask=ViewTypeMask.FILE.value, synapse_client=self.syn
        )
        assert len(new_entityview_instance.columns) == len(default_columns)
        assert len(default_columns) > 0
        for column in default_columns:
            assert column.name in new_entityview_instance.columns
            assert column == new_entityview_instance.columns[column.name]

    async def test_create_entityview_with_single_column(
        self, project_model: Project
    ) -> None:
        # GIVEN a entityview with a single column
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            columns=[Column(name="test_column", column_type=ColumnType.STRING)],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=False,
        )

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # THEN the entityview should be created
        assert entityview.id is not None

        # AND I can retrieve that entityview from Synapse
        new_entityview_instance = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_entityview_instance is not None
        assert new_entityview_instance.name == entityview_name
        assert new_entityview_instance.id == entityview.id
        assert new_entityview_instance.description == entityview_description
        assert new_entityview_instance.columns["test_column"].name == "test_column"
        assert (
            new_entityview_instance.columns["test_column"].column_type
            == ColumnType.STRING
        )
        assert len(new_entityview_instance.columns) == 1

    async def test_create_entityview_with_multiple_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a entityview with multiple columns
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=False,
        )

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # THEN the entityview should be created
        assert entityview.id is not None

        # AND I can retrieve that entityview from Synapse
        new_entityview_instance = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_entityview_instance is not None
        assert new_entityview_instance.name == entityview_name
        assert new_entityview_instance.id == entityview.id
        assert new_entityview_instance.description == entityview_description
        assert new_entityview_instance.columns["test_column"].name == "test_column"
        assert (
            new_entityview_instance.columns["test_column"].column_type
            == ColumnType.STRING
        )
        assert new_entityview_instance.columns["test_column2"].name == "test_column2"
        assert (
            new_entityview_instance.columns["test_column2"].column_type
            == ColumnType.INTEGER
        )
        assert len(new_entityview_instance.columns) == 2

    async def test_create_entityview_with_invalid_column(
        self, project_model: Project
    ) -> None:
        # GIVEN a entityview with an invalid column
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            columns=[
                Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    maximum_size=999999999,
                )
            ],
            view_type_mask=ViewTypeMask.FILE,
        )

        # WHEN I store the entityview
        with pytest.raises(SynapseHTTPError) as e:
            entityview.store(synapse_client=self.syn)

        # THEN the entityview should not be created
        assert (
            "400 Client Error: ColumnModel.maxSize for a STRING cannot exceed:"
            in str(e.value)
        )

    async def test_create_entityview_with_files_in_scope(
        self, project_model: Project
    ) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND 4 files stored in a folder in Synapse
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        file2 = File(
            parent_id=folder.id,
            name="file2",
            data_file_handle_id=file1.data_file_handle_id,
            description="file2_description",
        ).store(synapse_client=self.syn)
        file3 = File(
            parent_id=folder.id,
            name="file3",
            data_file_handle_id=file1.data_file_handle_id,
            description="file3_description",
        ).store(synapse_client=self.syn)
        file4 = File(
            parent_id=folder.id,
            name="file4",
            data_file_handle_id=file1.data_file_handle_id,
            description="file4_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        self.schedule_for_cleanup(file2.id)
        self.schedule_for_cleanup(file3.id)
        self.schedule_for_cleanup(file4.id)

        # AND a entityview with default columns defined with the folder in it's scope
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE,
            scope_ids=[folder.id],
        )

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # AND I query for the data in the file view
        results = query(f"SELECT * FROM {entityview.id}", synapse_client=self.syn)

        # THEN the data for the files should exist in the entityview
        assert len(results) == 4

        assert results["name"][0] == file1.name
        assert results["name"][1] == file2.name
        assert results["name"][2] == file3.name
        assert results["name"][3] == file4.name

        assert results["description"][0] == file1.description
        assert results["description"][1] == file2.description
        assert results["description"][2] == file3.description
        assert results["description"][3] == file4.description


class TestRowStorage:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_update_rows_from_csv(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND a entityview with default columns defined
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )

        # AND 4 files to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        file2 = File(
            parent_id=folder.id,
            name="file2",
            data_file_handle_id=file1.data_file_handle_id,
            description="file2_description",
        ).store(synapse_client=self.syn)
        file3 = File(
            parent_id=folder.id,
            name="file3",
            data_file_handle_id=file1.data_file_handle_id,
            description="file3_description",
        ).store(synapse_client=self.syn)
        file4 = File(
            parent_id=folder.id,
            name="file4",
            data_file_handle_id=file1.data_file_handle_id,
            description="file4_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        self.schedule_for_cleanup(file2.id)
        self.schedule_for_cleanup(file3.id)
        self.schedule_for_cleanup(file4.id)

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # AND I query for the data in the file view
        first_storage_results = query(
            f"SELECT * FROM {entityview.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        assert len(first_storage_results) == 4

        assert first_storage_results["name"][0] == file1.name
        assert first_storage_results["name"][1] == file2.name
        assert first_storage_results["name"][2] == file3.name
        assert first_storage_results["name"][3] == file4.name

        assert first_storage_results["description"][0] == file1.description
        assert first_storage_results["description"][1] == file2.description
        assert first_storage_results["description"][2] == file3.description
        assert first_storage_results["description"][3] == file4.description

        # WHEN I add new columns to the results
        first_storage_results["column_string"] = [
            "value1",
            "value2",
            "value3",
            "value4",
        ]
        first_storage_results["integer_column"] = [1, 2, 3, None]
        first_storage_results["float_column"] = [1.1, 2.2, 3.3, None]
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        first_storage_results.to_csv(filepath, index=False, float_format="%.12g")

        # AND I update rows in the entityview
        entityview.update_rows(
            values=filepath,
            primary_keys=["id"],
            synapse_client=self.syn,
            wait_for_eventually_consistent_view=True,
        )

        # THEN the spy should have been called
        spy_csv_file_conversion.assert_called_once()

        # AND the columns should exist
        assert "column_string" in entityview.columns
        assert "integer_column" in entityview.columns
        assert "float_column" in entityview.columns

        # AND I can query the entityview
        modified_data_results = query(
            f"SELECT * FROM {entityview.id}", synapse_client=self.syn
        )

        assert len(modified_data_results) == 4

        # AND the data in the new columns should match
        pd.testing.assert_series_equal(
            modified_data_results["column_string"],
            first_storage_results["column_string"],
        )
        pd.testing.assert_series_equal(
            modified_data_results["integer_column"],
            first_storage_results["integer_column"],
        )
        pd.testing.assert_series_equal(
            modified_data_results["float_column"], first_storage_results["float_column"]
        )

        # AND the data on the file entities should be updated
        file1_copy = File(id=file1.id, download_file=False).get(synapse_client=self.syn)
        assert file1_copy.annotations["column_string"] == ["value1"]
        assert file1_copy.annotations["integer_column"] == [1]
        assert file1_copy.annotations["float_column"] == [1.1]

        file2_copy = File(id=file2.id, download_file=False).get(synapse_client=self.syn)
        assert file2_copy.annotations["column_string"] == ["value2"]
        assert file2_copy.annotations["integer_column"] == [2]
        assert file2_copy.annotations["float_column"] == [2.2]

        file3_copy = File(id=file3.id, download_file=False).get(synapse_client=self.syn)
        assert file3_copy.annotations["column_string"] == ["value3"]
        assert file3_copy.annotations["integer_column"] == [3]
        assert file3_copy.annotations["float_column"] == [3.3]

        file4_copy = File(id=file4.id, download_file=False).get(synapse_client=self.syn)
        assert file4_copy.annotations["column_string"] == ["value4"]
        assert "integer_column" not in file4_copy.annotations.keys()
        assert "float_column" not in file4_copy.annotations.keys()

    async def test_update_rows_from_df(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND a entityview with default columns defined
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )

        # AND 4 files to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        file2 = File(
            parent_id=folder.id,
            name="file2",
            data_file_handle_id=file1.data_file_handle_id,
            description="file2_description",
        ).store(synapse_client=self.syn)
        file3 = File(
            parent_id=folder.id,
            name="file3",
            data_file_handle_id=file1.data_file_handle_id,
            description="file3_description",
        ).store(synapse_client=self.syn)
        file4 = File(
            parent_id=folder.id,
            name="file4",
            data_file_handle_id=file1.data_file_handle_id,
            description="file4_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        self.schedule_for_cleanup(file2.id)
        self.schedule_for_cleanup(file3.id)
        self.schedule_for_cleanup(file4.id)

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # AND I query for the data in the file view
        first_storage_results = query(
            f"SELECT * FROM {entityview.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        assert len(first_storage_results) == 4

        assert first_storage_results["name"][0] == file1.name
        assert first_storage_results["name"][1] == file2.name
        assert first_storage_results["name"][2] == file3.name
        assert first_storage_results["name"][3] == file4.name

        assert first_storage_results["description"][0] == file1.description
        assert first_storage_results["description"][1] == file2.description
        assert first_storage_results["description"][2] == file3.description
        assert first_storage_results["description"][3] == file4.description

        # WHEN I add new columns to the results
        first_storage_results["column_string"] = [
            "value1",
            "value2",
            "value3",
            "value4",
        ]
        first_storage_results["integer_column"] = [1, 2, 3, None]
        first_storage_results["float_column"] = [1.1, 2.2, 3.3, None]
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)

        # AND I update rows in the entityview
        entityview.update_rows(
            values=first_storage_results,
            primary_keys=["id"],
            synapse_client=self.syn,
            wait_for_eventually_consistent_view=True,
        )

        # THEN the spy should not have been called
        spy_csv_file_conversion.assert_not_called()

        # AND the columns should exist
        assert "column_string" in entityview.columns
        assert "integer_column" in entityview.columns
        assert "float_column" in entityview.columns

        # AND I can query the entityview
        modified_data_results = query(
            f"SELECT * FROM {entityview.id}", synapse_client=self.syn
        )

        assert len(modified_data_results) == 4

        # AND the data in the new columns should match
        pd.testing.assert_series_equal(
            modified_data_results["column_string"],
            first_storage_results["column_string"],
        )
        pd.testing.assert_series_equal(
            modified_data_results["integer_column"],
            first_storage_results["integer_column"],
        )
        pd.testing.assert_series_equal(
            modified_data_results["float_column"], first_storage_results["float_column"]
        )

        # AND the data on the file entities should be updated
        file1_copy = File(id=file1.id, download_file=False).get(synapse_client=self.syn)
        assert file1_copy.annotations["column_string"] == ["value1"]
        assert file1_copy.annotations["integer_column"] == [1]
        assert file1_copy.annotations["float_column"] == [1.1]

        file2_copy = File(id=file2.id, download_file=False).get(synapse_client=self.syn)
        assert file2_copy.annotations["column_string"] == ["value2"]
        assert file2_copy.annotations["integer_column"] == [2]
        assert file2_copy.annotations["float_column"] == [2.2]

        file3_copy = File(id=file3.id, download_file=False).get(synapse_client=self.syn)
        assert file3_copy.annotations["column_string"] == ["value3"]
        assert file3_copy.annotations["integer_column"] == [3]
        assert file3_copy.annotations["float_column"] == [3.3]

        file4_copy = File(id=file4.id, download_file=False).get(synapse_client=self.syn)
        assert file4_copy.annotations["column_string"] == ["value4"]
        assert "integer_column" not in file4_copy.annotations.keys()
        assert "float_column" not in file4_copy.annotations.keys()

    async def test_update_rows_from_dict(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND a entityview with default columns defined
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )

        # AND 4 files to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        file2 = File(
            parent_id=folder.id,
            name="file2",
            data_file_handle_id=file1.data_file_handle_id,
            description="file2_description",
        ).store(synapse_client=self.syn)
        file3 = File(
            parent_id=folder.id,
            name="file3",
            data_file_handle_id=file1.data_file_handle_id,
            description="file3_description",
        ).store(synapse_client=self.syn)
        file4 = File(
            parent_id=folder.id,
            name="file4",
            data_file_handle_id=file1.data_file_handle_id,
            description="file4_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        self.schedule_for_cleanup(file2.id)
        self.schedule_for_cleanup(file3.id)
        self.schedule_for_cleanup(file4.id)

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # AND I update rows with data in the entityview
        updated_content = {
            "id": [file1.id, file2.id, file3.id, file4.id],
            "column_string": ["value1", "value2", "value3", "value4"],
            "integer_column": [1, 2, 3, None],
            "float_column": [1.1, 2.2, 3.3, None],
        }

        entityview.update_rows(
            values=updated_content,
            primary_keys=["id"],
            synapse_client=self.syn,
            wait_for_eventually_consistent_view=True,
        )

        # THEN the spy should not have been called
        spy_csv_file_conversion.assert_not_called()

        # AND the columns should exist
        assert "column_string" in entityview.columns
        assert "integer_column" in entityview.columns
        assert "float_column" in entityview.columns

        # AND I can query the entityview
        modified_data_results = query(
            f"SELECT * FROM {entityview.id}", synapse_client=self.syn
        )

        assert len(modified_data_results) == 4

        # AND the data in the new columns should match
        updated_content_df = pd.DataFrame(updated_content)
        pd.testing.assert_series_equal(
            modified_data_results["column_string"], updated_content_df["column_string"]
        )
        pd.testing.assert_series_equal(
            modified_data_results["integer_column"],
            updated_content_df["integer_column"],
        )
        pd.testing.assert_series_equal(
            modified_data_results["float_column"], updated_content_df["float_column"]
        )

        # AND the data on the file entities should be updated
        file1_copy = File(id=file1.id, download_file=False).get(synapse_client=self.syn)
        assert file1_copy.annotations["column_string"] == ["value1"]
        assert file1_copy.annotations["integer_column"] == [1]
        assert file1_copy.annotations["float_column"] == [1.1]

        file2_copy = File(id=file2.id, download_file=False).get(synapse_client=self.syn)
        assert file2_copy.annotations["column_string"] == ["value2"]
        assert file2_copy.annotations["integer_column"] == [2]
        assert file2_copy.annotations["float_column"] == [2.2]

        file3_copy = File(id=file3.id, download_file=False).get(synapse_client=self.syn)
        assert file3_copy.annotations["column_string"] == ["value3"]
        assert file3_copy.annotations["integer_column"] == [3]
        assert file3_copy.annotations["float_column"] == [3.3]

        file4_copy = File(id=file4.id, download_file=False).get(synapse_client=self.syn)
        assert file4_copy.annotations["column_string"] == ["value4"]
        assert "integer_column" not in file4_copy.annotations.keys()
        assert "float_column" not in file4_copy.annotations.keys()

    async def test_update_rows_without_id_column(self, project_model: Project) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND a entityview with default columns defined
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
        )

        # AND the entityview is stored to Synapse
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # AND I remove the `id` column from the entityview
        entityview.delete_column(name="id")
        entityview.store(synapse_client=self.syn)

        # WHEN I try to update the rows
        with pytest.raises(ValueError) as e:
            entityview.update_rows(
                values={},
                primary_keys=["id"],
                synapse_client=self.syn,
                wait_for_eventually_consistent_view=True,
            )

        # THEN the entityview should raise an exception that I am missing the `id` column
        assert (
            "The 'id' column is required to wait for eventually consistent views."
            in str(e.value)
        )


class TestColumnModifications:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_column_rename(self, project_model: Project) -> None:
        # GIVEN a entityview in Synapse
        entityview_name = str(uuid.uuid4())
        old_column_name = "column_string"
        old_entityview_instance = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            columns=[Column(name=old_column_name, column_type=ColumnType.STRING)],
            view_type_mask=ViewTypeMask.FILE,
        )
        old_entityview_instance = old_entityview_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_entityview_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_entityview_instance.columns[old_column_name].name = new_column_name

        # AND I store the entityview
        old_entityview_instance.store(synapse_client=self.syn)

        # THEN the column name should be updated on the existing entityview instance
        assert old_entityview_instance.columns[new_column_name] is not None
        assert old_column_name not in old_entityview_instance.columns

        # AND the new column name should be reflected in the Synapse entityview
        new_entityview_instance = EntityView(
            id=old_entityview_instance.id, view_type_mask=ViewTypeMask.FILE
        ).get(synapse_client=self.syn)
        assert new_entityview_instance.columns[new_column_name] is not None
        assert old_column_name not in new_entityview_instance.columns

    async def test_delete_column(self, project_model: Project) -> None:
        # GIVEN a entityview in Synapse
        entityview_name = str(uuid.uuid4())
        old_column_name = "column_string"
        column_to_keep = "column_to_keep"
        old_entityview_instance = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
            view_type_mask=ViewTypeMask.FILE,
        )
        old_entityview_instance = old_entityview_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_entityview_instance.id)

        # WHEN I delete the column
        old_entityview_instance.delete_column(name=old_column_name)

        # AND I store the entityview
        old_entityview_instance.store(synapse_client=self.syn)

        # THEN the column should be removed from the entityview instance
        assert old_column_name not in old_entityview_instance.columns

        # AND the column to keep should still be in the entityview instance
        assert column_to_keep in old_entityview_instance.columns

        # AND the column should be removed from the Synapse entityview
        new_entityview_instance = EntityView(
            id=old_entityview_instance.id, view_type_mask=ViewTypeMask.FILE
        ).get(synapse_client=self.syn)
        assert old_column_name not in new_entityview_instance.columns

        # AND the column to keep should still be in the Synapse entityview
        assert column_to_keep in new_entityview_instance.columns


class TestQuerying:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_part_mask_query_everything(self, project_model: Project) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND a entityview
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
        )

        # AND 2 files to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        file2 = File(
            parent_id=folder.id,
            name="file2",
            data_file_handle_id=file1.data_file_handle_id,
            description="file2_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        self.schedule_for_cleanup(file2.id)

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # WHEN I query the entityview with a part mask
        query_results = 0x1
        query_count = 0x2
        sum_file_size_bytes = 0x40
        last_updated_on = 0x80
        part_mask = query_results | query_count | sum_file_size_bytes | last_updated_on

        results = query_part_mask(
            query=f"SELECT * FROM {entityview.id} ORDER BY id ASC",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN the part mask should be reflected in the results
        assert results.count == 2
        assert results.sum_file_sizes is not None
        assert results.sum_file_sizes.greater_than is not None
        assert results.sum_file_sizes.sum_file_size_bytes is not None
        assert results.last_updated_on is not None

        # AND The results should contain the expected files
        assert results.result["name"].tolist() == ["file1", "file2"]

    async def test_part_mask_query_results_only(self, project_model: Project) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND a entityview
        entityview_name = str(uuid.uuid4())
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
        )

        # AND 2 files to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        file2 = File(
            parent_id=folder.id,
            name="file2",
            data_file_handle_id=file1.data_file_handle_id,
            description="file2_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        self.schedule_for_cleanup(file2.id)

        # WHEN I store the entityview
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # WHEN I query the entityview with a part mask of only the results
        query_results = 0x1
        results = query_part_mask(
            query=f"SELECT * FROM {entityview.id} ORDER BY id ASC",
            synapse_client=self.syn,
            part_mask=query_results,
        )

        # THEN the part mask should be reflected in the results
        assert results.count is None
        assert results.sum_file_sizes is None
        assert results.last_updated_on is None

        # AND The results should contain the expected files
        assert results.result["name"].tolist() == ["file1", "file2"]


class TestEntityViewSnapshot:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_snapshot_with_activity(self, project_model: Project) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND 1 file to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)

        # AND a entityview
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE,
            activity=Activity(
                name="Activity for snapshot",
                used=[UsedURL(name="Synapse", url="https://synapse.org")],
            ),
        )

        # AND the entityview is stored in Synapse
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)
        assert entityview.id is not None

        # WHEN I snapshot the entityview
        snapshot = entityview.snapshot(
            comment="My snapshot",
            label="My snapshot label",
            include_activity=True,
            associate_activity_to_new_version=True,
            synapse_client=self.syn,
        )

        # THEN the table should be snapshotted
        assert snapshot.results is not None

        # AND getting the first version of the entityview should return the snapshot instance
        snapshot_instance = EntityView(id=entityview.id, version_number=1).get(
            synapse_client=self.syn, include_activity=True
        )
        assert snapshot_instance is not None
        assert snapshot_instance.version_number == 1
        assert snapshot_instance.id == entityview.id
        assert snapshot_instance.name == entityview_name
        assert snapshot_instance.description == entityview_description
        assert snapshot_instance.version_comment == "My snapshot"
        assert snapshot_instance.version_label == "My snapshot label"
        assert snapshot_instance.activity.name == "Activity for snapshot"
        assert snapshot_instance.activity.used[0].name == "Synapse"
        assert snapshot_instance.activity.used[0].url == "https://synapse.org"

        # AND The activity should be associated with the new version
        newest_instance = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_activity=True
        )
        assert newest_instance.version_number == 2
        assert newest_instance.activity is not None
        assert newest_instance.activity.name == "Activity for snapshot"

    async def test_snapshot_with_activity_not_pulled_forward(
        self, project_model: Project
    ) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND 1 file to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)

        # AND a entityview
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE,
            activity=Activity(
                name="Activity for snapshot",
                used=[UsedURL(name="Synapse", url="https://synapse.org")],
            ),
        )

        # AND the entityview is stored in Synapse
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)
        assert entityview.id is not None
        await asyncio.sleep(5)

        # WHEN I snapshot the entityview
        snapshot = entityview.snapshot(
            comment="My snapshot",
            label="My snapshot label",
            include_activity=True,
            associate_activity_to_new_version=False,
            synapse_client=self.syn,
        )

        # THEN the table should be snapshotted
        assert snapshot.results is not None

        # AND getting the first version of the entityview should return the snapshot instance
        snapshot_instance = EntityView(id=entityview.id, version_number=1).get(
            synapse_client=self.syn, include_activity=True
        )
        assert snapshot_instance is not None
        assert snapshot_instance.version_number == 1
        assert snapshot_instance.id == entityview.id
        assert snapshot_instance.name == entityview_name
        assert snapshot_instance.description == entityview_description
        assert snapshot_instance.version_comment == "My snapshot"
        assert snapshot_instance.version_label == "My snapshot label"
        assert snapshot_instance.activity.name == "Activity for snapshot"
        assert snapshot_instance.activity.used[0].name == "Synapse"
        assert snapshot_instance.activity.used[0].url == "https://synapse.org"

        # AND The activity should not be associated with the new version
        newest_instance = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_activity=True
        )
        assert newest_instance.version_number == 2
        assert newest_instance.activity is None

    async def test_snapshot_with_activity_not_in_snapshot(
        self, project_model: Project
    ) -> None:
        # GIVEN a unique folder for this test
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # AND 1 file to show up in that entityview
        filename = utils.make_bogus_uuid_file()
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)

        # AND a entityview
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE,
            activity=Activity(
                name="Activity for snapshot",
                used=[UsedURL(name="Synapse", url="https://synapse.org")],
            ),
        )

        # AND the entityview is stored in Synapse
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)
        assert entityview.id is not None

        # WHEN I snapshot the entityview
        snapshot = entityview.snapshot(
            comment="My snapshot",
            label="My snapshot label",
            include_activity=False,
            associate_activity_to_new_version=False,
            synapse_client=self.syn,
        )

        # THEN the table should be snapshotted
        assert snapshot.results is not None

        # AND getting the first version of the entityview should return the snapshot instance
        snapshot_instance = EntityView(id=entityview.id, version_number=1).get(
            synapse_client=self.syn, include_activity=True
        )
        assert snapshot_instance is not None
        assert snapshot_instance.version_number == 1
        assert snapshot_instance.id == entityview.id
        assert snapshot_instance.name == entityview_name
        assert snapshot_instance.description == entityview_description
        assert snapshot_instance.version_comment == "My snapshot"
        assert snapshot_instance.version_label == "My snapshot label"
        assert snapshot_instance.activity is None

        # AND The activity should not be associated with the new version
        newest_instance = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_activity=True
        )
        assert newest_instance.version_number == 2
        assert newest_instance.activity is None

    async def test_snapshot_with_no_scope(self, project_model: Project) -> None:
        # GIVEN a entityview
        entityview_name = str(uuid.uuid4())
        entityview_description = "Test entityview"
        entityview = EntityView(
            name=entityview_name,
            parent_id=project_model.id,
            description=entityview_description,
            view_type_mask=ViewTypeMask.FILE,
        )

        # AND the entityview is stored in Synapse
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)
        assert entityview.id is not None

        # WHEN I snapshot the entityview
        with pytest.raises(SynapseHTTPError) as e:
            entityview.snapshot(
                comment="My snapshot",
                label="My snapshot label",
                synapse_client=self.syn,
            )

        # THEN the entityview should not be snapshot
        assert (
            "400 Client Error: You cannot create a version of a view that has no scope."
            in str(e.value)
        )
