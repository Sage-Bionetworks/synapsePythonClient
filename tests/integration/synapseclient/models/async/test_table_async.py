import json
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import synapseclient.models.mixins.asynchronous_job as asynchronous_job_module
import synapseclient.models.mixins.table_operator as table_module
from synapseclient import Evaluation, Synapse
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Column,
    ColumnExpansionStrategy,
    ColumnType,
    File,
    Project,
    SchemaStorageStrategy,
    Table,
    query_async,
    query_part_mask_async,
)


class TestTableCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_table_with_no_columns(self, project_model: Project) -> None:
        # GIVEN a table with no columns
        table_name = str(uuid.uuid4())
        table_description = "Test table"
        table = Table(
            name=table_name, parent_id=project_model.id, description=table_description
        )

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = await Table(id=table.id).get_async(synapse_client=self.syn)
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.description == table_description

    async def test_create_table_with_single_column(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with a single column
        table_name = str(uuid.uuid4())
        table_description = "Test table"
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            description=table_description,
            columns=[Column(name="test_column", column_type=ColumnType.STRING)],
        )

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = await Table(id=table.id).get_async(
            synapse_client=self.syn, include_columns=True
        )
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.description == table_description
        assert new_table_instance.columns["test_column"].name == "test_column"
        assert (
            new_table_instance.columns["test_column"].column_type == ColumnType.STRING
        )

    async def test_create_table_with_multiple_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with multiple columns
        table_name = str(uuid.uuid4())
        table_description = "Test table"
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            description=table_description,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = await Table(id=table.id).get_async(
            synapse_client=self.syn, include_columns=True
        )
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.description == table_description
        assert new_table_instance.columns["test_column"].name == "test_column"
        assert (
            new_table_instance.columns["test_column"].column_type == ColumnType.STRING
        )
        assert new_table_instance.columns["test_column2"].name == "test_column2"
        assert (
            new_table_instance.columns["test_column2"].column_type == ColumnType.INTEGER
        )

    async def test_create_table_with_invalid_column(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with an invalid column
        table_name = str(uuid.uuid4())
        table_description = "Test table"
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            description=table_description,
            columns=[
                Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    maximum_size=999999999,
                )
            ],
        )

        # WHEN I store the table
        with pytest.raises(SynapseHTTPError) as e:
            await table.store_async(synapse_client=self.syn)

        # THEN the table should not be created
        assert (
            "400 Client Error: ColumnModel.maxSize for a STRING cannot exceed:"
            in str(e.value)
        )

    async def test_create_table_with_column_from_dict(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with no columns defined
        table_name = str(uuid.uuid4())
        table = Table(name=table_name, parent_id=project_model.id)

        # AND data for a column
        data_for_table = {
            "column_string": ["value1", "value2", "value3"],
        }

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table
        await table.store_rows_async(
            values=data_for_table,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = await Table(id=table.id).get_async(
            synapse_client=self.syn, include_columns=True
        )
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.columns["column_string"].name == "column_string"
        assert (
            new_table_instance.columns["column_string"].column_type == ColumnType.STRING
        )

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], pd.DataFrame(data_for_table)["column_string"]
        )

    async def test_create_table_with_column_from_dataframe(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with no columns defined
        table_name = str(uuid.uuid4())
        table = Table(name=table_name, parent_id=project_model.id)

        # AND data for a column
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
            }
        )

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table
        await table.store_rows_async(
            values=data_for_table,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = await Table(id=table.id).get_async(
            synapse_client=self.syn, include_columns=True
        )
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.columns["column_string"].name == "column_string"
        assert (
            new_table_instance.columns["column_string"].column_type == ColumnType.STRING
        )

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )

    async def test_create_table_with_column_from_csv(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with no columns defined
        table_name = str(uuid.uuid4())
        table = Table(name=table_name, parent_id=project_model.id)

        # AND data for a column
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table
        await table.store_rows_async(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = await Table(id=table.id).get_async(
            synapse_client=self.syn, include_columns=True
        )
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.columns["column_string"].name == "column_string"
        assert (
            new_table_instance.columns["column_string"].column_type == ColumnType.STRING
        )

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )


class TestRowStorage:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_store_rows_from_csv_infer_columns(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a table with no columns defined
        table_name = str(uuid.uuid4())
        table = Table(name=table_name, parent_id=project_model.id)

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_string": [1, 2, 3, None],
                "float_string": [1.1, 2.2, 3.3, None],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table with INFER_FROM_DATA schema storage strategy
        await table.store_rows_async(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should be created
        assert table.id is not None

        # AND the spy should have been called
        spy_csv_file_conversion.assert_called_once()

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["integer_string"], data_for_table["integer_string"]
        )
        pd.testing.assert_series_equal(
            results["float_string"], data_for_table["float_string"]
        )

    async def test_update_rows_from_csv_infer_columns_no_column_updates(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with no columns defined
        table_name = str(uuid.uuid4())
        table = Table(name=table_name, parent_id=project_model.id)

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # AND the table is stored in Synapse
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND Rows are stored into Synapse with the INFER_FROM_DATA schema storage strategy
        await table.store_rows_async(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )
        assert table.id is not None

        # AND a query of the data
        query_results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # WHEN I update the rows with new data
        query_results.loc[
            query_results["column_string"] == "value1", "column_string"
        ] = "value11"
        query_results.loc[
            query_results["column_string"] == "value3", "column_string"
        ] = "value33"

        # AND I store the rows back to Synapse
        await Table(id=table.id).store_rows_async(
            values=query_results,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the data should be stored in Synapse, and match the updated data
        updated_results_from_table = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )
        pd.testing.assert_series_equal(
            updated_results_from_table["column_string"], query_results["column_string"]
        )

    async def test_store_rows_from_csv_no_columns(self, project_model: Project) -> None:
        # GIVEN a table with no columns defined
        table_name = str(uuid.uuid4())
        table = Table(name=table_name, parent_id=project_model.id)

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN I store rows to the table with no schema storage strategy
        with pytest.raises(SynapseHTTPError) as e:
            await table.store_rows_async(
                values=filepath, schema_storage_strategy=None, synapse_client=self.syn
            )

        # THEN the table data should fail to be inserted
        assert (
            "400 Client Error: The first line is expected to be a header but the values do not match the names of of the columns of the table (column_string is not a valid column name or id). Header row: column_string"
            in str(e.value)
        )

    async def test_store_rows_from_manually_defined_columns(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_column": [1, 2, 3, None],
                "float_column": [1.1, 2.2, 3.3, None],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table with no schema storage strategy
        await table.store_rows_async(
            values=filepath, schema_storage_strategy=None, synapse_client=self.syn
        )

        # THEN the table should be created
        assert table.id is not None

        # AND the spy should not have been called
        spy_csv_file_conversion.assert_not_called()

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["integer_column"], data_for_table["integer_column"]
        )
        pd.testing.assert_series_equal(
            results["float_column"], data_for_table["float_column"]
        )

    async def test_store_rows_on_existing_table_with_schema_storage_strategy(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name="column_string", column_type=ColumnType.STRING)],
        )

        # AND the table exists in Synapse
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with a schema storage strategy
        await table.store_rows_async(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the spy should have been called
        spy_csv_file_conversion.assert_called_once()

        # AND the schema should not have been updated
        assert len(spy_send_job.call_args.kwargs["request"]["changes"]) == 1
        assert (
            spy_send_job.call_args.kwargs["request"]["changes"][0]["concreteType"]
            == concrete_types.UPLOAD_TO_TABLE_REQUEST
        )

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )

    async def test_store_rows_on_existing_table_with_expanding_string_column(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(
                    name="column_string", column_type=ColumnType.STRING, maximum_size=10
                )
            ],
        )

        # AND the table exists in Synapse
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": [
                    "long_string_value_over_maximum_size1",
                    "long_string_value_over_maximum_size2",
                    "long_string_value_over_maximum_size3",
                ],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with a schema storage strategy
        await table.store_rows_async(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            column_expansion_strategy=ColumnExpansionStrategy.AUTO_EXPAND_CONTENT_LENGTH,
            synapse_client=self.syn,
        )

        # THEN the spy should have been called
        spy_csv_file_conversion.assert_called_once()

        # AND the schema should have been updated before the data is stored
        assert len(spy_send_job.call_args.kwargs["request"]["changes"]) == 2
        assert (
            spy_send_job.call_args.kwargs["request"]["changes"][0]["concreteType"]
            == concrete_types.TABLE_SCHEMA_CHANGE_REQUEST
        )
        assert (
            spy_send_job.call_args.kwargs["request"]["changes"][1]["concreteType"]
            == concrete_types.UPLOAD_TO_TABLE_REQUEST
        )

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )

        # AND the column should have been expanded
        assert table.columns["column_string"].maximum_size == 54

    async def test_store_rows_on_existing_table_adding_column(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # SPYs
        spy_csv_file_conversion = mocker.spy(table_module, "csv_to_pandas_df")

        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name="column_string", column_type=ColumnType.STRING)],
        )

        # AND the table exists in Synapse
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with a schema storage strategy
        await table.store_rows_async(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the spy should have been called
        spy_csv_file_conversion.assert_called_once()

        # AND the schema should not have been updated
        assert len(spy_send_job.call_args.kwargs["request"]["changes"]) == 2
        assert (
            spy_send_job.call_args.kwargs["request"]["changes"][0]["concreteType"]
            == concrete_types.TABLE_SCHEMA_CHANGE_REQUEST
        )
        assert (
            spy_send_job.call_args.kwargs["request"]["changes"][1]["concreteType"]
            == concrete_types.UPLOAD_TO_TABLE_REQUEST
        )

        # AND I can query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], data_for_table["column_key_2"]
        )

    async def test_store_rows_on_existing_table_no_schema_storage_strategy(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name="column_string", column_type=ColumnType.STRING)],
        )

        # AND the table exists in Synapse
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with no schema storage strategy
        with pytest.raises(SynapseHTTPError) as e:
            await table.store_rows_async(
                values=filepath, schema_storage_strategy=None, synapse_client=self.syn
            )

        # THEN the table data should fail to be inserted
        assert (
            "400 Client Error: The first line is expected to be a header but the values do not match the names of of the columns of the table (column_key_2 is not a valid column name or id). Header row: column_string,column_key_2"
            in str(e.value)
        )


class TestUpsertRows:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_upsert_with_updates_and_no_insertions(
        self, project_model: Project
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # AND data I want to upsert the rows to
        modified_data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [4, 5, 6]}
        )

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=modified_data_for_table,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )

        # AND no additional rows exist on the table
        assert len(results) == 3

    async def test_upsert_with_updates_and_insertions_as_csv(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data I want to upsert the rows to
        modified_data_for_table = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [4, 5, 6, 7, 8, 9],
            }
        )

        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        modified_data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=filepath,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )

        # AND 3 additional rows exist on the table
        assert len(results) == 6

        # AND the spy should have been called in single batches for update and insert operations
        assert spy_send_job.call_count == 2

    async def test_upsert_with_updates_and_insertions_as_dict(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data I want to upsert the rows to
        modified_data_for_table = {
            "column_string": [
                "value1",
                "value2",
                "value3",
                "value4",
                "value5",
                "value6",
            ],
            "column_key_2": [4, 5, 6, 7, 8, 9],
        }

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=modified_data_for_table,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"],
            pd.DataFrame(modified_data_for_table)["column_string"],
        )
        pd.testing.assert_series_equal(
            results["column_key_2"],
            pd.DataFrame(modified_data_for_table)["column_key_2"],
        )

        # AND 3 additional rows exist on the table
        assert len(results) == 6

        # AND the spy should have been called in single batches for update and insert operations
        assert spy_send_job.call_count == 2

    async def test_upsert_with_dry_run(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data I want to upsert the rows to
        modified_data_for_table = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [4, 5, 6, 7, 8, 9],
            }
        )

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=modified_data_for_table,
            primary_keys=["column_string"],
            dry_run=True,
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the table should not contain any modified data from the original state
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], data_for_table["column_key_2"]
        )

        # AND the 3 original rows should exist on the table
        assert len(results) == 3

        # AND the spy should have not been called for update/insert operations
        assert spy_send_job.call_count == 0

    async def test_upsert_with_updates_and_insertions(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data I want to upsert the rows to
        modified_data_for_table = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [4, 5, 6, 7, 8, 9],
            }
        )

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=modified_data_for_table,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )

        # AND 3 additional rows exist on the table
        assert len(results) == 6

        # AND the spy should have been called in single batches for update and insert operations
        assert spy_send_job.call_count == 2

    async def test_upsert_with_updates_and_insertions_single_row_per_interaction(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
                Column(
                    name="large_string",
                    column_type=ColumnType.STRING,
                    maximum_size=1000,
                ),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        large_string_a = "A" * 1000
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
                "column_key_2": [1, 2, 3],
                "large_string": [large_string_a, large_string_a, large_string_a],
            }
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data I want to upsert the rows to
        large_string_b = "B" * 1000
        modified_data_for_table = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [4, 5, 6, 7, 8, 9],
                "large_string": [
                    large_string_b,
                    large_string_b,
                    large_string_b,
                    large_string_b,
                    large_string_b,
                    large_string_b,
                ],
            }
        )

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=modified_data_for_table,
            primary_keys=["column_string"],
            synapse_client=self.syn,
            rows_per_request=1,
            update_size_byte=1 * utils.KB,
            insert_size_byte=1 * utils.KB,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )
        pd.testing.assert_series_equal(
            results["large_string"], modified_data_for_table["large_string"]
        )

        # AND 3 additional rows exist on the table
        assert len(results) == 6

        # AND the spy should have been called in multiple batches for update and insert operations
        assert spy_send_job.call_count == 6

    async def test_upsert_with_multi_value_key(self, project_model: Project) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
                Column(name="column_key_3", column_type=ColumnType.BOOLEAN),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
                "column_key_2": [1, 2, 3],
                "column_key_3": [True, True, True],
            }
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # AND data I want to upsert the rows to
        modified_data_for_table = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [1, 2, 3, 4, 5, 6],
                "column_key_3": [False, False, False, False, False, False],
            }
        )

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=modified_data_for_table,
            primary_keys=["column_string", "column_key_2"],
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )
        pd.testing.assert_series_equal(
            results["column_key_3"], modified_data_for_table["column_key_3"]
        )

        # AND 3 additional rows exist on the table
        assert len(results) == 6

    async def test_upsert_with_multi_value_key_none_matching(
        self, project_model: Project
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_key_2", column_type=ColumnType.INTEGER),
                Column(name="column_key_3", column_type=ColumnType.BOOLEAN),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
                "column_key_2": [1, 2, 3],
                "column_key_3": [True, True, True],
            }
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # AND data I want to upsert the rows to. In this case the keys won't match any rows and should all be inserted
        data_that_will_not_match_to_any_rows = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [7, 8, 9, 10, 11, 12],
                "column_key_3": [False, False, False, False, False, False],
            }
        )

        # WHEN I upsert rows to the table based on the first column
        await table.upsert_rows_async(
            values=data_that_will_not_match_to_any_rows,
            primary_keys=["column_string", "column_key_2"],
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"],
            pd.concat(
                [
                    data_for_table["column_string"],
                    data_that_will_not_match_to_any_rows["column_string"],
                ],
                ignore_index=True,
            ),
        )
        pd.testing.assert_series_equal(
            results["column_key_2"],
            pd.concat(
                [
                    data_for_table["column_key_2"],
                    data_that_will_not_match_to_any_rows["column_key_2"],
                ],
                ignore_index=True,
            ),
        )
        pd.testing.assert_series_equal(
            results["column_key_3"],
            pd.concat(
                [
                    data_for_table["column_key_3"],
                    data_that_will_not_match_to_any_rows["column_key_3"],
                ],
                ignore_index=True,
            ),
        )

        # AND 6 additional rows exist on the table
        assert len(results) == 9

    async def test_upsert_all_data_types_single_key(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_double", column_type=ColumnType.DOUBLE),
                Column(name="column_integer", column_type=ColumnType.INTEGER),
                Column(name="column_boolean", column_type=ColumnType.BOOLEAN),
                Column(name="column_date", column_type=ColumnType.DATE),
                Column(name="column_filehandleid", column_type=ColumnType.FILEHANDLEID),
                Column(name="column_entityid", column_type=ColumnType.ENTITYID),
                Column(name="column_submissionid", column_type=ColumnType.SUBMISSIONID),
                Column(name="column_evaluationid", column_type=ColumnType.EVALUATIONID),
                Column(name="column_link", column_type=ColumnType.LINK),
                Column(name="column_mediumtext", column_type=ColumnType.MEDIUMTEXT),
                Column(name="column_largetext", column_type=ColumnType.LARGETEXT),
                Column(name="column_userid", column_type=ColumnType.USERID),
                Column(name="column_string_LIST", column_type=ColumnType.STRING_LIST),
                Column(name="column_integer_LIST", column_type=ColumnType.INTEGER_LIST),
                Column(name="column_boolean_LIST", column_type=ColumnType.BOOLEAN_LIST),
                Column(name="column_date_LIST", column_type=ColumnType.DATE_LIST),
                Column(
                    name="column_entity_id_list", column_type=ColumnType.ENTITYID_LIST
                ),
                Column(name="column_user_id_list", column_type=ColumnType.USERID_LIST),
                Column(name="column_json", column_type=ColumnType.JSON),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND A bogus file
        path = utils.make_bogus_data_file()
        self.schedule_for_cleanup(path)
        file = await File(parent_id=project_model.id, path=path).store_async(
            synapse_client=self.syn
        )

        # AND A bogus evaluation/submission
        name = "Test Evaluation %s" % str(uuid.uuid4())
        evaluation = Evaluation(
            name=name,
            description="Evaluation for testing",
            contentSource=project_model.id,
        )
        # TODO: When Evaluation and Submission are implemented with Async methods update this test
        evaluation = self.syn.store(evaluation)
        try:
            submission = self.syn.submit(
                evaluation, file.id, name="Submission 1", submitterAlias="My Team"
            )

            # AND data for a column already stored in Synapse
            data_for_table = pd.DataFrame(
                {
                    # STRING
                    "column_string": ["value1", "value2", "value3"],
                    # DOUBLE
                    "column_double": [1.1, 2.2, 3.3],
                    # INTEGER
                    "column_integer": [1, 2, 3],
                    # BOOLEAN
                    "column_boolean": [True, True, True],
                    # DATE
                    "column_date": [
                        utils.to_unix_epoch_time("2021-01-01"),
                        utils.to_unix_epoch_time("2021-01-02"),
                        utils.to_unix_epoch_time("2021-01-03"),
                    ],
                    # # FILEHANDLEID
                    "column_filehandleid": [
                        file.file_handle.id,
                        file.file_handle.id,
                        file.file_handle.id,
                    ],
                    # # ENTITYID
                    "column_entityid": [file.id, file.id, file.id],
                    # SUBMISSIONID
                    "column_submissionid": [
                        submission.id,
                        submission.id,
                        submission.id,
                    ],
                    # EVALUATIONID
                    "column_evaluationid": [
                        evaluation.id,
                        evaluation.id,
                        evaluation.id,
                    ],
                    # LINK
                    "column_link": [
                        "https://www.synapse.org/Profile:",
                        "https://www.synapse.org/Profile:",
                        "https://www.synapse.org/Profile:",
                    ],
                    # MEDIUMTEXT
                    "column_mediumtext": ["value1", "value2", "value3"],
                    # LARGETEXT
                    "column_largetext": ["value1", "value2", "value3"],
                    # USERID
                    "column_userid": [
                        self.syn.credentials.owner_id,
                        self.syn.credentials.owner_id,
                        self.syn.credentials.owner_id,
                    ],
                    # STRING_LIST
                    "column_string_LIST": [
                        ["value1", "value2"],
                        ["value3", "value4"],
                        ["value5", "value6"],
                    ],
                    # INTEGER_LIST
                    "column_integer_LIST": [[1, 2], [3, 4], [5, 6]],
                    # BOOLEAN_LIST
                    "column_boolean_LIST": [
                        [True, False],
                        [True, False],
                        [True, False],
                    ],
                    # DATE_LIST
                    "column_date_LIST": [
                        [
                            utils.to_unix_epoch_time("2021-01-01"),
                            utils.to_unix_epoch_time("2021-01-02"),
                        ],
                        [
                            utils.to_unix_epoch_time("2021-01-03"),
                            utils.to_unix_epoch_time("2021-01-04"),
                        ],
                        [
                            utils.to_unix_epoch_time("2021-01-05"),
                            utils.to_unix_epoch_time("2021-01-06"),
                        ],
                    ],
                    # ENTITYID_LIST
                    "column_entity_id_list": [
                        [file.id, file.id],
                        [file.id, file.id],
                        [file.id, file.id],
                    ],
                    # USERID_LIST
                    "column_user_id_list": [
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                    ],
                    # JSON
                    "column_json": [
                        {"key1": "value1", "key2": 2},
                        {"key3": "value3", "key4": 4},
                        {"key5": "value5", "key6": 6},
                    ],
                }
            )

            await table.store_rows_async(
                values=data_for_table,
                schema_storage_strategy=None,
                synapse_client=self.syn,
            )

            # AND A second bogus file to update the first one
            path = utils.make_bogus_data_file()
            self.schedule_for_cleanup(path)
            file = await File(parent_id=project_model.id, path=path).store_async(
                synapse_client=self.syn
            )

            # AND data I want to upsert the rows to
            modified_data_for_table = pd.DataFrame(
                {
                    # STRING
                    "column_string": ["value1", "value2", "value3"],
                    # DOUBLE
                    "column_double": [11.2, 22.3, 33.4],
                    # INTEGER
                    "column_integer": [11, 22, 33],
                    # BOOLEAN
                    "column_boolean": [False, False, False],
                    # DATE
                    "column_date": [
                        utils.to_unix_epoch_time("2022-01-01"),
                        utils.to_unix_epoch_time("2022-01-02"),
                        utils.to_unix_epoch_time("2022-01-03"),
                    ],
                    # # FILEHANDLEID
                    "column_filehandleid": [
                        int(file.file_handle.id),
                        int(file.file_handle.id),
                        int(file.file_handle.id),
                    ],
                    # # ENTITYID
                    "column_entityid": [file.id, file.id, file.id],
                    # Not testing the update for these 2 columns due to the cleanup overhead
                    # SUBMISSIONID
                    "column_submissionid": [
                        int(submission.id),
                        int(submission.id),
                        int(submission.id),
                    ],
                    # EVALUATIONID
                    "column_evaluationid": [
                        int(evaluation.id),
                        int(evaluation.id),
                        int(evaluation.id),
                    ],
                    # LINK
                    "column_link": [
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                    ],
                    # MEDIUMTEXT
                    "column_mediumtext": ["value11", "value22", "value33"],
                    # LARGETEXT
                    "column_largetext": ["value11", "value22", "value33"],
                    # USERID
                    "column_userid": [
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                    ],
                    # STRING_LIST
                    "column_string_LIST": [
                        ["value11", "value22"],
                        ["value33", "value44"],
                        ["value55", "value76"],
                    ],
                    # INTEGER_LIST
                    "column_integer_LIST": [[11, 22], [33, 44], [55, 66]],
                    # BOOLEAN_LIST
                    "column_boolean_LIST": [
                        [False, True],
                        [False, True],
                        [False, True],
                    ],
                    # DATE_LIST
                    "column_date_LIST": [
                        [
                            utils.to_unix_epoch_time("2022-01-01"),
                            utils.to_unix_epoch_time("2022-01-02"),
                        ],
                        [
                            utils.to_unix_epoch_time("2022-01-03"),
                            utils.to_unix_epoch_time("2022-01-04"),
                        ],
                        [
                            utils.to_unix_epoch_time("2022-01-05"),
                            utils.to_unix_epoch_time("2022-01-06"),
                        ],
                    ],
                    # ENTITYID_LIST
                    "column_entity_id_list": [
                        [file.id, file.id],
                        [file.id, file.id],
                        [file.id, file.id],
                    ],
                    # USERID_LIST
                    "column_user_id_list": [
                        [
                            int(self.syn.credentials.owner_id),
                            int(self.syn.credentials.owner_id),
                        ],
                        [
                            int(self.syn.credentials.owner_id),
                            int(self.syn.credentials.owner_id),
                        ],
                        [
                            int(self.syn.credentials.owner_id),
                            int(self.syn.credentials.owner_id),
                        ],
                    ],
                    # JSON
                    "column_json": [
                        json.dumps({"key11": "value11", "key22": 22}),
                        json.dumps({"key33": "value33", "key44": 44}),
                        json.dumps({"key55": "value55", "key66": 66}),
                    ],
                }
            )

            # WHEN I upsert rows to the table based on the first column
            await table.upsert_rows_async(
                values=modified_data_for_table,
                primary_keys=["column_string"],
                synapse_client=self.syn,
            )

            # AND I query the table
            results = await query_async(
                f"SELECT * FROM {table.id}",
                synapse_client=self.syn,
                include_row_id_and_row_version=False,
            )

            # THEN the data in the columns should match
            original_as_string = modified_data_for_table.to_json()
            modified_as_string = results.to_json()
            assert original_as_string == modified_as_string

            # AND the cells I expect to have been updated should have been updated
        finally:
            self.syn.delete(evaluation)

    async def test_upsert_all_data_types_multi_key(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_double", column_type=ColumnType.DOUBLE),
                Column(name="column_integer", column_type=ColumnType.INTEGER),
                Column(name="column_boolean", column_type=ColumnType.BOOLEAN),
                Column(name="column_date", column_type=ColumnType.DATE),
                Column(name="column_filehandleid", column_type=ColumnType.FILEHANDLEID),
                Column(name="column_entityid", column_type=ColumnType.ENTITYID),
                Column(name="column_submissionid", column_type=ColumnType.SUBMISSIONID),
                Column(name="column_evaluationid", column_type=ColumnType.EVALUATIONID),
                Column(name="column_link", column_type=ColumnType.LINK),
                Column(name="column_mediumtext", column_type=ColumnType.MEDIUMTEXT),
                Column(name="column_largetext", column_type=ColumnType.LARGETEXT),
                Column(name="column_userid", column_type=ColumnType.USERID),
                Column(name="column_string_LIST", column_type=ColumnType.STRING_LIST),
                Column(name="column_integer_LIST", column_type=ColumnType.INTEGER_LIST),
                Column(name="column_boolean_LIST", column_type=ColumnType.BOOLEAN_LIST),
                Column(name="column_date_LIST", column_type=ColumnType.DATE_LIST),
                Column(
                    name="column_entity_id_list", column_type=ColumnType.ENTITYID_LIST
                ),
                Column(name="column_user_id_list", column_type=ColumnType.USERID_LIST),
                Column(name="column_json", column_type=ColumnType.JSON),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND A bogus file
        path = utils.make_bogus_data_file()
        self.schedule_for_cleanup(path)
        file = await File(parent_id=project_model.id, path=path).store_async(
            synapse_client=self.syn
        )

        # AND A bogus evaluation/submission
        name = "Test Evaluation %s" % str(uuid.uuid4())
        evaluation = Evaluation(
            name=name,
            description="Evaluation for testing",
            contentSource=project_model.id,
        )
        # TODO: When Evaluation and Submission are implemented with Async methods update this test
        evaluation = self.syn.store(evaluation)
        try:
            submission = self.syn.submit(
                evaluation, file.id, name="Submission 1", submitterAlias="My Team"
            )

            # AND data for a column already stored in Synapse
            data_for_table = pd.DataFrame(
                {
                    # STRING
                    "column_string": ["value1", "value2", "value3"],
                    # DOUBLE
                    "column_double": [1.1, 2.2, 3.3],
                    # INTEGER
                    "column_integer": [1, 2, 3],
                    # BOOLEAN
                    "column_boolean": [True, True, True],
                    # DATE
                    "column_date": [
                        utils.to_unix_epoch_time("2021-01-01"),
                        utils.to_unix_epoch_time("2021-01-02"),
                        utils.to_unix_epoch_time("2021-01-03"),
                    ],
                    # FILEHANDLEID
                    "column_filehandleid": [
                        file.file_handle.id,
                        file.file_handle.id,
                        file.file_handle.id,
                    ],
                    # # ENTITYID
                    "column_entityid": [file.id, file.id, file.id],
                    # SUBMISSIONID
                    "column_submissionid": [
                        submission.id,
                        submission.id,
                        submission.id,
                    ],
                    # EVALUATIONID
                    "column_evaluationid": [
                        evaluation.id,
                        evaluation.id,
                        evaluation.id,
                    ],
                    # LINK
                    "column_link": [
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                    ],
                    # MEDIUMTEXT
                    "column_mediumtext": ["value1", "value2", "value3"],
                    # LARGETEXT
                    "column_largetext": ["value1", "value2", "value3"],
                    # USERID
                    "column_userid": [
                        self.syn.credentials.owner_id,
                        self.syn.credentials.owner_id,
                        self.syn.credentials.owner_id,
                    ],
                    # STRING_LIST
                    "column_string_LIST": [
                        ["value1", "value2"],
                        ["value3", "value4"],
                        ["value5", "value6"],
                    ],
                    # INTEGER_LIST
                    "column_integer_LIST": [[1, 2], [3, 4], [5, 6]],
                    # BOOLEAN_LIST
                    "column_boolean_LIST": [
                        [True, False],
                        [True, False],
                        [True, False],
                    ],
                    # DATE_LIST
                    "column_date_LIST": [
                        [
                            utils.to_unix_epoch_time("2021-01-01"),
                            utils.to_unix_epoch_time("2021-01-02"),
                        ],
                        [
                            utils.to_unix_epoch_time("2021-01-03"),
                            utils.to_unix_epoch_time("2021-01-04"),
                        ],
                        [
                            utils.to_unix_epoch_time("2021-01-05"),
                            utils.to_unix_epoch_time("2021-01-06"),
                        ],
                    ],
                    # ENTITYID_LIST
                    "column_entity_id_list": [
                        [file.id, file.id],
                        [file.id, file.id],
                        [file.id, file.id],
                    ],
                    # USERID_LIST
                    "column_user_id_list": [
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                    ],
                    # JSON
                    "column_json": [
                        {"key1": "value1", "key2": 2},
                        {"key3": "value3", "key4": 4},
                        {"key5": "value5", "key6": 6},
                    ],
                }
            )

            await table.store_rows_async(
                values=data_for_table,
                schema_storage_strategy=None,
                synapse_client=self.syn,
            )

            # AND data I want to upsert the rows to
            modified_data_for_table = pd.DataFrame(
                {
                    # STRING
                    "column_string": ["this", "is", "updated"],
                    # DOUBLE
                    "column_double": [1.1, 2.2, 3.3],
                    # INTEGER
                    "column_integer": [1, 2, 3],
                    # BOOLEAN
                    "column_boolean": [True, True, True],
                    # DATE
                    "column_date": [
                        utils.to_unix_epoch_time("2021-01-01"),
                        utils.to_unix_epoch_time("2021-01-02"),
                        utils.to_unix_epoch_time("2021-01-03"),
                    ],
                    # FILEHANDLEID
                    "column_filehandleid": [
                        int(file.file_handle.id),
                        int(file.file_handle.id),
                        int(file.file_handle.id),
                    ],
                    # # ENTITYID
                    "column_entityid": [file.id, file.id, file.id],
                    # SUBMISSIONID
                    "column_submissionid": [
                        int(submission.id),
                        int(submission.id),
                        int(submission.id),
                    ],
                    # EVALUATIONID
                    "column_evaluationid": [
                        int(evaluation.id),
                        int(evaluation.id),
                        int(evaluation.id),
                    ],
                    # LINK
                    "column_link": [
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                    ],
                    # MEDIUMTEXT
                    "column_mediumtext": ["value1", "value2", "value3"],
                    # LARGETEXT
                    "column_largetext": ["value1", "value2", "value3"],
                    # USERID
                    "column_userid": [
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                    ],
                    # STRING_LIST
                    "column_string_LIST": [
                        ["value1", "value2"],
                        ["value3", "value4"],
                        ["value5", "value6"],
                    ],
                    # INTEGER_LIST
                    "column_integer_LIST": [[1, 2], [3, 4], [5, 6]],
                    # BOOLEAN_LIST
                    "column_boolean_LIST": [
                        [True, False],
                        [True, False],
                        [True, False],
                    ],
                    # DATE_LIST
                    "column_date_LIST": [
                        [
                            utils.to_unix_epoch_time("2021-01-01"),
                            utils.to_unix_epoch_time("2021-01-02"),
                        ],
                        [
                            utils.to_unix_epoch_time("2021-01-03"),
                            utils.to_unix_epoch_time("2021-01-04"),
                        ],
                        [
                            utils.to_unix_epoch_time("2021-01-05"),
                            utils.to_unix_epoch_time("2021-01-06"),
                        ],
                    ],
                    # ENTITYID_LIST
                    "column_entity_id_list": [
                        [file.id, file.id],
                        [file.id, file.id],
                        [file.id, file.id],
                    ],
                    # USERID_LIST
                    "column_user_id_list": [
                        [
                            int(self.syn.credentials.owner_id),
                            int(self.syn.credentials.owner_id),
                        ],
                        [
                            int(self.syn.credentials.owner_id),
                            int(self.syn.credentials.owner_id),
                        ],
                        [
                            int(self.syn.credentials.owner_id),
                            int(self.syn.credentials.owner_id),
                        ],
                    ],
                    # JSON
                    "column_json": [
                        json.dumps({"key1": "value1", "key2": 2}),
                        json.dumps({"key3": "value3", "key4": 4}),
                        json.dumps({"key5": "value5", "key6": 6}),
                    ],
                }
            )

            # WHEN I upsert rows to the table based on all columns except the first one and list based columns
            primary_keys = [
                "column_double",
                "column_integer",
                "column_boolean",
                "column_date",
                "column_filehandleid",
                "column_entityid",
                "column_submissionid",
                "column_evaluationid",
                "column_link",
                "column_mediumtext",
                "column_largetext",
                "column_userid",
            ]
            await table.upsert_rows_async(
                values=modified_data_for_table,
                primary_keys=primary_keys,
                synapse_client=self.syn,
            )

            # AND I query the table
            results = await query_async(
                f"SELECT * FROM {table.id}",
                synapse_client=self.syn,
                include_row_id_and_row_version=False,
            )

            # THEN the data in the columns should match
            original_as_string = modified_data_for_table.to_json()
            modified_as_string = results.to_json()
            assert original_as_string == modified_as_string

            # AND the cells I expect to have been updated should have been updated
        finally:
            self.syn.delete(evaluation)


class TestDeleteRows:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_delete_single_row(self, project_model: Project) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name="column_string", column_type=ColumnType.STRING)],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I delete a single row from the table
        await table.delete_rows_async(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id} WHERE column_string = 'value2'",
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"],
            pd.DataFrame({"column_string": ["value1", "value3"]})["column_string"],
        )

        # AND only 2 rows should exist on the table
        assert len(results) == 2

    async def test_delete_multiple_rows(self, project_model: Project) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name="column_string", column_type=ColumnType.STRING)],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I delete a single row from the table
        await table.delete_rows_async(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id} WHERE column_string IN ('value2','value3')",
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"],
            pd.DataFrame({"column_string": ["value1"]})["column_string"],
        )

        # AND only 1 row should exist on the table
        assert len(results) == 1

    async def test_delete_no_rows(self, project_model: Project) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name="column_string", column_type=ColumnType.STRING)],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I delete a single row from the table
        await table.delete_rows_async(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id} WHERE column_string = 'foo'",
            synapse_client=self.syn,
        )

        # AND I query the table
        results = await query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )

        # AND 3 rows should exist on the table
        assert len(results) == 3


class TestColumnModifications:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_column_rename(self, project_model: Project) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        old_column_name = "column_string"
        old_table_instance = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[Column(name=old_column_name, column_type=ColumnType.STRING)],
        )
        old_table_instance = await old_table_instance.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_table_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_table_instance.columns[old_column_name].name = new_column_name

        # AND I store the table
        await old_table_instance.store_async(synapse_client=self.syn)

        # THEN the column name should be updated on the existing table instance
        assert old_table_instance.columns[new_column_name] is not None
        assert old_column_name not in old_table_instance.columns

        # AND the new column name should be reflected in the Synapse table
        new_table_instance = await Table(id=old_table_instance.id).get_async(
            synapse_client=self.syn
        )
        assert new_table_instance.columns[new_column_name] is not None
        assert old_column_name not in new_table_instance.columns

    async def test_delete_column(self, project_model: Project) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        old_column_name = "column_string"
        column_to_keep = "column_to_keep"
        old_table_instance = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
        )
        old_table_instance = await old_table_instance.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_table_instance.id)

        # WHEN I delete the column
        old_table_instance.delete_column(name=old_column_name)

        # AND I store the table
        await old_table_instance.store_async(synapse_client=self.syn)

        # THEN the column should be removed from the table instance
        assert old_column_name not in old_table_instance.columns

        # AND the column to keep should still be in the table instance
        assert column_to_keep in old_table_instance.columns
        assert len(old_table_instance.columns.values()) == 1

        # AND the column should be removed from the Synapse table
        new_table_instance = await Table(id=old_table_instance.id).get_async(
            synapse_client=self.syn
        )
        assert old_column_name not in new_table_instance.columns

        # AND the column to keep should still be in the Synapse table
        assert column_to_keep in new_table_instance.columns
        assert len(new_table_instance.columns.values()) == 1


class TestQuerying:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_part_mask_query_everything(self, project_model: Project) -> None:
        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for the table stored in synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_column": [1, 2, 3, None],
                "float_column": [1.1, 2.2, 3.3, None],
            }
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I query the table with a part mask
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZE_BYTES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZE_BYTES | LAST_UPDATED_ON

        results = await query_part_mask_async(
            query=f"SELECT * FROM {table.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results.result["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results.result["integer_column"], data_for_table["integer_column"]
        )
        pd.testing.assert_series_equal(
            results.result["float_column"], data_for_table["float_column"]
        )

        # AND the part mask should be reflected in the results
        assert results.count == 4
        assert results.sum_file_sizes is not None
        assert results.sum_file_sizes.greater_than is not None
        assert results.sum_file_sizes.sum_file_size_bytes is not None
        assert results.last_updated_on is not None

    async def test_part_mask_query_results_only(self, project_model: Project) -> None:
        # GIVEN a table with a column defined
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for the table stored in synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_column": [1, 2, 3, None],
                "float_column": [1.1, 2.2, 3.3, None],
            }
        )
        await table.store_rows_async(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I query the table with a part mask
        QUERY_RESULTS = 0x1
        results = await query_part_mask_async(
            query=f"SELECT * FROM {table.id}",
            synapse_client=self.syn,
            part_mask=QUERY_RESULTS,
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results.result["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results.result["integer_column"], data_for_table["integer_column"]
        )
        pd.testing.assert_series_equal(
            results.result["float_column"], data_for_table["float_column"]
        )

        # AND the part mask should be reflected in the results
        assert results.count is None
        assert results.sum_file_sizes is None
        assert results.last_updated_on is None
