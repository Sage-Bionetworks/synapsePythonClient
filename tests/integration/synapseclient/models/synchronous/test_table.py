import json
import os
import random
import string
import tempfile
import uuid
from typing import Callable
from unittest import skip

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import synapseclient.models.mixins.asynchronous_job as asynchronous_job_module
import synapseclient.models.mixins.table_components as table_module
from synapseclient import Evaluation, Synapse
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Activity,
    Column,
    ColumnExpansionStrategy,
    ColumnType,
    File,
    Project,
    SchemaStorageStrategy,
    Table,
    query,
    query_part_mask,
)


class TestTableCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_table_with_different_column_configurations(
        self, project_model: Project
    ) -> None:
        """Test creating tables with different column configurations."""
        # Test 1: Table with no columns
        # GIVEN a table with no columns
        table_name = str(uuid.uuid4())
        table_description = "Test table with no columns"
        table = Table(
            name=table_name, parent_id=project_model.id, description=table_description
        )

        # WHEN I store the table
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = Table(id=table.id).get(synapse_client=self.syn)
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.description == table_description

        # Test 2: Table with a single column
        # GIVEN a table with a single column
        table_name = str(uuid.uuid4())
        table_description = "Test table with single column"
        table_single_column = Table(
            name=table_name,
            parent_id=project_model.id,
            description=table_description,
            columns=[Column(name="test_column", column_type=ColumnType.STRING)],
        )

        # WHEN I store the table
        table_single_column = table_single_column.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table_single_column.id)

        # THEN the table should be created
        assert table_single_column.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = Table(id=table_single_column.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_table_instance.name == table_name
        assert new_table_instance.columns["test_column"].name == "test_column"
        assert (
            new_table_instance.columns["test_column"].column_type == ColumnType.STRING
        )

        # Test 3: Table with multiple columns
        # GIVEN a table with multiple columns
        table_name = str(uuid.uuid4())
        table_multi_columns = Table(
            name=table_name,
            parent_id=project_model.id,
            description="Test table with multiple columns",
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )

        # WHEN I store the table
        table_multi_columns = table_multi_columns.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table_multi_columns.id)

        # THEN the table should be created and columns are correct
        new_table_instance = Table(id=table_multi_columns.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert (
            new_table_instance.columns["test_column"].column_type == ColumnType.STRING
        )
        assert (
            new_table_instance.columns["test_column2"].column_type == ColumnType.INTEGER
        )

    async def test_create_table_with_many_column_types(
        self, project_model: Project
    ) -> None:
        """Test creating a table with many column types with different allowed characters."""
        # GIVEN a table with many columns with various naming patterns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            description="Test table with various column names",
            columns=[
                Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                Column(name="col 2", column_type=ColumnType.STRING, id="id2"),
                Column(name="col_3", column_type=ColumnType.STRING, id="id3"),
                Column(name="col-4", column_type=ColumnType.STRING, id="id4"),
                Column(name="col.5", column_type=ColumnType.STRING, id="id5"),
                Column(name="col+6", column_type=ColumnType.STRING, id="id6"),
                Column(name="col'7", column_type=ColumnType.STRING, id="id7"),
                Column(name="col(8)", column_type=ColumnType.STRING, id="id8"),
            ],
        )

        # WHEN I store the table
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be created with all columns
        new_table_instance = Table(id=table.id).get(
            synapse_client=self.syn, include_columns=True
        )

        # Verify all column names and types
        column_names = [
            "col1",
            "col 2",
            "col_3",
            "col-4",
            "col.5",
            "col+6",
            "col'7",
            "col(8)",
        ]
        for name in column_names:
            assert name in new_table_instance.columns
            assert new_table_instance.columns[name].column_type == ColumnType.STRING

    async def test_create_table_with_invalid_column(
        self, project_model: Project
    ) -> None:
        """Test creating a table with an invalid column configuration."""
        # GIVEN a table with an invalid column (maximum_size too large)
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            description="Test table with invalid column",
            columns=[
                Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    maximum_size=999999999,
                )
            ],
        )

        # WHEN I store the table, THEN it should fail with appropriate error
        with pytest.raises(SynapseHTTPError) as e:
            table.store(synapse_client=self.syn)

        # Verify error message
        assert (
            "400 Client Error: ColumnModel.maxSize for a STRING cannot exceed:"
            in str(e.value)
        )

    async def test_table_creation_with_data_sources(
        self, project_model: Project
    ) -> None:
        """Test creating tables with different data sources."""
        # Test with dictionary data
        # GIVEN a table with no columns defined and dictionary data
        table_name = str(uuid.uuid4())
        table_dict = Table(name=table_name, parent_id=project_model.id)
        dict_data = {
            "column_string": ["value1", "value2", "value3"],
        }

        # WHEN I store the table and then add data
        table_dict = table_dict.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table_dict.id)
        table_dict.store_rows(
            values=dict_data,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should have proper schema and data
        results = query(f"SELECT * FROM {table_dict.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], pd.DataFrame(dict_data)["column_string"]
        )

        # Test with DataFrame data
        # GIVEN a table with no columns defined and pandas DataFrame data
        table_name = str(uuid.uuid4())
        table_df = Table(name=table_name, parent_id=project_model.id)
        df_data = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})

        # WHEN I store the table and then add data
        table_df = table_df.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table_df.id)
        table_df.store_rows(
            values=df_data,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should have proper schema and data
        results = query(f"SELECT * FROM {table_df.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], df_data["column_string"]
        )

        # Test with CSV file data
        # GIVEN a table with no columns defined and CSV file data
        table_name = str(uuid.uuid4())
        table_csv = Table(name=table_name, parent_id=project_model.id)
        csv_data = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        csv_data.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table and add data from CSV
        table_csv = table_csv.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table_csv.id)
        table_csv.store_rows(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should have proper schema and data
        results = query(f"SELECT * FROM {table_csv.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], csv_data["column_string"]
        )

    async def test_create_table_with_string_column(
        self, project_model: Project
    ) -> None:
        """Test creating tables with string column configurations."""
        # GIVEN a table with columns
        table_name = str(uuid.uuid4())
        table_description = "Test table with columns"
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            description=table_description,
            columns=[
                Column(name="test_column", column_type="STRING"),
                Column(name="test_column2", column_type="INTEGER"),
            ],
        )

        # WHEN I store the table
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be created
        assert table.id is not None

        # AND I can retrieve that table from Synapse
        new_table_instance = Table(id=table.id).get(synapse_client=self.syn)
        assert new_table_instance is not None
        assert new_table_instance.name == table_name
        assert new_table_instance.id == table.id
        assert new_table_instance.description == table_description
        assert len(new_table_instance.columns) == 2
        assert new_table_instance.columns["test_column"].name == "test_column"
        assert (
            new_table_instance.columns["test_column"].column_type == ColumnType.STRING
        )
        assert new_table_instance.columns["test_column2"].name == "test_column2"
        assert (
            new_table_instance.columns["test_column2"].column_type == ColumnType.INTEGER
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
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table with INFER_FROM_DATA schema storage strategy
        table.store_rows(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the table should be created
        assert table.id is not None

        # AND the spy should have been called
        spy_csv_file_conversion.assert_called_once()

        # AND I can query the table
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # AND the table is stored in Synapse
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND Rows are stored into Synapse with the INFER_FROM_DATA schema storage strategy
        table.store_rows(
            values=filepath,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )
        assert table.id is not None

        # AND a query of the data
        query_results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

        # WHEN I update the rows with new data
        query_results.loc[
            query_results["column_string"] == "value1", "column_string"
        ] = "value11"
        query_results.loc[
            query_results["column_string"] == "value3", "column_string"
        ] = "value33"

        # AND I store the rows back to Synapse
        Table(id=table.id).store_rows(
            values=query_results,
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )

        # THEN the data should be stored in Synapse, and match the updated data
        updated_results_from_table = query(
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
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN I store rows to the table with no schema storage strategy
        with pytest.raises(SynapseHTTPError) as e:
            table.store_rows(
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
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the table
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I store rows to the table with no schema storage strategy
        table.store_rows(
            values=filepath, schema_storage_strategy=None, synapse_client=self.syn
        )

        # THEN the table should be created
        assert table.id is not None

        # AND the spy should not have been called
        spy_csv_file_conversion.assert_not_called()

        # AND I can query the table
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with a schema storage strategy
        table.store_rows(
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
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        table = table.store(synapse_client=self.syn)
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
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with a schema storage strategy
        table.store_rows(
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
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with a schema storage strategy
        table.store_rows(
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
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column stored to CSV
        data_for_table = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store rows to the table with no schema storage strategy
        with pytest.raises(SynapseHTTPError) as e:
            table.store_rows(
                values=filepath, schema_storage_strategy=None, synapse_client=self.syn
            )

        # THEN the table data should fail to be inserted
        assert (
            "400 Client Error: The first line is expected to be a header but the values do not match the names of of the columns of the table (column_key_2 is not a valid column name or id). Header row: column_string,column_key_2"
            in str(e.value)
        )

    async def test_store_rows_as_csv_being_split_and_uploaded(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_to_order_on", column_type=ColumnType.INTEGER),
                Column(
                    name="large_string",
                    column_type=ColumnType.STRING,
                    maximum_size=5,
                ),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data that will be split into multiple parts
        large_string_a = "A" * 5
        data_for_table = pd.DataFrame(
            {
                "column_string": [f"value{i}" for i in range(200)],
                "column_to_order_on": [i for i in range(200)],
                "large_string": [large_string_a for _ in range(200)],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        data_for_table.to_csv(filepath, index=False, float_format="%.12g")

        # WHEN I store the rows to the table
        table.store_rows(
            values=filepath,
            schema_storage_strategy=None,
            synapse_client=self.syn,
            insert_size_bytes=1 * utils.KB,
        )

        # AND I query the table
        results = query(
            f"SELECT * FROM {table.id} ORDER BY column_to_order_on ASC",
            synapse_client=self.syn,
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_to_order_on"], data_for_table["column_to_order_on"]
        )
        pd.testing.assert_series_equal(
            results["large_string"], data_for_table["large_string"]
        )

        # AND 200 rows exist on the table
        assert len(results) == 200

        # AND The spy should have been called in multiple batches
        assert spy_send_job.call_count == 4

    async def test_store_rows_as_df_being_split_and_uploaded(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_to_order_on", column_type=ColumnType.INTEGER),
                Column(
                    name="large_string",
                    column_type=ColumnType.STRING,
                    maximum_size=5,
                ),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data that will be split into multiple parts
        large_string_a = "A" * 5
        data_for_table = pd.DataFrame(
            {
                "column_string": [f"value{i}" for i in range(200)],
                "column_to_order_on": [i for i in range(200)],
                "large_string": [large_string_a for _ in range(200)],
            }
        )

        # WHEN I store the rows to the table
        table.store_rows(
            values=data_for_table,
            schema_storage_strategy=None,
            synapse_client=self.syn,
            insert_size_bytes=1 * utils.KB,
        )

        # AND I query the table
        results = query(
            f"SELECT * FROM {table.id} ORDER BY column_to_order_on ASC",
            synapse_client=self.syn,
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_to_order_on"], data_for_table["column_to_order_on"]
        )
        pd.testing.assert_series_equal(
            results["large_string"], data_for_table["large_string"]
        )

        # AND 200 rows exist on the table
        assert len(results) == 200

        # AND The spy should have been called in multiple batches
        # Note: DataFrames have a minimum of 100 rows per batch
        assert spy_send_job.call_count == 2

    @skip("Skip in normal testing because the large size makes it slow")
    async def test_store_rows_as_large_df_being_split_and_uploaded(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        # GIVEN a table in Synapse
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="column_to_order_on", column_type=ColumnType.INTEGER),
                Column(
                    name="large_string",
                    column_type=ColumnType.LARGETEXT,
                ),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # AND data that will be split into multiple parts
        rows_in_table = 20
        random_string = "".join(random.choices(string.ascii_uppercase, k=500000))
        data_for_table = pd.DataFrame(
            {
                "column_string": [f"value{i}" for i in range(rows_in_table)],
                "column_to_order_on": [i for i in range(rows_in_table)],
                "large_string": [random_string for _ in range(rows_in_table)],
            }
        )

        # WHEN I store the rows to the table
        table.store_rows(
            values=data_for_table,
            schema_storage_strategy=None,
            synapse_client=self.syn,
            insert_size_bytes=1 * utils.KB,
        )

        # AND I query the table
        results = query(
            f"SELECT * FROM {table.id} ORDER BY column_to_order_on ASC",
            synapse_client=self.syn,
        )

        # THEN the data in the columns should match
        pd.testing.assert_series_equal(
            results["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_to_order_on"], data_for_table["column_to_order_on"]
        )
        pd.testing.assert_series_equal(
            results["large_string"], data_for_table["large_string"]
        )

        # AND `rows_in_table` rows exist on the table
        assert len(results) == rows_in_table

        # AND The spy should have been called in multiple batches
        assert spy_send_job.call_count == 1


class TestUpsertRows:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_upsert_operations_with_various_data_sources(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        """Test various upsert operations with different data sources and options."""
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        initial_data = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [1, 2, 3]}
        )
        table.store_rows(
            values=initial_data, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # Test 1: Basic update with no insertions
        # WHEN I upsert rows with modified values but no new rows
        updated_data = pd.DataFrame(
            {"column_string": ["value1", "value2", "value3"], "column_key_2": [4, 5, 6]}
        )
        table.upsert_rows(
            values=updated_data,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # THEN the values should be updated with no new rows
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], updated_data["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], updated_data["column_key_2"]
        )
        assert len(results) == 3

        # Test 2: Upsert with updates and insertions from CSV
        # WHEN I upsert rows with modified values and new rows from CSV
        updated_and_new_data = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                    "value4",
                    "value5",
                    "value6",
                ],
                "column_key_2": [10, 11, 12, 13, 14, 15],
            }
        )
        filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
        self.schedule_for_cleanup(filepath)
        updated_and_new_data.to_csv(filepath, index=False, float_format="%.12g")

        table.upsert_rows(
            values=filepath,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # THEN the values should be updated and new rows added
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], updated_and_new_data["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], updated_and_new_data["column_key_2"]
        )
        assert len(results) == 6  # 3 original + 3 new

        # Test 3: Upsert with dictionary data source
        # WHEN I upsert rows with dictionary data
        dict_data = {
            "column_string": [
                "value1",
                "value2",
                "value3",
                "value7",
                "value8",
                "value9",
            ],
            "column_key_2": [20, 21, 22, 23, 24, 25],
        }

        # Reset the spy to count just this operation
        spy_send_job.reset_mock()

        table.upsert_rows(
            values=dict_data,
            primary_keys=["column_string"],
            synapse_client=self.syn,
        )

        # THEN the values should be updated and new rows added
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        # We should have 9 total rows now (6 from before + 3 new)
        assert len(results) == 9
        # The spy should have been called for update and insert operations
        assert spy_send_job.call_count == 2

        # Test 4: Dry run operation
        # WHEN I perform a dry run upsert
        dry_run_data = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
                "column_key_2": [99, 99, 99],
            }
        )

        # Reset the spy to count just this operation
        spy_send_job.reset_mock()

        table.upsert_rows(
            values=dry_run_data,
            primary_keys=["column_string"],
            dry_run=True,
            synapse_client=self.syn,
        )

        # THEN no changes should be applied
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        # Should still have 9 rows
        assert len(results) == 9
        # The values from the previous update should still be in place
        assert 99 not in results["column_key_2"].values
        # The spy should not have been called
        assert spy_send_job.call_count == 0

    async def test_upsert_with_multi_value_key(self, project_model: Project) -> None:
        """Test upserting rows using multiple columns as the primary key."""
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3"],
                "column_key_2": [1, 2, 3],
                "column_key_3": [True, True, True],
            }
        )
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I upsert rows with matching keys
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
        table.upsert_rows(
            values=modified_data_for_table,
            primary_keys=["column_string", "column_key_2"],
            synapse_client=self.syn,
        )

        # THEN matching rows should be updated and new rows added
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )
        pd.testing.assert_series_equal(
            results["column_key_3"], modified_data_for_table["column_key_3"]
        )
        assert len(results) == 6  # 3 updated + 3 new

        # WHEN I upsert rows with non-matching keys
        data_for_insertion_only = pd.DataFrame(
            {
                "column_string": [
                    "value1",
                    "value2",
                    "value3",
                ],
                "column_key_2": [7, 8, 9],  # Different key values
                "column_key_3": [True, True, True],
            }
        )
        table.upsert_rows(
            values=data_for_insertion_only,
            primary_keys=["column_string", "column_key_2"],
            synapse_client=self.syn,
        )

        # THEN all rows should be inserted (no updates)
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        # Should have 9 rows now (6 from before + 3 new)
        assert len(results) == 9

    async def test_upsert_with_large_data_and_batching(
        self, project_model: Project, mocker: MockerFixture
    ) -> None:
        """Test upserting with large data strings that require batching."""
        # GIVEN a table in Synapse with a large string column
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
        table = table.store(synapse_client=self.syn)
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
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )
        spy_send_job = mocker.spy(asynchronous_job_module, "send_job_async")

        # WHEN I upsert rows with large data and control batch size
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

        table.upsert_rows(
            values=modified_data_for_table,
            primary_keys=["column_string"],
            synapse_client=self.syn,
            rows_per_request=1,
            update_size_bytes=1 * utils.KB,
            insert_size_bytes=1 * utils.KB,
        )

        # THEN all rows should be updated or inserted correctly
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        pd.testing.assert_series_equal(
            results["column_string"], modified_data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            results["column_key_2"], modified_data_for_table["column_key_2"]
        )
        pd.testing.assert_series_equal(
            results["large_string"], modified_data_for_table["large_string"]
        )
        assert len(results) == 6

        # AND multiple batch jobs should have been created due to batching settings
        assert spy_send_job.call_count == 5  # More batches due to small size settings

    async def test_upsert_all_data_types(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        """Test upserting all supported data types to ensure type compatibility."""
        # GIVEN a table in Synapse with all data types
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Set up test resources
        path = utils.make_bogus_data_file()
        self.schedule_for_cleanup(path)
        file = File(parent_id=project_model.id, path=path).store(
            synapse_client=self.syn
        )

        # Create evaluation for testing
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

            # GIVEN initial data with all data types
            initial_data = pd.DataFrame(
                {
                    # Basic types
                    "column_string": ["value1", "value2", "value3"],
                    "column_double": [1.1, 2.2, 3.3],
                    "column_integer": [1, 2, 3],
                    "column_boolean": [True, True, True],
                    "column_date": [
                        utils.to_unix_epoch_time("2021-01-01"),
                        utils.to_unix_epoch_time("2021-01-02"),
                        utils.to_unix_epoch_time("2021-01-03"),
                    ],
                    # Reference types
                    "column_filehandleid": [
                        file.file_handle.id,
                        file.file_handle.id,
                        file.file_handle.id,
                    ],
                    "column_entityid": [file.id, file.id, file.id],
                    "column_submissionid": [
                        submission.id,
                        submission.id,
                        submission.id,
                    ],
                    "column_evaluationid": [
                        evaluation.id,
                        evaluation.id,
                        evaluation.id,
                    ],
                    # Text types
                    "column_link": [
                        "https://www.synapse.org/Profile:",
                        "https://www.synapse.org/Profile:",
                        "https://www.synapse.org/Profile:",
                    ],
                    "column_mediumtext": ["value1", "value2", "value3"],
                    "column_largetext": ["value1", "value2", "value3"],
                    # User IDs
                    "column_userid": [
                        self.syn.credentials.owner_id,
                        self.syn.credentials.owner_id,
                        self.syn.credentials.owner_id,
                    ],
                    # List types
                    "column_string_LIST": [
                        ["value1", "value2"],
                        ["value3", "value4"],
                        ["value5", "value6"],
                    ],
                    "column_integer_LIST": [[1, 2], [3, 4], [5, 6]],
                    "column_boolean_LIST": [
                        [True, False],
                        [True, False],
                        [True, False],
                    ],
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
                    "column_entity_id_list": [
                        [file.id, file.id],
                        [file.id, file.id],
                        [file.id, file.id],
                    ],
                    "column_user_id_list": [
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                        [self.syn.credentials.owner_id, self.syn.credentials.owner_id],
                    ],
                    # JSON type
                    "column_json": [
                        {"key1": "value1", "key2": 2},
                        {"key3": "value3", "key4": 4},
                        {"key5": "value5", "key6": 6},
                    ],
                }
            )

            # Store initial data
            table.store_rows(
                values=initial_data,
                schema_storage_strategy=None,
                synapse_client=self.syn,
            )

            # Create a second test file to update references
            path2 = utils.make_bogus_data_file()
            self.schedule_for_cleanup(path2)
            file2 = File(parent_id=project_model.id, path=path2).store(
                synapse_client=self.syn
            )

            # WHEN I upsert with updated data for all types
            updated_data = pd.DataFrame(
                {
                    # Basic types with updated values
                    "column_string": ["value1", "value2", "value3"],
                    "column_double": [11.2, 22.3, 33.4],
                    "column_integer": [11, 22, 33],
                    "column_boolean": [False, False, False],
                    "column_date": [
                        utils.to_unix_epoch_time("2022-01-01"),
                        utils.to_unix_epoch_time("2022-01-02"),
                        utils.to_unix_epoch_time("2022-01-03"),
                    ],
                    # Updated references
                    "column_filehandleid": [
                        int(file2.file_handle.id),
                        int(file2.file_handle.id),
                        int(file2.file_handle.id),
                    ],
                    "column_entityid": [file2.id, file2.id, file2.id],
                    "column_submissionid": [
                        int(submission.id),
                        int(submission.id),
                        int(submission.id),
                    ],
                    "column_evaluationid": [
                        int(evaluation.id),
                        int(evaluation.id),
                        int(evaluation.id),
                    ],
                    # Updated text
                    "column_link": [
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                    ],
                    "column_mediumtext": ["value11", "value22", "value33"],
                    "column_largetext": ["value11", "value22", "value33"],
                    # User IDs
                    "column_userid": [
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                    ],
                    # Updated list types
                    "column_string_LIST": [
                        ["value11", "value22"],
                        ["value33", "value44"],
                        ["value55", "value66"],
                    ],
                    "column_integer_LIST": [[11, 22], [33, 44], [55, 66]],
                    "column_boolean_LIST": [
                        [False, True],
                        [False, True],
                        [False, True],
                    ],
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
                    "column_entity_id_list": [
                        [file2.id, file2.id],
                        [file2.id, file2.id],
                        [file2.id, file2.id],
                    ],
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

            # Perform upsert based on string column
            table.upsert_rows(
                values=updated_data,
                primary_keys=["column_string"],
                synapse_client=self.syn,
            )

            # THEN all data types should be correctly updated
            results = query(
                f"SELECT * FROM {table.id}",
                synapse_client=self.syn,
                include_row_id_and_row_version=False,
            )

            # Check that all values were updated correctly
            # Convert to JSON for easy comparison
            original_as_string = updated_data.to_json()
            modified_as_string = results.to_json()
            assert original_as_string == modified_as_string

            # WHEN I upsert with multiple primary keys
            multi_key_data = pd.DataFrame(
                {
                    # Just using a subset of columns for this test case
                    "column_string": ["this", "is", "updated"],
                    "column_double": [1.1, 2.2, 3.3],
                    "column_integer": [1, 2, 3],
                    "column_boolean": [True, True, True],
                    "column_date": [
                        utils.to_unix_epoch_time("2021-01-01"),
                        utils.to_unix_epoch_time("2021-01-02"),
                        utils.to_unix_epoch_time("2021-01-03"),
                    ],
                    "column_filehandleid": [
                        int(file.file_handle.id),
                        int(file.file_handle.id),
                        int(file.file_handle.id),
                    ],
                    "column_entityid": [file.id, file.id, file.id],
                    "column_submissionid": [
                        int(submission.id),
                        int(submission.id),
                        int(submission.id),
                    ],
                    "column_evaluationid": [
                        int(evaluation.id),
                        int(evaluation.id),
                        int(evaluation.id),
                    ],
                    "column_link": [
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                        "https://www.synapse.org/",
                    ],
                    "column_mediumtext": ["updated1", "updated2", "updated3"],
                    "column_largetext": ["largetext1", "largetext2", "largetext3"],
                    "column_userid": [
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                        int(self.syn.credentials.owner_id),
                    ],
                    # Simplified list data
                    "column_string_LIST": [
                        ["a", "b"],
                        ["c", "d"],
                        ["e", "f"],
                    ],
                    "column_integer_LIST": [[9, 8], [7, 6], [5, 4]],
                    "column_boolean_LIST": [
                        [True, True],
                        [True, True],
                        [True, True],
                    ],
                    "column_date_LIST": [
                        [
                            utils.to_unix_epoch_time("2023-01-01"),
                            utils.to_unix_epoch_time("2023-01-02"),
                        ],
                        [
                            utils.to_unix_epoch_time("2023-01-03"),
                            utils.to_unix_epoch_time("2023-01-04"),
                        ],
                        [
                            utils.to_unix_epoch_time("2023-01-05"),
                            utils.to_unix_epoch_time("2023-01-06"),
                        ],
                    ],
                    "column_entity_id_list": [
                        [file.id, file.id],
                        [file.id, file.id],
                        [file.id, file.id],
                    ],
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
                    "column_json": [
                        json.dumps({"final1": "value1"}),
                        json.dumps({"final2": "value2"}),
                        json.dumps({"final3": "value3"}),
                    ],
                }
            )

            # Test multiple primary keys
            primary_keys = [
                "column_double",
                "column_integer",
                "column_boolean",
                "column_date",
            ]

            table.upsert_rows(
                values=multi_key_data,
                primary_keys=primary_keys,
                synapse_client=self.syn,
            )

            # THEN the new rows should be added (not updating existing)
            results_after_multi_key = query(
                f"SELECT * FROM {table.id}",
                synapse_client=self.syn,
                include_row_id_and_row_version=False,
            )

            # We should have more rows now (original 3 + 3 new ones)
            assert len(results_after_multi_key) == 6

        finally:
            # Clean up
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I delete a single row from the table
        table.delete_rows(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id} WHERE column_string = 'value2'",
            synapse_client=self.syn,
        )

        # AND I query the table
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I delete a single row from the table
        table.delete_rows(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id} WHERE column_string IN ('value2','value3')",
            synapse_client=self.syn,
        )

        # AND I query the table
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for a column already stored in Synapse
        data_for_table = pd.DataFrame({"column_string": ["value1", "value2", "value3"]})
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I delete a single row from the table
        table.delete_rows(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id} WHERE column_string = 'foo'",
            synapse_client=self.syn,
        )

        # AND I query the table
        results = query(f"SELECT * FROM {table.id}", synapse_client=self.syn)

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
        old_table_instance = old_table_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_table_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_table_instance.columns[old_column_name].name = new_column_name

        # AND I store the table
        old_table_instance.store(synapse_client=self.syn)

        # THEN the column name should be updated on the existing table instance
        assert old_table_instance.columns[new_column_name] is not None
        assert old_column_name not in old_table_instance.columns

        # AND the new column name should be reflected in the Synapse table
        new_table_instance = Table(id=old_table_instance.id).get(
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
        old_table_instance = old_table_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_table_instance.id)

        # WHEN I delete the column
        old_table_instance.delete_column(name=old_column_name)

        # AND I store the table
        old_table_instance.store(synapse_client=self.syn)

        # THEN the column should be removed from the table instance
        assert old_column_name not in old_table_instance.columns

        # AND the column to keep should still be in the table instance
        assert column_to_keep in old_table_instance.columns
        assert len(old_table_instance.columns.values()) == 1

        # AND the column should be removed from the Synapse table
        new_table_instance = Table(id=old_table_instance.id).get(
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

    async def test_query_to_csv(self, project_model: Project) -> None:
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for the table stored in synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_column": [1, 2, 3, None],
                "float_column": [1.1, 2.2, 3.3, None],
            }
        )
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I query the table with a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir_name:
            results = query(
                query=f"SELECT * FROM {table.id}",
                synapse_client=self.syn,
                download_location=temp_dir_name,
            )
            # THEN The returned result should be a path to the CSV
            assert isinstance(results, str)
            assert os.path.basename(results).endswith(".csv")
            as_dataframe = pd.read_csv(results)

        # AND the data in the columns should match
        pd.testing.assert_series_equal(
            as_dataframe["column_string"], data_for_table["column_string"]
        )
        pd.testing.assert_series_equal(
            as_dataframe["integer_column"], data_for_table["integer_column"]
        )
        pd.testing.assert_series_equal(
            as_dataframe["float_column"], data_for_table["float_column"]
        )

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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for the table stored in synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_column": [1, 2, 3, None],
                "float_column": [1.1, 2.2, 3.3, None],
            }
        )
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I query the table with a part mask
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZE_BYTES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZE_BYTES | LAST_UPDATED_ON

        results = query_part_mask(
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND data for the table stored in synapse
        data_for_table = pd.DataFrame(
            {
                "column_string": ["value1", "value2", "value3", "value4"],
                "integer_column": [1, 2, 3, None],
                "float_column": [1.1, 2.2, 3.3, None],
            }
        )
        table.store_rows(
            values=data_for_table, schema_storage_strategy=None, synapse_client=self.syn
        )

        # WHEN I query the table with a part mask
        QUERY_RESULTS = 0x1
        results = query_part_mask(
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


class TestTableSnapshot:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_snapshot_basic(self, project_model: Project) -> None:
        """Test creating a basic snapshot of a table."""
        # GIVEN a table with some data
        table = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="col1", column_type=ColumnType.STRING),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Store some data
        data = {"col1": ["A", "B"], "col2": [1, 2]}
        table.store_rows(values=data, synapse_client=self.syn)

        # WHEN I create a snapshot
        snapshot_response = table.snapshot(
            comment="Test snapshot", label="v1.0", synapse_client=self.syn
        )

        # THEN the snapshot should be created successfully
        assert snapshot_response is not None
        assert "snapshotVersionNumber" in snapshot_response
        assert snapshot_response["snapshotVersionNumber"] is not None

        # AND the snapshot version should be 1
        snapshot_version = snapshot_response["snapshotVersionNumber"]
        assert snapshot_version == 1

        # AND when I retrieve the snapshot version, it should have the correct comment and label
        snapshot_table = Table(id=table.id, version_number=snapshot_version).get(
            synapse_client=self.syn
        )
        assert snapshot_table.version_comment == "Test snapshot"
        assert snapshot_table.version_label == "v1.0"
        assert snapshot_table.version_number == 1

        # AND when I retrieve the latest version (without specifying version), it should be "in progress"
        latest_table = Table(id=table.id).get(synapse_client=self.syn)
        assert latest_table.version_label == "in progress"
        assert latest_table.version_comment == "in progress"
        assert latest_table.version_number > 1

    async def test_snapshot_with_activity(self, project_model: Project) -> None:
        """Test creating a snapshot with activity (provenance)."""
        # GIVEN a table with some data and an activity
        table = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="col1", column_type=ColumnType.STRING),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Create and store an activity
        activity = Activity(
            name="Test Activity",
            description="Test activity for snapshot",
        )
        table.activity = activity
        table.store(synapse_client=self.syn)

        # Store some data
        data = {"col1": ["A", "B"], "col2": [1, 2]}
        table.store_rows(values=data, synapse_client=self.syn)

        # WHEN I create a snapshot with activity included
        snapshot_response = table.snapshot(
            comment="Test snapshot with activity",
            label="v1.0",
            include_activity=True,
            associate_activity_to_new_version=False,
            synapse_client=self.syn,
        )

        # THEN the snapshot should be created successfully
        assert snapshot_response is not None
        assert "snapshotVersionNumber" in snapshot_response
        assert snapshot_response["snapshotVersionNumber"] is not None

        # AND the snapshot version should be 1
        snapshot_version = snapshot_response["snapshotVersionNumber"]
        assert snapshot_version == 1

        # AND when I retrieve the snapshot version, it should have the correct comment and label
        snapshot_table = Table(id=table.id, version_number=snapshot_version).get(
            synapse_client=self.syn
        )
        assert snapshot_table.version_comment == "Test snapshot with activity"
        assert snapshot_table.version_label == "v1.0"
        assert snapshot_table.version_number == 1

        # AND when I retrieve the latest version (without specifying version), it should be "in progress"
        latest_table = Table(id=table.id).get(synapse_client=self.syn)
        assert latest_table.version_label == "in progress"
        assert latest_table.version_comment == "in progress"
        assert latest_table.version_number > 1

    async def test_snapshot_without_activity(self, project_model: Project) -> None:
        """Test creating a snapshot without including activity."""
        # GIVEN a table with some data and an activity
        table = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="col1", column_type=ColumnType.STRING),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Create and store an activity
        activity = Activity(
            name="Test Activity",
            description="Test activity for snapshot",
        )
        table.activity = activity
        table.store(synapse_client=self.syn)

        # Store some data
        data = {"col1": ["A", "B"], "col2": [1, 2]}
        table.store_rows(values=data, synapse_client=self.syn)

        # WHEN I create a snapshot without including activity
        snapshot_response = table.snapshot(
            comment="Test snapshot without activity",
            label="v2.0",
            include_activity=False,
            synapse_client=self.syn,
        )

        # THEN the snapshot should be created successfully
        assert snapshot_response is not None
        assert "snapshotVersionNumber" in snapshot_response
        assert snapshot_response["snapshotVersionNumber"] is not None

        # AND the snapshot version should be 1
        snapshot_version = snapshot_response["snapshotVersionNumber"]
        assert snapshot_version == 1

        # AND when I retrieve the snapshot version, it should have the correct comment and label
        snapshot_table = Table(id=table.id, version_number=snapshot_version).get(
            synapse_client=self.syn
        )
        assert snapshot_table.version_comment == "Test snapshot without activity"
        assert snapshot_table.version_label == "v2.0"
        assert snapshot_table.version_number == 1

        # AND when I retrieve the latest version (without specifying version), it should be "in progress"
        latest_table = Table(id=table.id).get(synapse_client=self.syn)
        assert latest_table.version_label == "in progress"
        assert latest_table.version_comment == "in progress"
        assert latest_table.version_number > 1

    async def test_snapshot_minimal_args(self, project_model: Project) -> None:
        """Test creating a snapshot with minimal arguments."""
        # GIVEN a table with some data
        table = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="col1", column_type=ColumnType.STRING),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Store some data
        data = {"col1": ["A", "B"], "col2": [1, 2]}
        table.store_rows(values=data, synapse_client=self.syn)

        # WHEN I create a snapshot with minimal arguments
        snapshot_response = table.snapshot(synapse_client=self.syn)

        # THEN the snapshot should be created successfully
        assert snapshot_response is not None
        assert "snapshotVersionNumber" in snapshot_response
        assert snapshot_response["snapshotVersionNumber"] is not None

        # AND the snapshot version should be 1
        snapshot_version = snapshot_response["snapshotVersionNumber"]
        assert snapshot_version == 1

        # AND when I retrieve the snapshot version, it should have the correct version number
        snapshot_table = Table(id=table.id, version_number=snapshot_version).get(
            synapse_client=self.syn
        )
        assert snapshot_table.version_number == 1
        # Comment and label should be None or empty when not specified
        assert (
            snapshot_table.version_comment is None
            or snapshot_table.version_comment == ""
        )
        assert snapshot_table.version_label == "1"

        # AND when I retrieve the latest version (without specifying version), it should be "in progress"
        latest_table = Table(id=table.id).get(synapse_client=self.syn)
        assert latest_table.version_label == "in progress"
        assert latest_table.version_comment == "in progress"
        assert latest_table.version_number > 1
