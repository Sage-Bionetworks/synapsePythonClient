import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Column, ColumnType, Project, Table


class TestColumnAsync:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_column_by_id(self, project_model: Project) -> None:
        """Test getting a column by its ID."""
        # GIVEN a table with a column
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(
                    name="test_column", column_type=ColumnType.STRING, maximum_size=50
                )
            ],
        )

        # WHEN I store the table
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND I retrieve the table with columns
        retrieved_table = await Table(id=table.id).get_async(
            include_columns=True, synapse_client=self.syn
        )

        # AND I get the column ID
        column_id = retrieved_table.columns["test_column"].id

        # WHEN I get the column by ID
        column = await Column(id=column_id).get_async(synapse_client=self.syn)

        # THEN the column should be retrieved successfully
        assert column.id == column_id
        assert column.name == "test_column"
        assert column.column_type == ColumnType.STRING
        assert column.maximum_size == 50

    async def test_get_column_by_invalid_id(self) -> None:
        """Test getting a column by an invalid ID."""
        # GIVEN an invalid column ID
        invalid_id = "999999999"

        # WHEN I try to get the column by invalid ID
        # THEN it should raise an exception
        with pytest.raises(SynapseHTTPError) as exc_info:
            await Column(id=invalid_id).get_async(synapse_client=self.syn)

        # Verify the error is a 404 Not Found
        assert "404" in str(exc_info.value)

    async def test_list_all_columns(self, project_model: Project) -> None:
        """Test listing all columns without a prefix."""
        # GIVEN a single table with multiple columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_list_all_col1", column_type=ColumnType.STRING),
                Column(name="test_list_all_col2", column_type=ColumnType.INTEGER),
                Column(name="test_list_all_col3", column_type=ColumnType.DOUBLE),
                Column(name="test_list_all_col4", column_type=ColumnType.BOOLEAN),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN I list all columns with a limit of 5 to test pagination
        # Note: limit is per request, the function continues until all matching columns are returned
        columns = []
        async for column in Column.list_async(limit=5, synapse_client=self.syn):
            columns.append(column)
            # Limit to first 20 to avoid too many results
            if len(columns) >= 20:
                break

        # THEN I should get columns back
        assert len(columns) > 0
        assert all(isinstance(col, Column) for col in columns)
        assert all(col.id is not None for col in columns)
        assert all(col.name is not None for col in columns)

    async def test_list_columns_with_prefix(self, project_model: Project) -> None:
        """Test listing columns with a prefix filter."""
        # GIVEN a table with columns that have a specific prefix
        prefix = "test_prefix_column_filter"
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(
                    name="test_prefix_column_filter_col1", column_type=ColumnType.STRING
                ),
                Column(
                    name="test_prefix_column_filter_col2",
                    column_type=ColumnType.INTEGER,
                ),
                Column(name="other_col_static", column_type=ColumnType.DOUBLE),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN I list columns with the prefix using limit=5 to test pagination
        # Note: limit is per request, the function continues until all matching columns are returned
        columns = []
        async for column in Column.list_async(
            prefix=prefix, limit=5, synapse_client=self.syn
        ):
            columns.append(column)

        # THEN I should get only the columns with the prefix
        assert (
            len(columns) >= 2
        )  # May be more if other tests created columns with same prefix
        assert all(isinstance(col, Column) for col in columns)
        assert all(col.name.startswith(prefix) for col in columns)

        # Verify the specific columns we created are included
        column_names = [col.name for col in columns]
        assert "test_prefix_column_filter_col1" in column_names
        assert "test_prefix_column_filter_col2" in column_names
        assert "other_col_static" not in column_names

    async def test_list_columns_with_limit_and_offset(
        self, project_model: Project
    ) -> None:
        """Test listing columns with limit and offset parameters to verify pagination behavior."""
        # GIVEN a single table with multiple columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_limit_offset_col1", column_type=ColumnType.STRING),
                Column(name="test_limit_offset_col2", column_type=ColumnType.INTEGER),
                Column(name="test_limit_offset_col3", column_type=ColumnType.DOUBLE),
                Column(name="test_limit_offset_col4", column_type=ColumnType.BOOLEAN),
                Column(name="test_limit_offset_col5", column_type=ColumnType.DATE),
                Column(name="test_limit_offset_col6", column_type=ColumnType.STRING),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN I list columns with limit
        columns_page1 = []
        async for column in Column.list_async(
            limit=3, offset=0, synapse_client=self.syn
        ):
            columns_page1.append(column)
            # Limit to first 3 to test pagination behavior
            if len(columns_page1) >= 3:
                break

        # THEN I should get columns back
        assert len(columns_page1) <= 3
        assert all(isinstance(col, Column) for col in columns_page1)

        # WHEN I list columns with offset
        columns_page2 = []
        async for column in Column.list_async(
            limit=3, offset=3, synapse_client=self.syn
        ):
            columns_page2.append(column)
            # Limit to first 3 to test pagination behavior
            if len(columns_page2) >= 3:
                break

        # THEN I should get columns back
        assert len(columns_page2) <= 3
        assert all(isinstance(col, Column) for col in columns_page2)

    async def test_list_columns_with_no_prefix_match(self) -> None:
        """Test listing columns with a prefix that doesn't match any columns."""
        # GIVEN a unique prefix that won't match any existing columns
        unique_prefix = "nonexistent_prefix_static"

        # WHEN I list columns with the non-matching prefix using limit=5
        columns = []
        async for column in Column.list_async(
            prefix=unique_prefix, limit=5, synapse_client=self.syn
        ):
            columns.append(column)

        # THEN I should get no columns
        assert len(columns) == 0
