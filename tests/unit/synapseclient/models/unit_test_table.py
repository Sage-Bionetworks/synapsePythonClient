"""Tests for the Table class."""

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import FILE_ENTITY
from synapseclient.models import Column, ColumnType, Table

SYN_123 = "syn123"
SYN_456 = "syn456"
FOLDER_NAME = "example_folder"
PARENT_ID = "parent_id_value"
DESCRIPTION = "This is an example folder."
ETAG = "etag_value"
CREATED_ON = "createdOn_value"
MODIFIED_ON = "modifiedOn_value"
CREATED_BY = "createdBy_value"
MODIFIED_BY = "modifiedBy_value"


class TestDeleteColumn:
    """Tests for deleting columns on a Table"""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_delete_with_no_last_instance(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
        )

        # WHEN I Delete the column
        with pytest.raises(ValueError) as e:
            table.delete_column(name="col1")

        # THEN I expect an error
        assert (
            "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            in str(e.value)
        )

    async def test_delete_with_no_columns(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I Delete the column
        with pytest.raises(ValueError) as e:
            table.delete_column(name="col1")

        # THEN I expect an error
        assert (
            "There are no columns. Make sure you use the `include_columns` parameter in the `.get()` method."
            in str(e.value)
        )

    async def test_delete_column_does_not_exist(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I Delete the column
        with pytest.raises(ValueError) as e:
            table.delete_column(name="col2")

        # THEN I expect an error
        assert "Column with name col2 does not exist in the table." in str(e.value)

    async def test_delete_column_is_delete(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(
                    id="unique_id_from_synapse",
                    name="col1",
                    column_type=ColumnType.INTEGER,
                ),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I Delete the column
        table.delete_column(name="col1")

        # THEN the column should be marked for deletion
        assert "col1" not in table.columns
        assert "unique_id_from_synapse" in table._columns_to_delete


class TestAddColumn:
    """Tests for adding columns to a Table"""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_add_column_with_no_last_instance(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
        )

        # WHEN I add a column
        with pytest.raises(ValueError) as e:
            table.add_column(Column(name="col1", column_type=ColumnType.INTEGER))

        # THEN I expect an error
        assert (
            "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            in str(e.value)
        )

    async def test_add_column(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add a column
        table.add_column(Column(name="col1", column_type=ColumnType.INTEGER))

        # THEN the column should be added
        assert len(table.columns) == 1
        assert table.columns["col1"].name == "col1"
        assert table.columns["col1"].column_type == ColumnType.INTEGER

    async def test_add_column_with_duplicate_name(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add a column
        with pytest.raises(ValueError) as e:
            table.add_column(Column(name="col1", column_type=ColumnType.INTEGER))

        # THEN I expect an error
        assert "Duplicate column name: col1" in str(e.value)

    async def test_add_column_at_specific_index(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
                Column(name="col3", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add a column
        table.add_column(Column(name="col2", column_type=ColumnType.INTEGER), index=1)

        # THEN the column should be added
        assert len(table.columns) == 3
        assert table.columns["col1"].name == "col1"
        assert table.columns["col1"].column_type == ColumnType.INTEGER

        assert table.columns["col2"].name == "col2"
        assert table.columns["col2"].column_type == ColumnType.INTEGER

        assert table.columns["col3"].name == "col3"
        assert table.columns["col3"].column_type == ColumnType.INTEGER

    async def test_add_column_at_specific_index_out_of_bounds(self) -> None:
        """Test that adding at an out of bounds index adds the column to the end"""
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add a column
        table.add_column(Column(name="col2", column_type=ColumnType.INTEGER), index=99)

        # THEN the column should be added
        assert len(table.columns) == 2
        assert table.columns["col1"].name == "col1"
        assert table.columns["col1"].column_type == ColumnType.INTEGER

        assert table.columns["col2"].name == "col2"
        assert table.columns["col2"].column_type == ColumnType.INTEGER

    async def test_add_column_at_index_with_duplicate_name_errors(self) -> None:
        """Test that adding a column at an index with a duplicate name errors"""
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add a column
        with pytest.raises(ValueError) as e:
            table.add_column(
                Column(name="col2", column_type=ColumnType.INTEGER), index=1
            )

        # THEN I expect an error
        assert "Duplicate column name: col2" in str(e.value)

    async def test_add_multiple_columns(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add columns
        table.add_column(
            [
                Column(name="col1", column_type=ColumnType.INTEGER),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ]
        )

        # THEN the columns should be added
        assert len(table.columns) == 2
        assert table.columns["col1"].name == "col1"
        assert table.columns["col1"].column_type == ColumnType.INTEGER

        assert table.columns["col2"].name == "col2"
        assert table.columns["col2"].column_type == ColumnType.INTEGER

    async def test_add_multiple_columns_with_duplicate_name(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add columns
        with pytest.raises(ValueError) as e:
            table.add_column(
                [
                    Column(name="col1", column_type=ColumnType.INTEGER),
                    Column(name="col2", column_type=ColumnType.INTEGER),
                ]
            )

        # THEN I expect an error
        assert "Duplicate column name: col1" in str(e.value)

    async def test_add_multiple_columns_at_specific_index(self) -> None:
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
                Column(name="col4", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add columns
        table.add_column(
            [
                Column(name="col2", column_type=ColumnType.INTEGER),
                Column(name="col3", column_type=ColumnType.INTEGER),
            ],
            index=1,
        )

        # THEN the columns should be added
        assert len(table.columns) == 4
        assert table.columns["col1"].name == "col1"
        assert table.columns["col1"].column_type == ColumnType.INTEGER

        assert table.columns["col2"].name == "col2"
        assert table.columns["col2"].column_type == ColumnType.INTEGER

        assert table.columns["col3"].name == "col3"
        assert table.columns["col3"].column_type == ColumnType.INTEGER

        assert table.columns["col4"].name == "col4"
        assert table.columns["col4"].column_type == ColumnType.INTEGER

    async def test_add_multiple_columns_at_specific_index_out_of_bounds(self) -> None:
        """Test that adding at an out of bounds index adds the column to the end"""
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add columns
        table.add_column(
            [
                Column(name="col2", column_type=ColumnType.INTEGER),
                Column(name="col3", column_type=ColumnType.INTEGER),
            ],
            index=99,
        )

        # THEN the columns should be added
        assert len(table.columns) == 3
        assert table.columns["col1"].name == "col1"
        assert table.columns["col1"].column_type == ColumnType.INTEGER

        assert table.columns["col2"].name == "col2"
        assert table.columns["col2"].column_type == ColumnType.INTEGER

        assert table.columns["col3"].name == "col3"
        assert table.columns["col3"].column_type == ColumnType.INTEGER

    async def test_add_multiple_columns_at_index_with_duplicate_name_errors(
        self,
    ) -> None:
        """Test that adding a column at an index with a duplicate name errors"""
        # GIVEN a table
        table = Table(
            id=SYN_123,
            columns=[
                Column(name="col1", column_type=ColumnType.INTEGER),
                Column(name="col2", column_type=ColumnType.INTEGER),
            ],
            _last_persistent_instance=Table(
                id=SYN_123,
            ),
        )

        # WHEN I add columns
        with pytest.raises(ValueError) as e:
            table.add_column(
                [
                    Column(name="col2", column_type=ColumnType.INTEGER),
                    Column(name="col3", column_type=ColumnType.INTEGER),
                ],
                index=1,
            )

        # THEN I expect an error
        assert "Duplicate column name: col2" in str(e.value)
