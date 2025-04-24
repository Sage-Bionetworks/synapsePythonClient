import asyncio
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Column, ColumnType, Project, Table, VirtualTable


class TestVirtualTableWithoutData:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_empty_virtual_table(self, project_model: Project) -> None:
        # GIVEN a VirtualTable with an empty definingSQL
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            description="Test virtual table",
            parent_id=project_model.id,
            defining_sql="",
        )

        # WHEN I try to store the virtual table
        with pytest.raises(
            SynapseHTTPError,
            match="400 Client Error: The definingSQL of the virtual table is required "
            "and must not be the empty string.",
        ):
            virtual_table.store(synapse_client=self.syn)

    async def test_create_virtual_table_without_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with no columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a virtual table that uses the table in its defining SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table_description = "Test virtual table"
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            description=virtual_table_description,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN I try to store the virtual table
        with pytest.raises(
            SynapseHTTPError, match=f"400 Client Error: Schema for {table.id} is empty."
        ):
            virtual_table.store(synapse_client=self.syn)

    async def test_create_virtual_table_with_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a virtual table that uses the table in its defining SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table_description = "Test virtual table"
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            description=virtual_table_description,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN I store the virtual table
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # THEN the virtual table should be created
        assert virtual_table.id is not None

        # AND I can retrieve that virtual table from Synapse
        new_virtual_table_instance = VirtualTable(id=virtual_table.id).get(
            synapse_client=self.syn
        )
        assert new_virtual_table_instance is not None
        assert new_virtual_table_instance.name == virtual_table_name
        assert new_virtual_table_instance.id == virtual_table.id
        assert new_virtual_table_instance.description == virtual_table_description

    async def test_create_virtual_table_with_invalid_sql(
        self, project_model: Project
    ) -> None:
        # GIVEN a virtual table with invalid SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table_description = "Test virtual table"
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            description=virtual_table_description,
            defining_sql="INVALID SQL",
        )

        # WHEN I store the virtual table
        with pytest.raises(
            SynapseHTTPError,
            match='400 Client Error: Encountered " <regular_identifier> "INVALID "" '
            "at line 1, column 1.\nWas expecting one of:",
        ):
            virtual_table.store(synapse_client=self.syn)

    async def test_update_virtual_table_attributes(
        self, project_model: Project
    ) -> None:
        # GIVEN a table to use in the defining SQL
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a virtual table
        original_virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            description="Test virtual table",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        original_virtual_table = original_virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(original_virtual_table.id)

        # AND I update attributes of the virtual table
        updated_virtual_table = VirtualTable(id=original_virtual_table.id).get(
            synapse_client=self.syn
        )
        updated_virtual_table.name = str(uuid.uuid4())
        updated_virtual_table.description = "Updated description"
        updated_virtual_table.defining_sql = f"SELECT test_column FROM {table.id}"
        updated_virtual_table = updated_virtual_table.store(synapse_client=self.syn)

        # AND I retrieve the virtual table with its original id
        retrieved_virtual_table = VirtualTable(id=original_virtual_table.id).get(
            synapse_client=self.syn
        )

        # THEN the virtual table should be updated
        assert retrieved_virtual_table is not None
        assert retrieved_virtual_table.name == updated_virtual_table.name
        assert retrieved_virtual_table.description == updated_virtual_table.description
        assert (
            retrieved_virtual_table.defining_sql == updated_virtual_table.defining_sql
        )

        # AND all versions should have the same id
        assert (
            retrieved_virtual_table.id
            == updated_virtual_table.id
            == original_virtual_table.id
        )

    async def test_delete_virtual_table(self, project_model: Project) -> None:
        # GIVEN a table to use in the defining SQL
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a virtual table
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            description="Test virtual table",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)
        assert virtual_table.id is not None

        # WHEN I delete the virtual table
        virtual_table.delete(synapse_client=self.syn)

        # THEN the virtual table should be deleted
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {virtual_table.id} is in trash can.",
        ):
            VirtualTable(id=virtual_table.id).get(synapse_client=self.syn)


class TestVirtualTableWithData:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_query_data_from_virtual_table(self, project_model: Project) -> None:
        # GIVEN a table with data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table that uses the table in its defining SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for the virtual table to be ready
        await asyncio.sleep(2)

        # WHEN I query the virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should match the table data
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"].tolist() == [30, 25]

    async def test_update_defining_sql(self, project_model: Project) -> None:
        # GIVEN a table with data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table that uses the table in its defining SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # WHEN I update the defining SQL of the virtual table
        virtual_table.defining_sql = f"SELECT name FROM {table.id}"
        virtual_table = virtual_table.store(synapse_client=self.syn)

        # AND I query the updated virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should match the updated SQL
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert "age" not in query_result.columns

        # AND the column is not present the next time the `virtual_table` is retrieved
        retrieved_virtual_table = VirtualTable(id=virtual_table.id).get(
            synapse_client=self.syn
        )
        assert "age" not in retrieved_virtual_table.columns.keys()
        assert "name" in retrieved_virtual_table.columns.keys()

    async def test_virtual_table_updates_with_table_data(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with columns but no data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_name", column_type=ColumnType.STRING),
                Column(name="test_age", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a virtual table that uses the table in its defining SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait a moment for the virtual table to be ready
        await asyncio.sleep(2)

        # WHEN I query the virtual table before adding data to the table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN no data should be returned
        assert len(query_result) == 0

        # AND WHEN I add data to the table
        data = pd.DataFrame({"test_name": ["Alice", "Bob"], "test_age": [30, 25]})
        table.store_rows(data, synapse_client=self.syn)

        # Wait for updates to propagate
        await asyncio.sleep(2)

        # AND I query the virtual table again
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should match the table data
        assert len(query_result) == 2
        assert query_result["test_name"].tolist() == ["Alice", "Bob"]
        assert query_result["test_age"].tolist() == [30, 25]

    async def test_virtual_table_reflects_table_data_removal(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with columns and data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table that uses the table in its defining SQL
        virtual_table_name = str(uuid.uuid4())
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for the virtual table to be ready
        await asyncio.sleep(2)

        # WHEN I query the virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should match the table data
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"].tolist() == [30, 25]

        # AND WHEN I remove data from the table
        table.delete_rows(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id}", synapse_client=self.syn
        )

        # Ensure the table contains no rows after deletion
        query_result = table.query(f"SELECT * FROM {table.id}", synapse_client=self.syn)
        assert len(query_result) == 0

        # Wait for changes to propagate to the virtual table
        await asyncio.sleep(2)

        # AND I query the virtual table again
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN no data should be returned
        assert len(query_result) == 0

    async def test_virtual_table_with_column_selection(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
                Column(name="city", column_type=ColumnType.STRING),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [30, 25], "city": ["New York", "Boston"]}
        )
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table with column selection
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT name, city FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for the virtual table to be ready
        await asyncio.sleep(2)

        # WHEN I query the virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should only include the selected columns
        assert len(query_result) == 2
        assert "name" in query_result.columns
        assert "city" in query_result.columns
        assert "age" not in query_result.columns
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["city"].tolist() == ["New York", "Boston"]

    async def test_virtual_table_with_filtering(self, project_model: Project) -> None:
        # GIVEN a table with data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]})
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table with filtering
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id} WHERE age > 25",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # WHEN I query the virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should only include filtered rows
        assert len(query_result) == 2
        assert set(query_result["name"].tolist()) == {"Alice", "Charlie"}
        assert set(query_result["age"].tolist()) == {30, 35}

    async def test_virtual_table_with_ordering(self, project_model: Project) -> None:
        # GIVEN a table with data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]})
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table with ordering
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id} ORDER BY age DESC",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for the virtual table to be ready
        await asyncio.sleep(2)

        # WHEN I query the virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the data should be ordered by age descending
        assert len(query_result) == 3
        assert query_result["age"].tolist() == [35, 30, 25]
        assert query_result["name"].tolist() == ["Charlie", "Alice", "Bob"]

    async def test_virtual_table_with_aggregation(self, project_model: Project) -> None:
        # GIVEN a table with data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="department", column_type=ColumnType.STRING),
                Column(name="salary", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame(
            {
                "department": ["IT", "HR", "IT", "Finance", "HR"],
                "salary": [70000, 60000, 80000, 90000, 65000],
            }
        )
        table.store_rows(data, synapse_client=self.syn)

        # AND a virtual table with aggregation
        # Define SQL properly with consistent column names
        defining_sql = f"""SELECT
            department,
            COUNT(*) as employee_count
        FROM {table.id}
        GROUP BY department"""

        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=defining_sql,
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for virtual table to be ready
        await asyncio.sleep(3)

        # Query without column names to avoid mismatches
        query = f"SELECT * FROM {virtual_table.id}"

        # WHEN I query the virtual table
        query_result = virtual_table.query(query, synapse_client=self.syn)

        # THEN the data should be aggregated
        assert len(query_result) == 3

        # Sort the result by department to ensure consistent ordering for the test
        query_result = query_result.sort_values("department").reset_index(drop=True)

        assert query_result["department"].tolist() == ["Finance", "HR", "IT"]
        assert query_result["employee_count"].tolist() == [1, 2, 2]
