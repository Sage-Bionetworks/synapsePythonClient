import time
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Column, ColumnType, Project, Table, VirtualTable


class TestVirtualTableCreationAndManagement:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def base_table_with_columns(self, project_model: Project) -> Table:
        # Create a table with columns for testing
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
        return table

    def test_virtual_table_validation_scenarios(self, project_model: Project) -> None:
        # GIVEN different virtual table scenarios with validation issues

        # Test case 1: Empty defining SQL
        empty_sql_virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            description="Test virtual table",
            parent_id=project_model.id,
            defining_sql="",
        )

        # WHEN/THEN empty SQL should be rejected
        with pytest.raises(
            ValueError,
            match="The defining_sql attribute must be set for a",
        ):
            empty_sql_virtual_table.store(synapse_client=self.syn)

        # Test case 2: Table with no columns
        table_name = str(uuid.uuid4())
        empty_column_table = Table(
            name=table_name,
            parent_id=project_model.id,
        )
        empty_column_table = empty_column_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(empty_column_table.id)

        # WHEN/THEN using a table with empty schema should be rejected
        with pytest.raises(
            SynapseHTTPError,
            match=f"400 Client Error: Schema for {empty_column_table.id} is empty.",
        ):
            VirtualTable(
                name=str(uuid.uuid4()),
                parent_id=project_model.id,
                description="Test virtual table",
                defining_sql=f"SELECT * FROM {empty_column_table.id}",
            ).store(synapse_client=self.syn)

        # Test case 3: Invalid SQL syntax
        invalid_sql_virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test virtual table",
            defining_sql="INVALID SQL",
        )

        # WHEN/THEN invalid SQL should be rejected
        with pytest.raises(
            SynapseHTTPError,
            match='400 Client Error: Encountered " <regular_identifier> "INVALID "" '
            "at line 1, column 1.\nWas expecting one of:",
        ):
            invalid_sql_virtual_table.store(synapse_client=self.syn)

    def test_virtual_table_lifecycle(self, base_table_with_columns: Table) -> None:
        # GIVEN a table with columns and a new virtual table
        table = base_table_with_columns
        virtual_table_name = str(uuid.uuid4())
        virtual_table_description = "Test virtual table"
        virtual_table = VirtualTable(
            name=virtual_table_name,
            parent_id=table.parent_id,
            description=virtual_table_description,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN creating the virtual table
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # THEN the virtual table should be created successfully
        assert virtual_table.id is not None

        # WHEN retrieving the virtual table
        retrieved_virtual_table = VirtualTable(id=virtual_table.id).get(
            synapse_client=self.syn
        )

        # THEN the retrieved virtual table should match the created one
        assert retrieved_virtual_table is not None
        assert retrieved_virtual_table.name == virtual_table_name
        assert retrieved_virtual_table.id == virtual_table.id
        assert retrieved_virtual_table.description == virtual_table_description

        # WHEN updating the virtual table attributes
        updated_name = str(uuid.uuid4())
        updated_description = "Updated description"
        updated_sql = f"SELECT test_column FROM {table.id}"

        retrieved_virtual_table.name = updated_name
        retrieved_virtual_table.description = updated_description
        retrieved_virtual_table.defining_sql = updated_sql
        retrieved_virtual_table.store(synapse_client=self.syn)

        # THEN the updates should be reflected when retrieving again
        latest_virtual_table = VirtualTable(id=virtual_table.id).get(
            synapse_client=self.syn
        )
        assert latest_virtual_table.name == updated_name
        assert latest_virtual_table.description == updated_description
        assert latest_virtual_table.defining_sql == updated_sql
        assert latest_virtual_table.id == virtual_table.id  # ID should remain the same

        # WHEN deleting the virtual table
        virtual_table.delete(synapse_client=self.syn)

        # THEN the virtual table should be deleted
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {virtual_table.id} is in trash can.",
        ):
            VirtualTable(id=virtual_table.id).get(synapse_client=self.syn)


class TestVirtualTableWithDataOperations:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def base_table_with_data(self, project_model: Project) -> Table:
        # Create a table with columns and data
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
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
                "city": ["New York", "Boston", "Chicago"],
            }
        )
        table.store_rows(data, synapse_client=self.syn)

        return table

    def test_virtual_table_data_queries(
        self, project_model: Project, base_table_with_data: Table
    ) -> None:
        # GIVEN a table with data and various virtual tables with different SQL transformations
        table = base_table_with_data

        # Test case 1: Basic selection of all data
        virtual_table_all = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table_all = virtual_table_all.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table_all.id)

        # Test case 2: Column selection
        virtual_table_columns = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT name, city FROM {table.id}",
        )
        virtual_table_columns = virtual_table_columns.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table_columns.id)

        # Test case 3: Filtering
        virtual_table_filtered = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id} WHERE age > 25",
        )
        virtual_table_filtered = virtual_table_filtered.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table_filtered.id)

        # Test case 4: Ordering
        virtual_table_ordered = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id} ORDER BY age DESC",
        )
        virtual_table_ordered = virtual_table_ordered.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table_ordered.id)

        # Wait for the virtual tables to be ready
        time.sleep(2)

        # WHEN querying the full-data virtual table
        all_result = virtual_table_all.query(
            f"SELECT * FROM {virtual_table_all.id}", synapse_client=self.syn
        )

        # THEN all data should be returned
        assert len(all_result) == 3
        assert set(all_result["name"].tolist()) == {"Alice", "Bob", "Charlie"}
        assert set(all_result["age"].tolist()) == {30, 25, 35}
        assert set(all_result["city"].tolist()) == {"New York", "Boston", "Chicago"}

        # WHEN querying the column-selection virtual table
        columns_result = virtual_table_columns.query(
            f"SELECT * FROM {virtual_table_columns.id}", synapse_client=self.syn
        )

        # THEN only specified columns should be returned
        assert len(columns_result) == 3
        assert "name" in columns_result.columns
        assert "city" in columns_result.columns
        assert "age" not in columns_result.columns
        assert set(columns_result["name"].tolist()) == {"Alice", "Bob", "Charlie"}
        assert set(columns_result["city"].tolist()) == {"New York", "Boston", "Chicago"}

        # WHEN querying the filtered virtual table
        filtered_result = virtual_table_filtered.query(
            f"SELECT * FROM {virtual_table_filtered.id}", synapse_client=self.syn
        )

        # THEN only filtered rows should be returned
        assert len(filtered_result) == 2
        assert set(filtered_result["name"].tolist()) == {"Alice", "Charlie"}
        assert set(filtered_result["age"].tolist()) == {30, 35}

        # WHEN querying the ordered virtual table
        ordered_result = virtual_table_ordered.query(
            f"SELECT * FROM {virtual_table_ordered.id}", synapse_client=self.syn
        )

        # THEN data should be in the specified order
        assert len(ordered_result) == 3
        assert ordered_result["age"].tolist() == [35, 30, 25]
        assert ordered_result["name"].tolist() == ["Charlie", "Alice", "Bob"]

    def test_virtual_table_data_synchronization(self, project_model: Project) -> None:
        # GIVEN a table with columns but no initial data
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

        # AND a virtual table based on that table
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for the virtual table to be ready
        time.sleep(2)

        # WHEN querying the virtual table with empty source table
        empty_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN no data should be returned
        assert len(empty_result) == 0

        # WHEN adding data to the source table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        table.store_rows(data, synapse_client=self.syn)

        # Wait for the updates to propagate
        time.sleep(2)

        # AND querying the virtual table again
        added_data_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the virtual table should reflect the new data
        assert len(added_data_result) == 2
        assert added_data_result["name"].tolist() == ["Alice", "Bob"]
        assert added_data_result["age"].tolist() == [30, 25]

        # WHEN removing data from the source table
        table.delete_rows(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id}", synapse_client=self.syn
        )

        # Wait for changes to propagate
        time.sleep(2)

        # AND querying the virtual table again
        removed_data_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the virtual table should reflect the removed data
        assert len(removed_data_result) == 0

    def test_virtual_table_sql_updates(
        self, project_model: Project, base_table_with_data: Table
    ) -> None:
        # GIVEN a table with data and a virtual table with initial SQL
        table = base_table_with_data
        virtual_table = VirtualTable(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Wait for the virtual table to be ready
        time.sleep(2)

        # WHEN querying the virtual table with initial SQL
        initial_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN all columns should be present
        assert len(initial_result) == 3
        assert "name" in initial_result.columns
        assert "age" in initial_result.columns
        assert "city" in initial_result.columns

        # WHEN updating the defining SQL to select fewer columns
        virtual_table.defining_sql = f"SELECT name, city FROM {table.id}"
        virtual_table = virtual_table.store(synapse_client=self.syn)

        # Wait for the update to propagate
        time.sleep(2)

        # AND querying the virtual table again
        updated_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the result should reflect the SQL change
        assert len(updated_result) == 3
        assert "name" in updated_result.columns
        assert "city" in updated_result.columns
        assert "age" not in updated_result.columns

        # AND the schema should be updated when retrieving the virtual table
        retrieved_virtual_table = VirtualTable(id=virtual_table.id).get(
            synapse_client=self.syn
        )
        assert "name" in retrieved_virtual_table.columns.keys()
        assert "city" in retrieved_virtual_table.columns.keys()
        assert "age" not in retrieved_virtual_table.columns.keys()

    def test_virtual_table_with_aggregation(self, project_model: Project) -> None:
        # GIVEN a table with data suitable for aggregation
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

        # AND a virtual table with aggregation SQL
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
        time.sleep(3)

        # WHEN querying the aggregation virtual table
        query_result = virtual_table.query(
            f"SELECT * FROM {virtual_table.id}", synapse_client=self.syn
        )

        # THEN the result should contain the aggregated data
        assert len(query_result) == 3

        # Sort the result for consistent testing
        query_result = query_result.sort_values("department").reset_index(drop=True)

        # Verify departments
        assert query_result["department"].tolist() == ["Finance", "HR", "IT"]

        # Verify counts
        assert query_result["employee_count"].tolist() == [1, 2, 2]
