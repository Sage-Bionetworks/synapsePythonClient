import asyncio
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Column, ColumnType, MaterializedView, Project, Table
from tests.integration import QUERY_TIMEOUT_SEC


class TestMaterializedViewBasics:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_empty_defining_sql_validation(self, project_model: Project) -> None:
        # GIVEN a MaterializedView with an empty defining SQL
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            description="Test materialized view",
            parent_id=project_model.id,
            defining_sql="",
        )

        # WHEN storing the materialized view
        # THEN a 400 Client Error should be raised
        with pytest.raises(SynapseHTTPError) as e:
            await materialized_view.store_async(synapse_client=self.syn)

        assert (
            "400 Client Error: The definingSQL of the materialized view is required "
            "and must not be the empty string." in str(e.value)
        )

    async def test_table_without_columns_validation(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with no columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            description="Test materialized view",
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN trying to store the materialized view
        # THEN a 400 Client Error should be raised
        with pytest.raises(SynapseHTTPError) as e:
            await materialized_view.store_async(synapse_client=self.syn)

        assert f"400 Client Error: Schema for {table.id} is empty." in str(e.value)

    async def test_invalid_sql_validation(self, project_model: Project) -> None:
        # GIVEN a materialized view with invalid SQL
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test materialized view",
            defining_sql="INVALID SQL",
        )

        # WHEN storing the materialized view
        # THEN a 400 Client Error should be raised
        with pytest.raises(SynapseHTTPError) as e:
            await materialized_view.store_async(synapse_client=self.syn)

        assert (
            '400 Client Error: Encountered " <regular_identifier> "INVALID "" '
            "at line 1, column 1.\nWas expecting one of:" in str(e.value)
        )

    async def test_create_and_retrieve_materialized_view(
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN creating a materialized view
        materialized_view_name = str(uuid.uuid4())
        materialized_view_description = "Test materialized view"
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            description=materialized_view_description,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # THEN the materialized view should be created
        assert materialized_view.id is not None

        # AND when retrieving the materialized view
        new_materialized_view = await MaterializedView(
            id=materialized_view.id
        ).get_async(synapse_client=self.syn)

        # THEN it should have the expected properties
        assert new_materialized_view is not None
        assert new_materialized_view.name == materialized_view_name
        assert new_materialized_view.id == materialized_view.id
        assert new_materialized_view.description == materialized_view_description

    async def test_update_materialized_view(self, project_model: Project) -> None:
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Original description",
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN updating the materialized view properties
        new_name = str(uuid.uuid4())
        new_description = "Updated description"
        new_sql = f"SELECT test_column FROM {table.id}"

        materialized_view.name = new_name
        materialized_view.description = new_description
        materialized_view.defining_sql = new_sql

        updated_materialized_view = await materialized_view.store_async(
            synapse_client=self.syn
        )

        # THEN the updated properties should be reflected when retrieved
        retrieved_materialized_view = await MaterializedView(
            id=materialized_view.id
        ).get_async(synapse_client=self.syn)

        assert retrieved_materialized_view.name == new_name
        assert retrieved_materialized_view.description == new_description
        assert retrieved_materialized_view.defining_sql == new_sql

    async def test_delete_materialized_view(self, project_model: Project) -> None:
        # GIVEN a table with columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN deleting the materialized view
        await materialized_view.delete_async(synapse_client=self.syn)

        # THEN the materialized view should not be retrievable
        with pytest.raises(
            SynapseHTTPError,
            match=(f"404 Client Error: Entity {materialized_view.id} is in trash can."),
        ):
            await MaterializedView(id=materialized_view.id).get_async(
                synapse_client=self.syn
            )


class TestMaterializedViewWithData:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def setup_table_with_data(self, project_model: Project):
        """Helper method to create a table with data for testing"""
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        return table

    async def test_query_materialized_view(self, project_model: Project) -> None:
        # GIVEN a table with data
        table = await self.setup_table_with_data(project_model)

        # AND a materialized view based on the table
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN querying the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the query results should match the table data
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"].tolist() == [30, 25]

    async def test_update_defining_sql(self, project_model: Project) -> None:
        # GIVEN a table with data
        table = await self.setup_table_with_data(project_model)

        # AND a materialized view based on the table
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN updating the defining SQL
        materialized_view.defining_sql = f"SELECT name FROM {table.id}"
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)

        # AND querying the materialized view (with delay for eventual consistency)
        await asyncio.sleep(5)
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the query results should reflect the updated SQL
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert "age" not in query_result.columns

        # AND the column structure should be updated in the view metadata
        retrieved_view = await MaterializedView(id=materialized_view.id).get_async(
            synapse_client=self.syn
        )
        assert "age" not in retrieved_view.columns.keys()
        assert "name" in retrieved_view.columns.keys()

    async def test_materialized_view_reflects_table_updates(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with columns but no data
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view based on the table
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN querying before adding data
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN no data should be returned
        assert len(query_result) == 0

        # WHEN adding data to the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        # AND querying again (with delay for eventual consistency)
        await asyncio.sleep(5)
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )
        await asyncio.sleep(5)
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the query results should reflect the added data
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"].tolist() == [30, 25]

    async def test_materialized_view_reflects_table_data_removal(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with data
        table = await self.setup_table_with_data(project_model)

        # AND a materialized view based on the table
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN removing data from the table
        await table.delete_rows_async(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id}", synapse_client=self.syn
        )

        # AND querying the materialized view (with delay for eventual consistency)
        await asyncio.sleep(5)
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the query results should reflect the removed data
        assert len(query_result) == 0

    async def test_query_part_mask_async(self, project_model: Project) -> None:
        # GIVEN a table with data
        table = await self.setup_table_with_data(project_model)

        # AND a materialized view based on the table
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN querying with part_mask
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | LAST_UPDATED_ON

        query_result = await materialized_view.query_part_mask_async(
            query=f"SELECT * FROM {materialized_view.id}",
            part_mask=part_mask,
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the result should contain the specified parts
        assert query_result is not None
        assert len(query_result.result) == 2
        assert query_result.result["name"].tolist() == ["Alice", "Bob"]
        assert query_result.result["age"].tolist() == [30, 25]
        assert query_result.count == 2
        assert query_result.last_updated_on is not None

    async def test_materialized_view_with_left_join(
        self, project_model: Project
    ) -> None:
        # GIVEN two tables with related data
        table1 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="name", column_type=ColumnType.STRING),
            ],
        )
        table1 = await table1.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table1.id)

        table2 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame({"unique_identifier": [1, 3], "age": [30, 40]})
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # WHEN creating a materialized view with a LEFT JOIN
        left_join_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT t1.unique_identifier as unique_identifier, t1.name as name, "
                f"t2.age as age FROM {table1.id} t1 LEFT JOIN {table2.id} t2 "
                f"ON t1.unique_identifier = t2.unique_identifier"
            ),
        )
        left_join_view = await left_join_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(left_join_view.id)

        # AND querying the view
        result = await left_join_view.query_async(
            f"SELECT * FROM {left_join_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the results should match the expected LEFT JOIN result
        assert len(result) == 2
        assert result["unique_identifier"].tolist() == [1, 2]
        assert result["name"].tolist() == ["Alice", "Bob"]
        assert result["age"][0] == 30
        assert pd.isna(result["age"][1])

    async def test_materialized_view_with_right_join(
        self, project_model: Project
    ) -> None:
        # GIVEN two tables with related data
        table1 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="name", column_type=ColumnType.STRING),
            ],
        )
        table1 = await table1.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table1.id)

        table2 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame({"unique_identifier": [1, 3], "age": [30, 40]})
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # WHEN creating a materialized view with a RIGHT JOIN
        right_join_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT t2.unique_identifier as unique_identifier, t1.name as name, "
                f"t2.age as age FROM {table1.id} t1 RIGHT JOIN {table2.id} t2 "
                f"ON t1.unique_identifier = t2.unique_identifier"
            ),
        )
        right_join_view = await right_join_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(right_join_view.id)

        # AND querying the view
        result = await right_join_view.query_async(
            f"SELECT * FROM {right_join_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the results should match the expected RIGHT JOIN result
        assert len(result) == 2
        assert result["unique_identifier"].tolist() == [1, 3]
        assert result["name"][0] == "Alice"
        assert pd.isna(result["name"][1])
        assert result["age"].tolist() == [30, 40]

    async def test_materialized_view_with_inner_join(
        self, project_model: Project
    ) -> None:
        # GIVEN two tables with related data
        table1 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="name", column_type=ColumnType.STRING),
            ],
        )
        table1 = await table1.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table1.id)

        table2 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame({"unique_identifier": [1, 3], "age": [30, 40]})
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # WHEN creating a materialized view with an INNER JOIN
        inner_join_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT t1.unique_identifier as unique_identifier, t1.name as name, "
                f"t2.age as age FROM {table1.id} t1 INNER JOIN {table2.id} t2 "
                f"ON t1.unique_identifier = t2.unique_identifier"
            ),
        )
        inner_join_view = await inner_join_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(inner_join_view.id)

        # AND querying the view
        result = await inner_join_view.query_async(
            f"SELECT * FROM {inner_join_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the results should match the expected INNER JOIN result
        assert len(result) == 1
        assert result["unique_identifier"].tolist() == [1]
        assert result["name"].tolist() == ["Alice"]
        assert result["age"].tolist() == [30]

    async def test_materialized_view_with_union(self, project_model: Project) -> None:
        # GIVEN two tables with data
        table1 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="name", column_type=ColumnType.STRING),
            ],
        )
        table1 = await table1.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table1.id)

        table2 = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="unique_identifier", column_type=ColumnType.INTEGER),
                Column(name="name", column_type=ColumnType.STRING),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame(
            {"unique_identifier": [3, 4], "name": ["Charlie", "Diana"]}
        )
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # WHEN creating a materialized view with a UNION
        union_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT unique_identifier as unique_identifier, name as name "
                f"FROM {table1.id} UNION SELECT unique_identifier as unique_identifier, "
                f"name as name FROM {table2.id}"
            ),
        )
        union_view = await union_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(union_view.id)

        # AND querying the view
        result = await union_view.query_async(
            f"SELECT * FROM {union_view.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the results should match the expected UNION result
        assert len(result) == 4
        assert result["unique_identifier"].tolist() == [1, 2, 3, 4]
        assert result["name"].tolist() == ["Alice", "Bob", "Charlie", "Diana"]
