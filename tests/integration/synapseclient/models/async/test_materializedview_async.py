import asyncio
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Column, ColumnType, MaterializedView, Project, Table


class TestMaterializedViewWithoutData:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_empty_materialized_view(self, project_model: Project) -> None:
        # GIVEN a MaterializedView with an empty definingSQL
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            description="Test materialized view",
            parent_id=project_model.id,
            defining_sql="",
        )

        # WHEN I try to store the materialized view
        with pytest.raises(SynapseHTTPError) as e:
            await materialized_view.store_async(synapse_client=self.syn)

        # THEN a 400 Client Error should be raised
        assert (
            "400 Client Error: The definingSQL of the materialized view is required "
            "and must not be the empty string." in str(e.value)
        )

    async def test_create_materialized_view_without_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a table with columns
        table_name = str(uuid.uuid4())
        table = Table(
            name=table_name,
            parent_id=project_model.id,
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view_description = "Test materialized view"
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            description=materialized_view_description,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN I try to store the materialized view
        with pytest.raises(SynapseHTTPError) as e:
            await materialized_view.store_async(synapse_client=self.syn)

        # THEN a 400 Client Error should be raised
        assert f"400 Client Error: Schema for {table.id} is empty." in str(e.value)

    async def test_create_materialized_view_with_columns(
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

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view_description = "Test materialized view"
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            description=materialized_view_description,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN I store the materialized view
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # THEN the materialized view should be created
        assert materialized_view.id is not None

        # AND I can retrieve that materialized view from Synapse
        new_materialized_view_instance = await MaterializedView(
            id=materialized_view.id
        ).get_async(synapse_client=self.syn)
        assert new_materialized_view_instance is not None
        assert new_materialized_view_instance.name == materialized_view_name
        assert new_materialized_view_instance.id == materialized_view.id
        assert (
            new_materialized_view_instance.description == materialized_view_description
        )

    async def test_create_materialized_view_with_invalid_sql(
        self, project_model: Project
    ) -> None:
        # GIVEN a materialized view with invalid SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view_description = "Test materialized view"
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            description=materialized_view_description,
            defining_sql="INVALID SQL",
        )

        # WHEN I store the materialized view
        with pytest.raises(SynapseHTTPError) as e:
            await materialized_view.store_async(synapse_client=self.syn)

        # THEN the materialized view should not be created
        assert (
            '400 Client Error: Encountered " <regular_identifier> "INVALID "" '
            "at line 1, column 1.\nWas expecting one of:" in str(e.value)
        )

    async def test_update_materialized_view_attributes(
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view
        original_materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            description="Test materialized view",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        original_materialized_view = await original_materialized_view.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(original_materialized_view.id)

        # AND I update attributes of the materialized view
        updated_materialized_view = await MaterializedView(
            id=original_materialized_view.id
        ).get_async(synapse_client=self.syn)
        updated_materialized_view.name = str(uuid.uuid4())
        updated_materialized_view.description = "Updated description"
        updated_materialized_view.defining_sql = f"SELECT test_column FROM {table.id}"
        updated_materialized_view = await updated_materialized_view.store_async(
            synapse_client=self.syn
        )

        # AND I retrieve the materialized view with its original id
        retrieved_materialized_view = await MaterializedView(
            id=original_materialized_view.id
        ).get_async(synapse_client=self.syn)

        # THEN the materialized view should be updated
        assert retrieved_materialized_view is not None
        assert retrieved_materialized_view.name == updated_materialized_view.name
        assert (
            retrieved_materialized_view.description
            == updated_materialized_view.description
        )
        assert (
            retrieved_materialized_view.defining_sql
            == updated_materialized_view.defining_sql
        )

        # AND all versions should have the same id
        assert (
            retrieved_materialized_view.id
            == updated_materialized_view.id
            == original_materialized_view.id
        )

    async def test_delete_materialized_view(self, project_model: Project) -> None:
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a materialized view
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            description="Test materialized view",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)
        assert materialized_view.id is not None

        # WHEN I delete the materialized view
        await materialized_view.delete_async(synapse_client=self.syn)

        # THEN the materialized view should be deleted
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

    async def test_query_data_from_materialized_view(
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
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I update the defining SQL of the materialized view
        materialized_view.defining_sql = f"SELECT name FROM {table.id}"
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)

        # AND I query the updated materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )
        await asyncio.sleep(5)
        # Since the materialized view is eventually consistent, we need to query twice
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the updated SQL
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert "age" not in query_result.columns

        # AND the column is not present the next time the `materialized_view` is retrieved
        retrieved_materialized_view = await MaterializedView(
            id=materialized_view.id
        ).get_async(synapse_client=self.syn)
        assert "age" not in retrieved_materialized_view.columns.keys()
        assert "name" in retrieved_materialized_view.columns.keys()

    async def test_materialized_view_updates_with_table_data(
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

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view before adding data to the table
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN no data should be returned
        assert len(query_result) == 0

        # AND WHEN I add data to the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        # AND I query the materialized view again
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )
        await asyncio.sleep(5)
        # Since the materialized view is eventually consistent, we need to query twice
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the table data
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"].tolist() == [30, 25]

    async def test_materialized_view_reflects_table_data_removal(
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the table data
        assert len(query_result) == 2
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"].tolist() == [30, 25]

        # AND WHEN I remove data from the table
        await table.delete_rows_async(
            query=f"SELECT ROW_ID, ROW_VERSION FROM {table.id}", synapse_client=self.syn
        )

        # Ensure the table contains no rows after deletion
        query_result = await table.query_async(
            f"SELECT * FROM {table.id}", synapse_client=self.syn
        )
        assert len(query_result) == 0

        # AND I query the materialized view again
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )
        await asyncio.sleep(5)
        # Since the materialized view is eventually consistent, we need to query twice
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN no data should be returned
        assert len(query_result) == 0

    async def test_query_part_mask_async(self, project_model: Project) -> None:
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
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Insert data into the table
        data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        await table.store_rows_async(data, synapse_client=self.syn)

        # AND a materialized view that uses the table in its defining SQL
        materialized_view_name = str(uuid.uuid4())
        materialized_view = MaterializedView(
            name=materialized_view_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view using query_part_mask_async
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        LAST_UPDATED_ON = 0x80

        # Combine the part mask values using bitwise OR
        part_mask = QUERY_RESULTS | QUERY_COUNT | LAST_UPDATED_ON

        query_result = await materialized_view.query_part_mask_async(
            query=f"SELECT * FROM {materialized_view.id}",
            part_mask=part_mask,
            synapse_client=self.syn,
        )

        # THEN the data should match the table data
        assert query_result is not None
        assert len(query_result.result) == 2
        assert query_result.result["name"].tolist() == ["Alice", "Bob"]
        assert query_result.result["age"].tolist() == [30, 25]

        assert query_result.count == 2
        assert query_result.last_updated_on is not None

    async def test_materialized_view_with_left_join(
        self, project_model: Project
    ) -> None:
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
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        # Insert data into the tables
        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame({"unique_identifier": [1, 3], "age": [30, 40]})
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # AND a materialized view with a LEFT JOIN
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT t1.unique_identifier as unique_identifier, t1.name as name, "
                f"t2.age as age FROM {table1.id} t1 LEFT JOIN {table2.id} t2 "
                f"ON t1.unique_identifier = t2.unique_identifier"
            ),
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the LEFT JOIN result
        assert len(query_result) == 2
        assert query_result["unique_identifier"].tolist() == [1, 2]
        assert query_result["name"].tolist() == ["Alice", "Bob"]
        assert query_result["age"][0] == 30
        assert pd.isna(query_result["age"][1])

    async def test_materialized_view_with_right_join(
        self, project_model: Project
    ) -> None:
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
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        # Insert data into the tables
        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame({"unique_identifier": [1, 3], "age": [30, 40]})
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # AND a materialized view with a RIGHT JOIN
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT t2.unique_identifier as unique_identifier, t1.name as name, "
                f"t2.age as age FROM {table1.id} t1 RIGHT JOIN {table2.id} t2 "
                f"ON t1.unique_identifier = t2.unique_identifier"
            ),
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the RIGHT JOIN result
        assert len(query_result) == 2
        assert query_result["unique_identifier"].tolist() == [1, 3]
        assert query_result["name"][0] == "Alice"
        assert pd.isna(query_result["name"][1])
        assert query_result["age"].tolist() == [30, 40]

    async def test_materialized_view_with_inner_join(
        self, project_model: Project
    ) -> None:
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
                Column(name="age", column_type=ColumnType.INTEGER),
            ],
        )
        table2 = await table2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table2.id)

        # Insert data into the tables
        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame({"unique_identifier": [1, 3], "age": [30, 40]})
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # AND a materialized view with an INNER JOIN
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT t1.unique_identifier as unique_identifier, t1.name as name, "
                f"t2.age as age FROM {table1.id} t1 INNER JOIN {table2.id} t2 "
                f"ON t1.unique_identifier = t2.unique_identifier"
            ),
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the INNER JOIN result
        assert len(query_result) == 1
        assert query_result["unique_identifier"].tolist() == [1]
        assert query_result["name"].tolist() == ["Alice"]
        assert query_result["age"].tolist() == [30]

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

        # Insert data into the tables
        data1 = pd.DataFrame({"unique_identifier": [1, 2], "name": ["Alice", "Bob"]})
        await table1.store_rows_async(data1, synapse_client=self.syn)

        data2 = pd.DataFrame(
            {"unique_identifier": [3, 4], "name": ["Charlie", "Diana"]}
        )
        await table2.store_rows_async(data2, synapse_client=self.syn)

        # AND a materialized view with a UNION
        materialized_view = MaterializedView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=(
                f"SELECT unique_identifier as unique_identifier, name as name "
                f"FROM {table1.id} UNION SELECT unique_identifier as unique_identifier, "
                f"name as name FROM {table2.id}"
            ),
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # WHEN I query the materialized view
        query_result = await materialized_view.query_async(
            f"SELECT * FROM {materialized_view.id}", synapse_client=self.syn
        )

        # THEN the data should match the UNION result
        assert len(query_result) == 4
        assert query_result["unique_identifier"].tolist() == [1, 2, 3, 4]
        assert query_result["name"].tolist() == ["Alice", "Bob", "Charlie", "Diana"]
