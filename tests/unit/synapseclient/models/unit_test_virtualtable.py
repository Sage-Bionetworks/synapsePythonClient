from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseQueryError
from synapseclient.models import VirtualTable
from synapseclient.models.mixins.table_components import TableStoreMixin


class TestVirtualTable:
    """Tests for VirtualTable validation"""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_store_async_with_join_operation_raises_error(self):
        # GIVEN a VirtualTable with JOIN in the SQL
        virtual_table = VirtualTable(
            name="Test Virtual Table",
            description="A test virtual table",
            parent_id="syn12345",
            defining_sql="""
            SELECT t1.column1, t2.column2
            FROM syn12345 t1
            JOIN syn67890 t2
            ON t1.id = t2.foreign_id
            """,
        )

        with patch.object(TableStoreMixin, "store_async") as mock_super_store_async:
            # WHEN I store the VirtualTable
            # THEN I expect a SynapseQueryError to be raised
            with pytest.raises(
                SynapseQueryError,
                match="VirtualTables do not support JOIN or UNION operations in the defining_sql. "
                "If you need to combine data from multiple tables, "
                "consider using a MaterializedView instead.",
            ):
                await virtual_table.store_async(synapse_client=self.syn)

            # AND I expect the super().store_async method to not be called
            mock_super_store_async.assert_not_called()

    async def test_store_async_with_union_operation_raises_error(self):
        # GIVEN a VirtualTable with UNION in the SQL
        virtual_table = VirtualTable(
            name="Test Virtual Table",
            description="A test virtual table",
            parent_id="syn12345",
            defining_sql="""
            SELECT column1, column2 FROM syn12345
            UNION
            SELECT column1, column2 FROM syn67890
            """,
        )

        with patch.object(TableStoreMixin, "store_async") as mock_super_store_async:
            # WHEN I store the VirtualTable
            # THEN I expect a SynapseQueryError to be raised
            with pytest.raises(
                SynapseQueryError,
                match="VirtualTables do not support JOIN or UNION operations in the defining_sql. "
                "If you need to combine data from multiple tables, "
                "consider using a MaterializedView instead.",
            ):
                await virtual_table.store_async(synapse_client=self.syn)

            # AND I expect the super().store_async method to not be called
            mock_super_store_async.assert_not_called()

    async def test_store_async_with_valid_sql_calls_super_store(self):
        # GIVEN a VirtualTable with valid SQL
        virtual_table = VirtualTable(
            name="Test Virtual Table",
            description="A test virtual table",
            parent_id="syn12345",
            defining_sql="SELECT column1, column2 FROM syn12345",
        )

        with patch.object(TableStoreMixin, "store_async") as mock_super_store_async:
            # Set up the mock to return the virtual_table
            mock_super_store_async.return_value = virtual_table

            # WHEN I store the VirtualTable
            result = await virtual_table.store_async(synapse_client=self.syn)

            # THEN I expect the super().store_async method to be called
            mock_super_store_async.assert_called_once_with(
                dry_run=False, job_timeout=600, synapse_client=self.syn
            )

            # AND I expect the result to be the return value from super().store_async
            assert result == virtual_table

    async def test_store_async_with_join_in_column_value_or_name_succeeds(self):
        # GIVEN a VirtualTable with JOIN or UNION in column name/value but not as SQL keywords
        virtual_table = VirtualTable(
            name="Test Virtual Table",
            description="A test virtual table",
            parent_id="syn12345",
            defining_sql="""
            SELECT
              column1 as join_column,
              REUNION as text_value,
              concat(column1, 'joined_text') as concat_column
            FROM syn12345
            WHERE column2 = 'REUNION' AND column3 LIKE '%JOIN%'
            """,
        )

        with patch.object(TableStoreMixin, "store_async") as mock_super_store_async:
            # Set up the mock to return the virtual_table
            mock_super_store_async.return_value = virtual_table

            # WHEN I store the VirtualTable
            result = await virtual_table.store_async(synapse_client=self.syn)

            # THEN I expect the super().store_async method to be called
            mock_super_store_async.assert_called_once_with(
                dry_run=False, job_timeout=600, synapse_client=self.syn
            )

            # AND I expect the result to be the return value from super().store_async
            assert result == virtual_table
