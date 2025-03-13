import pytest
import os
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from unittest.mock import patch
from collections import OrderedDict
from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.models import Column, ColumnType, ColumnChange
from synapseclient.models.mixins.table_components import (
    TableStoreMixin,
    GetMixin,
    FailureStrategy,
)

POST_COLUMNS_PATCH = "synapseclient.models.mixins.table_components.post_columns"
GET_PATCH = "synapseclient.models.mixins.table_components.GetMixin.get_async"


class TestTableStoreMixin:
    @dataclass
    class TestClass(TableStoreMixin, GetMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        has_columns_changed: Optional[bool] = None
        has_changed: Optional[bool] = None
        columns: Optional[List[Column]] = None
        _columns_to_delete: Optional[Dict[str, Column]] = None
        _last_persistent_instance: Optional[Any] = None

        def _set_last_persistent_instance(self) -> None:
            """Create a copy of self as the last persistent instance"""
            self._last_persistent_instance = self.__class__(**self.__dict__)

        def to_synapse_request(self) -> Any:
            return {
                "id": None,
                "name": "test_table",
                "columns": {
                    "col1": Column(
                        id="id1",
                        name="col1",
                        column_type=ColumnType.STRING,
                        facet_type=None,
                        default_value=None,
                        maximum_size=None,
                        maximum_list_length=None,
                        enum_values=None,
                        json_sub_columns=None,
                    )
                },
            }

        def fill_from_dict(self, entity: Any, set_annotations: bool = True) -> None:
            self.__dict__.update(entity)

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_generate_schema_change_request_no_changes(self, syn: Synapse):
        # GIVEN a TestClass instance where has_columns_changed is False
        test_instance = self.TestClass(has_columns_changed=False, columns=[])
        # WHEN the _generate_schema_change_request method is called
        # THEN the method should return None
        assert (
            await test_instance._generate_schema_change_request(synapse_client=syn)
            is None
        )

    async def test_generate_schema_change_request_no_columns(self):
        # GIVEN a TestClass instance where has_columns_changed is True
        # AND columns is None
        test_instance = self.TestClass(has_columns_changed=True, columns=None)
        # WHEN the _generate_schema_change_request method is called
        # THEN the method should return None
        assert await test_instance._generate_schema_change_request() is None

    async def test_generate_schema_change_request_columns_changed(self):
        # GIVEN a TestClass instance where has_columns_changed is True AND columns have changes
        test_instance = self.TestClass(
            has_columns_changed=True,
            id="syn123",
            name="test_table",
            columns={
                "test_column_1": Column(
                    name="test_column_1",
                    column_type=ColumnType.STRING,
                    id="col1",
                    _last_persistent_instance=Column(
                        name="test_column_1",
                        column_type=ColumnType.STRING,
                        id="old_col1",
                    ),
                ),
                "test_column_2": Column(
                    name="test_column_2",
                    column_type=ColumnType.STRING,
                    id="col2",
                ),
            },
        )

        with patch(POST_COLUMNS_PATCH) as mock_post_columns:
            # WHEN the _generate_schema_change_request method is called
            request = await test_instance._generate_schema_change_request(
                synapse_client=self.syn
            )

            # THEN post_columns should be called with the changed column
            mock_post_columns.assert_awaited_once_with(
                columns=[
                    Column(
                        name="test_column_1", column_type=ColumnType.STRING, id="col1"
                    ),
                    Column(
                        name="test_column_2", column_type=ColumnType.STRING, id="col2"
                    ),
                ],
                synapse_client=self.syn,
            )

            # AND the request should contain the correct changes
            assert request.entity_id == "syn123"
            assert len(request.changes) == 2
            assert request.changes[0].old_column_id == "old_col1"
            assert request.changes[0].new_column_id == "col1"
            assert request.changes[1].old_column_id is None
            assert request.changes[1].new_column_id == "col2"
            assert request.ordered_column_ids == ["col1", "col2"]

    async def test_generate_schema_change_request_with_column_deletion(self):
        # GIVEN a TestClass instance with columns to delete
        test_instance = self.TestClass(
            has_columns_changed=True,
            id="syn123",
            name="test_table",
            columns={
                "remaining_column": Column(
                    name="remaining_column", column_type=ColumnType.STRING, id="col1"
                )
            },
        )
        test_instance._columns_to_delete = {
            "deleted_col_id": Column(
                name="deleted_column",
                column_type=ColumnType.STRING,
                id="deleted_col_id",
            )
        }
        test_instance._last_persistent_instance = self.TestClass(
            columns={
                "remaining_column": Column(
                    name="remaining_column", column_type=ColumnType.STRING, id="col1"
                ),
                "deleted_column": Column(
                    name="deleted_column",
                    column_type=ColumnType.STRING,
                    id="deleted_col_id",
                ),
            }
        )

        # Add patch for post_columns
        with patch(POST_COLUMNS_PATCH) as mock_post_columns:
            # WHEN the _generate_schema_change_request method is called
            request = await test_instance._generate_schema_change_request(
                synapse_client=self.syn
            )
            # THEN mock_post_columns should be called with the correct arguments
            mock_post_columns.assert_awaited_once_with(
                columns=[
                    Column(
                        name="remaining_column",
                        column_type=ColumnType.STRING,
                        id="col1",
                    ),
                ],
                synapse_client=self.syn,
            )
            # AND the request should contain the deletion
            assert request.entity_id == "syn123"
            assert len(request.changes) == 2
            assert request.changes[0].old_column_id is None
            assert request.changes[0].new_column_id == "col1"
            assert request.changes[1].old_column_id == "deleted_col_id"
            assert request.changes[1].new_column_id is None
            assert request.ordered_column_ids == ["col1"]

    async def test_generate_schema_change_request_column_order_change(self):
        # GIVEN a TestClass instance where column order has changed
        test_instance = self.TestClass(
            has_columns_changed=True,
            id="syn123",
            name="test_table",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="id2"),
            },
        )
        test_instance._last_persistent_instance = self.TestClass(
            columns={
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="id2"),
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            }
        )

        # Add patch for post_columns
        with patch(POST_COLUMNS_PATCH) as mock_post_columns:
            # WHEN the _generate_schema_change_request method is called
            request = await test_instance._generate_schema_change_request(
                synapse_client=self.syn
            )

            # THEN mock_post_columns should be called with the correct arguments
            mock_post_columns.assert_awaited_once_with(
                columns=[
                    Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                    Column(name="col2", column_type=ColumnType.STRING, id="id2"),
                ],
                synapse_client=self.syn,
            )

            # AND the request should reflect the new column order
            assert request.entity_id == "syn123"
            assert len(request.changes) == 2
            assert request.changes[0].old_column_id is None
            assert request.changes[0].new_column_id == "id1"
            assert request.changes[1].old_column_id is None
            assert request.changes[1].new_column_id == "id2"
            assert request.ordered_column_ids == ["id1", "id2"]

    async def test_generate_schema_change_request_with_dry_run(self):
        # GIVEN a TestClass instance with column changes
        test_instance = self.TestClass(
            has_columns_changed=True,
            id="syn123",
            name="test_table",
            columns={
                "test_column": Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    id="col1",
                )
            },
        )

        with patch(POST_COLUMNS_PATCH) as mock_post_columns:
            # WHEN the _generate_schema_change_request method is called with dry_run=True
            request = await test_instance._generate_schema_change_request(
                synapse_client=self.syn, dry_run=True
            )

            # THEN post_columns should not be called
            mock_post_columns.assert_not_awaited()

            # AND the request should contain no changes
            assert request.entity_id == "syn123"
            assert len(request.changes) == 0

    async def test_store_async_new_entity(self):
        # GIVEN a new TestClass instance
        test_instance = self.TestClass(
            name="test_table",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
            has_columns_changed=True,
            has_changed=True,
        )

        with (
            patch(
                "synapseclient.models.mixins.table_components.get_id", return_value=None
            ) as mock_get_id,
            patch(
                "synapseclient.models.mixins.table_components.post_entity_bundle2_create",
                return_value={"entity": {"id": "syn123", "name": "test_table"}},
            ) as mock_post_create_entity_bundle2_create,
            patch(
                "synapseclient.models.mixins.table_components.store_entity_components",
                return_value=False,
            ) as mock_store_entity_components,
            patch(POST_COLUMNS_PATCH) as mock_post_columns,
            patch(
                "synapseclient.models.mixins.table_components.TableUpdateTransaction.send_job_and_wait_async"
            ) as mock_send_job_and_wait_async,
            patch.object(
                self.TestClass, "get_async", return_value=test_instance
            ) as mock_get_async,
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(synapse_client=self.syn)

            # THEN we expect the following to be called:
            mock_get_id.assert_awaited_once_with(
                entity=test_instance, synapse_client=self.syn, failure_strategy=None
            )
            mock_post_create_entity_bundle2_create.assert_awaited_once_with(
                request=test_instance.to_synapse_request(), synapse_client=self.syn
            )
            mock_store_entity_components.assert_awaited_once_with(
                root_resource=test_instance,
                synapse_client=self.syn,
                failure_strategy=FailureStrategy.RAISE_EXCEPTION,
            )
            mock_post_columns.assert_awaited_once_with(
                columns=[Column(name="col1", column_type=ColumnType.STRING, id="id1")],
                synapse_client=self.syn,
            )
            mock_send_job_and_wait_async.assert_awaited_once_with(
                synapse_client=self.syn, timeout=600
            )
            mock_get_async.assert_awaited_once_with(
                include_columns=False, synapse_client=self.syn
            )
            # THEN the resulting instance should have the expected attributes
            assert result.id == "syn123"
            assert result.name == "test_table"
            assert result.has_columns_changed is True
            assert result.has_changed is True
            assert result.columns == OrderedDict(
                {
                    "col1": Column(
                        id="id1",
                        name="col1",
                        column_type=ColumnType.STRING,
                        facet_type=None,
                        default_value=None,
                        maximum_size=None,
                        maximum_list_length=None,
                        enum_values=None,
                        json_sub_columns=None,
                    )
                }
            )

    async def test_store_async_unchanged_entity(self):
        # GIVEN a TestClass instance that matches its last persistent instance
        test_instance = self.TestClass(
            id="syn123",
            name="test_table",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
            has_columns_changed=False,
            has_changed=False,
        )
        # Set the last persistent instance to match the current state
        test_instance._set_last_persistent_instance()

        with (
            patch(
                "synapseclient.models.mixins.table_components.get_id", return_value=None
            ) as mock_get_id,
            patch(
                "synapseclient.models.mixins.table_components.post_entity_bundle2_create",
                return_value={"entity": {"id": "syn123", "name": "test_table"}},
            ) as mock_post_create_entity_bundle2_create,
            patch(
                "synapseclient.models.mixins.table_components.store_entity_components",
                return_value=False,
            ) as mock_store_entity_components,
            patch(POST_COLUMNS_PATCH) as mock_post_columns,
            patch(
                "synapseclient.models.mixins.table_components.TableUpdateTransaction.send_job_and_wait_async"
            ) as mock_send_job_and_wait_async,
            patch.object(
                self.TestClass, "get_async", return_value=test_instance
            ) as mock_get_async,
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(synapse_client=self.syn)

            # THEN we expect none of the methods to be called except store_entity_components
            mock_get_id.assert_not_awaited()
            mock_post_create_entity_bundle2_create.assert_not_awaited()
            mock_store_entity_components.assert_awaited_once_with(
                root_resource=test_instance,
                synapse_client=self.syn,
                failure_strategy=FailureStrategy.RAISE_EXCEPTION,
            )
            mock_post_columns.assert_not_awaited()
            mock_send_job_and_wait_async.assert_not_awaited()
            mock_get_async.assert_not_awaited()

            # AND the result should be the same unchanged instance
            assert result == test_instance
            assert result.id == "syn123"
            assert result.name == "test_table"
            assert result.has_columns_changed is False
            assert result.has_changed is False

    async def test_store_async_with_dry_run(self):
        # GIVEN a TestClass instance with changes
        test_instance = self.TestClass(
            id="syn123",
            name="test_table",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
            has_columns_changed=True,
            has_changed=True,
        )

        with (
            patch(
                "synapseclient.models.mixins.table_components.get_id",
                return_value="syn123",
            ) as mock_get_id,
            patch(
                "synapseclient.models.mixins.table_components.post_entity_bundle2_create"
            ) as mock_post_create_entity_bundle2_create,
            patch(
                "synapseclient.models.mixins.table_components.store_entity_components"
            ) as mock_store_entity_components,
            patch(POST_COLUMNS_PATCH) as mock_post_columns,
            patch(
                "synapseclient.models.mixins.table_components.TableUpdateTransaction.send_job_and_wait_async"
            ) as mock_send_job_and_wait_async,
            patch.object(self.TestClass, "get_async") as mock_get_async,
            patch(
                "synapseclient.models.mixins.table_components.merge_dataclass_entities",
            ) as mock_merge_dataclass_entities,
        ):
            # WHEN store_async is awaited with dry_run=True
            result = await test_instance.store_async(
                synapse_client=self.syn, dry_run=True
            )

            # THEN we expect only get_id to be called
            mock_get_id.assert_awaited_once_with(
                entity=test_instance, synapse_client=self.syn, failure_strategy=None
            )

            # AND no other operations should be performed
            mock_merge_dataclass_entities.assert_called_once()
            mock_post_create_entity_bundle2_create.assert_not_awaited()
            mock_store_entity_components.assert_not_awaited()
            mock_post_columns.assert_not_awaited()
            mock_send_job_and_wait_async.assert_not_awaited()
            mock_get_async.assert_awaited_once_with(
                include_columns=True, synapse_client=self.syn
            )

            # AND the result should be the same instance
            assert result == test_instance
            assert result.id == "syn123"
            assert result.name == "test_table"
            assert result.has_columns_changed is True
            assert result.has_changed is True
