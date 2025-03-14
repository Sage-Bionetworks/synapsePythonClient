import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.api import ViewEntityType, ViewTypeMask
from synapseclient.core.utils import MB
from synapseclient.models import Activity, Column, ColumnChange, ColumnType
from synapseclient.models.mixins.table_components import (
    ColumnMixin,
    DeleteMixin,
    FailureStrategy,
    GetMixin,
    QueryMixin,
    SnapshotRequest,
    TableDeleteRowMixin,
    TableStoreMixin,
    TableStoreRowMixin,
    TableUpdateTransaction,
    TableUpsertMixin,
    ViewSnapshotMixin,
    ViewStoreMixin,
    ViewUpdateMixin,
)
from synapseclient.table import TableQueryResult

POST_COLUMNS_PATCH = "synapseclient.models.mixins.table_components.post_columns"
GET_ID_PATCH = "synapseclient.models.mixins.table_components.get_id"
POST_ENTITY_BUNDLE2_CREATE_PATCH = (
    "synapseclient.models.mixins.table_components.post_entity_bundle2_create"
)
STORE_ENTITY_COMPONENTS_PATCH = (
    "synapseclient.models.mixins.table_components.store_entity_components"
)
SEND_JOB_AND_WAIT_ASYNC_PATCH = "synapseclient.models.mixins.table_components.TableUpdateTransaction.send_job_and_wait_async"
GET_DEFAULT_COLUMNS_PATCH = (
    "synapseclient.models.mixins.table_components.get_default_columns"
)
TABLE_STORE_MIXIN_PATCH = (
    "synapseclient.models.mixins.table_components.TableStoreMixin.store_async"
)
DELETE_ENTITY_PATCH = "synapseclient.models.mixins.table_components.delete_entity"
_UPSERT_ROWS_ASYNC_PATCH = (
    "synapseclient.models.mixins.table_components._upsert_rows_async"
)


class TestTableStoreMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

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

    async def test_generate_schema_change_request_no_changes(self):
        # GIVEN a TestClass instance where has_columns_changed is False
        test_instance = self.TestClass(has_columns_changed=False, columns=[])
        # WHEN the _generate_schema_change_request method is called
        # THEN the method should return None
        assert (
            await test_instance._generate_schema_change_request(synapse_client=self.syn)
            is None
        )

    async def test_generate_schema_change_request_no_columns(self):
        # GIVEN a TestClass instance where has_columns_changed is True
        # AND columns is None
        test_instance = self.TestClass(has_columns_changed=True, columns=None)
        # WHEN the _generate_schema_change_request method is called
        # THEN the method should return None
        assert (
            await test_instance._generate_schema_change_request(synapse_client=self.syn)
            is None
        )

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
            patch(GET_ID_PATCH, return_value=None) as mock_get_id,
            patch(
                POST_ENTITY_BUNDLE2_CREATE_PATCH,
                return_value={"entity": {"id": "syn123", "name": "test_table"}},
            ) as mock_post_create_entity_bundle2_create,
            patch(
                STORE_ENTITY_COMPONENTS_PATCH,
                return_value=False,
            ) as mock_store_entity_components,
            patch(POST_COLUMNS_PATCH) as mock_post_columns,
            patch(
                SEND_JOB_AND_WAIT_ASYNC_PATCH,
                return_value=False,
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
            patch(GET_ID_PATCH, return_value="syn123") as mock_get_id,
            patch(
                POST_ENTITY_BUNDLE2_CREATE_PATCH
            ) as mock_post_create_entity_bundle2_create,
            patch(STORE_ENTITY_COMPONENTS_PATCH) as mock_store_entity_components,
            patch(POST_COLUMNS_PATCH) as mock_post_columns,
            patch(SEND_JOB_AND_WAIT_ASYNC_PATCH) as mock_send_job_and_wait_async,
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


class TestViewStoreMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(ViewStoreMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)
        include_default_columns: Optional[bool] = None
        view_entity_type: Optional[ViewEntityType] = ViewEntityType.DATASET
        view_type_mask: Optional[ViewTypeMask] = ViewTypeMask.DATASET

    async def test_store_async_include_default_columns_no_custom_columns(self):
        # GIVEN a TestClass instance with include_default_columns=True and no custom columns
        test_instance = self.TestClass(
            include_default_columns=True,
        )

        with (
            patch(
                GET_DEFAULT_COLUMNS_PATCH,
                return_value=[
                    Column(name="col2", column_type=ColumnType.STRING, id="id2")
                ],
            ) as mock_get_default_columns,
            patch(
                TABLE_STORE_MIXIN_PATCH,
                return_value=test_instance,
            ) as mock_table_store_async,
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(synapse_client=self.syn)

            # THEN mock_get_default_columns should be called
            mock_get_default_columns.assert_awaited_once_with(
                view_entity_type=ViewEntityType.DATASET,
                view_type_mask=ViewTypeMask.DATASET,
                synapse_client=self.syn,
            )
            # AND mock_store_async should be called
            mock_table_store_async.assert_awaited_once_with(
                dry_run=False,
                job_timeout=600,
                synapse_client=self.syn,
            )

            # AND the result should be the same instance with the default column added
            assert result.id is None
            assert result.name is None
            assert result.columns == OrderedDict(
                {"col2": Column(name="col2", column_type=ColumnType.STRING, id="id2")}
            )

    async def test_store_async_include_default_columns_with_custom_columns_and_overwrite(
        self,
    ):
        # GIVEN a TestClass instance with include_default_columns=True and two custom columns,
        # One of which shares a name with a default column
        test_instance = self.TestClass(
            include_default_columns=True,
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="CUSTOM"),
            },
        )

        with (
            patch(
                GET_DEFAULT_COLUMNS_PATCH,
                return_value=[
                    Column(name="col2", column_type=ColumnType.STRING, id="DEFAULT")
                ],
            ) as mock_get_default_columns,
            patch(
                TABLE_STORE_MIXIN_PATCH, return_value=test_instance
            ) as mock_table_store_async,
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(synapse_client=self.syn)

            # THEN mock_get_default_columns should be called
            mock_get_default_columns.assert_awaited_once_with(
                view_entity_type=ViewEntityType.DATASET,
                view_type_mask=ViewTypeMask.DATASET,
                synapse_client=self.syn,
            )
            # AND mock_table_store_async should be called
            mock_table_store_async.assert_awaited_once_with(
                dry_run=False,
                job_timeout=600,
                synapse_client=self.syn,
            )

            # AND the result should be the same instance with the default column
            # overwriting the custom column of the same name
            assert result.id is None
            assert result.name is None
            assert result.columns == OrderedDict(
                {
                    "col1": Column(
                        name="col1", column_type=ColumnType.STRING, id="id1"
                    ),
                    "col2": Column(
                        name="col2", column_type=ColumnType.STRING, id="DEFAULT"
                    ),
                }
            )

    async def test_store_async_no_default_columns(self):
        # GIVEN a TestClass instance with no default columns
        test_instance = self.TestClass(
            include_default_columns=False,
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
        )

        with (
            patch(
                GET_DEFAULT_COLUMNS_PATCH,
                return_value=[
                    Column(name="col2", column_type=ColumnType.STRING, id="DEFAULT")
                ],
            ) as mock_get_default_columns,
            patch(
                TABLE_STORE_MIXIN_PATCH, return_value=test_instance
            ) as mock_table_store_async,
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(synapse_client=self.syn)

            # THEN we expect no default columns to be fetched
            mock_get_default_columns.assert_not_awaited()
            # AND mock_table_store_async should be called
            mock_table_store_async.assert_awaited_once_with(
                dry_run=False,
                job_timeout=600,
                synapse_client=self.syn,
            )

            # AND the result should be the same instance
            assert result == test_instance

    async def test_store_async_invalid_character_in_column_name(self):
        # GIVEN a TestClass instance with an invalid character in a column name
        test_instance = self.TestClass(
            include_default_columns=False,
            columns={
                "col*1": Column(name="col*1", column_type=ColumnType.STRING, id="id1")
            },
        )

        with patch(TABLE_STORE_MIXIN_PATCH, return_value=test_instance):
            # WHEN store_async is awaited
            # THEN a ValueError should be raised
            with pytest.raises(
                ValueError,
                match=re.escape(
                    "Column name 'col*1' contains invalid characters. "
                    "Names may only contain: letters, numbers, spaces, underscores, "
                    "hyphens, periods, plus signs, apostrophes, and parentheses."
                ),
            ):
                await test_instance.store_async(synapse_client=self.syn)


class TestDeleteMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(DeleteMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        parent_id: Optional[str] = None

    async def test_delete_with_id(self):
        # GIVEN a TestClass instance with an id
        test_instance = self.TestClass(id="syn123")

        with patch(DELETE_ENTITY_PATCH, return_value=None) as mock_delete_entity:
            # WHEN delete_async is awaited
            await test_instance.delete_async(synapse_client=self.syn)

            # THEN mock_delete_entity should be called
            mock_delete_entity.assert_awaited_once_with(
                entity_id="syn123", synapse_client=self.syn
            )

    async def test_delete_with_name_and_parent_id(self):
        # GIVEN a TestClass instance with a name and parent_id
        test_instance = self.TestClass(name="test_table", parent_id="syn123")

        with (
            patch(
                GET_ID_PATCH,
                return_value="syn123",
            ) as mock_get_id,
            patch(DELETE_ENTITY_PATCH, return_value=None) as mock_delete_entity,
        ):
            # WHEN delete_async is awaited
            await test_instance.delete_async(synapse_client=self.syn)

            # THEN mock_get_id should be called
            mock_get_id.assert_awaited_once_with(
                entity=test_instance, synapse_client=self.syn
            )
            # AND mock_delete_entity should be called
            mock_delete_entity.assert_awaited_once_with(
                entity_id="syn123", synapse_client=self.syn
            )

    async def test_delete_with_no_id_or_name_and_parent_id(self):
        # GIVEN a TestClass instance with no id or name and parent_id
        test_instance = self.TestClass()

        with pytest.raises(
            ValueError,
            match=re.escape(
                "The table must have an id or a (name and `parent_id`) set."
            ),
        ):
            await test_instance.delete_async(synapse_client=self.syn)


class TestGetMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(GetMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        parent_id: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)
        _last_persistent_instance: Optional[Any] = None

        def _set_last_persistent_instance(self):
            self._last_persistent_instance = self

    async def test_get_async_include_columns_and_activity(self):
        # GIVEN a TestClass instance with an id
        test_instance = self.TestClass(id="syn123")

        with (
            patch(GET_ID_PATCH, return_value="syn123") as mock_get_id,
            patch(
                "synapseclient.models.mixins.table_components.get_from_entity_factory",
                return_value=test_instance,
            ) as mock_get_from_entity_factory,
            patch(
                "synapseclient.models.mixins.table_components.get_columns",
                return_value=[
                    Column(name="col1", column_type=ColumnType.STRING, id="id1")
                ],
            ) as mock_get_columns,
            patch.object(
                Activity,
                "from_parent_async",
                return_value=Activity(id="act1", name="activity1"),
            ) as mock_activity_from_parent_async,
        ):
            # WHEN get_async is awaited
            result = await test_instance.get_async(
                include_columns=True, include_activity=True, synapse_client=self.syn
            )

            # THEN mock_get_id should be called
            mock_get_id.assert_awaited_once_with(
                entity=test_instance, synapse_client=self.syn
            )
            # AND mock_get_from_entity_factory should be called
            mock_get_from_entity_factory.assert_awaited_once_with(
                entity_to_update=test_instance,
                synapse_id_or_path="syn123",
                synapse_client=self.syn,
            )
            # AND mock_get_columns should be called
            mock_get_columns.assert_awaited_once_with(
                table_id="syn123", synapse_client=self.syn
            )
            # AND mock_activity_from_parent_async should be called
            mock_activity_from_parent_async.assert_awaited_once_with(
                parent=test_instance, synapse_client=self.syn
            )
            # AND _last_persistent_instance should be set
            assert test_instance._last_persistent_instance == test_instance
            # AND the result should be the same instance
            assert result == test_instance

    async def test_get_async_no_id_or_name_and_parent_id(self):
        # GIVEN a TestClass instance with no id or name and parent_id
        test_instance = self.TestClass()
        # WHEN I await get_async
        # THEN I expect a ValueError to be raised
        with pytest.raises(
            ValueError,
            match=re.escape(
                "The table must have an id or a (name and `parent_id`) set."
            ),
        ):
            await test_instance.get_async(synapse_client=self.syn)


class TestColumnMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(ColumnMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)
        _last_persistent_instance: Optional[Any] = None
        _columns_to_delete: Dict[str, Column] = field(default_factory=dict)

    async def test_delete_column_no_persistent_instance(self):
        # GIVEN a TestClass instance with no persistent instance
        test_instance = self.TestClass()
        # WHEN I call delete_column
        # THEN I expect a ValueError to be raised
        with pytest.raises(
            ValueError,
            match=re.escape(
                "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            ),
        ):
            test_instance.delete_column(name="col1")

    async def test_delete_column_no_columns(self):
        # GIVEN a TestClass instance with no columns
        test_instance = self.TestClass(_last_persistent_instance=self.TestClass())
        # WHEN I call delete_column
        # THEN I expect a ValueError to be raised
        with pytest.raises(
            ValueError,
            match=re.escape(
                "There are no columns. Make sure you use the `include_columns` parameter in the `.get()` method."
            ),
        ):
            test_instance.delete_column(name="col1")

    async def test_delete_column_column_not_in_table(self):
        # GIVEN a TestClass instance with a column that is not in the table
        test_instance = self.TestClass(
            _last_persistent_instance=self.TestClass(),
            columns={
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="id2")
            },
        )
        # WHEN I call delete_column
        # THEN I expect a ValueError to be raised
        with pytest.raises(
            ValueError,
            match=re.escape("Column with name col1 does not exist in the table."),
        ):
            test_instance.delete_column(name="col1")

    async def test_delete_column_column_in_table(self):
        # GIVEN a TestClass instance with a column that is in the table
        test_instance = self.TestClass(
            _last_persistent_instance=self.TestClass(),
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1")
            },
        )
        # WHEN I call delete_column
        test_instance.delete_column(name="col1")
        # THEN I expect the column to be deleted
        assert "col1" not in test_instance.columns


class TestTableUpsertMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(TableUpsertMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_upsert_rows_async(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()
        # WHEN I call upsert_rows_async
        with patch(
            _UPSERT_ROWS_ASYNC_PATCH,
            return_value=None,
        ) as mock_upsert_rows_async:
            await test_instance.upsert_rows_async(
                values={"col1": ["A", "B"]},
                primary_keys=["col1"],
                synapse_client=self.syn,
            )
            # THEN mock_upsert_rows_async should be called
            mock_upsert_rows_async.assert_awaited_once_with(
                entity=test_instance,
                values={"col1": ["A", "B"]},
                primary_keys=["col1"],
                dry_run=False,
                rows_per_query=50000,
                update_size_bytes=1.9 * MB,
                insert_size_bytes=900 * MB,
                job_timeout=600,
                wait_for_eventually_consistent_view=False,
                wait_for_eventually_consistent_view_timeout=600,
                synapse_client=self.syn,
            )


class TestViewUpdateMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(ViewUpdateMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_update_rows_async(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()
        # WHEN I call upsert_rows_async
        with patch(
            _UPSERT_ROWS_ASYNC_PATCH,
            return_value=None,
        ) as mock_upsert_rows_async:
            await test_instance.update_rows_async(
                values={"col1": ["A", "B"]},
                primary_keys=["col1"],
                synapse_client=self.syn,
            )
            # THEN mock_upsert_rows_async should be called
            mock_upsert_rows_async.assert_awaited_once_with(
                entity=test_instance,
                values={"col1": ["A", "B"]},
                primary_keys=["col1"],
                dry_run=False,
                rows_per_query=50000,
                update_size_bytes=1.9 * MB,
                insert_size_bytes=900 * MB,
                job_timeout=600,
                wait_for_eventually_consistent_view=False,
                wait_for_eventually_consistent_view_timeout=600,
                synapse_client=self.syn,
            )


class TestQueryMixin:
    fake_query = "SELECT * FROM syn123"

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(QueryMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_query_async(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()

        # Create a mock TableQueryResult without calling __init__
        mock_query_result = MagicMock(spec=TableQueryResult)
        mock_query_result.asDataFrame.return_value = pd.DataFrame(
            {"col1": ["A", "B"], "col2": [1, 2]}
        )

        # WHEN I call query_async
        with patch.object(
            self.syn, "tableQuery", return_value=mock_query_result
        ) as mock_table_query:
            result = await test_instance.query_async(
                query=self.fake_query, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                includeRowIdAndRowVersion=True,
                quoteCharacter='"',
                escapeCharacter="\\",
                lineEnd=str(os.linesep),
                separator=",",
                header=True,
                downloadLocation=None,
            )

            # AND mock_as_data_frame should be called with correct args
            mock_query_result.asDataFrame.assert_called_once_with(
                rowIdAndVersionInIndex=False,
                convert_to_datetime=False,
            )

            # AND the result should match expected DataFrame
            assert result.equals(pd.DataFrame({"col1": ["A", "B"], "col2": [1, 2]}))

    async def test_query_part_mask_async(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()

        # Create mock query result with all possible part mask returns
        mock_query_result = MagicMock(spec=TableQueryResult)
        mock_query_result.asDataFrame.return_value = pd.DataFrame(
            {"col1": ["A", "B"], "col2": [1, 2]}
        )
        mock_query_result.count = 2
        mock_query_result.sumFileSizes = {
            "sumFileSizesBytes": 1000,
            "greaterThan": False,
        }
        mock_query_result.lastUpdatedOn = "2024-02-21"

        # Set up part mask combining all options
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZES | LAST_UPDATED_ON

        # WHEN I call query_part_mask_async
        with patch.object(
            self.syn, "tableQuery", return_value=mock_query_result
        ) as mock_table_query:
            result = await test_instance.query_part_mask_async(
                query=self.fake_query, part_mask=part_mask, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                resultsAs="rowset",
                partMask=part_mask,
            )

            # AND mock_as_data_frame should be called
            mock_query_result.asDataFrame.assert_called_once_with(
                rowIdAndVersionInIndex=False
            )

            # AND the result should contain all requested parts
            assert result.result.equals(
                pd.DataFrame({"col1": ["A", "B"], "col2": [1, 2]})
            )
            assert result.count == 2
            assert result.sum_file_sizes.sum_file_size_bytes == 1000
            assert result.sum_file_sizes.greater_than is False
            assert result.last_updated_on == "2024-02-21"

    async def test_query_part_mask_async_minimal(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()

        # Create mock with just query results
        mock_query_result = MagicMock(spec=TableQueryResult)
        mock_query_result.asDataFrame.return_value = pd.DataFrame(
            {"col1": ["A", "B"], "col2": [1, 2]}
        )
        mock_query_result.count = None
        mock_query_result.sumFileSizes = None
        mock_query_result.lastUpdatedOn = None

        # Use just QUERY_RESULTS mask
        part_mask = 0x1  # QUERY_RESULTS only

        # WHEN I call query_part_mask_async
        with patch.object(
            self.syn, "tableQuery", return_value=mock_query_result
        ) as mock_table_query:
            result = await test_instance.query_part_mask_async(
                query=self.fake_query, part_mask=part_mask, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                resultsAs="rowset",
                partMask=part_mask,
            )

            # AND mock_as_data_frame should be called
            mock_query_result.asDataFrame.assert_called_once_with(
                rowIdAndVersionInIndex=False
            )

            # AND the result should contain only the query results
            assert result.result.equals(
                pd.DataFrame({"col1": ["A", "B"], "col2": [1, 2]})
            )
            assert result.count is None
            assert result.sum_file_sizes is None
            assert result.last_updated_on is None


class TestViewSnapshotMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(ViewSnapshotMixin, GetMixin):
        id: Optional[str] = "syn123"
        name: Optional[str] = "test_view"
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_snapshot_async(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()
        expected_result = TableUpdateTransaction(
            entity_id=test_instance.id,
            changes=None,
            create_snapshot=True,
            snapshot_options=SnapshotRequest(
                comment="test comment",
                label="test label",
                activity=Activity(name="test activity"),
            ),
        )

        with (
            patch.object(test_instance, "get_async") as mock_get_async,
            patch(
                SEND_JOB_AND_WAIT_ASYNC_PATCH,
                return_value=expected_result,
            ) as mock_send_job_and_wait_async,
        ):
            # WHEN snapshot_async is called with all optional parameters
            result = await test_instance.snapshot_async(
                comment="test comment",
                label="test label",
                activity=Activity(name="test activity"),
                synapse_client=self.syn,
            )

            # THEN get_async should be called with include_activity=True
            mock_get_async.assert_awaited_once_with(
                include_activity=True,
                synapse_client=self.syn,
            )

            # AND send_job_and_wait_async should be called with correct parameters
            mock_send_job_and_wait_async.assert_awaited_once_with(
                synapse_client=self.syn
            )

            # AND the result should match the expected result
            assert result == expected_result


class TestTableDeleteRowMixin:
    fake_query = "SELECT * FROM syn123"

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class TestClass(TableDeleteRowMixin, QueryMixin):
        id: Optional[str] = "syn123"
        name: Optional[str] = "test_table"
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_delete_rows_async(self):
        # GIVEN a TestClass instance
        test_instance = self.TestClass()
        with (
            patch(
                "synapseclient.models.mixins.table_components.QueryMixin.query_async",
                return_value=pd.DataFrame(
                    {"ROW_ID": ["A", "B"], "ROW_VERSION": [1, 2]}
                ),
            ) as mock_query_async,
            patch(
                "synapseclient.models.mixins.table_components.multipart_upload_file_async",
                return_value="fake_file_handle_id",
            ) as mock_multipart_upload_file_async,
            patch(
                SEND_JOB_AND_WAIT_ASYNC_PATCH,
                return_value=TableUpdateTransaction(
                    entity_id=test_instance.id, changes=[]
                ),
            ) as mock_send_job_and_wait_async,
        ):
            # WHEN I call delete_rows_async
            result = await test_instance.delete_rows_async(
                query=self.fake_query, synapse_client=self.syn
            )

            # THEN mock_query_async should be called
            mock_query_async.assert_awaited_once_with(
                query=self.fake_query, synapse_client=self.syn
            )
            # AND mock_multipart_upload_file_async should be called
            mock_multipart_upload_file_async.assert_awaited_once()
            # AND mock_send_job_and_wait_async should be called
            mock_send_job_and_wait_async.assert_awaited_once_with(
                synapse_client=self.syn,
                timeout=600,
            )

            # AND the result should be the expected dataframe object
            assert result.equals(
                pd.DataFrame({"ROW_ID": ["A", "B"], "ROW_VERSION": [1, 2]})
            )
