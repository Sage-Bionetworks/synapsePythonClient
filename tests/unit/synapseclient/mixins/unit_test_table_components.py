import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from numpy import dtype

from synapseclient import Synapse
from synapseclient.api import ViewEntityType, ViewTypeMask
from synapseclient.core.utils import MB
from synapseclient.models import Activity, Column, ColumnType
from synapseclient.models.mixins.table_components import (
    ColumnMixin,
    DeleteMixin,
    FailureStrategy,
    GetMixin,
    QueryJob,
    QueryMixin,
    SnapshotRequest,
    TableDeleteRowMixin,
    TableStoreMixin,
    TableUpdateTransaction,
    TableUpsertMixin,
    ViewSnapshotMixin,
    ViewStoreMixin,
    ViewUpdateMixin,
    _query_table_csv,
    _query_table_next_page,
)
from synapseclient.models.table_components import (
    ColumnType,
    QueryNextPageToken,
    QueryResult,
    QueryResultBundle,
    QueryResultOutput,
    Row,
    RowSet,
    SelectColumn,
)

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
DELETE_ENTITY_PATCH = "synapseclient.models.mixins.table_components.delete_entity"
_UPSERT_ROWS_ASYNC_PATCH = (
    "synapseclient.models.mixins.table_components._upsert_rows_async"
)


class TestTableStoreMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class ClassForTest(TableStoreMixin, GetMixin):
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
            """Placeholder for fill_from_dict method"""
            self.__dict__.update(entity)

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_generate_schema_change_request_no_changes(self):
        # GIVEN a TestClass instance where has_columns_changed is False
        test_instance = self.ClassForTest(has_columns_changed=False, columns=[])
        # WHEN the _generate_schema_change_request method is called
        # THEN the method should return None
        assert (
            await test_instance._generate_schema_change_request(synapse_client=self.syn)
            is None
        )

    async def test_generate_schema_change_request_no_columns(self):
        # GIVEN a TestClass instance where has_columns_changed is True
        # AND columns is None
        test_instance = self.ClassForTest(has_columns_changed=True, columns=None)
        # WHEN the _generate_schema_change_request method is called
        # THEN the method should return None
        assert (
            await test_instance._generate_schema_change_request(synapse_client=self.syn)
            is None
        )

    async def test_generate_schema_change_request_columns_changed(self):
        # GIVEN a TestClass instance where has_columns_changed is True
        # AND columns have changes
        test_instance = self.ClassForTest(
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
        test_instance = self.ClassForTest(
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
        test_instance._last_persistent_instance = self.ClassForTest(
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
        test_instance = self.ClassForTest(
            has_columns_changed=True,
            id="syn123",
            name="test_table",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="id2"),
            },
        )
        test_instance._last_persistent_instance = self.ClassForTest(
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
        test_instance = self.ClassForTest(
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
        test_instance = self.ClassForTest(
            name="test_table",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
            has_columns_changed=True,
            has_changed=True,
        )

        with (
            patch(
                GET_ID_PATCH,
                return_value=None,
            ) as mock_get_id,
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
                self.ClassForTest, "get_async", return_value=test_instance
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
        test_instance = self.ClassForTest(
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
                self.ClassForTest, "get_async", return_value=test_instance
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
        test_instance = self.ClassForTest(
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
            patch.object(self.ClassForTest, "get_async") as mock_get_async,
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
    class ClassForTest(ViewStoreMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)
        include_default_columns: Optional[bool] = None
        view_entity_type: Optional[ViewEntityType] = ViewEntityType.DATASET
        view_type_mask: Optional[ViewTypeMask] = ViewTypeMask.DATASET
        _last_persistent_instance = None
        has_changed = False
        has_columns_changed = False

    async def test_store_async_include_default_columns_no_custom_columns(self):
        # GIVEN a TestClass instance with include_default_columns=True and no custom columns
        test_instance = self.ClassForTest(
            include_default_columns=True,
        )

        with (
            patch(
                GET_DEFAULT_COLUMNS_PATCH,
                return_value=[
                    Column(name="col2", column_type=ColumnType.STRING, id="id2")
                ],
            ) as mock_get_default_columns,
            patch(GET_ID_PATCH, return_value=None),
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(
                synapse_client=self.syn, dry_run=True
            )

            # THEN mock_get_default_columns should be called
            mock_get_default_columns.assert_awaited_once_with(
                view_entity_type=ViewEntityType.DATASET,
                view_type_mask=ViewTypeMask.DATASET,
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
        test_instance = self.ClassForTest(
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
            patch(GET_ID_PATCH, return_value=None),
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(
                synapse_client=self.syn, dry_run=True
            )

            # THEN mock_get_default_columns should be called
            mock_get_default_columns.assert_awaited_once_with(
                view_entity_type=ViewEntityType.DATASET,
                view_type_mask=ViewTypeMask.DATASET,
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
        test_instance = self.ClassForTest(
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
            patch(GET_ID_PATCH, return_value=None),
        ):
            # WHEN store_async is awaited
            result = await test_instance.store_async(
                synapse_client=self.syn, dry_run=True
            )

            # THEN we expect no default columns to be fetched
            mock_get_default_columns.assert_not_awaited()

            # AND the result should be the same instance
            assert result == test_instance

    @pytest.mark.parametrize(
        "invalid_column_name",
        [
            "col*1",  # Invalid character: *
            "col/1",  # Invalid character: /
            "col\\1",  # Invalid character: \
            "col:1",  # Invalid character: :
            "col;1",  # Invalid character: ;
            "col,1",  # Invalid character: ,
            "col?1",  # Invalid character: ?
            "col!1",  # Invalid character: !
            "col@1",  # Invalid character: @
            "col#1",  # Invalid character: #
        ],
    )
    async def test_store_async_invalid_character_in_column_name(
        self, invalid_column_name
    ):
        # GIVEN a TestClass instance with an invalid column name
        test_instance = TestViewStoreMixin.ClassForTest(
            include_default_columns=False,
            columns={
                invalid_column_name: Column(
                    name=invalid_column_name, column_type=ColumnType.STRING, id="id1"
                )
            },
        )

        # WHEN store_async is awaited
        # THEN a ValueError should be raised with the appropriate message
        with pytest.raises(
            ValueError,
            match=re.escape(
                f"Column name '{invalid_column_name}' contains invalid characters. "
                "Names may only contain: letters, numbers, spaces, underscores, "
                "hyphens, periods, plus signs, apostrophes, and parentheses."
            ),
        ):
            await test_instance.store_async(synapse_client=None, dry_run=True)

    async def test_store_async_valid_characters_in_column_name(self):
        # GIVEN a TestClass instance with valid characters in column names
        test_instance = self.ClassForTest(
            include_default_columns=False,
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col 2": Column(name="col 2", column_type=ColumnType.STRING, id="id2"),
                "col_3": Column(name="col_3", column_type=ColumnType.STRING, id="id3"),
                "col-4": Column(name="col-4", column_type=ColumnType.STRING, id="id4"),
                "col.5": Column(name="col.5", column_type=ColumnType.STRING, id="id5"),
                "col+6": Column(name="col+6", column_type=ColumnType.STRING, id="id6"),
                "col'7": Column(name="col'7", column_type=ColumnType.STRING, id="id7"),
                "col(8)": Column(
                    name="col(8)", column_type=ColumnType.STRING, id="id8"
                ),
            },
        )

        # WHEN store_async is awaited
        await test_instance.store_async(synapse_client=self.syn, dry_run=True)

        # THEN No exception should be raised


class TestDeleteMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class ClassForTest(DeleteMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        parent_id: Optional[str] = None

    async def test_delete_with_id(self):
        # GIVEN a TestClass instance with an id
        test_instance = self.ClassForTest(id="syn123")

        with patch(DELETE_ENTITY_PATCH, return_value=None) as mock_delete_entity:
            # WHEN delete_async is awaited
            await test_instance.delete_async(synapse_client=self.syn)

            # THEN mock_delete_entity should be called
            mock_delete_entity.assert_awaited_once_with(
                entity_id="syn123", synapse_client=self.syn
            )

    async def test_delete_with_name_and_parent_id(self):
        # GIVEN a TestClass instance with a name and parent_id
        test_instance = self.ClassForTest(name="test_table", parent_id="syn123")

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
        test_instance = self.ClassForTest()

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
    class ClassForTest(GetMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        parent_id: Optional[str] = None
        version_number: int = 1
        columns: Dict[str, Column] = field(default_factory=dict)
        _last_persistent_instance: Optional[Any] = None

        def _set_last_persistent_instance(self):
            self._last_persistent_instance = self

    async def test_get_async_include_columns_and_activity(self):
        # GIVEN a TestClass instance with an id
        test_instance = self.ClassForTest(id="syn123")

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
                version=1,
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
        test_instance = self.ClassForTest()
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
    class ClassForTest(ColumnMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)
        _last_persistent_instance: Optional[Any] = None
        _columns_to_delete: Dict[str, Column] = field(default_factory=dict)

    async def test_delete_column_no_persistent_instance(self):
        # GIVEN a TestClass instance with no persistent instance
        test_instance = self.ClassForTest()
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
        test_instance = self.ClassForTest(_last_persistent_instance=self.ClassForTest())
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
        test_instance = self.ClassForTest(
            _last_persistent_instance=self.ClassForTest(),
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
        test_instance = self.ClassForTest(
            _last_persistent_instance=self.ClassForTest(),
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
    class ClassForTest(TableUpsertMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_upsert_rows_async(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()
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
                synapse_client=self.syn,
            )


class TestViewUpdateMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class ClassForTest(ViewUpdateMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_update_rows_async(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()
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
    class ClassForTest(QueryMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_query_async(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()

        # CREATE a mock table query result
        mock_df = pd.DataFrame({"col1": ["A", "B"], "col2": [1, 2]})
        mock_query_result = mock_df, "dummy.csv"

        # WHEN I call query_async
        with (
            patch(
                "synapseclient.models.mixins.table_components._table_query",
                return_value=mock_query_result,
            ) as mock_table_query,
            patch(
                "synapseclient.models.mixins.table_components.csv_to_pandas_df",
                return_value=mock_df,
            ) as mock_csv_to_pandas_df,
            patch.object(os, "linesep", str(os.linesep)),
        ):
            result = await test_instance.query_async(
                query=self.fake_query, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                include_row_id_and_row_version=True,
                quote_char='"',
                escape_char="\\",
                line_end=str(os.linesep),
                separator=",",
                header=True,
                download_location=None,
            )

            # AND csv_to_pandas_df should be called with correct args
            mock_csv_to_pandas_df.assert_called_once_with(
                filepath="dummy.csv",
                separator=",",
                quote_char='"',
                escape_char="\\",
                row_id_and_version_in_index=False,
                date_columns=None,
                list_columns=None,
            )

            # AND the result should match expected DataFrame
            assert result.equals(mock_df)

    async def test_query_async_with_date_and_list_columns(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()

        # CREATE a mock table query result with headers containing date and list columns
        mock_df = pd.DataFrame(
            {
                "date_col": ["2024-01-01", "2024-01-02"],
                "list_col": [["item1", "item2"], ["item3", "item4"]],
                "string_col": ["A", "B"],
            }
        )

        # Mock query result with headers that include date and list column types
        mock_query_result_with_headers = (
            {
                "headers": [
                    {"name": "date_col", "columnType": "DATE"},
                    {"name": "list_col", "columnType": "STRING_LIST"},
                    {"name": "string_col", "columnType": "STRING"},
                ]
            },
            "dummy.csv",
        )

        # WHEN I call query_async with convert_to_datetime=True
        with (
            patch(
                "synapseclient.models.mixins.table_components._table_query",
                return_value=mock_query_result_with_headers,
            ) as mock_table_query,
            patch(
                "synapseclient.models.mixins.table_components.csv_to_pandas_df",
                return_value=mock_df,
            ) as mock_csv_to_pandas_df,
            patch.object(os, "linesep", str(os.linesep)),
        ):
            result = await test_instance.query_async(
                query=self.fake_query, convert_to_datetime=True, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                include_row_id_and_row_version=True,
                quote_char='"',
                escape_char="\\",
                line_end=str(os.linesep),
                separator=",",
                header=True,
                download_location=None,
            )

            # AND csv_to_pandas_df should be called with date_columns and list_columns populated
            mock_csv_to_pandas_df.assert_called_once_with(
                filepath="dummy.csv",
                separator=",",
                quote_char='"',
                escape_char="\\",
                row_id_and_version_in_index=False,
                date_columns=["date_col"],  # Should contain the DATE column
                list_columns=["list_col"],  # Should contain the STRING_LIST column
            )

            # AND the result should match expected DataFrame
            assert result.equals(mock_df)

    async def test_query_part_mask_async(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()

        # Create mock QueryResultBundle
        mock_query_result_bundle = QueryResultBundle(
            concrete_type="org.sagebionetworks.repo.model.table.QueryResultBundle",
            query_result=QueryResult(
                concrete_type="org.sagebionetworks.repo.model.table.QueryResult",
                query_results=RowSet(
                    concrete_type="org.sagebionetworks.repo.model.table.RowSet",
                    table_id="syn123",
                    etag="test etag",
                    headers=[
                        SelectColumn(
                            name="test_col", column_type=ColumnType.STRING, id="242777"
                        ),
                        SelectColumn(
                            name="test_col2", column_type=ColumnType.STRING, id="242778"
                        ),
                    ],
                    rows=[
                        Row(
                            row_id=1,
                            version_number=1,
                            values=["random string1", "random string2"],
                        ),
                        Row(
                            row_id=2,
                            version_number=1,
                            values=["random string3", "random string4"],
                        ),
                    ],
                ),
                next_page_token=None,
            ),
            query_count=2,
            last_updated_on="2025-08-17T09:50:35.248Z",
        )

        # Create expected DataFrame result
        expected_df = pd.DataFrame(
            {"test_col": ["random string1"], "test_col2": ["random string2"]}
        )

        # Set up part mask combining all options
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZES | LAST_UPDATED_ON

        # WHEN I call query_part_mask_async
        with (
            patch(
                "synapseclient.models.mixins.table_components._table_query",
                return_value=mock_query_result_bundle,
            ) as mock_table_query,
            patch(
                "synapseclient.models.mixins.table_components._rowset_to_pandas_df",
                return_value=expected_df,
            ) as mock_rowset_to_pandas_df,
        ):
            result = await test_instance.query_part_mask_async(
                query=self.fake_query, part_mask=part_mask, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                results_as="rowset",
                part_mask=part_mask,
                limit=None,
                offset=None,
            )
            # AND mock_rowset_to_pandas_df should be called with correct args
            mock_rowset_to_pandas_df.assert_called_once_with(
                query_result_bundle=mock_query_result_bundle,
                synapse=self.syn,
                row_id_and_version_in_index=False,
            )
            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                results_as="rowset",
                part_mask=part_mask,
                limit=None,
                offset=None,
            )
            # AND the result should be a QueryResultOutput with expected values
            assert isinstance(result, QueryResultOutput)
            assert result.result.equals(expected_df)
            assert result.count == mock_query_result_bundle.query_count
            assert result.last_updated_on == mock_query_result_bundle.last_updated_on
            assert result.sum_file_sizes is None  # Not set in mock, should be None

    async def test_query_part_mask_async_minimal(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()

        mock_query_result = QueryResult(
            concrete_type="org.sagebionetworks.repo.model.table.QueryResult",
            query_results=RowSet(
                concrete_type="org.sagebionetworks.repo.model.table.RowSet",
                table_id="syn456",
                etag="etag",
                headers=[
                    SelectColumn(
                        name="test_col", column_type=ColumnType.STRING, id="242777"
                    ),
                    SelectColumn(
                        name="test_col2", column_type=ColumnType.STRING, id="242778"
                    ),
                ],
                rows=[
                    Row(
                        row_id=1,
                        version_number=1,
                        values=["random string1", "random string2"],
                    ),
                    Row(
                        row_id=2,
                        version_number=1,
                        values=["random string3", "random string4"],
                    ),
                ],
            ),
            next_page_token=None,
        )
        mock_query_result_bundle = QueryResultBundle(
            concrete_type="org.sagebionetworks.repo.model.table.QueryResult",
            query_result=mock_query_result,
        )

        # Create expected DataFrame result
        expected_df = pd.DataFrame(
            {"test_col": ["random string1"], "test_col2": ["random string2"]}
        )

        # Use just QUERY_RESULTS mask
        part_mask = 0x1  # QUERY_RESULTS only

        # WHEN I call query_part_mask_async
        with (
            patch(
                "synapseclient.models.mixins.table_components._table_query",
                return_value=mock_query_result_bundle,
            ) as mock_table_query,
            patch(
                "synapseclient.models.mixins.table_components._rowset_to_pandas_df",
                return_value=expected_df,
            ) as mock_rowset_to_pandas_df,
        ):
            result = await test_instance.query_part_mask_async(
                query=self.fake_query, part_mask=part_mask, synapse_client=self.syn
            )

            # THEN mock_table_query should be called with correct args
            mock_table_query.assert_called_once_with(
                query=self.fake_query,
                results_as="rowset",
                part_mask=part_mask,
                limit=None,
                offset=None,
            )

            mock_rowset_to_pandas_df.assert_called_once_with(
                query_result_bundle=mock_query_result_bundle,
                synapse=self.syn,
                row_id_and_version_in_index=False,
            )

            # AND the result should be a QueryResultOutput with expected values
            assert isinstance(result, QueryResultOutput)
            assert result.result.equals(expected_df)
            assert result.count is None
            assert result.last_updated_on is None
            assert result.sum_file_sizes is None


class TestViewSnapshotMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class ClassForTest(ViewSnapshotMixin, GetMixin):
        id: Optional[str] = "syn123"
        name: Optional[str] = "test_view"
        columns: Dict[str, Column] = field(default_factory=dict)
        activity: Optional[Activity] = None
        version_number: Optional[int] = None

    async def test_snapshot_async(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()
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
                include_activity=True,
                synapse_client=self.syn,
            )

            # THEN get_async should be called
            mock_get_async.assert_called()

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
    class ClassForTest(TableDeleteRowMixin, QueryMixin):
        id: Optional[str] = "syn123"
        name: Optional[str] = "test_table"
        columns: Dict[str, Column] = field(default_factory=dict)

    async def test_delete_rows_async(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()
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


class TestQueryTableCsv:
    """Test suite for the _query_table_csv function."""

    @pytest.fixture
    def mock_synapse(self):
        """Create a mock Synapse client."""
        synapse = MagicMock(spec=Synapse)
        synapse._waitForAsync = MagicMock()
        synapse.cache = MagicMock()
        synapse.cache.get = MagicMock()
        synapse.cache.get_cache_dir = MagicMock()
        return synapse

    @pytest.fixture
    def sample_query(self):
        """Sample SQL query for testing."""
        return "SELECT * FROM syn1234"

    @pytest.fixture
    def mock_query_job_response(self, sample_query):
        """Sample query job response."""
        # Create a mock query job response after calling send_job_and_wait_async
        return QueryJob(
            entity_id="syn1234",
            sql=sample_query,
            # Response attributes populated after job completion
            job_id="1234",
            results_file_handle_id="5678",
            table_id="syn1234",
            etag="test_etag",
            headers=[
                {"name": "test_col", "columnType": "STRING", "id": "242777"},
                {"name": "test_col2", "columnType": "STRING", "id": "242778"},
            ],
            response_concrete_type="org.sagebionetworks.repo.model.table.DownloadFromTableResult",
        )

    @pytest.fixture
    def sample_file_path(self):
        """Sample file path for downloaded CSV."""
        return "/path/to/downloaded/file.csv"

    @pytest.mark.asyncio
    async def test_query_table_csv_request_generation(self, sample_query):
        """Test that QueryJob generates the correct synapse request."""
        # GIVEN custom parameters for CSV formatting
        custom_params = {
            "quote_character": "'",
            "escape_character": "/",
            "line_end": "\n",
            "separator": ";",
            "header": False,
            "include_row_id_and_row_version": False,
        }

        # WHEN creating a QueryJob with these parameters
        query_job = QueryJob(entity_id="syn1234", sql=sample_query, **custom_params)

        # THEN verify the to_synapse_request() method generates the correct request
        synapse_request = query_job.to_synapse_request()

        assert (
            synapse_request["concreteType"]
            == "org.sagebionetworks.repo.model.table.DownloadFromTableRequest"
        )
        assert synapse_request["entityId"] == "syn1234"
        assert synapse_request["sql"] == sample_query
        assert synapse_request["writeHeader"] == False
        assert synapse_request["includeRowIdAndRowVersion"] == False
        assert synapse_request["csvTableDescriptor"]["isFirstLineHeader"] == False
        assert synapse_request["csvTableDescriptor"]["quoteCharacter"] == "'"
        assert synapse_request["csvTableDescriptor"]["escapeCharacter"] == "/"
        assert synapse_request["csvTableDescriptor"]["lineEnd"] == "\n"
        assert synapse_request["csvTableDescriptor"]["separator"] == ";"

    @pytest.mark.asyncio
    async def test_query_table_csv_basic_functionality(
        self, mock_synapse, sample_query, sample_file_path, mock_query_job_response
    ):
        """Test basic functionality of _query_table_csv."""
        # GIVEN
        mock_synapse.cache.get.return_value = None
        mock_synapse.cache.get_cache_dir.return_value = "/cache/dir"

        with (
            patch(
                "synapseclient.models.mixins.table_components.extract_synapse_id_from_query"
            ) as mock_extract_id,
            patch(
                "synapseclient.models.mixins.table_components.ensure_download_location_is_directory"
            ) as mock_ensure_dir,
            patch(
                "synapseclient.models.mixins.table_components.download_by_file_handle"
            ) as mock_download,
            patch("os.makedirs") as mock_makedirs,
            patch(
                "synapseclient.models.table_components.QueryJob.send_job_and_wait_async"
            ) as mock_send_job_and_wait_async,
        ):
            mock_extract_id.return_value = "syn1234"
            mock_download.return_value = sample_file_path

            mock_send_job_and_wait_async.return_value = mock_query_job_response

            # WHEN calling the function
            completed_query_job, file_path = await _query_table_csv(
                query=sample_query, synapse=mock_synapse
            )

            # THEN ensure download file is correct
            assert file_path == sample_file_path
            assert completed_query_job.entity_id == "syn1234"

            # Verify API call was made correctly
            mock_send_job_and_wait_async.assert_called_once()

            # Verify the completed job has the expected response data
            assert completed_query_job.results_file_handle_id == "5678"
            assert completed_query_job.job_id == "1234"
            assert completed_query_job.table_id == "syn1234"
            assert len(completed_query_job.headers) == 2

    @pytest.mark.asyncio
    async def test_query_table_csv_with_download_location(
        self, mock_synapse, sample_query, sample_file_path, mock_query_job_response
    ):
        """Test _query_table_csv with specified download location."""
        # GIVEN a custom download location
        download_location = "/custom/download/path"
        mock_synapse.cache.get.return_value = None

        with (
            patch(
                "synapseclient.models.mixins.table_components.extract_synapse_id_from_query"
            ) as mock_extract_id,
            patch(
                "synapseclient.models.mixins.table_components.ensure_download_location_is_directory"
            ) as mock_ensure_dir,
            patch(
                "synapseclient.models.mixins.table_components.download_by_file_handle"
            ) as mock_download,
            patch("os.makedirs") as mock_makedirs,
            patch(
                "synapseclient.models.table_components.QueryJob.send_job_and_wait_async"
            ) as mock_send_job_and_wait_async,
        ):
            mock_extract_id.return_value = "syn1234"
            mock_ensure_dir.return_value = download_location
            mock_download.return_value = sample_file_path
            mock_send_job_and_wait_async.return_value = mock_query_job_response

            # WHEN calling the function with a download location
            result = await _query_table_csv(
                query=sample_query,
                synapse=mock_synapse,
                download_location=download_location,
            )

            # THEN verify ensure_download_location_is_directory is called with the correct location
            mock_ensure_dir.assert_called_once_with(download_location=download_location)
            mock_makedirs.assert_called_once_with(download_location, exist_ok=True)
            assert result == (mock_query_job_response, sample_file_path)


class TestQueryTableNextPage:
    """Test suite for the _query_table_next_page function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture
    def sample_table_id(self):
        """Sample table ID for testing."""
        return "syn123456"

    @pytest.fixture
    def sample_next_page_token(self):
        """Sample QueryNextPageToken for testing."""
        token = MagicMock(spec=QueryNextPageToken)
        token.token = "sample_token_string"
        return token

    @pytest.fixture
    def sample_synapse_response(self):
        """Sample response from Synapse API."""
        return {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryResultBundle",
            "queryResult": {
                "concreteType": "org.sagebionetworks.repo.model.table.QueryResult",
                "queryResults": {
                    "concreteType": "org.sagebionetworks.repo.model.table.RowSet",
                    "tableId": "syn123456",
                    "etag": "test-etag",
                    "headers": [
                        {"name": "col1", "columnType": "STRING", "id": "12345"},
                        {"name": "col2", "columnType": "INTEGER", "id": "12346"},
                    ],
                    "rows": [
                        {"rowId": 1, "versionNumber": 1, "values": ["test1", "100"]},
                        {"rowId": 2, "versionNumber": 1, "values": ["test2", "200"]},
                    ],
                },
                "nextPageToken": None,
            },
            "queryCount": 100,
            "lastUpdatedOn": "2025-08-20T10:00:00.000Z",
            "selectColumns": [
                {"name": "column1", "columnType": "STRING", "id": "12345"}
            ],
        }

    async def test_query_table_next_page_basic_functionality(
        self, sample_table_id, sample_next_page_token, sample_synapse_response
    ):
        """Test basic functionality of _query_table_next_page. Next page token is None"""
        with patch(
            "synapseclient.client.Synapse._waitForAsync",
            return_value=sample_synapse_response,
        ) as mock_wait_for_async:
            # WHEN calling _query_table_next_page function
            result = _query_table_next_page(
                next_page_token=sample_next_page_token,
                table_id=sample_table_id,
                synapse=self.syn,
            )
            # Verify API call was made correctly
            mock_wait_for_async.assert_called_once_with(
                uri="/entity/syn123456/table/query/nextPage/async",
                request="sample_token_string",
            )

            # Verify the QueryResultBundle was populated correctly from the response
            assert (
                result.concrete_type
                == "org.sagebionetworks.repo.model.table.QueryResultBundle"
            )
            assert result.query_count == 100
            assert result.last_updated_on == "2025-08-20T10:00:00.000Z"

            # Verify the nested QueryResult
            assert isinstance(result.query_result, QueryResult)
            assert (
                result.query_result.concrete_type
                == "org.sagebionetworks.repo.model.table.QueryResult"
            )
            assert result.query_result.next_page_token is None

            # Verify the nested RowSet
            assert isinstance(result.query_result.query_results, RowSet)
            assert result.query_result.query_results.table_id == sample_table_id
            assert result.query_result.query_results.etag == "test-etag"
            assert len(result.query_result.query_results.headers) == 2
            assert len(result.query_result.query_results.rows) == 2

            # Verify the nested SelectColumns
            assert isinstance(result.select_columns, list)
            assert len(result.select_columns) == 1
            assert result.select_columns[0].name == "column1"
            assert result.select_columns[0].column_type == "STRING"
            assert result.select_columns[0].id == "12345"
