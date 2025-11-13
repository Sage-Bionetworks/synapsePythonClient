import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import numpy as np
from synapseclient import Synapse
from synapseclient.api import ViewEntityType, ViewTypeMask
from synapseclient.core.constants.concrete_types import (
    QUERY_BUNDLE_REQUEST,
    QUERY_RESULT,
    QUERY_TABLE_CSV_REQUEST,
)
from synapseclient.core.utils import MB
from synapseclient.models import Activity, Column
from synapseclient.models.mixins.table_components import (
    ColumnMixin,
    DeleteMixin,
    FailureStrategy,
    GetMixin,
    QueryMixin,
    SnapshotRequest,
    TableDeleteRowMixin,
    TableStoreMixin,
    TableUpdateTransaction,
    TableUpsertMixin,
    ViewSnapshotMixin,
    ViewStoreMixin,
    ViewUpdateMixin,
    _construct_partial_rows_for_upsert,
    _query_table_csv,
    _query_table_next_page,
    _query_table_row_set,
    csv_to_pandas_df,
)
from synapseclient.models.table_components import (
    ActionRequiredCount,
    ColumnType,
    CsvTableDescriptor,
    PartialRow,
    Query,
    QueryBundleRequest,
    QueryJob,
    QueryNextPageToken,
    QueryResult,
    QueryResultBundle,
    QueryResultOutput,
    Row,
    RowSet,
    SelectColumn,
    SumFileSizes,
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
DEFAULT_QUOTE_CHARACTER = '"'
DEFAULT_SEPARATOR = ","
DEFAULT_ESCAPSE_CHAR = "\\"


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
        test_instance.__name__ = ""

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

    def test_construct_partial_rows_for_upsert_single_value_column_no_na_with_changes(
        self,
    ):
        # GIVEN an entity with single value columns without NA values
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.INTEGER, id="id2"),
            },
        )

        # Results from Synapse query (existing rows)
        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [1, 2],
            }
        )

        # Data to upsert (with changes)
        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [1, 20],  # Changed values
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect rows to be updated
        assert len(rows_to_update) == 1
        assert len(indexes_with_changes) == 1
        assert len(indexes_without_changes) == 1
        assert len(syn_id_and_etags) == 0

        # Verify the second row update
        assert rows_to_update[0].row_id == "row2"
        assert rows_to_update[0].etag is None
        assert len(rows_to_update[0].values) == 1
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == 20

        # verify first row without changes
        assert indexes_without_changes[0] == 0

    def test_construct_partial_rows_for_upsert_single_value_column_no_na_without_changes(
        self,
    ):
        # GIVEN an entity with single value columns without NA values where values don't change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.INTEGER, id="id2"),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [1, 2],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [1, 2],  # Same values, no changes
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect no rows to be updated
        assert len(rows_to_update) == 0
        assert len(indexes_with_changes) == 0
        assert len(indexes_without_changes) == 2
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_single_value_no_na_with_etag(self):
        # GIVEN an entity with single value columns without NA values and results containing ROW_ETAG
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.INTEGER, id="id2"),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1"],
                "ROW_ETAG": ["etag1"],
                "id": ["syn123"],
                "col1": ["A"],
                "col2": [1],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": [10],  # Changed value
            }
        )

        primary_keys = ["col1"]
        contains_etag = True
        wait_for_eventually_consistent_view = True

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect the row to be updated with etag
        assert len(rows_to_update) == 1
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].etag == "etag1"
        assert len(indexes_with_changes) == 1
        assert indexes_with_changes[0] == 0
        assert len(indexes_without_changes) == 0
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == 10
        assert len(syn_id_and_etags) == 1
        assert syn_id_and_etags["syn123"] == "etag1"

    def test_construct_partial_rows_for_upsert_single_value_column_with_na_values_changes(
        self,
    ):
        # GIVEN an entity with columns and dataframes containing NA values and values change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.INTEGER, id="id2"),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [1, pd.NA],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [
                    pd.NA,
                    pd.NA,
                ],  # row2 shouldn't be updated since it both cell and row are NA
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # Verify the first row update
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].etag is None
        assert len(rows_to_update[0].values) == 1
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == None
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_list_column__no_na_changes(self):
        # GIVEN an entity with a list column without NA values where values change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(
                    name="col2", column_type=ColumnType.STRING_LIST, id="id2"
                ),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [["item1", "item2"], ["item3", "item4"]],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [["item1", "item3"], ["item3", "item4"]],  # Changed list value
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect the row to be updated
        assert len(rows_to_update) == 1
        assert rows_to_update[0].row_id == "row1"
        assert len(indexes_with_changes) == 1
        assert indexes_with_changes[0] == 0
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == ["item1", "item3"]

        # Verify second row is not tracked since it has no changes
        assert len(indexes_without_changes) == 1
        assert indexes_without_changes[0] == 1
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_list_column_with_na_values_changes(
        self,
    ):
        # GIVEN an entity with a List column with NA values where values change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(
                    name="col2", column_type=ColumnType.STRING_LIST, id="id2"
                ),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [["item1", "item2"], [pd.NA, "item4"]],  # row2 has NA
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [
                    ["item1", "item3"],
                    ["item3", "item4"],
                ],  # row 1 and 2 both change
            }
        )
        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect both rows to be updated (value to NA, and NA to value)
        assert len(rows_to_update) == 2
        assert len(indexes_with_changes) == 2
        assert len(indexes_without_changes) == 0
        assert len(syn_id_and_etags) == 0

        # Verify first row: list value changes to NA
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == ["item1", "item3"]

        # Verify second row: NA changes to list value
        assert rows_to_update[1].row_id == "row2"
        assert rows_to_update[1].values[0]["key"] == "id2"
        assert rows_to_update[1].values[0]["value"] == ["item3", "item4"]

    def test_construct_partial_rows_for_upsert_with_list_column_with_na_values_no_changes(
        self,
    ):
        # GIVEN an entity with a LIST column where values don't change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(
                    name="col2", column_type=ColumnType.STRING_LIST, id="id2"
                ),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1"],
                "col1": ["A"],
                "col2": [["item1", "item2", pd.NA]],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": [["item1", "item2", pd.NA]],  # Same list value
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect no rows to be updated
        assert len(rows_to_update) == 0
        assert len(indexes_with_changes) == 0
        assert len(indexes_without_changes) == 1
        assert indexes_without_changes[0] == 0
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_list_column_changes_with_na_values_changes(
        self,
    ):
        # GIVEN an entity with a List column with NA values where values change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(
                    name="col2", column_type=ColumnType.STRING_LIST, id="id2"
                ),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [["item1", "item2"], [pd.NA, "item4"]],  # row2 has NA
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [
                    ["item1", "item3"],
                    ["item3", "item4"],
                ],  # row 1 and 2 both change
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect both rows to be updated (value to NA, and NA to value)
        assert len(rows_to_update) == 2
        assert len(indexes_with_changes) == 2
        assert len(indexes_without_changes) == 0
        assert len(syn_id_and_etags) == 0

        # Verify first row: list value changes to NA
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == ["item1", "item3"]

        # Verify second row: NA changes to list value
        assert rows_to_update[1].row_id == "row2"
        assert rows_to_update[1].values[0]["key"] == "id2"
        assert rows_to_update[1].values[0]["value"] == ["item3", "item4"]

    def test_construct_partial_rows_for_upsert_with_numpy_array_comparison_no_na_changes(
        self,
    ):
        # GIVEN an entity where values might be numpy arrays without NA values where values change
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(
                    name="col2", column_type=ColumnType.INTEGER_LIST, id="id2"
                ),
            },
        )

        # Create dataframes with numpy arrays
        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [np.array([1, 2, 3]), np.array([4, 5, 6])],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [
                    np.array([1, 2, 4]),
                    np.array([4, 5, 6]),
                ],  # Changed array value
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect the row to be updated (numpy array comparison should work)
        assert len(rows_to_update) == 1
        assert len(indexes_with_changes) == 1
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == [
            np.int64(1),
            np.int64(2),
            np.int64(4),
        ]
        assert len(indexes_without_changes) == 1
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_numpy_array_comparison_with_na_changes(
        self,
    ):
        # GIVEN an entity with numpy arrays that might contain NA values where values change
        import numpy as np

        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(
                    name="col2", column_type=ColumnType.INTEGER_LIST, id="id2"
                ),
            },
        )

        # Test with arrays containing pd.NA where values change
        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2"],
                "col1": ["A", "B"],
                "col2": [np.array([1, 2, pd.NA]), np.array([4, 5, 6])],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": [
                    np.array([1, 2, pd.NA]),
                    np.array([4, pd.NA, 6]),
                ],  # row 2 changes
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        # This should handle the pd.NA comparison gracefully
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN the function should handle this without crashing
        assert len(rows_to_update) == 2
        assert len(indexes_with_changes) == 2
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == [1, 2, pd.NA]
        assert indexes_with_changes[0] == 0
        assert rows_to_update[1].row_id == "row2"
        assert rows_to_update[1].values[0]["key"] == "id2"
        assert rows_to_update[1].values[0]["value"] == [4, pd.NA, 6]
        assert len(indexes_without_changes) == 0
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_multiple_primary_keys(self):
        # GIVEN an entity with columns and multiple primary keys
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="id2"),
                "col3": Column(name="col3", column_type=ColumnType.INTEGER, id="id3"),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1"],
                "col1": ["A"],
                "col2": ["B"],
                "col3": [1],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": ["B"],
                "col3": [10],  # Changed value
            }
        )

        primary_keys = ["col1", "col2"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect the row to be updated
        assert len(rows_to_update) == 1
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].values[0]["key"] == "id3"
        assert rows_to_update[0].values[0]["value"] == 10
        assert len(indexes_with_changes) == 1
        assert indexes_with_changes[0] == 0
        assert len(indexes_without_changes) == 0
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_column_not_in_entity(self):
        # GIVEN an entity with columns and upsert data containing a column not in entity and changes to the column should be ignored
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1"],
                "col1": ["A"],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": [10],  # Column not in entity.columns
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect no rows to be updated (col2 is ignored)
        assert len(rows_to_update) == 0
        assert len(indexes_with_changes) == 0
        assert len(indexes_without_changes) == 1
        assert indexes_without_changes[0] == 0
        assert len(syn_id_and_etags) == 0

    def test_construct_partial_rows_for_upsert_with_wait_for_eventually_consistent_view(
        self,
    ):
        # GIVEN an entity with columns and results containing id and ROW_ETAG
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.INTEGER, id="id2"),
            },
        )

        results = pd.DataFrame(
            {
                "ROW_ID": ["row1"],
                "ROW_ETAG": ["etag1"],
                "id": ["syn456"],
                "col1": ["A"],
                "col2": [1],
            }
        )

        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": [10],  # Changed value
            }
        )

        primary_keys = ["col1"]
        contains_etag = True
        wait_for_eventually_consistent_view = True

        # WHEN I call _construct_partial_rows_for_upsert
        (
            rows_to_update,
            indexes_with_changes,
            indexes_without_changes,
            syn_id_and_etags,
        ) = _construct_partial_rows_for_upsert(
            entity=test_instance,
            results=results,
            chunk_to_check_for_upsert=chunk_to_check_for_upsert,
            primary_keys=primary_keys,
            contains_etag=contains_etag,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
        )

        # THEN I expect the row to be updated and syn_id_and_etags to be populated
        assert len(rows_to_update) == 1
        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].etag == "etag1"
        assert len(syn_id_and_etags) == 1
        assert syn_id_and_etags["syn456"] == "etag1"


class TestQuery:
    """Test suite for the Query.to_synapse_request method."""

    def test_to_synapse_request_with_minimal_data(self):
        """Test to_synapse_request with only required SQL parameter."""
        # GIVEN a Query with minimal parameters
        query = Query(sql="SELECT * FROM syn123456")

        # WHEN calling to_synapse_request
        result = query.to_synapse_request()

        # THEN verify only sql and includeEntityEtag are included (None values are deleted)
        expected = {"sql": "SELECT * FROM syn123456", "includeEntityEtag": False}
        assert result == expected

    def test_to_synapse_request_with_all_parameters(self):
        """Test to_synapse_request with all parameters specified."""
        # GIVEN a Query with all parameters
        additional_filters = [
            {
                "concreteType": "org.example.Filter1",
                "column": "col1",
                "operator": "EQUALS",
                "values": ["value1"],
            },
            {
                "concreteType": "org.example.Filter2",
                "column": "col2",
                "operator": "GREATER_THAN",
                "values": [10],
            },
        ]
        selected_facets = [
            {
                "concreteType": "org.example.FacetColumnRangeRequest",
                "columnName": "age",
                "min": "18",
                "max": "65",
            },
            {
                "concreteType": "org.example.FacetColumnValuesRequest",
                "columnName": "category",
                "facetValues": ["A", "B"],
            },
        ]
        sort_items = [
            {"column": "name", "direction": "ASC"},
            {"column": "date_created", "direction": "DESC"},
        ]

        query = Query(
            sql="SELECT col1, col2, col3 FROM syn123456",
            additional_filters=additional_filters,
            selected_facets=selected_facets,
            include_entity_etag=True,
            select_file_column=123,
            select_file_version_column=456,
            offset=50,
            limit=100,
            sort=sort_items,
        )

        # WHEN calling to_synapse_request
        result = query.to_synapse_request()

        # THEN verify all parameters are included
        expected = {
            "sql": "SELECT col1, col2, col3 FROM syn123456",
            "additionalFilters": additional_filters,
            "selectedFacets": selected_facets,
            "includeEntityEtag": True,
            "selectFileColumn": 123,
            "selectFileVersionColumn": 456,
            "offset": 50,
            "limit": 100,
            "sort": sort_items,
        }
        assert result == expected

    def test_to_synapse_request_with_partial_parameters(self):
        """Test to_synapse_request with some parameters specified."""
        # GIVEN a Query with partial parameters
        query = Query(
            sql="SELECT COUNT(*) FROM syn123456",
            include_entity_etag=False,
            offset=0,
            limit=50,
        )

        # WHEN calling to_synapse_request
        result = query.to_synapse_request()

        # THEN verify only specified parameters are included
        expected = {
            "sql": "SELECT COUNT(*) FROM syn123456",
            "includeEntityEtag": False,
            "offset": 0,
            "limit": 50,
        }
        assert result == expected


class TestQueryBundleRequest:
    """Test suite for the QueryBundleRequest.to_synapse_request and fill_from_dict methods."""

    @pytest.fixture
    def sample_query(self):
        """Sample Query object for testing."""
        return Query(
            sql="SELECT * FROM syn123456", include_entity_etag=True, offset=0, limit=100
        )

    @pytest.fixture
    def sample_query_result_bundle_data(self):
        """Sample QueryResultBundle response data for testing."""
        return {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryResultBundle",
            "queryResult": {
                "concreteType": "org.sagebionetworks.repo.model.table.QueryResult",
                "queryResults": {
                    "concreteType": "org.sagebionetworks.repo.model.table.RowSet",
                    "tableId": "syn123456",
                    "etag": "rowset-etag",
                    "headers": [{"name": "col1", "columnType": "STRING", "id": "123"}],
                    "rows": [
                        {"rowId": 1, "versionNumber": 1, "values": ["test_value"]}
                    ],
                },
            },
            "queryCount": 250,
            "selectColumns": [
                {"name": "col1", "columnType": "STRING", "id": "123"},
                {"name": "col2", "columnType": "INTEGER", "id": "124"},
            ],
            "maxRowsPerPage": 100,
            "columnModels": [
                {"name": "col1", "columnType": "STRING", "id": "123"},
                {"name": "col2", "columnType": "INTEGER", "id": "124"},
            ],
            "facets": [
                {
                    "concreteType": "org.sagebionetworks.repo.model.table.FacetColumnResultValues",
                    "columnName": "status",
                    "facetType": "enumeration",
                    "facetValues": [
                        {"value": "active", "count": 100, "isSelected": False}
                    ],
                }
            ],
            "sumFileSizes": {"sumFileSizesBytes": 2048576, "greaterThan": True},
            "lastUpdatedOn": "2025-08-27T12:30:45.678Z",
            "combinedSql": "SELECT * FROM syn123456 WHERE status = 'active' LIMIT 100 OFFSET 0",
            "actionsRequired": [
                {
                    "action": {
                        "concreteType": "org.sagebionetworks.repo.model.download.MeetAccessRequirement",
                        "accessRequirementId": 12345,
                    },
                    "count": 5,
                }
            ],
        }

    def test_to_synapse_request_with_minimal_parameters(self, sample_query):
        """Test to_synapse_request with minimal parameters."""
        # GIVEN a QueryBundleRequest with minimal parameters
        request = QueryBundleRequest(entity_id="syn123456", query=sample_query)

        # WHEN calling to_synapse_request
        result = request.to_synapse_request()

        # THEN verify the correct request structure
        expected = {
            "concreteType": QUERY_BUNDLE_REQUEST,
            "entityId": "syn123456",
            "query": sample_query,
        }
        assert result == expected

    def test_to_synapse_request_with_part_mask(self, sample_query):
        """Test to_synapse_request with part_mask specified."""
        # GIVEN a QueryBundleRequest with part_mask
        part_mask = 0x1 | 0x2 | 0x4
        request = QueryBundleRequest(
            entity_id="syn789012", query=sample_query, part_mask=part_mask
        )

        # WHEN calling to_synapse_request
        result = request.to_synapse_request()

        # THEN verify part_mask is included
        expected = {
            "concreteType": QUERY_BUNDLE_REQUEST,
            "entityId": "syn789012",
            "query": sample_query,
            "partMask": part_mask,
        }
        assert result == expected

    def test_fill_from_dict_with_complete_bundle(
        self, sample_query, sample_query_result_bundle_data
    ):
        """Test fill_from_dict with complete QueryResultBundle response."""
        # GIVEN a QueryBundleRequest and complete response data
        request = QueryBundleRequest(
            entity_id="syn123456", query=sample_query, part_mask=0x3FF
        )

        # WHEN calling fill_from_dict
        result = request.fill_from_dict(sample_query_result_bundle_data)

        # THEN verify all response attributes are set
        assert result is request  # Should return self

        # Verify nested QueryResult
        assert isinstance(request.query_result, QueryResult)
        assert (
            request.query_result.concrete_type
            == "org.sagebionetworks.repo.model.table.QueryResult"
        )
        assert isinstance(request.query_result.query_results, RowSet)
        assert request.query_result.query_results.table_id == "syn123456"

        # Verify scalar fields
        assert request.query_count == 250
        assert request.max_rows_per_page == 100
        assert request.last_updated_on == "2025-08-27T12:30:45.678Z"
        assert (
            request.combined_sql
            == "SELECT * FROM syn123456 WHERE status = 'active' LIMIT 100 OFFSET 0"
        )

        # Verify SelectColumns
        assert len(request.select_columns) == 2
        assert isinstance(request.select_columns[0], SelectColumn)
        assert request.select_columns[0].name == "col1"
        assert request.select_columns[0].column_type == ColumnType.STRING

        # Verify ColumnModels
        assert len(request.column_models) == 2
        assert isinstance(request.column_models[0], Column)
        assert request.column_models[0].name == "col1"

        # Verify Facets (stored as raw data)
        assert len(request.facets) == 1
        assert request.facets[0]["columnName"] == "status"

        # Verify SumFileSizes
        assert isinstance(request.sum_file_sizes, SumFileSizes)
        assert request.sum_file_sizes.sum_file_size_bytes == 2048576
        assert request.sum_file_sizes.greater_than == True

        # Verify ActionsRequired
        assert len(request.actions_required) == 1
        assert isinstance(request.actions_required[0], ActionRequiredCount)
        assert request.actions_required[0].count == 5


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


class TestQueryResultBundle:
    """Test suite for the QueryResultBundle.fill_from_dict method."""

    @pytest.fixture
    def sample_query_result_data(self):
        """Sample QueryResult data for testing."""
        return {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryResult",
            "queryResults": {
                "concreteType": "org.sagebionetworks.repo.model.table.RowSet",
                "tableId": "syn123456",
                "etag": "rowset-etag",
                "headers": [
                    {"name": "col1", "columnType": "STRING", "id": "123"},
                    {"name": "col2", "columnType": "INTEGER", "id": "124"},
                ],
                "rows": [
                    {"rowId": 1, "versionNumber": 1, "values": ["test1", "100"]},
                    {"rowId": 2, "versionNumber": 1, "values": ["test2", "200"]},
                ],
            },
            "nextPageToken": {
                "concreteType": "org.sagebionetworks.repo.model.table.QueryNextPageToken",
                "entityId": "syn123456",
                "token": "next-page-token-abc",
            },
        }

    @pytest.fixture
    def sample_select_columns_data(self):
        """Sample SelectColumn data for testing."""
        return [
            {"name": "col1", "columnType": "STRING", "id": "123"},
            {"name": "col2", "columnType": "INTEGER", "id": "124"},
            {"name": "col3", "columnType": "BOOLEAN", "id": "125"},
        ]

    @pytest.fixture
    def sample_sum_file_sizes_data(self):
        """Sample SumFileSizes data for testing."""
        return {"sumFileSizesBytes": 1048576, "greaterThan": False}

    def test_fill_from_dict_with_complete_data(
        self,
        sample_query_result_data,
        sample_select_columns_data,
        sample_sum_file_sizes_data,
    ):
        """Test fill_from_dict with complete QueryResultBundle data."""
        # GIVEN complete QueryResultBundle data
        data = {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryResultBundle",
            "queryResult": sample_query_result_data,
            "queryCount": 150,
            "selectColumns": sample_select_columns_data,
            "maxRowsPerPage": 100,
            "columnModels": [
                {"name": "col1", "columnType": "STRING", "id": "123"},
                {"name": "col2", "columnType": "INTEGER", "id": "124"},
            ],
            "facets": [
                {
                    "concreteType": "org.sagebionetworks.repo.model.table.FacetColumnResultValues",
                    "columnName": "status",
                    "facetType": "enumeration",
                    "facetValues": [
                        {"value": "active", "count": 50, "isSelected": False},
                        {"value": "inactive", "count": 25, "isSelected": True},
                    ],
                }
            ],
            "sumFileSizes": sample_sum_file_sizes_data,
            "lastUpdatedOn": "2025-08-20T15:30:45.123Z",
            "combinedSql": "SELECT col1, col2 FROM syn123456 WHERE status = 'active'",
        }

        # WHEN calling fill_from_dict
        result = QueryResultBundle.fill_from_dict(data)

        # THEN verify all attributes are set correctly
        assert (
            result.concrete_type
            == "org.sagebionetworks.repo.model.table.QueryResultBundle"
        )

        # Verify nested QueryResult
        assert isinstance(result.query_result, QueryResult)
        assert (
            result.query_result.concrete_type
            == "org.sagebionetworks.repo.model.table.QueryResult"
        )
        assert isinstance(result.query_result.query_results, RowSet)
        assert result.query_result.query_results.table_id == "syn123456"

        # Verify scalar fields
        assert result.query_count == 150
        assert result.max_rows_per_page == 100
        assert result.last_updated_on == "2025-08-20T15:30:45.123Z"
        assert (
            result.combined_sql
            == "SELECT col1, col2 FROM syn123456 WHERE status = 'active'"
        )

        # Verify SelectColumns
        assert len(result.select_columns) == 3
        assert isinstance(result.select_columns[0], SelectColumn)
        assert result.select_columns[0].name == "col1"
        assert result.select_columns[0].column_type == ColumnType.STRING
        assert result.select_columns[1].name == "col2"
        assert result.select_columns[1].column_type == ColumnType.INTEGER
        assert result.select_columns[2].name == "col3"
        assert result.select_columns[2].column_type == ColumnType.BOOLEAN

        # Verify ColumnModels
        assert len(result.column_models) == 2
        assert result.column_models[0].name == "col1"
        assert result.column_models[1].column_type == "INTEGER"

        # Verify Facets (stored as raw data)
        assert len(result.facets) == 1
        assert result.facets[0]["columnName"] == "status"
        assert result.facets[0]["facetType"] == "enumeration"
        assert len(result.facets[0]["facetValues"]) == 2

        # Verify SumFileSizes
        assert isinstance(result.sum_file_sizes, SumFileSizes)
        assert result.sum_file_sizes.sum_file_size_bytes == 1048576
        assert result.sum_file_sizes.greater_than == False


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

        mock_query_job = QueryJob(
            entity_id="syn1234",
            sql="SELECT * FROM syn1234",
            # Response attributes populated after job completion
            job_id="1234",
            results_file_handle_id="5678",
            table_id="syn1234",
            etag="test_etag",
            headers=[
                SelectColumn(name="col1", column_type=ColumnType.STRING, id="111"),
                SelectColumn(name="col2", column_type=ColumnType.INTEGER, id="222"),
            ],
            response_concrete_type="org.sagebionetworks.repo.model.table.DownloadFromTableResult",
        )

        # CREATE a mock table query result
        mock_df = pd.DataFrame(
            {"test_col": ["random string1"], "test_col2": ["random string2"]}
        )
        mock_query_result = mock_query_job, "dummy.csv"

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
                timeout=250,
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
                dtype={"col1": str},
                list_column_types=None,
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

        csv_table_descriptor = CsvTableDescriptor(
            quote_character='"',
            escape_character="\\",
            line_end=os.linesep,
            separator=",",
            is_first_line_header=True,
        )

        # Mock query result with headers that include date and list column types
        mock_query_job_response = QueryJob(
            entity_id="syn123",
            sql="SELECT * FROM syn123",
            csv_table_descriptor=csv_table_descriptor,
            include_row_id_and_row_version=True,
            job_id="test-job-12345",
            response_concrete_type="org.sagebionetworks.repo.model.table.DownloadFromTableResult",
            results_file_handle_id="file-handle-67890",
            table_id="syn123",
            etag="test-etag-abc123",
            headers=[
                SelectColumn(name="date_col", column_type=ColumnType.DATE),
                SelectColumn(name="list_col", column_type=ColumnType.STRING_LIST),
                SelectColumn(name="string_col", column_type=ColumnType.STRING),
            ],
        )

        mock_query_result_with_headers = (
            mock_query_job_response,
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
                timeout=250,
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
                dtype={
                    "string_col": str,
                },
                list_column_types={
                    "list_col": ColumnType.STRING_LIST,
                },
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
                timeout=250,
            )
            # AND mock_rowset_to_pandas_df should be called with correct args
            mock_rowset_to_pandas_df.assert_called_once_with(
                query_result_bundle=mock_query_result_bundle,
                synapse=self.syn,
                row_id_and_version_in_index=False,
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
                timeout=250,
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
                synapse_client=self.syn, timeout=120
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

    async def test_delete_rows_async_via_query(self):
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
            patch.object(self.syn.logger, "info") as mock_logger_info,
        ):
            # WHEN I call delete_rows_async
            result = await test_instance.delete_rows_async(
                query=self.fake_query, synapse_client=self.syn
            )

            # THEN mock_logger_info should be called
            mock_logger_info.assert_called_once_with(
                f"Found 2 rows to delete for given query: {self.fake_query}"
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

    async def test_delete_rows_async_via_dataframe_pass(self):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()
        df = pd.DataFrame({"ROW_ID": ["A"], "ROW_VERSION": [1]})
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
            patch.object(self.syn.logger, "info") as mock_logger_info,
        ):
            # WHEN I call delete_rows_async
            result = await test_instance.delete_rows_async(
                df=df, synapse_client=self.syn
            )

            # THEN mock_logger_info should be called
            mock_logger_info.assert_called_once_with(
                f"Received 1 rows to delete for given dataframe."
            )
            # AND mock_multipart_upload_file_async should be called
            mock_multipart_upload_file_async.assert_awaited_once()
            # AND mock_send_job_and_wait_async should be called
            mock_send_job_and_wait_async.assert_awaited_once_with(
                synapse_client=self.syn,
                timeout=600,
            )

            # AND the result should be the expected dataframe object
            assert result.equals(pd.DataFrame({"ROW_ID": ["A"], "ROW_VERSION": [1]}))

    @pytest.mark.parametrize(
        "df, error_msg",
        [
            (
                pd.DataFrame(columns=["ROW_ID"]),  # Missing ROW_VERSION column
                "The dataframe must contain the 'ROW_ID' and 'ROW_VERSION' columns.",
            ),
            (
                pd.DataFrame(columns=["ROW_VERSION"]),  # Missing ROW_ID column
                "The dataframe must contain the 'ROW_ID' and 'ROW_VERSION' columns.",
            ),
            (
                pd.DataFrame(columns=["INVALID_COL", "ROW_VERSION"]),  # Invalid column
                "The dataframe must contain the 'ROW_ID' and 'ROW_VERSION' columns.",
            ),
            (
                pd.DataFrame(columns=["ROW_ID", "INVALID_COL"]),  # Invalid column
                "The dataframe must contain the 'ROW_ID' and 'ROW_VERSION' columns.",
            ),
            (
                pd.DataFrame(columns=["INVALID_COL1", "INVALID_COL2"]),  # Both invalid
                "The dataframe must contain the 'ROW_ID' and 'ROW_VERSION' columns.",
            ),
        ],
    )
    async def test_delete_rows_via_dataframe_fail_missing_columns(self, df, error_msg):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()

        # WHEN I call delete_rows_async
        with (
            patch(
                "synapseclient.models.mixins.table_components.QueryMixin.query_async",
                return_value=pd.DataFrame(
                    {"ROW_ID": ["A", "B"], "ROW_VERSION": [1, 2]}
                ),
            ) as mock_query_async,
            patch.object(self.syn.logger, "info") as mock_logger_info,
        ):
            with pytest.raises(ValueError, match=error_msg):
                result = await test_instance.delete_rows_async(
                    df=df, synapse_client=self.syn
                )

                # THEN mock_logger_info should not be called
                mock_logger_info.assert_not_called()

    @pytest.mark.parametrize(
        "df, error_msg",
        [
            (
                pd.DataFrame(
                    {"ROW_ID": ["C", "D"], "ROW_VERSION": [2, 2]}
                ),  # Both invalid
                "Rows with the following ROW_ID and ROW_VERSION pairs were not found in table syn123: \\(C, 2\\), \\(D, 2\\).",  # Special characters must be escaped due to use with regex in test
            ),
        ],
    )
    async def test_delete_rows_via_dataframe_fail_missing_rows(self, df, error_msg):
        # GIVEN a TestClass instance
        test_instance = self.ClassForTest()

        # WHEN I call delete_rows_async
        with (
            patch(
                "synapseclient.models.mixins.table_components.QueryMixin.query_async",
                return_value=pd.DataFrame(
                    {"ROW_ID": ["A", "B"], "ROW_VERSION": [1, 2]}
                ),
            ) as mock_query_async,
            patch.object(self.syn.logger, "info") as mock_logger_info,
        ):
            with pytest.raises(LookupError, match=error_msg):
                result = await test_instance.delete_rows_async(
                    df=df, synapse_client=self.syn
                )

                # THEN mock_logger_info should not be called
                mock_logger_info.assert_not_called()


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
                SelectColumn(name="col1", column_type=ColumnType.STRING, id="111"),
                SelectColumn(name="col2", column_type=ColumnType.INTEGER, id="222"),
            ],
            response_concrete_type="org.sagebionetworks.repo.model.table.DownloadFromTableResult",
        )

    @pytest.fixture
    def sample_file_path(self):
        """Sample file path for downloaded CSV."""
        return "/path/to/downloaded/file.csv"

    async def test_query_table_csv_request_generation(self, sample_query):
        """Test that QueryJob generates the correct synapse request."""
        # GIVEN custom parameters for CSV formatting
        custom_params = {
            "quote_character": "'",
            "escape_character": "/",
            "line_end": "\n",
            "separator": ";",
            "is_first_line_header": False,
        }
        csv_table_descriptor = CsvTableDescriptor(**custom_params)

        # WHEN creating a QueryJob with these parameters
        query_job = QueryJob(
            entity_id="syn1234",
            sql=sample_query,
            include_row_id_and_row_version=False,
            write_header=False,
            csv_table_descriptor=csv_table_descriptor,
        )

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


class TestQueryResultOutput:
    """Test suite for the QueryResultOutput.fill_from_dict method."""

    @pytest.fixture
    def sample_dataframe(self):
        """Sample pandas DataFrame for testing."""
        import pandas as pd

        return pd.DataFrame(
            {"col1": ["A", "B", "C"], "col2": [1, 2, 3], "col3": ["X", "Y", "Z"]}
        )

    def test_fill_from_dict_with_full_data(self, sample_dataframe):
        """Test fill_from_dict with complete data including sum_file_sizes."""
        # GIVEN a complete data dictionary
        data = {
            "count": 100,
            "last_updated_on": "2025-08-20T10:00:00.000Z",
            "sum_file_sizes": SumFileSizes(
                sum_file_size_bytes=1024000, greater_than=False
            ),
        }
        # WHEN calling fill_from_dict
        result = QueryResultOutput.fill_from_dict(result=sample_dataframe, data=data)

        # THEN verify all attributes are set correctly
        assert result.result.equals(sample_dataframe)
        assert result.count == 100
        assert result.last_updated_on == "2025-08-20T10:00:00.000Z"
        assert result.sum_file_sizes.sum_file_size_bytes == 1024000
        assert result.sum_file_sizes.greater_than == False


class TestRow:
    """Test suite for the Row class."""

    @pytest.fixture
    def sample_row_data(self):
        """Sample row data for testing."""
        return {
            "rowId": 12345,
            "versionNumber": 1,
            "etag": "test-etag-123",
            "values": ["A", "1", "true", "160000000"],
        }

    @pytest.fixture
    def sample_headers(self):
        """Sample headers for testing cast_values method."""
        return [
            {"columnType": "STRING", "name": "string_col"},
            {"columnType": "INTEGER", "name": "int_col"},
            {"columnType": "BOOLEAN", "name": "bool_col"},
            {"columnType": "DATE", "name": "date_col"},
        ]

    def test_fill_from_dict_complete_data(self, sample_row_data):
        """Test fill_from_dict with complete row data."""
        # WHEN creating Row from dictionary
        row = Row.fill_from_dict(sample_row_data)

        # THEN verify all fields are populated correctly
        assert row.row_id == 12345
        assert row.version_number == 1
        assert row.etag == "test-etag-123"
        assert row.values == ["A", "1", "true", "160000000"]

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("t", True),
            ("T", True),
            ("1", True),
            ("false", False),
            ("False", False),
            ("FALSE", False),
            ("f", False),
            ("F", False),
            ("0", False),
        ],
    )
    def test_to_boolean_valid_values(self, value, expected):
        """Test to_boolean method with valid boolean values."""
        # WHEN calling to_boolean with valid values
        result = Row.to_boolean(value)

        # THEN verify correct boolean conversion
        assert result == expected
        assert isinstance(result, bool)

    @pytest.mark.parametrize(
        "invalid_value",
        [
            "invalid",
            "yes",
            "no",
            "2",
            "",
            None,
        ],
    )
    def test_to_boolean_invalid_values(self, invalid_value):
        """Test to_boolean method with invalid values."""
        # WHEN calling to_boolean with invalid values
        # THEN verify ValueError is raised
        with pytest.raises(
            ValueError, match=f"Can't convert {invalid_value} to boolean"
        ):
            Row.to_boolean(invalid_value)

    def test_cast_values_string_column(self):
        """Test cast_values with STRING column type."""
        # GIVEN string values and headers
        values = ["hello", "world", "test"]
        headers = [
            {"columnType": "STRING"},
            {"columnType": "STRING"},
            {"columnType": "STRING"},
        ]

        # WHEN casting values
        result = Row.cast_values(values, headers)

        # THEN verify strings are preserved
        assert result == ["hello", "world", "test"]

    def test_cast_values_integer_column(self):
        """Test cast_values with INTEGER column type."""
        # GIVEN integer values and headers
        values = ["123", "456", "789"]
        headers = [
            {"columnType": "INTEGER"},
            {"columnType": "INTEGER"},
            {"columnType": "INTEGER"},
        ]

        # WHEN casting values
        result = Row.cast_values(values, headers)

        # THEN verify integers are converted
        assert result == [123, 456, 789]
        assert all(isinstance(val, int) for val in result)


class TestActionRequiredCount:
    """Test suite for the ActionRequiredCount.fill_from_dict method."""

    def test_fill_from_dict_with_complete_data(self):
        """Test fill_from_dict with complete action data."""
        # GIVEN complete action data
        data = {
            "action": {
                "concreteType": "org.sagebionetworks.repo.model.download.MeetAccessRequirement",
                "accessRequirementId": 12345,
            },
            "count": 42,
        }

        # WHEN calling fill_from_dict
        result = ActionRequiredCount.fill_from_dict(data)

        # THEN verify all attributes are set correctly
        assert result.action == data["action"]
        assert result.count == 42


class TestSelectColumn:
    """Test suite for the SelectColumn.fill_from_dict method."""

    def test_fill_from_dict_with_complete_data(self):
        """Test fill_from_dict with complete column data."""
        # GIVEN complete column data
        data = {"name": "test_column", "columnType": "STRING", "id": "123456"}

        # WHEN calling fill_from_dict
        result = SelectColumn.fill_from_dict(data)

        # THEN verify all attributes are set correctly
        assert result.name == "test_column"
        assert result.column_type == ColumnType.STRING
        assert result.id == "123456"

    def test_fill_from_dict_with_valid_column_types(self):
        """Test fill_from_dict with all valid column types."""
        valid_column_types = [
            "STRING",
            "DOUBLE",
            "INTEGER",
            "BOOLEAN",
            "DATE",
            "FILEHANDLEID",
            "ENTITYID",
            "LINK",
            "MEDIUMTEXT",
            "LARGETEXT",
            "USERID",
            "STRING_LIST",
            "INTEGER_LIST",
            "USERID_LIST",
            "JSON",
        ]

        for column_type_str in valid_column_types:
            # GIVEN data with valid column type
            data = {
                "name": f"test_{column_type_str.lower()}",
                "columnType": column_type_str,
                "id": "123",
            }

            # WHEN calling fill_from_dict
            result = SelectColumn.fill_from_dict(data)

            # THEN verify column type is converted correctly
            assert result.column_type == ColumnType(column_type_str)
            assert result.name == f"test_{column_type_str.lower()}"
            assert result.id == "123"


class TestQueryResult:
    """Test suite for the QueryResult.fill_from_dict method."""

    @pytest.fixture
    def sample_rowset_data(self):
        """Sample RowSet data for testing."""
        return {
            "concreteType": "org.sagebionetworks.repo.model.table.RowSet",
            "tableId": "syn123456",
            "etag": "rowset-etag",
            "headers": [{"name": "col1", "columnType": "STRING", "id": "123"}],
            "rows": [{"rowId": 1, "versionNumber": 1, "values": ["test_value"]}],
        }

    @pytest.fixture
    def sample_next_page_token_data(self):
        """Sample QueryNextPageToken data for testing."""
        return {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryNextPageToken",
            "entityId": "syn123456",
            "token": "next-page-token-xyz",
        }

    def test_fill_from_dict_with_complete_data(
        self, sample_rowset_data, sample_next_page_token_data
    ):
        """Test fill_from_dict with complete QueryResult data."""
        # GIVEN complete QueryResult data
        data = {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryResult",
            "queryResults": sample_rowset_data,
            "nextPageToken": sample_next_page_token_data,
        }

        # WHEN calling fill_from_dict
        result = QueryResult.fill_from_dict(data)

        # THEN verify all attributes are set correctly
        assert (
            result.concrete_type == "org.sagebionetworks.repo.model.table.QueryResult"
        )

        # Verify nested RowSet
        assert isinstance(result.query_results, RowSet)
        assert (
            result.query_results.concrete_type
            == "org.sagebionetworks.repo.model.table.RowSet"
        )
        assert result.query_results.table_id == "syn123456"
        assert result.query_results.etag == "rowset-etag"

        # Verify nested QueryNextPageToken
        assert isinstance(result.next_page_token, QueryNextPageToken)
        assert (
            result.next_page_token.concrete_type
            == "org.sagebionetworks.repo.model.table.QueryNextPageToken"
        )
        assert result.next_page_token.entity_id == "syn123456"
        assert result.next_page_token.token == "next-page-token-xyz"


class TestRowSet:
    """Test suite for the RowSet.fill_from_dict method."""

    @pytest.fixture
    def sample_row_data(self):
        """Sample row data for testing."""
        return [
            {
                "rowId": 1,
                "versionNumber": 1,
                "etag": "etag-1",
                "values": ["A", "1", "true"],
            },
            {
                "rowId": 2,
                "versionNumber": 2,
                "etag": "etag-2",
                "values": ["B", "2", "false"],
            },
        ]

    @pytest.fixture
    def sample_header_data(self):
        """Sample header data for testing."""
        return [
            {"name": "col1", "columnType": "STRING", "id": "123"},
            {"name": "col2", "columnType": "INTEGER", "id": "124"},
            {"name": "col3", "columnType": "BOOLEAN", "id": "125"},
        ]

    def test_fill_from_dict_with_complete_data(
        self, sample_row_data, sample_header_data
    ):
        """Test fill_from_dict with complete RowSet data."""
        # GIVEN complete RowSet data
        data = {
            "concreteType": "org.sagebionetworks.repo.model.table.RowSet",
            "tableId": "syn123456",
            "etag": "table-etag-123",
            "headers": sample_header_data,
            "rows": sample_row_data,
        }

        # WHEN calling fill_from_dict
        result = RowSet.fill_from_dict(data)

        # THEN verify all attributes are set correctly
        assert result.concrete_type == "org.sagebionetworks.repo.model.table.RowSet"
        assert result.table_id == "syn123456"
        assert result.etag == "table-etag-123"

        # Verify headers
        assert len(result.headers) == 3
        assert result.headers[0].name == "col1"
        assert result.headers[0].column_type == ColumnType.STRING
        assert result.headers[0].id == "123"
        assert result.headers[1].name == "col2"
        assert result.headers[1].column_type == ColumnType.INTEGER
        assert result.headers[1].id == "124"

        # Verify rows
        assert len(result.rows) == 2
        assert result.rows[0].row_id == 1
        assert result.rows[0].version_number == 1
        assert result.rows[0].etag == "etag-1"
        assert result.rows[0].values == ["A", 1, True]
        assert result.rows[1].row_id == 2
        assert result.rows[1].version_number == 2
        assert result.rows[1].etag == "etag-2"
        assert result.rows[1].values == ["B", 2, False]


class TestQueryNextPageToken:
    """Test suite for the QueryNextPageToken.fill_from_dict method."""

    def test_fill_from_dict_with_complete_data(self):
        """Test fill_from_dict with complete token data."""
        # GIVEN complete token data
        data = {
            "concreteType": "org.sagebionetworks.repo.model.table.QueryNextPageToken",
            "entityId": "syn123456",
            "token": "next-page-token-12345",
        }

        # WHEN calling fill_from_dict
        result = QueryNextPageToken.fill_from_dict(data)

        # THEN verify all attributes are set correctly
        assert (
            result.concrete_type
            == "org.sagebionetworks.repo.model.table.QueryNextPageToken"
        )
        assert result.entity_id == "syn123456"
        assert result.token == "next-page-token-12345"


class TestQueryJob:
    """Test suite for the QueryJob.to_synapse_request and fill_from_dict methods."""

    @pytest.fixture
    def sample_csv_descriptor(self):
        """Sample CsvTableDescriptor for testing."""
        return CsvTableDescriptor(
            quote_character="'",
            escape_character="/",
            line_end="\n",
            separator=";",
        )

    def test_to_synapse_request_with_defaults(self):
        """Test to_synapse_request with default parameters."""
        # GIVEN a QueryJob with minimal parameters (using defaults)
        job = QueryJob(entity_id="syn123456", sql="SELECT * FROM syn123456")

        # WHEN calling to_synapse_request
        result = job.to_synapse_request()

        # THEN verify default values are set correctly
        expected = {
            "concreteType": QUERY_TABLE_CSV_REQUEST,
            "entityId": "syn123456",
            "sql": "SELECT * FROM syn123456",
            "writeHeader": True,  # Default value
            "includeRowIdAndRowVersion": True,  # Default value
            "includeEntityEtag": False,  # Default value
        }
        assert result == expected

    def test_to_synapse_request_with_none_values(self):
        """Test that None values are properly excluded from request."""
        # GIVEN a QueryJob with some None values
        job = QueryJob(
            entity_id="syn123456",
            sql="SELECT * FROM syn123456",
            csv_table_descriptor=None,  # Should be excluded
            include_entity_etag=None,  # Should be excluded
        )

        # WHEN calling to_synapse_request
        result = job.to_synapse_request()

        # THEN verify None values are not included
        assert "csvTableDescriptor" not in result
        assert "includeEntityEtag" not in result

    def test_to_synapse_request_csv_descriptor_integration(self, sample_csv_descriptor):
        """Test that CsvTableDescriptor is properly integrated in request."""
        # GIVEN a QueryJob with CsvTableDescriptor
        job = QueryJob(
            entity_id="syn123456",
            sql="SELECT * FROM syn123456",
            csv_table_descriptor=sample_csv_descriptor,
        )

        # WHEN calling to_synapse_request
        result = job.to_synapse_request()

        # THEN verify CsvTableDescriptor is included correctly
        assert "csvTableDescriptor" in result
        csv_desc = result["csvTableDescriptor"]
        assert csv_desc["quoteCharacter"] == "'"
        assert csv_desc["escapeCharacter"] == "/"
        assert csv_desc["lineEnd"] == "\n"
        assert csv_desc["separator"] == ";"

    def test_fill_from_dict_with_complete_response(self):
        """Test fill_from_dict with complete DownloadFromTableResult response."""
        # GIVEN a QueryJob and complete response data
        job = QueryJob(entity_id="syn123456", sql="SELECT * FROM syn123456")
        response_data = {
            "jobId": "async-job-12345",
            "concreteType": "org.sagebionetworks.repo.model.table.DownloadFromTableResult",
            "resultsFileHandleId": "file-handle-67890",
            "tableId": "syn123456",
            "etag": "table-etag-abc123",
            "headers": [
                {"name": "col1", "columnType": "STRING", "id": "111"},
                {"name": "col2", "columnType": "INTEGER", "id": "222"},
            ],
        }

        # WHEN calling fill_from_dict
        result = job.fill_from_dict(response_data)

        # THEN verify all response attributes are set
        assert result is job  # Should return self
        assert job.job_id == "async-job-12345"
        assert (
            job.response_concrete_type
            == "org.sagebionetworks.repo.model.table.DownloadFromTableResult"
        )
        assert job.results_file_handle_id == "file-handle-67890"
        assert job.table_id == "syn123456"
        assert job.etag == "table-etag-abc123"

        # Verify the nested SelectColumns
        assert isinstance(result.headers, list)
        assert len(result.headers) == 2
        assert isinstance(result.headers[0], SelectColumn)
        assert isinstance(result.headers[1], SelectColumn)
        assert result.headers[0].name == "col1"
        assert result.headers[0].column_type == "STRING"
        assert result.headers[0].id == "111"
        assert result.headers[1].name == "col2"
        assert result.headers[1].column_type == "INTEGER"
        assert result.headers[1].id == "222"


class TestQueryTableRowSet:
    """Test suite for the _query_table_row_set function."""

    @pytest.fixture
    def mock_synapse_client(self):
        """Mock Synapse client for testing."""
        mock_client = MagicMock()
        return mock_client

    @pytest.fixture
    def sample_query_result_bundle(self):
        """Sample QueryResultBundle response."""
        return QueryResultBundle(
            query_result=QueryResult(
                concrete_type=QUERY_RESULT,
                query_results=RowSet(
                    table_id="syn123456",
                    etag="test-etag",
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
                            etag=None,
                            values=["random string1", "random string2"],
                        )
                    ],
                ),
                next_page_token=None,
            ),
            query_count=1,
            select_columns=[
                SelectColumn(name="col1", column_type=ColumnType.STRING, id="111"),
                SelectColumn(name="col2", column_type=ColumnType.INTEGER, id="222"),
            ],
            max_rows_per_page=1000,
            column_models=[
                Column(
                    id="242777",
                    name="test_col",
                    column_type=ColumnType.STRING,
                    facet_type=None,
                    default_value=None,
                    maximum_size=50,
                    maximum_list_length=None,
                    enum_values=None,
                    json_sub_columns=None,
                ),
                Column(
                    id="242778",
                    name="test_col2",
                    column_type=ColumnType.STRING,
                    facet_type=None,
                    default_value=None,
                    maximum_size=50,
                    maximum_list_length=None,
                    enum_values=None,
                    json_sub_columns=None,
                ),
            ],
            facets=[],
            sum_file_sizes=SumFileSizes(sum_file_size_bytes=1024, greater_than=False),
            last_updated_on="2025-08-26T21:38:31.677Z",
            combined_sql="SELECT col1, col2 FROM syn123456",
            actions_required=None,
        )

    async def test_query_table_row_set_basic(
        self, mock_synapse_client, sample_query_result_bundle
    ):
        """Test basic query_table_row_set functionality."""
        # GIVEN a query and mock response
        query = "SELECT col1, col2 FROM syn123456"

        with (
            patch(
                "synapseclient.models.mixins.table_components.extract_synapse_id_from_query",
                return_value="syn123456",
            ) as mock_extract_id,
            patch.object(
                QueryBundleRequest,
                "send_job_and_wait_async",
                return_value=sample_query_result_bundle,
            ) as mock_send_job,
        ):
            # WHEN calling _query_table_row_set
            result = await _query_table_row_set(
                query=query,
                synapse=mock_synapse_client,
            )

            # THEN verify the result
            assert isinstance(result, QueryResultBundle)
            assert result.query_count == 1
            assert result.query_result == sample_query_result_bundle.query_result
            assert result.select_columns == sample_query_result_bundle.select_columns
            assert result.sum_file_sizes == sample_query_result_bundle.sum_file_sizes
            assert result.last_updated_on == sample_query_result_bundle.last_updated_on
            assert result.combined_sql == sample_query_result_bundle.combined_sql
            assert result.column_models == sample_query_result_bundle.column_models
            assert result.facets == sample_query_result_bundle.facets
            assert (
                result.actions_required == sample_query_result_bundle.actions_required
            )

            # Verify extract_synapse_id_from_query was called correctly
            mock_extract_id.assert_called_once_with(query)

            # Verify send_job_and_wait_async was called correctly
            mock_send_job.assert_called_once_with(
                synapse_client=mock_synapse_client, timeout=250
            )

    async def test_query_table_row_set_with_parameters(
        self, mock_synapse_client, sample_query_result_bundle
    ):
        """Test _query_table_row_set with all optional parameters."""
        # GIVEN a query with all parameters
        query = "SELECT col1, col2 FROM syn123456"
        limit = 100
        offset = 50
        part_mask = 0x1 | 0x2 | 0x4  # Query results + count + select columns

        with (
            patch(
                "synapseclient.models.mixins.table_components.extract_synapse_id_from_query",
                return_value="syn123456",
            ) as mock_extract_id,
            patch(
                "synapseclient.models.mixins.table_components.Query"
            ) as mock_query_class,
            patch.object(
                QueryBundleRequest,
                "send_job_and_wait_async",
                return_value=sample_query_result_bundle,
            ) as mock_send_job,
        ):
            # Create mock instances
            mock_query_instance = MagicMock()
            mock_query_class.return_value = mock_query_instance

            # WHEN calling _query_table_row_set with parameters
            result = await _query_table_row_set(
                query=query,
                synapse=mock_synapse_client,
                limit=limit,
                offset=offset,
                part_mask=part_mask,
            )
            # THEN verify the Query was created with correct parameters
            mock_query_class.assert_called_once_with(
                sql=query,
                include_entity_etag=True,
                limit=limit,
                offset=offset,
            )

            # THEN verify the result structure
            assert isinstance(result, QueryResultBundle)
            assert result.query_count == 1
            assert result.query_result == sample_query_result_bundle.query_result

            # Verify the QueryBundleRequest was created with correct parameters
            mock_send_job.assert_called_once_with(
                synapse_client=mock_synapse_client, timeout=250
            )


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


class TestCsvToPandasDf:
    """Test suite for csv_to_pandas_df function focusing on date and list columns."""

    @pytest.fixture
    def csv_with_date_columns(self):
        """CSV content with date columns (epoch time in milliseconds)."""
        return "id,name,created_date\n1,Alice,1609459200000\n2,Bob,1609545600000\n3,Charlie,1609632000000"

    @pytest.fixture
    def csv_with_list_columns(self):
        """CSV content with integer, boolean, and string list columns with NAs."""
        return 'name,age,city,number,bool,string,number_list,bool_list,string_list\nAlice,30,New York,42,"True","hello","[1, 2, 3]","[true, false, true]","[1, 2]"\nBob,25,Los Angeles,10,"False","world",,"[false, true]","[3]"\nCharlie,35,Chicago,99,"True","test","[6, 7, 8, 9]",,"[4, 5, 6]"'

    @pytest.fixture
    def csv_with_list_columns_with_na_in_items_and_date_columns(self):
        """CSV content with list columns containing NA values within list items. Use null instead of None to avoid type errors for json.loads."""
        return 'name,age,city,number,bool,string,created_date,number_list,bool_list,string_list,userid_list,entityid_list\nAlice,30,New York,42,"True","hello",1609459200000,"[1, null, 3]","[true, null, false]","[\\"tag1\\", null, \\"tag3\\"]","[123, null, 456]","[\\"syn123\\", null, \\"syn456\\"]"\nBob,25,Los Angeles,,"False","world",1609545600000,"[null, 5]","[null, true]","[null, \\"tag2\\"]","[null, 789]","[null, \\"syn789\\"]"\nCharlie,35,Chicago,99,"True","test",1609632000000,"[6, null, null, 9]","[null, null, false]","[null, \\"tag4\\", null]","[101, null, null, 202]","[\\"syn101\\", null, null, \\"syn202\\"]"'

    @pytest.fixture
    def csv_with_date_and_list_columns(self):
        """CSV content with both date and list columns."""
        return 'id,name,created_date,number,bool,string,number_list,bool_list,string_list\n1,Alice,1609459200000,42,"True","hello","[1, 2, 3]","[true, false, true]","[\\"tag1\\", \\"tag2\\"]"\n2,Bob,1609545600000,10,"False","world",,"[false, true]","[\\"tag3\\"]"\n3,Charlie,1609632000000,99,"True","test","[6, 7, 8, 9]",,"[\\"tag4\\", \\"tag5\\", \\"tag6\\"]"'

    @pytest.fixture
    def csv_with_row_id_and_version_and_etag_in_index(self):
        """CSV content with row id, version, and etag in index."""
        return 'ROW_ID,ROW_VERSION,ROW_ETAG,name,age,city,number,bool,string,created_date,number_list,bool_list,string_list,userid_list,entityid_list\n1,1,test-etag,Alice,30,New York,42,"True","hello",1609459200000,"[1, null, 3]","[true, null, false]","[\\"tag1\\", null, \\"tag3\\"]","[123, null, 456]","[\\"syn123\\", null, \\"syn456\\"]"\n2,1,test-etag,Bob,25,Los Angeles,,"False","world",1609545600000,,"[null, true]","[null, \\"tag2\\"]","[null, 789]","[null, \\"syn789\\"]"\n3,1,test-etag,Charlie,35,Chicago,99,"True","test",1609632000000,"[6, null, null, 9]","[null, null, false]","[null, \\"tag4\\", null]","[101, null, null, 202]","[\\"syn101\\", null, null, \\"syn202\\"]"'

    def test_csv_to_pandas_df_with_date_columns(self, csv_with_date_columns):
        """Test csv_to_pandas_df correctly converts date columns to datetime."""
        # WHEN converting CSV with date columns
        csv_file = BytesIO(csv_with_date_columns.encode("utf-8"))
        df = csv_to_pandas_df(
            filepath=csv_file,
            date_columns=["created_date"],
        )
        # THEN assert the date column is converted to datetime
        assert str(df["created_date"].dtype) == "datetime64[ns, UTC]"

        expected_dates = pd.to_datetime(
            [1609459200000, 1609545600000, 1609632000000], unit="ms", utc=True
        )
        # THEN assert the create_date column is equal to the expected dates
        pd.testing.assert_series_equal(
            df["created_date"], pd.Series(expected_dates), check_names=False
        )

    def test_csv_to_pandas_df_with_all_list_columns(self, csv_with_list_columns):
        """Test csv_to_pandas_df correctly parses all list column types together."""
        # WHEN converting CSV with all list column types
        csv_file = BytesIO(csv_with_list_columns.encode("utf-8"))
        df = csv_to_pandas_df(
            filepath=csv_file,
            list_columns=["number_list", "bool_list", "string_list"],
            list_column_types={
                "number_list": "INTEGER_LIST",
                "bool_list": "BOOLEAN_LIST",
                "string_list": "STRING_LIST",
            },
        )
        # expected dataframe content
        expected_df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
                "city": ["New York", "Los Angeles", "Chicago"],
                "number": [42, 10, 99],
                "bool": [True, False, True],
                "string": ["hello", "world", "test"],
                "number_list": [[1, 2, 3], [], [6, 7, 8, 9]],
                "bool_list": [[True, False, True], [False, True], []],
                "string_list": [
                    ["1", "2"],
                    ["3"],
                    ["4", "5", "6"],
                ],  # integers are converted to strings
            }
        )
        # THEN assert the dataframe is equal to the expected dataframe
        pd.testing.assert_frame_equal(df, expected_df)

    def test_csv_to_pandas_df_with_date_and_list_columns(
        self, csv_with_date_and_list_columns
    ):
        """Test csv_to_pandas_df correctly handles both date and list columns together."""
        # WHEN converting CSV with both date and list columns
        csv_file = BytesIO(csv_with_date_and_list_columns.encode("utf-8"))
        df = csv_to_pandas_df(
            filepath=csv_file,
            date_columns=["created_date"],
            list_columns=["number_list", "bool_list", "string_list"],
            list_column_types={
                "number_list": "INTEGER_LIST",
                "bool_list": "BOOLEAN_LIST",
                "string_list": "STRING_LIST",
            },
        )
        # expected dataframe content
        expected_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "created_date": pd.to_datetime(
                    [1609459200000, 1609545600000, 1609632000000], unit="ms", utc=True
                ),
                "number": [42, 10, 99],
                "bool": [True, False, True],
                "string": ["hello", "world", "test"],
                "number_list": [[1, 2, 3], [], [6, 7, 8, 9]],
                "bool_list": [[True, False, True], [False, True], []],
                "string_list": [["tag1", "tag2"], ["tag3"], ["tag4", "tag5", "tag6"]],
            }
        )
        # THEN assert the dataframe is equal to the expected dataframe
        pd.testing.assert_frame_equal(df, expected_df)

    def test_csv_to_pandas_df_list_columns_without_types(self, csv_with_list_columns):
        """Test csv_to_pandas_df handles list columns without explicit list_column_types. NAs are filled with empty lists."""
        # WHEN converting CSV with list columns but no list_column_types
        csv_file = BytesIO(csv_with_list_columns.encode("utf-8"))
        df = csv_to_pandas_df(
            filepath=csv_file,
            list_columns=["number_list", "bool_list", "string_list"],
        )
        expected_df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
                "city": ["New York", "Los Angeles", "Chicago"],
                "number": [42, 10, 99],
                "bool": [True, False, True],
                "string": ["hello", "world", "test"],
                "number_list": [[1, 2, 3], [], [6, 7, 8, 9]],
                "bool_list": [[True, False, True], [False, True], []],
                "string_list": [
                    [1, 2],
                    [3],
                    [4, 5, 6],
                ],  # integers are not converted to strings since they are not in list_column_types
            }
        )
        # THEN assert the dataframe is equal to the expected dataframe
        pd.testing.assert_frame_equal(df, expected_df)

    def test_csv_to_pandas_df_all_list_types_with_na_in_items_and_date_columns(
        self, csv_with_list_columns_with_na_in_items_and_date_columns
    ):
        """Test csv_to_pandas_df handles NA values within all list column types and date columns."""
        # WHEN converting CSV with all list types containing None values and date columns
        csv_file = BytesIO(
            csv_with_list_columns_with_na_in_items_and_date_columns.encode("utf-8")
        )
        df = csv_to_pandas_df(
            filepath=csv_file,
            date_columns=["created_date"],
            list_columns=[
                "number_list",
                "bool_list",
                "string_list",
                "userid_list",
                "entityid_list",
            ],
            list_column_types={
                "number_list": "INTEGER_LIST",
                "bool_list": "BOOLEAN_LIST",
                "string_list": "STRING_LIST",
                "userid_list": "USERID_LIST",
                "entityid_list": "ENTITYID_LIST",
            },
        )
        # expected dataframe content
        expected_df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
                "city": ["New York", "Los Angeles", "Chicago"],
                "number": [
                    42.0,
                    np.nan,
                    99.0,
                ],  # Integers are converted to floats due to the presence of NaN values
                "bool": [
                    True,
                    False,
                    True,
                ],  # Read as strings from CSV, and converted to booleans
                "string": ["hello", "world", "test"],
                "created_date": pd.to_datetime(
                    [1609459200000, 1609545600000, 1609632000000], unit="ms", utc=True
                ),
                "number_list": [
                    [1, None, 3],
                    [None, 5],
                    [6, None, None, 9],
                ],  # None values remain as None
                "bool_list": [
                    [True, None, False],
                    [None, True],
                    [None, None, False],
                ],  # None values are preserved as None
                "string_list": [
                    ["tag1", "", "tag3"],
                    ["", "tag2"],
                    ["", "tag4", ""],
                ],  # None values are preserved as ""
                "userid_list": [
                    ["123", "", "456"],
                    ["", "789"],
                    ["101", "", "", "202"],
                ],  # None values are preserved as ""
                "entityid_list": [
                    ["syn123", "", "syn456"],
                    ["", "syn789"],
                    ["syn101", "", "", "syn202"],
                ],  # None values are preserved as ""
            }
        )
        # THEN assert the dataframe is equal to the expected dataframe
        pd.testing.assert_frame_equal(df, expected_df)

    def test_csv_pandas_df_with_row_id_and_version_etag_in_index(
        self, csv_with_row_id_and_version_and_etag_in_index
    ):
        """Test csv_to_pandas_df handles row id and version in index. NAs are filled with empty lists."""
        # WHEN converting CSV with row id and version in index
        csv_file = BytesIO(
            csv_with_row_id_and_version_and_etag_in_index.encode("utf-8")
        )
        df = csv_to_pandas_df(
            filepath=csv_file,
            row_id_and_version_in_index=True,
            date_columns=["created_date"],
            list_columns=[
                "number_list",
                "bool_list",
                "string_list",
                "userid_list",
                "entityid_list",
            ],
            list_column_types={
                "number_list": "INTEGER_LIST",
                "bool_list": "BOOLEAN_LIST",
                "string_list": "STRING_LIST",
                "userid_list": "USERID_LIST",
                "entityid_list": "ENTITYID_LIST",
            },
        )
        # expected dataframe content
        expected_df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
                "city": ["New York", "Los Angeles", "Chicago"],
                "number": [
                    42.0,
                    np.nan,
                    99.0,
                ],  # Integers are converted to floats due to the presence of NaN values
                "bool": [True, False, True],
                "string": ["hello", "world", "test"],
                "created_date": pd.to_datetime(
                    [1609459200000, 1609545600000, 1609632000000], unit="ms", utc=True
                ),
                "number_list": [[1, None, 3], [], [6, None, None, 9]],
                "bool_list": [[True, None, False], [None, True], [None, None, False]],
                "string_list": [["tag1", "", "tag3"], ["", "tag2"], ["", "tag4", ""]],
                "userid_list": [
                    ["123", "", "456"],
                    ["", "789"],
                    ["101", "", "", "202"],
                ],
                "entityid_list": [
                    ["syn123", "", "syn456"],
                    ["", "syn789"],
                    ["syn101", "", "", "syn202"],
                ],
            },
            index=["1_1_test-etag", "2_1_test-etag", "3_1_test-etag"],
        )
        # THEN assert the dataframe is equal to the expected dataframe
        pd.testing.assert_frame_equal(df, expected_df)
