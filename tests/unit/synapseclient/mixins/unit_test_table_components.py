import os
import re
import tempfile
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime
from io import BytesIO
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from pandas.api.types import is_integer_dtype, is_object_dtype

from synapseclient import Synapse
from synapseclient.api import ViewEntityType, ViewTypeMask
from synapseclient.core.constants.concrete_types import (
    APPENDABLE_ROWSET_REQUEST,
    ENTITY_UPDATE_RESULTS,
    QUERY_BUNDLE_REQUEST,
    QUERY_RESULT,
    QUERY_TABLE_CSV_REQUEST,
    ROW_REFERENCE_SET_RESULTS,
    TABLE_SCHEMA_CHANGE_REQUEST,
    TABLE_SCHEMA_CHANGE_RESPONSE,
    TABLE_SEARCH_CHANGE_REQUEST,
    TABLE_SEARCH_CHANGE_RESPONSE,
    UPLOAD_TO_TABLE_REQUEST,
    UPLOAD_TO_TABLE_RESULT,
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
    TableStoreRowMixin,
    TableUpdateTransaction,
    TableUpsertMixin,
    ViewBase,
    ViewSnapshotMixin,
    ViewStoreMixin,
    ViewUpdateMixin,
    _construct_composite_key_conditions,
    _construct_composite_key_where_statement,
    _construct_partial_rows_for_upsert,
    _construct_select_statement_for_upsert,
    _construct_single_key_where_statement,
    _convert_csv_date_cols_to_epoch_time,
    _convert_df_date_cols_to_epoch_time,
    _format_primary_key_value_for_where,
    _is_date_list_column,
    _log_upsert_summary,
    _parse_df_date_cols_to_datetime,
    _query_table_csv,
    _query_table_next_page,
    _query_table_row_set,
    _upsert_rows_async,
    _validate_primary_keys,
    convert_dtypes_to_json_serializable,
    csv_to_pandas_df,
)
from synapseclient.models.table_components import (
    ActionRequiredCount,
    AppendableRowSetRequest,
    ColumnChange,
    ColumnType,
    CsvTableDescriptor,
    EntityUpdateFailureCode,
    EntityUpdateResult,
    EntityUpdateResults,
    PartialRow,
    PartialRowSet,
    Query,
    QueryBundleRequest,
    QueryJob,
    QueryNextPageToken,
    QueryResult,
    QueryResultBundle,
    QueryResultOutput,
    Row,
    RowReference,
    RowReferenceSet,
    RowReferenceSetResults,
    RowSet,
    SchemaStorageStrategy,
    SelectColumn,
    SumFileSizes,
    TableSchemaChangeRequest,
    TableSchemaChangeResponse,
    TableSearchChangeRequest,
    TableSearchChangeResponse,
    TableUpdateRequest,
    TableUpdateResponse,
    UnknownTableUpdateResponse,
    UploadToTableRequest,
    UploadToTableResult,
    table_update_response_from_dict,
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
_PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH = (
    "synapseclient.models.mixins.table_components._push_row_updates_to_synapse"
)
DEFAULT_QUOTE_CHARACTER = '"'
DEFAULT_SEPARATOR = ","
DEFAULT_ESCAPE_CHAR = "\\"


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
                date_columns=None,
                date_format=None,
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

    def test_construct_partial_rows_for_upsert_date_column_from_csv_input_with_changes(
        self,
    ):
        # GIVEN an entity with a DATE column
        test_instance = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "date_col": Column(
                    name="date_col", column_type=ColumnType.DATE, id="id2"
                ),
            },
        )

        # Results from Synapse query (existing rows)
        # epoch ms value for 2024-01-15
        results = pd.DataFrame(
            {
                "ROW_ID": ["row1", "row2", "row3"],
                "col1": ["A", "B", "C"],
                "date_col": [1705276800000, 1705276800000, 1705276800000],
            }
        )

        # Data to upsert, as if parsed from a CSV file with date_col strings
        # "03/10/2024", "01/15/2024", and a blank date
        chunk_to_check_for_upsert = pd.DataFrame(
            {
                "col1": ["A", "B", "C"],
                "date_col": [1710028800000, 1705276800000, pd.NA],
            }
        )

        primary_keys = ["col1"]
        contains_etag = False
        wait_for_eventually_consistent_view = False

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

        assert len(rows_to_update) == 2
        assert len(indexes_with_changes) == 2
        assert len(indexes_without_changes) == 1
        assert len(syn_id_and_etags) == 0

        assert rows_to_update[0].row_id == "row1"
        assert rows_to_update[0].etag is None
        assert len(rows_to_update[0].values) == 1
        assert rows_to_update[0].values[0]["key"] == "id2"
        assert rows_to_update[0].values[0]["value"] == 1710028800000

        assert rows_to_update[1].row_id == "row3"
        assert rows_to_update[1].etag is None
        assert len(rows_to_update[1].values) == 1
        assert rows_to_update[1].values[0]["key"] == "id2"
        assert rows_to_update[1].values[0]["value"] is None

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


class TestFormatPrimaryKeyValueForWhere:
    """Test suite for _format_primary_key_value_for_where, which renders a single
    primary-key value as a SQL literal for an upsert WHERE clause."""

    @pytest.mark.parametrize(
        "value, column_type, expected",
        [
            # string-like column types are wrapped in single quotes
            ("abc", ColumnType.STRING, "'abc'"),
            ("abc", ColumnType.MEDIUMTEXT, "'abc'"),
            ("abc", ColumnType.LARGETEXT, "'abc'"),
            ("abc", ColumnType.LINK, "'abc'"),
            ("abc", ColumnType.ENTITYID, "'abc'"),
            # embedded single quotes are escaped by doubling them (guards against
            # SQL breakage/injection)
            ("O'Brien", ColumnType.STRING, "'O''Brien'"),
            ("a'b'c", ColumnType.STRING, "'a''b''c'"),
            # non-string values are coerced to str, then quoted
            (123, ColumnType.STRING, "'123'"),
            # only single quotes are escaped; double quotes and backslashes are
            # passed through unchanged (double quotes delimit identifiers, not
            # string literals, in Synapse SQL)
            ('foo"bar', ColumnType.STRING, "'foo\"bar'"),
            ("a\\b", ColumnType.STRING, "'a\\b'"),
            # boolean columns render as literal 'true'/'false'
            (True, ColumnType.BOOLEAN, "'true'"),
            (False, ColumnType.BOOLEAN, "'false'"),
            # numeric types and dates are rendered unquoted as their string representation
            (42, ColumnType.INTEGER, "42"),
            (-7, ColumnType.INTEGER, "-7"),
            (3.14, ColumnType.DOUBLE, "3.14"),
            (1700000000000, ColumnType.DATE, "1700000000000"),
        ],
    )
    def test_format_primary_key_value_for_where(self, value, column_type, expected):
        assert _format_primary_key_value_for_where(value, column_type) == expected


class TestConstructSelectStatementForUpsert:
    """Test suite for _construct_select_statement_for_upsert."""

    class ClassForTest(TableUpsertMixin):
        """A plain (non-etag) entity. Its class name is not in
        CLASSES_THAT_CONTAIN_ROW_ETAG, so the SELECT starts with ``ROW_ID``."""

        def __init__(self, id, columns):
            self.id = id
            self.columns = columns

    def test_single_string_primary_key(self):
        # GIVEN an entity with a single STRING primary key
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
                "col2": Column(name="col2", column_type=ColumnType.STRING, id="id2"),
            },
        )
        df = pd.DataFrame({"col1": ["A"], "col2": ["B"]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"col1"', '"col2"'],
            primary_keys=["col1"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN the string value is quoted and only the primary key is filtered
        assert (
            statement
            == 'SELECT ROW_ID, "col1", "col2" FROM syn123 WHERE "col1" IN (\'A\')'
        )

    def test_integer_primary_key_is_not_quoted(self):
        # GIVEN an entity whose primary key is an INTEGER column
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.INTEGER, id="id1"),
            },
        )
        df = pd.DataFrame({"col1": [1001]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"col1"'],
            primary_keys=["col1"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN the integer value is not quoted
        assert statement == 'SELECT ROW_ID, "col1" FROM syn123 WHERE "col1" IN (1001)'

    def test_composite_primary_keys_single_row_uses_tuple_matching(self):
        # GIVEN an entity with two STRING primary keys
        entity = self.ClassForTest(
            id="syn76174997",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1"),
                "synID": Column(name="synID", column_type=ColumnType.STRING, id="id2"),
            },
        )
        # AND a single-row DataFrame
        df = pd.DataFrame({"view": ["syn64762437"], "synID": ["syn66312955"]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"view"', '"synID"'],
            primary_keys=["view", "synID"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN the two keys are matched together as an exact tuple (AND of
        # equalities) rather than as independent IN clauses, so the query can only
        # match the exact (view, synID) pair present in the input.
        assert statement == (
            'SELECT ROW_ID, "view", "synID" FROM syn76174997 WHERE '
            "(\"view\" = 'syn64762437' AND \"synID\" = 'syn66312955')"
        )

    def test_composite_primary_keys_multiple_rows_use_or_of_ands(self):
        # GIVEN an entity with two STRING primary keys
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1"),
                "synID": Column(name="synID", column_type=ColumnType.STRING, id="id2"),
            },
        )
        # AND a two-row DataFrame whose values would form a cross-product if
        # filtered independently
        df = pd.DataFrame({"view": ["V1", "V2"], "synID": ["S1", "S2"]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"view"', '"synID"'],
            primary_keys=["view", "synID"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN each input row becomes its own (view = ... AND synID = ...) clause,
        # joined by OR, in input order. The spurious pairs (V1, S2) and (V2, S1)
        # are NOT selectable.
        assert statement == (
            'SELECT ROW_ID, "view", "synID" FROM syn123 WHERE '
            "(\"view\" = 'V1' AND \"synID\" = 'S1') OR "
            "(\"view\" = 'V2' AND \"synID\" = 'S2')"
        )

    def test_multiple_values_all_appear_in_in_clause(self):
        # GIVEN a DataFrame with multiple distinct primary key values
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
        )
        df = pd.DataFrame({"col1": ["A", "B", "A"]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"col1"'],
            primary_keys=["col1"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN both distinct values appear (order is non-deterministic because a
        # set is used internally, so assert membership rather than exact string)
        assert statement.startswith(
            'SELECT ROW_ID, "col1" FROM syn123 WHERE "col1" IN ('
        )
        assert "'A'" in statement
        assert "'B'" in statement

    def test_json_primary_key_raises_value_error(self):
        # GIVEN a primary key column of an unsupported type (JSON)
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.JSON, id="id1"),
            },
        )
        df = pd.DataFrame({"col1": ["{}"]})

        # WHEN/THEN constructing the select statement raises a ValueError
        with pytest.raises(ValueError, match="is not supported for primary_keys"):
            _construct_select_statement_for_upsert(
                entity=entity,
                df=df,
                all_columns_from_df=['"col1"'],
                primary_keys=["col1"],
                wait_for_eventually_consistent_view=False,
            )

    def test_composite_primary_keys_match_exact_tuples_not_cross_product(self):
        # GIVEN a table with two STRING primary keys
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1"),
                "synID": Column(name="synID", column_type=ColumnType.STRING, id="id2"),
            },
        )
        # AND a two-row input whose key values, if filtered independently, would
        # also select the spurious pairs (V1, S2) and (V2, S1)
        df = pd.DataFrame(
            {
                "view": ["V1", "V2"],
                "synID": ["S1", "S2"],
            }
        )

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"view"', '"synID"'],
            primary_keys=["view", "synID"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN the two key columns must be constrained together so the query
        # cannot return spurious combinations. Any correct tuple-based
        # implementation avoids emitting two standalone single-column IN clauses.
        # This is asserted in an implementation-agnostic way: the buggy
        # cross-product form uses both `"view" IN (` and `"synID" IN (`.
        uses_independent_in_clauses = (
            '"view" IN (' in statement and '"synID" IN (' in statement
        )
        assert not uses_independent_in_clauses, (
            "Composite primary keys should be matched as exact tuples, not as "
            f"independent IN clauses. Got: {statement}"
        )

    def test_single_string_primary_key_with_embedded_quote_is_escaped(self):
        # GIVEN a single STRING primary key whose value contains a single quote
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
            },
        )
        df = pd.DataFrame({"col1": ["O'Brien"]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"col1"'],
            primary_keys=["col1"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN the embedded quote is doubled so the literal stays well-formed and
        # cannot break out of the string (no malformed / injectable WHERE clause)
        assert statement == (
            "SELECT ROW_ID, \"col1\" FROM syn123 WHERE \"col1\" IN ('O''Brien')"
        )

    def test_composite_primary_keys_with_embedded_quote_are_escaped(self):
        # GIVEN composite STRING primary keys where a value contains a single quote
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1"),
                "label": Column(name="label", column_type=ColumnType.STRING, id="id2"),
            },
        )
        df = pd.DataFrame({"view": ["V1"], "label": ["O'Brien"]})

        # WHEN I construct the select statement
        statement = _construct_select_statement_for_upsert(
            entity=entity,
            df=df,
            all_columns_from_df=['"view"', '"label"'],
            primary_keys=["view", "label"],
            wait_for_eventually_consistent_view=False,
        )

        # THEN the embedded quote is doubled inside the tuple-matching clause
        assert statement == (
            'SELECT ROW_ID, "view", "label" FROM syn123 WHERE '
            "(\"view\" = 'V1' AND \"label\" = 'O''Brien')"
        )


class TestConstructCompositeKeyConditions:
    """Test suite for _construct_composite_key_conditions, which builds the per-column
    equality conditions for a single composite primary key tuple."""

    class ClassForTest(TableUpsertMixin):
        """A minimal entity exposing only the attributes the helper reads."""

        def __init__(self, id, columns):
            self.id = id
            self.columns = columns

    @pytest.mark.parametrize(
        "columns, primary_keys, row, expected",
        [
            pytest.param(
                {"view": Column(name="view", column_type=ColumnType.STRING, id="id1")},
                ["view"],
                ("V1",),
                ["\"view\" = 'V1'"],
                id="single_string_key",
            ),
            pytest.param(
                {
                    "view": Column(
                        name="view", column_type=ColumnType.STRING, id="id1"
                    ),
                    "synID": Column(
                        name="synID", column_type=ColumnType.STRING, id="id2"
                    ),
                },
                ["view", "synID"],
                ("V1", "S1"),
                ["\"view\" = 'V1'", "\"synID\" = 'S1'"],
                id="two_string_keys_preserve_order",
            ),
            pytest.param(
                {"num": Column(name="num", column_type=ColumnType.INTEGER, id="id1")},
                ["num"],
                (1001,),
                ['"num" = 1001'],
                id="integer_key_is_not_quoted",
            ),
            pytest.param(
                {"flag": Column(name="flag", column_type=ColumnType.BOOLEAN, id="id1")},
                ["flag"],
                (True,),
                ["\"flag\" = 'true'"],
                id="boolean_true_key",
            ),
            pytest.param(
                {"flag": Column(name="flag", column_type=ColumnType.BOOLEAN, id="id1")},
                ["flag"],
                (False,),
                ["\"flag\" = 'false'"],
                id="boolean_false_key",
            ),
            pytest.param(
                {
                    "label": Column(
                        name="label", column_type=ColumnType.STRING, id="id1"
                    )
                },
                ["label"],
                ("O'Brien",),
                ["\"label\" = 'O''Brien'"],
                id="embedded_quote_is_escaped",
            ),
            pytest.param(
                {
                    "view": Column(
                        name="view", column_type=ColumnType.STRING, id="id1"
                    ),
                    "num": Column(name="num", column_type=ColumnType.INTEGER, id="id2"),
                    "flag": Column(
                        name="flag", column_type=ColumnType.BOOLEAN, id="id3"
                    ),
                },
                ["view", "num", "flag"],
                ("V1", 7, True),
                ["\"view\" = 'V1'", '"num" = 7', "\"flag\" = 'true'"],
                id="mixed_column_types",
            ),
        ],
    )
    def test_conditions_construction(self, columns, primary_keys, row, expected):
        # GIVEN an entity and a single primary key tuple
        entity = self.ClassForTest(id="syn123", columns=columns)

        # WHEN I build the per-column conditions
        conditions = _construct_composite_key_conditions(entity, primary_keys, row)

        # THEN each column is matched with an equality condition, one per key, in
        # primary_keys order
        assert conditions == expected

    def test_only_primary_key_columns_are_used(self):
        # GIVEN an entity with more columns than are used as primary keys
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1"),
                "synID": Column(name="synID", column_type=ColumnType.STRING, id="id2"),
                "value": Column(name="value", column_type=ColumnType.INTEGER, id="id3"),
            },
        )

        # WHEN I build conditions for only a subset of columns as the primary key
        conditions = _construct_composite_key_conditions(
            entity, ["view", "synID"], ("V1", "S1")
        )

        # THEN only the primary key columns contribute conditions
        assert conditions == ["\"view\" = 'V1'", "\"synID\" = 'S1'"]


class TestConstructCompositeKeyWhereStatement:
    """Test suite for _construct_composite_key_where_statement, which builds the
    OR-of-ANDs WHERE clause used to match rows on a composite primary key."""

    class ClassForTest(TableUpsertMixin):
        """A minimal entity exposing only the attributes the helper reads."""

        def __init__(self, id, columns):
            self.id = id
            self.columns = columns

    @pytest.mark.parametrize(
        "columns, data, primary_keys, expected",
        [
            pytest.param(
                {
                    "view": Column(
                        name="view", column_type=ColumnType.STRING, id="id1"
                    ),
                    "synID": Column(
                        name="synID", column_type=ColumnType.STRING, id="id2"
                    ),
                },
                {"view": ["V1"], "synID": ["S1"]},
                ["view", "synID"],
                "(\"view\" = 'V1' AND \"synID\" = 'S1')",
                id="single_row_string_keys",
            ),
            pytest.param(
                {
                    "view": Column(
                        name="view", column_type=ColumnType.STRING, id="id1"
                    ),
                    "synID": Column(
                        name="synID", column_type=ColumnType.STRING, id="id2"
                    ),
                },
                {"view": ["V1", "V2"], "synID": ["S1", "S2"]},
                ["view", "synID"],
                (
                    "(\"view\" = 'V1' AND \"synID\" = 'S1') OR "
                    "(\"view\" = 'V2' AND \"synID\" = 'S2')"
                ),
                id="multiple_rows_use_or_of_ands",
            ),
            pytest.param(
                {
                    "view": Column(
                        name="view", column_type=ColumnType.STRING, id="id1"
                    ),
                    "num": Column(name="num", column_type=ColumnType.INTEGER, id="id2"),
                },
                {"view": ["V1"], "num": [1001]},
                ["view", "num"],
                '("view" = \'V1\' AND "num" = 1001)',
                id="integer_key_is_not_quoted",
            ),
            pytest.param(
                {
                    "view": Column(
                        name="view", column_type=ColumnType.STRING, id="id1"
                    ),
                    "label": Column(
                        name="label", column_type=ColumnType.STRING, id="id2"
                    ),
                },
                {"view": ["V1"], "label": ["O'Brien"]},
                ["view", "label"],
                "(\"view\" = 'V1' AND \"label\" = 'O''Brien')",
                id="embedded_quote_is_escaped",
            ),
        ],
    )
    def test_where_statement_construction(self, columns, data, primary_keys, expected):
        # GIVEN an entity and a DataFrame of composite primary key values
        entity = self.ClassForTest(id="syn123", columns=columns)
        df = pd.DataFrame(data)

        # WHEN I construct the composite-key WHERE statement
        where_statement = _construct_composite_key_where_statement(
            entity=entity, df=df, primary_keys=primary_keys
        )

        # THEN the keys are matched together as exact tuples
        assert where_statement == expected

    def test_duplicate_key_tuples_are_deduplicated(self):
        # GIVEN a DataFrame with a repeated (view, synID) tuple
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1"),
                "synID": Column(name="synID", column_type=ColumnType.STRING, id="id2"),
            },
        )
        df = pd.DataFrame({"view": ["V1", "V1", "V2"], "synID": ["S1", "S1", "S2"]})

        # WHEN I construct the composite-key WHERE statement
        where_statement = _construct_composite_key_where_statement(
            entity=entity, df=df, primary_keys=["view", "synID"]
        )

        # THEN the duplicated tuple appears only once, in first-seen order
        assert where_statement == (
            "(\"view\" = 'V1' AND \"synID\" = 'S1') OR "
            "(\"view\" = 'V2' AND \"synID\" = 'S2')"
        )


class TestConstructSingleKeyWhereStatement:
    """Test suite for _construct_single_key_where_statement, which builds the
    IN-clause WHERE statement used to match rows on a single-column primary key."""

    class ClassForTest(TableUpsertMixin):
        """A minimal entity exposing only the attributes the helper reads."""

        def __init__(self, id, columns):
            self.id = id
            self.columns = columns

    @pytest.mark.parametrize(
        "columns, data, primary_key, expected",
        [
            pytest.param(
                {"view": Column(name="view", column_type=ColumnType.STRING, id="id1")},
                {"view": ["V1"]},
                "view",
                "\"view\" IN ('V1')",
                id="single_string_key",
            ),
            pytest.param(
                {"num": Column(name="num", column_type=ColumnType.INTEGER, id="id1")},
                {"num": [1001]},
                "num",
                '"num" IN (1001)',
                id="integer_key_is_not_quoted",
            ),
            pytest.param(
                {
                    "label": Column(
                        name="label", column_type=ColumnType.STRING, id="id1"
                    )
                },
                {"label": ["O'Brien"]},
                "label",
                "\"label\" IN ('O''Brien')",
                id="embedded_quote_is_escaped",
            ),
        ],
    )
    def test_where_statement_construction(self, columns, data, primary_key, expected):
        # GIVEN an entity and a DataFrame of single primary key values
        entity = self.ClassForTest(id="syn123", columns=columns)
        df = pd.DataFrame(data)

        # WHEN I construct the single-key WHERE statement
        where_statement = _construct_single_key_where_statement(
            entity=entity, df=df, primary_key=primary_key
        )

        # THEN the values are matched with an IN clause
        assert where_statement == expected

    def test_duplicate_values_are_deduplicated(self):
        # GIVEN a DataFrame with a repeated primary key value
        entity = self.ClassForTest(
            id="syn123",
            columns={
                "view": Column(name="view", column_type=ColumnType.STRING, id="id1")
            },
        )
        df = pd.DataFrame({"view": ["V1", "V1", "V2"]})

        # WHEN I construct the single-key WHERE statement
        where_statement = _construct_single_key_where_statement(
            entity=entity, df=df, primary_key="view"
        )

        # THEN the duplicated value appears only once
        assert where_statement.count("'V1'") == 1
        assert where_statement.count("'V2'") == 1


class TestValidatePrimaryKeys:
    """Test suite for _validate_primary_keys, which rejects upserts whose primary
    key columns are missing from the data or contain null values."""

    def test_empty_primary_keys_raise(self):
        # GIVEN a DataFrame and an empty list of primary keys
        df = pd.DataFrame({"view": ["V1", "V2"], "value": [1, 2]})

        # WHEN I validate the primary keys THEN a ValueError is raised
        with pytest.raises(
            ValueError, match="At least one primary key column must be provided"
        ):
            _validate_primary_keys(df, [])

    @pytest.mark.parametrize(
        "primary_keys, offending",
        [
            pytest.param([1], "1", id="single_int"),
            pytest.param(["view", 2], "2", id="mixed_string_and_int"),
            pytest.param([None], "None", id="none"),
            pytest.param([("view",)], "('view',)", id="tuple"),
        ],
    )
    def test_non_string_primary_keys_raise(self, primary_keys, offending):
        # GIVEN a DataFrame and a primary key that is not a string
        df = pd.DataFrame({"view": ["V1", "V2"], "value": [1, 2]})

        # WHEN I validate the primary keys THEN a ValueError naming the offending
        # value is raised
        with pytest.raises(ValueError, match="must be strings") as exc:
            _validate_primary_keys(df, primary_keys)
        assert offending in str(exc.value)

    def test_no_null_primary_keys_passes(self):
        # GIVEN a DataFrame whose primary key columns have no null values
        df = pd.DataFrame(
            {"view": ["V1", "V2"], "synID": ["S1", "S2"], "value": [1, 2]}
        )

        # WHEN I validate the primary keys THEN no error is raised
        _validate_primary_keys(df, ["view", "synID"])

    def test_null_in_a_non_primary_key_column_is_allowed(self):
        # GIVEN a DataFrame with a null value only in a non-primary-key column
        df = pd.DataFrame(
            {"view": ["V1", "V2"], "synID": ["S1", "S2"], "value": [1, None]}
        )

        # WHEN I validate the primary keys THEN no error is raised
        _validate_primary_keys(df, ["view", "synID"])

    def test_empty_dataframe_passes(self):
        # GIVEN a DataFrame with primary key columns but no rows
        df = pd.DataFrame({"view": [], "synID": []})

        # WHEN I validate the primary keys THEN no error is raised (there are no
        # null values because there are no values at all)
        _validate_primary_keys(df, ["view", "synID"])

    @pytest.mark.parametrize(
        "data, primary_keys, expected_columns",
        [
            pytest.param(
                {"view": ["V1", "V2"], "value": [1, 2]},
                ["view", "synID"],
                ["synID"],
                id="one_of_two_keys_missing",
            ),
            pytest.param(
                {"value": [1, 2]},
                ["view", "synID"],
                ["view", "synID"],
                id="all_keys_missing",
            ),
        ],
    )
    def test_missing_primary_key_columns_raise(
        self, data, primary_keys, expected_columns
    ):
        # GIVEN a DataFrame that is missing one or more primary key columns
        df = pd.DataFrame(data)

        # WHEN I validate the primary keys THEN a ValueError naming the missing
        # column(s) is raised
        with pytest.raises(ValueError, match="are missing") as exc:
            _validate_primary_keys(df, primary_keys)
        for column in expected_columns:
            assert column in str(exc.value)

    def test_missing_column_is_reported_before_null_column(self):
        # GIVEN a DataFrame missing one primary key and with a null in another
        df = pd.DataFrame({"view": ["V1", None], "value": [1, 2]})

        # WHEN I validate primary keys where one is missing and one has a null
        # THEN the missing-column error is raised (presence is checked first, so
        # the null check never dereferences the absent column)
        with pytest.raises(ValueError, match="are missing") as exc:
            _validate_primary_keys(df, ["view", "synID"])
        assert "synID" in str(exc.value)

    @pytest.mark.parametrize(
        "null_value",
        [
            pytest.param(None, id="none"),
            pytest.param(np.nan, id="numpy_nan"),
            pytest.param(pd.NA, id="pandas_na"),
        ],
    )
    def test_all_null_representations_are_detected(self, null_value):
        # GIVEN a primary key column whose null is expressed as None, np.nan, or pd.NA
        df = pd.DataFrame({"view": ["V1", null_value], "value": [1, 2]})

        # WHEN I validate the primary keys THEN each null representation is detected
        with pytest.raises(ValueError, match="must not contain null values"):
            _validate_primary_keys(df, ["view"])

    def test_null_in_numeric_primary_key_is_detected(self):
        # GIVEN a numeric primary key column that contains a null value
        df = pd.DataFrame(
            {"num": pd.array([1, None, 3], dtype="Int64"), "value": [1, 2, 3]}
        )

        # WHEN I validate the primary keys THEN the null is detected
        with pytest.raises(ValueError, match="must not contain null values") as exc:
            _validate_primary_keys(df, ["num"])
        assert "num" in str(exc.value)

    @pytest.mark.parametrize(
        "data, primary_keys, expected_columns",
        [
            pytest.param(
                {"view": ["V1", None], "value": [1, 2]},
                ["view"],
                ["view"],
                id="single_primary_key_with_null",
            ),
            pytest.param(
                {"view": ["V1", "V2"], "synID": ["S1", None], "value": [1, 2]},
                ["view", "synID"],
                ["synID"],
                id="composite_key_with_partial_null",
            ),
            pytest.param(
                {"view": [None, None], "synID": [None, None], "value": [1, 2]},
                ["view", "synID"],
                ["view", "synID"],
                id="composite_key_all_null",
            ),
        ],
    )
    def test_null_primary_keys_raise(self, data, primary_keys, expected_columns):
        # GIVEN a DataFrame whose primary key column(s) contain null values
        df = pd.DataFrame(data)

        # WHEN I validate the primary keys THEN a ValueError naming the offending
        # column(s) is raised
        with pytest.raises(ValueError, match="must not contain null values") as exc:
            _validate_primary_keys(df, primary_keys)
        # AND only the offending columns are reported
        message = str(exc.value)
        for column in expected_columns:
            assert column in message
        non_offending = {"view", "synID"} - set(expected_columns)
        for column in non_offending:
            assert f"'{column}'" not in message


class TestLogUpsertSummary:
    """Test suite for the _log_upsert_summary function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class ClassForTest:
        id: Optional[str] = "syn123"
        name: Optional[str] = "test_table"

    @staticmethod
    def _table_transaction(rows_changed: int) -> TableUpdateTransaction:
        """A transaction against the rows of a table that changed the given row count."""
        return TableUpdateTransaction(
            entity_id="syn123",
            results=[
                RowReferenceSetResults(
                    row_reference_set=RowReferenceSet(
                        rows=[
                            RowReference(row_id=index, version_number=1)
                            for index in range(rows_changed)
                        ]
                    )
                )
            ],
        )

    @staticmethod
    def _view_transaction(
        update_results: List[EntityUpdateResult],
    ) -> TableUpdateTransaction:
        """A transaction against the entities that back a view."""
        return TableUpdateTransaction(
            entity_id="syn123",
            results=[EntityUpdateResults(update_results=update_results)],
        )

    def test_no_results_reports_the_client_side_count(self):
        # GIVEN no results, as is the case for a dry run
        test_instance = self.ClassForTest()
        with (
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
        ):
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[],
                total_row_count_to_update=5,
                row_count_to_insert=2,
                client=self.syn,
            )

            # THEN the count this client sent for update is reported
            mock_logger_info.assert_called_once_with(
                "[syn123:test_table]: Found 5 rows to update and 2 rows to insert"
            )
            # AND no gap is reported, because Synapse confirmed nothing
            mock_logger_debug.assert_not_called()

    def test_results_report_the_count_synapse_confirmed(self):
        # GIVEN results that confirm every row this client sent
        test_instance = self.ClassForTest()
        with (
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
        ):
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[
                    self._table_transaction(2),
                    self._table_transaction(1),
                ],
                total_row_count_to_update=3,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN the counts from every result are added together
            mock_logger_info.assert_called_once_with(
                "[syn123:test_table]: Found 3 rows to update and 0 rows to insert"
            )
            # AND no gap is reported
            mock_logger_debug.assert_not_called()

    @pytest.mark.parametrize(
        "results,expected_count",
        [
            # A transaction that has not been sent carries no count.
            ([TableUpdateTransaction(entity_id="syn123", results=None)], 0),
            # A schema change reports no row count.
            (
                [
                    TableUpdateTransaction(
                        entity_id="syn123",
                        results=[TableSchemaChangeResponse(schema=[])],
                    )
                ],
                0,
            ),
        ],
        ids=["unsent_transaction", "schema_change_only"],
    )
    def test_results_without_a_row_count_contribute_nothing(
        self, results: List[TableUpdateTransaction], expected_count: int
    ):
        # GIVEN results that carry no row count
        test_instance = self.ClassForTest()
        with patch.object(self.syn.logger, "info") as mock_logger_info:
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=results,
                total_row_count_to_update=4,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN those results are left out of the confirmed count
            mock_logger_info.assert_called_once_with(
                f"[syn123:test_table]: Found {expected_count} rows to update"
                " and 0 rows to insert"
            )

    @pytest.mark.parametrize(
        "failed_update,expected_detail",
        [
            (
                EntityUpdateResult(
                    entity_id="syn456",
                    failure_code=EntityUpdateFailureCode.UNAUTHORIZED,
                ),
                "syn456 (UNAUTHORIZED)",
            ),
            (
                EntityUpdateResult(
                    entity_id="syn456",
                    failure_code=EntityUpdateFailureCode.ILLEGAL_ARGUMENT,
                    failure_message="bad value",
                ),
                "syn456 (ILLEGAL_ARGUMENT: bad value)",
            ),
            # Synapse reported a message without a code.
            (
                EntityUpdateResult(entity_id="syn456", failure_message="bad value"),
                "syn456 (UNKNOWN: bad value)",
            ),
            # Synapse reported a failure without naming the entity.
            (
                EntityUpdateResult(
                    failure_code=EntityUpdateFailureCode.NOT_FOUND,
                ),
                "unknown row (NOT_FOUND)",
            ),
        ],
        ids=["code_only", "code_and_message", "message_only", "no_entity_id"],
    )
    def test_a_failed_row_update_is_described(
        self, failed_update: EntityUpdateResult, expected_detail: str
    ):
        # GIVEN a view result that holds one failed update
        test_instance = self.ClassForTest()
        with patch.object(self.syn.logger, "info") as mock_logger_info:
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[self._view_transaction([failed_update])],
                total_row_count_to_update=1,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN the failure is described with the reason Synapse gave
            mock_logger_info.assert_called_once_with(
                "[syn123:test_table]: Found 0 rows to update and 0 rows to insert."
                f" 1 rows could not be updated: {expected_detail}"
            )

    def test_failed_row_updates_from_every_result_are_reported(self):
        # GIVEN two results that each hold a failed update alongside a successful one
        test_instance = self.ClassForTest()
        with (
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
        ):
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[
                    self._view_transaction(
                        [
                            EntityUpdateResult(entity_id="syn1"),
                            EntityUpdateResult(
                                entity_id="syn2",
                                failure_code=EntityUpdateFailureCode.NOT_FOUND,
                            ),
                        ]
                    ),
                    self._view_transaction(
                        [
                            EntityUpdateResult(
                                entity_id="syn3",
                                failure_code=EntityUpdateFailureCode.CONCURRENT_UPDATE,
                            ),
                        ]
                    ),
                ],
                total_row_count_to_update=3,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN only the successful update is counted, and both failures are listed
            mock_logger_info.assert_called_once_with(
                "[syn123:test_table]: Found 1 rows to update and 0 rows to insert."
                " 2 rows could not be updated: syn2 (NOT_FOUND);"
                " syn3 (CONCURRENT_UPDATE)"
            )
            # AND the gap is not reported as an accounting gap, because the failures
            # already explain it
            mock_logger_debug.assert_not_called()

    def test_a_gap_without_a_reported_failure_is_logged_as_an_accounting_gap(self):
        # GIVEN a result that confirms fewer rows than this client sent, with no failure
        test_instance = self.ClassForTest()
        with (
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
        ):
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[self._table_transaction(1)],
                total_row_count_to_update=3,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN the confirmed count is reported
            mock_logger_info.assert_called_once_with(
                "[syn123:test_table]: Found 1 rows to update and 0 rows to insert"
            )
            # AND the gap is called out as a gap in this client, not a failed update
            mock_logger_debug.assert_called_once()
            debug_message = mock_logger_debug.call_args.args[0]
            assert "Synapse confirmed 1 of the 3 rows sent for update" in debug_message
            assert "not a failed update" in debug_message

    def test_a_success_with_no_entity_id_is_not_reported_as_a_gap(self):
        # GIVEN a view result where every row applied, but one reported no entity ID
        test_instance = self.ClassForTest()
        with (
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
        ):
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[
                    self._view_transaction(
                        [
                            EntityUpdateResult(entity_id="syn1"),
                            EntityUpdateResult(entity_id=None),
                        ]
                    )
                ],
                total_row_count_to_update=2,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN both rows are counted as updated
            mock_logger_info.assert_called_once_with(
                "[syn123:test_table]: Found 2 rows to update and 0 rows to insert"
            )
            # AND no accounting gap is reported, because nothing was lost
            mock_logger_debug.assert_not_called()

    def test_more_rows_confirmed_than_sent_is_not_a_gap(self):
        # GIVEN a result that confirms at least as many rows as this client sent
        test_instance = self.ClassForTest()
        with patch.object(self.syn.logger, "debug") as mock_logger_debug:
            # WHEN I log the summary
            _log_upsert_summary(
                entity=test_instance,
                row_update_results=[self._table_transaction(4)],
                total_row_count_to_update=3,
                row_count_to_insert=0,
                client=self.syn,
            )

            # THEN no gap is reported
            mock_logger_debug.assert_not_called()


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
                date_columns=None,
                date_format=None,
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
                synapse_client=self.syn,
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
                synapse_client=self.syn,
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
                synapse_client=self.syn,
            )
            # AND mock_rowset_to_pandas_df should be called with correct args
            mock_rowset_to_pandas_df.assert_called_once_with(
                query_result_bundle=mock_query_result_bundle,
                synapse_client=self.syn,
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
                synapse_client=self.syn,
            )

            mock_rowset_to_pandas_df.assert_called_once_with(
                query_result_bundle=mock_query_result_bundle,
                synapse_client=self.syn,
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
                # Special characters must be escaped due to use with regex in test
                "Rows with the following ROW_ID and ROW_VERSION pairs were not found in table syn123: \\(C, 2\\), \\(D, 2\\).",
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
                query=sample_query, synapse_client=mock_synapse
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
                synapse_client=mock_synapse,
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
                synapse_client=mock_synapse_client,
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
                synapse_client=mock_synapse_client,
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
                synapse_client=self.syn,
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

    def test_csv_to_pandas_df_with_date_columns_and_no_rows(self):
        # GIVEN a CSV with a date column but no data rows — e.g. an empty query
        # result for a table with a DATE column
        csv_file = BytesIO(b"id,name,created_date\n")

        # WHEN converting the CSV with date_columns specified
        df = csv_to_pandas_df(filepath=csv_file, date_columns=["created_date"])

        # THEN the date column is still datetime64, not left as the
        # intermediate float64 used to parse epoch milliseconds — otherwise
        # a `.dt` accessor on the result would raise an AttributeError
        assert df.empty
        assert str(df["created_date"].dtype) == "datetime64[ns, UTC]"

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
        ).convert_dtypes()  # resolve datatype issue such as StringDtype vs object
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
        ).convert_dtypes()  # resolve datatype issue such as StringDtype vs object
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
        ).convert_dtypes()  # resolve datatype issue such as StringDtype vs object
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
                    42,
                    None,
                    99,
                ],  # Integers are converted to floats due to the presence of NaN values, but convert_dtypes converts them to int via convert_dtypes
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
        ).convert_dtypes()  # resolve datatype issue such as StringDtype vs object
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
                    42,
                    None,
                    99,
                ],  # Integers are converted to floats due to the presence of NaN values, but convert_dtypes converts them to int via convert_dtypes
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
        ).convert_dtypes()  # resolve datatype issue such as StringDtype vs object
        # THEN assert the dataframe is equal to the expected dataframe
        pd.testing.assert_frame_equal(df, expected_df)

    @pytest.mark.parametrize(
        "list_column_types",
        [
            {"empty_list": "INTEGER_LIST"},
            {"empty_list": "BOOLEAN_LIST"},
            {"empty_list": "STRING_LIST"},
            {"empty_list": "USERID_LIST"},
            {"empty_list": "ENTITYID_LIST"},
            None,
        ],
        ids=[
            "INTEGER_LIST",
            "BOOLEAN_LIST",
            "STRING_LIST",
            "USERID_LIST",
            "ENTITYID_LIST",
            "no_types",
        ],
    )
    def test_csv_to_pandas_df_all_na_list_column(self, list_column_types):
        """Reproducer for the bug where querying a table with a list column whose
        values are all NA in the result set raised
        TypeError: Invalid value '[]' for dtype 'Int64'.

        pandas' read_csv().convert_dtypes() infers an all-empty column as the
        nullable Int64 dtype; the previous fillna({col: '[]'}) implementation
        could not store a string into that column."""
        # GIVEN a CSV where every row has an empty value for the list column
        csv_content = "name,empty_list\n" "Alice,\n" "Bob,\n" "Charlie,"
        csv_file = BytesIO(csv_content.encode("utf-8"))

        # WHEN csv_to_pandas_df is called for that list column
        df = csv_to_pandas_df(
            filepath=csv_file,
            list_columns=["empty_list"],
            list_column_types=list_column_types,
        )

        # THEN the all-NA column should become a column of empty lists, and the
        # other columns should still parse normally
        assert list(df["name"]) == ["Alice", "Bob", "Charlie"]
        assert list(df["empty_list"]) == [[], [], []]

    def test_csv_to_pandas_df_mixed_all_na_and_populated_list_columns(self):
        """When two list columns are present and only one is all-NA, the
        populated one must still parse correctly."""
        # GIVEN a CSV with one populated list column and one all-NA list column
        csv_content = (
            "name,populated_list,empty_list\n"
            'Alice,"[1, 2, 3]",\n'
            'Bob,"[4, 5]",\n'
            'Charlie,"[6]",'
        )
        csv_file = BytesIO(csv_content.encode("utf-8"))

        # WHEN csv_to_pandas_df is called
        df = csv_to_pandas_df(
            filepath=csv_file,
            list_columns=["populated_list", "empty_list"],
            list_column_types={
                "populated_list": "INTEGER_LIST",
                "empty_list": "INTEGER_LIST",
            },
        )

        # THEN both columns should have the correct contents
        assert list(df["populated_list"]) == [[1, 2, 3], [4, 5], [6]]
        assert list(df["empty_list"]) == [[], [], []]


class TestConvertDtypesToJsonSerializable:
    """Tests for convert_dtypes_to_json_serializable function"""

    def test_int_and_float_columns_converted_to_object(self):
        """Test that int64 and float64 columns are always cast to object dtype,
        even when no NA is present (values are preserved)."""
        df = pd.DataFrame({"int_col": [1, 2, 3, 4], "float_col": [1.1, 2.2, 3.3, 4.4]})
        assert df["int_col"].dtype == "int64"
        assert df["float_col"].dtype == "float64"

        result = convert_dtypes_to_json_serializable(df)
        assert is_object_dtype(result.int_col)
        assert is_object_dtype(result.float_col)
        assert list(result["int_col"]) == [1, 2, 3, 4]
        assert list(result["float_col"]) == [1.1, 2.2, 3.3, 4.4]

    def test_convert_na_and_columns_to_object(self):
        """Test that pd.NA values are converted to None for int64 and float64 columns by _serialize_json_value"""
        df = pd.DataFrame(
            {
                "int_col": pd.array([1, 2, pd.NA, 4], dtype="Int64"),
                "float_col": pd.array([1.1, 2.2, pd.NA, 4.4], dtype="Float64"),
            }
        )
        result = convert_dtypes_to_json_serializable(df)
        assert is_object_dtype(result.int_col)
        assert is_object_dtype(result.float_col)
        assert list(result["int_col"]) == [1, 2, None, 4]
        assert list(result["float_col"]) == [1.1, 2.2, None, 4.4]

    def test_row_columns_remain_int(self):
        """Test that ROW_ID, ROW_VERSION, and ROW_ID.1 columns remain as int while other columns become object"""
        # GIVEN a dataframe with special columns (ROW_ID, ROW_VERSION, ROW_ID.1) and a regular column
        df = pd.DataFrame(
            {
                "ROW_ID": pd.array([1, 2, 3, 4], dtype="Int64"),
                "ROW_VERSION": pd.array([5, 6, 7, 8], dtype="Int64"),
                "ROW_ID.1": pd.array([9, 10, 11, 12], dtype="Int64"),
                "other_col": [10, 20, 30, 40],  # Use regular list without pd.NA
            }
        )

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN all special columns should remain as int while other_col should become object
        assert is_integer_dtype(result.ROW_ID), "ROW_ID should remain integer dtype"
        assert is_integer_dtype(
            result.ROW_VERSION
        ), "ROW_VERSION should remain int64 dtype"
        assert is_integer_dtype(
            result["ROW_ID.1"]
        ), "ROW_ID.1 should remain int64 dtype"
        assert is_object_dtype(result.other_col), "other_col should become object dtype"

    def test_ellipsis_handling_in_list(self):
        """Test that Ellipsis (...) objects in lists are converted to '...' strings"""
        # GIVEN a dataframe with Ellipsis in a list
        df = pd.DataFrame({"list_with_ellipsis": [[1, 2, ...], [4, ..., 6]]})

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN Ellipsis should be converted to "..." in JSON string
        assert result["list_with_ellipsis"].iloc[0] == [1, 2, "..."]
        assert result["list_with_ellipsis"].iloc[1] == [4, "...", 6]
        assert is_object_dtype(result.list_with_ellipsis)

    def test_ellipsis_handling_in_dict(self):
        """Test that Ellipsis (...) objects in dicts are converted to '...' strings"""
        # GIVEN a dataframe with Ellipsis in a dict
        df = pd.DataFrame(
            {
                "dict_with_ellipsis": [
                    {"id": 1, "data": ...},
                    {"id": 2, "items": [1, ...]},
                ]
            }
        )

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN Ellipsis should be converted to "..." in JSON string
        assert result.dict_with_ellipsis.iloc[0] == {"id": 1, "data": "..."}
        assert result.dict_with_ellipsis.iloc[1] == {"id": 2, "items": [1, "..."]}
        assert is_object_dtype(result.dict_with_ellipsis)

    def test_standalone_ellipsis(self):
        """Test that standalone Ellipsis objects are converted to '...' strings"""
        # GIVEN a dataframe with standalone Ellipsis
        df = pd.DataFrame({"ellipsis_col": [1, ..., 3]})

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN Ellipsis should be converted to "..."
        assert result["ellipsis_col"].iloc[0] == 1
        assert result["ellipsis_col"].iloc[1] == "..."
        assert result["ellipsis_col"].iloc[2] == 3

    def test_none_in_list_column_remains_none(self):
        """Test that pd.NA values in a column of lists are normalized to None
        (not to an empty list)."""
        # GIVEN a dataframe with None in list column
        df = pd.DataFrame({"list_col": [[1, 2, 3], pd.NA, [7, 8, 9]]})

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN the pd.NA entry should become None (and surrounding lists preserved)
        assert result["list_col"].iloc[0] == [1, 2, 3]
        assert result["list_col"].iloc[1] is None
        assert result["list_col"].iloc[2] == [7, 8, 9]

    def test_dict_with_quotes_in_values(self):
        """Test that dicts with quotes in string values are properly handled"""
        # GIVEN a dataframe with dict containing quotes
        df = pd.DataFrame(
            {
                "dict_col": [
                    {"description": 'Text with "quotes" here'},
                    {"description": 'Another "quoted" text'},
                ]
            }
        )

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN the JSON string should be properly formatted
        assert result["dict_col"].iloc[0] == {"description": 'Text with "quotes" here'}
        assert result["dict_col"].iloc[1] == {"description": 'Another "quoted" text'}
        assert is_object_dtype(result.dict_col)

    def test_empty_dataframe(self):
        """Test that empty dataframe is handled correctly"""
        # GIVEN an empty dataframe
        df = pd.DataFrame()

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN it should return an empty dataframe
        assert len(result) == 0
        assert len(result.columns) == 0

    def test_mixed_column_types_no_conversion_needed(self):
        """Test that multiple column types without NA are handled correctly
        together: values are preserved, ROW_* stays int, other columns become
        object dtype."""
        # GIVEN a dataframe with mixed column types
        df = pd.DataFrame(
            {
                "ROW_ID": pd.array([1, 2, 3], dtype="Int64"),
                "ROW_VERSION": pd.array([1, 1, 1], dtype="Int64"),
                "int_col": [10, 20, 30],  # Use regular list without pd.NA
                "float_col": [1.1, 2.2, 3.3],
                "string_col": ["a", "b", "c"],
                "list_col": [[1, 2], [3, 4], None],
                "dict_col": [{"id": 1}, {"id": 2}, {"id": 3}],
                "bool_col": [True, False, True],
            }
        )
        # Snapshot before the function mutates df in place
        original = df.copy(deep=True)

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN values are preserved against the pre-call snapshot, ROW_* stay
        # int, and other columns are object dtype
        assert is_integer_dtype(result.ROW_ID)
        assert is_integer_dtype(result.ROW_VERSION)
        assert is_object_dtype(result.int_col)
        assert is_object_dtype(result.float_col)
        assert is_object_dtype(result.string_col)
        assert is_object_dtype(result.list_col)
        assert is_object_dtype(result.dict_col)
        assert is_object_dtype(result.bool_col)

        for col in original.columns:
            assert list(result[col]) == list(original[col])

    def test_nested_dict_with_ellipsis(self):
        """Test that nested dicts with Ellipsis are properly handled"""
        # GIVEN a dataframe with nested dict containing Ellipsis
        df = pd.DataFrame(
            {
                "nested_dict": [
                    {"outer": {"inner": ...}},
                    {"data": {"list": [1, 2, ...]}},
                ]
            }
        )

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN Ellipsis should be converted in nested structures
        assert result["nested_dict"].iloc[0] == {"outer": {"inner": "..."}}
        assert result["nested_dict"].iloc[1] == {"data": {"list": [1, 2, "..."]}}

    def test_nullable_int64_with_pd_na(self):
        """Test that Int64 columns with pd.NA get pd.NA converted to None by _serialize_json_value"""
        # GIVEN a dataframe with nullable Int64 column containing pd.NA
        df = pd.DataFrame(
            {"nullable_int_col": pd.array([1, 2, pd.NA, 4, pd.NA], dtype="Int64")}
        )

        # WHEN convert_dtypes_to_json_serializable is called
        result = convert_dtypes_to_json_serializable(df)

        # THEN the column should be object type and pd.NA should be converted to None
        assert is_object_dtype(result.nullable_int_col)
        expected_result = pd.DataFrame(
            {"nullable_int_col": [1, 2, None, 4, None]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        assert is_object_dtype(result.nullable_int_col)


def _row_reference_set_results(row_count: int) -> Dict[str, Any]:
    """A RowReferenceSetResults response, as Synapse returns it for the update half of
    a table upsert. Modeled on the response recorded from production for SYNPY-1912."""
    return {
        "concreteType": ROW_REFERENCE_SET_RESULTS,
        "rowReferenceSet": {
            "tableId": "syn76890550",
            "etag": "5aac0c05-c0dc-4119-b284-4c394a6044aa",
            "rows": [
                {"rowId": row_id, "versionNumber": 2}
                for row_id in range(1, row_count + 1)
            ],
        },
    }


def _entity_update_results(update_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """An EntityUpdateResults response, as Synapse returns it for a change applied to
    the entities that back a view."""
    return {
        "concreteType": ENTITY_UPDATE_RESULTS,
        "updateResults": update_results,
    }


class TestTableUpdateResponseFromDict:
    """Test suite for the table_update_response_from_dict dispatch function."""

    @pytest.mark.parametrize(
        "concrete_type,expected_class",
        [
            (ENTITY_UPDATE_RESULTS, EntityUpdateResults),
            (ROW_REFERENCE_SET_RESULTS, RowReferenceSetResults),
            (UPLOAD_TO_TABLE_RESULT, UploadToTableResult),
            (TABLE_SCHEMA_CHANGE_RESPONSE, TableSchemaChangeResponse),
            (TABLE_SEARCH_CHANGE_RESPONSE, TableSearchChangeResponse),
        ],
        ids=[
            "entity_update_results",
            "row_reference_set_results",
            "upload_to_table_result",
            "table_schema_change_response",
            "table_search_change_response",
        ],
    )
    def test_dispatch_on_known_concrete_type(self, concrete_type, expected_class):
        """Each concrete type that Synapse reports maps to the class that models it."""
        # GIVEN a response that reports a concrete type we model
        data = {"concreteType": concrete_type}

        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN it is the matching subclass and the reported type is kept
        assert isinstance(response, expected_class)
        assert response.concrete_type == concrete_type

    @pytest.mark.parametrize(
        "data",
        [
            {"updateResults": []},
            {"rowReferenceSet": {}},
            {"rowsProcessed": 0},
            {"schema": []},
            {"searchEnabled": True},
        ],
        ids=[
            "update_results_key",
            "row_reference_set_key",
            "rows_processed_key",
            "schema_key",
            "search_enabled_key",
        ],
    )
    def test_a_response_with_no_concrete_type_is_unknown(self, data):
        """A response is identified only by the concrete type Synapse reports. The keys
        it carries are not used to guess a type, so a response with no concrete type is
        held as-is rather than reported as the type it resembles."""
        # GIVEN a response that reports no concrete type
        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN it is unknown, the raw response is kept, and no row count is claimed
        assert isinstance(response, UnknownTableUpdateResponse)
        assert response.concrete_type is None
        assert response.data == data
        assert response.rows_changed is None

    def test_unrecognized_concrete_type_is_unknown_and_keeps_the_reported_type(self):
        """A concrete type added to Synapse after this release does not raise, and the
        type Synapse reported is preserved so that the response can be identified from
        the raw data."""
        # GIVEN a response with an unmodelled concrete type
        data = {
            "concreteType": "org.sagebionetworks.repo.model.table.FutureResponse",
            "rowReferenceSet": {
                "tableId": "syn123",
                "rows": [{"rowId": 1, "versionNumber": 2}],
            },
        }

        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN the raw response is held as-is and the reported type is kept
        assert isinstance(response, UnknownTableUpdateResponse)
        assert response.data == data
        assert (
            response.concrete_type
            == "org.sagebionetworks.repo.model.table.FutureResponse"
        )
        assert response.rows_changed is None

    def test_unidentifiable_response_is_unknown_and_keeps_the_raw_data(self):
        """A response type added to Synapse after this release neither raises nor is
        miscounted."""
        # GIVEN a response that can be identified neither by type nor by key
        data = {
            "concreteType": "org.sagebionetworks.repo.model.table.NewResponse",
            "somethingNew": 5,
        }

        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN the raw response is held as-is and it reports no row count
        assert isinstance(response, UnknownTableUpdateResponse)
        assert response.data == data
        assert (
            response.concrete_type == "org.sagebionetworks.repo.model.table.NewResponse"
        )
        assert response.rows_changed is None

    def test_empty_response_does_not_raise(self):
        """An empty response is unknown rather than an error."""
        # GIVEN an empty response
        # WHEN converting it
        response = table_update_response_from_dict({})

        # THEN it is unknown, with no concrete type and no row count
        assert isinstance(response, UnknownTableUpdateResponse)
        assert response.concrete_type is None
        assert response.data == {}
        assert response.rows_changed is None

    def test_abstract_base_class_cannot_be_instantiated(self):
        """TableUpdateResponse only exists to be subclassed."""
        # GIVEN the abstract base class
        # WHEN instantiating it THEN it raises
        with pytest.raises(TypeError):
            TableUpdateResponse()


class TestTableUpdateResponseRowsChanged:
    """Test suite for the rows_changed property of each TableUpdateResponse subclass.

    rows_changed is the source of the row count that upsert_rows reports, so the
    difference between a confirmed count of 0 and an absent count of None matters.
    """

    @pytest.mark.parametrize(
        "data,expected_rows_changed",
        [
            # The update half of a table upsert.
            (_row_reference_set_results(5), 5),
            (
                {
                    "concreteType": ROW_REFERENCE_SET_RESULTS,
                    "rowReferenceSet": {"tableId": "syn123", "rows": []},
                },
                0,
            ),
            ({"concreteType": ROW_REFERENCE_SET_RESULTS, "rowReferenceSet": {}}, 0),
            ({"concreteType": ROW_REFERENCE_SET_RESULTS}, 0),
            # The insert half of a table upsert.
            ({"concreteType": UPLOAD_TO_TABLE_RESULT, "rowsProcessed": 2}, 2),
            ({"concreteType": UPLOAD_TO_TABLE_RESULT, "rowsProcessed": 0}, 0),
            ({"concreteType": UPLOAD_TO_TABLE_RESULT}, None),
            # A change applied to the entities that back a view.
            (
                _entity_update_results(
                    [
                        {"entityId": "syn1"},
                        {"entityId": "syn2", "failureCode": "NOT_FOUND"},
                        {
                            "entityId": "syn3",
                            "failureCode": "ILLEGAL_ARGUMENT",
                            "failureMessage": "bad value",
                        },
                    ]
                ),
                1,
            ),
            (_entity_update_results([]), 0),
            ({"concreteType": ENTITY_UPDATE_RESULTS}, 0),
            # Changes that apply no rows.
            (
                {
                    "concreteType": TABLE_SCHEMA_CHANGE_RESPONSE,
                    "schema": [{"name": "col1", "columnType": "STRING"}],
                },
                None,
            ),
            (
                {"concreteType": TABLE_SEARCH_CHANGE_RESPONSE, "searchEnabled": True},
                None,
            ),
        ],
        ids=[
            "row_reference_set_five_rows",
            "row_reference_set_no_rows_is_zero",
            "row_reference_set_empty_is_zero",
            "row_reference_set_absent_is_zero",
            "upload_to_table_two_rows",
            "upload_to_table_zero_rows_is_zero",
            "upload_to_table_absent_count_is_none",
            "entity_update_one_success_two_failures",
            "entity_update_empty_is_zero",
            "entity_update_absent_is_zero",
            "schema_change_is_none",
            "search_change_is_none",
        ],
    )
    def test_rows_changed(self, data, expected_rows_changed):
        """Only a response that reports a row count contributes one."""
        # GIVEN a response from Synapse
        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN rows_changed reports the confirmed count, and None when the response
        # carries no count at all
        assert response.rows_changed == expected_rows_changed

    def test_row_reference_set_results_fields(self):
        """The full RowReferenceSetResults response is modeled, not just its count."""
        # GIVEN the response recorded from production for the update half of an upsert
        # WHEN converting it
        response = table_update_response_from_dict(_row_reference_set_results(5))

        # THEN the row references and the table etag are available
        assert response.row_reference_set.table_id == "syn76890550"
        assert response.row_reference_set.etag == "5aac0c05-c0dc-4119-b284-4c394a6044aa"
        assert response.row_reference_set.rows == [
            RowReference(row_id=row_id, version_number=2) for row_id in range(1, 6)
        ]

    def test_row_reference_set_parses_headers(self):
        """The optional headers of a RowReferenceSet are modeled as SelectColumns."""
        # GIVEN a row reference set that carries headers
        data = {
            "tableId": "syn123",
            "headers": [{"name": "col1", "columnType": "STRING", "id": "1"}],
            "rows": [{"rowId": 1, "versionNumber": 2}],
        }

        # WHEN converting it
        row_reference_set = RowReferenceSet.fill_from_dict(data)

        # THEN the headers are SelectColumn instances
        assert row_reference_set.headers == [
            SelectColumn(name="col1", column_type=ColumnType.STRING, id="1")
        ]

    def test_upload_to_table_result_keeps_the_etag(self):
        """The etag of the version applied to the table is retained."""
        # GIVEN an upload result
        data = {
            "concreteType": UPLOAD_TO_TABLE_RESULT,
            "rowsProcessed": 2,
            "etag": "new-etag",
        }

        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN the etag is available
        assert response.etag == "new-etag"

    def test_schema_change_response_parses_columns(self):
        """The resulting schema is modeled as Column instances."""
        # GIVEN a schema change response
        data = {
            "concreteType": TABLE_SCHEMA_CHANGE_RESPONSE,
            "schema": [
                {"name": "col1", "columnType": "STRING", "id": "1"},
                {"name": "col2", "columnType": "INTEGER", "id": "2"},
            ],
        }

        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN each column of the resulting schema is available
        assert [column.name for column in response.schema] == ["col1", "col2"]
        assert [column.column_type for column in response.schema] == [
            ColumnType.STRING,
            ColumnType.INTEGER,
        ]

    @pytest.mark.parametrize("search_enabled", [True, False])
    def test_search_change_response_parses_status(self, search_enabled):
        """The resulting search status is retained, including when it is False."""
        # GIVEN a search change response
        data = {
            "concreteType": TABLE_SEARCH_CHANGE_RESPONSE,
            "searchEnabled": search_enabled,
        }

        # WHEN converting it
        response = table_update_response_from_dict(data)

        # THEN the status is available
        assert response.search_enabled is search_enabled


class TestEntityUpdateResult:
    """Test suite for EntityUpdateResult and the failure detail it retains."""

    @pytest.mark.parametrize(
        "data,expected_succeeded",
        [
            ({"entityId": "syn1"}, True),
            ({"entityId": "syn1", "failureCode": "NOT_FOUND"}, False),
            ({"entityId": "syn1", "failureMessage": "something broke"}, False),
            (
                {
                    "entityId": "syn1",
                    "failureCode": "ILLEGAL_ARGUMENT",
                    "failureMessage": "bad value",
                },
                False,
            ),
        ],
        ids=[
            "no_failure_reported",
            "failure_code_only",
            "failure_message_only",
            "failure_code_and_message",
        ],
    )
    def test_succeeded(self, data, expected_succeeded):
        """An update failed when Synapse reported either a code or a message."""
        # GIVEN an entity update result
        # WHEN converting it
        update_result = EntityUpdateResult.fill_from_dict(data)

        # THEN succeeded reflects whether any failure was reported
        assert update_result.succeeded is expected_succeeded

    @pytest.mark.parametrize(
        "failure_code", [failure_code.value for failure_code in EntityUpdateFailureCode]
    )
    def test_known_failure_code_is_coerced_to_the_enum(self, failure_code):
        """Every documented failure code maps onto the enum."""
        # GIVEN a result with a documented failure code
        # WHEN converting it
        update_result = EntityUpdateResult.fill_from_dict(
            {"entityId": "syn1", "failureCode": failure_code}
        )

        # THEN the code is the matching enum member
        assert update_result.failure_code == EntityUpdateFailureCode(failure_code)

    def test_unrecognized_failure_code_is_retained(self):
        """A failure code added to Synapse after this release must not raise."""
        # GIVEN a result with a failure code we do not model
        # WHEN converting it
        update_result = EntityUpdateResult.fill_from_dict(
            {"entityId": "syn1", "failureCode": "SOMETHING_NEW"}
        )

        # THEN the raw code is kept rather than raising a ValueError
        assert update_result.failure_code == "SOMETHING_NEW"
        assert not update_result.succeeded

    def test_failure_detail_is_retained(self):
        """The failure code and message are kept, not used as a filter and discarded."""
        # GIVEN a failed entity update
        # WHEN converting it
        update_result = EntityUpdateResult.fill_from_dict(
            {
                "entityId": "syn1",
                "failureCode": "ILLEGAL_ARGUMENT",
                "failureMessage": "value is not a valid date",
            }
        )

        # THEN every part of the failure is available to report to the user
        assert update_result.entity_id == "syn1"
        assert update_result.failure_code == EntityUpdateFailureCode.ILLEGAL_ARGUMENT
        assert update_result.failure_message == "value is not a valid date"

    def test_successful_and_failed_updates_are_separated(self):
        """EntityUpdateResults splits the successes from the failures."""
        # GIVEN a mix of successful and failed updates
        response = table_update_response_from_dict(
            _entity_update_results(
                [
                    {"entityId": "syn1"},
                    {"entityId": "syn2", "failureCode": "NOT_FOUND"},
                    {"entityId": "syn3", "failureMessage": "something broke"},
                ]
            )
        )

        # THEN the successes are reported by ID and the failures are reported whole
        assert response.successful_entity_ids == ["syn1"]
        assert [
            update_result.entity_id for update_result in response.failed_entity_updates
        ] == ["syn2", "syn3"]

    def test_successful_update_with_no_entity_id_is_still_counted(self):
        """A success that Synapse reported with no entity ID cannot be reported by ID,
        but it did apply, so it must still be counted as a changed row."""
        # GIVEN a successful update that carries no entity ID
        response = table_update_response_from_dict(
            _entity_update_results([{"entityId": "syn1"}, {}])
        )

        # THEN only the identified success is reported by ID, nothing is treated as a
        # failure, and both successes are counted
        assert response.successful_entity_ids == ["syn1"]
        assert response.failed_entity_updates == []
        assert response.rows_changed == 2

    def test_absent_update_results_yields_empty_lists(self):
        """A response with no update results reports empty rather than raising."""
        # GIVEN an EntityUpdateResults with no update results at all
        response = EntityUpdateResults()

        # THEN both properties are empty and the count is 0
        assert response.update_results is None
        assert response.successful_entity_ids == []
        assert response.failed_entity_updates == []
        assert response.rows_changed == 0


class TestTableUpdateRequest:
    """Test suite for the changes that may be included in a TableUpdateTransaction.

    Synapse accepts four kinds of change within one transaction, and every one of them
    must be usable through TableUpdateTransaction.
    """

    @staticmethod
    def _appendable_row_set_request() -> AppendableRowSetRequest:
        return AppendableRowSetRequest(
            entity_id="syn123",
            to_append=PartialRowSet(
                table_id="syn123",
                rows=[PartialRow(values=[{"key": "1", "value": "a"}], row_id=1)],
            ),
        )

    @staticmethod
    def _upload_to_table_request() -> UploadToTableRequest:
        return UploadToTableRequest(
            table_id="syn123", upload_file_handle_id="456", update_etag="etag"
        )

    @staticmethod
    def _table_schema_change_request() -> TableSchemaChangeRequest:
        return TableSchemaChangeRequest(
            entity_id="syn123",
            changes=[ColumnChange(new_column_id="789")],
            ordered_column_ids=["789"],
        )

    @staticmethod
    def _table_search_change_request() -> TableSearchChangeRequest:
        return TableSearchChangeRequest(entity_id="syn123", search_enabled=True)

    @pytest.mark.parametrize(
        "request_class",
        [
            AppendableRowSetRequest,
            UploadToTableRequest,
            TableSchemaChangeRequest,
            TableSearchChangeRequest,
        ],
    )
    def test_every_change_is_a_table_update_request(self, request_class):
        """Every change that Synapse accepts within a transaction shares the base
        class, so a caller may type a change as TableUpdateRequest."""
        # GIVEN a class that models one of the changes documented for a transaction
        # THEN it is a TableUpdateRequest
        assert issubclass(request_class, TableUpdateRequest)

    def test_base_class_cannot_be_used_on_its_own(self):
        """The base class only describes the shared contract."""
        # WHEN the base class is instantiated
        # THEN it is rejected because it models no change of its own
        with pytest.raises(TypeError):
            TableUpdateRequest()

    def test_search_change_request_converts_to_a_synapse_request(self):
        """A search change is sent with the concrete type that Synapse expects."""
        # GIVEN a request to enable search on a table
        request = TableSearchChangeRequest(entity_id="syn123", search_enabled=True)

        # WHEN it is converted for the REST API
        # THEN the entity, the flag, and the concrete type are all sent
        assert request.to_synapse_request() == {
            "concreteType": TABLE_SEARCH_CHANGE_REQUEST,
            "entityId": "syn123",
            "searchEnabled": True,
        }

    def test_search_change_request_may_disable_search(self):
        """The same request turns search off, so False must not be dropped."""
        # GIVEN a request to disable search on a table
        request = TableSearchChangeRequest(entity_id="syn123", search_enabled=False)

        # WHEN it is converted for the REST API
        # THEN the flag is sent as False rather than left out
        assert request.to_synapse_request()["searchEnabled"] is False

    def test_upload_to_table_request_reports_its_entity_id(self):
        """A CSV upload names its entity table_id, and entity_id gives every change
        one way to report the entity it applies to."""
        # GIVEN a request to apply an uploaded file to a table
        request = self._upload_to_table_request()

        # THEN the entity is available under the shared name
        assert request.entity_id == "syn123"

    @pytest.mark.parametrize(
        "table_id,entity_id",
        [
            ("syn123", None),
            (None, "syn123"),
            ("syn123", "syn123"),
        ],
        ids=["table_id_only", "entity_id_only", "both_equal"],
    )
    def test_upload_to_table_request_aliases_table_id_and_entity_id(
        self, table_id, entity_id
    ):
        """table_id and entity_id are aliases, so giving either one, or both with the
        same value, names the table and fills the other field."""
        # GIVEN a request that names its table through one or both of the aliases
        request = UploadToTableRequest(
            table_id=table_id, entity_id=entity_id, upload_file_handle_id="456"
        )

        # THEN both fields hold the table
        assert request.table_id == "syn123"
        assert request.entity_id == "syn123"

    @pytest.mark.parametrize(
        "table_id,entity_id",
        [
            (None, None),
            ("syn123", "syn456"),
        ],
        ids=["neither_given", "both_given_but_different"],
    )
    def test_upload_to_table_request_rejects_an_unnamed_or_ambiguous_table(
        self, table_id, entity_id
    ):
        """A request that names no table, or two different tables, is rejected."""
        # WHEN a request is created without a table or with two conflicting tables
        # THEN it is rejected
        with pytest.raises(ValueError):
            UploadToTableRequest(
                table_id=table_id, entity_id=entity_id, upload_file_handle_id="456"
            )

    def test_transaction_accepts_every_kind_of_change(self):
        """A single transaction may mix all four kinds of change, and each is sent in
        the order it was given."""
        # GIVEN a transaction that holds one of each kind of change
        changes = [
            self._table_schema_change_request(),
            self._appendable_row_set_request(),
            self._upload_to_table_request(),
            self._table_search_change_request(),
        ]
        transaction = TableUpdateTransaction(entity_id="syn123", changes=changes)

        # WHEN it is converted for the REST API
        request = transaction.to_synapse_request()

        # THEN every change is sent, in the order it was given
        assert [change["concreteType"] for change in request["changes"]] == [
            TABLE_SCHEMA_CHANGE_REQUEST,
            APPENDABLE_ROWSET_REQUEST,
            UPLOAD_TO_TABLE_REQUEST,
            TABLE_SEARCH_CHANGE_REQUEST,
        ]
        # AND each change is converted by the class that models it
        assert request["changes"] == [change.to_synapse_request() for change in changes]


class TestTableUpdateTransactionFillFromDict:
    """Test suite for the aggregates that TableUpdateTransaction.fill_from_dict fills.

    total_rows_changed is the count that upsert_rows reports, and
    entities_with_changes_applied must keep its original meaning because it is used as
    a dictionary key when waiting for an eventually consistent view.
    """

    def test_table_update_response_is_counted(self):
        """A table update reports a row count even though it reports no entity."""
        # GIVEN the response recorded from production for a table upsert
        transaction = TableUpdateTransaction(entity_id="syn76890550").fill_from_dict(
            {"results": [_row_reference_set_results(5)]}
        )

        # THEN the confirmed row count is available
        assert transaction.total_rows_changed == 5
        # AND entities_with_changes_applied keeps its original meaning, which is that a
        # table update never fills it
        assert transaction.entities_with_changes_applied is None
        # AND the response is available as the class that models it
        assert len(transaction.results) == 1
        assert isinstance(transaction.results[0], RowReferenceSetResults)

    def test_view_update_response_is_counted(self):
        """A view update contributes both a row count and the successful entity IDs."""
        # GIVEN a view response with one success and two failures
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {
                "results": [
                    _entity_update_results(
                        [
                            {"entityId": "syn1"},
                            {"entityId": "syn2", "failureCode": "NOT_FOUND"},
                            {
                                "entityId": "syn3",
                                "failureCode": "ILLEGAL_ARGUMENT",
                                "failureMessage": "bad value",
                            },
                        ]
                    )
                ]
            }
        )

        # THEN only the successful update is counted
        assert transaction.total_rows_changed == 1
        # AND only the successful IDs are reported
        assert transaction.entities_with_changes_applied == ["syn1"]
        # AND the failures are reported with the detail Synapse gave for each one
        assert [
            (
                failed_update.entity_id,
                failed_update.failure_code,
                failed_update.failure_message,
            )
            for failed_update in transaction.failed_entity_updates
        ] == [
            ("syn2", EntityUpdateFailureCode.NOT_FOUND, None),
            ("syn3", EntityUpdateFailureCode.ILLEGAL_ARGUMENT, "bad value"),
        ]

    def test_failed_entity_updates_are_collected_across_every_response(self):
        """The failures of every response that reports one are flattened together, and a
        response that reports no per-entity outcome contributes nothing."""
        # GIVEN a transaction whose changes returned two view responses and one table
        # response
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {
                "results": [
                    _entity_update_results(
                        [
                            {"entityId": "syn1"},
                            {"entityId": "syn2", "failureCode": "NOT_FOUND"},
                        ]
                    ),
                    _row_reference_set_results(5),
                    _entity_update_results(
                        [{"entityId": "syn3", "failureCode": "UNAUTHORIZED"}]
                    ),
                ]
            }
        )

        # THEN both failures are reported, in the order the responses were returned
        assert [
            failed_update.entity_id
            for failed_update in transaction.failed_entity_updates
        ] == ["syn2", "syn3"]

    def test_table_update_response_reports_no_failed_entity_update(self):
        """A rejected row update on a table fails the asynchronous job and raises, so a
        table response never carries a per-row failure."""
        # GIVEN the response recorded from production for a table upsert
        transaction = TableUpdateTransaction(entity_id="syn76890550").fill_from_dict(
            {"results": [_row_reference_set_results(5)]}
        )

        # THEN no failure is reported
        assert transaction.failed_entity_updates == []

    def test_original_field_stays_none_when_no_entity_succeeded(self):
        """Regression guard: entities_with_changes_applied is only set when there is
        at least one success. It is used as a dictionary key at the call site, so its
        behaviour must not drift."""
        # GIVEN a view response in which every update failed
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {
                "results": [
                    _entity_update_results(
                        [
                            {"entityId": "syn1", "failureCode": "NOT_FOUND"},
                            {"entityId": "syn2", "failureCode": "UNAUTHORIZED"},
                        ]
                    )
                ]
            }
        )

        # THEN the field is left as None
        assert transaction.entities_with_changes_applied is None
        assert transaction.total_rows_changed == 0
        # AND both failures are still reported
        assert len(transaction.failed_entity_updates) == 2

    def test_counts_are_summed_across_every_response(self):
        """One response is returned per change in the transaction, and each that
        reports a count contributes to the total."""
        # GIVEN a transaction whose changes returned three different response types
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {
                "results": [
                    {
                        "concreteType": TABLE_SCHEMA_CHANGE_RESPONSE,
                        "schema": [{"name": "col1", "columnType": "STRING"}],
                    },
                    _row_reference_set_results(5),
                    {"concreteType": UPLOAD_TO_TABLE_RESULT, "rowsProcessed": 2},
                ]
            }
        )

        # THEN the schema change contributes nothing and the row counts are summed
        assert transaction.total_rows_changed == 7
        # AND the responses are kept in the order Synapse returned them
        assert [type(response) for response in transaction.results] == [
            TableSchemaChangeResponse,
            RowReferenceSetResults,
            UploadToTableResult,
        ]

    def test_a_row_change_that_created_no_row_contributes_a_confirmed_zero(self):
        """A table row change always carries a row count, so one that created no row
        version contributes a confirmed 0 rather than being dropped from the total as a
        response that reports no count at all."""
        # GIVEN a transaction whose row change reported no row reference set
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {
                "results": [
                    {"concreteType": ROW_REFERENCE_SET_RESULTS},
                    {"concreteType": UPLOAD_TO_TABLE_RESULT, "rowsProcessed": 2},
                ]
            }
        )

        # THEN the row change is counted as 0 rather than skipped
        assert transaction.results[0].rows_changed == 0
        assert transaction.total_rows_changed == 2

    def test_unmodelled_response_does_not_break_the_count(self):
        """An unmodelled response contributes nothing rather than raising."""
        # GIVEN a transaction that returned one known and one unknown response
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {
                "results": [
                    _row_reference_set_results(3),
                    {
                        "concreteType": "org.sagebionetworks.repo.model.table.New",
                        "somethingNew": 99,
                    },
                ]
            }
        )

        # THEN only the known response is counted
        assert transaction.total_rows_changed == 3
        assert isinstance(transaction.results[1], UnknownTableUpdateResponse)

    @pytest.mark.parametrize(
        "synapse_response",
        [{}, {"results": None}],
        ids=["results_absent", "results_null"],
    )
    def test_aggregates_stay_none_when_nothing_was_returned(self, synapse_response):
        """The aggregates are None before anything is reported, never 0. The call site
        relies on that to tell an absent count from a confirmed count of 0."""
        # GIVEN a response that carries no results
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            synapse_response
        )

        # THEN nothing was counted and nothing was parsed
        assert transaction.total_rows_changed is None
        assert transaction.results is None
        assert transaction.entities_with_changes_applied is None
        # AND the failure list is empty rather than None, since there is nothing to
        # tell apart: a transaction that reported nothing reported no failure
        assert transaction.failed_entity_updates == []

    def test_empty_results_array_is_kept_apart_from_an_absent_one(self):
        """An empty results array means Synapse reported the transaction and changed
        nothing. That is a confirmed count of 0, which the caller must be able to tell
        apart from a transaction that was never sent."""
        # GIVEN a response that reports an empty results array
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {"results": []}
        )

        # THEN the empty array is kept as such, and the count is a confirmed 0
        assert transaction.results == []
        assert transaction.total_rows_changed == 0
        assert transaction.entities_with_changes_applied is None
        assert transaction.failed_entity_updates == []

    def test_a_later_send_replaces_the_results_of_an_earlier_one(self):
        """The same transaction instance can be sent more than once, since
        send_job_and_wait_async returns self. A later response must replace the
        results of the earlier one rather than leave a stale count in place."""
        # GIVEN a transaction that already reported changed rows
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {"results": [_row_reference_set_results(5)]}
        )
        assert transaction.total_rows_changed == 5

        # WHEN the same instance is sent again and Synapse reports no results
        transaction.fill_from_dict({"results": None})

        # THEN the counts of the earlier send are gone
        assert transaction.results is None
        assert transaction.total_rows_changed is None

    def test_snapshot_version_number_is_filled(self):
        """A transaction that created a snapshot reports the new version number
        alongside the modelled responses."""
        # GIVEN a response from Synapse that reports a snapshot version
        transaction = TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {"results": [_row_reference_set_results(2)], "snapshotVersionNumber": 4}
        )

        # THEN the version number and the modelled responses are both available
        assert transaction.snapshot_version_number == 4
        assert [type(response) for response in transaction.results] == [
            RowReferenceSetResults
        ]


class TestUpsertRowsResultReporting:
    """Test suite for how _upsert_rows_async reports what Synapse confirmed.

    Regression coverage for SYNPY-1912, where every successful table upsert logged a
    contradictory message: the correct number of updated rows, followed by a claim that
    the same number of rows could not be updated.
    """

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    COLUMNS_FOR_TEST = {
        "col1": Column(name="col1", column_type=ColumnType.STRING, id="id1"),
        "col2": Column(name="col2", column_type=ColumnType.INTEGER, id="id2"),
    }

    @dataclass
    class TableForTest(TableUpsertMixin):
        """A minimal Table-like entity. The class name is deliberately not one of
        CLASSES_THAT_CONTAIN_ROW_ETAG, so the rows carry no etag."""

        id: Optional[str] = None
        name: Optional[str] = None
        columns: Dict[str, Column] = field(default_factory=dict)
        _last_persistent_instance: Optional[Any] = True
        query_results: List[Any] = field(default_factory=list)
        stored_rows: Optional[Any] = None

        async def query_async(self, query: str, synapse_client=None) -> Any:
            return self.query_results.pop(0)

        async def store_rows_async(self, values=None, **kwargs) -> None:
            self.stored_rows = values

    @dataclass
    class ViewForTest(ViewBase, TableUpsertMixin):
        """A minimal View-like entity. Only the entities that back a view report a
        per-row outcome, so this is the only kind of entity that can produce a failure
        clause."""

        columns: Dict[str, Column] = field(default_factory=dict)
        query_results: List[Any] = field(default_factory=list)

        async def query_async(self, query: str, synapse_client=None) -> Any:
            return self.query_results.pop(0)

    @staticmethod
    def _existing_rows(row_ids: List[str], col2_values: List[int]) -> Any:
        """The rows a query returns for the keys that are already in the table."""
        return pd.DataFrame(
            {
                "ROW_ID": row_ids,
                "col1": [f"key{row_id}" for row_id in row_ids],
                "col2": col2_values,
            }
        )

    @staticmethod
    def _values_to_upsert(keys: List[str]) -> Dict[str, Any]:
        """New values for each of the given keys. Every value differs from the value
        that _existing_rows returns, so every matched row is an update."""
        return {
            "col1": [f"key{key}" for key in keys],
            "col2": [int(key) * 100 for key in keys],
        }

    def _table(
        self, query_results: List[Any]
    ) -> "TestUpsertRowsResultReporting.TableForTest":
        return self.TableForTest(
            id="syn123",
            name="test-table",
            columns=dict(self.COLUMNS_FOR_TEST),
            query_results=query_results,
        )

    def _view(
        self, query_results: List[Any]
    ) -> "TestUpsertRowsResultReporting.ViewForTest":
        view = self.ViewForTest(
            id="syn456",
            name="test-view",
            columns=dict(self.COLUMNS_FOR_TEST),
            query_results=query_results,
        )
        view._last_persistent_instance = True
        return view

    @staticmethod
    def _table_transaction(row_count: int) -> TableUpdateTransaction:
        """The transaction Synapse returns for the update half of a table upsert."""
        return TableUpdateTransaction(entity_id="syn123").fill_from_dict(
            {"results": [_row_reference_set_results(row_count)]}
        )

    @staticmethod
    def _view_transaction(
        update_results: List[Dict[str, Any]],
    ) -> TableUpdateTransaction:
        """The transaction Synapse returns for a change applied to the entities that
        back a view."""
        return TableUpdateTransaction(entity_id="syn456").fill_from_dict(
            {"results": [_entity_update_results(update_results)]}
        )

    @staticmethod
    def _upsert_message(mock_info: MagicMock) -> str:
        """The single logged message that reports the upsert counts."""
        messages = [
            call.args[0]
            for call in mock_info.call_args_list
            if "rows to update" in call.args[0]
        ]
        assert len(messages) == 1
        return messages[0]

    async def test_table_upsert_reports_the_confirmed_count_with_no_failure_clause(
        self,
    ):
        """A successful table upsert must not claim that any row failed. This is the
        defect that was reported."""
        # GIVEN a table that holds 5 of the 7 keys being upserted
        entity = self._table(
            [self._existing_rows(["1", "2", "3", "4", "5"], [1, 2, 3, 4, 5])]
        )

        # WHEN Synapse confirms all 5 row updates
        with (
            patch(
                _PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH,
                new_callable=AsyncMock,
                return_value=[self._table_transaction(5)],
            ),
            patch.object(self.syn.logger, "info") as mock_info,
            patch.object(self.syn.logger, "debug") as mock_debug,
        ):
            await _upsert_rows_async(
                entity=entity,
                values=self._values_to_upsert(["1", "2", "3", "4", "5", "6", "7"]),
                primary_keys=["col1"],
                synapse_client=self.syn,
            )

        # THEN the confirmed count is reported and no failure is claimed
        assert (
            self._upsert_message(mock_info)
            == "[syn123:test-table]: Found 5 rows to update and 2 rows to insert"
        )
        # AND no accounting gap is reported, because every row was accounted for
        assert not [
            call for call in mock_debug.call_args_list if "gap in how" in call.args[0]
        ]
        # AND the 2 unmatched rows were inserted
        assert len(entity.stored_rows) == 2

    async def test_row_update_results_accumulate_across_query_chunks(self):
        """An upsert of more than rows_per_query rows reports the total across every
        chunk, not just the count from the last one."""
        # GIVEN 6 rows to upsert, queried 2 at a time, all of which already exist
        entity = self._table(
            [
                self._existing_rows(["1", "2"], [1, 2]),
                self._existing_rows(["3", "4"], [3, 4]),
                self._existing_rows(["5", "6"], [5, 6]),
            ]
        )

        # WHEN Synapse confirms 2 row updates per chunk
        with (
            patch(
                _PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH,
                new_callable=AsyncMock,
                side_effect=[
                    [self._table_transaction(2)],
                    [self._table_transaction(2)],
                    [self._table_transaction(2)],
                ],
            ) as mock_push,
            patch.object(self.syn.logger, "info") as mock_info,
        ):
            await _upsert_rows_async(
                entity=entity,
                values=self._values_to_upsert(["1", "2", "3", "4", "5", "6"]),
                primary_keys=["col1"],
                rows_per_query=2,
                synapse_client=self.syn,
            )

        # THEN every chunk was pushed
        assert mock_push.await_count == 3
        # AND the reported count is the total of all three chunks
        assert (
            self._upsert_message(mock_info)
            == "[syn123:test-table]: Found 6 rows to update and 0 rows to insert"
        )

    async def test_dry_run_reports_the_planned_count(self):
        """Nothing is pushed on a dry run, so the planned count is the only meaningful
        answer to what would happen."""
        # GIVEN a table that holds 5 of the 7 keys being upserted
        entity = self._table(
            [self._existing_rows(["1", "2", "3", "4", "5"], [1, 2, 3, 4, 5])]
        )

        # WHEN upserting as a dry run
        with (
            patch(
                _PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH, new_callable=AsyncMock
            ) as mock_push,
            patch.object(self.syn.logger, "info") as mock_info,
        ):
            await _upsert_rows_async(
                entity=entity,
                values=self._values_to_upsert(["1", "2", "3", "4", "5", "6", "7"]),
                primary_keys=["col1"],
                dry_run=True,
                synapse_client=self.syn,
            )

        # THEN nothing was sent to Synapse
        mock_push.assert_not_awaited()
        assert entity.stored_rows is None
        # AND the planned counts are reported, with no failure claimed
        assert (
            self._upsert_message(mock_info)
            == "[syn123:test-table]: Found 5 rows to update and 2 rows to insert"
        )

    async def test_confirmed_count_of_zero_is_reported_as_zero(self):
        """A push that Synapse confirmed changed nothing reports 0 rather than falling
        back to the planned count. The fallback is what hid the original defect."""
        # GIVEN a table that holds all 5 keys being upserted
        entity = self._table(
            [self._existing_rows(["1", "2", "3", "4", "5"], [1, 2, 3, 4, 5])]
        )

        # WHEN Synapse reports no row references and no failure
        with (
            patch(
                _PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH,
                new_callable=AsyncMock,
                return_value=[self._table_transaction(0)],
            ),
            patch.object(self.syn.logger, "info") as mock_info,
            patch.object(self.syn.logger, "debug") as mock_debug,
        ):
            await _upsert_rows_async(
                entity=entity,
                values=self._values_to_upsert(["1", "2", "3", "4", "5"]),
                primary_keys=["col1"],
                synapse_client=self.syn,
            )

        # THEN the confirmed count of 0 is reported, and still no failure is claimed
        assert (
            self._upsert_message(mock_info)
            == "[syn123:test-table]: Found 0 rows to update and 0 rows to insert"
        )
        # AND the shortfall is reported as a client accounting gap, at debug level
        gap_messages = [
            call.args[0]
            for call in mock_debug.call_args_list
            if "gap in how" in call.args[0]
        ]
        assert len(gap_messages) == 1
        assert "Synapse confirmed 0 of the 5 rows sent for update" in gap_messages[0]

    @pytest.mark.parametrize(
        "failed_updates,expected_clause",
        [
            (
                [{"entityId": "syn2", "failureCode": "NOT_FOUND"}],
                ". 1 rows could not be updated: syn2 (NOT_FOUND)",
            ),
            (
                [
                    {
                        "entityId": "syn2",
                        "failureCode": "ILLEGAL_ARGUMENT",
                        "failureMessage": "bad value",
                    }
                ],
                ". 1 rows could not be updated: syn2 (ILLEGAL_ARGUMENT: bad value)",
            ),
            (
                [{"entityId": "syn2", "failureCode": "SOMETHING_NEW"}],
                ". 1 rows could not be updated: syn2 (SOMETHING_NEW)",
            ),
            (
                [{"failureMessage": "something broke"}],
                ". 1 rows could not be updated: unknown row (UNKNOWN: something broke)",
            ),
            (
                [
                    {"entityId": "syn2", "failureCode": "NOT_FOUND"},
                    {
                        "entityId": "syn3",
                        "failureCode": "ILLEGAL_ARGUMENT",
                        "failureMessage": "bad value",
                    },
                ],
                ". 2 rows could not be updated: syn2 (NOT_FOUND);"
                " syn3 (ILLEGAL_ARGUMENT: bad value)",
            ),
        ],
        ids=[
            "code_only",
            "code_and_message",
            "unrecognized_code",
            "message_with_no_id_or_code",
            "two_failures",
        ],
    )
    async def test_view_upsert_reports_the_failure_detail_synapse_returned(
        self, failed_updates, expected_clause
    ):
        """A failure clause is built from the codes and messages Synapse reported, so
        the user has something actionable rather than a bare count."""
        # GIVEN a view that holds all 3 keys being upserted
        entity = self._view([self._existing_rows(["1", "2", "3"], [1, 2, 3])])

        # WHEN Synapse reports one success and the given failures
        with (
            patch(
                _PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH,
                new_callable=AsyncMock,
                return_value=[
                    self._view_transaction([{"entityId": "syn1"}] + failed_updates)
                ],
            ),
            patch.object(self.syn.logger, "info") as mock_info,
        ):
            await _upsert_rows_async(
                entity=entity,
                values=self._values_to_upsert(["1", "2", "3"]),
                primary_keys=["col1"],
                synapse_client=self.syn,
            )

        # THEN the confirmed count is followed by the detail of each failure
        assert self._upsert_message(mock_info) == (
            "[syn456:test-view]: Found 1 rows to update and 0 rows to insert"
            + expected_clause
        )

    async def test_view_upsert_with_no_failures_claims_none(self):
        """A view upsert that Synapse fully applied reports no failure either."""
        # GIVEN a view that holds all 3 keys being upserted
        entity = self._view([self._existing_rows(["1", "2", "3"], [1, 2, 3])])

        # WHEN Synapse confirms all 3 entity updates
        with (
            patch(
                _PUSH_ROW_UPDATES_TO_SYNAPSE_PATCH,
                new_callable=AsyncMock,
                return_value=[
                    self._view_transaction(
                        [
                            {"entityId": "syn1"},
                            {"entityId": "syn2"},
                            {"entityId": "syn3"},
                        ]
                    )
                ],
            ),
            patch.object(self.syn.logger, "info") as mock_info,
        ):
            await _upsert_rows_async(
                entity=entity,
                values=self._values_to_upsert(["1", "2", "3"]),
                primary_keys=["col1"],
                synapse_client=self.syn,
            )

        # THEN all 3 updates are reported as applied
        assert (
            self._upsert_message(mock_info)
            == "[syn456:test-view]: Found 3 rows to update and 0 rows to insert"
        )


@pytest.mark.parametrize(
    "series,expected",
    [
        pytest.param(
            pd.Series(
                [[datetime(2021, 1, 1), datetime(2021, 1, 2)], None], dtype=object
            ),
            True,
            id="list_of_datetimes",
        ),
        pytest.param(
            pd.Series([[date(2021, 1, 1), date(2021, 1, 2)], None], dtype=object),
            True,
            id="list_of_dates",
        ),
        pytest.param(
            pd.Series([(date(2021, 1, 1), date(2021, 1, 2))], dtype=object),
            True,
            id="tuple_of_dates",
        ),
        pytest.param(
            pd.Series([[None, date(2021, 1, 1)]], dtype=object),
            True,
            id="list_with_none_items_and_a_date",
        ),
        pytest.param(
            pd.Series([[1, 2], [5, 6], None], dtype=object),
            False,
            id="list_of_non_date_values",
        ),
        pytest.param(
            pd.Series([["a", "b"], None], dtype=object),
            False,
            id="list_of_strings",
        ),
        pytest.param(
            # a plain (non-list) datetime column, which
            # _convert_df_date_cols_to_epoch_time handles via a separate branch
            pd.Series([datetime(2021, 1, 1), None], dtype=object),
            False,
            id="scalar_datetime_column",
        ),
        pytest.param(
            pd.Series([None, None], dtype=object),
            False,
            id="all_null_column",
        ),
        pytest.param(
            pd.Series([[], []], dtype=object),
            False,
            id="empty_lists_only",
        ),
    ],
)
def test_is_date_list_column(series, expected):
    assert _is_date_list_column(series) is expected


class TestConvertDfDateColsToEpochTime:
    """Tests for _convert_df_date_cols_to_epoch_time. Unit tests run with
    TZ=UTC by default, so naive datetimes convert deterministically."""

    def test_dataframe_without_datetime_cols_is_unchanged(self):
        df = pd.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})

        result = _convert_df_date_cols_to_epoch_time(df=df)

        pd.testing.assert_frame_equal(result, df)

    def test_empty_datetime_column_is_still_converted(self):
        # GIVEN a datetime64 column with zero rows — e.g. an upsert/store call
        # made with an empty dataframe
        df = pd.DataFrame({"dt": pd.to_datetime([], utc=True)})

        result = _convert_df_date_cols_to_epoch_time(df=df)

        # THEN the column is still converted to the nullable Int64 dtype used
        # for epoch milliseconds, not left as datetime64
        assert result.empty
        assert is_integer_dtype(result["dt"])

    def test_null_values_keep_integer_dtype(self):
        df = pd.DataFrame(
            {
                "col1": ["a", "b"],
                "dt": pd.to_datetime([1487071391024, None], unit="ms", utc=True),
            }
        )

        result = _convert_df_date_cols_to_epoch_time(df=df)

        # THEN the values are interpreted as UTC wall-clock times
        expected_result = pd.DataFrame(
            {"col1": ["a", "b"], "dt": [1487071391024, None]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        assert is_integer_dtype(result.dt)

    def test_timezone_aware_datetimes_converted_to_utc_exactly(self):
        # GIVEN values localized to a zone that observes daylight saving time,
        # one in winter (PST, UTC-8) and one in summer (PDT, UTC-7)
        df = pd.DataFrame(
            {
                "dt_aware": pd.to_datetime(
                    [
                        datetime(2017, 2, 14, 11, 23, 11, 240000),
                        datetime(2018, 10, 1),
                    ]
                ).tz_localize("America/Los_Angeles"),
            }
        )

        result = _convert_df_date_cols_to_epoch_time(df=df)

        # THEN each value converts using the UTC offset in effect on its own
        # date (2017-02-14 19:23:11.240 UTC and 2018-10-01 07:00 UTC), not the
        # timezone of the machine running the conversion
        expected_result = pd.DataFrame(
            {"dt_aware": [1487100191240, 1538377200000]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)

    def test_naive_datetimes_interpreted_as_local_timezone(self):
        # GIVEN naive datetimes; unit tests run with TZ=UTC (see conftest.py),
        # so "the machine's local timezone at upload time" is UTC here
        df = pd.DataFrame(
            {
                "dt_naive": [
                    datetime(2017, 2, 14, 11, 23, 11, 240000),
                    datetime(
                        2018, 10, 1
                    ),  # convert to midnight of the date in the local timezone
                ]
            }
        )

        result = _convert_df_date_cols_to_epoch_time(df=df)

        # THEN the date column is converted to epoch ms (midnight local timezone,
        # unit tests run with TZ=UTC)
        expected_result = pd.DataFrame(
            {"dt_naive": [1487071391240, 1538352000000]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)

    def test_date_object_columns_are_converted(self):
        # GIVEN a column of plain datetime.date objects, conversion should be done to midnight local timezone.
        df = pd.DataFrame(
            {"date_col": [date(2017, 2, 14), date(2018, 10, 1)], "other": ["a", "b"]}
        )

        # WHEN the dataframe is converted
        result = _convert_df_date_cols_to_epoch_time(df=df)

        # THEN the date column is converted to epoch ms (midnight local timezone,
        # unit tests run with TZ=UTC)
        expected_result = pd.DataFrame(
            {"date_col": [1487030400000, 1538352000000], "other": ["a", "b"]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        assert is_integer_dtype(result["date_col"])

    def test_date_object_columns_with_nulls_keep_integer_dtype(self):
        # GIVEN a column of datetime.date objects mixed with a null value
        df = pd.DataFrame({"date_col": [date(2017, 2, 14), None]})

        result = _convert_df_date_cols_to_epoch_time(df=df)

        expected_result = pd.DataFrame(
            {"date_col": [1487030400000, None]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        assert is_integer_dtype(result["date_col"])

    def test_date_list_columns_are_converted(self):
        # GIVEN a DATE_LIST column: each cell is a Python list of datetimes,
        # which pandas can only ever infer as "mixed" dtype, not "datetime"
        df = pd.DataFrame(
            {
                "other": ["a", None],
                "date_list_col": [
                    [
                        datetime(2017, 2, 14, 11, 23, 11, 240000),
                        datetime(2018, 10, 1),
                    ],
                    None,
                ],
                "date_list_col": [[date(2017, 2, 14), date(2018, 10, 1)], None],
            }
        )

        result = _convert_df_date_cols_to_epoch_time(df=df)

        # THEN every element of every list cell is converted to epoch ms
        # (midnight local timezone for the date-only value; unit tests run
        # with TZ=UTC)
        expected_result = pd.DataFrame(
            {
                "other": ["a", None],
                "date_list_col": [
                    [1487071391240, 1538352000000],
                    None,
                ],
                "date_list_col": [[1487030400000, 1538352000000], None],
            }
        )
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)

    def test_integer_list_columns_are_unaffected(self):
        # GIVEN an INTEGER_LIST column, which also infers as "mixed" dtype but
        # holds no date/datetime values
        df = pd.DataFrame({"int_list_col": [[1, 2], None, [5, 6]]})

        result = _convert_df_date_cols_to_epoch_time(df=df)

        pd.testing.assert_frame_equal(result, df)


class TestParseDfDateColsToDatetime:
    """Tests for parsing date columns holding formatted date strings via
    `_parse_df_date_cols_to_datetime`."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_date_strings_parsed_with_format(self):
        csv_buffer = BytesIO(b"col1,date_col\na,01/15/2024\nb,\n")
        df = csv_to_pandas_df(filepath=csv_buffer, row_id_and_version_in_index=False)

        result = _parse_df_date_cols_to_datetime(
            df=df,
            date_columns=["date_col"],
            date_format="%m/%d/%Y",
        )
        expected_result = pd.DataFrame(
            {"col1": ["a", "b"], "date_col": [datetime(2024, 1, 15), None]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)

    def test_date_format_omitted_infers_format(self):
        # GIVEN date strings in a standard format and no `date_format` argument
        df = pd.DataFrame(
            {"col1": ["a", "b"], "date_col": ["2024-01-15", None]}
        ).convert_dtypes()

        # WHEN the column is parsed without specifying `date_format`
        result = _parse_df_date_cols_to_datetime(
            df=df,
            date_columns=["date_col"],
        )

        # THEN pandas infers the format on its own
        expected_result = pd.DataFrame(
            {"col1": ["a", "b"], "date_col": [datetime(2024, 1, 15), None]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)

    def test_date_format_as_dict_maps_columns_to_formats(self):
        # GIVEN date strings in different formats and a `date_format` argument as a dict
        df = pd.DataFrame(
            {
                "date_col1": ["01/15/2024"],
                "date_col2": ["2024-01-20"],
                "datetime_col3": ["2024-02-01 14:30:00"],
            }
        ).convert_dtypes()

        result = _parse_df_date_cols_to_datetime(
            df=df,
            date_columns=["date_col1", "date_col2", "datetime_col3"],
            date_format={
                "date_col1": "%m/%d/%Y",
                "date_col2": "%Y-%m-%d",
                "datetime_col3": "%Y-%m-%d %H:%M:%S",
            },
        )
        # naive datetime strings are converted to naive datetimes
        expected_result = pd.DataFrame(
            {
                "date_col1": [datetime(2024, 1, 15)],
                "date_col2": [datetime(2024, 1, 20)],
                "datetime_col3": [datetime(2024, 2, 1, 14, 30, 0)],
            }
        )
        pd.testing.assert_frame_equal(result, expected_result)

    def test_missing_date_column_raises(self):
        # GIVEN a dataframe with a date column that is not present in the dataframe
        df = pd.DataFrame({"col1": ["a"]})

        with pytest.raises(
            ValueError,
            match=re.escape(
                "The date column(s) date_col listed in `date_columns` "
                "are not present in the data. Please ensure that the date columns "
                "are already in the dataframe."
            ),
        ):
            _parse_df_date_cols_to_datetime(
                df=df,
                date_columns=["date_col"],
                date_format="%m/%d/%Y",
            )

    def test_date_string_not_matching_format_raises(self):
        # GIVEN date strings that do not match the given format
        df = pd.DataFrame({"date_col": ["2024-01-15"]})

        with pytest.raises(ValueError):
            _parse_df_date_cols_to_datetime(
                df=df,
                date_columns=["date_col"],
                date_format="%m/%d/%Y",
            )

    def test_uniform_utc_offsets_parsed_as_tz_aware(self):
        # GIVEN date strings that all carry the same UTC offset
        df = pd.DataFrame(
            {"date_col": ["01/15/2024 12:00 -0800", None, "02/20/2024 12:00 -0800"]}
        ).convert_dtypes()

        with patch.object(self.syn, "logger") as mock_logger:
            result = _parse_df_date_cols_to_datetime(
                df=df,
                date_columns=["date_col"],
                date_format="%m/%d/%Y %H:%M %z",
                synapse_client=self.syn,
            )

        expected_result = pd.DataFrame(
            {
                "date_col": [
                    pd.Timestamp("2024-01-15 12:00:00-0800", tz="UTC-08:00"),
                    None,
                    pd.Timestamp("2024-02-20 12:00:00-0800", tz="UTC-08:00"),
                ]
            }
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)
        mock_logger.info.assert_not_called()

    def test_mixed_utc_offsets_normalized_to_utc(self):
        # GIVEN date strings whose UTC offsets differ between rows, as produced
        # by exporting a zone that observes daylight saving time with each
        # value's true offset (winter -0800, summer -0700)
        df = pd.DataFrame(
            {"date_col": ["01/15/2024 12:00 -0800", "07/15/2024 12:00 -0700"]}
        ).convert_dtypes()

        with patch.object(self.syn, "logger") as mock_logger:
            result = _parse_df_date_cols_to_datetime(
                df=df,
                date_columns=["date_col"],
                date_format="%m/%d/%Y %H:%M %z",
                synapse_client=self.syn,
            )

        expected_result = pd.DataFrame(
            {
                "date_col": [
                    pd.Timestamp("2024-01-15 20:00:00", tz="UTC"),
                    pd.Timestamp("2024-07-15 19:00:00", tz="UTC"),
                ]
            }
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)
        mock_logger.info.assert_called_once_with(
            "The date column date_col holds mixed timezones/offsets and will be normalized to UTC."
        )

    def test_date_list_strings_parsed_with_format(self):
        # GIVEN a DATE_LIST column
        df = pd.DataFrame(
            {
                "col1": ["a", "b"],
                "date_list_col": [
                    ["01/15/2024", None, "02/20/2024"],
                    None,
                ],
            }
        ).convert_dtypes()

        result = _parse_df_date_cols_to_datetime(
            df=df,
            date_columns=["date_list_col"],
            date_format="%m/%d/%Y",
        )

        # THEN every item in every list is parsed to a datetime, `None`
        # items are preserved, and entirely `None` cells are left untouched
        expected_result = pd.DataFrame(
            {
                "col1": ["a", "b"],
                "date_list_col": [
                    [datetime(2024, 1, 15), None, datetime(2024, 2, 20)],
                    None,
                ],
            }
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)

    def test_date_list_uniform_utc_offsets_parsed_as_tz_aware(self):
        # GIVEN a DATE_LIST column whose items all carry the same UTC offset
        df = pd.DataFrame(
            {
                "date_list_col": [
                    ["01/15/2024 12:00 -0800", "02/20/2024 12:00 -0800"],
                    None,
                ]
            }
        )

        with patch.object(self.syn, "logger") as mock_logger:
            result = _parse_df_date_cols_to_datetime(
                df=df,
                date_columns=["date_list_col"],
                date_format="%m/%d/%Y %H:%M %z",
                synapse_client=self.syn,
            )

        expected_result = pd.DataFrame(
            {
                "date_list_col": [
                    [
                        pd.Timestamp("2024-01-15 12:00:00-0800", tz="UTC-08:00"),
                        pd.Timestamp("2024-02-20 12:00:00-0800", tz="UTC-08:00"),
                    ],
                    None,
                ]
            }
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)
        mock_logger.info.assert_not_called()

    def test_date_list_mixed_utc_offsets_normalized_to_utc(self):
        # GIVEN a DATE_LIST column whose items' UTC offsets differ, including
        # between items within the same cell
        df = pd.DataFrame(
            {
                "date_list_col": [
                    ["01/15/2024 12:00 -0800", None, "07/15/2024 12:00 -0700"],
                    None,
                ]
            }
        )

        with patch.object(self.syn, "logger") as mock_logger:
            result = _parse_df_date_cols_to_datetime(
                df=df,
                date_columns=["date_list_col"],
                date_format="%m/%d/%Y %H:%M %z",
                synapse_client=self.syn,
            )

        expected_result = pd.DataFrame(
            {
                "date_list_col": [
                    [
                        pd.Timestamp("2024-01-15 20:00:00", tz="UTC"),
                        None,
                        pd.Timestamp("2024-07-15 19:00:00", tz="UTC"),
                    ],
                    None,
                ]
            }
        ).convert_dtypes()
        pd.testing.assert_frame_equal(result, expected_result)
        mock_logger.info.assert_called_once_with(
            "The date column date_list_col holds mixed timezones/offsets and will be normalized to UTC."
        )


class TestConvertCsvDateColsToEpochTime:
    """Tests for _convert_csv_date_cols_to_epoch_time. Unit tests run with
    TZ=UTC, so naive datetimes convert deterministically."""

    def test_date_cols_converted_to_epoch_ms(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as csv_file:
            csv_file.write("col1,date_col\na,01/15/2024\nb,\nc,02/20/2024\n")
        result_path = None
        try:
            df = csv_to_pandas_df(
                filepath=csv_file.name,
                row_id_and_version_in_index=False,
            )
            df = _parse_df_date_cols_to_datetime(
                df=df, date_columns=["date_col"], date_format="%m/%d/%Y"
            )

            result_path = _convert_csv_date_cols_to_epoch_time(df=df)

            result = pd.read_csv(result_path)
            expected_result = pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "date_col": [
                        1705276800000,
                        None,
                        1708387200000,
                    ],  # date columns are converted to epoch ms (midnight local timezone, unit tests run with TZ=UTC)
                }
            ).convert_dtypes()
            pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        finally:
            os.remove(csv_file.name)
            if result_path:
                os.remove(result_path)

    def test_temp_file_written_with_csv_table_descriptor_format(self):
        # GIVEN a DataFrame with a parsed date column and a tab-separated descriptor
        df = pd.DataFrame(
            {"col1": ["a", "b"], "date_col": ["01/15/2024", "02/20/2024"]}
        ).convert_dtypes()
        df["date_col"] = pd.to_datetime(df["date_col"], format="%m/%d/%Y")

        result_path = _convert_csv_date_cols_to_epoch_time(
            df=df,
            csv_table_descriptor=CsvTableDescriptor(separator="\t"),
        )
        try:
            result = pd.read_csv(result_path, sep="\t")
            expected_result = pd.DataFrame(
                {
                    "col1": ["a", "b"],
                    "date_col": [1705276800000, 1708387200000],
                }  # date columns are converted to epoch ms (midnight local timezone, unit tests run with TZ=UTC)
            ).convert_dtypes()
            pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        finally:
            os.remove(result_path)

    def test_mixed_utc_offsets_converted_to_exact_epoch_ms(self):
        # GIVEN date strings whose UTC offsets differ between rows, as produced
        # by exporting a zone that observes daylight saving time with each
        # value's true offset (winter -0800, summer -0700). The parse step
        # normalizes them to UTC
        csv_buffer = BytesIO(
            b"date_col\n01/15/2024 12:00 -0800\n07/15/2024 12:00 -0700\n"
        )
        df = csv_to_pandas_df(
            filepath=csv_buffer,
            row_id_and_version_in_index=False,
        )
        df = _parse_df_date_cols_to_datetime(
            df=df, date_columns=["date_col"], date_format="%m/%d/%Y %H:%M %z"
        )

        # WHEN the dataframe is written to the upload file
        result_path = _convert_csv_date_cols_to_epoch_time(df=df)

        try:
            result = pd.read_csv(result_path)
            # THEN each value converts using the UTC offset it carried
            # (12:00-08:00 -> 20:00 UTC, 12:00-07:00 -> 19:00 UTC), preserving
            # the exact moments in time independent of the machine's timezone
            expected_result = pd.DataFrame(
                {"date_col": [1705348800000, 1721070000000]}
            ).convert_dtypes()
            pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)
        finally:
            os.remove(result_path)

    def test_headerless_descriptor_raises(self):
        # GIVEN a descriptor stating the CSV file has no header row
        df = pd.DataFrame({"col1": ["a"], "date_col": ["01/15/2024"]})

        # WHEN the date columns are converted THEN a ValueError is raised
        with pytest.raises(
            ValueError,
            match="The CSV file should have a header row to convert date columns to epoch time.",
        ):
            _convert_csv_date_cols_to_epoch_time(
                df=df,
                csv_table_descriptor=CsvTableDescriptor(is_first_line_header=False),
            )


class TestTableStoreRowMixin:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @dataclass
    class ClassForTest(TableStoreRowMixin, TableStoreMixin):
        id: Optional[str] = None
        name: Optional[str] = None
        _last_persistent_instance: Optional[Any] = None

    async def test_store_rows_async_converts_datetime_cols_to_epoch_in_dataframe(self):
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame(
            {
                "col1": ["a", "b"],
                "date_col": pd.to_datetime([1487071391024, None], unit="ms", utc=True),
            }
        )

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(values=df, synapse_client=self.syn)

        uploaded_df = mock_upload.call_args.kwargs["df"]
        expected_df = pd.DataFrame(
            {"col1": ["a", "b"], "date_col": [1487071391024, None]}
        ).convert_dtypes()
        pd.testing.assert_frame_equal(uploaded_df, expected_df, check_dtype=False)
        assert is_integer_dtype(uploaded_df["date_col"])

    async def test_store_rows_async_dict_input_is_converted_to_dataframe(self):
        # GIVEN a plain dict of columns rather than a DataFrame, including a
        # datetime column
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        values = {
            "col1": ["a", "b"],
            "col2": [1, 2],
            "date_col": pd.to_datetime([1487071391024, None], unit="ms", utc=True),
        }

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(values=values, synapse_client=self.syn)

        # THEN it is converted to a DataFrame before reaching the upload step,
        # and the datetime column is converted to epoch ms just as it would be
        # for a DataFrame passed in directly
        uploaded_df = mock_upload.call_args.kwargs["df"]
        expected_df = pd.DataFrame(
            {
                "col1": ["a", "b"],
                "col2": [1, 2],
                "date_col": [1487071391024, None],
            }
        ).convert_dtypes()
        pd.testing.assert_frame_equal(uploaded_df, expected_df, check_dtype=False)
        assert is_integer_dtype(uploaded_df["date_col"])

    async def test_store_rows_async_dataframe_defaults_to_csv_kwargs_escapechar(self):
        # GIVEN a dataframe stored without explicit to_csv_kwargs
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(values=df, synapse_client=self.syn)

        # THEN the default escapechar is used
        assert mock_upload.call_args.kwargs["to_csv_kwargs"] == {"escapechar": "\\"}

    async def test_store_rows_async_dataframe_merges_to_csv_kwargs_with_default(self):
        # GIVEN to_csv_kwargs that don't override escapechar
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(
                values=df, to_csv_kwargs={"sep": ";"}, synapse_client=self.syn
            )

        # THEN the caller's kwargs and the default escapechar are both present
        assert mock_upload.call_args.kwargs["to_csv_kwargs"] == {
            "escapechar": "\\",
            "sep": ";",
        }

    async def test_store_rows_async_dataframe_to_csv_kwargs_can_override_escapechar(
        self,
    ):
        # GIVEN to_csv_kwargs that explicitly override the default escapechar
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(
                values=df, to_csv_kwargs={"escapechar": "|"}, synapse_client=self.syn
            )

        assert mock_upload.call_args.kwargs["to_csv_kwargs"] == {"escapechar": "|"}

    async def test_store_rows_async_dataframe_forwards_additional_changes(self):
        # GIVEN additional_changes passed alongside a dataframe
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})
        additional_change = MagicMock(name="additional_change")

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(
                values=df,
                additional_changes=[additional_change],
                synapse_client=self.syn,
            )

        # THEN they are forwarded to the upload step unchanged
        assert mock_upload.call_args.kwargs["additional_changes"] == [additional_change]

    async def test_store_rows_async_dataframe_forwards_insert_size_and_timeout(self):
        # GIVEN a custom insert_size_bytes and job_timeout
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(
                values=df,
                insert_size_bytes=123,
                job_timeout=45,
                synapse_client=self.syn,
            )

        # THEN both are forwarded to the upload step
        assert mock_upload.call_args.kwargs["insert_size_bytes"] == 123
        assert mock_upload.call_args.kwargs["job_timeout"] == 45

    async def test_store_rows_async_dry_run_with_dataframe_does_not_upload(self):
        # GIVEN dry_run=True with a dataframe
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})

        with patch.object(
            table, "_chunk_and_upload_df", new_callable=AsyncMock
        ) as mock_upload:
            await table.store_rows_async(
                values=df, dry_run=True, synapse_client=self.syn
            )

        # THEN no data is actually uploaded
        mock_upload.assert_not_called()

    async def test_store_rows_async_dataframe_raises_without_id(self):
        # GIVEN a table with no id and no way to resolve one from Synapse
        table = self.ClassForTest(id=None, name="test_table")
        df = pd.DataFrame({"col1": ["a", "b"]})

        with (
            patch(GET_ID_PATCH, return_value=None),
            patch.object(
                table, "_chunk_and_upload_df", new_callable=AsyncMock
            ) as mock_upload,
        ):
            with pytest.raises(
                ValueError,
                match=(
                    "The table must have an ID to store rows, or the table could "
                    "not be found from the given name/parent_id."
                ),
            ):
                await table.store_rows_async(values=df, synapse_client=self.syn)

        mock_upload.assert_not_called()

    async def test_store_rows_async_dataframe_with_infer_from_data_generates_schema_change(
        self,
    ):
        # GIVEN schema_storage_strategy=INFER_FROM_DATA with a dataframe
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        df = pd.DataFrame({"col1": ["a", "b"]})
        schema_change_request = MagicMock(name="schema_change_request")

        with (
            patch.object(table, "_infer_columns_from_data") as mock_infer_columns,
            patch.object(
                table,
                "_generate_schema_change_request",
                new_callable=AsyncMock,
                return_value=schema_change_request,
            ) as mock_generate_schema_change_request,
            patch.object(
                table, "_chunk_and_upload_df", new_callable=AsyncMock
            ) as mock_upload,
        ):
            await table.store_rows_async(
                values=df,
                schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
                synapse_client=self.syn,
            )

        # THEN the columns are inferred from the data and the resulting schema
        # change request is passed through to the upload step
        mock_infer_columns.assert_called_once()
        mock_generate_schema_change_request.assert_awaited_once()
        assert (
            mock_upload.call_args.kwargs["schema_change_request"]
            == schema_change_request
        )

    async def test_store_rows_async_converts_csv_date_cols_to_epoch_in_csv(self):
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as csv_file:
            csv_file.write("col1,date_col\na,01/15/2024\nb,02/20/2024\n")

        uploaded = {}

        def capture_upload(**kwargs):
            # The temporary file is deleted after the upload, so read it here
            uploaded["path"] = kwargs["path_to_csv"]
            uploaded["df"] = pd.read_csv(kwargs["path_to_csv"])

        try:
            with patch.object(
                table, "_chunk_and_upload_csv", new_callable=AsyncMock
            ) as mock_upload:
                mock_upload.side_effect = capture_upload
                await table.store_rows_async(
                    values=csv_file.name,
                    date_columns=["date_col"],
                    date_format="%m/%d/%Y",
                    synapse_client=self.syn,
                )

            assert uploaded["path"] != csv_file.name
            assert not os.path.exists(uploaded["path"])
            expected_df = pd.DataFrame(
                {
                    "col1": ["a", "b"],
                    "date_col": [1705276800000, 1708387200000],
                }  # date columns are converted to epoch ms (midnight local timezone, unit tests run with TZ=UTC)
            ).convert_dtypes()
            pd.testing.assert_frame_equal(
                uploaded["df"], expected_df, check_dtype=False
            )
        finally:
            os.remove(csv_file.name)

    async def test_store_rows_async_csv_date_cols_forwards_synapse_client(self):
        # GIVEN no cached Synapse client is available — matching integration
        # tests, which run with Synapse.allow_client_caching(False) and never
        # call Synapse.set_client, so only an explicitly passed client works
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as csv_file:
            csv_file.write("col1,date_col\na,01/15/2024\n")

        cached_client = Synapse._synapse_client
        Synapse._synapse_client = None
        try:
            with patch.object(table, "_chunk_and_upload_csv", new_callable=AsyncMock):
                # THEN parsing date_columns must use the explicitly passed
                # client rather than raising for a missing cached instance
                await table.store_rows_async(
                    values=csv_file.name,
                    date_columns=["date_col"],
                    date_format="%m/%d/%Y",
                    synapse_client=self.syn,
                )
        finally:
            Synapse._synapse_client = cached_client
            os.remove(csv_file.name)

    async def test_store_rows_async_csv_date_cols_respects_non_default_separator(self):
        # GIVEN a TAB-separated CSV and a matching csv_table_descriptor telling
        # the client the file is tab-delimited
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as csv_file:
            csv_file.write("col1\tdate_col\na\t01/15/2024\nb\t02/20/2024\n")

        uploaded = {}

        def capture_upload(**kwargs):
            # The temp upload file is written with the descriptor's separator,
            # so read it back with the same separator
            uploaded["df"] = pd.read_csv(kwargs["path_to_csv"], sep="\t")

        try:
            with patch.object(
                table, "_chunk_and_upload_csv", new_callable=AsyncMock
            ) as mock_upload:
                mock_upload.side_effect = capture_upload
                # WHEN the tab-delimited file is stored with date_columns,
                # passing `sep="\t"` via read_csv_kwargs so the file is
                # parsed with the same separator used by csv_table_descriptor
                await table.store_rows_async(
                    values=csv_file.name,
                    date_columns=["date_col"],
                    date_format="%m/%d/%Y",
                    csv_table_descriptor=CsvTableDescriptor(separator="\t"),
                    read_csv_kwargs={"sep": "\t"},
                    synapse_client=self.syn,
                )

            expected_df = pd.DataFrame(
                {
                    "col1": ["a", "b"],
                    "date_col": [
                        1705276800000,
                        1708387200000,
                    ],  # date columns are converted to epoch ms (midnight local timezone, unit tests run with TZ=UTC)
                }
            ).convert_dtypes()
            pd.testing.assert_frame_equal(
                uploaded["df"], expected_df, check_dtype=False
            )
        finally:
            os.remove(csv_file.name)

    async def test_store_rows_async_keeps_row_id_and_version_with_date_cols_in_csv(
        self,
    ):
        table = self.ClassForTest(id="syn123", name="test_table")
        table._last_persistent_instance = self.ClassForTest(
            id="syn123", name="test_table"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as csv_file:
            csv_file.write(
                "ROW_ID,ROW_VERSION,col1,date_col\n"
                "1,1,a,01/15/2024\n"
                "2,1,b,02/20/2024\n"
            )

        uploaded = {}

        def capture_upload(**kwargs):
            uploaded["df"] = pd.read_csv(kwargs["path_to_csv"])

        try:
            with patch.object(
                table, "_chunk_and_upload_csv", new_callable=AsyncMock
            ) as mock_upload:
                mock_upload.side_effect = capture_upload
                await table.store_rows_async(
                    values=csv_file.name,
                    date_columns=["date_col"],
                    date_format="%m/%d/%Y",
                    synapse_client=self.syn,
                )

            expected_df = pd.DataFrame(
                {
                    "ROW_ID": [1, 2],
                    "ROW_VERSION": [1, 1],
                    "col1": ["a", "b"],
                    "date_col": [
                        1705276800000,
                        1708387200000,
                    ],  # date columns are converted to epoch ms (midnight local timezone, unit tests run with TZ=UTC)
                }
            ).convert_dtypes()
            pd.testing.assert_frame_equal(
                uploaded["df"], expected_df, check_dtype=False
            )
        finally:
            os.remove(csv_file.name)
