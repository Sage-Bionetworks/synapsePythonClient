"""Unit tests for the CurationTask and Grid models."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import (
    FILE_BASED_METADATA_TASK_PROPERTIES,
    GRID_CSV_IMPORT_REQUEST,
    RECORD_BASED_METADATA_TASK_PROPERTIES,
    UPLOAD_TO_TABLE_PREVIEW_REQUEST,
)
from synapseclient.models.curation import (
    CreateGridRequest,
    CurationTask,
    FileBasedMetadataTaskProperties,
    Grid,
    GridCsvImportRequest,
    GridRecordSetExportRequest,
    RecordBasedMetadataTaskProperties,
    UploadToTablePreviewRequest,
    _create_task_properties_from_dict,
)
from synapseclient.models.recordset import ValidationSummary
from synapseclient.models.table_components import Column, CsvTableDescriptor

TASK_ID = 42
TASK_ID_2 = 99
DATA_TYPE = "genomics_data"
PROJECT_ID = "syn9876543"
INSTRUCTIONS = "Upload your genomics files"
ETAG = "etag-abc-123"
CREATED_ON = "2024-01-01T00:00:00.000Z"
MODIFIED_ON = "2024-01-02T00:00:00.000Z"
CREATED_BY = "111111"
MODIFIED_BY = "222222"
ASSIGNEE_PRINCIPAL_ID = "333333"
UPLOAD_FOLDER_ID = "syn1234567"
FILE_VIEW_ID = "syn2345678"
RECORD_SET_ID = "syn3456789"
SESSION_ID = "session-abc-123"
SOURCE_ENTITY_ID = "syn5555555"
GRID_ETAG = "grid-etag-456"
STARTED_BY = "user-1"
STARTED_ON = "2024-03-01T00:00:00.000Z"
FILE_HANDLE_ID = "1234567"


def _get_file_based_task_api_response():
    """Return a mock CurationTask API response with file-based properties."""
    return {
        "taskId": TASK_ID,
        "dataType": DATA_TYPE,
        "projectId": PROJECT_ID,
        "instructions": INSTRUCTIONS,
        "etag": ETAG,
        "createdOn": CREATED_ON,
        "modifiedOn": MODIFIED_ON,
        "createdBy": CREATED_BY,
        "modifiedBy": MODIFIED_BY,
        "assigneePrincipalId": ASSIGNEE_PRINCIPAL_ID,
        "taskProperties": {
            "concreteType": FILE_BASED_METADATA_TASK_PROPERTIES,
            "uploadFolderId": UPLOAD_FOLDER_ID,
            "fileViewId": FILE_VIEW_ID,
        },
    }


def _get_record_based_task_api_response():
    """Return a mock CurationTask API response with record-based properties."""
    return {
        "taskId": TASK_ID,
        "dataType": DATA_TYPE,
        "projectId": PROJECT_ID,
        "instructions": INSTRUCTIONS,
        "etag": ETAG,
        "createdOn": CREATED_ON,
        "modifiedOn": MODIFIED_ON,
        "createdBy": CREATED_BY,
        "modifiedBy": MODIFIED_BY,
        "assigneePrincipalId": None,
        "taskProperties": {
            "concreteType": RECORD_BASED_METADATA_TASK_PROPERTIES,
            "recordSetId": RECORD_SET_ID,
        },
    }


def _get_grid_session_response():
    """Return a mock grid session API response."""
    return {
        "sessionId": SESSION_ID,
        "startedBy": STARTED_BY,
        "startedOn": STARTED_ON,
        "etag": GRID_ETAG,
        "modifiedOn": MODIFIED_ON,
        "lastReplicaIdClient": 10,
        "lastReplicaIdService": -5,
        "gridJsonSchema$Id": "my-schema-id",
        "sourceEntityId": SOURCE_ENTITY_ID,
    }


class TestFileBasedMetadataTaskProperties:
    """Tests for the FileBasedMetadataTaskProperties dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response dict with file-based metadata task properties
        response = {
            "uploadFolderId": UPLOAD_FOLDER_ID,
            "fileViewId": FILE_VIEW_ID,
        }

        # WHEN I fill a FileBasedMetadataTaskProperties from the dict
        props = FileBasedMetadataTaskProperties()
        props.fill_from_dict(response)

        # THEN the properties should be populated correctly
        assert props.upload_folder_id == UPLOAD_FOLDER_ID
        assert props.file_view_id == FILE_VIEW_ID

    def test_to_synapse_request(self) -> None:
        # GIVEN a FileBasedMetadataTaskProperties object
        props = FileBasedMetadataTaskProperties(
            upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=FILE_VIEW_ID
        )

        # WHEN I convert it to a request dict
        request = props.to_synapse_request()

        # THEN the request should contain the correct values
        assert request["concreteType"] == FILE_BASED_METADATA_TASK_PROPERTIES
        assert request["uploadFolderId"] == UPLOAD_FOLDER_ID
        assert request["fileViewId"] == FILE_VIEW_ID

    def test_to_synapse_request_none_values(self) -> None:
        # GIVEN a FileBasedMetadataTaskProperties with no values
        props = FileBasedMetadataTaskProperties()

        # WHEN I convert it to a request dict
        request = props.to_synapse_request()

        # THEN the request should only contain concreteType
        assert request == {"concreteType": FILE_BASED_METADATA_TASK_PROPERTIES}


class TestRecordBasedMetadataTaskProperties:
    """Tests for the RecordBasedMetadataTaskProperties dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response dict with record-based metadata task properties
        response = {"recordSetId": RECORD_SET_ID}

        # WHEN I fill a RecordBasedMetadataTaskProperties from the dict
        props = RecordBasedMetadataTaskProperties()
        props.fill_from_dict(response)

        # THEN the record_set_id should be populated
        assert props.record_set_id == RECORD_SET_ID

    def test_to_synapse_request(self) -> None:
        # GIVEN a RecordBasedMetadataTaskProperties object
        props = RecordBasedMetadataTaskProperties(record_set_id=RECORD_SET_ID)

        # WHEN I convert it to a request dict
        request = props.to_synapse_request()

        # THEN the request should contain the correct values
        assert request["concreteType"] == RECORD_BASED_METADATA_TASK_PROPERTIES
        assert request["recordSetId"] == RECORD_SET_ID


class TestCreateTaskPropertiesFromDict:
    """Tests for the _create_task_properties_from_dict factory function."""

    def test_file_based_properties(self) -> None:
        # GIVEN a dict with file-based concrete type
        data = {
            "concreteType": FILE_BASED_METADATA_TASK_PROPERTIES,
            "uploadFolderId": UPLOAD_FOLDER_ID,
            "fileViewId": FILE_VIEW_ID,
        }

        # WHEN I create task properties from the dict
        result = _create_task_properties_from_dict(data)

        # THEN it should be a FileBasedMetadataTaskProperties
        assert isinstance(result, FileBasedMetadataTaskProperties)
        assert result.upload_folder_id == UPLOAD_FOLDER_ID
        assert result.file_view_id == FILE_VIEW_ID

    def test_record_based_properties(self) -> None:
        # GIVEN a dict with record-based concrete type
        data = {
            "concreteType": RECORD_BASED_METADATA_TASK_PROPERTIES,
            "recordSetId": RECORD_SET_ID,
        }

        # WHEN I create task properties from the dict
        result = _create_task_properties_from_dict(data)

        # THEN it should be a RecordBasedMetadataTaskProperties
        assert isinstance(result, RecordBasedMetadataTaskProperties)
        assert result.record_set_id == RECORD_SET_ID

    def test_unknown_concrete_type_raises_error(self) -> None:
        # GIVEN a dict with an unknown concrete type
        data = {"concreteType": "org.sagebionetworks.Unknown"}

        # WHEN I attempt to create task properties
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="Unknown concreteType"):
            _create_task_properties_from_dict(data)


class TestCurationTask:
    """Unit tests for the CurationTask model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict_file_based(self) -> None:
        # GIVEN a CurationTask API response with file-based properties
        response = _get_file_based_task_api_response()

        # WHEN I fill a CurationTask from the response
        task = CurationTask()
        task.fill_from_dict(response)

        # THEN all fields should be populated correctly
        assert task.task_id == TASK_ID
        assert task.data_type == DATA_TYPE
        assert task.project_id == PROJECT_ID
        assert task.instructions == INSTRUCTIONS
        assert task.etag == ETAG
        assert task.created_on == CREATED_ON
        assert task.modified_on == MODIFIED_ON
        assert task.created_by == CREATED_BY
        assert task.modified_by == MODIFIED_BY
        assert task.assignee_principal_id == ASSIGNEE_PRINCIPAL_ID
        assert isinstance(task.task_properties, FileBasedMetadataTaskProperties)
        assert task.task_properties.upload_folder_id == UPLOAD_FOLDER_ID
        assert task.task_properties.file_view_id == FILE_VIEW_ID

    def test_fill_from_dict_record_based(self) -> None:
        # GIVEN a CurationTask API response with record-based properties
        response = _get_record_based_task_api_response()

        # WHEN I fill a CurationTask from the response
        task = CurationTask()
        task.fill_from_dict(response)

        # THEN the task_properties should be RecordBasedMetadataTaskProperties
        assert isinstance(task.task_properties, RecordBasedMetadataTaskProperties)
        assert task.task_properties.record_set_id == RECORD_SET_ID

    def test_to_synapse_request(self) -> None:
        # GIVEN a CurationTask with all fields set
        task = CurationTask(
            task_id=TASK_ID,
            data_type=DATA_TYPE,
            project_id=PROJECT_ID,
            instructions=INSTRUCTIONS,
            etag=ETAG,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=FILE_VIEW_ID
            ),
        )

        # WHEN I convert it to a Synapse request
        request = task.to_synapse_request()

        # THEN the request should contain the correct values
        assert request["taskId"] == TASK_ID
        assert request["dataType"] == DATA_TYPE
        assert request["projectId"] == PROJECT_ID
        assert request["instructions"] == INSTRUCTIONS
        assert request["etag"] == ETAG
        assert (
            request["taskProperties"]["concreteType"]
            == FILE_BASED_METADATA_TASK_PROPERTIES
        )
        assert request["taskProperties"]["uploadFolderId"] == UPLOAD_FOLDER_ID

    def test_has_changed_true_initially(self) -> None:
        # GIVEN a new CurationTask
        task = CurationTask(task_id=TASK_ID, data_type=DATA_TYPE)

        # WHEN I check has_changed before any persistent instance
        # THEN it should be True
        assert task.has_changed is True

    def test_has_changed_false_after_set(self) -> None:
        # GIVEN a CurationTask with a persistent instance set
        task = CurationTask(task_id=TASK_ID, data_type=DATA_TYPE)
        task._set_last_persistent_instance()

        # WHEN I check has_changed without modifying
        # THEN it should be False
        assert task.has_changed is False

    def test_has_changed_true_after_modification(self) -> None:
        # GIVEN a CurationTask with a persistent instance set
        task = CurationTask(task_id=TASK_ID, data_type=DATA_TYPE)
        task._set_last_persistent_instance()

        # WHEN I modify the task
        task.instructions = "new instructions"

        # THEN has_changed should be True
        assert task.has_changed is True

    async def test_get_async(self) -> None:
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # WHEN I call get_async
        with patch(
            "synapseclient.models.curation.get_curation_task",
            new_callable=AsyncMock,
            return_value=_get_file_based_task_api_response(),
        ) as mock_get:
            result = await task.get_async(synapse_client=self.syn)

            # THEN the API should be called with the task_id
            mock_get.assert_called_once_with(task_id=TASK_ID, synapse_client=self.syn)

            # AND the result should be populated
            assert result.task_id == TASK_ID
            assert result.data_type == DATA_TYPE
            assert result.project_id == PROJECT_ID
            assert result.instructions == INSTRUCTIONS
            assert isinstance(result.task_properties, FileBasedMetadataTaskProperties)

    async def test_get_async_without_task_id(self) -> None:
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call get_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="task_id is required to get"):
            await task.get_async(synapse_client=self.syn)

    async def test_delete_async(self) -> None:
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # WHEN I call delete_async
        with patch(
            "synapseclient.models.curation.delete_curation_task",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_delete:
            await task.delete_async(synapse_client=self.syn)

            # THEN the API should be called with the task_id
            mock_delete.assert_called_once_with(
                task_id=TASK_ID, synapse_client=self.syn
            )

    async def test_delete_async_without_task_id(self) -> None:
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call delete_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="task_id is required to delete"):
            await task.delete_async(synapse_client=self.syn)

    async def test_store_async_create_new_task(self) -> None:
        # GIVEN a new CurationTask with all required create fields
        file_props = FileBasedMetadataTaskProperties(
            upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=FILE_VIEW_ID
        )
        task = CurationTask(
            project_id=PROJECT_ID,
            data_type=DATA_TYPE,
            instructions=INSTRUCTIONS,
            task_properties=file_props,
        )

        # WHEN I call store_async and no existing task is found
        async def empty_list_gen(*args, **kwargs):
            return
            yield  # pragma: no cover

        with (
            patch(
                "synapseclient.models.curation.list_curation_tasks",
                return_value=empty_list_gen(),
            ),
            patch(
                "synapseclient.models.curation.create_curation_task",
                new_callable=AsyncMock,
                return_value=_get_file_based_task_api_response(),
            ) as mock_create,
        ):
            result = await task.store_async(synapse_client=self.syn)

            # THEN the create API should be called
            mock_create.assert_called_once()

            # AND the result should be populated with the response
            assert result.task_id == TASK_ID
            assert result.data_type == DATA_TYPE
            assert result.project_id == PROJECT_ID

    async def test_store_async_update_with_task_id(self) -> None:
        # GIVEN a CurationTask with a task_id (already persisted)
        task = CurationTask(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            data_type=DATA_TYPE,
            instructions="Updated instructions",
            etag=ETAG,
        )

        # Capture what to_synapse_request returns before the call
        expected_request = task.to_synapse_request()

        # WHEN I call store_async
        with patch(
            "synapseclient.models.curation.update_curation_task",
            new_callable=AsyncMock,
            return_value=_get_file_based_task_api_response(),
        ) as mock_update:
            result = await task.store_async(synapse_client=self.syn)

            # THEN the update API should be called with the task_id
            mock_update.assert_called_once_with(
                task_id=TASK_ID,
                curation_task=expected_request,
                synapse_client=self.syn,
            )

            # AND the result should be populated from the response
            assert result.task_id == TASK_ID
            assert result.data_type == DATA_TYPE

    async def test_store_async_merge_existing(self) -> None:
        # GIVEN a CurationTask that matches an existing task by project_id and data_type
        task = CurationTask(
            project_id=PROJECT_ID,
            data_type=DATA_TYPE,
            instructions="New instructions only",
        )

        existing_response = _get_file_based_task_api_response()

        # Mock list_curation_tasks to return the existing task
        async def mock_list(*args, **kwargs):
            yield existing_response

        # WHEN I call store_async
        with (
            patch(
                "synapseclient.models.curation.list_curation_tasks",
                return_value=mock_list(),
            ),
            patch(
                "synapseclient.models.curation.get_curation_task",
                new_callable=AsyncMock,
                return_value=existing_response,
            ),
            patch(
                "synapseclient.models.curation.update_curation_task",
                new_callable=AsyncMock,
                return_value=existing_response,
            ) as mock_update,
        ):
            result = await task.store_async(synapse_client=self.syn)

            # THEN it should have merged the existing task and done an update
            mock_update.assert_called_once()

            # AND the result should reflect the merged state
            assert result.task_id == TASK_ID

    async def test_store_async_no_project_id_raises(self) -> None:
        # GIVEN a CurationTask without a project_id
        task = CurationTask(data_type=DATA_TYPE)

        # WHEN I call store_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="project_id is required"):
            await task.store_async(synapse_client=self.syn)

    async def test_store_async_no_data_type_raises(self) -> None:
        # GIVEN a CurationTask without a data_type
        task = CurationTask(project_id=PROJECT_ID)

        # WHEN I call store_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="data_type is required"):
            await task.store_async(synapse_client=self.syn)

    async def test_store_async_create_missing_instructions_raises(self) -> None:
        # GIVEN a CurationTask without instructions (and no existing match)
        task = CurationTask(
            project_id=PROJECT_ID,
            data_type=DATA_TYPE,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID
            ),
        )

        async def empty_list_gen(*args, **kwargs):
            return
            yield  # pragma: no cover

        # WHEN I call store_async
        # THEN it should raise ValueError for missing instructions
        with patch(
            "synapseclient.models.curation.list_curation_tasks",
            return_value=empty_list_gen(),
        ):
            with pytest.raises(ValueError, match="instructions is required"):
                await task.store_async(synapse_client=self.syn)

    async def test_store_async_create_missing_task_properties_raises(self) -> None:
        # GIVEN a CurationTask without task_properties (and no existing match)
        task = CurationTask(
            project_id=PROJECT_ID,
            data_type=DATA_TYPE,
            instructions=INSTRUCTIONS,
        )

        async def empty_list_gen(*args, **kwargs):
            return
            yield  # pragma: no cover

        # WHEN I call store_async
        # THEN it should raise ValueError for missing task_properties
        with patch(
            "synapseclient.models.curation.list_curation_tasks",
            return_value=empty_list_gen(),
        ):
            with pytest.raises(ValueError, match="task_properties is required"):
                await task.store_async(synapse_client=self.syn)

    async def test_list_async(self) -> None:
        # GIVEN mock API responses for two tasks
        task_response_1 = _get_file_based_task_api_response()
        task_response_2 = _get_record_based_task_api_response()
        task_response_2["taskId"] = TASK_ID_2

        async def mock_list(*args, **kwargs):
            yield task_response_1
            yield task_response_2

        # WHEN I call list_async
        with patch(
            "synapseclient.models.curation.list_curation_tasks",
            return_value=mock_list(),
        ):
            results = []
            async for task in CurationTask.list_async(
                project_id=PROJECT_ID, synapse_client=self.syn
            ):
                results.append(task)

            # THEN I should get two CurationTask objects
            assert len(results) == 2
            assert results[0].task_id == TASK_ID
            assert results[0].data_type == DATA_TYPE
            assert isinstance(
                results[0].task_properties, FileBasedMetadataTaskProperties
            )
            assert results[1].task_id == TASK_ID_2
            assert isinstance(
                results[1].task_properties, RecordBasedMetadataTaskProperties
            )


class TestGrid:
    """Unit tests for the Grid model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self) -> None:
        # GIVEN a grid session API response
        response = _get_grid_session_response()

        # WHEN I fill a Grid from the response
        grid = Grid()
        grid.fill_from_dict(response)

        # THEN all fields should be populated correctly
        assert grid.session_id == SESSION_ID
        assert grid.started_by == STARTED_BY
        assert grid.started_on == STARTED_ON
        assert grid.etag == GRID_ETAG
        assert grid.modified_on == MODIFIED_ON
        assert grid.last_replica_id_client == 10
        assert grid.last_replica_id_service == -5
        assert grid.grid_json_schema_id == "my-schema-id"
        assert grid.source_entity_id == SOURCE_ENTITY_ID

    async def test_create_async_with_record_set_id(self) -> None:
        # GIVEN a Grid with a record_set_id
        grid = Grid(record_set_id=RECORD_SET_ID)

        # Mock the CreateGridRequest's send_job_and_wait_async
        mock_create_request = CreateGridRequest(record_set_id=RECORD_SET_ID)
        mock_create_request.session_id = SESSION_ID
        mock_create_request._grid_session_data = _get_grid_session_response()

        # WHEN I call create_async
        with patch.object(
            CreateGridRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_create_request,
        ):
            result = await grid.create_async(synapse_client=self.syn)

            # THEN the grid should be populated with session data
            assert result.session_id == SESSION_ID
            assert result.started_by == STARTED_BY
            assert result.started_on == STARTED_ON
            assert result.source_entity_id == SOURCE_ENTITY_ID

    async def test_create_async_no_record_set_or_query_raises(self) -> None:
        # GIVEN a Grid with neither record_set_id nor initial_query
        grid = Grid()

        # WHEN I call create_async
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="record_set_id or initial_query is required"
        ):
            await grid.create_async(synapse_client=self.syn)

    async def test_create_async_attach_to_previous_session(self) -> None:
        # GIVEN a Grid with a record_set_id
        grid = Grid(record_set_id=RECORD_SET_ID)

        # Mock list_async to return an existing session
        existing_grid = Grid()
        existing_grid.fill_from_dict(_get_grid_session_response())

        async def mock_list_async(*args, **kwargs):
            yield existing_grid

        # WHEN I call create_async with attach_to_previous_session=True
        with patch.object(
            Grid,
            "list_async",
            return_value=mock_list_async(),
        ):
            result = await grid.create_async(
                attach_to_previous_session=True, synapse_client=self.syn
            )

            # THEN the grid should attach to the existing session
            assert result.session_id == SESSION_ID
            assert result.started_by == STARTED_BY
            assert result.source_entity_id == SOURCE_ENTITY_ID

    async def test_create_async_attach_to_previous_no_existing(self) -> None:
        # GIVEN a Grid with a record_set_id
        grid = Grid(record_set_id=RECORD_SET_ID)

        # Mock list_async to return no existing sessions
        async def mock_list_async(*args, **kwargs):
            return
            yield  # pragma: no cover

        mock_create_request = CreateGridRequest(record_set_id=RECORD_SET_ID)
        mock_create_request.session_id = SESSION_ID
        mock_create_request._grid_session_data = _get_grid_session_response()

        # WHEN I call create_async with attach_to_previous_session=True and no
        # existing sessions
        with (
            patch.object(
                Grid,
                "list_async",
                return_value=mock_list_async(),
            ),
            patch.object(
                CreateGridRequest,
                "send_job_and_wait_async",
                new_callable=AsyncMock,
                return_value=mock_create_request,
            ),
        ):
            result = await grid.create_async(
                attach_to_previous_session=True, synapse_client=self.syn
            )

            # THEN a new grid session should be created
            assert result.session_id == SESSION_ID

    async def test_export_to_record_set_async(self) -> None:
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        mock_export_result = GridRecordSetExportRequest(session_id=SESSION_ID)
        mock_export_result.response_record_set_id = RECORD_SET_ID
        mock_export_result.record_set_version_number = 3
        mock_export_result.validation_summary_statistics = ValidationSummary(
            container_id="syn111",
            total_number_of_children=10,
            number_of_valid_children=8,
            number_of_invalid_children=1,
            number_of_unknown_children=1,
        )

        # WHEN I call export_to_record_set_async
        with patch.object(
            GridRecordSetExportRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_export_result,
        ):
            result = await grid.export_to_record_set_async(synapse_client=self.syn)

            # THEN the export result should be populated
            assert result.record_set_id == RECORD_SET_ID
            assert result.record_set_version_number == 3
            assert result.validation_summary_statistics.number_of_valid_children == 8

    async def test_export_to_record_set_async_without_session_id_raises(self) -> None:
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call export_to_record_set_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="session_id is required to export"):
            await grid.export_to_record_set_async(synapse_client=self.syn)

    async def test_delete_async(self) -> None:
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        # WHEN I call delete_async
        with patch(
            "synapseclient.models.curation.delete_grid_session",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_delete:
            await grid.delete_async(synapse_client=self.syn)

            # THEN the API should be called with the session_id
            mock_delete.assert_called_once_with(
                session_id=SESSION_ID, synapse_client=self.syn
            )

    async def test_delete_async_without_session_id_raises(self) -> None:
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call delete_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="session_id is required to delete"):
            await grid.delete_async(synapse_client=self.syn)

    async def test_list_async(self) -> None:
        # GIVEN mock API responses for grid sessions
        session_1 = _get_grid_session_response()
        session_2 = {
            "sessionId": "session-xyz-999",
            "startedBy": "user-2",
            "startedOn": "2024-04-01T00:00:00.000Z",
            "etag": "etag-2",
            "modifiedOn": "2024-04-02T00:00:00.000Z",
            "lastReplicaIdClient": 20,
            "lastReplicaIdService": -10,
            "gridJsonSchema$Id": None,
            "sourceEntityId": "syn6666666",
        }

        async def mock_list(*args, **kwargs):
            yield session_1
            yield session_2

        # WHEN I call list_async
        with patch(
            "synapseclient.models.curation.list_grid_sessions",
            return_value=mock_list(),
        ):
            results = []
            async for grid in Grid.list_async(synapse_client=self.syn):
                results.append(grid)

            # THEN I should get two Grid objects
            assert len(results) == 2
            assert results[0].session_id == SESSION_ID
            assert results[0].source_entity_id == SOURCE_ENTITY_ID
            assert results[1].session_id == "session-xyz-999"
            assert results[1].source_entity_id == "syn6666666"

    async def test_list_async_with_source_id(self) -> None:
        # GIVEN mock API responses filtered by source_id
        session_1 = _get_grid_session_response()

        async def mock_list(*args, **kwargs):
            yield session_1

        # WHEN I call list_async with a source_id
        with patch(
            "synapseclient.models.curation.list_grid_sessions",
            return_value=mock_list(),
        ):
            results = []
            async for grid in Grid.list_async(
                source_id=RECORD_SET_ID, synapse_client=self.syn
            ):
                results.append(grid)

            # THEN I should get the matching grid session
            assert len(results) == 1
            assert results[0].session_id == SESSION_ID

    async def test_import_csv_async_without_session_id(self) -> None:
        """Test that calling import_csv_async without a session_id raises a ValueError."""
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call import_csv_async
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="session_id is required to import a CSV into a GridSession",
        ):
            await grid.import_csv_async(
                synapse_client=self.syn, file_handle_id=FILE_HANDLE_ID
            )

    async def test_import_csv_async(self) -> None:
        """Test the import_csv_async method of the Grid class, ensuring it correctly calls the preview and import requests and logs the results."""
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        csv_table_descriptor = CsvTableDescriptor(
            separator=",",
            quote_character='"',
            escape_character="\\",
            line_end=os.linesep,
            is_first_line_header=True,
        )
        expected_columns = [Column(name="col1", column_type="STRING", maximum_size=50)]

        # Mock preview response with suggested columns
        mock_preview_response = UploadToTablePreviewRequest(
            csv_table_descriptor=csv_table_descriptor,
            suggested_columns=expected_columns,
            sample_rows=[["value1"]],
            rows_scanned=1,
        )
        # Mock import response with row counts
        mock_import_response = GridCsvImportRequest(
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            total_count=1,
            created_count=1,
            updated_count=1,
        )

        mock_preview_instance = MagicMock()
        mock_preview_instance.send_job_and_wait_async = AsyncMock(
            return_value=mock_preview_response
        )

        mock_import_instance = MagicMock()
        mock_import_instance.send_job_and_wait_async = AsyncMock(
            return_value=mock_import_response
        )

        # WHEN I call import_csv_async
        with (
            patch(
                "synapseclient.models.curation.UploadToTablePreviewRequest",
                return_value=mock_preview_instance,
            ) as MockPreview,
            patch(
                "synapseclient.models.curation.GridCsvImportRequest",
                return_value=mock_import_instance,
            ) as MockImport,
            patch.object(self.syn, "logger") as mock_logger,
        ):
            result = await grid.import_csv_async(
                synapse_client=self.syn,
                file_handle_id=FILE_HANDLE_ID,
                csv_table_descriptor=csv_table_descriptor,
            )

        # THEN the grid is returned with the same session
        assert result.session_id == SESSION_ID

        # AND UploadToTablePreviewRequest was constructed with the right arguments
        MockPreview.assert_called_once_with(
            csv_table_descriptor=csv_table_descriptor,
            upload_file_handle_id=FILE_HANDLE_ID,
        )

        # AND GridCsvImportRequest was constructed with the schema from the preview
        MockImport.assert_called_once_with(
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            schema=expected_columns,
        )

        # AND the log message contains the import counts
        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "total count: 1" in log_message
        assert "total created: 1" in log_message
        assert "total updated: 1" in log_message


class TestCreateGridRequest:
    """Tests for the CreateGridRequest helper dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with grid session data
        response = {"gridSession": _get_grid_session_response()}

        # WHEN I fill a CreateGridRequest from the response
        request = CreateGridRequest(record_set_id=RECORD_SET_ID)
        request.fill_from_dict(response)

        # THEN the session_id should be populated
        assert request.session_id == SESSION_ID

    def test_fill_grid_session_from_response(self) -> None:
        # GIVEN a CreateGridRequest with stored grid session data
        response = {"gridSession": _get_grid_session_response()}
        request = CreateGridRequest(record_set_id=RECORD_SET_ID)
        request.fill_from_dict(response)

        # WHEN I fill a Grid from the stored data
        grid = Grid()
        request.fill_grid_session_from_response(grid)

        # THEN the Grid should be populated
        assert grid.session_id == SESSION_ID
        assert grid.started_by == STARTED_BY
        assert grid.etag == GRID_ETAG
        assert grid.source_entity_id == SOURCE_ENTITY_ID

    def test_to_synapse_request_with_record_set_id(self) -> None:
        # GIVEN a CreateGridRequest with a record_set_id
        request = CreateGridRequest(record_set_id=RECORD_SET_ID)

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should contain the correct fields
        assert "concreteType" in result
        assert result["recordSetId"] == RECORD_SET_ID
        assert "initialQuery" not in result


class TestUploadToTablePreviewRequest:
    """Tests for the UploadToTablePreviewRequest helper dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with upload to table preview data
        raw_synapse_response = {
            "jobId": "1234",
            "concreteType": "org.sagebionetworks.repo.model.table.UploadToTablePreviewResult",
            "suggestedColumns": [
                {"name": "etag", "columnType": "STRING", "maximumSize": 50},
                {"name": "Sex", "columnType": "STRING", "maximumSize": 6},
                {"name": "Component", "columnType": "STRING", "maximumSize": 4},
                {"name": "Diagnosis", "columnType": "STRING", "maximumSize": 7},
                {"name": "PatientID", "columnType": "INTEGER"},
                {"name": "CancerType", "columnType": "STRING", "maximumSize": 50},
                {"name": "YearofBirth", "columnType": "STRING", "maximumSize": 50},
                {"name": "FamilyHistory", "columnType": "STRING", "maximumSize": 50},
            ],
            "sampleRows": [
                {"values": [None, "Female", "test", "Healthy", "1", None, None, None]}
            ],
            "rowsScanned": 1,
        }

        # WHEN I fill an UploadToTablePreviewRequest from the response
        preview_req = UploadToTablePreviewRequest()
        preview_response = preview_req.fill_from_dict(raw_synapse_response)

        # THEN the fields should be populated correctly
        assert len(preview_response.suggested_columns) == 8
        assert preview_response.suggested_columns[0] == Column(
            name="etag", column_type="STRING", maximum_size=50
        )
        assert preview_response.suggested_columns[1] == Column(
            name="Sex", column_type="STRING", maximum_size=6
        )
        assert preview_response.suggested_columns[2] == Column(
            name="Component", column_type="STRING", maximum_size=4
        )
        assert preview_response.suggested_columns[3] == Column(
            name="Diagnosis", column_type="STRING", maximum_size=7
        )
        assert preview_response.suggested_columns[4] == Column(
            name="PatientID", column_type="INTEGER", maximum_size=None
        )
        assert preview_response.sample_rows == [
            [None, "Female", "test", "Healthy", "1", None, None, None]
        ]
        assert preview_response.rows_scanned == 1

    def test_to_synapse_request(self) -> None:
        # GIVEN an UploadToTablePreviewRequest
        preview_req = UploadToTablePreviewRequest(
            upload_file_handle_id=FILE_HANDLE_ID,
            lines_to_skip=1,
            do_full_file_scan=True,
            csv_table_descriptor=CsvTableDescriptor(
                separator=";",
                quote_character='"',
                escape_character="\\",
                line_end="\n",
                is_first_line_header=True,
            ),
        )

        # WHEN I convert it to a synapse request
        result = preview_req.to_synapse_request()

        # THEN it should contain the correct fields
        assert result["concreteType"] == UPLOAD_TO_TABLE_PREVIEW_REQUEST
        assert result["uploadFileHandleId"] == FILE_HANDLE_ID
        assert result["linesToSkip"] == 1
        assert result["doFullFileScan"] is True
        assert result["csvTableDescriptor"]["separator"] == ";"
        assert result["csvTableDescriptor"]["quoteCharacter"] == '"'
        assert result["csvTableDescriptor"]["escapeCharacter"] == "\\"
        assert result["csvTableDescriptor"]["lineEnd"] == "\n"
        assert result["csvTableDescriptor"]["isFirstLineHeader"] is True


class TestGridCsvImportRequest:
    """Tests for the GridCsvImportRequest helper dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with grid CSV import data
        raw_synapse_response = {
            "jobId": "1234",
            "concreteType": "org.sagebionetworks.repo.model.grid.GridCsvImportResponse",
            "sessionId": SESSION_ID,
            "totalCount": 3,
            "createdCount": 1,
            "updatedCount": 2,
        }

        # WHEN I fill a GridCsvImportRequest from the response
        import_req = GridCsvImportRequest(
            session_id=SESSION_ID, file_handle_id=FILE_HANDLE_ID
        )
        result = import_req.fill_from_dict(raw_synapse_response)

        # THEN the response fields should be populated correctly
        assert result.session_id == SESSION_ID
        assert result.total_count == 3
        assert result.created_count == 1
        assert result.updated_count == 2

    def test_to_synapse_request(self) -> None:
        # GIVEN a GridCsvImportRequest with all fields set
        import_req = GridCsvImportRequest(
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            csv_descriptor=CsvTableDescriptor(
                separator=",",
                quote_character='"',
                escape_character="\\",
                line_end="\n",
                is_first_line_header=True,
            ),
            schema=[
                Column(name="ROW_ID", column_type="STRING"),
                Column(name="ROW_VERSION", column_type="STRING"),
                Column(name="PatientID", column_type="INTEGER"),
                Column(name="Diagnosis", column_type="STRING"),
            ],
        )

        # WHEN I convert it to a synapse request
        result = import_req.to_synapse_request()

        # THEN it should contain the correct fields
        assert result["concreteType"] == GRID_CSV_IMPORT_REQUEST
        assert result["sessionId"] == SESSION_ID
        assert result["fileHandleId"] == FILE_HANDLE_ID
        assert result["csvDescriptor"]["separator"] == ","
        assert result["csvDescriptor"]["quoteCharacter"] == '"'
        assert result["csvDescriptor"]["escapeCharacter"] == "\\"
        assert result["csvDescriptor"]["lineEnd"] == "\n"
        assert result["csvDescriptor"]["isFirstLineHeader"] is True
        assert len(result["schema"]) == 4
        assert (
            result["schema"][0]
            == Column(name="ROW_ID", column_type="STRING").to_synapse_request()
        )
        assert (
            result["schema"][2]
            == Column(name="PatientID", column_type="INTEGER").to_synapse_request()
        )
        assert (
            result["schema"][3]
            == Column(name="Diagnosis", column_type="STRING").to_synapse_request()
        )


class TestGridRecordSetExportRequest:
    """Tests for the GridRecordSetExportRequest helper dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with export data
        response = {
            "sessionId": SESSION_ID,
            "recordSetId": RECORD_SET_ID,
            "recordSetVersionNumber": 5,
            "validationSummaryStatistics": {
                "containerId": "syn111",
                "totalNumberOfChildren": 10,
                "numberOfValidChildren": 7,
                "numberOfInvalidChildren": 2,
                "numberOfUnknownChildren": 1,
                "generatedOn": "2024-05-01T00:00:00.000Z",
            },
        }

        # WHEN I fill a GridRecordSetExportRequest from the response
        export_req = GridRecordSetExportRequest(session_id=SESSION_ID)
        export_req.fill_from_dict(response)

        # THEN all fields should be populated
        assert export_req.response_session_id == SESSION_ID
        assert export_req.response_record_set_id == RECORD_SET_ID
        assert export_req.record_set_version_number == 5
        assert export_req.validation_summary_statistics.container_id == "syn111"
        assert export_req.validation_summary_statistics.total_number_of_children == 10
        assert export_req.validation_summary_statistics.number_of_valid_children == 7
        assert export_req.validation_summary_statistics.number_of_invalid_children == 2
        assert export_req.validation_summary_statistics.number_of_unknown_children == 1

    def test_fill_from_dict_without_validation_stats(self) -> None:
        # GIVEN a response without validation summary statistics
        response = {
            "sessionId": SESSION_ID,
            "recordSetId": RECORD_SET_ID,
            "recordSetVersionNumber": 1,
        }

        # WHEN I fill a GridRecordSetExportRequest from the response
        export_req = GridRecordSetExportRequest(session_id=SESSION_ID)
        export_req.fill_from_dict(response)

        # THEN the validation_summary_statistics should be None
        assert export_req.response_session_id == SESSION_ID
        assert export_req.validation_summary_statistics is None

    def test_to_synapse_request(self) -> None:
        # GIVEN a GridRecordSetExportRequest
        export_req = GridRecordSetExportRequest(session_id=SESSION_ID)

        # WHEN I convert it to a synapse request
        result = export_req.to_synapse_request()

        # THEN it should contain the correct fields
        assert "concreteType" in result
        assert result["sessionId"] == SESSION_ID
