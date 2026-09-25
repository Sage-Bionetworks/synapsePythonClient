"""Unit tests for the CurationTask and Grid models."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import (
    CELL_VALUE_FILTER,
    COMPUTE_TASK_EXECUTION_REQUEST,
    COUNT_STAR,
    FILE_BASED_METADATA_TASK_PROPERTIES,
    GRID_CSV_IMPORT_REQUEST,
    GRID_EXECUTION_DETAILS,
    GRID_QUERY_JOB_REQUEST,
    RECORD_BASED_METADATA_TASK_PROPERTIES,
    RECORD_SET_GENERATION_EXECUTION_DETAILS,
    RECORD_SET_GENERATION_EXECUTION_PROPERTIES,
    ROW_ID_FILTER,
    ROW_IS_VALID_FILTER,
    ROW_SELECTION_FILTER,
    ROW_VALIDATION_RESULT_FILTER,
    SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS,
    SAMPLE_SHEET_GENERATION_EXECUTION_PROPERTIES,
    SELECT_ALL,
    SELECT_BY_NAME,
    SELECT_SELECTION,
    UPLOAD_TO_TABLE_PREVIEW_REQUEST,
)
from synapseclient.core.exceptions import SynapseError
from synapseclient.models import EntityView, RecordSet
from synapseclient.models.curation import (
    AuthorizationMode,
    CellValueFilter,
    CellValueOperator,
    ComputeTaskExecutionRequest,
    CountStar,
    CreateGridRequest,
    CreateReplicaRequest,
    CurationTask,
    CurationTaskProperties,
    CurationTaskStatus,
    DownloadFromGridRequest,
    ExecutableTaskExecutionDetails,
    FileBasedMetadataTaskProperties,
    Grid,
    GridCsvImportRequest,
    GridExecutionDetails,
    GridQuery,
    GridQueryJobRequest,
    GridQueryResult,
    GridQueryValidationResult,
    GridRecordSetExportRequest,
    GridReplica,
    GridRow,
    QueryRequest,
    RecordBasedMetadataTaskProperties,
    RecordSetGenerationExecutionDetails,
    RecordSetGenerationExecutionProperties,
    RowIdFilter,
    RowIsValidFilter,
    RowSelectionFilter,
    RowValidationResultFilter,
    SampleSheetGenerationExecutionDetails,
    SampleSheetGenerationExecutionProperties,
    SelectAll,
    SelectByName,
    SelectColumn,
    SelectSelection,
    SynchronizeGridRequest,
    SyncType,
    TaskExecutionDetails,
    TaskState,
    UnknownCurationTaskProperties,
    UnknownTaskExecutionDetails,
    UploadToTablePreviewRequest,
    _create_task_execution_details_from_dict,
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
OWNER_PRINCIPAL_ID = 987654
ASYNC_JOB_ID = "async-job-abc-123"
ERROR_MESSAGE = "Execution failed"
ERROR_DETAILS = "A longer explanation of the failure"
UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE = (
    "org.sagebionetworks.repo.model.curation.execution.FutureExecutionDetails"
)
UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE = (
    "org.sagebionetworks.repo.model.curation.metadata.FutureTaskProperties"
)
REPLICA_ID = 12345


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
        # The server returns ownerPrincipalId as a string; the client coerces to int.
        "ownerPrincipalId": str(OWNER_PRINCIPAL_ID),
        "authorizationMode": "SESSION_OWNER",
    }


STATUS_ETAG = "status-etag-789"
STATUS_LAST_UPDATED_ON = "2024-04-01T00:00:00.000Z"
STATUS_LAST_UPDATED_BY = "444444"


def _get_curation_task_status_response(
    state: str = "NOT_STARTED",
    active_session_id: str | None = None,
):
    """Return a mock CurationTaskStatus API response."""
    response = {
        "taskId": TASK_ID,
        "state": state,
        "lastUpdatedBy": STATUS_LAST_UPDATED_BY,
        "lastUpdatedOn": STATUS_LAST_UPDATED_ON,
        "etag": STATUS_ETAG,
    }
    if active_session_id is not None:
        response["executionDetails"] = {
            "concreteType": GRID_EXECUTION_DETAILS,
            "activeSessionId": active_session_id,
        }
    return response


class TestCurationTaskProperties:
    """Tests for the CurationTaskProperties abstract base class."""

    def test_cannot_be_instantiated(self) -> None:
        # GIVEN the abstract base class
        # WHEN I try to instantiate it
        # THEN a TypeError is raised because it defines no concreteType
        with pytest.raises(TypeError, match="abstract"):
            CurationTaskProperties()

    @pytest.mark.parametrize(
        "properties_class,expected_concrete_type",
        [
            (FileBasedMetadataTaskProperties, FILE_BASED_METADATA_TASK_PROPERTIES),
            (RecordBasedMetadataTaskProperties, RECORD_BASED_METADATA_TASK_PROPERTIES),
            (
                SampleSheetGenerationExecutionProperties,
                SAMPLE_SHEET_GENERATION_EXECUTION_PROPERTIES,
            ),
            (
                RecordSetGenerationExecutionProperties,
                RECORD_SET_GENERATION_EXECUTION_PROPERTIES,
            ),
        ],
    )
    def test_implementations_share_the_base_contract(
        self, properties_class: type, expected_concrete_type: str
    ) -> None:
        # GIVEN an implementation of CurationTaskProperties
        properties = properties_class()

        # THEN it is an instance of the base class and reports its concreteType
        assert isinstance(properties, CurationTaskProperties)
        assert properties.concrete_type == expected_concrete_type

        # AND an empty instance serializes to just the concreteType
        assert properties.to_synapse_request() == {
            "concreteType": expected_concrete_type
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

        # THEN the request should only contain concreteType (all None-valued fields,
        # including suggested_authorization_mode, are dropped)
        assert request == {"concreteType": FILE_BASED_METADATA_TASK_PROPERTIES}

    def test_fill_from_dict_authorization_fields(self) -> None:
        # GIVEN a response dict including the authorization fields
        response = {
            "uploadFolderId": UPLOAD_FOLDER_ID,
            "fileViewId": FILE_VIEW_ID,
            "suggestedAuthorizationMode": "SOURCE_BENEFACTOR",
            "collaboratorPrincipalIds": ["111", "222"],
        }

        # WHEN I fill a FileBasedMetadataTaskProperties from the dict
        props = FileBasedMetadataTaskProperties()
        props.fill_from_dict(response)

        # THEN the mode is coerced to the enum and collaborators pass through
        assert props.suggested_authorization_mode == AuthorizationMode.SOURCE_BENEFACTOR
        assert isinstance(props.suggested_authorization_mode, AuthorizationMode)
        assert props.collaborator_principal_ids == ["111", "222"]

    def test_to_synapse_request_authorization_fields(self) -> None:
        # GIVEN properties with the authorization mode supplied as a plain string
        props = FileBasedMetadataTaskProperties(
            upload_folder_id=UPLOAD_FOLDER_ID,
            file_view_id=FILE_VIEW_ID,
            suggested_authorization_mode="SESSION_OWNER",
            collaborator_principal_ids=["111"],
        )

        # WHEN I convert it to a request dict
        request = props.to_synapse_request()

        # THEN the enum value is serialized as a string and collaborators pass through
        assert request["suggestedAuthorizationMode"] == "SESSION_OWNER"
        assert request["collaboratorPrincipalIds"] == ["111"]


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

    def test_fill_from_dict_authorization_fields(self) -> None:
        # GIVEN a response dict including the authorization fields
        response = {
            "recordSetId": RECORD_SET_ID,
            "suggestedAuthorizationMode": "SESSION_OWNER",
            "collaboratorPrincipalIds": ["111"],
        }

        # WHEN I fill a RecordBasedMetadataTaskProperties from the dict
        props = RecordBasedMetadataTaskProperties()
        props.fill_from_dict(response)

        # THEN the mode is coerced to the enum and collaborators pass through
        assert props.suggested_authorization_mode == AuthorizationMode.SESSION_OWNER
        assert isinstance(props.suggested_authorization_mode, AuthorizationMode)
        assert props.collaborator_principal_ids == ["111"]

    def test_to_synapse_request_authorization_fields(self) -> None:
        # GIVEN properties with an AuthorizationMode enum value set
        props = RecordBasedMetadataTaskProperties(
            record_set_id=RECORD_SET_ID,
            suggested_authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
        )

        # WHEN I convert it to a request dict
        request = props.to_synapse_request()

        # THEN the enum value is serialized as a string and the absent collaborators
        # are dropped by delete_none_keys
        assert request["suggestedAuthorizationMode"] == "SOURCE_BENEFACTOR"
        assert "collaboratorPrincipalIds" not in request


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

    def test_sample_sheet_generation_properties(self) -> None:
        # GIVEN a dict with the sample sheet generation concrete type
        data = {
            "concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_PROPERTIES,
            "inputTaskId": TASK_ID,
            "destinationTaskId": TASK_ID_2,
        }

        # WHEN I create task properties from the dict
        result = _create_task_properties_from_dict(data)

        # THEN it should be a SampleSheetGenerationExecutionProperties
        assert isinstance(result, SampleSheetGenerationExecutionProperties)
        assert result.input_task_id == TASK_ID
        assert result.destination_task_id == TASK_ID_2

    def test_record_set_generation_properties(self) -> None:
        # GIVEN a dict with the record set generation concrete type
        data = {
            "concreteType": RECORD_SET_GENERATION_EXECUTION_PROPERTIES,
            "folderId": UPLOAD_FOLDER_ID,
            "instructions": INSTRUCTIONS,
            "destinationTaskId": TASK_ID_2,
        }

        # WHEN I create task properties from the dict
        result = _create_task_properties_from_dict(data)

        # THEN it should be a RecordSetGenerationExecutionProperties
        assert isinstance(result, RecordSetGenerationExecutionProperties)
        assert result.folder_id == UPLOAD_FOLDER_ID
        assert result.instructions == INSTRUCTIONS
        assert result.destination_task_id == TASK_ID_2

    def test_unknown_concrete_type_falls_back(self) -> None:
        # GIVEN a dict with a concrete type this client does not recognize
        data = {
            "concreteType": UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE,
            "someFutureField": "a value this client knows nothing about",
        }

        # WHEN I create task properties from it
        result = _create_task_properties_from_dict(data)

        # THEN the properties are returned as the fallback rather than raising, so the
        # rest of the task remains readable
        assert isinstance(result, UnknownCurationTaskProperties)
        assert result.concrete_type == UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE
        assert result.raw_properties == data


class TestUnknownCurationTaskProperties:
    """Tests for the fallback used when a properties concreteType is not recognized."""

    def test_to_synapse_request_round_trips_unmodelled_fields(self) -> None:
        """Serializing back must not drop the fields this client cannot model."""
        # GIVEN properties of an unknown type carrying a field no known subtype has
        response = {
            "concreteType": UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE,
            "uploadFolderId": UPLOAD_FOLDER_ID,
            "someFutureField": "a value this client knows nothing about",
        }

        # WHEN I read the properties and serialize them back
        properties = UnknownCurationTaskProperties().fill_from_dict(response)
        request = properties.to_synapse_request()

        # THEN the response is reproduced exactly, so storing a task this client cannot
        # fully model does not erase the parts it does not understand
        assert request == response

    def test_nested_values_are_not_shared_with_the_response(self) -> None:
        """The copy must be deep: the point of raw_properties is a verbatim record."""
        # GIVEN a response whose unmodelled portion is a nested structure
        response = {
            "concreteType": UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE,
            "someFutureField": {"nested": "original"},
        }
        properties = UnknownCurationTaskProperties().fill_from_dict(response)

        # WHEN the caller mutates the nested value in the original response
        response["someFutureField"]["nested"] = "mutated"

        # THEN the properties still hold what Synapse actually sent
        assert properties.raw_properties["someFutureField"]["nested"] == "original"

    def test_to_synapse_request_is_a_copy(self) -> None:
        # GIVEN properties read from a response
        response = {
            "concreteType": UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE,
            "uploadFolderId": UPLOAD_FOLDER_ID,
        }
        properties = UnknownCurationTaskProperties().fill_from_dict(response)

        # WHEN a caller mutates the request dict
        request = properties.to_synapse_request()
        request["uploadFolderId"] = "mutated"

        # THEN neither the properties nor the original response are affected
        assert properties.to_synapse_request()["uploadFolderId"] == UPLOAD_FOLDER_ID
        assert response["uploadFolderId"] == UPLOAD_FOLDER_ID

    def test_concrete_type_is_empty_when_absent(self) -> None:
        # GIVEN properties that were never populated from a Synapse response
        properties = UnknownCurationTaskProperties()

        # THEN the concrete type reads as empty rather than raising
        assert properties.concrete_type == ""

    def test_to_synapse_request_without_a_response_raises(self) -> None:
        """An empty fallback must not be sent as taskProperties.

        The class is exported, so a user can construct one; serializing it would send
        a taskProperties with no concreteType for the server to dispatch on.
        """
        # GIVEN properties that were never populated from a Synapse response
        properties = UnknownCurationTaskProperties()

        # WHEN I convert them to a request dict
        # THEN it should raise rather than produce a payload with no concreteType
        with pytest.raises(
            ValueError, match="can only be serialized after being populated"
        ):
            properties.to_synapse_request()

    def test_to_synapse_request_without_a_concrete_type_raises(self) -> None:
        """Populated but unusable properties must not be sent either.

        The fallback catches a missing concreteType as well as an unrecognized one,
        so raw_properties can be non-empty while still carrying nothing for the
        server to dispatch on. Guarding on emptiness alone would let that through.
        """
        # GIVEN properties read from a response that named no concreteType
        properties = _create_task_properties_from_dict(
            {"someFutureField": "a value with nothing to dispatch on"}
        )

        # THEN the fallback should have accepted it, non-empty but with no type
        assert isinstance(properties, UnknownCurationTaskProperties)
        assert properties.raw_properties
        assert properties.concrete_type == ""

        # WHEN I convert them to a request dict
        # THEN it should raise here rather than sending a payload the server will
        # reject for having no concreteType
        with pytest.raises(
            ValueError, match="can only be serialized after being populated"
        ):
            properties.to_synapse_request()

    async def test_delete_source_is_refused(self, syn: Synapse) -> None:
        """delete_source cannot work when the source cannot be identified."""
        # GIVEN a task whose properties this client does not recognize
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=UnknownCurationTaskProperties().fill_from_dict(
                {"concreteType": UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE}
            ),
        )

        # WHEN I delete it with delete_source
        # THEN it should raise, naming the type Synapse reported
        with pytest.raises(
            ValueError,
            match=f"delete_source is not supported for task properties of type "
            f"{UNKNOWN_TASK_PROPERTIES_CONCRETE_TYPE}",
        ):
            await task.delete_async(delete_source=True, synapse_client=syn)


class TestSampleSheetGenerationExecutionProperties:
    """Tests for the SampleSheetGenerationExecutionProperties dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response dict where the task ids are returned as strings
        response = {
            "concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_PROPERTIES,
            "inputTaskId": str(TASK_ID),
            "destinationTaskId": str(TASK_ID_2),
        }

        # WHEN I fill the properties from the dict
        props = SampleSheetGenerationExecutionProperties().fill_from_dict(response)

        # THEN the task ids should be coerced to ints
        assert props.input_task_id == TASK_ID
        assert props.destination_task_id == TASK_ID_2

    def test_fill_from_dict_empty(self) -> None:
        # GIVEN a response dict with no task ids
        # WHEN I fill the properties from the dict
        props = SampleSheetGenerationExecutionProperties().fill_from_dict(
            {"concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_PROPERTIES}
        )

        # THEN both task ids should be None rather than coerced from None
        assert props.input_task_id is None
        assert props.destination_task_id is None

    def test_to_synapse_request(self) -> None:
        # GIVEN properties with only an input task id
        props = SampleSheetGenerationExecutionProperties(input_task_id=TASK_ID)

        # WHEN I convert them to a request dict
        request = props.to_synapse_request()

        # THEN the concreteType is included and the absent id is dropped
        assert request == {
            "concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_PROPERTIES,
            "inputTaskId": TASK_ID,
        }


class TestRecordSetGenerationExecutionProperties:
    """Tests for the RecordSetGenerationExecutionProperties dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response dict where the destination task id is returned as a string
        response = {
            "concreteType": RECORD_SET_GENERATION_EXECUTION_PROPERTIES,
            "folderId": UPLOAD_FOLDER_ID,
            "instructions": INSTRUCTIONS,
            "destinationTaskId": str(TASK_ID_2),
        }

        # WHEN I fill the properties from the dict
        props = RecordSetGenerationExecutionProperties().fill_from_dict(response)

        # THEN all fields should be populated and the id coerced to an int
        assert props.folder_id == UPLOAD_FOLDER_ID
        assert props.instructions == INSTRUCTIONS
        assert props.destination_task_id == TASK_ID_2

    def test_to_synapse_request(self) -> None:
        # GIVEN properties with a folder and instructions but no destination
        props = RecordSetGenerationExecutionProperties(
            folder_id=UPLOAD_FOLDER_ID, instructions=INSTRUCTIONS
        )

        # WHEN I convert them to a request dict
        request = props.to_synapse_request()

        # THEN the concreteType is included and the absent id is dropped
        assert request == {
            "concreteType": RECORD_SET_GENERATION_EXECUTION_PROPERTIES,
            "folderId": UPLOAD_FOLDER_ID,
            "instructions": INSTRUCTIONS,
        }


class TestExecutableTaskExecutionDetails:
    """Tests for the ExecutableTaskExecutionDetails implementations."""

    @pytest.mark.parametrize(
        "details_class,concrete_type",
        [
            (
                SampleSheetGenerationExecutionDetails,
                SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS,
            ),
            (
                RecordSetGenerationExecutionDetails,
                RECORD_SET_GENERATION_EXECUTION_DETAILS,
            ),
        ],
    )
    def test_fill_from_dict(self, details_class, concrete_type: str) -> None:
        # GIVEN a response dict for a failed execution
        response = {
            "concreteType": concrete_type,
            "asyncJobId": ASYNC_JOB_ID,
            "startedBy": STARTED_BY,
            "startedOn": STARTED_ON,
            "errorMessage": ERROR_MESSAGE,
            "errorDetails": ERROR_DETAILS,
        }

        # WHEN I fill the details from the dict
        details = details_class().fill_from_dict(response)

        # THEN every field should be populated
        assert details.async_job_id == ASYNC_JOB_ID
        assert details.started_by == STARTED_BY
        assert details.started_on == STARTED_ON
        assert details.error_message == ERROR_MESSAGE
        assert details.error_details == ERROR_DETAILS
        assert details.concrete_type == concrete_type

    @pytest.mark.parametrize(
        "details_class,concrete_type",
        [
            (
                SampleSheetGenerationExecutionDetails,
                SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS,
            ),
            (
                RecordSetGenerationExecutionDetails,
                RECORD_SET_GENERATION_EXECUTION_DETAILS,
            ),
        ],
    )
    def test_to_synapse_request_empty_sends_only_concrete_type(
        self, details_class, concrete_type: str
    ) -> None:
        # GIVEN empty details, as constructed to make a task executable
        details = details_class()

        # WHEN I convert them to a request dict
        request = details.to_synapse_request()

        # THEN only the concreteType is sent: every other field is None and dropped,
        # so set_execution_details does not claim ownership of server-managed fields
        assert request == {"concreteType": concrete_type}

    @pytest.mark.parametrize(
        "details_class,concrete_type",
        [
            (
                SampleSheetGenerationExecutionDetails,
                SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS,
            ),
            (
                RecordSetGenerationExecutionDetails,
                RECORD_SET_GENERATION_EXECUTION_DETAILS,
            ),
        ],
    )
    def test_to_synapse_request_round_trips_populated_fields(
        self, details_class, concrete_type: str
    ) -> None:
        # GIVEN details read back from an earlier failed run
        details = details_class(
            async_job_id=ASYNC_JOB_ID,
            started_by=STARTED_BY,
            started_on=STARTED_ON,
            error_message=ERROR_MESSAGE,
            error_details=ERROR_DETAILS,
        )

        # WHEN I convert them to a request dict
        request = details.to_synapse_request()

        # THEN every populated field is sent. The status endpoint replaces
        # executionDetails rather than merging, so a read-modify-write that omitted
        # these would delete the recorded failure reason server-side.
        assert request == {
            "concreteType": concrete_type,
            "asyncJobId": ASYNC_JOB_ID,
            "startedBy": STARTED_BY,
            "startedOn": STARTED_ON,
            "errorMessage": ERROR_MESSAGE,
            "errorDetails": ERROR_DETAILS,
        }


class TestUnknownTaskExecutionDetails:
    """Tests for the fallback used when a concreteType is not recognized."""

    def test_to_synapse_request_round_trips_unmodelled_fields(self) -> None:
        """Serializing back must not drop the fields this client cannot model."""
        # GIVEN details of an unknown type carrying a field no known subtype has
        response = {
            "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
            "startedOn": STARTED_ON,
            "errorMessage": ERROR_MESSAGE,
            "someFutureField": "a value this client knows nothing about",
        }

        # WHEN I read the details and serialize them back
        details = UnknownTaskExecutionDetails().fill_from_dict(response)
        request = details.to_synapse_request()

        # THEN the response is reproduced exactly. The status endpoint replaces
        # executionDetails rather than merging, so a set_task_state that serialized
        # only the fields this client understands would erase the rest server-side
        assert request == response

    def test_to_synapse_request_is_a_copy(self) -> None:
        # GIVEN details read from a response
        response = {
            "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
            "startedOn": STARTED_ON,
        }
        details = UnknownTaskExecutionDetails().fill_from_dict(response)

        # WHEN a caller mutates the request dict
        request = details.to_synapse_request()
        request["startedOn"] = "mutated"

        # THEN neither the details nor the original response are affected
        assert details.to_synapse_request()["startedOn"] == STARTED_ON
        assert response["startedOn"] == STARTED_ON

    def test_nested_values_are_not_shared_with_the_response(self) -> None:
        """The copy must be deep: the point of raw_details is a verbatim record."""
        # GIVEN a response whose unmodelled portion is a nested structure
        response = {
            "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
            "someFutureField": {"nested": "original"},
        }
        details = UnknownTaskExecutionDetails().fill_from_dict(response)

        # WHEN the caller mutates the nested value in the original response
        response["someFutureField"]["nested"] = "mutated"

        # THEN the details still hold what Synapse actually sent
        assert details.raw_details["someFutureField"]["nested"] == "original"

    def test_common_fields_are_read_from_the_raw_response(self) -> None:
        # GIVEN details of an unknown type carrying every common field
        details = UnknownTaskExecutionDetails().fill_from_dict(
            {
                "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
                "asyncJobId": ASYNC_JOB_ID,
                "startedBy": STARTED_BY,
                "startedOn": STARTED_ON,
                "errorMessage": ERROR_MESSAGE,
                "errorDetails": ERROR_DETAILS,
            }
        )

        # THEN every common field is readable as a property over the raw response
        assert details.concrete_type == UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE
        assert details.async_job_id == ASYNC_JOB_ID
        assert details.started_by == STARTED_BY
        assert details.started_on == STARTED_ON
        assert details.error_message == ERROR_MESSAGE
        assert details.error_details == ERROR_DETAILS

    def test_common_fields_are_none_when_absent(self) -> None:
        # GIVEN details carrying nothing but a concreteType
        details = UnknownTaskExecutionDetails().fill_from_dict(
            {"concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE}
        )

        # THEN the common fields read as None rather than raising
        assert details.async_job_id is None
        assert details.started_by is None
        assert details.started_on is None
        assert details.error_message is None
        assert details.error_details is None

    def test_to_synapse_request_without_a_response_raises(self) -> None:
        """An empty fallback must not be sent as executionDetails.

        The class is exported, so a user can construct one; serializing it would PUT
        an executionDetails with no concreteType, and the status endpoint replaces
        rather than merges.
        """
        # GIVEN details that were never populated from a Synapse response
        details = UnknownTaskExecutionDetails()

        # WHEN I convert them to a request dict
        # THEN it should raise rather than produce a payload with no concreteType
        with pytest.raises(
            ValueError, match="can only be serialized after being populated"
        ):
            details.to_synapse_request()

    def test_to_synapse_request_without_a_concrete_type_raises(self) -> None:
        """Populated but unusable details must not be PUT either.

        The fallback catches a missing concreteType as well as an unrecognized one,
        so raw_details can be non-empty while still carrying nothing for the server
        to dispatch on. Guarding on emptiness alone would let that through.
        """
        # GIVEN details read from a response that named no concreteType
        details = _create_task_execution_details_from_dict({"asyncJobId": ASYNC_JOB_ID})

        # THEN the fallback should have accepted it, non-empty but with no type
        assert isinstance(details, UnknownTaskExecutionDetails)
        assert details.raw_details
        assert details.concrete_type == ""

        # WHEN I convert them to a request dict
        # THEN it should raise here rather than PUTting a payload the server will
        # reject for having no concreteType
        with pytest.raises(
            ValueError, match="can only be serialized after being populated"
        ):
            details.to_synapse_request()

    def test_is_not_an_executable_task_execution_details(self) -> None:
        """An unrecognized concreteType must not claim to support execution.

        Not every TaskExecutionDetails subtype is executable (GridExecutionDetails is
        not), so a type added after this client was released cannot be assumed to be.
        Callers gate execute() on isinstance(details, ExecutableTaskExecutionDetails),
        which would wave through a task the server will refuse.
        """
        # GIVEN details of a concreteType this client does not recognize
        details = UnknownTaskExecutionDetails().fill_from_dict(
            {
                "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
                "asyncJobId": ASYNC_JOB_ID,
                "errorMessage": ERROR_MESSAGE,
            }
        )

        # THEN they are a TaskExecutionDetails, but not an executable one
        assert isinstance(details, TaskExecutionDetails)
        assert not isinstance(details, ExecutableTaskExecutionDetails)

        # AND the fields common to every execution details type are still readable,
        # which is the whole reason the fallback exists
        assert details.async_job_id == ASYNC_JOB_ID
        assert details.error_message == ERROR_MESSAGE


class TestGridExecutionDetails:
    """Tests for the GridExecutionDetails dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response dict with an active session id
        response = {
            "concreteType": GRID_EXECUTION_DETAILS,
            "activeSessionId": SESSION_ID,
        }

        # WHEN I fill a GridExecutionDetails from the dict
        details = GridExecutionDetails()
        details.fill_from_dict(response)

        # THEN the active_session_id should be populated
        assert details.active_session_id == SESSION_ID

    def test_to_synapse_request(self) -> None:
        # GIVEN a GridExecutionDetails with an active session id
        details = GridExecutionDetails(active_session_id=SESSION_ID)

        # WHEN I convert it to a request dict
        request = details.to_synapse_request()

        # THEN the request should contain the concreteType and activeSessionId
        assert request["concreteType"] == GRID_EXECUTION_DETAILS
        assert request["activeSessionId"] == SESSION_ID

    def test_concrete_type(self) -> None:
        # GIVEN a GridExecutionDetails
        # WHEN I read its concrete type
        # THEN it should be readable without serializing the object, the same as
        # every other TaskExecutionDetails implementation
        assert GridExecutionDetails().concrete_type == GRID_EXECUTION_DETAILS

    def test_concrete_type_is_not_a_dataclass_field(self) -> None:
        # GIVEN two GridExecutionDetails with the same session id
        # WHEN I compare them
        # THEN they should be equal: concrete_type is a property, so it does not
        # take part in the generated __init__ or __eq__
        assert GridExecutionDetails(active_session_id=SESSION_ID) == (
            GridExecutionDetails(active_session_id=SESSION_ID)
        )
        assert "concrete_type" not in GridExecutionDetails.__dataclass_fields__


class TestCurationTaskStatus:
    """Tests for the CurationTaskStatus dataclass."""

    def test_fill_from_dict_full(self) -> None:
        """All scalar fields and nested GridExecutionDetails are populated; state string is coerced to enum."""
        # GIVEN a full status response with execution details
        response = _get_curation_task_status_response(
            state="IN_PROGRESS", active_session_id=SESSION_ID
        )

        # WHEN I fill a CurationTaskStatus from it
        status = CurationTaskStatus().fill_from_dict(response)

        # THEN all fields should be populated and state coerced to the enum
        assert status.task_id == TASK_ID
        assert status.state == TaskState.IN_PROGRESS
        assert status.last_updated_by == STATUS_LAST_UPDATED_BY
        assert status.last_updated_on == STATUS_LAST_UPDATED_ON
        assert status.etag == STATUS_ETAG
        assert isinstance(status.execution_details, GridExecutionDetails)
        assert status.execution_details.active_session_id == SESSION_ID

    def test_fill_from_dict_without_execution_details(self) -> None:
        """execution_details is None when the response omits the executionDetails key."""
        # GIVEN a status response with no executionDetails
        response = _get_curation_task_status_response(state="NOT_STARTED")

        # WHEN I fill a CurationTaskStatus from it
        status = CurationTaskStatus().fill_from_dict(response)

        # THEN execution_details should be None
        assert status.execution_details is None
        assert status.state == TaskState.NOT_STARTED

    def test_fill_from_dict_unknown_execution_details_concrete_type(self) -> None:
        """An unrecognized concreteType in executionDetails does not raise."""
        # GIVEN a status response with an unknown executionDetails concreteType
        response = _get_curation_task_status_response(state="NOT_STARTED")
        response["executionDetails"] = {
            "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
            "startedOn": STARTED_ON,
            "errorMessage": ERROR_MESSAGE,
        }

        # WHEN I fill a CurationTaskStatus from it
        status = CurationTaskStatus().fill_from_dict(response)

        # THEN the status should still be readable. Execution details report on work
        # the server has already done, so a subtype added after this client was
        # released must not make the whole status unreadable
        assert isinstance(status.execution_details, UnknownTaskExecutionDetails)
        assert (
            status.execution_details.concrete_type
            == UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE
        )
        assert status.execution_details.started_on == STARTED_ON
        assert status.execution_details.error_message == ERROR_MESSAGE
        assert status.state == TaskState.NOT_STARTED

    @pytest.mark.parametrize(
        "concrete_type,details_class",
        [
            (
                SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS,
                SampleSheetGenerationExecutionDetails,
            ),
            (
                RECORD_SET_GENERATION_EXECUTION_DETAILS,
                RecordSetGenerationExecutionDetails,
            ),
        ],
    )
    def test_fill_from_dict_executable_execution_details(
        self, concrete_type: str, details_class
    ) -> None:
        """The executable execution details concrete types resolve to their classes."""
        # GIVEN a status response carrying executable execution details
        response = _get_curation_task_status_response(state="EXECUTING")
        response["executionDetails"] = {
            "concreteType": concrete_type,
            "asyncJobId": ASYNC_JOB_ID,
            "startedBy": STARTED_BY,
            "startedOn": STARTED_ON,
        }

        # WHEN I fill a CurationTaskStatus from it
        status = CurationTaskStatus().fill_from_dict(response)

        # THEN the execution details should be the matching class, fully populated
        assert isinstance(status.execution_details, details_class)
        assert status.execution_details.async_job_id == ASYNC_JOB_ID
        assert status.execution_details.started_by == STARTED_BY
        assert status.execution_details.started_on == STARTED_ON
        assert status.state == TaskState.EXECUTING


class TestComputeTaskExecutionRequest:
    """Tests for the ComputeTaskExecutionRequest async job dataclass."""

    def test_to_synapse_request(self) -> None:
        # GIVEN a request for a task
        request = ComputeTaskExecutionRequest(task_id=TASK_ID)

        # WHEN I convert it to a request dict
        result = request.to_synapse_request()

        # THEN it should carry the concreteType and the taskId, which the async job
        # layer also uses to resolve the /curation/task/{taskId}/execute/async URI
        assert result == {
            "concreteType": COMPUTE_TASK_EXECUTION_REQUEST,
            "taskId": TASK_ID,
        }

    def test_fill_from_dict(self) -> None:
        # GIVEN a ComputeTaskExecutionResponse body
        response = {
            "taskId": str(TASK_ID),
            "executionDetails": {
                "concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS,
                "startedBy": STARTED_BY,
                "startedOn": STARTED_ON,
            },
        }

        # WHEN I fill the request from the response
        request = ComputeTaskExecutionRequest().fill_from_dict(response)

        # THEN the task id is coerced to an int and the details are deserialized
        assert request.task_id == TASK_ID
        assert isinstance(
            request.execution_details, SampleSheetGenerationExecutionDetails
        )
        assert request.execution_details.started_by == STARTED_BY

    def test_fill_from_dict_without_execution_details(self) -> None:
        # GIVEN a response body with no execution details
        # WHEN I fill the request from the response
        request = ComputeTaskExecutionRequest(task_id=TASK_ID).fill_from_dict(
            {"taskId": TASK_ID}
        )

        # THEN execution_details should be None
        assert request.execution_details is None
        assert request.task_id == TASK_ID

    def test_fill_from_dict_unknown_execution_details_concrete_type(self) -> None:
        # GIVEN a response body for a job that ran to completion, carrying an
        # executionDetails concreteType this client does not know
        response = {
            "taskId": TASK_ID,
            "executionDetails": {
                "concreteType": UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE,
                "asyncJobId": ASYNC_JOB_ID,
                "startedBy": STARTED_BY,
                "startedOn": STARTED_ON,
            },
        }

        # WHEN I fill the request from the response
        request = ComputeTaskExecutionRequest().fill_from_dict(response)

        # THEN the details common to every execution type are still available. The
        # computation already ran server-side, so raising here would strand the user
        # with a completed job whose outcome they cannot read
        assert isinstance(request.execution_details, UnknownTaskExecutionDetails)
        assert (
            request.execution_details.concrete_type
            == UNKNOWN_EXECUTION_DETAILS_CONCRETE_TYPE
        )
        assert request.execution_details.async_job_id == ASYNC_JOB_ID
        assert request.execution_details.started_by == STARTED_BY
        assert request.execution_details.started_on == STARTED_ON
        assert request.task_id == TASK_ID


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

    def test_fill_from_dict_missing_task_properties_raises(self) -> None:
        # GIVEN a CurationTask API response with taskProperties omitted
        response = _get_file_based_task_api_response()
        del response["taskProperties"]

        # WHEN I fill a CurationTask from the response
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="taskProperties was not found"):
            CurationTask().fill_from_dict(response)

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

    def test_has_changed_true_after_nested_task_properties_mutation(self) -> None:
        # GIVEN a CurationTask with task_properties and a persistent instance set
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=FileBasedMetadataTaskProperties(
                file_view_id=FILE_VIEW_ID,
                upload_folder_id=UPLOAD_FOLDER_ID,
            ),
        )
        task._set_last_persistent_instance()

        # WHEN I mutate a field on the nested task_properties object in place
        task.task_properties.file_view_id = "syn_updated"

        # THEN has_changed should be True because task_properties was deep-copied
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

    @pytest.mark.parametrize(
        "task_properties",
        [
            SampleSheetGenerationExecutionProperties(destination_task_id=TASK_ID_2),
            RecordSetGenerationExecutionProperties(
                folder_id=UPLOAD_FOLDER_ID, destination_task_id=TASK_ID_2
            ),
        ],
        ids=["sample_sheet_generation", "record_set_generation"],
    )
    async def test_delete_async_source_for_compute_task_raises(
        self, task_properties
    ) -> None:
        """delete_source is refused for a compute task, which owns no source."""
        # GIVEN a compute task whose properties name a destination owned by another task
        task = CurationTask(task_id=TASK_ID, task_properties=task_properties)

        # WHEN I call delete_async with delete_source
        # THEN it should raise a ValueError naming the actual properties type rather
        # than claiming they are None, and nothing should be deleted
        with patch(
            "synapseclient.models.curation.delete_curation_task",
            new_callable=AsyncMock,
        ) as mock_delete:
            with pytest.raises(
                ValueError,
                match=(
                    "delete_source is not supported for "
                    f"{type(task_properties).__name__}"
                ),
            ):
                await task.delete_async(delete_source=True, synapse_client=self.syn)

        mock_delete.assert_not_called()

    async def test_delete_async_source_without_task_properties_raises(self) -> None:
        """delete_source with properties that stay None reports them as None."""
        # GIVEN a task whose properties are still None after being fetched
        task = CurationTask(task_id=TASK_ID)

        # WHEN I call delete_async with delete_source
        # THEN it should raise the ValueError for absent properties
        with (
            patch.object(
                CurationTask, "get_async", new_callable=AsyncMock, return_value=task
            ),
            patch(
                "synapseclient.models.curation.delete_curation_task",
                new_callable=AsyncMock,
            ) as mock_delete,
        ):
            with pytest.raises(
                ValueError,
                match=(
                    "delete_source requires task properties that identify a "
                    "source, but 'task_properties' is None."
                ),
            ):
                await task.delete_async(delete_source=True, synapse_client=self.syn)

        mock_delete.assert_not_called()

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

    async def test_get_status_async(self) -> None:
        """Verify that get_status_async calls the API with the task_id and returns a parsed CurationTaskStatus."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # WHEN I call get_status_async
        with patch(
            "synapseclient.models.curation.get_curation_task_status",
            new_callable=AsyncMock,
            return_value=_get_curation_task_status_response(
                state="IN_PROGRESS", active_session_id=SESSION_ID
            ),
        ) as mock_get_status:
            result = await task.get_status_async(synapse_client=self.syn)

            # THEN the API should be called with the task_id
            mock_get_status.assert_called_once_with(
                task_id=TASK_ID, synapse_client=self.syn
            )

            # AND the response should be parsed into a CurationTaskStatus
            assert isinstance(result, CurationTaskStatus)
            assert result.task_id == TASK_ID
            assert result.state == TaskState.IN_PROGRESS
            assert result.etag == STATUS_ETAG
            assert result.last_updated_by == STATUS_LAST_UPDATED_BY
            assert result.last_updated_on == STATUS_LAST_UPDATED_ON
            assert isinstance(result.execution_details, GridExecutionDetails)
            assert result.execution_details.active_session_id == SESSION_ID

    async def test_get_status_async_without_task_id(self) -> None:
        """Verify that get_status_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call get_status_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="task_id is required to get"):
            await task.get_status_async(synapse_client=self.syn)

    async def test_update_status_async(self) -> None:
        """Verify that update_status_async serializes the status correctly and returns the updated CurationTaskStatus."""
        # GIVEN a CurationTask with a task_id and a status to write
        task = CurationTask(task_id=TASK_ID)
        status_to_update = CurationTaskStatus(
            task_id=TASK_ID,
            state=TaskState.IN_PROGRESS,
            execution_details=GridExecutionDetails(active_session_id=SESSION_ID),
            etag=STATUS_ETAG,
        )
        expected_payload = status_to_update.to_synapse_request()

        # WHEN I call update_status_async
        with patch(
            "synapseclient.models.curation.update_curation_task_status",
            new_callable=AsyncMock,
            return_value=_get_curation_task_status_response(
                state="IN_PROGRESS", active_session_id=SESSION_ID
            ),
        ) as mock_update_status:
            result = await task.update_status_async(
                curation_task_status=status_to_update, synapse_client=self.syn
            )

            # THEN the API should be called with the serialized status
            mock_update_status.assert_called_once_with(
                task_id=TASK_ID,
                curation_task_status=expected_payload,
                synapse_client=self.syn,
            )

            # AND the state enum should be serialized as its string value
            assert expected_payload["state"] == "IN_PROGRESS"
            assert expected_payload["executionDetails"]["activeSessionId"] == SESSION_ID

            # AND the response should be parsed back into a CurationTaskStatus
            assert isinstance(result, CurationTaskStatus)
            assert result.state == TaskState.IN_PROGRESS
            assert isinstance(result.execution_details, GridExecutionDetails)
            assert result.execution_details.active_session_id == SESSION_ID

    async def test_update_status_async_without_task_id(self) -> None:
        """Verify that update_status_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call update_status_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="task_id is required to update"):
            await task.update_status_async(
                curation_task_status=CurationTaskStatus(),
                synapse_client=self.syn,
            )

    async def test_set_active_grid_session_async(self) -> None:
        """Verify that set_active_grid_session_async fetches the current status and PUTs a payload with the new session id."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # AND a current status fetched from the server with no execution details
        # AND an update response that reflects the new active grid session
        get_response = _get_curation_task_status_response(state="NOT_STARTED")
        put_response = _get_curation_task_status_response(
            state="NOT_STARTED", active_session_id=SESSION_ID
        )

        # WHEN I call set_active_grid_session_async
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=get_response,
            ) as mock_get_status,
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
                return_value=put_response,
            ) as mock_update_status,
        ):
            result = await task.set_active_grid_session_async(
                active_session_id=SESSION_ID, synapse_client=self.syn
            )

            # THEN it should fetch the current status first
            mock_get_status.assert_called_once_with(
                task_id=TASK_ID, synapse_client=self.syn
            )

            # AND PUT a payload that carries the fresh etag and the new
            # GridExecutionDetails linked to the given session id
            mock_update_status.assert_called_once()
            put_kwargs = mock_update_status.call_args.kwargs
            assert put_kwargs["task_id"] == TASK_ID
            payload = put_kwargs["curation_task_status"]
            assert payload["etag"] == STATUS_ETAG
            assert payload["state"] == "NOT_STARTED"
            assert payload["executionDetails"]["concreteType"] == GRID_EXECUTION_DETAILS
            assert payload["executionDetails"]["activeSessionId"] == SESSION_ID

            # AND it should return the parsed update response
            assert isinstance(result, CurationTaskStatus)
            assert isinstance(result.execution_details, GridExecutionDetails)
            assert result.execution_details.active_session_id == SESSION_ID

    async def test_set_execution_details_async(self) -> None:
        """Verify that set_execution_details_async PUTs the given details with a fresh etag."""
        # GIVEN a compute task with a task_id
        task = CurationTask(task_id=TASK_ID)

        # AND a current status with no execution details, and an update response
        # that reflects the newly attached ones
        get_response = _get_curation_task_status_response(state="NOT_STARTED")
        put_response = _get_curation_task_status_response(state="NOT_STARTED")
        put_response["executionDetails"] = {
            "concreteType": RECORD_SET_GENERATION_EXECUTION_DETAILS
        }

        # WHEN I call set_execution_details_async with empty executable details
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=get_response,
            ) as mock_get_status,
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
                return_value=put_response,
            ) as mock_update_status,
        ):
            result = await task.set_execution_details_async(
                execution_details=RecordSetGenerationExecutionDetails(),
                synapse_client=self.syn,
            )

            # THEN it should fetch the current status first
            mock_get_status.assert_called_once_with(
                task_id=TASK_ID, synapse_client=self.syn
            )

            # AND PUT a payload carrying the fresh etag, the unchanged state, and
            # the new execution details
            put_kwargs = mock_update_status.call_args.kwargs
            assert put_kwargs["task_id"] == TASK_ID
            payload = put_kwargs["curation_task_status"]
            assert payload["etag"] == STATUS_ETAG
            assert payload["state"] == "NOT_STARTED"
            assert payload["executionDetails"] == {
                "concreteType": RECORD_SET_GENERATION_EXECUTION_DETAILS
            }

            # AND it should return the parsed update response
            assert isinstance(result, CurationTaskStatus)
            assert isinstance(
                result.execution_details, RecordSetGenerationExecutionDetails
            )

    async def test_set_execution_details_async_replaces_existing_details(self) -> None:
        """Verify that existing execution details are replaced rather than merged."""
        # GIVEN a task whose status already carries grid execution details
        task = CurationTask(task_id=TASK_ID)
        get_response = _get_curation_task_status_response(
            state="NOT_STARTED", active_session_id=SESSION_ID
        )
        put_response = _get_curation_task_status_response(state="NOT_STARTED")
        put_response["executionDetails"] = {
            "concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS
        }

        # WHEN I attach details of a different type
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=get_response,
            ),
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
                return_value=put_response,
            ) as mock_update_status,
        ):
            await task.set_execution_details_async(
                execution_details=SampleSheetGenerationExecutionDetails(),
                synapse_client=self.syn,
            )

        # THEN the payload carries only the new details, with no trace of the
        # grid session that was there before
        payload = mock_update_status.call_args.kwargs["curation_task_status"]
        assert payload["executionDetails"] == {
            "concreteType": SAMPLE_SHEET_GENERATION_EXECUTION_DETAILS
        }

    async def test_set_execution_details_async_without_task_id(self) -> None:
        """Verify that set_execution_details_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call set_execution_details_async
        # THEN it should raise ValueError (propagated from get_status_async)
        with pytest.raises(ValueError, match="task_id is required to get"):
            await task.set_execution_details_async(
                execution_details=RecordSetGenerationExecutionDetails(),
                synapse_client=self.syn,
            )

    async def test_set_execution_details_async_requires_a_keyword(self) -> None:
        """execution_details is keyword-only, so a positional call is rejected.

        The tracing decorator builds its span name from a lambda that only accepts
        self, so a positional argument reaching it would raise a TypeError naming
        the lambda instead of the method. Keyword-only keeps that from happening.
        """
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # WHEN I pass the execution details positionally
        # THEN Python should reject the call by name, before any API call
        with pytest.raises(TypeError, match="set_execution_details_async"):
            await task.set_execution_details_async(
                RecordSetGenerationExecutionDetails(), synapse_client=self.syn
            )

    async def test_set_active_grid_session_async_without_task_id(self) -> None:
        """Verify that set_active_grid_session_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call set_active_grid_session_async
        # THEN it should raise ValueError (propagated from get_status_async)
        with pytest.raises(ValueError, match="task_id is required to get"):
            await task.set_active_grid_session_async(
                active_session_id=SESSION_ID, synapse_client=self.syn
            )

    @pytest.mark.parametrize(
        "input_state,expected_state_value",
        [
            (TaskState.IN_PROGRESS, "IN_PROGRESS"),
            (TaskState.COMPLETED, "COMPLETED"),
            (TaskState.CANCELED, "CANCELED"),
            ("IN_PROGRESS", "IN_PROGRESS"),
            ("COMPLETED", "COMPLETED"),
            ("CANCELED", "CANCELED"),
        ],
    )
    async def test_set_task_state_async(
        self, input_state, expected_state_value: str
    ) -> None:
        """Verify that set_task_state_async accepts a TaskState enum or string and PUTs the correct serialized state."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # AND a current status from the server with execution_details set
        get_response = _get_curation_task_status_response(
            state="NOT_STARTED", active_session_id=SESSION_ID
        )
        # AND an update response reflecting the new state but preserving
        # execution_details
        put_response = _get_curation_task_status_response(
            state=expected_state_value, active_session_id=SESSION_ID
        )

        # WHEN I call set_task_state_async with an enum or string
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=get_response,
            ) as mock_get_status,
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
                return_value=put_response,
            ) as mock_update_status,
        ):
            result = await task.set_task_state_async(
                state=input_state, synapse_client=self.syn
            )

            # THEN it fetches the current status first
            mock_get_status.assert_called_once_with(
                task_id=TASK_ID, synapse_client=self.syn
            )

            # AND PUTs a payload that carries the fresh etag, the coerced
            # state, and preserves the existing execution_details
            mock_update_status.assert_called_once()
            put_kwargs = mock_update_status.call_args.kwargs
            assert put_kwargs["task_id"] == TASK_ID
            payload = put_kwargs["curation_task_status"]
            assert payload["etag"] == STATUS_ETAG
            assert payload["state"] == expected_state_value
            assert payload["executionDetails"]["concreteType"] == GRID_EXECUTION_DETAILS
            assert payload["executionDetails"]["activeSessionId"] == SESSION_ID

            # AND it returns the parsed update response
            assert isinstance(result, CurationTaskStatus)
            assert result.state == TaskState(expected_state_value)
            assert isinstance(result.execution_details, GridExecutionDetails)
            assert result.execution_details.active_session_id == SESSION_ID

    async def test_set_task_state_async_preserves_executable_execution_details(
        self,
    ) -> None:
        """set_task_state_async must not drop the fields a failed run recorded.

        The status endpoint replaces executionDetails rather than merging, so a
        read-modify-write that serialized only the concreteType would delete the
        failure reason server-side.
        """
        # GIVEN a compute task whose last run failed, leaving the error on its
        # executable execution details
        task = CurationTask(task_id=TASK_ID)
        get_response = _get_curation_task_status_response(state="NOT_STARTED")
        get_response["executionDetails"] = {
            "concreteType": RECORD_SET_GENERATION_EXECUTION_DETAILS,
            "asyncJobId": ASYNC_JOB_ID,
            "startedBy": STARTED_BY,
            "startedOn": STARTED_ON,
            "errorMessage": ERROR_MESSAGE,
            "errorDetails": ERROR_DETAILS,
        }
        put_response = _get_curation_task_status_response(state="CANCELED")
        put_response["executionDetails"] = get_response["executionDetails"]

        # WHEN I transition the task to another state
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=get_response,
            ),
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
                return_value=put_response,
            ) as mock_update_status,
        ):
            await task.set_task_state_async(
                state=TaskState.CANCELED, synapse_client=self.syn
            )

        # THEN the PUT payload carries every field that was read, so nothing the
        # server recorded about the failed run is erased
        payload = mock_update_status.call_args.kwargs["curation_task_status"]
        assert payload["state"] == "CANCELED"
        assert payload["executionDetails"] == {
            "concreteType": RECORD_SET_GENERATION_EXECUTION_DETAILS,
            "asyncJobId": ASYNC_JOB_ID,
            "startedBy": STARTED_BY,
            "startedOn": STARTED_ON,
            "errorMessage": ERROR_MESSAGE,
            "errorDetails": ERROR_DETAILS,
        }

    async def test_set_task_state_async_accepts_a_positional_state(self) -> None:
        """state is positional-or-keyword, so passing it positionally must work.

        The tracing decorator forwards positional arguments into the lambda that
        builds the span name, so a lambda accepting only self would fail the call
        before it reached the method body.
        """
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)
        get_response = _get_curation_task_status_response(state="NOT_STARTED")
        put_response = _get_curation_task_status_response(state="CANCELED")

        # WHEN I pass the state positionally
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=get_response,
            ),
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
                return_value=put_response,
            ) as mock_update_status,
        ):
            result = await task.set_task_state_async(
                TaskState.CANCELED, synapse_client=self.syn
            )

        # THEN the transition should be sent, rather than the call failing inside
        # the tracing decorator
        payload = mock_update_status.call_args.kwargs["curation_task_status"]
        assert payload["state"] == "CANCELED"
        assert result.state == TaskState.CANCELED

    async def test_set_task_state_async_invalid_string(self) -> None:
        """Verify that set_task_state_async raises ValueError before any API call when given an unrecognized state string."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # WHEN I call set_task_state_async with a string that does not match
        # any TaskState member
        # THEN it raises ValueError before any API call is made
        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
            ) as mock_get_status,
            patch(
                "synapseclient.models.curation.update_curation_task_status",
                new_callable=AsyncMock,
            ) as mock_update_status,
        ):
            with pytest.raises(ValueError, match="is not a valid TaskState"):
                await task.set_task_state_async(
                    state="NOT_A_REAL_STATE", synapse_client=self.syn
                )

            mock_get_status.assert_not_called()
            mock_update_status.assert_not_called()

    async def test_set_task_state_async_without_task_id(self) -> None:
        """Verify that set_task_state_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call set_task_state_async
        # THEN it should raise ValueError (propagated from get_status_async)
        with pytest.raises(ValueError, match="task_id is required to get"):
            await task.set_task_state_async(
                state=TaskState.IN_PROGRESS, synapse_client=self.syn
            )

    async def test_create_grid_session_async_without_task_id(self) -> None:
        """Verify that create_grid_session_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call create_grid_session_async
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="task_id is required to create a CurationTask grid session",
        ):
            await task.create_grid_session_async(synapse_client=self.syn)

    async def test_create_grid_session_async_record_based_missing_record_set_id(
        self,
    ) -> None:
        """Verify that create_grid_session_async raises ValueError when task_properties.record_set_id is None."""
        # GIVEN a record-based CurationTask whose record_set_id is missing
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(record_set_id=None),
        )

        # WHEN I call create_grid_session_async
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="task_properties.record_set_id is missing"
        ):
            await task.create_grid_session_async(synapse_client=self.syn)

    async def test_create_grid_session_async_file_based_missing_file_view_id(
        self,
    ) -> None:
        """Verify that create_grid_session_async raises ValueError when task_properties.file_view_id is None."""
        # GIVEN a file-based CurationTask whose file_view_id is missing
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=None
            ),
        )

        # WHEN I call create_grid_session_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="task_properties.file_view_id is missing"):
            await task.create_grid_session_async(synapse_client=self.syn)

    async def test_create_grid_session_async_record_based_record_set_not_found(
        self,
    ) -> None:
        """Verify that a SynapseHTTPError from RecordSet.get_async propagates out of create_grid_session_async."""
        from synapseclient.core.exceptions import SynapseHTTPError

        # GIVEN a record-based CurationTask with a record_set_id that does not exist
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            ),
        )

        # AND RecordSet.get_async raises SynapseHTTPError (e.g. 404)
        with patch.object(
            RecordSet,
            "get_async",
            new_callable=AsyncMock,
            side_effect=SynapseHTTPError("404 Not Found"),
        ):
            # WHEN I call create_grid_session_async
            # THEN the SynapseHTTPError propagates and no Grid is created
            with pytest.raises(SynapseHTTPError):
                await task.create_grid_session_async(synapse_client=self.syn)

    async def test_create_grid_session_async_file_based_entity_view_not_found(
        self,
    ) -> None:
        """Verify that a SynapseHTTPError from EntityView.get_async propagates out of create_grid_session_async."""
        from synapseclient.core.exceptions import SynapseHTTPError

        # GIVEN a file-based CurationTask with a file_view_id that does not exist
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=FILE_VIEW_ID
            ),
        )

        # AND EntityView.get_async raises SynapseHTTPError (e.g. 404)
        with patch.object(
            EntityView,
            "get_async",
            new_callable=AsyncMock,
            side_effect=SynapseHTTPError("404 Not Found"),
        ):
            # WHEN I call create_grid_session_async
            # THEN the SynapseHTTPError propagates and no Grid is created
            with pytest.raises(SynapseHTTPError):
                await task.create_grid_session_async(synapse_client=self.syn)

    async def test_create_grid_session_async_unsupported_task_properties(
        self,
    ) -> None:
        """Verify that create_grid_session_async raises ValueError when task_properties is not a recognized type."""
        # GIVEN a CurationTask whose task_properties is of an unsupported type
        task = CurationTask(task_id=TASK_ID)
        task.task_properties = object()  # bypass dataclass typing

        # WHEN I call create_grid_session_async
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="task_properties must be a FileBasedMetadataTaskProperties",
        ):
            await task.create_grid_session_async(synapse_client=self.syn)

    async def test_create_grid_session_async_passes_authorization_mode_record_based(
        self,
    ) -> None:
        """A record-based task forwards its suggested_authorization_mode to the Grid."""
        # GIVEN a record-based task in SESSION_OWNER mode
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID,
                suggested_authorization_mode="SESSION_OWNER",
            ),
        )

        with (
            patch.object(RecordSet, "get_async", new_callable=AsyncMock),
            patch.object(task, "set_active_grid_session_async", new_callable=AsyncMock),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.session_id = SESSION_ID
            mock_grid.create_async = AsyncMock(return_value=mock_grid)

            # WHEN I create a grid session without an explicit owner
            await task.create_grid_session_async(synapse_client=self.syn)

            # THEN the Grid is constructed with the task's authorization mode and the
            # owner is left to the caller (None) for the server to resolve
            kwargs = mock_grid_cls.call_args.kwargs
            assert kwargs["authorization_mode"] == AuthorizationMode.SESSION_OWNER
            assert kwargs["owner_principal_id"] is None

    async def test_create_grid_session_async_passes_authorization_mode_file_based(
        self,
    ) -> None:
        """A file-based task forwards its suggested_authorization_mode to the Grid."""
        # GIVEN a file-based task in SOURCE_BENEFACTOR mode
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID,
                file_view_id=FILE_VIEW_ID,
                suggested_authorization_mode="SOURCE_BENEFACTOR",
            ),
        )

        with (
            patch.object(EntityView, "get_async", new_callable=AsyncMock),
            patch.object(task, "set_active_grid_session_async", new_callable=AsyncMock),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.session_id = SESSION_ID
            mock_grid.create_async = AsyncMock(return_value=mock_grid)

            # WHEN I create a grid session
            await task.create_grid_session_async(synapse_client=self.syn)

            # THEN the Grid is constructed with the task's authorization mode
            assert (
                mock_grid_cls.call_args.kwargs["authorization_mode"]
                == AuthorizationMode.SOURCE_BENEFACTOR
            )

    async def test_create_grid_session_async_passes_explicit_owner(self) -> None:
        """An explicit owner_principal_id is passed straight through to the Grid."""
        # GIVEN a record-based task
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID,
                suggested_authorization_mode="SESSION_OWNER",
            ),
        )

        with (
            patch.object(RecordSet, "get_async", new_callable=AsyncMock),
            patch.object(task, "set_active_grid_session_async", new_callable=AsyncMock),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.session_id = SESSION_ID
            mock_grid.create_async = AsyncMock(return_value=mock_grid)

            # WHEN I create a grid session with an explicit owner
            await task.create_grid_session_async(
                owner_principal_id=555, synapse_client=self.syn
            )

            # THEN that owner is forwarded to the Grid unchanged
            assert mock_grid_cls.call_args.kwargs["owner_principal_id"] == 555

    async def test_list_async_assigned_to_me_and_assignee_ids_raises(self) -> None:
        # GIVEN both assigned_to_me and assignee_ids are provided
        # WHEN I call list_async
        # THEN it should raise ValueError on first iteration (generators are lazy)
        with pytest.raises(
            ValueError, match="mutually exclusive.*Got assignee_ids=\\['123'\\]"
        ):
            async for _ in CurationTask.list_async(
                project_id=PROJECT_ID,
                assigned_to_me=True,
                assignee_ids=["123"],
                synapse_client=self.syn,
            ):
                pass  # pragma: no cover

    async def test_list_async_passes_assigned_to_me_to_api(self) -> None:
        # GIVEN assigned_to_me=True
        async def mock_list(*args, **kwargs):
            return
            yield  # pragma: no cover

        # WHEN I call list_async
        with patch(
            "synapseclient.models.curation.list_curation_tasks",
            side_effect=mock_list,
        ) as mock_api:
            async for _ in CurationTask.list_async(
                project_id=PROJECT_ID,
                assigned_to_me=True,
                synapse_client=self.syn,
            ):
                pass

            # THEN the API should be called with assigned_to_me=True
            mock_api.assert_called_once_with(
                project_id=PROJECT_ID,
                assigned_to_me=True,
                assignee_ids=None,
                state_filter=None,
                synapse_client=self.syn,
            )

    async def test_list_async_passes_assignee_ids_to_api(self) -> None:
        # GIVEN a list of assignee_ids
        async def mock_list(*args, **kwargs):
            return
            yield  # pragma: no cover

        # WHEN I call list_async
        with patch(
            "synapseclient.models.curation.list_curation_tasks",
            side_effect=mock_list,
        ) as mock_api:
            async for _ in CurationTask.list_async(
                project_id=PROJECT_ID,
                assignee_ids=["111", "222"],
                synapse_client=self.syn,
            ):
                pass

            # THEN the API should be called with the serialized assignee_ids
            mock_api.assert_called_once_with(
                project_id=PROJECT_ID,
                assigned_to_me=None,
                assignee_ids=["111", "222"],
                state_filter=None,
                synapse_client=self.syn,
            )

    async def test_list_async_passes_state_filter_to_api(self) -> None:
        # GIVEN a state_filter with TaskState enums
        async def mock_list(*args, **kwargs):
            return
            yield  # pragma: no cover

        # WHEN I call list_async
        with patch(
            "synapseclient.models.curation.list_curation_tasks",
            side_effect=mock_list,
        ) as mock_api:
            async for _ in CurationTask.list_async(
                project_id=PROJECT_ID,
                state_filter=[TaskState.NOT_STARTED, TaskState.IN_PROGRESS],
                synapse_client=self.syn,
            ):
                pass

            # THEN the API should be called with serialized string values
            mock_api.assert_called_once_with(
                project_id=PROJECT_ID,
                assigned_to_me=None,
                assignee_ids=None,
                state_filter=["NOT_STARTED", "IN_PROGRESS"],
                synapse_client=self.syn,
            )

    async def test_execute_async(self) -> None:
        """execute_async sends a ComputeTaskExecutionRequest and returns the details."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # AND a completed job carrying sample sheet generation execution details
        completed_request = ComputeTaskExecutionRequest(task_id=TASK_ID)
        completed_request.execution_details = SampleSheetGenerationExecutionDetails(
            async_job_id=ASYNC_JOB_ID, started_by=STARTED_BY, started_on=STARTED_ON
        )

        # WHEN I call execute_async
        with patch.object(
            ComputeTaskExecutionRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=completed_request,
        ) as mock_send_job:
            result = await task.execute_async(synapse_client=self.syn)

        # THEN the job should be awaited with the default timeout
        mock_send_job.assert_awaited_once_with(
            timeout=120,
            synapse_client=self.syn,
        )

        # AND the execution details from the response should be returned
        assert isinstance(result, SampleSheetGenerationExecutionDetails)
        assert result.async_job_id == ASYNC_JOB_ID
        assert result.started_by == STARTED_BY
        assert result.started_on == STARTED_ON

    async def test_execute_async_passes_timeout(self) -> None:
        """A caller-supplied timeout is forwarded to the async job."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)
        completed_request = ComputeTaskExecutionRequest(task_id=TASK_ID)
        completed_request.execution_details = RecordSetGenerationExecutionDetails(
            async_job_id=ASYNC_JOB_ID
        )

        # WHEN I call execute_async with a custom timeout
        with patch.object(
            ComputeTaskExecutionRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=completed_request,
        ) as mock_send_job:
            await task.execute_async(timeout=600, synapse_client=self.syn)

        # THEN the timeout should be forwarded
        mock_send_job.assert_awaited_once_with(
            timeout=600,
            synapse_client=self.syn,
        )

    async def test_execute_async_without_execution_details_raises(self) -> None:
        """A completed job that carries no execution details raises SynapseError."""
        # GIVEN a CurationTask with a task_id
        task = CurationTask(task_id=TASK_ID)

        # AND a completed job whose response carried no executionDetails
        completed_request = ComputeTaskExecutionRequest(task_id=TASK_ID)
        assert completed_request.execution_details is None

        # WHEN I call execute_async
        # THEN it should raise SynapseError rather than returning None, which callers
        # would dereference for started_on/error_message
        with patch.object(
            ComputeTaskExecutionRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=completed_request,
        ):
            with pytest.raises(
                SynapseError,
                match=f"execution job for CurationTask {TASK_ID} completed without",
            ):
                await task.execute_async(synapse_client=self.syn)

    async def test_execute_async_without_task_id(self) -> None:
        """execute_async raises ValueError when task_id is not set."""
        # GIVEN a CurationTask without a task_id
        task = CurationTask()

        # WHEN I call execute_async
        # THEN it should raise ValueError before any API call
        with pytest.raises(
            ValueError, match="task_id is required to execute a CurationTask"
        ):
            await task.execute_async(synapse_client=self.syn)

    async def test_list_async_state_filter_invalid_string_raises(self) -> None:
        # GIVEN a state_filter with an invalid string value
        # WHEN I call list_async
        # THEN it should raise ValueError before any API call
        with pytest.raises(ValueError, match="Invalid value"):
            async for _ in CurationTask.list_async(
                project_id=PROJECT_ID,
                state_filter=["invalid"],
                synapse_client=self.syn,
            ):
                pass  # pragma: no cover


class TestCurationTaskSynchronizeActiveGridSession:
    """Unit tests for CurationTask.synchronize_active_grid_session_async."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_record_based_returns_none_when_no_active_session(self) -> None:
        """When there is no active session, a warning is logged and None is returned; no new session is created."""
        # GIVEN a record-based task with no active grid session
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            ),
        )

        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(),
            ),
            patch.object(
                task,
                "create_grid_session_async",
                new_callable=AsyncMock,
            ) as mock_create_grid_session,
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            created_grid = MagicMock()
            created_grid.session_id = SESSION_ID
            mock_create_grid_session.return_value = created_grid

            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I synchronize the active grid session
            result = await task.synchronize_active_grid_session_async(
                sync_type=SyncType.PULL, synapse_client=self.syn
            )

            # THEN no session was created and nothing is returned
            assert result is None

    async def test_record_based_reuses_existing_session(self) -> None:
        """When a session is already active, it is reused and no new session is created."""
        # GIVEN a record-based task with an already-active grid session
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            ),
        )

        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(
                    active_session_id=SESSION_ID
                ),
            ),
            patch.object(
                task,
                "create_grid_session_async",
                new_callable=AsyncMock,
            ) as mock_create_grid_session,
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I synchronize the active grid session
            result = await task.synchronize_active_grid_session_async(
                sync_type=SyncType.PULL_PUSH, synapse_client=self.syn
            )

            # THEN no new grid session is created, and the existing session is synchronized
            mock_create_grid_session.assert_not_called()
            mock_grid_cls.assert_called_once_with(session_id=SESSION_ID)
            mock_grid.synchronize_async.assert_called_once_with(
                synapse_client=self.syn, sync_type=SyncType.PULL_PUSH
            )
            assert result is mock_grid

    async def test_record_based_explicit_none_sync_type_raises(self) -> None:
        """Record-based tasks reject an explicit sync_type=None with a clear
        ValueError, since PULL vs. PULL_PUSH is ambiguous for this task type."""
        # GIVEN a record-based task
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            ),
        )

        # WHEN I call synchronize_active_grid_session_async with sync_type=None
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="sync_type must be provided for RecordBasedMetadataTaskProperties",
        ):
            await task.synchronize_active_grid_session_async(
                sync_type=None, synapse_client=self.syn
            )

    async def test_unrecognized_sync_type_string_is_forward_compatible(self) -> None:
        """SyncType is forward-compatible, so an unrecognized string is passed
        through rather than rejected, in case the server has added a new value."""
        # GIVEN a record-based task with an already-active grid session
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            ),
        )

        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(
                    active_session_id=SESSION_ID
                ),
            ),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I call synchronize_active_grid_session_async with an unrecognized
            # string THEN no exception is raised, and the value is forwarded as-is
            await task.synchronize_active_grid_session_async(
                sync_type="INVALID", synapse_client=self.syn
            )

            mock_grid.synchronize_async.assert_called_once_with(
                synapse_client=self.syn, sync_type="INVALID"
            )

    async def test_valid_sync_type_string_is_coerced(self) -> None:
        """Valid sync_type strings are coerced to SyncType enum."""
        # GIVEN a record-based task with an already-active grid session
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            ),
        )

        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(
                    active_session_id=SESSION_ID
                ),
            ),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I pass sync_type as a string "PULL_PUSH"
            await task.synchronize_active_grid_session_async(
                sync_type="PULL_PUSH", synapse_client=self.syn
            )

            # THEN the string is coerced to the enum and used correctly
            mock_grid.synchronize_async.assert_called_once_with(
                synapse_client=self.syn, sync_type=SyncType.PULL_PUSH
            )

    async def test_file_based_ignores_sync_type(self) -> None:
        """File-based tasks always synchronize with PULL_PUSH regardless of the sync_type passed in."""
        # GIVEN a file-based task with an already-active grid session
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=FILE_VIEW_ID
            ),
        )

        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(
                    active_session_id=SESSION_ID
                ),
            ),
            patch.object(
                task, "create_grid_session_async", new_callable=AsyncMock
            ) as mock_create_grid_session,
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I pass sync_type=PULL (only valid for record-based tasks)
            await task.synchronize_active_grid_session_async(
                sync_type=SyncType.PULL, synapse_client=self.syn
            )

            # THEN the file-based task ignores it and always synchronizes with PULL_PUSH
            mock_create_grid_session.assert_not_called()
            mock_grid.synchronize_async.assert_called_once_with(
                synapse_client=self.syn, sync_type=SyncType.PULL_PUSH
            )

    async def test_file_based_ignores_none_sync_type(self) -> None:
        """sync_type is a required parameter for file-based tasks too, but its
        value (including None) is ignored -- file-based tasks always use
        PULL_PUSH."""
        # GIVEN a file-based task with an already-active grid session
        task = CurationTask(
            task_id=TASK_ID,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=UPLOAD_FOLDER_ID, file_view_id=FILE_VIEW_ID
            ),
        )

        with (
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(
                    active_session_id=SESSION_ID
                ),
            ),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I synchronize with sync_type explicitly set to None
            await task.synchronize_active_grid_session_async(
                sync_type=None, synapse_client=self.syn
            )

            # THEN it still resolves to PULL_PUSH without raising
            mock_grid.synchronize_async.assert_called_once_with(
                synapse_client=self.syn, sync_type=SyncType.PULL_PUSH
            )

    async def test_fetches_task_properties_when_missing(self) -> None:
        """If task_properties is not yet populated, it is fetched from Synapse first."""
        # GIVEN a CurationTask with only a task_id set (no task_properties)
        task = CurationTask(task_id=TASK_ID)

        async def fake_get_async(*, synapse_client=None):
            task.task_properties = RecordBasedMetadataTaskProperties(
                record_set_id=RECORD_SET_ID
            )
            return task

        with (
            patch.object(
                task, "get_async", new_callable=AsyncMock, side_effect=fake_get_async
            ) as mock_get_async,
            patch(
                "synapseclient.models.curation.get_curation_task_status",
                new_callable=AsyncMock,
                return_value=_get_curation_task_status_response(
                    active_session_id=SESSION_ID
                ),
            ),
            patch("synapseclient.models.curation.Grid") as mock_grid_cls,
        ):
            mock_grid = mock_grid_cls.return_value
            mock_grid.synchronize_async = AsyncMock(return_value=mock_grid)

            # WHEN I synchronize the active grid session
            await task.synchronize_active_grid_session_async(
                sync_type=SyncType.PULL_PUSH, synapse_client=self.syn
            )

            # THEN task_properties was fetched before the type check ran
            mock_get_async.assert_called_once_with(synapse_client=self.syn)
            assert isinstance(task.task_properties, RecordBasedMetadataTaskProperties)

    async def test_without_task_id_raises(self) -> None:
        """Without a task_id, fetching task_properties fails with ValueError."""
        # GIVEN a CurationTask with neither task_id nor task_properties set
        task = CurationTask()

        # WHEN I call synchronize_active_grid_session_async
        # THEN it should raise ValueError (propagated from get_async)
        with pytest.raises(ValueError, match="task_id is required to get"):
            await task.synchronize_active_grid_session_async(
                sync_type=SyncType.PULL_PUSH, synapse_client=self.syn
            )

    async def test_unsupported_task_properties_type_raises(self) -> None:
        """Unsupported task_properties types raise ValueError with the type name."""

        # GIVEN a fake task properties type that doesn't exist yet
        class ComputeBasedMetadataTaskProperties:
            """A hypothetical future task properties type."""

            pass

        # AND a CurationTask with this unsupported type
        task = CurationTask(task_id=TASK_ID)
        task.task_properties = ComputeBasedMetadataTaskProperties()

        # WHEN I call synchronize_active_grid_session_async
        # THEN it should raise ValueError mentioning the actual type name
        with pytest.raises(
            ValueError,
            match="Synchronization only supports FileBasedMetadataTaskProperties or "
            "RecordBasedMetadataTaskProperties, got ComputeBasedMetadataTaskProperties",
        ):
            await task.synchronize_active_grid_session_async(
                sync_type=SyncType.PULL_PUSH, synapse_client=self.syn
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
        # AND the owner principal id is coerced from the response string to an int
        assert grid.owner_principal_id == OWNER_PRINCIPAL_ID
        assert isinstance(grid.owner_principal_id, int)
        # AND the authorization mode is coerced from the string to the enum
        assert grid.authorization_mode == AuthorizationMode.SESSION_OWNER
        assert isinstance(grid.authorization_mode, AuthorizationMode)

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

            # THEN the grid should be populated with session data, including the
            # authorization mode coerced from the response string to the enum
            assert result.session_id == SESSION_ID
            assert result.started_by == STARTED_BY
            assert result.started_on == STARTED_ON
            assert result.source_entity_id == SOURCE_ENTITY_ID
            assert result.authorization_mode == AuthorizationMode.SESSION_OWNER

    async def test_create_async_forwards_authorization_mode_to_request(self) -> None:
        # GIVEN a Grid with a record_set_id and an explicit authorization mode
        grid = Grid(
            record_set_id=RECORD_SET_ID,
            authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
        )

        # WHEN I call create_async (patching the CreateGridRequest the Grid builds)
        with patch(
            "synapseclient.models.curation.CreateGridRequest"
        ) as mock_request_cls:
            mock_request = mock_request_cls.return_value
            mock_request.send_job_and_wait_async = AsyncMock(return_value=mock_request)
            await grid.create_async(synapse_client=self.syn)

            # THEN the request is constructed once with the grid's authorization mode
            # forwarded alongside the other session parameters
            mock_request_cls.assert_called_once_with(
                record_set_id=RECORD_SET_ID,
                initial_query=None,
                owner_principal_id=None,
                authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
            )

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
        with (
            patch("synapseclient.models.curation.os.path.isfile", return_value=True),
            pytest.raises(
                ValueError,
                match="session_id is required to import a CSV into a GridSession",
            ),
        ):
            await grid.import_csv_async(synapse_client=self.syn, path="test.csv")

    async def test_import_csv_async(self) -> None:
        """Test the import_csv_async method of the Grid class, ensuring it correctly calls the preview and import requests and logs the results."""
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        csv_table_descriptor = CsvTableDescriptor(
            separator=";",
            quote_character='"',
            escape_character="\\",
            line_end=os.linesep,
            is_first_line_header=True,
        )
        expected_columns = [Column(name="col1", column_type="STRING", maximum_size=50)]

        # Mock preview response with suggested columns
        mock_preview_response = UploadToTablePreviewRequest(
            upload_file_handle_id=FILE_HANDLE_ID,
            csv_table_descriptor=csv_table_descriptor,
            suggested_columns=expected_columns,
            sample_rows=[["value1"]],
            rows_scanned=1,
        )
        # Mock import response with row counts
        mock_import_response = GridCsvImportRequest(
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            schema=expected_columns,
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
            patch("synapseclient.models.curation.os.path.isfile", return_value=True),
            patch(
                "synapseclient.models.curation.upload_synapse_s3",
                new_callable=AsyncMock,
                return_value={"id": FILE_HANDLE_ID},
            ),
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
                path="test.csv",
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
            csv_descriptor=csv_table_descriptor,
        )

        # AND the log message contains the import counts
        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "total count: 1" in log_message
        assert "total created: 1" in log_message
        assert "total updated: 1" in log_message

    async def test_import_csv_no_suggested_columns_async(self) -> None:
        """Raises ValueError when the CSV preview returns no suggested columns."""
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        csv_table_descriptor = CsvTableDescriptor(
            separator=";",
            quote_character='"',
            escape_character="\\",
            line_end=os.linesep,
            is_first_line_header=True,
        )

        # AND a preview response with no columns (e.g. empty CSV file)
        mock_preview_response = UploadToTablePreviewRequest(
            upload_file_handle_id=FILE_HANDLE_ID,
            csv_table_descriptor=csv_table_descriptor,
            suggested_columns=[],
            sample_rows=[],
            rows_scanned=0,
        )

        mock_preview_instance = MagicMock()
        mock_preview_instance.send_job_and_wait_async = AsyncMock(
            return_value=mock_preview_response
        )

        # WHEN I call import_csv_async
        # THEN a ValueError is raised before the import is attempted
        with (
            patch("synapseclient.models.curation.os.path.isfile", return_value=True),
            patch(
                "synapseclient.models.curation.upload_synapse_s3",
                new_callable=AsyncMock,
                return_value={"id": FILE_HANDLE_ID},
            ),
            patch(
                "synapseclient.models.curation.UploadToTablePreviewRequest",
                return_value=mock_preview_instance,
            ),
        ):
            with pytest.raises(
                ValueError,
                match=rf"CSV preview for file handle {FILE_HANDLE_ID} returned no suggested columns \(rows scanned: 0\).*separator=';'",
            ):
                await grid.import_csv_async(
                    synapse_client=self.syn,
                    path="test.csv",
                    csv_table_descriptor=csv_table_descriptor,
                )


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
        # AND the owner principal id is coerced from the response string to an int
        assert grid.owner_principal_id == OWNER_PRINCIPAL_ID
        assert isinstance(grid.owner_principal_id, int)
        # AND the authorization mode is coerced from the response string to the enum
        assert grid.authorization_mode == AuthorizationMode.SESSION_OWNER
        assert isinstance(grid.authorization_mode, AuthorizationMode)

    def test_to_synapse_request_with_record_set_id(self) -> None:
        # GIVEN a CreateGridRequest with a record_set_id
        request = CreateGridRequest(record_set_id=RECORD_SET_ID)

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should contain the correct fields
        assert "concreteType" in result
        assert result["recordSetId"] == RECORD_SET_ID
        assert "initialQuery" not in result
        # AND the absent authorization mode is dropped by delete_none_keys
        assert "authorizationMode" not in result

    def test_to_synapse_request_with_authorization_mode(self) -> None:
        # GIVEN a CreateGridRequest with the authorization mode supplied as a string
        request = CreateGridRequest(
            record_set_id=RECORD_SET_ID,
            authorization_mode="SOURCE_BENEFACTOR",
        )

        # THEN the string is coerced to the enum on assignment by EnumCoercionMixin
        assert request.authorization_mode == AuthorizationMode.SOURCE_BENEFACTOR
        assert isinstance(request.authorization_mode, AuthorizationMode)

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN the enum value is serialized back to its string form
        assert result["authorizationMode"] == "SOURCE_BENEFACTOR"


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
        preview_req = UploadToTablePreviewRequest(upload_file_handle_id=FILE_HANDLE_ID)
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

    def test_to_synapse_request_with_minimal_fields(self) -> None:
        # GIVEN an UploadToTablePreviewRequest with only required fields set
        preview_req = UploadToTablePreviewRequest(
            upload_file_handle_id=FILE_HANDLE_ID,
        )

        # WHEN I convert it to a synapse request
        result = preview_req.to_synapse_request()

        # THEN it should contain the correct fields and defaults
        assert result["concreteType"] == UPLOAD_TO_TABLE_PREVIEW_REQUEST
        assert result["uploadFileHandleId"] == FILE_HANDLE_ID
        assert result["csvTableDescriptor"] == CsvTableDescriptor().to_synapse_request()

    def test_to_synapse_request_with_all_fields(self) -> None:
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
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            schema=[Column(name="col1", column_type="STRING")],
        )
        result = import_req.fill_from_dict(raw_synapse_response)

        # THEN the response fields should be populated correctly
        assert result.session_id == SESSION_ID
        assert result.total_count == 3
        assert result.created_count == 1
        assert result.updated_count == 2

    def test_to_synapse_request_with_minimal_fields(self) -> None:
        # GIVEN a GridCsvImportRequest with only required fields set
        import_req = GridCsvImportRequest(
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            schema=[Column(name="col1", column_type="STRING")],
        )

        # WHEN I convert it to a synapse request
        result = import_req.to_synapse_request()

        # THEN it should contain the correct fields and defaults
        assert result["concreteType"] == GRID_CSV_IMPORT_REQUEST
        assert result["sessionId"] == SESSION_ID
        assert result["fileHandleId"] == FILE_HANDLE_ID
        assert result["csvDescriptor"] == CsvTableDescriptor().to_synapse_request()
        assert len(result["schema"]) == 1
        assert (
            result["schema"][0]
            == Column(name="col1", column_type="STRING").to_synapse_request()
        )

    def test_to_synapse_request_with_all_fields(self) -> None:
        # GIVEN a GridCsvImportRequest with all fields set
        import_req = GridCsvImportRequest(
            session_id=SESSION_ID,
            file_handle_id=FILE_HANDLE_ID,
            csv_descriptor=CsvTableDescriptor(
                separator=";",
                quote_character='"',
                escape_character="\\",
                line_end="\t",
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
        assert result["csvDescriptor"]["separator"] == ";"
        assert result["csvDescriptor"]["quoteCharacter"] == '"'
        assert result["csvDescriptor"]["escapeCharacter"] == "\\"
        assert result["csvDescriptor"]["lineEnd"] == "\t"
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


class TestDownloadFromGridRequest:
    """Tests for the DownloadFromGridRequest helper dataclass."""

    def test_to_synapse_request(self) -> None:
        # GIVEN a DownloadFromGridRequest with a session_id
        request = DownloadFromGridRequest(session_id=SESSION_ID)

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should contain the correct fields
        assert "concreteType" in result
        assert result["sessionId"] == SESSION_ID

    def test_to_synapse_request_all_fields(self) -> None:
        # GIVEN a DownloadFromGridRequest with all fields set
        table_descriptor = CsvTableDescriptor(
            quote_character='"',
            escape_character="\\",
            line_end=os.linesep,
            separator=";",
            is_first_line_header=False,
        )
        request = DownloadFromGridRequest(
            session_id=SESSION_ID,
            write_header=False,
            include_row_id_and_row_version=False,
            include_etag=False,
            csv_table_descriptor=table_descriptor,
            file_name="my_grid_data.csv",
        )

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should contain all the correct fields
        assert "concreteType" in result
        assert result["sessionId"] == SESSION_ID
        assert result["includeRowIdAndRowVersion"] is False
        assert result["includeEtag"] is False
        assert result["fileName"] == "my_grid_data.csv"
        assert result["csvTableDescriptor"]["quoteCharacter"] == '"'
        assert result["csvTableDescriptor"]["escapeCharacter"] == "\\"
        assert result["csvTableDescriptor"]["lineEnd"] == os.linesep
        assert result["csvTableDescriptor"]["separator"] == ";"
        assert result["csvTableDescriptor"]["isFirstLineHeader"] is False

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with download data
        raw_synapse_response = {
            "jobId": "123",
            "concreteType": "org.sagebionetworks.repo.model.grid.DownloadFromGridResult",
            "sessionId": SESSION_ID,
            "resultsFileHandleId": FILE_HANDLE_ID,
        }
        response = DownloadFromGridRequest(session_id=SESSION_ID).fill_from_dict(
            raw_synapse_response
        )
        assert response.session_id == SESSION_ID
        assert response.results_file_handle_id == FILE_HANDLE_ID


class TestGridDownloadCsv:

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_download_csv_without_session_id(self) -> None:
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call download_csv
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="session_id is required to download"):
            await grid.download_csv_async(synapse_client=self.syn)

    async def test_download_csv_async(self):
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        # Mock the DownloadFromGridRequest's send_job_and_wait_async
        mock_download_request = DownloadFromGridRequest(session_id=SESSION_ID)
        mock_download_request.results_file_handle_id = FILE_HANDLE_ID

        mock_file_handle = {
            "id": "172705303",
            "etag": "mock-etag",
            "createdBy": "3443707",
            "createdOn": "2026-04-30T21:21:40.000Z",
            "modifiedOn": "2026-04-30T21:21:40.000Z",
            "concreteType": "org.sagebionetworks.repo.model.file.S3FileHandle",
            "contentType": "text/csv",
            "contentMd5": "mock-md5",
            "fileName": "Job-1234.csv",
            "contentSize": 100,
            "status": "AVAILABLE",
            "bucketName": "proddata.sagebase.org",
            "key": "1234/5678/Job-1234.csv",
            "isPreview": False,
        }
        mock_presigned_url = "https://presigned.example.com/file.csv"
        expected_download_path = "/tmp/downloaded.csv"

        with (
            patch.object(
                DownloadFromGridRequest,
                "send_job_and_wait_async",
                new_callable=AsyncMock,
                return_value=mock_download_request,
            ) as mock_send,
            patch(
                "synapseclient.models.curation.get_file_handle",
                new_callable=AsyncMock,
                return_value=mock_file_handle,
            ) as mock_get_file_handle,
            patch(
                "synapseclient.models.curation.get_file_handle_presigned_url",
                new_callable=AsyncMock,
                return_value=mock_presigned_url,
            ) as mock_get_presigned_url,
            patch(
                "synapseclient.models.curation.download_from_url",
                return_value=expected_download_path,
            ) as mock_download_from_url,
        ):
            result = await grid.download_csv_async(synapse_client=self.syn)
            current_dir = os.getcwd()

            # THEN the async job should be submitted
            mock_send.assert_called_once()

            # AND the file handle metadata should be fetched
            mock_get_file_handle.assert_called_once_with(
                file_handle_id=FILE_HANDLE_ID,
                synapse_client=self.syn,
            )

            # AND a presigned URL should be fetched
            mock_get_presigned_url.assert_called_once_with(
                file_handle_id=FILE_HANDLE_ID,
                synapse_client=self.syn,
            )

            # AND download_from_url should be called with the presigned URL and MD5
            call_kwargs = mock_download_from_url.call_args
            assert call_kwargs.kwargs["url"] == mock_presigned_url
            assert call_kwargs.kwargs["file_handle_id"] == mock_file_handle["id"]
            assert call_kwargs.kwargs["expected_md5"] == mock_file_handle["contentMd5"]
            assert call_kwargs.kwargs["url_is_presigned"] is True
            # AND the destination filename follows the grid_{session_id}-{timestamp}.csv convention
            assert call_kwargs.kwargs["destination"].startswith(current_dir)
            assert f"grid_{SESSION_ID}-" in call_kwargs.kwargs["destination"]
            assert call_kwargs.kwargs["destination"].endswith(".csv")

            # AND the result is the path returned by download_from_url
            assert result == expected_download_path

    async def test_download_csv_async_with_custom_file_name(self):
        # GIVEN a Grid with a session_id and a caller-supplied file_name
        grid = Grid(session_id=SESSION_ID)
        custom_file_name = "my_export.csv"

        mock_download_request = DownloadFromGridRequest(session_id=SESSION_ID)
        mock_download_request.results_file_handle_id = FILE_HANDLE_ID

        mock_file_handle = {
            "id": "172705303",
            "contentMd5": "mock-md5",
            "fileName": "Job-1234.csv",
        }
        expected_download_path = "/tmp/my_export.csv"

        with (
            patch.object(
                DownloadFromGridRequest,
                "send_job_and_wait_async",
                new_callable=AsyncMock,
                return_value=mock_download_request,
            ),
            patch(
                "synapseclient.models.curation.get_file_handle",
                new_callable=AsyncMock,
                return_value=mock_file_handle,
            ),
            patch(
                "synapseclient.models.curation.get_file_handle_presigned_url",
                new_callable=AsyncMock,
                return_value="https://presigned.example.com/file.csv",
            ),
            patch(
                "synapseclient.models.curation.download_from_url",
                return_value=expected_download_path,
            ) as mock_download_from_url,
        ):
            result = await grid.download_csv_async(
                file_name=custom_file_name, synapse_client=self.syn
            )
            current_dir = os.getcwd()

            # THEN the destination uses exactly the caller-supplied file_name
            call_kwargs = mock_download_from_url.call_args
            assert call_kwargs.kwargs["destination"] == os.path.join(
                current_dir, custom_file_name
            )

            # AND the result is the path returned by download_from_url
            assert result == expected_download_path

    async def test_download_csv_async_with_invalid_dir(self):
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        # WHEN I call download_csv_async with an invalid destination
        with pytest.raises(
            ValueError, match="Destination ./nonexistent_dir is not a valid directory."
        ):
            await grid.download_csv_async(
                synapse_client=self.syn, destination="./nonexistent_dir"
            )

    async def test_download_csv_async_empty_file_handle_id(self):
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)

        # Mock the DownloadFromGridRequest's send_job_and_wait_async to return an empty file handle ID
        mock_download_request = DownloadFromGridRequest(session_id=SESSION_ID)
        mock_download_request.results_file_handle_id = ""

        with patch.object(
            DownloadFromGridRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_download_request,
        ):
            # WHEN I call download_csv_async
            # THEN it should raise ValueError for empty file handle ID
            with pytest.raises(
                ValueError,
                match=f"Download job for grid session '{SESSION_ID}' completed but "
                "did not return a file handle ID. The CSV result may be empty or "
                "the job may have failed silently.",
            ):
                await grid.download_csv_async(synapse_client=self.syn)


class TestSynchronizeGridRequest:

    @pytest.mark.parametrize(
        "sync_type",
        [None, SyncType.PULL, SyncType.PULL_PUSH, "PULL", "PULL_PUSH"],
        ids=["omitted", "pull", "pull_push", "string_pull", "string_pull_push"],
    )
    def test_to_synapse_request(self, sync_type: SyncType) -> None:
        # GIVEN a SynchronizeGridRequest with the given sync_type
        sync_req = SynchronizeGridRequest(
            grid_session_id=SESSION_ID, sync_type=sync_type
        )

        # WHEN I convert it to a synapse request
        result = sync_req.to_synapse_request()

        # THEN it should contain the correct fields
        assert "concreteType" in result
        assert result["gridSessionId"] == SESSION_ID

        # AND syncType is omitted when not set, and EnumCoercionMixin normalizes it to a SyncType
        # member on assignment
        if sync_type is None:
            assert "syncType" not in result
        else:
            assert result["syncType"] == SyncType(sync_type).value

    def test_unrecognized_sync_type_is_forward_compatible(self) -> None:
        # GIVEN a sync_type that doesn't match any declared SyncType member
        # (wrong case, or a value the server may add in the future)
        # WHEN constructing a SynchronizeGridRequest with it
        # THEN SyncType is forward-compatible, so it is accepted as-is
        # rather than rejected, in case the server has added a new value
        sync_req = SynchronizeGridRequest(grid_session_id=SESSION_ID, sync_type="pull")
        assert sync_req.sync_type == "pull"

        sync_req = SynchronizeGridRequest(
            grid_session_id=SESSION_ID, sync_type="NOT_REAL"
        )
        assert sync_req.sync_type == "NOT_REAL"

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with synchronize grid session data
        raw_response = {
            "jobId": "1234",
            "concreteType": "org.sagebionetworks.repo.model.grid.SynchronizeGridResponse",
            "gridSessionId": SESSION_ID,
            "errorMessages": ["test_error"],
        }

        # WHEN I fill a SynchronizeGridRequest from the response
        sync_req = SynchronizeGridRequest(grid_session_id=SESSION_ID)
        response = sync_req.fill_from_dict(raw_response)
        assert "test_error" in response.error_messages
        assert response.grid_session_id == SESSION_ID


class TestSynchronizeGrid:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_synchronize_grid_async_without_session_id_raises(self) -> None:
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call synchronize_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="session_id is required to synchronize"):
            await grid.synchronize_async(synapse_client=self.syn)

    async def test_synchronize_grid_async_empty_error(self) -> None:
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)
        mock_sync_response = SynchronizeGridRequest(
            grid_session_id=SESSION_ID,
            error_messages=[],
        )

        # WHEN I call synchronize_async
        with patch(
            "synapseclient.models.curation.SynchronizeGridRequest.send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_sync_response,
        ) as mock_sync:
            await grid.synchronize_async(synapse_client=self.syn)

            # THEN the API should be called with the session_id
            mock_sync.assert_called_once_with(synapse_client=self.syn, timeout=120)

    async def test_synchronize_grid_async_with_errors(self) -> None:
        # GIVEN a Grid with a session_id
        grid = Grid(session_id=SESSION_ID)
        mock_sync_response = SynchronizeGridRequest(
            grid_session_id=SESSION_ID,
            error_messages=["sync_error_1", "sync_error_2"],
        )

        # WHEN I call synchronize_async
        with patch(
            "synapseclient.models.curation.SynchronizeGridRequest.send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_sync_response,
        ):
            with patch.object(self.syn, "logger") as mock_logger:
                await grid.synchronize_async(synapse_client=self.syn)

                # THEN the error messages should be logged as an error
                mock_logger.error.assert_called_once()
                error_message = mock_logger.error.call_args[0][0]
                assert "sync_error_1" in error_message
                assert "sync_error_2" in error_message


class TestSelectColumn:
    """Tests for the SelectColumn dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with a column name
        response = {"columnName": "diagnosis"}

        # WHEN I fill a SelectColumn from the response
        result = SelectColumn().fill_from_dict(response)

        # THEN the column_name should be populated
        assert result.column_name == "diagnosis"

    def test_fill_from_dict_missing_column_name(self) -> None:
        # GIVEN a response without a column name
        # WHEN I fill a SelectColumn from the response
        result = SelectColumn().fill_from_dict({})

        # THEN the column_name should be None
        assert result.column_name is None


class TestGridRow:
    """Tests for the GridRow dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with row data and validation results
        response = {
            "rowId": "1.2",
            "data": {"diagnosis": "flu"},
            "validationResults": {"isValid": True},
        }

        # WHEN I fill a GridRow from the response
        result = GridRow().fill_from_dict(response)

        # THEN the fields should be populated
        assert result.row_id == "1.2"
        assert result.data == {"diagnosis": "flu"}
        assert isinstance(result.validation_results, GridQueryValidationResult)
        assert result.validation_results.is_valid is True

    def test_fill_from_dict_without_validation_results(self) -> None:
        # GIVEN a response without validation results
        response = {"rowId": "1.3", "data": {"diagnosis": "cold"}}

        # WHEN I fill a GridRow from the response
        result = GridRow().fill_from_dict(response)

        # THEN validation_results should be None
        assert result.row_id == "1.3"
        assert result.validation_results is None


class TestGridQueryValidationResult:
    """Tests for the GridQueryValidationResult dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with validation result data
        response = {
            "isValid": False,
            "validationErrorMessage": "#: only 1 subschema matches out of 2",
            "allValidationMessages": ["error one", "error two"],
        }

        # WHEN I fill a GridQueryValidationResult from the response
        result = GridQueryValidationResult().fill_from_dict(response)

        # THEN the fields should be populated
        assert result.is_valid is False
        assert result.validation_error_message == (
            "#: only 1 subschema matches out of 2"
        )
        assert result.all_validation_messages == ["error one", "error two"]

    def test_fill_from_dict_valid_row(self) -> None:
        # GIVEN a response for a valid row with no error messages
        response = {"isValid": True}

        # WHEN I fill a GridQueryValidationResult from the response
        result = GridQueryValidationResult().fill_from_dict(response)

        # THEN is_valid should be True and the message fields should be None
        assert result.is_valid is True
        assert result.validation_error_message is None
        assert result.all_validation_messages is None


class TestGridQueryResult:
    """Tests for the GridQueryResult dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with select columns and rows
        response = {
            "selectColumns": [{"columnName": "diagnosis"}, {"columnName": "age"}],
            "rows": [
                {"rowId": "1.1", "data": {"diagnosis": "flu", "age": 30}},
                {"rowId": "1.2", "data": {"diagnosis": "cold", "age": 40}},
            ],
        }

        # WHEN I fill a GridQueryResult from the response
        result = GridQueryResult().fill_from_dict(response)

        # THEN the select_columns and rows should be populated
        assert len(result.select_columns) == 2
        assert all(isinstance(col, SelectColumn) for col in result.select_columns)
        assert result.select_columns[0].column_name == "diagnosis"
        assert len(result.rows) == 2
        assert all(isinstance(row, GridRow) for row in result.rows)
        assert result.rows[1].row_id == "1.2"

    def test_fill_from_dict_empty_response(self) -> None:
        # GIVEN a response with no select columns or rows
        # WHEN I fill a GridQueryResult from the response
        result = GridQueryResult().fill_from_dict({})

        # THEN both fields should be None
        assert result.select_columns is None
        assert result.rows is None


class TestSelectItemSubclasses:
    """Tests for the SelectItem subclasses: SelectByName, SelectAll, CountStar,
    and SelectSelection."""

    def test_select_by_name_to_synapse_request(self) -> None:
        # GIVEN a SelectByName with a column name
        item = SelectByName(column_name="diagnosis")

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should contain the concreteType and columnName
        assert result == {"concreteType": SELECT_BY_NAME, "columnName": "diagnosis"}

    def test_select_all_to_synapse_request(self) -> None:
        # GIVEN a SelectAll item
        item = SelectAll()

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should only contain the concreteType
        assert result == {"concreteType": SELECT_ALL}

    def test_count_star_to_synapse_request_without_alias(self) -> None:
        # GIVEN a CountStar with no alias
        item = CountStar()

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN the alias key should be omitted
        assert result == {"concreteType": COUNT_STAR}

    def test_select_selection_to_synapse_request(self) -> None:
        # GIVEN a SelectSelection item
        item = SelectSelection()

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should only contain the concreteType
        assert result == {"concreteType": SELECT_SELECTION}


class TestFilterSubclasses:
    """Tests for the Filter subclasses: RowValidationResultFilter, CellValueFilter,
    RowSelectionFilter, RowIsValidFilter, and RowIdFilter."""

    def test_row_validation_result_filter_to_synapse_request(self) -> None:
        # GIVEN a RowValidationResultFilter constructed with a string operator
        item = RowValidationResultFilter(
            operator="LIKE", validation_result_value="%expected type:%"
        )

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN the operator should be serialized as its string value
        assert result == {
            "concreteType": ROW_VALIDATION_RESULT_FILTER,
            "operator": "LIKE",
            "validationResultValue": "%expected type:%",
        }

    def test_cell_value_filter_to_synapse_request(self) -> None:
        # GIVEN a CellValueFilter with an enum operator
        item = CellValueFilter(
            column_name="Project",
            operator=CellValueOperator.EQUALS,
            value=["Alpha"],
        )

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should contain the correct fields
        assert result == {
            "concreteType": CELL_VALUE_FILTER,
            "columnName": "Project",
            "operator": "EQUALS",
            "value": ["Alpha"],
        }

    def test_row_selection_filter_to_synapse_request(self) -> None:
        # GIVEN a RowSelectionFilter
        item = RowSelectionFilter(is_selected=False)

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should contain the correct fields
        assert result == {
            "concreteType": ROW_SELECTION_FILTER,
            "isSelected": False,
        }

    def test_row_is_valid_filter_to_synapse_request(self) -> None:
        # GIVEN a RowIsValidFilter
        item = RowIsValidFilter(value=True)

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should contain the correct fields
        assert result == {"concreteType": ROW_IS_VALID_FILTER, "value": True}

    def test_row_id_filter_to_synapse_request(self) -> None:
        # GIVEN a RowIdFilter
        item = RowIdFilter(row_ids_in=["1.1", "1.2"])

        # WHEN I convert it to a synapse request
        result = item.to_synapse_request()

        # THEN it should contain the correct fields
        assert result == {
            "concreteType": ROW_ID_FILTER,
            "rowIdsIn": ["1.1", "1.2"],
        }


class TestGridQuery:
    """Tests for the GridQuery dataclass."""

    def test_to_synapse_request(self) -> None:
        # GIVEN a GridQuery with select items and filters
        query = GridQuery(
            column_selection=[SelectAll(), SelectByName(column_name="diagnosis")],
            filters=[RowIsValidFilter(value=True)],
            limit=25,
            offset=5,
            include_validation_messages=False,
        )

        # WHEN I convert it to a synapse request
        result = query.to_synapse_request()

        # THEN it should contain the serialized select items and filters
        assert result["columnSelection"] == [
            {"concreteType": SELECT_ALL},
            {"concreteType": SELECT_BY_NAME, "columnName": "diagnosis"},
        ]
        assert result["filters"] == [
            {"concreteType": ROW_IS_VALID_FILTER, "value": True}
        ]
        assert result["limit"] == 25
        assert result["offset"] == 5
        assert result["includeValidationMessages"] is False

    def test_to_synapse_request_without_filters(self) -> None:
        # GIVEN a GridQuery with no filters set
        query = GridQuery(column_selection=[SelectAll()], limit=10)

        # WHEN I convert it to a synapse request
        result = query.to_synapse_request()

        # THEN the filters key should be omitted
        assert "filters" not in result
        assert result["columnSelection"] == [{"concreteType": SELECT_ALL}]
        assert result["limit"] == 10

    def test_to_synapse_request_with_empty_column_selection_raises(self) -> None:
        # GIVEN a GridQuery with no column_selection set
        query = GridQuery()

        # WHEN I convert it to a synapse request
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="column_selection is required"):
            query.to_synapse_request()


class TestQueryRequest:
    """Tests for the QueryRequest dataclass."""

    def test_to_synapse_request(self) -> None:
        # GIVEN a QueryRequest with a GridQuery
        request = QueryRequest(
            query=GridQuery(column_selection=[SelectAll()], limit=10)
        )

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should contain the serialized query
        assert result["query"]["columnSelection"] == [{"concreteType": SELECT_ALL}]
        assert result["query"]["limit"] == 10

    def test_to_synapse_request_without_query(self) -> None:
        # GIVEN a QueryRequest with no query set
        request = QueryRequest()

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN the query key should be omitted
        assert result == {}


class TestGridQueryJobRequest:
    """Tests for the GridQueryJobRequest dataclass."""

    def test_to_synapse_request(self) -> None:
        # GIVEN a GridQueryJobRequest with a query
        job_request = GridQueryJobRequest(
            session_id=SESSION_ID,
            replica_id=REPLICA_ID,
            query_request=QueryRequest(
                query=GridQuery(column_selection=[SelectAll()], limit=50)
            ),
        )

        # WHEN I convert it to a synapse request
        result = job_request.to_synapse_request()

        # THEN it should contain the correct fields
        assert result["concreteType"] == GRID_QUERY_JOB_REQUEST
        assert result["sessionId"] == SESSION_ID
        assert result["replicaId"] == REPLICA_ID
        assert result["queryRequest"]["query"]["columnSelection"] == [
            {"concreteType": SELECT_ALL}
        ]
        assert result["queryRequest"]["query"]["limit"] == 50

    def test_to_synapse_request_with_default_query_request_raises(self) -> None:
        # GIVEN a GridQueryJobRequest with no query set on its query_request
        job_request = GridQueryJobRequest(session_id=SESSION_ID, replica_id=REPLICA_ID)

        # WHEN I convert it to a synapse request
        # THEN it should raise ValueError since Synapse would reject the request
        with pytest.raises(ValueError, match="query_request.query is required"):
            job_request.to_synapse_request()

    def test_to_synapse_request_with_query_request_none_raises(self) -> None:
        # GIVEN a GridQueryJobRequest with query_request explicitly set to None
        job_request = GridQueryJobRequest(
            session_id=SESSION_ID, replica_id=REPLICA_ID, query_request=None
        )

        # WHEN I convert it to a synapse request
        # THEN it should raise ValueError since Synapse would reject the request
        with pytest.raises(ValueError, match="query_request.query is required"):
            job_request.to_synapse_request()

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with a queryResult
        response = {
            "concreteType": "org.sagebionetworks.repo.model.grid.GridQueryJobResponse",
            "queryResult": {
                "selectColumns": [{"columnName": "diagnosis"}],
                "rows": [{"rowId": "1.1", "data": {"diagnosis": "flu"}}],
            },
        }

        # WHEN I fill a GridQueryJobRequest from the response
        job_request = GridQueryJobRequest(session_id=SESSION_ID, replica_id=REPLICA_ID)
        job_request.fill_from_dict(response)

        # THEN query_result should be populated as a GridQueryResult
        assert isinstance(job_request.query_result, GridQueryResult)
        assert job_request.query_result.select_columns[0].column_name == "diagnosis"
        assert job_request.query_result.rows[0].row_id == "1.1"

    def test_fill_from_dict_without_query_result(self) -> None:
        # GIVEN a response without a queryResult
        response = {
            "concreteType": "org.sagebionetworks.repo.model.grid.GridQueryJobResponse"
        }

        # WHEN I fill a GridQueryJobRequest from the response
        job_request = GridQueryJobRequest(session_id=SESSION_ID, replica_id=REPLICA_ID)
        job_request.fill_from_dict(response)

        # THEN query_result should be None
        assert job_request.query_result is None


class TestGridReplica:
    """Tests for the GridReplica dataclass."""

    def test_fill_from_dict(self) -> None:
        # GIVEN a response with replica data
        response = {
            "gridSessionId": SESSION_ID,
            "replicaId": REPLICA_ID,
            "createdBy": CREATED_BY,
            "isAgentReplica": False,
            "createdOn": CREATED_ON,
        }

        # WHEN I fill a GridReplica from the response
        result = GridReplica().fill_from_dict(response)

        # THEN the fields should be populated
        assert result.grid_session_id == SESSION_ID
        assert result.replica_id == REPLICA_ID
        assert result.created_by == CREATED_BY
        assert result.is_agent_replica is False
        assert result.created_on == CREATED_ON


class TestCreateReplicaRequest:
    """Tests for the CreateReplicaRequest dataclass."""

    def test_to_synapse_request(self) -> None:
        # GIVEN a CreateReplicaRequest with a grid_session_id
        request = CreateReplicaRequest(grid_session_id=SESSION_ID)

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should contain the gridSessionId
        assert result == {"gridSessionId": SESSION_ID}


class TestGridCreateReplica:
    """Tests for Grid._create_replica_async."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_create_replica_async_without_session_id_raises(self) -> None:
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call _create_replica_async
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="session_id is required to create a replica"
        ):
            await grid._create_replica_async(synapse_client=self.syn)

    async def test_create_replica_async_returns_grid_replica(self) -> None:
        # GIVEN a Grid with a session_id and a mocked API response
        grid = Grid(session_id=SESSION_ID)
        mock_response = {
            "replica": {
                "gridSessionId": SESSION_ID,
                "replicaId": REPLICA_ID,
                "createdBy": CREATED_BY,
                "isAgentReplica": False,
                "createdOn": CREATED_ON,
            }
        }

        # WHEN I call _create_replica_async
        with patch(
            "synapseclient.models.curation.create_grid_replica",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_create:
            result = await grid._create_replica_async(synapse_client=self.syn)

            # THEN the API should be called with the session_id and request body
            mock_create.assert_called_once_with(
                session_id=SESSION_ID,
                create_replica_request={"gridSessionId": SESSION_ID},
                synapse_client=self.syn,
            )

            # THEN the result should be a populated GridReplica
            assert isinstance(result, GridReplica)
            assert result.replica_id == REPLICA_ID
            assert result.grid_session_id == SESSION_ID
            assert result.created_by == CREATED_BY
            assert result.is_agent_replica is False
            assert result.created_on == CREATED_ON

    async def test_create_replica_async_raises_without_replica_in_response(
        self,
    ) -> None:
        # GIVEN a Grid with a session_id and a response with no replica data
        grid = Grid(session_id=SESSION_ID)

        # WHEN I call _create_replica_async
        # THEN it should raise ValueError since no replica was returned
        with patch(
            "synapseclient.models.curation.create_grid_replica",
            new_callable=AsyncMock,
            return_value={},
        ):
            with pytest.raises(ValueError, match="Replica could not be created"):
                await grid._create_replica_async(synapse_client=self.syn)


class TestGridConnect:
    """Tests for Grid.connect_async."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_connect_async_creates_session_when_no_session_id(self) -> None:
        # GIVEN a Grid with no session_id, so a new session needs to be created
        grid = Grid(record_set_id=RECORD_SET_ID)

        with (
            patch.object(
                Grid,
                "create_async",
                new_callable=AsyncMock,
                side_effect=lambda **kwargs: grid,
            ) as mock_create_async,
            patch.object(
                Grid,
                "_create_replica_async",
                new_callable=AsyncMock,
                return_value=GridReplica(replica_id=REPLICA_ID),
            ) as mock_create_replica,
        ):
            # WHEN I connect to the grid
            async with grid.connect_async(synapse_client=self.syn) as session:
                # THEN create_async should be called since no session_id was set
                mock_create_async.assert_called_once_with(
                    attach_to_previous_session=False,
                    timeout=120,
                    synapse_client=self.syn,
                )
                # AND _create_replica_async should be called to bind a replica
                mock_create_replica.assert_called_once_with(synapse_client=self.syn)
                # AND the replica_id should be bound on the yielded Grid
                assert session._replica_id == REPLICA_ID

            # THEN the replica_id should be cleared after exiting the block
            assert grid._replica_id is None

    async def test_connect_async_does_not_create_session_when_session_id_provided(
        self,
    ) -> None:
        # GIVEN a Grid that already has a session_id (e.g. an existing session)
        grid = Grid(session_id=SESSION_ID)

        with (
            patch.object(
                Grid, "create_async", new_callable=AsyncMock
            ) as mock_create_async,
            patch.object(
                Grid,
                "_create_replica_async",
                new_callable=AsyncMock,
                return_value=GridReplica(replica_id=REPLICA_ID),
            ) as mock_create_replica,
        ):
            # WHEN I connect to the grid
            async with grid.connect_async(synapse_client=self.syn) as session:
                # THEN create_async should NOT be called since session_id was
                # already set
                mock_create_async.assert_not_called()
                # AND _create_replica_async should still be called to bind a
                # replica to the existing session
                mock_create_replica.assert_called_once_with(synapse_client=self.syn)
                assert session.session_id == SESSION_ID
                assert session._replica_id == REPLICA_ID

            # THEN the replica_id should be cleared after exiting the block
            assert grid._replica_id is None


class TestGridValidateRows:
    """Tests for Grid.validate_rows_async."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_validate_rows_async_without_session_id_raises(self) -> None:
        # GIVEN a Grid without a session_id
        grid = Grid()

        # WHEN I call validate_rows_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="session_id is required to validate rows"):
            await grid.validate_rows_async(
                synapse_client=self.syn, query_request=QueryRequest()
            )

    async def test_validate_rows_async_without_replica_id_raises(self) -> None:
        # GIVEN a Grid with a session_id but no replica bound to it
        grid = Grid(session_id=SESSION_ID)

        # WHEN I call validate_rows_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="No replica is bound to this Grid"):
            await grid.validate_rows_async(
                synapse_client=self.syn, query_request=QueryRequest()
            )

    async def test_validate_rows_async_returns_query_result(self) -> None:
        # GIVEN a Grid with a session_id and a replica already bound to it (as
        # would be the case after `connect_async`/`connect`), and a mocked API
        # response
        grid = Grid(session_id=SESSION_ID)
        grid._replica_id = REPLICA_ID

        # Build a GridQueryJobRequest with query_result already populated
        mock_job_request = GridQueryJobRequest(
            session_id=SESSION_ID, replica_id=REPLICA_ID
        )
        grid_query_result = GridQueryResult().fill_from_dict(
            {
                "selectColumns": [
                    {"columnName": "Sex"},
                    {"columnName": "Diagnosis"},
                ],
                "rows": [
                    {
                        "rowId": "123",
                        "data": {"Sex": "Female", "Diagnosis": "Cancer"},
                        "validationResults": {
                            "isValid": False,
                            "validationErrorMessage": "#: only 1 subschema matches out of 2",
                        },
                    },
                    {
                        "rowId": "456",
                        "data": {"Sex": "Male", "Diagnosis": "Cancer"},
                        "validationResults": {
                            "isValid": False,
                            "validationErrorMessage": "#: only 1 subschema matches out of 2",
                        },
                    },
                ],
            }
        )
        mock_job_request.query_result = grid_query_result

        # WHEN I call validate_rows_async
        with patch.object(
            GridQueryJobRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_job_request,
        ):
            query_request = QueryRequest(
                query=GridQuery(column_selection=[SelectAll()])
            )
            result = await grid.validate_rows_async(
                synapse_client=self.syn,
                query_request=query_request,
            )

            # THEN the result should be a populated GridQueryResult
            assert isinstance(result, GridQueryResult)
            assert result.select_columns[0].column_name == "Sex"
            assert result.select_columns[1].column_name == "Diagnosis"
            assert result.rows[0].row_id == "123"
            assert result.rows[0].data == {"Sex": "Female", "Diagnosis": "Cancer"}
            assert result.rows[0].validation_results.is_valid is False
            assert result.rows[1].row_id == "456"
            assert result.rows[1].data == {"Sex": "Male", "Diagnosis": "Cancer"}
            assert result.rows[1].validation_results.is_valid is False
            # AND the replica_id is cached on the Grid instance
            assert grid._replica_id == REPLICA_ID

    async def test_validate_rows_async_no_rows_returns_existing_result_and_warns(
        self,
    ) -> None:
        # GIVEN a Grid with a session_id and a replica already bound to it, and
        # a mocked API response with a query_result that has no rows
        grid = Grid(session_id=SESSION_ID)
        grid._replica_id = REPLICA_ID

        mock_job_request = GridQueryJobRequest(
            session_id=SESSION_ID, replica_id=REPLICA_ID
        )
        mock_job_request.query_result = GridQueryResult().fill_from_dict(
            {"selectColumns": [{"columnName": "Sex"}], "rows": []}
        )

        # WHEN I call validate_rows_async
        with (
            patch.object(
                GridQueryJobRequest,
                "send_job_and_wait_async",
                new_callable=AsyncMock,
                return_value=mock_job_request,
            ),
            patch.object(self.syn.logger, "warning") as mock_warning,
        ):
            query_request = QueryRequest(
                query=GridQuery(column_selection=[SelectAll()])
            )
            result = await grid.validate_rows_async(
                synapse_client=self.syn,
                query_request=query_request,
            )

            # THEN it should return the (empty-rows) query_result rather than
            # discarding it, and log a warning instead of raising
            assert result is mock_job_request.query_result
            assert result.rows == []
            assert result.select_columns[0].column_name == "Sex"
            mock_warning.assert_called_once()
            assert SESSION_ID in mock_warning.call_args[0][0]
