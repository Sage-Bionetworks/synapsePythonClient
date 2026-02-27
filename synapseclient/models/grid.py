"""
Grid session dataclasses for managing Grid sessions in Synapse.

Grid sessions are used for curation workflows where data can be edited in a grid
format and then exported back to record sets or synchronized with data sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    Generator,
    List,
    Optional,
    Protocol,
    Union,
)

if TYPE_CHECKING:
    from synapseclient.models.grid_query import GridSnapshot

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import (
    delete_grid_session,
    list_grid_sessions,
)
from synapseclient.core.async_utils import (
    async_to_sync,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.constants.concrete_types import (
    CREATE_GRID_REQUEST,
    DOWNLOAD_FROM_GRID_REQUEST,
    GRID_CSV_IMPORT_REQUEST,
    GRID_RECORD_SET_EXPORT_REQUEST,
    LIST_GRID_SESSIONS_REQUEST,
    LIST_GRID_SESSIONS_RESPONSE,
    SYNCHRONIZE_GRID_REQUEST,
)
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.recordset import ValidationSummary
from synapseclient.models.table_components import Column, CsvTableDescriptor, Query


@dataclass
class CreateGridRequest(AsynchronousCommunicator):
    """
    A request to create a new grid session.

    Represents a
    [Synapse CreateGridRequest]\
(https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/CreateGridRequest.html).

    Attributes:
        concrete_type: The concrete type for the request
        record_set_id: The synId of the RecordSet to use for initializing the grid
        initial_query: Initialize a grid session from an EntityView.
            Mutually exclusive with record_set_id.
        session_id: The session ID of the created grid (populated from response)
    """

    concrete_type: str = CREATE_GRID_REQUEST
    """The concrete type for the request"""

    record_set_id: Optional[str] = None
    """When provided, the grid will be initialized using the CSV file stored for
    the given record set id. The grid columns will match the header of the CSV.
    Optional, if present the initialQuery cannot be included."""

    initial_query: Optional[Query] = None
    """Initialize a grid session from an EntityView.
    Mutually exclusive with record_set_id."""

    session_id: Optional[str] = None
    """The session ID of the created grid (populated from response)"""

    _grid_session_data: Optional[Dict[str, Any]] = field(
        default=None, compare=False
    )
    """Internal storage of the full grid session data from the response."""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "CreateGridRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The CreateGridRequest object.
        """
        grid_session_data = synapse_response.get("gridSession", {})
        self.session_id = grid_session_data.get("sessionId", None)
        self._grid_session_data = grid_session_data
        return self

    def fill_grid_session_from_response(self, grid_session: "Grid") -> "Grid":
        """
        Fills a Grid object with data from the stored response.

        Arguments:
            grid_session: The Grid object to populate.

        Returns:
            The populated Grid object.
        """
        if not hasattr(self, "_grid_session_data"):
            return grid_session

        data = self._grid_session_data

        grid_session.session_id = data.get("sessionId", None)
        grid_session.started_by = data.get("startedBy", None)
        grid_session.started_on = data.get("startedOn", None)
        grid_session.etag = data.get("etag", None)
        grid_session.modified_on = data.get("modifiedOn", None)
        grid_session.last_replica_id_client = data.get(
            "lastReplicaIdClient", None
        )
        grid_session.last_replica_id_service = data.get(
            "lastReplicaIdService", None
        )
        grid_session.grid_json_schema_id = data.get(
            "gridJsonSchema$Id", None
        )
        grid_session.source_entity_id = data.get("sourceEntityId", None)

        return grid_session

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API
        request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        request_dict["recordSetId"] = self.record_set_id
        request_dict["initialQuery"] = (
            self.initial_query.to_synapse_request()
            if self.initial_query
            else None
        )
        delete_none_keys(request_dict)
        return request_dict


@dataclass
class GridRecordSetExportRequest(AsynchronousCommunicator):
    """
    A request to export a grid created from a record set back to the original
    record set. A CSV file will be generated and set as a new version of the
    recordset.

    Represents a
    [Synapse GridRecordSetExportRequest]\
(https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/GridRecordSetExportRequest.html).

    Attributes:
        concrete_type: The concrete type for the request
        session_id: The grid session ID
        response_session_id: The session ID from the export response
        response_record_set_id: The record set ID from the export response
        record_set_version_number: The version number from the export response
        validation_summary_statistics: Summary statistics from the export
    """

    concrete_type: str = GRID_RECORD_SET_EXPORT_REQUEST
    """The concrete type for the request"""

    session_id: Optional[str] = None
    """The grid session ID"""

    response_session_id: Optional[str] = None
    """The session ID from the export response"""

    response_record_set_id: Optional[str] = None
    """The record set ID from the export response"""

    record_set_version_number: Optional[int] = None
    """The version number from the export response"""

    validation_summary_statistics: Optional[ValidationSummary] = None
    """Summary statistics from the export response"""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "GridRecordSetExportRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The GridRecordSetExportRequest object.
        """
        self.response_session_id = synapse_response.get("sessionId", None)
        self.response_record_set_id = synapse_response.get(
            "recordSetId", None
        )
        self.record_set_version_number = synapse_response.get(
            "recordSetVersionNumber", None
        )

        validation_stats_dict = synapse_response.get(
            "validationSummaryStatistics", None
        )
        if validation_stats_dict:
            self.validation_summary_statistics = ValidationSummary(
                container_id=validation_stats_dict.get("containerId", None),
                total_number_of_children=validation_stats_dict.get(
                    "totalNumberOfChildren", None
                ),
                number_of_valid_children=validation_stats_dict.get(
                    "numberOfValidChildren", None
                ),
                number_of_invalid_children=validation_stats_dict.get(
                    "numberOfInvalidChildren", None
                ),
                number_of_unknown_children=validation_stats_dict.get(
                    "numberOfUnknownChildren", None
                ),
                generated_on=validation_stats_dict.get("generatedOn", None),
            )

        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API
        request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        if self.session_id is not None:
            request_dict["sessionId"] = self.session_id
        return request_dict


@dataclass
class GridCsvImportRequest(AsynchronousCommunicator):
    """
    A request to import a CSV file into an existing grid session.
    Currently supports only grids started using a RecordSet.

    Attributes:
        concrete_type: The concrete type for the request
        session_id: The grid session ID
        file_handle_id: The ID of the file handle containing the CSV data
        csv_descriptor: The description of the CSV for upload
        schema: The list of ColumnModel objects describing the CSV file
        response_session_id: The session ID from the import response
        total_count: Total number of processed rows
        created_count: Number of newly created rows in the grid
        updated_count: Number of updated rows in the grid
    """

    concrete_type: str = GRID_CSV_IMPORT_REQUEST
    """The concrete type for the request"""

    session_id: Optional[str] = None
    """The grid session ID"""

    file_handle_id: Optional[str] = None
    """The ID of the file handle containing the CSV data"""

    csv_descriptor: Optional[CsvTableDescriptor] = None
    """The description of the CSV for upload"""

    schema: Optional[List[Column]] = None
    """The list of Column objects describing the CSV file (required).
    Each Column must have at least ``name`` and ``column_type`` set,
    and the order must match the CSV header columns exactly."""

    # Response fields
    response_session_id: Optional[str] = None
    """The session ID from the import response"""

    total_count: Optional[int] = None
    """Total number of processed rows"""

    created_count: Optional[int] = None
    """Number of newly created rows in the grid"""

    updated_count: Optional[int] = None
    """Number of updated rows in the grid"""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "GridCsvImportRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The GridCsvImportRequest object.
        """
        self.response_session_id = synapse_response.get("sessionId", None)
        self.total_count = synapse_response.get("totalCount", None)
        self.created_count = synapse_response.get("createdCount", None)
        self.updated_count = synapse_response.get("updatedCount", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API
        request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        if self.session_id is not None:
            request_dict["sessionId"] = self.session_id
        if self.file_handle_id is not None:
            request_dict["fileHandleId"] = self.file_handle_id
        csv_desc = (
            self.csv_descriptor
            if self.csv_descriptor is not None
            else CsvTableDescriptor()
        )
        request_dict["csvDescriptor"] = csv_desc.to_synapse_request()
        if self.schema is not None:
            request_dict["schema"] = [
                col.to_synapse_request() for col in self.schema
            ]
        return request_dict


@dataclass
class DownloadFromGridRequest(AsynchronousCommunicator):
    """
    A request to download grid data as a CSV file.

    Note: The downloaded CSV does NOT include validation columns.

    Attributes:
        concrete_type: The concrete type for the request
        session_id: The grid session ID
        write_header: Whether to include column names as header
        include_row_id_and_row_version: Whether to include row ID and version
        include_etag: Whether to include row etag
        csv_table_descriptor: The description of the CSV for download
        file_name: Optional name for the downloaded file
        response_session_id: The session ID from the download response
        results_file_handle_id: The file handle ID for the resulting CSV
    """

    concrete_type: str = DOWNLOAD_FROM_GRID_REQUEST
    """The concrete type for the request"""

    session_id: Optional[str] = None
    """The grid session ID"""

    write_header: Optional[bool] = True
    """Whether to include column names as header. Defaults to True."""

    include_row_id_and_row_version: Optional[bool] = True
    """Whether to include row ID and version columns. Defaults to True."""

    include_etag: Optional[bool] = True
    """Whether to include row etag column. Defaults to True."""

    csv_table_descriptor: Optional[CsvTableDescriptor] = None
    """The description of the CSV for download"""

    file_name: Optional[str] = None
    """Optional name for the downloaded file"""

    # Response fields
    response_session_id: Optional[str] = None
    """The session ID from the download response"""

    results_file_handle_id: Optional[str] = None
    """The file handle ID for the resulting CSV"""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "DownloadFromGridRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The DownloadFromGridRequest object.
        """
        self.response_session_id = synapse_response.get("sessionId", None)
        self.results_file_handle_id = synapse_response.get(
            "resultsFileHandleId", None
        )
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API
        request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        if self.session_id is not None:
            request_dict["sessionId"] = self.session_id
        if self.write_header is not None:
            request_dict["writeHeader"] = self.write_header
        if self.include_row_id_and_row_version is not None:
            request_dict["includeRowIdAndRowVersion"] = (
                self.include_row_id_and_row_version
            )
        if self.include_etag is not None:
            request_dict["includeEtag"] = self.include_etag
        if self.csv_table_descriptor is not None:
            request_dict["csvTableDescriptor"] = (
                self.csv_table_descriptor.to_synapse_request()
            )
        if self.file_name is not None:
            request_dict["fileName"] = self.file_name
        return request_dict


@dataclass
class SynchronizeGridRequest(AsynchronousCommunicator):
    """
    A request to synchronize a grid session with its data source.
    Synchronization is a two-phase process that ensures consistency between
    the grid and its source.

    Attributes:
        concrete_type: The concrete type for the request
        grid_session_id: The ID of the grid session to synchronize
        response_grid_session_id: The grid session ID from the response
        error_messages: Any error messages from the synchronization
    """

    concrete_type: str = SYNCHRONIZE_GRID_REQUEST
    """The concrete type for the request"""

    grid_session_id: Optional[str] = None
    """The ID of the grid session to synchronize"""

    # Response fields
    response_grid_session_id: Optional[str] = None
    """The grid session ID from the response"""

    error_messages: Optional[List[str]] = None
    """Any error messages generated during synchronization"""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "SynchronizeGridRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The SynchronizeGridRequest object.
        """
        self.response_grid_session_id = synapse_response.get(
            "gridSessionId", None
        )
        self.error_messages = synapse_response.get("errorMessages", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API
        request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        if self.grid_session_id is not None:
            request_dict["gridSessionId"] = self.grid_session_id
        return request_dict


@dataclass
class GridSession:
    """
    Basic information about a grid session.

    Represents a
    [Synapse GridSession](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/GridSession.html).

    Attributes:
        session_id: The unique sessionId that identifies the grid session
        started_by: The user that started this session
        started_on: The date-time when the session was started
        etag: Changes when the session changes
        modified_on: The date-time when the session was last changed
        last_replica_id_client: The last replica ID issued to a client
        last_replica_id_service: The last replica ID issued to a service
        grid_json_schema_id: The $id of the JSON schema used for validation
        source_entity_id: The synId of the source table/view/csv
    """

    session_id: Optional[str] = None
    """The unique sessionId that identifies the grid session"""

    started_by: Optional[str] = None
    """The user that started this session"""

    started_on: Optional[str] = None
    """The date-time when the session was started"""

    etag: Optional[str] = None
    """Changes when the session changes"""

    modified_on: Optional[str] = None
    """The date-time when the session was last changed"""

    last_replica_id_client: Optional[int] = None
    """The last replica ID issued to a client."""

    last_replica_id_service: Optional[int] = None
    """The last replica ID issued to a service."""

    grid_json_schema_id: Optional[str] = None
    """The $id of the JSON schema used for model validation"""

    source_entity_id: Optional[str] = None
    """The synId of the table/view/csv that this grid was cloned from"""

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "GridSession":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The GridSession object.
        """
        self.session_id = synapse_response.get("sessionId", None)
        self.started_by = synapse_response.get("startedBy", None)
        self.started_on = synapse_response.get("startedOn", None)
        self.etag = synapse_response.get("etag", None)
        self.modified_on = synapse_response.get("modifiedOn", None)
        self.last_replica_id_client = synapse_response.get(
            "lastReplicaIdClient", None
        )
        self.last_replica_id_service = synapse_response.get(
            "lastReplicaIdService", None
        )
        self.grid_json_schema_id = synapse_response.get(
            "gridJsonSchema$Id", None
        )
        self.source_entity_id = synapse_response.get("sourceEntityId", None)
        return self


@dataclass
class ListGridSessionsRequest:
    """
    Request to list a user's active grid sessions.

    Attributes:
        concrete_type: The concrete type for the request
        source_id: Optional filter by source entity synId
        next_page_token: Pagination token
    """

    concrete_type: str = LIST_GRID_SESSIONS_REQUEST
    """The concrete type for the request"""

    source_id: Optional[str] = None
    """Optional. When provided, only sessions with this synId are returned"""

    next_page_token: Optional[str] = None
    """Forward the returned 'nextPageToken' to get the next page"""

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API
        request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        if self.source_id is not None:
            request_dict["sourceId"] = self.source_id
        if self.next_page_token is not None:
            request_dict["nextPageToken"] = self.next_page_token
        delete_none_keys(request_dict)
        return request_dict


@dataclass
class ListGridSessionsResponse:
    """
    Response to a request to list a user's active grid sessions.

    Attributes:
        concrete_type: The concrete type for the response
        page: A single page of results
        next_page_token: Forward this token to get the next page
    """

    concrete_type: str = LIST_GRID_SESSIONS_RESPONSE
    """The concrete type for the response"""

    page: Optional[list[GridSession]] = None
    """A single page of results"""

    next_page_token: Optional[str] = None
    """Forward this token to get the next page of results"""

    def fill_from_dict(
        self, synapse_response: Dict[str, Any]
    ) -> "ListGridSessionsResponse":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The ListGridSessionsResponse object.
        """
        self.next_page_token = synapse_response.get("nextPageToken", None)
        page_data = synapse_response.get("page", [])
        if page_data:
            self.page = []
            for session_dict in page_data:
                session = GridSession()
                session.fill_from_dict(session_dict)
                self.page.append(session)
        return self


class GridSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def create(
        self,
        attach_to_previous_session=False,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """Creates a new grid session from a `record_set_id` or
        `initial_query`."""
        return self

    def export_to_record_set(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """Exports the grid session data back to a record set."""
        return self

    def import_csv(
        self,
        file_handle_id: Optional[str] = None,
        path: Optional[str] = None,
        dataframe: Optional[Any] = None,
        schema: Optional[List[Column]] = None,
        csv_descriptor: Optional[CsvTableDescriptor] = None,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """Imports CSV data into the grid session. Provide a file path,
        DataFrame, or file handle ID. Schema is auto-derived when
        omitted (requires path or dataframe)."""
        return self

    def download_csv(
        self,
        download_location: Optional[str] = None,
        write_header: bool = True,
        include_row_id_and_row_version: bool = True,
        include_etag: bool = True,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        file_name: Optional[str] = None,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """Downloads grid data as a CSV file. Returns local file path."""
        return ""

    def synchronize(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """Synchronizes the grid session with its data source."""
        return self

    def get_snapshot(
        self,
        *,
        connect_timeout: float = 30.0,
        synapse_client: Optional[Synapse] = None,
    ) -> GridSnapshot:
        """Get a read-only snapshot of the grid session's current state."""
        return GridSnapshot()

    def get_validation(
        self,
        *,
        connect_timeout: float = 30.0,
        synapse_client: Optional[Synapse] = None,
    ) -> GridSnapshot:
        """Get per-row validation results from the grid session."""
        return GridSnapshot()

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the grid session."""
        return None

    @classmethod
    def list(
        cls,
        source_id: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["Grid", None, None]:
        """Generator to get a list of active grid sessions for the user."""
        yield from []


@dataclass
@async_to_sync
class Grid(GridSynchronousProtocol):
    """
    A Grid provides functionality to create and manage grid sessions in Synapse.
    Grid sessions are used for curation workflows where data can be edited in a
    grid format and then exported back to record sets.

    Attributes:
        record_set_id: The synId of the RecordSet to initialize the grid
        initial_query: Initialize from an EntityView query.
            Mutually exclusive with record_set_id.
        session_id: The unique sessionId for this grid session
        started_by: The user that started this session
        started_on: The date-time when the session was started
        etag: Changes when the session changes
        modified_on: The date-time when the session was last changed
        last_replica_id_client: The last replica ID issued to a client
        last_replica_id_service: The last replica ID issued to a service
        grid_json_schema_id: The $id of the JSON schema for validation
        source_entity_id: The synId of the source table/view/csv
        record_set_version_number: The version number of the exported record set
        validation_summary_statistics: Summary statistics for validation results
        csv_import_total_count: Total rows processed in last CSV import
        csv_import_created_count: Rows created in last CSV import
        csv_import_updated_count: Rows updated in last CSV import
        synchronize_error_messages: Error messages from last synchronization
    """

    record_set_id: Optional[str] = None
    """The synId of the RecordSet to use for initializing the grid"""

    initial_query: Optional[Query] = None
    """Initialize a grid session from an EntityView.
    Mutually exclusive with record_set_id."""

    session_id: Optional[str] = None
    """The unique sessionId that identifies the grid session"""

    started_by: Optional[str] = None
    """The user that started this session"""

    started_on: Optional[str] = None
    """The date-time when the session was started"""

    etag: Optional[str] = None
    """Changes when the session changes"""

    modified_on: Optional[str] = None
    """The date-time when the session was last changed"""

    last_replica_id_client: Optional[int] = None
    """The last replica ID issued to a client."""

    last_replica_id_service: Optional[int] = None
    """The last replica ID issued to a service."""

    grid_json_schema_id: Optional[str] = None
    """The $id of the JSON schema for model validation in this grid session"""

    source_entity_id: Optional[str] = None
    """The synId of the table/view/csv that this grid was cloned from"""

    record_set_version_number: Optional[int] = None
    """The version number of the exported record set"""

    validation_summary_statistics: Optional[ValidationSummary] = None
    """Summary statistics for validation results"""

    csv_import_total_count: Optional[int] = None
    """Total rows processed in last CSV import"""

    csv_import_created_count: Optional[int] = None
    """Rows created in last CSV import"""

    csv_import_updated_count: Optional[int] = None
    """Rows updated in last CSV import"""

    synchronize_error_messages: Optional[List[str]] = None
    """Error messages from last synchronization"""

    async def create_async(
        self,
        attach_to_previous_session=False,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """
        Creates a new grid session from a `record_set_id` or `initial_query`.

        Arguments:
            attach_to_previous_session: If True and using `record_set_id`,
                will attach to an existing active session if one exists.
            timeout: Seconds to wait for the job to complete. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            Grid: The Grid object with populated session_id.

        Raises:
            ValueError: If `record_set_id` or `initial_query` is not provided.
        """
        if not self.record_set_id and not self.initial_query:
            raise ValueError(
                "record_set_id or initial_query is required to create a "
                "GridSession"
            )

        trace.get_current_span().set_attributes(
            {
                "synapse.record_set_id": self.record_set_id or "",
                "synapse.session_id": self.session_id or "",
            }
        )

        if self.record_set_id and attach_to_previous_session:
            async for existing_session in self.list_async(
                source_id=self.record_set_id, synapse_client=synapse_client
            ):
                self.session_id = existing_session.session_id
                self.started_by = existing_session.started_by
                self.started_on = existing_session.started_on
                self.etag = existing_session.etag
                self.modified_on = existing_session.modified_on
                self.last_replica_id_client = (
                    existing_session.last_replica_id_client
                )
                self.last_replica_id_service = (
                    existing_session.last_replica_id_service
                )
                self.grid_json_schema_id = (
                    existing_session.grid_json_schema_id
                )
                self.source_entity_id = existing_session.source_entity_id
                return self

        create_request = CreateGridRequest(
            record_set_id=self.record_set_id, initial_query=self.initial_query
        )
        result = await create_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        result.fill_grid_session_from_response(self)

        return self

    async def export_to_record_set_async(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """
        Exports the grid session data back to a record set. This will create
        a new version of the original record set with the modified data.

        Arguments:
            timeout: Seconds to wait for the job to complete. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            Grid: The Grid object with export information populated.

        Raises:
            ValueError: If session_id is not provided.
        """
        if not self.session_id:
            raise ValueError("session_id is required to export a GridSession")

        trace.get_current_span().set_attributes(
            {"synapse.session_id": self.session_id or ""}
        )

        export_request = GridRecordSetExportRequest(
            session_id=self.session_id
        )
        result = await export_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        self.record_set_id = result.response_record_set_id
        self.record_set_version_number = result.record_set_version_number
        self.validation_summary_statistics = (
            result.validation_summary_statistics
        )

        return self

    async def _derive_schema_async(
        self,
        path: Optional[str] = None,
        dataframe: Optional[Any] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List[Column]:
        """Derive the column schema from a CSV file or DataFrame.

        Column names come from the CSV header or DataFrame columns.
        Column types are resolved from the JSON schema bound to the
        grid session. Columns not found in the schema default to STRING.
        """
        import csv as csv_module

        from synapseclient.api.json_schema_services import (
            get_json_schema_body,
        )
        from synapseclient.extensions.curator.file_based_metadata_task import (
            _get_column_type_from_js_property,
        )
        from synapseclient.models.table_components import ColumnType

        # 1. Get column names from the data source
        if path is not None:
            with open(path, newline="") as f:
                reader = csv_module.reader(f)
                column_names = next(reader)
        elif dataframe is not None:
            column_names = list(dataframe.columns)
        else:
            raise ValueError(
                "Either path or dataframe must be provided to "
                "derive the schema"
            )

        # 2. Get column types from the bound JSON schema
        type_map: Dict[str, ColumnType] = {}

        # Ensure we have grid session info with the schema ID
        if self.grid_json_schema_id is None and self.session_id:
            from synapseclient.api import get_grid_session

            session_info = await get_grid_session(
                session_id=self.session_id,
                synapse_client=synapse_client,
            )
            self.grid_json_schema_id = session_info.get(
                "gridJsonSchema$Id", None
            )

        if self.grid_json_schema_id:
            try:
                schema_body = await get_json_schema_body(
                    json_schema_uri=self.grid_json_schema_id,
                    synapse_client=synapse_client,
                )
                properties = schema_body.get("properties", {})
                for prop_name, prop_def in properties.items():
                    type_map[prop_name] = (
                        _get_column_type_from_js_property(prop_def)
                    )
            except Exception:
                pass  # Fall back to STRING for all columns

        # 3. Build Column list
        return [
            Column(
                name=name,
                column_type=type_map.get(name, ColumnType.STRING),
            )
            for name in column_names
        ]

    async def import_csv_async(
        self,
        file_handle_id: Optional[str] = None,
        path: Optional[str] = None,
        dataframe: Optional[Any] = None,
        schema: Optional[List[Column]] = None,
        csv_descriptor: Optional[CsvTableDescriptor] = None,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """
        Imports CSV data into the grid session. Currently supports only
        grids started using a RecordSet.

        Provide exactly one of ``file_handle_id``, ``path``, or
        ``dataframe``. When a local file path or DataFrame is provided,
        it is uploaded automatically to obtain a file handle.

        When ``schema`` is omitted the column schema is derived
        automatically: column names come from the CSV header (or
        DataFrame columns) and column types are resolved from the
        JSON schema bound to the grid session. If no JSON schema is
        bound, all columns default to ``ColumnType.STRING``.

        Arguments:
            file_handle_id: The ID of an already-uploaded file handle
                containing the CSV data. When using this option,
                ``schema`` is required.
            path: Local file path to a CSV file. The file will be
                uploaded automatically.
            dataframe: A pandas DataFrame. It will be written as CSV
                and uploaded automatically.
            schema: List of Column objects describing the CSV columns.
                Optional when ``path`` or ``dataframe`` is provided;
                required when using ``file_handle_id``.
            csv_descriptor: Optional description of the CSV format.
            timeout: Seconds to wait for the job to complete.
                Defaults to 120.
            synapse_client: If not passed in and caching was not disabled
                by ``Synapse.allow_client_caching(False)`` this will use
                the last created instance from the Synapse class
                constructor.

        Returns:
            Grid: The Grid object with import counts populated.

        Raises:
            ValueError: If session_id is not provided or if the
                source arguments are invalid.
        """
        from synapseclient.core.upload.multipart_upload_async import (
            multipart_upload_dataframe_async,
            multipart_upload_file_async,
        )

        if not self.session_id:
            raise ValueError(
                "session_id is required to import CSV into a GridSession"
            )

        sources = sum(
            x is not None
            for x in (file_handle_id, path, dataframe)
        )
        if sources != 1:
            raise ValueError(
                "Provide exactly one of file_handle_id, path, "
                "or dataframe"
            )

        if file_handle_id is not None and schema is None:
            raise ValueError(
                "schema is required when using file_handle_id "
                "directly (column names cannot be read from the "
                "file). Provide a path or dataframe instead to "
                "auto-derive the schema."
            )

        client = Synapse.get_client(synapse_client=synapse_client)

        trace.get_current_span().set_attributes(
            {"synapse.session_id": self.session_id or ""}
        )

        # Auto-derive schema when not provided
        if schema is None:
            schema = await self._derive_schema_async(
                path=path,
                dataframe=dataframe,
                synapse_client=client,
            )

        if path is not None:
            file_handle_id = await multipart_upload_file_async(
                syn=client,
                file_path=path,
                content_type="text/csv",
            )
        elif dataframe is not None:
            file_handle_id = await multipart_upload_dataframe_async(
                syn=client,
                df=dataframe,
                content_type="text/csv",
            )

        import_request = GridCsvImportRequest(
            session_id=self.session_id,
            file_handle_id=file_handle_id,
            schema=schema,
            csv_descriptor=csv_descriptor,
        )
        result = await import_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        self.csv_import_total_count = result.total_count
        self.csv_import_created_count = result.created_count
        self.csv_import_updated_count = result.updated_count

        return self

    async def download_csv_async(
        self,
        download_location: Optional[str] = None,
        write_header: bool = True,
        include_row_id_and_row_version: bool = True,
        include_etag: bool = True,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        file_name: Optional[str] = None,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """
        Downloads grid data as a CSV file.

        Note: The downloaded CSV does NOT include validation columns.

        Arguments:
            download_location: Directory to download the CSV file to.
                Defaults to Synapse cache directory.
            write_header: Include column names as header. Defaults to True.
            include_row_id_and_row_version: Include row ID and version
                columns. Defaults to True.
            include_etag: Include row etag column. Defaults to True.
            csv_table_descriptor: Optional CSV format description.
            file_name: Optional name for the downloaded file.
            timeout: Seconds to wait for the job to complete. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            str: The local file path of the downloaded CSV.

        Raises:
            ValueError: If session_id is not provided.
        """
        import os

        from synapseclient.core.download.download_async import (
            download_by_file_handle,
        )

        if not self.session_id:
            raise ValueError(
                "session_id is required to download CSV from a GridSession"
            )

        trace.get_current_span().set_attributes(
            {"synapse.session_id": self.session_id or ""}
        )

        download_request = DownloadFromGridRequest(
            session_id=self.session_id,
            write_header=write_header,
            include_row_id_and_row_version=include_row_id_and_row_version,
            include_etag=include_etag,
            csv_table_descriptor=csv_table_descriptor,
            file_name=file_name,
        )
        result = await download_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        client = Synapse.get_client(synapse_client=synapse_client)

        if download_location is None:
            download_location = client.cache.get_cache_dir(0)

        actual_file_name = file_name or "grid_download.csv"
        destination = os.path.join(download_location, actual_file_name)

        path = await download_by_file_handle(
            file_handle_id=result.results_file_handle_id,
            synapse_id=self.source_entity_id or self.session_id,
            entity_type="TableEntity",
            destination=destination,
            synapse_client=client,
        )

        return path

    async def synchronize_async(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """
        Synchronizes the grid session with its data source. This is a
        two-phase process that ensures consistency between the grid and source.

        Arguments:
            timeout: Seconds to wait for the job to complete. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            Grid: The Grid object with synchronization results.

        Raises:
            ValueError: If session_id is not provided.
        """
        if not self.session_id:
            raise ValueError(
                "session_id is required to synchronize a GridSession"
            )

        trace.get_current_span().set_attributes(
            {"synapse.session_id": self.session_id or ""}
        )

        sync_request = SynchronizeGridRequest(
            grid_session_id=self.session_id
        )
        result = await sync_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        self.synchronize_error_messages = result.error_messages

        return self

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "Grid":
        """Converts a response from the REST API into this dataclass."""
        self.session_id = synapse_response.get("sessionId", None)
        self.started_by = synapse_response.get("startedBy", None)
        self.started_on = synapse_response.get("startedOn", None)
        self.etag = synapse_response.get("etag", None)
        self.modified_on = synapse_response.get("modifiedOn", None)
        self.last_replica_id_client = synapse_response.get(
            "lastReplicaIdClient", None
        )
        self.last_replica_id_service = synapse_response.get(
            "lastReplicaIdService", None
        )
        self.grid_json_schema_id = synapse_response.get(
            "gridJsonSchema$Id", None
        )
        self.source_entity_id = synapse_response.get("sourceEntityId", None)
        return self

    @skip_async_to_sync
    @classmethod
    async def list_async(
        cls,
        source_id: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["Grid", None]:
        """
        Generator to get a list of active grid sessions for the user.

        Arguments:
            source_id: Optional. When provided, only sessions with this synId
                will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Yields:
            Grid objects representing active grid sessions.
        """
        async for session_dict in list_grid_sessions(
            source_id=source_id, synapse_client=synapse_client
        ):
            grid = cls()
            grid.fill_from_dict(session_dict)
            yield grid

    @classmethod
    def list(
        cls,
        source_id: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["Grid", None, None]:
        """
        Generator to get a list of active grid sessions for the user.

        Arguments:
            source_id: Optional. When provided, only sessions with this synId
                will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Yields:
            Grid objects representing active grid sessions.
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=cls.list_async,
            source_id=source_id,
            synapse_client=synapse_client,
        )

    async def get_snapshot_async(
        self,
        *,
        connect_timeout: float = 30.0,
        synapse_client: Optional[Synapse] = None,
    ) -> "GridSnapshot":
        """Get a read-only snapshot of the grid's current state including
        per-row validation results. Does NOT commit changes.

        This connects via WebSocket, receives the current grid state,
        extracts row data and validation results, and disconnects.

        Arguments:
            connect_timeout: Timeout in seconds for the WebSocket connection.
                Defaults to 30.0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            GridSnapshot with column names, row data, and per-row validation.

        Raises:
            ValueError: If session_id is not provided.
        """
        from synapseclient.api import create_grid_replica, get_grid_presigned_url
        from synapseclient.core.grid_websocket import GridWebSocketClient

        if not self.session_id:
            raise ValueError(
                "session_id is required to get a grid snapshot"
            )

        trace.get_current_span().set_attributes(
            {"synapse.session_id": self.session_id or ""}
        )

        # 1. Create a replica for this read-only connection
        replica_response = await create_grid_replica(
            session_id=self.session_id,
            synapse_client=synapse_client,
        )
        replica = replica_response.get("replica", {})
        replica_id = replica.get("replicaId")

        if replica_id is None:
            raise ValueError(
                "Failed to create grid replica - no replicaId returned"
            )

        # 2. Get a presigned WebSocket URL
        presigned_url = await get_grid_presigned_url(
            session_id=self.session_id,
            replica_id=replica_id,
            synapse_client=synapse_client,
        )

        if not presigned_url:
            raise ValueError(
                "Failed to get presigned WebSocket URL for grid session"
            )

        # 3. Connect, receive snapshot, extract data
        ws_client = GridWebSocketClient(
            connect_timeout=connect_timeout
        )
        snapshot = await ws_client.get_snapshot(
            presigned_url=presigned_url,
            replica_id=replica_id,
        )

        return snapshot

    async def get_validation_async(
        self,
        *,
        connect_timeout: float = 30.0,
        synapse_client: Optional[Synapse] = None,
    ) -> "GridSnapshot":
        """Get per-row validation results from the grid session.

        Convenience alias for get_snapshot_async.

        Arguments:
            connect_timeout: Timeout in seconds. Defaults to 30.0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            GridSnapshot with per-row validation data.
        """
        return await self.get_snapshot_async(
            connect_timeout=connect_timeout,
            synapse_client=synapse_client,
        )

    async def delete_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> None:
        """
        Delete the grid session.

        Note: Only the user that created a grid session may delete it.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Raises:
            ValueError: If session_id is not provided.
        """
        if not self.session_id:
            raise ValueError(
                "session_id is required to delete a GridSession"
            )

        trace.get_current_span().set_attributes(
            {"synapse.session_id": self.session_id or ""}
        )

        await delete_grid_session(
            session_id=self.session_id, synapse_client=synapse_client
        )
