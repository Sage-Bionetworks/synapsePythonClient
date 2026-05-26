"""
Curation Task dataclasses for managing Curation Tasks in Synapse.

Curation tasks are used to guide data contributors through the process of contributing
data or metadata in Synapse.
"""

import asyncio
import os
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    ClassVar,
    Dict,
    Generator,
    Optional,
    Protocol,
    Union,
)

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import (
    create_curation_task,
    delete_curation_task,
    delete_grid_session,
    get_curation_task,
    get_curation_task_status,
    get_file_handle,
    get_file_handle_presigned_url,
    list_curation_tasks,
    list_grid_sessions,
    update_curation_task,
    update_curation_task_status,
)
from synapseclient.core.async_utils import (
    async_to_sync,
    otel_trace_method,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.constants.concrete_types import (
    CREATE_GRID_REQUEST,
    DOWNLOAD_FROM_GRID_REQUEST,
    FILE_BASED_METADATA_TASK_PROPERTIES,
    GRID_CSV_IMPORT_REQUEST,
    GRID_EXECUTION_DETAILS,
    GRID_RECORD_SET_EXPORT_REQUEST,
    LIST_GRID_SESSIONS_REQUEST,
    LIST_GRID_SESSIONS_RESPONSE,
    RECORD_BASED_METADATA_TASK_PROPERTIES,
    SYNCHRONIZE_GRID_REQUEST,
    UPLOAD_TO_TABLE_PREVIEW_REQUEST,
)
from synapseclient.core.download.download_functions import download_from_url
from synapseclient.core.upload.upload_functions_async import upload_synapse_s3
from synapseclient.core.utils import (
    coerce_enum_list,
    delete_none_keys,
    merge_dataclass_entities,
)
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.mixins.enum_coercion import EnumCoercionMixin
from synapseclient.models.recordset import ValidationSummary
from synapseclient.models.table_components import Column, CsvTableDescriptor, Query


class TaskState(str, Enum):
    """
    The state of a CurationTask.

    See <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/TaskState.html>.
    """

    NOT_STARTED = "NOT_STARTED"
    """The task has been created and assigned but work has not yet started."""

    IN_PROGRESS = "IN_PROGRESS"
    """The assignee has actively started the task."""

    COMPLETED = "COMPLETED"
    """The task has been completed and verified."""

    CANCELED = "CANCELED"
    """The task has been canceled and is no longer needed."""


@dataclass
class FileBasedMetadataTaskProperties:
    """
    A CurationTaskProperties for file-based data, describing where data is uploaded
    and a view which contains the annotations.

    Represents a [Synapse FileBasedMetadataTaskProperties](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/metadata/FileBasedMetadataTaskProperties.html).

    Attributes:
        upload_folder_id: The synId of the folder where data files of this type are to be uploaded
        file_view_id: The synId of the FileView that shows all data of this type
    """

    upload_folder_id: Optional[str] = None
    """The synId of the folder where data files of this type are to be uploaded"""

    file_view_id: Optional[str] = None
    """The synId of the FileView that shows all data of this type"""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "FileBasedMetadataTaskProperties":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The FileBasedMetadataTaskProperties object.
        """
        self.upload_folder_id = synapse_response.get("uploadFolderId", None)
        self.file_view_id = synapse_response.get("fileViewId", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": FILE_BASED_METADATA_TASK_PROPERTIES}
        if self.upload_folder_id is not None:
            request_dict["uploadFolderId"] = self.upload_folder_id
        if self.file_view_id is not None:
            request_dict["fileViewId"] = self.file_view_id
        return request_dict


@dataclass
class RecordBasedMetadataTaskProperties:
    """
    A CurationTaskProperties for record-based metadata.

    Represents a [Synapse RecordBasedMetadataTaskProperties](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/metadata/RecordBasedMetadataTaskProperties.html).

    Attributes:
        record_set_id: The synId of the RecordSet that will contain all record-based metadata
    """

    record_set_id: Optional[str] = None
    """The synId of the RecordSet that will contain all record-based metadata"""

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "RecordBasedMetadataTaskProperties":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The RecordBasedMetadataTaskProperties object.
        """
        self.record_set_id = synapse_response.get("recordSetId", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": RECORD_BASED_METADATA_TASK_PROPERTIES}
        if self.record_set_id is not None:
            request_dict["recordSetId"] = self.record_set_id
        return request_dict


def _create_task_properties_from_dict(
    properties_dict: Dict[str, Any],
) -> Union[FileBasedMetadataTaskProperties, RecordBasedMetadataTaskProperties]:
    """
    Factory method to create the appropriate FileBasedMetadataTaskProperties/RecordBasedMetadataTaskProperties
    based on the concreteType.

    Arguments:
        properties_dict: Dictionary containing task properties data

    Returns:
        The appropriate FileBasedMetadataTaskProperties/RecordBasedMetadataTaskProperties instance
    """
    concrete_type = properties_dict.get("concreteType", "")

    if concrete_type == FILE_BASED_METADATA_TASK_PROPERTIES:
        return FileBasedMetadataTaskProperties().fill_from_dict(properties_dict)
    elif concrete_type == RECORD_BASED_METADATA_TASK_PROPERTIES:
        return RecordBasedMetadataTaskProperties().fill_from_dict(properties_dict)
    else:
        raise ValueError(
            f"Unknown concreteType for CurationTaskProperties: {concrete_type}"
        )


@dataclass
class TaskExecutionDetails(ABC):
    """
    Base class for task-specific execution details attached to a CurationTaskStatus.

    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/TaskExecutionDetails.html>

    The concrete subclass is determined by the concreteType field in the REST response.
    """

    @abstractmethod
    def fill_from_dict(
        self, synapse_response: dict[str, Any]
    ) -> "TaskExecutionDetails":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The TaskExecutionDetails object.
        """
        ...

    @abstractmethod
    def to_synapse_request(self) -> dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        ...


@dataclass
class GridExecutionDetails(TaskExecutionDetails):
    """
    Execution details for a metadata curation task involving a collaborative grid session.

    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/execution/GridExecutionDetails.html>

    Attributes:
        active_session_id: The unique identifier of the active CRDT grid session linked to this task.
    """

    active_session_id: str | None = None
    """The unique identifier of the active CRDT grid session linked to this task."""

    def fill_from_dict(
        self, synapse_response: dict[str, Any]
    ) -> "GridExecutionDetails":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The GridExecutionDetails object.
        """
        self.active_session_id = synapse_response.get("activeSessionId", None)
        return self

    def to_synapse_request(self) -> dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict: dict[str, Any] = {"concreteType": GRID_EXECUTION_DETAILS}
        if self.active_session_id is not None:
            request_dict["activeSessionId"] = self.active_session_id
        return request_dict


TASK_EXECUTION_DETAILS_DICT: dict[str, type[TaskExecutionDetails]] = {
    GRID_EXECUTION_DETAILS: GridExecutionDetails,
}


@dataclass
class CurationTaskStatus(EnumCoercionMixin):
    """
    The status of a CurationTask in its lifecycle.

    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/curation/TaskStatus.html>

    Attributes:
        task_id: The unique identifier of the associated curation task.
        state: The state of a curation task in its lifecycle.
        execution_details: Task-specific execution details. The concrete type
            determines which task-type-specific properties are available.
        last_updated_by: The principal ID of the user who last updated the status.
        last_updated_on: Timestamp of when the status was last updated.
        etag: Optimistic concurrency control token for the task status.
    """

    _ENUM_FIELDS: ClassVar[dict[str, type]] = {"state": TaskState}

    task_id: int | None = None
    """The unique identifier of the associated curation task."""

    state: str | TaskState | None = None
    """The state of a curation task in its lifecycle."""

    execution_details: TaskExecutionDetails | None = None
    """Task-specific execution details. The concrete type determines which
    task-type-specific properties are available."""

    last_updated_by: str | None = None
    """The principal ID of the user who last updated the status."""

    last_updated_on: str | None = None
    """Timestamp of when the status was last updated."""

    etag: str | None = None
    """Optimistic concurrency control token for the task status."""

    def fill_from_dict(self, synapse_response: dict[str, Any]) -> "CurationTaskStatus":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The CurationTaskStatus object.
        """
        task_id_value = synapse_response.get("taskId", None)
        self.task_id = int(task_id_value) if task_id_value is not None else None
        self.state = synapse_response.get("state", None)
        self.last_updated_by = synapse_response.get("lastUpdatedBy", None)
        self.last_updated_on = synapse_response.get("lastUpdatedOn", None)
        self.etag = synapse_response.get("etag", None)

        details_dict: dict[str, Any] | None = synapse_response.get(
            "executionDetails", None
        )
        if details_dict is None:
            self.execution_details = None
        else:
            concrete_type = details_dict.get("concreteType", "")
            cls = TASK_EXECUTION_DETAILS_DICT.get(concrete_type)
            if cls is None:
                raise ValueError(
                    f"Unknown concreteType for TaskExecutionDetails: {concrete_type}"
                )
            self.execution_details = cls().fill_from_dict(details_dict)
        return self

    def to_synapse_request(self) -> dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict: dict[str, Any] = {
            "taskId": self.task_id,
            "state": self.state.value if self.state is not None else None,
            "etag": self.etag,
        }
        if self.execution_details is not None:
            request_dict["executionDetails"] = (
                self.execution_details.to_synapse_request()
            )
        delete_none_keys(request_dict)
        return request_dict


async def _get_existing_curation_task_id(
    project_id: str,
    data_type: str,
    synapse_client: Optional[Synapse] = None,
) -> Optional[int]:
    """
    Helper function to find an existing curation task by project_id and data_type.

    Arguments:
        project_id: The synId of the project.
        data_type: The data type to search for.
        synapse_client: The Synapse client to use.

    Returns:
        The task_id if found, None otherwise.
    """
    async for task_dict in list_curation_tasks(
        project_id=project_id, synapse_client=synapse_client
    ):
        if task_dict.get("dataType") == data_type:
            return task_dict.get("taskId")
    return None


class CurationTaskSynchronousProtocol(Protocol):
    def get(self, *, synapse_client: Optional[Synapse] = None) -> "CurationTask":
        """
        Gets a CurationTask from Synapse by ID.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            CurationTask: The CurationTask object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Get a curation task by ID
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            task = CurationTask(task_id=123).get()
            print(task.data_type)
            print(task.instructions)
            ```
        """
        return self

    def get_status(
        self, *, synapse_client: Synapse | None = None
    ) -> "CurationTaskStatus":
        """
        Gets the status of this CurationTask from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Get the status of a curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            status = CurationTask(task_id=123).get_status()
            print(status.state)
            ```
        """
        return CurationTaskStatus()

    def update_status(
        self,
        curation_task_status: "CurationTaskStatus",
        *,
        synapse_client: Synapse | None = None,
    ) -> "CurationTaskStatus":
        """
        Updates the status of this CurationTask on Synapse.

        Arguments:
            curation_task_status: The complete CurationTaskStatus object to update.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Update the status of a curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import (
                CurationTask,
                TaskState,
                CurationTaskStatus,
            )

            syn = Synapse()
            syn.login()

            task = CurationTask(task_id=123)
            current = task.get_status()
            current.state = TaskState.COMPLETED
            updated = task.update_status(curation_task_status=current)
            print(updated.state)
            ```
        """
        return CurationTaskStatus()

    def set_active_grid_session(
        self,
        active_session_id: str,
        *,
        synapse_client: Synapse | None = None,
    ) -> "CurationTaskStatus":
        """
        Set the active grid session on this CurationTask's status by replacing
        execution_details with a GridExecutionDetails carrying the given session id.

        Does not transition the task state.

        Arguments:
            active_session_id: The unique identifier of the active grid session to link.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Link a grid session to a curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, Grid

            syn = Synapse()
            syn.login()

            grid = Grid(record_set_id="syn1234567").create()
            CurationTask(task_id=123).set_active_grid_session(
                active_session_id=grid.session_id
            )
            ```
        """
        return CurationTaskStatus()

    def set_task_state(
        self,
        state: "TaskState | str",
        *,
        synapse_client: Synapse | None = None,
    ) -> "CurationTaskStatus":
        """
        Set the state on this CurationTask's status.

        Does not modify execution_details. Fetches the current CurationTaskStatus
        first so the update carries a fresh etag.

        Arguments:
            state: The state to set on this task's status. Accepts a
                TaskState or a case-insensitive string matching one of
                its members (e.g. NOT_STARTED, IN_PROGRESS, COMPLETED, CANCELED).
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id, or
                if state is a string that does not match a TaskState member.

        Example: Mark a curation task as completed
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, TaskState

            syn = Synapse()
            syn.login()

            CurationTask(task_id=123).set_task_state(
                state=TaskState.COMPLETED
            )
            ```
        """
        return CurationTaskStatus()

    def create_grid_session(
        self,
        *,
        owner_principal_id: int | None = None,
        timeout: int = 120,
        synapse_client: Synapse | None = None,
    ) -> "Grid":
        """
        Create a Grid session for this CurationTask and link it to the task status.

        Picks the Grid seed from this task's task_properties:

        - RecordBasedMetadataTaskProperties uses record_set_id
        - FileBasedMetadataTaskProperties uses an initial_query that selects from
          the file_view_id

        Always creates a new Grid session. To attach an existing session to a task,
        use set_active_grid_session instead.

        After the Grid is created, updates the CurationTaskStatus to point its
        active_session_id at the new session. If that update fails for any reason,
        the newly created Grid is deleted on a best-effort basis and the original
        exception is re-raised.

        Arguments:
            owner_principal_id: The principal ID (user or team) that will own the
                created grid session. When not provided, the principal ID of the
                caller is used.
            timeout: Seconds to wait for the grid creation job. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The newly created Grid.

        Raises:
            ValueError: If task_id is unset or task_properties is of an unsupported type.
            SynapseHTTPError: If the status update fails. The orphan Grid is
                deleted on a best-effort basis before the error is re-raised.

        Example: Create a grid session for a curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            grid = CurationTask(task_id=123).create_grid_session()
            print(grid.session_id)
            ```
        """
        return Grid()

    def delete(
        self,
        delete_source: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Deletes a CurationTask from Synapse.

        Arguments:
            delete_source: If True, the associated source data (EntityView or RecordSet) will also be deleted
                if the task is a FileBasedMetadataTask or RecordBasedMetadataTask respectively. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Delete a curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            task = CurationTask(task_id=123)
            task.delete()
            ```

        Example: Delete a curation task and its associated data source
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            task = CurationTask(task_id=123)
            task.delete(delete_source=True)
            ```
        """
        return None

    def store(self, *, synapse_client: Optional[Synapse] = None) -> "CurationTask":
        """
        Creates a new CurationTask or updates an existing one on Synapse.

        This method implements non-destructive updates. If a CurationTask with the same
        project_id and data_type exists and this instance hasn't been retrieved from
        Synapse before, it will merge the existing task data with the current instance
        before updating.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            CurationTask: The CurationTask object.

        Example: Create a new file-based curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, FileBasedMetadataTaskProperties

            syn = Synapse()
            syn.login()

            # Create file-based task properties
            file_properties = FileBasedMetadataTaskProperties(
                upload_folder_id="syn1234567",
                file_view_id="syn2345678"
            )

            # Create the curation task
            task = CurationTask(
                project_id="syn9876543",
                data_type="genomics_data",
                instructions="Upload your genomics files to the specified folder",
                task_properties=file_properties
            )
            task = task.store()
            print(f"Created task with ID: {task.task_id}")
            ```

        Example: Create a new record-based curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, RecordBasedMetadataTaskProperties

            syn = Synapse()
            syn.login()

            # Create record-based task properties
            record_properties = RecordBasedMetadataTaskProperties(
                record_set_id="syn3456789"
            )

            # Create the curation task
            task = CurationTask(
                project_id="syn9876543",
                data_type="clinical_data",
                instructions="Fill out the clinical data form",
                task_properties=record_properties
            )
            task = task.store()
            print(f"Created task with ID: {task.task_id}")
            ```

        Example: Update an existing curation task
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            # Get existing task and update
            task = CurationTask(task_id=123).get()
            task.instructions = "Updated instructions for data contributors"
            task = task.store()
            ```
        """
        return self

    @classmethod
    def list(
        cls,
        project_id: str,
        *,
        assigned_to_me: Optional[bool] = None,
        assignee_ids: Optional[list[str]] = None,
        state_filter: Optional[list[Union["TaskState", str]]] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["CurationTask", None, None]:
        """
        Generator that yields CurationTasks for a project as they become available.

        Arguments:
            project_id: The synId of the project.
            assigned_to_me: When True, only return tasks assigned to the current user.
                Cannot be combined with assignee_ids.
                False does not mean "tasks not assigned to me".
                Defaults to None.
            assignee_ids: Optional list of principal IDs (users or teams) to filter
                tasks by assignee. Cannot be combined with assigned_to_me=True.
                Passing an empty list raises a ValueError; pass None to return tasks
                for any assignee. Defaults to None.
            state_filter: Optional list of TaskState values or exact-case strings to
                filter tasks by their current state (e.g., "IN_PROGRESS"). Defaults to
                None (all states returned). Passing an empty list raises a ValueError;
                pass None to return tasks in any state.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Yields:
            CurationTask objects as they are retrieved from the API.

        Raises:
            ValueError: If state_filter is an empty list.
            ValueError: If assignee_ids is an empty list.
            ValueError: If assigned_to_me is True and assignee_ids is also provided.
            ValueError: If any value in state_filter is not a TaskState member or
                an exact-case string matching a TaskState value (e.g., "IN_PROGRESS").

        Note: Due to generator semantics, argument validation runs on the first
            iteration of the generator, not at the point where list() is called.

        Example: List all curation tasks in a project
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            # List all curation tasks in the project
            for task in CurationTask.list(project_id="syn9876543"):
                print(f"Task ID: {task.task_id}")
                print(f"Data Type: {task.data_type}")
                print(f"Instructions: {task.instructions}")
                print("---")
            ```

        Example: List only curation tasks assigned to the current user
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            for task in CurationTask.list(project_id="syn9876543", assigned_to_me=True):
                print(f"Task ID: {task.task_id}")
                print(f"Data Type: {task.data_type}")
                print("---")
            ```

        Example: List only in-progress curation tasks
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, TaskState

            syn = Synapse()
            syn.login()

            for task in CurationTask.list(
                project_id="syn9876543",
                state_filter=[TaskState.IN_PROGRESS],
            ):
                print(f"Task ID: {task.task_id}")
                print(f"Data Type: {task.data_type}")
                print("---")
            ```

        Example: List only in-progress curation tasks using a string state filter
            &nbsp;

            state_filter also accepts plain strings matching TaskState names exactly.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            for task in CurationTask.list(
                project_id="syn9876543",
                state_filter=["IN_PROGRESS"],
            ):
                print(f"Task ID: {task.task_id}")
                print(f"Data Type: {task.data_type}")
                print("---")
            ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=cls.list_async,
            project_id=project_id,
            assigned_to_me=assigned_to_me,
            assignee_ids=assignee_ids,
            state_filter=state_filter,
            synapse_client=synapse_client,
        )


@dataclass
@async_to_sync
class CurationTask(CurationTaskSynchronousProtocol):
    """
    The CurationTask provides instructions for a Data Contributor on how data or metadata
    of a specific type should be both added to a project and curated.

    Represents a [Synapse CurationTask](https://rest-docs.synapse.org/org/sagebionetworks/repo/model/curation/CurationTask.html).

    Attributes:
        task_id: The unique identifier issued to this task when it was created
        data_type: Will match the data type that a contributor plans to contribute
        project_id: The synId of the project
        instructions: Instructions to the data contributor
        task_properties: The properties of a CurationTask. This can be either
            FileBasedMetadataTaskProperties or RecordBasedMetadataTaskProperties.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: (Read Only) The date this task was created
        modified_on: (Read Only) The date this task was last modified
        created_by: (Read Only) The ID of the user that created this task
        modified_by: (Read Only) The ID of the user that last modified this task

    Example: Complete curation task workflow
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import CurationTask, FileBasedMetadataTaskProperties

        syn = Synapse()
        syn.login()

        # Create a new file-based curation task
        file_properties = FileBasedMetadataTaskProperties(
            upload_folder_id="syn1234567",
            file_view_id="syn2345678"
        )

        task = CurationTask(
            project_id="syn9876543",
            data_type="genomics_data",
            instructions="Upload your genomics files and complete metadata",
            task_properties=file_properties
        )
        task = task.store()
        print(f"Created task: {task.task_id}")

        # Later, retrieve and update the task
        existing_task = CurationTask(task_id=task.task_id).get()
        existing_task.instructions = "Updated instructions with new requirements"
        existing_task.store()

        # List all tasks in the project
        for project_task in CurationTask.list(project_id="syn9876543"):
            print(f"Task: {project_task.data_type} - {project_task.task_id}")
        ```
    """

    task_id: Optional[int] = None
    """The unique identifier issued to this task when it was created"""

    data_type: Optional[str] = None
    """Will match the data type that a contributor plans to contribute. The dataType must be unique within a project"""

    project_id: Optional[str] = None
    """The synId of the project"""

    instructions: Optional[str] = None
    """Instructions to the data contributor"""

    task_properties: Optional[
        Union[FileBasedMetadataTaskProperties, RecordBasedMetadataTaskProperties]
    ] = None
    """The properties of a CurationTask"""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates. Since the E-Tag changes every time an entity is updated it is used to detect when a client's current representation of an entity is out-of-date"""

    created_on: Optional[str] = None
    """(Read Only) The date this task was created"""

    modified_on: Optional[str] = None
    """(Read Only) The date this task was last modified"""

    created_by: Optional[str] = None
    """(Read Only) The ID of the user that created this task"""

    modified_by: Optional[str] = None
    """(Read Only) The ID of the user that last modified this task"""

    assignee_principal_id: Optional[str] = None
    """The principal ID of the user or team assigned to this task. Null if unassigned. For metadata
    tasks, determines the owner of the grid session. Team members can all join grid sessions
    owned by their team, while user-owned grid sessions are restricted to that user only."""

    _last_persistent_instance: Optional["CurationTask"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = replace(self)
        self._last_persistent_instance.task_properties = (
            deepcopy(self.task_properties) if self.task_properties else None
        )

    def fill_from_dict(
        self, synapse_response: Union[Dict[str, Any], Any]
    ) -> "CurationTask":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The CurationTask object.
        """
        self.task_id = (
            int(synapse_response.get("taskId", None))
            if synapse_response.get("taskId", None)
            else None
        )
        self.data_type = synapse_response.get("dataType", None)
        self.project_id = synapse_response.get("projectId", None)
        self.instructions = synapse_response.get("instructions", None)
        self.etag = synapse_response.get("etag", None)
        self.created_on = synapse_response.get("createdOn", None)
        self.modified_on = synapse_response.get("modifiedOn", None)
        self.created_by = synapse_response.get("createdBy", None)
        self.modified_by = synapse_response.get("modifiedBy", None)
        self.assignee_principal_id = synapse_response.get("assigneePrincipalId", None)

        task_properties_dict = synapse_response.get("taskProperties", None)
        if task_properties_dict is None:
            raise ValueError(
                "taskProperties was not found in the Synapse response for this CurationTask. "
                "This means it is likely an older CurationTask from before taskProperties was added. "
                "It is recommended that this task be deleted: task.delete(delete_source=False)"
            )
        self.task_properties = _create_task_properties_from_dict(task_properties_dict)

        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {}
        request_dict["taskId"] = self.task_id
        request_dict["dataType"] = self.data_type
        request_dict["projectId"] = self.project_id
        request_dict["instructions"] = self.instructions
        request_dict["etag"] = self.etag
        request_dict["createdOn"] = self.created_on
        request_dict["modifiedOn"] = self.modified_on
        request_dict["createdBy"] = self.created_by
        request_dict["modifiedBy"] = self.modified_by
        request_dict["assigneePrincipalId"] = self.assignee_principal_id

        if self.task_properties is not None:
            request_dict["taskProperties"] = self.task_properties.to_synapse_request()

        delete_none_keys(request_dict)
        return request_dict

    async def get_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "CurationTask":
        """
        Gets a CurationTask from Synapse by ID.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            CurationTask: The CurationTask object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.
            ValueError: If the Synapse response does not contain taskProperties.

        Example: Get a curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                task = await CurationTask(task_id=123).get_async()
                print(f"Data type: {task.data_type}")
                print(f"Instructions: {task.instructions}")

            asyncio.run(main())
            ```
        """
        if not self.task_id:
            raise ValueError("task_id is required to get a CurationTask")

        trace.get_current_span().set_attributes(
            {
                "synapse.task_id": str(self.task_id),
            }
        )

        task_result = await get_curation_task(
            task_id=self.task_id, synapse_client=synapse_client
        )
        self.fill_from_dict(synapse_response=task_result)
        self._set_last_persistent_instance()
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: (
            f"CurationTask_GetStatus: ID: {self.task_id}"
        )
    )
    async def get_status_async(
        self, *, synapse_client: Synapse | None = None
    ) -> "CurationTaskStatus":
        """
        Gets the status of this CurationTask from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Get the status of a curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                status = await CurationTask(task_id=123).get_status_async()
                print(status.state)

            asyncio.run(main())
            ```
        """
        if not self.task_id:
            raise ValueError("task_id is required to get a CurationTask status")

        status_result = await get_curation_task_status(
            task_id=self.task_id, synapse_client=synapse_client
        )
        return CurationTaskStatus().fill_from_dict(status_result)

    async def delete_async(
        self,
        delete_source: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Deletes a CurationTask from Synapse.

        Arguments:
            delete_source: If True, the associated source data (EntityView or RecordSet) will also be deleted
                if the task is a FileBasedMetadataTask or RecordBasedMetadataTask respectively. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.
            ValueError: If delete_source is True but the task properties are not properly set
              to identify the source to delete.

        Example: Delete a curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                task = CurationTask(task_id=123)
                await task.delete_async()
                print("Task deleted successfully")

            asyncio.run(main())
            ```

        Example: Delete a curation task and its associated data source asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                task = CurationTask(task_id=123)
                await task.delete_async(delete_source=True)
                print("Task and record set deleted successfully")

            asyncio.run(main())
            ```
        """
        if not self.task_id:
            raise ValueError("task_id is required to delete a CurationTask")

        trace.get_current_span().set_attributes(
            {
                "synapse.task_id": str(self.task_id),
            }
        )

        if delete_source:
            if not self.task_properties:
                await self.get_async(synapse_client=synapse_client)

            if isinstance(self.task_properties, FileBasedMetadataTaskProperties):
                if not self.task_properties.file_view_id:
                    raise ValueError(
                        "Cannot delete Fileview: "
                        "'file_view_id' attribute is missing."
                    )
                from synapseclient.models import EntityView

                await EntityView(id=self.task_properties.file_view_id).delete_async(
                    synapse_client=synapse_client
                )

            elif isinstance(self.task_properties, RecordBasedMetadataTaskProperties):
                if not self.task_properties.record_set_id:
                    raise ValueError(
                        "Cannot delete RecordSet: "
                        "'record_set_id' attribute is missing."
                    )
                from synapseclient.models import RecordSet

                await RecordSet(id=self.task_properties.record_set_id).delete_async(
                    synapse_client=synapse_client
                )

            else:
                raise ValueError(
                    "'task_property' attribute is None. "
                    "Deletion only supports FileBasedMetadataTaskProperties or "
                    "RecordBasedMetadataTaskProperties."
                )

        await delete_curation_task(task_id=self.task_id, synapse_client=synapse_client)

    async def store_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "CurationTask":
        """
        Creates a new CurationTask or updates an existing one on Synapse.

        This method implements non-destructive updates. If a CurationTask with the same
        project_id and data_type exists and this instance hasn't been retrieved from
        Synapse before, it will merge the existing task data with the current instance
        before updating.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            CurationTask: The CurationTask object.

        Example: Create a new curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, FileBasedMetadataTaskProperties

            syn = Synapse()
            syn.login()

            async def main():
                # Create file-based task properties
                file_properties = FileBasedMetadataTaskProperties(
                    upload_folder_id="syn1234567",
                    file_view_id="syn2345678"
                )

                # Create and store the curation task
                task = CurationTask(
                    project_id="syn9876543",
                    data_type="genomics_data",
                    instructions="Upload your genomics files to the specified folder",
                    task_properties=file_properties
                )
                task = await task.store_async()
                print(f"Created task with ID: {task.task_id}")

            asyncio.run(main())
            ```
        """
        if not self.project_id:
            raise ValueError("project_id is required")
        if not self.data_type:
            raise ValueError("data_type is required")

        trace.get_current_span().set_attributes(
            {
                "synapse.data_type": self.data_type or "",
                "synapse.project_id": self.project_id or "",
                "synapse.task_id": str(self.task_id) if self.task_id else "",
            }
        )

        if (
            not self._last_persistent_instance
            and not self.task_id
            and (
                existing_task_id := await _get_existing_curation_task_id(
                    project_id=self.project_id,
                    data_type=self.data_type,
                    synapse_client=synapse_client,
                )
            )
            and (
                existing_task := await CurationTask(task_id=existing_task_id).get_async(
                    synapse_client=synapse_client
                )
            )
        ):
            merge_dataclass_entities(source=existing_task, destination=self)

        if self.task_id:
            task_result = await update_curation_task(
                task_id=self.task_id,
                curation_task=self.to_synapse_request(),
                synapse_client=synapse_client,
            )
            self.fill_from_dict(synapse_response=task_result)
            self._set_last_persistent_instance()
            return self
        else:
            if not self.project_id:
                raise ValueError("project_id is required to create a CurationTask")
            if not self.data_type:
                raise ValueError("data_type is required to create a CurationTask")
            if not self.instructions:
                raise ValueError("instructions is required to create a CurationTask")
            if not self.task_properties:
                raise ValueError("task_properties is required to create a CurationTask")

            task_result = await create_curation_task(
                curation_task=self.to_synapse_request(),
                synapse_client=synapse_client,
            )
            self.fill_from_dict(synapse_response=task_result)
            self._set_last_persistent_instance()
            return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: (
            f"CurationTask_UpdateStatus: ID: {self.task_id}"
        )
    )
    async def update_status_async(
        self,
        curation_task_status: "CurationTaskStatus",
        *,
        synapse_client: Synapse | None = None,
    ) -> "CurationTaskStatus":
        """
        Updates the status of this CurationTask on Synapse.

        Arguments:
            curation_task_status: The complete CurationTaskStatus object to update.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Update the status of a curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import (
                CurationTask,
                TaskState,
                CurationTaskStatus,
            )

            syn = Synapse()
            syn.login()

            async def main():
                task = CurationTask(task_id=123)
                current = await task.get_status_async()
                current.state = TaskState.COMPLETED
                updated = await task.update_status_async(curation_task_status=current)
                print(updated.state)

            asyncio.run(main())
            ```
        """
        if not self.task_id:
            raise ValueError("task_id is required to update a CurationTask status")

        status_result = await update_curation_task_status(
            task_id=self.task_id,
            curation_task_status=curation_task_status.to_synapse_request(),
            synapse_client=synapse_client,
        )
        return CurationTaskStatus().fill_from_dict(status_result)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: (
            f"CurationTask_SetActiveGridSession: ID: {self.task_id}"
        )
    )
    async def set_active_grid_session_async(
        self,
        active_session_id: str,
        *,
        synapse_client: Synapse | None = None,
    ) -> "CurationTaskStatus":
        """
        Set the active grid session on this CurationTask's status by replacing
        execution_details with a GridExecutionDetails carrying the given session id.

        Does not transition the task state. Fetches the current CurationTaskStatus
        first so the update carries a fresh etag.

        Arguments:
            active_session_id: The unique identifier of the active grid session to link.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

        Example: Link a grid session to a curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, Grid

            syn = Synapse()
            syn.login()

            async def main():
                grid = await Grid(record_set_id="syn1234567").create_async()
                await CurationTask(task_id=123).set_active_grid_session_async(
                    active_session_id=grid.session_id
                )

            asyncio.run(main())
            ```
        """
        status = await self.get_status_async(synapse_client=synapse_client)
        status.execution_details = GridExecutionDetails(
            active_session_id=active_session_id
        )
        return await self.update_status_async(
            curation_task_status=status, synapse_client=synapse_client
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: (
            f"CurationTask_SetTaskState: ID: {self.task_id}"
        )
    )
    async def set_task_state_async(
        self,
        state: "TaskState | str",
        *,
        synapse_client: Synapse | None = None,
    ) -> "CurationTaskStatus":
        """
        Set the state on this CurationTask's status.

        Does not modify execution_details. Fetches the current CurationTaskStatus
        first so the update carries a fresh etag.

        Arguments:
            state: The state to set on this task's status. Accepts a
                TaskState or a case-insensitive string matching one of
                its members (e.g. NOT_STARTED, IN_PROGRESS, COMPLETED, CANCELED).
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated CurationTaskStatus object.

        Raises:
            ValueError: If the CurationTask object does not have a task_id, or
                if state is a string that does not match a TaskState member.

        Example: Mark a curation task as completed asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, TaskState

            syn = Synapse()
            syn.login()

            async def main():
                await CurationTask(task_id=123).set_task_state_async(
                    state=TaskState.COMPLETED
                )

            asyncio.run(main())
            ```
        """
        normalized = state.upper() if isinstance(state, str) else state
        try:
            coerced_state = TaskState(normalized)
        except ValueError as exc:
            raise ValueError(
                f"{state!r} is not a valid TaskState. "
                f"Expected one of: {[s.value for s in TaskState]}."
            ) from exc

        status = await self.get_status_async(synapse_client=synapse_client)
        status.state = coerced_state
        return await self.update_status_async(
            curation_task_status=status, synapse_client=synapse_client
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: (
            f"CurationTask_CreateGridSession: ID: {self.task_id}"
        )
    )
    async def create_grid_session_async(
        self,
        *,
        owner_principal_id: int | None = None,
        timeout: int = 120,
        synapse_client: Synapse | None = None,
    ) -> "Grid":
        """
        Create a new Grid session for this CurationTask and set it as the active session.

        Picks the Grid seed from this task's task_properties:

        - RecordBasedMetadataTaskProperties uses record_set_id
        - FileBasedMetadataTaskProperties uses an initial_query that selects from
          the file_view_id

        Always creates a new Grid session. To attach an existing session to a task,
        use set_active_grid_session_async instead.

        After the Grid is created, updates the CurationTaskStatus to point its
        active_session_id at the new session. If that update fails for any reason,
        the newly created Grid is deleted on a best-effort basis and the original
        exception is re-raised.

        Arguments:
            owner_principal_id: The principal ID (user or team) that will own the
                created grid session. When not provided, the principal ID of the
                caller is used.
            timeout: Seconds to wait for the grid creation job. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The newly created Grid.

        Raises:
            ValueError: If task_id is unset or task_properties is of an unsupported type.
            SynapseHTTPError: If the RecordSet or EntityView does not exist, or if the
                status update fails. The orphan Grid is deleted on a best-effort basis
                before the error is re-raised.

        Example: Create a grid session for a curation task asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                grid = await CurationTask(task_id=123).create_grid_session_async()
                print(grid.session_id)

            asyncio.run(main())
            ```
        """
        if not self.task_id:
            raise ValueError(
                "task_id is required to create a CurationTask grid session"
            )

        if not self.task_properties:
            await self.get_async(synapse_client=synapse_client)

        if isinstance(self.task_properties, RecordBasedMetadataTaskProperties):
            if not self.task_properties.record_set_id:
                raise ValueError(
                    "Cannot create grid session: "
                    "task_properties.record_set_id is missing."
                )
            from synapseclient.models import RecordSet

            # raises SynapseHTTPError if RecordSet does not exist
            await RecordSet(id=self.task_properties.record_set_id).get_async(
                synapse_client=synapse_client
            )
            grid = Grid(
                record_set_id=self.task_properties.record_set_id,
                owner_principal_id=owner_principal_id,
            )
        elif isinstance(self.task_properties, FileBasedMetadataTaskProperties):
            if not self.task_properties.file_view_id:
                raise ValueError(
                    "Cannot create grid session: "
                    "task_properties.file_view_id is missing."
                )
            from synapseclient.models import EntityView

            # raises SynapseHTTPError if EntityView does not exist
            await EntityView(id=self.task_properties.file_view_id).get_async(
                synapse_client=synapse_client
            )
            grid = Grid(
                initial_query=Query(
                    sql=f"SELECT * FROM {self.task_properties.file_view_id}"
                ),
                owner_principal_id=owner_principal_id,
            )
        else:
            raise ValueError(
                "task_properties must be a FileBasedMetadataTaskProperties or "
                "RecordBasedMetadataTaskProperties to create a grid session"
            )

        grid = await grid.create_async(
            timeout=timeout,
            synapse_client=synapse_client,
        )

        try:
            await self.set_active_grid_session_async(
                active_session_id=grid.session_id, synapse_client=synapse_client
            )
        except Exception:
            try:
                await grid.delete_async(synapse_client=synapse_client)
            except Exception:
                Synapse.get_client(synapse_client=synapse_client).logger.warning(
                    "Failed to delete orphan grid session %s after status "
                    "update failure; manual cleanup may be required.",
                    grid.session_id,
                )
            raise

        return grid

    @skip_async_to_sync
    @classmethod
    async def list_async(
        cls,
        project_id: str,
        *,
        assigned_to_me: Optional[bool] = None,
        assignee_ids: Optional[list[str]] = None,
        state_filter: Optional[list[Union["TaskState", str]]] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["CurationTask", None]:
        """
        Generator that yields CurationTasks for a project as they become available.

        Arguments:
            project_id: The synId of the project.
            assigned_to_me: When True, only return tasks assigned to the current user.
                Cannot be combined with assignee_ids.
                False does not mean "tasks not assigned to me".
                Defaults to None.
            assignee_ids: Optional list of principal IDs (users or teams) to filter
                tasks by assignee. Cannot be combined with assigned_to_me=True.
                Passing an empty list raises a ValueError; pass None to return tasks
                for any assignee. Defaults to None.
            state_filter: Optional list of TaskState values or exact-case strings to
                filter tasks by their current state (e.g., "IN_PROGRESS"). Defaults to
                None (all states returned). Passing an empty list raises a ValueError;
                pass None to return tasks in any state.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Yields:
            CurationTask objects as they are retrieved from the API.

        Raises:
            ValueError: If state_filter is an empty list.
            ValueError: If assignee_ids is an empty list.
            ValueError: If assigned_to_me is True and assignee_ids is also provided.
            ValueError: If any value in state_filter is not a TaskState member or
                an exact-case string matching a TaskState value (e.g., "IN_PROGRESS").
            ValueError: If the Synapse response for any task does not contain
                taskProperties.

        Example: List all curation tasks in a project asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                # List all curation tasks in the project
                async for task in CurationTask.list_async(project_id="syn9876543"):
                    print(f"Task ID: {task.task_id}")
                    print(f"Data Type: {task.data_type}")
                    print(f"Instructions: {task.instructions}")
                    print("---")

            asyncio.run(main())
            ```

        Example: List only curation tasks assigned to the current user asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                async for task in CurationTask.list_async(
                    project_id="syn9876543", assigned_to_me=True
                ):
                    print(f"Task ID: {task.task_id}")
                    print(f"Data Type: {task.data_type}")
                    print("---")

            asyncio.run(main())
            ```

        Example: List only in-progress curation tasks asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask, TaskState

            syn = Synapse()
            syn.login()

            async def main():
                async for task in CurationTask.list_async(
                    project_id="syn9876543",
                    state_filter=[TaskState.IN_PROGRESS],
                ):
                    print(f"Task ID: {task.task_id}")
                    print(f"Data Type: {task.data_type}")
                    print("---")

            asyncio.run(main())
            ```

        Example: List only in-progress curation tasks using a string state filter asynchronously
            &nbsp;

            state_filter also accepts plain strings matching TaskState names exactly.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import CurationTask

            syn = Synapse()
            syn.login()

            async def main():
                async for task in CurationTask.list_async(
                    project_id="syn9876543",
                    state_filter=["IN_PROGRESS"],
                ):
                    print(f"Task ID: {task.task_id}")
                    print(f"Data Type: {task.data_type}")
                    print("---")

            asyncio.run(main())
            ```
        """
        if state_filter == []:
            raise ValueError(
                "state_filter must not be empty. Pass None to return tasks in any state."
            )
        if assignee_ids == []:
            raise ValueError(
                "assignee_ids must not be empty. Pass None to return tasks for any assignee."
            )
        if assigned_to_me is True and assignee_ids is not None:
            raise ValueError(
                f"assigned_to_me and assignee_ids are mutually exclusive "
                f"and cannot be used together. Got assignee_ids={assignee_ids!r}."
            )

        if state_filter is not None:
            state_filter = coerce_enum_list(TaskState, state_filter)

        trace.get_current_span().set_attributes(
            {
                "synapse.project_id": project_id,
            }
        )

        async for task_dict in list_curation_tasks(
            project_id=project_id,
            assigned_to_me=assigned_to_me,
            assignee_ids=assignee_ids,
            state_filter=state_filter,
            synapse_client=synapse_client,
        ):
            task = cls().fill_from_dict(synapse_response=task_dict)
            yield task


@dataclass
class CreateGridRequest(AsynchronousCommunicator):
    """
    Start a job to create a new Grid session.

    Represents a [Synapse CreateGridRequest](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/CreateGridRequest.html).

    Attributes:
        concrete_type: The concrete type for the request
        record_set_id: When provided, the grid will be initialized using the CSV file
            stored for the given record set id
        initial_query: Initialize a grid session from an EntityView.
            Mutually exclusive with record_set_id.
        owner_principal_id: The owner of the grid determines who is allowed to join and participate in the grid's session.
            The default owner will be the user that started the grid session, but only that user will have access to the grid.
            In order to allow other users to access the grid, set this value to the id of a team.
            When a team ID is provided as the owner, all members of that team will have equal access to the grid.
            Note: If a team ID is provided, the creator of the grid must be a member of the team.
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

    owner_principal_id: int | None = None
    """The owner of the grid determines who is allowed to join and participate in the grid's session.
    The default owner will be the user that started the grid session, but only that user will have access to the grid.
    In order to allow other users to access the grid, set this value to the id of a team.
    When a team ID is provided as the owner, all members of that team will have equal access to the grid.
    Note: If a team ID is provided, the creator of the grid must be a member of the team."""

    session_id: Optional[str] = None
    """The session ID of the created grid (populated from response)"""

    _grid_session_data: Optional[Dict[str, Any]] = field(default=None, compare=False)
    """Internal storage of the full grid session data from the response for later use."""

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
        # Extract session ID from the response body
        grid_session_data = synapse_response.get("gridSession", {})
        self.session_id = grid_session_data.get("sessionId", None)

        # Store the full grid session data for later use
        self._grid_session_data = grid_session_data

        return self

    def fill_grid_session_from_response(self, grid_session: "Grid") -> "Grid":
        """
        Fills a GridSession object with data from the stored response.

        Arguments:
            grid_session: The GridSession object to populate.

        Returns:
            The populated GridSession object.
        """
        if not hasattr(self, "_grid_session_data"):
            return grid_session

        data = self._grid_session_data

        grid_session.session_id = data.get("sessionId", None)
        grid_session.started_by = data.get("startedBy", None)
        grid_session.started_on = data.get("startedOn", None)
        grid_session.etag = data.get("etag", None)
        grid_session.modified_on = data.get("modifiedOn", None)
        grid_session.last_replica_id_client = data.get("lastReplicaIdClient", None)
        grid_session.last_replica_id_service = data.get("lastReplicaIdService", None)
        grid_session.grid_json_schema_id = data.get("gridJsonSchema$Id", None)
        grid_session.source_entity_id = data.get("sourceEntityId", None)
        grid_session.owner_principal_id = data.get("ownerPrincipalId", None)

        return grid_session

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        request_dict["recordSetId"] = self.record_set_id
        request_dict["initialQuery"] = (
            self.initial_query.to_synapse_request() if self.initial_query else None
        )
        request_dict["ownerPrincipalId"] = self.owner_principal_id
        delete_none_keys(request_dict)
        return request_dict


@dataclass
class GridCsvImportRequest(AsynchronousCommunicator):
    """
    A request to import a CSV file into a grid. Currently supports only grid
    created from a record set.

    This request is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/GridCsvImportRequest.html>

    The response is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/GridCsvImportResponse.html>
    """

    session_id: str
    """The grid session ID."""

    file_handle_id: str
    """The id of the file handle that contains the CSV data."""

    schema: list[Column]
    """The list of ColumnModel that describe the CSV file. Currently this is required."""

    concrete_type: str = GRID_CSV_IMPORT_REQUEST
    """The concrete type for this request."""

    csv_descriptor: CsvTableDescriptor = field(default_factory=CsvTableDescriptor)
    """The description of a csv for upload or download."""

    # Response fields (populated by fill_from_dict)
    total_count: Optional[int] = field(default=None, compare=False)
    """The total number of rows in the CSV."""

    created_count: Optional[int] = field(default=None, compare=False)
    """The number of rows that were created."""

    updated_count: Optional[int] = field(default=None, compare=False)
    """The number of rows that were updated."""

    def fill_from_dict(
        self, synapse_response: Dict[str, Any]
    ) -> "GridCsvImportRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The GridCsvImportRequest object.
        """
        self.session_id = synapse_response.get("sessionId", self.session_id)
        self.total_count = synapse_response.get("totalCount", None)
        self.created_count = synapse_response.get("createdCount", None)
        self.updated_count = synapse_response.get("updatedCount", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {
            "concreteType": self.concrete_type,
            "sessionId": self.session_id,
            "fileHandleId": self.file_handle_id,
            "csvDescriptor": self.csv_descriptor.to_synapse_request(),
            "schema": [col.to_synapse_request() for col in self.schema],
        }
        delete_none_keys(request_dict)
        return request_dict


@dataclass
class DownloadFromGridRequest(AsynchronousCommunicator):
    """
    A CSV Grid download request.

    This request is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/DownloadFromGridRequest.html>

    The response is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/DownloadFromGridResult.html>
    """

    session_id: str
    """The grid session ID."""

    concrete_type: str = DOWNLOAD_FROM_GRID_REQUEST
    """The concrete type for this request."""

    write_header: bool = True
    """Should the first line contain the columns names as a header in the resulting file? Set to 'true' to include the headers else, 'false'."""

    include_row_id_and_row_version: bool = False
    """Should the first two columns contain the row ID and row version?"""

    include_etag: bool = False
    """Should the first (or third if includeRowIdAndRowVersion is true) column contain the row etag?"""

    csv_table_descriptor: CsvTableDescriptor = field(default_factory=CsvTableDescriptor)
    """The description of a csv for upload or download."""

    file_name: Optional[str] = None
    """The optional name for the downloaded table."""

    # Response fields (populated by fill_from_dict)
    results_file_handle_id: Optional[str] = None
    """The file handle ID of the generated CSV. Populated from the async job response."""

    def fill_from_dict(
        self, synapse_response: Dict[str, Any]
    ) -> "DownloadFromGridRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The DownloadFromGridRequest object.
        """
        self.results_file_handle_id = synapse_response.get("resultsFileHandleId")
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {
            "concreteType": self.concrete_type,
            "sessionId": self.session_id,
            "writeHeader": self.write_header,
            "includeRowIdAndRowVersion": self.include_row_id_and_row_version,
            "includeEtag": self.include_etag,
            "csvTableDescriptor": self.csv_table_descriptor.to_synapse_request(),
            "fileName": self.file_name,
        }
        delete_none_keys(request_dict)
        return request_dict


@dataclass
class UploadToTablePreviewRequest(AsynchronousCommunicator):
    """
    Request for a preview of an upload to a Table.

    This request is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/UploadToTablePreviewRequest.html>

    This response is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/UploadToTablePreviewResult.html>
    """

    upload_file_handle_id: str
    """The ID of the file handle for a type of UPLOAD"""

    concrete_type: str = UPLOAD_TO_TABLE_PREVIEW_REQUEST
    """The concrete type for this request."""

    lines_to_skip: Optional[int] = None
    """The number of lines to skip from the start of the file. The default value of 0 will be used if this is not provided by the caller."""

    csv_table_descriptor: CsvTableDescriptor = field(default_factory=CsvTableDescriptor)
    """The description of a csv for upload or download."""

    do_full_file_scan: Optional[bool] = None
    """When set to true the full file will be scanned for a schema suggestions. A full scan is more accurate but can take more time. When set to false only a sub-set of the first rows will be scanned, which can be faster but is less accurate. The default value is false."""

    # Response fields (populated by fill_from_dict)
    suggested_columns: Optional[list[Column]] = field(default=None, compare=False)
    """The suggested columns for the table based on the file scan."""

    sample_rows: Optional[list[list[Optional[str]]]] = field(
        default=None, compare=False
    )
    """A sample of the rows in the file."""

    rows_scanned: Optional[int] = field(default=None, compare=False)
    """The number of rows scanned from the file."""

    def fill_from_dict(
        self, synapse_response: Dict[str, Any]
    ) -> "UploadToTablePreviewRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The UploadToTablePreviewRequest object.
        """
        suggested_columns_data = synapse_response.get("suggestedColumns", None)
        if suggested_columns_data is not None:
            self.suggested_columns = [
                Column().fill_from_dict(col) for col in suggested_columns_data
            ]

        sample_rows_data = synapse_response.get("sampleRows", None)
        if sample_rows_data is not None:
            self.sample_rows = [row.get("values", []) for row in sample_rows_data]

        self.rows_scanned = synapse_response.get("rowsScanned", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {
            "concreteType": self.concrete_type,
            "uploadFileHandleId": self.upload_file_handle_id,
            "linesToSkip": self.lines_to_skip,
            "doFullFileScan": self.do_full_file_scan,
            "csvTableDescriptor": self.csv_table_descriptor.to_synapse_request(),
        }
        delete_none_keys(request_dict)
        return request_dict


@dataclass
class GridRecordSetExportRequest(AsynchronousCommunicator):
    """
    A request to export a grid created from a record set back to the original record set.
    A CSV file will be generated and set as a new version of the recordset.

    Represents a [Synapse GridRecordSetExportRequest](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/GridRecordSetExportRequest.html).

    Attributes:
        concrete_type: The concrete type for the request
        session_id: The grid session ID
        response_session_id: The session ID from the export response
        response_record_set_id: The record set ID from the export response
        record_set_version_number: The version number from the export response
        validation_summary_statistics: Summary statistics from the export response
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
        self.response_record_set_id = synapse_response.get("recordSetId", None)
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
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {"concreteType": self.concrete_type}
        if self.session_id is not None:
            request_dict["sessionId"] = self.session_id
        return request_dict


@dataclass
class SynchronizeGridRequest(AsynchronousCommunicator):
    """
    A request to synchronize a grid session.

    The request is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/SynchronizeGridRequest.html>

    The response is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/SynchronizeGridResponse.html>
    """

    grid_session_id: str
    """The ID of the grid session to synchronize."""

    concrete_type: str = field(default=SYNCHRONIZE_GRID_REQUEST)
    """The concrete type for this request."""

    error_messages: Optional[list[str]] = field(default=None, compare=False)
    """Any error messages generated during the synchronization process."""

    def fill_from_dict(
        self, synapse_response: Dict[str, Any]
    ) -> "SynchronizeGridRequest":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The SynchronizeGridRequest object.
        """
        self.error_messages = synapse_response.get("errorMessages", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        return {
            "concreteType": self.concrete_type,
            "gridSessionId": self.grid_session_id,
        }


@dataclass
class GridSession:
    """
    Basic information about a grid session.

    Represents a [Synapse GridSession](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/GridSession.html).

    Attributes:
        session_id: The unique sessionId that identifies the grid session
        started_by: The user that started this session
        started_on: The date-time when the session was started
        etag: Changes when the session changes
        modified_on: The date-time when the session was last changed
        last_replica_id_client: The last replica ID issued to a client
        last_replica_id_service: The last replica ID issued to a service
        grid_json_schema_id: The $id of the JSON schema used for model validation
        source_entity_id: The synId of the table/view/csv that this grid was cloned from
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
    """The last replica ID issued to a client. Client replica IDs are incremented."""

    last_replica_id_service: Optional[int] = None
    """The last replica ID issued to a service. Service replica IDs are decremented."""

    grid_json_schema_id: Optional[str] = None
    """The $id of the JSON schema that will be used for model validation in this grid session"""

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
        self.last_replica_id_client = synapse_response.get("lastReplicaIdClient", None)
        self.last_replica_id_service = synapse_response.get(
            "lastReplicaIdService", None
        )
        self.grid_json_schema_id = synapse_response.get("gridJsonSchema$Id", None)
        self.source_entity_id = synapse_response.get("sourceEntityId", None)
        return self


@dataclass
class ListGridSessionsRequest:
    """
    Request to list a user's active grid sessions.

    Represents a [Synapse ListGridSessionsRequest](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/ListGridSessionsRequest.html).

    Attributes:
        concrete_type: The concrete type for the request
        source_id: Optional. When provided, only sessions with this synId will be returned
        next_page_token: Forward the returned 'nextPageToken' to get the next page of results
    """

    concrete_type: str = LIST_GRID_SESSIONS_REQUEST
    """The concrete type for the request"""

    source_id: Optional[str] = None
    """Optional. When provided, only sessions with this synId will be returned"""

    next_page_token: Optional[str] = None
    """Forward the returned 'nextPageToken' to get the next page of results"""

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

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

    Represents a [Synapse ListGridSessionsResponse](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/grid/ListGridSessionsResponse.html).

    Attributes:
        concrete_type: The concrete type for the response
        page: A single page of results that match the request parameters
        next_page_token: Forward this token to get the next page of results
    """

    concrete_type: str = LIST_GRID_SESSIONS_RESPONSE
    """The concrete type for the response"""

    page: Optional[list[GridSession]] = None
    """A single page of results that match the request parameters"""

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
        """
        Creates a new grid session from a `record_set_id` or `initial_query`.

        Arguments:
            attach_to_previous_session: If True and using `record_set_id`, will attach
                to an existing active session if one exists. Defaults to False.
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            GridSession: The GridSession object with populated session_id.

        Raises:
            ValueError: If `record_set_id` or `initial_query` is not provided.

        Example: Create a grid session from a record set
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            # Create a grid session from a record set
            grid = Grid(record_set_id="syn1234567")
            grid = grid.create()
            print(f"Created grid session: {grid.session_id}")
            ```

        Example: Create a grid session from a query
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid
            from synapseclient.models.table_components import Query

            syn = Synapse()
            syn.login()

            # Create a grid session from an entity view query
            query = Query(sql="SELECT * FROM syn1234567")
            grid = Grid(initial_query=query)
            grid = grid.create()
            print(f"Created grid session: {grid.session_id}")
            ```
        """
        return self

    def export_to_record_set(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """
        Exports the grid session data back to a record set. This will create a new version
        of the original record set with the modified data from the grid session.

        Arguments:
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            GridSession: The GridSession object with export information populated.

        Raises:
            ValueError: If session_id is not provided.

        Example: Export grid session data back to record set
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            # Export modified grid data back to the record set
            grid = Grid(session_id="abc-123-def")
            grid = grid.export_to_record_set()
            print(f"Exported to record set: {grid.record_set_id}")
            print(f"Version number: {grid.record_set_version_number}")
            if grid.validation_summary_statistics:
                print(f"Valid records: {grid.validation_summary_statistics.number_of_valid_children}")
            ```
        """
        return self

    def synchronize(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """
        Synchronizes the grid session's schema and row data against its source entity.

        This is intended for grid sessions created from a file view via `initial_query`.
        Grid sessions backed by a RecordSet should use `export_to_record_set` instead.

        Arguments:
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Grid: The Grid object.

        Raises:
            ValueError: If session_id is not provided.

        Example: Synchronize a grid session created from a file view
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid
            from synapseclient.models.table_components import Query

            syn = Synapse()
            syn.login()

            # First create a grid session from a file view
            query = Query(sql="SELECT * FROM syn1234567")
            grid = Grid(initial_query=query)
            grid = grid.create()

            # Synchronize the grid with the latest state of the file view
            grid = grid.synchronize()
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Delete the grid session.

        Note: Only the user that created a grid session may delete it.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If session_id is not provided.

        Example: Delete a grid session
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            # Delete the grid session
            grid = Grid(session_id="abc-123-def")
            grid.delete()
            ```
        """
        return None

    def download_csv(
        self,
        *,
        destination: Optional[str] = None,
        write_header: bool = True,
        include_row_id_and_row_version: bool = False,
        include_etag: bool = False,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        file_name: Optional[str] = None,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """
        Download the current state of this grid session as a CSV file.

        Submits a DownloadFromGridRequest async job, waits for it to complete,
        then downloads the resulting CSV to the local filesystem.

        Arguments:
            destination: Local directory path where the CSV will be saved.
                If not provided, defaults to the current working directory. The directory must already exist.
            write_header: Whether the first line should contain column names
                as a header. Defaults to True.
            include_row_id_and_row_version: Whether the first two columns
                should contain row ID and version. Defaults to False.
            include_etag: Whether a column should contain the row etag.
                Defaults to False.
            csv_table_descriptor: The description of the CSV format (delimiter,
                quote character, etc.). If not provided, the default CSV format
                will be used.
            file_name: The optional name for the downloaded file. If not
                provided, defaults to `grid_{session_id}-{timestamp}.csv`.
            timeout: The number of seconds to wait for the async job to
                complete or progress before raising a SynapseTimeoutError.
                Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            The local path to the downloaded CSV file.

        Raises:
            ValueError: If session_id is not provided.

        Example: Download a grid session as a CSV
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            grid = Grid(session_id="abc-123-def")
            path = grid.download_csv(destination="./downloads")
            print(f"Downloaded CSV to: {path}")
            ```

        Example: Download a grid session as a CSV with a custom file name
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            grid = Grid(session_id="abc-123-def")
            path = grid.download_csv(destination="./downloads", file_name="my_export.csv")
            print(f"Downloaded CSV to: {path}")
            ```
        """
        return ""

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
            source_id: Optional. When provided, only sessions with this synId will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Grid objects representing active grid sessions.

        Example: List all active grid sessions
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            # List all active grid sessions for the user
            for grid in Grid.list():
                print(f"Session ID: {grid.session_id}")
                print(f"Source Entity: {grid.source_entity_id}")
                print(f"Started: {grid.started_on}")
                print("---")
            ```

        Example: List grid sessions for a specific source
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            # List grid sessions for a specific record set
            for grid in Grid.list(source_id="syn1234567"):
                print(f"Session ID: {grid.session_id}")
                print(f"Modified: {grid.modified_on}")
            ```
        """

    def import_csv(
        self,
        path: str,
        *,
        timeout: int = 120,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """
        Import a CSV file into this grid session. Previews the file to determine
        the column schema, then imports the data. Currently supports only grids
        created from a record set.

        Arguments:
            path: Local path to the CSV file to import.
            csv_table_descriptor: The description of the CSV format (delimiter,
                quote character, etc.). If not provided, the default CSV format
                will be used.
            timeout: The number of seconds to wait for each async job to complete
                or progress before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Grid object.

        Raises:
            ValueError: If session_id is not provided.

        Example: Import a CSV file into a grid session
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            grid = Grid(session_id="abc-123-def")
            grid = grid.import_csv(path="/local/path/to/data.csv")
            print(f"Import complete for session: {grid.session_id}")
            ```
        """
        return self


@dataclass
@async_to_sync
class Grid(GridSynchronousProtocol):
    """
    A GridSession provides functionality to create and manage grid sessions in Synapse.
    Grid sessions are used for curation workflows where data can be edited in a grid format
    and then exported back to record sets.

    Attributes:
        record_set_id: The synId of the RecordSet to use for initializing the grid
        initial_query: Initialize a grid session from an EntityView.
            Mutually exclusive with record_set_id.
        owner_principal_id: The principal ID (user or team) that will own the
            created grid session. When not provided, the principal ID of the
            caller is used.
        session_id: The unique sessionId that identifies the grid session
        started_by: The user that started this session
        started_on: The date-time when the session was started
        etag: Changes when the session changes
        modified_on: The date-time when the session was last changed
        last_replica_id_client: The last replica ID issued to a client
        last_replica_id_service: The last replica ID issued to a service
        grid_json_schema_id: The $id of the JSON schema used for model validation
        source_entity_id: The synId of the table/view/csv that this grid was cloned from
        record_set_version_number: The version number of the exported record set
        validation_summary_statistics: Summary statistics for validation results

    Example: Create and manage a grid session workflow
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Grid

        syn = Synapse()
        syn.login()

        # Create a new grid session from a record set
        grid = Grid(record_set_id="syn1234567")
        grid = grid.create()
        print(f"Created grid session: {grid.session_id}")

        # Later, export the modified data back to the record set
        grid = grid.export_to_record_set()
        print(f"Exported to version: {grid.record_set_version_number}")

        # Clean up by deleting the session when done
        grid.delete()
        ```

    Example: Working with grid sessions using queries
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Grid
        from synapseclient.models.table_components import Query

        syn = Synapse()
        syn.login()

        # Create a grid from an entity view query
        query = Query(sql="SELECT * FROM syn1234567")
        grid = Grid(initial_query=query)
        grid = grid.create()

        # Work with the grid session...
        # Export when ready
        grid = grid.export_to_record_set()
        ```
    """

    record_set_id: Optional[str] = None
    """The synId of the RecordSet to use for initializing the grid"""

    initial_query: Optional[Query] = None
    """Initialize a grid session from an EntityView.
    Mutually exclusive with record_set_id."""

    owner_principal_id: int | None = None
    """The principal ID (user or team) that will own the created grid session.
    When not provided, the principal ID of the caller is used."""

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
    """The last replica ID issued to a client. Client replica IDs are incremented."""

    last_replica_id_service: Optional[int] = None
    """The last replica ID issued to a service. Service replica IDs are decremented."""

    grid_json_schema_id: Optional[str] = None
    """The $id of the JSON schema that will be used for model validation in this grid session"""

    source_entity_id: Optional[str] = None
    """The synId of the table/view/csv that this grid was cloned from"""

    record_set_version_number: Optional[int] = None
    """The version number of the exported record set"""

    validation_summary_statistics: Optional[ValidationSummary] = None
    """Summary statistics for validation results"""

    async def create_async(
        self,
        attach_to_previous_session=False,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """
        Creates a new grid session from a `record_set_id` or `initial_query`.

        When using `record_set_id`, first checks for existing active sessions that match
        the record set before creating a new one. When using `initial_query`, always
        creates a new session due to the complexity of matching query parameters.

        Arguments:
            attach_to_previous_session: If True and using `record_set_id`, will attach
                to an existing active session if one exists. Defaults to False.
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            GridSession: The GridSession object with populated session_id.

        Raises:
            ValueError: If `record_set_id` or `initial_query` is not provided.

        Example: Create a grid session asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                # Create a grid session from a record set
                grid = Grid(record_set_id="syn1234567")
                grid = await grid.create_async()
                print(f"Created grid session: {grid.session_id}")

            asyncio.run(main())
            ```
        """
        if not self.record_set_id and not self.initial_query:
            raise ValueError(
                "record_set_id or initial_query is required to create a GridSession"
            )

        trace.get_current_span().set_attributes(
            {
                "synapse.record_set_id": self.record_set_id or "",
                "synapse.session_id": self.session_id or "",
            }
        )

        # Check for existing active sessions only when using record_set_id
        # For initial_query, always create a new session due to complexity of matching
        if self.record_set_id and attach_to_previous_session:
            # Look for existing active sessions for this record set
            async for existing_session in self.list_async(
                source_id=self.record_set_id, synapse_client=synapse_client
            ):
                # Found an existing session, populate this object with its data and return
                self.session_id = existing_session.session_id
                self.started_by = existing_session.started_by
                self.started_on = existing_session.started_on
                self.etag = existing_session.etag
                self.modified_on = existing_session.modified_on
                self.last_replica_id_client = existing_session.last_replica_id_client
                self.last_replica_id_service = existing_session.last_replica_id_service
                self.grid_json_schema_id = existing_session.grid_json_schema_id
                self.source_entity_id = existing_session.source_entity_id
                return self

        # No existing session found, create a new one
        create_request = CreateGridRequest(
            record_set_id=self.record_set_id,
            initial_query=self.initial_query,
            owner_principal_id=self.owner_principal_id,
        )
        result = await create_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        # Fill this GridSession with the grid session data from the async job response
        result.fill_grid_session_from_response(self)

        return self

    async def export_to_record_set_async(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """
        Exports the grid session data back to a record set. This will create a new version
        of the original record set with the modified data from the grid session.

        Arguments:
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            GridSession: The GridSession object with export information populated.

        Raises:
            ValueError: If session_id is not provided.

        Example: Export grid session data back to record set asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                # Export modified grid data back to the record set
                grid = Grid(session_id="abc-123-def")
                grid = await grid.export_to_record_set_async()
                print(f"Exported to record set: {grid.record_set_id}")
                print(f"Version number: {grid.record_set_version_number}")
                if grid.validation_summary_statistics:
                    print(f"Valid records: {grid.validation_summary_statistics.number_of_valid_children}")

            asyncio.run(main())
            ```
        """
        if not self.session_id:
            raise ValueError("session_id is required to export a GridSession")

        trace.get_current_span().set_attributes(
            {
                "synapse.session_id": self.session_id or "",
            }
        )

        # Create and send the export request
        export_request = GridRecordSetExportRequest(session_id=self.session_id)
        result = await export_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        self.record_set_id = result.response_record_set_id
        self.record_set_version_number = result.record_set_version_number
        self.validation_summary_statistics = result.validation_summary_statistics

        return self

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "Grid":
        """Converts a response from the REST API into this dataclass."""
        self.session_id = synapse_response.get("sessionId", None)
        self.started_by = synapse_response.get("startedBy", None)
        self.started_on = synapse_response.get("startedOn", None)
        self.etag = synapse_response.get("etag", None)
        self.modified_on = synapse_response.get("modifiedOn", None)
        self.last_replica_id_client = synapse_response.get("lastReplicaIdClient", None)
        self.last_replica_id_service = synapse_response.get(
            "lastReplicaIdService", None
        )
        self.grid_json_schema_id = synapse_response.get("gridJsonSchema$Id", None)
        self.source_entity_id = synapse_response.get("sourceEntityId", None)
        self.owner_principal_id = synapse_response.get("ownerPrincipalId", None)
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
            source_id: Optional. When provided, only sessions with this synId will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Grid objects representing active grid sessions.

        Example: List all active grid sessions asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                # List all active grid sessions for the user
                async for grid in Grid.list_async():
                    print(f"Session ID: {grid.session_id}")
                    print(f"Source Entity: {grid.source_entity_id}")
                    print(f"Started: {grid.started_on}")
                    print("---")

                # List grid sessions for a specific source
                async for grid in Grid.list_async(source_id="syn1234567"):
                    print(f"Session ID: {grid.session_id}")
                    print(f"Modified: {grid.modified_on}")

            asyncio.run(main())
            ```
        """
        async for session_dict in list_grid_sessions(
            source_id=source_id, synapse_client=synapse_client
        ):
            # Convert the dictionary to a Grid object
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
            source_id: Optional. When provided, only sessions with this synId will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Grid objects representing active grid sessions.
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=cls.list_async,
            source_id=source_id,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Grid_Delete: ID: {self.session_id}"
    )
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Delete the grid session.

        Note: Only the user that created a grid session may delete it.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If session_id is not provided.

        Example: Delete a grid session asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                # Delete the grid session
                grid = Grid(session_id="abc-123-def")
                await grid.delete_async()
                print("Grid session deleted successfully")

            asyncio.run(main())
            ```
        """
        if not self.session_id:
            raise ValueError("session_id is required to delete a GridSession")

        trace.get_current_span().set_attributes(
            {
                "synapse.session_id": self.session_id or "",
            }
        )

        await delete_grid_session(
            session_id=self.session_id, synapse_client=synapse_client
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Grid_ImportCsv: ID: {self.session_id}"
    )
    async def import_csv_async(
        self,
        path: str,
        *,
        timeout: int = 120,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """
        Import a CSV file into this grid session. Previews the file to determine
        the column schema, then imports the data. Currently supports only grids
        created from a record set.

        Arguments:
            path: Local path to the CSV file to import.
            csv_table_descriptor: The description of the CSV format (delimiter,
                quote character, etc.). If not provided, the default CSV format
                will be used.
            timeout: The number of seconds to wait for each async job to complete
                or progress before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Grid object.

        Raises:
            ValueError: If session_id is not provided.

        Example: Import a CSV file into a grid session asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                grid = Grid(session_id="abc-123-def")
                grid = await grid.import_csv_async(path="/local/path/to/data.csv")
                print(f"Import complete for session: {grid.session_id}")

            asyncio.run(main())
            ```
        """

        if not self.session_id:
            raise ValueError(
                "session_id is required to import a CSV into a GridSession"
            )

        if not os.path.isfile(path):
            raise ValueError(f"Path '{path}' is not a valid file.")

        trace.get_current_span().set_attributes(
            {
                "synapse.session_id": self.session_id,
            }
        )

        client = Synapse.get_client(synapse_client=synapse_client)
        file_handle = await upload_synapse_s3(syn=client, file_path=path)
        file_handle_id = file_handle["id"]

        effective_descriptor = csv_table_descriptor or CsvTableDescriptor()

        upload_to_table_preview = UploadToTablePreviewRequest(
            csv_table_descriptor=effective_descriptor,
            upload_file_handle_id=file_handle_id,
        )

        preview_response = await upload_to_table_preview.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )
        if not preview_response.suggested_columns:
            raise ValueError(
                f"CSV preview for file handle {file_handle_id} returned no suggested "
                f"columns (rows scanned: {preview_response.rows_scanned}). The file may "
                f"be empty, contain only a header row, or use a separator different "
                f"from the configured csv_table_descriptor "
                f"(separator={repr(effective_descriptor.separator)})."
            )

        import_request = GridCsvImportRequest(
            session_id=self.session_id,
            file_handle_id=file_handle_id,
            schema=preview_response.suggested_columns,
            csv_descriptor=effective_descriptor,
        )
        import_response = await import_request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )
        client.logger.info(
            f"CSV import to grid session {self.session_id} completed successfully, "
            f"total count: {import_response.total_count}, "
            f"total created: {import_response.created_count}, "
            f"total updated: {import_response.updated_count}"
        )

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, *args, **kwargs: f"Grid_DownloadCsv: ID: {self.session_id}"
    )
    async def download_csv_async(
        self,
        *,
        destination: Optional[str] = None,
        write_header: bool = True,
        include_row_id_and_row_version: bool = False,
        include_etag: bool = False,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        file_name: Optional[str] = None,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """
        Asynchronously download the current state of this grid session as a CSV file.

        Submits a DownloadFromGridRequest async job, waits for it to complete,
        then downloads the resulting CSV to the local filesystem.

        Arguments:
            destination: Local directory path where the CSV will be saved. The directory must already exist.
                If not provided, defaults to the current working directory.
            write_header: Whether the first line should contain column names
                as a header. Defaults to True.
            include_row_id_and_row_version: Whether the first two columns
                should contain row ID and version. Defaults to False.
            include_etag: Whether a column should contain the row etag.
                Defaults to False.
            csv_table_descriptor: The description of the CSV format (delimiter,
                quote character, etc.). If not provided, the default CSV format
                will be used.
            file_name: The optional name for the downloaded file. If not
                provided, defaults to `grid_{session_id}-{timestamp}.csv`.
            timeout: The number of seconds to wait for the async job to
                complete or progress before raising a SynapseTimeoutError.
                Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last
                created instance from the Synapse class constructor.

        Returns:
            The local path to the downloaded CSV file.

        Raises:
            ValueError: If session_id is not provided.

        Example: Download a grid session as a CSV asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                grid = Grid(session_id="abc-123-def")
                path = await grid.download_csv_async(destination="./downloads")
                print(f"Downloaded CSV to: {path}")

            asyncio.run(main())
            ```

        Example: Download a grid session as a CSV with a custom file name asynchronously
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid

            syn = Synapse()
            syn.login()

            async def main():
                grid = Grid(session_id="abc-123-def")
                path = await grid.download_csv_async(
                    destination="./downloads", file_name="my_export.csv"
                )
                print(f"Downloaded CSV to: {path}")

            asyncio.run(main())
            ```
        """
        if not self.session_id:
            raise ValueError("session_id is required to download a GridSession as CSV")

        if not destination:
            destination = os.getcwd()

        if not os.path.isdir(destination):
            raise ValueError(f"Destination {destination} is not a valid directory.")

        trace.get_current_span().set_attributes({"synapse.session_id": self.session_id})

        effective_descriptor = csv_table_descriptor or CsvTableDescriptor()
        request = DownloadFromGridRequest(
            session_id=self.session_id,
            write_header=write_header,
            include_row_id_and_row_version=include_row_id_and_row_version,
            include_etag=include_etag,
            csv_table_descriptor=effective_descriptor,
            file_name=file_name,
        )
        download_response = await request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )
        if not download_response.results_file_handle_id:
            raise ValueError(
                f"Download job for grid session '{self.session_id}' completed but "
                "did not return a file handle ID. The CSV result may be empty or "
                "the job may have failed silently."
            )
        file_handle, presigned_url = await asyncio.gather(
            get_file_handle(
                file_handle_id=download_response.results_file_handle_id,
                synapse_client=synapse_client,
            ),
            get_file_handle_presigned_url(
                file_handle_id=download_response.results_file_handle_id,
                synapse_client=synapse_client,
            ),
        )
        if not file_name:
            timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
            file_name = f"grid_{self.session_id}-{timestamp}.csv"
        file_path = os.path.join(destination, file_name)
        return await asyncio.to_thread(
            download_from_url,
            url=presigned_url,
            destination=file_path,
            file_handle_id=file_handle["id"],
            expected_md5=file_handle.get("contentMd5"),
            url_is_presigned=True,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Grid_Synchronize: ID: {self.session_id}"
    )
    async def synchronize_async(
        self, *, timeout: int = 120, synapse_client: Optional[Synapse] = None
    ) -> "Grid":
        """
        Synchronizes the grid session's schema and row data against its source entity.

        This is intended for grid sessions created from a file view via `initial_query`.
        Grid sessions backed by a RecordSet should use `export_to_record_set` instead.

        Arguments:
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Grid: The Grid object.

        Raises:
            ValueError: If session_id is not provided.

        Example: Synchronize a grid session created from a file view
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Grid
            from synapseclient.models.table_components import Query

            syn = Synapse()
            syn.login()

            async def main():
                # First create a grid session from a file view
                query = Query(sql="SELECT * FROM syn1234567")
                grid = Grid(initial_query=query)
                grid = await grid.create_async()

                # Synchronize the grid with the latest state of the file view
                grid = await grid.synchronize_async()

            asyncio.run(main())
            ```
        """
        if not self.session_id:
            raise ValueError("session_id is required to synchronize a GridSession")

        request = SynchronizeGridRequest(grid_session_id=self.session_id)
        result = await request.send_job_and_wait_async(
            timeout=timeout, synapse_client=synapse_client
        )

        if result.error_messages:
            client = Synapse.get_client(synapse_client=synapse_client)
            client.logger.error(
                f"Grid session '{self.session_id}' synchronization completed with "
                f"error messages: {result.error_messages}"
            )

        return self
