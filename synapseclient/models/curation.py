"""
Curation Task dataclasses for managing Curation Tasks in Synapse.

Curation tasks are used to guide data contributors through the process of contributing
data or metadata in Synapse.
"""

from dataclasses import dataclass, field, replace
from typing import Any, AsyncGenerator, Dict, Generator, Optional, Protocol, Union

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import (
    create_curation_task,
    delete_curation_task,
    delete_grid_session,
    get_curation_task,
    list_curation_tasks,
    list_grid_sessions,
    update_curation_task,
)
from synapseclient.core.async_utils import (
    async_to_sync,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.constants.concrete_types import (
    CREATE_GRID_REQUEST,
    FILE_BASED_METADATA_TASK_PROPERTIES,
    GRID_RECORD_SET_EXPORT_REQUEST,
    LIST_GRID_SESSIONS_REQUEST,
    LIST_GRID_SESSIONS_RESPONSE,
    RECORD_BASED_METADATA_TASK_PROPERTIES,
)
from synapseclient.core.utils import delete_none_keys, merge_dataclass_entities
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.recordset import ValidationSummary
from synapseclient.models.table_components import Query


@dataclass
class FileBasedMetadataTaskProperties:
    """
    A CurationTaskProperties for file-based data, describing where data is uploaded
    and a view which contains the annotations.

    Represents a [Synapse FileBasedMetadataTaskProperties](https://rest-docs.synapse.org/org/sagebionetworks/repo/model/curation/metadata/FileBasedMetadataTaskProperties.html).

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

    Represents a [Synapse RecordBasedMetadataTaskProperties](https://rest-docs.synapse.org/org/sagebionetworks/repo/model/curation/metadata/RecordBasedMetadataTaskProperties.html).

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
    properties_dict: Dict[str, Any]
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

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Deletes a CurationTask from Synapse.

        Arguments:
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
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["CurationTask", None, None]:
        """
        Generator that yields CurationTasks for a project as they become available.

        Arguments:
            project_id: The synId of the project.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            CurationTask objects as they are retrieved from the API.

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
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=cls.list_async,
            project_id=project_id,
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

        task_properties_dict = synapse_response.get("taskProperties", None)
        if task_properties_dict:
            self.task_properties = _create_task_properties_from_dict(
                task_properties_dict
            )

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

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Deletes a CurationTask from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.

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
        """
        if not self.task_id:
            raise ValueError("task_id is required to delete a CurationTask")

        trace.get_current_span().set_attributes(
            {
                "synapse.task_id": str(self.task_id),
            }
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

    @skip_async_to_sync
    @classmethod
    async def list_async(
        cls,
        project_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["CurationTask", None]:
        """
        Generator that yields CurationTasks for a project as they become available.

        Arguments:
            project_id: The synId of the project.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            CurationTask objects as they are retrieved from the API.

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
        """
        trace.get_current_span().set_attributes(
            {
                "synapse.project_id": project_id,
            }
        )

        async for task_dict in list_curation_tasks(
            project_id=project_id, synapse_client=synapse_client
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
        attach_to_previous_session=True,
        *,
        timeout: int = 120,
        synapse_client: Optional[Synapse] = None,
    ) -> "Grid":
        """
        Creates a new grid session from a `record_set_id` or `initial_query`.

        Arguments:
            attach_to_previous_session: If True and using `record_set_id`, will attach
                to an existing active session if one exists. Defaults to True.
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
        attach_to_previous_session=True,
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
                to an existing active session if one exists. Defaults to True.
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
            record_set_id=self.record_set_id, initial_query=self.initial_query
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
