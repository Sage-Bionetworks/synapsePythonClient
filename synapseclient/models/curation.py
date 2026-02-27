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
    get_curation_task,
    list_curation_tasks,
    update_curation_task,
)
from synapseclient.core.async_utils import (
    async_to_sync,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.constants.concrete_types import (
    FILE_BASED_METADATA_TASK_PROPERTIES,
    RECORD_BASED_METADATA_TASK_PROPERTIES,
)
from synapseclient.core.utils import delete_none_keys, merge_dataclass_entities


@dataclass
class FileBasedMetadataTaskProperties:
    """
    A CurationTaskProperties for file-based data, describing where data is uploaded
    and a view which contains the annotations.

    Represents a [Synapse FileBasedMetadataTaskProperties]\
(https://rest-docs.synapse.org/org/sagebionetworks/repo/model/curation/metadata/FileBasedMetadataTaskProperties.html).

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

    Represents a [Synapse RecordBasedMetadataTaskProperties]\
(https://rest-docs.synapse.org/org/sagebionetworks/repo/model/curation/metadata/RecordBasedMetadataTaskProperties.html).

    Attributes:
        record_set_id: The synId of the RecordSet that will contain all record-based metadata
    """

    record_set_id: Optional[str] = None
    """The synId of the RecordSet that will contain all record-based metadata of this type"""

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
    Factory method to create the appropriate task properties based on the concreteType.

    Arguments:
        properties_dict: Dictionary containing task properties data

    Returns:
        The appropriate task properties instance
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
        """Gets a CurationTask from Synapse by ID."""
        return self

    def delete(
        self,
        delete_file_view: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Deletes a CurationTask from Synapse.

        Arguments:
            delete_file_view: If True and the task has FileBasedMetadataTaskProperties,
                also delete the associated EntityView. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
        """
        return None

    def store(self, *, synapse_client: Optional[Synapse] = None) -> "CurationTask":
        """Creates a new CurationTask or updates an existing one on Synapse."""
        return self

    @classmethod
    def list(
        cls,
        project_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["CurationTask", None, None]:
        """Generator that yields CurationTasks for a project."""
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

    Represents a [Synapse CurationTask]\
(https://rest-docs.synapse.org/org/sagebionetworks/repo/model/curation/CurationTask.html).

    Attributes:
        task_id: The unique identifier issued to this task when it was created
        data_type: Will match the data type that a contributor plans to contribute
        project_id: The synId of the project
        instructions: Instructions to the data contributor
        task_properties: The properties of a CurationTask. This can be either
            FileBasedMetadataTaskProperties or RecordBasedMetadataTaskProperties.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme
        created_on: (Read Only) The date this task was created
        modified_on: (Read Only) The date this task was last modified
        created_by: (Read Only) The ID of the user that created this task
        modified_by: (Read Only) The ID of the user that last modified this task
        assignee_principal_id: The principal ID of the user or team assigned
    """

    task_id: Optional[int] = None
    """The unique identifier issued to this task when it was created"""

    data_type: Optional[str] = None
    """Will match the data type that a contributor plans to contribute"""

    project_id: Optional[str] = None
    """The synId of the project"""

    instructions: Optional[str] = None
    """Instructions to the data contributor"""

    task_properties: Optional[
        Union[FileBasedMetadataTaskProperties, RecordBasedMetadataTaskProperties]
    ] = None
    """The properties of a CurationTask"""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme"""

    created_on: Optional[str] = None
    """(Read Only) The date this task was created"""

    modified_on: Optional[str] = None
    """(Read Only) The date this task was last modified"""

    created_by: Optional[str] = None
    """(Read Only) The ID of the user that created this task"""

    modified_by: Optional[str] = None
    """(Read Only) The ID of the user that last modified this task"""

    assignee_principal_id: Optional[str] = None
    """The principal ID of the user or team assigned to this task."""

    _last_persistent_instance: Optional["CurationTask"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object."""

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse."""
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
        self.assignee_principal_id = synapse_response.get("assigneePrincipalId", None)

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
        """
        if not self.task_id:
            raise ValueError("task_id is required to get a CurationTask")

        trace.get_current_span().set_attributes(
            {"synapse.task_id": str(self.task_id)}
        )

        task_result = await get_curation_task(
            task_id=self.task_id, synapse_client=synapse_client
        )
        self.fill_from_dict(synapse_response=task_result)
        self._set_last_persistent_instance()
        return self

    async def delete_async(
        self,
        delete_file_view: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Deletes a CurationTask from Synapse.

        Arguments:
            delete_file_view: If True and the task has FileBasedMetadataTaskProperties,
                also delete the associated EntityView. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the CurationTask object does not have a task_id.
        """
        if not self.task_id:
            raise ValueError("task_id is required to delete a CurationTask")

        trace.get_current_span().set_attributes(
            {"synapse.task_id": str(self.task_id)}
        )

        file_view_id = None
        if delete_file_view:
            if not self.task_properties and self.task_id:
                await self.get_async(synapse_client=synapse_client)
            if isinstance(self.task_properties, FileBasedMetadataTaskProperties):
                file_view_id = self.task_properties.file_view_id

        await delete_curation_task(task_id=self.task_id, synapse_client=synapse_client)

        if delete_file_view and file_view_id:
            from synapseclient.api.entity_services import delete_entity

            client = Synapse.get_client(synapse_client=synapse_client)
            await delete_entity(
                entity_id=file_view_id, synapse_client=client
            )

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
                existing_task := await CurationTask(
                    task_id=existing_task_id
                ).get_async(synapse_client=synapse_client)
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
                raise ValueError(
                    "project_id is required to create a CurationTask"
                )
            if not self.data_type:
                raise ValueError(
                    "data_type is required to create a CurationTask"
                )
            if not self.instructions:
                raise ValueError(
                    "instructions is required to create a CurationTask"
                )
            if not self.task_properties:
                raise ValueError(
                    "task_properties is required to create a CurationTask"
                )

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
        """
        async for task_dict in list_curation_tasks(
            project_id=project_id, synapse_client=synapse_client
        ):
            task = cls()
            task.fill_from_dict(task_dict)
            task._set_last_persistent_instance()
            yield task

    @classmethod
    def list(
        cls,
        project_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["CurationTask", None, None]:
        """
        Generator that yields CurationTasks for a project.

        Arguments:
            project_id: The synId of the project.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            CurationTask objects as they are retrieved from the API.
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=cls.list_async,
            project_id=project_id,
            synapse_client=synapse_client,
        )
