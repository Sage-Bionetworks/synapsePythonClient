import asyncio
from dataclasses import dataclass
from typing import Dict, Union
from opentelemetry import trace, context
from synapseclient.models import AnnotationsValue, Annotations

# import uuid

from synapseclient.entity import File as Synapse_File
from synapseclient import Synapse

from typing import Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from synapseclient.models import Folder, Project


tracer = trace.get_tracer("synapseclient")


@dataclass()
class File:
    """A file within Synapse.

    Attributes:
        id: The unique immutable ID for this file. A new ID will be generated for new Files.
            Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this entity. Must be 256 characters or less.
            Names may only contain: letters, numbers, spaces, underscores, hyphens, periods,
            plus signs, apostrophes, and parentheses.
        path: The path to the file.
        description: The description of this file. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated it
            is used to detect when a client's current representation of an entity is out-of-date.
        created_on: The date this entity was created.
        modified_on: The date this entity was last modified.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        parent_id: The ID of the Entity that is the parent of this Entity.
        version_number: Indicates which implementation of Entity this object represents. The value is
            the fully qualified class name, e.g. org.sagebionetworks.repo.model.FileEntity.
        version_label: The version label for this entity.
        version_comment: The version comment for this entity.
        is_latest_version: If this is the latest version of the object.
        data_file_handle_id: ID of the file associated with this entity.
        file_name_override: An optional replacement for the name of the uploaded file. This is distinct
            from the entity name. If omitted the file will retain its original name.
        annotations: Additional metadata associated with the folder. The key is the name of your
            desired annotations. The value is an object containing a list of values
            (use empty list to represent no values for key) and the value type associated with
            all values in the list.
        is_loaded: If the file has been loaded from Synapse.


    """

    id: Optional[str] = None
    """The unique immutable ID for this file. A new ID will be generated for new Files.
    Once issued, this ID is guaranteed to never change or be re-issued."""

    name: Optional[str] = None
    """The name of this entity. Must be 256 characters or less.
    Names may only contain: letters, numbers, spaces, underscores, hyphens, periods,
    plus signs, apostrophes, and parentheses."""

    path: Optional[str] = None
    # TODO - Should a file also have a folder, or a method that figures out the folder class?

    # TODO: Description doesn't seem to be working properly
    description: Optional[str] = None
    """The description of this file. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it
    is used to detect when a client's current representation of an entity is out-of-date."""

    created_on: Optional[str] = None
    """The date this entity was created."""

    modified_on: Optional[str] = None
    """The date this entity was last modified."""

    created_by: Optional[str] = None
    """The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this Entity."""

    version_number: Optional[int] = None
    """Indicates which implementation of Entity this object represents. The value is
    the fully qualified class name, e.g. org.sagebionetworks.repo.model.FileEntity."""

    version_label: Optional[str] = None
    """The version label for this entity"""

    version_comment: Optional[str] = None
    """The version comment for this entity"""

    is_latest_version: Optional[bool] = False
    """If this is the latest version of the object."""

    data_file_handle_id: Optional[str] = None
    """ID of the file associated with this entity."""

    file_name_override: Optional[str] = None
    """An optional replacement for the name of the uploaded file. This is distinct
    from the entity name. If omitted the file will retain its original name."""

    annotations: Optional[Dict[str, AnnotationsValue]] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list."""

    # TODO: We need to provide functionality for folks to store the file in Synapse, but
    # TODO: Not upload the file.

    is_loaded: bool = False

    def convert_from_api_parameters(
        self, synapse_file: Synapse_File, set_annotations: bool = True
    ) -> "File":
        self.id = synapse_file.get("id", None)
        self.name = synapse_file.get("name", None)
        self.path = synapse_file.get("path", None)
        self.description = synapse_file.get("description", None)
        self.etag = synapse_file.get("etag", None)
        self.created_on = synapse_file.get("createdOn", None)
        self.modified_on = synapse_file.get("modifiedOn", None)
        self.created_by = synapse_file.get("createdBy", None)
        self.modified_by = synapse_file.get("modifiedBy", None)
        self.parent_id = synapse_file.get("parentId", None)
        self.version_number = synapse_file.get("versionNumber", None)
        self.version_label = synapse_file.get("versionLabel", None)
        self.version_comment = synapse_file.get("versionComment", None)
        self.is_latest_version = synapse_file.get("isLatestVersion", None)
        self.data_file_handle_id = synapse_file.get("dataFileHandleId", None)
        self.file_name_override = synapse_file.get("fileNameOverride", None)
        if set_annotations:
            self.annotations = Annotations.convert_from_api_parameters(
                synapse_file.get("annotations", None)
            )
        return self

    # TODO: How the parent is stored/referenced needs to be thought through
    async def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """Store the file in Synapse.

        Args:
            parent: The parent folder or project to store the file in.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The file object.
        """
        with tracer.start_as_current_span(
            f"File_Store: {self.path if self.path else self.id}"
        ):
            # TODO - We need to add in some validation before the store to verify we have enough
            # information to store the data

            # Call synapse
            if self.path:
                loop = asyncio.get_event_loop()
                synapse_file = Synapse_File(
                    path=self.path,
                    name=self.name,
                    parent=parent.id if parent else self.parent_id,
                )
                # TODO: Propogating OTEL context is not working in this case
                current_context = context.get_current()
                entity = await loop.run_in_executor(
                    None,
                    lambda: Synapse.get_client(synapse_client=synapse_client).store(
                        obj=synapse_file, opentelemetry_context=current_context
                    ),
                )

                self.convert_from_api_parameters(
                    synapse_file=entity, set_annotations=False
                )

                print(f"Stored file {self.name}, id: {self.id}: {self.path}")
            elif self.id and not self.etag:
                # This elif is to handle if only annotations are being stored without
                # a file path.
                annotations_to_persist = self.annotations
                await self.get(synapse_client=synapse_client, download_file=False)
                self.annotations = annotations_to_persist

            if self.annotations:
                result = await Annotations(
                    id=self.id, etag=self.etag, annotations=self.annotations
                ).store(synapse_client=synapse_client)
                self.annotations = result.annotations

            return self

    # TODO: We need to provide all the other options that can be provided here, like collision, follow link ect...
    async def get(
        self,
        download_file: Optional[bool] = True,
        download_location: Optional[str] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """Get the file metadata from Synapse.

        Args:
            download_file: If True the file will be downloaded.
            download_location: The location to download the file to.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The file object.
        """
        with tracer.start_as_current_span(f"File_Get: {self.id}"):
            loop = asyncio.get_event_loop()
            current_context = context.get_current()
            entity = await loop.run_in_executor(
                None,
                lambda: Synapse.get_client(synapse_client=synapse_client).get(
                    entity=self.id,
                    downloadFile=download_file,
                    downloadLocation=download_location,
                    opentelemetry_context=current_context,
                ),
            )

            self.convert_from_api_parameters(synapse_file=entity, set_annotations=True)
            return self

    async def delete(self, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the file from Synapse.

        Args:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            None
        """
        with tracer.start_as_current_span(f"File_Delete: {self.id}"):
            loop = asyncio.get_event_loop()
            current_context = context.get_current()
            await loop.run_in_executor(
                None,
                lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                    obj=self.id,
                    opentelemetry_context=current_context,
                ),
            )
