import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Union
from typing import Optional, TYPE_CHECKING
from opentelemetry import trace, context

from synapseclient import Synapse
from synapseclient.entity import Folder as Synapse_Folder
from synapseclient.models import File, Annotations
from synapseclient.core.async_utils import otel_trace_method

if TYPE_CHECKING:
    from synapseclient.models import Project

tracer = trace.get_tracer("synapseclient")


@dataclass()
class Folder:
    """Folder is a hierarchical container for organizing data in Synapse.

    Attributes:
        id: The unique immutable ID for this folder. A new ID will be generated for new
            Folders. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this folder. Must be 256 characters or less. Names may only contain:
            letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
            and parentheses.
        parent_id: The ID of the Project or Folder that is the parent of this Folder.
        description: The description of this entity. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent
            updates. Since the E-Tag changes every time an entity is updated it is used to detect
            when a client's current representation of an entity is out-of-date.
        created_on: The date this entity was created.
        modified_on: The date this entity was last modified.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        files: Files that exist within this folder.
        folders: Folders that exist within this folder.
        annotations: Additional metadata associated with the folder. The key is the name of your
            desired annotations. The value is an object containing a list of values
            (use empty list to represent no values for key) and the value type associated with
            all values in the list.
    """

    id: Optional[str] = None
    """The unique immutable ID for this folder. A new ID will be generated for new
    Folders. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this folder. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses"""

    # TODO: What are all of the things that could be the parent of a folder?
    parent_id: Optional[str] = None
    """The ID of the Project or Folder that is the parent of this Folder."""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it
    is used to detect when a client's current representation of an entity
    is out-of-date."""

    created_on: Optional[str] = None
    """The date this entity was created."""

    modified_on: Optional[str] = None
    """The date this entity was last modified."""

    created_by: Optional[str] = None
    """The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this entity."""

    files: Optional[List["File"]] = field(default_factory=list)
    """Files that exist within this folder."""

    folders: Optional[List["Folder"]] = field(default_factory=list)
    """Folders that exist within this folder."""

    annotations: Optional[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ]
    ] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list."""

    # def __post_init__(self):
    #     # TODO - What is the best way to enforce this, basically we need a minimum amount
    #     # of information to be required such that we can save or load the data properly
    #     if not ((self.name is not None and self.parentId is not None) or self.id is not None):
    #         raise ValueError("Either name and parentId or id must be present")

    def fill_from_dict(
        self, synapse_folder: Synapse_Folder, set_annotations: bool = True
    ) -> "Folder":
        self.id = synapse_folder.get("id", None)
        self.name = synapse_folder.get("name", None)
        self.parent_id = synapse_folder.get("parentId", None)
        self.description = synapse_folder.get("description", None)
        self.etag = synapse_folder.get("etag", None)
        self.created_on = synapse_folder.get("createdOn", None)
        self.modified_on = synapse_folder.get("modifiedOn", None)
        self.created_by = synapse_folder.get("createdBy", None)
        self.modified_by = synapse_folder.get("modifiedBy", None)
        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_folder.get("annotations", None)
            )
        # TODO: Do I get information about the files/folders contained within this folder?
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Store: {self.name}"
    )
    async def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Storing folder and files to synapse.

        Arguments:
            parent: The parent folder or project to store the folder in.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The folder object.
        """
        # TODO - We need to add in some validation before the store to verify we have enough
        # information to store the data

        # Call synapse
        loop = asyncio.get_event_loop()
        synapse_folder = Synapse_Folder(
            name=self.name, parent=parent.id if parent else self.parent_id
        )
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).store(
                obj=synapse_folder, opentelemetry_context=current_context
            ),
        )

        self.fill_from_dict(synapse_folder=entity, set_annotations=False)

        tasks = []
        if self.files:
            tasks.extend(
                file.store(parent=self, synapse_client=synapse_client)
                for file in self.files
            )

        if self.folders:
            tasks.extend(
                folder.store(parent=self, synapse_client=synapse_client)
                for folder in self.folders
            )

        if self.annotations:
            tasks.append(
                asyncio.create_task(
                    Annotations(
                        id=self.id, etag=self.etag, annotations=self.annotations
                    ).store(synapse_client=synapse_client)
                )
            )

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # TODO: Proper exception handling
            for result in results:
                if isinstance(result, Folder):
                    print(f"Stored {result.name}")
                elif isinstance(result, File):
                    print(f"Stored {result.name} at: {result.path}")
                elif isinstance(result, Annotations):
                    self.annotations = result.annotations
                    print(f"Stored annotations id: {result.id}, etag: {result.etag}")
                else:
                    if isinstance(result, BaseException):
                        raise result
                    raise ValueError(f"Unknown type: {type(result)}", result)
        except Exception as ex:
            Synapse.get_client(synapse_client=synapse_client).logger.exception(ex)
            print("I hit an exception")

        print(f"Saved all files and folders in {self.name}")

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Get: {self.id}"
    )
    async def get(
        self,
        include_children: Optional[bool] = False,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Get the folder metadata from Synapse.

        Arguments:
            include_children: Whether or not to include the children of this folder.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The folder object.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get(
                entity=self.id,
                opentelemetry_context=current_context,
            ),
        )

        self.fill_from_dict(synapse_folder=entity, set_annotations=True)
        if include_children:
            children_objects = await loop.run_in_executor(
                None,
                lambda: Synapse.get_client(synapse_client=synapse_client).getChildren(
                    parent=self.id,
                    includeTypes=["folder", "file"],
                    opentelemetry_context=current_context,
                ),
            )

            folders = []
            files = []
            for child in children_objects:
                if (
                    "type" in child
                    and child["type"] == "org.sagebionetworks.repo.model.Folder"
                ):
                    folder = Folder().fill_from_dict(synapse_folder=child)
                    folder.parent_id = self.id
                    folders.append(folder)

                elif (
                    "type" in child
                    and child["type"] == "org.sagebionetworks.repo.model.FileEntity"
                ):
                    file = File().fill_from_dict(synapse_file=child)
                    file.parent_id = self.id
                    files.append(file)

            self.files.extend(files)
            self.folders.extend(folders)

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Delete: {self.id}"
    )
    async def delete(self, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the folder from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            None
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                obj=self.id,
                opentelemetry_context=current_context,
            ),
        )
