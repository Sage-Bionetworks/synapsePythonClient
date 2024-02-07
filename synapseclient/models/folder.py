import asyncio
from dataclasses import dataclass, field
import dataclasses
from datetime import date, datetime
import os
from typing import Dict, List, Union
from typing import Optional, TYPE_CHECKING
from opentelemetry import trace, context

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.entity import Folder as Synapse_Folder
from synapseclient.models import File, Annotations
from synapseclient.core.async_utils import (
    otel_trace_method,
)
from synapseclient.core.utils import run_and_attach_otel_context, delete_none_keys
from synapseclient.core.exceptions import SynapseError
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
    FailureStrategy,
    wrap_coroutine,
)
from synapseutils import copy

if TYPE_CHECKING:
    from synapseclient.models import Project

tracer = trace.get_tracer("synapseclient")


@dataclass()
class Folder(AccessControllable):
    """Folder is a hierarchical container for organizing data in Synapse.

    Attributes:
        id: The unique immutable ID for this folder. A new ID will be generated for new
            Folders. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this folder. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
            apostrophes, and parentheses.
        parent_id: The ID of the Project or Folder that is the parent of this Folder.
        description: The description of this entity. Must be 1000 characters or less.
        etag: (Read Only)
            Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: (Read Only) The date this entity was created.
        modified_on: (Read Only) The date this entity was last modified.
        created_by: (Read Only) The ID of the user that created this entity.
        modified_by: (Read Only) The ID of the user that last modified this entity.
        files: Files that exist within this folder.
        folders: Folders that exist within this folder.
        annotations: Additional metadata associated with the folder. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list.  To remove all annotations set this
            to an empty dict `{}`.
    """

    id: Optional[str] = None
    """The unique immutable ID for this folder. A new ID will be generated for new
    Folders. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this folder. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses"""

    parent_id: Optional[str] = None
    """The ID of the Project or Folder that is the parent of this Folder."""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """(Read Only)
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it
    is used to detect when a client's current representation of an entity
    is out-of-date."""

    created_on: Optional[str] = None
    """(Read Only) The date this entity was created."""

    modified_on: Optional[str] = None
    """(Read Only) The date this entity was last modified."""

    created_by: Optional[str] = None
    """(Read Only) The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """(Read Only) The ID of the user that last modified this entity."""

    files: Optional[List["File"]] = field(default_factory=list, compare=False)
    """Files that exist within this folder."""

    folders: Optional[List["Folder"]] = field(default_factory=list, compare=False)
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
    all values in the list. To remove all annotations set this to an empty dict `{}`."""

    _last_persistent_instance: Optional["Folder"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = dataclasses.replace(self)

    def fill_from_dict(
        self, synapse_folder: Synapse_Folder, set_annotations: bool = True
    ) -> "Folder":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_file: The response from the REST API.
            set_annotations: Whether to set the annotations from the response.

        Returns:
            The Folder object.
        """
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
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Store: {self.name}"
    )
    async def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Storing folder and files to synapse.

        Arguments:
            parent: The parent folder or project to store the folder in.
            failure_strategy: Determines how to handle failures when storing attached
                Files and Folders under this Folder and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The folder object.

        Raises:
            ValueError: If the folder does not have an id or a
                (name and (`parent_id` or parent with an id)) set.
        """
        parent_id = parent.id if parent else self.parent_id
        if not (self.id or (self.name and parent_id)):
            raise ValueError(
                "The folder must have an id or a "
                "(name and (`parent_id` or parent with an id)) set."
            )

        if not self._last_persistent_instance or self._last_persistent_instance != self:
            loop = asyncio.get_event_loop()
            synapse_folder = Synapse_Folder(
                id=self.id,
                name=self.name,
                parent=parent_id,
                etag=self.etag,
                description=self.description,
            )
            delete_none_keys(synapse_folder)
            current_context = context.get_current()
            entity = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(synapse_client=synapse_client).store(
                        obj=synapse_folder,
                        set_annotations=False,
                    ),
                    current_context,
                ),
            )

            self.fill_from_dict(synapse_folder=entity, set_annotations=False)

        await store_entity_components(
            root_resource=self,
            failure_strategy=failure_strategy,
            synapse_client=synapse_client,
        )
        self._set_last_persistent_instance()
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Saved Folder {self.name}, id: {self.id}: parent: {self.parent_id}"
        )

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Get: {self.id}"
    )
    async def get(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Get the folder metadata from Synapse.

        Arguments:
            parent: The parent folder or project this folder exists under.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The folder object.

        Raises:
            ValueError: If the folder does not have an id or a
                (name and (`parent_id` or parent with an id)) set.
        """
        parent_id = parent.id if parent else self.parent_id
        if not (self.id or (self.name and parent_id)):
            raise ValueError(
                "The folder must have an id or a "
                "(name and (`parent_id` or parent with an id)) set."
            )

        loop = asyncio.get_event_loop()
        current_context = context.get_current()

        entity_id = self.id or await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).findEntityId(
                    name=self.name,
                    parent=parent_id,
                ),
                current_context,
            ),
        )

        if entity_id is None:
            raise SynapseNotFoundError(
                f"Folder [Id: {self.id}, Name: {self.name}, Parent: {parent_id}] not found in Synapse."
            )

        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).get(
                    entity=entity_id,
                ),
                current_context,
            ),
        )

        self.fill_from_dict(synapse_folder=entity, set_annotations=True)

        self._set_last_persistent_instance()
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Children: {self.id}"
    )
    async def sync_from_synapse(
        self,
        recursive: bool = False,
        download_file=False,
        path: Optional[str] = None,
        if_collision: str = "overwrite.local",
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        synapse_client: Optional[Synapse] = None,
    ):
        """
        Sync this folder and all possible sub-folders from Synapse. By default this
        will not download the files that are found, however, it will populate the
        `files` and `folders` attributes with the found files and folders.

        This works similar to [synapseutils.syncFromSynapse][], however, this does not
        currently support the writing of data to a manifest TSV file. This will be a
        future enhancement.

        Only Files and Folders are supported at this time to be synced from synapse.

        Arguments:
            recursive: Whether or not to recursively get the entire hierarchy of the
                folder and sub-folders.
            download_file: Whether to download the files found or not.
            path: An optional path where the file hierarchy will be reproduced. If not
                specified the files will by default be placed in the synapseCache.
            if_collision: Determines how to handle file collisions. May be

                - `overwrite.local`
                - `keep.local`
                - `keep.both`
            failure_strategy: Determines how to handle failures when retrieving children
                under this Folder and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Raises:
            ValueError: If the folder does not have an id set.
        """
        if not self.id:
            raise ValueError("The folder must have an id set.")

        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        children_objects = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).getChildren(
                    parent=self.id,
                    includeTypes=["folder", "file"],
                ),
                current_context,
            ),
        )

        pending_tasks = []
        for child in children_objects:
            pending_tasks.extend(
                self._create_task_for_child(
                    child=child,
                    recursive=recursive,
                    path=path,
                    download_file=download_file,
                    if_collision=if_collision,
                    failure_strategy=failure_strategy,
                    synapse_client=synapse_client,
                )
            )
        self.folders = []
        self.files = []

        for task in asyncio.as_completed(pending_tasks):
            result = await task
            self._resolve_sync_from_synapse_result(
                result=result,
                failure_strategy=failure_strategy,
                synapse_client=synapse_client,
            )
        return self

    def _create_task_for_child(
        self,
        child,
        recursive: bool = False,
        path: Optional[str] = None,
        download_file=False,
        if_collision: str = "overwrite.local",
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        synapse_client: Optional[Synapse] = None,
    ) -> List[asyncio.Task]:
        """
        Determines based off the type of child which tasks should be created to handle
        the child. This will return a list of tasks that will be executed in parallel
        to handle the child. The tasks will retrieve the File and Folder objects from
        Synapse. In the case of a Folder object, it will also retrieve the children of
        that folder if `recursive` is set to True.


        Arguments:
            recursive: Whether or not to recursively get the entire hierarchy of the
                folder and sub-folders.
            download_file: Whether to download the files found or not.
            path: An optional path where the file hierarchy will be reproduced. If not
                specified the files will by default be placed in the synapseCache.
            if_collision: Determines how to handle file collisions. May be

                - `overwrite.local`
                - `keep.local`
                - `keep.both`
            failure_strategy: Determines how to handle failures when retrieving children
                under this Folder and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        """

        async def wrap_recursive_get_children(
            folder: "Folder",
            recursive: bool = False,
            path: Optional[str] = None,
            download_file=False,
            if_collision: str = "overwrite.local",
            failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
            synapse_client: Optional[Synapse] = None,
        ) -> None:
            """
            Wrap the recursive get children method to return nothing. We are updating
            the folder object in place. We do not want to cause the result of this
            method to cause any folder of file objects to be added to this level of the
            hierarchy.
            """
            new_resolved_path = (
                os.path.join(path, folder.name) if path and folder.name else None
            )
            if new_resolved_path and not os.path.exists(new_resolved_path):
                os.makedirs(new_resolved_path)
            await folder.sync_from_synapse(
                recursive=recursive,
                download_file=download_file,
                path=new_resolved_path,
                if_collision=if_collision,
                failure_strategy=failure_strategy,
                synapse_client=synapse_client,
            )
            return

        pending_tasks = []
        synapse_id = child.get("id", None)
        child_type = child.get("type", None)
        name = child.get("name", None)
        if synapse_id and child_type == "org.sagebionetworks.repo.model.Folder":
            folder = Folder(id=synapse_id, name=name)
            pending_tasks.append(asyncio.create_task(wrap_coroutine(folder.get())))

            if recursive:
                pending_tasks.append(
                    asyncio.create_task(
                        wrap_recursive_get_children(
                            folder=folder,
                            recursive=recursive,
                            path=path,
                            download_file=download_file,
                            if_collision=if_collision,
                            failure_strategy=failure_strategy,
                            synapse_client=synapse_client,
                        )
                    )
                )

        elif synapse_id and child_type == "org.sagebionetworks.repo.model.FileEntity":
            file = File(id=synapse_id, download_file=download_file)
            if path:
                file.download_location = path
            if if_collision:
                file.if_collision = if_collision

            pending_tasks.append(asyncio.create_task(wrap_coroutine(file.get())))
        return pending_tasks

    def _resolve_sync_from_synapse_result(
        self, result, failure_strategy: FailureStrategy, synapse_client: Synapse
    ) -> None:
        """TODO: Fill me out"""
        if result.__class__.__name__ == "Folder":
            self.folders.append(result)
        elif result.__class__.__name__ == "File":
            self.files.append(result)
        elif result is None:
            pass
        elif isinstance(result, BaseException):
            Synapse.get_client(synapse_client=synapse_client).logger.exception(result)

            if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                raise result
        else:
            exception = SynapseError(
                f"Unknown failure retrieving children of Folder ({self.id}): {type(result)}",
                result,
            )
            Synapse.get_client(synapse_client=synapse_client).logger.exception(
                exception
            )
            if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                raise exception

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Delete: {self.id}"
    )
    async def delete(self, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the folder from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            None

        Raises:
            ValueError: If the folder does not have an id set.
        """
        if not self.id:
            raise ValueError("The folder must have an id set.")
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                    obj=self.id,
                ),
                current_context,
            ),
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Copy: {self.id}"
    )
    async def copy(
        self,
        destination_id: str,
        copy_annotations: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """
        Copy the folder to another Synpase location. Defaults to the latest version of the
        file, or the version_number specified in the instance.

        Arguments:
            destination_id: Synapse ID of a folder/project that the copied entity is
                being copied to
            copy_annotations: True to copy the annotations.
            exclude_types: Accepts a list of entity types ["file", "table", "link"]
                        which determines which entity types to not copy.
                        Defaults to an empty list.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The copied folder object.

        Example: Using this function
            Assuming you have a folder with the ID "syn123" and you want to copy it to a project with the ID "syn456":

                new_folder_instance = await Folder(id="syn123").copy(destination_id="syn456")

            Copy the folder but do not persist annotations or activity:

                new_folder_instance = await Folder(id="syn123").copy(destination_id="syn456", copy_annotations=False)

        Raises:
            ValueError: If the folder does not have an ID and destination_id to copy.
        """
        if not self.id or not destination_id:
            raise ValueError("The folder must have an ID and destination_id to copy.")

        loop = asyncio.get_event_loop()

        current_context = context.get_current()
        syn = Synapse.get_client(synapse_client=synapse_client)
        source_and_destination = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: copy(
                    syn=syn,
                    entity=self.id,
                    destinationId=destination_id,
                    skipCopyAnnotations=not copy_annotations,
                ),
                current_context,
            ),
        )

        destination_id = source_and_destination.get(self.id, None)
        if not destination_id:
            raise SynapseError("Failed to copy folder.")
        folder_copy = await Folder(id=destination_id).get(synapse_client=synapse_client)
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Copied from folder {self.id} to {destination_id}"
        )
        return folder_copy
