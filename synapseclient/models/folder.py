import asyncio
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from opentelemetry import context, trace

from synapseclient import Synapse
from synapseclient.api import get_from_entity_factory
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.utils import (
    delete_none_keys,
    merge_dataclass_entities,
    run_and_attach_otel_context,
)
from synapseclient.entity import Folder as Synapse_Folder
from synapseclient.models import Annotations, File
from synapseclient.models.mixins import AccessControllable, StorableContainer
from synapseclient.models.protocols.folder_protocol import FolderSynchronousProtocol
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)
from synapseutils import copy

if TYPE_CHECKING:
    from synapseclient.models import Project


@dataclass()
@async_to_sync
class Folder(FolderSynchronousProtocol, AccessControllable, StorableContainer):
    """Folder is a hierarchical container for organizing data in Synapse.

    Attributes:
        id: The unique immutable ID for this folder. A new ID will be generated for new
            Folders. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this folder. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses.
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
            associated with all values in the list. To remove all annotations set this
            to an empty dict `{}` or None and store the entity.
        create_or_update: (Store only) Indicates whether the method should
            automatically perform an update if the resource conflicts with an existing
            Synapse object. When True this means that any changes to the resource will
            be non-destructive.

            This boolean is ignored if you've already stored or retrieved the resource
            from Synapse for this instance at least once. Any changes to the resource
            will be destructive in this case. For example if you want to delete the
            content for a field you will need to call `.get()` and then modify the
            field.
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

    files: List["File"] = field(default_factory=list, compare=False)
    """Files that exist within this folder."""

    folders: List["Folder"] = field(default_factory=list, compare=False)
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
    ] = field(default_factory=dict, compare=False)
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`
    or None and store the entity."""

    is_restricted: bool = field(default=False, repr=False)
    """
    (Store only)

    If set to true, an email will be sent to the Synapse access control team to start
    the process of adding terms-of-use or review board approval for this entity.
    You will be contacted with regards to the specific data being restricted and the
    requirements of access.
    """

    create_or_update: bool = field(default=True, repr=False)
    """
    (Store only)

    Indicates whether the method should automatically perform an update if the resource
    conflicts with an existing Synapse object. When True this means that any changes
    to the resource will be non-destructive.

    This boolean is ignored if you've already stored or retrieved the resource from
    Synapse for this instance at least once. Any changes to the resource will be
    destructive in this case. For example if you want to delete the content for a field
    you will need to call `.get()` and then modify the field.
    """

    _last_persistent_instance: Optional["Folder"] = field(
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
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else {}
        )

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
    async def store_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Store folders and files to synapse. If you have any files or folders attached
        to this folder they will be stored as well. You may attach files and folders
        to this folder by setting the `files` and `folders` attributes.

        By default the store operation will non-destructively update the folder if
        you have not already retrieved the folder from Synapse. If you have already
        retrieved the folder from Synapse then the store operation will be destructive
        and will overwrite the folder with the current state of this object. See the
        `create_or_update` attribute for more information.

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
        self.parent_id = parent_id

        if (
            self.create_or_update
            and not self._last_persistent_instance
            and (
                existing_folder_id := await get_id(
                    entity=self, failure_strategy=None, synapse_client=synapse_client
                )
            )
            and (existing_folder := await Folder(id=existing_folder_id).get_async())
        ):
            merge_dataclass_entities(source=existing_folder, destination=self)
        trace.get_current_span().set_attributes(
            {
                "synapse.name": self.name or "",
                "synapse.id": self.id or "",
            }
        )
        if self.has_changed:
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
                        isRestricted=self.is_restricted,
                        createOrUpdate=False,
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
    async def get_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Get the folder metadata from Synapse. You are able to find a folder by
        either the id or the name and parent_id.

        Arguments:
            parent: The parent folder or project this folder exists under.
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
        self.parent_id = parent_id

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await get_from_entity_factory(
            entity_to_update=self,
            synapse_id_or_path=entity_id,
        )

        self._set_last_persistent_instance()
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Delete: {self.id}"
    )
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the folder from Synapse by its id.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

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
                current_context=current_context,
            ),
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Folder_Copy: {self.id}"
    )
    async def copy_async(
        self,
        parent_id: str,
        copy_annotations: bool = True,
        exclude_types: Optional[List[str]] = None,
        file_update_existing: bool = False,
        file_copy_activity: Union[str, None] = "traceback",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """
        Copy the folder to another Synapse location. This will recursively copy all
        Tables, Links, Files, and Folders within the folder.

        Arguments:
            parent_id: Synapse ID of a folder/project that the copied entity is
                being copied to
            copy_annotations: True to copy the annotations.
            exclude_types: A list of entity types ['file', 'table', 'link'] which
                determines which entity types to not copy. Defaults to an empty list.
            file_update_existing: When the destination has a file that has the same name,
                users can choose to update that file.
            file_copy_activity: Has three options to set the activity of the copied file:

                    - traceback: Creates a copy of the source files Activity.
                    - existing: Link to the source file's original Activity (if it exists)
                    - None: No activity is set
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The copied folder object.

        Example: Using this function
            Assuming you have a folder with the ID "syn123" and you want to copy it to a
            project with the ID "syn456":

                new_folder_instance = await Folder(id="syn123").copy_async(parent_id="syn456")

            Copy the folder but do not persist annotations:

                new_folder_instance = await Folder(id="syn123").copy_async(parent_id="syn456", copy_annotations=False)

        Raises:
            ValueError: If the folder does not have an ID and parent_id to copy.
        """
        if not self.id or not parent_id:
            raise ValueError("The folder must have an ID and parent_id to copy.")

        loop = asyncio.get_event_loop()

        current_context = context.get_current()
        syn = Synapse.get_client(synapse_client=synapse_client)
        source_and_destination = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: copy(
                    syn=syn,
                    entity=self.id,
                    destinationId=parent_id,
                    excludeTypes=exclude_types or [],
                    skipCopyAnnotations=not copy_annotations,
                    updateExisting=file_update_existing,
                    setProvenance=file_copy_activity,
                ),
                current_context,
            ),
        )

        new_folder_id = source_and_destination.get(self.id, None)
        if not new_folder_id:
            raise SynapseError("Failed to copy folder.")
        folder_copy = await (
            await Folder(id=new_folder_id).get_async()
        ).sync_from_synapse_async(
            download_file=False,
            synapse_client=synapse_client,
        )
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Copied from folder {self.id} to {parent_id} with new id of {folder_copy.id}"
        )
        return folder_copy
