import asyncio
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Dict, List, Optional, Union

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
from synapseclient.entity import Project as Synapse_Project
from synapseclient.models import Annotations, File, Folder
from synapseclient.models.mixins import AccessControllable, StorableContainer
from synapseclient.models.protocols.project_protocol import ProjectSynchronousProtocol
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)
from synapseutils.copy_functions import copy


@dataclass()
@async_to_sync
class Project(ProjectSynchronousProtocol, AccessControllable, StorableContainer):
    """A Project is a top-level container for organizing data in Synapse.

    Attributes:
        id: The unique immutable ID for this project. A new ID will be generated for new
            Projects. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this project. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        description: The description of this entity. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity
            is out-of-date.
        created_on: The date this entity was created.
        modified_on: The date this entity was last modified.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        alias: The project alias for use in friendly project urls.
        files: Any files that are at the root directory of the project.
        folders: Any folders that are at the root directory of the project.
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
        parent_id: The parent ID of the project. In practice projects do not have a
            parent, but this is required for the inner workings of Synapse.

    Example: Creating a project
        This example shows how to create a project

            from synapseclient.models import Project, File
            import synapseclient

            synapseclient.login()

            my_annotations = {
                "my_single_key_string": "a",
                "my_key_string": ["b", "a", "c"],
            }
            project = Project(
                name="My unique project name",
                annotations=my_annotations,
                description="This is a project with random data.",
            )

            project = project.store()

            print(project)

    Example: Storing several files to a project
        This example shows how to store several files to a project

            file_1 = File(
                path=path_to_file_1,
                name=name_of_file_1,
            )
            file_2 = File(
                path=path_to_file_2,
                name=name_of_file_2,
            )
            project.files = [file_1, file_2]
            project = project.store()

    """

    id: Optional[str] = None
    """The unique immutable ID for this project. A new ID will be generated for new
    Projects. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this project. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses"""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it
    is used to detect when a client's current representation of an entity is out-of-date."""

    created_on: Optional[str] = field(default=None, compare=False)
    """(Read Only) The date this entity was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """(Read Only) The date this entity was last modified."""

    created_by: Optional[str] = field(default=None, compare=False)
    """(Read Only) The ID of the user that created this entity."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """(Read Only) The ID of the user that last modified this entity."""

    alias: Optional[str] = None
    """The project alias for use in friendly project urls."""

    files: List["File"] = field(default_factory=list, compare=False)
    """Any files that are at the root directory of the project."""

    folders: List["Folder"] = field(default_factory=list, compare=False)
    """Any folders that are at the root directory of the project."""

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

    parent_id: Optional[str] = None
    """The parent ID of the project. In practice projects do not have a parent, but this
    is required for the inner workings of Synapse."""

    _last_persistent_instance: Optional["Project"] = field(
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
        self,
        synapse_project: Union[Synapse_Project, Dict],
        set_annotations: bool = True,
    ) -> "Project":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_project: The response from the REST API.

        Returns:
            The Project object.
        """
        self.id = synapse_project.get("id", None)
        self.name = synapse_project.get("name", None)
        self.description = synapse_project.get("description", None)
        self.etag = synapse_project.get("etag", None)
        self.created_on = synapse_project.get("createdOn", None)
        self.modified_on = synapse_project.get("modifiedOn", None)
        self.created_by = synapse_project.get("createdBy", None)
        self.modified_by = synapse_project.get("modifiedBy", None)
        self.alias = synapse_project.get("alias", None)
        self.parent_id = synapse_project.get("parentId", None)
        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_project.get("annotations", {})
            )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Project_Store: ID: {self.id}, Name: {self.name}"
    )
    async def store_async(
        self,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """
        Store project, files, and folders to synapse. If you have any files or folders
        attached to this project they will be stored as well. You may attach files
        and folders to this project by setting the `files` and `folders` attributes.

        By default the store operation will non-destructively update the project if
        you have not already retrieved the project from Synapse. If you have already
        retrieved the project from Synapse then the store operation will be destructive
        and will overwrite the project with the current state of this object. See the
        `create_or_update` attribute for more information.

        Arguments:
            failure_strategy: Determines how to handle failures when storing attached
                Files and Folders under this Project and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The project object.

        Example: Using this method to update the description
            Store the project to Synapse using ID

                project = await Project(id="syn123", description="new").store_async()

            Store the project to Synapse using Name

                project = await Project(name="my_project", description="new").store_async()

        Raises:
            ValueError: If the project name is not set.
        """
        if not self.name and not self.id:
            raise ValueError("Project ID or Name is required")

        if (
            self.create_or_update
            and not self._last_persistent_instance
            and (
                existing_project_id := await get_id(
                    entity=self, synapse_client=synapse_client, failure_strategy=None
                )
            )
            and (
                existing_project := await Project(id=existing_project_id).get_async(
                    synapse_client=synapse_client
                )
            )
        ):
            merge_dataclass_entities(source=existing_project, destination=self)
        trace.get_current_span().set_attributes(
            {
                "synapse.name": self.name or "",
                "synapse.id": self.id or "",
            }
        )
        if self.has_changed:
            loop = asyncio.get_event_loop()
            synapse_project = Synapse_Project(
                id=self.id,
                etag=self.etag,
                name=self.name,
                description=self.description,
                alias=self.alias,
                parentId=self.parent_id,
            )
            delete_none_keys(synapse_project)
            current_context = context.get_current()
            entity = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(synapse_client=synapse_client).store(
                        obj=synapse_project,
                        set_annotations=False,
                        createOrUpdate=False,
                    ),
                    current_context,
                ),
            )
            self.fill_from_dict(synapse_project=entity, set_annotations=False)

        await store_entity_components(
            root_resource=self,
            failure_strategy=failure_strategy,
            synapse_client=synapse_client,
        )

        self._set_last_persistent_instance()
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Saved Project {self.name}, id: {self.id}"
        )

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Project_Get: ID: {self.id}, Name: {self.name}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """Get the project metadata from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The project object.

        Example: Using this method
            Retrieve the project from Synapse using ID

                project = await Project(id="syn123").get_async()

            Retrieve the project from Synapse using Name

                project = await Project(name="my_project").get_async()

        Raises:
            ValueError: If the project ID or Name is not set.
            SynapseNotFoundError: If the project is not found in Synapse.
        """
        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await get_from_entity_factory(
            entity_to_update=self,
            synapse_id_or_path=entity_id,
        )

        self._set_last_persistent_instance()
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Project_Delete: {self.id}, Name: {self.name}"
    )
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the project from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            None

        Example: Using this method
            Delete the project from Synapse using ID

                await Project(id="syn123").delete_async()

            Delete the project from Synapse using Name

                await Project(name="my_project").delete_async()

        Raises:
            ValueError: If the project ID or Name is not set.
            SynapseNotFoundError: If the project is not found in Synapse.
        """
        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                    obj=entity_id,
                ),
                current_context,
            ),
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Project_Copy: {self.id}"
    )
    async def copy_async(
        self,
        destination_id: str,
        copy_annotations: bool = True,
        copy_wiki: bool = True,
        exclude_types: Optional[List[str]] = None,
        file_update_existing: bool = False,
        file_copy_activity: Union[str, None] = "traceback",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """
        You must have already created the Project you will be copying to. It will have
        it's own Synapse ID and unique name that you will use as the destination_id.


        Copy the project to another Synapse project. This will recursively copy all
        Tables, Links, Files, and Folders within the project.

        Arguments:
            destination_id: Synapse ID of a project to copy to.
            copy_annotations: True to copy the annotations.
            copy_wiki: True to copy the wiki pages.
            exclude_types: A list of entity types ['file', 'table', 'link'] which
                determines which entity types to not copy. Defaults to an empty list.
            file_update_existing: When the destination has a file that has the same
                name, users can choose to update that file.
            file_copy_activity: Has three options to set the activity of the copied file:

                    - traceback: Creates a copy of the source files Activity.
                    - existing: Link to the source file's original Activity (if it exists)
                    - None: No activity is set
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The copied project object.

        Example: Using this function
            Assuming you have a project with the ID "syn123" and you want to copy it to a
            project with the ID "syn456":

                new_instance = await Project(id="syn123").copy_async(destination_id="syn456")

            Copy the project but do not persist annotations:

                new_instance = await Project(id="syn123").copy_async(destination_id="syn456", copy_annotations=False)

        Raises:
            ValueError: If the project does not have an ID and destination_id to copy.
        """
        if not self.id or not destination_id:
            raise ValueError("The project must have an ID and destination_id to copy.")

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
                    excludeTypes=exclude_types or [],
                    skipCopyAnnotations=not copy_annotations,
                    skipCopyWikiPage=not copy_wiki,
                    updateExisting=file_update_existing,
                    setProvenance=file_copy_activity,
                ),
                current_context,
            ),
        )

        new_project_id = source_and_destination.get(self.id, None)
        if not new_project_id:
            raise SynapseError("Failed to copy project.")
        project_copy = await (
            await Project(id=new_project_id).get_async()
        ).sync_from_synapse_async(
            download_file=False,
            synapse_client=synapse_client,
        )
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Copied from project {self.id} to {destination_id}"
        )
        return project_copy
