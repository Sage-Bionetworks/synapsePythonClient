import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Dict, Union

from synapseclient.entity import Project as Synapse_Project
from opentelemetry import trace, context

from typing import Optional

from synapseclient.models import Folder, File, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient import Synapse
from synapseclient.core.async_utils import otel_trace_method
from synapseclient.core.utils import run_and_attach_otel_context


tracer = trace.get_tracer("synapseclient")


@dataclass()
class Project(AccessControllable):
    """A Project is a top-level container for organizing data in Synapse.

    Attributes:
        id: The unique immutable ID for this project. A new ID will be generated for new
            Projects. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this project. Must be 256 characters or less. Names may only contain:
                letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
                and parentheses
        description: The description of this entity. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
                concurrent updates. Since the E-Tag changes every time an entity is updated it
                is used to detect when a client's current representation of an entity is out-of-date.
        created_on: The date this entity was created.
        modified_on: The date this entity was last modified.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        alias: The project alias for use in friendly project urls.
        files: Any files that are at the root directory of the project.
        folders: Any folders that are at the root directory of the project.
        annotations: Additional metadata associated with the folder. The key is the name of your
                        desired annotations. The value is an object containing a list of values
                        (use empty list to represent no values for key) and the value type associated with
                        all values in the list.

    Functions:
        store: Store project, files, and folders to synapse.
        get: Get the project metadata from Synapse.
        delete: Delete the project from Synapse.


    Example: Creating a project
        This example shows how to create a project

            project = Project(
                name="bfauble_my_new_project_for_testing",
                annotations=my_annotations,
                description="This is a project with random data.",
            )

            project = await project.store()

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
            project = await project.store()

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

    created_on: Optional[str] = None
    """The date this entity was created."""

    modified_on: Optional[str] = None
    """The date this entity was last modified."""

    created_by: Optional[str] = None
    """The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this entity."""

    alias: Optional[str] = None
    """The project alias for use in friendly project urls."""

    files: Optional[List["File"]] = field(default_factory=list)
    """Any files that are at the root directory of the project."""

    folders: Optional[List["Folder"]] = field(default_factory=list)
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
    ] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list."""

    def fill_from_dict(
        self, synapse_project: Synapse_Project, set_annotations: bool = True
    ) -> "Project":
        self.id = synapse_project.get("id", None)
        self.name = synapse_project.get("name", None)
        self.description = synapse_project.get("description", None)
        self.etag = synapse_project.get("etag", None)
        self.created_on = synapse_project.get("createdOn", None)
        self.modified_on = synapse_project.get("modifiedOn", None)
        self.created_by = synapse_project.get("createdBy", None)
        self.modified_by = synapse_project.get("modifiedBy", None)
        self.alias = synapse_project.get("alias", None)
        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_project.get("annotations", None)
            )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Project_Store: {self.name}"
    )
    async def store(self, synapse_client: Optional[Synapse] = None) -> "Project":
        """Store project, files, and folders to synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The project object.
        """
        # Call synapse
        loop = asyncio.get_event_loop()
        synapse_project = Synapse_Project(self.name)
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).store(
                    obj=synapse_project
                ),
                current_context,
            ),
        )
        self.fill_from_dict(synapse_project=entity, set_annotations=False)

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
        method_to_trace_name=lambda self, **kwargs: f"Project_Get: ID: {self.id}, Name: {self.name}"
    )
    async def get(
        self,
        include_children: Optional[bool] = False,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """Get the project metadata from Synapse.

        Arguments:
            include_children: If True, will also get the children of the project.
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            The project object.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).get(
                    entity=self.id,
                ),
                current_context,
            ),
        )

        self.fill_from_dict(synapse_project=entity, set_annotations=True)
        if include_children:
            children_objects = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).getChildren(
                        parent=self.id,
                        includeTypes=["folder", "file"],
                    ),
                    current_context,
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
        method_to_trace_name=lambda self, **kwargs: f"Project_Delete: {self.id}"
    )
    async def delete(self, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the project from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            None
        """
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
