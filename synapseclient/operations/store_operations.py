"""Factory method for storing resources to Synapse."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Union

from synapseclient.core.async_utils import wrap_async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import (
        AgentSession,
        CurationTask,
        Dataset,
        DatasetCollection,
        EntityView,
        Evaluation,
        File,
        Folder,
        FormData,
        FormGroup,
        Grid,
        JSONSchema,
        Link,
        MaterializedView,
        Project,
        RecordSet,
        SchemaOrganization,
        SubmissionView,
        Table,
        Team,
        VirtualTable,
    )


@dataclass
class StoreFileOptions:
    """Options for storing File entities.

    Attributes:
        synapse_store: Whether to store the file in Synapse or use an external URL.
        content_type: The content type of the file.
        merge_existing_annotations: If True, merge existing annotations with new ones.
            If False, replace all annotations.
        associate_activity_to_new_version: If True, associate activity with new version.
            If False, do not associate activity.
    """

    synapse_store: Optional[bool] = None
    content_type: Optional[str] = None
    merge_existing_annotations: Optional[bool] = None
    associate_activity_to_new_version: Optional[bool] = None


@dataclass
class StoreContainerOptions:
    """Options for storing container entities (Project, Folder).

    Attributes:
        failure_strategy: Strategy for handling failures when storing child entities.
            Valid values: "LOG_EXCEPTION", "RAISE_EXCEPTION".
    """

    failure_strategy: Optional[str] = None


@dataclass
class StoreTableOptions:
    """Options for storing Table-like entities.

    Attributes:
        dry_run: If True, will not actually store but will log what would be done.
        job_timeout: Maximum time to wait for table schema update job to complete.
    """

    dry_run: bool = False
    job_timeout: int = 600


@dataclass
class StoreJSONSchemaOptions:
    """Options for storing JSONSchema entities.

    Attributes:
        schema_body: The body of the JSONSchema to store.
        version: The version of the JSONSchema body to store.
        dry_run: Whether or not to do a dry-run.
    """

    schema_body: dict
    version: Optional[str] = None
    dry_run: bool = False


@dataclass
class StoreGridOptions:
    """Options for storing Grid entities.

    Attributes:
        attach_to_previous_session: If True and using record_set_id, will attach
            to an existing active session if one exists.
        timeout: The number of seconds to wait for the job to complete.
    """

    attach_to_previous_session: bool = False
    timeout: int = 120


async def _handle_store_file_entity(
    entity: "File",
    parent: Optional[Union["Folder", "Project"]] = None,
    file_options: Optional[StoreFileOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> "File":
    """
    Handle storing a File entity with file-specific options.
    """
    if file_options:
        if file_options.synapse_store is not None:
            entity.synapse_store = file_options.synapse_store
        if file_options.content_type is not None:
            entity.content_type = file_options.content_type
        if file_options.merge_existing_annotations is not None:
            entity.merge_existing_annotations = file_options.merge_existing_annotations
        if file_options.associate_activity_to_new_version is not None:
            entity.associate_activity_to_new_version = (
                file_options.associate_activity_to_new_version
            )

    return await entity.store_async(parent=parent, synapse_client=synapse_client)


async def _handle_store_container_entity(
    entity: Union["Project", "Folder"],
    parent: Optional[Union["Folder", "Project"]] = None,
    container_options: Optional[StoreContainerOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union["Project", "Folder"]:
    """
    Handle storing a container entity (Project or Folder) with container-specific options.
    """
    from synapseclient.models import Folder
    from synapseclient.models.services.storable_entity_components import FailureStrategy

    failure_strategy = FailureStrategy.LOG_EXCEPTION
    if container_options and container_options.failure_strategy:
        if container_options.failure_strategy == "RAISE_EXCEPTION":
            failure_strategy = FailureStrategy.RAISE_EXCEPTION

    if isinstance(entity, Folder):
        return await entity.store_async(
            parent=parent,
            failure_strategy=failure_strategy,
            synapse_client=synapse_client,
        )
    else:  # Project
        return await entity.store_async(
            failure_strategy=failure_strategy, synapse_client=synapse_client
        )


async def _handle_store_table_entity(
    entity: Union[
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "MaterializedView",
        "SubmissionView",
        "Table",
        "VirtualTable",
    ],
    table_options: Optional[StoreTableOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "MaterializedView",
    "SubmissionView",
    "Table",
    "VirtualTable",
]:
    """
    Handle storing table-like entities with table-specific options.
    """
    dry_run = table_options.dry_run if table_options else False
    job_timeout = table_options.job_timeout if table_options else 600

    return await entity.store_async(
        dry_run=dry_run, job_timeout=job_timeout, synapse_client=synapse_client
    )


async def _handle_store_link_entity(
    entity: "Link",
    parent: Optional[Union["Folder", "Project"]] = None,
    synapse_client: Optional["Synapse"] = None,
) -> "Link":
    """
    Handle storing a Link entity.
    """
    return await entity.store_async(parent=parent, synapse_client=synapse_client)


async def _handle_store_json_schema_entity(
    entity: "JSONSchema",
    schema_body: dict,
    version: Optional[str] = None,
    dry_run: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> "JSONSchema":
    """
    Handle storing a JSONSchema entity.
    """
    return await entity.store_async(
        schema_body=schema_body,
        version=version,
        dry_run=dry_run,
        synapse_client=synapse_client,
    )


def store(
    entity: Union[
        "AgentSession",
        "CurationTask",
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "Evaluation",
        "File",
        "Folder",
        "FormData",
        "FormGroup",
        "Grid",
        "JSONSchema",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SchemaOrganization",
        "SubmissionView",
        "Table",
        "Team",
        "VirtualTable",
    ],
    parent: Optional[Union["Folder", "Project"]] = None,
    *,
    file_options: Optional[StoreFileOptions] = None,
    container_options: Optional[StoreContainerOptions] = None,
    table_options: Optional[StoreTableOptions] = None,
    json_schema_options: Optional[StoreJSONSchemaOptions] = None,
    grid_options: Optional[StoreGridOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "AgentSession",
    "CurationTask",
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "Evaluation",
    "File",
    "Folder",
    "FormData",
    "FormGroup",
    "Grid",
    "JSONSchema",
    "Link",
    "MaterializedView",
    "Project",
    "RecordSet",
    "SchemaOrganization",
    "SubmissionView",
    "Table",
    "Team",
    "VirtualTable",
]:
    """
    Factory method to store any Synapse entity.

    This method serves as a unified interface for storing any type of Synapse entity.
    It automatically applies the appropriate store logic based on the entity type and
    the options provided.

    Arguments:
        entity: The entity instance to store. Must be one of the supported entity types.
        parent: The parent folder or project to store the entity in. Only applicable
            for File, Folder, Link, and RecordSet entities. Ignored for other entity types.
        file_options: File-specific configuration options. Only applies to File and
            RecordSet entities. Ignored for other entity types.
        container_options: Container-specific configuration options. Only applies to
            Project and Folder entities. Ignored for other entity types.
        table_options: Table-specific configuration options. Only applies to Table-like
            entities (Table, Dataset, EntityView, MaterializedView, SubmissionView,
            VirtualTable, DatasetCollection). Ignored for other entity types.
        json_schema_options: JSONSchema-specific configuration options. Only applies to
            JSONSchema entities. Required for JSONSchema. Ignored for other entity types.
        grid_options: Grid-specific configuration options. Only applies to Grid entities.
            Ignored for other entity types.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The stored Synapse entity model instance.

    Raises:
        ValueError: If the entity is not a supported type.

    Example: Storing a file
        Store a file to Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        file = File(path="/path/to/file.txt", parent_id="syn123456")
        stored_file = store(file)
        print(f"Stored file: {stored_file.name} (ID: {stored_file.id})")
        ```

    Example: Storing a file with custom options
        Store a file with custom options:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import store, StoreFileOptions

        syn = Synapse()
        syn.login()

        file = File(path="/path/to/file.txt", parent_id="syn123456")
        stored_file = store(
            file,
            file_options=StoreFileOptions(
                synapse_store=True,
                content_type="text/plain",
                merge_existing_annotations=True
            )
        )
        print(f"Stored file: {stored_file.name}")
        ```

    Example: Storing a folder
        Store a folder to Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Folder
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        folder = Folder(name="My Folder", parent_id="syn123456")
        stored_folder = store(folder)
        print(f"Stored folder: {stored_folder.name} (ID: {stored_folder.id})")
        ```

    Example: Storing a project
        Store a project to Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Project
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        project = Project(name="My Project")
        stored_project = store(project)
        print(f"Stored project: {stored_project.name} (ID: {stored_project.id})")
        ```

    Example: Storing a table
        Store a table to Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Table, Column, ColumnType
        from synapseclient.operations import store, StoreTableOptions

        syn = Synapse()
        syn.login()

        table = Table(
            name="My Table",
            parent_id="syn123456",
            columns=[Column(name="col1", column_type=ColumnType.STRING, maximum_size=50)]
        )
        stored_table = store(
            table,
            table_options=StoreTableOptions(dry_run=False, job_timeout=600)
        )
        print(f"Stored table: {stored_table.name} (ID: {stored_table.id})")
        ```

    Example: Storing a link
        Store a link to Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Link
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        link = Link(name="My Link", parent_id="syn123456", target_id="syn789")
        stored_link = store(link)
        print(f"Stored link: {stored_link.name} (ID: {stored_link.id})")
        ```

    Example: Storing with parent parameter
        Store an entity by passing the parent as a parameter:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File, Folder
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        parent_folder = Folder(id="syn123456")
        file = File(path="/path/to/file.txt")
        stored_file = store(file, parent=parent_folder)
        print(f"Stored file: {stored_file.name} in folder {parent_folder.id}")
        ```

    Example: Storing an Evaluation
        Store an evaluation (challenge queue):

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Evaluation
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        evaluation = Evaluation(
            name="My Challenge Evaluation",
            description="Evaluation for my data challenge",
            content_source="syn123456"
        )
        stored_evaluation = store(evaluation)
        print(f"Stored evaluation: {stored_evaluation.name} (ID: {stored_evaluation.id})")
        ```

    Example: Creating a Team
        Create a new team (note: Teams use create, not update):

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Team
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        team = Team(
            name="My Research Team",
            description="A team for collaborative research"
        )
        created_team = store(team)
        print(f"Created team: {created_team.name} (ID: {created_team.id})")
        ```

    Example: Storing a CurationTask
        Store a curation task:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import CurationTask, FileBasedMetadataTaskProperties
        from synapseclient.operations import store

        syn = Synapse()
        syn.login()

        file_properties = FileBasedMetadataTaskProperties(
            upload_folder_id="syn1234567",
            file_view_id="syn2345678"
        )
        task = CurationTask(
            project_id="syn9876543",
            data_type="genomics_data",
            instructions="Upload your genomics files",
            task_properties=file_properties
        )
        stored_task = store(task)
        print(f"Stored curation task: {stored_task.task_id}")
        ```
    """
    return wrap_async_to_sync(
        coroutine=store_async(
            entity=entity,
            parent=parent,
            file_options=file_options,
            container_options=container_options,
            table_options=table_options,
            json_schema_options=json_schema_options,
            grid_options=grid_options,
            synapse_client=synapse_client,
        )
    )


async def store_async(
    entity: Union[
        "AgentSession",
        "CurationTask",
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "Evaluation",
        "File",
        "Folder",
        "FormData",
        "FormGroup",
        "Grid",
        "JSONSchema",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SchemaOrganization",
        "SubmissionView",
        "Table",
        "Team",
        "VirtualTable",
    ],
    parent: Optional[Union["Folder", "Project"]] = None,
    *,
    file_options: Optional[StoreFileOptions] = None,
    container_options: Optional[StoreContainerOptions] = None,
    table_options: Optional[StoreTableOptions] = None,
    json_schema_options: Optional[StoreJSONSchemaOptions] = None,
    grid_options: Optional[StoreGridOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "AgentSession",
    "CurationTask",
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "Evaluation",
    "File",
    "Folder",
    "FormData",
    "FormGroup",
    "Grid",
    "JSONSchema",
    "Link",
    "MaterializedView",
    "Project",
    "RecordSet",
    "SchemaOrganization",
    "SubmissionView",
    "Table",
    "Team",
    "VirtualTable",
]:
    """
    Factory method to store any Synapse entity asynchronously.

    This method serves as a unified interface for storing any type of Synapse entity
    asynchronously. It automatically applies the appropriate store logic based on the
    entity type and the options provided.

    Arguments:
        entity: The entity instance to store. Must be one of the supported entity types.
        parent: The parent folder or project to store the entity in. Only applicable
            for File, Folder, Link, and RecordSet entities. Ignored for other entity types.
        file_options: File-specific configuration options. Only applies to File and
            RecordSet entities. Ignored for other entity types.
        container_options: Container-specific configuration options. Only applies to
            Project and Folder entities. Ignored for other entity types.
        table_options: Table-specific configuration options. Only applies to Table-like
            entities (Table, Dataset, EntityView, MaterializedView, SubmissionView,
            VirtualTable, DatasetCollection). Ignored for other entity types.
        json_schema_options: JSONSchema-specific configuration options. Only applies to
            JSONSchema entities. Required for JSONSchema. Ignored for other entity types.
        grid_options: Grid-specific configuration options. Only applies to Grid entities.
            Ignored for other entity types.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The stored Synapse entity model instance.

    Raises:
        ValueError: If the entity is not a supported type.

    Example: Storing a file
        Store a file to Synapse:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import store_async

        async def main():
            syn = Synapse()
            syn.login()

            file = File(path="/path/to/file.txt", parent_id="syn123456")
            stored_file = await store_async(file)
            print(f"Stored file: {stored_file.name} (ID: {stored_file.id})")

        asyncio.run(main())
        ```

    Example: Storing a file with custom options
        Store a file with custom options:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import store_async, StoreFileOptions

        async def main():
            syn = Synapse()
            syn.login()

            file = File(path="/path/to/file.txt", parent_id="syn123456")
            stored_file = await store_async(
                file,
                file_options=StoreFileOptions(
                    synapse_store=True,
                    content_type="text/plain",
                    merge_existing_annotations=True
                )
            )
            print(f"Stored file: {stored_file.name}")

        asyncio.run(main())
        ```

    Example: Storing a folder
        Store a folder to Synapse:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import Folder
        from synapseclient.operations import store_async

        async def main():
            syn = Synapse()
            syn.login()

            folder = Folder(name="My Folder", parent_id="syn123456")
            stored_folder = await store_async(folder)
            print(f"Stored folder: {stored_folder.name} (ID: {stored_folder.id})")

        asyncio.run(main())
        ```

    Example: Storing a project
        Store a project to Synapse:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import Project
        from synapseclient.operations import store_async

        async def main():
            syn = Synapse()
            syn.login()

            project = Project(name="My Project")
            stored_project = await store_async(project)
            print(f"Stored project: {stored_project.name} (ID: {stored_project.id})")

        asyncio.run(main())
        ```

    Example: Storing a table
        Store a table to Synapse:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import Table, Column, ColumnType
        from synapseclient.operations import store_async, StoreTableOptions

        async def main():
            syn = Synapse()
            syn.login()

            table = Table(
                name="My Table",
                parent_id="syn123456",
                columns=[Column(name="col1", column_type=ColumnType.STRING, maximum_size=50)]
            )
            stored_table = await store_async(
                table,
                table_options=StoreTableOptions(dry_run=False, job_timeout=600)
            )
            print(f"Stored table: {stored_table.name} (ID: {stored_table.id})")

        asyncio.run(main())
        ```

    Example: Storing a link
        Store a link to Synapse:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import Link
        from synapseclient.operations import store_async

        async def main():
            syn = Synapse()
            syn.login()

            link = Link(name="My Link", parent_id="syn123456", target_id="syn789")
            stored_link = await store_async(link)
            print(f"Stored link: {stored_link.name} (ID: {stored_link.id})")

        asyncio.run(main())
        ```

    Example: Storing with parent parameter
        Store an entity by passing the parent as a parameter:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File, Folder
        from synapseclient.operations import store_async

        async def main():
            syn = Synapse()
            syn.login()

            parent_folder = Folder(id="syn123456")
            file = File(path="/path/to/file.txt")
            stored_file = await store_async(file, parent=parent_folder)
            print(f"Stored file: {stored_file.name} in folder {parent_folder.id}")

        asyncio.run(main())
        ```
    """
    from synapseclient.models import (
        AgentSession,
        CurationTask,
        Dataset,
        DatasetCollection,
        EntityView,
        Evaluation,
        File,
        Folder,
        FormData,
        FormGroup,
        Grid,
        JSONSchema,
        Link,
        MaterializedView,
        Project,
        RecordSet,
        SchemaOrganization,
        SubmissionView,
        Table,
        Team,
        VirtualTable,
    )

    # Determine entity type and route to appropriate handler
    if isinstance(entity, (File, RecordSet)):
        return await _handle_store_file_entity(
            entity=entity,
            parent=parent,
            file_options=file_options,
            synapse_client=synapse_client,
        )

    elif isinstance(entity, (Project, Folder)):
        return await _handle_store_container_entity(
            entity=entity,
            parent=parent,
            container_options=container_options,
            synapse_client=synapse_client,
        )

    elif isinstance(
        entity,
        (
            Dataset,
            DatasetCollection,
            EntityView,
            MaterializedView,
            SubmissionView,
            Table,
            VirtualTable,
        ),
    ):
        return await _handle_store_table_entity(
            entity=entity,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif isinstance(entity, Link):
        return await _handle_store_link_entity(
            entity=entity,
            parent=parent,
            synapse_client=synapse_client,
        )

    elif isinstance(entity, Evaluation):
        return await entity.store_async(synapse_client=synapse_client)

    elif isinstance(entity, Team):
        if entity.id:
            return await entity.store_async(synapse_client=synapse_client)
        else:
            return await entity.create_async(synapse_client=synapse_client)

    elif isinstance(entity, SchemaOrganization):
        return await entity.store_async(synapse_client=synapse_client)

    elif isinstance(entity, CurationTask):
        return await entity.store_async(synapse_client=synapse_client)

    elif isinstance(entity, AgentSession):
        return await entity.update_async(synapse_client=synapse_client)

    elif isinstance(entity, (FormGroup, FormData)):
        return await entity.create_or_get_async(synapse_client=synapse_client)

    elif isinstance(entity, JSONSchema):
        if not json_schema_options or not json_schema_options.schema_body:
            raise ValueError(
                "json_schema_options with schema_body is required for JSONSchema entities"
            )
        return await _handle_store_json_schema_entity(
            entity=entity,
            schema_body=json_schema_options.schema_body,
            version=json_schema_options.version,
            dry_run=json_schema_options.dry_run,
            synapse_client=synapse_client,
        )

    elif isinstance(entity, Grid):
        attach_to_previous = (
            grid_options.attach_to_previous_session if grid_options else False
        )
        timeout = grid_options.timeout if grid_options else 120
        return await entity.create_async(
            attach_to_previous_session=attach_to_previous,
            timeout=timeout,
            synapse_client=synapse_client,
        )

    else:
        raise ValueError(
            f"Unsupported entity type: {type(entity).__name__}. "
            "Supported types are: AgentSession, CurationTask, "
            "Dataset, DatasetCollection, EntityView, Evaluation, File, Folder, FormData, "
            "FormGroup, Grid, JSONSchema, Link, MaterializedView, Project, RecordSet, "
            "SchemaOrganization, SubmissionView, Table, Team, VirtualTable."
        )
