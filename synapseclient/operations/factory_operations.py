"""Factory method for retrieving entities by Synapse ID."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Union

from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.exceptions import SynapseNotFoundError

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        Link,
        MaterializedView,
        Project,
        SubmissionView,
        Table,
        VirtualTable,
    )


@dataclass
class FileOptions:
    """
    Configuration options specific to File entities when using the factory methods.

    This dataclass allows you to customize how File entities are handled during
    retrieval, including download behavior, file location, and collision handling.

    Attributes:
        download_file: Whether to automatically download the file content when
            retrieving the File entity. If True, the file will be downloaded to
            the local filesystem. If False, only the metadata will be retrieved.
            Default is True.
        download_location: The local directory path where the file should be
            downloaded. If None, the file will be downloaded to the default Synapse
            cache location. If specified,
            must be a valid directory path. Default is None.
        if_collision: Strategy to use when a file with the same name already
            exists at the download location. Valid options are:
            - "keep.both": Keep both files by appending a number to the new file
            - "overwrite.local": Overwrite the existing local file
            - "keep.local": Keep the existing local file and skip download
            Default is "keep.both".

    Example:
        Configure file download options:

        ```python
        from synapseclient.operations import FileOptions

        # Download file to specific location with overwrite
        file_options = FileOptions(
            download_file=True,
            download_location="/path/to/downloads/",
            if_collision="overwrite.local"
        )

        # Only retrieve metadata, don't download file content
        metadata_only = FileOptions(download_file=False)
        ```
    """

    download_file: bool = True
    download_location: Optional[str] = None
    if_collision: str = "keep.both"


@dataclass
class ActivityOptions:
    """
    Configuration options for entities that support activity/provenance tracking.

    This dataclass controls whether activity information (provenance data) should
    be included when retrieving entities. Activity information tracks the computational
    steps, data sources, and relationships that led to the creation of an entity.

    Attributes:
        include_activity: Whether to include activity/provenance information when
            retrieving the entity. If True, the returned entity will have its
            activity attribute populated with provenance data (if available).
            If False, the activity attribute will be None. Including activity
            may result in additional API calls and slower retrieval times.
            Default is False.

    Example:
        Configure activity inclusion:

        ```python
        from synapseclient.operations import ActivityOptions

        # Include activity information
        with_activity = ActivityOptions(include_activity=True)

        # Skip activity information (faster retrieval)
        without_activity = ActivityOptions(include_activity=False)
        ```

    Note:
        Activity information is only available for entities that support provenance
        tracking (File, Table, Dataset, etc...). For other entity
        types, this option is ignored.
    """

    include_activity: bool = False


@dataclass
class TableOptions:
    """
    Configuration options for table-like entities when using the factory methods.

    This dataclass controls how table-like entities (Table, Dataset, EntityView,
    MaterializedView, SubmissionView, VirtualTable, and DatasetCollection) are
    retrieved, particularly whether column schema information should be included.

    Attributes:
        include_columns: Whether to include column schema information when
            retrieving table-like entities. If True, the returned entity will
            have its columns attribute populated with Column objects containing
            schema information (name, column_type, etc.). If False, the columns
            attribute will be an empty dict. Including columns may result in
            additional API calls but provides complete table structure information.
            Default is True.

    Example:
        Configure table column inclusion:

        ```python
        from synapseclient.operations import TableOptions

        # Include column schema information
        with_columns = TableOptions(include_columns=True)

        # Skip column information (faster retrieval)
        without_columns = TableOptions(include_columns=False)
        ```
    """

    include_columns: bool = True


@dataclass
class LinkOptions:
    """
    Configuration options specific to Link entities when using the factory methods.

    This dataclass controls how Link entities are handled during retrieval,
    particularly whether the link should be followed to return the target entity
    or if the Link entity itself should be returned.

    Attributes:
        follow_link: Whether to follow the link and return the target entity
            instead of the Link entity itself. If True, the factory method will
            return the entity that the Link points to (e.g., if a Link points
            to a File, a File object will be returned). If False, the Link
            entity itself will be returned, allowing you to inspect the link's
            properties such as target_id, target_version, etc. Default is True.

    Example:
        Configure link following behavior:

        ```python
        from synapseclient.operations import LinkOptions

        # Follow the link and return the target entity
        follow_target = LinkOptions(follow_link=True)

        # Return the Link entity itself
        return_link = LinkOptions(follow_link=False)
        ```

    Note:
        - When follow_link=True, the returned entity type depends on what the
          Link points to (could be File, Project, Folder, etc.)
        - When follow_link=False, a Link entity is always returned
    """

    follow_link: bool = True


async def _handle_entity_instance(
    entity,
    version_number: Optional[int] = None,
    activity_options: Optional[ActivityOptions] = None,
    file_options: Optional[FileOptions] = None,
    table_options: Optional[TableOptions] = None,
    link_options: Optional[LinkOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "File",
    "Folder",
    "Link",
    "MaterializedView",
    "Project",
    "SubmissionView",
    "Table",
    "VirtualTable",
]:
    """
    Handle the case where an entity instance is passed directly to get_async.

    This private function encapsulates the logic for applying options and calling
    get_async on an existing entity instance.
    """
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Link,
        MaterializedView,
        SubmissionView,
        Table,
        VirtualTable,
    )

    if version_number is not None and hasattr(entity, "version_number"):
        entity.version_number = version_number

    get_kwargs = {"synapse_client": synapse_client}

    if activity_options and activity_options.include_activity:
        get_kwargs["include_activity"] = True

    table_like_entities = (
        Dataset,
        DatasetCollection,
        EntityView,
        MaterializedView,
        SubmissionView,
        Table,
        VirtualTable,
    )
    if table_options and isinstance(entity, table_like_entities):
        get_kwargs["include_columns"] = table_options.include_columns

    if file_options and isinstance(entity, File):
        if hasattr(file_options, "download_file"):
            entity.download_file = file_options.download_file
        if (
            hasattr(file_options, "download_location")
            and file_options.download_location
        ):
            entity.path = file_options.download_location
        if hasattr(file_options, "if_collision"):
            entity.if_collision = file_options.if_collision

    if link_options and isinstance(entity, Link):
        if hasattr(link_options, "follow_link"):
            get_kwargs["follow_link"] = link_options.follow_link

    return await entity.get_async(**get_kwargs)


async def _handle_simple_entity(
    entity_class,
    synapse_id: str,
    version_number: Optional[int] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union["Project", "Folder"]:
    """
    Handle simple entities that only need basic setup (Project, Folder, DatasetCollection).
    """
    entity = entity_class(id=synapse_id)
    if version_number and hasattr(entity, "version_number"):
        entity.version_number = version_number
    return await entity.get_async(synapse_client=synapse_client)


async def _handle_table_like_entity(
    entity_class,
    synapse_id: str,
    version_number: Optional[int] = None,
    activity_options: Optional[ActivityOptions] = None,
    table_options: Optional[TableOptions] = None,
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
    Handle table-like entities (Table, Dataset, EntityView, MaterializedView, SubmissionView, VirtualTable).
    """
    entity = entity_class(id=synapse_id)
    if version_number:
        entity.version_number = version_number

    kwargs = {"synapse_client": synapse_client}

    if table_options:
        kwargs["include_columns"] = table_options.include_columns
    if activity_options and activity_options.include_activity:
        kwargs["include_activity"] = True

    return await entity.get_async(**kwargs)


async def _handle_file_entity(
    synapse_id: str,
    version_number: Optional[int] = None,
    activity_options: Optional[ActivityOptions] = None,
    file_options: Optional[FileOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> "File":
    """
    Handle File entities with file-specific options.
    """
    from synapseclient.models import File

    file_kwargs = {"id": synapse_id}

    if version_number:
        file_kwargs["version_number"] = version_number

    if file_options:
        file_kwargs["download_file"] = file_options.download_file
        if file_options.download_location:
            file_kwargs["path"] = file_options.download_location
        file_kwargs["if_collision"] = file_options.if_collision

    entity = File(**file_kwargs)

    get_kwargs = {"synapse_client": synapse_client}

    if activity_options and activity_options.include_activity:
        get_kwargs["include_activity"] = True

    return await entity.get_async(**get_kwargs)


async def _handle_link_entity(
    synapse_id: str,
    link_options: Optional[LinkOptions] = None,
    file_options: Optional[FileOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "File",
    "Folder",
    "Link",
    "MaterializedView",
    "Project",
    "SubmissionView",
    "Table",
    "VirtualTable",
]:
    """
    Handle Link entities with link-specific options.

    Note: Links don't support versioning, so version_number is not included.
    """
    from synapseclient.models import Link

    entity = Link(id=synapse_id)

    kwargs = {"synapse_client": synapse_client}

    if link_options:
        kwargs["follow_link"] = link_options.follow_link

    if file_options:
        kwargs["file_options"] = file_options

    return await entity.get_async(**kwargs)


def get(
    synapse_id: Optional[str] = None,
    *,
    entity_name: Optional[str] = None,
    parent_id: Optional[str] = None,
    version_number: Optional[int] = None,
    activity_options: Optional[ActivityOptions] = None,
    file_options: Optional[FileOptions] = None,
    table_options: Optional[TableOptions] = None,
    link_options: Optional[LinkOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "File",
    "Folder",
    "Link",
    "MaterializedView",
    "Project",
    "SubmissionView",
    "Table",
    "VirtualTable",
]:
    """
    Factory method to retrieve any Synapse entity by its ID or by name and parent ID.

    This method serves as a unified interface for retrieving any type of Synapse entity
    without needing to know the specific entity type beforehand. It automatically
    determines the entity type and returns the appropriate model instance.

    You can retrieve entities in two ways:

    1. By providing a synapse_id directly
    2. By providing entity_name and optionally parent_id for lookup

    Arguments:
        synapse_id: The Synapse ID of the entity to retrieve (e.g., 'syn123456').
            Mutually exclusive with entity_name.
        entity_name: The name of the entity to find. Must be used with this approach
            instead of synapse_id. When looking up projects, parent_id should be None.
        parent_id: The parent entity ID when looking up by name. Set to None when
            looking up projects by name. Only used with entity_name.
        version_number: The specific version number of the entity to retrieve. Only
            applies to versionable entities (File, Table, Dataset). If not specified,
            the most recent version will be retrieved. Ignored for other entity types.
        activity_options: Activity-specific configuration options. Can be applied to
            any entity type to include activity information.
        file_options: File-specific configuration options. Only applies to File entities.
            Ignored for other entity types.
        table_options: Table-specific configuration options. Only applies to Table-like
            entities (Table, Dataset, EntityView, MaterializedView, SubmissionView,
            VirtualTable, DatasetCollection). Ignored for other entity types.
        link_options: Link-specific configuration options. Only applies when the entity
            is a Link. Ignored for other entity types.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The appropriate Synapse entity model instance based on the entity type.

    Raises:
        ValueError: If both synapse_id and entity_name are provided, or if neither is provided.
        ValueError: If entity_name is provided without this being a valid lookup scenario.
        ValueError: If the synapse_id is not a valid Synapse ID format.

    Note:
        When using entity_name lookup:

        - For projects: leave parent_id=None
        - For all other entities: provide the parent_id of the containing folder/project

    Example: Retrieving entities by ID
        Get any entity by Synapse ID:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File, Project
        from synapseclient.operations import get

        syn = Synapse()
        syn.login()

        # Works for any entity type
        entity = get(synapse_id="syn123456")

        # The returned object will be the appropriate type
        if isinstance(entity, File):
            print(f"File: {entity.name}")
        elif isinstance(entity, Project):
            print(f"Project: {entity.name}")
        ```

    Example: Retrieving entities by name
        Get an entity by name and parent:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get

        syn = Synapse()
        syn.login()

        # Get a file by name within a folder
        entity = get(
            entity_name="my_file.txt",
            parent_id="syn123456"
        )

        # Get a project by name (parent_id=None)
        project = get(
            entity_name="My Project",
            parent_id=None
        )
        ```

    Example: Retrieving a specific version
        Get a specific version of a versionable entity:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get

        syn = Synapse()
        syn.login()

        entity = get(synapse_id="syn123456", version_number=2)
        ```

    Example: Retrieving a file with custom options
        Get file metadata with specific download options:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, FileOptions, ActivityOptions

        syn = Synapse()
        syn.login()

        file_entity = get(
            synapse_id="syn123456",
            activity_options=ActivityOptions(include_activity=True),
            file_options=FileOptions(
                download_file=False
            )
        )
        ```

    Example: Retrieving a table with activity and columns
        Get table with activity and column information:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, ActivityOptions, TableOptions

        syn = Synapse()
        syn.login()

        table_entity = get(
            synapse_id="syn123456",
            activity_options=ActivityOptions(include_activity=True),
            table_options=TableOptions(include_columns=True)
        )
        ```

    Example: Following links
        Get the target of a link entity:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, LinkOptions

        syn = Synapse()
        syn.login()

        target_entity = get(
            synapse_id="syn123456",
            link_options=LinkOptions(follow_link=True)
        )
        ```

    Example: Working with Link entities
        Get a Link entity without following it:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, LinkOptions

        syn = Synapse()
        syn.login()

        # Get the link entity itself
        link_entity = get(
            synapse_id="syn123456",  # Example link ID
            link_options=LinkOptions(follow_link=False)
        )
        print(f"Link: {link_entity.name} -> {link_entity.target_id}")

        # Then follow the link to get the target
        target_entity = get(
            synapse_id="syn123456",
            link_options=LinkOptions(follow_link=True)
        )
        print(f"Target: {target_entity.name} (type: {type(target_entity).__name__})")
        ```

    Example: Comprehensive File options
        Download file with custom location and collision handling:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, FileOptions

        syn = Synapse()
        syn.login()

        file_entity = get(
            synapse_id="syn123456",
            file_options=FileOptions(
                download_file=True,
                download_location="/path/to/download/",
                if_collision="overwrite.local"
            )
        )
        print(f"Downloaded file: {file_entity.name} to {file_entity.path}")
        ```

    Example: Table options for table-like entities
        Get table entities with column information:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, TableOptions

        syn = Synapse()
        syn.login()

        # Works for Table, Dataset, EntityView, MaterializedView,
        # SubmissionView, VirtualTable, and DatasetCollection
        table_entity = get(
            synapse_id="syn123456",  # Example table ID
            table_options=TableOptions(include_columns=True)
        )
        print(f"Table: {table_entity.name} with {len(table_entity.columns)} columns")
        ```

    Example: Combining multiple options
        Get a File with both activity and custom download options:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, FileOptions, ActivityOptions

        syn = Synapse()
        syn.login()

        file_entity = get(
            synapse_id="syn123456",
            activity_options=ActivityOptions(include_activity=True),
            file_options=FileOptions(
                download_file=False
            )
        )
        print(f"File: {file_entity.name} (activity included: {file_entity.activity is not None})")
        ```

    Example: Working with entity instances
        Pass an existing entity instance to refresh or apply new options:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import get, FileOptions

        syn = Synapse()
        syn.login()

        # Get an entity first
        entity = get(synapse_id="syn123456")
        print(f"Original entity: {entity.name}")

        # Then use the entity instance to get it again with different options
        refreshed_entity = get(
            entity,
            file_options=FileOptions(download_file=False)
        )
        print(f"Refreshed entity: {refreshed_entity.name} (download_file: {refreshed_entity.download_file})")
        ```
    """
    return wrap_async_to_sync(
        coroutine=get_async(
            synapse_id=synapse_id,
            entity_name=entity_name,
            parent_id=parent_id,
            version_number=version_number,
            activity_options=activity_options,
            file_options=file_options,
            table_options=table_options,
            link_options=link_options,
            synapse_client=synapse_client,
        )
    )


async def get_async(
    synapse_id: Optional[str] = None,
    *,
    entity_name: Optional[str] = None,
    parent_id: Optional[str] = None,
    version_number: Optional[int] = None,
    activity_options: Optional[ActivityOptions] = None,
    file_options: Optional[FileOptions] = None,
    table_options: Optional[TableOptions] = None,
    link_options: Optional[LinkOptions] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Union[
    "Dataset",
    "DatasetCollection",
    "EntityView",
    "File",
    "Folder",
    "Link",
    "MaterializedView",
    "Project",
    "SubmissionView",
    "Table",
    "VirtualTable",
]:
    """
    Factory method to retrieve any Synapse entity by its ID or by name and parent ID.

    This method serves as a unified interface for retrieving any type of Synapse entity
    without needing to know the specific entity type beforehand. It automatically
    determines the entity type and returns the appropriate model instance.

    You can retrieve entities in two ways:

    1. By providing a synapse_id directly
    2. By providing entity_name and optionally parent_id for lookup

    Arguments:
        synapse_id: The Synapse ID of the entity to retrieve (e.g., 'syn123456').
            Mutually exclusive with entity_name.
        entity_name: The name of the entity to find. Must be used with this approach
            instead of synapse_id. When looking up projects, parent_id should be None.
        parent_id: The parent entity ID when looking up by name. Set to None when
            looking up projects by name. Only used with entity_name.
        version_number: The specific version number of the entity to retrieve. Only
            applies to versionable entities (File, Table, Dataset). If not specified,
            the most recent version will be retrieved. Ignored for other entity types.
        file_options: File-specific configuration options. Only applies to File entities.
            Ignored for other entity types.
        link_options: Link-specific configuration options. Only applies when the entity
            is a Link. Ignored for other entity types.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The appropriate Synapse entity model instance based on the entity type.

    Raises:
        ValueError: If both synapse_id and entity_name are provided, or if neither is provided.
        ValueError: If entity_name is provided without this being a valid lookup scenario.
        ValueError: If the synapse_id is not a valid Synapse ID format.

    Note:
        When using entity_name lookup:

        - For projects: leave parent_id=None
        - For all other entities: provide the parent_id of the containing folder/project

    Example: Retrieving entities by ID
        Get any entity by Synapse ID:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File, Project
        from synapseclient.operations import get_async

        async def main():
            syn = Synapse()
            syn.login()

            # Works for any entity type
            entity = await get_async(synapse_id="syn123456")

            # The returned object will be the appropriate type
            if isinstance(entity, File):
                print(f"File: {entity.name}")
            elif isinstance(entity, Project):
                print(f"Project: {entity.name}")

        asyncio.run(main())
        ```

    Example: Retrieving entities by name
        Get an entity by name and parent:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async

        async def main():
            syn = Synapse()
            syn.login()

            # Get a file by name within a folder
            entity = await get_async(
                entity_name="my_file.txt",
                parent_id="syn123456"
            )

            # Get a project by name (parent_id=None)
            project = await get_async(
                entity_name="My Project",
                parent_id=None
            )

        asyncio.run(main())
        ```

    Example: Retrieving a specific version
        Get a specific version of a versionable entity:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async

        async def main():
            syn = Synapse()
            syn.login()

            entity = await get_async(synapse_id="syn123456", version_number=2)

        asyncio.run(main())
        ```

    Example: Retrieving a file with custom options
        Get file metadata with specific download options:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, FileOptions, ActivityOptions

        async def main():
            syn = Synapse()
            syn.login()

            file_entity = await get_async(
                synapse_id="syn123456",
                activity_options=ActivityOptions(include_activity=True),
                file_options=FileOptions(
                    download_file=False
                )
            )

        asyncio.run(main())
        ```

    Example: Retrieving a table with activity and columns
        Get table with activity and column information:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, ActivityOptions, TableOptions

        async def main():
            syn = Synapse()
            syn.login()

            table_entity = await get_async(
                synapse_id="syn123456",
                activity_options=ActivityOptions(include_activity=True),
                table_options=TableOptions(include_columns=True)
            )

        asyncio.run(main())
        ```

    Example: Following links
        Get the target of a link entity:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, LinkOptions

        async def main():
            syn = Synapse()
            syn.login()

            target_entity = await get_async(
                synapse_id="syn123456",
                link_options=LinkOptions(follow_link=True)
            )

        asyncio.run(main())
        ```

    Example: Working with Link entities
        Get a Link entity without following it:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, LinkOptions

        async def main():
            syn = Synapse()
            syn.login()

            # Get the link entity itself
            link_entity = await get_async(
                synapse_id="syn123456",  # Example link ID
                link_options=LinkOptions(follow_link=False)
            )
            print(f"Link: {link_entity.name} -> {link_entity.target_id}")

            # Then follow the link to get the target
            target_entity = await get_async(
                synapse_id="syn123456",
                link_options=LinkOptions(follow_link=True)
            )
            print(f"Target: {target_entity.name} (type: {type(target_entity).__name__})")

        asyncio.run(main())
        ```

    Example: Comprehensive File options
        Download file with custom location and collision handling:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, FileOptions

        async def main():
            syn = Synapse()
            syn.login()

            file_entity = await get_async(
                synapse_id="syn123456",
                file_options=FileOptions(
                    download_file=True,
                    download_location="/path/to/download/",
                    if_collision="overwrite.local"
                )
            )
            print(f"Downloaded file: {file_entity.name} to {file_entity.path}")

        asyncio.run(main())
        ```

    Example: Table options for table-like entities
        Get table entities with column information:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, TableOptions

        async def main():
            syn = Synapse()
            syn.login()

            # Works for Table, Dataset, EntityView, MaterializedView,
            # SubmissionView, VirtualTable, and DatasetCollection
            table_entity = await get_async(
                synapse_id="syn123456",  # Example table ID
                table_options=TableOptions(include_columns=True)
            )
            print(f"Table: {table_entity.name} with {len(table_entity.columns)} columns")

        asyncio.run(main())
        ```

    Example: Combining multiple options
        Get a File with both activity and custom download options:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, FileOptions, ActivityOptions

        async def main():
            syn = Synapse()
            syn.login()

            file_entity = await get_async(
                synapse_id="syn123456",
                activity_options=ActivityOptions(include_activity=True),
                file_options=FileOptions(
                    download_file=False
                )
            )
            print(f"File: {file_entity.name} (activity included: {file_entity.activity is not None})")

        asyncio.run(main())
        ```

    Example: Working with entity instances
        Pass an existing entity instance to refresh or apply new options:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import get_async, FileOptions

        async def main():
            syn = Synapse()
            syn.login()

            # Get an entity first
            entity = await get_async(synapse_id="syn123456")
            print(f"Original entity: {entity.name}")

            # Then use the entity instance to get it again with different options
            refreshed_entity = await get_async(
                entity,
                file_options=FileOptions(download_file=False)
            )
            print(f"Refreshed entity: {refreshed_entity.name} (download_file: {refreshed_entity.download_file})")

        asyncio.run(main())
        ```
    """
    from synapseclient.api.entity_bundle_services_v2 import (
        get_entity_id_bundle2,
        get_entity_id_version_bundle2,
    )
    from synapseclient.api.entity_services import get_child, get_entity_type
    from synapseclient.core.constants import concrete_types
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        Link,
        MaterializedView,
        Project,
        SubmissionView,
        Table,
        VirtualTable,
    )

    activity_options = activity_options or ActivityOptions()
    file_options = file_options or FileOptions()
    table_options = table_options or TableOptions()
    link_options = link_options or LinkOptions()

    # Handle case where an entity instance is passed directly
    entity_types = (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        Link,
        MaterializedView,
        Project,
        SubmissionView,
        Table,
        VirtualTable,
    )
    if isinstance(synapse_id, entity_types):
        return await _handle_entity_instance(
            entity=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            file_options=file_options,
            table_options=table_options,
            link_options=link_options,
            synapse_client=synapse_client,
        )

    # Validate input parameters
    if synapse_id is not None and entity_name is not None:
        raise ValueError(
            "Cannot specify both synapse_id and entity_name. "
            "Use synapse_id for direct lookup or entity_name with optional parent_id for name-based lookup."
        )

    if synapse_id is None and entity_name is None:
        raise ValueError(
            "Must specify either synapse_id or entity_name. "
            "Use synapse_id for direct lookup or entity_name with optional parent_id for name-based lookup."
        )

    # If looking up by name, get the synapse_id first
    if entity_name is not None and synapse_id is None:
        synapse_id = await get_child(
            entity_name=entity_name, parent_id=parent_id, synapse_client=synapse_client
        )
        if synapse_id is None:
            if parent_id is None:
                raise SynapseNotFoundError(
                    f"Project with name '{entity_name}' not found."
                )
            else:
                raise SynapseNotFoundError(
                    f"Entity with name '{entity_name}' not found in parent '{parent_id}'."
                )

    entity_header = await get_entity_type(
        entity_id=synapse_id, synapse_client=synapse_client
    )
    entity_type = entity_header.type

    if entity_type == concrete_types.LINK_ENTITY:
        return await _handle_link_entity(
            synapse_id=synapse_id,
            link_options=link_options,
            file_options=file_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.FILE_ENTITY:
        return await _handle_file_entity(
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            file_options=file_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.PROJECT_ENTITY:
        return await _handle_simple_entity(
            entity_class=Project,
            synapse_id=synapse_id,
            version_number=version_number,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.FOLDER_ENTITY:
        return await _handle_simple_entity(
            entity_class=Folder,
            synapse_id=synapse_id,
            version_number=version_number,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.TABLE_ENTITY:
        return await _handle_table_like_entity(
            entity_class=Table,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.DATASET_ENTITY:
        return await _handle_table_like_entity(
            entity_class=Dataset,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.DATASET_COLLECTION_ENTITY:
        return await _handle_table_like_entity(
            entity_class=DatasetCollection,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.ENTITY_VIEW:
        return await _handle_table_like_entity(
            entity_class=EntityView,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.MATERIALIZED_VIEW:
        return await _handle_table_like_entity(
            entity_class=MaterializedView,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.SUBMISSION_VIEW:
        return await _handle_table_like_entity(
            entity_class=SubmissionView,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    elif entity_type == concrete_types.VIRTUAL_TABLE:
        return await _handle_table_like_entity(
            entity_class=VirtualTable,
            synapse_id=synapse_id,
            version_number=version_number,
            activity_options=activity_options,
            table_options=table_options,
            synapse_client=synapse_client,
        )

    else:
        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.warning(
            "Unknown entity type: %s. Falling back to returning %s as a dictionary bundle matching "
            "https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html",
            entity_type,
            synapse_id,
        )

        # This allows the function to handle new entity types that may be added in the future
        if version_number is not None:
            return await get_entity_id_version_bundle2(
                entity_id=synapse_id,
                version=version_number,
                synapse_client=synapse_client,
            )
        else:
            return await get_entity_id_bundle2(
                entity_id=synapse_id, synapse_client=synapse_client
            )
