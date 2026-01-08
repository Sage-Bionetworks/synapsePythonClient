"""Factory method for deleting resources from Synapse."""

from typing import TYPE_CHECKING, Optional, Union

from synapseclient.api import delete_entity
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.utils import is_synapse_id_str

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import (
        CurationTask,
        Dataset,
        DatasetCollection,
        EntityView,
        Evaluation,
        File,
        Folder,
        Grid,
        JSONSchema,
        MaterializedView,
        Project,
        RecordSet,
        SchemaOrganization,
        SubmissionView,
        Table,
        Team,
        VirtualTable,
    )


def delete(
    entity: Union[
        str,
        "CurationTask",
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "Evaluation",
        "File",
        "Folder",
        "Grid",
        "JSONSchema",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SchemaOrganization",
        "SubmissionView",
        "Table",
        "Team",
        "VirtualTable",
    ],
    version: Optional[Union[int, str]] = None,
    version_only: bool = False,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Factory method to delete any Synapse entity.

    This method serves as a unified interface for deleting any type of Synapse entity.
    It automatically applies the appropriate delete logic based on the entity type.

    Version Deletion Behavior:
        The function supports deleting specific versions of entities with the following
        precedence order:

        1. **version parameter** (highest priority): When provided, this version number
           will be used for deletion, overriding any version information from the entity
           object or the entity ID string.

        2. **Entity's version_number attribute** (secondary): If the entity object has
           a `version_number` attribute set and no explicit `version` parameter is
           provided, this version will be used.

        3. **version_only parameter**: When set to True, indicates that only a specific
           version should be deleted (not the entire entity). This parameter is only
           meaningful when combined with a version number from either the `version`
           parameter or the entity's `version_number` attribute.

        **Supported for version-specific deletion:**

        - String ID with version (e.g., "syn123.4")
        - File, RecordSet (use version_only=True)
        - Table, Dataset, DatasetCollection, EntityView, MaterializedView,
          SubmissionView, VirtualTable (use version_only=True)

        **Not supported for version-specific deletion:**

        - Project, Folder, Evaluation, Team, SchemaOrganization, CurationTask, Grid

    Arguments:
        entity: The entity instance to delete, or a Synapse ID string (e.g., "syn123456"
            or "syn123456.4" for a specific version). Must be one of the supported
            entity types or a valid Synapse ID.
        version: Optional version number to delete. Takes precedence over any version
            information in the entity object or ID string. When provided with
            `version_only=True`, deletes only this specific version.
        version_only: If True, only the specified version will be deleted, not the
            entire entity. Requires a version number from either the `version` parameter
            or the entity's `version_number` attribute. This parameter is applicable for
            entities that support version-specific deletion (File, RecordSet, Table,
            Dataset, DatasetCollection, EntityView, MaterializedView, SubmissionView,
            VirtualTable, JSONSchema).
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None

    Raises:
        ValueError: If the entity is not a supported type or not a valid Synapse ID.
        ValueError: If version_only is True but no version number is available.

    Example: Deleting a file by object
        Delete a file from Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        file = File(id="syn123456")
        delete(file)
        print("File deleted successfully")
        ```

    Example: Deleting a specific version of a file
        Delete only version 2 of a file, keeping other versions:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        # Method 1: Using version parameter (highest priority)
        file = File(id="syn123456")
        delete(file, version=2, version_only=True)

        # Method 2: Using entity's version_number attribute
        file = File(id="syn123456", version_number=2)
        delete(file, version_only=True)

        # Method 3: Using synapse ID with version
        delete("syn123456.2", version_only=True)
        print("File version 2 deleted successfully")
        ```

    Example: Deleting a file by ID string
        Delete a file from Synapse using just its ID:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        delete("syn123456")
        print("Entity deleted successfully")
        ```

    Example: Deleting a project
        Delete a project from Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Project
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        project = Project(id="syn123456")
        delete(project)
        print("Project deleted successfully")
        ```

    Example: Deleting a specific version of a RecordSet
        Delete only a specific version of a RecordSet:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import RecordSet
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        record_set = RecordSet(id="syn123456", version_number=3)
        delete(record_set, version_only=True)
        print("RecordSet version 3 deleted successfully")
        ```

    Example: Deleting a table
        Delete a table from Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Table
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        table = Table(id="syn123456")
        delete(table)
        print("Table deleted successfully")
        ```

    Example: Deleting a team
        Delete a team from Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Team
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        team = Team(id="123456")
        delete(team)
        print("Team deleted successfully")
        ```

    Example: Deleting a curation task
        Delete a curation task from Synapse:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import CurationTask
        from synapseclient.operations import delete

        syn = Synapse()
        syn.login()

        task = CurationTask(task_id=123)
        delete(task)
        print("Curation task deleted successfully")
        ```
    """
    return wrap_async_to_sync(
        coroutine=delete_async(
            entity=entity,
            version=version,
            version_only=version_only,
            synapse_client=synapse_client,
        )
    )


async def delete_async(
    entity: Union[
        str,
        "CurationTask",
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "Evaluation",
        "File",
        "Folder",
        "Grid",
        "JSONSchema",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SchemaOrganization",
        "SubmissionView",
        "Table",
        "Team",
        "VirtualTable",
    ],
    version: Optional[Union[int, str]] = None,
    version_only: bool = False,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Factory method to delete any Synapse entity asynchronously.

    This method serves as a unified interface for deleting any type of Synapse entity
    asynchronously. It automatically applies the appropriate delete logic based on the
    entity type.

    Version Deletion Behavior:
        The function supports deleting specific versions of entities with the following
        precedence order:

        1. **version parameter** (highest priority): When provided, this version number
           will be used for deletion, overriding any version information from the entity
           object or the entity ID string.

        2. **Entity's version_number attribute** (secondary): If the entity object has
           a `version_number` attribute set and no explicit `version` parameter is
           provided, this version will be used.

        3. **version_only parameter**: When set to True, indicates that only a specific
           version should be deleted (not the entire entity). This parameter is only
           meaningful when combined with a version number from either the `version`
           parameter or the entity's `version_number` attribute.

        **Supported for version-specific deletion:**

        - String ID with version (e.g., "syn123.4")
        - File, RecordSet (use version_only=True)
        - Table, Dataset, DatasetCollection, EntityView, MaterializedView,
          SubmissionView, VirtualTable (use version_only=True)

        **Not supported for version-specific deletion:**

        - Project, Folder, Evaluation, Team, SchemaOrganization, CurationTask, Grid

    Arguments:
        entity: The entity instance to delete, or a Synapse ID string (e.g., "syn123456"
            or "syn123456.4" for a specific version). Must be one of the supported
            entity types or a valid Synapse ID.
        version: Optional version number to delete. Takes precedence over any version
            information in the entity object or ID string. When provided with
            `version_only=True`, deletes only this specific version.
        version_only: If True, only the specified version will be deleted, not the
            entire entity. Requires a version number from either the `version` parameter
            or the entity's `version_number` attribute. This parameter is applicable for
            entities that support version-specific deletion (File, RecordSet, Table,
            Dataset, DatasetCollection, EntityView, MaterializedView, SubmissionView,
            VirtualTable, JSONSchema).
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None

    Raises:
        ValueError: If the entity is not a supported type or not a valid Synapse ID.
        ValueError: If version_only is True but no version number is available.

    Example: Deleting a file by object
        Delete a file from Synapse:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import delete_async

        async def main():
            syn = Synapse()
            syn.login()

            file = File(id="syn123456")
            await delete_async(file)
            print("File deleted successfully")

        asyncio.run(main())
        ```

    Example: Deleting a specific version of a file
        Delete only version 2 of a file, keeping other versions:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import delete_async

        async def main():
            syn = Synapse()
            syn.login()

            # Method 1: Using version parameter (highest priority)
            file = File(id="syn123456")
            await delete_async(file, version=2, version_only=True)

            # Method 2: Using entity's version_number attribute
            file = File(id="syn123456", version_number=2)
            await delete_async(file, version_only=True)

            # Method 3: Using synapse ID with version
            await delete_async("syn123456.2", version_only=True)
            print("File version 2 deleted successfully")

        asyncio.run(main())
        ```

    Example: Deleting a file by ID string
        Delete a file from Synapse using just its ID:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import delete_async

        async def main():
            syn = Synapse()
            syn.login()

            await delete_async("syn123456")
            print("Entity deleted successfully")

        asyncio.run(main())
        ```

    Example: Deleting a specific version of a RecordSet
        Delete only a specific version of a RecordSet:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import RecordSet
        from synapseclient.operations import delete_async

        async def main():
            syn = Synapse()
            syn.login()

            record_set = RecordSet(id="syn123456", version_number=3)
            await delete_async(record_set, version_only=True)
            print("RecordSet version 3 deleted successfully")

        asyncio.run(main())
        ```
    """
    from synapseclient.models import (
        CurationTask,
        Dataset,
        DatasetCollection,
        EntityView,
        Evaluation,
        File,
        Folder,
        Grid,
        JSONSchema,
        MaterializedView,
        Project,
        RecordSet,
        SchemaOrganization,
        SubmissionView,
        Table,
        Team,
        VirtualTable,
    )

    # Handle string synapse ID
    if isinstance(entity, str):
        from synapseclient.core.utils import get_synid_and_version

        synapse_id = is_synapse_id_str(entity)
        if not synapse_id:
            raise ValueError(
                f"Invalid Synapse ID: {entity}. "
                "Expected a valid Synapse ID string (e.g., 'syn123456' or 'syn123456.4')."
            )

        # Check if version is embedded in the string ID (e.g., "syn123.4")
        syn_id, syn_version = get_synid_and_version(synapse_id)

        # Determine final version: explicit version parameter takes precedence
        final_version = version if version is not None else syn_version

        # If there's a version to delete, require version_only=True for safety
        if final_version is not None and not version_only:
            raise ValueError(
                f"Deleting a specific version requires version_only=True. "
                f"Use delete('{entity}', version_only=True) to delete version {final_version}."
            )

        await delete_entity(
            entity_id=syn_id,
            version_number=final_version,
            synapse_client=synapse_client,
        )
        return None

    # Determine final version to use based on precedence
    final_version_for_entity = version
    entity_version = None

    # Check if entity has version_number attribute
    if hasattr(entity, "version_number"):
        entity_version = getattr(entity, "version_number", None)
        if final_version_for_entity is None and entity_version is not None:
            final_version_for_entity = entity_version

    # Emit warning only when there's an actual version conflict (both are set and different)
    if (
        version_only
        and version is not None
        and entity_version is not None
        and version != entity_version
    ):
        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.warning(
            f"Version conflict: 'version' parameter ({version}) differs from "
            f"entity's 'version_number' attribute ({entity_version}). "
            "Using 'version' parameter as it takes precedence."
        )

    # Handle entities that support version-specific deletion with version_only parameter
    if isinstance(entity, (File, RecordSet)):
        # Validate that if version_only is True, we have a version number
        if version_only and final_version_for_entity is None:
            raise ValueError(
                "version_only=True requires a version number. "
                "Provide either 'version' parameter or set 'version_number' on the entity."
            )

        # Only pass version_only=True when we actually have a version to delete
        if version_only and final_version_for_entity is not None:
            # Set the entity's version_number to the final version so delete_async uses it
            entity.version_number = final_version_for_entity
            return await entity.delete_async(
                version_only=True, synapse_client=synapse_client
            )
        else:
            return await entity.delete_async(
                version_only=False, synapse_client=synapse_client
            )

    # Handle table-like entities that support version deletion through the API
    elif isinstance(
        entity,
        (
            Table,
            Dataset,
            DatasetCollection,
            EntityView,
            MaterializedView,
            SubmissionView,
            VirtualTable,
        ),
    ):
        # Validate that if version_only is True, we have a version number
        if version_only and final_version_for_entity is None:
            raise ValueError(
                "version_only=True requires a version number. "
                "Provide either 'version' parameter or set 'version_number' on the entity."
            )

        # Only use version-specific deletion when version_only=True AND we have a version
        if version_only and final_version_for_entity is not None:
            # These entities support version deletion through the API
            return await delete_entity(
                entity_id=entity.id,
                version_number=final_version_for_entity,
                synapse_client=synapse_client,
            )
        else:
            # Delete entire entity (DeleteMixin will strip any version from entity.id)
            return await entity.delete_async(synapse_client=synapse_client)

    # Handle entities without version support
    elif isinstance(
        entity,
        (
            Folder,
            Project,
            Evaluation,
            Team,
            SchemaOrganization,
            CurationTask,
            Grid,
        ),
    ):
        if version_only or final_version_for_entity is not None:
            from synapseclient import Synapse

            client = Synapse.get_client(synapse_client=synapse_client)
            client.logger.warning(
                f"{type(entity).__name__} does not support version-specific deletion. "
                "The entire entity will be deleted."
            )
        return await entity.delete_async(synapse_client=synapse_client)

    # JSONSchema supports version parameter
    elif isinstance(entity, JSONSchema):
        if final_version_for_entity is not None:
            return await entity.delete_async(
                version=str(final_version_for_entity), synapse_client=synapse_client
            )
        else:
            return await entity.delete_async(synapse_client=synapse_client)

    else:
        raise ValueError(
            f"Unsupported entity type: {type(entity).__name__}. "
            "Supported types are: str (Synapse ID), CurationTask, Dataset, DatasetCollection, "
            "EntityView, Evaluation, File, Folder, Grid, JSONSchema, MaterializedView, "
            "Project, RecordSet, SchemaOrganization, SubmissionView, Table, Team, VirtualTable."
        )
