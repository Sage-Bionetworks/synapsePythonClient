"""Mixins for entities that can have their storage location and project settings configured."""

import asyncio
from typing import Any, List, Optional, Union

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.models.project_setting import ProjectSetting
from synapseclient.models.protocols.storage_location_mixin_protocol import (
    StorageLocationConfigurableSynchronousProtocol,
)
from synapseclient.models.services.migration import (
    index_files_for_migration_async as _index_files_for_migration_async,
)
from synapseclient.models.services.migration import (
    migrate_indexed_files_async as _migrate_indexed_files_async,
)
from synapseclient.models.services.migration_types import MigrationResult

# Default storage location ID used by Synapse
DEFAULT_STORAGE_LOCATION_ID = 1


@async_to_sync
class StorageLocationConfigurable(StorageLocationConfigurableSynchronousProtocol):
    """Mixin for objects that can have their storage location configured.

    In order to use this mixin, the class must have an `id` attribute.

    This mixin provides methods for:
    - Getting STS (AWS Security Token Service) credentials for direct S3 access
    - Migrating files to a new storage location
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity."""

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Entity_GetStsStorageToken: {self.id}"
    )
    async def get_sts_storage_token_async(
        self,
        permission: str,
        *,
        output_format: str = "json",
        min_remaining_life: Optional[int] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> Any:
        """Get STS (AWS Security Token Service) credentials for direct access to
        the storage location backing this entity. These credentials can be used
        with AWS tools like awscli and boto3.
        Note: The entity must use a storage location that has STS enabled.

        Arguments:
            permission: The permission level for the token. Must be 'read_only'
                or 'read_write'.
            output_format: The output format for the credentials. Options:
                'json' (default), 'boto', 'shell', 'bash', 'cmd', 'powershell'.
            min_remaining_life: The minimum remaining life (in seconds) for a
                cached token before a new one is fetched.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The STS credentials in the requested format.

        Raises:
            ValueError: If the entity does not have an id set.

        Example: Using credentials with boto3
            Get STS credentials for an STS-enabled folder and use with boto3:

                import asyncio
                import boto3
                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                async def main():
                    folder = await Folder(id="syn123").get_async()
                    credentials = await folder.get_sts_storage_token_async(
                        permission="read_write",
                        output_format="boto",
                    )
                    s3_client = boto3.client('s3', **credentials)

                asyncio.run(main())
        """
        if not self.id:
            raise ValueError("The entity must have an id set.")

        from synapseclient.core import sts_transfer

        client = Synapse.get_client(synapse_client=synapse_client)

        return await asyncio.to_thread(
            sts_transfer.get_sts_credentials,
            client,
            self.id,
            permission,
            output_format=output_format,
            min_remaining_life=min_remaining_life,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Entity_IndexFilesForMigration: {self.id}"
    )
    async def index_files_for_migration_async(
        self,
        dest_storage_location_id: int,
        db_path: Optional[str] = None,
        *,
        source_storage_location_ids: Optional[List[int]] = None,
        file_version_strategy: str = "new",
        include_table_files: bool = False,
        continue_on_error: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> MigrationResult:
        """Index files in this entity for migration to a new storage location.

        This is the first step in migrating files to a new storage location.
        After indexing, use `migrate_indexed_files` to perform the actual migration.

        Arguments:
            dest_storage_location_id: The destination storage location ID.
            db_path: Path to the SQLite database file for tracking migration state.
                If not provided, a temporary directory will be used. The path
                can be retrieved from the returned MigrationResult.db_path.
            source_storage_location_ids: Optional list of source storage location IDs
                to filter which files to migrate. If None, all files are indexed.
            file_version_strategy: Strategy for handling file versions. Options:
                'new' (default) - create new versions, 'all' - migrate all versions,
                'latest' - only migrate latest version, 'skip' - skip if file exists.
            include_table_files: Whether to include files attached to tables.
            continue_on_error: Whether to continue indexing if an error occurs.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A MigrationResult object containing indexing statistics and the database
            path (accessible via result.db_path).

        Example: Indexing files for migration
            Index files in a project for migration:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Project

                syn = Synapse()
                syn.login()

                async def main():
                    project = await Project(id="syn123").get_async()
                    result = await project.index_files_for_migration_async(
                        dest_storage_location_id=12345,
                    )
                    print(f"Database path: {result.db_path}")
                    print(f"Indexed {result.counts_by_status}")

                asyncio.run(main())
        """
        if not self.id:
            raise ValueError("The entity must have an id set.")

        return await _index_files_for_migration_async(
            self,
            dest_storage_location_id=str(dest_storage_location_id),
            db_path=db_path,
            source_storage_location_ids=(
                [str(s) for s in source_storage_location_ids]
                if source_storage_location_ids
                else None
            ),
            file_version_strategy=file_version_strategy,
            include_table_files=include_table_files,
            continue_on_error=continue_on_error,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Entity_MigrateIndexedFiles: {self.id}"
    )
    async def migrate_indexed_files_async(
        self,
        db_path: str,
        *,
        create_table_snapshots: bool = True,
        continue_on_error: bool = False,
        force: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional[MigrationResult]:
        """Migrate files that have been indexed with `index_files_for_migration`.

        This is the second step in migrating files to a new storage location.
        Files must first be indexed using `index_files_for_migration`.

        **Interactive confirmation:** When called from an interactive shell and
        ``force=False`` (the default), this method will print the number of items
        queued for migration and prompt for confirmation before proceeding. If
        standard output is not connected to an interactive terminal (e.g. a script
        or CI environment), migration is aborted unless ``force=True`` is set.

        Arguments:
            db_path: Path to the SQLite database file created by
                `index_files_for_migration`. You can get this from the
                MigrationResult.db_path returned by index_files_for_migration.
            create_table_snapshots: Whether to create table snapshots before
                migrating table files.
            continue_on_error: Whether to continue migration if an error occurs.
            force: Skip the interactive confirmation prompt and proceed with
                migration automatically. Set to ``True`` when running
                non-interactively (scripts, CI, automated pipelines).
                Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A MigrationResult object containing migration statistics, or None
            if migration was aborted (user declined the confirmation prompt, or
            the session is non-interactive and force=False).

        Example: Migrating indexed files
            Migrate previously indexed files:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Project

                syn = Synapse()
                syn.login()

                async def main():
                    project = await Project(id="syn123").get_async()

                    # Index first
                    index_result = await project.index_files_for_migration_async(
                        dest_storage_location_id=12345,
                    )

                    # Then migrate using the db_path from index result
                    result = await project.migrate_indexed_files_async(
                        db_path=index_result.db_path,
                        force=True,  # Skip interactive confirmation
                    )
                    print(f"Migrated {result.counts_by_status}")

                asyncio.run(main())
        """
        if not self.id:
            raise ValueError("The entity must have an id set.")

        return await _migrate_indexed_files_async(
            db_path=db_path,
            create_table_snapshots=create_table_snapshots,
            continue_on_error=continue_on_error,
            force=force,
            synapse_client=synapse_client,
        )


@async_to_sync
class ProjectSettingsMixin(StorageLocationConfigurable):
    """Mixin for objects that can have their project settings configured.

    Extends StorageLocationConfigurable with methods for managing project
    settings such as upload storage locations.

    In order to use this mixin, the class must have an `id` attribute.
    """

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Entity_SetStorageLocation: {self.id}"
    )
    async def set_storage_location_async(
        self,
        storage_location_id: Optional[
            Union[int, List[int]]
        ] = DEFAULT_STORAGE_LOCATION_ID,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "ProjectSetting":
        """Set the upload storage location for this entity. This configures where
        files uploaded to this entity will be stored.

        **This is a destructive update.** The provided `storage_location_id` value(s)
        will **replace** any storage locations previously configured on this entity.
        To add a storage location without removing existing ones, first retrieve the
        current setting via `get_project_setting_async`, append to its `locations`
        list, and call `store_async` on the returned `ProjectSetting` directly.
        The first ID in the list is the default upload destination.
        To obtain a storage location ID, create a
        [StorageLocation][synapseclient.models.StorageLocation] and use its
        `storage_location_id`. See
        [StorageLocationType][synapseclient.models.StorageLocationType] for the
        available storage backend types.

        Arguments:
            storage_location_id: The storage location ID(s) to set. Can be a single
                ID, a list of IDs (first is default, max 10). By default, the
                default Synapse S3 storage location is used.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The ProjectSetting object reflecting the current state after the operation.

        Raises:
            ValueError: If the entity does not have an id set.

        Example: Replace all storage locations
            Fully replace the storage location on a folder with a single location:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                async def main():
                    folder = await Folder(id="syn123").get_async()
                    setting = await folder.set_storage_location_async(
                        storage_location_id=12345
                    )
                    print(setting)

                asyncio.run(main())

        Example: Partial update — add a storage location without removing existing ones
            Retrieve the current setting and append a new location:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                async def main():
                    folder = await Folder(id="syn123").get_async()
                    setting = await folder.get_project_setting_async(setting_type="upload")
                    if setting:
                        setting.locations.append(67890)
                        await setting.store_async()

                asyncio.run(main())
        """
        if not self.id:
            raise ValueError("The entity must have an id set.")

        if storage_location_id is None:
            locations = [DEFAULT_STORAGE_LOCATION_ID]
        elif isinstance(storage_location_id, list):
            locations = storage_location_id
        else:
            locations = [storage_location_id]
        setting = await ProjectSetting(
            project_id=self.id, settings_type="upload"
        ).get_async(synapse_client=synapse_client)

        if setting is None:
            setting = ProjectSetting(
                project_id=self.id,
                settings_type="upload",
                locations=locations,
            )
        else:
            setting.locations = locations
        return await setting.store_async(synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Entity_GetProjectSetting: {self.id}"
    )
    async def get_project_setting_async(
        self,
        setting_type: str = "upload",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional["ProjectSetting"]:
        """Get the project setting for this entity.

        Arguments:
            setting_type: The type of setting to retrieve. Currently only 'upload' is supported.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The ProjectSetting object, or None if no setting exists.

        Raises:
            ValueError: If the entity does not have an id set.

        Example: Using this function
            Get the upload settings for a folder:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                async def main():
                    folder = await Folder(id="syn123").get_async()
                    setting = await folder.get_project_setting_async(setting_type="upload")
                    if setting:
                        print(f"Storage locations: {setting.locations}")

                asyncio.run(main())
        """
        if not self.id:
            raise ValueError("The entity must have an id set.")

        return await ProjectSetting(
            project_id=self.id, settings_type=setting_type
        ).get_async(synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Entity_DeleteProjectSetting: {self.id}"
    )
    async def delete_project_setting_async(
        self,
        setting_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Delete a project setting by its setting ID.

        Arguments:
            setting_id: The ID of the project setting to delete.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If the entity does not have an id set.

        Example: Using this function
            Delete the upload settings for a folder:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                async def main():
                    folder = await Folder(id="syn123").get_async()
                    await folder.delete_project_setting_async(setting_id="123")

                asyncio.run(main())
        """
        if not setting_id:
            raise ValueError("The id is required to delete a project setting.")
        await ProjectSetting(id=setting_id).delete_async(synapse_client=synapse_client)
