"""Protocol for the specific methods of StorageLocationConfigurable mixin that have
synchronous counterparts generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models.services.migration_types import MigrationResult


class StorageLocationConfigurableSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def set_storage_location(
        self,
        storage_location_id: Optional[Union[int, List[int]]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, Any]:
        """Set the upload storage location for this entity. This configures where
        files uploaded to this entity will be stored.

        Arguments:
            storage_location_id: The storage location ID(s) to set. Can be a single
                ID, a list of IDs (first is default, max 10), or None to use
                Synapse default storage.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The project setting dict returned from Synapse.

        Raises:
            ValueError: If the entity does not have an id set.

        Example: Setting storage location on a folder
            Set storage location on a folder:

                from synapseclient.models import Folder

                import synapseclient
                synapseclient.login()

                folder = Folder(id="syn123").get()
                setting = folder.set_storage_location(storage_location_id=12345)
                print(setting)
        """
        return {}

    def get_project_setting(
        self,
        setting_type: str = "upload",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get the project setting for this entity.

        Arguments:
            setting_type: The type of setting to retrieve. One of:
                'upload', 'external_sync', 'requester_pays'. Default: 'upload'.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The project setting as a dictionary, or None if no setting exists.

        Raises:
            ValueError: If the entity does not have an id set.

        Example: Getting project settings
            Get the upload settings for a folder:

                from synapseclient.models import Folder

                import synapseclient
                synapseclient.login()

                folder = Folder(id="syn123").get()
                setting = folder.get_project_setting(setting_type="upload")
                if setting:
                    print(f"Storage locations: {setting.locations}")
        """
        return {}

    def delete_project_setting(
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

        Example: Deleting a project setting
            Delete the upload settings for a folder:

                from synapseclient.models import Folder

                import synapseclient
                synapseclient.login()

                folder = Folder(id="syn123").get()
                setting = folder.get_project_setting(setting_type="upload")
                if setting:
                    folder.delete_project_setting(setting_id=setting['id'])
        """
        return None

    def get_sts_storage_token(
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

                import boto3
                from synapseclient.models import Folder

                import synapseclient
                synapseclient.login()

                folder = Folder(id="syn123").get()
                credentials = folder.get_sts_storage_token(
                    permission="read_write",
                    output_format="boto",
                )
                s3_client = boto3.client('s3', **credentials)
        """
        return {}

    def index_files_for_migration(
        self,
        dest_storage_location_id: int,
        db_path: Optional[str] = None,
        *,
        source_storage_location_ids: Optional[List[int]] = None,
        file_version_strategy: str = "new",
        include_table_files: bool = False,
        continue_on_error: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> "MigrationResult":
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

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                project = Project(id="syn123").get()
                result = project.index_files_for_migration(
                    dest_storage_location_id=12345,
                )
                print(f"Database path: {result.db_path}")
                print(f"Indexed {result.counts_by_status}")
        """
        return None

    def migrate_indexed_files(
        self,
        db_path: str,
        *,
        create_table_snapshots: bool = True,
        continue_on_error: bool = False,
        force: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional["MigrationResult"]:
        """Migrate files that have been indexed with `index_files_for_migration`.

        This is the second step in migrating files to a new storage location.
        Files must first be indexed using `index_files_for_migration`.

        Arguments:
            db_path: Path to the SQLite database file created by
                `index_files_for_migration`. You can get this from the
                MigrationResult.db_path returned by index_files_for_migration.
            create_table_snapshots: Whether to create table snapshots before
                migrating table files.
            continue_on_error: Whether to continue migration if an error occurs.
            force: Whether to force migration of files that have already been
                migrated. Also bypasses interactive confirmation.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A MigrationResult object containing migration statistics, or None
            if the user declined the confirmation prompt.

        Example: Migrating indexed files
            Migrate previously indexed files:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                project = Project(id="syn123").get()

                # Index first
                index_result = project.index_files_for_migration(
                    dest_storage_location_id=12345,
                )

                # Then migrate using the db_path from index result
                result = project.migrate_indexed_files(
                    db_path=index_result.db_path,
                    force=True,  # Skip interactive confirmation
                )
                print(f"Migrated {result.counts_by_status}")
        """
        return None
