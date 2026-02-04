"""Protocol for the specific methods of StorageLocation that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol, Tuple

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Folder
    from synapseclient.models.storage_location import StorageLocation


class StorageLocationSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "StorageLocation":
        """Create this storage location in Synapse. Storage locations are immutable;
        this always creates a new one. If a storage location with identical properties
        already exists for this user, the existing one is returned (idempotent).

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The StorageLocation object with server-assigned fields populated.

        Raises:
            ValueError: If `storage_type` is not set.

        Example: Creating an external S3 storage location
            Create a storage location backed by your own S3 bucket:

                from synapseclient.models import StorageLocation, StorageLocationType

                import synapseclient
                synapseclient.login()

                storage = StorageLocation(
                    storage_type=StorageLocationType.EXTERNAL_S3,
                    bucket="my-external-synapse-bucket",
                    base_key="path/within/bucket",
                ).store()

                print(f"Storage location ID: {storage.storage_location_id}")
        """
        return self

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "StorageLocation":
        """Retrieve this storage location from Synapse by its ID. Only the creator of
        a StorageLocationSetting can retrieve it by its id.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The StorageLocation object populated with data from Synapse.

        Raises:
            ValueError: If `storage_location_id` is not set.

        Example: Retrieving a storage location
            Retrieve a storage location by ID:

                from synapseclient.models import StorageLocation

                import synapseclient
                synapseclient.login()

                storage = StorageLocation(storage_location_id=12345).get()
                print(f"Type: {storage.storage_type}, Bucket: {storage.bucket}")
        """
        return self

    @classmethod
    def setup_s3(
        cls,
        *,
        parent: str,
        folder_name: Optional[str] = None,
        folder: Optional["Folder"] = None,
        bucket_name: Optional[str] = None,
        base_key: Optional[str] = None,
        sts_enabled: bool = False,
        synapse_client: Optional[Synapse] = None,
    ) -> Tuple["Folder", "StorageLocation"]:
        """Convenience method to create a folder backed by S3 storage. This will:

        1. Create or retrieve the folder
        2. Create the storage location setting
        3. Apply the storage location to the folder via project settings

        Arguments:
            parent: The parent project or folder ID (e.g., "syn123").
            folder_name: Name for a new folder. Either `folder_name` or `folder`
                must be provided.
            folder: An existing Folder object or Synapse ID. Either `folder_name`
                or `folder` must be provided.
            bucket_name: The S3 bucket name. If None, uses Synapse default storage.
            base_key: The base key (prefix) within the bucket. Optional.
            sts_enabled: Whether to enable STS credentials for this storage location.
                Default: False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A tuple of (Folder, StorageLocation).

        Raises:
            ValueError: If neither `folder_name` nor `folder` is provided, or if both
                are provided.

        Example: Creating an STS-enabled folder with external S3 storage
            Create a folder with STS-enabled storage:

                from synapseclient.models import StorageLocation

                import synapseclient
                synapseclient.login()

                folder, storage = StorageLocation.setup_s3(
                    folder_name="my-sts-folder",
                    parent="syn123",
                    bucket_name="my-external-synapse-bucket",
                    base_key="path/within/bucket",
                    sts_enabled=True,
                )
                print(f"Folder: {folder.id}, Storage: {storage.storage_location_id}")

        Example: Using an existing folder
            Apply S3 storage to an existing folder:

                from synapseclient.models import StorageLocation, Folder

                import synapseclient
                synapseclient.login()

                existing_folder = Folder(id="syn456").get()
                folder, storage = StorageLocation.setup_s3(
                    folder=existing_folder,
                    bucket_name="my-bucket",
                )
        """
        return None
