"""Protocol for the specific methods of StorageLocation that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
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
