"""StorageLocation model for managing storage location settings in Synapse."""

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from synapseclient import Synapse
from synapseclient.api.storage_location_services import (
    create_storage_location_setting,
    get_storage_location_setting,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.models.protocols.storage_location_protocol import (
    StorageLocationSynchronousProtocol,
)

if TYPE_CHECKING:
    from synapseclient.models import Folder


class StorageLocationType(str, Enum):
    """Enumeration of storage location types supported by Synapse.

    Each type maps to a specific concreteType suffix in the REST API.

    Attributes:
        SYNAPSE_S3: Synapse-managed S3 storage (default).
        EXTERNAL_S3: User-owned Amazon S3 bucket accessed by Synapse.
        EXTERNAL_GOOGLE_CLOUD: User-owned Google Cloud Storage bucket.
        EXTERNAL_SFTP: External SFTP server not accessed by Synapse.
        EXTERNAL_OBJECT_STORE: S3-like bucket (e.g., AWS S3 or OpenStack) not
            accessed by Synapse.
        PROXY: A proxy server that controls access to storage.
    """

    SYNAPSE_S3 = "S3StorageLocationSetting"
    EXTERNAL_S3 = "ExternalS3StorageLocationSetting"
    EXTERNAL_GOOGLE_CLOUD = "ExternalGoogleCloudStorageLocationSetting"
    EXTERNAL_SFTP = "ExternalStorageLocationSetting"
    EXTERNAL_OBJECT_STORE = "ExternalObjectStorageLocationSetting"
    PROXY = "ProxyStorageLocationSettings"


class UploadType(str, Enum):
    """Enumeration of upload types for storage locations.

    Attributes:
        S3: Amazon S3 compatible upload.
        GOOGLE_CLOUD_STORAGE: Google Cloud Storage upload.
        SFTP: SFTP upload.
        HTTPS: HTTPS upload (typically used with proxy storage).
        NONE: No upload type specified.
    """

    S3 = "S3"
    GOOGLE_CLOUD_STORAGE = "GOOGLECLOUDSTORAGE"
    SFTP = "SFTP"
    HTTPS = "HTTPS"
    NONE = "NONE"


# Mapping from StorageLocationType to default UploadType
_STORAGE_TYPE_TO_UPLOAD_TYPE: Dict[StorageLocationType, UploadType] = {
    StorageLocationType.SYNAPSE_S3: UploadType.S3,
    StorageLocationType.EXTERNAL_S3: UploadType.S3,
    StorageLocationType.EXTERNAL_GOOGLE_CLOUD: UploadType.GOOGLE_CLOUD_STORAGE,
    StorageLocationType.EXTERNAL_SFTP: UploadType.SFTP,
    StorageLocationType.EXTERNAL_OBJECT_STORE: UploadType.S3,
    StorageLocationType.PROXY: UploadType.HTTPS,
}

# Mapping from concreteType suffix to StorageLocationType
_CONCRETE_TYPE_TO_STORAGE_TYPE: Dict[str, StorageLocationType] = {
    storage_type.value: storage_type for storage_type in StorageLocationType
}


@dataclass()
@async_to_sync
class StorageLocation(StorageLocationSynchronousProtocol):
    """A storage location setting describes where files are uploaded to and
    downloaded from via Synapse. Storage location settings may be created for
    external locations, such as user-owned Amazon S3 buckets, Google Cloud
    Storage buckets, SFTP servers, or proxy storage.

    Attributes:
        storage_location_id: (Read Only) The unique ID for this storage location,
            assigned by the server on creation.
        storage_type: The type of storage location. Required when creating a new
            storage location via `store()`. Determines the `concreteType` sent to
            the Synapse REST API.
        banner: The banner text to display to a user every time a file is uploaded.
            This field is optional.
        description: A description of the storage location. This description is
            shown when a user has to choose which upload destination to use.

    Attributes:
        bucket: The name of the S3 or Google Cloud Storage bucket. Applicable to
            SYNAPSE_S3, EXTERNAL_S3, EXTERNAL_GOOGLE_CLOUD, and
            EXTERNAL_OBJECT_STORE types.
        base_key: The optional base key (prefix/folder) within the bucket.
            Applicable to SYNAPSE_S3, EXTERNAL_S3, and EXTERNAL_GOOGLE_CLOUD types.
        sts_enabled: Whether STS (AWS Security Token Service) is enabled on this
            storage location. Applicable to SYNAPSE_S3 and EXTERNAL_S3 types.
        endpoint_url: The endpoint URL of the S3 service. Applicable to
            EXTERNAL_S3 (default: https://s3.amazonaws.com) and
            EXTERNAL_OBJECT_STORE types.

    Attributes:
        url: The base URL for uploading to the external destination. Applicable to
            EXTERNAL_SFTP type.
        supports_subfolders: Whether the destination supports creating subfolders
            under the base url. Applicable to EXTERNAL_SFTP type. Default: False.

    Attributes:
        proxy_url: The HTTPS URL of the proxy used for upload and download.
            Applicable to PROXY type.
        secret_key: The encryption key used to sign all pre-signed URLs used to
            communicate with the proxy. Applicable to PROXY type.
        benefactor_id: An Entity ID (such as a Project ID). When set, any user with
            the 'create' permission on the given benefactorId will be allowed to
            create ProxyFileHandle using its storage location ID. Applicable to
            PROXY type.

    Attributes:
        upload_type: (Read Only) The upload type for this storage location.
            Automatically derived from `storage_type`.
        etag: (Read Only) Synapse employs an Optimistic Concurrency Control (OCC)
            scheme. The E-Tag changes every time the setting is updated.
        created_on: (Read Only) The date this storage location setting was created.
        created_by: (Read Only) The ID of the user that created this storage
            location setting.

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

    Example: Creating an STS-enabled S3 storage location with a folder
        Use the convenience classmethod to create a folder with STS-enabled
        storage:

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

    Example: Creating a Google Cloud storage location
        Create a storage location backed by your own GCS bucket:

            from synapseclient.models import StorageLocation, StorageLocationType

            import synapseclient
            synapseclient.login()

            storage = StorageLocation(
                storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                bucket="my-gcs-bucket",
                base_key="path/within/bucket",
            ).store()
    """

    # Core fields - present on all storage locations
    storage_location_id: Optional[int] = None
    """(Read Only) The unique ID for this storage location, assigned by the server
    on creation."""

    storage_type: Optional[StorageLocationType] = None
    """The type of storage location. Required when creating a new storage location
    via `store()`. Determines the `concreteType` sent to the Synapse REST API."""

    banner: Optional[str] = None
    """The banner text to display to a user every time a file is uploaded."""

    description: Optional[str] = None
    """A description of the storage location. This description is shown when a user
    has to choose which upload destination to use."""

    # S3/GCS specific fields
    bucket: Optional[str] = None
    """The name of the S3 or Google Cloud Storage bucket. Applicable to SYNAPSE_S3,
    EXTERNAL_S3, EXTERNAL_GOOGLE_CLOUD, and EXTERNAL_OBJECT_STORE types."""

    base_key: Optional[str] = None
    """The optional base key (prefix/folder) within the bucket. Applicable to
    SYNAPSE_S3, EXTERNAL_S3, and EXTERNAL_GOOGLE_CLOUD types."""

    sts_enabled: Optional[bool] = None
    """Whether STS (AWS Security Token Service) is enabled on this storage location.
    Applicable to SYNAPSE_S3 and EXTERNAL_S3 types."""

    endpoint_url: Optional[str] = None
    """The endpoint URL of the S3 service. Applicable to EXTERNAL_S3
    (default: https://s3.amazonaws.com) and EXTERNAL_OBJECT_STORE types."""

    # SFTP specific fields
    url: Optional[str] = None
    """The base URL for uploading to the external destination. Applicable to
    EXTERNAL_SFTP type."""

    supports_subfolders: Optional[bool] = None
    """Whether the destination supports creating subfolders under the base url.
    Applicable to EXTERNAL_SFTP type. Default: False."""

    # Proxy specific fields
    proxy_url: Optional[str] = None
    """The HTTPS URL of the proxy used for upload and download. Applicable to
    PROXY type."""

    secret_key: Optional[str] = None
    """The encryption key used to sign all pre-signed URLs used to communicate
    with the proxy. Applicable to PROXY type."""

    benefactor_id: Optional[str] = None
    """An Entity ID (such as a Project ID). When set, any user with the 'create'
    permission on the given benefactorId will be allowed to create ProxyFileHandle
    using its storage location ID. Applicable to PROXY type."""

    # Read-only fields
    upload_type: Optional[UploadType] = field(default=None, repr=False, compare=False)
    """(Read Only) The upload type for this storage location. Automatically derived
    from `storage_type`."""

    etag: Optional[str] = field(default=None, compare=False)
    """(Read Only) Synapse employs an Optimistic Concurrency Control (OCC) scheme.
    The E-Tag changes every time the setting is updated."""

    created_on: Optional[str] = field(default=None, compare=False)
    """(Read Only) The date this storage location setting was created."""

    created_by: Optional[int] = field(default=None, compare=False)
    """(Read Only) The ID of the user that created this storage location setting."""

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "StorageLocation":
        """Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The StorageLocation object.
        """
        self.storage_location_id = synapse_response.get("storageLocationId", None)
        self.banner = synapse_response.get("banner", None)
        self.description = synapse_response.get("description", None)
        self.etag = synapse_response.get("etag", None)
        self.created_on = synapse_response.get("createdOn", None)
        self.created_by = synapse_response.get("createdBy", None)

        # Parse upload type
        upload_type_str = synapse_response.get("uploadType", None)
        if upload_type_str:
            try:
                self.upload_type = UploadType(upload_type_str)
            except ValueError:
                self.upload_type = None

        # Parse storage type from concreteType
        concrete_type = synapse_response.get("concreteType", "")
        if concrete_type:
            # Extract the suffix after the last dot
            type_suffix = concrete_type.split(".")[-1] if "." in concrete_type else ""
            if type_suffix in _CONCRETE_TYPE_TO_STORAGE_TYPE:
                self.storage_type = _CONCRETE_TYPE_TO_STORAGE_TYPE[type_suffix]

        # S3/GCS fields
        self.bucket = synapse_response.get("bucket", None)
        self.base_key = synapse_response.get("baseKey", None)
        self.sts_enabled = synapse_response.get("stsEnabled", None)
        self.endpoint_url = synapse_response.get("endpointUrl", None)

        # SFTP fields
        self.url = synapse_response.get("url", None)
        self.supports_subfolders = synapse_response.get("supportsSubfolders", None)

        # Proxy fields
        self.proxy_url = synapse_response.get("proxyUrl", None)
        self.secret_key = synapse_response.get("secretKey", None)
        self.benefactor_id = synapse_response.get("benefactorId", None)

        return self

    def _to_synapse_request(self) -> Dict[str, Any]:
        """Convert this dataclass to a request body for the REST API.

        Returns:
            A dictionary suitable for the REST API.
        """
        if not self.storage_type:
            raise ValueError(
                "storage_type is required when creating a storage location"
            )

        # Build the concrete type
        concrete_type = (
            f"org.sagebionetworks.repo.model.project.{self.storage_type.value}"
        )

        # Determine upload type
        upload_type = self.upload_type or _STORAGE_TYPE_TO_UPLOAD_TYPE.get(
            self.storage_type, UploadType.S3
        )

        body: Dict[str, Any] = {
            "concreteType": concrete_type,
            "uploadType": upload_type.value,
        }

        # Add optional common fields
        if self.banner is not None:
            body["banner"] = self.banner
        if self.description is not None:
            body["description"] = self.description

        # Add type-specific fields
        if self.storage_type in (
            StorageLocationType.SYNAPSE_S3,
            StorageLocationType.EXTERNAL_S3,
            StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
            StorageLocationType.EXTERNAL_OBJECT_STORE,
        ):
            if self.bucket is not None:
                body["bucket"] = self.bucket
            if self.base_key is not None:
                body["baseKey"] = self.base_key

        if self.storage_type in (
            StorageLocationType.SYNAPSE_S3,
            StorageLocationType.EXTERNAL_S3,
        ):
            if self.sts_enabled is not None:
                body["stsEnabled"] = self.sts_enabled

        if self.storage_type in (
            StorageLocationType.EXTERNAL_S3,
            StorageLocationType.EXTERNAL_OBJECT_STORE,
        ):
            if self.endpoint_url is not None:
                body["endpointUrl"] = self.endpoint_url

        if self.storage_type == StorageLocationType.EXTERNAL_SFTP:
            if self.url is not None:
                body["url"] = self.url
            if self.supports_subfolders is not None:
                body["supportsSubfolders"] = self.supports_subfolders

        if self.storage_type == StorageLocationType.PROXY:
            if self.proxy_url is not None:
                body["proxyUrl"] = self.proxy_url
            if self.secret_key is not None:
                body["secretKey"] = self.secret_key
            if self.benefactor_id is not None:
                body["benefactorId"] = self.benefactor_id

        return body

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"StorageLocation_Store: {self.storage_type}"
    )
    async def store_async(
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

        Example: Using this function
            Create an external S3 storage location:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import StorageLocation, StorageLocationType

                syn = Synapse()
                syn.login()

                async def main():
                    storage = await StorageLocation(
                        storage_type=StorageLocationType.EXTERNAL_S3,
                        bucket="my-bucket",
                        base_key="my/prefix",
                    ).store_async()
                    print(f"Created storage location: {storage.storage_location_id}")

                asyncio.run(main())
        """
        body = self._to_synapse_request()
        response = await create_storage_location_setting(
            body=body,
            synapse_client=synapse_client,
        )
        self.fill_from_dict(response)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"StorageLocation_Get: {self.storage_location_id}"
    )
    async def get_async(
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

        Example: Using this function
            Retrieve a storage location by ID:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import StorageLocation

                syn = Synapse()
                syn.login()

                async def main():
                    storage = await StorageLocation(storage_location_id=12345).get_async()
                    print(f"Type: {storage.storage_type}, Bucket: {storage.bucket}")

                asyncio.run(main())
        """
        if not self.storage_location_id:
            raise ValueError(
                "storage_location_id is required to retrieve a storage location"
            )

        response = await get_storage_location_setting(
            storage_location_id=self.storage_location_id,
            synapse_client=synapse_client,
        )
        self.fill_from_dict(response)
        return self

    @classmethod
    async def setup_s3_async(
        cls,
        *,
        parent: str,
        folder_name: Optional[str] = None,
        folder: Optional[Union["Folder", str]] = None,
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

        Example: Using this function
            Create an STS-enabled folder with external S3 storage:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import StorageLocation

                syn = Synapse()
                syn.login()

                async def main():
                    folder, storage = await StorageLocation.setup_s3_async(
                        folder_name="my-sts-folder",
                        parent="syn123",
                        bucket_name="my-external-synapse-bucket",
                        base_key="path/within/bucket",
                        sts_enabled=True,
                    )
                    print(f"Folder: {folder.id}, Storage: {storage.storage_location_id}")

                asyncio.run(main())

        Example: Using existing folder
            Apply S3 storage to an existing folder:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import StorageLocation

                syn = Synapse()
                syn.login()

                async def main():
                    folder, storage = await StorageLocation.setup_s3_async(
                        folder="syn456",
                        bucket_name="my-bucket",
                    )

                asyncio.run(main())
        """
        # Import here to avoid circular imports
        from synapseclient.models import Folder as FolderModel

        # Validate parameters
        if folder_name and folder:
            raise ValueError(
                "folder and folder_name are mutually exclusive, only one should be passed"
            )
        if not folder_name and not folder:
            raise ValueError("Either folder or folder_name is required")

        # Create or get the folder
        if folder_name:
            target_folder = await FolderModel(
                name=folder_name, parent_id=parent
            ).store_async(synapse_client=synapse_client)
        elif isinstance(folder, str):
            target_folder = await FolderModel(id=folder).get_async(
                synapse_client=synapse_client
            )
        else:
            target_folder = folder

        # Determine storage type
        if bucket_name:
            storage_type = StorageLocationType.EXTERNAL_S3
        else:
            storage_type = StorageLocationType.SYNAPSE_S3

        # Create the storage location
        storage_location = await cls(
            storage_type=storage_type,
            bucket=bucket_name,
            base_key=base_key,
            sts_enabled=sts_enabled,
        ).store_async(synapse_client=synapse_client)

        # Apply the storage location to the folder
        await target_folder.set_storage_location_async(
            storage_location_id=storage_location.storage_location_id,
            synapse_client=synapse_client,
        )

        return target_folder, storage_location
