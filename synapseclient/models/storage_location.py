"""StorageLocation model for managing storage location settings in Synapse."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

from synapseclient import Synapse
from synapseclient.api.storage_location_services import (
    create_storage_location_setting,
    get_storage_location_setting,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.models.mixins.enum_coercion import EnumCoercionMixin
from synapseclient.models.protocols.storage_location_protocol import (
    StorageLocationSynchronousProtocol,
)


@dataclass(frozen=True)
class StorageLocationType:
    """Describes a Synapse storage location type.

    Each instance is a distinct object identified by its ``name``, so SFTP and
    HTTPS remain separate even though they share the same backend
    ``concreteType`` (``ExternalStorageLocationSetting``).

    Attributes:
        name: Human-readable identifier (e.g. ``"EXTERNAL_SFTP"``).
        concrete_type: The ``concreteType`` suffix sent to the Synapse REST API.
    """

    name: str
    concrete_type: str = field(repr=False)


StorageLocationType.SYNAPSE_S3 = StorageLocationType(
    "SYNAPSE_S3", "S3StorageLocationSetting"
)
StorageLocationType.EXTERNAL_S3 = StorageLocationType(
    "EXTERNAL_S3", "ExternalS3StorageLocationSetting"
)
StorageLocationType.EXTERNAL_GOOGLE_CLOUD = StorageLocationType(
    "EXTERNAL_GOOGLE_CLOUD", "ExternalGoogleCloudStorageLocationSetting"
)
StorageLocationType.EXTERNAL_SFTP = StorageLocationType(
    "EXTERNAL_SFTP", "ExternalStorageLocationSetting"
)
StorageLocationType.EXTERNAL_HTTPS = StorageLocationType(
    "EXTERNAL_HTTPS", "ExternalStorageLocationSetting"
)
StorageLocationType.EXTERNAL_OBJECT_STORE = StorageLocationType(
    "EXTERNAL_OBJECT_STORE", "ExternalObjectStorageLocationSetting"
)
StorageLocationType.PROXY = StorageLocationType("PROXY", "ProxyStorageLocationSettings")


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
    PROXYLOCAL = "PROXYLOCAL"
    NONE = "NONE"


# Mapping from StorageLocationType to default UploadType
_STORAGE_TYPE_TO_UPLOAD_TYPE: Dict[StorageLocationType, UploadType] = {
    StorageLocationType.SYNAPSE_S3: UploadType.S3,
    StorageLocationType.EXTERNAL_S3: UploadType.S3,
    StorageLocationType.EXTERNAL_GOOGLE_CLOUD: UploadType.GOOGLE_CLOUD_STORAGE,
    StorageLocationType.EXTERNAL_SFTP: UploadType.SFTP,
    StorageLocationType.EXTERNAL_HTTPS: UploadType.HTTPS,
    StorageLocationType.EXTERNAL_OBJECT_STORE: UploadType.S3,
    StorageLocationType.PROXY: UploadType.PROXYLOCAL,
}

# Mapping from (concreteType suffix, uploadType value) -> StorageLocationType.
# The tuple key is required because EXTERNAL_SFTP and EXTERNAL_HTTPS share the
# same concreteType and are disambiguated by uploadType.
_CONCRETE_UPLOAD_TO_STORAGE_TYPE: Dict[tuple, StorageLocationType] = {
    (storage_type.concrete_type, upload_type.value): storage_type
    for storage_type, upload_type in _STORAGE_TYPE_TO_UPLOAD_TYPE.items()
}

# Mapping from StorageLocationType to its type-specific (field_name, api_key) pairs.
# Only fields listed here are populated by fill_from_dict for a given type.
_STORAGE_TYPE_SPECIFIC_FIELDS: Dict[StorageLocationType, Dict[str, str]] = {
    StorageLocationType.SYNAPSE_S3: {
        "base_key": "baseKey",
        "sts_enabled": "stsEnabled",
    },
    StorageLocationType.EXTERNAL_S3: {
        "bucket": "bucket",
        "base_key": "baseKey",
        "sts_enabled": "stsEnabled",
        "endpoint_url": "endpointUrl",
    },
    StorageLocationType.EXTERNAL_GOOGLE_CLOUD: {
        "bucket": "bucket",
        "base_key": "baseKey",
    },
    StorageLocationType.EXTERNAL_OBJECT_STORE: {
        "bucket": "bucket",
        "endpoint_url": "endpointUrl",
    },
    StorageLocationType.EXTERNAL_SFTP: {
        "url": "url",
        "supports_subfolders": "supportsSubfolders",
    },
    StorageLocationType.EXTERNAL_HTTPS: {
        "url": "url",
        "supports_subfolders": "supportsSubfolders",
    },
    StorageLocationType.PROXY: {
        "proxy_url": "proxyUrl",
        "secret_key": "secretKey",
        "benefactor_id": "benefactorId",
    },
}

# Subset of _STORAGE_TYPE_SPECIFIC_FIELDS that are required (no default value).
# Fields with defaults (e.g. base_key=None, sts_enabled=False) are omitted.
_REQUIRED_STORAGE_TYPE_SPECIFIC_FIELDS: Dict[str, set] = {
    StorageLocationType.EXTERNAL_S3: {"bucket", "endpoint_url"},
    StorageLocationType.EXTERNAL_GOOGLE_CLOUD: {"bucket"},
    StorageLocationType.EXTERNAL_OBJECT_STORE: {"bucket", "endpoint_url"},
    StorageLocationType.EXTERNAL_SFTP: {"url"},
    StorageLocationType.EXTERNAL_HTTPS: {"url"},
    StorageLocationType.PROXY: {"proxy_url", "secret_key"},
}


@dataclass()
@async_to_sync
class StorageLocation(EnumCoercionMixin, StorageLocationSynchronousProtocol):
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

    _ENUM_FIELDS = {
        "upload_type": UploadType,
    }

    # REQUIRED fields
    _REQUIRED_FIELDS = {
        StorageLocationType.EXTERNAL_S3: {"bucket"},
        StorageLocationType.EXTERNAL_GOOGLE_CLOUD: {"bucket"},
        StorageLocationType.EXTERNAL_OBJECT_STORE: {"bucket", "endpoint_url"},
        StorageLocationType.EXTERNAL_SFTP: {"url"},
        StorageLocationType.EXTERNAL_HTTPS: {"url"},
        StorageLocationType.PROXY: {"proxy_url", "secret_key", "benefactor_id"},
    }
    # Core fields - present on all storage locations
    storage_location_id: Optional[int] = None
    """(Read Only) The unique ID for this storage location, assigned by the server
    on creation."""

    storage_type: Optional[StorageLocationType] = None
    """The type of storage location. Required when creating a new storage location
    via `store()`. Determines the `concreteType` sent to the Synapse REST API."""

    concrete_type: Optional[str] = field(default=None, compare=False)
    """The concrete type of the storage location indicating which implementation this object represents. """

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

    sts_enabled: Optional[bool] = False
    """Whether STS (AWS Security Token Service) is enabled on this storage location.
    Applicable to SYNAPSE_S3 and EXTERNAL_S3 types. Default: False."""

    endpoint_url: Optional[str] = "https://s3.amazonaws.com"
    """The endpoint URL of the S3 service. Applicable to EXTERNAL_S3
    (default: https://s3.amazonaws.com) and EXTERNAL_OBJECT_STORE types."""

    # SFTP specific fields
    url: Optional[str] = None
    """The base URL for uploading to the external destination. Applicable to
    EXTERNAL_SFTP type."""

    supports_subfolders: Optional[bool] = False
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
    upload_type: Optional[UploadType] = field(default=None, compare=False)
    """(Read Only) The upload type for this storage location. Automatically derived
    from `storage_type`."""

    etag: Optional[str] = field(default=None, compare=False)
    """(Read Only) Synapse employs an Optimistic Concurrency Control (OCC) scheme.
    The E-Tag changes every time the setting is updated."""

    created_on: Optional[str] = field(default=None, compare=False)
    """(Read Only) The date this storage location setting was created."""

    created_by: Optional[int] = field(default=None, compare=False)
    """(Read Only) The ID of the user that created this storage location setting."""

    def __repr__(self) -> str:
        common = {
            "concrete_type": self.concrete_type,
            "storage_location_id": self.storage_location_id,
            "storage_type": self.storage_type,
            "upload_type": self.upload_type,
            "banner": self.banner,
            "description": self.description,
            "etag": self.etag,
            "created_on": self.created_on,
            "created_by": self.created_by,
        }
        type_specific = {
            field_name: getattr(self, field_name)
            for field_name in _STORAGE_TYPE_SPECIFIC_FIELDS.get(self.storage_type, {})
        }
        parts = [f"{k}={v!r}" for k, v in {**common, **type_specific}.items()]
        return f"StorageLocation({', '.join(parts)})"

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "StorageLocation":
        """Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The StorageLocation object.
        """
        self.storage_location_id = synapse_response.get("storageLocationId", None)
        self.banner = (
            synapse_response.get("banner", None)
            if synapse_response.get("banner", None) is not None
            else None
        )
        self.description = (
            synapse_response.get("description", None)
            if synapse_response.get("description", None) is not None
            else None
        )
        self.etag = (
            synapse_response.get("etag", None)
            if synapse_response.get("etag", None) is not None
            else None
        )
        self.created_on = (
            synapse_response.get("createdOn", None)
            if synapse_response.get("createdOn", None) is not None
            else None
        )
        self.created_by = (
            synapse_response.get("createdBy", None)
            if synapse_response.get("createdBy", None) is not None
            else None
        )

        self.upload_type = (
            synapse_response.get("uploadType", None)
            if synapse_response.get("uploadType", None) is not None
            else None
        )

        # Parse storage type from concreteType + uploadType.
        # Both are needed to distinguish EXTERNAL_SFTP from EXTERNAL_HTTPS.
        self.concrete_type = (
            synapse_response.get("concreteType", "")
            if synapse_response.get("concreteType", "") is not None
            else None
        )
        if self.concrete_type:
            type_suffix = (
                self.concrete_type.split(".")[-1] if "." in self.concrete_type else ""
            )
            key = (type_suffix, self.upload_type)
            if key in _CONCRETE_UPLOAD_TO_STORAGE_TYPE:
                self.storage_type = _CONCRETE_UPLOAD_TO_STORAGE_TYPE[key]
        # Type-specific fields — only populate attributes relevant to this storage type
        if self.storage_type:
            for field_name, api_key in _STORAGE_TYPE_SPECIFIC_FIELDS.get(
                self.storage_type, {}
            ).items():
                setattr(self, field_name, synapse_response.get(api_key, None))
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
            f"org.sagebionetworks.repo.model.project.{self.storage_type.concrete_type}"
        )
        # Determine upload type
        upload_type = self.upload_type or _STORAGE_TYPE_TO_UPLOAD_TYPE.get(
            self.storage_type
        )

        body: Dict[str, Any] = {
            "concreteType": concrete_type,
            "uploadType": upload_type.value,
        }

        # Add optional common fields
        body["banner"] = self.banner if self.banner is not None else None
        body["description"] = self.description if self.description is not None else None
        # Add type-specific fields using the same mapping used by fill_from_dict
        for field_name, api_key in _STORAGE_TYPE_SPECIFIC_FIELDS.get(
            self.storage_type, {}
        ).items():
            value = getattr(self, field_name, None)
            if value is not None:
                body[api_key] = value
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
        # check if the attributes without default values for a specific storage type are present
        for field_name in self._REQUIRED_FIELDS.get(self.storage_type, {}):
            if getattr(self, field_name, None) is None:
                raise ValueError(
                    f"missing the '{field_name}' attribute for {self.storage_type}"
                )
        request = self._to_synapse_request()
        response = await create_storage_location_setting(
            request=request,
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
