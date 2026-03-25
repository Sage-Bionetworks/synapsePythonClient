"""Unit tests for the synapseclient.models.StorageLocation class."""

from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models.storage_location import (
    _CONCRETE_UPLOAD_TO_STORAGE_TYPE,
    _STORAGE_TYPE_TO_UPLOAD_TYPE,
    StorageLocation,
    StorageLocationType,
    UploadType,
)


def test_storage_location_type_concrete_type_values():
    """Test that StorageLocationType instances have the correct concrete_type values."""
    assert StorageLocationType.SYNAPSE_S3.concrete_type == "S3StorageLocationSetting"
    assert (
        StorageLocationType.EXTERNAL_S3.concrete_type
        == "ExternalS3StorageLocationSetting"
    )
    assert (
        StorageLocationType.EXTERNAL_GOOGLE_CLOUD.concrete_type
        == "ExternalGoogleCloudStorageLocationSetting"
    )
    assert (
        StorageLocationType.EXTERNAL_SFTP.concrete_type
        == "ExternalStorageLocationSetting"
    )
    assert (
        StorageLocationType.EXTERNAL_HTTPS.concrete_type
        == "ExternalStorageLocationSetting"
    )
    assert StorageLocationType.EXTERNAL_SFTP is not StorageLocationType.EXTERNAL_HTTPS
    assert (
        StorageLocationType.EXTERNAL_OBJECT_STORE.concrete_type
        == "ExternalObjectStorageLocationSetting"
    )
    assert StorageLocationType.PROXY.concrete_type == "ProxyStorageLocationSettings"


def test_upload_type_enum_values():
    """Test that UploadType enum has correct values."""
    assert UploadType.S3.value == "S3"
    assert UploadType.GOOGLE_CLOUD_STORAGE.value == "GOOGLECLOUDSTORAGE"
    assert UploadType.SFTP.value == "SFTP"
    assert UploadType.HTTPS.value == "HTTPS"
    assert UploadType.PROXYLOCAL.value == "PROXYLOCAL"
    assert UploadType.NONE.value == "NONE"


def test_storage_location_type_to_upload_type_mapping():
    """Test that StorageLocationType to UploadType mapping is correct."""
    assert _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.SYNAPSE_S3] == UploadType.S3
    assert (
        _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.EXTERNAL_S3] == UploadType.S3
    )
    assert (
        _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.EXTERNAL_GOOGLE_CLOUD]
        == UploadType.GOOGLE_CLOUD_STORAGE
    )
    assert (
        _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.EXTERNAL_SFTP]
        == UploadType.SFTP
    )
    assert (
        _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.EXTERNAL_HTTPS]
        == UploadType.HTTPS
    )
    assert (
        _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.EXTERNAL_OBJECT_STORE]
        == UploadType.S3
    )
    assert (
        _STORAGE_TYPE_TO_UPLOAD_TYPE[StorageLocationType.PROXY] == UploadType.PROXYLOCAL
    )


def test_concrete_upload_to_storage_type_mapping():
    """Test that concrete type to StorageLocationType mapping is correct."""
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[("S3StorageLocationSetting", "S3")]
        == StorageLocationType.SYNAPSE_S3
    )
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[("ExternalS3StorageLocationSetting", "S3")]
        == StorageLocationType.EXTERNAL_S3
    )
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[
            ("ExternalGoogleCloudStorageLocationSetting", "GOOGLECLOUDSTORAGE")
        ]
        == StorageLocationType.EXTERNAL_GOOGLE_CLOUD
    )
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[("ExternalStorageLocationSetting", "SFTP")]
        == StorageLocationType.EXTERNAL_SFTP
    )
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[("ExternalStorageLocationSetting", "HTTPS")]
        == StorageLocationType.EXTERNAL_HTTPS
    )
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[("ExternalObjectStorageLocationSetting", "S3")]
        == StorageLocationType.EXTERNAL_OBJECT_STORE
    )
    assert (
        _CONCRETE_UPLOAD_TO_STORAGE_TYPE[("ProxyStorageLocationSettings", "PROXYLOCAL")]
        == StorageLocationType.PROXY
    )


class TestStorageLocation:
    """Unit tests for basic StorageLocation model functionality."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            pytest.param(
                dict(storage_type=StorageLocationType.SYNAPSE_S3, sts_enabled=False),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.S3StorageLocationSetting",
                    "uploadType": "S3",
                    "banner": None,
                    "description": None,
                    "stsEnabled": False,
                },
                id="synapse_s3",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_S3,
                    bucket="my-bucket",
                    base_key="my/prefix",
                    sts_enabled=True,
                    banner="Upload banner",
                    description="Test storage location",
                ),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
                    "uploadType": "S3",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "bucket": "my-bucket",
                    "baseKey": "my/prefix",
                    "stsEnabled": True,
                    "endpointUrl": "https://s3.amazonaws.com",
                },
                id="external_s3",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                    bucket="my-gcs-bucket",
                    base_key="gcs/prefix",
                ),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
                    "uploadType": "GOOGLECLOUDSTORAGE",
                    "banner": None,
                    "description": None,
                    "bucket": "my-gcs-bucket",
                    "baseKey": "gcs/prefix",
                },
                id="external_google_cloud",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_SFTP,
                    url="sftp://example.com/path",
                    supports_subfolders=True,
                ),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "SFTP",
                    "banner": None,
                    "description": None,
                    "url": "sftp://example.com/path",
                    "supportsSubfolders": True,
                },
                id="external_sftp",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_HTTPS,
                    url="https://example.com/data",
                    supports_subfolders=False,
                ),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "HTTPS",
                    "banner": None,
                    "description": None,
                    "url": "https://example.com/data",
                    "supportsSubfolders": False,
                },
                id="external_https",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.PROXY,
                    proxy_url="https://proxy.example.com",
                    secret_key="my-secret-key",
                    benefactor_id="syn123",
                ),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
                    "uploadType": "PROXYLOCAL",
                    "banner": None,
                    "description": None,
                    "proxyUrl": "https://proxy.example.com",
                    "secretKey": "my-secret-key",
                    "benefactorId": "syn123",
                },
                id="proxy",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
                    bucket="my-s3-like-bucket",
                    endpoint_url="https://s3.custom.com",
                ),
                {
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
                    "uploadType": "S3",
                    "banner": None,
                    "description": None,
                    "bucket": "my-s3-like-bucket",
                    "endpointUrl": "https://s3.custom.com",
                },
                id="external_object_store",
            ),
        ],
    )
    def test_to_synapse_request(self, kwargs, expected):
        """Test generating a request body for each storage location type."""
        # GIVEN a storage location constructed with the given kwargs
        storage = StorageLocation(**kwargs)
        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should match the expected structure
        assert request_body == expected

    def test_to_synapse_request_missing_storage_type(self):
        """Test that _to_synapse_request raises ValueError when storage_type is missing."""
        # GIVEN a storage location without a storage_type
        storage = StorageLocation(
            bucket="my-bucket",
        )

        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="storage_type is required"):
            storage._to_synapse_request()

    @pytest.mark.parametrize(
        "response,expected_attrs",
        [
            pytest.param(
                {
                    "storageLocationId": 12345,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-bucket",
                    "baseKey": "my/prefix",
                    "stsEnabled": True,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                {
                    "storage_location_id": 12345,
                    "concrete_type": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
                    "storage_type": StorageLocationType.EXTERNAL_S3,
                    "upload_type": UploadType.S3,
                    "bucket": "my-bucket",
                    "base_key": "my/prefix",
                    "sts_enabled": True,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "created_on": "2024-01-01T00:00:00.000Z",
                    "created_by": 123456,
                },
                id="external_s3",
            ),
            pytest.param(
                {
                    "storageLocationId": 67890,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
                    "uploadType": "GOOGLECLOUDSTORAGE",
                    "bucket": "my-gcs-bucket",
                    "baseKey": "gcs/prefix",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                {
                    "storage_location_id": 67890,
                    "concrete_type": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
                    "storage_type": StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                    "upload_type": UploadType.GOOGLE_CLOUD_STORAGE,
                    "bucket": "my-gcs-bucket",
                    "base_key": "gcs/prefix",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "created_on": "2024-01-01T00:00:00.000Z",
                    "created_by": 123456,
                },
                id="external_google_cloud",
            ),
            pytest.param(
                {
                    "storageLocationId": 11111,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "SFTP",
                    "url": "sftp://example.com/path",
                    "supportsSubfolders": True,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                {
                    "storage_location_id": 11111,
                    "storage_type": StorageLocationType.EXTERNAL_SFTP,
                    "upload_type": UploadType.SFTP,
                    "url": "sftp://example.com/path",
                    "supports_subfolders": True,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "created_on": "2024-01-01T00:00:00.000Z",
                    "created_by": 123456,
                },
                id="external_sftp",
            ),
            pytest.param(
                {
                    "storageLocationId": 11112,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "HTTPS",
                    "url": "https://example.com/data",
                    "supportsSubfolders": False,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                {
                    "storage_location_id": 11112,
                    "storage_type": StorageLocationType.EXTERNAL_HTTPS,
                    "upload_type": UploadType.HTTPS,
                    "url": "https://example.com/data",
                    "supports_subfolders": False,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "created_on": "2024-01-01T00:00:00.000Z",
                    "created_by": 123456,
                },
                id="external_https",
            ),
            pytest.param(
                {
                    "storageLocationId": 22222,
                    "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
                    "uploadType": "PROXYLOCAL",
                    "proxyUrl": "https://proxy.example.com",
                    "secretKey": "my-secret-key",
                    "benefactorId": "syn123",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                {
                    "storage_location_id": 22222,
                    "storage_type": StorageLocationType.PROXY,
                    "upload_type": UploadType.PROXYLOCAL,
                    "proxy_url": "https://proxy.example.com",
                    "secret_key": "my-secret-key",
                    "benefactor_id": "syn123",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "created_on": "2024-01-01T00:00:00.000Z",
                    "created_by": 123456,
                },
                id="proxy",
            ),
            pytest.param(
                {
                    "storageLocationId": 33333,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-object-store-bucket",
                    "endpointUrl": "https://s3.custom.com",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                {
                    "storage_location_id": 33333,
                    "storage_type": StorageLocationType.EXTERNAL_OBJECT_STORE,
                    "upload_type": UploadType.S3,
                    "bucket": "my-object-store-bucket",
                    "endpoint_url": "https://s3.custom.com",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "created_on": "2024-01-01T00:00:00.000Z",
                    "created_by": 123456,
                },
                id="external_object_store",
            ),
        ],
    )
    def test_fill_from_dict(self, response, expected_attrs):
        """Test filling from a REST API response for each storage location type."""
        # GIVEN a storage location
        storage = StorageLocation()

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        for attr, value in expected_attrs.items():
            assert getattr(storage, attr) == value

    def test_fill_from_dict_type_isolation(self):
        """Test that fill_from_dict only populates fields relevant to the storage type."""
        # GIVEN an EXTERNAL_SFTP response (no S3 or proxy fields)
        sftp_response = {
            "storageLocationId": 44444,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
            "uploadType": "SFTP",
            "url": "sftp://example.com/path",
        }
        storage = StorageLocation()
        storage.fill_from_dict(sftp_response)

        # THEN S3/proxy fields are not populated
        assert storage.bucket is None
        assert storage.base_key is None
        assert storage.sts_enabled is False
        assert storage.endpoint_url == "https://s3.amazonaws.com"
        assert storage.proxy_url is None
        assert storage.secret_key is None

    def test_upload_type_enum_coercion_on_init(self):
        """Test that upload_type string values are coerced to UploadType via EnumCoercionMixin."""
        # GIVEN a StorageLocation constructed with a string value for upload_type
        # (upload_type is the only field in _ENUM_FIELDS; storage_type is not coerced)
        storage = StorageLocation(upload_type="S3")

        # THEN upload_type is coerced to the enum type
        assert storage.upload_type is UploadType.S3

    def test_upload_type_enum_coercion_on_setattr(self):
        """Test that assigning a string to upload_type coerces it to the enum type."""
        # GIVEN a StorageLocation
        storage = StorageLocation()

        # WHEN we assign a string value to upload_type
        storage.upload_type = "HTTPS"

        # THEN it is coerced to the enum type
        assert storage.upload_type is UploadType.HTTPS

    @pytest.mark.asyncio
    async def test_store_async_missing_storage_type(self):
        """Test that store_async raises ValueError when storage_type is missing."""
        # GIVEN a storage location without a storage_type
        storage = StorageLocation(bucket="my-bucket")

        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="storage_type is required when creating a storage location",
        ):
            await storage.store_async()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "storage_type,kwargs,missing_field",
        [
            (StorageLocationType.EXTERNAL_S3, {}, "bucket"),
            (StorageLocationType.EXTERNAL_GOOGLE_CLOUD, {}, "bucket"),
            (
                StorageLocationType.EXTERNAL_OBJECT_STORE,
                {"bucket": "12345", "endpoint_url": None},
                "endpoint_url",
            ),
            (StorageLocationType.EXTERNAL_SFTP, {}, "url"),
            (StorageLocationType.EXTERNAL_HTTPS, {}, "url"),
            (
                StorageLocationType.PROXY,
                {"secret_key": "key", "benefactor_id": "syn123"},
                "proxy_url",
            ),
            (
                StorageLocationType.PROXY,
                {"proxy_url": "https://proxy.example.com", "benefactor_id": "syn123"},
                "secret_key",
            ),
            (
                StorageLocationType.PROXY,
                {"proxy_url": "https://proxy.example.com", "secret_key": "key"},
                "benefactor_id",
            ),
        ],
    )
    async def test_store_async_missing_attributes(
        self, storage_type, kwargs, missing_field
    ):
        """Test that store_async raises ValueError when missing required attributes."""
        # GIVEN a storage location with missing required attributes
        storage = StorageLocation(storage_type=storage_type, **kwargs)

        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match=f"missing the '{missing_field}' attribute for {storage_type}",
        ):
            await storage.store_async(synapse_client=self.syn)

    @pytest.mark.parametrize(
        "kwargs,mock_response,expected_attrs",
        [
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_S3,
                    bucket="my-bucket",
                    base_key="my/prefix",
                ),
                {
                    "storageLocationId": 12345,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-bucket",
                    "baseKey": "my/prefix",
                    "stsEnabled": False,
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=12345,
                    storage_type=StorageLocationType.EXTERNAL_S3,
                    upload_type=UploadType.S3,
                    bucket="my-bucket",
                    base_key="my/prefix",
                    sts_enabled=False,
                    banner=None,
                    description=None,
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
                id="external_s3",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                    bucket="my-gcs-bucket",
                ),
                {
                    "storageLocationId": 67890,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
                    "uploadType": "GOOGLECLOUDSTORAGE",
                    "bucket": "my-gcs-bucket",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=67890,
                    storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                    upload_type=UploadType.GOOGLE_CLOUD_STORAGE,
                    bucket="my-gcs-bucket",
                ),
                id="external_google_cloud",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
                    bucket="my-object-store-bucket",
                    endpoint_url="https://s3.custom.com",
                ),
                {
                    "storageLocationId": 33333,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-object-store-bucket",
                    "endpointUrl": "https://s3.custom.com",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=33333,
                    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
                    upload_type=UploadType.S3,
                    bucket="my-object-store-bucket",
                    endpoint_url="https://s3.custom.com",
                    banner=None,
                    description=None,
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
                id="external_object_store",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_SFTP,
                    url="sftp://example.com/path",
                ),
                {
                    "storageLocationId": 11111,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "SFTP",
                    "url": "sftp://example.com/path",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=11111,
                    storage_type=StorageLocationType.EXTERNAL_SFTP,
                    upload_type=UploadType.SFTP,
                    url="sftp://example.com/path",
                ),
                id="external_sftp",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.EXTERNAL_HTTPS,
                    url="https://example.com/data",
                ),
                {
                    "storageLocationId": 11112,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "HTTPS",
                    "url": "https://example.com/data",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=11112,
                    storage_type=StorageLocationType.EXTERNAL_HTTPS,
                    upload_type=UploadType.HTTPS,
                    url="https://example.com/data",
                    banner=None,
                    description=None,
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
                id="external_https",
            ),
            pytest.param(
                dict(
                    storage_type=StorageLocationType.PROXY,
                    proxy_url="https://proxy.example.com",
                    secret_key="my-secret-key",
                    benefactor_id="syn123",
                ),
                {
                    "storageLocationId": 22222,
                    "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
                    "uploadType": "PROXYLOCAL",
                    "proxyUrl": "https://proxy.example.com",
                    "secretKey": "my-secret-key",
                    "benefactorId": "syn123",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=22222,
                    storage_type=StorageLocationType.PROXY,
                    upload_type=UploadType.PROXYLOCAL,
                    proxy_url="https://proxy.example.com",
                    secret_key="my-secret-key",
                    benefactor_id="syn123",
                    banner=None,
                    description=None,
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
                id="proxy",
            ),
        ],
    )
    async def test_store_async_successful_creation(
        self, kwargs, mock_response, expected_attrs
    ):
        """Test that store_async creates a storage location successfully for each storage type."""
        # GIVEN a storage location
        storage = StorageLocation(**kwargs)

        # WHEN we create the storage location
        with patch(
            "synapseclient.models.storage_location.create_storage_location_setting",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            await storage.store_async(synapse_client=self.syn)

        # THEN it should be populated from the mock response
        for attr, value in expected_attrs.items():
            assert getattr(storage, attr) == value

    @pytest.mark.asyncio
    async def test_get_async_missing_id(self):
        """Test that get_async raises ValueError when storage_location_id is missing."""
        # GIVEN a storage location without an ID
        storage = StorageLocation()

        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="storage_location_id is required to retrieve a storage location",
        ):
            await storage.get_async(synapse_client=self.syn)

    @pytest.mark.parametrize(
        "mock_response,expected_attrs",
        [
            (
                {
                    "storageLocationId": 12345,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-bucket",
                    "baseKey": "my/prefix",
                    "stsEnabled": False,
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=12345,
                    storage_type=StorageLocationType.EXTERNAL_S3,
                    upload_type=UploadType.S3,
                    bucket="my-bucket",
                    base_key="my/prefix",
                    sts_enabled=False,
                    banner=None,
                    description=None,
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
            (
                {
                    "storageLocationId": 67890,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
                    "uploadType": "GOOGLECLOUDSTORAGE",
                    "bucket": "my-gcs-bucket",
                    "baseKey": "gcs/prefix",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=67890,
                    storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                    upload_type=UploadType.GOOGLE_CLOUD_STORAGE,
                    bucket="my-gcs-bucket",
                    base_key="gcs/prefix",
                    banner="Upload banner",
                    description="Test storage location",
                ),
            ),
            (
                {
                    "storageLocationId": 33333,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-object-store-bucket",
                    "endpointUrl": "https://s3.custom.com",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=33333,
                    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
                    upload_type=UploadType.S3,
                    bucket="my-object-store-bucket",
                    endpoint_url="https://s3.custom.com",
                    banner="Upload banner",
                    description="Test storage location",
                ),
            ),
            (
                {
                    "storageLocationId": 12345,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-bucket",
                    "baseKey": "my/prefix",
                    "stsEnabled": False,
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=12345,
                    storage_type=StorageLocationType.EXTERNAL_S3,
                    upload_type=UploadType.S3,
                    bucket="my-bucket",
                    base_key="my/prefix",
                    sts_enabled=False,
                    banner=None,
                    description=None,
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
            (
                {
                    "storageLocationId": 67890,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
                    "uploadType": "GOOGLECLOUDSTORAGE",
                    "bucket": "my-gcs-bucket",
                    "baseKey": "gcs/prefix",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=67890,
                    storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                    upload_type=UploadType.GOOGLE_CLOUD_STORAGE,
                    bucket="my-gcs-bucket",
                    base_key="gcs/prefix",
                    banner="Upload banner",
                    description="Test storage location",
                ),
            ),
            (
                {
                    "storageLocationId": 33333,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-object-store-bucket",
                    "endpointUrl": "https://s3.custom.com",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=33333,
                    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
                    upload_type=UploadType.S3,
                    bucket="my-object-store-bucket",
                    endpoint_url="https://s3.custom.com",
                    banner="Upload banner",
                    description="Test storage location",
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
            (
                {
                    "storageLocationId": 11111,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "SFTP",
                    "url": "sftp://example.com/path",
                    "supportsSubfolders": True,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=11111,
                    storage_type=StorageLocationType.EXTERNAL_SFTP,
                    upload_type=UploadType.SFTP,
                    url="sftp://example.com/path",
                    supports_subfolders=True,
                    banner="Upload banner",
                    description="Test storage location",
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
            (
                {
                    "storageLocationId": 11112,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
                    "uploadType": "HTTPS",
                    "url": "https://example.com/data",
                    "supportsSubfolders": False,
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=11112,
                    storage_type=StorageLocationType.EXTERNAL_HTTPS,
                    upload_type=UploadType.HTTPS,
                    url="https://example.com/data",
                    supports_subfolders=False,
                    banner="Upload banner",
                    description="Test storage location",
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
            (
                {
                    "storageLocationId": 22222,
                    "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
                    "uploadType": "PROXYLOCAL",
                    "proxyUrl": "https://proxy.example.com",
                    "secretKey": "my-secret-key",
                    "benefactorId": "syn123",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=22222,
                    storage_type=StorageLocationType.PROXY,
                    upload_type=UploadType.PROXYLOCAL,
                    proxy_url="https://proxy.example.com",
                    secret_key="my-secret-key",
                    benefactor_id="syn123",
                    banner="Upload banner",
                    description="Test storage location",
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
            (
                {
                    "storageLocationId": 33333,
                    "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
                    "uploadType": "S3",
                    "bucket": "my-object-store-bucket",
                    "endpointUrl": "https://s3.custom.com",
                    "banner": "Upload banner",
                    "description": "Test storage location",
                    "etag": "abc123",
                    "createdOn": "2024-01-01T00:00:00.000Z",
                    "createdBy": 123456,
                },
                dict(
                    storage_location_id=33333,
                    storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
                    upload_type=UploadType.S3,
                    bucket="my-object-store-bucket",
                    endpoint_url="https://s3.custom.com",
                    banner="Upload banner",
                    description="Test storage location",
                    etag="abc123",
                    created_on="2024-01-01T00:00:00.000Z",
                    created_by=123456,
                ),
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_get_async_successful_retrieval(self, mock_response, expected_attrs):
        """Test that get_async retrieves a storage location successfully."""
        # GIVEN a storage location with an ID
        storage = StorageLocation(storage_location_id=12345)

        # WHEN we retrieve the storage location
        with patch(
            "synapseclient.models.storage_location.get_storage_location_setting",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            await storage.get_async(synapse_client=self.syn)

        # THEN it should be populated from the mock response
        for attr, value in expected_attrs.items():
            assert getattr(storage, attr) == value
