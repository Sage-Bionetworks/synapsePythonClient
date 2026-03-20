"""Unit tests for the synapseclient.models.StorageLocation class."""

import pytest

from synapseclient.models import StorageLocation, StorageLocationType, UploadType


class TestStorageLocation:
    """Unit tests for basic StorageLocation model functionality."""

    def test_storage_location_type_concrete_type_values(self):
        """Test that StorageLocationType instances have the correct concrete_type values."""
        assert (
            StorageLocationType.SYNAPSE_S3.concrete_type == "S3StorageLocationSetting"
        )
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
        # EXTERNAL_SFTP and EXTERNAL_HTTPS share the same concreteType but are distinct objects
        assert (
            StorageLocationType.EXTERNAL_HTTPS.concrete_type
            == "ExternalStorageLocationSetting"
        )
        assert (
            StorageLocationType.EXTERNAL_SFTP is not StorageLocationType.EXTERNAL_HTTPS
        )
        assert (
            StorageLocationType.EXTERNAL_OBJECT_STORE.concrete_type
            == "ExternalObjectStorageLocationSetting"
        )
        assert StorageLocationType.PROXY.concrete_type == "ProxyStorageLocationSettings"

    def test_upload_type_enum_values(self):
        """Test that UploadType enum has correct values."""
        assert UploadType.S3.value == "S3"
        assert UploadType.GOOGLE_CLOUD_STORAGE.value == "GOOGLECLOUDSTORAGE"
        assert UploadType.SFTP.value == "SFTP"
        assert UploadType.HTTPS.value == "HTTPS"
        assert UploadType.PROXYLOCAL.value == "PROXYLOCAL"
        assert UploadType.NONE.value == "NONE"

    def test_to_synapse_request_external_s3(self):
        """Test generating a request body for EXTERNAL_S3 storage location."""
        # GIVEN an EXTERNAL_S3 storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.EXTERNAL_S3,
            bucket="my-bucket",
            base_key="my/prefix",
            sts_enabled=True,
            banner="Upload banner",
            description="Test storage location",
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the correct structure
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
            "uploadType": "S3",
            "banner": "Upload banner",
            "description": "Test storage location",
            "bucket": "my-bucket",
            "baseKey": "my/prefix",
            "stsEnabled": True,
        }

    def test_to_synapse_request_synapse_s3(self):
        """Test generating a request body for SYNAPSE_S3 storage location."""
        # GIVEN a SYNAPSE_S3 storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.SYNAPSE_S3,
            sts_enabled=False,
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the correct structure
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.S3StorageLocationSetting",
            "uploadType": "S3",
            "banner": None,
            "description": None,
            "stsEnabled": False,
        }

    def test_to_synapse_request_google_cloud(self):
        """Test generating a request body for EXTERNAL_GOOGLE_CLOUD storage location."""
        # GIVEN a EXTERNAL_GOOGLE_CLOUD storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
            bucket="my-gcs-bucket",
            base_key="gcs/prefix",
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the correct structure
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
            "uploadType": "GOOGLECLOUDSTORAGE",
            "banner": None,
            "description": None,
            "bucket": "my-gcs-bucket",
            "baseKey": "gcs/prefix",
        }

    def test_to_synapse_request_sftp(self):
        """Test generating a request body for EXTERNAL_SFTP storage location."""
        # GIVEN an EXTERNAL_SFTP storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.EXTERNAL_SFTP,
            url="sftp://example.com/path",
            supports_subfolders=True,
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the correct structure with SFTP uploadType.
        # EXTERNAL_SFTP and EXTERNAL_HTTPS are distinct objects with separate entries
        # in _STORAGE_TYPE_TO_UPLOAD_TYPE, so EXTERNAL_SFTP correctly maps to SFTP.
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
            "uploadType": "SFTP",
            "banner": None,
            "description": None,
            "url": "sftp://example.com/path",
            "supportsSubfolders": True,
        }

    def test_to_synapse_request_external_https(self):
        """Test generating a request body for EXTERNAL_HTTPS storage location."""
        # GIVEN an EXTERNAL_HTTPS storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.EXTERNAL_HTTPS,
            url="https://example.com/data",
            supports_subfolders=False,
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the same concreteType as EXTERNAL_SFTP but HTTPS uploadType
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
            "uploadType": "HTTPS",
            "banner": None,
            "description": None,
            "url": "https://example.com/data",
            "supportsSubfolders": False,
        }

    def test_to_synapse_request_proxy(self):
        """Test generating a request body for PROXY storage location."""
        # GIVEN a PROXY storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.PROXY,
            proxy_url="https://proxy.example.com",
            secret_key="my-secret-key",
            benefactor_id="syn123",
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the correct structure
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
            "uploadType": "PROXYLOCAL",
            "banner": None,
            "description": None,
            "proxyUrl": "https://proxy.example.com",
            "secretKey": "my-secret-key",
            "benefactorId": "syn123",
        }

    def test_to_synapse_request_external_object_store(self):
        """Test generating a request body for EXTERNAL_OBJECT_STORE storage location."""
        # GIVEN an EXTERNAL_OBJECT_STORE storage location
        storage = StorageLocation(
            storage_type=StorageLocationType.EXTERNAL_OBJECT_STORE,
            bucket="my-s3-like-bucket",
            endpoint_url="https://s3.custom.com",
        )

        # WHEN we generate a request body
        request_body = storage._to_synapse_request()

        # THEN it should have the correct structure
        assert request_body == {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
            "uploadType": "S3",
            "banner": None,
            "description": None,
            "bucket": "my-s3-like-bucket",
            "endpointUrl": "https://s3.custom.com",
        }

    def test_to_synapse_request_missing_storage_type(self):
        """Test that _to_synapse_request raises ValueError when storage_type is missing."""
        # GIVEN a storage location without a storage_type
        storage = StorageLocation(
            bucket="my-bucket",
        )

        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="storage_type is required"):
            storage._to_synapse_request()

    def test_fill_from_dict_external_s3(self):
        """Test filling from a REST API response for EXTERNAL_S3."""
        # GIVEN a storage location
        storage = StorageLocation()

        # AND a response from the REST API
        response = {
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
        }

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        assert storage.storage_location_id == 12345
        assert storage.storage_type == StorageLocationType.EXTERNAL_S3
        assert storage.upload_type == UploadType.S3
        assert storage.bucket == "my-bucket"
        assert storage.base_key == "my/prefix"
        assert storage.sts_enabled is True
        assert storage.banner == "Upload banner"
        assert storage.description == "Test storage location"
        assert storage.etag == "abc123"
        assert storage.created_on == "2024-01-01T00:00:00.000Z"
        assert storage.created_by == 123456

    def test_fill_from_dict_synapse_s3(self):
        """Test filling from a REST API response for SYNAPSE_S3."""
        # GIVEN a storage location
        storage = StorageLocation()

        # AND a response from the REST API
        response = {
            "storageLocationId": 1,
            "concreteType": "org.sagebionetworks.repo.model.project.S3StorageLocationSetting",
            "uploadType": "S3",
        }

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        assert storage.storage_location_id == 1
        assert storage.storage_type == StorageLocationType.SYNAPSE_S3

    def test_fill_from_dict_google_cloud(self):
        """Test filling from a REST API response for EXTERNAL_GOOGLE_CLOUD."""
        # GIVEN a storage location
        storage = StorageLocation()

        # AND a response from the REST API
        response = {
            "storageLocationId": 67890,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting",
            "uploadType": "GOOGLECLOUDSTORAGE",
            "bucket": "my-gcs-bucket",
        }

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        assert storage.storage_location_id == 67890
        assert storage.storage_type == StorageLocationType.EXTERNAL_GOOGLE_CLOUD
        assert storage.upload_type == UploadType.GOOGLE_CLOUD_STORAGE
        assert storage.bucket == "my-gcs-bucket"

    def test_fill_from_dict_sftp(self):
        """Test filling from a REST API response for EXTERNAL_SFTP."""
        # GIVEN a storage location
        storage = StorageLocation()

        # AND a response from the REST API
        response = {
            "storageLocationId": 11111,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
            "uploadType": "SFTP",
            "url": "sftp://example.com/path",
            "supportsSubfolders": True,
        }

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        assert storage.storage_location_id == 11111
        assert storage.storage_type == StorageLocationType.EXTERNAL_SFTP
        assert storage.upload_type == UploadType.SFTP
        assert storage.url == "sftp://example.com/path"
        assert storage.supports_subfolders is True

    def test_fill_from_dict_external_https(self):
        """Test filling from a REST API response for EXTERNAL_HTTPS."""
        storage = StorageLocation()

        response = {
            "storageLocationId": 11112,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting",
            "uploadType": "HTTPS",
            "url": "https://example.com/data",
            "supportsSubfolders": False,
        }

        storage.fill_from_dict(response)

        assert storage.storage_location_id == 11112
        assert storage.storage_type == StorageLocationType.EXTERNAL_HTTPS
        assert storage.upload_type == UploadType.HTTPS
        assert storage.url == "https://example.com/data"
        assert storage.supports_subfolders is False

    def test_fill_from_dict_proxy(self):
        """Test filling from a REST API response for PROXY."""
        # GIVEN a storage location
        storage = StorageLocation()

        # AND a response from the REST API
        response = {
            "storageLocationId": 22222,
            "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
            "uploadType": "PROXYLOCAL",
            "proxyUrl": "https://proxy.example.com",
            "secretKey": "my-secret-key",
            "benefactorId": "syn123",
        }

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        assert storage.storage_location_id == 22222
        assert storage.storage_type == StorageLocationType.PROXY
        assert storage.upload_type == UploadType.PROXYLOCAL
        assert storage.proxy_url == "https://proxy.example.com"
        assert storage.secret_key == "my-secret-key"
        assert storage.benefactor_id == "syn123"

    def test_fill_from_dict_external_object_store(self):
        """Test filling from a REST API response for EXTERNAL_OBJECT_STORE."""
        # GIVEN a storage location
        storage = StorageLocation()

        # AND a response from the REST API
        response = {
            "storageLocationId": 33333,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
            "uploadType": "S3",
            "bucket": "my-object-store-bucket",
            "endpointUrl": "https://s3.custom.com",
        }

        # WHEN we fill from the response
        storage.fill_from_dict(response)

        # THEN the storage location should be populated correctly
        assert storage.storage_location_id == 33333
        assert storage.storage_type == StorageLocationType.EXTERNAL_OBJECT_STORE
        assert storage.upload_type == UploadType.S3
        assert storage.bucket == "my-object-store-bucket"
        assert storage.endpoint_url == "https://s3.custom.com"

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
        assert storage.sts_enabled is None
        assert storage.endpoint_url is None
        assert storage.proxy_url is None
        assert storage.secret_key is None

        # GIVEN a PROXY response (no S3 or SFTP fields)
        proxy_response = {
            "storageLocationId": 55555,
            "concreteType": "org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings",
            "uploadType": "PROXYLOCAL",
            "proxyUrl": "https://proxy.example.com",
        }
        storage2 = StorageLocation()
        storage2.fill_from_dict(proxy_response)

        # THEN SFTP/S3 fields are not populated
        assert storage2.bucket is None
        assert storage2.url is None
        assert storage2.supports_subfolders is None
        assert storage2.sts_enabled is None

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


class TestStorageLocationAsync:
    """Async unit tests for StorageLocation model."""

    @pytest.mark.asyncio
    async def test_get_async_missing_id(self):
        """Test that get_async raises ValueError when storage_location_id is missing."""
        # GIVEN a storage location without an ID
        storage = StorageLocation()

        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="storage_location_id is required"):
            await storage.get_async()

    @pytest.mark.asyncio
    async def test_store_async_missing_storage_type(self):
        """Test that store_async raises ValueError when storage_type is missing."""
        # GIVEN a storage location without a storage_type
        storage = StorageLocation(bucket="my-bucket")

        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="storage_type is required"):
            await storage.store_async()
