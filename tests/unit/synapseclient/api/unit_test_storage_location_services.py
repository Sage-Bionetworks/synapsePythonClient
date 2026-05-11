"""Unit tests for storage_location_services utility functions."""

from unittest.mock import AsyncMock, patch

import pytest

import synapseclient.api.storage_location_services as storage_location_services


class TestCreateStorageLocationSetting:
    """Tests for create_storage_location_setting function."""

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_create_storage_location_setting(self, mock_synapse):
        """Test create_storage_location_setting creates a storage location."""
        # GIVEN a mock client that returns a storage location
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_post_async.return_value = {
            "storageLocationId": 12345,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
            "uploadType": "S3",
            "bucket": "my-bucket",
        }

        # WHEN I call create_storage_location_setting
        request = {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
            "uploadType": "S3",
            "bucket": "my-bucket",
        }
        result = await storage_location_services.create_storage_location_setting(
            request=request,
            synapse_client=None,
        )

        # THEN I expect the storage location to be returned
        assert result["storageLocationId"] == 12345
        assert result["bucket"] == "my-bucket"
        mock_client.rest_post_async.assert_awaited_once()


class TestGetStorageLocationSetting:
    """Tests for get_storage_location_setting function."""

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_get_storage_location_setting(self, mock_synapse):
        """Test get_storage_location_setting retrieves a storage location."""
        # GIVEN a mock client that returns a storage location
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {
            "storageLocationId": 12345,
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
            "uploadType": "S3",
            "bucket": "my-bucket",
        }

        # WHEN I call get_storage_location_setting
        result = await storage_location_services.get_storage_location_setting(
            storage_location_id=12345,
            synapse_client=None,
        )

        # THEN I expect the storage location to be returned
        assert result["storageLocationId"] == 12345
        assert result["bucket"] == "my-bucket"
        mock_client.rest_get_async.assert_awaited_once_with(
            uri="/storageLocation/12345",
        )
