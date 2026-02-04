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
        body = {
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting",
            "uploadType": "S3",
            "bucket": "my-bucket",
        }
        result = await storage_location_services.create_storage_location_setting(
            body=body,
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


class TestGetProjectSetting:
    """Tests for get_project_setting function."""

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_get_project_setting_exists(self, mock_synapse):
        """Test get_project_setting when setting exists."""
        # GIVEN a mock client that returns a project setting
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {
            "id": "setting123",
            "projectId": "syn456",
            "settingsType": "upload",
            "locations": [12345],
        }

        # WHEN I call get_project_setting
        result = await storage_location_services.get_project_setting(
            project_id="syn456",
            setting_type="upload",
            synapse_client=None,
        )

        # THEN I expect the project setting to be returned
        assert result["id"] == "setting123"
        assert result["locations"] == [12345]
        mock_client.rest_get_async.assert_awaited_once_with(
            uri="/projectSettings/syn456/type/upload",
        )

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_get_project_setting_not_exists(self, mock_synapse):
        """Test get_project_setting when setting does not exist."""
        # GIVEN a mock client that returns empty response
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = ""

        # WHEN I call get_project_setting
        result = await storage_location_services.get_project_setting(
            project_id="syn456",
            setting_type="upload",
            synapse_client=None,
        )

        # THEN I expect None to be returned
        assert result is None


class TestCreateProjectSetting:
    """Tests for create_project_setting function."""

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_create_project_setting(self, mock_synapse):
        """Test create_project_setting creates a project setting."""
        # GIVEN a mock client that returns a project setting
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_post_async.return_value = {
            "id": "setting123",
            "projectId": "syn456",
            "settingsType": "upload",
            "locations": [12345],
        }

        # WHEN I call create_project_setting
        body = {
            "concreteType": "org.sagebionetworks.repo.model.project.UploadDestinationListSetting",
            "settingsType": "upload",
            "locations": [12345],
            "projectId": "syn456",
        }
        result = await storage_location_services.create_project_setting(
            body=body,
            synapse_client=None,
        )

        # THEN I expect the project setting to be returned
        assert result["id"] == "setting123"
        mock_client.rest_post_async.assert_awaited_once()


class TestUpdateProjectSetting:
    """Tests for update_project_setting function."""

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_update_project_setting(self, mock_synapse):
        """Test update_project_setting updates a project setting."""
        # GIVEN a mock client that returns an updated project setting
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_put_async.return_value = {
            "id": "setting123",
            "projectId": "syn456",
            "settingsType": "upload",
            "locations": [12345, 67890],
        }

        # WHEN I call update_project_setting
        body = {
            "id": "setting123",
            "projectId": "syn456",
            "settingsType": "upload",
            "locations": [12345, 67890],
        }
        result = await storage_location_services.update_project_setting(
            body=body,
            synapse_client=None,
        )

        # THEN I expect the updated project setting to be returned
        assert result["locations"] == [12345, 67890]
        mock_client.rest_put_async.assert_awaited_once()


class TestDeleteProjectSetting:
    """Tests for delete_project_setting function."""

    @pytest.mark.asyncio
    @patch("synapseclient.Synapse")
    async def test_delete_project_setting(self, mock_synapse):
        """Test delete_project_setting deletes a project setting."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_delete_async.return_value = None

        # WHEN I call delete_project_setting
        await storage_location_services.delete_project_setting(
            setting_id="setting123",
            synapse_client=None,
        )

        # THEN I expect the delete to be called
        mock_client.rest_delete_async.assert_awaited_once_with(
            uri="/projectSettings/setting123",
        )
