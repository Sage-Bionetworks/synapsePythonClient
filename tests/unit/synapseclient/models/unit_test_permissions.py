"""Unit tests for permissions-related functionality in the AccessControllable mixin."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder


class TestDeletePermissionsAsync:
    """Unit tests for delete_permissions_async method in AccessControllable."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        self.synapse_client = MagicMock(spec=Synapse)
        self.synapse_client.logger = MagicMock()

        # Mock the Synapse.get_client to return our mock client
        self.get_client_patcher = patch(
            "synapseclient.models.mixins.access_control.Synapse.get_client"
        )
        self.mock_get_client = self.get_client_patcher.start()
        self.mock_get_client.return_value = self.synapse_client

        # Mock delete_entity_acl
        self.delete_acl_patcher = patch(
            "synapseclient.models.mixins.access_control.delete_entity_acl"
        )
        self.mock_delete_acl = self.delete_acl_patcher.start()
        self.mock_delete_acl.return_value = None

        yield

        # Clean up patchers
        self.get_client_patcher.stop()
        self.delete_acl_patcher.stop()

    async def test_delete_permissions_no_id(self):
        """Test that attempting to delete permissions without an ID raises an error."""
        # GIVEN a file with no ID
        file = File()

        # WHEN attempting to delete permissions
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="The entity must have an ID to delete permissions."
        ):
            file.delete_permissions()

    async def test_delete_permissions_invalid_entity_type(self):
        """Test that providing an invalid entity type raises a ValueError."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN attempting to delete permissions with an invalid entity type
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="Invalid entity type"):
            file.delete_permissions(target_entity_types=["invalid_type"])

    async def test_delete_permissions_already_inherits(self):
        """Test handling of 403 error when entity already inherits permissions."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a mock HTTP error for already inheriting permissions
        error_response = MagicMock()
        error_response.status_code = 403
        error_response.text = "Resource already inherits its permissions."

        # Create SynapseHTTPError with a message string, not with keyword arguments
        http_error = SynapseHTTPError(
            "403 error: Resource already inherits its permissions."
        )
        http_error.response = (
            error_response  # Set the response attribute after creation
        )
        self.mock_delete_acl.side_effect = http_error

        # WHEN deleting permissions
        file.delete_permissions()

        # THEN the delete_entity_acl function should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

        # AND a debug log message should be generated
        self.synapse_client.logger.debug.assert_called_once()

    async def test_delete_permissions_other_http_error(self):
        """Test handling of other HTTP errors during deletion."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a mock HTTP error for other permission issues
        error_response = MagicMock()
        error_response.status_code = 403
        error_response.text = "Permission denied"

        # Create SynapseHTTPError with a message string, not with keyword arguments
        http_error = SynapseHTTPError("403 error: Permission denied")
        http_error.response = (
            error_response  # Set the response attribute after creation
        )
        self.mock_delete_acl.side_effect = http_error

        # WHEN deleting permissions
        file.delete_permissions()

        # THEN the delete_entity_acl function should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

        # AND a warning log message should be generated
        self.synapse_client.logger.warning.assert_called_once()

    async def test_delete_permissions_skip_self(self):
        """Test that when include_self=False, the entity's ACL is not deleted."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with include_self=False
        file.delete_permissions(include_self=False)

        # THEN the delete_entity_acl function should not be called
        self.mock_delete_acl.assert_not_called()

    async def test_delete_permissions_folder_recursive(self):
        """Test recursive deletion on a folder structure."""
        # GIVEN a folder with an ID and mocked sync_from_synapse_async method
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        # AND mocked child folders and files
        child_folder = Folder(id="syn456")
        child_folder.delete_permissions_async = AsyncMock()

        child_file = File(id="syn789")
        child_file.delete_permissions_async = AsyncMock()

        # Set up the folder structure
        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions recursively
        folder.delete_permissions(recursive=True, include_container_content=True)

        # THEN sync_from_synapse_async should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND delete_permissions_async should be called on child folder and file
        # We use assert_called instead of assert_called_once because the mock may be called multiple times
        # due to the recursive nature of the delete_permissions_async method
        assert child_folder.delete_permissions_async.called
        assert child_file.delete_permissions_async.called

        # AND delete_entity_acl should be called on the folder itself
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_filter_by_entity_type_folder(self):
        """Test filtering deletion by folder entity type."""
        # GIVEN a folder with an ID and child folder and file
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456")
        child_folder.delete_permissions_async = AsyncMock()

        child_file = File(id="syn789")
        child_file.delete_permissions_async = AsyncMock()

        # Set up the folder structure
        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions filtered by folder type
        folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
        )

        # THEN sync_from_synapse_async should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND delete_permissions_async should be called on child folder
        # We use assert_called instead of assert_called_once because the mock may be called multiple times
        assert child_folder.delete_permissions_async.called

        # AND delete_permissions_async should NOT be called on child file
        child_file.delete_permissions_async.assert_not_called()

        # AND delete_entity_acl should be called on the folder itself
        self.mock_delete_acl.assert_called_once()

    async def test_filter_by_entity_type_file(self):
        """Test filtering deletion by file entity type."""
        # GIVEN a folder with an ID and child folder and file
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456")
        child_folder.delete_permissions_async = AsyncMock()

        child_file = File(id="syn789")
        child_file.delete_permissions_async = AsyncMock()

        # Set up the folder structure
        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions filtered by file type
        folder.delete_permissions(
            recursive=True, include_container_content=True, target_entity_types=["file"]
        )

        # THEN sync_from_synapse_async should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND delete_permissions_async should NOT be called on child folder for deletion
        # but will be called for recursion with include_self=False
        args, kwargs = child_folder.delete_permissions_async.call_args
        assert kwargs.get("include_self") is False

        # AND delete_permissions_async should be called on child file
        child_file.delete_permissions_async.assert_called_once()

        # AND delete_entity_acl should NOT be called on the folder itself
        # because it's not a file
        self.mock_delete_acl.assert_not_called()

    async def test_case_insensitive_entity_types(self):
        """Test that entity type matching is case-insensitive."""
        # GIVEN a folder with an ID
        folder = Folder(id="syn123")

        # WHEN deleting permissions with mixed-case entity types
        folder.delete_permissions(target_entity_types=["FoLdEr"])

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_multiple_entity_types(self):
        """Test handling multiple entity types."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with multiple entity types including 'file'
        file.delete_permissions(target_entity_types=["folder", "file"])

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_general_exception_during_delete(self):
        """Test handling of general exceptions during deletion."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a general exception during deletion
        self.mock_delete_acl.side_effect = Exception("General error")

        # WHEN deleting permissions
        file.delete_permissions()

        # THEN the delete_entity_acl function should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

        # AND a warning log message should be generated
        self.synapse_client.logger.warning.assert_called_once()

    async def test_invalid_recursive_without_include_container_content(self):
        """Test that setting recursive=True without include_container_content=True raises ValueError."""
        # GIVEN a folder with an ID
        folder = Folder(id="syn123")

        # WHEN attempting to delete permissions with recursive=True but include_container_content=False
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError,
            match="When recursive=True, include_container_content must also be True",
        ):
            folder.delete_permissions(recursive=True, include_container_content=False)

        # AND the delete_entity_acl function should still be called for the folder itself
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )
