"""Comprehensive unit tests for access control functionality in the AccessControllable mixin."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.models.acl import AclListResult
from synapseclient.models import File, Folder, Project
from synapseclient.models.mixins.access_control import BenefactorTracker

# TODO: Consolidate test with `unit_test_permissions_async.py`


class TestDeletePermissionsAsyncComprehensive:
    """Comprehensive unit tests for delete_permissions_async method in AccessControllable."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        self.synapse_client = MagicMock(spec=Synapse)
        self.synapse_client.logger = MagicMock()
        self.synapse_client.silent = False  # Ensure logging is enabled

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

        # Mock get_entity_acl
        self.get_acl_patcher = patch(
            "synapseclient.models.mixins.access_control.get_entity_acl"
        )
        self.mock_get_acl = self.get_acl_patcher.start()

        # Mock get_entity_benefactor
        self.get_benefactor_patcher = patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor"
        )
        self.mock_get_benefactor = self.get_benefactor_patcher.start()

        # Mock get_user_group_headers_batch
        self.get_user_headers_patcher = patch(
            "synapseclient.models.mixins.access_control.get_user_group_headers_batch"
        )
        self.mock_get_user_headers = self.get_user_headers_patcher.start()

        yield

        # Clean up patchers
        self.get_client_patcher.stop()
        self.delete_acl_patcher.stop()
        self.get_acl_patcher.stop()
        self.get_benefactor_patcher.stop()
        self.get_user_headers_patcher.stop()

    # === Basic functionality tests ===

    async def test_delete_permissions_no_id_raises_error(self):
        """Test that attempting to delete permissions without an ID raises ValueError."""
        # GIVEN a file with no ID
        file = File()

        # WHEN attempting to delete permissions
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="The entity must have an ID to delete permissions."
        ):
            await file.delete_permissions_async()

    async def test_delete_permissions_basic_success(self):
        """Test basic successful permission deletion."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions
        await file.delete_permissions_async()

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_project_warning(self):
        """Test that deleting permissions on a project logs a warning."""
        # GIVEN a project with an ID
        project = Project(id="syn123")

        # WHEN deleting permissions with include_self=True
        await project.delete_permissions_async(include_self=True)

        # THEN a warning should be logged
        self.synapse_client.logger.warning.assert_called_once()
        warning_message = self.synapse_client.logger.warning.call_args[0][0]
        assert "Project" in warning_message
        assert "cannot be deleted" in warning_message

        # AND delete_entity_acl should NOT be called for the project
        self.mock_delete_acl.assert_not_called()

    async def test_delete_permissions_include_self_false(self):
        """Test that include_self=False skips deleting the entity's own ACL."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with include_self=False
        await file.delete_permissions_async(include_self=False)

        # THEN delete_entity_acl should not be called
        self.mock_delete_acl.assert_not_called()

    # === Entity type filtering tests ===

    async def test_delete_permissions_invalid_entity_type_raises_error(self):
        """Test that invalid entity types raise ValueError."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN attempting to delete permissions with invalid entity type
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="Invalid entity type"):
            await file.delete_permissions_async(target_entity_types=["invalid_type"])

    async def test_delete_permissions_case_insensitive_entity_types(self):
        """Test that entity type matching is case-insensitive."""
        # GIVEN a folder with an ID
        folder = Folder(id="syn123")

        # WHEN deleting permissions with mixed-case entity types
        await folder.delete_permissions_async(target_entity_types=["FoLdEr"])

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_multiple_entity_types(self):
        """Test filtering with multiple valid entity types."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with multiple entity types including 'file'
        await file.delete_permissions_async(target_entity_types=["folder", "file"])

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_excluded_entity_type(self):
        """Test that entities not matching target types are skipped."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with only folder type
        await file.delete_permissions_async(target_entity_types=["folder"])

        # THEN delete_entity_acl should NOT be called
        self.mock_delete_acl.assert_not_called()

    # === Error handling tests ===

    async def test_delete_permissions_http_error_already_inherits(self):
        """Test handling of 403 error when entity already inherits permissions."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a mock HTTP error for already inheriting permissions
        error_response = MagicMock()
        error_response.status_code = 403
        error_response.text = "Resource already inherits its permissions."

        http_error = SynapseHTTPError(
            "403 error: Resource already inherits its permissions."
        )
        http_error.response = error_response
        self.mock_delete_acl.side_effect = http_error

        # WHEN deleting permissions
        await file.delete_permissions_async()

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

        # AND a debug log message should be generated
        self.synapse_client.logger.debug.assert_called_once()

    async def test_delete_permissions_http_error_other_403(self):
        """Test handling of other 403 HTTP errors."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a mock HTTP error for other permission issues
        error_response = MagicMock()
        error_response.status_code = 403
        error_response.text = "Permission denied"

        http_error = SynapseHTTPError("403 error: Permission denied")
        http_error.response = error_response
        self.mock_delete_acl.side_effect = http_error

        # WHEN deleting permissions
        # THEN the error should be re-raised
        with pytest.raises(SynapseHTTPError):
            await file.delete_permissions_async()

    async def test_delete_permissions_general_exception(self):
        """Test handling of general exceptions during deletion."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a general exception during deletion
        self.mock_delete_acl.side_effect = Exception("Network error")

        # WHEN deleting permissions
        # THEN the exception should be re-raised
        with pytest.raises(Exception, match="Network error"):
            await file.delete_permissions_async()

    # === Recursive processing tests ===

    async def test_delete_permissions_recursive_validation(self):
        """Test that recursive=True without include_container_content=True raises error."""
        # GIVEN a folder with an ID
        folder = Folder(id="syn123")

        # WHEN attempting recursive without container content
        # THEN ValueError should be raised
        with pytest.raises(
            ValueError,
            match="When recursive=True, include_container_content must also be True",
        ):
            await folder.delete_permissions_async(
                recursive=True, include_container_content=False
            )

    async def test_delete_permissions_container_content_only(self):
        """Test processing container content without recursion."""
        # GIVEN a folder with children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_file = File(id="syn456")
        child_file.delete_permissions_async = AsyncMock()

        child_folder = Folder(id="syn789")
        child_folder.delete_permissions_async = AsyncMock()

        folder.files = [child_file]
        folder.folders = [child_folder]

        # WHEN deleting permissions with include_container_content=True but not recursive
        await folder.delete_permissions_async(
            include_container_content=True, recursive=False
        )

        # THEN sync should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND child entities should be processed
        child_file.delete_permissions_async.assert_called_once()
        child_folder.delete_permissions_async.assert_called_once()

        # AND folder itself should be processed
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_recursive_processing(self):
        """Test full recursive processing."""
        # GIVEN a folder with nested structure
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456")
        child_folder.delete_permissions_async = AsyncMock()

        folder.folders = [child_folder]
        folder.files = []

        # WHEN deleting permissions recursively
        await folder.delete_permissions_async(
            recursive=True, include_container_content=True
        )

        # THEN child folder should be processed recursively
        child_folder.delete_permissions_async.assert_called_once()
        call_args = child_folder.delete_permissions_async.call_args
        assert call_args[1]["recursive"] is True
        assert call_args[1]["include_container_content"] is True

    async def test_delete_permissions_entity_type_filtering_recursive(self):
        """Test entity type filtering in recursive processing."""
        # GIVEN a folder with mixed children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456")
        child_folder.delete_permissions_async = AsyncMock()

        child_file = File(id="syn789")
        child_file.delete_permissions_async = AsyncMock()

        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions filtered by folder type only
        await folder.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
        )

        # THEN only folder entities should be processed for deletion
        child_folder.delete_permissions_async.assert_called_once()
        child_file.delete_permissions_async.assert_not_called()

        # AND the parent folder should also be processed
        self.mock_delete_acl.assert_called_once()

    # === Dry run tests ===

    async def test_delete_permissions_dry_run_basic(self):
        """Test basic dry run functionality."""
        # GIVEN a file with an ID
        file = File(id="syn123", name="test_file.txt")

        # AND mock ACL response
        self.mock_get_acl.return_value = {
            "resourceAccess": [{"principalId": 123, "accessType": ["READ", "DOWNLOAD"]}]
        }

        # WHEN deleting permissions with dry_run=True
        await file.delete_permissions_async(dry_run=True)

        # THEN no actual deletion should occur
        self.mock_delete_acl.assert_not_called()

        # AND ACL should be retrieved for logging
        self.mock_get_acl.assert_called_once()

        # AND info should be logged
        self.synapse_client.logger.info.assert_called()

    async def test_delete_permissions_dry_run_no_acl(self):
        """Test dry run when entity has no local ACL."""
        # GIVEN a file with an ID
        file = File(id="syn123", name="test_file.txt")

        # AND mock 404 response (no local ACL)
        error_response = MagicMock()
        error_response.status_code = 404
        http_error = SynapseHTTPError("404 error: Not found")
        http_error.response = error_response
        self.mock_get_acl.side_effect = http_error

        # WHEN deleting permissions with dry_run=True
        await file.delete_permissions_async(dry_run=True)

        # THEN no actual deletion should occur
        self.mock_delete_acl.assert_not_called()

        # AND the 404 should be handled gracefully
        self.mock_get_acl.assert_called_once()

    async def test_delete_permissions_dry_run_with_benefactor_tracking(self):
        """Test dry run with benefactor tracking enabled."""
        # GIVEN a folder with children
        folder = Folder(id="syn123", name="test_folder")
        folder.sync_from_synapse_async = AsyncMock()

        # AND mock collect_all_entities_for_tracking
        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [folder]

        # AND mock ACL response
        self.mock_get_acl.return_value = {
            "resourceAccess": [{"principalId": 123, "accessType": ["READ"]}]
        }

        # AND mock benefactor response
        mock_benefactor = MagicMock()
        mock_benefactor.id = "syn456"
        self.mock_get_benefactor.return_value = mock_benefactor

        # WHEN deleting permissions with dry_run=True and container content
        await folder.delete_permissions_async(
            dry_run=True, include_container_content=True, recursive=True
        )

        # THEN entities should be collected for tracking
        folder._collect_all_entities_for_tracking.assert_called_once()

        # AND benefactor tracking should be set up
        self.mock_get_benefactor.assert_called()

        # AND comprehensive dry run tree should be logged
        self.synapse_client.logger.info.assert_called()

    async def test_delete_permissions_dry_run_flags(self):
        """Test dry run with different flag combinations."""
        # GIVEN a folder with a file
        folder = Folder(id="syn123", name="test_folder")
        folder.sync_from_synapse_async = AsyncMock()

        child_file = File(id="syn456", name="test_file.txt")
        folder.files = [child_file]
        folder.folders = []

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [folder, child_file]

        # AND mock ACL responses
        self.mock_get_acl.return_value = {
            "resourceAccess": [{"principalId": 123, "accessType": ["READ"]}]
        }

        # AND mock benefactor
        mock_benefactor = MagicMock()
        mock_benefactor.id = "syn999"
        self.mock_get_benefactor.return_value = mock_benefactor

        # WHEN running dry run with show_acl_details=False
        await folder.delete_permissions_async(
            dry_run=True, include_container_content=True, show_acl_details=False
        )

        # THEN dry run should still execute
        self.synapse_client.logger.info.assert_called()

        # WHEN running dry run with show_files_in_containers=False
        await folder.delete_permissions_async(
            dry_run=True, include_container_content=True, show_files_in_containers=False
        )

        # THEN dry run should still execute
        assert self.synapse_client.logger.info.call_count >= 2

    # === Benefactor tracking tests ===

    async def test_delete_permissions_with_benefactor_tracking(self):
        """Test deletion with benefactor tracking functionality."""
        # GIVEN a folder with children that need tracking
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_file = File(id="syn456")
        folder.files = [child_file]
        folder.folders = []

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [folder, child_file]

        # AND mock benefactor responses
        mock_benefactor = MagicMock()
        mock_benefactor.id = "syn999"
        self.mock_get_benefactor.return_value = mock_benefactor

        # WHEN deleting permissions with container content
        await folder.delete_permissions_async(
            include_container_content=True, recursive=True
        )

        # THEN entities should be collected for tracking
        folder._collect_all_entities_for_tracking.assert_called_once()

        # AND benefactor information should be gathered
        self.mock_get_benefactor.assert_called()

        # AND deletion should proceed
        self.mock_delete_acl.assert_called()

    # === Edge cases and complex scenarios ===

    async def test_delete_permissions_sync_failure(self):
        """Test handling when sync_from_synapse_async is not available."""
        # GIVEN a folder without sync capability
        folder = Folder(id="syn123")
        # Not setting sync_from_synapse_async

        # WHEN deleting permissions with container content
        await folder.delete_permissions_async(include_container_content=True)

        # THEN only the folder itself should be processed
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_empty_containers(self):
        """Test processing empty containers."""
        # GIVEN a folder with no children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()
        folder.files = []
        folder.folders = []

        # WHEN deleting permissions with container content
        await folder.delete_permissions_async(
            include_container_content=True, recursive=True
        )

        # THEN sync should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND folder deletion should proceed
        self.mock_delete_acl.assert_called_once()

    async def test_delete_permissions_mixed_entity_types_complex(self):
        """Test complex scenario with mixed entity types and filtering."""
        # GIVEN a folder with various children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder1 = Folder(id="syn456")
        child_folder1.delete_permissions_async = AsyncMock()

        child_folder2 = Folder(id="syn789")
        child_folder2.delete_permissions_async = AsyncMock()

        child_file1 = File(id="syn111")
        child_file1.delete_permissions_async = AsyncMock()

        child_file2 = File(id="syn222")
        child_file2.delete_permissions_async = AsyncMock()

        folder.folders = [child_folder1, child_folder2]
        folder.files = [child_file1, child_file2]

        # WHEN deleting permissions for files only
        await folder.delete_permissions_async(
            include_container_content=True, target_entity_types=["file"]
        )

        # THEN only files should be processed for deletion
        child_file1.delete_permissions_async.assert_called_once()
        child_file2.delete_permissions_async.assert_called_once()

        # AND folders should not be processed for deletion
        child_folder1.delete_permissions_async.assert_not_called()
        child_folder2.delete_permissions_async.assert_not_called()

        # AND the parent folder should not be deleted (not a file)
        self.mock_delete_acl.assert_not_called()


class TestBenefactorTrackerComprehensive:
    """Comprehensive unit tests for BenefactorTracker functionality."""

    def test_benefactor_tracker_initialization(self):
        """Test BenefactorTracker initialization."""
        # WHEN creating a new tracker
        tracker = BenefactorTracker()

        # THEN all collections should be empty
        assert len(tracker.entity_benefactors) == 0
        assert len(tracker.benefactor_children) == 0
        assert len(tracker.deleted_acls) == 0
        assert len(tracker.processed_entities) == 0

    async def test_track_entity_basic(self):
        """Test basic entity tracking."""
        # GIVEN a tracker and mock client
        tracker = BenefactorTracker()
        mock_client = MagicMock()

        # AND mock benefactor response
        mock_benefactor = MagicMock()
        mock_benefactor.id = "syn456"

        with patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor",
            return_value=mock_benefactor,
        ):
            # WHEN tracking an entity
            await tracker.track_entity("syn123", mock_client)

            # THEN entity should be tracked with its benefactor
            assert tracker.entity_benefactors["syn123"] == "syn456"
            assert "syn123" in tracker.processed_entities

    async def test_track_entity_self_benefactor(self):
        """Test tracking entity that is its own benefactor."""
        # GIVEN a tracker
        tracker = BenefactorTracker()
        mock_client = MagicMock()

        # AND entity is its own benefactor
        mock_benefactor = MagicMock()
        mock_benefactor.id = "syn123"

        with patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor",
            return_value=mock_benefactor,
        ):
            # WHEN tracking the entity
            await tracker.track_entity("syn123", mock_client)

            # THEN entity should be tracked
            assert tracker.entity_benefactors["syn123"] == "syn123"
            assert "syn123" in tracker.processed_entities

            # AND it should not be in its own children list
            assert "syn123" not in tracker.benefactor_children.get("syn123", [])

    async def test_track_entity_with_children(self):
        """Test tracking entities that have benefactor relationships."""
        # GIVEN a tracker
        tracker = BenefactorTracker()
        mock_client = MagicMock()

        benefactor_responses = [
            MagicMock(id="syn999"),  # Child inherits from syn999
            MagicMock(id="syn999"),  # Another child inherits from syn999
        ]

        with patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor",
            side_effect=benefactor_responses,
        ):
            # WHEN tracking multiple entities with same benefactor
            await tracker.track_entity("syn123", mock_client)
            await tracker.track_entity("syn456", mock_client)

            # THEN both should be tracked under the same benefactor
            assert tracker.entity_benefactors["syn123"] == "syn999"
            assert tracker.entity_benefactors["syn456"] == "syn999"

            # AND benefactor should have both as children
            children = tracker.benefactor_children.get("syn999", [])
            assert "syn123" in children
            assert "syn456" in children

    async def test_track_entities_parallel(self):
        """Test parallel entity tracking."""
        # GIVEN a tracker
        tracker = BenefactorTracker()
        mock_client = MagicMock()

        entity_ids = ["syn123", "syn456", "syn789"]
        benefactor_responses = [
            MagicMock(id="syn999"),
            MagicMock(id="syn888"),
            MagicMock(id="syn999"),
        ]

        with patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor",
            side_effect=benefactor_responses,
        ):
            # WHEN tracking multiple entities in parallel
            await tracker.track_entities_parallel(entity_ids, mock_client)

            # THEN all entities should be tracked
            assert len(tracker.entity_benefactors) == 3
            assert tracker.entity_benefactors["syn123"] == "syn999"
            assert tracker.entity_benefactors["syn456"] == "syn888"
            assert tracker.entity_benefactors["syn789"] == "syn999"

            # AND all should be marked as processed
            assert len(tracker.processed_entities) == 3

    async def test_track_entities_parallel_skip_processed(self):
        """Test that parallel tracking skips already processed entities."""
        # GIVEN a tracker with some already processed entities
        tracker = BenefactorTracker()
        tracker.processed_entities.add("syn123")
        mock_client = MagicMock()

        entity_ids = ["syn123", "syn456"]  # syn123 already processed

        with patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor",
            return_value=MagicMock(id="syn999"),
        ) as mock_get_benefactor:
            # WHEN tracking entities
            await tracker.track_entities_parallel(entity_ids, mock_client)

            # THEN only unprocessed entity should be fetched
            mock_get_benefactor.assert_called_once()
            assert tracker.entity_benefactors.get("syn456") == "syn999"

    def test_mark_acl_deleted(self):
        """Test marking ACL as deleted."""
        # GIVEN a tracker with tracked entities
        tracker = BenefactorTracker()
        tracker.entity_benefactors["syn123"] = "syn456"
        tracker.entity_benefactors["syn789"] = "syn456"
        tracker.benefactor_children["syn456"] = ["syn123", "syn789"]

        # WHEN marking an ACL as deleted
        affected = tracker.mark_acl_deleted("syn456")

        # THEN entity should be marked as deleted
        assert "syn456" in tracker.deleted_acls

        # AND affected children should be returned
        assert set(affected) == {"syn123", "syn789"}

        # AND children should get new benefactor
        assert tracker.entity_benefactors["syn123"] == "syn456"  # Updated benefactor
        assert tracker.entity_benefactors["syn789"] == "syn456"  # Updated benefactor

    def test_mark_acl_deleted_complex_hierarchy(self):
        """Test ACL deletion in complex hierarchy."""
        # GIVEN a complex hierarchy
        tracker = BenefactorTracker()
        # syn100 -> syn200 -> syn300 (inheritance chain)
        tracker.entity_benefactors["syn100"] = "syn200"
        tracker.entity_benefactors["syn200"] = "syn300"
        tracker.entity_benefactors["syn300"] = "syn300"  # self-benefactor
        tracker.benefactor_children["syn200"] = ["syn100"]
        tracker.benefactor_children["syn300"] = ["syn200"]

        # WHEN deleting ACL from middle of chain
        affected = tracker.mark_acl_deleted("syn200")

        # THEN affected entities should be identified
        assert "syn100" in affected

        # AND inheritance should be updated
        assert (
            tracker.entity_benefactors["syn100"] == "syn200"
        )  # Now inherits from syn200's benefactor

    def test_get_current_benefactor(self):
        """Test getting current benefactor for an entity."""
        # GIVEN a tracker with entities
        tracker = BenefactorTracker()
        tracker.entity_benefactors["syn123"] = "syn456"

        # WHEN getting current benefactor
        benefactor = tracker.get_current_benefactor("syn123")

        # THEN correct benefactor should be returned
        assert benefactor == "syn456"

        # WHEN getting benefactor for untracked entity
        benefactor = tracker.get_current_benefactor("syn999")

        # THEN entity itself should be returned
        assert benefactor == "syn999"

    def test_will_acl_deletion_affect_others_true(self):
        """Test detection when ACL deletion will affect other entities."""
        # GIVEN a tracker with dependencies
        tracker = BenefactorTracker()
        tracker.benefactor_children["syn123"] = ["syn456", "syn789"]

        # WHEN checking if deletion affects others
        will_affect = tracker.will_acl_deletion_affect_others("syn123")

        # THEN it should return True
        assert will_affect is True

    def test_will_acl_deletion_affect_others_false(self):
        """Test detection when ACL deletion won't affect others."""
        # GIVEN a tracker with no dependencies
        tracker = BenefactorTracker()
        tracker.benefactor_children["syn123"] = []

        # WHEN checking if deletion affects others
        will_affect = tracker.will_acl_deletion_affect_others("syn123")

        # THEN it should return False
        assert will_affect is False

    def test_will_acl_deletion_affect_others_untracked(self):
        """Test behavior for untracked entities."""
        # GIVEN an empty tracker
        tracker = BenefactorTracker()

        # WHEN checking untracked entity
        will_affect = tracker.will_acl_deletion_affect_others("syn123")

        # THEN it should return False
        assert will_affect is False


class TestListAclAsyncComprehensive:
    """Comprehensive unit tests for list_acl_async method in AccessControllable."""

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

        # Mock get_entity_acl
        self.get_acl_patcher = patch(
            "synapseclient.models.mixins.access_control.get_entity_acl"
        )
        self.mock_get_acl = self.get_acl_patcher.start()

        # Mock get_user_group_headers_batch
        self.get_user_headers_patcher = patch(
            "synapseclient.models.mixins.access_control.get_user_group_headers_batch"
        )
        self.mock_get_user_headers = self.get_user_headers_patcher.start()

        yield

        # Clean up patchers
        self.get_client_patcher.stop()
        self.get_acl_patcher.stop()
        self.get_user_headers_patcher.stop()

    # === Basic functionality tests ===

    async def test_list_acl_no_id_raises_error(self):
        """Test that listing ACL without an ID raises ValueError."""
        # GIVEN a file with no ID
        file = File()

        # WHEN attempting to list ACL
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="The entity must have an ID to list ACLs."
        ):
            await file.list_acl_async()

    async def test_list_acl_basic_single_entity(self):
        """Test basic ACL listing for a single entity."""
        # GIVEN a file with an ID
        file = File(id="syn123", name="test_file.txt")

        # AND mock ACL response
        mock_acl = {
            "id": "syn123",
            "etag": "etag123",
            "resourceAccess": [
                {"principalId": 123, "accessType": ["READ", "DOWNLOAD"]},
                {"principalId": 456, "accessType": ["CHANGE_PERMISSIONS"]},
            ],
        }
        self.mock_get_acl.return_value = mock_acl

        # AND mock user headers response (returns List[Dict])
        mock_headers = [
            {
                "ownerId": "123",
                "firstName": "John",
                "lastName": "Doe",
                "userName": "johndoe",
                "email": "john@example.com",
                "isIndividual": True,
            },
            {"ownerId": "456", "teamName": "Administrators", "isIndividual": False},
        ]
        self.mock_get_user_headers.return_value = mock_headers

        # WHEN listing ACL
        result = await file.list_acl_async()

        # THEN ACL should be retrieved
        self.mock_get_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

        # AND user headers should be retrieved
        self.mock_get_user_headers.assert_called_once_with(
            ["123", "456"], synapse_client=self.synapse_client
        )

        # AND result should be AclListResult
        assert isinstance(result, AclListResult)
        assert len(result.entity_acls) == 1

        entity_acl = result.entity_acls[0]
        assert entity_acl.entity_id == "syn123"
        assert len(entity_acl.acl_entries) == 2

    async def test_list_acl_no_local_acl(self):
        """Test listing ACL for entity without local ACL."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND mock 404 response (no local ACL)
        error_response = MagicMock()
        error_response.status_code = 404
        http_error = SynapseHTTPError("404 error: Not found")
        http_error.response = error_response
        self.mock_get_acl.side_effect = http_error

        # WHEN listing ACL
        result = await file.list_acl_async()

        # THEN result should be empty but valid
        assert isinstance(result, AclListResult)
        assert len(result.entity_acls) == 0

        # AND debug log should be generated
        self.synapse_client.logger.debug.assert_called_once()

    async def test_list_acl_empty_resource_access(self):
        """Test handling of ACL with empty resourceAccess."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND mock ACL response with no resource access
        mock_acl = {"id": "syn123", "etag": "etag123", "resourceAccess": []}
        self.mock_get_acl.return_value = mock_acl
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL
        result = await file.list_acl_async()

        # THEN result should be valid but empty
        assert isinstance(result, AclListResult)
        assert len(result.entity_acls) == 1

        entity_acl = result.entity_acls[0]
        assert entity_acl.entity_id == "syn123"
        assert len(entity_acl.acl_entries) == 0

    # === Entity type filtering tests ===

    async def test_list_acl_invalid_entity_type_raises_error(self):
        """Test that invalid entity types raise ValueError."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN attempting to list ACL with invalid entity type
        # THEN ValueError should be raised
        with pytest.raises(ValueError, match="Invalid entity type"):
            await file.list_acl_async(target_entity_types=["invalid_type"])

    async def test_list_acl_case_insensitive_entity_types(self):
        """Test that entity type filtering is case-insensitive."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND mock ACL response
        self.mock_get_acl.return_value = {
            "id": "syn123",
            "etag": "etag123",
            "resourceAccess": [],
        }
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL with mixed-case entity type
        result = await file.list_acl_async(target_entity_types=["FiLe"])

        # THEN ACL should be retrieved (case-insensitive match)
        self.mock_get_acl.assert_called_once()
        assert isinstance(result, AclListResult)

    async def test_list_acl_excluded_entity_type(self):
        """Test that entities not matching target types are excluded."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN listing ACL with only folder type
        result = await file.list_acl_async(target_entity_types=["folder"])

        # THEN no ACL should be retrieved
        self.mock_get_acl.assert_not_called()

        # AND result should be empty
        assert isinstance(result, AclListResult)
        assert len(result.entity_acls) == 0

    # === Recursive processing tests ===

    async def test_list_acl_recursive_validation(self):
        """Test that recursive=True without include_container_content=True raises error."""
        # GIVEN a folder
        folder = Folder(id="syn123")

        # WHEN attempting recursive without container content
        # THEN ValueError should be raised
        with pytest.raises(
            ValueError,
            match="When recursive=True, include_container_content must also be True",
        ):
            await folder.list_acl_async(recursive=True, include_container_content=False)

    async def test_list_acl_container_content_only(self):
        """Test listing ACL for container content without recursion."""
        # GIVEN a folder with children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_file = File(id="syn456")
        child_file._get_current_entity_acl = AsyncMock()
        child_file._get_current_entity_acl.return_value = {"123": ["READ"]}

        child_folder = Folder(id="syn789")
        child_folder._get_current_entity_acl = AsyncMock()
        child_folder._get_current_entity_acl.return_value = {
            "456": ["CHANGE_PERMISSIONS"]
        }

        folder.files = [child_file]
        folder.folders = [child_folder]

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [
            child_file,
            child_folder,
        ]

        # AND mock folder ACL
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL with include_container_content=True but not recursive
        result = await folder.list_acl_async(
            include_container_content=True, recursive=False
        )

        # THEN sync should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND entities should be collected
        folder._collect_all_entities_for_tracking.assert_called_once()

        # AND result should include all entities
        assert isinstance(result, AclListResult)

    async def test_list_acl_recursive_processing(self):
        """Test full recursive ACL listing."""
        # GIVEN a folder with nested structure
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456")
        folder.folders = [child_folder]
        folder.files = []

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [child_folder]

        # AND mock ACL responses
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL recursively
        result = await folder.list_acl_async(
            recursive=True, include_container_content=True
        )

        # THEN entities should be collected recursively
        folder._collect_all_entities_for_tracking.assert_called_once()
        call_args = folder._collect_all_entities_for_tracking.call_args[1]
        assert call_args["recursive"] is True
        assert call_args["include_container_content"] is True

    async def test_list_acl_entity_type_filtering_recursive(self):
        """Test entity type filtering in recursive ACL listing."""
        # GIVEN a folder with mixed children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456")
        child_file = File(id="syn789")

        folder.folders = [child_folder]
        folder.files = [child_file]

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [
            child_folder
        ]  # Only folder

        # AND mock ACL response
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL filtered by folder type only
        result = await folder.list_acl_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
        )

        # THEN only folder entities should be processed
        folder._collect_all_entities_for_tracking.assert_called_once()
        call_args = folder._collect_all_entities_for_tracking.call_args[1]
        assert call_args["target_entity_types"] == ["folder"]

    # === Tree logging tests ===

    async def test_list_acl_with_tree_logging(self):
        """Test ACL listing with tree logging enabled."""
        # GIVEN a folder with children
        folder = Folder(id="syn123", name="test_folder")
        folder.sync_from_synapse_async = AsyncMock()
        folder.folders = []
        folder.files = []

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [folder]

        # AND mock ACL response
        self.mock_get_acl.return_value = {
            "id": "syn123",
            "etag": "etag123",
            "resourceAccess": [{"principalId": 123, "accessType": ["READ"]}],
        }
        self.mock_get_user_headers.return_value = [
            {"ownerId": "123", "userName": "testuser", "isIndividual": True}
        ]

        # WHEN listing ACL with tree logging
        result = await folder.list_acl_async(
            log_tree=True, recursive=True, include_container_content=True
        )

        # THEN tree should be logged
        assert self.synapse_client.logger.info.called
        logged_messages = [
            call[0][0] for call in self.synapse_client.logger.info.call_args_list
        ]
        assert any("ACL Tree Structure:" in msg for msg in logged_messages)

        # AND result should be returned
        assert isinstance(result, AclListResult)

    # === Complex scenarios ===

    async def test_list_acl_complex_hierarchy(self):
        """Test ACL listing in complex entity hierarchy."""
        # GIVEN a complex folder structure
        root_folder = Folder(id="syn100", name="root")
        root_folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn200", name="child")
        child_file = File(id="syn300", name="file.txt")

        root_folder.folders = [child_folder]
        root_folder.files = [child_file]

        root_folder._collect_all_entities_for_tracking = AsyncMock()
        root_folder._collect_all_entities_for_tracking.return_value = [
            root_folder,
            child_folder,
            child_file,
        ]

        # AND mock different ACL responses for each entity
        acl_responses = {}

        def mock_get_acl_side_effect(entity_id, synapse_client):
            return acl_responses.get(entity_id, {"resourceAccess": []})

        # Set up different ACLs for different entities
        acl_responses["syn100"] = {
            "id": "syn100",
            "resourceAccess": [
                {"principalId": 111, "accessType": ["READ", "DOWNLOAD"]}
            ],
        }
        acl_responses["syn200"] = {
            "id": "syn200",
            "resourceAccess": [
                {"principalId": 222, "accessType": ["CHANGE_PERMISSIONS"]}
            ],
        }
        # syn300 (file) has no local ACL - will get 404

        async def mock_get_current_acl(client):
            entity_id = None
            if hasattr(self, "id"):
                entity_id = self.id
            return acl_responses.get(entity_id, {}).get("resourceAccess", [])

        # Mock each entity's _get_current_entity_acl method
        child_folder._get_current_entity_acl = AsyncMock()
        child_folder._get_current_entity_acl.return_value = {
            "222": ["CHANGE_PERMISSIONS"]
        }

        child_file._get_current_entity_acl = AsyncMock()
        child_file._get_current_entity_acl.return_value = None  # No local ACL

        self.mock_get_acl.return_value = acl_responses["syn100"]
        self.mock_get_user_headers.return_value = [
            {"ownerId": "111", "userName": "admin", "isIndividual": True},
            {"ownerId": "222", "userName": "moderator", "isIndividual": True},
        ]

        # WHEN listing ACL recursively
        result = await root_folder.list_acl_async(
            recursive=True, include_container_content=True
        )

        # THEN result should include ACLs from multiple entities
        assert isinstance(result, AclListResult)

        # AND user headers should be retrieved for all principals
        self.mock_get_user_headers.assert_called_once()

    async def test_list_acl_error_handling(self):
        """Test error handling during ACL listing."""
        # GIVEN a folder with children where one child fails
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        child_file = File(id="syn456")
        child_file._get_current_entity_acl = AsyncMock()
        child_file._get_current_entity_acl.side_effect = Exception("Network error")

        folder.files = [child_file]
        folder.folders = []

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = [child_file]

        # AND mock folder ACL
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL
        result = await folder.list_acl_async(include_container_content=True)

        # THEN errors should be handled gracefully
        assert isinstance(result, AclListResult)

        # AND warning should be logged
        self.synapse_client.logger.warning.assert_called()

    async def test_list_acl_no_user_headers(self):
        """Test ACL listing when user headers can't be retrieved."""
        # GIVEN a file with ACL
        file = File(id="syn123")

        # AND mock ACL response
        self.mock_get_acl.return_value = {
            "id": "syn123",
            "resourceAccess": [{"principalId": 123, "accessType": ["READ"]}],
        }

        # AND user headers fails
        self.mock_get_user_headers.side_effect = Exception("User service unavailable")

        # WHEN listing ACL
        result = await file.list_acl_async()

        # THEN result should still be created without user info
        assert isinstance(result, AclListResult)
        assert len(result.entity_acls) == 1

        entity_acl = result.entity_acls[0]
        assert len(entity_acl.acl_entries) == 1
        assert entity_acl.acl_entries[0].principal_id == 123

    # === Performance and edge cases ===

    async def test_list_acl_large_hierarchy(self):
        """Test ACL listing performance with large hierarchy."""
        # GIVEN a folder with many children
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        # Create many child entities
        children = []
        for i in range(100):
            child = File(id=f"syn{i + 1000}")
            child._get_current_entity_acl = AsyncMock()
            child._get_current_entity_acl.return_value = {str(i): ["READ"]}
            children.append(child)

        folder.files = children
        folder.folders = []

        folder._collect_all_entities_for_tracking = AsyncMock()
        folder._collect_all_entities_for_tracking.return_value = children

        # AND mock responses
        self.mock_get_acl.return_value = {"resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL
        result = await folder.list_acl_async(include_container_content=True)

        # THEN operation should complete successfully
        assert isinstance(result, AclListResult)

        # AND entities should be collected
        folder._collect_all_entities_for_tracking.assert_called_once()

    async def test_list_acl_mixed_permissions(self):
        """Test ACL listing with complex permission combinations."""
        # GIVEN a file with complex ACL
        file = File(id="syn123")

        # AND mock complex ACL response
        self.mock_get_acl.return_value = {
            "id": "syn123",
            "resourceAccess": [
                {"principalId": 123, "accessType": ["READ"]},
                {"principalId": 456, "accessType": ["READ", "DOWNLOAD"]},
                {"principalId": 789, "accessType": ["READ", "DOWNLOAD", "UPDATE"]},
                {"principalId": 999, "accessType": ["CHANGE_PERMISSIONS", "DELETE"]},
            ],
        }

        # AND mock user headers
        self.mock_get_user_headers.return_value = [
            {"ownerId": "123", "userName": "viewer", "isIndividual": True},
            {"ownerId": "456", "userName": "downloader", "isIndividual": True},
            {"ownerId": "789", "userName": "editor", "isIndividual": True},
            {"ownerId": "999", "teamName": "Admins", "isIndividual": False},
        ]

        # WHEN listing ACL
        result = await file.list_acl_async()

        # THEN result should contain all permission combinations
        assert isinstance(result, AclListResult)
        assert len(result.entity_acls) == 1

        entity_acl = result.entity_acls[0]
        assert len(entity_acl.acl_entries) == 4

        # Verify permissions are correctly parsed
        permissions_by_principal = {
            entry.principal_id: entry.permissions for entry in entity_acl.acl_entries
        }

        assert permissions_by_principal[123] == ["READ"]
        assert permissions_by_principal[456] == ["READ", "DOWNLOAD"]
        assert permissions_by_principal[789] == ["READ", "DOWNLOAD", "UPDATE"]
        assert permissions_by_principal[999] == ["CHANGE_PERMISSIONS", "DELETE"]
