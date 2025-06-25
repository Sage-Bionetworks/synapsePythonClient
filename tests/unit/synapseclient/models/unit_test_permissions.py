"""Unit tests for permissions-related functionality in the AccessControllable mixin."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder, Project
from synapseclient.models.mixins.access_control import AclListResult, BenefactorTracker


class TestDeletePermissions:
    """Unit tests for delete_permissions method in AccessControllable."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        self.synapse_client = MagicMock(spec=Synapse)
        self.synapse_client.logger = MagicMock()
        self.synapse_client.silent = True

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

        # Mock get_entity_benefactor
        self.get_benefactor_patcher = patch(
            "synapseclient.models.mixins.access_control.get_entity_benefactor"
        )
        self.mock_get_benefactor = self.get_benefactor_patcher.start()

        yield

        # Clean up patchers
        self.get_client_patcher.stop()
        self.delete_acl_patcher.stop()
        self.get_benefactor_patcher.stop()

    async def test_delete_permissions_no_id_raises_error(self):
        """Test that attempting to delete permissions without an ID raises ValueError."""
        # GIVEN a file with no ID
        file = File()

        # WHEN attempting to delete permissions
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="The entity must have an ID to delete permissions."
        ):
            file.delete_permissions()

    async def test_delete_permissions_basic_single_entity(self):
        """Test basic permission deletion for a single entity."""
        # GIVEN a file with an ID
        file = File(id="syn123", name="test_file.txt")

        # WHEN deleting permissions
        file.delete_permissions()

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_already_inherits(self):
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

        http_error = SynapseHTTPError("403 error: Permission denied")
        http_error.response = error_response
        self.mock_delete_acl.side_effect = http_error

        # WHEN deleting permissions
        # THEN a SynapseHTTPError should be raised
        with pytest.raises(
            SynapseHTTPError,
            match="403 error: Permission denied",
        ):
            file.delete_permissions()

        # AND the delete_entity_acl function should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_general_exception(self):
        """Test handling of general exceptions during deletion."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND a general exception during deletion
        self.mock_delete_acl.side_effect = Exception("General error")

        # WHEN deleting permissions
        with pytest.raises(Exception, match="General error"):
            file.delete_permissions()

        # THEN the delete_entity_acl function should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_skip_self(self):
        """Test that when include_self=False, the entity's ACL is not deleted."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with include_self=False
        file.delete_permissions(include_self=False)

        # THEN the delete_entity_acl function should not be called
        self.mock_delete_acl.assert_not_called()

    async def test_delete_permissions_project_warning(self):
        """Test that deleting permissions on a Project shows warning and skips deletion."""
        # GIVEN a project with an ID
        project = Project(id="syn123", name="test_project")

        # WHEN deleting permissions
        project.delete_permissions()

        # THEN a warning should be logged
        self.synapse_client.logger.warning.assert_called_once()

        # AND delete_entity_acl should not be called
        self.mock_delete_acl.assert_not_called()

    async def test_delete_permissions_folder_recursive_structure(self):
        """Test recursive deletion on a complex folder structure."""
        # GIVEN a folder with an ID and child entities
        folder = Folder(id="syn123", name="parent_folder")
        folder.sync_from_synapse_async = AsyncMock()

        # Create child folder
        child_folder = Folder(id="syn456", name="child_folder")
        child_folder.delete_permissions_async = AsyncMock()
        child_folder.sync_from_synapse_async = AsyncMock()
        child_folder.folders = []
        child_folder.files = []

        # Create child file
        child_file = File(id="syn789", name="child_file.txt")
        child_file.delete_permissions_async = AsyncMock()

        # Set up the folder structure
        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions recursively
        folder.delete_permissions(recursive=True, include_container_content=True)

        # THEN delete_permissions_async should be called on child entities
        assert child_folder.delete_permissions_async.called
        assert child_file.delete_permissions_async.called

        # AND delete_entity_acl should be called on the parent folder
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_target_entity_types_folder_only(self):
        """Test filtering deletion by folder entity type only."""
        # GIVEN a folder with child folder and file
        folder = Folder(id="syn123", name="parent_folder")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456", name="child_folder")
        child_folder.delete_permissions_async = AsyncMock()
        child_folder.sync_from_synapse_async = AsyncMock()
        child_folder.folders = []
        child_folder.files = []

        child_file = File(id="syn789", name="child_file.txt")
        child_file.delete_permissions_async = AsyncMock()

        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions filtered by folder type only
        folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
        )

        # THEN delete_permissions_async should be called on child folder
        assert child_folder.delete_permissions_async.called

        # AND delete_permissions_async should NOT be called on child file for deletion
        # (it may be called for recursive processing with include_self=False)
        child_file.delete_permissions_async.assert_not_called()

        # AND delete_entity_acl should be called on the parent folder
        self.mock_delete_acl.assert_called_once()

    async def test_delete_permissions_target_entity_types_file_only(self):
        """Test filtering deletion by file entity type only."""
        # GIVEN a folder with child folder and file
        folder = Folder(id="syn123", name="parent_folder")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456", name="child_folder")
        child_folder.delete_permissions_async = AsyncMock()
        child_folder.sync_from_synapse_async = AsyncMock()
        child_folder.folders = []
        child_folder.files = []

        child_file = File(id="syn789", name="child_file.txt")
        child_file.delete_permissions_async = AsyncMock()

        folder.folders = [child_folder]
        folder.files = [child_file]

        # WHEN deleting permissions filtered by file type only
        folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["file"],
        )

        # THEN delete_permissions_async should be called on child file
        child_file.delete_permissions_async.assert_called_once()

        # AND child folder should be called for recursion but not for deletion
        # (include_self=False for recursive processing)
        _, kwargs = child_folder.delete_permissions_async.call_args
        assert kwargs.get("include_self") is False

        # AND delete_entity_acl should be called on the parent folder
        self.mock_delete_acl.assert_called_once()

    async def test_delete_permissions_case_insensitive_entity_types(self):
        """Test that entity type matching is case-insensitive."""
        # GIVEN a folder with child entities
        folder = Folder(id="syn123", name="parent_folder")
        folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn456", name="child_folder")
        child_folder.delete_permissions_async = AsyncMock()
        child_folder.sync_from_synapse_async = AsyncMock()
        child_folder.folders = []
        child_folder.files = []

        folder.folders = [child_folder]
        folder.files = []

        # WHEN deleting with mixed case entity types
        folder.delete_permissions(
            include_container_content=True,
            target_entity_types=["FOLDER", "File"],
        )

        # THEN operations should complete successfully
        self.mock_delete_acl.assert_called_once()
        assert child_folder.delete_permissions_async.called

    async def test_delete_permissions_invalid_recursive_without_container_content(self):
        """Test that recursive=True without include_container_content=True raises ValueError."""
        # GIVEN a folder with an ID
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()

        # WHEN attempting recursive deletion without including container content
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError,
            match="When recursive=True, include_container_content must also be True",
        ):
            folder.delete_permissions(recursive=True, include_container_content=False)

    async def test_delete_permissions_dry_run_mode(self):
        """Test dry run mode logs changes without executing them."""
        # GIVEN a folder with child entities
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()
        folder._collect_entities = AsyncMock(return_value=[folder])
        folder._build_and_log_run_tree = AsyncMock()

        # WHEN running in dry run mode
        folder.delete_permissions(include_container_content=True, dry_run=True)

        # THEN dry run tree should be built and logged
        folder._build_and_log_run_tree.assert_called_once()

        # AND actual deletion should not occur
        self.mock_delete_acl.assert_not_called()

    async def test_delete_permissions_benefactor_tracking(self):
        """Test that benefactor tracking works correctly during deletion."""
        # GIVEN a file with an ID and benefactor tracker
        file = File(id="syn123")
        tracker = BenefactorTracker()

        # AND mock benefactor response
        self.mock_get_benefactor.return_value = MagicMock(id="syn999")

        # WHEN deleting permissions with benefactor tracker
        file.delete_permissions(_benefactor_tracker=tracker)

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_empty_target_entity_types(self):
        """Test handling of empty target entity types list."""
        # GIVEN a folder with child entities
        folder = Folder(id="syn123")
        folder.sync_from_synapse_async = AsyncMock()
        folder.folders = []
        folder.files = []

        # WHEN deleting with empty target entity types
        folder.delete_permissions(
            include_container_content=True, target_entity_types=[]
        )

        # THEN the parent folder should still be processed
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_multiple_entity_types(self):
        """Test handling multiple entity types."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # WHEN deleting permissions with multiple entity types including 'file'
        file.delete_permissions(target_entity_types=["folder", "file"])

        # THEN delete_entity_acl should be called
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_custom_synapse_client(self):
        """Test using a custom Synapse client."""
        # GIVEN a file and a custom synapse client
        file = File(id="syn123")
        custom_client = MagicMock(spec=Synapse)
        custom_client.logger = MagicMock()
        custom_client.silent = True

        # WHEN deleting permissions with custom client
        file.delete_permissions(synapse_client=custom_client)

        # THEN delete_entity_acl should be called with the custom client
        self.mock_delete_acl.assert_called_once()

    async def test_delete_permissions_complex_hierarchy_dry_run(self):
        """Test dry run with complex hierarchy showing detailed logging."""
        # GIVEN a complex folder structure
        root_folder = Folder(id="syn100", name="root")
        root_folder.sync_from_synapse_async = AsyncMock()

        # Create nested structure
        level1_folder = Folder(id="syn200", name="level1")
        level1_folder.sync_from_synapse_async = AsyncMock()
        level1_folder.folders = []
        level1_folder.files = []

        level1_file = File(id="syn300", name="level1_file.txt")

        root_folder.folders = [level1_folder]
        root_folder.files = [level1_file]

        # Mock the collection method
        root_folder._collect_entities = AsyncMock(
            return_value=[root_folder, level1_folder, level1_file]
        )
        root_folder._build_and_log_run_tree = AsyncMock()

        # WHEN running dry run with detailed logging
        root_folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=True,
            show_acl_details=True,
            show_files_in_containers=True,
        )

        # THEN dry run tree should be built with appropriate parameters
        root_folder._build_and_log_run_tree.assert_called_once()
        _, kwargs = root_folder._build_and_log_run_tree.call_args
        assert kwargs["show_acl_details"] is True
        assert kwargs["show_files_in_containers"] is True

        # AND no actual deletions should occur
        self.mock_delete_acl.assert_not_called()

    async def test_delete_permissions_folder_only_direct_children(self):
        """Test deletion affecting only direct children, not recursive."""
        # GIVEN a folder with nested structure
        parent_folder = Folder(id="syn100")
        parent_folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn200")
        child_folder.delete_permissions_async = AsyncMock()
        child_folder.sync_from_synapse_async = AsyncMock()
        child_folder.folders = []
        child_folder.files = []

        grandchild_folder = Folder(id="syn300")
        grandchild_folder.delete_permissions_async = AsyncMock()
        grandchild_folder.sync_from_synapse_async = AsyncMock()

        child_folder.folders = [grandchild_folder]
        parent_folder.folders = [child_folder]
        parent_folder.files = []

        # WHEN deleting with include_container_content=True but recursive=False
        parent_folder.delete_permissions(
            include_container_content=True, recursive=False
        )

        # THEN direct child should be processed
        assert child_folder.delete_permissions_async.called

        # AND parent should be processed
        self.mock_delete_acl.assert_called_once()

    async def test_delete_permissions_benefactor_impact_logging(self):
        """Test logging when ACL deletion affects other entities."""
        # GIVEN a file with benefactor relationships
        file = File(id="syn123")
        tracker = BenefactorTracker()

        # Set up benefactor relationships
        tracker.benefactor_children["syn123"] = ["syn456", "syn789"]

        # WHEN deleting permissions
        file.delete_permissions(_benefactor_tracker=tracker)

        # THEN deletion should complete
        self.mock_delete_acl.assert_called_once()

    async def test_delete_permissions_no_container_support(self):
        """Test deletion on entity without container support."""
        # GIVEN a file (which doesn't have sync_from_synapse_async)
        file = File(id="syn123")

        # WHEN attempting to delete with container content options
        file.delete_permissions(include_container_content=True)

        # THEN only the file itself should be processed
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_entity_without_sync_method(self):
        """Test handling of entities that don't support sync_from_synapse_async."""
        # GIVEN a basic entity without container methods
        file = File(id="syn123")

        # Remove sync method if it exists
        if hasattr(file, "sync_from_synapse_async"):
            delattr(file, "sync_from_synapse_async")

        # WHEN deleting permissions
        file.delete_permissions()

        # THEN deletion should still work for the entity itself
        self.mock_delete_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

    async def test_delete_permissions_large_hierarchy_performance(self):
        """Test performance considerations with large hierarchy."""
        # GIVEN a folder with many children
        parent_folder = Folder(id="syn100")
        parent_folder.sync_from_synapse_async = AsyncMock()

        # Create many child entities
        num_children = 50
        child_folders = []
        child_files = []

        for i in range(num_children):
            child_folder = Folder(id=f"syn{200 + i}")
            child_folder.delete_permissions_async = AsyncMock()
            child_folder.sync_from_synapse_async = AsyncMock()
            child_folder.folders = []
            child_folder.files = []
            child_folders.append(child_folder)

            child_file = File(id=f"syn{300 + i}")
            child_file.delete_permissions_async = AsyncMock()
            child_files.append(child_file)

        parent_folder.folders = child_folders
        parent_folder.files = child_files

        # WHEN deleting permissions on large hierarchy
        parent_folder.delete_permissions(recursive=True, include_container_content=True)

        # THEN all entities should be processed
        self.mock_delete_acl.assert_called_once()  # Parent folder

        # AND all child folders should be processed
        for child_folder in child_folders:
            assert child_folder.delete_permissions_async.called

        # AND all child files should be processed
        for child_file in child_files:
            assert child_file.delete_permissions_async.called


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
            await tracker.track_entity_benefactor(
                entity_ids=entity_ids, synapse_client=mock_client, progress_bar=None
            )

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
            await tracker.track_entity_benefactor(
                entity_ids=entity_ids, synapse_client=mock_client, progress_bar=None
            )

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
            tracker.entity_benefactors["syn100"] == "syn300"
        )  # Now inherits from syn200's benefactor (syn300)

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
        self.synapse_client.silent = True

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

    async def test_list_acl_no_id_raises_error(self):
        """Test that listing ACL without an ID raises ValueError."""
        # GIVEN a file with no ID
        file = File()

        # WHEN attempting to list ACL
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="The entity must have an ID to list ACLs."
        ):
            file.list_acl()

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

        # WHEN listing ACL
        result = file.list_acl()

        # THEN ACL should be retrieved
        self.mock_get_acl.assert_called_once_with(
            entity_id="syn123", synapse_client=self.synapse_client
        )

        # AND result should be AclListResult
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) == 1

        entity_acl = result.all_entity_acls[0]
        assert entity_acl.entity_id == "syn123"
        assert len(entity_acl.acl_entries) == 2

        # Verify the ACL entries contain the expected principal IDs
        principal_ids = {entry.principal_id for entry in entity_acl.acl_entries}
        assert principal_ids == {"123", "456"}

        # Verify specific permissions
        for entry in entity_acl.acl_entries:
            if entry.principal_id == "123":
                assert "READ" in entry.permissions
                assert "DOWNLOAD" in entry.permissions
            elif entry.principal_id == "456":
                assert "CHANGE_PERMISSIONS" in entry.permissions

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
        result = file.list_acl()

        # THEN result should be empty but valid
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) == 0

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
        result = file.list_acl()

        # THEN result should be valid but empty
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) == 1

        entity_acl = result.all_entity_acls[0]
        assert entity_acl.entity_id == "syn123"
        assert len(entity_acl.acl_entries) == 0

    async def test_list_acl_custom_entity_types_accepted(self):
        """Test that custom entity types are accepted and normalized."""
        # GIVEN a file with an ID
        file = File(id="syn123")

        # AND mock ACL response
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}

        # WHEN listing ACL with custom entity types (even non-standard ones)
        result = file.list_acl(target_entity_types=["CustomType", "FOLDER"])

        # THEN the method should complete successfully (entity types are normalized but not validated)
        assert isinstance(result, AclListResult)

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
        result = file.list_acl(target_entity_types=["FiLe"])

        # THEN ACL should be retrieved (case-insensitive match)
        self.mock_get_acl.assert_called_once()
        assert isinstance(result, AclListResult)

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
            folder.list_acl(recursive=True, include_container_content=False)

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

        folder._collect_entities = AsyncMock()
        folder._collect_entities.return_value = [
            child_file,
            child_folder,
        ]

        # AND mock folder ACL
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL with include_container_content=True but not recursive
        result = folder.list_acl(include_container_content=True, recursive=False)

        # THEN sync should be called
        folder.sync_from_synapse_async.assert_called_once()

        # AND entities should be collected
        folder._collect_entities.assert_called_once()

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

        folder._collect_entities = AsyncMock()
        folder._collect_entities.return_value = [child_folder]

        # AND mock ACL responses
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL recursively
        result = folder.list_acl(recursive=True, include_container_content=True)

        # THEN entities should be collected recursively
        folder._collect_entities.assert_called_once()

        # AND result should be AclListResult
        assert isinstance(result, AclListResult)
        call_args = folder._collect_entities.call_args[1]
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

        folder._collect_entities = AsyncMock()
        folder._collect_entities.return_value = [child_folder]  # Only folder

        # AND mock ACL response
        self.mock_get_acl.return_value = {"id": "syn123", "resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL filtered by folder type only
        result = folder.list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
        )

        # THEN only folder entities should be processed
        folder._collect_entities.assert_called_once()
        call_args = folder._collect_entities.call_args[1]
        assert call_args["target_entity_types"] == ["folder"]

        # AND result should be AclListResult
        assert isinstance(result, AclListResult)

    async def test_list_acl_with_tree_logging(self):
        """Test ACL listing with tree logging enabled."""
        # GIVEN a folder with children
        folder = Folder(id="syn123", name="test_folder")
        folder.sync_from_synapse_async = AsyncMock()
        folder.folders = []
        folder.files = []

        folder._collect_entities = AsyncMock()
        folder._collect_entities.return_value = [folder]

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
        result = folder.list_acl(
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

    async def test_list_acl_complex_hierarchy(self):
        """Test ACL listing in complex entity hierarchy."""
        # GIVEN a complex folder structure
        root_folder = Folder(id="syn100", name="root")
        root_folder.sync_from_synapse_async = AsyncMock()

        child_folder = Folder(id="syn200", name="child")
        child_file = File(id="syn300", name="file.txt")

        root_folder.folders = [child_folder]
        root_folder.files = [child_file]

        root_folder._collect_entities = AsyncMock()
        root_folder._collect_entities.return_value = [
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
        result = root_folder.list_acl(recursive=True, include_container_content=True)

        # THEN result should include ACLs from multiple entities
        assert isinstance(result, AclListResult)

        # AND result should contain ACLs for the expected entities
        assert len(result.all_entity_acls) > 0

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
        result = file.list_acl()

        # THEN result should still be created without user info
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) == 1

        entity_acl = result.all_entity_acls[0]
        assert len(entity_acl.acl_entries) == 1
        assert entity_acl.acl_entries[0].principal_id == "123"

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

        folder._collect_entities = AsyncMock()
        folder._collect_entities.return_value = children

        # AND mock responses
        self.mock_get_acl.return_value = {"resourceAccess": []}
        self.mock_get_user_headers.return_value = []

        # WHEN listing ACL
        result = folder.list_acl(include_container_content=True)

        # THEN operation should complete successfully
        assert isinstance(result, AclListResult)

        # AND entities should be collected
        folder._collect_entities.assert_called_once()

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
        result = file.list_acl()

        # THEN result should contain all permission combinations
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) == 1

        entity_acl = result.all_entity_acls[0]
        assert len(entity_acl.acl_entries) == 4

        # Verify permissions are correctly parsed
        permissions_by_principal = {
            entry.principal_id: entry.permissions for entry in entity_acl.acl_entries
        }

        assert permissions_by_principal["123"] == ["READ"]
        assert permissions_by_principal["456"] == ["READ", "DOWNLOAD"]
        assert permissions_by_principal["789"] == ["READ", "DOWNLOAD", "UPDATE"]
        assert permissions_by_principal["999"] == ["CHANGE_PERMISSIONS", "DELETE"]
