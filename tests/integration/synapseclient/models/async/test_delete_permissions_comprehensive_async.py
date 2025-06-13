"""
Comprehensive integration tests for the delete_permissions_async functionality.

This module tests all possible code paths and tree structures for the
delete_permissions_async method, focusing on verifying the actual changes
when dry_run=False. These tests cover various folder/file hierarchies,
nested structures, and edge cases.
"""

import asyncio
import uuid
from typing import Callable, Dict, List, Union

import pytest

from synapseclient import Synapse
from synapseclient.client import AUTHENTICATED_USERS
from synapseclient.core import utils
from synapseclient.models import File, Folder, Project

# TODO: Consolidate with `test_permissions_async.py`


class TestDeletePermissionsComprehensive:
    """Comprehensive test class for delete_permissions_async functionality."""

    @pytest.fixture(autouse=True, scope="function")
    def init(
        self,
        syn: Synapse,
        syn_with_logger: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup
        self.syn_with_logger = syn_with_logger

    @pytest.fixture(scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(path=filename)

    async def _set_custom_permissions(
        self, entity: Union[File, Folder, Project]
    ) -> None:
        """Helper to set custom permissions on an entity so we can verify deletion."""
        await entity.set_permissions_async(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ"],
            synapse_client=self.syn,
        )

        # Verify permissions were set
        acl = await entity.get_acl_async(
            principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
        )
        assert "READ" in acl

    async def _verify_permissions_deleted(
        self, entity: Union[File, Folder, Project]
    ) -> None:
        """Helper to verify that permissions have been deleted (entity inherits from parent)."""
        acl = await entity.get_acl_async(
            principal_id=AUTHENTICATED_USERS,
            check_benefactor=False,
            synapse_client=self.syn,
        )

        assert not acl, (
            f"Permissions should be deleted, but they still exist on "
            f"[id: {entity.id}, name: {entity.name}, {entity.__class__}]."
        )

    async def _verify_permissions_not_deleted(
        self, entity: Union[File, Folder, Project]
    ) -> None:
        """Helper to verify that permissions are still set on an entity."""
        acl = await entity.get_acl_async(
            principal_id=AUTHENTICATED_USERS,
            check_benefactor=False,
            synapse_client=self.syn,
        )
        assert "READ" in acl

    async def create_simple_tree_structure(
        self, project_model: Project
    ) -> Dict[str, Union[Folder, File]]:
        """
        Create a simple 2-level tree structure.

        Structure:
        ```
        Project
        └── folder_a
            └── file_1
        ```
        """
        folder_a = await Folder(name=f"folder_a_{uuid.uuid4()}").store_async(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder_a.id)

        file_1 = await File(
            path=utils.make_bogus_uuid_file(), name=f"file_1_{uuid.uuid4()}"
        ).store_async(parent=folder_a, synapse_client=self.syn)
        self.schedule_for_cleanup(file_1.id)

        return {
            "folder_a": folder_a,
            "file_1": file_1,
        }

    async def create_deep_nested_structure(
        self, project_model: Project
    ) -> Dict[str, Union[Folder, File]]:
        """
        Create a deeply nested folder structure with files at various levels.

        Structure:
        ```
        Project
        └── level_1
            ├── file_at_1
            └── level_2
                ├── file_at_2
                └── level_3
                    ├── file_at_3
                    └── level_4
                        └── file_at_4
        ```
        """
        level_1 = await Folder(name=f"level_1_{uuid.uuid4()}").store_async(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(level_1.id)

        # Create file_at_1 and level_2 in parallel since they don't depend on each other
        file_at_1_task = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_1_{uuid.uuid4()}"
        ).store_async(parent=level_1, synapse_client=self.syn)
        level_2_task = Folder(name=f"level_2_{uuid.uuid4()}").store_async(
            parent=level_1, synapse_client=self.syn
        )

        file_at_1, level_2 = await asyncio.gather(file_at_1_task, level_2_task)
        self.schedule_for_cleanup(file_at_1.id)
        self.schedule_for_cleanup(level_2.id)

        # Create file_at_2 and level_3 in parallel since they don't depend on each other
        file_at_2_task = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_2_{uuid.uuid4()}"
        ).store_async(parent=level_2, synapse_client=self.syn)
        level_3_task = Folder(name=f"level_3_{uuid.uuid4()}").store_async(
            parent=level_2, synapse_client=self.syn
        )

        file_at_2, level_3 = await asyncio.gather(file_at_2_task, level_3_task)
        self.schedule_for_cleanup(file_at_2.id)
        self.schedule_for_cleanup(level_3.id)

        # Create file_at_3 and level_4 in parallel since they don't depend on each other
        file_at_3_task = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_3_{uuid.uuid4()}"
        ).store_async(parent=level_3, synapse_client=self.syn)
        level_4_task = Folder(name=f"level_4_{uuid.uuid4()}").store_async(
            parent=level_3, synapse_client=self.syn
        )

        file_at_3, level_4 = await asyncio.gather(file_at_3_task, level_4_task)
        self.schedule_for_cleanup(file_at_3.id)
        self.schedule_for_cleanup(level_4.id)

        file_at_4 = await File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_4_{uuid.uuid4()}"
        ).store_async(parent=level_4, synapse_client=self.syn)
        self.schedule_for_cleanup(file_at_4.id)

        return {
            "level_1": level_1,
            "level_2": level_2,
            "level_3": level_3,
            "level_4": level_4,
            "file_at_1": file_at_1,
            "file_at_2": file_at_2,
            "file_at_3": file_at_3,
            "file_at_4": file_at_4,
        }

    async def create_wide_tree_structure(
        self, project_model: Project
    ) -> Dict[str, Union[Folder, List[Union[Folder, File]]]]:
        """
        Create a wide tree structure with multiple siblings.

        Structure:
        ```
        Project
        ├── folder_a
        │   └── file_a
        ├── folder_b
        │   └── file_b
        ├── folder_c
        │   └── file_c
        └── root_file
        ```
        """
        # Create folders in parallel
        folder_tasks = [
            Folder(name=f"folder_{folder_letter}_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            )
            for folder_letter in ["a", "b", "c"]
        ]
        folders = await asyncio.gather(*folder_tasks)

        # Schedule cleanup for folders
        for folder in folders:
            self.schedule_for_cleanup(folder.id)

        # Create files in parallel
        file_tasks = [
            File(
                path=utils.make_bogus_uuid_file(),
                name=f"file_{folder_letter}_{uuid.uuid4()}",
            ).store_async(parent=folder, synapse_client=self.syn)
            for folder_letter, folder in zip(["a", "b", "c"], folders)
        ]

        # Create root file task
        root_file_task = File(
            path=utils.make_bogus_uuid_file(), name=f"root_file_{uuid.uuid4()}"
        ).store_async(parent=project_model, synapse_client=self.syn)

        # Execute file creation tasks in parallel
        file_results = await asyncio.gather(*file_tasks, root_file_task)
        all_files = file_results[:-1]  # All but the last (root file)
        root_file = file_results[-1]  # The last one (root file)

        # Schedule cleanup for files
        for file in all_files:
            self.schedule_for_cleanup(file.id)
        self.schedule_for_cleanup(root_file.id)

        return {
            "folders": folders,
            "all_files": all_files,
            "root_file": root_file,
        }

    async def create_complex_mixed_structure(
        self, project_model: Project
    ) -> Dict[str, Union[Folder, File, List]]:
        """
        Create a complex mixed structure combining depth and width.

        Structure:
        ```
        Project
        ├── shallow_folder
        │   └── shallow_file
        ├── deep_branch
        │   ├── deep_file_1
        │   └── sub_deep
        │       ├── deep_file_2
        │       └── sub_sub_deep
        │           └── deep_file_3
        └── mixed_folder
            ├── mixed_file
            ├── mixed_sub_a
            │   └── mixed_file_a
            └── mixed_sub_b
                └── mixed_file_b
        ```
        """
        # Create top-level folders in parallel
        top_folder_tasks = [
            Folder(name=f"shallow_folder_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            ),
            Folder(name=f"deep_branch_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            ),
            Folder(name=f"mixed_folder_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            ),
        ]
        shallow_folder, deep_branch, mixed_folder = await asyncio.gather(
            *top_folder_tasks
        )

        # Schedule cleanup for top-level folders
        for folder in [shallow_folder, deep_branch, mixed_folder]:
            self.schedule_for_cleanup(folder.id)

        # Create first level files and folders
        shallow_file = await File(
            path=utils.make_bogus_uuid_file(), name=f"shallow_file_{uuid.uuid4()}"
        ).store_async(parent=shallow_folder, synapse_client=self.syn)
        self.schedule_for_cleanup(shallow_file.id)

        # Deep branch structure
        deep_file_1_task = File(
            path=utils.make_bogus_uuid_file(), name=f"deep_file_1_{uuid.uuid4()}"
        ).store_async(parent=deep_branch, synapse_client=self.syn)

        sub_deep_task = Folder(name=f"sub_deep_{uuid.uuid4()}").store_async(
            parent=deep_branch, synapse_client=self.syn
        )

        deep_file_1, sub_deep = await asyncio.gather(deep_file_1_task, sub_deep_task)
        self.schedule_for_cleanup(deep_file_1.id)
        self.schedule_for_cleanup(sub_deep.id)

        # Continue deep structure
        deep_file_2_task = File(
            path=utils.make_bogus_uuid_file(), name=f"deep_file_2_{uuid.uuid4()}"
        ).store_async(parent=sub_deep, synapse_client=self.syn)

        sub_sub_deep_task = Folder(name=f"sub_sub_deep_{uuid.uuid4()}").store_async(
            parent=sub_deep, synapse_client=self.syn
        )

        deep_file_2, sub_sub_deep = await asyncio.gather(
            deep_file_2_task, sub_sub_deep_task
        )
        self.schedule_for_cleanup(deep_file_2.id)
        self.schedule_for_cleanup(sub_sub_deep.id)

        deep_file_3 = await File(
            path=utils.make_bogus_uuid_file(), name=f"deep_file_3_{uuid.uuid4()}"
        ).store_async(parent=sub_sub_deep, synapse_client=self.syn)
        self.schedule_for_cleanup(deep_file_3.id)

        # Mixed folder structure
        mixed_file_task = File(
            path=utils.make_bogus_uuid_file(), name=f"mixed_file_{uuid.uuid4()}"
        ).store_async(parent=mixed_folder, synapse_client=self.syn)

        mixed_sub_a_task = Folder(name=f"mixed_sub_a_{uuid.uuid4()}").store_async(
            parent=mixed_folder, synapse_client=self.syn
        )
        mixed_sub_b_task = Folder(name=f"mixed_sub_b_{uuid.uuid4()}").store_async(
            parent=mixed_folder, synapse_client=self.syn
        )

        mixed_file, mixed_sub_a, mixed_sub_b = await asyncio.gather(
            mixed_file_task, mixed_sub_a_task, mixed_sub_b_task
        )

        # Schedule cleanup
        self.schedule_for_cleanup(mixed_file.id)
        self.schedule_for_cleanup(mixed_sub_a.id)
        self.schedule_for_cleanup(mixed_sub_b.id)

        # Create files in mixed sub-folders in parallel
        mixed_file_a_task = File(
            path=utils.make_bogus_uuid_file(), name=f"mixed_file_a_{uuid.uuid4()}"
        ).store_async(parent=mixed_sub_a, synapse_client=self.syn)

        mixed_file_b_task = File(
            path=utils.make_bogus_uuid_file(), name=f"mixed_file_b_{uuid.uuid4()}"
        ).store_async(parent=mixed_sub_b, synapse_client=self.syn)

        mixed_file_a, mixed_file_b = await asyncio.gather(
            mixed_file_a_task, mixed_file_b_task
        )
        self.schedule_for_cleanup(mixed_file_a.id)
        self.schedule_for_cleanup(mixed_file_b.id)

        return {
            "shallow_folder": shallow_folder,
            "shallow_file": shallow_file,
            "deep_branch": deep_branch,
            "sub_deep": sub_deep,
            "sub_sub_deep": sub_sub_deep,
            "deep_files": [deep_file_1, deep_file_2, deep_file_3],
            "mixed_folder": mixed_folder,
            "mixed_sub_folders": [mixed_sub_a, mixed_sub_b],
            "mixed_files": [mixed_file, mixed_file_a, mixed_file_b],
        }

    # Basic functionality tests
    async def test_delete_permissions_simple_tree_structure(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a simple tree structure."""
        # GIVEN a simple tree structure with permissions
        structure = await self.create_simple_tree_structure(project_model)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities using asyncio.gather for parallel execution
        await asyncio.gather(
            self._set_custom_permissions(folder_a),
            self._set_custom_permissions(file_1),
        )

        # WHEN I delete permissions recursively
        await folder_a.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_deleted(folder_a),
            self._verify_permissions_deleted(file_1),
        )

    async def test_delete_permissions_deep_nested_structure(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a deeply nested structure."""
        # GIVEN a deeply nested structure with permissions
        structure = await self.create_deep_nested_structure(project_model)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in structure.values()]
        )

        # WHEN I delete permissions recursively from the top level
        await structure["level_1"].delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in structure.values()]
        )

    async def test_delete_permissions_wide_tree_structure(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a wide tree structure with multiple siblings."""
        # GIVEN a wide tree structure with permissions
        structure = await self.create_wide_tree_structure(project_model)
        folders = structure["folders"]
        all_files = structure["all_files"]
        root_file = structure["root_file"]

        # Set permissions on all entities using asyncio.gather
        entities_to_set = folders + all_files + [root_file]
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )

        # WHEN I delete permissions recursively from the project
        await project_model.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted (except project which can't be deleted)
        entities_to_verify = folders + all_files + [root_file]
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in entities_to_verify]
        )

    async def test_delete_permissions_complex_mixed_structure(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a complex mixed structure."""
        # GIVEN a complex mixed structure with permissions
        structure = await self.create_complex_mixed_structure(project_model)

        # Set permissions on all entities using asyncio.gather
        entities_to_set = (
            [
                structure["shallow_folder"],
                structure["shallow_file"],
                structure["deep_branch"],
                structure["sub_deep"],
                structure["sub_sub_deep"],
                structure["mixed_folder"],
            ]
            + structure["deep_files"]
            + structure["mixed_sub_folders"]
            + structure["mixed_files"]
        )

        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )

        # WHEN I delete permissions recursively from the project
        await project_model.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in entities_to_set]
        )

    # Edge case tests
    async def test_delete_permissions_empty_folder(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on an empty folder."""
        # GIVEN an empty folder with custom permissions
        empty_folder = await Folder(name=f"empty_folder_{uuid.uuid4()}").store_async(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(empty_folder.id)
        await self._set_custom_permissions(empty_folder)

        # WHEN I delete permissions recursively
        await empty_folder.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN the folder permissions should be deleted
        await self._verify_permissions_deleted(empty_folder)

    async def test_delete_permissions_folder_with_only_files(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a folder that contains only files."""
        # GIVEN a folder with only one file
        folder = await Folder(name=f"files_only_folder_{uuid.uuid4()}").store_async(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        file = await File(
            path=utils.make_bogus_uuid_file(), name=f"only_file_{uuid.uuid4()}"
        ).store_async(parent=folder, synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(folder),
            self._set_custom_permissions(file),
        )

        # WHEN I delete permissions recursively
        await folder.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_deleted(folder),
            self._verify_permissions_deleted(file),
        )

    async def test_delete_permissions_folder_with_only_folders(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a folder that contains only sub-folders."""
        # GIVEN a folder with only sub-folders
        parent_folder = await Folder(
            name=f"folders_only_parent_{uuid.uuid4()}"
        ).store_async(parent=project_model, synapse_client=self.syn)
        self.schedule_for_cleanup(parent_folder.id)

        # Create sub-folders in parallel
        sub_folder_tasks = [
            Folder(name=f"only_subfolder_{i}_{uuid.uuid4()}").store_async(
                parent=parent_folder, synapse_client=self.syn
            )
            for i in range(3)
        ]
        sub_folders = await asyncio.gather(*sub_folder_tasks)

        # Schedule cleanup for sub-folders
        for sub_folder in sub_folders:
            self.schedule_for_cleanup(sub_folder.id)

        # Set permissions on all entities using asyncio.gather
        entities_to_set = [parent_folder] + sub_folders
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )

        # WHEN I delete permissions recursively
        await parent_folder.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in entities_to_set]
        )

    # Target entity type filtering tests
    async def test_delete_permissions_target_folders_only_complex(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions targeting only folders in a complex structure."""
        # GIVEN a complex structure with permissions
        structure = await self.create_complex_mixed_structure(project_model)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(structure["shallow_folder"]),
            self._set_custom_permissions(structure["shallow_file"]),
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["sub_deep"]),
            *[self._set_custom_permissions(file) for file in structure["deep_files"]],
        )

        # WHEN I delete permissions targeting only folders
        await project_model.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only folder permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_deleted(structure["shallow_folder"]),
            self._verify_permissions_deleted(structure["deep_branch"]),
            self._verify_permissions_deleted(structure["sub_deep"]),
        )

        # BUT file permissions should remain
        await asyncio.gather(
            self._verify_permissions_not_deleted(structure["shallow_file"]),
            *[
                self._verify_permissions_not_deleted(file)
                for file in structure["deep_files"]
            ],
        )

    async def test_delete_permissions_target_files_only_complex(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions targeting only files in a complex structure."""
        # GIVEN a complex structure with permissions
        structure = await self.create_complex_mixed_structure(project_model)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(structure["shallow_folder"]),
            self._set_custom_permissions(structure["shallow_file"]),
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["sub_deep"]),
            *[self._set_custom_permissions(file) for file in structure["deep_files"]],
        )

        # WHEN I delete permissions targeting only files
        await project_model.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["file"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only file permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_deleted(structure["shallow_file"]),
            *[
                self._verify_permissions_deleted(file)
                for file in structure["deep_files"]
            ],
        )

        # BUT folder permissions should remain
        await asyncio.gather(
            self._verify_permissions_not_deleted(structure["shallow_folder"]),
            self._verify_permissions_not_deleted(structure["deep_branch"]),
            self._verify_permissions_not_deleted(structure["sub_deep"]),
        )

    # Include container content vs recursive tests
    async def test_delete_permissions_include_container_only_deep_structure(
        self, project_model: Project
    ) -> None:
        """Test include_container_content=True without recursive on deep structure."""
        # GIVEN a deep nested structure with permissions
        structure = await self.create_deep_nested_structure(project_model)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in structure.values()]
        )

        # WHEN I delete permissions with include_container_content=True but recursive=False
        await structure["level_1"].delete_permissions_async(
            include_self=True,
            include_container_content=True,
            recursive=False,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only level_1 and its direct children should have permissions deleted
        await asyncio.gather(
            self._verify_permissions_deleted(structure["level_1"]),
            self._verify_permissions_deleted(structure["file_at_1"]),
            self._verify_permissions_deleted(structure["level_2"]),
        )

        # BUT deeper nested entities should retain permissions
        await asyncio.gather(
            self._verify_permissions_not_deleted(structure["level_3"]),
            self._verify_permissions_not_deleted(structure["level_4"]),
            self._verify_permissions_not_deleted(structure["file_at_2"]),
            self._verify_permissions_not_deleted(structure["file_at_3"]),
            self._verify_permissions_not_deleted(structure["file_at_4"]),
        )

    async def test_delete_permissions_skip_self_complex_structure(
        self, project_model: Project
    ) -> None:
        """Test include_self=False on a complex structure."""
        # GIVEN a complex mixed structure with permissions
        structure = await self.create_complex_mixed_structure(project_model)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(structure["mixed_folder"]),
            *[
                self._set_custom_permissions(folder)
                for folder in structure["mixed_sub_folders"]
            ],
            *[self._set_custom_permissions(file) for file in structure["mixed_files"]],
        )

        # WHEN I delete permissions with include_self=False
        await structure["mixed_folder"].delete_permissions_async(
            include_self=False,
            include_container_content=True,
            recursive=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN the mixed_folder permissions should remain
        await self._verify_permissions_not_deleted(structure["mixed_folder"])

        # BUT child permissions should be deleted using asyncio.gather
        await asyncio.gather(
            *[
                self._verify_permissions_deleted(folder)
                for folder in structure["mixed_sub_folders"]
            ],
            *[
                self._verify_permissions_deleted(file)
                for file in structure["mixed_files"]
            ],
        )

    # Dry run functionality tests
    async def test_delete_permissions_dry_run_no_changes(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that dry_run=True makes no actual changes."""
        # GIVEN a simple structure with permissions
        structure = await self.create_simple_tree_structure(project_model)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(folder_a),
            self._set_custom_permissions(file_1),
        )

        # WHEN I run delete_permissions with dry_run=True
        await folder_a.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=True,
            synapse_client=self.syn_with_logger,
        )

        # THEN no permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_not_deleted(folder_a),
            self._verify_permissions_not_deleted(file_1),
        )

        # AND dry run messages should be logged
        assert "DRY RUN" in caplog.text

    async def test_delete_permissions_dry_run_complex_logging(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test dry run logging for complex structures."""
        # GIVEN a complex structure with permissions
        structure = await self.create_complex_mixed_structure(project_model)

        # Set permissions on a subset of entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["sub_deep"]),
            self._set_custom_permissions(structure["deep_files"][0]),
        )

        # WHEN I run delete_permissions with dry_run=True
        await structure["deep_branch"].delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=True,
            show_acl_details=True,
            show_files_in_containers=True,
            synapse_client=self.syn_with_logger,
        )

        # THEN no permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_not_deleted(structure["deep_branch"]),
            self._verify_permissions_not_deleted(structure["sub_deep"]),
            self._verify_permissions_not_deleted(structure["deep_files"][0]),
        )

        # AND comprehensive dry run analysis should be logged
        assert "DRY RUN: Permission Deletion Impact Analysis" in caplog.text
        assert "End of Dry Run Analysis" in caplog.text

    # Performance and stress tests
    async def test_delete_permissions_large_flat_structure(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a large flat structure."""
        # GIVEN a folder with many files
        large_folder = await Folder(name=f"large_folder_{uuid.uuid4()}").store_async(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(large_folder.id)

        # Create files in parallel
        file_tasks = [
            File(
                path=utils.make_bogus_uuid_file(), name=f"large_file_{i}_{uuid.uuid4()}"
            ).store_async(parent=large_folder, synapse_client=self.syn)
            for i in range(10)  # Reduced from larger number for test performance
        ]
        files = await asyncio.gather(*file_tasks)

        # Schedule cleanup for files
        for file in files:
            self.schedule_for_cleanup(file.id)

        # Set permissions on all entities using asyncio.gather
        entities_to_set = [large_folder] + files
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )

        # WHEN I delete permissions recursively
        await large_folder.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in entities_to_set]
        )

    async def test_delete_permissions_multiple_nested_branches(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on multiple nested branches simultaneously."""
        # GIVEN multiple complex nested branches
        branch_tasks = [
            Folder(name=f"branch_{branch_name}_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            )
            for branch_name in ["alpha", "beta", "gamma"]
        ]
        branches = await asyncio.gather(*branch_tasks)

        all_entities = list(branches)

        # Schedule cleanup for branches
        for branch in branches:
            self.schedule_for_cleanup(branch.id)

        # Create nested structure in each branch in parallel
        nested_tasks = []
        for branch_name, branch in zip(["alpha", "beta", "gamma"], branches):
            current_parent = branch
            for level in range(3):
                # Create sub-folder and file tasks for this level
                sub_folder_task = Folder(
                    name=f"{branch_name}_level_{level}_{uuid.uuid4()}"
                ).store_async(parent=current_parent, synapse_client=self.syn)

                nested_tasks.append(sub_folder_task)

        # Execute all nested folder creation tasks in parallel
        nested_folders = await asyncio.gather(*nested_tasks)

        # Add nested folders to all_entities and schedule cleanup
        all_entities.extend(nested_folders)
        for folder in nested_folders:
            self.schedule_for_cleanup(folder.id)

        # Now create files for each nested folder in parallel
        file_tasks = []
        folder_index = 0
        for branch_name in ["alpha", "beta", "gamma"]:
            for level in range(3):
                parent_folder = nested_folders[folder_index]
                file_task = File(
                    path=utils.make_bogus_uuid_file(),
                    name=f"{branch_name}_file_{level}_{uuid.uuid4()}",
                ).store_async(parent=parent_folder, synapse_client=self.syn)
                file_tasks.append(file_task)
                folder_index += 1

        # Execute all file creation tasks in parallel
        files = await asyncio.gather(*file_tasks)

        # Add files to all_entities and schedule cleanup
        all_entities.extend(files)
        for file in files:
            self.schedule_for_cleanup(file.id)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in all_entities]
        )

        # WHEN I delete permissions recursively from the project
        await project_model.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted using asyncio.gather
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in all_entities]
        )

    # Selective deletion tests
    async def test_delete_permissions_selective_branches(
        self, project_model: Project
    ) -> None:
        """Test selectively deleting permissions from specific branches."""
        # GIVEN multiple branches with permissions
        # Create branches in parallel
        branch_tasks = [
            Folder(name=f"branch_a_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            ),
            Folder(name=f"branch_b_{uuid.uuid4()}").store_async(
                parent=project_model, synapse_client=self.syn
            ),
        ]
        branch_a, branch_b = await asyncio.gather(*branch_tasks)

        # Schedule cleanup for branches
        self.schedule_for_cleanup(branch_a.id)
        self.schedule_for_cleanup(branch_b.id)

        # Create files in each branch in parallel
        file_tasks = [
            File(
                path=utils.make_bogus_uuid_file(), name=f"file_a_{uuid.uuid4()}"
            ).store_async(parent=branch_a, synapse_client=self.syn),
            File(
                path=utils.make_bogus_uuid_file(), name=f"file_b_{uuid.uuid4()}"
            ).store_async(parent=branch_b, synapse_client=self.syn),
        ]
        file_a, file_b = await asyncio.gather(*file_tasks)

        # Schedule cleanup for files
        self.schedule_for_cleanup(file_a.id)
        self.schedule_for_cleanup(file_b.id)

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(branch_a),
            self._set_custom_permissions(branch_b),
            self._set_custom_permissions(file_a),
            self._set_custom_permissions(file_b),
        )

        # WHEN I delete permissions only from branch_a
        await branch_a.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only branch_a and its contents should have permissions deleted
        await asyncio.gather(
            self._verify_permissions_deleted(branch_a),
            self._verify_permissions_deleted(file_a),
        )

        # BUT branch_b should retain permissions
        await asyncio.gather(
            self._verify_permissions_not_deleted(branch_b),
            self._verify_permissions_not_deleted(file_b),
        )

    async def test_delete_permissions_mixed_entity_types_in_structure(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions with mixed entity types in complex structure."""
        # GIVEN a structure with both files and folders at multiple levels
        structure = await self.create_complex_mixed_structure(project_model)

        # Set permissions on a mix of entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(structure["shallow_folder"]),
            self._set_custom_permissions(structure["shallow_file"]),
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["deep_files"][1]),
            self._set_custom_permissions(structure["mixed_sub_folders"][0]),
        )

        # WHEN I delete permissions targeting both files and folders
        await project_model.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["file", "folder"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all targeted entities should have permissions deleted using asyncio.gather
        await asyncio.gather(
            self._verify_permissions_deleted(structure["shallow_folder"]),
            self._verify_permissions_deleted(structure["shallow_file"]),
            self._verify_permissions_deleted(structure["deep_branch"]),
            self._verify_permissions_deleted(structure["deep_files"][1]),
            self._verify_permissions_deleted(structure["mixed_sub_folders"][0]),
        )

    async def test_delete_permissions_no_container_content_but_has_children(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions without include_container_content when children exist."""
        # GIVEN a folder with children and custom permissions
        parent_folder = await Folder(name=f"parent_folder_{uuid.uuid4()}").store_async(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(parent_folder.id)

        child_file = await File(
            path=utils.make_bogus_uuid_file(), name=f"child_file_{uuid.uuid4()}"
        ).store_async(parent=parent_folder, synapse_client=self.syn)
        self.schedule_for_cleanup(child_file.id)

        # Set permissions on both entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(parent_folder),
            self._set_custom_permissions(child_file),
        )

        # WHEN I delete permissions without include_container_content
        await parent_folder.delete_permissions_async(
            include_self=True,
            include_container_content=False,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only parent permissions should be deleted
        await self._verify_permissions_deleted(parent_folder)

        # AND child permissions should remain
        await self._verify_permissions_not_deleted(child_file)

    async def test_delete_permissions_case_insensitive_entity_types(
        self, project_model: Project
    ) -> None:
        """Test that target_entity_types are case-insensitive."""
        # GIVEN a simple structure with permissions
        structure = await self.create_simple_tree_structure(project_model)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities using asyncio.gather
        await asyncio.gather(
            self._set_custom_permissions(folder_a),
            self._set_custom_permissions(file_1),
        )

        # WHEN I delete permissions using mixed case entity types
        await folder_a.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["FOLDER", "file"],  # Mixed case
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            self._verify_permissions_deleted(folder_a),
            self._verify_permissions_deleted(file_1),
        )
