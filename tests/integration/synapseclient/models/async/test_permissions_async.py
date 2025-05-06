"""Integration tests for ACL on several models."""

import uuid
from typing import Callable, Dict, List, Optional, Type, Union

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.models import (
    Column,
    ColumnType,
    File,
    Folder,
    Project,
    Table,
    Team,
    UserProfile,
)

PUBLIC = 273949  # PrincipalId of public "user"
AUTHENTICATED_USERS = 273948

TEAM_PREFIX = "My Uniquely Named Team "
DESCRIPTION_FAKE_TEAM = "A fake team for testing permissions"


class TestAcl:
    """Testing ACL on various entity models."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(path=filename)

    @pytest.fixture(scope="function")
    def table(self, project_model: Project) -> Table:
        columns = [
            Column(id=None, name="my_string_column", column_type=ColumnType.STRING),
        ]
        return Table(
            name="my_test_table" + str(uuid.uuid4()),
            columns=columns,
            parent_id=project_model.id,
        )

    async def create_entity(
        self,
        entity_type: Type[Union[Project, Folder, File, Table]],
        project_model: Optional[Project] = None,
        file_fixture: Optional[File] = None,
        table_fixture: Optional[Table] = None,
        name_suffix: str = "",
    ) -> Union[Project, Folder, File, Table]:
        """Helper to create different entity types with consistent naming"""
        entity_name = str(uuid.uuid4()) + name_suffix

        if entity_type == Project:
            entity = await Project(name=entity_name).store_async()
        elif entity_type == Folder:
            entity = await Folder(name=entity_name).store_async(parent=project_model)
        elif entity_type == File:
            file_fixture.name = entity_name
            entity = await file_fixture.store_async(parent=project_model)
        elif entity_type == Table:
            table_fixture.name = entity_name
            entity = await table_fixture.store_async()
        else:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        self.schedule_for_cleanup(entity.id)
        return entity

    async def create_team(self, description: str = DESCRIPTION_FAKE_TEAM) -> Team:
        """Helper to create a team with cleanup handling"""
        name = TEAM_PREFIX + str(uuid.uuid4())
        team = await Team(name=name, description=description).create_async()
        self.schedule_for_cleanup(team)
        return team

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    async def test_get_acl_default(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = await self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_default"
        )

        # AND the user that created the entity
        user = await UserProfile().get_async(synapse_client=self.syn)

        # WHEN getting the permissions for the user on the entity
        permissions = await entity.get_acl_async(principal_id=user.id)

        # THEN the expected default admin permissions should be present
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions)

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    async def test_get_acl_limited_permissions(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = await self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_limited"
        )

        # AND the user that created the entity
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND the permissions for the user are set to a limited set
        limited_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        await entity.set_permissions_async(
            principal_id=user.id,
            access_type=limited_permissions,
        )

        # WHEN getting the permissions for the user on the entity
        permissions = await entity.get_acl_async(principal_id=user.id)

        # THEN only the limited permissions should be present
        assert set(limited_permissions) == set(permissions)

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    async def test_get_acl_through_team(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN a team
        team = await self.create_team()

        # AND an entity created with default permissions
        entity = await self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_team"
        )

        # AND the user that created the entity
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND the team has specific permissions (all except DOWNLOAD)
        team_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        await entity.set_permissions_async(
            principal_id=team.id,
            access_type=team_permissions,
        )

        # AND the user has no direct permissions
        await entity.set_permissions_async(principal_id=user.id, access_type=[])

        # WHEN getting the permissions for the user on the entity
        permissions = await entity.get_acl_async(principal_id=user.id)

        # THEN the permissions should match the team's permissions
        assert set(team_permissions) == set(permissions)

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    async def test_get_acl_through_multiple_teams(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN two teams
        team_1 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 1")
        team_2 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 2")

        # AND an entity created with default permissions
        entity = await self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_get_acl_multiple_teams",
        )

        # AND the user that created the entity
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND the first team has specific permissions (all except DOWNLOAD)
        team_1_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        await entity.set_permissions_async(
            principal_id=team_1.id,
            access_type=team_1_permissions,
        )

        # AND the second team has only READ and DOWNLOAD permissions
        team_2_permissions = ["READ", "DOWNLOAD"]
        await entity.set_permissions_async(
            principal_id=team_2.id, access_type=team_2_permissions
        )

        # AND the user has no direct permissions
        await entity.set_permissions_async(principal_id=user.id, access_type=[])

        # WHEN getting the permissions for the user on the entity
        permissions = await entity.get_acl_async(principal_id=user.id)

        # THEN the permissions should be the combined set from both teams
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions)

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    async def test_get_acl_with_public_and_authenticated_users(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = await self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_get_acl_public_auth",
        )

        # AND the user that created the entity
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND public users have READ permission
        await entity.set_permissions_async(principal_id=PUBLIC, access_type=["READ"])

        # AND authenticated users have READ and DOWNLOAD permissions
        await entity.set_permissions_async(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the user has specific permissions (excluding DOWNLOAD)
        user_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        await entity.set_permissions_async(
            principal_id=user.id,
            access_type=user_permissions,
        )

        # WHEN getting public permissions (no principal_id)
        public_permissions = await entity.get_acl_async()

        # THEN only public permissions should be present
        assert set(["READ"]) == set(public_permissions)

        # WHEN getting the permissions for the authenticated user
        user_permissions = await entity.get_acl_async(principal_id=user.id)

        # THEN the permissions should include direct user permissions plus
        # permissions from authenticated users and public users
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(user_permissions)

    async def test_get_acl_for_subfolder_with_different_permissions(
        self, project_model: Project
    ) -> None:
        # GIVEN a parent folder with default permissions
        parent_folder = await Folder(
            name=str(uuid.uuid4()) + "_parent_folder_test"
        ).store_async(parent=project_model)
        self.schedule_for_cleanup(parent_folder.id)

        # AND a subfolder created inside the parent folder
        subfolder = await Folder(
            name=str(uuid.uuid4()) + "_subfolder_test"
        ).store_async(parent=parent_folder)
        self.schedule_for_cleanup(subfolder.id)

        # AND the user that created the folders
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND the subfolder has limited permissions
        limited_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        await subfolder.set_permissions_async(
            principal_id=user.id,
            access_type=limited_permissions,
        )

        # WHEN getting permissions for the subfolder
        subfolder_permissions = await subfolder.get_acl_async(principal_id=user.id)

        # AND getting permissions for the parent folder
        parent_permissions = await parent_folder.get_acl_async(principal_id=user.id)

        # THEN the subfolder should have the limited permissions
        assert set(limited_permissions) == set(subfolder_permissions)

        # AND the parent folder should have the default admin permissions
        expected_parent_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_parent_permissions) == set(parent_permissions)


class TestPermissionsForCaller:
    """Test the permissions that the current caller has for an entity."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def create_team(self, description: str = DESCRIPTION_FAKE_TEAM) -> Team:
        """Helper to create a team with cleanup handling"""
        name = TEAM_PREFIX + str(uuid.uuid4())
        team = await Team(name=name, description=description).create_async()
        self.schedule_for_cleanup(team)
        return team

    async def test_get_permissions_default(self) -> None:
        # GIVEN a project created with default permissions
        project = await Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_default"
        ).store_async()
        self.schedule_for_cleanup(project.id)

        # WHEN getting the permissions for the current user
        permissions = await project.get_permissions_async()

        # THEN all default permissions should be present
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_with_limited_access(self) -> None:
        # GIVEN a project created with default permissions
        project = await Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_limited"
        ).store_async()
        self.schedule_for_cleanup(project.id)

        # AND the current user that created the project
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND the permissions for the user are set to READ only
        await project.set_permissions_async(principal_id=user.id, access_type=["READ"])

        # WHEN getting the permissions for the current user
        permissions = await project.get_permissions_async()

        # THEN READ and CHANGE_SETTINGS permissions should be present
        # Note: CHANGE_SETTINGS is bound to ownerId and can't be removed
        expected_permissions = ["READ", "CHANGE_SETTINGS"]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_through_teams(self) -> None:
        # GIVEN two teams
        team_1 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 1")
        team_2 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 2")

        # AND a project created with default permissions
        project = await Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_through_teams"
        ).store_async()
        self.schedule_for_cleanup(project.id)

        # AND the current user that created the project
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND team 1 has all permissions except DOWNLOAD
        team_1_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        await project.set_permissions_async(
            principal_id=team_1.id,
            access_type=team_1_permissions,
        )

        # AND team 2 has only READ and DOWNLOAD permissions
        await project.set_permissions_async(
            principal_id=team_2.id, access_type=["READ", "DOWNLOAD"]
        )

        # AND the user has no direct permissions
        await project.set_permissions_async(principal_id=user.id, access_type=[])

        # WHEN getting the permissions for the current user
        permissions = await project.get_permissions_async()

        # THEN the effective permissions should be the combined permissions from both teams
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_with_authenticated_users(self) -> None:
        # GIVEN a project created with default permissions
        project = await Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_authenticated"
        ).store_async()
        self.schedule_for_cleanup(project.id)

        # AND the current user that created the project
        user = await UserProfile().get_async(synapse_client=self.syn)

        # AND authenticated users have READ and DOWNLOAD permissions
        await project.set_permissions_async(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the current user has specific permissions (without DOWNLOAD)
        user_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        await project.set_permissions_async(
            principal_id=user.id,
            access_type=user_permissions,
        )

        # WHEN getting the permissions for the current user
        permissions = await project.get_permissions_async()

        # THEN the permissions should include user permissions plus
        # the DOWNLOAD permission from authenticated users
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions.access_types)


class TestDeletePermissions:
    """Test delete_permissions_async functionality across entities."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(path=filename)

    async def create_folder_structure(
        self, project_model: Project
    ) -> Dict[str, Union[Folder, List[Union[Folder, File]]]]:
        """Create a folder structure for testing permissions.

        Structure:
        ```
        Project_model
        └── top_level_folder
            ├── file_1
            ├── file_2
            └── folder_1
                ├── sub_folder_1
                │   └── file_3
                └── file_4
        ```
        """
        # Create top level folder
        top_level_folder = await Folder(
            name=f"top_level_folder_{uuid.uuid4()}"
        ).store_async(parent=project_model)
        self.schedule_for_cleanup(top_level_folder.id)

        # Create 2 files in top level folder
        file_1 = await File(
            path=utils.make_bogus_uuid_file(), name=f"file_1_{uuid.uuid4()}"
        ).store_async(parent=top_level_folder)
        self.schedule_for_cleanup(file_1.id)

        file_2 = await File(
            path=utils.make_bogus_uuid_file(), name=f"file_2_{uuid.uuid4()}"
        ).store_async(parent=top_level_folder)
        self.schedule_for_cleanup(file_2.id)

        # Create folder_1 in top level folder
        folder_1 = await Folder(name=f"folder_1_{uuid.uuid4()}").store_async(
            parent=top_level_folder
        )
        self.schedule_for_cleanup(folder_1.id)

        # Create sub_folder_1 in folder_1
        sub_folder_1 = await Folder(name=f"sub_folder_1_{uuid.uuid4()}").store_async(
            parent=folder_1
        )
        self.schedule_for_cleanup(sub_folder_1.id)

        # Create file_3 in sub_folder_1
        file_3 = await File(
            path=utils.make_bogus_uuid_file(), name=f"file_3_{uuid.uuid4()}"
        ).store_async(parent=sub_folder_1)
        self.schedule_for_cleanup(file_3.id)

        # Create file_4 in folder_1
        file_4 = await File(
            path=utils.make_bogus_uuid_file(), name=f"file_4_{uuid.uuid4()}"
        ).store_async(parent=folder_1)
        self.schedule_for_cleanup(file_4.id)

        return {
            "top_level_folder": top_level_folder,
            "files": [file_1, file_2],
            "folder_1": folder_1,
            "sub_folder_1": sub_folder_1,
            "file_3": file_3,
            "file_4": file_4,
        }

    async def _set_custom_permissions(
        self, entity: Union[File, Folder, Project]
    ) -> None:
        """Helper to set custom permissions on an entity so we can verify deletion."""
        # Set custom permissions for authenticated users
        await entity.set_permissions_async(
            principal_id=AUTHENTICATED_USERS, access_type=["READ"]
        )

        # Verify permissions were set
        acl = await entity.get_acl_async(principal_id=AUTHENTICATED_USERS)
        assert "READ" in acl

        return acl

    async def _verify_permissions_deleted(
        self, entity: Union[File, Folder, Project]
    ) -> None:
        """Helper to verify that permissions have been deleted (entity inherits from parent)."""

        acl = await entity.get_acl_async(
            principal_id=AUTHENTICATED_USERS, check_benefactor=False
        )

        assert (
            not acl
        ), f"Permissions should be deleted, but they still exist on [id: {entity.id}, name: {entity.name}, {entity.__class__}]."

    async def _verify_permissions_not_deleted(
        self, entity: Union[File, Folder, Project]
    ) -> None:
        """Helper to verify that permissions are still set on an entity."""
        acl = await entity.get_acl_async(
            principal_id=AUTHENTICATED_USERS, check_benefactor=False
        )
        assert "READ" in acl
        return True

    async def test_delete_permissions_single_file(
        self, project_model: Project, file: File
    ) -> None:
        """Test deleting permissions on a single file."""
        # GIVEN a file with custom permissions
        file.name = f"test_file_{uuid.uuid4()}"
        stored_file = await file.store_async(parent=project_model)
        self.schedule_for_cleanup(stored_file.id)

        await self._set_custom_permissions(stored_file)

        # WHEN I delete permissions on the file
        await stored_file.delete_permissions_async()

        # THEN the permissions should be deleted
        await self._verify_permissions_deleted(stored_file)

    async def test_delete_permissions_single_folder(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions on a single folder."""
        # GIVEN folder structure with permissions on top level folder
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]

        await self._set_custom_permissions(top_level_folder)

        # WHEN I delete permissions on the folder
        await top_level_folder.delete_permissions_async()

        # THEN the permissions should be deleted
        await self._verify_permissions_deleted(top_level_folder)

    async def test_delete_permissions_skip_self(self, project_model: Project) -> None:
        """Test deleting permissions with include_self=False."""
        # GIVEN a folder structure with permissions set on the top level folder
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]
        files = folder_structure["files"]

        # AND custom permissions are set on top level folder and a file
        await self._set_custom_permissions(top_level_folder)
        await self._set_custom_permissions(files[0])

        # WHEN I delete permissions with include_self=False and include_container_content=True
        await top_level_folder.delete_permissions_async(
            include_self=False, include_container_content=True
        )

        # THEN the top level folder permissions should remain
        assert await self._verify_permissions_not_deleted(top_level_folder)

        # AND the file permissions should be deleted
        await self._verify_permissions_deleted(files[0])

    async def test_delete_permissions_include_container_content(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions with include_container_content=True."""
        # GIVEN a folder structure with permissions set on top level folder and files
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]
        files = folder_structure["files"]
        folder_1 = folder_structure["folder_1"]

        # AND permissions are set on top level folder, files, and folder_1
        await self._set_custom_permissions(top_level_folder)
        await self._set_custom_permissions(files[0])
        await self._set_custom_permissions(folder_1)

        # WHEN I delete permissions with include_container_content=True but recursive=False
        await top_level_folder.delete_permissions_async(
            include_self=True, include_container_content=True, recursive=False
        )

        # THEN the top level folder permissions should be deleted
        await self._verify_permissions_deleted(top_level_folder)

        # AND the files permissions should be deleted
        await self._verify_permissions_deleted(files[0])

        # AND the folder_1 permissions should be deleted
        await self._verify_permissions_deleted(folder_1)

    async def test_delete_permissions_recursive(self, project_model: Project) -> None:
        """Test deleting permissions recursively."""
        # GIVEN a folder structure with permissions set throughout
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]
        folder_1 = folder_structure["folder_1"]
        file_3 = folder_structure["file_3"]

        # AND permissions are set on top_level_folder, folder_1, and file_3
        await self._set_custom_permissions(top_level_folder)
        await self._set_custom_permissions(folder_1)
        await self._set_custom_permissions(file_3)

        # WHEN I delete permissions recursively but without container content
        await top_level_folder.delete_permissions_async(
            recursive=True, include_container_content=False
        )

        # THEN the top_level_folder permissions should be deleted
        await self._verify_permissions_deleted(top_level_folder)

        # AND the folder_1 permissions should remain (because include_container_content=False)
        await self._verify_permissions_not_deleted(folder_1)

        # BUT the file_3 permissions should remain (because include_container_content=False)
        assert await self._verify_permissions_not_deleted(file_3)

    async def test_delete_permissions_recursive_with_container_content(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions recursively with container content."""
        # GIVEN a folder structure with permissions set throughout
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]
        files = folder_structure["files"]
        folder_1 = folder_structure["folder_1"]
        file_3 = folder_structure["file_3"]

        # AND permissions are set on top_level_folder, files, folder_1, and file_3
        await self._set_custom_permissions(top_level_folder)
        await self._set_custom_permissions(files[0])
        await self._set_custom_permissions(folder_1)
        await self._set_custom_permissions(file_3)

        # WHEN I delete permissions recursively with container content
        await top_level_folder.delete_permissions_async(
            recursive=True, include_container_content=True
        )

        # THEN all permissions should be deleted
        await self._verify_permissions_deleted(top_level_folder)
        await self._verify_permissions_deleted(files[0])
        await self._verify_permissions_deleted(folder_1)
        await self._verify_permissions_deleted(file_3)

    async def test_delete_permissions_target_entity_types_folder_only(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions with target_entity_types=['folder']."""
        # GIVEN a folder structure with permissions set throughout
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]
        files = folder_structure["files"]
        folder_1 = folder_structure["folder_1"]
        sub_folder_1 = folder_structure["sub_folder_1"]

        # AND permissions are set on top_level_folder, files, folder_1, and sub_folder_1
        await self._set_custom_permissions(top_level_folder)
        await self._set_custom_permissions(files[0])
        await self._set_custom_permissions(folder_1)
        await self._set_custom_permissions(sub_folder_1)

        # WHEN I delete permissions recursively but only for folder entity types
        await top_level_folder.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["folder"],
        )

        # THEN folder permissions should be deleted
        await self._verify_permissions_deleted(top_level_folder)
        await self._verify_permissions_deleted(folder_1)
        await self._verify_permissions_deleted(sub_folder_1)

        # BUT file permissions should remain
        assert await self._verify_permissions_not_deleted(files[0])

    async def test_delete_permissions_target_entity_types_file_only(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions with target_entity_types=['file']."""
        # GIVEN a folder structure with permissions set throughout
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]
        files = folder_structure["files"]
        folder_1 = folder_structure["folder_1"]
        file_3 = folder_structure["file_3"]
        file_4 = folder_structure["file_4"]

        # AND permissions are set on top_level_folder, files, folder_1, file_3, and file_4
        await self._set_custom_permissions(top_level_folder)
        await self._set_custom_permissions(files[0])
        await self._set_custom_permissions(folder_1)
        await self._set_custom_permissions(file_3)
        await self._set_custom_permissions(file_4)

        # WHEN I delete permissions recursively but only for file entity types
        await top_level_folder.delete_permissions_async(
            recursive=True, include_container_content=True, target_entity_types=["file"]
        )

        # THEN file permissions should be deleted
        await self._verify_permissions_deleted(files[0])
        await self._verify_permissions_deleted(file_3)
        await self._verify_permissions_deleted(file_4)

        # BUT folder permissions should remain
        assert await self._verify_permissions_not_deleted(top_level_folder)
        assert await self._verify_permissions_not_deleted(folder_1)

    async def test_delete_permissions_invalid_entity_type(
        self, project_model: Project
    ) -> None:
        """Test deleting permissions with an invalid entity type."""
        # GIVEN a folder structure
        folder_structure = await self.create_folder_structure(project_model)
        top_level_folder = folder_structure["top_level_folder"]

        # WHEN I try to delete permissions with an invalid entity type
        # THEN it should raise a ValueError
        with pytest.raises(ValueError) as exc_info:
            await top_level_folder.delete_permissions_async(
                target_entity_types=["invalid_type"]
            )

        # AND the error message should mention allowed values
        assert "Invalid entity type" in str(exc_info.value)
        assert "folder" in str(exc_info.value)

    async def test_delete_permissions_on_new_project(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a newly created project."""
        # GIVEN a newly created project with custom permissions
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async()
        self.schedule_for_cleanup(project.id)

        # AND custom permissions are set for authenticated users
        await self._set_custom_permissions(project)

        # WHEN I delete permissions on the project
        await project.delete_permissions_async()

        # THEN the permissions should not be deleted
        assert (
            "Cannot restore inheritance for resource which has no parent."
            in caplog.text
        )
