"""Integration tests for ACL on several models."""

import uuid
from typing import Callable, Optional, Type, Union

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
        # GIVEN an entity created with default permissions
        entity = await self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_team"
        )

        # AND a team
        team = await self.create_team()

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
        # GIVEN an entity created with default permissions
        entity = await self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_get_acl_multiple_teams",
        )

        # AND two teams
        team_1 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 1")
        team_2 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 2")

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
        # GIVEN a project created with default permissions
        project = await Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_through_teams"
        ).store_async()
        self.schedule_for_cleanup(project.id)

        # AND two teams
        team_1 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 1")
        team_2 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 2")

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
