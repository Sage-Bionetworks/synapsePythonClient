"""Integration tests for ACL on several models."""

import asyncio
import logging
import random
import uuid
from typing import Callable, Dict, List, Optional, Type, Union

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.models.acl import AclListResult
from synapseclient.models import (
    Column,
    ColumnType,
    EntityView,
    File,
    Folder,
    Project,
    SubmissionView,
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

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    async def test_remove_team_permissions_with_empty_access_type(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = await self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_remove_team_permissions",
        )

        # AND a test team
        team = await self.create_team()

        # AND the team initially has specific permissions
        initial_team_permissions = ["READ", "UPDATE", "CREATE", "DOWNLOAD"]
        await entity.set_permissions_async(
            principal_id=team.id,
            access_type=initial_team_permissions,
        )

        # WHEN verifying the team has the initial permissions
        team_acl_before = await entity.get_acl_async(principal_id=team.id)
        assert set(initial_team_permissions) == set(team_acl_before)

        # AND WHEN removing the team's permissions by setting access_type to empty list
        await entity.set_permissions_async(
            principal_id=team.id,
            access_type=[],  # Empty list to remove permissions
        )

        # THEN the team should have no permissions
        team_acl_after = await entity.get_acl_async(principal_id=team.id)
        assert team_acl_after == []

        # AND the team should not appear in the full ACL list with any permissions
        all_acls = await entity.list_acl_async()
        team_acl_entries = [
            acl
            for acl in all_acls.entity_acl
            if acl.principal_id == team.id and acl.access_type
        ]
        assert (
            len(team_acl_entries) == 0
        ), f"Team {team.id} should have no ACL entries but found: {team_acl_entries}"

        # AND other entities should still maintain their permissions (verify no side effects)
        user = await UserProfile().get_async(synapse_client=self.syn)
        user_acl = await entity.get_acl_async(principal_id=user.id)
        assert len(user_acl) > 0, "User permissions should remain intact"

    async def test_table_permissions(self, project_model: Project) -> None:
        """Comprehensive test for Table permissions - setting, listing, and deleting."""
        # GIVEN a table with test data
        columns = [
            Column(id=None, name="test_column", column_type=ColumnType.STRING),
            Column(id=None, name="number_column", column_type=ColumnType.INTEGER),
        ]
        table = Table(
            name=f"test_table_permissions_{uuid.uuid4()}",
            columns=columns,
            parent_id=project_model.id,
        )
        table = await table.store_async()
        self.schedule_for_cleanup(table.id)

        # AND a test team
        team = await self.create_team()
        user = await UserProfile().get_async(synapse_client=self.syn)

        # WHEN setting various permissions
        # Set team permissions
        team_permissions = ["READ", "UPDATE", "CREATE"]
        await table.set_permissions_async(
            principal_id=team.id,
            access_type=team_permissions,
        )

        # Set authenticated users permissions
        await table.set_permissions_async(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
        )

        await asyncio.sleep(random.randint(1, 5))

        # THEN listing permissions should show all set permissions
        # Check team permissions
        team_acl = await table.get_acl_async(principal_id=team.id)
        assert set(team_permissions) == set(team_acl)

        # Check authenticated users permissions
        auth_acl = await table.get_acl_async(principal_id=AUTHENTICATED_USERS)
        assert set(["READ", "DOWNLOAD"]) == set(auth_acl)

        # Check user effective permissions (should include permissions from all sources)
        user_effective_acl = await table.get_acl_async(principal_id=user.id)
        expected_user_permissions = {
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        }
        assert expected_user_permissions.issubset(set(user_effective_acl))

        # AND listing all ACLs should return complete ACL information
        all_acls = await table.list_acl_async()
        assert isinstance(all_acls, AclListResult)
        assert len(all_acls.entity_acl) >= 3  # User, team, authenticated_users

        # WHEN deleting specific permissions for the team
        await table.set_permissions_async(principal_id=team.id, access_type=[])

        await asyncio.sleep(random.randint(1, 5))

        # THEN team should no longer have permissions
        team_acl_after_delete = await table.get_acl_async(principal_id=team.id)
        assert team_acl_after_delete == []

        # BUT other permissions should remain
        auth_acl_after = await table.get_acl_async(principal_id=AUTHENTICATED_USERS)
        assert set(["READ", "DOWNLOAD"]) == set(auth_acl_after)

    async def test_entity_view_permissions(self, project_model: Project) -> None:
        """Comprehensive test for EntityView permissions - setting, listing, and deleting."""
        # GIVEN an entity view
        entity_view = EntityView(
            name=f"test_entity_view_permissions_{uuid.uuid4()}",
            parent_id=project_model.id,
            scope_ids=[project_model.id],
            view_type_mask=0x01,  # File view
        )
        entity_view = await entity_view.store_async()
        self.schedule_for_cleanup(entity_view.id)

        # AND test subjects
        team = await self.create_team()
        user = await UserProfile().get_async(synapse_client=self.syn)

        # WHEN setting comprehensive permissions
        # Set team permissions (moderate permissions)
        team_permissions = ["READ", "UPDATE", "MODERATE"]
        await entity_view.set_permissions_async(
            principal_id=team.id,
            access_type=team_permissions,
        )

        # Set authenticated users permissions
        await entity_view.set_permissions_async(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
        )

        # Set limited user permissions (remove some admin permissions)
        limited_user_permissions = [
            "READ",
            "UPDATE",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "DELETE",
            "MODERATE",
        ]
        await entity_view.set_permissions_async(
            principal_id=user.id,
            access_type=limited_user_permissions,
        )

        await asyncio.sleep(random.randint(1, 5))

        # THEN listing permissions should reflect all changes
        # Verify team permissions
        team_acl = await entity_view.get_acl_async(principal_id=team.id)
        assert set(team_permissions) == set(team_acl)

        # Verify authenticated users permissions
        auth_acl = await entity_view.get_acl_async(principal_id=AUTHENTICATED_USERS)
        assert set(["READ", "DOWNLOAD"]) == set(auth_acl)

        # Verify user permissions include both direct and inherited permissions
        user_acl = await entity_view.get_acl_async(principal_id=user.id)
        expected_user_permissions = set(
            limited_user_permissions + ["DOWNLOAD"]
        )  # Includes auth users perm
        assert expected_user_permissions == set(user_acl)

        # Verify complete ACL listing
        all_acls = await entity_view.list_acl_async()
        assert isinstance(all_acls, AclListResult)
        assert len(all_acls.entity_acl) >= 3  # User, team, authenticated_users

        # WHEN deleting authenticated users permissions
        await entity_view.set_permissions_async(
            principal_id=AUTHENTICATED_USERS, access_type=[]
        )

        await asyncio.sleep(random.randint(1, 5))

        # THEN authenticated users should lose permissions
        auth_acl_after = await entity_view.get_acl_async(
            principal_id=AUTHENTICATED_USERS
        )
        assert auth_acl_after == []

        # AND user permissions should no longer include DOWNLOAD
        user_acl_after = await entity_view.get_acl_async(principal_id=user.id)
        assert set(limited_user_permissions) == set(user_acl_after)

        # BUT team permissions should remain
        team_acl_after = await entity_view.get_acl_async(principal_id=team.id)
        assert set(team_permissions) == set(team_acl_after)

    async def test_submission_view_permissions(self, project_model: Project) -> None:
        """Comprehensive test for SubmissionView permissions - setting, listing, and deleting."""
        # GIVEN a submission view
        submission_view = SubmissionView(
            name=f"test_submission_view_permissions_{uuid.uuid4()}",
            parent_id=project_model.id,
            scope_ids=[project_model.id],
        )
        submission_view = await submission_view.store_async()
        self.schedule_for_cleanup(submission_view.id)

        # AND test subjects
        team1 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - team1")
        team2 = await self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - team2")
        user = await UserProfile().get_async(synapse_client=self.syn)

        # WHEN setting overlapping permissions across multiple teams
        # Team1 gets full read/write access
        team1_permissions = ["READ", "UPDATE", "CREATE", "DELETE"]
        await submission_view.set_permissions_async(
            principal_id=team1.id,
            access_type=team1_permissions,
        )

        # Team2 gets read-only access with download
        team2_permissions = ["READ", "DOWNLOAD"]
        await submission_view.set_permissions_async(
            principal_id=team2.id,
            access_type=team2_permissions,
        )

        # Public gets read access
        await submission_view.set_permissions_async(
            principal_id=PUBLIC,
            access_type=["READ"],
        )

        # User gets minimal direct permissions
        user_direct_permissions = ["READ", "CHANGE_SETTINGS", "CHANGE_PERMISSIONS"]
        await submission_view.set_permissions_async(
            principal_id=user.id,
            access_type=user_direct_permissions,
        )

        await asyncio.sleep(random.randint(1, 5))

        # THEN listing permissions should show proper aggregation
        # Check individual team permissions
        team1_acl = await submission_view.get_acl_async(principal_id=team1.id)
        assert set(team1_permissions) == set(team1_acl)

        team2_acl = await submission_view.get_acl_async(principal_id=team2.id)
        assert set(team2_permissions) == set(team2_acl)

        # Check public permissions
        public_acl = await submission_view.get_acl_async()
        assert set(["READ"]) == set(public_acl)

        # Check user effective permissions (should aggregate from all teams)
        user_effective_acl = await submission_view.get_acl_async(principal_id=user.id)
        expected_effective = set(
            user_direct_permissions + team1_permissions + team2_permissions
        )
        assert expected_effective == set(user_effective_acl)

        # Verify complete ACL structure
        all_acls = await submission_view.list_acl_async()
        assert isinstance(all_acls, AclListResult)
        assert len(all_acls.entity_acl) >= 4  # User, team1, team2, public

        # WHEN selectively deleting permissions
        # Remove PUBLIC and team permissions
        await submission_view.set_permissions_async(principal_id=PUBLIC, access_type=[])
        await submission_view.set_permissions_async(
            principal_id=team1.id, access_type=[]
        )

        await asyncio.sleep(random.randint(1, 5))

        # THEN PUBLIC should lose all permissions
        public_acl_after = await submission_view.get_acl_async(principal_id=PUBLIC)
        assert public_acl_after == []

        # AND team should lose all permissions
        team_acl_after = await submission_view.get_acl_async(principal_id=team1.id)
        assert team_acl_after == []

        # AND user effective permissions should no longer include team1 permissions
        user_effective_after = await submission_view.get_acl_async(principal_id=user.id)
        expected_after = set(user_direct_permissions + team2_permissions)
        assert expected_after == set(user_effective_after)

        # BUT other permissions should remain unchanged
        team2_acl_after = await submission_view.get_acl_async(principal_id=team2.id)
        assert set(team2_permissions) == set(team2_acl_after)


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
    def init(
        self,
        syn: Synapse,
        syn_with_logger: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup
        self.syn_with_logger = syn_with_logger
        self.verification_attempts = 10

    @pytest.fixture(scope="function")
    def project_object(self) -> Project:
        return Project(name="integration_test_project" + str(uuid.uuid4()))

    @pytest.fixture(scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(path=filename)

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
        for attempt in range(self.verification_attempts):
            await asyncio.sleep(random.randint(1, 5))

            acl = await entity.get_acl_async(
                principal_id=AUTHENTICATED_USERS, check_benefactor=False
            )

            if not acl:
                return  # Verification successful

            if attempt == self.verification_attempts - 1:  # Last attempt
                assert not acl, (
                    f"Permissions should be deleted, but they still exist on "
                    f"[id: {entity.id}, name: {entity.name}, {entity.__class__}]."
                )

    async def _verify_permissions_not_deleted(
        self, entity: Union[File, Folder, Project]
    ) -> bool:
        """Helper to verify that permissions are still set on an entity."""
        for attempt in range(self.verification_attempts):
            await asyncio.sleep(random.randint(1, 5))
            acl = await entity.get_acl_async(
                principal_id=AUTHENTICATED_USERS, check_benefactor=False
            )
            if "READ" in acl:
                return True

            if attempt == self.verification_attempts - 1:  # Last attempt
                assert "READ" in acl

        return True

    async def _verify_list_acl_functionality(
        self,
        entity: Union[File, Folder, Project],
        expected_entity_count: int,
        recursive: bool = True,
        include_container_content: bool = True,
        target_entity_types: Optional[List[str]] = None,
        log_tree: bool = True,
    ) -> AclListResult:
        """Helper to verify list_acl_async functionality and return results."""
        for attempt in range(self.verification_attempts):
            await asyncio.sleep(random.randint(1, 5))
            acl_result = await entity.list_acl_async(
                recursive=recursive,
                include_container_content=include_container_content,
                target_entity_types=target_entity_types,
                log_tree=log_tree,
                synapse_client=self.syn_with_logger,
            )

            if (
                isinstance(acl_result, AclListResult)
                and len(acl_result.all_entity_acls) >= expected_entity_count
            ):
                return acl_result

            if attempt == self.verification_attempts - 1:  # Last attempt
                assert isinstance(acl_result, AclListResult)
                assert len(acl_result.all_entity_acls) >= expected_entity_count

        return acl_result

    def _verify_log_messages(
        self,
        caplog: pytest.LogCaptureFixture,
        list_acl_called: bool = True,
        delete_permissions_called: bool = True,
        dry_run: bool = False,
        tree_logging: bool = True,
    ) -> None:
        """Helper to verify expected log messages from both methods."""
        for attempt in range(self.verification_attempts):
            log_text = caplog.text
            all_checks_passed = True

            # Check tree logging if required
            if list_acl_called and tree_logging:
                if "ACL Tree Structure:" not in log_text:
                    all_checks_passed = False

            # Check dry run messages if required
            if delete_permissions_called and dry_run:
                if (
                    "DRY RUN" not in log_text
                    or "Permission Deletion Impact Analysis" not in log_text
                    or "End of Dry Run Analysis" not in log_text
                ):
                    all_checks_passed = False

            # If all checks passed, we're done
            if all_checks_passed:
                break

            # On last attempt, assert all required conditions
            if attempt == self.verification_attempts - 1:
                if list_acl_called and tree_logging:
                    assert "ACL Tree Structure:" in log_text
                if delete_permissions_called and dry_run:
                    assert "DRY RUN" in log_text
                    assert "Permission Deletion Impact Analysis" in log_text
                    assert "End of Dry Run Analysis" in log_text

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

    async def test_delete_permissions_on_new_project(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a newly created project."""
        # Set the log level to capture DEBUG messages
        caplog.set_level(logging.DEBUG)

        # GIVEN a newly created project with custom permissions
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async()
        self.schedule_for_cleanup(project.id)

        # AND custom permissions are set for authenticated users
        await self._set_custom_permissions(project)
        await asyncio.sleep(random.randint(1, 5))

        # WHEN I delete permissions on the project
        await project.delete_permissions_async()

        # THEN the permissions should not be deleted
        # Check either for the log message or verify the permissions still exist
        if (
            "Cannot restore inheritance for resource which has no parent."
            in caplog.text
        ):
            # Original assertion passes if the log is captured
            assert True
        else:
            # Alternatively, verify that the permissions weren't actually deleted
            # by checking if they still exist
            assert await self._verify_permissions_not_deleted(project)

    async def test_delete_permissions_simple_tree_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a simple tree structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a simple tree structure with permissions
        structure = await self.create_simple_tree_structure(project_object)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(folder_a),
            self._set_custom_permissions(file_1),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion
        await self._verify_list_acl_functionality(
            entity=folder_a,
            expected_entity_count=2,  # folder_a and file_1
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()  # Clear logs for next verification

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a deeply nested structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a deeply nested structure with permissions
        structure = await self.create_deep_nested_structure(project_object)

        # Set permissions on all entities
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in structure.values()]
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion
        await self._verify_list_acl_functionality(
            entity=structure["level_1"],
            expected_entity_count=8,  # all levels and files
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a wide tree structure with multiple siblings."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a wide tree structure with permissions
        structure = await self.create_wide_tree_structure(project_object)
        folders = structure["folders"]
        all_files = structure["all_files"]
        root_file = structure["root_file"]

        # Set permissions on all entities
        entities_to_set = folders + all_files + [root_file]
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion
        await self._verify_list_acl_functionality(
            entity=project_object,
            expected_entity_count=7,  # 3 folders + 3 files + 1 root file
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

        # WHEN I delete permissions recursively from the project
        await project_object.delete_permissions_async(
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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a complex mixed structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex mixed structure with permissions
        structure = await self.create_complex_mixed_structure(project_object)

        # Set permissions on all entities
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
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_functionality before deletion
        await self._verify_list_acl_functionality(
            entity=project_object,
            expected_entity_count=12,  # complex structure with multiple entities
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

        # WHEN I delete permissions recursively from the project
        await project_object.delete_permissions_async(
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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on an empty folder."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN an empty folder with custom permissions
        empty_folder = await Folder(name=f"empty_folder_{uuid.uuid4()}").store_async(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(empty_folder.id)
        await self._set_custom_permissions(empty_folder)
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion (empty folder)
        await self._verify_list_acl_functionality(
            entity=empty_folder,
            expected_entity_count=1,  # just the empty folder itself
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred for empty structure
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a folder that contains only files."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with only one file
        folder = await Folder(name=f"files_only_folder_{uuid.uuid4()}").store_async(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        file = await File(
            path=utils.make_bogus_uuid_file(), name=f"only_file_{uuid.uuid4()}"
        ).store_async(parent=folder, synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(folder),
            self._set_custom_permissions(file),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion
        await self._verify_list_acl_functionality(
            entity=folder,
            expected_entity_count=2,  # folder and file
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a folder that contains only sub-folders."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with only sub-folders
        parent_folder = await Folder(
            name=f"folders_only_parent_{uuid.uuid4()}"
        ).store_async(parent=project_object, synapse_client=self.syn)
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

        # Set permissions on all entities
        entities_to_set = [parent_folder] + sub_folders
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion
        await self._verify_list_acl_functionality(
            entity=parent_folder,
            expected_entity_count=4,  # parent + 3 sub-folders
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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

    async def test_delete_permissions_target_files_only_complex(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions targeting only files in a complex structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex structure with permissions
        structure = await self.create_complex_mixed_structure(project_object)

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(structure["shallow_folder"]),
            self._set_custom_permissions(structure["shallow_file"]),
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["sub_deep"]),
            *[self._set_custom_permissions(file) for file in structure["deep_files"]],
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async with target_entity_types for files only
        await self._verify_list_acl_functionality(
            entity=project_object,
            expected_entity_count=4,  # shallow_file + 3 deep_files
            recursive=True,
            include_container_content=True,
            target_entity_types=["file"],
            log_tree=True,
        )

        # Verify tree logging occurred with file filtering
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

        # WHEN I delete permissions targeting only files
        await project_object.delete_permissions_async(
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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test include_container_content=True without recursive on deep structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a deep nested structure with permissions
        structure = await self.create_deep_nested_structure(project_object)

        # Set permissions on all entities
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in structure.values()]
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async with include_container_content=True
        await self._verify_list_acl_functionality(
            entity=structure["level_1"],
            expected_entity_count=3,  # level_1, file_at_1, level_2 (non-recursive)
            recursive=False,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test include_self=False on a complex structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex mixed structure with permissions
        structure = await self.create_complex_mixed_structure(project_object)

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(structure["mixed_folder"]),
            *[
                self._set_custom_permissions(folder)
                for folder in structure["mixed_sub_folders"]
            ],
            *[self._set_custom_permissions(file) for file in structure["mixed_files"]],
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion (should show all entities)
        await self._verify_list_acl_functionality(
            entity=structure["mixed_folder"],
            expected_entity_count=4,  # mixed_folder + 2 sub_folders + 1 mixed_file
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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

        # BUT child permissions should be deleted
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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that dry_run=True makes no actual changes."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a simple structure with permissions
        structure = await self.create_simple_tree_structure(project_object)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(folder_a),
            self._set_custom_permissions(file_1),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before dry run
        initial_acl_result = await self._verify_list_acl_functionality(
            entity=folder_a,
            expected_entity_count=2,  # folder_a and file_1
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self._verify_log_messages(
            caplog,
            list_acl_called=False,
            delete_permissions_called=True,
            dry_run=True,
            tree_logging=False,
        )

        # WHEN - Verify list_acl_async after dry run (should be identical)
        caplog.clear()
        final_acl_result = await self._verify_list_acl_functionality(
            entity=folder_a,
            expected_entity_count=2,  # should be same as before
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Results should be identical since dry run made no changes
        assert len(initial_acl_result.all_entity_acls[0].acl_entries) == len(
            final_acl_result.all_entity_acls[0].acl_entries
        )

    async def test_delete_permissions_dry_run_complex_logging(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test dry run logging for complex structures."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex structure with permissions
        structure = await self.create_complex_mixed_structure(project_object)

        # Set permissions on a subset of entities
        await asyncio.gather(
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["sub_deep"]),
            self._set_custom_permissions(structure["deep_files"][0]),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async with detailed logging before dry run
        await self._verify_list_acl_functionality(
            entity=structure["deep_branch"],
            expected_entity_count=3,  # deep_branch, sub_deep, and one deep_file
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self._verify_log_messages(
            caplog,
            list_acl_called=False,
            delete_permissions_called=True,
            dry_run=True,
            tree_logging=False,
        )

        # Verify specific detailed logging messages
        assert "DRY RUN: Permission Deletion Impact Analysis" in caplog.text
        assert "End of Dry Run Analysis" in caplog.text

    # Performance and stress tests
    async def test_delete_permissions_large_flat_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a large flat structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with many files
        large_folder = await Folder(name=f"large_folder_{uuid.uuid4()}").store_async(
            parent=project_object, synapse_client=self.syn
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

        # Set permissions on all entities
        entities_to_set = [large_folder] + files
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in entities_to_set]
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async performance with large structure
        await self._verify_list_acl_functionality(
            entity=large_folder,
            expected_entity_count=11,  # large_folder + 10 files
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred for large structure
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on multiple nested branches simultaneously."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN multiple complex nested branches
        branch_tasks = [
            Folder(name=f"branch_{branch_name}_{uuid.uuid4()}").store_async(
                parent=project_object, synapse_client=self.syn
            )
            for branch_name in ["alpha", "beta"]
        ]
        branches = await asyncio.gather(*branch_tasks)

        all_entities = list(branches)

        # Schedule cleanup for branches
        for branch in branches:
            self.schedule_for_cleanup(branch.id)

        # Create nested structure in each branch in parallel
        nested_tasks = []
        for branch_name, branch in zip(["alpha", "beta"], branches):
            current_parent = branch
            for level in range(2):
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
        for branch_name in ["alpha", "beta"]:
            for level in range(2):
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

        # Set permissions on all entities
        await asyncio.gather(
            *[self._set_custom_permissions(entity) for entity in all_entities]
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before deletion (complex multiple branches)
        await self._verify_list_acl_functionality(
            entity=project_object,
            expected_entity_count=11,
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred for multiple branches
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

        # WHEN I delete permissions recursively from the project
        await project_object.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        await asyncio.gather(
            *[self._verify_permissions_deleted(entity) for entity in all_entities]
        )

    async def test_delete_permissions_selective_branches(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test selectively deleting permissions from specific branches."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN multiple branches with permissions
        # Create branches in parallel
        branch_tasks = [
            Folder(name=f"branch_a_{uuid.uuid4()}").store_async(
                parent=project_object, synapse_client=self.syn
            ),
            Folder(name=f"branch_b_{uuid.uuid4()}").store_async(
                parent=project_object, synapse_client=self.syn
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

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(branch_a),
            self._set_custom_permissions(branch_b),
            self._set_custom_permissions(file_a),
            self._set_custom_permissions(file_b),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before selective deletion
        await self._verify_list_acl_functionality(
            entity=branch_a,
            expected_entity_count=2,  # branch_a and file_a
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Verify tree logging occurred for selective branch
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions with mixed entity types in complex structure."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a structure with both files and folders at multiple levels
        structure = await self.create_complex_mixed_structure(project_object)

        # Set permissions on a mix of entities
        await asyncio.gather(
            self._set_custom_permissions(structure["shallow_folder"]),
            self._set_custom_permissions(structure["shallow_file"]),
            self._set_custom_permissions(structure["deep_branch"]),
            self._set_custom_permissions(structure["deep_files"][1]),
            self._set_custom_permissions(structure["mixed_sub_folders"][0]),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async with mixed entity types
        await self._verify_list_acl_functionality(
            entity=project_object,
            expected_entity_count=5,  # All the entities we set permissions on
            recursive=True,
            include_container_content=True,
            target_entity_types=["file", "folder"],
            log_tree=True,
        )

        # Verify tree logging occurred for mixed entity types
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

        # WHEN I delete permissions targeting both files and folders
        await project_object.delete_permissions_async(
            recursive=True,
            include_container_content=True,
            target_entity_types=["file", "folder"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all targeted entities should have permissions deleted
        await asyncio.gather(
            self._verify_permissions_deleted(structure["shallow_folder"]),
            self._verify_permissions_deleted(structure["shallow_file"]),
            self._verify_permissions_deleted(structure["deep_branch"]),
            self._verify_permissions_deleted(structure["deep_files"][1]),
            self._verify_permissions_deleted(structure["mixed_sub_folders"][0]),
        )

    async def test_delete_permissions_no_container_content_but_has_children(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions without include_container_content when children exist."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with children and custom permissions
        parent_folder = await Folder(name=f"parent_folder_{uuid.uuid4()}").store_async(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(parent_folder.id)

        child_file = await File(
            path=utils.make_bogus_uuid_file(), name=f"child_file_{uuid.uuid4()}"
        ).store_async(parent=parent_folder, synapse_client=self.syn)
        self.schedule_for_cleanup(child_file.id)

        # Set permissions on both entities
        await asyncio.gather(
            self._set_custom_permissions(parent_folder),
            self._set_custom_permissions(child_file),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async before testing container content exclusion
        await self._verify_list_acl_functionality(
            entity=parent_folder,
            expected_entity_count=1,  # Only parent_folder, child excluded due to include_container_content=False
            recursive=False,
            include_container_content=False,
            log_tree=True,
        )

        # Verify tree logging occurred (should show limited structure)
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that target_entity_types are case-insensitive."""
        await project_object.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a simple structure with permissions
        structure = await self.create_simple_tree_structure(project_object)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities
        await asyncio.gather(
            self._set_custom_permissions(folder_a),
            self._set_custom_permissions(file_1),
        )
        await asyncio.sleep(random.randint(1, 5))

        # WHEN - Verify list_acl_async with case-insensitive entity types
        await self._verify_list_acl_functionality(
            entity=folder_a,
            expected_entity_count=2,  # folder_a and file_1 (case-insensitive filtering)
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "FOLDER",
                "file",
            ],  # Mixed case to test case-insensitivity
            log_tree=True,
        )

        # Verify tree logging occurred with case-insensitive filtering
        assert "ACL Tree Structure:" in caplog.text
        caplog.clear()

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
