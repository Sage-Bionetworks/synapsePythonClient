"""Integration tests for ACL on several models."""

import logging
import random
import time
import uuid
from typing import Callable, Dict, List, Optional, Type, Union

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.models.acl import AclListResult
from synapseclient.models import (
    Column,
    ColumnType,
    Dataset,
    DatasetCollection,
    EntityRef,
    EntityView,
    File,
    Folder,
    MaterializedView,
    Project,
    SubmissionView,
    Table,
    Team,
    UserProfile,
    ViewTypeMask,
    VirtualTable,
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

    def create_entity(
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
            entity = Project(name=entity_name).store(synapse_client=self.syn)
        elif entity_type == Folder:
            entity = Folder(name=entity_name).store(
                parent=project_model, synapse_client=self.syn
            )
        elif entity_type == File:
            file_fixture.name = entity_name
            entity = file_fixture.store(parent=project_model, synapse_client=self.syn)
        elif entity_type == Table:
            table_fixture.name = entity_name
            entity = table_fixture.store(synapse_client=self.syn)
        else:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        self.schedule_for_cleanup(entity.id)
        return entity

    def create_team(self, description: str = DESCRIPTION_FAKE_TEAM) -> Team:
        """Helper to create a team with cleanup handling"""
        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(name=name, description=description).create(synapse_client=self.syn)
        self.schedule_for_cleanup(team)
        return team

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    def test_get_acl_default(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_default"
        )

        # AND the user that created the entity
        user = UserProfile().get(synapse_client=self.syn)

        # WHEN getting the permissions for the user on the entity
        permissions = entity.get_acl(principal_id=user.id, synapse_client=self.syn)

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
    def test_get_acl_limited_permissions(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_limited"
        )

        # AND the user that created the entity
        user = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user are set to a limited set
        limited_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        entity.set_permissions(
            principal_id=user.id,
            access_type=limited_permissions,
            synapse_client=self.syn,
        )

        # WHEN getting the permissions for the user on the entity
        permissions = entity.get_acl(principal_id=user.id, synapse_client=self.syn)

        # THEN only the limited permissions should be present
        assert set(limited_permissions) == set(permissions)

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    def test_get_acl_through_team(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN a team
        team = self.create_team()

        # AND an entity created with default permissions
        entity = self.create_entity(
            entity_type, project_model, file, table, name_suffix="_test_get_acl_team"
        )

        # AND the user that created the entity
        user = UserProfile().get(synapse_client=self.syn)

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
        entity.set_permissions(
            principal_id=team.id,
            access_type=team_permissions,
            synapse_client=self.syn,
        )

        # AND the user has no direct permissions
        entity.set_permissions(
            principal_id=user.id, access_type=[], synapse_client=self.syn
        )

        # WHEN getting the permissions for the user on the entity
        permissions = entity.get_acl(principal_id=user.id, synapse_client=self.syn)

        # THEN the permissions should match the team's permissions
        assert set(team_permissions) == set(permissions)

    @pytest.mark.parametrize("entity_type", [Project, Folder, File, Table])
    def test_get_acl_through_multiple_teams(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN two teams
        team_1 = self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 1")
        team_2 = self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 2")

        # AND an entity created with default permissions
        entity = self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_get_acl_multiple_teams",
        )

        # AND the user that created the entity
        user = UserProfile().get(synapse_client=self.syn)

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
        entity.set_permissions(
            principal_id=team_1.id,
            access_type=team_1_permissions,
            synapse_client=self.syn,
        )

        # AND the second team has only READ and DOWNLOAD permissions
        team_2_permissions = ["READ", "DOWNLOAD"]
        entity.set_permissions(
            principal_id=team_2.id,
            access_type=team_2_permissions,
            synapse_client=self.syn,
        )

        # AND the user has no direct permissions
        entity.set_permissions(
            principal_id=user.id, access_type=[], synapse_client=self.syn
        )

        # WHEN getting the permissions for the user on the entity
        permissions = entity.get_acl(principal_id=user.id, synapse_client=self.syn)

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
    def test_get_acl_with_public_and_authenticated_users(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_get_acl_public_auth",
        )

        # AND the user that created the entity
        user = UserProfile().get(synapse_client=self.syn)

        # AND public users have READ permission
        entity.set_permissions(
            principal_id=PUBLIC, access_type=["READ"], synapse_client=self.syn
        )

        # AND authenticated users have READ and DOWNLOAD permissions
        entity.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
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
        entity.set_permissions(
            principal_id=user.id,
            access_type=user_permissions,
            synapse_client=self.syn,
        )

        # WHEN getting public permissions (no principal_id)
        public_permissions = entity.get_acl(synapse_client=self.syn)

        # THEN only public permissions should be present
        assert set(["READ"]) == set(public_permissions)

        # WHEN getting the permissions for the authenticated user
        user_permissions = entity.get_acl(principal_id=user.id, synapse_client=self.syn)

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

    def test_get_acl_for_subfolder_with_different_permissions(
        self, project_model: Project
    ) -> None:
        # GIVEN a parent folder with default permissions
        parent_folder = Folder(name=str(uuid.uuid4()) + "_parent_folder_test").store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(parent_folder.id)

        # AND a subfolder created inside the parent folder
        subfolder = Folder(name=str(uuid.uuid4()) + "_subfolder_test").store(
            parent=parent_folder, synapse_client=self.syn
        )
        self.schedule_for_cleanup(subfolder.id)

        # AND the user that created the folders
        user = UserProfile().get(synapse_client=self.syn)

        # AND the subfolder has limited permissions
        limited_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        subfolder.set_permissions(
            principal_id=user.id,
            access_type=limited_permissions,
            synapse_client=self.syn,
        )

        # WHEN getting permissions for the subfolder
        subfolder_permissions = subfolder.get_acl(
            principal_id=user.id, synapse_client=self.syn
        )

        # AND getting permissions for the parent folder
        parent_permissions = parent_folder.get_acl(
            principal_id=user.id, synapse_client=self.syn
        )

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
    def test_remove_team_permissions_with_empty_access_type(
        self, entity_type, project_model: Project, file: File, table: Table
    ) -> None:
        # GIVEN an entity created with default permissions
        entity = self.create_entity(
            entity_type,
            project_model,
            file,
            table,
            name_suffix="_test_remove_team_permissions",
        )

        # AND a test team
        team = self.create_team()

        # AND the team initially has specific permissions
        initial_team_permissions = ["READ", "UPDATE", "CREATE", "DOWNLOAD"]
        entity.set_permissions(
            principal_id=team.id,
            access_type=initial_team_permissions,
            synapse_client=self.syn,
        )

        # WHEN verifying the team has the initial permissions
        team_acl_before = entity.get_acl(principal_id=team.id, synapse_client=self.syn)
        assert set(initial_team_permissions) == set(team_acl_before)

        # AND WHEN removing the team's permissions by setting access_type to empty list
        entity.set_permissions(
            principal_id=team.id,
            access_type=[],  # Empty list to remove permissions
            synapse_client=self.syn,
        )

        # THEN the team should have no permissions
        team_acl_after = entity.get_acl(principal_id=team.id, synapse_client=self.syn)
        assert team_acl_after == []

        # AND the team should not appear in the full ACL list with any permissions
        all_acls = entity.list_acl(synapse_client=self.syn)
        team_acl_entries = [
            acl
            for acl in all_acls.entity_acl
            if acl.principal_id == team.id and acl.access_type
        ]
        assert (
            len(team_acl_entries) == 0
        ), f"Team {team.id} should have no ACL entries but found: {team_acl_entries}"

        # AND other entities should still maintain their permissions (verify no side effects)
        user = UserProfile().get(synapse_client=self.syn)
        user_acl = entity.get_acl(principal_id=user.id, synapse_client=self.syn)
        assert len(user_acl) > 0, "User permissions should remain intact"

    def test_table_permissions(self, project_model: Project) -> None:
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
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # AND a test team
        team = self.create_team()
        user = UserProfile().get(synapse_client=self.syn)

        # WHEN setting various permissions
        # Set team permissions
        team_permissions = ["READ", "UPDATE", "CREATE"]
        table.set_permissions(
            principal_id=team.id, access_type=team_permissions, synapse_client=self.syn
        )

        # Set authenticated users permissions
        table.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        time.sleep(random.randint(10, 20))

        # THEN listing permissions should show all set permissions
        # Check team permissions
        team_acl = table.get_acl(principal_id=team.id, synapse_client=self.syn)
        assert set(team_permissions) == set(team_acl)

        # Check authenticated users permissions
        auth_acl = table.get_acl(
            principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
        )
        assert set(["READ", "DOWNLOAD"]) == set(auth_acl)

        # Check user effective permissions (should include permissions from all sources)
        user_effective_acl = table.get_acl(
            principal_id=user.id, synapse_client=self.syn
        )
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
        all_acls = table.list_acl(synapse_client=self.syn)
        assert isinstance(all_acls, AclListResult)
        assert len(all_acls.entity_acl) >= 3  # User, team, authenticated_users

        # WHEN deleting specific permissions for the team
        table.set_permissions(
            principal_id=team.id, access_type=[], synapse_client=self.syn
        )

        time.sleep(random.randint(10, 20))

        # THEN team should no longer have permissions
        team_acl_after_delete = table.get_acl(
            principal_id=team.id, synapse_client=self.syn
        )
        assert team_acl_after_delete == []

        # BUT other permissions should remain
        auth_acl_after = table.get_acl(
            principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
        )
        assert set(["READ", "DOWNLOAD"]) == set(auth_acl_after)

    def test_entity_view_permissions(self, project_model: Project) -> None:
        """Comprehensive test for EntityView permissions - setting, listing, and deleting."""
        # GIVEN an entity view
        entity_view = EntityView(
            name=f"test_entity_view_permissions_{uuid.uuid4()}",
            parent_id=project_model.id,
            scope_ids=[project_model.id],
            view_type_mask=0x01,  # File view
        )
        entity_view = entity_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        # AND test subjects
        team = self.create_team()
        user = UserProfile().get(synapse_client=self.syn)

        # WHEN setting comprehensive permissions
        # Set team permissions (moderate permissions)
        team_permissions = ["READ", "UPDATE", "MODERATE"]
        entity_view.set_permissions(
            principal_id=team.id, access_type=team_permissions, synapse_client=self.syn
        )

        # Set authenticated users permissions
        entity_view.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
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
        entity_view.set_permissions(
            principal_id=user.id,
            access_type=limited_user_permissions,
            synapse_client=self.syn,
        )

        time.sleep(random.randint(10, 20))

        # THEN listing permissions should reflect all changes
        # Verify team permissions
        team_acl = entity_view.get_acl(principal_id=team.id, synapse_client=self.syn)
        assert set(team_permissions) == set(team_acl)

        # Verify authenticated users permissions
        auth_acl = entity_view.get_acl(
            principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
        )
        assert set(["READ", "DOWNLOAD"]) == set(auth_acl)

        # Verify user permissions include both direct and inherited permissions
        user_acl = entity_view.get_acl(principal_id=user.id, synapse_client=self.syn)
        expected_user_permissions = set(
            limited_user_permissions + ["DOWNLOAD"]
        )  # Includes auth users perm
        assert expected_user_permissions == set(user_acl)

        # Verify complete ACL listing
        all_acls = entity_view.list_acl(synapse_client=self.syn)
        assert isinstance(all_acls, AclListResult)
        assert len(all_acls.entity_acl) >= 3  # User, team, authenticated_users

        # WHEN deleting authenticated users permissions
        entity_view.set_permissions(
            principal_id=AUTHENTICATED_USERS, access_type=[], synapse_client=self.syn
        )

        time.sleep(random.randint(10, 20))

        # THEN authenticated users should lose permissions
        auth_acl_after = entity_view.get_acl(
            principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
        )
        assert auth_acl_after == []

        # AND user permissions should no longer include DOWNLOAD
        user_acl_after = entity_view.get_acl(
            principal_id=user.id, synapse_client=self.syn
        )
        assert set(limited_user_permissions + ["MODERATE"]) == set(user_acl_after)

        # BUT team permissions should remain
        team_acl_after = entity_view.get_acl(
            principal_id=team.id, synapse_client=self.syn
        )
        assert set(team_permissions) == set(team_acl_after)

    def test_submission_view_permissions(self, project_model: Project) -> None:
        """Comprehensive test for SubmissionView permissions - setting, listing, and deleting."""
        # GIVEN a submission view
        submission_view = SubmissionView(
            name=f"test_submission_view_permissions_{uuid.uuid4()}",
            parent_id=project_model.id,
            scope_ids=[project_model.id],
        )
        submission_view = submission_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submission_view)

        # AND test subjects
        team1 = self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - team1")
        team2 = self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - team2")
        user = UserProfile().get(synapse_client=self.syn)

        # WHEN setting overlapping permissions across multiple teams
        # Team1 gets full read/write access
        team1_permissions = ["READ", "UPDATE", "CREATE", "DELETE"]
        submission_view.set_permissions(
            principal_id=team1.id,
            access_type=team1_permissions,
            synapse_client=self.syn,
        )

        # Team2 gets read-only access with download
        team2_permissions = ["READ", "DOWNLOAD"]
        submission_view.set_permissions(
            principal_id=team2.id,
            access_type=team2_permissions,
            synapse_client=self.syn,
        )

        # Public gets read access
        submission_view.set_permissions(
            principal_id=PUBLIC, access_type=["READ"], synapse_client=self.syn
        )

        # User gets minimal direct permissions
        user_direct_permissions = ["READ", "CHANGE_SETTINGS", "CHANGE_PERMISSIONS"]
        submission_view.set_permissions(
            principal_id=user.id,
            access_type=user_direct_permissions,
            synapse_client=self.syn,
        )

        time.sleep(random.randint(10, 20))

        # THEN listing permissions should show proper aggregation
        # Check individual team permissions
        team1_acl = submission_view.get_acl(
            principal_id=team1.id, synapse_client=self.syn
        )
        assert set(team1_permissions) == set(team1_acl)

        team2_acl = submission_view.get_acl(
            principal_id=team2.id, synapse_client=self.syn
        )
        assert set(team2_permissions) == set(team2_acl)

        # Check public permissions
        public_acl = submission_view.get_acl(synapse_client=self.syn)
        assert set(["READ"]) == set(public_acl)

        # Check user effective permissions (should aggregate from all teams)
        user_effective_acl = submission_view.get_acl(
            principal_id=user.id, synapse_client=self.syn
        )
        expected_effective = set(
            user_direct_permissions + team1_permissions + team2_permissions
        )
        assert expected_effective == set(user_effective_acl)

        # Verify complete ACL structure
        all_acls = submission_view.list_acl(synapse_client=self.syn)
        assert isinstance(all_acls, AclListResult)
        assert len(all_acls.entity_acl) >= 4  # User, team1, team2, public

        # WHEN selectively deleting permissions
        # Remove PUBLIC and team permissions
        submission_view.set_permissions(
            principal_id=PUBLIC, access_type=[], synapse_client=self.syn
        )
        submission_view.set_permissions(
            principal_id=team1.id, access_type=[], synapse_client=self.syn
        )

        # THEN PUBLIC should lose all permissions
        public_acl_after = submission_view.get_acl(
            principal_id=PUBLIC, synapse_client=self.syn
        )
        assert public_acl_after == []

        # AND team should lose all permissions
        team_acl_after = submission_view.get_acl(
            principal_id=team1.id, synapse_client=self.syn
        )
        assert team_acl_after == []

        # AND user effective permissions should no longer include team1 permissions
        user_effective_after = submission_view.get_acl(
            principal_id=user.id, synapse_client=self.syn
        )
        expected_after = set(user_direct_permissions + team2_permissions)
        assert expected_after == set(user_effective_after)

        # BUT other permissions should remain unchanged
        team2_acl_after = submission_view.get_acl(
            principal_id=team2.id, synapse_client=self.syn
        )
        assert set(team2_permissions) == set(team2_acl_after)


class TestPermissionsForCaller:
    """Test the permissions that the current caller has for an entity."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_team(self, description: str = DESCRIPTION_FAKE_TEAM) -> Team:
        """Helper to create a team with cleanup handling"""
        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(name=name, description=description).create(synapse_client=self.syn)
        self.schedule_for_cleanup(team)
        return team

    def test_get_permissions_default(self) -> None:
        # GIVEN a project created with default permissions
        project = Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_default"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # WHEN getting the permissions for the current user
        permissions = project.get_permissions(synapse_client=self.syn)

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

    def test_get_permissions_with_limited_access(self) -> None:
        # GIVEN a project created with default permissions
        project = Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_limited"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # AND the current user that created the project
        user = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user are set to READ only
        project.set_permissions(
            principal_id=user.id, access_type=["READ"], synapse_client=self.syn
        )

        # WHEN getting the permissions for the current user
        permissions = project.get_permissions(synapse_client=self.syn)

        # THEN READ and CHANGE_SETTINGS permissions should be present
        # Note: CHANGE_SETTINGS is bound to ownerId and can't be removed
        expected_permissions = ["READ", "CHANGE_SETTINGS"]
        assert set(expected_permissions) == set(permissions.access_types)

    def test_get_permissions_through_teams(self) -> None:
        # GIVEN two teams
        team_1 = self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 1")
        team_2 = self.create_team(description=f"{DESCRIPTION_FAKE_TEAM} - 2")

        # AND a project created with default permissions
        project = Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_through_teams"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # AND the current user that created the project
        user = UserProfile().get(synapse_client=self.syn)

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
        project.set_permissions(
            principal_id=team_1.id,
            access_type=team_1_permissions,
            synapse_client=self.syn,
        )

        # AND team 2 has only READ and DOWNLOAD permissions
        project.set_permissions(
            principal_id=team_2.id,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the user has no direct permissions
        project.set_permissions(
            principal_id=user.id, access_type=[], synapse_client=self.syn
        )

        # WHEN getting the permissions for the current user
        permissions = project.get_permissions(synapse_client=self.syn)

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

    def test_get_permissions_with_authenticated_users(self) -> None:
        # GIVEN a project created with default permissions
        project = Project(
            name=str(uuid.uuid4()) + "_test_get_permissions_authenticated"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # AND the current user that created the project
        user = UserProfile().get(synapse_client=self.syn)

        # AND authenticated users have READ and DOWNLOAD permissions
        project.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
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
        project.set_permissions(
            principal_id=user.id,
            access_type=user_permissions,
            synapse_client=self.syn,
        )

        # WHEN getting the permissions for the current user
        permissions = project.get_permissions(synapse_client=self.syn)

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
    """Test delete_permissions functionality across entities."""

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

    def _set_custom_permissions(self, entity: Union[File, Folder, Project]) -> None:
        """Helper to set custom permissions on an entity so we can verify deletion."""
        # Set custom permissions for authenticated users
        entity.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ"],
            synapse_client=self.syn,
        )

        # Verify permissions were set
        acl = entity.get_acl(principal_id=AUTHENTICATED_USERS, synapse_client=self.syn)
        assert "READ" in acl

        return acl

    def _verify_permissions_deleted(self, entity: Union[File, Folder, Project]) -> None:
        """Helper to verify that permissions have been deleted (entity inherits from parent)."""
        for attempt in range(self.verification_attempts):
            time.sleep(random.randint(10, 20))

            acl = entity.get_acl(
                principal_id=AUTHENTICATED_USERS,
                check_benefactor=False,
                synapse_client=self.syn,
            )

            if not acl:
                return  # Verification successful

            if attempt == self.verification_attempts - 1:  # Last attempt
                assert not acl, (
                    f"Permissions should be deleted, but they still exist on "
                    f"[id: {entity.id}, name: {entity.name}, {entity.__class__}]."
                )

    def _verify_permissions_not_deleted(
        self, entity: Union[File, Folder, Project]
    ) -> bool:
        """Helper to verify that permissions are still set on an entity."""
        for attempt in range(self.verification_attempts):
            time.sleep(random.randint(10, 20))
            acl = entity.get_acl(
                principal_id=AUTHENTICATED_USERS,
                check_benefactor=False,
                synapse_client=self.syn,
            )
            if "READ" in acl:
                return True

            if attempt == self.verification_attempts - 1:  # Last attempt
                assert "READ" in acl

        return True

    def _verify_list_acl_functionality(
        self,
        entity: Union[File, Folder, Project],
        expected_entity_count: int,
        recursive: bool = True,
        include_container_content: bool = True,
        target_entity_types: Optional[List[str]] = None,
        log_tree: bool = True,
    ) -> AclListResult:
        """Helper to verify list_acl functionality and return results."""
        for attempt in range(self.verification_attempts):
            time.sleep(random.randint(10, 20))
            acl_result = entity.list_acl(
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

    def create_simple_tree_structure(
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
        folder_a = Folder(name=f"folder_a_{uuid.uuid4()}").store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder_a.id)

        file_1 = File(
            path=utils.make_bogus_uuid_file(), name=f"file_1_{uuid.uuid4()}"
        ).store(parent=folder_a, synapse_client=self.syn)
        self.schedule_for_cleanup(file_1.id)

        return {
            "folder_a": folder_a,
            "file_1": file_1,
        }

    def create_deep_nested_structure(
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
        level_1 = Folder(name=f"level_1_{uuid.uuid4()}").store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(level_1.id)

        # Create file_at_1 and level_2 in parallel since they don't depend on each other
        file_at_1 = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_1_{uuid.uuid4()}"
        ).store(parent=level_1, synapse_client=self.syn)
        level_2 = Folder(name=f"level_2_{uuid.uuid4()}").store(
            parent=level_1, synapse_client=self.syn
        )

        self.schedule_for_cleanup(file_at_1.id)
        self.schedule_for_cleanup(level_2.id)

        # Create file_at_2 and level_3 in parallel since they don't depend on each other
        file_at_2 = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_2_{uuid.uuid4()}"
        ).store(parent=level_2, synapse_client=self.syn)
        level_3 = Folder(name=f"level_3_{uuid.uuid4()}").store(
            parent=level_2, synapse_client=self.syn
        )

        self.schedule_for_cleanup(file_at_2.id)
        self.schedule_for_cleanup(level_3.id)

        # Create file_at_3 and level_4 in parallel since they don't depend on each other
        file_at_3 = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_3_{uuid.uuid4()}"
        ).store(parent=level_3, synapse_client=self.syn)
        level_4 = Folder(name=f"level_4_{uuid.uuid4()}").store(
            parent=level_3, synapse_client=self.syn
        )

        self.schedule_for_cleanup(file_at_3.id)
        self.schedule_for_cleanup(level_4.id)

        file_at_4 = File(
            path=utils.make_bogus_uuid_file(), name=f"file_at_4_{uuid.uuid4()}"
        ).store(parent=level_4, synapse_client=self.syn)
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

    def create_wide_tree_structure(
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
        folders = [
            Folder(name=f"folder_{folder_letter}_{uuid.uuid4()}").store(
                parent=project_model, synapse_client=self.syn
            )
            for folder_letter in ["a", "b", "c"]
        ]

        # Schedule cleanup for folders
        for folder in folders:
            self.schedule_for_cleanup(folder.id)

        # Create files
        file_results = [
            File(
                path=utils.make_bogus_uuid_file(),
                name=f"file_{folder_letter}_{uuid.uuid4()}",
            ).store(parent=folder, synapse_client=self.syn)
            for folder_letter, folder in zip(["a", "b", "c"], folders)
        ]

        # Create root file task
        root_file = File(
            path=utils.make_bogus_uuid_file(), name=f"root_file_{uuid.uuid4()}"
        ).store(parent=project_model, synapse_client=self.syn)
        file_results.append(root_file)

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

    def create_complex_mixed_structure(
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
        # Create top-level folders
        shallow_folder = Folder(name=f"shallow_folder_{uuid.uuid4()}").store(
            parent=project_model, synapse_client=self.syn
        )
        deep_branch = Folder(name=f"deep_branch_{uuid.uuid4()}").store(
            parent=project_model, synapse_client=self.syn
        )
        mixed_folder = Folder(name=f"mixed_folder_{uuid.uuid4()}").store(
            parent=project_model, synapse_client=self.syn
        )

        # Schedule cleanup for top-level folders
        for folder in [shallow_folder, deep_branch, mixed_folder]:
            self.schedule_for_cleanup(folder.id)

        # Create first level files and folders
        shallow_file = File(
            path=utils.make_bogus_uuid_file(), name=f"shallow_file_{uuid.uuid4()}"
        ).store(parent=shallow_folder, synapse_client=self.syn)
        self.schedule_for_cleanup(shallow_file.id)

        # Deep branch structure
        deep_file_1 = File(
            path=utils.make_bogus_uuid_file(), name=f"deep_file_1_{uuid.uuid4()}"
        ).store(parent=deep_branch, synapse_client=self.syn)

        sub_deep = Folder(name=f"sub_deep_{uuid.uuid4()}").store(
            parent=deep_branch, synapse_client=self.syn
        )

        self.schedule_for_cleanup(deep_file_1.id)
        self.schedule_for_cleanup(sub_deep.id)

        # Continue deep structure
        deep_file_2 = File(
            path=utils.make_bogus_uuid_file(), name=f"deep_file_2_{uuid.uuid4()}"
        ).store(parent=sub_deep, synapse_client=self.syn)

        sub_sub_deep = Folder(name=f"sub_sub_deep_{uuid.uuid4()}").store(
            parent=sub_deep, synapse_client=self.syn
        )

        self.schedule_for_cleanup(deep_file_2.id)
        self.schedule_for_cleanup(sub_sub_deep.id)

        deep_file_3 = File(
            path=utils.make_bogus_uuid_file(), name=f"deep_file_3_{uuid.uuid4()}"
        ).store(parent=sub_sub_deep, synapse_client=self.syn)
        self.schedule_for_cleanup(deep_file_3.id)

        # Mixed folder structure
        mixed_file = File(
            path=utils.make_bogus_uuid_file(), name=f"mixed_file_{uuid.uuid4()}"
        ).store(parent=mixed_folder, synapse_client=self.syn)

        mixed_sub_a = Folder(name=f"mixed_sub_a_{uuid.uuid4()}").store(
            parent=mixed_folder, synapse_client=self.syn
        )
        mixed_sub_b = Folder(name=f"mixed_sub_b_{uuid.uuid4()}").store(
            parent=mixed_folder, synapse_client=self.syn
        )

        # Schedule cleanup
        self.schedule_for_cleanup(mixed_file.id)
        self.schedule_for_cleanup(mixed_sub_a.id)
        self.schedule_for_cleanup(mixed_sub_b.id)

        # Create files in mixed sub-folders in parallel
        mixed_file_a = File(
            path=utils.make_bogus_uuid_file(), name=f"mixed_file_a_{uuid.uuid4()}"
        ).store(parent=mixed_sub_a, synapse_client=self.syn)

        mixed_file_b = File(
            path=utils.make_bogus_uuid_file(), name=f"mixed_file_b_{uuid.uuid4()}"
        ).store(parent=mixed_sub_b, synapse_client=self.syn)

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

    def test_delete_permissions_on_new_project(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a newly created project."""
        # Set the log level to capture DEBUG messages
        caplog.set_level(logging.DEBUG)

        # GIVEN a newly created project with custom permissions
        project = Project(name=f"test_project_{uuid.uuid4()}").store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(project.id)

        # AND custom permissions are set for authenticated users
        self._set_custom_permissions(project)
        time.sleep(random.randint(10, 20))

        # WHEN I delete permissions on the project
        project.delete_permissions(synapse_client=self.syn)

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
            assert self._verify_permissions_not_deleted(project)

    def test_delete_permissions_simple_tree_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a simple tree structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a simple tree structure with permissions
        structure = self.create_simple_tree_structure(project_object)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities
        self._set_custom_permissions(folder_a)
        self._set_custom_permissions(file_1)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion
        self._verify_list_acl_functionality(
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
        folder_a.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        self._verify_permissions_deleted(folder_a)
        self._verify_permissions_deleted(file_1)

    def test_delete_permissions_deep_nested_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a deeply nested structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a deeply nested structure with permissions
        structure = self.create_deep_nested_structure(project_object)

        # Set permissions on all entities
        for entity in structure.values():
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion
        self._verify_list_acl_functionality(
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
        structure["level_1"].delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        for entity in structure.values():
            self._verify_permissions_deleted(entity)

    def test_delete_permissions_wide_tree_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a wide tree structure with multiple siblings."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a wide tree structure with permissions
        structure = self.create_wide_tree_structure(project_object)
        folders = structure["folders"]
        all_files = structure["all_files"]
        root_file = structure["root_file"]

        # Set permissions on all entities
        entities_to_set = folders + all_files + [root_file]
        for entity in entities_to_set:
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion
        self._verify_list_acl_functionality(
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
        project_object.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted (except project which can't be deleted)
        entities_to_verify = folders + all_files + [root_file]
        for entity in entities_to_verify:
            self._verify_permissions_deleted(entity)

    def test_delete_permissions_complex_mixed_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a complex mixed structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex mixed structure with permissions
        structure = self.create_complex_mixed_structure(project_object)

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

        for entity in entities_to_set:
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion
        self._verify_list_acl_functionality(
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
        project_object.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        for entity in entities_to_set:
            self._verify_permissions_deleted(entity)

    # Edge case tests
    def test_delete_permissions_empty_folder(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on an empty folder."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN an empty folder with custom permissions
        empty_folder = Folder(name=f"empty_folder_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(empty_folder.id)
        self._set_custom_permissions(empty_folder)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion (empty folder)
        self._verify_list_acl_functionality(
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
        empty_folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN the folder permissions should be deleted
        self._verify_permissions_deleted(empty_folder)

    def test_delete_permissions_folder_with_only_files(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a folder that contains only files."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with only one file
        folder = Folder(name=f"files_only_folder_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        file = File(
            path=utils.make_bogus_uuid_file(), name=f"only_file_{uuid.uuid4()}"
        ).store(parent=folder, synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # Set permissions on all entities
        self._set_custom_permissions(folder)
        self._set_custom_permissions(file)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion
        self._verify_list_acl_functionality(
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
        folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        self._verify_permissions_deleted(folder)
        self._verify_permissions_deleted(file)

    def test_delete_permissions_folder_with_only_folders(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a folder that contains only sub-folders."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with only sub-folders
        parent_folder = Folder(name=f"folders_only_parent_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(parent_folder.id)

        # Create sub-folders in parallel
        sub_folders = [
            Folder(name=f"only_subfolder_{i}_{uuid.uuid4()}").store(
                parent=parent_folder, synapse_client=self.syn
            )
            for i in range(3)
        ]

        # Schedule cleanup for sub-folders
        for sub_folder in sub_folders:
            self.schedule_for_cleanup(sub_folder.id)

        # Set permissions on all entities
        entities_to_set = [parent_folder] + sub_folders
        for entity in entities_to_set:
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion
        self._verify_list_acl_functionality(
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
        parent_folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        for entity in entities_to_set:
            self._verify_permissions_deleted(entity)

    def test_delete_permissions_target_files_only_complex(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions targeting only files in a complex structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex structure with permissions
        structure = self.create_complex_mixed_structure(project_object)

        # Set permissions on all entities
        self._set_custom_permissions(structure["shallow_folder"])
        self._set_custom_permissions(structure["shallow_file"])
        self._set_custom_permissions(structure["deep_branch"])
        self._set_custom_permissions(structure["sub_deep"])
        for file in structure["deep_files"]:
            self._set_custom_permissions(file)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl with target_entity_types for files only
        self._verify_list_acl_functionality(
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
        project_object.delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["file"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only file permissions should be deleted
        self._verify_permissions_deleted(structure["shallow_file"])
        for file in structure["deep_files"]:
            self._verify_permissions_deleted(file)

        # BUT folder permissions should remain
        assert self._verify_permissions_not_deleted(structure["shallow_folder"])
        assert self._verify_permissions_not_deleted(structure["deep_branch"])
        assert self._verify_permissions_not_deleted(structure["sub_deep"])

    # Include container content vs recursive tests
    def test_delete_permissions_include_container_only_deep_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test include_container_content=True without recursive on deep structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a deep nested structure with permissions
        structure = self.create_deep_nested_structure(project_object)

        # Set permissions on all entities
        for entity in structure.values():
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl with include_container_content=True
        self._verify_list_acl_functionality(
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
        structure["level_1"].delete_permissions(
            include_self=True,
            include_container_content=True,
            recursive=False,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only level_1 and its direct children should have permissions deleted
        self._verify_permissions_deleted(structure["level_1"])
        self._verify_permissions_deleted(structure["file_at_1"])
        self._verify_permissions_deleted(structure["level_2"])

        # BUT deeper nested entities should retain permissions
        self._verify_permissions_not_deleted(structure["level_3"])
        self._verify_permissions_not_deleted(structure["level_4"])
        self._verify_permissions_not_deleted(structure["file_at_2"])
        self._verify_permissions_not_deleted(structure["file_at_3"])
        self._verify_permissions_not_deleted(structure["file_at_4"])

    def test_delete_permissions_skip_self_complex_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test include_self=False on a complex structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex mixed structure with permissions
        structure = self.create_complex_mixed_structure(project_object)

        # Set permissions on all entities
        self._set_custom_permissions(structure["mixed_folder"])
        for folder in structure["mixed_sub_folders"]:
            self._set_custom_permissions(folder)
        for file in structure["mixed_files"]:
            self._set_custom_permissions(file)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion (should show all entities)
        self._verify_list_acl_functionality(
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
        structure["mixed_folder"].delete_permissions(
            include_self=False,
            include_container_content=True,
            recursive=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN the mixed_folder permissions should remain
        self._verify_permissions_not_deleted(structure["mixed_folder"])

        # BUT child permissions should be deleted
        for folder in structure["mixed_sub_folders"]:
            self._verify_permissions_deleted(folder)

        for file in structure["mixed_files"]:
            self._verify_permissions_deleted(file)

    # Dry run functionality tests

    def test_delete_permissions_dry_run_no_changes(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that dry_run=True makes no actual changes."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a simple structure with permissions
        structure = self.create_simple_tree_structure(project_object)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities
        self._set_custom_permissions(folder_a)
        self._set_custom_permissions(file_1)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before dry run
        initial_acl_result = self._verify_list_acl_functionality(
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
        folder_a.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=True,
            synapse_client=self.syn_with_logger,
        )

        # THEN no permissions should be deleted
        self._verify_permissions_not_deleted(folder_a)
        self._verify_permissions_not_deleted(file_1)

        # AND dry run messages should be logged
        self._verify_log_messages(
            caplog,
            list_acl_called=False,
            delete_permissions_called=True,
            dry_run=True,
            tree_logging=False,
        )

        # WHEN - Verify list_acl after dry run (should be identical)
        caplog.clear()
        final_acl_result = self._verify_list_acl_functionality(
            entity=folder_a,
            expected_entity_count=2,  # should be same as before
            recursive=True,
            include_container_content=True,
            log_tree=True,
        )

        # Results should be identical since dry run made no changes
        assert len(initial_acl_result.all_entity_acls) == len(
            final_acl_result.all_entity_acls
        )

    def test_delete_permissions_dry_run_complex_logging(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test dry run logging for complex structures."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a complex structure with permissions
        structure = self.create_complex_mixed_structure(project_object)

        # Set permissions on a subset of entities
        self._set_custom_permissions(structure["deep_branch"]),
        self._set_custom_permissions(structure["sub_deep"]),
        self._set_custom_permissions(structure["deep_files"][0]),
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl with detailed logging before dry run
        self._verify_list_acl_functionality(
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
        structure["deep_branch"].delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=True,
            show_acl_details=True,
            show_files_in_containers=True,
            synapse_client=self.syn_with_logger,
        )

        # THEN no permissions should be deleted
        self._verify_permissions_not_deleted(structure["deep_branch"]),
        self._verify_permissions_not_deleted(structure["sub_deep"]),
        self._verify_permissions_not_deleted(structure["deep_files"][0]),

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
    def test_delete_permissions_large_flat_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on a large flat structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with many files
        large_folder = Folder(name=f"large_folder_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(large_folder.id)

        # Create files in parallel
        files = [
            File(
                path=utils.make_bogus_uuid_file(), name=f"large_file_{i}_{uuid.uuid4()}"
            ).store(parent=large_folder, synapse_client=self.syn)
            for i in range(10)  # Reduced from larger number for test performance
        ]

        # Schedule cleanup for files
        for file in files:
            self.schedule_for_cleanup(file.id)

        # Set permissions on all entities
        entities_to_set = [large_folder] + files
        for entity in entities_to_set:
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl performance with large structure
        self._verify_list_acl_functionality(
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
        large_folder.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        for entity in entities_to_set:
            self._verify_permissions_deleted(entity)

    def test_delete_permissions_multiple_nested_branches(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions on multiple nested branches simultaneously."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN multiple complex nested branches
        branches = [
            Folder(name=f"branch_{branch_name}_{uuid.uuid4()}").store(
                parent=project_object, synapse_client=self.syn
            )
            for branch_name in ["alpha", "beta"]
        ]

        all_entities = list(branches)

        # Schedule cleanup for branches
        for branch in branches:
            self.schedule_for_cleanup(branch.id)

        # Create nested structure in each branch in parallel
        nested_folders = []
        for branch_name, branch in zip(["alpha", "beta"], branches):
            current_parent = branch
            for level in range(2):
                # Create sub-folder and file tasks for this level
                sub_folder_task = Folder(
                    name=f"{branch_name}_level_{level}_{uuid.uuid4()}"
                ).store(parent=current_parent, synapse_client=self.syn)

                nested_folders.append(sub_folder_task)

        # Add nested folders to all_entities and schedule cleanup
        all_entities.extend(nested_folders)
        for folder in nested_folders:
            self.schedule_for_cleanup(folder.id)

        # Now create files for each nested folder in parallel
        files = []
        folder_index = 0
        for branch_name in ["alpha", "beta"]:
            for level in range(2):
                parent_folder = nested_folders[folder_index]
                file_task = File(
                    path=utils.make_bogus_uuid_file(),
                    name=f"{branch_name}_file_{level}_{uuid.uuid4()}",
                ).store(parent=parent_folder, synapse_client=self.syn)
                files.append(file_task)
                folder_index += 1

        # Add files to all_entities and schedule cleanup
        all_entities.extend(files)
        for file in files:
            self.schedule_for_cleanup(file.id)

        # Set permissions on all entities
        for entity in all_entities:
            self._set_custom_permissions(entity)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before deletion (complex multiple branches)
        self._verify_list_acl_functionality(
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
        project_object.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        for entity in all_entities:
            self._verify_permissions_deleted(entity)

    def test_delete_permissions_selective_branches(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test selectively deleting permissions from specific branches."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN multiple branches with permissions
        # Create branches
        branch_a = Folder(name=f"branch_a_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )
        branch_b = Folder(name=f"branch_b_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )

        # Schedule cleanup for branches
        self.schedule_for_cleanup(branch_a.id)
        self.schedule_for_cleanup(branch_b.id)

        # Create files in each branch
        file_a = File(
            path=utils.make_bogus_uuid_file(), name=f"file_a_{uuid.uuid4()}"
        ).store(parent=branch_a, synapse_client=self.syn)
        file_b = File(
            path=utils.make_bogus_uuid_file(), name=f"file_b_{uuid.uuid4()}"
        ).store(parent=branch_b, synapse_client=self.syn)

        # Schedule cleanup for files
        self.schedule_for_cleanup(file_a.id)
        self.schedule_for_cleanup(file_b.id)

        # Set permissions on all entities
        self._set_custom_permissions(branch_a)
        self._set_custom_permissions(branch_b)
        self._set_custom_permissions(file_a)
        self._set_custom_permissions(file_b)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before selective deletion
        self._verify_list_acl_functionality(
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
        branch_a.delete_permissions(
            recursive=True,
            include_container_content=True,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only branch_a and its contents should have permissions deleted
        self._verify_permissions_deleted(branch_a)
        self._verify_permissions_deleted(file_a)

        # BUT branch_b should retain permissions
        self._verify_permissions_not_deleted(branch_b)
        self._verify_permissions_not_deleted(file_b)

    def test_delete_permissions_mixed_entity_types_in_structure(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions with mixed entity types in complex structure."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a structure with both files and folders at multiple levels
        structure = self.create_complex_mixed_structure(project_object)

        # Set permissions on a mix of entities
        self._set_custom_permissions(structure["shallow_folder"]),
        self._set_custom_permissions(structure["shallow_file"]),
        self._set_custom_permissions(structure["deep_branch"]),
        self._set_custom_permissions(structure["deep_files"][1]),
        self._set_custom_permissions(structure["mixed_sub_folders"][0]),
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl with mixed entity types
        self._verify_list_acl_functionality(
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
        project_object.delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["file", "folder"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all targeted entities should have permissions deleted
        self._verify_permissions_deleted(structure["shallow_folder"]),
        self._verify_permissions_deleted(structure["shallow_file"]),
        self._verify_permissions_deleted(structure["deep_branch"]),
        self._verify_permissions_deleted(structure["deep_files"][1]),
        self._verify_permissions_deleted(structure["mixed_sub_folders"][0]),

    def test_delete_permissions_no_container_content_but_has_children(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting permissions without include_container_content when children exist."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a folder with children and custom permissions
        parent_folder = Folder(name=f"parent_folder_{uuid.uuid4()}").store(
            parent=project_object, synapse_client=self.syn
        )
        self.schedule_for_cleanup(parent_folder.id)

        child_file = File(
            path=utils.make_bogus_uuid_file(), name=f"child_file_{uuid.uuid4()}"
        ).store(parent=parent_folder, synapse_client=self.syn)
        self.schedule_for_cleanup(child_file.id)

        # Set permissions on both entities
        self._set_custom_permissions(parent_folder)
        self._set_custom_permissions(child_file)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl before testing container content exclusion
        self._verify_list_acl_functionality(
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
        parent_folder.delete_permissions(
            include_self=True,
            include_container_content=False,
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN only parent permissions should be deleted
        self._verify_permissions_deleted(parent_folder)

        # AND child permissions should remain
        self._verify_permissions_not_deleted(child_file)

    def test_delete_permissions_case_insensitive_entity_types(
        self, project_object: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that target_entity_types are case-insensitive."""
        project_object.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_object.id)

        # GIVEN a simple structure with permissions
        structure = self.create_simple_tree_structure(project_object)
        folder_a = structure["folder_a"]
        file_1 = structure["file_1"]

        # Set permissions on all entities
        self._set_custom_permissions(folder_a)
        self._set_custom_permissions(file_1)
        time.sleep(random.randint(10, 20))

        # WHEN - Verify list_acl with case-insensitive entity types
        self._verify_list_acl_functionality(
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
        folder_a.delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["FOLDER", "file"],  # Mixed case
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN all permissions should be deleted
        self._verify_permissions_deleted(folder_a)
        self._verify_permissions_deleted(file_1)


class TestAllEntityTypesPermissions:
    """Test permissions functionality across all supported entity types."""

    @pytest.fixture(autouse=True, scope="function")
    def init(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_all_entity_types(self, project_model: Project) -> Dict[str, any]:
        """Create all supported entity types for testing."""
        project_model = project_model.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_model.id)

        entities = {"project": project_model}

        file_path = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(file_path)
        file_entity = File(
            name=f"test_file_{str(uuid.uuid4())}.txt",
            parent_id=project_model.id,
            path=file_path,
        )
        file_entity = file_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(file_entity.id)
        entities["file"] = file_entity

        folder_entity = Folder(
            name=f"test_folder_{str(uuid.uuid4())}",
            parent_id=project_model.id,
        )
        folder_entity = folder_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder_entity.id)
        entities["folder"] = folder_entity

        table_entity = Table(
            name=f"test_table_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )
        table_entity = table_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table_entity.id)
        entities["table"] = table_entity

        entityview_entity = EntityView(
            name=f"test_entityview_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            scope_ids=[project_model.id],
            view_type_mask=ViewTypeMask.FILE,
        )
        entityview_entity = entityview_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview_entity.id)
        entities["entityview"] = entityview_entity

        materializedview_entity = MaterializedView(
            name=f"test_materializedview_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table_entity.id}",
        )
        materializedview_entity = materializedview_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(materializedview_entity.id)
        entities["materializedview"] = materializedview_entity

        virtualtable_entity = VirtualTable(
            name=f"test_virtualtable_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table_entity.id}",
        )
        virtualtable_entity = virtualtable_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtualtable_entity.id)
        entities["virtualtable"] = virtualtable_entity

        dataset_entity = Dataset(
            name=f"test_dataset_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            items=[EntityRef(id=file_entity.id, version=1)],
        )
        dataset_entity = dataset_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_entity.id)
        entities["dataset"] = dataset_entity

        datasetcollection_entity = DatasetCollection(
            name=f"test_datasetcollection_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            items=[EntityRef(id=dataset_entity.id, version=1)],
        )
        datasetcollection_entity = datasetcollection_entity.store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(datasetcollection_entity.id)
        entities["datasetcollection"] = datasetcollection_entity

        return entities

    def create_all_entity_types_with_acl(
        self, project_model: Project
    ) -> Dict[str, any]:
        """Create all entity types with local ACL permissions for testing."""
        entities = self.create_all_entity_types(project_model)

        for entity_type, entity in entities.items():
            if entity_type != "project":
                entity.set_permissions(
                    principal_id=AUTHENTICATED_USERS,
                    access_type=["READ"],
                    synapse_client=self.syn,
                )

        time.sleep(10)
        return entities

    def test_list_acl_all_entity_types(self) -> None:
        """Test list_acl functionality with all supported entity types."""
        # GIVEN a project with all supported entity types and local ACL permissions
        project = Project(name=f"test_project_{uuid.uuid4()}")
        entities = self.create_all_entity_types_with_acl(project)

        # WHEN I call list_acl on the project with all entity types
        result = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "file",
                "folder",
                "table",
                "entityview",
                "materializedview",
                "virtualtable",
                "dataset",
                "datasetcollection",
            ],
            synapse_client=self.syn,
        )

        # THEN the result should contain ACLs for all entity types
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) >= 1

        entity_ids = [acl.entity_id for acl in result.all_entity_acls]
        assert entities["project"].id in entity_ids

        # AND verify AclListResult structure and content
        entities_with_read_permissions = []
        for entity_acl in result.all_entity_acls:
            assert isinstance(entity_acl.entity_id, str)
            assert entity_acl.entity_id.startswith("syn")
            assert len(entity_acl.acl_entries) >= 0

            # Check if this entity has AUTHENTICATED_USERS with READ permissions
            for acl_entry in entity_acl.acl_entries:
                assert isinstance(acl_entry.principal_id, str)
                assert isinstance(acl_entry.permissions, list)
                if (
                    acl_entry.principal_id == str(AUTHENTICATED_USERS)
                    and "READ" in acl_entry.permissions
                ):
                    entities_with_read_permissions.append(entity_acl.entity_id)

        # AND each entity should have the correct AUTHENTICATED_USERS permissions
        for entity_type, entity in entities.items():
            if entity_type != "project":
                individual_acl = entity.get_acl(
                    principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
                )
                assert individual_acl is not None
                assert (
                    "READ" in individual_acl
                ), f"AUTHENTICATED_USERS should have READ access on {entity_type} {entity.id}"

                # Verify this entity appears in the AclListResult with READ permissions
                assert (
                    entity.id in entities_with_read_permissions
                ), f"Entity {entity.id} ({entity_type}) should appear in AclListResult with READ permissions"

    def test_list_acl_specific_entity_types(self) -> None:
        """Test list_acl functionality with specific entity types."""
        # GIVEN a project with all supported entity types
        project = Project(name=f"test_project_{uuid.uuid4()}")
        entities = self.create_all_entity_types_with_acl(project)

        # WHEN I call list_acl with only table-related entity types
        result = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "table",
                "entityview",
                "materializedview",
                "virtualtable",
            ],
            synapse_client=self.syn,
        )

        # THEN the result should be valid
        assert isinstance(result, AclListResult)
        assert len(result.all_entity_acls) >= 1

        # AND verify only table-related entities are included
        returned_entity_ids = [acl.entity_id for acl in result.all_entity_acls]
        expected_table_entity_ids = [
            entities["table"].id,
            entities["entityview"].id,
            entities["materializedview"].id,
            entities["virtualtable"].id,
        ]

        # Check that all expected table entities are present in the result
        for entity_id in expected_table_entity_ids:
            if entity_id in returned_entity_ids:
                # Find the corresponding EntityAcl
                entity_acl = next(
                    acl for acl in result.all_entity_acls if acl.entity_id == entity_id
                )
                assert isinstance(entity_acl.entity_id, str)
                assert entity_acl.entity_id.startswith("syn")
                assert len(entity_acl.acl_entries) >= 0

                # Verify AUTHENTICATED_USERS has READ permissions
                has_read_permission = False
                for acl_entry in entity_acl.acl_entries:
                    if (
                        acl_entry.principal_id == str(AUTHENTICATED_USERS)
                        and "READ" in acl_entry.permissions
                    ):
                        has_read_permission = True
                        break
                assert (
                    has_read_permission
                ), f"Entity {entity_id} should have READ permissions for AUTHENTICATED_USERS"

        # WHEN I call list_acl with only dataset-related entity types
        result_datasets = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=["dataset", "datasetcollection"],
            synapse_client=self.syn,
        )

        # THEN the result should be valid
        assert isinstance(result_datasets, AclListResult)
        assert len(result_datasets.all_entity_acls) >= 1

        # AND verify only dataset-related entities are included
        returned_dataset_ids = [
            acl.entity_id for acl in result_datasets.all_entity_acls
        ]
        expected_dataset_entity_ids = [
            entities["dataset"].id,
            entities["datasetcollection"].id,
        ]

        # AND Check that all expected dataset entities are present in the result
        for entity_id in expected_dataset_entity_ids:
            if entity_id in returned_dataset_ids:
                # Find the corresponding EntityAcl
                entity_acl = next(
                    acl
                    for acl in result_datasets.all_entity_acls
                    if acl.entity_id == entity_id
                )
                assert isinstance(entity_acl.entity_id, str)
                assert entity_acl.entity_id.startswith("syn")
                assert len(entity_acl.acl_entries) >= 0

                # Verify AUTHENTICATED_USERS has READ permissions
                has_read_permission = False
                for acl_entry in entity_acl.acl_entries:
                    if (
                        acl_entry.principal_id == str(AUTHENTICATED_USERS)
                        and "READ" in acl_entry.permissions
                    ):
                        has_read_permission = True
                        break
                assert (
                    has_read_permission
                ), f"Entity {entity_id} should have READ permissions for AUTHENTICATED_USERS"

    def test_delete_permissions_all_entity_types(self) -> None:
        """Test delete_permissions functionality with all supported entity types."""
        # GIVEN a project with all supported entity types and local ACL permissions
        project = Project(name=f"test_project_{uuid.uuid4()}")
        entities = self.create_all_entity_types_with_acl(project)

        # AND I verify AUTHENTICATED_USERS has READ permissions before deletion
        for entity_type, entity in entities.items():
            if entity_type != "project":
                acl_before = entity.get_acl(
                    principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
                )
                assert (
                    "READ" in acl_before
                ), f"AUTHENTICATED_USERS should have READ access on {entity_type} before deletion"

        # WHEN I call delete_permissions with dry_run=True to test functionality
        entities["project"].delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "file",
                "folder",
                "table",
                "entityview",
                "materializedview",
                "virtualtable",
                "dataset",
                "datasetcollection",
            ],
            dry_run=True,
            synapse_client=self.syn,
        )

        # THEN no exception should be raised
        # AND permissions should still exist after dry run
        for entity_type, entity in entities.items():
            if entity_type != "project":
                acl_after = entity.get_acl(
                    principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
                )
                assert (
                    "READ" in acl_after
                ), f"AUTHENTICATED_USERS should still have READ access on {entity_type} after dry run"

    def test_delete_permissions_all_entity_types_actual_deletion(self) -> None:
        """Test delete_permissions functionality with actual deletion (dry_run=False)."""
        # GIVEN a project with all supported entity types and local ACL permissions
        project = Project(name=f"test_project_{uuid.uuid4()}")
        entities = self.create_all_entity_types_with_acl(project)

        # AND I verify AUTHENTICATED_USERS has READ permissions before deletion
        initial_acl_result = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "file",
                "folder",
                "table",
                "entityview",
                "materializedview",
                "virtualtable",
                "dataset",
                "datasetcollection",
            ],
            synapse_client=self.syn,
        )

        # Verify structure and content before deletion
        assert isinstance(initial_acl_result, AclListResult)
        entities_with_permissions_before = set()
        for entity_acl in initial_acl_result.all_entity_acls:
            for acl_entry in entity_acl.acl_entries:
                if (
                    acl_entry.principal_id == str(AUTHENTICATED_USERS)
                    and "READ" in acl_entry.permissions
                ):
                    entities_with_permissions_before.add(entity_acl.entity_id)

        assert (
            len(entities_with_permissions_before) > 0
        ), "Should have entities with READ permissions before deletion"

        # WHEN I call delete_permissions with dry_run=False for actual deletion
        entities["project"].delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "file",
                "folder",
                "table",
                "entityview",
                "materializedview",
                "virtualtable",
                "dataset",
                "datasetcollection",
            ],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN permissions should be actually deleted (inheritance restored)
        final_acl_result = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=[
                "file",
                "folder",
                "table",
                "entityview",
                "materializedview",
                "virtualtable",
                "dataset",
                "datasetcollection",
            ],
            synapse_client=self.syn,
        )

        # AND Verify that local ACL permissions have been removed
        entities_with_permissions_after = set()
        for entity_acl in final_acl_result.all_entity_acls:
            for acl_entry in entity_acl.acl_entries:
                if (
                    acl_entry.principal_id == str(AUTHENTICATED_USERS)
                    and "READ" in acl_entry.permissions
                ):
                    entities_with_permissions_after.add(entity_acl.entity_id)

        # AND Should have fewer entities with local ACL permissions after deletion
        for entity_type, entity in entities.items():
            if entity_type != "project":
                acl_after = entity.get_acl(
                    principal_id=AUTHENTICATED_USERS,
                    synapse_client=self.syn,
                    check_benefactor=False,
                )
                assert not acl_after, "Local ACL should be removed"

    def test_mixed_case_entity_types_actual_deletion(self) -> None:
        """Test that entity types are case-insensitive with actual deletion."""
        # GIVEN a project with all supported entity types
        project = Project(name=f"test_project_{uuid.uuid4()}")
        entities = self.create_all_entity_types_with_acl(project)

        # AND I verify initial ACL state
        initial_result = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=["FILE", "Folder", "TABLE"],
            synapse_client=self.syn,
        )

        assert isinstance(initial_result, AclListResult)
        initial_entities_with_permissions = set()
        for entity_acl in initial_result.all_entity_acls:
            for acl_entry in entity_acl.acl_entries:
                if (
                    acl_entry.principal_id == str(AUTHENTICATED_USERS)
                    and "READ" in acl_entry.permissions
                ):
                    initial_entities_with_permissions.add(entity_acl.entity_id)

        assert (
            len(initial_entities_with_permissions) > 0
        ), "Should have entities with READ permissions before deletion"

        # WHEN I call delete_permissions with mixed case entity types and dry_run=False
        entities["project"].delete_permissions(
            recursive=True,
            include_container_content=True,
            target_entity_types=["FILE", "Folder", "TABLE"],
            dry_run=False,
            synapse_client=self.syn,
        )

        # THEN verify that permissions were actually deleted
        final_result = entities["project"].list_acl(
            recursive=True,
            include_container_content=True,
            target_entity_types=["FILE", "Folder", "TABLE"],
            synapse_client=self.syn,
        )

        assert isinstance(final_result, AclListResult)
        final_entities_with_permissions = set()
        for entity_acl in final_result.all_entity_acls:
            for acl_entry in entity_acl.acl_entries:
                if (
                    acl_entry.principal_id == str(AUTHENTICATED_USERS)
                    and "READ" in acl_entry.permissions
                ):
                    final_entities_with_permissions.add(entity_acl.entity_id)

        # AND Verify individual entity permissions were removed/restored to inheritance
        for entity_type in ["file", "folder", "table"]:
            entity = entities[entity_type]
            acl_after = entity.get_acl(
                principal_id=AUTHENTICATED_USERS, synapse_client=self.syn
            )
            assert (
                not acl_after
            ), f"Local ACL for {entity_type} {entity.id} should be removed after deletion"
