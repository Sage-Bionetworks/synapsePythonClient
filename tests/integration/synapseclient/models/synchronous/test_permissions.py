"""Integration tests for ACL on several model."""

import uuid
from typing import Callable

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
DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT = (
    "A fake team for testing permissions assigned to a single project"
)
DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_1 = (
    "A fake team for testing permissions assigned to a single project - 1"
)
DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_2 = (
    "A fake team for testing permissions assigned to a single project - 2"
)


class TestAclOnProject:
    """Testing ACL for the Project model."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_acl_default(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_default_permissions = Project(
            name=str(uuid.uuid4()) + "test_get_acl_default_permissions"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_default_permissions.id)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_default_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the default admin permissions
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

    async def test_get_acl_read_only_permissions_on_entity(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_read_only_permissions = Project(
            name=str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user on the entity are set to READ only
        project_with_read_only_permissions.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "CHANGE_SETTINGS",
                "CHANGE_PERMISSIONS",
                "UPDATE",
                "DELETE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_read_only_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see read only permissions
        expected_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_team_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_single_team = Project(
            name=str(uuid.uuid4())
            + "test_get_acl_through_team_assigned_to_user_and_project"
        ).store(synapse_client=self.syn)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(project_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        project_with_permissions_through_single_team.set_permissions(
            principal_id=team.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        project_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_permissions_through_single_team.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_multiple_teams_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_multiple_teams = Project(
            name=str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        ).store(synapse_client=self.syn)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_1 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_1,
        ).create(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_2 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_2,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(project_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity

        permissions = project_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of both teams
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

    async def test_get_acl_for_project_with_public_and_registered_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_for_public_and_authenticated_users = Project(
            name=str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(
            project_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for PUBLIC are set to 'READ'
        project_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"], synapse_client=self.syn
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        project_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        project_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            project_with_permissions_for_public_and_authenticated_users.get_acl(
                synapse_client=self.syn
            )
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = (
            project_with_permissions_for_public_and_authenticated_users.get_acl(
                principal_id=p1.id, synapse_client=self.syn
            )
        )

        # THEN I expect to see the permissions of the user, and the authenticated user, and the public user
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


class TestAclOnFolder:
    """Testing ACL for the Folder model."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_acl_default(self, project_model: Project) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_default_permissions = Folder(
            name=str(uuid.uuid4()) + "test_get_acl_default_permissions",
        ).store(parent=project_model, synapse_client=self.syn)
        self.schedule_for_cleanup(folder_with_default_permissions.id)

        # AND the user that created the folder
        p1 = UserProfile().get(synapse_client=self.syn)

        # WHEN I get the permissions for the user on the entity
        permissions = folder_with_default_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the default admin permissions
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

    async def test_get_acl_minimal_permissions_on_entity(
        self, project_model: Project
    ) -> None:
        # GIVEN a folder created with default permissions of administrator
        project_with_minimal_permissions = Folder(
            name=str(uuid.uuid4()) + "test_get_acl_minimal_permissions_on_project"
        ).store(parent=project_model, synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_minimal_permissions.id)

        # AND the user that created the folder
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user on the entity are set
        project_with_minimal_permissions.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "CHANGE_SETTINGS",
                "CHANGE_PERMISSIONS",
                "UPDATE",
                "DELETE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_minimal_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see minimal permissions
        expected_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_minimal_permissions_on_sub_folder(
        self, project_model: Project
    ) -> None:
        # GIVEN a parent folder with default permissions
        parent_folder = Folder(
            name=str(uuid.uuid4()) + "test_get_acl_read_permissions_on_sub_folder"
        ).store(parent=project_model, synapse_client=self.syn)

        # AND a folder created with default permissions of administrator
        folder_with_minimal_permissions = Folder(
            name=str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        ).store(parent=parent_folder, synapse_client=self.syn)
        self.schedule_for_cleanup(folder_with_minimal_permissions.id)

        # AND the user that created the folder
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user on the entity are set
        folder_with_minimal_permissions.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "CHANGE_SETTINGS",
                "CHANGE_PERMISSIONS",
                "UPDATE",
                "DELETE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for the user on the entity
        permissions = folder_with_minimal_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # AND I get the permissions for the user on the parent entity
        permissions_on_parent = parent_folder.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see minimal permissions on the sub-folder
        expected_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        assert set(expected_permissions) == set(permissions)

        # AND I expect to see the default admin permissions on the parent-folder
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
        assert set(expected_permissions) == set(permissions_on_parent)

    async def test_get_acl_through_team_assigned_to_user(
        self, project_model: Project
    ) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_permissions_through_single_team = Folder(
            name=str(uuid.uuid4())
            + "test_get_acl_through_team_assigned_to_user_and_project"
        ).store(parent=project_model, synapse_client=self.syn)

        # AND the user that created the folder
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(folder_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        folder_with_permissions_through_single_team.set_permissions(
            principal_id=team.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        folder_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = folder_with_permissions_through_single_team.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_multiple_teams_assigned_to_user(
        self, project_model: Project
    ) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_permissions_through_multiple_teams = Folder(
            name=str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        ).store(parent=project_model, synapse_client=self.syn)

        # AND the user that created the folder
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_1 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_1,
        ).create(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_2 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_2,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(folder_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        folder_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        folder_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        folder_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity

        permissions = folder_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of both teams
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

    async def test_get_acl_for_project_with_public_and_registered_user(
        self, project_model: Project
    ) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_permissions_for_public_and_authenticated_users = Folder(
            name=str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        ).store(parent=project_model, synapse_client=self.syn)
        self.schedule_for_cleanup(
            folder_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the folder
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for PUBLIC are set to 'READ'
        folder_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"], synapse_client=self.syn
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        folder_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        folder_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            folder_with_permissions_for_public_and_authenticated_users.get_acl(
                synapse_client=self.syn
            )
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = (
            folder_with_permissions_for_public_and_authenticated_users.get_acl(
                principal_id=p1.id, synapse_client=self.syn
            )
        )

        # THEN I expect to see the permissions of the user, and the authenticated user, and the public user
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


class TestAclOnFile:
    """Testing ACL for the File model."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> None:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(path=filename)

    async def test_get_acl_default(self, project_model: Project, file: File) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = str(uuid.uuid4()) + "test_get_acl_default_permissions"
        file_with_default_permissions = file.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(file_with_default_permissions.id)

        # AND the user that created the file
        p1 = UserProfile().get(synapse_client=self.syn)

        # WHEN I get the permissions for the user on the entity
        permissions = file_with_default_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the default admin permissions
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

    async def test_get_acl_minimal_permissions_on_entity(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        project_with_minimal_permissions = file.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(project_with_minimal_permissions.id)

        # AND the user that created the file
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user on the entity are set
        project_with_minimal_permissions.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "CHANGE_SETTINGS",
                "CHANGE_PERMISSIONS",
                "UPDATE",
                "DELETE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_minimal_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see minimal permissions
        expected_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_team_assigned_to_user(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = (
            str(uuid.uuid4()) + "test_get_acl_through_team_assigned_to_user_and_project"
        )
        file_with_permissions_through_single_team = file.store(
            parent=project_model, synapse_client=self.syn
        )

        # AND the user that created the file
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(file_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        file_with_permissions_through_single_team.set_permissions(
            principal_id=team.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        file_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = file_with_permissions_through_single_team.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_multiple_teams_assigned_to_user(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = (
            str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        )
        file_with_permissions_through_multiple_teams = file.store(
            parent=project_model, synapse_client=self.syn
        )

        # AND the user that created the file
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_1 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_1,
        ).create(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_2 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_2,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(file_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        file_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        file_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        file_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity

        permissions = file_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of both teams
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

    async def test_get_acl_for_project_with_public_and_registered_user(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        file_with_permissions_for_public_and_authenticated_users = file.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(
            file_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the file
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for PUBLIC are set to 'READ'
        file_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"], synapse_client=self.syn
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        file_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        file_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = file_with_permissions_for_public_and_authenticated_users.get_acl(
            synapse_client=self.syn
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = file_with_permissions_for_public_and_authenticated_users.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the user, and the authenticated user, and the public user
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


class TestAclOnTable:
    """Testing ACL for the Table model."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def table(self, project_model: Project) -> Table:
        # Creating columns for my table ======================================================
        columns = [
            Column(id=None, name="my_string_column", column_type=ColumnType.STRING),
        ]

        # Creating a table ===============================================================
        table = Table(
            name="my_test_table" + str(uuid.uuid4()),
            columns=columns,
            parent_id=project_model.id,
        )

        return table

    async def test_get_acl_default(self, table: Table) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = str(uuid.uuid4()) + "test_get_acl_default_permissions"
        table_with_default_permissions = table.store_schema(synapse_client=self.syn)
        self.schedule_for_cleanup(table_with_default_permissions.id)

        # AND the user that created the table
        p1 = UserProfile().get(synapse_client=self.syn)

        # WHEN I get the permissions for the user on the entity
        permissions = table_with_default_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the default admin permissions
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

    async def test_get_acl_minimal_permissions_on_entity(self, table: Table) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        project_with_minimal_permissions = table.store_schema(synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_minimal_permissions.id)

        # AND the user that created the table
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user on the entity are set
        project_with_minimal_permissions.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "CHANGE_SETTINGS",
                "CHANGE_PERMISSIONS",
                "UPDATE",
                "DELETE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_minimal_permissions.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see minimal permissions
        expected_permissions = [
            "READ",
            "CHANGE_SETTINGS",
            "CHANGE_PERMISSIONS",
            "UPDATE",
            "DELETE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_team_assigned_to_user(self, table: Table) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = (
            str(uuid.uuid4()) + "test_get_acl_through_team_assigned_to_user_and_project"
        )
        table_with_permissions_through_single_team = table.store_schema(
            synapse_client=self.syn
        )

        # AND the user that created the table
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(table_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        table_with_permissions_through_single_team.set_permissions(
            principal_id=team.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        table_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = table_with_permissions_through_single_team.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_multiple_teams_assigned_to_user(
        self, table: Table
    ) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = (
            str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        )
        table_with_permissions_through_multiple_teams = table.store_schema(
            synapse_client=self.syn
        )

        # AND the user that created the table
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_1 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_1,
        ).create(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_2 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_2,
        ).create(synapse_client=self.syn)

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(table_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        table_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        table_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        table_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity

        permissions = table_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of both teams
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

    async def test_get_acl_for_project_with_public_and_registered_user(
        self, table: Table
    ) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        table_with_permissions_for_public_and_authenticated_users = table.store_schema(
            synapse_client=self.syn
        )

        self.schedule_for_cleanup(
            table_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the table
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for PUBLIC are set to 'READ'
        table_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"], synapse_client=self.syn
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        table_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        table_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            table_with_permissions_for_public_and_authenticated_users.get_acl()
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = table_with_permissions_for_public_and_authenticated_users.get_acl(
            principal_id=p1.id, synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the user, and the authenticated user, and the public user
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


class TestPermissionsOnEntityForCaller:
    """
    Test the permissions a caller has for an entity
    """

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_permissions_default(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_default_permissions = Project(
            name=str(uuid.uuid4()) + "test_get_permissions_default_permissions"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_default_permissions.id)

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_default_permissions.get_permissions(
            synapse_client=self.syn
        )

        # THEN I expect to see the default admin permissions
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

    async def test_get_permissions_read_only_permissions_on_entity(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_read_only_permissions = Project(
            name=str(uuid.uuid4()) + "test_get_permissions_read_permissions_on_project"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for the user on the entity are set to READ only
        project_with_read_only_permissions.set_permissions(
            principal_id=p1.id, access_type=["READ"], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_read_only_permissions.get_permissions(
            synapse_client=self.syn
        )

        # THEN I expect to see read only permissions. CHANGE_SETTINGS is bound to ownerId.
        # Since the entity is created by the Caller, the CHANGE_SETTINGS will always be True.
        expected_permissions = ["READ", "CHANGE_SETTINGS"]

        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_through_team_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_single_team = Project(
            name=str(uuid.uuid4())
            + "test_get_permissions_through_team_assigned_to_user_and_project"
        ).store(synapse_client=self.syn)

        # AND the user that created the project
        p1: UserProfile = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT,
        ).create(synapse_client=self.syn)

        # Note: When running this schedule for cleanup order can matter when
        # there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(project_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        project_with_permissions_through_single_team.set_permissions(
            principal_id=team.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        project_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_permissions_through_single_team.get_permissions(
            synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_through_multiple_teams_assigned_to_user(
        self,
    ) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_multiple_teams = Project(
            name=str(uuid.uuid4())
            + "test_get_permissions_through_two_teams_assigned_to_user_and_project"
        ).store(synapse_client=self.syn)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_1 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_1,
        ).create(synapse_client=self.syn)

        name = TEAM_PREFIX + str(uuid.uuid4())
        team_2 = Team(
            name=name,
            description=DESCRIPTION_FAKE_TEAM_SINGLE_PROJECT_2,
        ).create(synapse_client=self.syn)

        # Note: When running this schedule for cleanup order can matter when
        # there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(project_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity are set to NONE
        project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[], synapse_client=self.syn
        )

        # WHEN I get the permissions for the user on the entity
        permissions = project_with_permissions_through_multiple_teams.get_permissions(
            synapse_client=self.syn
        )

        # THEN I expect to see the permissions of both teams
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

    async def test_get_permissions_for_project_with_registered_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_for_authenticated_users = Project(
            name=str(uuid.uuid4())
            + "test_get_permissions_for_project_with_registered_user"
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(project_with_permissions_for_authenticated_users.id)

        # AND the user that created the project
        p1 = UserProfile().get(synapse_client=self.syn)

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        project_with_permissions_for_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS,
            access_type=["READ", "DOWNLOAD"],
            synapse_client=self.syn,
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        project_with_permissions_for_authenticated_users.set_permissions(
            principal_id=p1.id,
            access_type=[
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
            synapse_client=self.syn,
        )

        # and WHEN I get the permissions for the user on the entity
        permissions = project_with_permissions_for_authenticated_users.get_permissions(
            synapse_client=self.syn
        )

        # THEN I expect to see the permissions of the user, and the authenticated user
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
