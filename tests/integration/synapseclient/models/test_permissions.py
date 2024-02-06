import uuid

import pytest

from synapseclient import Synapse
from synapseclient import Team
from synapseclient.core import utils

from synapseclient.models import (
    Project,
    UserProfile,
    Folder,
    File,
    ColumnType,
    Column,
    Table,
)
from typing import Callable

PUBLIC = 273949  # PrincipalId of public "user"
AUTHENTICATED_USERS = 273948


class TestAclOnProject:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.mark.asyncio
    async def test_get_acl_default(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_default_permissions = await Project(
            name=str(uuid.uuid4()) + "test_get_acl_default_permissions"
        ).store()
        self.schedule_for_cleanup(project_with_default_permissions.id)

        # AND the user that created the project
        p1 = await UserProfile().get()

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_default_permissions.get_acl(principal_id=p1.id)

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

    @pytest.mark.asyncio
    async def test_get_acl_read_only_permissions_on_entity(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_read_only_permissions = await Project(
            name=str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        ).store()
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the project
        p1 = await UserProfile().get()

        # AND the permissions for the user on the entity are set to READ only
        await project_with_read_only_permissions.set_permissions(
            principal_id=p1.id, access_type=["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_read_only_permissions.get_acl(
            principal_id=p1.id
        )

        # THEN I expect to see read only permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

    @pytest.mark.asyncio
    async def test_get_acl_through_team_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_single_team = await Project(
            name=str(uuid.uuid4())
            + "test_get_acl_through_team_assigned_to_user_and_project"
        ).store()

        # AND the user that created the project
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(project_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        await project_with_permissions_through_single_team.set_permissions(
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
        )

        # AND the permissions for the user on the entity are set to NONE
        await project_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_permissions_through_single_team.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_through_multiple_teams_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_multiple_teams = await Project(
            name=str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        ).store()

        # AND the user that created the project
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # TODO: Replace with the Team OOP model
        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(project_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        await project_with_permissions_through_multiple_teams.set_permissions(
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
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        await project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity are set to NONE
        await project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity

        permissions = await project_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_for_project_with_public_and_registered_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_for_public_and_authenticated_users = await Project(
            name=str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        ).store()
        self.schedule_for_cleanup(
            project_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the project
        p1 = await UserProfile().get()

        # AND the permissions for PUBLIC are set to 'READ'
        await project_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"]
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        await project_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        await project_with_permissions_for_public_and_authenticated_users.set_permissions(
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
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            await project_with_permissions_for_public_and_authenticated_users.get_acl()
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = (
            await project_with_permissions_for_public_and_authenticated_users.get_acl(
                principal_id=p1.id
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
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.mark.asyncio
    async def test_get_acl_default(self, project_model) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_default_permissions = await Folder(
            name=str(uuid.uuid4()) + "test_get_acl_default_permissions",
        ).store(parent=project_model)
        self.schedule_for_cleanup(folder_with_default_permissions.id)

        # AND the user that created the folder
        p1 = await UserProfile().get()

        # WHEN I get the permissions for the user on the entity
        permissions = await folder_with_default_permissions.get_acl(principal_id=p1.id)

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

    @pytest.mark.asyncio
    async def test_get_acl_read_only_permissions_on_entity(self, project_model) -> None:
        # GIVEN a folder created with default permissions of administrator
        project_with_read_only_permissions = await Folder(
            name=str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        ).store(parent=project_model)
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the folder
        p1 = await UserProfile().get()

        # AND the permissions for the user on the entity are set to READ only
        await project_with_read_only_permissions.set_permissions(
            principal_id=p1.id, access_type=["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_read_only_permissions.get_acl(
            principal_id=p1.id
        )

        # THEN I expect to see read only permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

    @pytest.mark.asyncio
    async def test_get_acl_through_team_assigned_to_user(self, project_model) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_permissions_through_single_team = await Folder(
            name=str(uuid.uuid4())
            + "test_get_acl_through_team_assigned_to_user_and_project"
        ).store(parent=project_model)

        # AND the user that created the folder
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(folder_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        await folder_with_permissions_through_single_team.set_permissions(
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
        )

        # AND the permissions for the user on the entity are set to NONE
        await folder_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await folder_with_permissions_through_single_team.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_through_multiple_teams_assigned_to_user(
        self, project_model
    ) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_permissions_through_multiple_teams = await Folder(
            name=str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        ).store(parent=project_model)

        # AND the user that created the folder
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # TODO: Replace with the Team OOP model
        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(folder_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        await folder_with_permissions_through_multiple_teams.set_permissions(
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
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        await folder_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity are set to NONE
        await folder_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity

        permissions = await folder_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_for_project_with_public_and_registered_user(
        self, project_model
    ) -> None:
        # GIVEN a folder created with default permissions of administrator
        folder_with_permissions_for_public_and_authenticated_users = await Folder(
            name=str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        ).store(parent=project_model)
        self.schedule_for_cleanup(
            folder_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the folder
        p1 = await UserProfile().get()

        # AND the permissions for PUBLIC are set to 'READ'
        await folder_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"]
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        await folder_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        await folder_with_permissions_for_public_and_authenticated_users.set_permissions(
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
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            await folder_with_permissions_for_public_and_authenticated_users.get_acl()
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = (
            await folder_with_permissions_for_public_and_authenticated_users.get_acl(
                principal_id=p1.id
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
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> None:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(path=filename)

    @pytest.mark.asyncio
    async def test_get_acl_default(self, project_model: Project, file: File) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = str(uuid.uuid4()) + "test_get_acl_default_permissions"
        file_with_default_permissions = await file.store(parent=project_model)
        self.schedule_for_cleanup(file_with_default_permissions.id)

        # AND the user that created the file
        p1 = await UserProfile().get()

        # WHEN I get the permissions for the user on the entity
        permissions = await file_with_default_permissions.get_acl(principal_id=p1.id)

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

    @pytest.mark.asyncio
    async def test_get_acl_read_only_permissions_on_entity(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        project_with_read_only_permissions = await file.store(parent=project_model)
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the file
        p1 = await UserProfile().get()

        # AND the permissions for the user on the entity are set to READ only
        await project_with_read_only_permissions.set_permissions(
            principal_id=p1.id, access_type=["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_read_only_permissions.get_acl(
            principal_id=p1.id
        )

        # THEN I expect to see read only permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

    @pytest.mark.asyncio
    async def test_get_acl_through_team_assigned_to_user(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = (
            str(uuid.uuid4()) + "test_get_acl_through_team_assigned_to_user_and_project"
        )
        file_with_permissions_through_single_team = await file.store(
            parent=project_model
        )

        # AND the user that created the file
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(file_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        await file_with_permissions_through_single_team.set_permissions(
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
        )

        # AND the permissions for the user on the entity are set to NONE
        await file_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await file_with_permissions_through_single_team.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_through_multiple_teams_assigned_to_user(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = (
            str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        )
        file_with_permissions_through_multiple_teams = await file.store(
            parent=project_model
        )

        # AND the user that created the file
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # TODO: Replace with the Team OOP model
        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(file_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        await file_with_permissions_through_multiple_teams.set_permissions(
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
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        await file_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity are set to NONE
        await file_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity

        permissions = await file_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_for_project_with_public_and_registered_user(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file created with default permissions of administrator
        file.name = str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        file_with_permissions_for_public_and_authenticated_users = await file.store(
            parent=project_model
        )
        self.schedule_for_cleanup(
            file_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the file
        p1 = await UserProfile().get()

        # AND the permissions for PUBLIC are set to 'READ'
        await file_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"]
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        await file_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        await file_with_permissions_for_public_and_authenticated_users.set_permissions(
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
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            await file_with_permissions_for_public_and_authenticated_users.get_acl()
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = (
            await file_with_permissions_for_public_and_authenticated_users.get_acl(
                principal_id=p1.id
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


class TestAclOnTable:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
    async def test_get_acl_default(self, table: Table) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = str(uuid.uuid4()) + "test_get_acl_default_permissions"
        table_with_default_permissions = await table.store_schema()
        self.schedule_for_cleanup(table_with_default_permissions.id)

        # AND the user that created the table
        p1 = await UserProfile().get()

        # WHEN I get the permissions for the user on the entity
        permissions = await table_with_default_permissions.get_acl(principal_id=p1.id)

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

    @pytest.mark.asyncio
    async def test_get_acl_read_only_permissions_on_entity(self, table: Table) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project"
        project_with_read_only_permissions = await table.store_schema()
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the table
        p1 = await UserProfile().get()

        # AND the permissions for the user on the entity are set to READ only
        await project_with_read_only_permissions.set_permissions(
            principal_id=p1.id, access_type=["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_read_only_permissions.get_acl(
            principal_id=p1.id
        )

        # THEN I expect to see read only permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

    @pytest.mark.asyncio
    async def test_get_acl_through_team_assigned_to_user(self, table: Table) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = (
            str(uuid.uuid4()) + "test_get_acl_through_team_assigned_to_user_and_project"
        )
        table_with_permissions_through_single_team = await table.store_schema()

        # AND the user that created the table
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(table_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        await table_with_permissions_through_single_team.set_permissions(
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
        )

        # AND the permissions for the user on the entity are set to NONE
        await table_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await table_with_permissions_through_single_team.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_through_multiple_teams_assigned_to_user(
        self, table: Table
    ) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = (
            str(uuid.uuid4())
            + "test_get_acl_through_two_teams_assigned_to_user_and_project"
        )
        table_with_permissions_through_multiple_teams = await table.store_schema()

        # AND the user that created the table
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model when it is available
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # TODO: Replace with the Team OOP model
        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(table_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        await table_with_permissions_through_multiple_teams.set_permissions(
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
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        await table_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity are set to NONE
        await table_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity

        permissions = await table_with_permissions_through_multiple_teams.get_acl(
            principal_id=p1.id
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

    @pytest.mark.asyncio
    async def test_get_acl_for_project_with_public_and_registered_user(
        self, table: Table
    ) -> None:
        # GIVEN a table created with default permissions of administrator
        table.name = str(uuid.uuid4()) + "test_get_acl_for_project_with_registered_user"
        table_with_permissions_for_public_and_authenticated_users = (
            await table.store_schema()
        )

        self.schedule_for_cleanup(
            table_with_permissions_for_public_and_authenticated_users.id
        )

        # AND the user that created the table
        p1 = await UserProfile().get()

        # AND the permissions for PUBLIC are set to 'READ'
        await table_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=PUBLIC, access_type=["READ"]
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        await table_with_permissions_for_public_and_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        await table_with_permissions_for_public_and_authenticated_users.set_permissions(
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
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = (
            await table_with_permissions_for_public_and_authenticated_users.get_acl()
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = (
            await table_with_permissions_for_public_and_authenticated_users.get_acl(
                principal_id=p1.id
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


class TestPermissionsOnEntityForCaller:
    """
    Test the permissions a caller has for an entity
    """

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.mark.asyncio
    async def test_get_permissions_default(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_default_permissions = await Project(
            name=str(uuid.uuid4()) + "test_get_permissions_default_permissions"
        ).store()
        self.schedule_for_cleanup(project_with_default_permissions.id)

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_default_permissions.get_permissions()

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

    @pytest.mark.asyncio
    async def test_get_permissions_read_only_permissions_on_entity(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_read_only_permissions = await Project(
            name=str(uuid.uuid4()) + "test_get_permissions_read_permissions_on_project"
        ).store()
        self.schedule_for_cleanup(project_with_read_only_permissions.id)

        # AND the user that created the project
        p1 = await UserProfile().get()

        # AND the permissions for the user on the entity are set to READ only
        await project_with_read_only_permissions.set_permissions(
            principal_id=p1.id, access_type=["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = await project_with_read_only_permissions.get_permissions()

        # THEN I expect to see read only permissions. CHANGE_SETTINGS is bound to ownerId.
        # Since the entity is created by the Caller, the CHANGE_SETTINGS will always be True.
        expected_permissions = ["READ", "CHANGE_SETTINGS"]

        assert set(expected_permissions) == set(permissions.access_types)

    @pytest.mark.asyncio
    async def test_get_permissions_through_team_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_single_team = await Project(
            name=str(uuid.uuid4())
            + "test_get_permissions_through_team_assigned_to_user_and_project"
        ).store()

        # AND the user that created the project
        p1: UserProfile = await UserProfile().get()

        # TODO: Replace with the Team OOP model
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(project_with_permissions_through_single_team.id)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        await project_with_permissions_through_single_team.set_permissions(
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
        )

        # AND the permissions for the user on the entity are set to NONE
        await project_with_permissions_through_single_team.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = (
            await project_with_permissions_through_single_team.get_permissions()
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

    @pytest.mark.asyncio
    async def test_get_permissions_through_multiple_teams_assigned_to_user(
        self,
    ) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_multiple_teams = await Project(
            name=str(uuid.uuid4())
            + "test_get_permissions_through_two_teams_assigned_to_user_and_project"
        ).store()

        # AND the user that created the project
        p1 = await UserProfile().get()

        # TODO: Replace with the Team OOP model
        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # TODO: Replace with the Team OOP model
        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(project_with_permissions_through_multiple_teams.id)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        await project_with_permissions_through_multiple_teams.set_permissions(
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
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        await project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=team_2.id, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity are set to NONE
        await project_with_permissions_through_multiple_teams.set_permissions(
            principal_id=p1.id, access_type=[]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = (
            await project_with_permissions_through_multiple_teams.get_permissions()
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

    @pytest.mark.asyncio
    async def test_get_permissions_for_project_with_registered_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_for_authenticated_users = await Project(
            name=str(uuid.uuid4())
            + "test_get_permissions_for_project_with_registered_user"
        ).store()
        self.schedule_for_cleanup(project_with_permissions_for_authenticated_users.id)

        # AND the user that created the project
        p1 = await UserProfile().get()

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        await project_with_permissions_for_authenticated_users.set_permissions(
            principal_id=AUTHENTICATED_USERS, access_type=["READ", "DOWNLOAD"]
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        await project_with_permissions_for_authenticated_users.set_permissions(
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
        )

        # and WHEN I get the permissions for the user on the entity
        permissions = (
            await project_with_permissions_for_authenticated_users.get_permissions()
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
