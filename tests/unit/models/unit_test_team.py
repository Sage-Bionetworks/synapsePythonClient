from synapseclient.team import Team as Synapse_Team, TeamMember as Synapse_TeamMember
from synapseclient.models.team import Team, TeamMember
from synapseclient.models.user import UserGroupHeader
from synapseclient import Synapse

from unittest.mock import patch

import pytest


class TestTeamMember:
    def test_fill_from_dict(self) -> None:
        # GIVEN a blank TeamMember
        team_member = TeamMember()
        # WHEN I fill it with a dictionary
        team_member.fill_from_dict(
            synapse_team_member={"teamId": 1, "member": {"ownerId": 2}, "isAdmin": True}
        )
        # THEN I expect all fields to be set
        assert team_member.team_id == 1
        assert team_member.member.owner_id == 2
        assert team_member.is_admin is True


class TestTeam:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse):
        self.syn = syn
        self.MESSAGE = "test message"
        self.USER = "test_user"
        self.DESCRIPTION = "test description"
        self.NAME = "test_team"
        self.TIMESTAMP = "2020-01-01T00:00:00.000Z"
        self.invite_response = {
            "id": "1",
            "teamId": "2",
            "inviteeId": "3",
            "message": self.MESSAGE,
            "createdOn": "2000-01-01T00:00:00.000Z",
            "createdBy": "4",
        }

    def test_fill_from_dict(self) -> None:
        # GIVEN a blank Team
        team = Team()
        # WHEN I fill it with a dictionary
        team.fill_from_dict(
            synapse_team={
                "id": "1",
                "name": self.NAME,
                "description": self.DESCRIPTION,
                "icon": "test_file_handle_id",
                "canPublicJoin": True,
                "canRequestMembership": True,
                "etag": "11111111-1111-1111-1111-111111111111",
                "createdOn": self.TIMESTAMP,
                "modifiedOn": self.TIMESTAMP,
                "createdBy": self.USER,
                "modifiedBy": self.USER,
            }
        )
        # THEN I expect all fields to be set
        assert team.id == 1
        assert team.name == self.NAME
        assert team.description == self.DESCRIPTION
        assert team.icon == "test_file_handle_id"
        assert team.can_public_join is True
        assert team.can_request_membership is True
        assert team.etag == "11111111-1111-1111-1111-111111111111"
        assert team.created_on == self.TIMESTAMP
        assert team.modified_on == self.TIMESTAMP
        assert team.created_by == self.USER
        assert team.modified_by == self.USER

    @pytest.mark.asyncio
    async def test_create(self) -> None:
        with patch.object(
            self.syn,
            "create_team",
            return_value=Synapse_Team(
                id=1, name=self.NAME, description=self.DESCRIPTION
            ),
        ) as patch_create_team:
            # GIVEN a team object
            team = Team(name=self.NAME, description=self.DESCRIPTION)
            # WHEN I create the team
            team = await team.create()
            # THEN I expect the patched method to be called as expected
            patch_create_team.assert_called_once_with(
                name=self.NAME,
                description=self.DESCRIPTION,
                icon=None,
                canPublicJoin=False,
                canRequestMembership=True,
            )
            # AND I expect the original team to be returned
            assert team.id == 1
            assert team.name == self.NAME
            assert team.description == self.DESCRIPTION
            assert team.icon is None
            assert team.can_public_join is False
            assert team.can_request_membership is True

    @pytest.mark.asyncio
    async def test_delete(self) -> None:
        with patch.object(
            self.syn,
            "delete_team",
            return_value=Synapse_Team(id=1, name=self.NAME),
        ) as patch_delete_team:
            # GIVEN a team object
            team = Team(id=1, name=self.NAME)
            # WHEN I delete the team
            await team.delete()
            # THEN I expect the patched method to be called as expected
            patch_delete_team.assert_called_once_with(id=1)

    @pytest.mark.asyncio
    async def test_get_with_id(self) -> None:
        with patch.object(
            self.syn,
            "getTeam",
            return_value=Synapse_Team(
                id=1, name=self.NAME, description=self.DESCRIPTION
            ),
        ) as patch_from_id:
            # GIVEN a team object with an id
            team = Team(id=1)
            # WHEN I retrieve a team using its id
            team = await team.get()
            # THEN I expect the patched method to be called as expected
            patch_from_id.assert_called_once_with(id=1)
            # AND I expect the intended team to be returned
            assert team.id == 1
            assert team.name == self.NAME
            assert team.description == self.DESCRIPTION

    @pytest.mark.asyncio
    async def test_get_with_name(self) -> None:
        with patch.object(
            self.syn,
            "getTeam",
            return_value=Synapse_Team(
                id=1, name=self.NAME, description=self.DESCRIPTION
            ),
        ) as patch_from_name:
            # GIVEN a team object with a name
            team = Team(name=self.NAME)
            # WHEN I retrieve a team using its name
            team = await team.get()
            # THEN I expect the patched method to be called as expected
            patch_from_name.assert_called_once_with(id=self.NAME)
            # AND I expect the intended team to be returned
            assert team.id == 1
            assert team.name == self.NAME
            assert team.description == self.DESCRIPTION

    @pytest.mark.asyncio
    async def test_get_with_no_id_or_name(self) -> None:
        # GIVEN a team object with no id or name
        team = Team()
        # WHEN I retrieve a team
        with pytest.raises(ValueError, match="Team must have either an id or a name"):
            # THEN I expect an error to be raised
            await team.get()

    @pytest.mark.asyncio
    async def test_from_id(self) -> None:
        with patch.object(
            Team,
            "get",
            return_value=Team(id=1, name=self.NAME, description=self.DESCRIPTION),
        ) as patch_get:
            # WHEN I retrieve a team using its id
            team = await Team.from_id(id=1)
            # THEN I expect the patched method to be called as expected
            patch_get.assert_called_once_with(synapse_client=None)
            # AND I expect the intended team to be returned
            assert team.id == 1
            assert team.name == self.NAME
            assert team.description == self.DESCRIPTION

    @pytest.mark.asyncio
    async def test_from_name(self) -> None:
        with patch.object(
            Team,
            "get",
            return_value=Team(id=1, name=self.NAME, description=self.DESCRIPTION),
        ) as patch_get:
            # WHEN I retrieve a team using its name
            team = await Team.from_name(name=self.NAME)
            # THEN I expect the patched method to be called as expected
            patch_get.assert_called_once_with(synapse_client=None)
            # AND I expect the intended team to be returned
            assert team.id == 1
            assert team.name == self.NAME
            assert team.description == self.DESCRIPTION

    @pytest.mark.asyncio
    async def test_members(self) -> None:
        with patch.object(
            self.syn,
            "getTeamMembers",
            return_value=[Synapse_TeamMember(teamId=1, member={})],
        ) as patch_get_team_members:
            # GIVEN a team object
            team = Team(id=1)
            # WHEN I get the team members
            team_members = await team.members()
            # THEN I expect the patched method to be called as expected
            patch_get_team_members.assert_called_once_with(team=team)
            # AND I expect the expected team members to be returned
            assert len(team_members) == 1
            assert team_members[0].team_id == 1
            assert isinstance(team_members[0].member, UserGroupHeader)

    @pytest.mark.asyncio
    async def test_invite(self) -> None:
        with patch.object(
            self.syn,
            "invite_to_team",
            return_value=self.invite_response,
        ) as patch_invite_team_member:
            # GIVEN a team object
            team = Team(id=1)
            # WHEN I invite a user to the team
            invite = await team.invite(
                user=self.USER,
                message=self.MESSAGE,
            )
            # THEN I expect the patched method to be called as expected
            patch_invite_team_member.assert_called_once_with(
                team=team,
                user=self.USER,
                message=self.MESSAGE,
                force=True,
            )
            # AND I expect the expected invite to be returned
            assert invite == self.invite_response

    @pytest.mark.asyncio
    async def test_open_invitations(self) -> None:
        with patch.object(
            self.syn,
            "get_team_open_invitations",
            return_value=[self.invite_response],
        ) as patch_get_open_team_invitations:
            # GIVEN a team object
            team = Team(id=1)
            # WHEN I get the open team invitations
            open_team_invitations = await team.open_invitations()
            # THEN I expect the patched method to be called as expected
            patch_get_open_team_invitations.assert_called_once_with(team=team)
            # AND I expect the expected invitations to be returned
            assert len(open_team_invitations) == 1
            assert open_team_invitations[0] == self.invite_response
