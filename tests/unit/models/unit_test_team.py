from synapseclient.team import Team as Synapse_Team, TeamMember as Synapse_TeamMember
from synapseclient.models.team import Team, TeamMember
from synapseclient import Synapse

from unittest.mock import patch

import pytest


class TestTeamMember:
    def test_fill_from_dict(self):
        team_member = TeamMember()
        team_member.fill_from_dict(
            synapse_team_member={"teamId": 1, "member": {"ownerId": 2}, "isAdmin": True}
        )
        assert team_member.team_id == 1
        assert team_member.member.owner_id == 2
        assert team_member.is_admin is True


class TestTeam:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def test_fill_from_dict(self):
        team = Team()
        team.fill_from_dict(
            synapse_team={
                "id": "1",
                "name": "test_team",
                "description": "test description",
                "icon": "test_file_handle_id",
                "canPublicJoin": True,
                "canRequestMembership": True,
                "etag": "11111111-1111-1111-1111-111111111111",
                "createdOn": "2020-01-01T00:00:00.000Z",
                "modifiedOn": "2020-01-01T00:00:00.000Z",
                "createdBy": "test_user",
                "modifiedBy": "test_user",
            }
        )
        assert team.id == 1
        assert team.name == "test_team"
        assert team.description == "test description"
        assert team.icon == "test_file_handle_id"
        assert team.can_public_join is True
        assert team.can_request_membership is True
        assert team.etag == "11111111-1111-1111-1111-111111111111"
        assert team.created_on == "2020-01-01T00:00:00.000Z"
        assert team.modified_on == "2020-01-01T00:00:00.000Z"
        assert team.created_by == "test_user"
        assert team.modified_by == "test_user"

    @pytest.mark.asyncio
    async def test_create(self):
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "create_team",
            return_value=Synapse_Team(
                id=1, name="test_team", description="test description"
            ),
        ) as patch_create_team:
            team = Team(name="test_team", description="test description")
            team = await team.create()
            patch_create_team.assert_called_once_with(
                name="test_team",
                description="test description",
                icon=None,
                opentelemetry_context={},
            )
            assert team.id == 1
            assert team.name == "test_team"

    @pytest.mark.asyncio
    async def test_delete(self):
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "delete_team",
            return_value=Synapse_Team(id=1, name="test_team"),
        ) as patch_delete_team:
            team = Team(id=1, name="test_team")
            await team.delete()
            patch_delete_team.assert_called_once_with(id=1, opentelemetry_context={})

    @pytest.mark.asyncio
    async def test_from_id(self):
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "getTeam",
            return_value=Synapse_Team(
                id=1, name="test_team", description="test description"
            ),
        ) as patch_get_team:
            team = await Team().from_id(id=1)
            patch_get_team.assert_called_once_with(id=1, opentelemetry_context={})
            assert team.id == 1
            assert team.name == "test_team"

    @pytest.mark.asyncio
    async def test_from_name(self):
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "getTeam",
            return_value=Synapse_Team(
                id=1, name="test_team", description="test description"
            ),
        ) as patch_get_team:
            team = await Team().from_name(name="test_team")
            patch_get_team.assert_called_once_with(
                id="test_team", opentelemetry_context={}
            )
            assert team.id == 1
            assert team.name == "test_team"

    @pytest.mark.asyncio
    async def test_members(self):
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "getTeamMembers",
            return_value=[Synapse_TeamMember(teamId=1, member={"id": 2})],
        ) as patch_get_team_members:
            team = Team(id=1)
            team_members = await team.members()
            patch_get_team_members.assert_called_once_with(
                team=team, opentelemetry_context={}
            )
            assert len(team_members) == 1
            assert team_members[0].team_id == 1

    @pytest.mark.asyncio
    async def test_invite(self):
        response_body = {
            "id": "1",
            "teamId": "2",
            "inviteeId": "3",
            "message": "test message",
            "createdOn": "2000-01-01T00:00:00.000Z",
            "createdBy": "4",
        }
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "invite_to_team",
            return_value=response_body,
        ) as patch_invite_team_member:
            team = Team(id=1)
            invite = await team.invite(
                user="test_user",
                message="test message",
            )
            patch_invite_team_member.assert_called_once_with(
                team=team,
                user="test_user",
                message="test message",
                force=True,
                opentelemetry_context={},
            )
            assert invite == response_body

    @pytest.mark.asyncio
    async def test_open_invitations(self):
        response_body = {
            "id": "1",
            "teamId": "2",
            "inviteeId": "3",
            "message": "test message",
            "createdOn": "2000-01-01T00:00:00.000Z",
            "createdBy": "4",
        }
        Synapse.set_client(self.syn)
        with patch.object(
            self.syn,
            "get_team_open_invitations",
            return_value=[response_body],
        ) as patch_get_open_team_invitations:
            team = Team(id=1)
            open_team_invitations = await team.open_invitations()
            patch_get_open_team_invitations.assert_called_once_with(
                team=team, opentelemetry_context={}
            )
            assert len(open_team_invitations) == 1
            assert open_team_invitations[0] == response_body
