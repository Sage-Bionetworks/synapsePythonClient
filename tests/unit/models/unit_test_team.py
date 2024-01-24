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
    async def test_create(self, syn):
        with patch.object(
            Synapse,
            "get_client",
            return_value=syn,
        ) as patch_get_client:
            patch_get_client.create_team.return_value = (
                Synapse_Team(id=1, name="test_team", description="test description"),
            )
            team = Team(name="test_team", description="test description")
            team = await team.create()
            patch_get_client.create_team.assert_called_once_with(id=1, name="test_team")
            assert team.id == 1
            assert team.name == "test_team"
