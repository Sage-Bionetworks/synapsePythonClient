from synapseclient.team import Team as Synapse_Team, TeamMember as Synapse_TeamMember
from synapseclient.models.team import Team, TeamMember


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
    ...
