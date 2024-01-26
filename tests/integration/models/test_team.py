import asyncio
import uuid

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Team
from synapseclient.models.user import UserGroupHeader


class TestTeam:
    """Integration tests for Team."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn
        self.expected_name = "test_team_" + str(uuid.uuid4())
        self.expected_description = "test description"
        self.expected_icon = None

    @pytest.mark.asyncio
    async def test_create(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # THEN I expect the created team to be returned
        assert test_team.id is not None
        assert test_team.name == self.expected_name
        assert test_team.description == self.expected_description
        assert test_team.icon is None
        assert test_team.etag is not None
        assert test_team.created_on is not None
        assert test_team.modified_on is not None
        assert test_team.created_by is not None
        assert test_team.modified_by is not None
        # Clean up
        await test_team.delete()

    @pytest.mark.asyncio
    async def test_delete(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # AND I delete the team
        await test_team.delete()
        # THEN I expect the team to no longer exist
        with pytest.raises(SynapseHTTPError):
            await Team().from_id(id=test_team.id)

    @pytest.mark.asyncio
    async def test_from_id(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # THEN I expect the team to be returned by from_id
        test_team_from_id = await Team().from_id(id=test_team.id)
        assert test_team_from_id.id == test_team.id
        assert test_team_from_id.name == test_team.name
        assert test_team_from_id.description == test_team.description
        assert test_team_from_id.icon == test_team.icon
        assert test_team_from_id.etag == test_team.etag
        assert test_team_from_id.created_on == test_team.created_on
        assert test_team_from_id.modified_on == test_team.modified_on
        assert test_team_from_id.created_by == test_team.created_by
        assert test_team_from_id.modified_by == test_team.modified_by
        # Clean up
        await test_team.delete()

    @pytest.mark.asyncio
    async def test_from_name(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # TODO why do we need to sleep here?
        await asyncio.sleep(5)
        # THEN I expect the team to be returned by from_name
        test_team_from_name = await Team().from_name(name=test_team.name)
        assert test_team_from_name.id == test_team.id
        assert test_team_from_name.name == test_team.name
        assert test_team_from_name.description == test_team.description
        assert test_team_from_name.icon == test_team.icon
        assert test_team_from_name.etag == test_team.etag
        assert test_team_from_name.created_on == test_team.created_on
        assert test_team_from_name.modified_on == test_team.modified_on
        assert test_team_from_name.created_by == test_team.created_by
        assert test_team_from_name.modified_by == test_team.modified_by
        # Clean up
        await test_team.delete()

    @pytest.mark.asyncio
    async def test_members(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # THEN I expect the team members to be returned by members
        test_team_members = await test_team.members()
        assert len(test_team_members) == 1
        assert test_team_members[0].team_id == test_team.id
        assert isinstance(test_team_members[0].member, UserGroupHeader)
        assert test_team_members[0].is_admin == True
        # Clean up
        await test_team.delete()

    @pytest.mark.asyncio
    async def test_invite(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # AND I invite a user to the team
        test_invite = await test_team.invite(
            user="test_account_synapse_client",
            message="test message",
        )
        # THEN I expect the invite to be returned
        assert test_invite["id"] is not None
        assert test_invite["teamId"] == str(test_team.id)
        assert test_invite["inviteeId"] is not None
        assert test_invite["message"] == "test message"
        assert test_invite["createdOn"] is not None
        assert test_invite["createdBy"] is not None

        # Clean up
        await test_team.delete()

    @pytest.mark.asyncio
    async def test_open_invitations(self):
        # GIVEN a team
        test_team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        # WHEN I create the team
        test_team = await test_team.create()
        # AND I invite a user to the team
        await test_team.invite(
            user="test_account_synapse_client",
            message="test message",
        )
        # THEN I expect the invite to be returned by open_invitations
        test_open_invitations = await test_team.open_invitations()
        assert len(test_open_invitations) == 1
        assert test_open_invitations[0]["id"] is not None
        assert test_open_invitations[0]["teamId"] == str(test_team.id)
        assert test_open_invitations[0]["inviteeId"] is not None
        assert test_open_invitations[0]["message"] == "test message"
        assert test_open_invitations[0]["createdOn"] is not None
        assert test_open_invitations[0]["createdBy"] is not None

        # Clean up
        await test_team.delete()
