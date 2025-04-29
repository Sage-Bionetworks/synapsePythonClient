"""Integration tests for Team."""

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
        self.team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        self.TEST_USER = "DPETestUser2"
        self.TEST_MESSAGE = "test message"

    def verify_team_properties(self, actual_team, expected_team):
        """Helper to verify team properties match"""
        assert actual_team.id == expected_team.id
        assert actual_team.name == expected_team.name
        assert actual_team.description == expected_team.description
        assert actual_team.icon == expected_team.icon
        assert actual_team.etag == expected_team.etag
        assert actual_team.created_on == expected_team.created_on
        assert actual_team.modified_on == expected_team.modified_on
        assert actual_team.created_by == expected_team.created_by
        assert actual_team.modified_by == expected_team.modified_by

    async def test_team_lifecycle(self) -> None:
        """Test create, retrieve (by ID, name), and delete operations"""
        # GIVEN a team object

        # WHEN I create the team on Synapse
        test_team = self.team.create()

        # THEN I expect the created team to be returned with correct properties
        assert test_team.id is not None
        assert test_team.name == self.expected_name
        assert test_team.description == self.expected_description
        assert test_team.icon is None
        assert test_team.can_public_join is False
        assert test_team.can_request_membership is True
        assert test_team.etag is not None
        assert test_team.created_on is not None
        assert test_team.modified_on is not None
        assert test_team.created_by is not None
        assert test_team.modified_by is not None

        # WHEN I retrieve the team using a Team object with ID
        id_team = Team(id=test_team.id)
        id_team = id_team.get(synapse_client=self.syn)

        # THEN the retrieved team should match the created team
        self.verify_team_properties(id_team, test_team)

        # WHEN I retrieve the team using the static from_id method
        from_id_team = Team.from_id(id=test_team.id)

        # THEN the retrieved team should match the created team
        self.verify_team_properties(from_id_team, test_team)

        # Name-based retrieval is eventually consistent, so we need to wait
        await asyncio.sleep(10)

        # WHEN I retrieve the team using a Team object with name
        name_team = Team(name=test_team.name)
        name_team = name_team.get(synapse_client=self.syn)

        # THEN the retrieved team should match the created team
        self.verify_team_properties(name_team, test_team)

        # WHEN I retrieve the team using the static from_name method
        from_name_team = Team.from_name(name=test_team.name)

        # THEN the retrieved team should match the created team
        self.verify_team_properties(from_name_team, test_team)

        # WHEN I delete the team
        test_team.delete()

        # THEN the team should no longer exist
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: \nTeam id: '{test_team.id}' does not exist",
        ):
            Team.from_id(id=test_team.id)

    async def test_team_membership_and_invitations(self) -> None:
        """Test team membership and invitation functionality"""
        # GIVEN a team object

        # WHEN I create the team on Synapse
        test_team = self.team.create()

        # AND check the team members
        members = test_team.members()

        # THEN the team should have exactly one member (the creator), who is an admin
        assert len(members) == 1
        assert members[0].team_id == test_team.id
        assert isinstance(members[0].member, UserGroupHeader)
        assert members[0].is_admin is True

        # WHEN I invite a user to the team
        invite = test_team.invite(
            user=self.TEST_USER,
            message=self.TEST_MESSAGE,
        )

        # THEN the invite should be created successfully
        assert invite["id"] is not None
        assert invite["teamId"] == str(test_team.id)
        assert invite["inviteeId"] is not None
        assert invite["message"] == self.TEST_MESSAGE
        assert invite["createdOn"] is not None
        assert invite["createdBy"] is not None

        # WHEN I check the open invitations
        invitations = test_team.open_invitations()

        # THEN I should see the invitation I just created
        assert len(invitations) == 1
        assert invitations[0]["id"] is not None
        assert invitations[0]["teamId"] == str(test_team.id)
        assert invitations[0]["inviteeId"] is not None
        assert invitations[0]["message"] == self.TEST_MESSAGE
        assert invitations[0]["createdOn"] is not None
        assert invitations[0]["createdBy"] is not None

        # Clean up
        test_team.delete()
