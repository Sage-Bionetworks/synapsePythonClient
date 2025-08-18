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

    async def verify_team_properties(self, actual_team, expected_team):
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
        test_team = await self.team.create_async()

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
        id_team = await id_team.get_async(synapse_client=self.syn)

        # THEN the retrieved team should match the created team
        await self.verify_team_properties(id_team, test_team)

        # WHEN I retrieve the team using the static from_id method
        from_id_team = await Team.from_id_async(id=test_team.id)

        # THEN the retrieved team should match the created team
        await self.verify_team_properties(from_id_team, test_team)

        # Name-based retrieval is eventually consistent, so we need to wait
        await asyncio.sleep(10)

        # WHEN I retrieve the team using a Team object with name
        name_team = Team(name=test_team.name)
        name_team = await name_team.get_async(synapse_client=self.syn)

        # THEN the retrieved team should match the created team
        await self.verify_team_properties(name_team, test_team)

        # WHEN I retrieve the team using the static from_name method
        from_name_team = await Team.from_name_async(name=test_team.name)

        # THEN the retrieved team should match the created team
        await self.verify_team_properties(from_name_team, test_team)

        # WHEN I delete the team
        await test_team.delete_async()

        # THEN the team should no longer exist
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Team id: '{test_team.id}' does not exist",
        ):
            await Team.from_id_async(id=test_team.id)

    async def test_team_membership_and_invitations(self) -> None:
        """Test team membership and invitation functionality"""
        # GIVEN a team object

        # WHEN I create the team on Synapse
        test_team = await self.team.create_async()

        # AND check the team members
        members = await test_team.members_async()

        # THEN the team should have exactly one member (the creator), who is an admin
        assert len(members) == 1
        assert members[0].team_id == test_team.id
        assert isinstance(members[0].member, UserGroupHeader)
        assert members[0].is_admin is True

        # WHEN I invite a user to the team
        invite = await test_team.invite_async(
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
        invitations = await test_team.open_invitations_async()

        # THEN I should see the invitation I just created
        assert len(invitations) == 1
        assert invitations[0]["id"] is not None
        assert invitations[0]["teamId"] == str(test_team.id)
        assert invitations[0]["inviteeId"] is not None
        assert invitations[0]["message"] == self.TEST_MESSAGE
        assert invitations[0]["createdOn"] is not None
        assert invitations[0]["createdBy"] is not None

        # Clean up
        await test_team.delete_async()

    async def test_get_user_membership_status(self) -> None:
        """Test getting user membership status for a team"""
        # WHEN I create the team on Synapse
        test_team = await self.team.create_async()

        try:
            # AND I get the membership status for the creator (who should be a member)
            creator_status = await test_team.get_user_membership_status_async(
                user_id=self.syn.getUserProfile().ownerId, team=test_team.id
            )

            # THEN the creator should have membership status indicating they are a member
            assert creator_status is not None
            assert "teamId" in creator_status
            assert creator_status["teamId"] == str(test_team.id)
            assert "userId" in creator_status
            assert "isMember" in creator_status
            assert creator_status["isMember"] is True

            # WHEN I invite a test user to the team
            invite = await test_team.invite_async(
                user=self.TEST_USER,
                message=self.TEST_MESSAGE,
            )

            # Check the invited user's status
            invited_status = await test_team.get_user_membership_status_async(
                user_id=self.syn.getUserProfile(self.TEST_USER).ownerId,
                team=test_team.id,
            )

            # THEN the invited user should show they have an open invitation
            assert invited_status is not None
            assert invited_status["teamId"] == str(test_team.id)
            assert invited_status["hasOpenInvitation"] is True
            assert invited_status["membershipApprovalRequired"] is True
            assert invited_status["canSendEmail"] is True
            assert invited_status["isMember"] is False

        finally:
            # Clean up
            await test_team.delete_async()
