"""Unit tests for team_services utility functions."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import synapseclient.api.team_services as team_services

TEAM_ID = 3412345
TEAM_NAME = "My Test Team"
USER_ID = "1234567"
USER_NAME = "testuser"
INVITATION_ID = "inv-999"
INVITEE_EMAIL = "user@example.com"
MESSAGE = "Please join our team!"


class TestPostTeamList:
    """Tests for post_team_list function."""

    @patch("synapseclient.Synapse")
    async def test_post_team_list_returns_teams(self, mock_synapse):
        """Test retrieving a list of teams by IDs."""
        # GIVEN a mock client that returns a team list
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        team_ids = [111, 222, 333]
        expected_teams = [
            {"id": "111", "name": "Team A"},
            {"id": "222", "name": "Team B"},
            {"id": "333", "name": "Team C"},
        ]
        mock_client.rest_post_async.return_value = {"list": expected_teams}

        # WHEN I call post_team_list
        result = await team_services.post_team_list(
            team_ids=team_ids, synapse_client=None
        )

        # THEN I expect the list of teams to be returned
        assert result == expected_teams
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/teamList", body=json.dumps({"list": team_ids})
        )

    @patch("synapseclient.Synapse")
    async def test_post_team_list_empty_list_returns_none(self, mock_synapse):
        """Test that an empty list response returns None."""
        # GIVEN a mock client that returns an empty list
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_post_async.return_value = {"list": []}

        # WHEN I call post_team_list
        result = await team_services.post_team_list(team_ids=[999], synapse_client=None)

        # THEN I expect None (empty list is falsy)
        assert result is None

    @patch("synapseclient.Synapse")
    async def test_post_team_list_no_list_key_returns_none(self, mock_synapse):
        """Test that a response without 'list' key returns None."""
        # GIVEN a mock client that returns a response without 'list'
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_post_async.return_value = {}

        # WHEN I call post_team_list
        result = await team_services.post_team_list(team_ids=[999], synapse_client=None)

        # THEN I expect None
        assert result is None


class TestCreateTeam:
    """Tests for create_team function."""

    @patch("synapseclient.Synapse")
    async def test_create_team(self, mock_synapse):
        """Test creating a new team."""
        # GIVEN a mock client that returns a created team
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "id": str(TEAM_ID),
            "name": TEAM_NAME,
            "description": "A test team",
            "canPublicJoin": False,
            "canRequestMembership": True,
        }
        mock_client.rest_post_async.return_value = expected_response

        # WHEN I call create_team
        result = await team_services.create_team(
            name=TEAM_NAME,
            description="A test team",
            icon=None,
            can_public_join=False,
            can_request_membership=True,
            synapse_client=None,
        )

        # THEN I expect a POST to /team with the correct body
        assert result == expected_response
        expected_body = {
            "name": TEAM_NAME,
            "description": "A test team",
            "icon": None,
            "canPublicJoin": False,
            "canRequestMembership": True,
        }
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/team", body=json.dumps(expected_body)
        )


class TestDeleteTeam:
    """Tests for delete_team function."""

    @patch("synapseclient.Synapse")
    async def test_delete_team(self, mock_synapse):
        """Test deleting a team by ID."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call delete_team
        await team_services.delete_team(id=TEAM_ID, synapse_client=None)

        # THEN I expect a DELETE to /team/{id}
        mock_client.rest_delete_async.assert_awaited_once_with(uri=f"/team/{TEAM_ID}")


class TestGetTeam:
    """Tests for get_team function."""

    @patch("synapseclient.Synapse")
    async def test_get_team_by_id(self, mock_synapse):
        """Test getting a team by numeric ID."""
        # GIVEN a mock client that returns a team
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": str(TEAM_ID), "name": TEAM_NAME}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_team with a numeric ID
        result = await team_services.get_team(id=TEAM_ID, synapse_client=None)

        # THEN I expect a GET to /team/{id}
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(uri=f"/team/{TEAM_ID}")

    @patch("synapseclient.api.team_services.find_team")
    @patch("synapseclient.Synapse")
    async def test_get_team_by_name(self, mock_synapse, mock_find_team):
        """Test getting a team by name (string that is not a number)."""
        # GIVEN a mock client and find_team that returns a matching team
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_find_team.return_value = [
            {"id": str(TEAM_ID), "name": TEAM_NAME},
        ]
        expected_response = {"id": str(TEAM_ID), "name": TEAM_NAME}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_team with a string name
        result = await team_services.get_team(id=TEAM_NAME, synapse_client=None)

        # THEN I expect find_team to be called and then a GET to /team/{id}
        assert result == expected_response
        mock_find_team.assert_awaited_once_with(TEAM_NAME, synapse_client=mock_client)
        mock_client.rest_get_async.assert_awaited_once_with(uri=f"/team/{TEAM_ID}")

    @patch("synapseclient.api.team_services.find_team")
    @patch("synapseclient.Synapse")
    async def test_get_team_by_name_not_found(self, mock_synapse, mock_find_team):
        """Test getting a team by name when the team does not exist."""
        # GIVEN a mock client and find_team that returns no match
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_find_team.return_value = [
            {"id": "999", "name": "Other Team"},
        ]

        # WHEN I call get_team with a name that doesn't match
        # THEN I expect a ValueError
        with pytest.raises(ValueError, match="Can't find team"):
            await team_services.get_team(id=TEAM_NAME, synapse_client=None)


class TestFindTeam:
    """Tests for find_team function."""

    @patch("synapseclient.api.team_services.rest_get_paginated_async")
    @patch("synapseclient.Synapse")
    async def test_find_team(self, mock_synapse, mock_paginated):
        """Test finding teams by name fragment."""
        # GIVEN a mock client and paginated results
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        async def mock_gen(*args, **kwargs):
            for item in [
                {"id": "111", "name": "Team Alpha"},
                {"id": "222", "name": "Team Beta"},
            ]:
                yield item

        mock_paginated.return_value = mock_gen()

        # WHEN I call find_team
        result = await team_services.find_team(name="Team", synapse_client=None)

        # THEN I expect a list of matching teams
        assert len(result) == 2
        assert result[0]["name"] == "Team Alpha"
        mock_paginated.assert_called_once_with(
            uri="/teams?fragment=Team", synapse_client=mock_client
        )


class TestGetTeamMembers:
    """Tests for get_team_members function."""

    @patch("synapseclient.api.team_services.rest_get_paginated_async")
    @patch("synapseclient.Synapse")
    async def test_get_team_members(self, mock_synapse, mock_paginated):
        """Test getting team members."""
        # GIVEN a mock client and paginated results
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        async def mock_gen(*args, **kwargs):
            for item in [
                {"member": {"ownerId": "111", "userName": "user1"}},
                {"member": {"ownerId": "222", "userName": "user2"}},
            ]:
                yield item

        mock_paginated.return_value = mock_gen()

        # WHEN I call get_team_members
        result = await team_services.get_team_members(team=TEAM_ID, synapse_client=None)

        # THEN I expect a list of team member dicts
        assert len(result) == 2
        mock_paginated.assert_called_once_with(
            uri=f"/teamMembers/{TEAM_ID}", synapse_client=mock_client
        )


class TestSendMembershipInvitation:
    """Tests for send_membership_invitation function."""

    @patch("synapseclient.Synapse")
    async def test_send_membership_invitation_with_user_id(self, mock_synapse):
        """Test sending invitation with invitee_id."""
        # GIVEN a mock client that returns an invitation
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "id": INVITATION_ID,
            "teamId": str(TEAM_ID),
            "inviteeId": USER_ID,
        }
        mock_client.rest_post_async.return_value = expected_response

        # WHEN I call send_membership_invitation with user id
        result = await team_services.send_membership_invitation(
            team_id=TEAM_ID,
            invitee_id=USER_ID,
            message=MESSAGE,
            synapse_client=None,
        )

        # THEN I expect a POST to /membershipInvitation
        assert result == expected_response
        expected_body = {
            "teamId": str(TEAM_ID),
            "message": MESSAGE,
            "inviteeId": str(USER_ID),
        }
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/membershipInvitation", body=json.dumps(expected_body)
        )

    @patch("synapseclient.Synapse")
    async def test_send_membership_invitation_with_email(self, mock_synapse):
        """Test sending invitation with invitee_email."""
        # GIVEN a mock client that returns an invitation
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "id": INVITATION_ID,
            "teamId": str(TEAM_ID),
            "inviteeEmail": INVITEE_EMAIL,
        }
        mock_client.rest_post_async.return_value = expected_response

        # WHEN I call send_membership_invitation with email
        result = await team_services.send_membership_invitation(
            team_id=TEAM_ID,
            invitee_email=INVITEE_EMAIL,
            message=MESSAGE,
            synapse_client=None,
        )

        # THEN I expect a POST to /membershipInvitation
        assert result == expected_response
        expected_body = {
            "teamId": str(TEAM_ID),
            "message": MESSAGE,
            "inviteeEmail": str(INVITEE_EMAIL),
        }
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/membershipInvitation", body=json.dumps(expected_body)
        )


class TestInviteToTeam:
    """Tests for invite_to_team function."""

    @patch("synapseclient.api.team_services.send_membership_invitation")
    @patch("synapseclient.api.team_services.get_membership_status")
    @patch("synapseclient.api.team_services.get_team_open_invitations")
    @patch("synapseclient.models.UserProfile.get_async")
    async def test_invite_to_team_already_member(
        self, mock_get_profile, mock_open_invites, mock_membership_status, mock_send
    ):
        """Test invite_to_team when user is already a member returns None."""
        # GIVEN a user who is already a member
        mock_profile = MagicMock()
        mock_profile.id = int(USER_ID)
        mock_profile.username = USER_NAME
        mock_get_profile.return_value = mock_profile
        mock_open_invites.return_value = []
        mock_membership_status.return_value = {"isMember": True}

        # WHEN I call invite_to_team
        with patch("synapseclient.Synapse") as mock_syn_class:
            mock_client = MagicMock()
            mock_client.logger = MagicMock()
            mock_syn_class.get_client.return_value = mock_client
            result = await team_services.invite_to_team(
                team=TEAM_ID, user=USER_NAME, synapse_client=None
            )

        # THEN I expect None and no invitation sent
        assert result is None
        mock_send.assert_not_awaited()

    @patch("synapseclient.api.team_services.send_membership_invitation")
    @patch("synapseclient.api.team_services.delete_membership_invitation")
    @patch("synapseclient.api.team_services.get_membership_status")
    @patch("synapseclient.api.team_services.get_team_open_invitations")
    @patch("synapseclient.models.UserProfile.get_async")
    async def test_invite_to_team_force_delete_existing_invitation(
        self,
        mock_get_profile,
        mock_open_invites,
        mock_membership_status,
        mock_delete_invite,
        mock_send,
    ):
        """Test invite_to_team with force=True deletes existing invitations."""
        # GIVEN a user with an existing open invitation
        mock_profile = MagicMock()
        mock_profile.id = int(USER_ID)
        mock_profile.username = USER_NAME
        mock_get_profile.return_value = mock_profile
        mock_open_invites.return_value = [
            {"id": INVITATION_ID, "inviteeId": int(USER_ID)},
        ]
        mock_membership_status.return_value = {"isMember": False}
        expected_invite = {"id": "new-inv", "teamId": str(TEAM_ID)}
        mock_send.return_value = expected_invite

        # WHEN I call invite_to_team with force=True
        result = await team_services.invite_to_team(
            team=TEAM_ID, user=USER_NAME, force=True, synapse_client=None
        )

        # THEN I expect the old invitation to be deleted and a new one created
        assert result == expected_invite
        mock_delete_invite.assert_awaited_once_with(
            invitation_id=INVITATION_ID, synapse_client=None
        )
        mock_send.assert_awaited_once()

    async def test_invite_to_team_both_user_and_email_raises(self):
        """Test that providing both user and inviteeEmail raises ValueError."""
        # GIVEN both user and invitee_email are specified
        # WHEN I call invite_to_team
        # THEN I expect a ValueError
        with pytest.raises(
            ValueError, match="Must specify either 'user' or 'inviteeEmail'"
        ):
            await team_services.invite_to_team(
                team=TEAM_ID,
                user=USER_NAME,
                invitee_email=INVITEE_EMAIL,
                synapse_client=None,
            )

    async def test_invite_to_team_neither_user_nor_email_raises(self):
        """Test that providing neither user nor inviteeEmail raises ValueError."""
        # GIVEN neither user nor invitee_email is specified
        # WHEN I call invite_to_team
        # THEN I expect a ValueError
        with pytest.raises(
            ValueError, match="Must specify either 'user' or 'inviteeEmail'"
        ):
            await team_services.invite_to_team(
                team=TEAM_ID,
                synapse_client=None,
            )

    @patch("synapseclient.api.team_services.send_membership_invitation")
    @patch("synapseclient.api.team_services.get_team_open_invitations")
    async def test_invite_to_team_by_email(self, mock_open_invites, mock_send):
        """Test invite_to_team with email only (no user)."""
        # GIVEN no existing invitations for the email
        mock_open_invites.return_value = []
        expected_invite = {
            "id": "new-inv",
            "teamId": str(TEAM_ID),
            "inviteeEmail": INVITEE_EMAIL,
        }
        mock_send.return_value = expected_invite

        # WHEN I call invite_to_team with email
        result = await team_services.invite_to_team(
            team=TEAM_ID,
            invitee_email=INVITEE_EMAIL,
            message=MESSAGE,
            synapse_client=None,
        )

        # THEN I expect the invitation to be created
        assert result == expected_invite
        mock_send.assert_awaited_once_with(
            str(TEAM_ID),
            invitee_id=None,
            invitee_email=INVITEE_EMAIL,
            message=MESSAGE,
            synapse_client=None,
        )
