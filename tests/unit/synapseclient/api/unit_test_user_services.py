"""Unit tests for user_services utility functions."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import synapseclient.api.user_services as user_services
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseHTTPError,
    SynapseNotFoundError,
)

USER_ID = 1234567
USER_NAME = "testuser"
OWNER_ID = "1234567"
PUBLIC_ID = 273949
BUNDLE_MASK = 63


class TestGetUserProfileById:
    """Tests for get_user_profile_by_id function."""

    @patch("synapseclient.Synapse")
    async def test_get_user_profile_by_id(self, mock_synapse):
        """Test getting a user profile by numeric ID."""
        # GIVEN a mock client that returns a user profile
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "ownerId": OWNER_ID,
            "userName": USER_NAME,
            "firstName": "Test",
        }
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_user_profile_by_id with an id
        result = await user_services.get_user_profile_by_id(
            id=USER_ID, synapse_client=None
        )

        # THEN I expect a GET to /userProfile/{id}
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/userProfile/{USER_ID}"
        )

    @patch("synapseclient.Synapse")
    async def test_get_user_profile_by_id_no_id(self, mock_synapse):
        """Test getting current user profile when id is omitted."""
        # GIVEN a mock client that returns the current user profile
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"ownerId": "999", "userName": "currentuser"}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_user_profile_by_id without an id
        result = await user_services.get_user_profile_by_id(synapse_client=None)

        # THEN I expect a GET to /userProfile/ (empty id)
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(uri="/userProfile/")

    @patch("synapseclient.Synapse")
    async def test_get_user_profile_by_id_non_int_raises_type_error(self, mock_synapse):
        """Test that passing a non-int id raises TypeError."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call get_user_profile_by_id with a string id
        # THEN I expect a TypeError
        with pytest.raises(TypeError, match="id must be an 'ownerId' integer"):
            await user_services.get_user_profile_by_id(
                id="not_an_int", synapse_client=None
            )


class TestGetUserProfileByUsername:
    """Tests for get_user_profile_by_username function."""

    @patch("synapseclient.api.user_services._find_principals")
    @patch("synapseclient.Synapse")
    async def test_get_user_profile_by_username(
        self, mock_synapse, mock_find_principals
    ):
        """Test getting a user profile by username."""
        # GIVEN a mock client and matching principal
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_find_principals.return_value = [
            {"userName": USER_NAME, "ownerId": OWNER_ID},
        ]
        expected_response = {"ownerId": OWNER_ID, "userName": USER_NAME}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_user_profile_by_username
        result = await user_services.get_user_profile_by_username(
            username=USER_NAME, synapse_client=None
        )

        # THEN I expect a GET to /userProfile/{id} after principal lookup
        assert result == expected_response
        mock_find_principals.assert_awaited_once_with(USER_NAME, synapse_client=None)
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/userProfile/{OWNER_ID}"
        )

    @patch("synapseclient.Synapse")
    async def test_get_user_profile_by_username_none(self, mock_synapse):
        """Test getting current user profile when username is None."""
        # GIVEN a mock client that returns current user
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"ownerId": "999", "userName": "currentuser"}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_user_profile_by_username with None
        result = await user_services.get_user_profile_by_username(
            username=None, synapse_client=None
        )

        # THEN I expect a GET to /userProfile/ (empty id)
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(uri="/userProfile/")

    @patch("synapseclient.api.user_services._find_principals")
    @patch("synapseclient.Synapse")
    async def test_get_user_profile_by_username_not_found(
        self, mock_synapse, mock_find_principals
    ):
        """Test that SynapseNotFoundError is raised when username is not found."""
        # GIVEN a mock client and no matching principals
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_find_principals.return_value = [
            {"userName": "otheruser", "ownerId": "999"},
        ]

        # WHEN I call get_user_profile_by_username with a non-matching name
        # THEN I expect SynapseNotFoundError
        with pytest.raises(SynapseNotFoundError, match="Can't find user"):
            await user_services.get_user_profile_by_username(
                username="nonexistent", synapse_client=None
            )


class TestIsUserCertified:
    """Tests for is_user_certified function."""

    @patch("synapseclient.api.user_services._get_certified_passing_record")
    async def test_is_user_certified_true(self, mock_get_record):
        """Test that a certified user returns True."""
        # GIVEN a user who has passed the certification quiz
        mock_get_record.return_value = {"passed": True, "quizId": 1}

        # WHEN I call is_user_certified with a numeric user id
        result = await user_services.is_user_certified(
            user=USER_ID, synapse_client=None
        )

        # THEN I expect True
        assert result is True
        mock_get_record.assert_awaited_once_with(USER_ID, synapse_client=None)

    @patch("synapseclient.api.user_services._get_certified_passing_record")
    async def test_is_user_certified_false(self, mock_get_record):
        """Test that a non-certified user returns False."""
        # GIVEN a user who has not passed the certification quiz
        mock_get_record.return_value = {"passed": False, "quizId": 1}

        # WHEN I call is_user_certified
        result = await user_services.is_user_certified(
            user=USER_ID, synapse_client=None
        )

        # THEN I expect False
        assert result is False

    @patch("synapseclient.api.user_services._get_certified_passing_record")
    async def test_is_user_certified_not_found_returns_false(self, mock_get_record):
        """Test that a 404 for passing record returns False."""
        # GIVEN a user who hasn't taken the quiz (404 response)
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get_record.side_effect = SynapseHTTPError(
            "Not found", response=mock_response
        )

        # WHEN I call is_user_certified
        result = await user_services.is_user_certified(
            user=USER_ID, synapse_client=None
        )

        # THEN I expect False
        assert result is False

    @patch("synapseclient.api.user_services._find_principals")
    async def test_is_user_certified_username_not_found_raises(
        self, mock_find_principals
    ):
        """Test that ValueError is raised when username cannot be resolved."""
        # GIVEN no matching principals for the username
        mock_find_principals.return_value = [
            {"userName": "someone_else", "ownerId": "999"},
        ]

        # WHEN I call is_user_certified with a non-matching username
        # THEN I expect a ValueError
        with pytest.raises(ValueError, match="Can't find user"):
            await user_services.is_user_certified(
                user="nonexistent_user", synapse_client=None
            )


class TestGetUserByPrincipalIdOrName:
    """Tests for get_user_by_principal_id_or_name function."""

    @patch("synapseclient.Synapse")
    async def test_get_user_none_returns_public(self, mock_synapse):
        """Test that None principal_id returns PUBLIC constant."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call get_user_by_principal_id_or_name with None
        result = await user_services.get_user_by_principal_id_or_name(
            principal_id=None, synapse_client=None
        )

        # THEN I expect PUBLIC id (273949)
        assert result == PUBLIC_ID

    @patch("synapseclient.Synapse")
    async def test_get_user_public_string_returns_public(self, mock_synapse):
        """Test that 'PUBLIC' string returns PUBLIC constant."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call get_user_by_principal_id_or_name with "PUBLIC"
        result = await user_services.get_user_by_principal_id_or_name(
            principal_id="PUBLIC", synapse_client=None
        )

        # THEN I expect PUBLIC id (273949)
        assert result == PUBLIC_ID

    @patch("synapseclient.Synapse")
    async def test_get_user_by_int(self, mock_synapse):
        """Test that an integer principal_id is returned directly."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call get_user_by_principal_id_or_name with an int
        result = await user_services.get_user_by_principal_id_or_name(
            principal_id=USER_ID, synapse_client=None
        )

        # THEN I expect the int ID returned directly
        assert result == USER_ID

    @patch("synapseclient.Synapse")
    async def test_get_user_by_string_single_match(self, mock_synapse):
        """Test looking up a user by name string with a single match."""
        # GIVEN a mock client that returns a single matching user
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {
            "children": [{"ownerId": OWNER_ID, "userName": USER_NAME}]
        }

        # WHEN I call get_user_by_principal_id_or_name with a string name
        result = await user_services.get_user_by_principal_id_or_name(
            principal_id=USER_NAME, synapse_client=None
        )

        # THEN I expect the user's ownerId as int
        assert result == USER_ID
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/userGroupHeaders?prefix={USER_NAME}"
        )

    @patch("synapseclient.Synapse")
    async def test_get_user_by_string_multiple_matches_exact(self, mock_synapse):
        """Test looking up a user by name string with multiple matches but exact match exists."""
        # GIVEN a mock client that returns multiple matching users
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {
            "children": [
                {"ownerId": "999", "userName": "testuser2"},
                {"ownerId": OWNER_ID, "userName": USER_NAME},
            ]
        }

        # WHEN I call get_user_by_principal_id_or_name with the exact name
        result = await user_services.get_user_by_principal_id_or_name(
            principal_id=USER_NAME, synapse_client=None
        )

        # THEN I expect the exact match's ownerId
        assert result == USER_ID

    @patch("synapseclient.Synapse")
    async def test_get_user_by_string_no_match_raises(self, mock_synapse):
        """Test that SynapseError is raised when no match is found."""
        # GIVEN a mock client that returns no matching users
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {"children": []}

        # WHEN I call get_user_by_principal_id_or_name with an unknown name
        # THEN I expect a SynapseError
        with pytest.raises(SynapseError, match="Unknown Synapse user.*No matches"):
            await user_services.get_user_by_principal_id_or_name(
                principal_id="unknown_user", synapse_client=None
            )

    @patch("synapseclient.Synapse")
    async def test_get_user_by_string_ambiguous_raises(self, mock_synapse):
        """Test that SynapseError is raised when multiple matches exist and none is exact."""
        # GIVEN a mock client that returns multiple users with no exact match
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {
            "children": [
                {"ownerId": "111", "userName": "testuser1"},
                {"ownerId": "222", "userName": "testuser2"},
            ]
        }

        # WHEN I call get_user_by_principal_id_or_name with an ambiguous prefix
        # THEN I expect a SynapseError asking to be more specific
        with pytest.raises(
            SynapseError, match="Unknown Synapse user.*Please be more specific"
        ):
            await user_services.get_user_by_principal_id_or_name(
                principal_id="testuser", synapse_client=None
            )


class TestGetUserBundle:
    """Tests for get_user_bundle function."""

    @patch("synapseclient.Synapse")
    async def test_get_user_bundle_success(self, mock_synapse):
        """Test getting a user bundle successfully."""
        # GIVEN a mock client that returns a user bundle
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "userId": str(USER_ID),
            "userProfile": {"ownerId": OWNER_ID, "userName": USER_NAME},
        }
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_user_bundle
        result = await user_services.get_user_bundle(
            user_id=USER_ID, mask=BUNDLE_MASK, synapse_client=None
        )

        # THEN I expect the user bundle
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/user/{USER_ID}/bundle?mask={BUNDLE_MASK}"
        )

    @patch("synapseclient.Synapse")
    async def test_get_user_bundle_not_found_returns_none(self, mock_synapse):
        """Test that a 404 error returns None."""
        # GIVEN a mock client that raises a 404 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client.rest_get_async.side_effect = SynapseHTTPError(
            "Not found", response=mock_response
        )

        # WHEN I call get_user_bundle for a non-existent user
        result = await user_services.get_user_bundle(
            user_id=9999999, mask=BUNDLE_MASK, synapse_client=None
        )

        # THEN I expect None
        assert result is None

    @patch("synapseclient.Synapse")
    async def test_get_user_bundle_other_error_raises(self, mock_synapse):
        """Test that non-404 HTTP errors are raised."""
        # GIVEN a mock client that raises a 500 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client.rest_get_async.side_effect = SynapseHTTPError(
            "Server error", response=mock_response
        )

        # WHEN I call get_user_bundle
        # THEN I expect the error to be raised
        with pytest.raises(SynapseHTTPError):
            await user_services.get_user_bundle(
                user_id=USER_ID, mask=BUNDLE_MASK, synapse_client=None
            )
