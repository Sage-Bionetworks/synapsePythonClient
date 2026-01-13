"""Unit tests for entity_services utility functions."""
from unittest.mock import AsyncMock, patch

import pytest

import synapseclient.api.entity_services as entity_services
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
)


class TestGetChild:
    """Tests for get_child function."""

    @patch("synapseclient.Synapse")
    async def test_get_child_found(self, mock_synapse):
        """Test get_child when entity is found."""
        # GIVEN a mock client that returns an entity ID
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_post_async.return_value = {"id": "syn123456"}

        # WHEN I call get_child
        result = await entity_services.get_child(
            entity_name="test_entity",
            parent_id="syn789",
            synapse_client=None,
        )

        # THEN I expect the entity ID to be returned
        assert result == "syn123456"
        mock_client.rest_post_async.assert_awaited_once()

    @patch("synapseclient.Synapse")
    async def test_get_child_not_found(self, mock_synapse):
        """Test get_child when entity is not found."""
        # GIVEN a mock client that raises a 404 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # Create a proper SynapseHTTPError with response attribute
        mock_response = AsyncMock()
        mock_response.status_code = 404
        error = SynapseHTTPError("Not found", response=mock_response)
        mock_client.rest_post_async.side_effect = error

        # WHEN I call get_child
        result = await entity_services.get_child(
            entity_name="nonexistent",
            parent_id="syn789",
            synapse_client=None,
        )

        # THEN I expect None to be returned
        assert result is None

    @patch("synapseclient.Synapse")
    async def test_get_child_other_error(self, mock_synapse):
        """Test get_child when a non-404 error occurs."""
        # GIVEN a mock client that raises a non-404 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        mock_response = AsyncMock()
        mock_response.status_code = 500
        error = SynapseHTTPError("Server error", response=mock_response)
        mock_client.rest_post_async.side_effect = error

        # WHEN I call get_child
        # THEN I expect the error to be raised
        with pytest.raises(SynapseHTTPError):
            await entity_services.get_child(
                entity_name="test",
                parent_id="syn789",
                synapse_client=None,
            )

    @patch("synapseclient.Synapse")
    async def test_get_child_project_lookup(self, mock_synapse):
        """Test get_child for project lookup with None parent."""
        # GIVEN a mock client that returns a project ID
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_post_async.return_value = {"id": "syn123456"}

        # WHEN I call get_child with None parent (for project lookup)
        result = await entity_services.get_child(
            entity_name="My Project",
            parent_id=None,
            synapse_client=None,
        )

        # THEN I expect the project ID to be returned
        assert result == "syn123456"
        # Verify the request body had parentId as None
        call_args = mock_client.rest_post_async.call_args
        assert '"parentId": null' in call_args.kwargs["body"]


class TestIsSynapseId:
    """Tests for is_synapse_id function."""

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_valid(self, mock_synapse, mock_get_entity):
        """Test is_synapse_id with a valid ID."""
        # GIVEN a valid synapse ID that exists
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_get_entity.return_value = {"id": "syn123456"}

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id("syn123456", synapse_client=None)

        # THEN I expect True
        assert result is True
        mock_get_entity.assert_awaited_once()

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_file_not_found(self, mock_synapse, mock_get_entity):
        """Test is_synapse_id with file not found error."""
        # GIVEN a synapse ID that raises SynapseFileNotFoundError
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_get_entity.side_effect = SynapseFileNotFoundError("Not found")

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id("syn999999", synapse_client=None)

        # THEN I expect False
        assert result is False

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_404_error(self, mock_synapse, mock_get_entity):
        """Test is_synapse_id with 404 HTTP error."""
        # GIVEN a synapse ID that raises a 404 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        mock_response = AsyncMock()
        mock_response.status_code = 404
        error = SynapseHTTPError("Not found", response=mock_response)
        mock_get_entity.side_effect = error

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id("syn999999", synapse_client=None)

        # THEN I expect False
        assert result is False

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_400_error(self, mock_synapse, mock_get_entity):
        """Test is_synapse_id with 400 HTTP error."""
        # GIVEN a synapse ID that raises a 400 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        mock_response = AsyncMock()
        mock_response.status_code = 400
        error = SynapseHTTPError("Bad request", response=mock_response)
        mock_get_entity.side_effect = error

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id("invalid", synapse_client=None)

        # THEN I expect False
        assert result is False

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_403_error(self, mock_synapse, mock_get_entity):
        """Test is_synapse_id with 403 permission error."""
        # GIVEN a synapse ID that raises a 403 error (no permission)
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        mock_response = AsyncMock()
        mock_response.status_code = 403
        error = SynapseHTTPError("Forbidden", response=mock_response)
        mock_get_entity.side_effect = error

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id("syn123456", synapse_client=None)

        # THEN I expect True (valid ID but no permission)
        assert result is True

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_403_auth_error_context(
        self, mock_synapse, mock_get_entity
    ):
        """Test is_synapse_id with 403 error in __context__."""
        # GIVEN a synapse ID that raises auth error with context
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # Create an error with __context__ that has response
        mock_response = AsyncMock()
        mock_response.status_code = 403
        context_error = SynapseHTTPError("Forbidden", response=mock_response)
        error = SynapseAuthenticationError("Auth failed")
        error.__context__ = context_error
        mock_get_entity.side_effect = error

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id("syn123456", synapse_client=None)

        # THEN I expect True (valid ID but not authenticated)
        assert result is True

    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_not_string(self, mock_synapse):
        """Test is_synapse_id with non-string input."""
        # GIVEN a non-string value
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I check if it's valid
        result = await entity_services.is_synapse_id(123456, synapse_client=None)

        # THEN I expect False
        assert result is False

    @patch("synapseclient.api.entity_services.get_entity")
    @patch("synapseclient.Synapse")
    async def test_is_synapse_id_500_error_raises(self, mock_synapse, mock_get_entity):
        """Test is_synapse_id with 500 error that should be raised."""
        # GIVEN a synapse ID that raises a 500 error
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        mock_response = AsyncMock()
        mock_response.status_code = 500
        error = SynapseHTTPError("Server error", response=mock_response)
        mock_get_entity.side_effect = error

        # WHEN I check if it's valid
        # THEN I expect the error to be raised
        with pytest.raises(SynapseHTTPError):
            await entity_services.is_synapse_id("syn123456", synapse_client=None)


class TestGetEntitiesByMd5:
    """Tests for get_entities_by_md5 function."""

    @patch("synapseclient.Synapse")
    async def test_get_entities_by_md5_with_results(self, mock_synapse):
        """Test get_entities_by_md5 with matching entities."""
        # GIVEN a mock client that returns matching entities
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {
            "results": [
                {"id": "syn123456", "name": "file1.txt"},
                {"id": "syn789012", "name": "file2.txt"},
            ]
        }

        # WHEN I query by MD5
        result = await entity_services.get_entities_by_md5(
            md5="abc123def456", synapse_client=None
        )

        # THEN I expect the results dictionary
        assert result["results"] is not None
        assert len(result["results"]) == 2
        assert result["results"][0]["id"] == "syn123456"
        mock_client.rest_get_async.assert_awaited_once()

    @patch("synapseclient.Synapse")
    async def test_get_entities_by_md5_no_results(self, mock_synapse):
        """Test get_entities_by_md5 with no matching entities."""
        # GIVEN a mock client that returns empty results
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = {"results": []}

        # WHEN I query by MD5
        result = await entity_services.get_entities_by_md5(
            md5="nonexistent", synapse_client=None
        )

        # THEN I expect empty results
        assert result["results"] == []
        mock_client.rest_get_async.assert_awaited_once()
