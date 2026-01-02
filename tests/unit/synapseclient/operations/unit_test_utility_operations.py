"""Unit tests for utility_operations wrapper functions."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient.operations import (
    find_entity_id,
    find_entity_id_async,
    is_synapse_id,
    is_synapse_id_async,
    md5_query,
    md5_query_async,
    onweb,
    onweb_async,
)


class TestFindEntityId:
    """Tests for find_entity_id wrapper functions."""

    @patch("synapseclient.api.get_child")
    @patch("synapseclient.core.utils.id_of")
    async def test_find_entity_id_async_with_parent_id(
        self, mock_id_of, mock_get_child
    ):
        """Test find_entity_id_async with parent ID string."""
        # GIVEN a mock get_child that returns an entity ID
        mock_get_child.return_value = "syn123456"
        mock_id_of.return_value = "syn789"

        # WHEN I call find_entity_id_async with parent ID
        result = await find_entity_id_async(
            name="test_entity", parent="syn789", synapse_client=None
        )

        # THEN I expect the correct entity ID
        assert result == "syn123456"
        mock_id_of.assert_called_once_with("syn789")
        mock_get_child.assert_awaited_once_with(
            entity_name="test_entity", parent_id="syn789", synapse_client=None
        )

    @patch("synapseclient.api.get_child")
    @patch("synapseclient.core.utils.id_of")
    async def test_find_entity_id_async_with_parent_object(
        self, mock_id_of, mock_get_child
    ):
        """Test find_entity_id_async with parent object."""
        # GIVEN a mock get_child that returns an entity ID
        mock_get_child.return_value = "syn123456"
        mock_id_of.return_value = "syn999"

        mock_parent = MagicMock()

        # WHEN I call find_entity_id_async with parent object
        result = await find_entity_id_async(
            name="test_entity", parent=mock_parent, synapse_client=None
        )

        # THEN I expect the correct entity ID
        assert result == "syn123456"
        mock_id_of.assert_called_once_with(mock_parent)
        mock_get_child.assert_awaited_once_with(
            entity_name="test_entity", parent_id="syn999", synapse_client=None
        )

    @patch("synapseclient.api.get_child")
    async def test_find_entity_id_async_no_parent(self, mock_get_child):
        """Test find_entity_id_async without parent (project lookup)."""
        # GIVEN a mock get_child that returns a project ID
        mock_get_child.return_value = "syn123456"

        # WHEN I call find_entity_id_async without parent
        result = await find_entity_id_async(
            name="My Project", parent=None, synapse_client=None
        )

        # THEN I expect the correct project ID
        assert result == "syn123456"
        # Note: id_of is not called when parent is None due to short-circuit evaluation
        mock_get_child.assert_awaited_once_with(
            entity_name="My Project", parent_id=None, synapse_client=None
        )

    @patch("synapseclient.operations.utility_operations.wrap_async_to_sync")
    def test_find_entity_id_sync(self, mock_wrap):
        """Test find_entity_id synchronous wrapper."""
        # GIVEN a mock wrap_async_to_sync
        mock_wrap.return_value = "syn123456"

        # WHEN I call find_entity_id
        result = find_entity_id(name="test", parent="syn789", synapse_client=None)

        # THEN I expect wrap_async_to_sync to be called
        assert result == "syn123456"
        mock_wrap.assert_called_once()


class TestIsSynapseId:
    """Tests for is_synapse_id wrapper functions."""

    @patch(
        "synapseclient.api.entity_services.is_synapse_id",
        new_callable=AsyncMock,
    )
    async def test_is_synapse_id_async_valid(self, mock_api_is_synapse_id):
        """Test is_synapse_id_async with valid ID."""
        # GIVEN a mock API function that returns True
        mock_api_is_synapse_id.return_value = True

        # WHEN I call is_synapse_id_async
        result = await is_synapse_id_async("syn123456", synapse_client=None)

        # THEN I expect True
        assert result is True
        mock_api_is_synapse_id.assert_awaited_once_with(
            syn_id="syn123456", synapse_client=None
        )

    @patch(
        "synapseclient.api.entity_services.is_synapse_id",
        new_callable=AsyncMock,
    )
    async def test_is_synapse_id_async_invalid(self, mock_api_is_synapse_id):
        """Test is_synapse_id_async with invalid ID."""
        # GIVEN a mock API function that returns False
        mock_api_is_synapse_id.return_value = False

        # WHEN I call is_synapse_id_async
        result = await is_synapse_id_async("syn999999", synapse_client=None)

        # THEN I expect False
        assert result is False
        mock_api_is_synapse_id.assert_awaited_once_with(
            syn_id="syn999999", synapse_client=None
        )

    @patch("synapseclient.operations.utility_operations.wrap_async_to_sync")
    def test_is_synapse_id_sync(self, mock_wrap):
        """Test is_synapse_id synchronous wrapper."""
        # GIVEN a mock wrap_async_to_sync
        mock_wrap.return_value = True

        # WHEN I call is_synapse_id
        result = is_synapse_id("syn123456", synapse_client=None)

        # THEN I expect wrap_async_to_sync to be called
        assert result is True
        mock_wrap.assert_called_once()


class TestOnweb:
    """Tests for onweb wrapper functions."""

    @patch(
        "synapseclient.api.web_services.open_entity_in_browser",
        new_callable=AsyncMock,
    )
    async def test_onweb_async_with_id(self, mock_open_entity):
        """Test onweb_async with Synapse ID."""
        # GIVEN a mock API function that returns a URL
        mock_open_entity.return_value = "https://www.synapse.org#!Synapse:syn123456"

        # WHEN I call onweb_async
        result = await onweb_async("syn123456", synapse_client=None)

        # THEN I expect the URL to be returned
        assert result == "https://www.synapse.org#!Synapse:syn123456"
        mock_open_entity.assert_awaited_once_with(
            entity="syn123456", subpage_id=None, synapse_client=None
        )

    @patch(
        "synapseclient.api.web_services.open_entity_in_browser",
        new_callable=AsyncMock,
    )
    async def test_onweb_async_with_subpage(self, mock_open_entity):
        """Test onweb_async with subpage ID."""
        # GIVEN a mock API function that returns a wiki URL
        mock_open_entity.return_value = (
            "https://www.synapse.org#!Wiki:syn123456/ENTITY/12345"
        )

        # WHEN I call onweb_async with subpage_id
        result = await onweb_async("syn123456", subpage_id="12345", synapse_client=None)

        # THEN I expect the wiki URL to be returned
        assert result == "https://www.synapse.org#!Wiki:syn123456/ENTITY/12345"
        mock_open_entity.assert_awaited_once_with(
            entity="syn123456", subpage_id="12345", synapse_client=None
        )

    @patch("synapseclient.operations.utility_operations.wrap_async_to_sync")
    def test_onweb_sync(self, mock_wrap):
        """Test onweb synchronous wrapper."""
        # GIVEN a mock wrap_async_to_sync
        mock_wrap.return_value = "https://www.synapse.org#!Synapse:syn123456"

        # WHEN I call onweb
        result = onweb("syn123456", synapse_client=None)

        # THEN I expect wrap_async_to_sync to be called
        assert result == "https://www.synapse.org#!Synapse:syn123456"
        mock_wrap.assert_called_once()


class TestMd5Query:
    """Tests for md5_query wrapper functions."""

    @patch(
        "synapseclient.api.get_entities_by_md5",
        new_callable=AsyncMock,
    )
    async def test_md5_query_async_with_results(self, mock_get_entities):
        """Test md5_query_async with matching entities."""
        # GIVEN a mock API function that returns results
        mock_get_entities.return_value = {
            "results": [
                {"id": "syn123456", "name": "file1.txt"},
                {"id": "syn789012", "name": "file2.txt"},
            ]
        }

        # WHEN I call md5_query_async
        result = await md5_query_async("abc123", synapse_client=None)

        # THEN I expect the results list to be returned
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == "syn123456"
        mock_get_entities.assert_awaited_once_with(md5="abc123", synapse_client=None)

    @patch(
        "synapseclient.api.get_entities_by_md5",
        new_callable=AsyncMock,
    )
    async def test_md5_query_async_no_results(self, mock_get_entities):
        """Test md5_query_async with no matching entities."""
        # GIVEN a mock API function that returns empty results
        mock_get_entities.return_value = {"results": []}

        # WHEN I call md5_query_async
        result = await md5_query_async("nonexistent", synapse_client=None)

        # THEN I expect an empty list
        assert isinstance(result, list)
        assert len(result) == 0
        mock_get_entities.assert_awaited_once_with(
            md5="nonexistent", synapse_client=None
        )

    @patch(
        "synapseclient.api.get_entities_by_md5",
        new_callable=AsyncMock,
    )
    async def test_md5_query_async_missing_results_key(self, mock_get_entities):
        """Test md5_query_async when results key is missing."""
        # GIVEN a mock API function that returns dict without results key
        mock_get_entities.return_value = {}

        # WHEN I call md5_query_async
        result = await md5_query_async("abc123", synapse_client=None)

        # THEN I expect an empty list
        assert isinstance(result, list)
        assert len(result) == 0

    @patch("synapseclient.operations.utility_operations.wrap_async_to_sync")
    def test_md5_query_sync(self, mock_wrap):
        """Test md5_query synchronous wrapper."""
        # GIVEN a mock wrap_async_to_sync
        mock_wrap.return_value = [{"id": "syn123456"}]

        # WHEN I call md5_query
        result = md5_query("abc123", synapse_client=None)

        # THEN I expect wrap_async_to_sync to be called
        assert result == [{"id": "syn123456"}]
        mock_wrap.assert_called_once()
