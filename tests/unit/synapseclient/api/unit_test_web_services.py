"""Unit tests for web_services functions."""
from unittest.mock import AsyncMock, MagicMock, patch

import synapseclient.api.web_services as web_services


class TestOpenEntityInBrowser:
    """Tests for open_entity_in_browser function."""

    @patch("synapseclient.api.web_services.webbrowser")
    @patch("synapseclient.Synapse")
    async def test_open_entity_in_browser_with_synapse_id(
        self, mock_synapse, mock_webbrowser
    ):
        """Test opening entity in browser with Synapse ID."""
        # GIVEN a mock client with portal endpoint
        mock_client = AsyncMock()
        mock_client.portalEndpoint = "https://www.synapse.org"
        mock_synapse.get_client.return_value = mock_client
        mock_webbrowser.open = MagicMock()

        # WHEN I call open_entity_in_browser with a synapse ID
        result = await web_services.open_entity_in_browser(
            "syn123456", synapse_client=None
        )

        # THEN I expect the correct URL to be opened
        assert result == "https://www.synapse.org#!Synapse:syn123456"
        mock_webbrowser.open.assert_called_once_with(
            "https://www.synapse.org#!Synapse:syn123456"
        )

    @patch("synapseclient.api.web_services.webbrowser")
    @patch("synapseclient.Synapse")
    async def test_open_entity_in_browser_with_object(
        self, mock_synapse, mock_webbrowser
    ):
        """Test opening entity in browser with entity object."""
        # GIVEN a mock client and an entity object
        mock_client = AsyncMock()
        mock_client.portalEndpoint = "https://www.synapse.org"
        mock_synapse.get_client.return_value = mock_client
        mock_webbrowser.open = MagicMock()

        # Create a mock entity object with id attribute
        mock_entity = MagicMock()
        mock_entity.id = "syn789012"

        # WHEN I call open_entity_in_browser with an entity object
        result = await web_services.open_entity_in_browser(
            mock_entity, synapse_client=None
        )

        # THEN I expect the correct URL to be opened
        assert result == "https://www.synapse.org#!Synapse:syn789012"
        mock_webbrowser.open.assert_called_once_with(
            "https://www.synapse.org#!Synapse:syn789012"
        )

    @patch("synapseclient.api.web_services.webbrowser")
    @patch("synapseclient.Synapse")
    async def test_open_entity_in_browser_with_subpage(
        self, mock_synapse, mock_webbrowser
    ):
        """Test opening entity wiki subpage in browser."""
        # GIVEN a mock client with portal endpoint
        mock_client = AsyncMock()
        mock_client.portalEndpoint = "https://www.synapse.org"
        mock_synapse.get_client.return_value = mock_client
        mock_webbrowser.open = MagicMock()

        # WHEN I call open_entity_in_browser with subpage_id
        result = await web_services.open_entity_in_browser(
            "syn123456", subpage_id="12345", synapse_client=None
        )

        # THEN I expect the correct wiki URL to be opened
        expected_url = "https://www.synapse.org#!Wiki:syn123456/ENTITY/12345"
        assert result == expected_url
        mock_webbrowser.open.assert_called_once_with(expected_url)

    @patch("synapseclient.api.web_services.os.path.isfile")
    @patch("synapseclient.api.web_services.get_entity")
    @patch("synapseclient.api.web_services.webbrowser")
    @patch("synapseclient.Synapse")
    async def test_open_entity_in_browser_with_file_path(
        self, mock_synapse, mock_webbrowser, mock_get_entity, mock_isfile
    ):
        """Test opening entity in browser when entity is a file path."""
        # GIVEN a mock client and a file path that exists
        mock_client = AsyncMock()
        mock_client.portalEndpoint = "https://www.synapse.org"
        mock_synapse.get_client.return_value = mock_client
        mock_webbrowser.open = MagicMock()
        mock_isfile.return_value = True
        mock_get_entity.return_value = {"id": "syn123456"}

        # WHEN I call open_entity_in_browser with a file path
        result = await web_services.open_entity_in_browser(
            "/path/to/file.txt", synapse_client=None
        )

        # THEN I expect the entity to be fetched and correct URL opened
        assert result == "https://www.synapse.org#!Synapse:syn123456"
        mock_get_entity.assert_awaited_once()
        mock_webbrowser.open.assert_called_once_with(
            "https://www.synapse.org#!Synapse:syn123456"
        )

    @patch("synapseclient.api.web_services.webbrowser")
    @patch("synapseclient.Synapse")
    async def test_open_entity_in_browser_different_portal(
        self, mock_synapse, mock_webbrowser
    ):
        """Test opening entity with different portal endpoint."""
        # GIVEN a mock client with custom portal endpoint
        mock_client = AsyncMock()
        mock_client.portalEndpoint = "https://staging.synapse.org"
        mock_synapse.get_client.return_value = mock_client
        mock_webbrowser.open = MagicMock()

        # WHEN I call open_entity_in_browser
        result = await web_services.open_entity_in_browser(
            "syn123456", synapse_client=None
        )

        # THEN I expect the staging URL to be used
        assert result == "https://staging.synapse.org#!Synapse:syn123456"
        mock_webbrowser.open.assert_called_once_with(
            "https://staging.synapse.org#!Synapse:syn123456"
        )
