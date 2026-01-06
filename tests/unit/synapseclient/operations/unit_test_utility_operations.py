"""Unit tests for utility_operations wrapper functions."""
import json
from unittest.mock import AsyncMock, MagicMock, patch

from synapseclient.operations import (
    find_entity_id,
    find_entity_id_async,
    is_synapse_id,
    is_synapse_id_async,
    md5_query,
    md5_query_async,
    onweb,
    onweb_async,
    print_entity,
    print_entity_async,
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
        mock_open_entity.return_value = "https://www.synapse.org/Synapse:syn123456"

        # WHEN I call onweb_async
        result = await onweb_async("syn123456", synapse_client=None)

        # THEN I expect the URL to be returned
        assert result == "https://www.synapse.org/Synapse:syn123456"
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
            "https://www.synapse.org/Wiki:syn123456/ENTITY/12345"
        )

        # WHEN I call onweb_async with subpage_id
        result = await onweb_async("syn123456", subpage_id="12345", synapse_client=None)

        # THEN I expect the wiki URL to be returned
        assert result == "https://www.synapse.org/Wiki:syn123456/ENTITY/12345"
        mock_open_entity.assert_awaited_once_with(
            entity="syn123456", subpage_id="12345", synapse_client=None
        )

    @patch("synapseclient.operations.utility_operations.wrap_async_to_sync")
    def test_onweb_sync(self, mock_wrap):
        """Test onweb synchronous wrapper."""
        # GIVEN a mock wrap_async_to_sync
        mock_wrap.return_value = "https://www.synapse.org/Synapse:syn123456"

        # WHEN I call onweb
        result = onweb("syn123456", synapse_client=None)

        # THEN I expect wrap_async_to_sync to be called
        assert result == "https://www.synapse.org/Synapse:syn123456"
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


class TestPrintEntity:
    """Tests for print_entity wrapper functions."""

    @patch(
        "synapseclient.operations.factory_operations.get_async", new_callable=AsyncMock
    )
    @patch("synapseclient.operations.utility_operations.logging.getLogger")
    @patch("synapseclient.Synapse.get_client")
    async def test_print_entity_async_with_synapse_id(
        self, mock_get_client, mock_get_logger, mock_get_async
    ):
        """Test print_entity_async with a Synapse ID string."""
        # GIVEN a mock Synapse client and entity
        mock_syn = MagicMock()
        mock_logger = MagicMock()
        mock_logger.name = "synapse"
        mock_syn.logger = mock_logger
        mock_get_client.return_value = mock_syn
        mock_get_logger.return_value = mock_logger

        # Create a mock dataclass entity with fields
        mock_entity = MagicMock()
        mock_entity.__dataclass_fields__ = {}  # Mark as dataclass
        mock_entity.id = "syn123456"
        mock_entity.name = "test_file.txt"
        mock_entity.concreteType = "org.sagebionetworks.repo.model.FileEntity"

        # Mock dataclasses.fields to return mock field objects with repr=True
        with patch("dataclasses.fields") as mock_fields:
            mock_field_id = MagicMock()
            mock_field_id.name = "id"
            mock_field_id.repr = True
            mock_field_name = MagicMock()
            mock_field_name.name = "name"
            mock_field_name.repr = True
            mock_field_concrete = MagicMock()
            mock_field_concrete.name = "concreteType"
            mock_field_concrete.repr = True

            mock_fields.return_value = [
                mock_field_id,
                mock_field_name,
                mock_field_concrete,
            ]
            mock_get_async.return_value = mock_entity

            # WHEN I call print_entity_async with a Synapse ID
            await print_entity_async("syn123456", synapse_client=None)

            # THEN I expect the entity to be fetched and logged as JSON
            mock_get_async.assert_awaited_once()
            mock_logger.info.assert_called_once()
            logged_output = mock_logger.info.call_args[0][0]
            assert "syn123456" in logged_output
            assert "test_file.txt" in logged_output

    @patch("synapseclient.operations.utility_operations.logging.getLogger")
    @patch("synapseclient.Synapse.get_client")
    async def test_print_entity_async_with_dict(self, mock_get_client, mock_get_logger):
        """Test print_entity_async with a dictionary entity."""
        # GIVEN a mock Synapse client and entity dict
        mock_syn = MagicMock()
        mock_logger = MagicMock()
        mock_logger.name = "synapse"
        mock_syn.logger = mock_logger
        mock_get_client.return_value = mock_syn
        mock_get_logger.return_value = mock_logger

        entity_dict = {
            "id": "syn789012",
            "name": "test_project",
            "concreteType": "org.sagebionetworks.repo.model.Project",
        }

        # WHEN I call print_entity_async with a dict
        await print_entity_async(entity_dict, synapse_client=None)

        # THEN I expect the dict to be logged as JSON
        mock_logger.info.assert_called_once()
        logged_output = mock_logger.info.call_args[0][0]
        assert "syn789012" in logged_output
        assert "test_project" in logged_output
        # Verify it's valid JSON
        parsed = json.loads(logged_output)
        assert parsed["id"] == "syn789012"

    @patch("synapseclient.operations.utility_operations.logging.getLogger")
    @patch("synapseclient.Synapse.get_client")
    async def test_print_entity_async_ensure_ascii(
        self, mock_get_client, mock_get_logger
    ):
        """Test print_entity_async with ensure_ascii parameter."""
        # GIVEN a mock Synapse client and entity with unicode characters
        mock_syn = MagicMock()
        mock_logger = MagicMock()
        mock_logger.name = "synapse"
        mock_syn.logger = mock_logger
        mock_get_client.return_value = mock_syn
        mock_get_logger.return_value = mock_logger

        entity_dict = {"id": "syn123", "name": "test_file_café"}

        # WHEN I call print_entity_async with ensure_ascii=True
        await print_entity_async(entity_dict, ensure_ascii=True, synapse_client=None)

        # THEN I expect unicode to be escaped
        mock_logger.info.assert_called_once()
        logged_output = mock_logger.info.call_args[0][0]
        assert "\\u" in logged_output or "café" not in logged_output

    @patch("synapseclient.operations.utility_operations.logging.getLogger")
    @patch("synapseclient.Synapse.get_client")
    async def test_print_entity_async_non_serializable(
        self, mock_get_client, mock_get_logger
    ):
        """Test print_entity_async with non-JSON-serializable entity."""
        # GIVEN a mock Synapse client and non-serializable entity
        mock_syn = MagicMock()
        mock_logger = MagicMock()
        mock_logger.name = "synapse"
        mock_syn.logger = mock_logger
        mock_get_client.return_value = mock_syn
        mock_get_logger.return_value = mock_logger

        # Create a mock object that can't be JSON serialized
        mock_entity = MagicMock()
        mock_entity.__str__ = lambda self: "Mock Entity String"

        # WHEN I call print_entity_async with non-serializable object
        await print_entity_async(mock_entity, synapse_client=None)

        # THEN I expect str() to be used instead
        mock_logger.info.assert_called_once()
        logged_output = mock_logger.info.call_args[0][0]
        assert "Mock Entity String" in logged_output

    @patch(
        "synapseclient.operations.factory_operations.get_async", new_callable=AsyncMock
    )
    @patch("synapseclient.operations.utility_operations.logging.getLogger")
    @patch("synapseclient.Synapse.get_client")
    async def test_print_entity_async_with_version(
        self, mock_get_client, mock_get_logger, mock_get_async
    ):
        """Test print_entity_async with a versioned Synapse ID."""
        # GIVEN a mock Synapse client and versioned entity
        mock_syn = MagicMock()
        mock_logger = MagicMock()
        mock_logger.name = "synapse"
        mock_syn.logger = mock_logger
        mock_get_client.return_value = mock_syn
        mock_get_logger.return_value = mock_logger

        # Create a mock dataclass entity with fields
        mock_entity = MagicMock()
        mock_entity.__dataclass_fields__ = {}  # Mark as dataclass
        mock_entity.id = "syn123456"
        mock_entity.versionNumber = 2

        # Mock dataclasses.fields to return mock field objects with repr=True
        with patch("dataclasses.fields") as mock_fields:
            mock_field_id = MagicMock()
            mock_field_id.name = "id"
            mock_field_id.repr = True
            mock_field_version = MagicMock()
            mock_field_version.name = "versionNumber"
            mock_field_version.repr = True

            mock_fields.return_value = [mock_field_id, mock_field_version]
            mock_get_async.return_value = mock_entity

            # WHEN I call print_entity_async with a versioned ID
            await print_entity_async("syn123456.2", synapse_client=None)

            # THEN I expect the entity to be fetched with version
            mock_get_async.assert_awaited_once()
            call_kwargs = mock_get_async.call_args[1]
            assert call_kwargs["synapse_id"] == "syn123456.2"

    @patch("synapseclient.operations.utility_operations.wrap_async_to_sync")
    def test_print_entity_sync(self, mock_wrap):
        """Test print_entity synchronous wrapper."""
        # GIVEN a mock wrap_async_to_sync
        mock_wrap.return_value = None

        # WHEN I call print_entity
        result = print_entity("syn123456", ensure_ascii=True, synapse_client=None)

        # THEN I expect wrap_async_to_sync to be called
        assert result is None
        mock_wrap.assert_called_once()
