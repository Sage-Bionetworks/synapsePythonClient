"""Unit tests for factory_operations (get/get_async) routing logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.operations.factory_operations import (
    ActivityOptions,
    FileOptions,
    LinkOptions,
    TableOptions,
    get_async,
)

# Patch paths for locally-imported functions inside get_async
_PATCH_GET_ENTITY_TYPE = "synapseclient.api.entity_services.get_entity_type"
_PATCH_GET_CHILD = "synapseclient.api.entity_services.get_child"
_PATCH_GET_BUNDLE = "synapseclient.api.entity_bundle_services_v2.get_entity_id_bundle2"
_PATCH_GET_VERSION_BUNDLE = (
    "synapseclient.api.entity_bundle_services_v2.get_entity_id_version_bundle2"
)


class TestGetAsyncInputValidation:
    """Tests for input parameter validation in get_async."""

    async def test_both_synapse_id_and_entity_name_raises_value_error(self):
        """Test that providing both synapse_id and entity_name raises ValueError."""
        # WHEN/THEN get_async raises ValueError
        with pytest.raises(
            ValueError, match="Cannot specify both synapse_id and entity_name"
        ):
            await get_async(
                synapse_id="syn123456",
                entity_name="my_file",
                synapse_client=MagicMock(),
            )

    async def test_neither_synapse_id_nor_entity_name_raises_value_error(self):
        """Test that providing neither synapse_id nor entity_name raises ValueError."""
        # WHEN/THEN get_async raises ValueError
        with pytest.raises(
            ValueError, match="Must specify either synapse_id or entity_name"
        ):
            await get_async(synapse_client=MagicMock())


class TestGetAsyncByEntityName:
    """Tests for entity name-based lookup in get_async."""

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    @patch(_PATCH_GET_CHILD, new_callable=AsyncMock)
    async def test_get_by_entity_name_and_parent_id(
        self, mock_get_child, mock_get_entity_type
    ):
        """Test get_async by entity_name with parent_id resolves the name first."""
        # GIVEN get_child returns a synapse ID
        mock_get_child.return_value = "syn123456"

        # And get_entity_type returns a Project header
        mock_header = MagicMock()
        mock_header.type = concrete_types.PROJECT_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Project

        with patch.object(Project, "get_async", new_callable=AsyncMock) as mock_get:
            mock_project = Project(id="syn123456", name="My Project")
            mock_get.return_value = mock_project

            # WHEN I call get_async by entity_name
            result = await get_async(
                entity_name="My Project",
                parent_id=None,
                synapse_client=MagicMock(),
            )

            # THEN get_child is called to resolve the name
            mock_get_child.assert_awaited_once()
            call_kwargs = mock_get_child.call_args[1]
            assert call_kwargs["entity_name"] == "My Project"
            assert call_kwargs["parent_id"] is None
            # And the entity is retrieved
            mock_get.assert_awaited_once()
            assert result is mock_project

    @patch(_PATCH_GET_CHILD, new_callable=AsyncMock)
    async def test_get_by_entity_name_not_found_with_parent(self, mock_get_child):
        """Test that entity_name not found with parent_id raises SynapseNotFoundError."""
        # GIVEN get_child returns None
        mock_get_child.return_value = None

        # WHEN/THEN get_async raises SynapseNotFoundError
        with pytest.raises(SynapseNotFoundError, match="not found in parent"):
            await get_async(
                entity_name="nonexistent",
                parent_id="syn789",
                synapse_client=MagicMock(),
            )

    @patch(_PATCH_GET_CHILD, new_callable=AsyncMock)
    async def test_get_by_entity_name_project_not_found(self, mock_get_child):
        """Test that project name not found raises SynapseNotFoundError."""
        # GIVEN get_child returns None
        mock_get_child.return_value = None

        # WHEN/THEN get_async raises SynapseNotFoundError with project message
        with pytest.raises(SynapseNotFoundError, match="Project with name"):
            await get_async(
                entity_name="Nonexistent Project",
                parent_id=None,
                synapse_client=MagicMock(),
            )


class TestGetAsyncFileEntityRoute:
    """Tests for File entity routing in get_async."""

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_file_entity_default_options(self, mock_get_entity_type):
        """Test that File entity is retrieved with default options."""
        # GIVEN get_entity_type returns FILE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.FILE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import File

        with patch.object(File, "get_async", new_callable=AsyncMock) as mock_get:
            mock_file = File(id="syn123456")
            mock_get.return_value = mock_file

            # WHEN I call get_async for a file
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN File.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_file

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_file_entity_with_file_options(self, mock_get_entity_type):
        """Test that FileOptions are applied when retrieving a File."""
        # GIVEN get_entity_type returns FILE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.FILE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import File

        with patch.object(File, "get_async", new_callable=AsyncMock) as mock_get:
            mock_file = File(id="syn123456")
            mock_get.return_value = mock_file

            file_options = FileOptions(
                download_file=False,
                download_location="/tmp/downloads",
                if_collision="overwrite.local",
            )

            # WHEN I call get_async with file options
            result = await get_async(
                synapse_id="syn123456",
                file_options=file_options,
                synapse_client=MagicMock(),
            )

            # THEN File is created with the file options applied
            mock_get.assert_awaited_once()
            assert result is mock_file

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_file_entity_with_version_number(self, mock_get_entity_type):
        """Test that version_number is passed through to File entity."""
        # GIVEN get_entity_type returns FILE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.FILE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import File

        with patch.object(File, "get_async", new_callable=AsyncMock) as mock_get:
            mock_file = File(id="syn123456", version_number=3)
            mock_get.return_value = mock_file

            # WHEN I call get_async with a version number
            result = await get_async(
                synapse_id="syn123456",
                version_number=3,
                synapse_client=MagicMock(),
            )

            # THEN File.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_file

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_file_entity_with_activity_options(self, mock_get_entity_type):
        """Test that ActivityOptions are passed through for File retrieval."""
        # GIVEN get_entity_type returns FILE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.FILE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import File

        with patch.object(File, "get_async", new_callable=AsyncMock) as mock_get:
            mock_file = File(id="syn123456")
            mock_get.return_value = mock_file

            activity_options = ActivityOptions(include_activity=True)

            # WHEN I call get_async with activity options
            result = await get_async(
                synapse_id="syn123456",
                activity_options=activity_options,
                synapse_client=MagicMock(),
            )

            # THEN File.get_async is called with include_activity=True
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs.get("include_activity") is True
            assert result is mock_file


class TestGetAsyncLinkEntityRoute:
    """Tests for Link entity routing in get_async."""

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_link_entity_follow_link_true(self, mock_get_entity_type):
        """Test that Link with follow_link=True follows the link."""
        # GIVEN get_entity_type returns LINK_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.LINK_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Link

        with patch.object(Link, "get_async", new_callable=AsyncMock) as mock_get:
            mock_target = MagicMock()
            mock_get.return_value = mock_target

            link_options = LinkOptions(follow_link=True)

            # WHEN I call get_async with follow_link=True
            result = await get_async(
                synapse_id="syn123456",
                link_options=link_options,
                synapse_client=MagicMock(),
            )

            # THEN Link.get_async is called with follow_link=True
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["follow_link"] is True
            assert result is mock_target

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_link_entity_follow_link_false(self, mock_get_entity_type):
        """Test that Link with follow_link=False returns the Link itself."""
        # GIVEN get_entity_type returns LINK_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.LINK_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Link

        with patch.object(Link, "get_async", new_callable=AsyncMock) as mock_get:
            mock_link = Link(id="syn123456")
            mock_get.return_value = mock_link

            link_options = LinkOptions(follow_link=False)

            # WHEN I call get_async with follow_link=False
            result = await get_async(
                synapse_id="syn123456",
                link_options=link_options,
                synapse_client=MagicMock(),
            )

            # THEN Link.get_async is called with follow_link=False
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["follow_link"] is False
            assert result is mock_link

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_link_entity_default_follows_link(self, mock_get_entity_type):
        """Test that Link with default options follows the link (follow_link=True)."""
        # GIVEN get_entity_type returns LINK_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.LINK_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Link

        with patch.object(Link, "get_async", new_callable=AsyncMock) as mock_get:
            mock_target = MagicMock()
            mock_get.return_value = mock_target

            # WHEN I call get_async with default link options
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN Link.get_async is called with follow_link=True (default)
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["follow_link"] is True
            assert result is mock_target


class TestGetAsyncTableEntityRoute:
    """Tests for table-like entity routing in get_async."""

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_table_entity_with_table_options(self, mock_get_entity_type):
        """Test that Table entity is retrieved with table options."""
        # GIVEN get_entity_type returns TABLE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.TABLE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Table

        with patch.object(Table, "get_async", new_callable=AsyncMock) as mock_get:
            mock_table = Table(id="syn123456")
            mock_get.return_value = mock_table

            table_options = TableOptions(include_columns=True)

            # WHEN I call get_async with table options
            result = await get_async(
                synapse_id="syn123456",
                table_options=table_options,
                synapse_client=MagicMock(),
            )

            # THEN Table.get_async is called with include_columns=True
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["include_columns"] is True
            assert result is mock_table

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_table_entity_without_columns(self, mock_get_entity_type):
        """Test that Table entity can be retrieved without columns."""
        # GIVEN get_entity_type returns TABLE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.TABLE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Table

        with patch.object(Table, "get_async", new_callable=AsyncMock) as mock_get:
            mock_table = Table(id="syn123456")
            mock_get.return_value = mock_table

            table_options = TableOptions(include_columns=False)

            # WHEN I call get_async with include_columns=False
            result = await get_async(
                synapse_id="syn123456",
                table_options=table_options,
                synapse_client=MagicMock(),
            )

            # THEN Table.get_async is called with include_columns=False
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["include_columns"] is False
            assert result is mock_table

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_table_with_version_number(self, mock_get_entity_type):
        """Test that version_number is passed through for table entity."""
        # GIVEN get_entity_type returns TABLE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.TABLE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Table

        with patch.object(Table, "get_async", new_callable=AsyncMock) as mock_get:
            mock_table = Table(id="syn123456", version_number=5)
            mock_get.return_value = mock_table

            # WHEN I call get_async with version_number
            result = await get_async(
                synapse_id="syn123456",
                version_number=5,
                synapse_client=MagicMock(),
            )

            # THEN Table.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_table

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_dataset_routes_to_table_handler(self, mock_get_entity_type):
        """Test that Dataset routes to table-like handler."""
        # GIVEN get_entity_type returns DATASET_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.DATASET_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Dataset

        with patch.object(Dataset, "get_async", new_callable=AsyncMock) as mock_get:
            mock_dataset = Dataset(id="syn123456")
            mock_get.return_value = mock_dataset

            # WHEN I call get_async
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN Dataset.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_dataset

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_entityview_routes_to_table_handler(self, mock_get_entity_type):
        """Test that EntityView routes to table-like handler."""
        # GIVEN get_entity_type returns ENTITY_VIEW
        mock_header = MagicMock()
        mock_header.type = concrete_types.ENTITY_VIEW
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import EntityView

        with patch.object(EntityView, "get_async", new_callable=AsyncMock) as mock_get:
            mock_view = EntityView(id="syn123456")
            mock_get.return_value = mock_view

            # WHEN I call get_async
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN EntityView.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_view

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_materialized_view_routes_to_table_handler(
        self, mock_get_entity_type
    ):
        """Test that MaterializedView routes to table-like handler."""
        # GIVEN get_entity_type returns MATERIALIZED_VIEW
        mock_header = MagicMock()
        mock_header.type = concrete_types.MATERIALIZED_VIEW
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import MaterializedView

        with patch.object(
            MaterializedView, "get_async", new_callable=AsyncMock
        ) as mock_get:
            mock_mv = MaterializedView(id="syn123456")
            mock_get.return_value = mock_mv

            # WHEN I call get_async
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN MaterializedView.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_mv

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_virtual_table_routes_to_table_handler(
        self, mock_get_entity_type
    ):
        """Test that VirtualTable routes to table-like handler."""
        # GIVEN get_entity_type returns VIRTUAL_TABLE
        mock_header = MagicMock()
        mock_header.type = concrete_types.VIRTUAL_TABLE
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import VirtualTable

        with patch.object(
            VirtualTable, "get_async", new_callable=AsyncMock
        ) as mock_get:
            mock_vt = VirtualTable(id="syn123456")
            mock_get.return_value = mock_vt

            # WHEN I call get_async
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN VirtualTable.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_vt

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_table_with_activity_options(self, mock_get_entity_type):
        """Test that ActivityOptions are passed through for table retrieval."""
        # GIVEN get_entity_type returns TABLE_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.TABLE_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Table

        with patch.object(Table, "get_async", new_callable=AsyncMock) as mock_get:
            mock_table = Table(id="syn123456")
            mock_get.return_value = mock_table

            activity_options = ActivityOptions(include_activity=True)

            # WHEN I call get_async with activity options
            result = await get_async(
                synapse_id="syn123456",
                activity_options=activity_options,
                synapse_client=MagicMock(),
            )

            # THEN Table.get_async is called with include_activity=True
            mock_get.assert_awaited_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs.get("include_activity") is True
            assert result is mock_table


class TestGetAsyncSimpleEntityRoute:
    """Tests for simple entity (Project, Folder) routing in get_async."""

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_project_entity(self, mock_get_entity_type):
        """Test that Project entity is retrieved correctly."""
        # GIVEN get_entity_type returns PROJECT_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.PROJECT_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Project

        with patch.object(Project, "get_async", new_callable=AsyncMock) as mock_get:
            mock_project = Project(id="syn123456")
            mock_get.return_value = mock_project

            # WHEN I call get_async
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN Project.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_project

    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_folder_entity(self, mock_get_entity_type):
        """Test that Folder entity is retrieved correctly."""
        # GIVEN get_entity_type returns FOLDER_ENTITY
        mock_header = MagicMock()
        mock_header.type = concrete_types.FOLDER_ENTITY
        mock_get_entity_type.return_value = mock_header

        from synapseclient.models import Folder

        with patch.object(Folder, "get_async", new_callable=AsyncMock) as mock_get:
            mock_folder = Folder(id="syn123456")
            mock_get.return_value = mock_folder

            # WHEN I call get_async
            result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

            # THEN Folder.get_async is called
            mock_get.assert_awaited_once()
            assert result is mock_folder


class TestGetAsyncEntityInstance:
    """Tests for passing entity instances directly to get_async."""

    async def test_get_with_file_entity_instance(self):
        """Test that passing a File instance calls _handle_entity_instance."""
        # GIVEN a mock File entity instance
        from synapseclient.models import File

        mock_file = File(id="syn123456")
        mock_file.get_async = AsyncMock(return_value=mock_file)

        # WHEN I call get_async with the entity instance
        result = await get_async(synapse_id=mock_file, synapse_client=MagicMock())

        # THEN the entity's get_async is called
        mock_file.get_async.assert_awaited_once()
        assert result is mock_file

    async def test_get_with_file_entity_instance_applies_file_options(self):
        """Test that FileOptions are applied when passing a File instance."""
        # GIVEN a mock File entity instance
        from synapseclient.models import File

        mock_file = File(id="syn123456")
        mock_file.get_async = AsyncMock(return_value=mock_file)

        file_options = FileOptions(
            download_file=False,
            download_location="/tmp/downloads",
            if_collision="overwrite.local",
        )

        # WHEN I call get_async with file options
        result = await get_async(
            synapse_id=mock_file,
            file_options=file_options,
            synapse_client=MagicMock(),
        )

        # THEN the file options are applied to the entity
        assert mock_file.download_file is False
        assert mock_file.path == "/tmp/downloads"
        assert mock_file.if_collision == "overwrite.local"
        mock_file.get_async.assert_awaited_once()
        assert result is mock_file

    async def test_get_with_entity_instance_applies_version_number(self):
        """Test that version_number is set on the entity instance."""
        # GIVEN a mock File entity instance
        from synapseclient.models import File

        mock_file = File(id="syn123456")
        mock_file.get_async = AsyncMock(return_value=mock_file)

        # WHEN I call get_async with version_number
        result = await get_async(
            synapse_id=mock_file,
            version_number=5,
            synapse_client=MagicMock(),
        )

        # THEN the version_number is set on the entity
        assert mock_file.version_number == 5
        mock_file.get_async.assert_awaited_once()
        assert result is mock_file

    async def test_get_with_link_entity_instance(self):
        """Test that passing a Link instance applies link options."""
        # GIVEN a mock Link entity instance
        from synapseclient.models import Link

        mock_link = Link(id="syn123456")
        mock_link.get_async = AsyncMock(return_value=mock_link)

        link_options = LinkOptions(follow_link=False)

        # WHEN I call get_async with link options
        result = await get_async(
            synapse_id=mock_link,
            link_options=link_options,
            synapse_client=MagicMock(),
        )

        # THEN get_async is called with follow_link=False
        mock_link.get_async.assert_awaited_once()
        call_kwargs = mock_link.get_async.call_args[1]
        assert call_kwargs["follow_link"] is False
        assert result is mock_link

    async def test_get_with_table_entity_instance_applies_table_options(self):
        """Test that TableOptions are applied when passing a Table instance."""
        # GIVEN a mock Table entity instance
        from synapseclient.models import Table

        mock_table = Table(id="syn123456")
        mock_table.get_async = AsyncMock(return_value=mock_table)

        table_options = TableOptions(include_columns=False)

        # WHEN I call get_async with table options
        result = await get_async(
            synapse_id=mock_table,
            table_options=table_options,
            synapse_client=MagicMock(),
        )

        # THEN get_async is called with include_columns=False
        mock_table.get_async.assert_awaited_once()
        call_kwargs = mock_table.get_async.call_args[1]
        assert call_kwargs["include_columns"] is False
        assert result is mock_table


class TestGetAsyncUnknownEntityType:
    """Tests for unknown/fallback entity type handling."""

    @patch("synapseclient.Synapse.get_client")
    @patch(_PATCH_GET_BUNDLE, new_callable=AsyncMock)
    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_unknown_entity_type_falls_back_to_bundle(
        self, mock_get_entity_type, mock_get_bundle, mock_get_client
    ):
        """Test that unknown entity type falls back to entity bundle."""
        # GIVEN get_entity_type returns an unknown type
        mock_header = MagicMock()
        mock_header.type = "org.sagebionetworks.repo.model.UnknownEntity"
        mock_get_entity_type.return_value = mock_header

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_bundle = {"entity": {"id": "syn123456"}}
        mock_get_bundle.return_value = mock_bundle

        # WHEN I call get_async
        result = await get_async(synapse_id="syn123456", synapse_client=MagicMock())

        # THEN a warning is logged and the bundle is returned
        mock_client.logger.warning.assert_called_once()
        mock_get_bundle.assert_awaited_once()
        assert result == mock_bundle

    @patch("synapseclient.Synapse.get_client")
    @patch(_PATCH_GET_VERSION_BUNDLE, new_callable=AsyncMock)
    @patch(_PATCH_GET_ENTITY_TYPE, new_callable=AsyncMock)
    async def test_get_unknown_entity_type_with_version_uses_version_bundle(
        self, mock_get_entity_type, mock_get_version_bundle, mock_get_client
    ):
        """Test that unknown entity type with version uses versioned bundle API."""
        # GIVEN get_entity_type returns an unknown type
        mock_header = MagicMock()
        mock_header.type = "org.sagebionetworks.repo.model.UnknownEntity"
        mock_get_entity_type.return_value = mock_header

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_bundle = {"entity": {"id": "syn123456", "version": 3}}
        mock_get_version_bundle.return_value = mock_bundle

        # WHEN I call get_async with version_number
        result = await get_async(
            synapse_id="syn123456",
            version_number=3,
            synapse_client=MagicMock(),
        )

        # THEN the versioned bundle API is called
        mock_get_version_bundle.assert_awaited_once()
        call_kwargs = mock_get_version_bundle.call_args[1]
        assert call_kwargs["entity_id"] == "syn123456"
        assert call_kwargs["version"] == 3
        assert result == mock_bundle


class TestGetSyncWrapper:
    """Tests for the synchronous get() wrapper."""

    @patch("synapseclient.operations.factory_operations.wrap_async_to_sync")
    def test_get_sync_calls_wrap_async_to_sync(self, mock_wrap):
        """Test that the synchronous get() calls wrap_async_to_sync."""
        from synapseclient.operations.factory_operations import get

        # GIVEN a mock wrap_async_to_sync
        mock_entity = MagicMock()
        mock_wrap.return_value = mock_entity

        # WHEN I call the sync get
        result = get(synapse_id="syn123456", synapse_client=MagicMock())

        # THEN wrap_async_to_sync is called
        assert result is mock_entity
        mock_wrap.assert_called_once()
