"""Unit tests for delete_operations routing logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient.operations.delete_operations import delete_async


class TestDeleteStringIdRoute:
    """Tests for string ID deletion routing in delete_async."""

    @patch(
        "synapseclient.operations.delete_operations.delete_entity",
        new_callable=AsyncMock,
    )
    @patch("synapseclient.operations.delete_operations.is_synapse_id_str")
    async def test_delete_string_id(self, mock_is_synapse_id_str, mock_delete_entity):
        """Test that a plain string Synapse ID is deleted via delete_entity."""
        # GIVEN a valid string synapse ID
        mock_is_synapse_id_str.return_value = "syn123456"

        # WHEN I call delete_async with a string ID
        result = await delete_async(entity="syn123456", synapse_client=MagicMock())

        # THEN delete_entity is called with the ID and no version
        mock_delete_entity.assert_awaited_once()
        call_kwargs = mock_delete_entity.call_args[1]
        assert call_kwargs["entity_id"] == "syn123456"
        assert call_kwargs["version_number"] is None
        assert result is None

    @patch(
        "synapseclient.operations.delete_operations.delete_entity",
        new_callable=AsyncMock,
    )
    @patch("synapseclient.operations.delete_operations.is_synapse_id_str")
    async def test_delete_string_id_with_embedded_version(
        self, mock_is_synapse_id_str, mock_delete_entity
    ):
        """Test that a string ID with embedded version (syn123.4) requires version_only=True."""
        # GIVEN a string synapse ID with embedded version
        mock_is_synapse_id_str.return_value = "syn123.4"

        # WHEN/THEN delete_async raises ValueError without version_only=True
        with pytest.raises(ValueError, match="version_only=True"):
            await delete_async(entity="syn123.4", synapse_client=MagicMock())

    @patch(
        "synapseclient.operations.delete_operations.delete_entity",
        new_callable=AsyncMock,
    )
    @patch("synapseclient.operations.delete_operations.is_synapse_id_str")
    async def test_delete_string_id_with_embedded_version_and_version_only(
        self, mock_is_synapse_id_str, mock_delete_entity
    ):
        """Test that a string ID with embedded version deletes when version_only=True."""
        # GIVEN a string synapse ID with embedded version
        mock_is_synapse_id_str.return_value = "syn123.4"

        # WHEN I call delete_async with version_only=True
        result = await delete_async(
            entity="syn123.4", version_only=True, synapse_client=MagicMock()
        )

        # THEN delete_entity is called with the parsed ID and version
        mock_delete_entity.assert_awaited_once()
        call_kwargs = mock_delete_entity.call_args[1]
        assert call_kwargs["entity_id"] == "syn123"
        assert call_kwargs["version_number"] == 4
        assert result is None

    @patch(
        "synapseclient.operations.delete_operations.delete_entity",
        new_callable=AsyncMock,
    )
    @patch("synapseclient.operations.delete_operations.is_synapse_id_str")
    async def test_delete_string_id_with_explicit_version_overrides(
        self, mock_is_synapse_id_str, mock_delete_entity
    ):
        """Test that explicit version parameter overrides embedded version."""
        # GIVEN a string synapse ID with embedded version
        mock_is_synapse_id_str.return_value = "syn123.4"

        # WHEN I call delete_async with version=7 and version_only=True
        result = await delete_async(
            entity="syn123.4",
            version=7,
            version_only=True,
            synapse_client=MagicMock(),
        )

        # THEN delete_entity is called with version=7 (not 4)
        mock_delete_entity.assert_awaited_once()
        call_kwargs = mock_delete_entity.call_args[1]
        assert call_kwargs["entity_id"] == "syn123"
        assert call_kwargs["version_number"] == 7
        assert result is None

    async def test_delete_invalid_string_raises_value_error(self):
        """Test that an invalid string raises ValueError."""
        # GIVEN an invalid string
        # WHEN/THEN delete_async raises ValueError
        with pytest.raises(ValueError, match="Invalid Synapse ID"):
            await delete_async(entity="not_a_synapse_id", synapse_client=MagicMock())


class TestDeleteFileEntityRoute:
    """Tests for File entity deletion routing in delete_async."""

    async def test_delete_file_entity(self):
        """Test that a File entity is deleted via delete_async."""
        # GIVEN a mock File entity
        from synapseclient.models import File

        mock_file = File(id="syn123456")
        mock_file.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_file, synapse_client=MagicMock())

        # THEN delete_async is called with version_only=False
        mock_file.delete_async.assert_awaited_once()
        call_kwargs = mock_file.delete_async.call_args[1]
        assert call_kwargs["version_only"] is False
        assert result is None

    async def test_delete_file_entity_version_only_true(self):
        """Test that File version-specific deletion works with version_only=True."""
        # GIVEN a mock File entity with version_number set
        from synapseclient.models import File

        mock_file = File(id="syn123456", version_number=3)
        mock_file.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async with version_only=True
        result = await delete_async(
            entity=mock_file, version_only=True, synapse_client=MagicMock()
        )

        # THEN delete_async is called with version_only=True
        mock_file.delete_async.assert_awaited_once()
        call_kwargs = mock_file.delete_async.call_args[1]
        assert call_kwargs["version_only"] is True
        assert result is None

    async def test_delete_file_entity_version_only_with_explicit_version(self):
        """Test that explicit version overrides entity version_number."""
        # GIVEN a mock File entity with version_number=3
        from synapseclient.models import File

        mock_file = File(id="syn123456", version_number=3)
        mock_file.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async with version=5, version_only=True
        result = await delete_async(
            entity=mock_file,
            version=5,
            version_only=True,
            synapse_client=MagicMock(),
        )

        # THEN the file's version_number is set to 5 and delete_async is called
        assert mock_file.version_number == 5
        mock_file.delete_async.assert_awaited_once()
        call_kwargs = mock_file.delete_async.call_args[1]
        assert call_kwargs["version_only"] is True
        assert result is None

    async def test_delete_file_entity_version_only_without_version_raises(self):
        """Test that version_only=True without version raises ValueError."""
        # GIVEN a mock File entity without a version number
        from synapseclient.models import File

        mock_file = File(id="syn123456")
        mock_file.delete_async = AsyncMock(return_value=None)

        # WHEN/THEN delete_async raises ValueError
        with pytest.raises(
            ValueError, match="version_only=True requires a version number"
        ):
            await delete_async(
                entity=mock_file, version_only=True, synapse_client=MagicMock()
            )


class TestDeleteTableLikeEntityRoute:
    """Tests for table-like entity deletion routing in delete_async."""

    async def test_delete_table_entity(self):
        """Test that a Table entity is deleted via delete_async."""
        # GIVEN a mock Table entity
        from synapseclient.models import Table

        mock_table = Table(id="syn123456")
        mock_table.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_table, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_table.delete_async.assert_awaited_once()
        assert result is None

    @patch(
        "synapseclient.operations.delete_operations.delete_entity",
        new_callable=AsyncMock,
    )
    async def test_delete_table_version_only(self, mock_delete_entity):
        """Test that Table version-specific deletion uses delete_entity API."""
        # GIVEN a mock Table entity with a version
        from synapseclient.models import Table

        mock_delete_entity.return_value = None
        mock_table = Table(id="syn123456", version_number=2)
        mock_table.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async with version_only=True
        result = await delete_async(
            entity=mock_table, version_only=True, synapse_client=MagicMock()
        )

        # THEN delete_entity API is called (not the entity's delete_async)
        mock_delete_entity.assert_awaited_once()
        call_kwargs = mock_delete_entity.call_args[1]
        assert call_kwargs["entity_id"] == "syn123456"
        assert call_kwargs["version_number"] == 2
        mock_table.delete_async.assert_not_awaited()
        assert result is None

    async def test_delete_table_version_only_without_version_raises(self):
        """Test that Table version_only=True without version raises ValueError."""
        # GIVEN a mock Table entity without a version number
        from synapseclient.models import Table

        mock_table = Table(id="syn123456")
        mock_table.delete_async = AsyncMock(return_value=None)

        # WHEN/THEN delete_async raises ValueError
        with pytest.raises(
            ValueError, match="version_only=True requires a version number"
        ):
            await delete_async(
                entity=mock_table, version_only=True, synapse_client=MagicMock()
            )

    async def test_delete_dataset_entity(self):
        """Test that a Dataset entity routes through table-like deletion."""
        # GIVEN a mock Dataset entity
        from synapseclient.models import Dataset

        mock_dataset = Dataset(id="syn123456")
        mock_dataset.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_dataset, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_dataset.delete_async.assert_awaited_once()
        assert result is None


class TestDeleteNonVersionableEntityRoute:
    """Tests for non-versionable entity deletion (Project, Folder, etc.)."""

    async def test_delete_project_entity(self):
        """Test that a Project entity is deleted normally."""
        # GIVEN a mock Project entity
        from synapseclient.models import Project

        mock_project = Project(id="syn123456")
        mock_project.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_project, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_project.delete_async.assert_awaited_once()
        assert result is None

    async def test_delete_folder_entity(self):
        """Test that a Folder entity is deleted normally."""
        # GIVEN a mock Folder entity
        from synapseclient.models import Folder

        mock_folder = Folder(id="syn123456")
        mock_folder.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_folder, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_folder.delete_async.assert_awaited_once()
        assert result is None

    @patch("synapseclient.Synapse.get_client")
    async def test_delete_project_with_version_only_warns(self, mock_get_client):
        """Test that Project with version_only=True emits a warning."""
        # GIVEN a mock Project entity with version_only=True
        from synapseclient.models import Project

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_project = Project(id="syn123456")
        mock_project.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async with version_only=True
        result = await delete_async(
            entity=mock_project, version_only=True, synapse_client=MagicMock()
        )

        # THEN a warning is emitted and the entity is still deleted
        mock_client.logger.warning.assert_called_once()
        warning_msg = mock_client.logger.warning.call_args[0][0]
        assert "does not support version-specific deletion" in warning_msg
        mock_project.delete_async.assert_awaited_once()
        assert result is None

    async def test_delete_evaluation_entity(self):
        """Test that an Evaluation entity is deleted normally."""
        # GIVEN a mock Evaluation entity
        from synapseclient.models import Evaluation

        mock_eval = Evaluation(id="syn123456")
        mock_eval.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_eval, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_eval.delete_async.assert_awaited_once()
        assert result is None

    async def test_delete_team_entity(self):
        """Test that a Team entity is deleted normally."""
        # GIVEN a mock Team entity
        from synapseclient.models import Team

        mock_team = Team(id=12345, name="Test Team")
        mock_team.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_team, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_team.delete_async.assert_awaited_once()
        assert result is None

    async def test_delete_schema_organization_entity(self):
        """Test that a SchemaOrganization entity is deleted normally."""
        # GIVEN a mock SchemaOrganization entity
        from synapseclient.models import SchemaOrganization

        mock_org = SchemaOrganization(name="testorg")
        mock_org.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_org, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_org.delete_async.assert_awaited_once()
        assert result is None

    async def test_delete_curation_task_entity(self):
        """Test that a CurationTask entity is deleted normally."""
        # GIVEN a mock CurationTask entity
        from synapseclient.models import CurationTask

        mock_task = CurationTask(project_id="syn123")
        mock_task.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_task, synapse_client=MagicMock())

        # THEN delete_async is called
        mock_task.delete_async.assert_awaited_once()
        assert result is None


class TestDeleteJSONSchemaRoute:
    """Tests for JSONSchema entity deletion routing in delete_async."""

    async def test_delete_json_schema_without_version(self):
        """Test that JSONSchema is deleted without version parameter."""
        # GIVEN a mock JSONSchema entity
        from synapseclient.models import JSONSchema

        mock_schema = JSONSchema(organization_name="testorg", name="testschema")
        mock_schema.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async
        result = await delete_async(entity=mock_schema, synapse_client=MagicMock())

        # THEN delete_async is called without version
        mock_schema.delete_async.assert_awaited_once()
        call_kwargs = mock_schema.delete_async.call_args[1]
        assert "version" not in call_kwargs
        assert result is None

    async def test_delete_json_schema_with_version(self):
        """Test that JSONSchema is deleted with version parameter."""
        # GIVEN a mock JSONSchema entity
        from synapseclient.models import JSONSchema

        mock_schema = JSONSchema(organization_name="testorg", name="testschema")
        mock_schema.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async with a version
        result = await delete_async(
            entity=mock_schema, version="1.0.0", synapse_client=MagicMock()
        )

        # THEN delete_async is called with version as string
        mock_schema.delete_async.assert_awaited_once()
        call_kwargs = mock_schema.delete_async.call_args[1]
        assert call_kwargs["version"] == "1.0.0"
        assert result is None


class TestDeleteUnsupportedEntity:
    """Tests for unsupported entity type in delete_async."""

    async def test_unsupported_entity_raises_value_error(self):
        """Test that an unsupported entity type raises ValueError."""
        # GIVEN an object that is not a supported entity type
        unsupported_entity = MagicMock()
        unsupported_entity.__class__ = type("UnsupportedEntity", (), {})

        # WHEN/THEN delete_async raises ValueError
        with pytest.raises(ValueError, match="Unsupported entity type"):
            await delete_async(entity=unsupported_entity, synapse_client=MagicMock())


class TestDeleteVersionConflictWarning:
    """Tests for version conflict warning behavior."""

    @patch("synapseclient.Synapse.get_client")
    async def test_version_conflict_emits_warning(self, mock_get_client):
        """Test that conflicting version parameter and entity version emits warning."""
        # GIVEN a mock File entity with version_number=3 but passing version=5
        from synapseclient.models import File

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_file = File(id="syn123456", version_number=3)
        mock_file.delete_async = AsyncMock(return_value=None)

        # WHEN I call delete_async with version=5, version_only=True
        result = await delete_async(
            entity=mock_file,
            version=5,
            version_only=True,
            synapse_client=MagicMock(),
        )

        # THEN a warning is logged about the version conflict
        mock_client.logger.warning.assert_called_once()
        warning_msg = mock_client.logger.warning.call_args[0][0]
        assert "Version conflict" in warning_msg
        assert "5" in warning_msg
        assert "3" in warning_msg
        assert result is None


class TestDeleteSyncWrapper:
    """Tests for the synchronous delete() wrapper."""

    @patch("synapseclient.operations.delete_operations.wrap_async_to_sync")
    def test_delete_sync_calls_wrap_async_to_sync(self, mock_wrap):
        """Test that the synchronous delete() calls wrap_async_to_sync."""
        from synapseclient.operations.delete_operations import delete

        # GIVEN a mock entity and wrap_async_to_sync
        mock_entity = MagicMock()
        mock_wrap.return_value = None

        # WHEN I call the sync delete
        result = delete(entity=mock_entity, synapse_client=MagicMock())

        # THEN wrap_async_to_sync is called
        assert result is None
        mock_wrap.assert_called_once()
