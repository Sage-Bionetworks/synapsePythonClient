"""Unit tests for store_operations routing logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient.models.services.storable_entity_components import FailureStrategy
from synapseclient.operations.store_operations import (
    StoreContainerOptions,
    StoreFileOptions,
    StoreGridOptions,
    StoreJSONSchemaOptions,
    StoreTableOptions,
    store_async,
)


class TestStoreFileEntityRoute:
    """Tests for File entity routing in store_async."""

    async def test_store_file_entity_default_options(self):
        """Test that a File entity is routed to _handle_store_file_entity."""
        # GIVEN a mock File entity
        from synapseclient.models import File

        mock_file = File(path="/tmp/test.txt", parent_id="syn123")
        mock_file.store_async = AsyncMock(return_value=mock_file)

        # WHEN I call store_async
        result = await store_async(entity=mock_file, synapse_client=MagicMock())

        # THEN the file's store_async is called with default params
        mock_file.store_async.assert_awaited_once()
        assert result is mock_file

    async def test_store_file_entity_with_file_options(self):
        """Test that StoreFileOptions are applied to the File entity before storing."""
        # GIVEN a mock File entity with file options
        from synapseclient.models import File

        mock_file = File(path="/tmp/test.txt", parent_id="syn123")
        mock_file.store_async = AsyncMock(return_value=mock_file)

        file_options = StoreFileOptions(
            synapse_store=False,
            content_type="text/plain",
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )

        # WHEN I call store_async with file options
        result = await store_async(
            entity=mock_file, file_options=file_options, synapse_client=MagicMock()
        )

        # THEN the file options should be applied to the entity
        assert mock_file.synapse_store is False
        assert mock_file.content_type == "text/plain"
        assert mock_file.merge_existing_annotations is True
        assert mock_file.associate_activity_to_new_version is False
        mock_file.store_async.assert_awaited_once()
        assert result is mock_file

    async def test_store_file_entity_with_parent(self):
        """Test that a parent is passed through when storing a File."""
        # GIVEN a mock File entity and a parent Folder
        from synapseclient.models import File, Folder

        mock_file = File(path="/tmp/test.txt")
        mock_file.store_async = AsyncMock(return_value=mock_file)
        parent_folder = Folder(id="syn456")

        # WHEN I call store_async with a parent
        result = await store_async(
            entity=mock_file, parent=parent_folder, synapse_client=MagicMock()
        )

        # THEN store_async is called with the parent
        call_kwargs = mock_file.store_async.call_args[1]
        assert call_kwargs["parent"] is parent_folder
        assert result is mock_file

    async def test_store_recordset_routes_to_file_handler(self):
        """Test that RecordSet also routes through the file entity handler."""
        # GIVEN a mock RecordSet entity
        from synapseclient.models import RecordSet

        mock_recordset = RecordSet(id="syn789")
        mock_recordset.store_async = AsyncMock(return_value=mock_recordset)

        # WHEN I call store_async
        result = await store_async(entity=mock_recordset, synapse_client=MagicMock())

        # THEN the recordset's store_async is called
        mock_recordset.store_async.assert_awaited_once()
        assert result is mock_recordset


class TestStoreContainerEntityRoute:
    """Tests for container entity (Project, Folder) routing in store_async."""

    async def test_store_project_default_options(self):
        """Test that a Project entity is stored with default failure strategy."""
        # GIVEN a mock Project entity
        from synapseclient.models import Project

        mock_project = Project(name="Test Project")
        mock_project.store_async = AsyncMock(return_value=mock_project)

        # WHEN I call store_async
        result = await store_async(entity=mock_project, synapse_client=MagicMock())

        # THEN store_async is called with LOG_EXCEPTION as the default failure strategy
        call_kwargs = mock_project.store_async.call_args[1]
        assert call_kwargs["failure_strategy"] == FailureStrategy.LOG_EXCEPTION
        assert result is mock_project

    async def test_store_folder_with_raise_exception_strategy(self):
        """Test that a Folder entity uses RAISE_EXCEPTION failure strategy."""
        # GIVEN a mock Folder entity with container options
        from synapseclient.models import Folder

        mock_folder = Folder(name="Test Folder", parent_id="syn123")
        mock_folder.store_async = AsyncMock(return_value=mock_folder)
        container_options = StoreContainerOptions(
            failure_strategy=FailureStrategy.RAISE_EXCEPTION
        )

        # WHEN I call store_async with container options
        result = await store_async(
            entity=mock_folder,
            container_options=container_options,
            synapse_client=MagicMock(),
        )

        # THEN store_async is called with RAISE_EXCEPTION strategy
        call_kwargs = mock_folder.store_async.call_args[1]
        assert call_kwargs["failure_strategy"] == FailureStrategy.RAISE_EXCEPTION
        assert result is mock_folder

    async def test_store_folder_with_string_failure_strategy(self):
        """Test that a string failure strategy is converted to enum."""
        # GIVEN a mock Folder entity with string failure strategy
        from synapseclient.models import Folder

        mock_folder = Folder(name="Test Folder", parent_id="syn123")
        mock_folder.store_async = AsyncMock(return_value=mock_folder)
        container_options = StoreContainerOptions(failure_strategy="RAISE_EXCEPTION")

        # WHEN I call store_async with string failure strategy
        result = await store_async(
            entity=mock_folder,
            container_options=container_options,
            synapse_client=MagicMock(),
        )

        # THEN store_async is called with RAISE_EXCEPTION enum
        call_kwargs = mock_folder.store_async.call_args[1]
        assert call_kwargs["failure_strategy"] == FailureStrategy.RAISE_EXCEPTION
        assert result is mock_folder

    async def test_store_folder_with_parent(self):
        """Test that a parent is passed through when storing a Folder."""
        # GIVEN a mock Folder entity and a parent Project
        from synapseclient.models import Folder, Project

        mock_folder = Folder(name="Test Folder")
        mock_folder.store_async = AsyncMock(return_value=mock_folder)
        parent_project = Project(id="syn456")

        # WHEN I call store_async with parent
        result = await store_async(
            entity=mock_folder, parent=parent_project, synapse_client=MagicMock()
        )

        # THEN store_async is called with the parent
        call_kwargs = mock_folder.store_async.call_args[1]
        assert call_kwargs["parent"] is parent_project
        assert result is mock_folder

    async def test_store_project_ignores_parent(self):
        """Test that Project ignores parent parameter (projects have no parent)."""
        # GIVEN a mock Project entity
        from synapseclient.models import Project

        mock_project = Project(name="Test Project")
        mock_project.store_async = AsyncMock(return_value=mock_project)

        # WHEN I call store_async
        result = await store_async(entity=mock_project, synapse_client=MagicMock())

        # THEN store_async is called without parent kwarg
        call_kwargs = mock_project.store_async.call_args[1]
        assert "parent" not in call_kwargs
        assert result is mock_project


class TestStoreTableEntityRoute:
    """Tests for table-like entity routing in store_async."""

    async def test_store_table_default_options(self):
        """Test that a Table entity is stored with default table options."""
        # GIVEN a mock Table entity
        from synapseclient.models import Table

        mock_table = Table(name="Test Table", parent_id="syn123")
        mock_table.store_async = AsyncMock(return_value=mock_table)

        # WHEN I call store_async with no table options
        result = await store_async(entity=mock_table, synapse_client=MagicMock())

        # THEN store_async is called with default dry_run=False and job_timeout=600
        call_kwargs = mock_table.store_async.call_args[1]
        assert call_kwargs["dry_run"] is False
        assert call_kwargs["job_timeout"] == 600
        assert result is mock_table

    async def test_store_table_with_options(self):
        """Test that StoreTableOptions are applied to table entity."""
        # GIVEN a mock Table entity with table options
        from synapseclient.models import Table

        mock_table = Table(name="Test Table", parent_id="syn123")
        mock_table.store_async = AsyncMock(return_value=mock_table)
        table_options = StoreTableOptions(dry_run=True, job_timeout=300)

        # WHEN I call store_async with table options
        result = await store_async(
            entity=mock_table,
            table_options=table_options,
            synapse_client=MagicMock(),
        )

        # THEN store_async is called with the specified options
        call_kwargs = mock_table.store_async.call_args[1]
        assert call_kwargs["dry_run"] is True
        assert call_kwargs["job_timeout"] == 300
        assert result is mock_table

    async def test_store_dataset_routes_to_table_handler(self):
        """Test that Dataset routes through the table entity handler."""
        # GIVEN a mock Dataset entity
        from synapseclient.models import Dataset

        mock_dataset = Dataset(name="Test Dataset", parent_id="syn123")
        mock_dataset.store_async = AsyncMock(return_value=mock_dataset)

        # WHEN I call store_async
        result = await store_async(entity=mock_dataset, synapse_client=MagicMock())

        # THEN store_async is called with table-like defaults
        call_kwargs = mock_dataset.store_async.call_args[1]
        assert call_kwargs["dry_run"] is False
        assert result is mock_dataset

    async def test_store_entityview_routes_to_table_handler(self):
        """Test that EntityView routes through the table entity handler."""
        # GIVEN a mock EntityView entity
        from synapseclient.models import EntityView

        mock_view = EntityView(name="Test View", parent_id="syn123")
        mock_view.store_async = AsyncMock(return_value=mock_view)

        # WHEN I call store_async
        result = await store_async(entity=mock_view, synapse_client=MagicMock())

        # THEN store_async is called
        mock_view.store_async.assert_awaited_once()
        assert result is mock_view

    async def test_store_materialized_view_routes_to_table_handler(self):
        """Test that MaterializedView routes through the table entity handler."""
        # GIVEN a mock MaterializedView entity
        from synapseclient.models import MaterializedView

        mock_mv = MaterializedView(name="Test MV", parent_id="syn123")
        mock_mv.store_async = AsyncMock(return_value=mock_mv)

        # WHEN I call store_async
        result = await store_async(entity=mock_mv, synapse_client=MagicMock())

        # THEN store_async is called
        mock_mv.store_async.assert_awaited_once()
        assert result is mock_mv

    async def test_store_virtual_table_routes_to_table_handler(self):
        """Test that VirtualTable routes through the table entity handler."""
        # GIVEN a mock VirtualTable entity
        from synapseclient.models import VirtualTable

        mock_vt = VirtualTable(name="Test VT", parent_id="syn123")
        mock_vt.store_async = AsyncMock(return_value=mock_vt)

        # WHEN I call store_async
        result = await store_async(entity=mock_vt, synapse_client=MagicMock())

        # THEN store_async is called
        mock_vt.store_async.assert_awaited_once()
        assert result is mock_vt


class TestStoreLinkEntityRoute:
    """Tests for Link entity routing in store_async."""

    async def test_store_link_entity(self):
        """Test that a Link entity is routed correctly."""
        # GIVEN a mock Link entity
        from synapseclient.models import Link

        mock_link = Link(name="Test Link", parent_id="syn123", target_id="syn456")
        mock_link.store_async = AsyncMock(return_value=mock_link)

        # WHEN I call store_async
        result = await store_async(entity=mock_link, synapse_client=MagicMock())

        # THEN store_async is called
        mock_link.store_async.assert_awaited_once()
        assert result is mock_link

    async def test_store_link_entity_with_parent(self):
        """Test that a parent is passed through when storing a Link."""
        # GIVEN a mock Link entity and a parent Folder
        from synapseclient.models import Folder, Link

        mock_link = Link(name="Test Link", target_id="syn789")
        mock_link.store_async = AsyncMock(return_value=mock_link)
        parent_folder = Folder(id="syn456")

        # WHEN I call store_async with a parent
        result = await store_async(
            entity=mock_link, parent=parent_folder, synapse_client=MagicMock()
        )

        # THEN store_async is called with the parent
        call_kwargs = mock_link.store_async.call_args[1]
        assert call_kwargs["parent"] is parent_folder
        assert result is mock_link


class TestStoreJSONSchemaRoute:
    """Tests for JSONSchema entity routing in store_async."""

    async def test_store_json_schema_with_options(self):
        """Test that JSONSchema is stored with schema options."""
        # GIVEN a mock JSONSchema entity with schema options
        from synapseclient.models import JSONSchema

        mock_schema = JSONSchema(organization_name="testorg", name="testschema")
        mock_schema.store_async = AsyncMock(return_value=mock_schema)
        json_schema_options = StoreJSONSchemaOptions(
            schema_body={"type": "object"},
            version="1.0.0",
            dry_run=False,
        )

        # WHEN I call store_async with json schema options
        result = await store_async(
            entity=mock_schema,
            json_schema_options=json_schema_options,
            synapse_client=MagicMock(),
        )

        # THEN store_async is called with schema_body, version, and dry_run
        call_kwargs = mock_schema.store_async.call_args[1]
        assert call_kwargs["schema_body"] == {"type": "object"}
        assert call_kwargs["version"] == "1.0.0"
        assert call_kwargs["dry_run"] is False
        assert result is mock_schema

    async def test_store_json_schema_with_dry_run(self):
        """Test that JSONSchema respects dry_run option."""
        # GIVEN a mock JSONSchema entity with dry_run=True
        from synapseclient.models import JSONSchema

        mock_schema = JSONSchema(organization_name="testorg", name="testschema")
        mock_schema.store_async = AsyncMock(return_value=mock_schema)
        json_schema_options = StoreJSONSchemaOptions(
            schema_body={"type": "string"},
            dry_run=True,
        )

        # WHEN I call store_async with dry_run
        result = await store_async(
            entity=mock_schema,
            json_schema_options=json_schema_options,
            synapse_client=MagicMock(),
        )

        # THEN store_async is called with dry_run=True
        call_kwargs = mock_schema.store_async.call_args[1]
        assert call_kwargs["dry_run"] is True
        assert result is mock_schema

    async def test_store_json_schema_without_options_raises_value_error(self):
        """Test that JSONSchema without options raises ValueError."""
        # GIVEN a mock JSONSchema entity without options
        from synapseclient.models import JSONSchema

        mock_schema = JSONSchema(organization_name="testorg", name="testschema")

        # WHEN/THEN store_async raises ValueError
        with pytest.raises(
            ValueError, match="json_schema_options with schema_body is required"
        ):
            await store_async(entity=mock_schema, synapse_client=MagicMock())

    async def test_store_json_schema_with_empty_schema_body_raises_value_error(self):
        """Test that JSONSchema with falsy schema_body raises ValueError."""
        # GIVEN a mock JSONSchema entity with empty schema_body
        from synapseclient.models import JSONSchema

        mock_schema = JSONSchema(organization_name="testorg", name="testschema")
        json_schema_options = StoreJSONSchemaOptions(schema_body={})

        # WHEN/THEN store_async raises ValueError for empty body
        with pytest.raises(
            ValueError, match="json_schema_options with schema_body is required"
        ):
            await store_async(
                entity=mock_schema,
                json_schema_options=json_schema_options,
                synapse_client=MagicMock(),
            )


class TestStoreTeamRoute:
    """Tests for Team entity routing in store_async."""

    async def test_store_team_without_id_calls_create(self):
        """Test that a Team without an ID calls create_async."""
        # GIVEN a mock Team entity without an ID
        from synapseclient.models import Team

        mock_team = Team(name="Test Team")
        mock_team.create_async = AsyncMock(return_value=mock_team)
        mock_team.store_async = AsyncMock(return_value=mock_team)

        # WHEN I call store_async
        result = await store_async(entity=mock_team, synapse_client=MagicMock())

        # THEN create_async is called (not store_async)
        mock_team.create_async.assert_awaited_once()
        mock_team.store_async.assert_not_awaited()
        assert result is mock_team

    async def test_store_team_with_id_calls_store(self):
        """Test that a Team with an ID calls store_async (update)."""
        # GIVEN a mock Team entity with an ID
        from synapseclient.models import Team

        mock_team = Team(id=12345, name="Test Team")
        mock_team.store_async = AsyncMock(return_value=mock_team)
        mock_team.create_async = AsyncMock(return_value=mock_team)

        # WHEN I call store_async
        result = await store_async(entity=mock_team, synapse_client=MagicMock())

        # THEN store_async is called (not create_async)
        mock_team.store_async.assert_awaited_once()
        mock_team.create_async.assert_not_awaited()
        assert result is mock_team


class TestStoreEvaluationRoute:
    """Tests for Evaluation entity routing in store_async."""

    async def test_store_evaluation_entity(self):
        """Test that an Evaluation entity routes to store_async."""
        # GIVEN a mock Evaluation entity
        from synapseclient.models import Evaluation

        mock_eval = Evaluation(name="Test Eval", content_source="syn123")
        mock_eval.store_async = AsyncMock(return_value=mock_eval)

        # WHEN I call store_async
        result = await store_async(entity=mock_eval, synapse_client=MagicMock())

        # THEN store_async is called
        mock_eval.store_async.assert_awaited_once()
        assert result is mock_eval


class TestStoreSchemaOrganizationRoute:
    """Tests for SchemaOrganization entity routing in store_async."""

    async def test_store_schema_organization(self):
        """Test that SchemaOrganization routes to store_async."""
        # GIVEN a mock SchemaOrganization entity
        from synapseclient.models import SchemaOrganization

        mock_org = SchemaOrganization(name="testorg")
        mock_org.store_async = AsyncMock(return_value=mock_org)

        # WHEN I call store_async
        result = await store_async(entity=mock_org, synapse_client=MagicMock())

        # THEN store_async is called
        mock_org.store_async.assert_awaited_once()
        assert result is mock_org


class TestStoreFormRoute:
    """Tests for Form entity routing in store_async."""

    async def test_store_form_group_calls_create_or_get(self):
        """Test that FormGroup routes to create_or_get_async."""
        # GIVEN a mock FormGroup entity
        from synapseclient.models import FormGroup

        mock_form_group = FormGroup(name="Test Form Group")
        mock_form_group.create_or_get_async = AsyncMock(return_value=mock_form_group)

        # WHEN I call store_async
        result = await store_async(entity=mock_form_group, synapse_client=MagicMock())

        # THEN create_or_get_async is called
        mock_form_group.create_or_get_async.assert_awaited_once()
        assert result is mock_form_group

    async def test_store_form_data_calls_create_or_get(self):
        """Test that FormData routes to create_or_get_async."""
        # GIVEN a mock FormData entity
        from synapseclient.models import FormData

        mock_form_data = FormData(name="Test Form Data", group_id="test_group_id")
        mock_form_data.create_or_get_async = AsyncMock(return_value=mock_form_data)

        # WHEN I call store_async
        result = await store_async(entity=mock_form_data, synapse_client=MagicMock())

        # THEN create_or_get_async is called
        mock_form_data.create_or_get_async.assert_awaited_once()
        assert result is mock_form_data


class TestStoreAgentSessionRoute:
    """Tests for AgentSession entity routing in store_async."""

    async def test_store_agent_session_calls_update(self):
        """Test that AgentSession routes to update_async."""
        # GIVEN a mock AgentSession entity
        from synapseclient.models import AgentSession

        mock_session = AgentSession(id="session123")
        mock_session.update_async = AsyncMock(return_value=mock_session)

        # WHEN I call store_async
        result = await store_async(entity=mock_session, synapse_client=MagicMock())

        # THEN update_async is called
        mock_session.update_async.assert_awaited_once()
        assert result is mock_session


class TestStoreCurationTaskRoute:
    """Tests for CurationTask entity routing in store_async."""

    async def test_store_curation_task(self):
        """Test that CurationTask routes to store_async."""
        # GIVEN a mock CurationTask entity
        from synapseclient.models import CurationTask

        mock_task = CurationTask(project_id="syn123")
        mock_task.store_async = AsyncMock(return_value=mock_task)

        # WHEN I call store_async
        result = await store_async(entity=mock_task, synapse_client=MagicMock())

        # THEN store_async is called
        mock_task.store_async.assert_awaited_once()
        assert result is mock_task


class TestStoreGridRoute:
    """Tests for Grid entity routing in store_async."""

    async def test_store_grid_default_options(self):
        """Test that Grid entity is stored with default grid options."""
        # GIVEN a mock Grid entity
        from synapseclient.models import Grid

        mock_grid = Grid(record_set_id="syn123")
        mock_grid.create_async = AsyncMock(return_value=mock_grid)

        # WHEN I call store_async with no grid options
        result = await store_async(entity=mock_grid, synapse_client=MagicMock())

        # THEN create_async is called with defaults
        call_kwargs = mock_grid.create_async.call_args[1]
        assert call_kwargs["attach_to_previous_session"] is False
        assert call_kwargs["timeout"] == 120
        assert result is mock_grid

    async def test_store_grid_with_options(self):
        """Test that StoreGridOptions are applied to Grid entity."""
        # GIVEN a mock Grid entity with grid options
        from synapseclient.models import Grid

        mock_grid = Grid(record_set_id="syn123")
        mock_grid.create_async = AsyncMock(return_value=mock_grid)
        grid_options = StoreGridOptions(attach_to_previous_session=True, timeout=60)

        # WHEN I call store_async with grid options
        result = await store_async(
            entity=mock_grid,
            grid_options=grid_options,
            synapse_client=MagicMock(),
        )

        # THEN create_async is called with the specified options
        call_kwargs = mock_grid.create_async.call_args[1]
        assert call_kwargs["attach_to_previous_session"] is True
        assert call_kwargs["timeout"] == 60
        assert result is mock_grid


class TestStoreUnsupportedEntity:
    """Tests for unsupported entity type in store_async."""

    async def test_unsupported_entity_raises_value_error(self):
        """Test that an unsupported entity type raises ValueError."""
        # GIVEN an object that is not a supported entity type
        unsupported_entity = MagicMock()
        # Make sure isinstance checks for all known types fail
        unsupported_entity.__class__ = type("UnsupportedEntity", (), {})

        # WHEN/THEN store_async raises ValueError
        with pytest.raises(ValueError, match="Unsupported entity type"):
            await store_async(entity=unsupported_entity, synapse_client=MagicMock())


class TestStoreSyncWrapper:
    """Tests for the synchronous store() wrapper."""

    @patch("synapseclient.operations.store_operations.wrap_async_to_sync")
    def test_store_sync_calls_wrap_async_to_sync(self, mock_wrap):
        """Test that the synchronous store() calls wrap_async_to_sync."""
        from synapseclient.operations.store_operations import store

        # GIVEN a mock entity and wrap_async_to_sync
        mock_entity = MagicMock()
        mock_wrap.return_value = mock_entity

        # WHEN I call the sync store
        result = store(entity=mock_entity, synapse_client=MagicMock())

        # THEN wrap_async_to_sync is called
        assert result is mock_entity
        mock_wrap.assert_called_once()
