"""
Unit tests for synapseclient.extensions.curator module.

This module contains comprehensive unit tests for the three public functions:
- create_file_based_metadata_task
- create_record_based_metadata_task
- query_schema_registry
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.extensions.curator import (
    create_file_based_metadata_task,
    create_record_based_metadata_task,
    query_schema_registry,
)
from synapseclient.extensions.curator.file_based_metadata_task import (
    _create_columns_from_json_schema,
    _get_column_type_from_js_one_of_list,
    _get_column_type_from_js_property,
    _get_list_column_type_from_js_property,
    create_entity_view_wiki,
    create_json_schema_entity_view,
    create_or_update_wiki_with_entity_view,
    update_wiki_with_entity_view,
)
from synapseclient.extensions.curator.record_based_metadata_task import (
    create_dataframe_from_titles,
    extract_property_titles,
    extract_schema_properties_from_dict,
    extract_schema_properties_from_web,
)
from synapseclient.extensions.curator.schema_generation import (
    generate_jsonld,
    generate_jsonschema,
)
from synapseclient.extensions.curator.schema_registry import (
    SCHEMA_REGISTRY_TABLE_ID,
    SchemaRegistryColumnConfig,
    get_latest_schema_uri,
)
from synapseclient.models import ColumnType
from synapseclient.models.mixins import JSONSchemaBinding
from synapseclient.models.mixins.json_schema import JSONSchemaVersionInfo


@pytest.fixture(name="test_directory", scope="function")
def fixture_test_directory(tmp_path) -> str:
    """Returns a directory for creating test files in.

    pytest automatically handles cleanup of the temporary directory.
    """
    return str(tmp_path)
    return str(tmp_path)


class TestCreateFileBasedMetadataTask(unittest.TestCase):
    """Test cases for create_file_based_metadata_task function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def setUp(self):
        """Set up test fixtures."""
        self.mock_syn = Mock(spec=Synapse)
        self.mock_syn.logger = Mock()
        self.folder_id = "syn12345678"
        self.curation_task_name = "TestCurationTask"
        self.instructions = "Test instructions"
        self.entity_view_name = "Test Entity View"
        self.schema_uri = "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_json_schema_entity_view"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_or_update_wiki_with_entity_view"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.CurationTask")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.get")
    def test_create_file_based_metadata_task_success_with_schema(
        self,
        mock_get,
        mock_curation_task_cls,
        mock_folder_cls,
        mock_create_wiki,
        mock_create_entity_view,
        mock_get_client,
    ):
        """Test successful creation with schema binding."""
        # GIVEN a file-based metadata task with schema binding
        mock_get_client.return_value = self.mock_syn
        mock_create_entity_view.return_value = "syn87654321"

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder
        mock_folder.parent_id = "syn11111111"

        mock_project = Mock()
        mock_project.concreteType = "org.sagebionetworks.repo.model.Project"
        mock_project.id = "syn22222222"
        self.mock_syn.get.return_value = mock_project

        mock_task = Mock()
        mock_task.task_id = "task123"
        mock_curation_task = Mock()
        mock_curation_task.store.return_value = mock_task
        mock_curation_task_cls.return_value = mock_curation_task

        # WHEN I create the file-based metadata task
        result = create_file_based_metadata_task(
            folder_id=self.folder_id,
            curation_task_name=self.curation_task_name,
            instructions=self.instructions,
            attach_wiki=True,
            entity_view_name=self.entity_view_name,
            schema_uri=self.schema_uri,
            enable_derived_annotations=True,
            synapse_client=self.mock_syn,
        )

        # THEN the task should be created successfully
        assert result == ("syn87654321", "task123")
        mock_folder.bind_schema.assert_called_once_with(
            json_schema_uri=self.schema_uri,
            enable_derived_annotations=True,
            synapse_client=self.mock_syn,
        )
        mock_create_entity_view.assert_called_once_with(
            syn=self.mock_syn,
            synapse_entity_id=self.folder_id,
            entity_view_name=self.entity_view_name,
        )
        mock_create_wiki.assert_called_once_with(
            syn=self.mock_syn, entity_view_id="syn87654321", owner_id=self.folder_id
        )
        mock_get.assert_called_once_with(self.folder_id, synapse_client=self.mock_syn)

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_json_schema_entity_view"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.CurationTask")
    def test_create_file_based_metadata_task_success_without_schema_no_wiki(
        self,
        mock_curation_task_cls,
        mock_folder_cls,
        mock_create_entity_view,
        mock_get_client,
    ):
        """Test successful creation without schema binding and without wiki."""
        # GIVEN a file-based metadata task without schema binding or wiki
        mock_get_client.return_value = self.mock_syn
        mock_create_entity_view.return_value = "syn87654321"

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder
        mock_folder.parent_id = "syn11111111"

        mock_project = Mock()
        mock_project.concreteType = "org.sagebionetworks.repo.model.Project"
        mock_project.id = "syn22222222"
        self.mock_syn.get.return_value = mock_project

        mock_task = Mock()
        mock_task.task_id = "task123"
        mock_curation_task = Mock()
        mock_curation_task.store.return_value = mock_task
        mock_curation_task_cls.return_value = mock_curation_task

        # WHEN I create the file-based metadata task
        result = create_file_based_metadata_task(
            folder_id=self.folder_id,
            curation_task_name=self.curation_task_name,
            instructions=self.instructions,
            attach_wiki=False,
            synapse_client=self.mock_syn,
        )

        # THEN the task should be created successfully without schema binding
        assert result == ("syn87654321", "task123")
        mock_folder.bind_schema.assert_not_called()

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    def test_create_file_based_metadata_task_missing_folder_id(self, mock_get_client):
        """Test ValueError when folder_id is missing."""
        # GIVEN a file-based metadata task with missing folder_id
        mock_get_client.return_value = self.mock_syn

        # WHEN I create the file-based metadata task
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="folder_id is required"):
            create_file_based_metadata_task(
                folder_id="",
                curation_task_name=self.curation_task_name,
                instructions=self.instructions,
                attach_wiki=True,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    def test_create_file_based_metadata_task_missing_curation_task_name(
        self, mock_get_client
    ):
        """Test ValueError when curation_task_name is missing."""
        # GIVEN a file-based metadata task with missing curation_task_name
        mock_get_client.return_value = self.mock_syn

        # WHEN I create the file-based metadata task
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="curation_task_name is required"):
            create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name="",
                instructions=self.instructions,
                attach_wiki=True,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    def test_create_file_based_metadata_task_missing_instructions(
        self, mock_get_client
    ):
        """Test ValueError when instructions is missing."""
        # GIVEN a file-based metadata task with missing instructions
        mock_get_client.return_value = self.mock_syn

        # WHEN I create the file-based metadata task
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="instructions is required"):
            create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name=self.curation_task_name,
                instructions="",
                attach_wiki=True,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    def test_create_file_based_metadata_task_schema_binding_error(
        self, mock_folder_cls, mock_get_client
    ):
        """Test error handling during schema binding."""
        # GIVEN a file-based metadata task where schema binding fails
        mock_get_client.return_value = self.mock_syn

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder
        mock_folder.bind_schema.side_effect = Exception("Schema binding failed")

        # WHEN I create the file-based metadata task
        # THEN it should raise the schema binding exception
        with pytest.raises(Exception, match="Schema binding failed"):
            create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name=self.curation_task_name,
                instructions=self.instructions,
                attach_wiki=True,
                schema_uri=self.schema_uri,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_json_schema_entity_view"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    def test_create_file_based_metadata_task_entity_view_creation_error(
        self, mock_folder_cls, mock_create_entity_view, mock_get_client
    ):
        """Test error handling during entity view creation."""
        # GIVEN a file-based metadata task where entity view creation fails
        mock_get_client.return_value = self.mock_syn
        mock_create_entity_view.side_effect = Exception("Entity view creation failed")

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder

        # WHEN I create the file-based metadata task
        # THEN it should raise the entity view creation exception
        with pytest.raises(Exception, match="Entity view creation failed"):
            create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name=self.curation_task_name,
                instructions=self.instructions,
                attach_wiki=True,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_json_schema_entity_view"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_or_update_wiki_with_entity_view"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    def test_create_file_based_metadata_task_wiki_creation_error(
        self,
        mock_folder_cls,
        mock_create_wiki,
        mock_create_entity_view,
        mock_get_client,
    ):
        """Test error handling during wiki creation."""
        mock_get_client.return_value = self.mock_syn
        mock_create_entity_view.return_value = "syn87654321"
        mock_create_wiki.side_effect = Exception("Wiki creation failed")

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder

        with pytest.raises(Exception, match="Wiki creation failed"):
            create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name=self.curation_task_name,
                instructions=self.instructions,
                attach_wiki=True,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_json_schema_entity_view"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.get")
    def test_create_file_based_metadata_task_schema_retrieval_error(
        self, mock_folder_cls, mock_create_entity_view, mock_get_client, mock_get
    ):
        """Test error handling during schema retrieval."""
        mock_get_client.return_value = self.mock_syn
        mock_create_entity_view.return_value = "syn87654321"

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder

        mock_get.side_effect = Exception("Schema retrieval failed")

        with pytest.raises(Exception, match="Schema retrieval failed"):
            create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name=self.curation_task_name,
                instructions=self.instructions,
                attach_wiki=False,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_json_schema_entity_view"
    )
    @patch("synapseclient.extensions.curator.file_based_metadata_task.Folder")
    def test_create_file_based_metadata_task_project_traversal(
        self, mock_folder_cls, mock_create_entity_view, mock_get_client
    ):
        """Test project traversal to find parent project."""
        mock_get_client.return_value = self.mock_syn
        mock_create_entity_view.return_value = "syn87654321"

        mock_folder = Mock()
        mock_folder_cls.return_value = mock_folder
        mock_folder.get.return_value = mock_folder
        mock_folder.parent_id = "syn11111111"

        # Mock parent folder
        mock_parent_folder = Mock()
        mock_parent_folder.concreteType = "org.sagebionetworks.repo.model.Folder"
        mock_parent_folder.parentId = "syn22222222"

        # Mock project
        mock_project = Mock()
        mock_project.concreteType = "org.sagebionetworks.repo.model.Project"
        mock_project.id = "syn22222222"

        self.mock_syn.get.side_effect = [mock_parent_folder, mock_project]

        mock_task = Mock()
        mock_task.task_id = "task123"

        with patch(
            "synapseclient.extensions.curator.file_based_metadata_task.CurationTask"
        ) as mock_curation_task_cls:
            mock_curation_task = Mock()
            mock_curation_task.store.return_value = mock_task
            mock_curation_task_cls.return_value = mock_curation_task

            result = create_file_based_metadata_task(
                folder_id=self.folder_id,
                curation_task_name=self.curation_task_name,
                instructions=self.instructions,
                attach_wiki=False,
                synapse_client=self.mock_syn,
            )

        self.assertEqual(result, ("syn87654321", "task123"))
        # Verify that syn.get was called twice (for parent folder and project)
        self.assertEqual(self.mock_syn.get.call_count, 2)


class TestCreateRecordBasedMetadataTask(unittest.TestCase):
    """Test cases for create_record_based_metadata_task function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def setUp(self):
        """Set up test fixtures."""
        self.mock_syn = Mock(spec=Synapse)
        self.mock_syn.logger = Mock()
        self.project_id = "syn11111111"
        self.folder_id = "syn12345678"
        self.record_set_name = "TestRecordSet"
        self.record_set_description = "Test description"
        self.curation_task_name = "TestCurationTask"
        self.upsert_keys = ["specimenID"]
        self.instructions = "Test instructions"
        self.schema_uri = "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.extract_schema_properties_from_web"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.tempfile.NamedTemporaryFile"
    )
    @patch("synapseclient.extensions.curator.record_based_metadata_task.RecordSet")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.CurationTask")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.Grid")
    @patch("builtins.open")
    def test_create_record_based_metadata_task_success(
        self,
        mock_open,
        mock_grid_cls,
        mock_curation_task_cls,
        mock_record_set_cls,
        mock_temp_file,
        mock_extract_schema,
        mock_get_client,
    ):
        """Test successful creation of record-based metadata task."""
        # GIVEN a record-based metadata task with all required components
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(columns=["specimenID", "age", "diagnosis"])
        mock_extract_schema.return_value = mock_df

        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value = mock_temp

        mock_record_set = Mock()
        mock_record_set.id = "syn87654321"
        mock_record_set_instance = Mock()
        mock_record_set_instance.store.return_value = mock_record_set
        mock_record_set_cls.return_value = mock_record_set_instance

        mock_task = Mock()
        mock_task.task_id = "task123"
        mock_curation_task = Mock()
        mock_curation_task.store.return_value = mock_task
        mock_curation_task_cls.return_value = mock_curation_task

        mock_grid = Mock()
        mock_grid_instance = Mock()
        mock_grid_instance.export_to_record_set.return_value = mock_grid
        mock_grid_cls.return_value = mock_grid_instance

        # WHEN I create the record-based metadata task
        result = create_record_based_metadata_task(
            project_id=self.project_id,
            folder_id=self.folder_id,
            record_set_name=self.record_set_name,
            record_set_description=self.record_set_description,
            curation_task_name=self.curation_task_name,
            upsert_keys=self.upsert_keys,
            instructions=self.instructions,
            schema_uri=self.schema_uri,
            bind_schema_to_record_set=True,
            enable_derived_annotations=True,
            synapse_client=self.mock_syn,
        )

        # THEN the task should be created successfully
        assert isinstance(result, tuple)
        assert len(result) == 3
        record_set, task, grid = result
        assert record_set == mock_record_set
        assert task == mock_task
        assert grid == mock_grid

        mock_extract_schema.assert_called_once_with(
            syn=self.mock_syn, schema_uri=self.schema_uri
        )
        mock_record_set.bind_schema.assert_called_once_with(
            json_schema_uri=self.schema_uri,
            enable_derived_annotations=True,
            synapse_client=self.mock_syn,
        )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.extract_schema_properties_from_web"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.tempfile.NamedTemporaryFile"
    )
    @patch("synapseclient.extensions.curator.record_based_metadata_task.RecordSet")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.CurationTask")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.Grid")
    @patch("builtins.open")
    def test_create_record_based_metadata_task_no_schema_binding(
        self,
        mock_open,
        mock_grid_cls,
        mock_curation_task_cls,
        mock_record_set_cls,
        mock_temp_file,
        mock_extract_schema,
        mock_get_client,
    ):
        """Test creation without schema binding."""
        # Setup mocks
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(columns=["specimenID", "age", "diagnosis"])
        mock_extract_schema.return_value = mock_df

        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value = mock_temp

        mock_record_set = Mock()
        mock_record_set.id = "syn87654321"
        mock_record_set_instance = Mock()
        mock_record_set_instance.store.return_value = mock_record_set
        mock_record_set_cls.return_value = mock_record_set_instance

        mock_task = Mock()
        mock_task.task_id = "task123"
        mock_curation_task = Mock()
        mock_curation_task.store.return_value = mock_task
        mock_curation_task_cls.return_value = mock_curation_task

        mock_grid = Mock()
        mock_grid_instance = Mock()
        mock_grid_instance.export_to_record_set.return_value = mock_grid
        mock_grid_cls.return_value = mock_grid_instance

        # Call function
        result = create_record_based_metadata_task(
            project_id=self.project_id,
            folder_id=self.folder_id,
            record_set_name=self.record_set_name,
            record_set_description=self.record_set_description,
            curation_task_name=self.curation_task_name,
            upsert_keys=self.upsert_keys,
            instructions=self.instructions,
            schema_uri=self.schema_uri,
            bind_schema_to_record_set=False,
            synapse_client=self.mock_syn,
        )

        # Assertions
        mock_record_set.bind_schema.assert_not_called()

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    def test_create_record_based_metadata_task_missing_project_id(
        self, mock_get_client
    ):
        """Test ValueError when project_id is missing."""
        mock_get_client.return_value = self.mock_syn

        with pytest.raises(ValueError, match="project_id is required"):
            create_record_based_metadata_task(
                project_id="",
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=self.upsert_keys,
                instructions=self.instructions,
                schema_uri=self.schema_uri,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    def test_create_record_based_metadata_task_missing_upsert_keys(
        self, mock_get_client
    ):
        """Test ValueError when upsert_keys is empty."""
        mock_get_client.return_value = self.mock_syn

        with pytest.raises(
            ValueError, match="upsert_keys is required and must be a non-empty list"
        ):
            create_record_based_metadata_task(
                project_id=self.project_id,
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=[],
                instructions=self.instructions,
                schema_uri=self.schema_uri,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    def test_create_record_based_metadata_task_missing_schema_uri(
        self, mock_get_client
    ):
        """Test ValueError when schema_uri is missing."""
        mock_get_client.return_value = self.mock_syn

        with pytest.raises(ValueError, match="schema_uri is required"):
            create_record_based_metadata_task(
                project_id=self.project_id,
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=self.upsert_keys,
                instructions=self.instructions,
                schema_uri="",
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.extract_schema_properties_from_web"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.tempfile.NamedTemporaryFile"
    )
    @patch("builtins.open")
    def test_create_record_based_metadata_task_csv_write_error(
        self, mock_open, mock_temp_file, mock_extract_schema, mock_get_client
    ):
        """Test error handling during CSV file writing."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(columns=["specimenID", "age", "diagnosis"])
        mock_extract_schema.return_value = mock_df

        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value = mock_temp

        mock_open.side_effect = Exception("File write error")

        with pytest.raises(Exception, match="File write error"):
            create_record_based_metadata_task(
                project_id=self.project_id,
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=self.upsert_keys,
                instructions=self.instructions,
                schema_uri=self.schema_uri,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.extract_schema_properties_from_web"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.tempfile.NamedTemporaryFile"
    )
    @patch("synapseclient.extensions.curator.record_based_metadata_task.RecordSet")
    @patch("builtins.open")
    def test_create_record_based_metadata_task_record_set_creation_error(
        self,
        mock_open,
        mock_record_set_cls,
        mock_temp_file,
        mock_extract_schema,
        mock_get_client,
    ):
        """Test error handling during RecordSet creation."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(columns=["specimenID", "age", "diagnosis"])
        mock_extract_schema.return_value = mock_df

        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value = mock_temp

        mock_record_set_instance = Mock()
        mock_record_set_instance.store.side_effect = Exception(
            "RecordSet creation failed"
        )
        mock_record_set_cls.return_value = mock_record_set_instance

        with pytest.raises(Exception, match="RecordSet creation failed"):
            create_record_based_metadata_task(
                project_id=self.project_id,
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=self.upsert_keys,
                instructions=self.instructions,
                schema_uri=self.schema_uri,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.extract_schema_properties_from_web"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.tempfile.NamedTemporaryFile"
    )
    @patch("synapseclient.extensions.curator.record_based_metadata_task.RecordSet")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.CurationTask")
    @patch("builtins.open")
    def test_create_record_based_metadata_task_curation_task_creation_error(
        self,
        mock_open,
        mock_curation_task_cls,
        mock_record_set_cls,
        mock_temp_file,
        mock_extract_schema,
        mock_get_client,
    ):
        """Test error handling during CurationTask creation."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(columns=["specimenID", "age", "diagnosis"])
        mock_extract_schema.return_value = mock_df

        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value = mock_temp

        mock_record_set = Mock()
        mock_record_set.id = "syn87654321"
        mock_record_set_instance = Mock()
        mock_record_set_instance.store.return_value = mock_record_set
        mock_record_set_cls.return_value = mock_record_set_instance

        mock_curation_task = Mock()
        mock_curation_task.store.side_effect = Exception("CurationTask creation failed")
        mock_curation_task_cls.return_value = mock_curation_task

        with pytest.raises(Exception, match="CurationTask creation failed"):
            create_record_based_metadata_task(
                project_id=self.project_id,
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=self.upsert_keys,
                instructions=self.instructions,
                schema_uri=self.schema_uri,
                bind_schema_to_record_set=False,
                synapse_client=self.mock_syn,
            )

    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.Synapse.get_client"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.extract_schema_properties_from_web"
    )
    @patch(
        "synapseclient.extensions.curator.record_based_metadata_task.tempfile.NamedTemporaryFile"
    )
    @patch("synapseclient.extensions.curator.record_based_metadata_task.RecordSet")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.CurationTask")
    @patch("synapseclient.extensions.curator.record_based_metadata_task.Grid")
    @patch("builtins.open")
    def test_create_record_based_metadata_task_grid_creation_error(
        self,
        mock_open,
        mock_grid_cls,
        mock_curation_task_cls,
        mock_record_set_cls,
        mock_temp_file,
        mock_extract_schema,
        mock_get_client,
    ):
        """Test error handling during Grid creation."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(columns=["specimenID", "age", "diagnosis"])
        mock_extract_schema.return_value = mock_df

        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value = mock_temp

        mock_record_set = Mock()
        mock_record_set.id = "syn87654321"
        mock_record_set_instance = Mock()
        mock_record_set_instance.store.return_value = mock_record_set
        mock_record_set_cls.return_value = mock_record_set_instance

        mock_task = Mock()
        mock_task.task_id = "task123"
        mock_curation_task = Mock()
        mock_curation_task.store.return_value = mock_task
        mock_curation_task_cls.return_value = mock_curation_task

        mock_grid_instance = Mock()
        mock_grid_instance.create.side_effect = Exception("Grid creation failed")
        mock_grid_cls.return_value = mock_grid_instance

        with pytest.raises(Exception, match="Grid creation failed"):
            create_record_based_metadata_task(
                project_id=self.project_id,
                folder_id=self.folder_id,
                record_set_name=self.record_set_name,
                record_set_description=self.record_set_description,
                curation_task_name=self.curation_task_name,
                upsert_keys=self.upsert_keys,
                instructions=self.instructions,
                schema_uri=self.schema_uri,
                bind_schema_to_record_set=False,
                synapse_client=self.mock_syn,
            )


class TestQuerySchemaRegistry(unittest.TestCase):
    """Test cases for query_schema_registry function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def setUp(self):
        """Set up test fixtures."""
        self.mock_syn = Mock(spec=Synapse)
        self.mock_syn.logger = Mock()

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_success_latest_only(
        self, mock_table_cls, mock_get_client
    ):
        """Test successful query returning latest only."""
        # GIVEN a schema registry query that should return the latest schema only
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(
            {
                "dcc": ["ad", "amp", "mc2"],
                "datatype": ["Analysis", "Biospecimen", "Biospecimen"],
                "version": ["0.0.0", "0.0.1", "9.0.0"],
                "uri": [
                    "sage.schemas.v2571-ad.Analysis.schema-0.0.0",
                    "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1",
                    "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0",
                ],
            }
        )

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # WHEN I query the schema registry for the latest result only
        result = query_schema_registry(
            synapse_client=self.mock_syn,
            dcc="mc2",
            datatype="Biospecimen",
            return_latest_only=True,
        )

        # THEN it should return the first result from the sorted DataFrame
        assert (
            result == "sage.schemas.v2571-ad.Analysis.schema-0.0.0"
        )  # First in sorted order
        mock_table_cls.assert_called_once_with(id=SCHEMA_REGISTRY_TABLE_ID)

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_success_all_results(
        self, mock_table_cls, mock_get_client
    ):
        """Test successful query returning all results."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(
            {
                "dcc": ["amp", "mc2"],
                "datatype": ["Biospecimen", "Biospecimen"],
                "version": ["0.0.1", "9.0.0"],
                "uri": [
                    "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1",
                    "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0",
                ],
            }
        )

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # Call function
        result = query_schema_registry(
            synapse_client=self.mock_syn,
            datatype="Biospecimen",
            return_latest_only=False,
        )

        # Assertions
        expected = [
            "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1",
            "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0",
        ]
        self.assertEqual(result, expected)

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_pattern_matching(
        self, mock_table_cls, mock_get_client
    ):
        """Test query with pattern matching using wildcards."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(
            {
                "dcc": ["mc2", "MC2"],
                "datatype": ["Biospecimen", "Biospecimen"],
                "version": ["9.0.0", "12.0.0"],
                "uri": [
                    "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0",
                    "MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0",
                ],
            }
        )

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # Call function with wildcard pattern
        result = query_schema_registry(
            synapse_client=self.mock_syn,
            dcc="%C2",  # Should match both 'mc2' and 'MC2'
            return_latest_only=False,
        )

        # Assertions
        expected = [
            "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0",
            "MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0",
        ]
        self.assertEqual(result, expected)

        # Verify LIKE was used in query
        args, kwargs = mock_table.query.call_args
        query_string = kwargs["query"]
        self.assertIn("LIKE '%C2'", query_string)

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_empty_results(self, mock_table_cls, mock_get_client):
        """Test query with no matching results."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame()  # Empty DataFrame

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # Test latest only
        result = query_schema_registry(
            synapse_client=self.mock_syn, dcc="nonexistent", return_latest_only=True
        )
        self.assertIsNone(result)

        # Test all results
        result = query_schema_registry(
            synapse_client=self.mock_syn, dcc="nonexistent", return_latest_only=False
        )
        self.assertEqual(result, [])

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    def test_query_schema_registry_no_filters(self, mock_get_client):
        """Test ValueError when no filters are provided."""
        mock_get_client.return_value = self.mock_syn

        with pytest.raises(
            ValueError, match="At least one filter parameter must be provided"
        ):
            query_schema_registry(synapse_client=self.mock_syn)

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_custom_table_id(
        self, mock_table_cls, mock_get_client
    ):
        """Test query with custom schema registry table ID."""
        mock_get_client.return_value = self.mock_syn
        custom_table_id = "syn99999999"

        mock_df = pd.DataFrame({"version": ["1.0.0"], "uri": ["custom.schema.uri"]})

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # Call function
        result = query_schema_registry(
            synapse_client=self.mock_syn,
            schema_registry_table_id=custom_table_id,
            dcc="test",
        )

        # Assertions
        mock_table_cls.assert_called_once_with(id=custom_table_id)

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_custom_column_config(
        self, mock_table_cls, mock_get_client
    ):
        """Test query with custom column configuration."""
        mock_get_client.return_value = self.mock_syn

        custom_config = SchemaRegistryColumnConfig(
            version_column="custom_version", uri_column="custom_uri"
        )

        mock_df = pd.DataFrame(
            {"custom_version": ["1.0.0"], "custom_uri": ["custom.schema.uri"]}
        )

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # Call function
        result = query_schema_registry(
            synapse_client=self.mock_syn, column_config=custom_config, dcc="test"
        )

        # Verify the query used custom column names
        args, kwargs = mock_table.query.call_args
        query_string = kwargs["query"]
        self.assertIn("ORDER BY custom_version DESC", query_string)

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.Table")
    def test_query_schema_registry_multiple_filters(
        self, mock_table_cls, mock_get_client
    ):
        """Test query with multiple filter conditions."""
        mock_get_client.return_value = self.mock_syn

        mock_df = pd.DataFrame(
            {
                "dcc": ["amp"],
                "datatype": ["Biospecimen"],
                "version": ["0.0.1"],
                "uri": ["sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"],
            }
        )

        mock_table = Mock()
        mock_table.query.return_value = mock_df
        mock_table_cls.return_value = mock_table

        # Call function with multiple filters
        result = query_schema_registry(
            synapse_client=self.mock_syn,
            dcc="amp",
            datatype="Biospecimen",
            version="0.0.1",
        )

        # Verify AND logic was used
        args, kwargs = mock_table.query.call_args
        query_string = kwargs["query"]
        self.assertIn(
            "dcc = 'amp' AND datatype = 'Biospecimen' AND version = '0.0.1'",
            query_string,
        )


class TestRecordBasedHelperFunctions(unittest.TestCase):
    """Test cases for helper functions in record_based_metadata_task module."""

    def test_extract_property_titles_success(self):
        """Test successful extraction of property titles from schema."""
        # GIVEN a schema with properties
        schema_data = {
            "properties": {
                "specimenID": {"type": "string"},
                "age": {"type": "integer"},
                "diagnosis": {"type": "string"},
            }
        }

        # WHEN I extract property titles
        result = extract_property_titles(schema_data)

        # THEN it should return the expected property names
        expected = ["specimenID", "age", "diagnosis"]
        assert result == expected

    def test_extract_property_titles_no_properties(self):
        """Test extraction when no properties field exists."""
        # GIVEN a schema without properties
        schema_data = {}

        # WHEN I extract property titles
        result = extract_property_titles(schema_data)

        # THEN it should return an empty list
        assert result == []

    def test_create_dataframe_from_titles_success(self):
        """Test successful DataFrame creation from titles."""
        titles = ["specimenID", "age", "diagnosis"]

        result = create_dataframe_from_titles(titles)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), titles)
        self.assertEqual(len(result), 0)  # Empty DataFrame

    def test_create_dataframe_from_titles_empty(self):
        """Test DataFrame creation with empty titles."""
        titles = []

        result = create_dataframe_from_titles(titles)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result.columns), 0)

    def test_extract_schema_properties_from_dict_success(self):
        """Test successful schema property extraction from dictionary."""
        schema_data = {
            "properties": {"specimenID": {"type": "string"}, "age": {"type": "integer"}}
        }

        result = extract_schema_properties_from_dict(schema_data)

        self.assertIsInstance(result, pd.DataFrame)
        expected_columns = ["specimenID", "age"]
        self.assertEqual(list(result.columns), expected_columns)

    @patch("synapseclient.extensions.curator.record_based_metadata_task.JSONSchema")
    def test_extract_schema_properties_from_web_success(self, mock_schema_cls):
        """Test successful schema property extraction from web."""
        mock_syn = Mock(spec=Synapse)
        schema_uri = "org-schema.name.schema-1.0.0"

        mock_body = {
            "properties": {"specimenID": {"type": "string"}, "age": {"type": "integer"}}
        }
        mock_schema = Mock()
        mock_schema.get_body.return_value = mock_body
        mock_schema_cls.get.return_value = mock_schema
        mock_schema_cls.from_uri.return_value = mock_schema

        result = extract_schema_properties_from_web(mock_syn, schema_uri)

        self.assertIsInstance(result, pd.DataFrame)
        expected_columns = ["specimenID", "age"]
        self.assertEqual(list(result.columns), expected_columns)
        mock_schema.get.assert_called_once()
        mock_schema.get_body.assert_called_once()


class TestFileBasedHelperFunctions(unittest.TestCase):
    """Test cases for helper functions in file_based_metadata_task module."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_syn = Mock(spec=Synapse)
        self.mock_syn.logger = Mock()

    @patch("synapseclient.extensions.curator.file_based_metadata_task.isinstance")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.EntityView")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.get")
    @patch("synapseclient.extensions.curator.file_based_metadata_task.JSONSchema")
    def test_create_json_schema_entity_view_success(
        self,
        mock_json_schema_cls,
        mock_get,
        mock_entity_view_cls,
        mock_isinstance,
    ):
        """Test successful creation of JSON schema entity view."""
        # GIVEN a valid synapse entity with a JSON schema
        entity_id = "syn12345678"
        entity_view_name = "Test View"

        mock_entity = Mock()
        mock_entity.get_schema.return_value = JSONSchemaBinding(
            object_id=1,
            object_type="",
            created_on="",
            created_by="",
            enable_derived_annotations=True,
            json_schema_version_info=JSONSchemaVersionInfo(
                organization_id="",
                organization_name="org.name",
                schema_id="",
                id="",
                schema_name="schema.name",
                version_id="",
                semantic_version="0.0.1",
                json_sha256_hex="",
                created_on="",
                created_by="",
            ),
        )
        mock_get.return_value = mock_entity
        mock_isinstance.return_value = True

        mock_json_schema = Mock()
        mock_json_schema.get_body.return_value = {
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}
        }
        mock_json_schema_cls.return_value = mock_json_schema

        mock_view = Mock()
        mock_view.id = "syn87654321"
        mock_view.store.return_value = mock_view
        mock_entity_view_cls.return_value = mock_view

        # WHEN I create the JSON schema entity view
        result = create_json_schema_entity_view(
            syn=self.mock_syn,
            synapse_entity_id=entity_id,
            entity_view_name=entity_view_name,
        )

        # THEN the entity view should be created successfully
        assert result == "syn87654321"
        mock_view.reorder_column.assert_any_call(name="createdBy", index=0)
        mock_view.reorder_column.assert_any_call(name="name", index=0)
        mock_view.reorder_column.assert_any_call(name="id", index=0)

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.update_wiki_with_entity_view"
    )
    def test_create_or_update_wiki_existing_wiki(self, mock_update_wiki):
        """Test creating or updating wiki when wiki already exists."""
        # GIVEN an entity with an existing wiki
        entity_id = "syn12345678"
        entity_view_id = "syn87654321"
        title = "Test Wiki"

        mock_entity = Mock()
        self.mock_syn.get.return_value = mock_entity

        mock_wiki = Mock()
        self.mock_syn.getWiki.return_value = mock_wiki

        mock_update_wiki.return_value = mock_wiki

        # WHEN I create or update the wiki
        result = create_or_update_wiki_with_entity_view(
            syn=self.mock_syn,
            entity_view_id=entity_view_id,
            owner_id=entity_id,
            title=title,
        )

        # THEN the existing wiki should be updated
        assert result == mock_wiki
        mock_update_wiki.assert_called_once_with(
            self.mock_syn, entity_view_id, entity_id, title
        )

    @patch(
        "synapseclient.extensions.curator.file_based_metadata_task.create_entity_view_wiki"
    )
    def test_create_or_update_wiki_no_existing_wiki(self, mock_create_wiki):
        """Test creating or updating wiki when no wiki exists."""
        # GIVEN an entity without an existing wiki
        entity_id = "syn12345678"
        entity_view_id = "syn87654321"

        mock_entity = Mock()
        self.mock_syn.get.return_value = mock_entity

        self.mock_syn.getWiki.side_effect = SynapseHTTPError("Wiki not found")

        mock_new_wiki = Mock()
        mock_create_wiki.return_value = mock_new_wiki

        # WHEN I create or update the wiki
        result = create_or_update_wiki_with_entity_view(
            syn=self.mock_syn, entity_view_id=entity_view_id, owner_id=entity_id
        )

        # THEN a new wiki should be created
        assert result == mock_new_wiki
        mock_create_wiki.assert_called_once()

    @patch("synapseclient.extensions.curator.file_based_metadata_task.Wiki")
    def test_create_entity_view_wiki_with_title(self, mock_wiki_cls):
        """Test creating entity view wiki with a custom title."""
        # GIVEN an entity view ID and a custom title
        entity_view_id = "syn87654321"
        owner_id = "syn12345678"
        title = "Custom Title"

        mock_wiki = Mock()
        mock_wiki_cls.return_value = mock_wiki
        self.mock_syn.store.return_value = mock_wiki

        # WHEN I create the entity view wiki
        result = create_entity_view_wiki(
            syn=self.mock_syn,
            entity_view_id=entity_view_id,
            owner_id=owner_id,
            title=title,
        )

        # THEN a wiki should be created with the custom title
        assert result == mock_wiki
        mock_wiki_cls.assert_called_once()
        args, kwargs = mock_wiki_cls.call_args
        assert kwargs["title"] == title

    @patch("synapseclient.extensions.curator.file_based_metadata_task.Wiki")
    def test_create_entity_view_wiki_default_title(self, mock_wiki_cls):
        """Test creating entity view wiki with default title."""
        # GIVEN an entity view ID without a custom title
        entity_view_id = "syn87654321"
        owner_id = "syn12345678"

        mock_wiki = Mock()
        mock_wiki_cls.return_value = mock_wiki
        self.mock_syn.store.return_value = mock_wiki

        # WHEN I create the entity view wiki
        result = create_entity_view_wiki(
            syn=self.mock_syn,
            entity_view_id=entity_view_id,
            owner_id=owner_id,
            title=None,
        )

        # THEN a wiki should be created with the default title
        assert result == mock_wiki
        mock_wiki_cls.assert_called_once()
        args, kwargs = mock_wiki_cls.call_args
        assert kwargs["title"] == "Entity View"

    def test_update_wiki_with_entity_view_content_already_exists(self):
        """Test updating wiki when content already contains the entity view."""
        # GIVEN a wiki that already contains the entity view query
        entity_view_id = "syn87654321"
        owner_id = "syn12345678"

        mock_entity = Mock()
        self.mock_syn.get.return_value = mock_entity

        mock_wiki = Mock()
        mock_wiki.markdown = (
            "Existing content\n"
            "${synapsetable?query=select %2A from syn87654321"
            "&showquery=false&tableonly=false}"
        )
        self.mock_syn.getWiki.return_value = mock_wiki

        # WHEN I update the wiki
        result = update_wiki_with_entity_view(
            syn=self.mock_syn, entity_view_id=entity_view_id, owner_id=owner_id
        )

        # THEN the wiki should be returned unchanged
        assert result == mock_wiki
        self.mock_syn.store.assert_not_called()

    def test_update_wiki_with_entity_view_add_content(self):
        """Test updating wiki by adding entity view content."""
        # GIVEN a wiki without the entity view query
        entity_view_id = "syn87654321"
        owner_id = "syn12345678"
        title = "New Title"

        mock_entity = Mock()
        self.mock_syn.get.return_value = mock_entity

        mock_wiki = Mock()
        mock_wiki.markdown = "Existing content"
        self.mock_syn.getWiki.return_value = mock_wiki

        # WHEN I update the wiki
        result = update_wiki_with_entity_view(
            syn=self.mock_syn,
            entity_view_id=entity_view_id,
            owner_id=owner_id,
            title=title,
        )

        # THEN the wiki content and title should be updated
        assert "${synapsetable?query=select %2A from syn87654321" in mock_wiki.markdown
        assert mock_wiki.title == title
        self.mock_syn.store.assert_called_once()

    def test_create_columns_from_json_schema_no_properties(self):
        """Test error when JSON schema has no properties."""
        # GIVEN a JSON schema without properties field
        json_schema = {"type": "object"}

        # WHEN I create columns from the schema
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="The JSON Schema is missing a 'properties' field"
        ):
            _create_columns_from_json_schema(json_schema)

    def test_create_columns_from_json_schema_properties_not_dict(self):
        """Test error when JSON schema properties is not a dict."""
        # GIVEN a JSON schema where properties is not a dictionary
        json_schema = {"properties": ["not", "a", "dict"]}

        # WHEN I create columns from the schema
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError,
            match="The 'properties' field in the JSON Schema must be a dictionary",
        ):
            _create_columns_from_json_schema(json_schema)

    def test_create_columns_from_json_schema_success(self):
        """Test successful column creation from JSON schema."""
        # GIVEN a valid JSON schema with properties
        json_schema = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "active": {"type": "boolean"},
            }
        }

        # WHEN I create columns from the schema
        columns = _create_columns_from_json_schema(json_schema)

        # THEN columns should be created for all properties
        assert len(columns) == 3
        assert all(hasattr(col, "name") for col in columns)
        assert all(hasattr(col, "column_type") for col in columns)

    def test_get_column_type_from_js_property_enum(self):
        """Test getting column type for enum property."""
        # GIVEN a JSON schema property with an enum
        js_property = {"enum": ["option1", "option2", "option3"]}

        # WHEN I get the column type
        result = _get_column_type_from_js_property(js_property)

        # THEN it should return STRING type
        assert result == ColumnType.STRING

    def test_get_column_type_from_js_property_array(self):
        """Test getting column type for array property."""
        # GIVEN a JSON schema property with array type
        js_property = {"type": "array", "items": {"type": "string"}}

        # WHEN I get the column type
        result = _get_column_type_from_js_property(js_property)

        # THEN it should return a list type
        assert result == ColumnType.STRING_LIST

    def test_get_column_type_from_js_property_one_of(self):
        """Test getting column type for oneOf property."""
        # GIVEN a JSON schema property with oneOf
        js_property = {"oneOf": [{"type": "string"}, {"type": "null"}]}

        # WHEN I get the column type
        result = _get_column_type_from_js_property(js_property)

        # THEN it should return STRING type
        assert result == ColumnType.STRING

    def test_get_column_type_from_js_property_fallback(self):
        """Test getting column type fallback to STRING."""
        # GIVEN a JSON schema property without recognizable type
        js_property = {"description": "some property"}

        # WHEN I get the column type
        result = _get_column_type_from_js_property(js_property)

        # THEN it should return STRING type as fallback
        assert result == ColumnType.STRING

    def test_get_column_type_from_js_one_of_list_with_enum(self):
        """Test getting column type from oneOf list containing enum."""
        # GIVEN a oneOf list with an enum
        js_one_of_list = [{"enum": ["a", "b", "c"]}, {"type": "null"}]

        # WHEN I get the column type
        result = _get_column_type_from_js_one_of_list(js_one_of_list)

        # THEN it should return STRING type
        assert result == ColumnType.STRING

    def test_get_column_type_from_js_one_of_list_single_type(self):
        """Test getting column type from oneOf list with single non-null type."""
        # GIVEN a oneOf list with one non-null type
        js_one_of_list = [{"type": "integer"}, {"type": "null"}]

        # WHEN I get the column type
        result = _get_column_type_from_js_one_of_list(js_one_of_list)

        # THEN it should return INTEGER type
        assert result == ColumnType.INTEGER

    def test_get_column_type_from_js_one_of_list_array_type(self):
        """Test getting column type from oneOf list with array type."""
        # GIVEN a oneOf list with an array type
        js_one_of_list = [
            {"type": "array", "items": {"type": "boolean"}},
            {"type": "null"},
        ]

        # WHEN I get the column type
        result = _get_column_type_from_js_one_of_list(js_one_of_list)

        # THEN it should return BOOLEAN_LIST type
        assert result == ColumnType.BOOLEAN_LIST

    def test_get_column_type_from_js_one_of_list_fallback(self):
        """Test getting column type from oneOf list fallback."""
        # GIVEN a oneOf list with multiple non-null types
        js_one_of_list = [{"type": "string"}, {"type": "integer"}]

        # WHEN I get the column type
        result = _get_column_type_from_js_one_of_list(js_one_of_list)

        # THEN it should return STRING type as fallback
        assert result == ColumnType.STRING

    def test_get_list_column_type_from_js_property_with_enum(self):
        """Test getting list column type for property with enum items."""
        # GIVEN a JSON schema array property with enum items
        js_property = {"type": "array", "items": {"enum": ["red", "green", "blue"]}}

        # WHEN I get the column type
        result = _get_list_column_type_from_js_property(js_property)

        # THEN it should return STRING_LIST type
        assert result == ColumnType.STRING_LIST

    def test_get_list_column_type_from_js_property_with_type(self):
        """Test getting list column type for property with typed items."""
        # GIVEN a JSON schema array property with integer items
        js_property = {"type": "array", "items": {"type": "integer"}}

        # WHEN I get the column type
        result = _get_list_column_type_from_js_property(js_property)

        # THEN it should return INTEGER_LIST type
        assert result == ColumnType.INTEGER_LIST

    def test_get_list_column_type_from_js_property_fallback(self):
        """Test getting list column type fallback to STRING_LIST."""
        # GIVEN a JSON schema array property without items
        js_property = {"type": "array"}

        # WHEN I get the column type
        result = _get_list_column_type_from_js_property(js_property)

        # THEN it should return STRING_LIST type as fallback
        assert result == ColumnType.STRING_LIST


class TestGetLatestSchemaUri(unittest.TestCase):
    """Test cases for get_latest_schema_uri function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def setUp(self):
        """Set up test fixtures."""
        self.mock_syn = Mock(spec=Synapse)
        self.mock_syn.logger = Mock()

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.query_schema_registry")
    def test_get_latest_schema_uri_found(self, mock_query, mock_get_client):
        """Test getting latest schema URI when a match is found."""
        # GIVEN a query that returns a matching schema URI
        mock_get_client.return_value = self.mock_syn
        mock_query.return_value = "sage.schemas.v2571-ad.Analysis.schema-0.0.0"

        # WHEN I get the latest schema URI
        result = get_latest_schema_uri(
            synapse_client=self.mock_syn, dcc="ad", datatype="Analysis"
        )

        # THEN the URI should be returned
        assert result == "sage.schemas.v2571-ad.Analysis.schema-0.0.0"
        mock_query.assert_called_once()
        self.mock_syn.logger.info.assert_called()

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.query_schema_registry")
    def test_get_latest_schema_uri_not_found(self, mock_query, mock_get_client):
        """Test getting latest schema URI when no match is found."""
        # GIVEN a query that returns no matching schema
        mock_get_client.return_value = self.mock_syn
        mock_query.return_value = None

        # WHEN I get the latest schema URI
        result = get_latest_schema_uri(
            synapse_client=self.mock_syn, dcc="nonexistent", datatype="NonExistent"
        )

        # THEN None should be returned
        assert result is None
        mock_query.assert_called_once()
        self.mock_syn.logger.info.assert_called()

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.query_schema_registry")
    def test_get_latest_schema_uri_with_custom_table(self, mock_query, mock_get_client):
        """Test getting latest schema URI with custom table ID."""
        # GIVEN a custom schema registry table ID
        mock_get_client.return_value = self.mock_syn
        mock_query.return_value = "custom-schema-uri"
        custom_table_id = "syn99999999"

        # WHEN I get the latest schema URI
        result = get_latest_schema_uri(
            synapse_client=self.mock_syn,
            schema_registry_table_id=custom_table_id,
            dcc="test",
        )

        # THEN the query should use the custom table ID
        assert result == "custom-schema-uri"
        mock_query.assert_called_once()
        args, kwargs = mock_query.call_args
        assert kwargs["schema_registry_table_id"] == custom_table_id

    @patch("synapseclient.extensions.curator.schema_registry.Synapse.get_client")
    @patch("synapseclient.extensions.curator.schema_registry.query_schema_registry")
    def test_get_latest_schema_uri_with_custom_column_config(
        self, mock_query, mock_get_client
    ):
        """Test getting latest schema URI with custom column configuration."""
        # GIVEN a custom column configuration
        mock_get_client.return_value = self.mock_syn
        mock_query.return_value = "schema-uri"
        custom_config = SchemaRegistryColumnConfig(
            version_column="custom_version", uri_column="custom_uri"
        )

        # WHEN I get the latest schema URI
        result = get_latest_schema_uri(
            synapse_client=self.mock_syn,
            column_config=custom_config,
            custom_filter="value",
        )

        # THEN the query should use the custom configuration
        assert result == "schema-uri"
        mock_query.assert_called_once()
        args, kwargs = mock_query.call_args
        assert kwargs["column_config"] == custom_config


class TestGenerateJsonld(unittest.TestCase):
    """Test cases for generate_jsonld function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def setUp(self):
        """Set up test fixtures."""
        # Get the path to the test schema file
        self.test_schema_path = os.path.join(
            os.path.dirname(__file__),
            "schema_files",
            "data_models/example.model.csv",
        )

    def test_generate_jsonld_with_default_output_path(self):
        """Test generate_jsonld with default output path (None)."""
        # GIVEN a CSV schema file and no specified output path
        # WHEN I generate JSONLD
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=None,
            synapse_client=self.syn,
        )

        # THEN the result should be a dictionary
        assert isinstance(result, dict)
        assert "@context" in result
        assert "@graph" in result

        # AND the JSONLD file should be created alongside the CSV
        expected_output = self.test_schema_path.replace(".csv", ".jsonld")
        assert os.path.exists(expected_output), f"Expected file at {expected_output}"

        # Clean up the generated file
        if os.path.exists(expected_output):
            os.remove(expected_output)

    def test_generate_jsonld_with_custom_output_path(self, test_directory):
        """Test generate_jsonld with custom output path."""
        # GIVEN a CSV schema file and a custom output path
        custom_output = os.path.join(test_directory, "custom_output.jsonld")

        # WHEN I generate JSONLD
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=custom_output,
            synapse_client=self.syn,
        )

        # THEN the result should be a dictionary with expected structure
        assert isinstance(result, dict)
        assert "@context" in result
        assert "@graph" in result

        # AND the file should exist at the custom path
        assert os.path.exists(custom_output)

        # AND the file should be valid JSON
        with open(custom_output, "r", encoding="utf-8") as f:
            file_content = json.load(f)
            assert file_content == result

    def test_generate_jsonld_with_display_label(self, test_directory):
        """Test generate_jsonld with display_label format."""
        # GIVEN a CSV schema file
        output_path = os.path.join(test_directory, "display_label.jsonld")

        # WHEN I generate JSONLD with display_label
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="display_label",
            output_jsonld=output_path,
            synapse_client=self.syn,
        )

        # THEN the result should contain the expected structure
        assert isinstance(result, dict)
        assert "@graph" in result

        # AND display names should be used where appropriate
        graph = result["@graph"]
        assert len(graph) > 0

        # Check that some entries exist in the graph
        labels = [entry.get("rdfs:label") for entry in graph if "rdfs:label" in entry]
        assert len(labels) > 0

    def test_generate_jsonld_validates_data_model(self, test_directory):
        """Test that generate_jsonld performs validation checks."""
        # GIVEN a CSV schema file
        output_path = os.path.join(test_directory, "validated.jsonld")

        # WHEN I generate JSONLD
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=output_path,
            synapse_client=self.syn,
        )

        # THEN the function should complete without errors
        # (validation is performed internally and logs warnings/errors)
        assert isinstance(result, dict)
        assert os.path.exists(output_path)

    def test_generate_jsonld_contains_expected_components(self, test_directory):
        """Test that generated JSONLD contains expected components from CSV."""
        # GIVEN a CSV schema file with known components
        output_path = os.path.join(test_directory, "components.jsonld")

        # WHEN I generate JSONLD
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=output_path,
            synapse_client=self.syn,
        )

        # THEN the result should contain expected components
        graph = result["@graph"]
        labels = [entry.get("rdfs:label") for entry in graph if "rdfs:label" in entry]

        # Check for some known components from example.model.csv
        expected_components = ["Component", "Patient", "Biospecimen"]
        for component in expected_components:
            assert component in labels, f"Expected component {component} not found"

    def test_generate_jsonld_preserves_relationships(self, test_directory):
        """Test that JSONLD preserves relationships from the CSV."""
        # GIVEN a CSV schema file with relationships
        output_path = os.path.join(test_directory, "relationships.jsonld")

        # WHEN I generate JSONLD
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=output_path,
            synapse_client=self.syn,
        )

        # THEN relationships should be preserved
        graph = result["@graph"]

        # Find Patient component and check it has dependencies
        patient_entries = [
            entry for entry in graph if entry.get("rdfs:label") == "Patient"
        ]
        assert len(patient_entries) > 0

        patient = patient_entries[0]
        # Patient should have requiresDependency field
        assert "sms:requiresDependency" in patient, "Patient should have dependencies"

    def test_generate_jsonld_includes_validation_rules(self, test_directory):
        """Test that JSONLD includes validation rules from CSV."""
        # GIVEN a CSV schema file with validation rules
        output_path = os.path.join(test_directory, "validation_rules.jsonld")

        # WHEN I generate JSONLD
        result = generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=output_path,
            synapse_client=self.syn,
        )

        # THEN validation rules should be included
        graph = result["@graph"]

        # Find entries with validation rules
        entries_with_rules = [
            entry
            for entry in graph
            if "sms:validationRules" in entry and entry["sms:validationRules"]
        ]

        # MockComponent has many validation rules, so it should be present
        assert len(entries_with_rules) > 0, "Expected validation rules in JSONLD"


class TestGenerateJsonschema(unittest.TestCase):
    """Test cases for generate_jsonschema function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def setUp(self):
        """Set up test fixtures."""
        # Get the path to the test schema file
        self.test_schema_path = os.path.join(
            os.path.dirname(__file__),
            "schema_files",
            "data_models/example.model.csv",
        )

    def test_generate_jsonschema_from_csv(self, test_directory):
        """Test generate_jsonschema from CSV file."""
        # GIVEN a CSV schema file
        # WHEN I generate JSON schemas
        schemas, file_paths = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=None,
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN schemas should be generated
        assert isinstance(schemas, list)
        assert len(schemas) > 0
        assert isinstance(file_paths, list)
        assert len(file_paths) == len(schemas)

        # AND files should exist
        for file_path in file_paths:
            assert os.path.exists(file_path), f"Expected file at {file_path}"

        # AND each schema should be valid JSON Schema
        for schema in schemas:
            assert isinstance(schema, dict)
            assert "$schema" in schema
            assert "properties" in schema

    def test_generate_jsonschema_from_jsonld(self, test_directory):
        """Test generate_jsonschema from JSONLD file."""
        # GIVEN a JSONLD file (first generate it from CSV)
        jsonld_path = os.path.join(test_directory, "test.jsonld")
        generate_jsonld(
            schema=self.test_schema_path,
            data_model_labels="class_label",
            output_jsonld=jsonld_path,
            synapse_client=self.syn,
        )

        # WHEN I generate JSON schemas from the JSONLD
        schemas, file_paths = generate_jsonschema(
            data_model_source=jsonld_path,
            output_directory=test_directory,
            data_type=None,
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN schemas should be generated
        assert len(schemas) > 0
        assert len(file_paths) > 0

        # AND files should exist
        for file_path in file_paths:
            assert os.path.exists(file_path)

    def test_generate_jsonschema_specific_components(self, tmp_path):
        """Test generate_jsonschema for specific components only."""
        # GIVEN a CSV schema file and specific components
        test_directory = str(tmp_path)  # pytest automatically handles cleanup
        target_components = ["Patient", "Biospecimen"]

        # WHEN I generate JSON schemas for specific components
        schemas, file_paths = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=target_components,
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN only the specified components should have schemas
        assert len(schemas) == len(target_components)
        assert len(file_paths) == len(target_components)

        # AND the file names should match the components
        for component in target_components:
            matching_files = [fp for fp in file_paths if component in fp]
            assert len(matching_files) > 0, f"Expected file for {component}"

        # AND all files should be in the temp directory
        for file_path in file_paths:
            assert file_path.startswith(
                test_directory
            ), f"File {file_path} was not created in temp directory {test_directory}"

    def test_generate_jsonschema_with_display_label(self, test_directory):
        """Test generate_jsonschema with display_label format."""
        # GIVEN a CSV schema file
        # WHEN I generate schemas with display_label
        schemas, file_paths = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["Patient"],
            data_model_labels="display_label",
            synapse_client=self.syn,
        )

        # THEN schemas should be generated
        assert len(schemas) > 0
        assert len(file_paths) > 0

        # AND properties might use display names
        patient_schema = schemas[0]
        assert "properties" in patient_schema

    def test_generate_jsonschema_validates_required_fields(self, test_directory):
        """Test that generated schemas include required field constraints."""
        # GIVEN a CSV schema file
        # WHEN I generate schemas
        schemas, _ = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["Patient"],
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN the Patient schema should have required fields
        patient_schema = schemas[0]
        assert "required" in patient_schema or "properties" in patient_schema

        # Patient should have required properties based on CSV
        if "required" in patient_schema:
            required_fields = patient_schema["required"]
            assert isinstance(required_fields, list)
            # Component is required for Patient
            assert "Component" in required_fields

    def test_generate_jsonschema_includes_enums(self, test_directory):
        """Test that generated schemas include enum constraints."""
        # GIVEN a CSV schema file with enum values
        # WHEN I generate schemas
        schemas, _ = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["Patient"],
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN enums should be present in the schema
        patient_schema = schemas[0]
        properties = patient_schema.get("properties", {})

        # Sex field has enum values: Female, Male, Other
        if "Sex" in properties:
            sex_property = properties["Sex"]
            # Check for enum in oneOf structure or directly
            has_enum = False
            if "oneOf" in sex_property:
                for option in sex_property["oneOf"]:
                    if "enum" in option:
                        has_enum = True
                        enum_values = option["enum"]
                        assert "Female" in enum_values
                        assert "Male" in enum_values
                        break
            elif "enum" in sex_property:
                has_enum = True

            assert has_enum, "Expected enum constraint for Sex field"

    def test_generate_jsonschema_includes_validation_rules(self, test_directory):
        """Test that schemas include validation rules like inRange, regex, etc."""
        # GIVEN a CSV with validation rules
        # WHEN I generate schemas for MockComponent (has many validation rules)
        schemas, _ = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["MockComponent"],
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN validation rules should be included
        assert len(schemas) > 0
        mock_schema = schemas[0]
        properties = mock_schema.get("properties", {})

        # Check for range validation (inRange rule)
        if "CheckRange" in properties:
            check_range = properties["CheckRange"]
            # Should have minimum and maximum
            has_range = "minimum" in check_range or "maximum" in check_range
            assert has_range, "Expected range constraints"

        # Check for regex validation
        if "CheckRegexFormat" in properties:
            check_regex = properties["CheckRegexFormat"]
            # Should have pattern
            assert "pattern" in check_regex or "oneOf" in check_regex

        # Check for URL validation
        if "CheckURL" in properties:
            check_url = properties["CheckURL"]
            # Should have format: uri
            has_uri_format = False
            if "format" in check_url:
                has_uri_format = check_url["format"] == "uri"
            elif "oneOf" in check_url:
                for option in check_url["oneOf"]:
                    if option.get("format") == "uri":
                        has_uri_format = True
                        break
            assert has_uri_format, "Expected URI format for URL field"

    def test_generate_jsonschema_includes_conditional_dependencies(
        self, test_directory
    ):
        """Test that schemas include conditional dependencies (if/then)."""
        # GIVEN a CSV with conditional dependencies
        # Patient -> Diagnosis=Cancer -> requires CancerType and FamilyHistory
        # WHEN I generate schemas
        schemas, _ = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["Patient"],
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN conditional dependencies should be in allOf
        patient_schema = schemas[0]

        # Check for allOf with if/then conditions
        if "allOf" in patient_schema:
            all_of = patient_schema["allOf"]
            assert isinstance(all_of, list)
            assert len(all_of) > 0

            # Find if/then structure for Cancer diagnosis
            has_cancer_dependency = False
            for condition in all_of:
                if "if" in condition and "then" in condition:
                    if_clause = condition["if"]
                    # Check if it's about Diagnosis=Cancer
                    if "properties" in if_clause:
                        diagnosis = if_clause["properties"].get("Diagnosis", {})
                        if "enum" in diagnosis and "Cancer" in diagnosis["enum"]:
                            has_cancer_dependency = True
                            # Check that then clause requires CancerType or FamilyHistory
                            then_clause = condition["then"]
                            assert (
                                "properties" in then_clause or "required" in then_clause
                            )
                            break

            assert has_cancer_dependency, "Expected conditional dependency for Cancer"

    def test_generate_jsonschema_handles_array_types(self, test_directory):
        """Test that schemas handle array/list types correctly."""
        # GIVEN a CSV with list validation rules
        # WHEN I generate schemas for MockComponent (has list rules)
        schemas, _ = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["MockComponent"],
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN list properties should have array type
        mock_schema = schemas[0]
        properties = mock_schema.get("properties", {})

        # CheckList has list validation rule
        if "CheckList" in properties:
            check_list = properties["CheckList"]
            # Should have type array or oneOf with array
            has_array_type = False
            if "type" in check_list:
                has_array_type = check_list["type"] == "array"
            elif "oneOf" in check_list:
                for option in check_list["oneOf"]:
                    if option.get("type") == "array":
                        has_array_type = True
                        break

            assert has_array_type, "Expected array type for list field"

    def test_generate_jsonschema_file_content_matches_schema_dict(self, test_directory):
        """Test that saved files match the returned schema dictionaries."""
        # GIVEN a CSV schema file
        # WHEN I generate schemas
        schemas, file_paths = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=["Patient"],
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN file content should match schema dict
        for schema, file_path in zip(schemas, file_paths):
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = json.load(f)
                assert file_content == schema

    def test_generate_jsonschema_creates_valid_json_schema_structure(self, tmp_path):
        """Test that generated schemas follow JSON Schema specification."""
        # GIVEN a CSV schema file and a temporary directory
        test_directory = str(tmp_path)  # pytest automatically handles cleanup

        # WHEN I generate schemas
        schemas, file_paths = generate_jsonschema(
            data_model_source=self.test_schema_path,
            output_directory=test_directory,
            data_type=None,
            data_model_labels="class_label",
            synapse_client=self.syn,
        )

        # THEN each schema should follow JSON Schema spec
        for schema in schemas:
            # Required JSON Schema fields
            assert "$schema" in schema, "Missing $schema field"
            assert "properties" in schema, "Missing properties field"

            # $schema should point to JSON Schema draft
            assert "json-schema.org" in schema["$schema"]

            # Properties should be a dictionary
            assert isinstance(schema["properties"], dict)

            # Each property should have valid structure
            for prop_name, prop_value in schema["properties"].items():
                assert isinstance(
                    prop_value, dict
                ), f"Property {prop_name} is not a dict"

        # AND all files should be in the temp directory
        for file_path in file_paths:
            assert file_path.startswith(
                test_directory
            ), f"File {file_path} was not created in temp directory {test_directory}"
