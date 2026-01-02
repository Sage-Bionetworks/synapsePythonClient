"""Integration tests for the synapseclient.operations.store function."""

import os
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.api.table_services import ViewTypeMask
from synapseclient.core import utils
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    CurationTask,
    Dataset,
    DatasetCollection,
    EntityView,
    Evaluation,
    File,
    Folder,
    FormData,
    FormGroup,
    Grid,
    JSONSchema,
    Link,
    MaterializedView,
    Project,
    RecordBasedMetadataTaskProperties,
    RecordSet,
    SchemaOrganization,
    SubmissionView,
    Table,
    Team,
    UsedEntity,
    UsedURL,
    VirtualTable,
)
from synapseclient.operations import (
    StoreContainerOptions,
    StoreFileOptions,
    StoreGridOptions,
    StoreJSONSchemaOptions,
    StoreTableOptions,
    delete,
    store,
)


class TestFactoryOperationsStore:
    """Tests for the synapseclient.operations.store function."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_file_instance(self) -> File:
        """Helper method to create a test file."""
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        return File(
            path=filename,
            description="Test file for store factory operations",
            content_type="text/plain",
            name=f"test_file_{str(uuid.uuid4())[:8]}.txt",
        )

    def create_activity(self) -> Activity:
        """Helper method to create a test activity."""
        return Activity(
            name="Test Activity",
            description="Activity for testing store factory operations",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn123456", target_version_number=1),
            ],
        )

    def test_store_project_basic(self) -> None:
        """Test storing a Project entity."""
        # GIVEN a new project
        project = Project(
            name=f"test_project_{str(uuid.uuid4())[:8]}",
            description="Test project for store factory operations",
        )

        # WHEN I store the project using store
        stored_project = store(project, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_project.id)

        # THEN the project is created in Synapse
        assert stored_project.id is not None
        assert stored_project.name == project.name
        assert stored_project.description == project.description
        assert stored_project.etag is not None

        # WHEN I delete the project using delete
        delete(stored_project, synapse_client=self.syn)

        # THEN the project should no longer be retrievable
        with pytest.raises(Exception):
            Project(id=stored_project.id).get(synapse_client=self.syn)

    def test_store_project_with_container_options(self) -> None:
        """Test storing a Project with container options."""
        # GIVEN a new project with container options
        project = Project(
            name=f"test_project_{str(uuid.uuid4())[:8]}",
            description="Test project with container options",
        )
        container_options = StoreContainerOptions(failure_strategy="LOG_EXCEPTION")

        # WHEN I store the project with options
        stored_project = store(
            project,
            container_options=container_options,
            synapse_client=self.syn,
        )
        self.schedule_for_cleanup(stored_project.id)

        # THEN the project is created successfully
        assert stored_project.id is not None
        assert stored_project.name == project.name

    def test_store_folder_basic(self, project_model: Project) -> None:
        """Test storing a Folder entity."""
        # GIVEN a new folder
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            description="Test folder for store factory operations",
            parent_id=project_model.id,
        )

        # WHEN I store the folder using store
        stored_folder = store(folder, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_folder.id)

        # THEN the folder is created in Synapse with all fields
        assert stored_folder.id is not None
        assert stored_folder.name == folder.name
        assert stored_folder.description == folder.description
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.etag is not None

        # WHEN I delete the folder using delete
        delete(stored_folder, synapse_client=self.syn)

        # THEN the folder should no longer be retrievable
        with pytest.raises(Exception):
            Folder(id=stored_folder.id).get(synapse_client=self.syn)

    def test_store_folder_with_parent_param(self, project_model: Project) -> None:
        """Test storing a Folder with parent parameter."""
        # GIVEN a new folder without parent_id set
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            description="Test folder with parent parameter",
        )
        # Verify parent_id is not set initially
        assert folder.parent_id is None

        # WHEN I store the folder with parent parameter
        stored_folder = store(folder, parent=project_model, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_folder.id)

        # THEN the folder is created with the correct parent
        assert stored_folder.id is not None
        # Verify the parent parameter was applied
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.name == folder.name

    def test_store_file_basic(self, project_model: Project) -> None:
        """Test storing a File entity."""
        # GIVEN a new file
        file = self.create_file_instance()
        file.parent_id = project_model.id

        # WHEN I store the file using store
        stored_file = store(file, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # THEN the file is uploaded to Synapse with all fields
        assert stored_file.id is not None
        assert stored_file.name == file.name
        assert stored_file.description == file.description
        assert stored_file.parent_id == project_model.id
        assert stored_file.data_file_handle_id is not None
        assert stored_file.version_number == 1
        assert stored_file.etag is not None

        # WHEN I delete the file using delete
        delete(stored_file, synapse_client=self.syn)

        # THEN the file should no longer be retrievable (would raise 404)
        with pytest.raises(Exception):
            File(id=stored_file.id).get(synapse_client=self.syn)

    def test_store_and_delete_file_by_id_string(self, project_model: Project) -> None:
        """Test storing a File and deleting it using a string ID."""
        # GIVEN a new file
        file = self.create_file_instance()
        file.parent_id = project_model.id

        # WHEN I store the file using store
        stored_file = store(file, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # THEN the file is uploaded to Synapse
        assert stored_file.id is not None

        # WHEN I delete the file using a string ID
        delete(stored_file.id, synapse_client=self.syn)

        # THEN the file should no longer be retrievable
        with pytest.raises(Exception):
            File(id=stored_file.id).get(synapse_client=self.syn)

    def test_store_file_with_file_options(self, project_model: Project) -> None:
        """Test storing a File with custom file options."""
        # GIVEN a file with custom options
        file = self.create_file_instance()
        file.parent_id = project_model.id
        file_options = StoreFileOptions(
            synapse_store=True,
            content_type="application/json",
            merge_existing_annotations=True,
        )

        # WHEN I store the file with options
        stored_file = store(file, file_options=file_options, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # THEN the file is stored with the specified options
        assert stored_file.id is not None
        # Verify content_type was applied
        assert stored_file.content_type == "application/json"
        # Verify file was stored in Synapse (has file handle)
        assert stored_file.data_file_handle_id is not None

    def test_store_file_update(self, project_model: Project) -> None:
        """Test updating an existing File entity."""
        # GIVEN an existing file
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = store(file, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)
        original_version = stored_file.version_number

        # WHEN I update the file description and store again
        stored_file.description = "Updated description"
        updated_file = store(stored_file, synapse_client=self.syn)

        # THEN the file is updated
        assert updated_file.id == stored_file.id
        assert updated_file.description == "Updated description"
        # Verify version number incremented
        assert updated_file.version_number == original_version + 1

    def test_store_recordset_basic(self, project_model: Project) -> None:
        """Test storing a RecordSet entity."""
        # GIVEN a new recordset
        recordset_file = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(recordset_file)

        recordset = RecordSet(
            name=f"test_recordset_{str(uuid.uuid4())[:8]}",
            description="Test recordset for store factory operations",
            parent_id=project_model.id,
            path=recordset_file,
            upsert_keys=["id"],
        )

        # WHEN I store the recordset using store
        stored_recordset = store(recordset, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_recordset.id)

        # THEN the recordset is created in Synapse
        assert stored_recordset.id is not None
        assert stored_recordset.name == recordset.name
        assert stored_recordset.description == recordset.description
        # Verify RecordSet was stored as a file with data
        assert stored_recordset.data_file_handle_id is not None
        assert stored_recordset.parent_id == project_model.id
        assert stored_recordset.etag is not None

    def test_store_link_basic(self, project_model: Project) -> None:
        """Test storing a Link entity."""
        # GIVEN a file to link to
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = store(file, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND a new link
        link = Link(
            name=f"test_link_{str(uuid.uuid4())[:8]}",
            description="Test link for store factory operations",
            parent_id=project_model.id,
            target_id=stored_file.id,
        )

        # WHEN I store the link using store
        stored_link = store(link, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_link.id)

        # THEN the link is created in Synapse
        assert stored_link.id is not None
        assert stored_link.name == link.name
        assert stored_link.description == link.description
        # Verify link points to the correct target
        assert stored_link.target_id == stored_file.id
        assert stored_link.parent_id == project_model.id
        assert stored_link.etag is not None

    def test_store_table_basic(self, project_model: Project) -> None:
        """Test storing a Table entity."""
        # GIVEN a new table
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
            Column(name="col2", column_type=ColumnType.INTEGER),
        ]
        table = Table(
            name=f"test_table_{str(uuid.uuid4())[:8]}",
            description="Test table for store factory operations",
            parent_id=project_model.id,
            columns=columns,
        )

        # WHEN I store the table using store
        stored_table = store(table, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_table.id)

        # THEN the table is created with columns
        assert stored_table.id is not None
        assert stored_table.name == table.name
        assert stored_table.description == table.description
        assert stored_table.parent_id == project_model.id
        assert len(stored_table.columns) == 2
        # Verify column details were preserved (columns is a dict)
        assert "col1" in stored_table.columns
        assert "col2" in stored_table.columns
        assert stored_table.columns["col1"].column_type == ColumnType.STRING
        assert stored_table.columns["col2"].column_type == ColumnType.INTEGER
        assert stored_table.etag is not None

        # WHEN I delete the table using delete
        delete(stored_table, synapse_client=self.syn)

        # THEN the table should no longer be retrievable
        with pytest.raises(Exception):
            Table(id=stored_table.id).get(synapse_client=self.syn)

    def test_store_table_with_table_options(self, project_model: Project) -> None:
        """Test storing a Table with custom table options."""
        # GIVEN a table with options
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
        ]
        table = Table(
            name=f"test_table_{str(uuid.uuid4())[:8]}",
            description="Test table with options",
            parent_id=project_model.id,
            columns=columns,
        )
        table_options = StoreTableOptions(dry_run=False, job_timeout=600)

        # WHEN I store the table with options
        stored_table = store(
            table, table_options=table_options, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_table.id)

        # THEN the table is created (dry_run=False means it persists)
        assert stored_table.id is not None
        assert stored_table.id.startswith("syn")
        # Verify table was actually created with columns (columns is a dict)
        assert len(stored_table.columns) == 1
        assert "col1" in stored_table.columns
        assert stored_table.columns["col1"].id is not None

    def test_store_dataset_basic(self, project_model: Project) -> None:
        """Test storing a Dataset entity."""
        # GIVEN a new dataset
        columns = [
            Column(name="itemId", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
        ]
        dataset = Dataset(
            name=f"test_dataset_{str(uuid.uuid4())[:8]}",
            description="Test dataset for store factory operations",
            parent_id=project_model.id,
            columns=columns,
            include_default_columns=False,
        )

        # WHEN I store the dataset using store
        stored_dataset = store(dataset, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_dataset.id)

        # THEN the dataset is created with all fields
        assert stored_dataset.id is not None
        assert stored_dataset.name == dataset.name
        assert stored_dataset.description == dataset.description
        assert stored_dataset.parent_id == project_model.id
        assert len(stored_dataset.columns) == 2
        # Verify columns are accessible by name (columns is a dict)
        assert "itemId" in stored_dataset.columns
        assert "name" in stored_dataset.columns
        assert stored_dataset.columns["itemId"].column_type == ColumnType.ENTITYID
        assert stored_dataset.columns["name"].column_type == ColumnType.STRING
        assert stored_dataset.etag is not None

        # WHEN I delete the dataset using delete
        delete(stored_dataset, synapse_client=self.syn)

        # THEN the dataset should no longer be retrievable
        with pytest.raises(Exception):
            Dataset(id=stored_dataset.id).get(synapse_client=self.syn)

    def test_store_dataset_collection_basic(self, project_model: Project) -> None:
        """Test storing a DatasetCollection entity."""
        # GIVEN a new dataset collection
        dataset_collection = DatasetCollection(
            name=f"test_dataset_collection_{str(uuid.uuid4())[:8]}",
            description="Test dataset collection for store factory operations",
            parent_id=project_model.id,
            include_default_columns=False,
        )

        # WHEN I store the dataset collection using store
        stored_collection = store(dataset_collection, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_collection.id)

        # THEN the dataset collection is created with all fields
        assert stored_collection.id is not None
        assert stored_collection.name == dataset_collection.name
        assert stored_collection.description == dataset_collection.description
        assert stored_collection.parent_id == project_model.id
        assert stored_collection.etag is not None

    def test_store_entity_view_basic(self, project_model: Project) -> None:
        """Test storing an EntityView entity."""
        # GIVEN a new entity view
        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
        ]
        entity_view = EntityView(
            name=f"test_entity_view_{str(uuid.uuid4())[:8]}",
            description="Test entity view for store factory operations",
            parent_id=project_model.id,
            columns=columns,
            scope_ids=[project_model.id],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=False,
        )

        # WHEN I store the entity view using store
        stored_view = store(entity_view, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_view.id)

        # THEN the entity view is created with all fields
        assert stored_view.id is not None
        assert stored_view.name == entity_view.name
        assert stored_view.description == entity_view.description
        assert stored_view.parent_id == project_model.id
        assert len(stored_view.columns) == 2
        # Verify columns are accessible by name (columns is a dict)
        assert "id" in stored_view.columns
        assert "name" in stored_view.columns
        # scope_ids is returned as a set, convert both for comparison
        assert set(stored_view.scope_ids) == {project_model.id}
        assert stored_view.view_type_mask == ViewTypeMask.FILE
        assert stored_view.etag is not None

    def test_store_submission_view_basic(self, project_model: Project) -> None:
        """Test storing a SubmissionView entity."""
        # GIVEN a new submission view
        columns = [
            Column(name="id", column_type=ColumnType.SUBMISSIONID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
        ]
        submission_view = SubmissionView(
            name=f"test_submission_view_{str(uuid.uuid4())[:8]}",
            description="Test submission view for store factory operations",
            parent_id=project_model.id,
            columns=columns,
            scope_ids=[project_model.id],
            include_default_columns=False,
        )

        # WHEN I store the submission view using store
        stored_view = store(submission_view, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_view.id)

        # THEN the submission view is created with all fields
        assert stored_view.id is not None
        assert stored_view.name == submission_view.name
        assert stored_view.description == submission_view.description
        assert stored_view.parent_id == project_model.id
        assert len(stored_view.columns) == 2
        # Verify columns are accessible by name (columns is a dict)
        assert "id" in stored_view.columns
        assert "name" in stored_view.columns
        assert stored_view.columns["id"].column_type == ColumnType.SUBMISSIONID
        # scope_ids may have numeric strings without 'syn' prefix
        assert len(stored_view.scope_ids) == 1
        # Check if it matches with or without the 'syn' prefix
        scope_id = stored_view.scope_ids[0]
        assert scope_id == project_model.id or scope_id == project_model.id.replace(
            "syn", ""
        )
        assert stored_view.etag is not None

    def test_store_materialized_view_basic(self, project_model: Project) -> None:
        """Test storing a MaterializedView entity."""
        # GIVEN a source table for the materialized view
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
        ]
        source_table = Table(
            name=f"source_table_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
            columns=columns,
        )
        stored_source = store(source_table, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_source.id)

        # AND a new materialized view
        materialized_view = MaterializedView(
            name=f"test_materialized_view_{str(uuid.uuid4())[:8]}",
            description="Test materialized view for store factory operations",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {stored_source.id}",
        )

        # WHEN I store the materialized view using store
        stored_view = store(materialized_view, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_view.id)

        # THEN the materialized view is created with all fields
        assert stored_view.id is not None
        assert stored_view.name == materialized_view.name
        assert stored_view.description == materialized_view.description
        assert stored_view.parent_id == project_model.id
        assert stored_view.defining_sql == materialized_view.defining_sql
        assert stored_view.etag is not None

    def test_store_virtual_table_basic(self, project_model: Project) -> None:
        """Test storing a VirtualTable entity."""
        # GIVEN a source table for the virtual table
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
        ]
        source_table = Table(
            name=f"source_table_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
            columns=columns,
        )
        stored_source = store(source_table, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_source.id)

        # AND a new virtual table
        virtual_table = VirtualTable(
            name=f"test_virtual_table_{str(uuid.uuid4())[:8]}",
            description="Test virtual table for store factory operations",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {stored_source.id}",
        )

        # WHEN I store the virtual table using store
        stored_virtual = store(virtual_table, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_virtual.id)

        # THEN the virtual table is created with all fields
        assert stored_virtual.id is not None
        assert stored_virtual.name == virtual_table.name
        assert stored_virtual.description == virtual_table.description
        assert stored_virtual.parent_id == project_model.id
        assert stored_virtual.defining_sql == virtual_table.defining_sql
        assert stored_virtual.etag is not None

    def test_store_evaluation_basic(self, project_model: Project) -> None:
        """Test storing an Evaluation entity."""
        # GIVEN a new evaluation
        evaluation = Evaluation(
            name=f"test_evaluation_{str(uuid.uuid4())[:8]}",
            description="Test evaluation for store factory operations",
            content_source=project_model.id,
            submission_instructions_message="Instructions for submission",
            submission_receipt_message="Thank you for your submission",
        )

        # WHEN I store the evaluation using store
        stored_evaluation = store(evaluation, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_evaluation.id)

        # THEN the evaluation is created
        assert stored_evaluation.id is not None
        assert stored_evaluation.name == evaluation.name
        # Verify evaluation properties were preserved
        assert stored_evaluation.description == evaluation.description
        assert stored_evaluation.content_source == project_model.id

    def test_store_team_basic(self) -> None:
        """Test storing a Team entity."""
        # GIVEN a new team
        team = Team(
            name=f"test_team_{str(uuid.uuid4())[:8]}",
            description="Test team for store factory operations",
        )

        # WHEN I store the team using store
        stored_team = store(team, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_team.id)

        # THEN the team is created with all fields
        assert stored_team.id is not None
        assert stored_team.name == team.name
        assert stored_team.description == team.description
        assert stored_team.etag is not None

        # WHEN I delete the team using delete
        delete(stored_team, synapse_client=self.syn)

        # THEN the team should no longer be retrievable
        with pytest.raises(Exception):
            Team(id=stored_team.id).get(synapse_client=self.syn)

    def test_store_curation_task_basic(self, project_model: Project) -> None:
        """Test storing a CurationTask entity."""
        # GIVEN a folder for the curation task
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # AND a RecordSet
        test_data = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        temp_fd, filename = tempfile.mkstemp(suffix=".csv")
        try:
            os.close(temp_fd)
            test_data.to_csv(filename, index=False)
            self.schedule_for_cleanup(filename)

            record_set = RecordSet(
                name=f"test_recordset_{str(uuid.uuid4())[:8]}",
                parent_id=folder.id,
                path=filename,
                upsert_keys=["col1"],
            ).store(synapse_client=self.syn)
            self.schedule_for_cleanup(record_set.id)
        except Exception:
            if os.path.exists(filename):
                os.unlink(filename)
            raise

        # AND task properties
        task_properties = RecordBasedMetadataTaskProperties(
            record_set_id=record_set.id,
        )

        # AND a new curation task
        curation_task = CurationTask(
            project_id=project_model.id,
            data_type="test_data_type",
            instructions="Test instructions for curation task",
            task_properties=task_properties,
        )

        # WHEN I store the curation task using store
        stored_task = store(curation_task, synapse_client=self.syn)

        # THEN the curation task is created with all fields
        assert stored_task.task_id is not None
        assert stored_task.project_id == project_model.id
        assert stored_task.instructions == "Test instructions for curation task"
        assert stored_task.etag is not None

    def test_store_schema_organization_basic(self) -> None:
        """Test storing a SchemaOrganization entity."""
        # GIVEN a new schema organization
        # Name must have each part start with a letter
        schema_org = SchemaOrganization(
            name=f"test.schema.org.test{str(uuid.uuid4())[:8]}",
        )

        # WHEN I store the schema organization using store
        stored_org = store(schema_org, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_org.id)

        # THEN the schema organization is created with all fields
        assert stored_org.id is not None
        assert stored_org.name == schema_org.name
        assert stored_org.created_on is not None

        # WHEN I delete the schema organization using delete
        delete(stored_org, synapse_client=self.syn)

        # THEN the schema organization should no longer be retrievable
        with pytest.raises(Exception):
            SchemaOrganization(organization_name=stored_org.name).get(
                synapse_client=self.syn
            )

    def test_store_unsupported_entity_raises_error(self) -> None:
        """Test that storing an unsupported entity type raises an error."""
        # GIVEN an unsupported entity type (using a dict as a proxy)
        invalid_entity = {"type": "InvalidEntity"}

        # WHEN/THEN storing the invalid entity raises ValueError
        with pytest.raises(ValueError, match="Unsupported entity type"):
            store(invalid_entity, synapse_client=self.syn)

    def test_store_file_with_activity(self, project_model: Project) -> None:
        """Test storing a File with activity."""
        # GIVEN a file with activity
        file = self.create_file_instance()
        file.parent_id = project_model.id
        activity = self.create_activity()
        file.activity = activity

        # WHEN I store the file
        stored_file = store(file, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # THEN the file is stored with activity
        assert stored_file.id is not None
        assert stored_file.activity is not None
        # Verify activity details were preserved
        assert stored_file.activity.name == activity.name
        assert stored_file.activity.description == activity.description
        assert len(stored_file.activity.used) == 2

    def test_store_table_dry_run(self, project_model: Project) -> None:
        """Test storing a Table with dry_run option."""
        # GIVEN a table and dry_run options
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
        ]
        table = Table(
            name=f"test_table_{str(uuid.uuid4())[:8]}",
            description="Test table for dry run",
            parent_id=project_model.id,
            columns=columns,
        )
        table_options = StoreTableOptions(dry_run=True)

        # WHEN I store with dry_run=True
        result = store(table, table_options=table_options, synapse_client=self.syn)

        # THEN the result is returned but no actual entity is created
        # dry_run validates the table structure without persisting
        assert result is not None
        # In dry run mode, the table should either have no ID or the same ID if updating
        # The key is that it doesn't create a new permanent entity

    def test_store_container_with_raise_exception_strategy(
        self, project_model: Project
    ) -> None:
        """Test storing a container with RAISE_EXCEPTION failure strategy."""
        # GIVEN a folder with failure strategy option
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            description="Test folder with raise exception",
            parent_id=project_model.id,
        )
        container_options = StoreContainerOptions(failure_strategy="RAISE_EXCEPTION")

        # WHEN I store the folder with RAISE_EXCEPTION
        stored_folder = store(
            folder,
            container_options=container_options,
            synapse_client=self.syn,
        )
        self.schedule_for_cleanup(stored_folder.id)

        # THEN the folder is stored successfully
        assert stored_folder.id is not None

    def test_store_form_group_basic(self, project_model: Project) -> None:
        """Test storing a FormGroup entity."""
        # GIVEN a new form group
        form_group = FormGroup(
            name=f"test_form_group_{str(uuid.uuid4())[:8]}",
        )

        # WHEN I store the form group using store
        stored_form_group = store(form_group, synapse_client=self.syn)

        # THEN the form group is created
        assert stored_form_group.group_id is not None
        assert stored_form_group.name == form_group.name

    def test_store_form_data_basic(self, project_model: Project) -> None:
        """Test storing a FormData entity."""
        # GIVEN a form group first
        form_group = FormGroup(
            name=f"test_form_group_{str(uuid.uuid4())[:8]}",
        )
        stored_form_group = store(form_group, synapse_client=self.syn)

        # AND a file to get a file handle ID
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = store(file, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND form data using the file handle
        form_data = FormData(
            group_id=stored_form_group.group_id,
            name=f"test_form_data_{str(uuid.uuid4())[:8]}",
            data_file_handle_id=stored_file.data_file_handle_id,
        )

        # WHEN I store the form data using store
        stored_form_data = store(form_data, synapse_client=self.syn)

        # THEN the form data is created
        assert stored_form_data.form_data_id is not None
        assert stored_form_data.group_id == stored_form_group.group_id
        assert stored_form_data.name == form_data.name

    def test_store_json_schema_basic(self) -> None:
        """Test storing a JSONSchema entity."""
        # GIVEN a schema organization first
        schema_org = SchemaOrganization(
            name=f"test.schema.org.test{str(uuid.uuid4())[:8]}",
        )
        stored_org = store(schema_org, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_org.id)

        # AND a JSON schema
        json_schema = JSONSchema(
            organization_name=stored_org.name,
            name="TestSchema",
        )
        schema_body = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
        }
        json_schema_options = StoreJSONSchemaOptions(
            schema_body=schema_body,
            version="1.0.0",
        )

        # WHEN I store the JSON schema using store
        stored_schema = store(
            json_schema,
            json_schema_options=json_schema_options,
            synapse_client=self.syn,
        )

        # THEN the JSON schema is created
        assert stored_schema.created_on is not None
        assert stored_schema.organization_name == stored_org.name
        assert stored_schema.name == "TestSchema"

        # WHEN I delete the JSON schema using delete
        delete(stored_schema, synapse_client=self.syn)

        # THEN the JSON schema should no longer be retrievable
        with pytest.raises(Exception):
            JSONSchema(
                organization_name=stored_schema.organization_name,
                name=stored_schema.name,
            ).get(synapse_client=self.syn)

        # Clean up the schema organization
        delete(stored_org, synapse_client=self.syn)

    def test_store_grid_basic(self, project_model: Project) -> None:
        """Test storing a Grid entity."""
        # GIVEN a RecordSet first
        recordset_file = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(recordset_file)

        recordset = RecordSet(
            name=f"test_recordset_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
            path=recordset_file,
            upsert_keys=["id"],
        )
        stored_recordset = store(recordset, synapse_client=self.syn)
        self.schedule_for_cleanup(stored_recordset.id)

        # AND a Grid
        grid = Grid(
            record_set_id=stored_recordset.id,
        )
        grid_options = StoreGridOptions(
            attach_to_previous_session=False,
            timeout=120,
        )

        # WHEN I store the grid using store
        stored_grid = store(
            grid,
            grid_options=grid_options,
            synapse_client=self.syn,
        )

        # THEN the grid is created
        assert stored_grid.session_id is not None
        assert stored_grid.record_set_id == stored_recordset.id

        # WHEN I delete the grid using delete
        delete(stored_grid, synapse_client=self.syn)
        # Grid deletion is fire-and-forget, no need to verify
