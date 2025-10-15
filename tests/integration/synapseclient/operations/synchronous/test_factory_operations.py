"""Integration tests for the synapseclient.models.factory_operations get function."""

import tempfile
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.api.table_services import ViewTypeMask
from synapseclient.core import utils
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    Dataset,
    DatasetCollection,
    EntityView,
    File,
    Folder,
    Link,
    MaterializedView,
    Project,
    SubmissionView,
    Table,
    UsedEntity,
    UsedURL,
    VirtualTable,
)
from synapseclient.operations import (
    ActivityOptions,
    FileOptions,
    LinkOptions,
    TableOptions,
    get,
)


class TestFactoryOperationsGetAsync:
    """Tests for the synapseclient.models.factory_operations.get method."""

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
            description="Test file for factory operations",
            content_type="text/plain",
            name=f"test_file_{str(uuid.uuid4())[:8]}.txt",
        )

    def create_activity(self) -> Activity:
        """Helper method to create a test activity."""
        return Activity(
            name="Test Activity",
            description="Activity for testing factory operations",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn123456", target_version_number=1),
            ],
        )

    async def test_get_project_by_id(self, project_model: Project) -> None:
        """Test retrieving a Project entity by Synapse ID."""
        # GIVEN a project exists
        project_id = project_model.id

        # WHEN I retrieve the project using get
        retrieved_project = get(synapse_id=project_id, synapse_client=self.syn)

        # THEN the correct Project entity is returned
        assert isinstance(retrieved_project, Project)
        assert retrieved_project.id == project_id
        assert retrieved_project.name == project_model.name
        assert retrieved_project.description == project_model.description
        assert retrieved_project.parent_id is not None
        assert retrieved_project.etag is not None
        assert retrieved_project.created_on is not None
        assert retrieved_project.modified_on is not None
        assert retrieved_project.created_by is not None
        assert retrieved_project.modified_by is not None

    async def test_get_project_by_name(self, project_model: Project) -> None:
        """Test retrieving a Project entity by name."""
        # GIVEN a project exists
        project_name = project_model.name

        # WHEN I retrieve the project using get with entity_name
        retrieved_project = get(
            entity_name=project_name, parent_id=None, synapse_client=self.syn
        )

        # THEN the correct Project entity is returned
        assert isinstance(retrieved_project, Project)
        assert retrieved_project.id == project_model.id
        assert retrieved_project.name == project_name

    async def test_get_folder_by_id(self, project_model: Project) -> None:
        """Test retrieving a Folder entity by Synapse ID."""
        # GIVEN a folder in a project
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            description="Test folder for factory operations",
            parent_id=project_model.id,
        )
        stored_folder = folder.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_folder.id)

        # WHEN I retrieve the folder using get
        retrieved_folder = get(synapse_id=stored_folder.id, synapse_client=self.syn)

        # THEN the correct Folder entity is returned
        assert isinstance(retrieved_folder, Folder)
        assert retrieved_folder.id == stored_folder.id
        assert retrieved_folder.name == stored_folder.name
        assert retrieved_folder.description == stored_folder.description
        assert retrieved_folder.parent_id == project_model.id
        assert retrieved_folder.etag is not None
        assert retrieved_folder.created_on is not None

    async def test_get_folder_by_name(self, project_model: Project) -> None:
        """Test retrieving a Folder entity by name and parent ID."""
        # GIVEN a folder in a project
        folder_name = f"test_folder_{str(uuid.uuid4())[:8]}"
        folder = Folder(
            name=folder_name,
            description="Test folder for factory operations",
            parent_id=project_model.id,
        )
        stored_folder = folder.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_folder.id)

        # WHEN I retrieve the folder using get with entity_name
        retrieved_folder = get(
            entity_name=folder_name, parent_id=project_model.id, synapse_client=self.syn
        )

        # THEN the correct Folder entity is returned
        assert isinstance(retrieved_folder, Folder)
        assert retrieved_folder.id == stored_folder.id
        assert retrieved_folder.name == folder_name

    async def test_get_file_by_id_default_options(self, project_model: Project) -> None:
        """Test retrieving a File entity by Synapse ID with default options."""
        # GIVEN a file in a project
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # WHEN I retrieve the file using get with default options
        retrieved_file = get(synapse_id=stored_file.id, synapse_client=self.syn)

        # THEN the correct File entity is returned with default behavior
        assert isinstance(retrieved_file, File)
        assert retrieved_file.id == stored_file.id
        assert retrieved_file.name == stored_file.name
        assert retrieved_file.path is not None  # File should be downloaded by default
        assert retrieved_file.download_file is True
        assert retrieved_file.data_file_handle_id is not None
        assert retrieved_file.file_handle is not None

    async def test_get_file_by_id_with_file_options(
        self, project_model: Project
    ) -> None:
        """Test retrieving a File entity by Synapse ID with custom FileOptions."""
        # GIVEN a file in a project
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND custom file download options
        with tempfile.TemporaryDirectory() as temp_dir:
            file_options = FileOptions(
                download_file=True,
                download_location=temp_dir,
                if_collision="overwrite.local",
            )

            # WHEN I retrieve the file using get with custom options
            retrieved_file = get(
                synapse_id=stored_file.id,
                file_options=file_options,
                synapse_client=self.syn,
            )

            # THEN the file is retrieved with the specified options
            assert isinstance(retrieved_file, File)
            assert retrieved_file.id == stored_file.id
            assert retrieved_file.download_file is True
            assert retrieved_file.if_collision == "overwrite.local"
            assert utils.normalize_path(temp_dir) in utils.normalize_path(
                retrieved_file.path
            )

    async def test_get_file_by_id_metadata_only(self, project_model: Project) -> None:
        """Test retrieving a File entity metadata without downloading."""
        # GIVEN a file in a project
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND file options to skip download
        file_options = FileOptions(download_file=False)

        # WHEN I retrieve the file using get without downloading
        retrieved_file = get(
            synapse_id=stored_file.id,
            file_options=file_options,
            synapse_client=self.syn,
        )

        # THEN the file metadata is retrieved without download
        assert isinstance(retrieved_file, File)
        assert retrieved_file.id == stored_file.id
        assert retrieved_file.download_file is False
        assert retrieved_file.data_file_handle_id is not None

    async def test_get_file_by_id_with_activity(self, project_model: Project) -> None:
        """Test retrieving a File entity with activity information."""
        # GIVEN a file with activity in a project
        file = self.create_file_instance()
        file.parent_id = project_model.id
        file.activity = self.create_activity()
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND activity options to include activity
        activity_options = ActivityOptions(include_activity=True)

        # WHEN I retrieve the file using get with activity options
        retrieved_file = get(
            synapse_id=stored_file.id,
            activity_options=activity_options,
            synapse_client=self.syn,
        )

        # THEN the file is retrieved with activity information
        assert isinstance(retrieved_file, File)
        assert retrieved_file.id == stored_file.id
        assert retrieved_file.activity is not None
        assert retrieved_file.activity.name == "Test Activity"
        assert (
            retrieved_file.activity.description
            == "Activity for testing factory operations"
        )

    async def test_get_file_by_id_specific_version(
        self, project_model: Project
    ) -> None:
        """Test retrieving a specific version of a File entity."""
        # GIVEN a file in a project
        file = self.create_file_instance()
        file.parent_id = project_model.id
        file.version_comment = "Version 1"
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND I update the file to create version 2
        file.version_comment = "Version 2"
        file.store(synapse_client=self.syn)

        # WHEN I retrieve version 1 specifically
        retrieved_file = get(
            synapse_id=stored_file.id, version_number=1, synapse_client=self.syn
        )

        # THEN version 1 is returned
        assert isinstance(retrieved_file, File)
        assert retrieved_file.id == stored_file.id
        assert retrieved_file.version_number == 1
        assert retrieved_file.version_comment == "Version 1"

    async def test_get_table_by_id_default_options(
        self, project_model: Project
    ) -> None:
        """Test retrieving a Table entity by Synapse ID with default options."""
        # GIVEN a table in a project
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
            Column(name="col2", column_type=ColumnType.INTEGER),
        ]
        table = Table(
            name=f"test_table_{str(uuid.uuid4())[:8]}",
            description="Test table for factory operations",
            parent_id=project_model.id,
            columns=columns,
        )
        stored_table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_table.id)

        # WHEN I retrieve the table using get
        retrieved_table = get(synapse_id=stored_table.id, synapse_client=self.syn)

        # THEN the correct Table entity is returned with columns
        assert isinstance(retrieved_table, Table)
        assert retrieved_table.id == stored_table.id
        assert retrieved_table.name == stored_table.name
        assert len(retrieved_table.columns) == 2
        assert any(col.name == "col1" for col in retrieved_table.columns.values())
        assert any(col.name == "col2" for col in retrieved_table.columns.values())

    async def test_get_table_by_id_with_table_options(
        self, project_model: Project
    ) -> None:
        """Test retrieving a Table entity with custom TableOptions."""
        # GIVEN a table in a project
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
            Column(name="col2", column_type=ColumnType.INTEGER),
        ]
        table = Table(
            name=f"test_table_{str(uuid.uuid4())[:8]}",
            description="Test table for factory operations",
            parent_id=project_model.id,
            columns=columns,
        )
        stored_table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_table.id)

        # AND table options to exclude columns
        table_options = TableOptions(include_columns=False)

        # WHEN I retrieve the table using get without columns
        retrieved_table = get(
            synapse_id=stored_table.id,
            table_options=table_options,
            synapse_client=self.syn,
        )

        # THEN the table is retrieved without column information
        assert isinstance(retrieved_table, Table)
        assert retrieved_table.id == stored_table.id
        assert len(retrieved_table.columns) == 0

    async def test_get_table_by_id_with_activity(self, project_model: Project) -> None:
        """Test retrieving a Table entity with activity information."""
        # GIVEN a table with activity in a project
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
        ]
        table = Table(
            name=f"test_table_{str(uuid.uuid4())[:8]}",
            description="Test table for factory operations",
            parent_id=project_model.id,
            columns=columns,
            activity=self.create_activity(),
        )
        stored_table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_table.id)

        # AND activity options to include activity
        activity_options = ActivityOptions(include_activity=True)

        # WHEN I retrieve the table using get with activity options
        retrieved_table = get(
            synapse_id=stored_table.id,
            activity_options=activity_options,
            synapse_client=self.syn,
        )

        # THEN the table is retrieved with activity information
        assert isinstance(retrieved_table, Table)
        assert retrieved_table.id == stored_table.id
        assert retrieved_table.activity is not None
        assert retrieved_table.activity.name == "Test Activity"

    async def test_get_dataset_by_id(self, project_model: Project) -> None:
        """Test retrieving a Dataset entity by Synapse ID."""
        # GIVEN a dataset in a project
        columns = [
            Column(name="itemId", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
        ]
        dataset = Dataset(
            name=f"test_dataset_{str(uuid.uuid4())[:8]}",
            description="Test dataset for factory operations",
            parent_id=project_model.id,
            columns=columns,
            include_default_columns=False,
        )
        stored_dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_dataset.id)

        # WHEN I retrieve the dataset using get
        retrieved_dataset = get(synapse_id=stored_dataset.id, synapse_client=self.syn)

        # THEN the correct Dataset entity is returned
        assert isinstance(retrieved_dataset, Dataset)
        assert retrieved_dataset.id == stored_dataset.id
        assert retrieved_dataset.name == stored_dataset.name
        assert len(retrieved_dataset.columns) == 2

    async def test_get_dataset_collection_by_id(self, project_model: Project) -> None:
        """Test retrieving a DatasetCollection entity by Synapse ID."""
        # GIVEN a dataset collection in a project
        dataset_collection = DatasetCollection(
            name=f"test_dataset_collection_{str(uuid.uuid4())[:8]}",
            description="Test dataset collection for factory operations",
            parent_id=project_model.id,
            include_default_columns=False,
        )
        stored_collection = dataset_collection.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_collection.id)

        # WHEN I retrieve the dataset collection using get
        retrieved_collection = get(
            synapse_id=stored_collection.id, synapse_client=self.syn
        )

        # THEN the correct DatasetCollection entity is returned
        assert isinstance(retrieved_collection, DatasetCollection)
        assert retrieved_collection.id == stored_collection.id
        assert retrieved_collection.name == stored_collection.name

    async def test_get_entity_view_by_id(self, project_model: Project) -> None:
        """Test retrieving an EntityView entity by Synapse ID."""
        # GIVEN an entity view in a project
        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
        ]
        entity_view = EntityView(
            name=f"test_entity_view_{str(uuid.uuid4())[:8]}",
            description="Test entity view for factory operations",
            parent_id=project_model.id,
            columns=columns,
            scope_ids=[project_model.id],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=False,
        )
        stored_view = entity_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_view.id)

        # WHEN I retrieve the entity view using get
        retrieved_view = get(synapse_id=stored_view.id, synapse_client=self.syn)

        # THEN the correct EntityView entity is returned
        assert isinstance(retrieved_view, EntityView)
        assert retrieved_view.id == stored_view.id
        assert retrieved_view.name == stored_view.name
        assert len(retrieved_view.columns) >= 2  # May include default columns

    async def test_get_submission_view_by_id(self, project_model: Project) -> None:
        """Test retrieving a SubmissionView entity by Synapse ID."""
        # GIVEN a submission view in a project
        columns = [
            Column(name="id", column_type=ColumnType.SUBMISSIONID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
        ]
        submission_view = SubmissionView(
            name=f"test_submission_view_{str(uuid.uuid4())[:8]}",
            description="Test submission view for factory operations",
            parent_id=project_model.id,
            columns=columns,
            scope_ids=[project_model.id],
            include_default_columns=False,
        )
        stored_view = submission_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_view.id)

        # WHEN I retrieve the submission view using get
        retrieved_view = get(synapse_id=stored_view.id, synapse_client=self.syn)

        # THEN the correct SubmissionView entity is returned
        assert isinstance(retrieved_view, SubmissionView)
        assert retrieved_view.id == stored_view.id
        assert retrieved_view.name == stored_view.name

    async def test_get_materialized_view_by_id(self, project_model: Project) -> None:
        """Test retrieving a MaterializedView entity by Synapse ID."""
        # GIVEN a simple table to create materialized view from
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
            Column(name="col2", column_type=ColumnType.INTEGER),
        ]
        source_table = Table(
            name=f"source_table_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
            columns=columns,
        )
        stored_source = source_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_source.id)

        # AND a materialized view
        materialized_view = MaterializedView(
            name=f"test_materialized_view_{str(uuid.uuid4())[:8]}",
            description="Test materialized view for factory operations",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {stored_source.id}",
        )
        stored_view = materialized_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_view.id)

        # WHEN I retrieve the materialized view using get
        retrieved_view = get(synapse_id=stored_view.id, synapse_client=self.syn)

        # THEN the correct MaterializedView entity is returned
        assert isinstance(retrieved_view, MaterializedView)
        assert retrieved_view.id == stored_view.id
        assert retrieved_view.name == stored_view.name
        assert retrieved_view.defining_sql is not None

    async def test_get_virtual_table_by_id(self, project_model: Project) -> None:
        """Test retrieving a VirtualTable entity by Synapse ID."""
        # GIVEN a simple table to create virtual table from
        columns = [
            Column(name="col1", column_type=ColumnType.STRING, maximum_size=50),
            Column(name="col2", column_type=ColumnType.INTEGER),
        ]
        source_table = Table(
            name=f"source_table_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
            columns=columns,
        )
        stored_source = source_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_source.id)

        # AND a virtual table
        virtual_table = VirtualTable(
            name=f"test_virtual_table_{str(uuid.uuid4())[:8]}",
            description="Test virtual table for factory operations",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {stored_source.id}",
        )
        stored_virtual = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_virtual.id)

        # WHEN I retrieve the virtual table using get
        retrieved_virtual = get(synapse_id=stored_virtual.id, synapse_client=self.syn)

        # THEN the correct VirtualTable entity is returned
        assert isinstance(retrieved_virtual, VirtualTable)
        assert retrieved_virtual.id == stored_virtual.id
        assert retrieved_virtual.name == stored_virtual.name
        assert retrieved_virtual.defining_sql is not None

    async def test_get_link_by_id_without_following(
        self, project_model: Project
    ) -> None:
        """Test retrieving a Link entity by Synapse ID without following the link."""
        # GIVEN a file and a link to that file
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        link = Link(
            name=f"test_link_{str(uuid.uuid4())[:8]}",
            description="Test link for factory operations",
            parent_id=project_model.id,
            target_id=stored_file.id,
        )
        stored_link = link.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_link.id)

        # AND link options to not follow the link
        link_options = LinkOptions(follow_link=False)

        # WHEN I retrieve the link using get without following
        retrieved_link = get(
            synapse_id=stored_link.id,
            link_options=link_options,
            synapse_client=self.syn,
        )

        # THEN the Link entity itself is returned
        assert isinstance(retrieved_link, Link)
        assert retrieved_link.id == stored_link.id
        assert retrieved_link.name == stored_link.name
        assert retrieved_link.target_id == stored_file.id

    async def test_get_link_by_id_default_follows_link(
        self, project_model: Project
    ) -> None:
        """Test that getting a Link by ID follows the link by default (no LinkOptions provided)."""
        # GIVEN a file and a link to that file
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        link = Link(
            name=f"test_link_{str(uuid.uuid4())[:8]}",
            description="Test link for factory operations",
            parent_id=project_model.id,
            target_id=stored_file.id,
        )
        stored_link = link.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_link.id)

        # WHEN I retrieve the link without any options (should use defaults)
        retrieved_entity = get(
            synapse_id=stored_link.id,
            synapse_client=self.syn,
        )

        # THEN the target File entity is returned (default follow_link=True behavior)
        assert isinstance(retrieved_entity, File)
        assert retrieved_entity.id == stored_file.id
        assert retrieved_entity.name == stored_file.name

    async def test_get_link_by_id_with_following(self, project_model: Project) -> None:
        """Test retrieving a Link entity by Synapse ID and following to the target (default behavior)."""
        # GIVEN a file and a link to that file
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        link = Link(
            name=f"test_link_{str(uuid.uuid4())[:8]}",
            description="Test link for factory operations",
            parent_id=project_model.id,
            target_id=stored_file.id,
        )
        stored_link = link.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_link.id)

        # WHEN I retrieve the link using get with default behavior (follow_link=True)
        retrieved_entity = get(
            synapse_id=stored_link.id,
            synapse_client=self.syn,
        )

        # THEN the target File entity is returned instead of the Link (default behavior is now follow_link=True)
        assert isinstance(retrieved_entity, File)
        assert retrieved_entity.id == stored_file.id
        assert retrieved_entity.name == stored_file.name

    async def test_get_link_by_id_with_following_explicit(
        self, project_model: Project
    ) -> None:
        """Test retrieving a Link entity by Synapse ID with explicit follow_link=True."""
        # GIVEN a file and a link to that file
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        link = Link(
            name=f"test_link_{str(uuid.uuid4())[:8]}",
            description="Test link for factory operations",
            parent_id=project_model.id,
            target_id=stored_file.id,
        )
        stored_link = link.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_link.id)

        # AND link options to follow the link
        link_options = LinkOptions(follow_link=True)

        # WHEN I retrieve the link using get with following
        retrieved_entity = get(
            synapse_id=stored_link.id,
            link_options=link_options,
            synapse_client=self.syn,
        )

        # THEN the target File entity is returned instead of the Link
        assert isinstance(retrieved_entity, File)
        assert retrieved_entity.id == stored_file.id
        assert retrieved_entity.name == stored_file.name

    async def test_get_link_by_id_with_file_options(
        self, project_model: Project
    ) -> None:
        """Test retrieving a Link entity that points to a File with custom file options."""
        # GIVEN a file and a link to that file
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        link = Link(
            name=f"test_link_{str(uuid.uuid4())[:8]}",
            description="Test link for factory operations",
            parent_id=project_model.id,
            target_id=stored_file.id,
        )
        stored_link = link.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_link.id)

        # AND custom file download options and link options
        with tempfile.TemporaryDirectory() as temp_dir:
            file_options = FileOptions(
                download_file=True,
                download_location=temp_dir,
                if_collision="overwrite.local",
            )
            link_options = LinkOptions(follow_link=True)

            # WHEN I retrieve the link using get with both link and file options
            retrieved_entity = get(
                synapse_id=stored_link.id,
                link_options=link_options,
                file_options=file_options,
                synapse_client=self.syn,
            )

            # THEN the target File entity is returned with the custom options applied
            assert isinstance(retrieved_entity, File)
            assert retrieved_entity.id == stored_file.id
            assert retrieved_entity.name == stored_file.name
            assert utils.normalize_path(temp_dir) in utils.normalize_path(
                retrieved_entity.path
            )
            assert retrieved_entity.download_file is True

    async def test_get_with_entity_instance(self, project_model: Project) -> None:
        """Test get when passing an entity instance directly."""
        # GIVEN an existing File entity instance
        file = self.create_file_instance()
        file.parent_id = project_model.id
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND file options to change behavior
        file_options = FileOptions(download_file=False)

        # WHEN I pass the entity instance to get with new options
        refreshed_file = get(
            stored_file, file_options=file_options, synapse_client=self.syn
        )

        # THEN the entity is refreshed with the new options applied
        assert isinstance(refreshed_file, File)
        assert refreshed_file.id == stored_file.id
        assert refreshed_file.download_file is False

    async def test_get_combined_options(self, project_model: Project) -> None:
        """Test get with multiple option types combined."""
        # GIVEN a file with activity
        file = self.create_file_instance()
        file.parent_id = project_model.id
        file.activity = self.create_activity()
        stored_file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(stored_file.id)

        # AND combined options
        activity_options = ActivityOptions(include_activity=True)
        file_options = FileOptions(download_file=False)

        # WHEN I retrieve the file with combined options
        retrieved_file = get(
            synapse_id=stored_file.id,
            activity_options=activity_options,
            file_options=file_options,
            synapse_client=self.syn,
        )

        # THEN both options are applied
        assert isinstance(retrieved_file, File)
        assert retrieved_file.id == stored_file.id
        assert retrieved_file.download_file is False
        assert retrieved_file.activity is not None
        assert retrieved_file.activity.name == "Test Activity"

    async def test_get_invalid_synapse_id_raises_error(self) -> None:
        """Test that get raises appropriate error for invalid Synapse ID."""
        # GIVEN an invalid synapse ID
        invalid_id = "syn999999999999"

        # WHEN I try to retrieve the entity
        # THEN an appropriate error is raised
        with pytest.raises(Exception):  # Could be SynapseNotFoundError or similar
            get(synapse_id=invalid_id, synapse_client=self.syn)

    async def test_get_invalid_entity_name_raises_error(
        self, project_model: Project
    ) -> None:
        """Test that get raises appropriate error for invalid entity name."""
        # GIVEN an invalid entity name
        invalid_name = f"nonexistent_entity_{str(uuid.uuid4())}"

        # WHEN I try to retrieve the entity by name
        # THEN an appropriate error is raised
        with pytest.raises(Exception):  # Could be SynapseNotFoundError or similar
            get(
                entity_name=invalid_name,
                parent_id=project_model.id,
                synapse_client=self.syn,
            )

    async def test_get_validation_errors(self) -> None:
        """Test validation errors for invalid parameter combinations."""
        # WHEN I provide both synapse_id and entity_name
        # THEN ValueError is raised
        with pytest.raises(
            ValueError, match="Cannot specify both synapse_id and entity_name"
        ):
            get(
                synapse_id="syn123456",
                entity_name="test_entity",
                synapse_client=self.syn,
            )

        # WHEN I provide neither synapse_id nor entity_name
        # THEN ValueError is raised
        with pytest.raises(
            ValueError, match="Must specify either synapse_id or entity_name"
        ):
            get(synapse_client=self.syn)
