"""Integration tests for the synapseclient.models.Project class."""

import os
import uuid
from typing import Callable, List

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Column,
    ColumnType,
    Dataset,
    DatasetCollection,
    EntityRef,
    EntityView,
    File,
    Folder,
    MaterializedView,
    Project,
    Table,
    ViewTypeMask,
    VirtualTable,
)

CONTENT_TYPE = "text/plain"
DESCRIPTION_FILE = "This is an example file."
DESCRIPTION_PROJECT = "This is an example project."


class TestProjectStore:
    """Tests for the synapseclient.models.Project.store method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_file_instance(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(
            path=filename,
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE,
        )

    def create_files(self, count: int) -> List[File]:
        """Helper method to create multiple file instances"""
        return [
            self.create_file_instance(self.schedule_for_cleanup) for _ in range(count)
        ]

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        return self.create_file_instance(schedule_for_cleanup)

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT)
        return project

    def verify_project_properties(
        self,
        project: Project,
        expected_files: list = None,
        expected_folders: list = None,
    ):
        """Helper method to verify project properties"""
        assert project.id is not None
        assert project.name is not None
        assert project.parent_id is not None
        assert project.description is not None
        assert project.etag is not None
        assert project.created_on is not None
        assert project.modified_on is not None
        assert project.created_by is not None
        assert project.modified_by is not None

        if expected_files is None:
            assert project.files == []
        else:
            assert project.files == expected_files
            # Verify files properties
            for file in project.files:
                assert file.id is not None
                assert file.name is not None
                assert file.parent_id == project.id
                assert file.path is not None

        if expected_folders is None:
            assert project.folders == []
        else:
            assert project.folders == expected_folders
            # Verify folders properties
            for folder in project.folders:
                assert folder.id is not None
                assert folder.name is not None
                assert folder.parent_id == project.id

                # Verify files in folders
                for sub_file in folder.files:
                    assert sub_file.id is not None
                    assert sub_file.name is not None
                    assert sub_file.parent_id == folder.id
                    assert sub_file.path is not None

        # Only check for empty annotations if this is a basic project test without files or folders
        # and there are no expected annotation values from the test case
        if (
            not expected_files
            and not expected_folders
            and "my_key_" not in str(project.annotations)
        ):
            assert not project.annotations and isinstance(project.annotations, dict)

    async def test_store_project_basic(self, project: Project) -> None:
        # Test Case 1: Basic project storage
        # GIVEN a Project object

        # WHEN I store the Project on Synapse
        stored_project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties
        self.verify_project_properties(stored_project)

        # Test Case 2: Project with annotations
        # GIVEN a Project object with annotations
        project_with_annotations = Project(
            name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT
        )
        annotations = {
            "my_single_key_string": ["a"],
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        project_with_annotations.annotations = annotations

        # WHEN I store the Project on Synapse
        stored_project_with_annotations = await project_with_annotations.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(project_with_annotations.id)

        # THEN I expect the stored Project to have the expected properties and annotations
        self.verify_project_properties(stored_project_with_annotations)
        assert stored_project_with_annotations.annotations == annotations
        assert (
            await Project(id=stored_project_with_annotations.id).get_async(
                synapse_client=self.syn
            )
        ).annotations == annotations

    async def test_store_project_with_files(self, file: File, project: Project) -> None:
        # Test Case 1: Project with a single file
        # GIVEN a File on the project
        project.files.append(file)

        # WHEN I store the Project on Synapse
        stored_project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties and files
        self.verify_project_properties(stored_project, expected_files=[file])

        # Test Case 2: Project with multiple files
        # GIVEN multiple files in a project
        project_multiple_files = Project(
            name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT
        )
        files = self.create_files(3)
        project_multiple_files.files = files

        # WHEN I store the Project on Synapse
        stored_project_multiple_files = await project_multiple_files.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(project_multiple_files.id)

        # THEN I expect the stored Project to have the expected properties and files
        self.verify_project_properties(
            stored_project_multiple_files, expected_files=files
        )

    async def test_store_project_with_nested_structure(
        self, file: File, project: Project
    ) -> None:
        # GIVEN a project with files and folders

        # Create files for the project
        project_files = self.create_files(3)
        project.files = project_files

        # Create folders with files
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            sub_folder.files = self.create_files(2)
            folders.append(sub_folder)
        project.folders = folders

        # WHEN I store the Project on Synapse
        stored_project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties, files, and folders
        self.verify_project_properties(
            stored_project, expected_files=project_files, expected_folders=folders
        )

        # Test Case 2: Store with existing project and nested structure
        # GIVEN that a project is already stored in Synapse
        existing_project = Project(
            name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT
        )
        existing_project = await existing_project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(existing_project.id)

        # AND a Folder with a File under the project
        folder = Folder(name=str(uuid.uuid4()))
        folder.files.append(file)
        existing_project.folders.append(folder)

        # WHEN I store the Project on Synapse
        stored_existing_project = await existing_project.store_async(
            synapse_client=self.syn
        )

        # THEN I expect the stored Project to have the expected properties
        self.verify_project_properties(
            stored_existing_project, expected_folders=[folder]
        )

        # AND I expect the Folder to be stored in Synapse
        assert folder.id is not None
        assert folder.name is not None
        assert folder.parent_id == stored_existing_project.id

        # AND I expect the File to be stored on Synapse
        assert file.id is not None
        assert file.name is not None
        assert file.parent_id == folder.id
        assert file.path is not None


class TestProjectGetDelete:
    """Tests for the synapseclient.models.Project.get and delete methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT)
        return project

    def verify_project_properties(self, project: Project):
        """Helper method to verify project properties"""
        assert project.id is not None
        assert project.name is not None
        assert project.parent_id is not None
        assert project.description is not None
        assert project.etag is not None
        assert project.created_on is not None
        assert project.modified_on is not None
        assert project.created_by is not None
        assert project.modified_by is not None
        assert project.files == []
        assert project.folders == []
        assert not project.annotations and isinstance(project.annotations, dict)

    async def test_get_project_methods(self, project: Project) -> None:
        # GIVEN a Project object stored in Synapse
        stored_project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # Test Case 1: Get project by ID
        # WHEN I get the Project from Synapse by ID
        project_by_id = await Project(id=stored_project.id).get_async(
            synapse_client=self.syn
        )

        # THEN I expect the retrieved Project to have the expected properties
        self.verify_project_properties(project_by_id)

        # Test Case 2: Get project by name attribute
        # WHEN I get the Project from Synapse by name
        project_by_name = await Project(name=stored_project.name).get_async(
            synapse_client=self.syn
        )

        # THEN I expect the retrieved Project to have the expected properties
        self.verify_project_properties(project_by_name)

    async def test_delete_project(self, project: Project) -> None:
        # GIVEN a Project object stored in Synapse
        stored_project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # WHEN I delete the Project from Synapse
        await stored_project.delete_async(synapse_client=self.syn)

        # THEN I expect the project to have been deleted
        with pytest.raises(SynapseHTTPError) as e:
            await stored_project.get_async(synapse_client=self.syn)

        assert f"404 Client Error: Entity {stored_project.id} is in trash can." in str(
            e.value
        )


class TestProjectCopySync:
    """Tests for the synapseclient.models.Project.copy and sync_from_synapse methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_file_instance(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(
            path=filename,
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE,
        )

    def create_files(self, count: int) -> List[File]:
        """Helper method to create multiple file instances"""
        return [
            self.create_file_instance(self.schedule_for_cleanup) for _ in range(count)
        ]

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        return self.create_file_instance(schedule_for_cleanup)

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT)
        return project

    def create_nested_project(self) -> Project:
        """Helper method to create a project with files and folders"""
        project = Project(name=str(uuid.uuid4()), description=DESCRIPTION_PROJECT)

        # Add files to project
        project.files = self.create_files(3)

        # Add folders with files
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            sub_folder.files = self.create_files(2)
            folders.append(sub_folder)
        project.folders = folders

        # Add annotations
        project.annotations = {"test": ["test"]}

        return project

    def verify_copied_project(
        self,
        copied_project: Project,
        original_project: Project,
        expected_files_empty: bool = False,
    ):
        """Helper method to verify copied project properties"""
        assert copied_project.id is not None
        assert copied_project.id is not original_project.id
        assert copied_project.name is not None
        assert copied_project.parent_id is not None
        assert copied_project.description is not None
        assert copied_project.etag is not None
        assert copied_project.created_on is not None
        assert copied_project.modified_on is not None
        assert copied_project.created_by is not None
        assert copied_project.modified_by is not None
        assert copied_project.annotations == original_project.annotations

        if expected_files_empty:
            assert copied_project.files == []
        else:
            assert len(copied_project.files) == len(original_project.files)
            for file in copied_project.files:
                assert file.id is not None
                assert file.name is not None
                assert file.parent_id == copied_project.id

        if len(copied_project.folders) > 0:
            for folder in copied_project.folders:
                assert folder.id is not None
                assert folder.name is not None
                assert folder.parent_id == copied_project.id

                for sub_file in folder.files:
                    assert sub_file.id is not None
                    assert sub_file.name is not None
                    assert sub_file.parent_id == folder.id

    async def test_copy_project_variations(self) -> None:
        # GIVEN a nested source project and a destination project
        source_project = self.create_nested_project()
        stored_source_project = await source_project.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_source_project.id)

        # Test Case 1: Copy project with all contents
        # Create first destination project
        destination_project_1 = await Project(
            name=str(uuid.uuid4()), description="Destination for project copy 1"
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(destination_project_1.id)

        # WHEN I copy the project to the destination project
        copied_project = await stored_source_project.copy_async(
            destination_id=destination_project_1.id, synapse_client=self.syn
        )

        # AND I sync the destination project from Synapse
        await destination_project_1.sync_from_synapse_async(
            recursive=False, download_file=False, synapse_client=self.syn
        )

        # THEN I expect the copied Project to have the expected properties
        assert len(destination_project_1.files) == 3
        assert len(destination_project_1.folders) == 2
        self.verify_copied_project(copied_project, stored_source_project)

        # Test Case 2: Copy project excluding files
        # Create a second destination project for the second test case
        destination_project_2 = await Project(
            name=str(uuid.uuid4()), description="Destination for project copy 2"
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(destination_project_2.id)

        # WHEN I copy the project to the destination project excluding files
        copied_project_no_files = await stored_source_project.copy_async(
            destination_id=destination_project_2.id,
            exclude_types=["file"],
            synapse_client=self.syn,
        )

        # THEN I expect the copied Project to have the expected properties but no files
        self.verify_copied_project(
            copied_project_no_files, stored_source_project, expected_files_empty=True
        )

    async def test_sync_from_synapse(self, file: File) -> None:
        # GIVEN a nested project structure
        root_directory_path = os.path.dirname(file.path)

        project = self.create_nested_project()

        # WHEN I store the Project on Synapse
        stored_project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # AND I sync the project from Synapse
        copied_project = await stored_project.sync_from_synapse_async(
            path=root_directory_path, synapse_client=self.syn
        )

        # THEN I expect that the project and its contents are synced from Synapse to disk
        # Verify files in root folder
        for file in copied_project.files:
            assert os.path.exists(file.path)
            assert os.path.isfile(file.path)
            assert (
                utils.md5_for_file(file.path).hexdigest()
                == file.file_handle.content_md5
            )

        # Verify folders and their files
        for folder in stored_project.folders:
            resolved_path = os.path.join(root_directory_path, folder.name)
            assert os.path.exists(resolved_path)
            assert os.path.isdir(resolved_path)

            for sub_file in folder.files:
                assert os.path.exists(sub_file.path)
                assert os.path.isfile(sub_file.path)
                assert (
                    utils.md5_for_file(sub_file.path).hexdigest()
                    == sub_file.file_handle.content_md5
                )

    async def test_sync_all_entity_types(self) -> None:
        """Test syncing a project with all supported entity types."""
        # GIVEN a project with one of each entity type

        # Create a unique project for this test
        project_model = Project(
            name=f"test_sync_project_{str(uuid.uuid4())}",
            description="Test project for sync all entity types",
        )
        project_model = await project_model.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_model.id)

        # Create and store a Table
        table = Table(
            name=f"test_table_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Create and store an EntityView
        entity_view = EntityView(
            name=f"test_entityview_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            scope_ids=[project_model.id],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=True,
        )
        entity_view = await entity_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        # Create and store a MaterializedView
        materialized_view = MaterializedView(
            name=f"test_materializedview_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = await materialized_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # Create and store a VirtualTable
        virtual_table = VirtualTable(
            name=f"test_virtualtable_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = await virtual_table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Create and store a File for the dataset
        file = File(
            name=f"test_file_{str(uuid.uuid4())}.txt",
            parent_id=project_model.id,
            path=utils.make_bogus_uuid_file(),
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # Create and store a Dataset
        dataset = Dataset(
            name=f"test_dataset_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            items=[EntityRef(id=file.id, version=1)],
        )
        dataset = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # Create and store a DatasetCollection
        dataset_collection = DatasetCollection(
            name=f"test_dataset_collection_{str(uuid.uuid4())}",
            parent_id=project_model.id,
            items=[EntityRef(id=dataset.id, version=1)],
        )
        dataset_collection = await dataset_collection.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I sync the project from Synapse
        synced_project = await project_model.sync_from_synapse_async(
            recursive=False, download_file=False, synapse_client=self.syn
        )

        # THEN all entity types should be present
        assert len(synced_project.tables) == 1
        assert synced_project.tables[0].id == table.id
        assert synced_project.tables[0].name == table.name

        assert len(synced_project.entityviews) == 1
        assert synced_project.entityviews[0].id == entity_view.id
        assert synced_project.entityviews[0].name == entity_view.name

        assert len(synced_project.materializedviews) == 1
        assert synced_project.materializedviews[0].id == materialized_view.id
        assert synced_project.materializedviews[0].name == materialized_view.name

        assert len(synced_project.virtualtables) == 1
        assert synced_project.virtualtables[0].id == virtual_table.id
        assert synced_project.virtualtables[0].name == virtual_table.name

        assert len(synced_project.datasets) == 1
        assert synced_project.datasets[0].id == dataset.id
        assert synced_project.datasets[0].name == dataset.name

        assert len(synced_project.datasetcollections) == 1
        assert synced_project.datasetcollections[0].id == dataset_collection.id
        assert synced_project.datasetcollections[0].name == dataset_collection.name

        # Verify that submission views are empty (since we didn't create any evaluation queues)
        assert len(synced_project.submissionviews) == 0


class TestProjectWalk:
    """Tests for the walk_async methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init(
        self, syn_with_logger: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        self.syn = syn_with_logger
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_file_instance(self, schedule_for_cleanup: Callable[..., None]) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(
            path=filename,
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE,
        )

    async def create_test_hierarchy(self, project: Project) -> dict:
        """Create a test hierarchy for walk testing."""
        # Create root level folder and file
        root_folder = Folder(
            name=f"root_folder_{str(uuid.uuid4())[:8]}", parent_id=project.id
        )
        root_folder = await root_folder.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(root_folder.id)

        root_file = self.create_file_instance(self.schedule_for_cleanup)
        root_file.parent_id = project.id
        root_file = await root_file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(root_file.id)

        # Create nested folder and file
        nested_folder = Folder(
            name=f"nested_folder_{str(uuid.uuid4())[:8]}", parent_id=root_folder.id
        )
        nested_folder = await nested_folder.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(nested_folder.id)

        nested_file = self.create_file_instance(self.schedule_for_cleanup)
        nested_file.parent_id = nested_folder.id
        nested_file = await nested_file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(nested_file.id)

        return {
            "project": project,
            "root_folder": root_folder,
            "root_file": root_file,
            "nested_folder": nested_folder,
            "nested_file": nested_file,
        }

    async def test_walk_async_recursive_true(self) -> None:
        """Test walk_async method with recursive=True."""
        # GIVEN: A unique project with a hierarchical structure
        project_model = Project(
            name=f"integration_test_project{str(uuid.uuid4())}",
            description=DESCRIPTION_PROJECT,
        )
        project_model = await project_model.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_model.id)
        hierarchy = await self.create_test_hierarchy(project_model)

        # WHEN: Walking through the project asynchronously with recursive=True
        results = []
        async for result in project_model.walk_async(
            recursive=True, synapse_client=self.syn
        ):
            results.append(result)

        # THEN: Should get 3 results (project root, root_folder, nested_folder)
        assert len(results) == 3

        # AND: Project root result should contain correct structure
        project_result = results[0]
        dirpath, dirs, nondirs = project_result
        assert dirpath[0] == hierarchy["project"].name
        assert dirpath[1] == hierarchy["project"].id
        assert len(dirs) == 1  # root_folder
        assert len(nondirs) == 1  # root_file

        # AND: All returned objects should be EntityHeader instances
        assert hasattr(dirs[0], "name")
        assert hasattr(dirs[0], "id")
        assert hasattr(dirs[0], "type")
        assert hasattr(nondirs[0], "name")
        assert hasattr(nondirs[0], "id")
        assert hasattr(nondirs[0], "type")

        # AND: Should be able to find nested content
        nested_results = [r for r in results if "nested_folder" in r[0][0]]
        assert len(nested_results) == 1
        _, nested_dirs, nested_nondirs = nested_results[0]
        assert len(nested_dirs) == 0
        assert len(nested_nondirs) == 1  # nested_file

        # AND: Nested objects should also be EntityHeader instances
        assert hasattr(nested_nondirs[0], "name")
        assert hasattr(nested_nondirs[0], "id")
        assert hasattr(nested_nondirs[0], "type")

    async def test_walk_async_recursive_false(self) -> None:
        """Test walk_async method with recursive=False."""
        # GIVEN: A unique project with a hierarchical structure
        project_model = Project(
            name=f"integration_test_project{str(uuid.uuid4())}",
            description=DESCRIPTION_PROJECT,
        )
        project_model = await project_model.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project_model.id)
        hierarchy = await self.create_test_hierarchy(project_model)

        # WHEN: Walking through the project asynchronously with recursive=False
        results = []
        async for result in project_model.walk_async(
            recursive=False, synapse_client=self.syn
        ):
            results.append(result)

        # THEN: Should get only 1 result (project root only)
        assert len(results) == 1

        # AND: Project root should contain direct children only
        dirpath, dirs, nondirs = results[0]
        assert dirpath[0] == hierarchy["project"].name
        assert dirpath[1] == hierarchy["project"].id
        assert len(dirs) == 1  # root_folder
        assert len(nondirs) == 1  # root_file

        # AND: All returned objects should be EntityHeader instances
        assert hasattr(dirs[0], "name")
        assert hasattr(dirs[0], "id")
        assert hasattr(dirs[0], "type")
        assert hasattr(nondirs[0], "name")
        assert hasattr(nondirs[0], "id")
        assert hasattr(nondirs[0], "type")
