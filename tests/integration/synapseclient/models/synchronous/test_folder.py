"""Integration tests for the synapseclient.models.Folder class."""

import os
import uuid
from typing import Callable, List

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder, Project

DESCRIPTION_FOLDER = "This is an example folder."
DESCRIPTION_FILE = "This is an example file."
CONTENT_TYPE = "text/plain"


class TestFolderStore:
    """Tests for the synapseclient.models.Folder.store method."""

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
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    def verify_folder_properties(
        self,
        folder: Folder,
        parent_id: str,
        expected_files: list = None,
        expected_folders: list = None,
    ):
        """Helper method to verify folder properties"""
        assert folder.id is not None
        assert folder.name is not None
        assert folder.parent_id == parent_id
        assert folder.description is not None
        assert folder.etag is not None
        assert folder.created_on is not None
        assert folder.modified_on is not None
        assert folder.created_by is not None
        assert folder.modified_by is not None

        if expected_files is None:
            assert folder.files == []
        else:
            assert folder.files == expected_files
            # Verify files properties
            for file in folder.files:
                assert file.id is not None
                assert file.name is not None
                assert file.parent_id == folder.id
                assert file.path is not None

        if expected_folders is None:
            assert folder.folders == []
        else:
            assert folder.folders == expected_folders
            # Verify sub-folders properties
            for sub_folder in folder.folders:
                assert sub_folder.id is not None
                assert sub_folder.name is not None
                assert sub_folder.parent_id == folder.id
                # Verify files in sub-folders
                for sub_file in sub_folder.files:
                    assert sub_file.id is not None
                    assert sub_file.name is not None
                    assert sub_file.parent_id == sub_folder.id
                    assert sub_file.path is not None

        assert isinstance(folder.annotations, dict)

    async def test_store_folder_variations(
        self, project_model: Project, folder: Folder
    ) -> None:
        # Test Case 1: Simple folder storage
        # GIVEN a Folder object and a Project object

        # WHEN I store the Folder on Synapse
        stored_folder = folder.store(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties
        self.verify_folder_properties(stored_folder, project_model.id)
        assert not stored_folder.annotations

        # Test Case 2: Folder with annotations
        # GIVEN a Folder object with annotations
        folder_with_annotations = Folder(
            name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER
        )
        annotations = {
            "my_single_key_string": ["a"],
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        folder_with_annotations.annotations = annotations

        # WHEN I store the Folder on Synapse
        stored_folder_with_annotations = folder_with_annotations.store(
            parent=project_model
        )
        self.schedule_for_cleanup(folder_with_annotations.id)

        # THEN I expect the stored Folder to have the expected properties and annotations
        self.verify_folder_properties(stored_folder_with_annotations, project_model.id)
        assert stored_folder_with_annotations.annotations == annotations
        assert (
            Folder(id=stored_folder_with_annotations.id).get(synapse_client=self.syn)
        ).annotations == annotations

    async def test_store_folder_with_files(
        self, project_model: Project, file: File, folder: Folder
    ) -> None:
        # Test Case 1: Folder with a single file
        # GIVEN a File on the folder
        folder.files.append(file)

        # WHEN I store the Folder on Synapse
        stored_folder = folder.store(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties and files
        self.verify_folder_properties(
            stored_folder, project_model.id, expected_files=[file]
        )

        # Test Case 2: Folder with multiple files
        # GIVEN a folder with multiple files
        folder_with_multiple_files = Folder(
            name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER
        )
        files = self.create_files(3)
        folder_with_multiple_files.files = files

        # WHEN I store the Folder on Synapse
        stored_folder_with_multiple_files = folder_with_multiple_files.store(
            parent=project_model
        )
        self.schedule_for_cleanup(folder_with_multiple_files.id)

        # THEN I expect the stored Folder to have the expected properties and files
        self.verify_folder_properties(
            stored_folder_with_multiple_files, project_model.id, expected_files=files
        )

    async def test_store_folder_with_files_and_folders(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a folder with nested structure (files and sub-folders with files)
        files = self.create_files(3)
        folder.files = files

        # Create sub-folders with files
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            sub_folder.files = self.create_files(2)
            folders.append(sub_folder)
        folder.folders = folders

        # WHEN I store the Folder on Synapse
        stored_folder = folder.store(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties, files, and folders
        self.verify_folder_properties(
            stored_folder,
            project_model.id,
            expected_files=files,
            expected_folders=folders,
        )


class TestFolderGetDelete:
    """Tests for the synapseclient.models.Folder.get and delete methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    async def test_get_folder_methods(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder object stored in Synapse
        stored_folder = folder.store(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Test Case 1: Get folder by ID
        # WHEN I get the Folder from Synapse by ID
        folder_by_id = Folder(id=stored_folder.id).get(synapse_client=self.syn)

        # THEN I expect the retrieved Folder to have the expected properties
        self.verify_folder_properties(folder_by_id, project_model.id)

        # Test Case 2: Get folder by name and parent_id attribute
        # WHEN I get the Folder from Synapse by name and parent_id
        folder_by_name_parent_id = Folder(
            name=stored_folder.name, parent_id=stored_folder.parent_id
        ).get(synapse_client=self.syn)

        # THEN I expect the retrieved Folder to have the expected properties
        self.verify_folder_properties(folder_by_name_parent_id, project_model.id)

        # Test Case 3: Get folder by name and parent object
        # WHEN I get the Folder from Synapse by name and parent object
        folder_by_name_parent = Folder(name=stored_folder.name).get(
            parent=project_model, synapse_client=self.syn
        )

        # THEN I expect the retrieved Folder to have the expected properties
        self.verify_folder_properties(folder_by_name_parent, project_model.id)

    def verify_folder_properties(self, folder: Folder, parent_id: str):
        """Helper method to verify folder properties"""
        assert folder.id is not None
        assert folder.name is not None
        assert folder.parent_id == parent_id
        assert folder.description is not None
        assert folder.etag is not None
        assert folder.created_on is not None
        assert folder.modified_on is not None
        assert folder.created_by is not None
        assert folder.modified_by is not None
        assert folder.files == []
        assert folder.folders == []
        assert not folder.annotations and isinstance(folder.annotations, dict)

    async def test_delete_folder(self, project_model: Project, folder: Folder) -> None:
        # GIVEN a Folder object stored in Synapse
        stored_folder = folder.store(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # WHEN I delete the Folder from Synapse
        stored_folder.delete()

        # THEN I expect the folder to have been deleted
        with pytest.raises(SynapseHTTPError) as e:
            stored_folder.get()

        assert f"404 Client Error: Entity {stored_folder.id} is in trash can." in str(
            e.value
        )


class TestFolderCopy:
    """Tests for the synapseclient.models.Folder.copy method."""

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
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    def create_nested_folder(self) -> Folder:
        """Helper method to create a folder with files and subfolders"""
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)

        # Add files to folder
        folder.files = self.create_files(3)

        # Add subfolders with files
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            sub_folder.files = self.create_files(2)
            folders.append(sub_folder)
        folder.folders = folders

        return folder

    def verify_copied_folder(
        self,
        copied_folder: Folder,
        original_folder: Folder,
        expected_files_empty: bool = False,
    ):
        """Helper method to verify copied folder properties"""
        assert copied_folder.id is not None
        assert copied_folder.id != original_folder.id
        assert copied_folder.name == original_folder.name
        assert copied_folder.parent_id != original_folder.parent_id
        assert copied_folder.description == original_folder.description
        assert copied_folder.etag is not None
        assert copied_folder.created_on is not None
        assert copied_folder.modified_on is not None
        assert copied_folder.created_by is not None
        assert copied_folder.modified_by is not None
        assert copied_folder.annotations == original_folder.annotations

        if expected_files_empty:
            assert copied_folder.files == []
        else:
            assert len(copied_folder.files) == len(original_folder.files)
            for file in copied_folder.files:
                assert file.id is not None
                assert file.name is not None
                assert file.parent_id == copied_folder.id

        if len(copied_folder.folders) > 0:
            for i, sub_folder in enumerate(copied_folder.folders):
                assert sub_folder.id is not None
                assert sub_folder.name is not None
                assert sub_folder.parent_id == copied_folder.id

                if expected_files_empty:
                    assert sub_folder.files == []
                else:
                    for sub_file in sub_folder.files:
                        assert sub_file.id is not None
                        assert sub_file.name is not None
                        assert sub_file.parent_id == sub_folder.id

    async def test_copy_folder_with_files_and_folders(
        self, project_model: Project
    ) -> None:
        # GIVEN a nested folder structure with files and folders
        source_folder = self.create_nested_folder()
        source_folder.annotations = {"test": ["test"]}
        stored_source_folder = source_folder.store(parent=project_model)
        self.schedule_for_cleanup(stored_source_folder.id)

        # Test Case 1: Copy folder with all contents
        # Create first destination folder
        destination_folder_1 = Folder(
            name=str(uuid.uuid4()), description="Destination for folder copy 1"
        ).store(parent=project_model)
        self.schedule_for_cleanup(destination_folder_1.id)

        # WHEN I copy the folder to the destination folder
        copied_folder = stored_source_folder.copy(parent_id=destination_folder_1.id)

        # AND I sync the destination folder from Synapse
        destination_folder_1.sync_from_synapse(recursive=False, download_file=False)

        # THEN I expect the copied Folder to have the expected properties
        assert len(destination_folder_1.folders) == 1
        assert destination_folder_1.folders == [copied_folder]
        self.verify_copied_folder(copied_folder, stored_source_folder)

        # Test Case 2: Copy folder excluding files
        # Create a second destination folder for the second test case
        destination_folder_2 = Folder(
            name=str(uuid.uuid4()), description="Destination for folder copy 2"
        ).store(parent=project_model)
        self.schedule_for_cleanup(destination_folder_2.id)

        # WHEN I copy the folder to the destination folder excluding files
        copied_folder_no_files = stored_source_folder.copy(
            parent_id=destination_folder_2.id, exclude_types=["file"]
        )

        # AND I sync the destination folder from Synapse
        destination_folder_2.sync_from_synapse(recursive=False, download_file=False)

        # THEN I expect the copied Folder to have the expected properties but no files
        assert len(destination_folder_2.folders) == 1
        assert destination_folder_2.folders == [copied_folder_no_files]
        self.verify_copied_folder(
            copied_folder_no_files, stored_source_folder, expected_files_empty=True
        )


class TestFolderSyncFromSynapse:
    """Tests for the synapseclient.models.Folder.sync_from_synapse method."""

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
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    async def test_sync_from_synapse(
        self, project_model: Project, file: File, folder: Folder
    ) -> None:
        # GIVEN a nested folder structure with files and folders
        root_directory_path = os.path.dirname(file.path)

        # Add files to folder
        folder.files = self.create_files(3)

        # Add subfolders with files
        sub_folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            sub_folder.files = self.create_files(2)
            sub_folders.append(sub_folder)
        folder.folders = sub_folders

        # WHEN I store the Folder on Synapse
        stored_folder = folder.store(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # AND I sync the folder from Synapse
        copied_folder = stored_folder.sync_from_synapse(path=root_directory_path)

        # THEN I expect that the folder and its contents are synced from Synapse to disk
        # Verify files in root folder
        for file in copied_folder.files:
            assert os.path.exists(file.path)
            assert os.path.isfile(file.path)
            assert (
                utils.md5_for_file(file.path).hexdigest()
                == file.file_handle.content_md5
            )

        # Verify subfolders and their files
        for sub_folder in stored_folder.folders:
            resolved_path = os.path.join(root_directory_path, sub_folder.name)
            assert os.path.exists(resolved_path)
            assert os.path.isdir(resolved_path)

            for sub_file in sub_folder.files:
                assert os.path.exists(sub_file.path)
                assert os.path.isfile(sub_file.path)
                assert (
                    utils.md5_for_file(sub_file.path).hexdigest()
                    == sub_file.file_handle.content_md5
                )

    async def test_sync_all_entity_types(self, project_model: Project) -> None:
        """Test syncing a folder with all supported entity types."""
        # GIVEN a folder with one of each entity type
        from synapseclient.models import (
            Column,
            ColumnType,
            Dataset,
            DatasetCollection,
            EntityRef,
            EntityView,
            File,
            MaterializedView,
            SubmissionView,
            Table,
            ViewTypeMask,
            VirtualTable,
        )

        # Create the folder first
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())}",
            parent_id=project_model.id,
        )
        folder = folder.store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # Create and store a File
        file = File(
            name=f"test_file_{str(uuid.uuid4())}.txt",
            parent_id=folder.id,
            path=utils.make_bogus_uuid_file(),
        )
        file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # Create and store a nested Folder
        nested_folder = Folder(
            name=f"test_nested_folder_{str(uuid.uuid4())}",
            parent_id=folder.id,
        )
        nested_folder = nested_folder.store(synapse_client=self.syn)
        self.schedule_for_cleanup(nested_folder.id)

        # Create and store a Table
        table = Table(
            name=f"test_table_{str(uuid.uuid4())}",
            parent_id=folder.id,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
        )
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # Create and store an EntityView
        entity_view = EntityView(
            name=f"test_entityview_{str(uuid.uuid4())}",
            parent_id=folder.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=True,
        )
        entity_view = entity_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        # Create and store a MaterializedView
        materialized_view = MaterializedView(
            name=f"test_materializedview_{str(uuid.uuid4())}",
            parent_id=folder.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        materialized_view = materialized_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(materialized_view.id)

        # Create and store a VirtualTable
        virtual_table = VirtualTable(
            name=f"test_virtualtable_{str(uuid.uuid4())}",
            parent_id=folder.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )
        virtual_table = virtual_table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(virtual_table.id)

        # Create and store a Dataset (reusing the existing file)
        dataset = Dataset(
            name=f"test_dataset_{str(uuid.uuid4())}",
            parent_id=folder.id,
            items=[EntityRef(id=file.id, version=1)],
        )
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # Create and store a DatasetCollection
        dataset_collection = DatasetCollection(
            name=f"test_datasetcollection_{str(uuid.uuid4())}",
            parent_id=folder.id,
            items=[EntityRef(id=dataset.id, version=1)],
        )
        dataset_collection = dataset_collection.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # Create and store a SubmissionView
        submission_view = SubmissionView(
            name=f"test_submissionview_{str(uuid.uuid4())}",
            parent_id=folder.id,
            scope_ids=[folder.id],
        )
        submission_view = submission_view.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submission_view.id)

        # WHEN I sync the folder from Synapse
        synced_folder = folder.sync_from_synapse(recursive=False, download_file=False)

        # THEN all entity types should be present
        assert len(synced_folder.files) == 1
        assert synced_folder.files[0].id == file.id
        assert synced_folder.files[0].name == file.name

        assert len(synced_folder.folders) == 1
        assert synced_folder.folders[0].id == nested_folder.id
        assert synced_folder.folders[0].name == nested_folder.name

        assert len(synced_folder.tables) == 1
        assert synced_folder.tables[0].id == table.id
        assert synced_folder.tables[0].name == table.name

        assert len(synced_folder.entityviews) == 1
        assert synced_folder.entityviews[0].id == entity_view.id
        assert synced_folder.entityviews[0].name == entity_view.name

        assert len(synced_folder.materializedviews) == 1
        assert synced_folder.materializedviews[0].id == materialized_view.id
        assert synced_folder.materializedviews[0].name == materialized_view.name

        assert len(synced_folder.virtualtables) == 1
        assert synced_folder.virtualtables[0].id == virtual_table.id
        assert synced_folder.virtualtables[0].name == virtual_table.name

        assert len(synced_folder.datasets) == 1
        assert synced_folder.datasets[0].id == dataset.id
        assert synced_folder.datasets[0].name == dataset.name

        assert len(synced_folder.datasetcollections) == 1
        assert synced_folder.datasetcollections[0].id == dataset_collection.id
        assert synced_folder.datasetcollections[0].name == dataset_collection.name

        assert len(synced_folder.submissionviews) == 1
        assert synced_folder.submissionviews[0].id == submission_view.id
        assert synced_folder.submissionviews[0].name == submission_view.name
