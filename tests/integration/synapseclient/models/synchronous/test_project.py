"""Integration tests for the synapseclient.models.Project class."""

import os
import uuid
from typing import Callable, List

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder, Project

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
        stored_project = project.store()
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
        stored_project_with_annotations = project_with_annotations.store()
        self.schedule_for_cleanup(project_with_annotations.id)

        # THEN I expect the stored Project to have the expected properties and annotations
        self.verify_project_properties(stored_project_with_annotations)
        assert stored_project_with_annotations.annotations == annotations
        assert (
            Project(id=stored_project_with_annotations.id).get(synapse_client=self.syn)
        ).annotations == annotations

    async def test_store_project_with_files(self, file: File, project: Project) -> None:
        # Test Case 1: Project with a single file
        # GIVEN a File on the project
        project.files.append(file)

        # WHEN I store the Project on Synapse
        stored_project = project.store()
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
        stored_project_multiple_files = project_multiple_files.store()
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
        stored_project = project.store()
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
        existing_project = existing_project.store()
        self.schedule_for_cleanup(existing_project.id)

        # AND a Folder with a File under the project
        folder = Folder(name=str(uuid.uuid4()))
        folder.files.append(file)
        existing_project.folders.append(folder)

        # WHEN I store the Project on Synapse
        stored_existing_project = existing_project.store()

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
        stored_project = project.store()
        self.schedule_for_cleanup(project.id)

        # Test Case 1: Get project by ID
        # WHEN I get the Project from Synapse by ID
        project_by_id = Project(id=stored_project.id).get(synapse_client=self.syn)

        # THEN I expect the retrieved Project to have the expected properties
        self.verify_project_properties(project_by_id)

        # Test Case 2: Get project by name attribute
        # WHEN I get the Project from Synapse by name
        project_by_name = Project(name=stored_project.name).get(synapse_client=self.syn)

        # THEN I expect the retrieved Project to have the expected properties
        self.verify_project_properties(project_by_name)

    async def test_delete_project(self, project: Project) -> None:
        # GIVEN a Project object stored in Synapse
        stored_project = project.store()
        self.schedule_for_cleanup(project.id)

        # WHEN I delete the Project from Synapse
        stored_project.delete()

        # THEN I expect the project to have been deleted
        with pytest.raises(SynapseHTTPError) as e:
            stored_project.get(synapse_client=self.syn)

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
        stored_source_project = source_project.store()
        self.schedule_for_cleanup(stored_source_project.id)

        # Test Case 1: Copy project with all contents
        # Create first destination project
        destination_project_1 = Project(
            name=str(uuid.uuid4()), description="Destination for project copy 1"
        ).store()
        self.schedule_for_cleanup(destination_project_1.id)

        # WHEN I copy the project to the destination project
        copied_project = stored_source_project.copy(
            destination_id=destination_project_1.id
        )

        # AND I sync the destination project from Synapse
        destination_project_1.sync_from_synapse(recursive=False, download_file=False)

        # THEN I expect the copied Project to have the expected properties
        assert len(destination_project_1.files) == 3
        assert len(destination_project_1.folders) == 2
        self.verify_copied_project(copied_project, stored_source_project)

        # Test Case 2: Copy project excluding files
        # Create a second destination project for the second test case
        destination_project_2 = Project(
            name=str(uuid.uuid4()), description="Destination for project copy 2"
        ).store()
        self.schedule_for_cleanup(destination_project_2.id)

        # WHEN I copy the project to the destination project excluding files
        copied_project_no_files = stored_source_project.copy(
            destination_id=destination_project_2.id, exclude_types=["file"]
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
        stored_project = project.store()
        self.schedule_for_cleanup(project.id)

        # AND I sync the project from Synapse
        copied_project = stored_project.sync_from_synapse(path=root_directory_path)

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
