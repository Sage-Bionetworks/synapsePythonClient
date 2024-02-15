"""Integration tests for the synapseclient.models.Project class."""

import os
import uuid

from typing import Callable
import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError

from synapseclient.models import Project, File, Folder

CONTENT_TYPE = "text/plain"
DESCRIPTION_FILE = "This is an example file."
DERSCRIPTION_PROJECT = "This is an example project."


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

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        return self.create_file_instance(schedule_for_cleanup)

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DERSCRIPTION_PROJECT)
        return project

    @pytest.mark.asyncio
    async def test_store_project(self, project: Project) -> None:
        # GIVEN a Project object

        # WHEN I store the Project on Synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties
        assert stored_project.id is not None
        assert stored_project.name is not None
        assert stored_project.parent_id is not None
        assert stored_project.description is not None
        assert stored_project.etag is not None
        assert stored_project.created_on is not None
        assert stored_project.modified_on is not None
        assert stored_project.created_by is not None
        assert stored_project.modified_by is not None
        assert stored_project.files == []
        assert stored_project.folders == []
        assert stored_project.annotations is None

    @pytest.mark.asyncio
    async def test_store_project_with_file(self, file: File, project: Project) -> None:
        # GIVEN a File on the project
        project.files.append(file)

        # WHEN I store the Project on Synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties
        assert stored_project.id is not None
        assert stored_project.name is not None
        assert stored_project.parent_id is not None
        assert stored_project.description is not None
        assert stored_project.etag is not None
        assert stored_project.created_on is not None
        assert stored_project.modified_on is not None
        assert stored_project.created_by is not None
        assert stored_project.modified_by is not None
        assert len(stored_project.files) == 1
        assert stored_project.files == [file]
        assert stored_project.folders == []
        assert stored_project.annotations is None

        # AND I expect the File to be stored on Synapse
        assert file.id is not None
        assert file.name is not None
        assert file.parent_id == stored_project.id
        assert file.path is not None

    @pytest.mark.asyncio
    async def test_store_project_with_multiple_files(self, project: Project) -> None:
        # GIVEN multiple files in a project
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        project.files = files

        # WHEN I store the Project on Synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties
        assert stored_project.id is not None
        assert stored_project.name is not None
        assert stored_project.parent_id is not None
        assert stored_project.description is not None
        assert stored_project.etag is not None
        assert stored_project.created_on is not None
        assert stored_project.modified_on is not None
        assert stored_project.created_by is not None
        assert stored_project.modified_by is not None
        assert len(stored_project.files) == 3
        assert stored_project.files == files
        assert stored_project.folders == []
        assert stored_project.annotations is None

        # AND I expect the Files to be stored on Synapse
        for file in files:
            assert file.id is not None
            assert file.name is not None
            assert file.parent_id == stored_project.id
            assert file.path is not None

    @pytest.mark.asyncio
    async def test_store_project_with_multiple_files_and_folders(
        self, project: Project
    ) -> None:
        # GIVEN multiple files in a project
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        project.files = files

        # AND multiple folders in a project
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a project
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        project.folders = folders

        # WHEN I store the Project on Synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties
        assert stored_project.id is not None
        assert stored_project.name is not None
        assert stored_project.parent_id is not None
        assert stored_project.description is not None
        assert stored_project.etag is not None
        assert stored_project.created_on is not None
        assert stored_project.modified_on is not None
        assert stored_project.created_by is not None
        assert stored_project.modified_by is not None
        assert len(stored_project.files) == 3
        assert stored_project.files == files
        assert len(stored_project.folders) == 2
        assert stored_project.folders == folders
        assert stored_project.annotations is None

        # AND I expect the Files to be stored on Synapse
        for file in files:
            assert file.id is not None
            assert file.name is not None
            assert file.parent_id == stored_project.id
            assert file.path is not None

        # AND I expect the Folders to be stored on Synapse
        for sub_folder in stored_project.folders:
            assert sub_folder.id is not None
            assert sub_folder.name is not None
            assert sub_folder.parent_id == stored_project.id
            # AND I expect the Files in the Folders to be stored on Synapse
            for sub_file in sub_folder.files:
                assert sub_file.id is not None
                assert sub_file.name is not None
                assert sub_file.parent_id == sub_folder.id
                assert sub_file.path is not None

    @pytest.mark.asyncio
    async def test_store_project_with_annotations(self, project: Project) -> None:
        # GIVEN a Project object and a Project object
        # AND annotations on the Project
        annotations = {
            "my_single_key_string": ["a"],
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        project.annotations = annotations

        # WHEN I store the Project on Synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # THEN I expect the stored Project to have the expected properties
        assert stored_project.id is not None
        assert stored_project.name is not None
        assert stored_project.parent_id is not None
        assert stored_project.description is not None
        assert stored_project.etag is not None
        assert stored_project.created_on is not None
        assert stored_project.modified_on is not None
        assert stored_project.created_by is not None
        assert stored_project.modified_by is not None
        assert stored_project.files == []
        assert stored_project.folders == []
        assert stored_project.annotations is not None

        # AND I expect the annotations to be stored on Synapse
        assert stored_project.annotations == annotations
        assert (await Project(id=stored_project.id).get()).annotations == annotations


class TestProjectGet:
    """Tests for the synapseclient.models.Project.get method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DERSCRIPTION_PROJECT)
        return project

    @pytest.mark.asyncio
    async def test_get_project_by_id(self, project: Project) -> None:
        # GIVEN a Project object

        # AND the project is stored in synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # WHEN I get the Project from Synapse
        project_copy = await Project(id=stored_project.id).get()

        # THEN I expect the stored Project to have the expected properties
        assert project_copy.id is not None
        assert project_copy.name is not None
        assert stored_project.parent_id is not None
        assert project_copy.description is not None
        assert project_copy.etag is not None
        assert project_copy.created_on is not None
        assert project_copy.modified_on is not None
        assert project_copy.created_by is not None
        assert project_copy.modified_by is not None
        assert project_copy.files == []
        assert project_copy.folders == []
        assert project_copy.annotations is None

    @pytest.mark.asyncio
    async def test_get_project_by_name_attribute(self, project: Project) -> None:
        # GIVEN a Project object

        # AND the project is stored in synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # WHEN I get the Project from Synapse
        project_copy = await Project(name=stored_project.name).get()

        # THEN I expect the stored Project to have the expected properties
        assert project_copy.id is not None
        assert project_copy.name is not None
        assert stored_project.parent_id is not None
        assert project_copy.description is not None
        assert project_copy.etag is not None
        assert project_copy.created_on is not None
        assert project_copy.modified_on is not None
        assert project_copy.created_by is not None
        assert project_copy.modified_by is not None
        assert project_copy.files == []
        assert project_copy.folders == []
        assert project_copy.annotations is None


class TestProjectDelete:
    """Tests for the synapseclient.models.Project.delete method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DERSCRIPTION_PROJECT)
        return project

    @pytest.mark.asyncio
    async def test_delete_project(self, project: Project) -> None:
        # GIVEN a Project object

        # AND the project is stored in synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # WHEN I delete the Project from Synapse
        await stored_project.delete()

        # THEN I expect the project to have been deleted
        with pytest.raises(SynapseHTTPError) as e:
            await stored_project.get()

        assert (
            str(e.value)
            == f"404 Client Error: \nEntity {stored_project.id} is in trash can."
        )


class TestProjectCopy:
    """Tests for the synapseclient.models.Project.copy method."""

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

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DERSCRIPTION_PROJECT)
        return project

    @pytest.mark.asyncio
    async def test_copy_project_with_multiple_files_and_projects(
        self, project: Project
    ) -> None:
        # GIVEN a project to copy to
        destination_project = await Project(
            name=str(uuid.uuid4()), description="Destination for project copy"
        ).store()
        self.schedule_for_cleanup(destination_project.id)

        # AND multiple files in the source project
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        project.files = files

        # AND multiple folders in the source project
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a project
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        project.folders = folders

        # WHEN I store the Project on Synapse
        project.annotations = {"test": ["test"]}
        project = await project.store()
        self.schedule_for_cleanup(project.id)

        # AND I copy the project to the destination project
        copied_project = await project.copy(destination_id=destination_project.id)

        # AND I sync the destination project from Synapse
        await destination_project.sync_from_synapse(
            recursive=False, download_file=False
        )

        # THEN I expect the copied Project to have the expected properties
        assert len(destination_project.files) == 3
        assert len(destination_project.folders) == 2
        assert copied_project.id is not None
        assert copied_project.id is not project.id
        assert copied_project.name is not None
        assert copied_project.parent_id is not None
        assert copied_project.description is not None
        assert copied_project.etag is not None
        assert copied_project.created_on is not None
        assert copied_project.modified_on is not None
        assert copied_project.created_by is not None
        assert copied_project.modified_by is not None
        assert copied_project.annotations == {"test": ["test"]}

        # AND I expect the Files to have been copied
        for file in copied_project.files:
            assert file.id is not None
            assert file.name is not None
            assert file.parent_id == copied_project.id

        # AND I expect the Folders to have been copied
        for sub_folder in copied_project.folders:
            assert sub_folder.id is not None
            assert sub_folder.name is not None
            assert sub_folder.parent_id == copied_project.id
            # AND I expect the Files in the Folders to have been copied
            for sub_file in sub_folder.files:
                assert sub_file.id is not None
                assert sub_file.name is not None
                assert sub_file.parent_id == sub_folder.id

    @pytest.mark.asyncio
    async def test_copy_project_exclude_files(self, project: Project) -> None:
        # GIVEN a project to copy to
        destination_project = await Project(
            name=str(uuid.uuid4()), description="Destination for project copy"
        ).store()
        self.schedule_for_cleanup(destination_project.id)

        # AND multiple files in the source project
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        project.files = files

        # AND multiple folders in the source project
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a project
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        project.folders = folders

        # WHEN I store the Project on Synapse
        project.annotations = {"test": ["test"]}
        project = await project.store()
        self.schedule_for_cleanup(project.id)

        # AND I copy the project to the destination project
        copied_project = await project.copy(
            destination_id=destination_project.id, exclude_types=["file"]
        )

        # AND I sync the destination project from Synapse
        await destination_project.sync_from_synapse(
            recursive=False, download_file=False
        )

        # THEN I expect the copied Project to have the expected properties
        assert len(destination_project.folders) == 2
        assert copied_project.id is not None
        assert copied_project.id is not project.id
        assert copied_project.name is not None
        assert copied_project.parent_id is not None
        assert copied_project.description is not None
        assert copied_project.etag is not None
        assert copied_project.created_on is not None
        assert copied_project.modified_on is not None
        assert copied_project.created_by is not None
        assert copied_project.modified_by is not None
        assert copied_project.annotations == {"test": ["test"]}
        assert copied_project.files == []

        # AND I expect the Folders to have been copied
        for sub_folder in copied_project.folders:
            assert sub_folder.id is not None
            assert sub_folder.name is not None
            assert sub_folder.parent_id == copied_project.id
            assert sub_folder.files == []


class TestProjectSyncFromSynapse:
    """Tests for the synapseclient.models.Project.sync_from_synapse method."""

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

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        return self.create_file_instance(schedule_for_cleanup)

    @pytest.fixture(autouse=True, scope="function")
    def project(self) -> Project:
        project = Project(name=str(uuid.uuid4()), description=DERSCRIPTION_PROJECT)
        return project

    @pytest.mark.asyncio
    async def test_sync_from_synapse(self, file: File, project: Project) -> None:
        root_directory_path = os.path.dirname(file.path)

        # GIVEN multiple files in the source project
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        project.files = files

        # AND multiple folders in the source project
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a project
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        project.folders = folders

        # WHEN I store the Project on Synapse
        stored_project = await project.store()
        self.schedule_for_cleanup(project.id)

        # AND I sync the project from Synapse
        copied_project = await stored_project.sync_from_synapse(
            path=root_directory_path
        )

        # THEN I expect that the project and its contents are synced from Synapse to disk
        for file in copied_project.files:
            assert os.path.exists(file.path)
            assert os.path.isfile(file.path)
            assert (
                utils.md5_for_file(file.path).hexdigest()
                == file.file_handle.content_md5
            )

        # AND I expect the Folders to be retrieved from Synapse
        for sub_folder in stored_project.folders:
            resolved_path = os.path.join(root_directory_path, sub_folder.name)
            assert os.path.exists(resolved_path)
            assert os.path.isdir(resolved_path)
            # AND I expect the Files in the Folders to be retrieved from Synapse
            for sub_file in sub_folder.files:
                assert os.path.exists(sub_file.path)
                assert os.path.isfile(sub_file.path)
                assert (
                    utils.md5_for_file(sub_file.path).hexdigest()
                    == sub_file.file_handle.content_md5
                )
