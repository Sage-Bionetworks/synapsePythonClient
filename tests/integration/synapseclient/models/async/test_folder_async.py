"""Integration tests for the synapseclient.models.Folder class."""

import os
import uuid

from typing import Callable
import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError

from synapseclient.models import (
    Project,
    Folder,
    File,
)

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

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> File:
        return self.create_file_instance(schedule_for_cleanup)

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    @pytest.mark.asyncio
    async def test_store_folder(self, project_model: Project, folder: Folder) -> None:
        # GIVEN a Folder object and a Project object

        # WHEN I store the Folder on Synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties
        assert stored_folder.id is not None
        assert stored_folder.name is not None
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.description is not None
        assert stored_folder.etag is not None
        assert stored_folder.created_on is not None
        assert stored_folder.modified_on is not None
        assert stored_folder.created_by is not None
        assert stored_folder.modified_by is not None
        assert stored_folder.files == []
        assert stored_folder.folders == []
        assert not stored_folder.annotations and isinstance(
            stored_folder.annotations, dict
        )

    @pytest.mark.asyncio
    async def test_store_folder_with_file(
        self, project_model: Project, file: File, folder: Folder
    ) -> None:
        # GIVEN a File on the folder
        folder.files.append(file)

        # WHEN I store the Folder on Synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties
        assert stored_folder.id is not None
        assert stored_folder.name is not None
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.description is not None
        assert stored_folder.etag is not None
        assert stored_folder.created_on is not None
        assert stored_folder.modified_on is not None
        assert stored_folder.created_by is not None
        assert stored_folder.modified_by is not None
        assert len(stored_folder.files) == 1
        assert stored_folder.files == [file]
        assert stored_folder.folders == []
        assert not stored_folder.annotations and isinstance(
            stored_folder.annotations, dict
        )

        # AND I expect the File to be stored on Synapse
        assert file.id is not None
        assert file.name is not None
        assert file.parent_id == stored_folder.id
        assert file.path is not None

    @pytest.mark.asyncio
    async def test_store_folder_with_multiple_files(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN multiple files in a folder
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        folder.files = files

        # WHEN I store the Folder on Synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties
        assert stored_folder.id is not None
        assert stored_folder.name is not None
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.description is not None
        assert stored_folder.etag is not None
        assert stored_folder.created_on is not None
        assert stored_folder.modified_on is not None
        assert stored_folder.created_by is not None
        assert stored_folder.modified_by is not None
        assert len(stored_folder.files) == 3
        assert stored_folder.files == files
        assert stored_folder.folders == []
        assert not stored_folder.annotations and isinstance(
            stored_folder.annotations, dict
        )
        # AND I expect the Files to be stored on Synapse
        for file in files:
            assert file.id is not None
            assert file.name is not None
            assert file.parent_id == stored_folder.id
            assert file.path is not None

    @pytest.mark.asyncio
    async def test_store_folder_with_multiple_files_and_folders(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN multiple files in a folder
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        folder.files = files

        # AND multiple folders in a folder
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a folder
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        folder.folders = folders

        # WHEN I store the Folder on Synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties
        assert stored_folder.id is not None
        assert stored_folder.name is not None
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.description is not None
        assert stored_folder.etag is not None
        assert stored_folder.created_on is not None
        assert stored_folder.modified_on is not None
        assert stored_folder.created_by is not None
        assert stored_folder.modified_by is not None
        assert len(stored_folder.files) == 3
        assert stored_folder.files == files
        assert len(stored_folder.folders) == 2
        assert stored_folder.folders == folders
        assert not stored_folder.annotations and isinstance(
            stored_folder.annotations, dict
        )

        # AND I expect the Files to be stored on Synapse
        for file in files:
            assert file.id is not None
            assert file.name is not None
            assert file.parent_id == stored_folder.id
            assert file.path is not None

        # AND I expect the Folders to be stored on Synapse
        for sub_folder in stored_folder.folders:
            assert sub_folder.id is not None
            assert sub_folder.name is not None
            assert sub_folder.parent_id == stored_folder.id
            # AND I expect the Files in the Folders to be stored on Synapse
            for sub_file in sub_folder.files:
                assert sub_file.id is not None
                assert sub_file.name is not None
                assert sub_file.parent_id == sub_folder.id
                assert sub_file.path is not None

    @pytest.mark.asyncio
    async def test_store_folder_with_annotations(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder object and a Project object
        # AND annotations on the Folder
        annotations = {
            "my_single_key_string": ["a"],
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        folder.annotations = annotations

        # WHEN I store the Folder on Synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # THEN I expect the stored Folder to have the expected properties
        assert stored_folder.id is not None
        assert stored_folder.name is not None
        assert stored_folder.parent_id == project_model.id
        assert stored_folder.description is not None
        assert stored_folder.etag is not None
        assert stored_folder.created_on is not None
        assert stored_folder.modified_on is not None
        assert stored_folder.created_by is not None
        assert stored_folder.modified_by is not None
        assert stored_folder.files == []
        assert stored_folder.folders == []
        assert stored_folder.annotations is not None

        # AND I expect the annotations to be stored on Synapse
        assert stored_folder.annotations == annotations
        assert (
            await Folder(id=stored_folder.id).get_async()
        ).annotations == annotations


class TestFolderGet:
    """Tests for the synapseclient.models.Folder.get method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    @pytest.mark.asyncio
    async def test_get_folder_by_id(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder object and a Project object

        # AND the folder is stored in synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # WHEN I get the Folder from Synapse
        folder_copy = await Folder(id=stored_folder.id).get_async()

        # THEN I expect the stored Folder to have the expected properties
        assert folder_copy.id is not None
        assert folder_copy.name is not None
        assert folder_copy.parent_id == project_model.id
        assert folder_copy.description is not None
        assert folder_copy.etag is not None
        assert folder_copy.created_on is not None
        assert folder_copy.modified_on is not None
        assert folder_copy.created_by is not None
        assert folder_copy.modified_by is not None
        assert folder_copy.files == []
        assert folder_copy.folders == []
        assert not folder_copy.annotations and isinstance(folder_copy.annotations, dict)

    @pytest.mark.asyncio
    async def test_get_folder_by_name_and_parent_id_attribute(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder object and a Project object

        # AND the folder is stored in synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # WHEN I get the Folder from Synapse
        folder_copy = await Folder(
            name=stored_folder.name, parent_id=stored_folder.parent_id
        ).get_async()

        # THEN I expect the stored Folder to have the expected properties
        assert folder_copy.id is not None
        assert folder_copy.name is not None
        assert folder_copy.parent_id == project_model.id
        assert folder_copy.description is not None
        assert folder_copy.etag is not None
        assert folder_copy.created_on is not None
        assert folder_copy.modified_on is not None
        assert folder_copy.created_by is not None
        assert folder_copy.modified_by is not None
        assert folder_copy.files == []
        assert folder_copy.folders == []
        assert not folder_copy.annotations and isinstance(folder_copy.annotations, dict)

    @pytest.mark.asyncio
    async def test_get_folder_by_name_and_parent(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder object and a Project object

        # AND the folder is stored in synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # WHEN I get the Folder from Synapse
        folder_copy = await Folder(name=stored_folder.name).get_async(
            parent=project_model
        )

        # THEN I expect the stored Folder to have the expected properties
        assert folder_copy.id is not None
        assert folder_copy.name is not None
        assert folder_copy.parent_id == project_model.id
        assert folder_copy.description is not None
        assert folder_copy.etag is not None
        assert folder_copy.created_on is not None
        assert folder_copy.modified_on is not None
        assert folder_copy.created_by is not None
        assert folder_copy.modified_by is not None
        assert folder_copy.files == []
        assert folder_copy.folders == []
        assert not folder_copy.annotations and isinstance(folder_copy.annotations, dict)


class TestFolderDelete:
    """Tests for the synapseclient.models.Folder.delete method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    @pytest.mark.asyncio
    async def test_delete_folder(self, project_model: Project, folder: Folder) -> None:
        # GIVEN a Folder object and a Project object

        # AND the folder is stored in synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # WHEN I delete the Folder from Synapse
        await stored_folder.delete_async()

        # THEN I expect the folder to have been deleted
        with pytest.raises(SynapseHTTPError) as e:
            await stored_folder.get()

        assert (
            str(e.value)
            == f"404 Client Error: \nEntity {stored_folder.id} is in trash can."
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

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    @pytest.mark.asyncio
    async def test_copy_folder_with_multiple_files_and_folders(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a folder to copy to
        destination_folder = await Folder(
            name=str(uuid.uuid4()), description="Destination for folder copy"
        ).store_async(parent=project_model)

        # AND multiple files in the source folder
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        folder.files = files

        # AND multiple folders in the source folder
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a folder
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        folder.folders = folders

        # WHEN I store the Folder on Synapse
        folder.annotations = {"test": ["test"]}
        folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # AND I copy the folder to the destination folder
        copied_folder = await folder.copy_async(parent_id=destination_folder.id)

        # AND I sync the destination folder from Synapse
        await destination_folder.sync_from_synapse_async(
            recursive=False, download_file=False
        )

        # THEN I expect the copied Folder to have the expected properties
        assert len(destination_folder.folders) == 1
        assert destination_folder.folders == [copied_folder]
        assert copied_folder.id is not None
        assert copied_folder.id is not folder.id
        assert copied_folder.name is not None
        assert copied_folder.parent_id != project_model.id
        assert copied_folder.description is not None
        assert copied_folder.etag is not None
        assert copied_folder.created_on is not None
        assert copied_folder.modified_on is not None
        assert copied_folder.created_by is not None
        assert copied_folder.modified_by is not None
        assert copied_folder.annotations == {"test": ["test"]}

        # AND I expect the Files to have been copied
        for file in copied_folder.files:
            assert file.id is not None
            assert file.name is not None
            assert file.parent_id == copied_folder.id

        # AND I expect the Folders to have been copied
        for sub_folder in copied_folder.folders:
            assert sub_folder.id is not None
            assert sub_folder.name is not None
            assert sub_folder.parent_id == copied_folder.id
            # AND I expect the Files in the Folders to have been copied
            for sub_file in sub_folder.files:
                assert sub_file.id is not None
                assert sub_file.name is not None
                assert sub_file.parent_id == sub_folder.id

    @pytest.mark.asyncio
    async def test_copy_folder_exclude_files(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a folder to copy to
        destination_folder = await Folder(
            name=str(uuid.uuid4()), description="Destination for folder copy"
        ).store_async(parent=project_model)

        # AND multiple files in the source folder
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        folder.files = files

        # AND multiple folders in the source folder
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a folder
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        folder.folders = folders

        # WHEN I store the Folder on Synapse
        folder.annotations = {"test": ["test"]}
        folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # AND I copy the folder to the destination folder
        copied_folder = await folder.copy_async(
            parent_id=destination_folder.id, exclude_types=["file"]
        )

        # AND I sync the destination folder from Synapse
        await destination_folder.sync_from_synapse_async(
            recursive=False, download_file=False
        )

        # THEN I expect the copied Folder to have the expected properties
        assert len(destination_folder.folders) == 1
        assert destination_folder.folders == [copied_folder]
        assert copied_folder.id is not None
        assert copied_folder.id is not folder.id
        assert copied_folder.name is not None
        assert copied_folder.parent_id != project_model.id
        assert copied_folder.description is not None
        assert copied_folder.etag is not None
        assert copied_folder.created_on is not None
        assert copied_folder.modified_on is not None
        assert copied_folder.created_by is not None
        assert copied_folder.modified_by is not None
        assert copied_folder.annotations == {"test": ["test"]}
        assert copied_folder.files == []

        # AND I expect the Folders to have been copied
        for sub_folder in copied_folder.folders:
            assert sub_folder.id is not None
            assert sub_folder.name is not None
            assert sub_folder.parent_id == copied_folder.id
            assert sub_folder.files == []


class TestFolderSyncFromSynapse:
    """Tests for the synapseclient.models.Folder.sync_from_synapse_async method."""

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
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    @pytest.mark.asyncio
    async def test_sync_from_synapse(
        self, project_model: Project, file: File, folder: Folder
    ) -> None:
        root_directory_path = os.path.dirname(file.path)

        # GIVEN multiple files in the source folder
        files = []
        for _ in range(3):
            files.append(self.create_file_instance(self.schedule_for_cleanup))
        folder.files = files

        # AND multiple folders in the source folder
        folders = []
        for _ in range(2):
            sub_folder = Folder(name=str(uuid.uuid4()))
            folders.append(sub_folder)

            # AND multiple files in a folder
            sub_folder_files = []
            for _ in range(2):
                sub_folder_files.append(
                    self.create_file_instance(self.schedule_for_cleanup)
                )
            sub_folder.files = sub_folder_files
        folder.folders = folders

        # WHEN I store the Folder on Synapse
        stored_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # AND I sync the folder from Synapse
        copied_folder = await stored_folder.sync_from_synapse_async(
            path=root_directory_path
        )

        # THEN I expect that the folder and its contents are synced from Synapse to disk
        for file in copied_folder.files:
            assert os.path.exists(file.path)
            assert os.path.isfile(file.path)
            assert (
                utils.md5_for_file(file.path).hexdigest()
                == file.file_handle.content_md5
            )

        # AND I expect the Folders to be stored on Synapse
        for sub_folder in stored_folder.folders:
            resolved_path = os.path.join(root_directory_path, sub_folder.name)
            assert os.path.exists(resolved_path)
            assert os.path.isdir(resolved_path)
            # AND I expect the Files in the Folders to be stored on Synapse
            for sub_file in sub_folder.files:
                assert os.path.exists(sub_file.path)
                assert os.path.isfile(sub_file.path)
                assert (
                    utils.md5_for_file(sub_file.path).hexdigest()
                    == sub_file.file_handle.content_md5
                )
