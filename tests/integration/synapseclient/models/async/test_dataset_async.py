import uuid
from typing import Callable, Dict, List

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.models import Dataset, EntityRef, File, Folder, Project

CONTENT_TYPE = "text/plain"
DESCRIPTION_FILE = "This is an example file."
DESCRIPTION_FOLDER = "This is an example folder."
DERSCRIPTION_PROJECT = "This is an example project."

DEFAULT_COLUMNS = [
    "id",
    "name",
    "description",
    "createdOn",
    "createdBy",
    "etag",
    "modifiedOn",
    "modifiedBy",
    "path",
    "type",
    "currentVersion",
    "parentId",
    "benefactorId",
    "projectId",
    "dataFileHandleId",
    "dataFileName",
    "dataFileSizeBytes",
    "dataFileMD5Hex",
    "dataFileConcreteType",
    "dataFileBucket",
    "dataFileKey",
]


class TestDatasetCreation:
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

    async def test_create_empty_dataset(
        self, syn: Synapse, project_model: Project
    ) -> None:
        # GIVEN an empty Dataset
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )

        # WHEN I store the dataset
        dataset = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # THEN the dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that dataset from Synapse
        new_dataset_instance = await Dataset(id=dataset.id).get_async(
            synapse_client=self.syn
        )
        assert new_dataset_instance is not None
        assert new_dataset_instance.name == dataset.name
        assert new_dataset_instance.id == dataset.id
        assert new_dataset_instance.description == dataset.description
        print(new_dataset_instance)

    async def test_create_dataset_with_file(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a File on Synapse
        file_1 = await file.store_async(parent=project_model)

        # WHEN I create a Dataset with that File
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        await dataset.add_item_async(file_1)

        await dataset.store_async(synapse_client=self.syn)

        # THEN the dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that dataset from Synapse
        new_dataset_instance = await Dataset(id=dataset.id).get_async(
            synapse_client=self.syn
        )
        assert new_dataset_instance is not None
        assert new_dataset_instance.name == dataset.name
        assert new_dataset_instance.id == dataset.id
        assert new_dataset_instance.description == dataset.description
        assert list(new_dataset_instance.columns.keys()) == DEFAULT_COLUMNS
        assert new_dataset_instance.items == [
            EntityRef(id=file_1.id, version=file_1.version_number),
        ]

    async def test_create_dataset_with_folder(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder with 3 files on Synapse
        files = [self.create_file_instance(self.schedule_for_cleanup) for _ in range(3)]
        folder.files = files
        folder = await folder.store_async(parent=project_model)
        # AND an Dataset with that Folder
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        await dataset.add_item_async(folder)
        # WHEN I store the Dataset on Synapse
        await dataset.store_async(synapse_client=self.syn)
        # THEN the Dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that Dataset from Synapse
        new_dataset_instance = await Dataset(id=dataset.id).get_async(
            synapse_client=self.syn
        )
        assert new_dataset_instance is not None
        assert new_dataset_instance.name == dataset.name
        assert new_dataset_instance.id == dataset.id
        assert new_dataset_instance.description == dataset.description
        assert list(new_dataset_instance.columns.keys()) == DEFAULT_COLUMNS
        # AND the Dataset has all of the files in the Folder
        assert new_dataset_instance.items == [
            EntityRef(id=file.id, version=file.version_number) for file in files
        ]

    async def test_create_dataset_with_files_and_folders(
        self, project_model: Project, file: File, folder: Folder
    ) -> None:
        # GIVEN a File and a Folder with 3 files on Synapse
        file = await file.store_async(parent=project_model)
        files = [self.create_file_instance(self.schedule_for_cleanup) for _ in range(3)]
        folder.files = files
        folder = await folder.store_async(parent=project_model)

        # WHEN I create a Dataset with both the File and the Folder
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        await dataset.add_item_async(file)
        await dataset.add_item_async(folder)
        await dataset.store_async(synapse_client=self.syn)

        # THEN the Dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that Dataset from Synapse
        new_dataset_instance = await Dataset(id=dataset.id).get_async(
            synapse_client=self.syn
        )
        assert new_dataset_instance is not None
        assert new_dataset_instance.name == dataset.name
        assert new_dataset_instance.id == dataset.id
        assert new_dataset_instance.description == dataset.description
        assert list(new_dataset_instance.columns.keys()) == DEFAULT_COLUMNS
        expected_items = [
            EntityRef(id=file.id, version=file.version_number),
        ] + [
            EntityRef(id=file.id, version=file.version_number) for file in folder.files
        ]
        for item in new_dataset_instance.items:
            assert item in expected_items


# async def test_update_dataset_attributes(syn, project, dataset: Dataset) -> None:
#     """Test updating dataset attributes like name, description, annotations"""
#     pass


#     syn, project, dataset: Dataset, files: List[str], folders: List[str]
# ) -> None:
#     """Test adding additional files and folders to existing dataset"""
#     pass


# async def test_update_dataset_upsert_rows(
#     syn, project, dataset: Dataset, rows: List[Dict]
# ) -> None:
#     """Test upserting rows into an existing dataset"""
#     pass


# async def test_update_dataset_remove_rows(
#     syn, project, dataset: Dataset, row_ids: List[str]
# ) -> None:
#     """Test removing rows from an existing dataset"""
#     pass


# async def test_delete_dataset(syn, project, dataset: Dataset) -> None:
#     """Test deleting an entire dataset"""
#     pass


# async def test_reorder_dataset_columns(
#     syn, project, dataset: Dataset, column_order: List[str]
# ) -> None:
#     """Test reordering columns in a dataset"""
#     pass


# async def test_rename_dataset_columns(
#     syn, project, dataset: Dataset, column_renames: Dict[str, str]
# ) -> None:
#     """Test renaming columns in a dataset"""
#     pass


# async def test_delete_dataset_columns(
#     syn, project, dataset: Dataset, columns_to_delete: List[str]
# ) -> None:
#     """Test deleting columns from a dataset"""
#     pass


# async def test_query_dataset(
#     syn, project, dataset: Dataset, query: str
# ) -> pd.DataFrame:
#     """Test querying data from a dataset"""
#     pass
