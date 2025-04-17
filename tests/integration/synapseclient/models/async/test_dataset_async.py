import uuid
from typing import Callable

import pandas as pd
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
    File,
    Folder,
    Project,
)

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


class TestDataset:
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
        dataset.add_item(file_1)

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
        # AND a Dataset with that Folder
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(folder)
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
        expected_items = [
            EntityRef(id=file.id, version=file.version_number) for file in files
        ]
        for item in new_dataset_instance.items:
            assert item in expected_items

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
        dataset.add_item(file)
        dataset.add_item(folder)
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

    async def test_update_dataset_attributes(
        self, syn: Synapse, project_model: Project
    ) -> None:
        # GIVEN an empty Dataset
        original_dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        # WHEN I store the Dataset
        original_dataset = await original_dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(original_dataset.id)

        # AND I update attributes of the dataset
        updated_dataset = await Dataset(id=original_dataset.id).get_async(
            synapse_client=self.syn
        )
        updated_dataset.name = str(uuid.uuid4())
        updated_dataset.description = "Updated description"
        # AND I store the updated dataset
        updated_dataset = await updated_dataset.store_async(synapse_client=self.syn)

        # AND I retrieve the dataset with its original id
        retrieved_dataset = await Dataset(id=original_dataset.id).get_async(
            synapse_client=self.syn
        )
        # THEN the dataset should be updated
        assert retrieved_dataset is not None
        assert retrieved_dataset.name == updated_dataset.name
        assert retrieved_dataset.description == updated_dataset.description
        # AND all versions should have the same id
        assert retrieved_dataset.id == updated_dataset.id == original_dataset.id

    async def test_query_dataset(self, project_model: Project, file: File) -> None:
        # GIVEN a Dataset with a File
        file = await file.store_async(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        await dataset.store_async(synapse_client=self.syn)
        # WHEN I query the dataset
        row = await Dataset.query_async(
            query=f"SELECT * FROM {dataset.id} WHERE id = '{file.id}'"
        )
        # THEN the dataset row contain expected values
        assert row["id"][0] == file.id
        assert row["name"][0] == file.name
        assert row["description"][0] == file.description

    async def test_part_mask_query_everything(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset with a File
        file = await file.store_async(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I query the dataset with a part mask
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZE_BYTES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZE_BYTES | LAST_UPDATED_ON

        results = await Dataset.query_part_mask_async(
            query=f"SELECT * FROM {dataset.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN the data in the columns should match
        assert results.result["id"][0] == file.id
        assert results.result["name"][0] == file.name
        assert results.result["description"][0] == file.description

        # AND the part mask should be reflected in the results
        assert results.count == 1
        assert results.sum_file_sizes is not None
        assert results.sum_file_sizes.greater_than is not None
        assert results.sum_file_sizes.sum_file_size_bytes is not None
        assert results.last_updated_on is not None

    async def test_part_mask_query_results_only(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset with a File
        file = await file.store_async(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I query the dataset with a part mask
        QUERY_RESULTS = 0x1
        results = await Dataset.query_part_mask_async(
            query=f"SELECT * FROM {dataset.id}",
            synapse_client=self.syn,
            part_mask=QUERY_RESULTS,
        )

        # THEN the data in the columns should match
        assert results.result["id"][0] == file.id
        assert results.result["name"][0] == file.name
        assert results.result["description"][0] == file.description

        # AND the part mask should be reflected in the results
        assert results.count is None
        assert results.sum_file_sizes is None
        assert results.last_updated_on is None

    async def test_update_dataset_rows(
        self, syn: Synapse, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset with a File and a custom column
        file = await file.store_async(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
            columns=[Column(name="my_annotation", column_type=ColumnType.STRING)],
        )
        dataset.add_item(file)
        await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I update rows in the dataset
        modified_data = pd.DataFrame(
            {
                "id": [file.id],
                "my_annotation": ["good data"],
            }
        )
        await dataset.update_rows_async(
            values=modified_data,
            primary_keys=["id"],
            wait_for_eventually_consistent_view=True,
            dry_run=False,
        )
        await dataset.store_async(synapse_client=self.syn)
        # AND I query the dataset
        row = await Dataset.query_async(
            query=f"SELECT my_annotation FROM {dataset.id} WHERE id = '{file.id}'"
        )
        # THEN the dataset row should be updated
        assert row["my_annotation"][0] == "good data"

    async def test_update_dataset_remove_item(
        self,
        project_model: Project,
    ) -> None:
        # GIVEN a Dataset with three Files
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        files = [self.create_file_instance(self.schedule_for_cleanup) for _ in range(3)]
        for file in files:
            file = await file.store_async(parent=project_model)
            dataset.add_item(file)
        await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        # WHEN I remove one of the Files
        assert len(dataset.items) == 3
        dataset.remove_item(files[0])
        await dataset.store_async(synapse_client=self.syn)
        # THEN the dataset should only have two Files
        assert len(dataset.items) == 2
        assert (
            EntityRef(id=files[0].id, version=files[0].version_number)
            not in dataset.items
        )
        assert (
            EntityRef(id=files[1].id, version=files[1].version_number) in dataset.items
        )
        assert (
            EntityRef(id=files[2].id, version=files[2].version_number) in dataset.items
        )

    async def test_delete_dataset(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN an empty Dataset
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        assert dataset.id is not None
        # WHEN I delete the Dataset
        await dataset.delete_async(synapse_client=self.syn)
        # THEN the Dataset should be deleted
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {dataset.id} is in trash can.",
        ):
            await Dataset(id=dataset.id).get_async(synapse_client=self.syn)

    async def test_snapshot_dataset(
        self, syn: Synapse, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # AND two files to use for our Dataset
        file_1 = self.create_file_instance(self.schedule_for_cleanup)
        file_1 = await file_1.store_async(parent=project_model)
        file_2 = self.create_file_instance(self.schedule_for_cleanup)
        file_2 = await file_2.store_async(parent=project_model)
        # WHEN I add the first file to the Dataset and create a snapshot of it
        dataset.add_item(file_1)
        await dataset.store_async(synapse_client=self.syn)
        await dataset.snapshot_async(synapse_client=self.syn)

        # AND I add the second file to the Dataset and create a snapshot of it again
        dataset.add_item(file_2)
        await dataset.store_async(synapse_client=self.syn)
        await dataset.snapshot_async(synapse_client=self.syn)

        # THEN the versions of the Dataset should have the expected items
        dataset_version_1 = await Dataset(id=dataset.id, version_number=1).get_async(
            synapse_client=self.syn
        )
        assert dataset_version_1.items == [
            EntityRef(id=file_1.id, version=file_1.version_number)
        ]

        dataset_version_2 = await Dataset(id=dataset.id, version_number=2).get_async(
            synapse_client=self.syn
        )
        assert dataset_version_2.items == [
            EntityRef(id=file_1.id, version=file_1.version_number),
            EntityRef(id=file_2.id, version=file_2.version_number),
        ]


class TestDatasetColumns:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_add_column(
        self,
        syn: Synapse,
        project_model: Project,
    ) -> None:
        # GIVEN a Dataset with only default columns
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        # WHEN I add a column to the dataset
        dataset.add_column(
            column=Column(name="my_annotation", column_type=ColumnType.STRING)
        )
        await dataset.store_async(synapse_client=self.syn)
        # THEN the dataset should have the new column
        assert "my_annotation" in dataset.columns

    async def test_delete_column(self, project_model: Project) -> None:
        # GIVEN a Dataset in Synapse
        dataset_name = str(uuid.uuid4())
        old_column_name = "column_string"
        column_to_keep = "column_to_keep"
        old_dataset_instance = Dataset(
            name=dataset_name,
            parent_id=project_model.id,
            include_default_columns=False,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
        )
        old_dataset_instance = await old_dataset_instance.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_dataset_instance.id)

        # WHEN I delete the column
        old_dataset_instance.delete_column(name=old_column_name)

        # AND I store the dataset
        await old_dataset_instance.store_async(synapse_client=self.syn)

        # THEN the column should be removed from the dataset instance
        assert old_column_name not in old_dataset_instance.columns

        # AND the column to keep should still be in the dataset instance
        assert column_to_keep in old_dataset_instance.columns
        assert len(old_dataset_instance.columns.values()) == 1

        # AND the column should be removed from the Synapse dataset
        new_dataset_instance = await Dataset(id=old_dataset_instance.id).get_async(
            synapse_client=self.syn
        )
        assert old_column_name not in new_dataset_instance.columns

        # AND the column to keep should still be in the Synapse dataset
        assert column_to_keep in new_dataset_instance.columns
        assert len(new_dataset_instance.columns.values()) == 1

    async def test_reorder_column(self, project_model: Project) -> None:
        # GIVEN a Dataset in Synapse
        dataset_name = str(uuid.uuid4())
        first_column_name = "first"
        second_column_name = "second"
        old_dataset_instance = Dataset(
            name=dataset_name,
            parent_id=project_model.id,
            include_default_columns=False,
            columns=[
                Column(name=first_column_name, column_type=ColumnType.STRING),
                Column(name=second_column_name, column_type=ColumnType.STRING),
            ],
        )
        old_dataset_instance = await old_dataset_instance.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_dataset_instance.id)

        # WHEN I reorder the columns
        old_dataset_instance.reorder_column(
            name=second_column_name,
            index=0,
        )
        await old_dataset_instance.store_async(synapse_client=self.syn)

        # THEN the columns should be reordered
        assert list(old_dataset_instance.columns.keys()) == [
            second_column_name,
            first_column_name,
        ]

    async def test_rename_column(self, project_model: Project) -> None:
        # GIVEN a dataset in Synapse
        dataset_name = str(uuid.uuid4())
        old_column_name = "column_string"
        old_dataset_instance = Dataset(
            name=dataset_name,
            parent_id=project_model.id,
            include_default_columns=False,
            columns=[Column(name=old_column_name, column_type=ColumnType.STRING)],
        )
        old_dataset_instance = await old_dataset_instance.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_dataset_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_dataset_instance.columns[old_column_name].name = new_column_name

        # AND I store the dataset
        await old_dataset_instance.store_async(synapse_client=self.syn)

        # THEN the column name should be updated on the existing dataset instance
        assert old_dataset_instance.columns[new_column_name] is not None
        assert old_column_name not in old_dataset_instance.columns

        # AND the new column name should be reflected in the Synapse dataset
        new_dataset_instance = await Dataset(id=old_dataset_instance.id).get_async(
            synapse_client=self.syn
        )
        assert new_dataset_instance.columns[new_column_name] is not None
        assert old_column_name not in new_dataset_instance.columns


class TestDatasetCollection:
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

    def create_dataset_instance(self, project_model: Project) -> Dataset:
        dataset_name = str(uuid.uuid4())
        return Dataset(
            name=dataset_name,
            description="Test dataset",
            parent_id=project_model.id,
        )

    @pytest.fixture(autouse=True, scope="function")
    def dataset(self, project_model: Project) -> Dataset:
        return self.create_dataset_instance(project_model)

    async def test_create_empty_dataset_collection(
        self, syn: Synapse, project_model: Project
    ) -> None:
        # GIVEN an empty DatasetCollection
        dataset_collection = DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        )
        dataset_collection = await dataset_collection.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I store the DatasetCollection
        dataset_collection = await dataset_collection.store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(dataset_collection.id)

        # THEN the DatasetCollection should be created
        assert dataset_collection.id is not None

        # AND I can retrieve that DatasetCollection from Synapse
        new_dataset_collection_instance = await DatasetCollection(
            id=dataset_collection.id
        ).get_async(synapse_client=self.syn)
        assert new_dataset_collection_instance is not None
        assert new_dataset_collection_instance.name == dataset_collection.name
        assert new_dataset_collection_instance.id == dataset_collection.id
        assert (
            new_dataset_collection_instance.description
            == dataset_collection.description
        )

    async def test_create_dataset_collection_with_dataset(
        self, syn: Synapse, project_model: Project, file: File, dataset: Dataset
    ) -> None:
        # GIVEN a Dataset with a file
        file_1 = await file.store_async(parent=project_model)
        self.schedule_for_cleanup(file_1.id)
        dataset.add_item(file_1)
        dataset_1 = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_1.id)

        # WHEN I create a DatasetCollection with that Dataset
        dataset_collection = DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        )
        dataset_collection.add_item(dataset_1)
        await dataset_collection.store_async(synapse_client=self.syn)

        # THEN the DatasetCollection should be created
        assert dataset_collection.id is not None

        # AND I can retrieve that DatasetCollection from Synapse
        new_dataset_collection_instance = await DatasetCollection(
            id=dataset_collection.id
        ).get_async(synapse_client=self.syn)
        assert new_dataset_collection_instance is not None
        assert new_dataset_collection_instance.name == dataset_collection.name
        assert new_dataset_collection_instance.id == dataset_collection.id
        assert (
            new_dataset_collection_instance.description
            == dataset_collection.description
        )
        assert new_dataset_collection_instance.items == [
            EntityRef(id=dataset_1.id, version=dataset_1.version_number),
        ]

    async def test_update_dataset_collection_attributes(
        self, syn: Synapse, project_model: Project
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I update the DatasetCollection attributes
        updated_dataset_collection = await DatasetCollection(
            id=dataset_collection.id
        ).get_async(synapse_client=self.syn)
        updated_dataset_collection.name = str(uuid.uuid4())
        updated_dataset_collection.description = "Updated description"
        await updated_dataset_collection.store_async(synapse_client=self.syn)

        # AND I retrieve the DatasetCollection
        my_retrieved_dataset_collection = await DatasetCollection(
            id=dataset_collection.id
        ).get_async(synapse_client=self.syn)

        # THEN the DatasetCollection should be updated
        assert my_retrieved_dataset_collection is not None
        assert my_retrieved_dataset_collection.name == updated_dataset_collection.name
        assert (
            my_retrieved_dataset_collection.description
            == updated_dataset_collection.description
        )
        # AND all versions should have the same id
        assert my_retrieved_dataset_collection.id == updated_dataset_collection.id

    async def test_query_dataset_collection(
        self, syn: Synapse, project_model: Project, dataset: Dataset
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I add a Dataset to the DatasetCollection
        dataset_1 = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_1.id)
        dataset_collection.add_item(dataset_1)
        await dataset_collection.store_async(synapse_client=self.syn)

        # AND I query the DatasetCollection
        row = await DatasetCollection.query_async(
            query=f"SELECT * FROM {dataset_collection.id} WHERE id = '{dataset_1.id}'",
        )
        # THEN I expect the row to contain expected values
        assert row["id"][0] == dataset_1.id
        assert row["name"][0] == dataset_1.name
        assert row["description"][0] == dataset_1.description

    async def test_dataset_collection_part_mask_query_everything(
        self, syn: Synapse, project_model: Project, dataset: Dataset
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I add a Dataset to the DatasetCollection
        dataset_1 = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_1.id)
        dataset_collection.add_item(dataset_1)
        await dataset_collection.store_async(synapse_client=self.syn)

        # AND I query the DatasetCollection with a part mask with everything included
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZE_BYTES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZE_BYTES | LAST_UPDATED_ON

        row = await DatasetCollection.query_part_mask_async(
            query=f"SELECT * FROM {dataset_collection.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN I expect the row to contain expected values
        assert row.result["id"][0] == dataset_1.id
        assert row.result["name"][0] == dataset_1.name
        assert row.result["description"][0] == dataset_1.description

        # AND the part mask should be reflected in the row
        assert row.count == 1
        assert row.sum_file_sizes is not None
        assert row.sum_file_sizes.greater_than is not None
        assert row.sum_file_sizes.sum_file_size_bytes is not None
        assert row.last_updated_on is not None

    async def test_dataset_collection_part_mask_query_results_only(
        self, syn: Synapse, project_model: Project, dataset: Dataset
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I add a Dataset to the DatasetCollection
        dataset_1 = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_1.id)
        dataset_collection.add_item(dataset_1)
        await dataset_collection.store_async(synapse_client=self.syn)

        # AND I query the DatasetCollection with a part mask with results only
        QUERY_RESULTS = 0x1
        row = await DatasetCollection.query_part_mask_async(
            query=f"SELECT * FROM {dataset_collection.id}", part_mask=QUERY_RESULTS
        )
        # THEN the data in the columns should match
        assert row.result["id"][0] == dataset_1.id
        assert row.result["name"][0] == dataset_1.name
        assert row.result["description"][0] == dataset_1.description

        # AND the part mask should be reflected in the results
        assert row.count is None
        assert row.sum_file_sizes is None
        assert row.last_updated_on is None

    async def test_dataset_collection_update_rows(
        self, syn: Synapse, project_model: Project, dataset: Dataset
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test dataset collection",
            columns=[
                Column(name="my_annotation", column_type=ColumnType.STRING),
            ],
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I add a Dataset to the DatasetCollection
        dataset_1 = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_1.id)
        dataset_collection.add_item(dataset_1)
        await dataset_collection.store_async(synapse_client=self.syn)

        # AND I update rows in the dataset collection
        modified_data = pd.DataFrame(
            {
                "id": [dataset_1.id],
                "my_annotation": ["good dataset"],
            }
        )
        await dataset_collection.update_rows_async(
            values=modified_data,
            primary_keys=["id"],
            wait_for_eventually_consistent_view=True,
            dry_run=False,
        )

        # AND I query the dataset collection
        row = await DatasetCollection.query_async(
            query=f"SELECT my_annotation FROM {dataset_collection.id} WHERE id = '{dataset_1.id}'",
        )
        assert row["my_annotation"][0] == "good dataset"

    async def test_dataset_collection_snapshot(
        self, syn: Synapse, project_model: Project, dataset: Dataset
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I add a Dataset to the DatasetCollection
        dataset_1 = await dataset.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_1.id)
        dataset_collection.add_item(dataset_1)
        await dataset_collection.store_async(synapse_client=self.syn)

        # AND I take a snapshot of the DatasetCollection
        await dataset_collection.snapshot_async(synapse_client=self.syn)
        # AND I update the DatasetCollection
        dataset_collection.name = "Updated dataset collection"
        # AND I take a new snapshot of the DatasetCollection
        await dataset_collection.snapshot_async(synapse_client=self.syn)

        # THEN the first snapshot should be the same as the original dataset collection
        dataset_collection_version_1 = await DatasetCollection(
            id=dataset_collection.id, version_number=1
        ).get_async(synapse_client=self.syn)
        assert dataset_collection_version_1.id == dataset_collection.id
        assert dataset_collection_version_1.name == dataset_collection.name
        assert (
            dataset_collection_version_1.description == dataset_collection.description
        )
        assert dataset_collection_version_1.items == dataset_collection.items

        # AND the second snapshot should be the updated dataset collection
        dataset_collection_version_2 = await DatasetCollection(
            id=dataset_collection.id, version_number=2
        ).get_async(synapse_client=self.syn)
        assert dataset_collection_version_2.id == dataset_collection.id
        assert dataset_collection_version_2.name == dataset_collection.name
        assert (
            dataset_collection_version_2.description == dataset_collection.description
        )
        assert dataset_collection_version_2.items == dataset_collection.items

    async def test_delete_dataset_collection(
        self, syn: Synapse, project_model: Project
    ) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I delete the DatasetCollection
        await dataset_collection.delete_async(synapse_client=self.syn)

        # THEN the DatasetCollection should be deleted
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {dataset_collection.id} is in trash can.",
        ):
            await DatasetCollection(id=dataset_collection.id).get_async(
                synapse_client=self.syn
            )


class TestDatasetCollectionColumns:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_add_column(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection = await DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset_collection.id)

        # WHEN I add a column to the DatasetCollection
        dataset_collection.add_column(
            Column(name="my_annotation", column_type=ColumnType.STRING)
        )
        await dataset_collection.store_async(synapse_client=self.syn)

        # AND I retrieve the DatasetCollection
        new_dataset_collection_instance = await DatasetCollection(
            id=dataset_collection.id
        ).get_async(synapse_client=self.syn)

        # THEN the column should be added to the DatasetCollection
        assert "my_annotation" in new_dataset_collection_instance.columns

    async def test_delete_column(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN a DatasetCollection with custom columns in Synapse
        dataset_collection_name = str(uuid.uuid4())
        old_column_name = "my_annotation"
        column_to_keep = "my_annotation_2"
        old_dataset_collection_instance = await DatasetCollection(
            name=dataset_collection_name,
            parent_id=project_model.id,
            include_default_columns=False,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(old_dataset_collection_instance.id)

        # WHEN I delete a column from the DatasetCollection
        old_dataset_collection_instance.delete_column(name=old_column_name)

        # AND I store the DatasetCollection
        await old_dataset_collection_instance.store_async(synapse_client=self.syn)

        # THEN the column should be deleted from the DatasetCollection
        assert old_column_name not in old_dataset_collection_instance.columns

        # AND the column to keep should be in the DatasetCollection
        assert column_to_keep in old_dataset_collection_instance.columns
        assert len(old_dataset_collection_instance.columns) == 1

    async def test_reorder_column(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection_name = str(uuid.uuid4())
        first_column_name = "first"
        second_column_name = "second"
        old_dataset_collection_instance = DatasetCollection(
            name=dataset_collection_name,
            parent_id=project_model.id,
            include_default_columns=False,
            columns=[
                Column(name=first_column_name, column_type=ColumnType.STRING),
                Column(name=second_column_name, column_type=ColumnType.STRING),
            ],
        )
        old_dataset_collection_instance = (
            await old_dataset_collection_instance.store_async(synapse_client=self.syn)
        )
        self.schedule_for_cleanup(old_dataset_collection_instance.id)

        # WHEN I reorder the columns
        old_dataset_collection_instance.reorder_column(
            name=second_column_name,
            index=0,
        )
        await old_dataset_collection_instance.store_async(synapse_client=self.syn)

        # THEN the columns should be reordered
        assert list(old_dataset_collection_instance.columns.keys()) == [
            second_column_name,
            first_column_name,
        ]

    async def test_rename_column(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN a DatasetCollection in Synapse
        dataset_collection_name = str(uuid.uuid4())
        old_column_name = "column_string"
        old_dataset_collection_instance = DatasetCollection(
            name=dataset_collection_name,
            parent_id=project_model.id,
            columns=[Column(name=old_column_name, column_type=ColumnType.STRING)],
        )
        old_dataset_collection_instance = (
            await old_dataset_collection_instance.store_async(synapse_client=self.syn)
        )
        self.schedule_for_cleanup(old_dataset_collection_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_dataset_collection_instance.columns[old_column_name].name = new_column_name

        # AND I store the DatasetCollection
        await old_dataset_collection_instance.store_async(synapse_client=self.syn)

        # THEN the column name should be updated on the existing DatasetCollection instance
        assert old_dataset_collection_instance.columns[new_column_name] is not None
        assert old_column_name not in old_dataset_collection_instance.columns

        # AND the new column name should be reflected in the Synapse DatasetCollection
        new_dataset_collection_instance = await DatasetCollection(
            id=old_dataset_collection_instance.id
        ).get_async(synapse_client=self.syn)
        assert new_dataset_collection_instance.columns[new_column_name] is not None
        assert old_column_name not in new_dataset_collection_instance.columns
