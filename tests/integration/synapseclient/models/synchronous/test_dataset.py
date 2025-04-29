import uuid
from typing import Callable, List, Optional

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
    """Integration tests for Dataset functionality."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_file_instance(self) -> File:
        """Helper to create a file instance"""
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        return File(
            path=filename,
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE,
        )

    async def create_dataset_with_items(
        self,
        project_model: Project,
        files: Optional[List[File]] = None,
        folders: Optional[List[Folder]] = None,
        name: Optional[str] = None,
        description: str = "Test dataset",
        columns: Optional[List[Column]] = None,
    ) -> Dataset:
        """Helper to create a dataset with optional items"""
        dataset = Dataset(
            name=name or str(uuid.uuid4()),
            description=description,
            parent_id=project_model.id,
            columns=columns or [],
        )

        # Add files if provided
        if files:
            for file in files:
                stored_file = file.store(parent=project_model)
                dataset.add_item(stored_file)

        # Add folders if provided
        if folders:
            for folder in folders:
                stored_folder = folder.store(parent=project_model)
                dataset.add_item(stored_folder)

        # Store the dataset
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        return dataset

    async def test_dataset_basic_operations(self, project_model: Project) -> None:
        """Test dataset creation, retrieval, updating and deletion"""
        # GIVEN a name and description for a dataset
        dataset_name = str(uuid.uuid4())
        dataset_description = "Test dataset basic operations"

        # WHEN I create an empty dataset
        dataset = Dataset(
            name=dataset_name,
            description=dataset_description,
            parent_id=project_model.id,
        )
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # THEN the dataset should be created with an ID
        assert dataset.id is not None

        # WHEN I retrieve the dataset
        retrieved_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)

        # THEN it should have the expected properties
        assert retrieved_dataset is not None
        assert retrieved_dataset.name == dataset_name
        assert retrieved_dataset.id == dataset.id
        assert retrieved_dataset.description == dataset_description

        # WHEN I update the dataset attributes
        updated_name = str(uuid.uuid4())
        updated_description = "Updated description"
        dataset.name = updated_name
        dataset.description = updated_description
        dataset.store(synapse_client=self.syn)

        # THEN the updates should be reflected when retrieved
        retrieved_updated = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert retrieved_updated.name == updated_name
        assert retrieved_updated.description == updated_description
        assert retrieved_updated.id == dataset.id  # ID remains the same

        # WHEN I delete the dataset
        dataset.delete(synapse_client=self.syn)

        # THEN it should no longer be accessible
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {dataset.id} is in trash can.",
        ):
            Dataset(id=dataset.id).get(synapse_client=self.syn)

    async def test_dataset_with_items(self, project_model: Project) -> None:
        """Test creating and managing a dataset with various items (files, folders)"""
        # GIVEN 3 files and a folder with 2 files
        files = [self.create_file_instance() for _ in range(3)]
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        folder_files = [self.create_file_instance() for _ in range(2)]

        # WHEN I store the files and folder
        stored_files = []
        for file in files:
            stored_file = file.store(parent=project_model)
            stored_files.append(stored_file)

        folder.files = folder_files
        stored_folder = folder.store(parent=project_model)

        # AND create a dataset with these items
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset with items",
            parent_id=project_model.id,
        )

        # Add individual files
        for file in stored_files:
            dataset.add_item(file)

        # Add folder
        dataset.add_item(stored_folder)

        # Store the dataset
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # THEN the dataset should contain all expected items
        retrieved_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)

        # Verify dataset has all expected files
        expected_items = [
            EntityRef(id=file.id, version=file.version_number) for file in stored_files
        ] + [
            EntityRef(id=file.id, version=file.version_number)
            for file in stored_folder.files
        ]

        assert len(retrieved_dataset.items) == len(expected_items)
        for item in expected_items:
            assert item in retrieved_dataset.items

        # WHEN I remove one file from the dataset
        dataset.remove_item(stored_files[0])
        dataset.store(synapse_client=self.syn)

        # THEN that file should no longer be in the dataset
        updated_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert (
            EntityRef(id=stored_files[0].id, version=stored_files[0].version_number)
            not in updated_dataset.items
        )
        assert len(updated_dataset.items) == len(expected_items) - 1

    async def test_dataset_query_operations(self, project_model: Project) -> None:
        """Test querying a dataset and different query modes"""
        # GIVEN a dataset with a file and custom column
        file = self.create_file_instance()
        stored_file = file.store(parent=project_model)

        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset for queries",
            parent_id=project_model.id,
            columns=[Column(name="my_annotation", column_type=ColumnType.STRING)],
        )
        dataset.add_item(stored_file)
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I update rows in the dataset
        modified_data = pd.DataFrame(
            {
                "id": [stored_file.id],
                "my_annotation": ["test_value"],
            }
        )
        dataset.update_rows(
            values=modified_data,
            primary_keys=["id"],
            wait_for_eventually_consistent_view=True,
            dry_run=False,
        )

        # THEN I can query the data
        row = Dataset.query(
            query=f"SELECT * FROM {dataset.id} WHERE id = '{stored_file.id}'"
        )

        # AND the query results should match the expected values
        assert row["id"][0] == stored_file.id
        assert row["name"][0] == stored_file.name
        assert row["description"][0] == stored_file.description
        assert row["my_annotation"][0] == "test_value"

        # WHEN I use part_mask to query with additional information
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZE_BYTES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZE_BYTES | LAST_UPDATED_ON

        results = Dataset.query_part_mask(
            query=f"SELECT * FROM {dataset.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN all requested parts should be included in the result
        assert results.result["id"][0] == stored_file.id
        assert results.count == 1
        assert results.sum_file_sizes is not None
        assert results.last_updated_on is not None

        # WHEN I query with only results requested
        results_only = Dataset.query_part_mask(
            query=f"SELECT * FROM {dataset.id}",
            synapse_client=self.syn,
            part_mask=QUERY_RESULTS,
        )

        # THEN only the results should be included (not count, sum_file_sizes, or last_updated_on)
        assert results_only.result["id"][0] == stored_file.id
        assert results_only.count is None
        assert results_only.sum_file_sizes is None
        assert results_only.last_updated_on is None

    async def test_dataset_column_operations(self, project_model: Project) -> None:
        """Test operations on dataset columns: add, rename, reorder, delete"""
        # GIVEN a dataset with no custom columns
        dataset = await self.create_dataset_with_items(project_model)

        # WHEN I add a column to the dataset
        column_name = "test_column"
        dataset.add_column(
            column=Column(name=column_name, column_type=ColumnType.STRING)
        )
        dataset.store(synapse_client=self.syn)

        # THEN the column should be present in the dataset
        updated_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert column_name in updated_dataset.columns

        # WHEN I add a second column and rename the first
        second_column = "second_column"
        dataset.add_column(
            column=Column(name=second_column, column_type=ColumnType.INTEGER)
        )
        new_name = "renamed_column"
        dataset.columns[column_name].name = new_name
        dataset.store(synapse_client=self.syn)

        # THEN the columns should reflect these changes
        updated_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert new_name in updated_dataset.columns
        assert second_column in updated_dataset.columns
        assert column_name not in updated_dataset.columns

        # WHEN I reorder the columns
        dataset.reorder_column(name=second_column, index=0)
        dataset.store(synapse_client=self.syn)

        # THEN the columns should be in the new order
        updated_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)
        column_keys = [
            k for k in updated_dataset.columns.keys() if k not in DEFAULT_COLUMNS
        ]
        assert column_keys[0] == second_column
        assert column_keys[1] == new_name

        # WHEN I delete a column
        dataset.delete_column(name=second_column)
        dataset.store(synapse_client=self.syn)

        # THEN the column should be removed
        updated_dataset = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert second_column not in updated_dataset.columns
        assert new_name in updated_dataset.columns

    async def test_dataset_versioning(self, project_model: Project) -> None:
        """Test creating snapshots and versioning of datasets"""
        # GIVEN a dataset and two files
        file1 = self.create_file_instance()
        file2 = self.create_file_instance()

        file1 = file1.store(parent=project_model)
        file2 = file2.store(parent=project_model)

        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset versioning",
            parent_id=project_model.id,
        )
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I add the first file and create a snapshot
        dataset.add_item(file1)
        dataset.store(synapse_client=self.syn)
        dataset.snapshot(synapse_client=self.syn)

        # AND I add the second file and create another snapshot
        dataset.add_item(file2)
        dataset.store(synapse_client=self.syn)
        dataset.snapshot(synapse_client=self.syn)

        # THEN version 1 should only contain the first file
        dataset_v1 = Dataset(id=dataset.id, version_number=1).get(
            synapse_client=self.syn
        )
        assert len(dataset_v1.items) == 1
        assert dataset_v1.items[0] == EntityRef(
            id=file1.id, version=file1.version_number
        )

        # AND version 2 should contain both files
        dataset_v2 = Dataset(id=dataset.id, version_number=2).get(
            synapse_client=self.syn
        )
        assert len(dataset_v2.items) == 2
        assert EntityRef(id=file1.id, version=file1.version_number) in dataset_v2.items
        assert EntityRef(id=file2.id, version=file2.version_number) in dataset_v2.items


class TestDatasetCollection:
    """Integration tests for DatasetCollection functionality."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def create_file_instance(self) -> File:
        """Helper to create a file instance"""
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        return File(
            path=filename,
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE,
        )

    async def create_dataset(
        self, project_model: Project, has_file: bool = False
    ) -> Dataset:
        """Helper to create a dataset"""
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )

        if has_file:
            file = self.create_file_instance()
            stored_file = file.store(parent=project_model)
            dataset.add_item(stored_file)

        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        return dataset

    async def test_dataset_collection_lifecycle(self, project_model: Project) -> None:
        """Test creating, updating, and deleting a DatasetCollection"""
        # GIVEN two datasets
        dataset1 = await self.create_dataset(project_model, has_file=True)
        dataset2 = await self.create_dataset(project_model, has_file=True)

        # WHEN I create a DatasetCollection with the first dataset
        collection = DatasetCollection(
            name=str(uuid.uuid4()),
            description="Test collection",
            parent_id=project_model.id,
        )
        collection.add_item(dataset1)
        collection = collection.store(synapse_client=self.syn)
        self.schedule_for_cleanup(collection.id)

        # THEN the collection should be created and contain the dataset
        assert collection.id is not None
        assert collection.items == [
            EntityRef(id=dataset1.id, version=dataset1.version_number)
        ]

        # WHEN I retrieve the collection
        retrieved = DatasetCollection(id=collection.id).get(synapse_client=self.syn)

        # THEN it should match the original
        assert retrieved.id == collection.id
        assert retrieved.name == collection.name
        assert retrieved.description == collection.description
        assert retrieved.items == collection.items

        # WHEN I update the collection attributes and add another dataset
        new_name = str(uuid.uuid4())
        new_description = "Updated description"
        collection.name = new_name
        collection.description = new_description
        collection.add_item(dataset2)
        collection.store(synapse_client=self.syn)

        # THEN the updates should be reflected
        updated = DatasetCollection(id=collection.id).get(synapse_client=self.syn)
        assert updated.name == new_name
        assert updated.description == new_description
        assert len(updated.items) == 2
        assert (
            EntityRef(id=dataset1.id, version=dataset1.version_number) in updated.items
        )
        assert (
            EntityRef(id=dataset2.id, version=dataset2.version_number) in updated.items
        )

        # WHEN I delete the collection
        collection.delete(synapse_client=self.syn)

        # THEN it should no longer be accessible
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {collection.id} is in trash can.",
        ):
            DatasetCollection(id=collection.id).get(synapse_client=self.syn)

    async def test_dataset_collection_queries(self, project_model: Project) -> None:
        """Test querying DatasetCollections with various part masks"""
        # GIVEN a dataset and a collection with that dataset
        dataset = await self.create_dataset(project_model, has_file=True)

        collection = DatasetCollection(
            name=str(uuid.uuid4()),
            description="Test collection for queries",
            parent_id=project_model.id,
            columns=[Column(name="my_annotation", column_type=ColumnType.STRING)],
        )
        collection.add_item(dataset)
        collection = collection.store(synapse_client=self.syn)
        self.schedule_for_cleanup(collection.id)

        # WHEN I add annotations via row updates
        modified_data = pd.DataFrame(
            {
                "id": [dataset.id],
                "my_annotation": ["collection_value"],
            }
        )
        collection.update_rows(
            values=modified_data,
            primary_keys=["id"],
            wait_for_eventually_consistent_view=True,
            dry_run=False,
        )

        # THEN I can query and get the updated data
        row = DatasetCollection.query(
            query=f"SELECT * FROM {collection.id} WHERE id = '{dataset.id}'"
        )
        assert row["id"][0] == dataset.id
        assert row["name"][0] == dataset.name
        assert row["my_annotation"][0] == "collection_value"

        # WHEN I query with a part mask with all parts
        QUERY_RESULTS = 0x1
        QUERY_COUNT = 0x2
        SUM_FILE_SIZE_BYTES = 0x40
        LAST_UPDATED_ON = 0x80
        part_mask = QUERY_RESULTS | QUERY_COUNT | SUM_FILE_SIZE_BYTES | LAST_UPDATED_ON

        row = DatasetCollection.query_part_mask(
            query=f"SELECT * FROM {collection.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN I expect the row to contain expected values
        assert row.result["id"][0] == dataset.id
        assert row.result["name"][0] == dataset.name
        assert row.result["description"][0] == dataset.description

        # AND the part mask should be reflected in the row
        assert row.count == 1
        assert row.sum_file_sizes is not None
        assert row.sum_file_sizes.greater_than is not None
        assert row.sum_file_sizes.sum_file_size_bytes is not None
        assert row.last_updated_on is not None

        # WHEN I query with only results
        results_only = DatasetCollection.query_part_mask(
            query=f"SELECT * FROM {collection.id}", part_mask=QUERY_RESULTS
        )
        # THEN the data in the columns should match
        assert results_only.result["id"][0] == dataset.id
        assert results_only.result["name"][0] == dataset.name
        assert results_only.result["description"][0] == dataset.description

        # AND the part mask should be reflected in the results
        assert results_only.count is None
        assert results_only.sum_file_sizes is None
        assert results_only.last_updated_on is None

    async def test_dataset_collection_columns(self, project_model: Project) -> None:
        """Test column operations on DatasetCollections"""
        # GIVEN a DatasetCollection
        collection = DatasetCollection(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        )
        collection = collection.store(synapse_client=self.syn)
        self.schedule_for_cleanup(collection.id)

        # WHEN I add columns to the collection
        first_col = "first_column"
        second_col = "second_column"
        collection.add_column(Column(name=first_col, column_type=ColumnType.STRING))
        collection.add_column(Column(name=second_col, column_type=ColumnType.INTEGER))
        collection.store(synapse_client=self.syn)

        # THEN the columns should be in the collection
        updated = DatasetCollection(id=collection.id).get(synapse_client=self.syn)
        assert first_col in updated.columns
        assert second_col in updated.columns

        # WHEN I reorder the columns
        collection.reorder_column(name=second_col, index=0)
        collection.reorder_column(name=first_col, index=1)
        collection.store(synapse_client=self.syn)

        # THEN the columns should be in the new order
        updated = DatasetCollection(id=collection.id).get(synapse_client=self.syn)
        columns = [k for k in updated.columns.keys() if k not in DEFAULT_COLUMNS]
        assert columns[0] == second_col
        assert columns[1] == first_col

        # WHEN I rename a column
        new_name = "renamed_column"
        collection.columns[first_col].name = new_name
        collection.store(synapse_client=self.syn)

        # THEN the column should have the new name
        updated = DatasetCollection(id=collection.id).get(synapse_client=self.syn)
        assert new_name in updated.columns
        assert first_col not in updated.columns

        # WHEN I delete a column
        collection.delete_column(name=second_col)
        collection.store(synapse_client=self.syn)

        # THEN the column should be removed
        updated = DatasetCollection(id=collection.id).get(synapse_client=self.syn)
        assert second_col not in updated.columns
        assert new_name in updated.columns

    async def test_dataset_collection_versioning(self, project_model: Project) -> None:
        """Test versioning of DatasetCollections"""
        # GIVEN a DatasetCollection and datasets
        dataset1 = await self.create_dataset(project_model)
        dataset2 = await self.create_dataset(project_model)

        collection = DatasetCollection(
            name=str(uuid.uuid4()),
            description="Original description",
            parent_id=project_model.id,
        )
        collection.add_item(dataset1)
        collection = collection.store(synapse_client=self.syn)
        self.schedule_for_cleanup(collection.id)

        # WHEN I create a snapshot of version 1
        collection.snapshot(synapse_client=self.syn)

        # AND I update the collection and make version 2
        collection.name = "Updated collection"
        collection.add_item(dataset2)
        collection.store(synapse_client=self.syn)
        collection.snapshot(synapse_client=self.syn)

        # THEN version 1 should only contain the first dataset
        v1 = DatasetCollection(id=collection.id, version_number=1).get(
            synapse_client=self.syn
        )
        assert len(v1.items) == 1
        assert v1.items[0] == EntityRef(id=dataset1.id, version=dataset1.version_number)

        # AND version 2 should contain both datasets and the updated name
        v2 = DatasetCollection(id=collection.id, version_number=2).get(
            synapse_client=self.syn
        )
        assert len(v2.items) == 2
        assert v2.name == "Updated collection"
        assert EntityRef(id=dataset1.id, version=dataset1.version_number) in v2.items
        assert EntityRef(id=dataset2.id, version=dataset2.version_number) in v2.items
