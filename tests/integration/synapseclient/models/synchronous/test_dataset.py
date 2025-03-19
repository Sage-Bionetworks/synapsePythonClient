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

    def test_create_empty_dataset(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN an empty Dataset
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )

        # WHEN I store the dataset
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # THEN the dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that dataset from Synapse
        new_dataset_instance = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert new_dataset_instance is not None
        assert new_dataset_instance.name == dataset.name
        assert new_dataset_instance.id == dataset.id
        assert new_dataset_instance.description == dataset.description

    def test_create_dataset_with_file(self, project_model: Project, file: File) -> None:
        # GIVEN a File on Synapse
        file_1 = file.store(parent=project_model)

        # WHEN I create a Dataset with that File
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file_1)
        dataset.store(synapse_client=self.syn)

        # THEN the dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that dataset from Synapse
        new_dataset_instance = Dataset(id=dataset.id).get(synapse_client=self.syn)
        assert new_dataset_instance is not None
        assert new_dataset_instance.name == dataset.name
        assert new_dataset_instance.id == dataset.id
        assert new_dataset_instance.description == dataset.description
        assert list(new_dataset_instance.columns.keys()) == DEFAULT_COLUMNS
        assert new_dataset_instance.items == [
            EntityRef(id=file_1.id, version=file_1.version_number),
        ]

    def test_create_dataset_with_folder(
        self, project_model: Project, folder: Folder
    ) -> None:
        # GIVEN a Folder with 3 files on Synapse
        files = [self.create_file_instance(self.schedule_for_cleanup) for _ in range(3)]
        folder.files = files
        folder = folder.store(parent=project_model)
        # AND an Dataset with that Folder
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(folder)
        # WHEN I store the Dataset on Synapse
        dataset.store(synapse_client=self.syn)
        # THEN the Dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that Dataset from Synapse
        new_dataset_instance = Dataset(id=dataset.id).get(synapse_client=self.syn)
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

    def test_create_dataset_with_files_and_folders(
        self, project_model: Project, file: File, folder: Folder
    ) -> None:
        # GIVEN a File and a Folder with 3 files on Synapse
        file = file.store(parent=project_model)
        files = [self.create_file_instance(self.schedule_for_cleanup) for _ in range(3)]
        folder.files = files
        folder = folder.store(parent=project_model)

        # WHEN I create a Dataset with both the File and the Folder
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        dataset.add_item(folder)
        dataset.store(synapse_client=self.syn)

        # THEN the Dataset should be created
        assert dataset.id is not None

        # AND I can retrieve that Dataset from Synapse
        new_dataset_instance = Dataset(id=dataset.id).get(synapse_client=self.syn)
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

    def test_update_dataset_attributes(
        self, syn: Synapse, project_model: Project
    ) -> None:
        # GIVEN an empty Dataset
        original_dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        # WHEN I store the Dataset
        original_dataset = original_dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(original_dataset.id)

        # AND I update attributes of the dataset
        updated_dataset = Dataset(id=original_dataset.id).get(synapse_client=self.syn)
        updated_dataset.name = str(uuid.uuid4())
        updated_dataset.description = "Updated description"
        # AND I store the updated dataset
        updated_dataset = updated_dataset.store(synapse_client=self.syn)

        # AND I retrieve the dataset with its original id
        retrieved_dataset = Dataset(id=original_dataset.id).get(synapse_client=self.syn)
        # THEN the dataset should be updated
        assert retrieved_dataset is not None
        assert retrieved_dataset.name == updated_dataset.name
        assert retrieved_dataset.description == updated_dataset.description
        # AND all versions should have the same id
        assert retrieved_dataset.id == updated_dataset.id == original_dataset.id

    def test_query_dataset(self, project_model: Project, file: File) -> None:
        # GIVEN a Dataset with a File
        file = file.store(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        dataset.store(synapse_client=self.syn)
        # WHEN I query the dataset
        row = Dataset.query(query=f"SELECT * FROM {dataset.id} WHERE id = '{file.id}'")
        # THEN the dataset row contain expected values
        assert row["id"][0] == file.id
        assert row["name"][0] == file.name
        assert row["description"][0] == file.description

    def test_part_mask_query_everything(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset with a File
        file = file.store(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I query the dataset with a part mask
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

    def test_part_mask_query_results_only(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset with a File
        file = file.store(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset.add_item(file)
        dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I query the dataset with a part mask
        QUERY_RESULTS = 0x1
        results = Dataset.query_part_mask(
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

    def test_update_dataset_rows(
        self, syn: Synapse, project_model: Project, file: File
    ) -> None:
        # GIVEN a Dataset with a File and a custom column
        file = file.store(parent=project_model)
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
            columns=[Column(name="my_annotation", column_type=ColumnType.STRING)],
        )
        dataset.add_item(file)
        dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)

        # WHEN I update rows in the dataset
        modified_data = pd.DataFrame(
            {
                "id": [file.id],
                "my_annotation": ["good data"],
            }
        )
        dataset.update_rows(
            values=modified_data,
            primary_keys=["id"],
            wait_for_eventually_consistent_view=True,
            dry_run=False,
        )
        dataset.store(synapse_client=self.syn)
        # AND I query the dataset
        row = Dataset.query(
            query=f"SELECT my_annotation FROM {dataset.id} WHERE id = '{file.id}'"
        )
        # THEN the dataset row should be updated
        assert row["my_annotation"][0] == "good data"

    def test_update_dataset_remove_item(
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
            file = file.store(parent=project_model)
            dataset.add_item(file)
        dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        # WHEN I remove one of the Files
        assert len(dataset.items) == 3
        dataset.remove_item(files[0])
        dataset.store(synapse_client=self.syn)
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

    def test_delete_dataset(self, syn: Synapse, project_model: Project) -> None:
        # GIVEN an empty Dataset
        dataset = Dataset(
            name=str(uuid.uuid4()),
            description="Test dataset",
            parent_id=project_model.id,
        )
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        assert dataset.id is not None
        # WHEN I delete the Dataset
        dataset.delete(synapse_client=self.syn)
        # THEN the Dataset should be deleted
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: Entity {dataset.id} is in trash can.",
        ):
            Dataset(id=dataset.id).get(synapse_client=self.syn)


class TestDatasetColumns:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_add_column(
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
        dataset = dataset.store(synapse_client=self.syn)
        self.schedule_for_cleanup(dataset.id)
        # WHEN I add a column to the dataset
        dataset.add_column(
            column=Column(name="my_annotation", column_type=ColumnType.STRING)
        )
        dataset.store(synapse_client=self.syn)
        # THEN the dataset should have the new column
        assert "my_annotation" in dataset.columns

    def test_delete_column(self, project_model: Project) -> None:
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
        old_dataset_instance = old_dataset_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_dataset_instance.id)

        # WHEN I delete the column
        old_dataset_instance.delete_column(name=old_column_name)

        # AND I store the dataset
        old_dataset_instance.store(synapse_client=self.syn)

        # THEN the column should be removed from the dataset instance
        assert old_column_name not in old_dataset_instance.columns

        # AND the column to keep should still be in the dataset instance
        assert column_to_keep in old_dataset_instance.columns
        assert len(old_dataset_instance.columns.values()) == 1

        # AND the column should be removed from the Synapse dataset
        new_dataset_instance = Dataset(id=old_dataset_instance.id).get(
            synapse_client=self.syn
        )
        assert old_column_name not in new_dataset_instance.columns

        # AND the column to keep should still be in the Synapse dataset
        assert column_to_keep in new_dataset_instance.columns
        assert len(new_dataset_instance.columns.values()) == 1

    def test_reorder_column(self, project_model: Project) -> None:
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
        old_dataset_instance = old_dataset_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_dataset_instance.id)

        # WHEN I reorder the columns
        old_dataset_instance.reorder_column(
            name=second_column_name,
            index=0,
        )
        old_dataset_instance.store(synapse_client=self.syn)

        # THEN the columns should be reordered
        assert list(old_dataset_instance.columns.keys()) == [
            second_column_name,
            first_column_name,
        ]

    def test_rename_column(self, project_model: Project) -> None:
        # GIVEN a dataset in Synapse
        dataset_name = str(uuid.uuid4())
        old_column_name = "column_string"
        old_dataset_instance = Dataset(
            name=dataset_name,
            parent_id=project_model.id,
            include_default_columns=False,
            columns=[Column(name=old_column_name, column_type=ColumnType.STRING)],
        )
        old_dataset_instance = old_dataset_instance.store(synapse_client=self.syn)
        self.schedule_for_cleanup(old_dataset_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_dataset_instance.columns[old_column_name].name = new_column_name

        # AND I store the dataset
        old_dataset_instance.store(synapse_client=self.syn)

        # THEN the column name should be updated on the existing dataset instance
        assert old_dataset_instance.columns[new_column_name] is not None
        assert old_column_name not in old_dataset_instance.columns

        # AND the new column name should be reflected in the Synapse dataset
        new_dataset_instance = Dataset(id=old_dataset_instance.id).get(
            synapse_client=self.syn
        )
        assert new_dataset_instance.columns[new_column_name] is not None
        assert old_column_name not in new_dataset_instance.columns
