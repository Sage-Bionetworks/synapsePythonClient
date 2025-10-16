"""Integration tests for the synapseclient.models.CurationTask class (async)."""

import os
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Column,
    ColumnType,
    CurationTask,
    EntityView,
    FileBasedMetadataTaskProperties,
    Folder,
    Project,
    RecordBasedMetadataTaskProperties,
    RecordSet,
    ViewTypeMask,
)


class TestCurationTaskStoreAsync:
    """Tests for the CurationTask.store_async method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def folder_with_view(
        self, project_model: Project
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView for file-based testing."""
        # Create a folder
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # Create an EntityView for the folder
        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
            Column(name="createdOn", column_type=ColumnType.DATE),
            Column(name="createdBy", column_type=ColumnType.USERID),
            Column(name="etag", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="type", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="parentId", column_type=ColumnType.ENTITYID),
            Column(name="benefactorId", column_type=ColumnType.ENTITYID),
            Column(name="projectId", column_type=ColumnType.ENTITYID),
            Column(name="modifiedOn", column_type=ColumnType.DATE),
            Column(name="modifiedBy", column_type=ColumnType.USERID),
            Column(name="dataFileHandleId", column_type=ColumnType.FILEHANDLEID),
        ]

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
            columns=columns,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    @pytest.fixture(scope="function")
    async def record_set(self, project_model: Project) -> RecordSet:
        """Create a RecordSet for record-based testing."""
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # Create test data as a pandas DataFrame
        test_data = pd.DataFrame(
            {
                "title": [
                    "Pasta Carbonara",
                    "Chicken Tikka Masala",
                    "Beef Tacos",
                    "Sushi Roll",
                    "French Onion Soup",
                ],
                "regional_cuisine": [
                    "Italian",
                    "Indian",
                    "Mexican",
                    "Japanese",
                    "French",
                ],
                "prep_time_minutes": [30, 45, 20, 60, 90],
                "difficulty": ["Medium", "Hard", "Easy", "Hard", "Medium"],
                "vegetarian": [False, False, False, False, True],
            }
        )

        # Create a temporary CSV file
        temp_fd, filename = tempfile.mkstemp(suffix=".csv")
        try:
            os.close(temp_fd)  # Close the file descriptor
            test_data.to_csv(filename, index=False)
            self.schedule_for_cleanup(filename)

            record_set = await RecordSet(
                name=str(uuid.uuid4()),
                parent_id=folder.id,
                path=filename,
                upsert_keys=["title", "regional_cuisine"],
            ).store_async(synapse_client=self.syn)
            self.schedule_for_cleanup(record_set.id)

            return record_set
        except Exception:
            # Clean up the temp file if something goes wrong
            if os.path.exists(filename):
                os.unlink(filename)
            raise

    async def test_store_file_based_curation_task_async(
        self, project_model: Project, folder_with_view: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # AND a FileBasedMetadataTaskProperties
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )

        # AND a CurationTask
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        curation_task = CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Please curate this test data.",
            task_properties=task_properties,
        )

        # WHEN I store the curation task asynchronously
        stored_task = await curation_task.store_async(synapse_client=self.syn)

        # THEN the task should be stored successfully
        assert stored_task.task_id is not None
        assert stored_task.data_type == data_type
        assert stored_task.project_id == project_model.id
        assert stored_task.instructions == "Please curate this test data."
        assert isinstance(stored_task.task_properties, FileBasedMetadataTaskProperties)
        assert stored_task.task_properties.upload_folder_id == folder.id
        assert stored_task.task_properties.file_view_id == entity_view.id
        assert stored_task.etag is not None
        assert stored_task.created_on is not None
        assert stored_task.created_by is not None

    async def test_store_record_based_curation_task_async(
        self, project_model: Project, record_set: RecordSet
    ) -> None:
        # GIVEN a project and record set
        # AND a RecordBasedMetadataTaskProperties
        task_properties = RecordBasedMetadataTaskProperties(
            record_set_id=record_set.id,
        )

        # AND a CurationTask
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        curation_task = CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Please curate this record-based test data.",
            task_properties=task_properties,
        )

        # WHEN I store the curation task asynchronously
        stored_task = await curation_task.store_async(synapse_client=self.syn)

        # THEN the task should be stored successfully
        assert stored_task.task_id is not None
        assert stored_task.data_type == data_type
        assert stored_task.project_id == project_model.id
        assert stored_task.instructions == "Please curate this record-based test data."
        assert isinstance(
            stored_task.task_properties, RecordBasedMetadataTaskProperties
        )
        assert stored_task.task_properties.record_set_id == record_set.id
        assert stored_task.etag is not None
        assert stored_task.created_on is not None
        assert stored_task.created_by is not None

    async def test_store_update_existing_curation_task_async(
        self, project_model: Project, record_set: RecordSet
    ) -> None:
        # GIVEN an existing curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        original_properties = RecordBasedMetadataTaskProperties(
            record_set_id=record_set.id
        )
        original_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Original instructions",
            task_properties=original_properties,
        ).store_async(synapse_client=self.syn)

        # WHEN I create a new task with the same data_type and project_id but different instructions
        updated_task = CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Updated instructions",
        )

        stored_updated_task = await updated_task.store_async(synapse_client=self.syn)

        # THEN the existing task should be updated
        assert stored_updated_task.task_id == original_task.task_id
        assert stored_updated_task.instructions == "Updated instructions"
        assert stored_updated_task.data_type == data_type
        assert stored_updated_task.project_id == project_model.id
        # The task_properties should be preserved from the original task
        assert isinstance(
            stored_updated_task.task_properties, RecordBasedMetadataTaskProperties
        )
        assert stored_updated_task.task_properties.record_set_id == record_set.id

    async def test_store_validation_errors_async(self) -> None:
        # GIVEN a CurationTask without required fields
        curation_task = CurationTask()

        # WHEN I try to store it without project_id
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="project_id is required"):
            await curation_task.store_async(synapse_client=self.syn)

        # AND WHEN I provide project_id but not data_type
        curation_task.project_id = "syn123"
        with pytest.raises(ValueError, match="data_type is required"):
            await curation_task.store_async(synapse_client=self.syn)


class TestCurationTaskGetAsync:
    """Tests for the CurationTask.get_async method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def folder_with_view(
        self, project_model: Project
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView for file-based testing."""
        # Create a folder
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # Create required columns for the EntityView
        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
            Column(name="createdOn", column_type=ColumnType.DATE),
            Column(name="createdBy", column_type=ColumnType.USERID),
            Column(name="etag", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="type", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="parentId", column_type=ColumnType.ENTITYID),
            Column(name="benefactorId", column_type=ColumnType.ENTITYID),
            Column(name="projectId", column_type=ColumnType.ENTITYID),
            Column(name="modifiedOn", column_type=ColumnType.DATE),
            Column(name="modifiedBy", column_type=ColumnType.USERID),
            Column(name="dataFileHandleId", column_type=ColumnType.FILEHANDLEID),
        ]

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
            columns=columns,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    async def test_get_curation_task_async(
        self, project_model: Project, folder_with_view: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # GIVEN an existing curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )
        original_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Test instructions",
            task_properties=task_properties,
        ).store_async(synapse_client=self.syn)

        # WHEN I get the task by ID asynchronously
        retrieved_task = await CurationTask(task_id=original_task.task_id).get_async(
            synapse_client=self.syn
        )

        # THEN the retrieved task should match the original
        assert retrieved_task.task_id == original_task.task_id
        assert retrieved_task.data_type == data_type
        assert retrieved_task.project_id == project_model.id
        assert retrieved_task.instructions == "Test instructions"
        assert isinstance(
            retrieved_task.task_properties, FileBasedMetadataTaskProperties
        )
        assert retrieved_task.task_properties.upload_folder_id == folder.id
        assert retrieved_task.task_properties.file_view_id == entity_view.id
        assert retrieved_task.etag == original_task.etag
        assert retrieved_task.created_on == original_task.created_on
        assert retrieved_task.created_by == original_task.created_by

    async def test_get_validation_error_async(self) -> None:
        # GIVEN a CurationTask without a task_id
        curation_task = CurationTask()

        # WHEN I try to get it
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="task_id is required to get a CurationTask"
        ):
            await curation_task.get_async(synapse_client=self.syn)

    async def test_get_non_existent_task_async(self) -> None:
        # GIVEN a non-existent task ID
        curation_task = CurationTask(task_id=999999)

        # WHEN I try to get it
        # THEN it should raise a SynapseHTTPError
        with pytest.raises(SynapseHTTPError):
            await curation_task.get_async(synapse_client=self.syn)


class TestCurationTaskDeleteAsync:
    """Tests for the CurationTask.delete_async method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def folder_with_view(
        self, project_model: Project
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView for file-based testing."""
        # Create a folder
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # Create required columns for the EntityView
        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
            Column(name="createdOn", column_type=ColumnType.DATE),
            Column(name="createdBy", column_type=ColumnType.USERID),
            Column(name="etag", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="type", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="parentId", column_type=ColumnType.ENTITYID),
            Column(name="benefactorId", column_type=ColumnType.ENTITYID),
            Column(name="projectId", column_type=ColumnType.ENTITYID),
            Column(name="modifiedOn", column_type=ColumnType.DATE),
            Column(name="modifiedBy", column_type=ColumnType.USERID),
            Column(name="dataFileHandleId", column_type=ColumnType.FILEHANDLEID),
        ]

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
            columns=columns,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    async def test_delete_curation_task_async(
        self, project_model: Project, folder_with_view: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # GIVEN an existing curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )
        curation_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Task to be deleted",
            task_properties=task_properties,
        ).store_async(synapse_client=self.syn)

        task_id = curation_task.task_id
        assert task_id is not None

        # WHEN I delete the task asynchronously
        await curation_task.delete_async(synapse_client=self.syn)

        # THEN the task should be deleted and no longer retrievable
        with pytest.raises(SynapseHTTPError):
            await CurationTask(task_id=task_id).get_async(synapse_client=self.syn)

    async def test_delete_validation_error_async(self) -> None:
        # GIVEN a CurationTask without a task_id
        curation_task = CurationTask()

        # WHEN I try to delete it
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="task_id is required to delete a CurationTask"
        ):
            await curation_task.delete_async(synapse_client=self.syn)


class TestCurationTaskListAsync:
    """Tests for the CurationTask.list_async method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def folder_with_view(
        self, project_model: Project
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView for file-based testing."""
        # Create a folder
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # Create required columns for the EntityView
        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="name", column_type=ColumnType.STRING, maximum_size=256),
            Column(name="createdOn", column_type=ColumnType.DATE),
            Column(name="createdBy", column_type=ColumnType.USERID),
            Column(name="etag", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="type", column_type=ColumnType.STRING, maximum_size=64),
            Column(name="parentId", column_type=ColumnType.ENTITYID),
            Column(name="benefactorId", column_type=ColumnType.ENTITYID),
            Column(name="projectId", column_type=ColumnType.ENTITYID),
            Column(name="modifiedOn", column_type=ColumnType.DATE),
            Column(name="modifiedBy", column_type=ColumnType.USERID),
            Column(name="dataFileHandleId", column_type=ColumnType.FILEHANDLEID),
        ]

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
            columns=columns,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    async def test_list_curation_tasks_async(
        self, project_model: Project, folder_with_view: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # GIVEN multiple curation tasks in a project
        tasks_data = []
        for i in range(3):
            data_type = f"test_data_type_{i}_{str(uuid.uuid4()).replace('-', '_')}"
            task_properties = FileBasedMetadataTaskProperties(
                upload_folder_id=folder.id,
                file_view_id=entity_view.id,
            )
            task = await CurationTask(
                data_type=data_type,
                project_id=project_model.id,
                instructions=f"Instructions for task {i}",
                task_properties=task_properties,
            ).store_async(synapse_client=self.syn)
            tasks_data.append((data_type, task.task_id))

        # WHEN I list all curation tasks for the project asynchronously
        listed_tasks = []
        async for task in CurationTask.list_async(
            project_id=project_model.id, synapse_client=self.syn
        ):
            listed_tasks.append(task)

        # THEN I should get all the created tasks
        assert len(listed_tasks) >= 3  # There might be other tasks from other tests

        # Check that our created tasks are in the list
        listed_task_ids = [task.task_id for task in listed_tasks]
        listed_data_types = [task.data_type for task in listed_tasks]

        for data_type, task_id in tasks_data:
            assert task_id in listed_task_ids
            assert data_type in listed_data_types

        # Verify the structure of retrieved tasks
        for task in listed_tasks:
            if task.task_id in [t[1] for t in tasks_data]:
                assert task.project_id == project_model.id
                assert task.task_properties is not None
                assert task.etag is not None
                assert task.created_on is not None
                assert task.created_by is not None

    async def test_list_empty_project_async(self) -> None:
        # GIVEN a project with no curation tasks
        empty_project = await Project(name=str(uuid.uuid4())).store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(empty_project.id)

        # WHEN I list curation tasks for the project asynchronously
        listed_tasks = []
        async for task in CurationTask.list_async(
            project_id=empty_project.id, synapse_client=self.syn
        ):
            listed_tasks.append(task)

        # THEN I should get an empty list
        assert len(listed_tasks) == 0
