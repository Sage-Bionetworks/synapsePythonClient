"""Integration tests for the synapseclient.models.CurationTask class (async)."""

import os
import tempfile
import uuid
from typing import AsyncGenerator, Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.utils import make_bogus_uuid_file
from synapseclient.models import (
    AuthorizationMode,
    CurationTask,
    CurationTaskStatus,
    EntityView,
    File,
    FileBasedMetadataTaskProperties,
    Folder,
    Grid,
    GridExecutionDetails,
    JSONSchema,
    Project,
    RecordBasedMetadataTaskProperties,
    RecordSet,
    RecordSetGenerationExecutionDetails,
    RecordSetGenerationExecutionProperties,
    SampleSheetGenerationExecutionDetails,
    SampleSheetGenerationExecutionProperties,
    SchemaOrganization,
    TaskState,
    UserProfile,
    ViewTypeMask,
    query_async,
)
from synapseclient.models.table_components import Query
from tests.integration import ASYNC_JOB_TIMEOUT_SEC, QUERY_TIMEOUT_SEC
from tests.integration.helpers import wait_for_condition


@pytest.fixture(scope="function")
async def folder_with_view(
    project_model: Project,
    syn: Synapse,
    schedule_for_cleanup: Callable[..., None],
) -> tuple[Folder, EntityView]:
    """Create a folder with an associated EntityView for file-based testing."""
    folder = await Folder(
        name=str(uuid.uuid4()),
        parent_id=project_model.id,
    ).store_async(synapse_client=syn)
    schedule_for_cleanup(folder.id)

    entity_view = await EntityView(
        name=str(uuid.uuid4()),
        parent_id=project_model.id,
        scope_ids=[folder.id],
        view_type_mask=ViewTypeMask.FILE.value,
    ).store_async(synapse_client=syn)
    schedule_for_cleanup(entity_view.id)

    return folder, entity_view


class TestCurationTaskStoreAsync:
    """Tests for the CurationTask.store_async method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    async def record_set(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> RecordSet:
        """Create a RecordSet for record-based testing."""
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

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
            schedule_for_cleanup(filename)

            record_set = await RecordSet(
                name=str(uuid.uuid4()),
                parent_id=folder.id,
                path=filename,
                upsert_keys=["title", "regional_cuisine"],
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(record_set.id)

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
            suggested_authorization_mode=AuthorizationMode.SESSION_OWNER,
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
        assert (
            stored_task.task_properties.suggested_authorization_mode
            == AuthorizationMode.SESSION_OWNER
        )
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
            suggested_authorization_mode=AuthorizationMode.SESSION_OWNER,
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
        assert (
            stored_task.task_properties.suggested_authorization_mode
            == AuthorizationMode.SESSION_OWNER
        )
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
            suggested_authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
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
        assert (
            retrieved_task.task_properties.suggested_authorization_mode
            == AuthorizationMode.SOURCE_BENEFACTOR
        )
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
    async def folder_with_record_set(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> tuple[Folder, EntityView]:
        """Create a folder with a a record set for record-based testing."""
        # Create a folder
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

        filename = make_bogus_uuid_file()
        schedule_for_cleanup(filename)

        record_set = await RecordSet(
            name=str(uuid.uuid4()),
            parent_id=folder.id,
            path=filename,
            upsert_keys=["xxx"],
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(record_set.id)

        return folder, record_set

    async def test_delete_file_based_curation_task_async(
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

        # WHEN I delete the task asynchronously, without deleting the file view
        await curation_task.delete_async(synapse_client=self.syn, delete_source=False)

        # THEN the task should be deleted and no longer retrievable
        with pytest.raises(SynapseHTTPError):
            await CurationTask(task_id=task_id).get_async(synapse_client=self.syn)

        # AND the file view should not be deleted
        await EntityView(entity_view.id).get_async(synapse_client=self.syn)

    async def test_delete_file_based_curation_task_and_fileview_async(
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

        # WHEN I delete the task and fileview asynchronously
        await curation_task.delete_async(synapse_client=self.syn, delete_source=True)

        # THEN the task should be deleted and no longer retrievable
        with pytest.raises(SynapseHTTPError):
            await CurationTask(task_id=task_id).get_async(synapse_client=self.syn)

        # AND the file view should be deleted and no longer retrievable
        with pytest.raises(SynapseHTTPError):
            await EntityView(entity_view.id).get_async(synapse_client=self.syn)

    async def test_delete_record_based_curation_task_async(
        self, project_model: Project, folder_with_record_set: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a folder, and record set
        _, record_set = folder_with_record_set

        # GIVEN an existing curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = RecordBasedMetadataTaskProperties(
            record_set_id=record_set.id,
        )
        curation_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Task to be deleted",
            task_properties=task_properties,
        ).store_async(synapse_client=self.syn)

        task_id = curation_task.task_id
        assert task_id is not None

        # WHEN I delete the task asynchronously, without deleting the record set
        await curation_task.delete_async(synapse_client=self.syn, delete_source=False)

        # THEN the task should be deleted and no longer retrievable
        with pytest.raises(SynapseHTTPError):
            await CurationTask(task_id=task_id).get_async(synapse_client=self.syn)

        # AND the record set should not be deleted
        await RecordSet(record_set.id).get_async(synapse_client=self.syn)

    async def test_delete_record_based_curation_task_and_record_set_async(
        self, project_model: Project, folder_with_record_set: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a folder, and record set
        _, record_set = folder_with_record_set

        # GIVEN an existing curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = RecordBasedMetadataTaskProperties(
            record_set_id=record_set.id,
        )
        curation_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Task to be deleted",
            task_properties=task_properties,
        ).store_async(synapse_client=self.syn)

        task_id = curation_task.task_id
        assert task_id is not None

        # WHEN I delete the task asynchronously, without deleting the record set
        await curation_task.delete_async(synapse_client=self.syn, delete_source=True)

        # THEN the task should be deleted and no longer retrievable
        with pytest.raises(SynapseHTTPError):
            await CurationTask(task_id=task_id).get_async(synapse_client=self.syn)

        # AND the record set should be deleted and not retrievable
        with pytest.raises(SynapseHTTPError):
            await RecordSet(record_set.id).get_async(synapse_client=self.syn)

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
    async def project_for_test(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Project:
        """Create a fresh project per test so tasks from other tests don't appear in listings."""
        project = await Project(name=str(uuid.uuid4())).store_async(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def folder_with_view(
        self,
        project_for_test: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView for file-based testing."""
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_for_test.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_for_test.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    async def test_list_curation_tasks_async(
        self, project_for_test: Project, folder_with_view: tuple[Folder, EntityView]
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
                project_id=project_for_test.id,
                instructions=f"Instructions for task {i}",
                task_properties=task_properties,
            ).store_async(synapse_client=self.syn)
            tasks_data.append((data_type, task.task_id))

        # WHEN I list all curation tasks for the project asynchronously
        listed_tasks = []
        async for task in CurationTask.list_async(
            project_id=project_for_test.id, synapse_client=self.syn
        ):
            listed_tasks.append(task)

        # THEN I should get exactly the 3 created tasks (isolated project)
        assert len(listed_tasks) == 3

        # Check that our created tasks are in the list
        listed_task_ids = [task.task_id for task in listed_tasks]
        listed_data_types = [task.data_type for task in listed_tasks]

        for data_type, task_id in tasks_data:
            assert task_id in listed_task_ids
            assert data_type in listed_data_types

        # Verify the structure of retrieved tasks
        for task in listed_tasks:
            assert task.project_id == project_for_test.id
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

    async def test_list_filters_async(
        self, project_for_test: Project, folder_with_view: tuple[Folder, EntityView]
    ) -> None:
        # GIVEN a newly created curation task (default state is NOT_STARTED)
        folder, entity_view = folder_with_view
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task = await CurationTask(
            data_type=data_type,
            project_id=project_for_test.id,
            instructions="Test instructions",
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=folder.id,
                file_view_id=entity_view.id,
            ),
        ).store_async(synapse_client=self.syn)

        # WHEN I list tasks filtered to NOT_STARTED
        listed_task_ids = [
            t.task_id
            async for t in CurationTask.list_async(
                project_id=project_for_test.id,
                state_filter=[TaskState.NOT_STARTED],
                synapse_client=self.syn,
            )
        ]

        # THEN exactly 1 task should appear and it should be our task
        assert len(listed_task_ids) == 1
        assert task.task_id in listed_task_ids

        # WHEN I list tasks filtered to COMPLETED
        listed_task_ids = [
            t.task_id
            async for t in CurationTask.list_async(
                project_id=project_for_test.id,
                state_filter=[TaskState.COMPLETED],
                synapse_client=self.syn,
            )
        ]

        # THEN no tasks should appear (the only task is NOT_STARTED)
        assert len(listed_task_ids) == 0


class TestCurationTaskStatusAsync:
    """Tests for the CurationTask.get_status_async and CurationTask.update_status_async methods."""

    @pytest.fixture(scope="function")
    async def grid(
        self,
        syn: Synapse,
        folder_with_view: tuple[Folder, EntityView],
        request: pytest.FixtureRequest,
    ) -> Grid:
        """Create a Grid backed by the entity view; delete it after the test."""
        _, entity_view = folder_with_view
        grid = await Grid(
            initial_query=Query(sql=f"SELECT * FROM {entity_view.id}")
        ).create_async(timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=syn)

        def delete_grid() -> None:
            grid.delete(synapse_client=syn)

        request.addfinalizer(delete_grid)
        return grid

    async def test_get_and_update_curation_task_status_async(
        self,
        syn: Synapse,
        project_model: Project,
        folder_with_view: tuple[Folder, EntityView],
        grid: Grid,
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # AND a stored curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )
        stored_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Test instructions for status flow.",
            task_properties=task_properties,
        ).store_async(synapse_client=syn)

        # WHEN I get the initial status of the task
        initial_status = await stored_task.get_status_async(synapse_client=syn)

        # THEN it should be parsed into a CurationTaskStatus tied to this task
        assert isinstance(initial_status, CurationTaskStatus)
        assert initial_status.task_id == stored_task.task_id
        assert initial_status.state == TaskState.NOT_STARTED
        # AND it should not yet reference an active grid session
        assert initial_status.execution_details is None

        # AND WHEN I modify the state to IN_PROGRESS, attach a GridExecutionDetails
        # pointing to the active grid session, and store the status
        initial_status.state = TaskState.IN_PROGRESS
        initial_status.execution_details = GridExecutionDetails(
            active_session_id=grid.session_id
        )
        updated_status = await stored_task.update_status_async(
            curation_task_status=initial_status, synapse_client=syn
        )

        # THEN the update response should reflect the new state and execution details
        assert isinstance(updated_status, CurationTaskStatus)
        assert updated_status.task_id == stored_task.task_id
        assert updated_status.state == TaskState.IN_PROGRESS
        assert isinstance(updated_status.execution_details, GridExecutionDetails)
        assert updated_status.execution_details.active_session_id == grid.session_id

        # AND WHEN I get the status again
        refetched_status = await stored_task.get_status_async(synapse_client=syn)

        # THEN the modification should have persisted on the server
        assert refetched_status.task_id == stored_task.task_id
        assert refetched_status.state == TaskState.IN_PROGRESS
        assert isinstance(refetched_status.execution_details, GridExecutionDetails)
        assert refetched_status.execution_details.active_session_id == grid.session_id


class TestCurationTaskCreateGridSessionAsync:
    """Tests for the CurationTask.create_grid_session_async method."""

    async def test_create_grid_session_async(
        self,
        syn: Synapse,
        project_model: Project,
        folder_with_view: tuple[Folder, EntityView],
        request: pytest.FixtureRequest,
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # AND a stored file-based curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
            suggested_authorization_mode=AuthorizationMode.SESSION_OWNER,
        )
        stored_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Create a grid session for this task.",
            task_properties=task_properties,
        ).store_async(synapse_client=syn)

        # WHEN I create a grid session for the task asynchronously, owned by the
        # current user
        current_user_id = (await UserProfile().get_async(synapse_client=syn)).id
        grid = await stored_task.create_grid_session_async(
            owner_principal_id=current_user_id,
            timeout=ASYNC_JOB_TIMEOUT_SEC,
            synapse_client=syn,
        )
        request.addfinalizer(lambda: grid.delete(synapse_client=syn))

        # THEN a Grid is returned with a populated session_id owned by that user
        # AND the grid carries the task's suggested authorization mode
        assert isinstance(grid, Grid)
        assert grid.session_id is not None
        assert grid.owner_principal_id == current_user_id
        assert grid.authorization_mode == AuthorizationMode.SESSION_OWNER

        # AND the curation task status now references the new grid session
        status = await stored_task.get_status_async(synapse_client=syn)
        assert isinstance(status, CurationTaskStatus)
        assert isinstance(status.execution_details, GridExecutionDetails)
        assert status.execution_details.active_session_id == grid.session_id


class TestCurationTaskSetActiveGridSessionAsync:
    """Tests for the CurationTask.set_active_grid_session_async method."""

    @pytest.fixture(scope="class")
    async def folder_with_view(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView, shared by every test in
        this class. Every consumer only reads folder.id/entity_view.id; none of them
        mutates the folder or entity view."""
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    @pytest.fixture(scope="class")
    async def grid(
        self,
        syn: Synapse,
        folder_with_view: tuple[Folder, EntityView],
        request: pytest.FixtureRequest,
    ) -> Grid:
        """Create a Grid backed by the entity view, shared by every test in this
        class. Every consumer only reads grid.session_id; none of them mutates the
        grid itself."""
        _, entity_view = folder_with_view
        grid = await Grid(
            initial_query=Query(sql=f"SELECT * FROM {entity_view.id}")
        ).create_async(timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=syn)

        def delete_grid() -> None:
            grid.delete(synapse_client=syn)

        request.addfinalizer(delete_grid)
        return grid

    async def test_set_active_grid_session_async(
        self,
        syn: Synapse,
        project_model: Project,
        folder_with_view: tuple[Folder, EntityView],
        grid: Grid,
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # AND a stored file-based curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )
        stored_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Attach an existing grid session to this task.",
            task_properties=task_properties,
        ).store_async(synapse_client=syn)

        # AND the task's initial status has no execution details
        initial_status = await stored_task.get_status_async(synapse_client=syn)
        assert initial_status.execution_details is None

        # WHEN I attach the existing grid session to the task
        updated_status = await stored_task.set_active_grid_session_async(
            active_session_id=grid.session_id, synapse_client=syn
        )

        # THEN the returned status references the grid session
        assert isinstance(updated_status, CurationTaskStatus)
        assert updated_status.task_id == stored_task.task_id
        assert isinstance(updated_status.execution_details, GridExecutionDetails)
        assert updated_status.execution_details.active_session_id == grid.session_id
        # AND the task state is not transitioned by this call
        assert updated_status.state == initial_status.state

        # AND the change persists on the server
        refetched_status = await stored_task.get_status_async(synapse_client=syn)
        assert isinstance(refetched_status.execution_details, GridExecutionDetails)
        assert refetched_status.execution_details.active_session_id == grid.session_id

    async def test_set_active_grid_session_async_replaces_existing_session(
        self,
        syn: Synapse,
        project_model: Project,
        folder_with_view: tuple[Folder, EntityView],
        grid: Grid,
        request: pytest.FixtureRequest,
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # AND a stored file-based curation task that already has an active
        # grid session linked via create_grid_session_async
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )
        stored_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Replace the active grid session on this task.",
            task_properties=task_properties,
        ).store_async(synapse_client=syn)

        original_grid = await stored_task.create_grid_session_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=syn
        )
        request.addfinalizer(lambda: original_grid.delete(synapse_client=syn))
        assert original_grid.session_id is not None
        assert original_grid.session_id != grid.session_id

        # WHEN I point the task at a different existing grid session
        updated_status = await stored_task.set_active_grid_session_async(
            active_session_id=grid.session_id, synapse_client=syn
        )

        # THEN the status now references the new session, not the original
        assert isinstance(updated_status.execution_details, GridExecutionDetails)
        assert updated_status.execution_details.active_session_id == grid.session_id

        # AND the change persists on the server
        refetched_status = await stored_task.get_status_async(synapse_client=syn)
        assert refetched_status.execution_details.active_session_id == grid.session_id

    async def test_set_active_grid_session_async_validation_error(
        self, syn: Synapse
    ) -> None:
        # GIVEN a CurationTask without a task_id
        curation_task = CurationTask()

        # WHEN I try to set an active grid session
        # THEN it should raise a ValueError from the underlying get_status call
        with pytest.raises(
            ValueError, match="task_id is required to get a CurationTask status"
        ):
            await curation_task.set_active_grid_session_async(
                active_session_id="some-session-id", synapse_client=syn
            )


class TestCurationTaskSetTaskStateAsync:
    """Test for the CurationTask.set_task_state_async method."""

    async def test_set_task_state_async(
        self,
        syn: Synapse,
        project_model: Project,
        folder_with_view: tuple[Folder, EntityView],
    ) -> None:
        # GIVEN a project, folder, and entity view
        folder, entity_view = folder_with_view

        # AND a stored file-based curation task
        data_type = f"test_data_type_{str(uuid.uuid4()).replace('-', '_')}"
        task_properties = FileBasedMetadataTaskProperties(
            upload_folder_id=folder.id,
            file_view_id=entity_view.id,
        )
        stored_task = await CurationTask(
            data_type=data_type,
            project_id=project_model.id,
            instructions="Set the task state on this curation task.",
            task_properties=task_properties,
        ).store_async(synapse_client=syn)

        # AND the task's status starts at NOT_STARTED
        initial_status = await stored_task.get_status_async(synapse_client=syn)
        assert initial_status.state == TaskState.NOT_STARTED

        # WHEN I transition the state to IN_PROGRESS
        updated_status = await stored_task.set_task_state_async(
            state=TaskState.IN_PROGRESS, synapse_client=syn
        )

        # THEN the returned status reflects the new state
        assert isinstance(updated_status, CurationTaskStatus)
        assert updated_status.task_id == stored_task.task_id
        assert updated_status.state == TaskState.IN_PROGRESS

        # AND the change persists on the server
        refetched_status = await stored_task.get_status_async(synapse_client=syn)
        assert refetched_status.state == TaskState.IN_PROGRESS


class TestCurationTaskExecuteAsync:
    """Tests for the CurationTask.execute_async method."""

    # The generated sample sheet format is established by the JSON Schema bound to the
    # destination RecordSet, so both compute task kinds need a destination whose
    # RecordSet carries a schema.
    SAMPLE_SHEET_COLUMNS = ["specimen_id", "assay", "read_length"]

    @pytest.fixture(scope="function")
    async def sample_sheet_schema_uri(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> str:
        """Create a JSON schema describing the target sample sheet format."""
        random_name = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
        organization = SchemaOrganization(name=f"SYNPY.TEST.{random_name}")
        await organization.store_async(synapse_client=syn)
        # The cleanup list is reversed at teardown, so the organization must be
        # scheduled before its schema: Synapse refuses to delete an organization that
        # still owns a schema.
        schedule_for_cleanup(organization)

        json_schema = JSONSchema(
            name="curation.compute.samplesheet", organization_name=organization.name
        )
        await json_schema.store_async(
            schema_body={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Curation Compute Sample Sheet",
                "type": "object",
                "properties": {
                    "specimen_id": {
                        "description": "The identifier of the specimen",
                        "type": "string",
                    },
                    "assay": {
                        "description": "The assay the specimen was run through",
                        "type": "string",
                    },
                    "read_length": {
                        "description": "The sequencing read length",
                        "type": "integer",
                    },
                },
            },
            version="0.0.1",
            synapse_client=syn,
        )
        schedule_for_cleanup(json_schema)

        return json_schema.uri

    @pytest.fixture(scope="function")
    async def destination_task(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
        sample_sheet_schema_uri: str,
    ) -> AsyncGenerator[CurationTask, None]:
        """
        Create the record-based CurationTask that a compute task writes its output to.

        Its RecordSet is seeded with the target columns and has the sample sheet
        schema bound to it.
        """
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

        temp_fd, filename = tempfile.mkstemp(suffix=".csv")
        os.close(temp_fd)
        schedule_for_cleanup(filename)
        pd.DataFrame(columns=self.SAMPLE_SHEET_COLUMNS).to_csv(filename, index=False)

        record_set = await RecordSet(
            name=str(uuid.uuid4()),
            parent_id=folder.id,
            path=filename,
            upsert_keys=["specimen_id"],
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(record_set.id)

        await record_set.bind_schema_async(
            json_schema_uri=sample_sheet_schema_uri, synapse_client=syn
        )

        yield await CurationTask(
            data_type=f"destination_{str(uuid.uuid4()).replace('-', '_')}",
            project_id=project_model.id,
            instructions="Receives the output of a compute task.",
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=record_set.id
            ),
        ).store_async(synapse_client=syn)

        # Synapse refuses to delete a schema that is still bound to an object, and the
        # RecordSet is not deleted until session teardown. Unbind here, at function
        # teardown, so the scheduled cleanup can delete the schema.
        await record_set.unbind_schema_async(synapse_client=syn)

    async def test_execute_record_set_generation_async(
        self,
        syn: Synapse,
        project_model: Project,
        destination_task: CurationTask,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a folder of small source files to transform
        source_folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(source_folder.id)

        for index, specimen in enumerate(["SPEC_001", "SPEC_002"]):
            temp_fd, source_path = tempfile.mkstemp(suffix=".csv")
            os.close(temp_fd)
            schedule_for_cleanup(source_path)
            pd.DataFrame(
                {
                    "specimen_id": [specimen],
                    "assay": ["rna_seq"],
                    "read_length": [100 + index],
                }
            ).to_csv(source_path, index=False)

            source_file = await File(
                parent_id=source_folder.id,
                name=f"source_{index}.csv",
                path=source_path,
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(source_file.id)

        # AND a compute task that transforms that folder into the destination RecordSet
        compute_task = await CurationTask(
            data_type=f"record_set_generation_{str(uuid.uuid4()).replace('-', '_')}",
            project_id=project_model.id,
            instructions="Transform the source files into the destination RecordSet.",
            task_properties=RecordSetGenerationExecutionProperties(
                folder_id=source_folder.id,
                instructions=(
                    "Each file describes one specimen. Produce one row per specimen "
                    "with its specimen_id, assay, and read_length."
                ),
                destination_task_id=destination_task.task_id,
            ),
        ).store_async(synapse_client=syn)

        # AND the round trip preserved the compute properties
        assert isinstance(
            compute_task.task_properties, RecordSetGenerationExecutionProperties
        )
        assert compute_task.task_properties.folder_id == source_folder.id
        assert (
            compute_task.task_properties.destination_task_id == destination_task.task_id
        )

        # AND the task is made executable, since a new task has no execution details
        # and Synapse will not dispatch one without them
        status_with_details = await compute_task.set_execution_details_async(
            execution_details=RecordSetGenerationExecutionDetails(),
            synapse_client=syn,
        )
        assert isinstance(
            status_with_details.execution_details, RecordSetGenerationExecutionDetails
        )
        # AND attaching the details did not transition the task out of NOT_STARTED
        assert status_with_details.state == TaskState.NOT_STARTED

        # WHEN I execute the task
        details = await compute_task.execute_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=syn
        )

        # THEN the job reports back details of the matching kind
        assert isinstance(details, RecordSetGenerationExecutionDetails)
        assert details.started_by is not None
        assert details.started_on is not None
        assert details.error_message is None, (
            "The record set generation job failed: "
            f"{details.error_message} {details.error_details}"
        )

        # AND the task's status carries the same kind of details, with the results
        # left pending human review
        final_status = await compute_task.get_status_async(synapse_client=syn)
        assert isinstance(
            final_status.execution_details, RecordSetGenerationExecutionDetails
        )
        assert final_status.execution_details.started_on == details.started_on
        assert final_status.state == TaskState.IN_REVIEW

    async def test_execute_sample_sheet_generation_async(
        self,
        syn: Synapse,
        project_model: Project,
        destination_task: CurationTask,
        folder_with_view: tuple[Folder, EntityView],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a folder whose files carry the annotations the sample sheet is built
        # from, and an EntityView over that folder
        folder, entity_view = folder_with_view

        for index, specimen in enumerate(["SPEC_001", "SPEC_002"]):
            annotated_file = await File(
                parent_id=folder.id,
                name=f"annotated_{index}.txt",
                path=make_bogus_uuid_file(),
                annotations={
                    "specimen_id": [specimen],
                    "assay": ["rna_seq"],
                    "read_length": [100 + index],
                },
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(annotated_file.id)

        # AND the view has indexed those files, since the computation reads the
        # annotations through the view rather than from the file contents
        async def view_has_indexed_the_files() -> bool:
            rows = await query_async(
                query=f"SELECT * FROM {entity_view.id}", synapse_client=syn
            )
            return len(rows) >= 2

        await wait_for_condition(
            view_has_indexed_the_files,
            timeout_seconds=QUERY_TIMEOUT_SEC,
            description="entity view to index the annotated files",
        )

        # AND a file-based curation task over that folder to act as the input
        input_task = await CurationTask(
            data_type=f"sample_sheet_input_{str(uuid.uuid4()).replace('-', '_')}",
            project_id=project_model.id,
            instructions="Provides the annotated files for the sample sheet.",
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=folder.id,
                file_view_id=entity_view.id,
            ),
        ).store_async(synapse_client=syn)

        # AND a compute task that builds a sample sheet from the input task
        compute_task = await CurationTask(
            data_type=f"sample_sheet_generation_{str(uuid.uuid4()).replace('-', '_')}",
            project_id=project_model.id,
            instructions="Generate the sample sheet from the annotated files.",
            task_properties=SampleSheetGenerationExecutionProperties(
                input_task_id=input_task.task_id,
                destination_task_id=destination_task.task_id,
            ),
        ).store_async(synapse_client=syn)

        # AND the round trip preserved the compute properties
        assert isinstance(
            compute_task.task_properties, SampleSheetGenerationExecutionProperties
        )
        assert compute_task.task_properties.input_task_id == input_task.task_id
        assert (
            compute_task.task_properties.destination_task_id == destination_task.task_id
        )

        # AND the task is made executable, since a new task has no execution details
        # and Synapse will not dispatch one without them
        status_with_details = await compute_task.set_execution_details_async(
            execution_details=SampleSheetGenerationExecutionDetails(),
            synapse_client=syn,
        )
        assert isinstance(
            status_with_details.execution_details,
            SampleSheetGenerationExecutionDetails,
        )
        # AND attaching the details did not transition the task out of NOT_STARTED
        assert status_with_details.state == TaskState.NOT_STARTED

        # WHEN I execute the task
        details = await compute_task.execute_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=syn
        )

        # THEN the job reports back details of the matching kind
        assert isinstance(details, SampleSheetGenerationExecutionDetails)
        assert details.started_by is not None
        assert details.started_on is not None
        assert details.error_message is None, (
            "The sample sheet generation job failed: "
            f"{details.error_message} {details.error_details}"
        )

        # AND the task's status carries the same kind of details, with the results
        # left pending human review
        final_status = await compute_task.get_status_async(synapse_client=syn)
        assert isinstance(
            final_status.execution_details, SampleSheetGenerationExecutionDetails
        )
        assert final_status.execution_details.started_on == details.started_on
        assert final_status.state == TaskState.IN_REVIEW

    async def test_execute_validation_error_async(self, syn: Synapse) -> None:
        # GIVEN a CurationTask without a task_id
        # WHEN I try to execute it
        # THEN it should raise a ValueError before any request is sent
        with pytest.raises(
            ValueError, match="task_id is required to execute a CurationTask"
        ):
            await CurationTask().execute_async(synapse_client=syn)
