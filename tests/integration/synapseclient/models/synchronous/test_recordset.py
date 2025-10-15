"""Integration tests for the synapseclient.models.RecordSet class."""

import os
import tempfile
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Activity,
    Folder,
    Project,
    RecordSet,
    UsedEntity,
    UsedURL,
)


class TestRecordSetStore:
    """Tests for the RecordSet.store method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def record_set_fixture(
        self, schedule_for_cleanup: Callable[..., None]
    ) -> RecordSet:
        """Create a RecordSet fixture for testing."""
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return RecordSet(
            path=filename,
            description="This is a test RecordSet.",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
            upsert_keys=["id", "name"],
        )

    async def test_store_in_project(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet
        record_set_fixture.name = str(uuid.uuid4())

        # WHEN I store the RecordSet in a project
        stored_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet should be stored successfully
        assert stored_record_set.id is not None
        assert stored_record_set.name == record_set_fixture.name
        assert stored_record_set.description == "This is a test RecordSet."
        assert stored_record_set.version_comment == "My version comment"
        assert stored_record_set.upsert_keys == ["id", "name"]
        assert stored_record_set.parent_id == project_model.id
        assert stored_record_set.etag is not None
        assert stored_record_set.created_on is not None
        assert stored_record_set.created_by is not None

    async def test_store_in_folder(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a folder within a project
        folder = Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # AND a RecordSet
        record_set_fixture.name = str(uuid.uuid4())

        # WHEN I store the RecordSet in the folder
        stored_record_set = record_set_fixture.store(
            parent=folder, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet should be stored successfully in the folder
        assert stored_record_set.id is not None
        assert stored_record_set.name == record_set_fixture.name
        assert stored_record_set.parent_id == folder.id
        assert stored_record_set.etag is not None

    async def test_store_with_activity(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet with activity
        record_set_fixture.name = str(uuid.uuid4())
        activity = Activity(
            name="Test Activity",
            description="Test activity for RecordSet",
            used=[
                UsedURL(name="Example URL", url="https://example.com"),
                UsedEntity(target_id="syn123456"),
            ],
        )
        record_set_fixture.activity = activity

        # WHEN I store the RecordSet
        stored_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet and activity should be stored successfully
        assert stored_record_set.id is not None
        assert stored_record_set.activity is not None
        assert stored_record_set.activity.name == "Test Activity"
        assert stored_record_set.activity.description == "Test activity for RecordSet"
        assert len(stored_record_set.activity.used) == 2

    async def test_store_with_annotations(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet with annotations
        record_set_fixture.name = str(uuid.uuid4())
        record_set_fixture.annotations = {
            "test_annotation": ["test_value"],
            "numeric_annotation": [42],
            "boolean_annotation": [True],
        }

        # WHEN I store the RecordSet
        stored_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet should be stored with annotations
        assert stored_record_set.id is not None
        assert stored_record_set.annotations is not None
        assert "test_annotation" in stored_record_set.annotations
        assert "numeric_annotation" in stored_record_set.annotations
        assert "boolean_annotation" in stored_record_set.annotations
        assert stored_record_set.annotations["test_annotation"] == ["test_value"]
        assert stored_record_set.annotations["numeric_annotation"] == [42]
        assert stored_record_set.annotations["boolean_annotation"] == [True]

    async def test_store_update_existing_record_set(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN an existing RecordSet
        record_set_fixture.name = str(uuid.uuid4())
        original_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(original_record_set.id)

        # WHEN I update the RecordSet with new metadata
        original_record_set.description = "Updated description"
        original_record_set.version_comment = "Updated version comment"

        updated_record_set = original_record_set.store(synapse_client=self.syn)

        # THEN the RecordSet should be updated successfully
        assert updated_record_set.id == original_record_set.id
        assert updated_record_set.description == "Updated description"
        assert updated_record_set.version_comment == "Updated version comment"
        assert updated_record_set.version_number >= original_record_set.version_number

    async def test_store_validation_errors(self) -> None:
        # GIVEN a RecordSet without required fields
        record_set = RecordSet()

        # WHEN I try to store it without proper configuration
        # THEN it should raise a ValueError for missing required fields
        with pytest.raises(ValueError):
            record_set.store(synapse_client=self.syn)


class TestRecordSetGet:
    """Tests for the RecordSet.get method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def stored_record_set(self, project_model: Project) -> RecordSet:
        """Create and store a RecordSet for testing get operations."""
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            description="Test RecordSet for get operations",
            parent_id=project_model.id,
            upsert_keys=["id", "name"],
            version_comment="Initial version",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)
        return record_set

    async def test_get_record_set_by_id(self, stored_record_set: RecordSet) -> None:
        # GIVEN an existing RecordSet
        original_id = stored_record_set.id

        # WHEN I get the RecordSet by ID
        retrieved_record_set = RecordSet(id=original_id).get(synapse_client=self.syn)

        # THEN the retrieved RecordSet should match the original
        assert retrieved_record_set.id == original_id
        assert retrieved_record_set.name == stored_record_set.name
        assert retrieved_record_set.description == stored_record_set.description
        assert retrieved_record_set.parent_id == stored_record_set.parent_id
        assert retrieved_record_set.upsert_keys == stored_record_set.upsert_keys
        assert retrieved_record_set.version_comment == stored_record_set.version_comment
        assert retrieved_record_set.etag == stored_record_set.etag
        assert retrieved_record_set.version_number == stored_record_set.version_number

    async def test_get_record_set_with_activity(self, project_model: Project) -> None:
        # GIVEN a RecordSet with activity
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        activity = Activity(
            name="Test Activity",
            description="Test activity for RecordSet",
            used=[UsedURL(name="Test URL", url="https://example.com")],
        )

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            parent_id=project_model.id,
            upsert_keys=["id"],
            activity=activity,
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)

        # WHEN I get the RecordSet with activity
        retrieved_record_set = RecordSet(id=record_set.id).get(
            include_activity=True, synapse_client=self.syn
        )

        # THEN the RecordSet should include the activity
        assert retrieved_record_set.activity is not None
        assert retrieved_record_set.activity.name == "Test Activity"
        assert (
            retrieved_record_set.activity.description == "Test activity for RecordSet"
        )
        assert len(retrieved_record_set.activity.used) == 1
        assert retrieved_record_set.path is not None

    async def test_get_validation_error(self) -> None:
        # GIVEN a RecordSet without an ID
        record_set = RecordSet()

        # WHEN I try to get it
        # THEN it should raise a ValueError
        with pytest.raises(ValueError):
            record_set.get(synapse_client=self.syn)

    async def test_get_non_existent_record_set(self) -> None:
        # GIVEN a non-existent RecordSet ID
        record_set = RecordSet(id="syn999999999")

        # WHEN I try to get it
        # THEN it should raise a SynapseHTTPError
        with pytest.raises(SynapseHTTPError):
            record_set.get(synapse_client=self.syn)


class TestRecordSetDelete:
    """Tests for the RecordSet.delete method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_delete_entire_record_set(self, project_model: Project) -> None:
        # GIVEN an existing RecordSet
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            description="RecordSet to be deleted",
            parent_id=project_model.id,
            upsert_keys=["id"],
        ).store(synapse_client=self.syn)

        record_set_id = record_set.id

        # WHEN I delete the entire RecordSet
        record_set.delete(synapse_client=self.syn)

        # THEN the RecordSet should be deleted and no longer accessible
        with pytest.raises(SynapseHTTPError):
            RecordSet(id=record_set_id).get(synapse_client=self.syn)

    async def test_delete_specific_version(self, project_model: Project) -> None:
        # GIVEN an existing RecordSet with multiple versions
        filename1 = utils.make_bogus_uuid_file()
        filename2 = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename1)
        self.schedule_for_cleanup(filename2)

        # Create initial version
        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename1,
            description="RecordSet version 1",
            parent_id=project_model.id,
            upsert_keys=["id"],
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)

        # Create second version
        record_set.path = filename2
        record_set.description = "RecordSet version 2"
        v2_record_set = record_set.store(synapse_client=self.syn)

        # WHEN I delete only version 2
        v2_record_set.delete(version_only=True, synapse_client=self.syn)

        # THEN the RecordSet should still exist but version 2 should be gone
        current_record_set = RecordSet(id=record_set.id).get(synapse_client=self.syn)
        assert current_record_set.id == record_set.id
        assert current_record_set.version_number == 1  # Should be back to version 1
        assert current_record_set.description == "RecordSet version 1"

    async def test_delete_validation_errors(self) -> None:
        # GIVEN a RecordSet without an ID
        record_set = RecordSet()

        # WHEN I try to delete it
        # THEN it should raise a ValueError
        with pytest.raises(ValueError):
            record_set.delete(synapse_client=self.syn)

        # AND WHEN I try to delete a specific version without version number
        record_set_with_id = RecordSet(id="syn123456")
        with pytest.raises(ValueError):
            record_set_with_id.delete(version_only=True, synapse_client=self.syn)
