"""Integration tests for Activity."""

import uuid
from typing import Callable

import pytest

import synapseclient.core.utils as utils
from synapseclient import Project as Synapse_Project
from synapseclient import Synapse
from synapseclient.models import Activity, File, UsedEntity, UsedURL

BOGUS_URL = "https://www.synapse.org/"


class TestActivity:
    """Integration tests for Activity."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def create_file_with_activity(
        self,
        project: Synapse_Project,
        activity: Activity = None,
        store_file: bool = True,
    ) -> File:
        """Helper to create a file with optional activity"""
        path = utils.make_bogus_uuid_file()
        file = File(
            parent_id=project["id"],
            path=path,
            name=f"bogus_file_{str(uuid.uuid4())}",
            activity=activity,
        )
        self.schedule_for_cleanup(file.path)

        if store_file:
            file.store()
            self.schedule_for_cleanup(file.id)

        return file

    async def verify_activity_properties(
        self, activity, expected_name, expected_description, has_references=False
    ):
        """Helper to verify common activity properties"""
        assert activity.name == expected_name
        assert activity.description == expected_description
        assert activity.id is not None
        assert activity.etag is not None
        assert activity.created_on is not None
        assert activity.modified_on is not None
        assert activity.created_by is not None
        assert activity.modified_by is not None

        if has_references:
            assert activity.used[0].url == BOGUS_URL
            assert activity.used[0].name == "example"
            assert activity.used[1].target_id == "syn456"
            assert activity.used[1].target_version_number == 1
            assert activity.executed[0].url == BOGUS_URL
            assert activity.executed[0].name == "example"
            assert activity.executed[1].target_id == "syn789"
            assert activity.executed[1].target_version_number == 1
        else:
            assert activity.used == []
            assert activity.executed == []

    async def test_activity_lifecycle(self, project: Synapse_Project) -> None:
        """Test complete activity lifecycle - create, update, retrieve, and delete"""
        # GIVEN a file in a project
        file = await self.create_file_with_activity(project)

        # AND an activity with references
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url=BOGUS_URL),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url=BOGUS_URL),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )

        # WHEN I store the activity
        result = activity.store(parent=file)
        self.schedule_for_cleanup(result.id)

        # THEN I expect the activity to be stored correctly
        assert result == activity
        self.verify_activity_properties(
            result, "some_name", "some_description", has_references=True
        )

        # WHEN I modify and store the activity
        result.name = "modified_name"
        result.description = "modified_description"
        modified_result = result.store()

        # THEN I expect the modified activity to be stored
        self.verify_activity_properties(
            modified_result,
            "modified_name",
            "modified_description",
            has_references=True,
        )

        # WHEN I get the activity from the file
        retrieved_activity = Activity.from_parent(parent=file)

        # THEN I expect the retrieved activity to match the modified one
        assert retrieved_activity.name == "modified_name"
        assert retrieved_activity.description == "modified_description"
        self.verify_activity_properties(
            retrieved_activity,
            "modified_name",
            "modified_description",
            has_references=True,
        )

        # WHEN I delete the activity
        result.delete(parent=file)

        # THEN I expect no activity to be associated with the file
        activity_after_delete = Activity.from_parent(parent=file)
        assert activity_after_delete is None

    async def test_store_activity_with_no_references(
        self, project: Synapse_Project
    ) -> None:
        """Test storing an activity without references"""
        # GIVEN an activity with no references
        activity = Activity(
            name="simple_activity",
            description="activity with no references",
        )

        # AND a file with that activity
        file = await self.create_file_with_activity(project, activity=activity)

        # THEN I expect the activity to have been stored properly
        self.verify_activity_properties(
            file.activity,
            "simple_activity",
            "activity with no references",
            has_references=False,
        )

        # Clean up
        file.activity.delete(parent=file)

    async def test_store_activity_via_file_creation(
        self, project: Synapse_Project
    ) -> None:
        """Test storing an activity as part of file creation"""
        # GIVEN an activity with references
        activity = Activity(
            name="file_activity",
            description="activity stored with file",
            used=[
                UsedURL(name="example", url=BOGUS_URL),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url=BOGUS_URL),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )

        # WHEN I create a file with the activity
        file = await self.create_file_with_activity(project, activity=activity)

        # THEN I expect the activity to have been stored with the file
        self.verify_activity_properties(
            file.activity,
            "file_activity",
            "activity stored with file",
            has_references=True,
        )

        # Clean up
        file.activity.delete(parent=file)
