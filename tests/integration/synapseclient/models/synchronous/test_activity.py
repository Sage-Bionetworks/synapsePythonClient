"""Integration tests for Activity."""

import asyncio
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
            # Verify used references
            assert len(activity.used) > 0
            assert activity.used[0].url == BOGUS_URL
            assert activity.used[0].name == "example"
            if len(activity.used) > 1:
                assert activity.used[1].target_id == "syn456"
                assert activity.used[1].target_version_number == 1

            # Verify executed references if they exist
            if len(activity.executed) > 0:
                assert activity.executed[0].url == BOGUS_URL
                assert activity.executed[0].name == "example"
                if len(activity.executed) > 1:
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
            name=f"some_name_{str(uuid.uuid4())}",
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
        await self.verify_activity_properties(
            result, activity.name, "some_description", has_references=True
        )

        # WHEN I modify and store the activity
        modified_name = f"modified_name_{str(uuid.uuid4())}"
        result.name = modified_name
        result.description = "modified_description"
        modified_result = result.store()

        # THEN I expect the modified activity to be stored
        await self.verify_activity_properties(
            modified_result,
            modified_name,
            "modified_description",
            has_references=True,
        )

        # WHEN I get the activity from the file
        retrieved_activity = Activity.from_parent(parent=file)

        # THEN I expect the retrieved activity to match the modified one
        assert retrieved_activity.name == modified_name
        assert retrieved_activity.description == "modified_description"
        await self.verify_activity_properties(
            retrieved_activity,
            modified_name,
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
            name=f"simple_activity_{str(uuid.uuid4())}",
            description="activity with no references",
        )

        # AND a file with that activity
        file = await self.create_file_with_activity(project, activity=activity)

        # THEN I expect the activity to have been stored properly
        await self.verify_activity_properties(
            file.activity,
            activity.name,
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
            name=f"file_activity_{str(uuid.uuid4())}",
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
        await self.verify_activity_properties(
            file.activity,
            activity.name,
            "activity stored with file",
            has_references=True,
        )

        # Clean up
        file.activity.delete(parent=file)

    async def test_get_by_activity_id(self, project: Synapse_Project) -> None:
        """Test retrieving an activity by its ID"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"test_get_by_id_{str(uuid.uuid4())}",
            description="activity for get by id test",
            used=[
                UsedURL(name="example", url=BOGUS_URL),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity

        # WHEN I retrieve the activity by its ID
        retrieved_activity = Activity.get(activity_id=stored_activity.id)

        # THEN I expect to get the same activity
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name
        assert retrieved_activity.description == "activity for get by id test"
        await self.verify_activity_properties(
            retrieved_activity,
            activity.name,
            "activity for get by id test",
            has_references=True,
        )

        # Clean up
        stored_activity.delete(parent=file)

    async def test_get_by_parent_id(self, project: Synapse_Project) -> None:
        """Test retrieving an activity by parent entity ID"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"test_get_by_parent_{str(uuid.uuid4())}",
            description="activity for get by parent test",
            used=[UsedURL(name="example", url=BOGUS_URL)],
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity
        await asyncio.sleep(2)

        # WHEN I retrieve the activity by parent ID
        retrieved_activity = Activity.get(parent_id=file.id)

        # THEN I expect to get the same activity
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name
        assert retrieved_activity.description == "activity for get by parent test"
        await self.verify_activity_properties(
            retrieved_activity,
            activity.name,
            "activity for get by parent test",
            has_references=True,
        )

        # Clean up
        stored_activity.delete(parent=file)

    async def test_get_by_parent_id_with_version(
        self, project: Synapse_Project
    ) -> None:
        """Test retrieving an activity by parent entity ID with version number"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"test_get_by_parent_version_{str(uuid.uuid4())}",
            description="activity for get by parent version test",
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity
        await asyncio.sleep(2)

        # WHEN I retrieve the activity by parent ID with version
        retrieved_activity = Activity.get(
            parent_id=file.id, parent_version_number=file.version_number
        )

        # THEN I expect to get the same activity
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name
        assert (
            retrieved_activity.description == "activity for get by parent version test"
        )

        # Clean up
        stored_activity.delete(parent=file)

    async def test_get_nonexistent_activity(self) -> None:
        """Test retrieving a nonexistent activity returns None"""
        # WHEN I try to retrieve a nonexistent activity by ID
        retrieved_activity = Activity.get(activity_id="syn999999999")

        # THEN I expect to get None
        assert retrieved_activity is None

        # AND when I try to retrieve by nonexistent parent ID
        retrieved_activity = Activity.get(parent_id="syn999999999")

        # THEN I expect to get None
        assert retrieved_activity is None

    async def test_get_activity_id_takes_precedence(
        self, project: Synapse_Project
    ) -> None:
        """Test that activity_id takes precedence over parent_id when both are provided"""
        # GIVEN two files with different activities
        activity1 = Activity(
            name=f"activity_1_{str(uuid.uuid4())}",
            description="first activity",
        )
        activity2 = Activity(
            name=f"activity_2_{str(uuid.uuid4())}",
            description="second activity",
        )

        file1 = await self.create_file_with_activity(project, activity=activity1)
        file2 = await self.create_file_with_activity(project, activity=activity2)

        stored_activity1 = file1.activity
        stored_activity2 = file2.activity
        await asyncio.sleep(2)

        # WHEN I retrieve using activity_id from first activity and parent_id from second
        retrieved_activity = Activity.get(
            activity_id=stored_activity1.id, parent_id=file2.id
        )

        # THEN I expect to get the first activity (activity_id takes precedence)
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity1.id
        assert retrieved_activity.name == activity1.name
        assert retrieved_activity.description == "first activity"

        # Clean up
        stored_activity1.delete(parent=file1)
        stored_activity2.delete(parent=file2)

    async def test_get_no_parameters_raises_error(self) -> None:
        """Test that calling get() without parameters raises ValueError"""
        # WHEN I try to call get() without any parameters
        # THEN I expect a ValueError to be raised
        with pytest.raises(
            ValueError, match="Either activity_id or parent_id must be provided"
        ):
            Activity.get()

    async def test_store_activity_with_string_parent(
        self, project: Synapse_Project
    ) -> None:
        """Test storing an activity with a string parent ID"""
        # GIVEN a file in a project
        file = await self.create_file_with_activity(project)

        # AND an activity with references
        activity = Activity(
            name=f"string_parent_test_{str(uuid.uuid4())}",
            description="testing string parent ID",
            used=[
                UsedURL(name="example", url=BOGUS_URL),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
        )

        # WHEN I store the activity using a string parent ID
        result = activity.store(parent=file.id)
        self.schedule_for_cleanup(result.id)

        # THEN I expect the activity to be stored correctly
        assert result == activity
        await self.verify_activity_properties(
            result, activity.name, "testing string parent ID", has_references=True
        )

        # AND when I retrieve it from the file
        retrieved_activity = Activity.from_parent(parent=file)
        assert retrieved_activity.id == result.id
        assert retrieved_activity.name == activity.name

        # Clean up
        Activity.delete(parent=file.id)

    async def test_from_parent_with_string_parent(
        self, project: Synapse_Project
    ) -> None:
        """Test retrieving an activity using a string parent ID"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"from_parent_string_test_{str(uuid.uuid4())}",
            description="testing from_parent with string",
            used=[UsedURL(name="example", url=BOGUS_URL)],
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity

        # WHEN I retrieve the activity using a string parent ID
        retrieved_activity = Activity.from_parent(parent=file.id)

        # THEN I expect to get the same activity
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name
        assert retrieved_activity.description == "testing from_parent with string"

        # Clean up
        Activity.delete(parent=file)

    async def test_from_parent_with_string_parent_and_version(
        self, project: Synapse_Project
    ) -> None:
        """Test retrieving an activity using a string parent ID with version"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"from_parent_string_version_test_{str(uuid.uuid4())}",
            description="testing from_parent with string and version",
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity

        # WHEN I retrieve the activity using a string parent ID with version parameter
        retrieved_activity = Activity.from_parent(
            parent=file.id, parent_version_number=file.version_number
        )

        # THEN I expect to get the same activity
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name

        # Clean up
        Activity.delete(parent=file)

    async def test_from_parent_with_string_parent_with_embedded_version(
        self, project: Synapse_Project
    ) -> None:
        """Test retrieving an activity using a string parent ID with embedded version"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"from_parent_embedded_version_test_{str(uuid.uuid4())}",
            description="testing from_parent with embedded version",
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity

        # WHEN I retrieve the activity using a string parent ID with embedded version
        parent_with_version = f"{file.id}.{file.version_number}"
        retrieved_activity = Activity.from_parent(parent=parent_with_version)

        # THEN I expect to get the same activity
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name

        # Clean up
        Activity.delete(parent=file)

    async def test_from_parent_version_precedence(
        self, project: Synapse_Project
    ) -> None:
        """Test that embedded version takes precedence over parent_version_number parameter"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"version_precedence_test_{str(uuid.uuid4())}",
            description="testing version precedence",
        )
        file = await self.create_file_with_activity(project, activity=activity)
        stored_activity = file.activity

        # WHEN I retrieve the activity using a string parent ID with embedded version
        # and also provide a different parent_version_number parameter
        parent_with_version = f"{file.id}.{file.version_number}"
        wrong_version = file.version_number + 1 if file.version_number > 1 else 999
        retrieved_activity = Activity.from_parent(
            parent=parent_with_version, parent_version_number=wrong_version
        )

        # THEN I expect to get the activity (embedded version should take precedence)
        assert retrieved_activity is not None
        assert retrieved_activity.id == stored_activity.id
        assert retrieved_activity.name == activity.name

        # Clean up
        Activity.delete(parent=file)

    async def test_delete_with_string_parent(self, project: Synapse_Project) -> None:
        """Test deleting an activity using a string parent ID"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"delete_string_test_{str(uuid.uuid4())}",
            description="testing delete with string parent",
        )
        file = await self.create_file_with_activity(project, activity=activity)

        # WHEN I delete the activity using a string parent ID
        Activity.delete(parent=file.id)

        # THEN I expect no activity to be associated with the file
        activity_after_delete = Activity.from_parent(parent=file)
        assert activity_after_delete is None

    async def test_disassociate_with_string_parent(
        self, project: Synapse_Project
    ) -> None:
        """Test disassociating an activity using a string parent ID"""
        # GIVEN a file with an activity
        activity = Activity(
            name=f"disassociate_string_test_{str(uuid.uuid4())}",
            description="testing disassociate with string parent",
        )
        file = await self.create_file_with_activity(project, activity=activity)

        # WHEN I disassociate the activity using a string parent ID
        Activity.disassociate_from_entity(parent=file.id)

        # THEN I expect no activity to be associated with the file
        activity_after_disassociate = Activity.from_parent(parent=file)
        assert activity_after_disassociate is None
