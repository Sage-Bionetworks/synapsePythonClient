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

    async def test_store_with_parent_and_id(self, project: Synapse_Project) -> None:
        # GIVEN a file in a project
        path = utils.make_bogus_uuid_file()
        file = File(
            parent_id=project["id"], path=path, name=f"bogus_file_{str(uuid.uuid4())}"
        )
        self.schedule_for_cleanup(file.path)
        file.store()
        self.schedule_for_cleanup(file.id)

        # AND an activity I want to store
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

        # THEN I expect the activity to be stored
        assert result == activity
        assert result.id is not None
        assert result.etag is not None
        assert result.created_on is not None
        assert result.modified_on is not None
        assert result.created_by is not None
        assert result.modified_by is not None
        assert result.used[0].url == BOGUS_URL
        assert result.used[0].name == "example"
        assert result.used[1].target_id == "syn456"
        assert result.used[1].target_version_number == 1
        assert result.executed[0].url == BOGUS_URL
        assert result.executed[0].name == "example"
        assert result.executed[1].target_id == "syn789"
        assert result.executed[1].target_version_number == 1

        # GIVEN our already stored activity
        modified_activity = activity

        # WHEN I modify the activity
        modified_activity.name = "modified_name"
        modified_activity.description = "modified_description"

        # AND I store the modified activity without a parent
        modified_result = modified_activity.store()

        # THEN I expect the modified activity to be stored
        assert modified_result == modified_activity
        assert modified_result.id is not None
        assert modified_result.etag is not None
        assert modified_result.created_on is not None
        assert modified_result.modified_on is not None
        assert modified_result.created_by is not None
        assert modified_result.modified_by is not None
        assert modified_result.name == "modified_name"
        assert modified_result.description == "modified_description"
        assert modified_result.used[0].url == BOGUS_URL
        assert modified_result.used[0].name == "example"
        assert modified_result.used[1].target_id == "syn456"
        assert modified_result.used[1].target_version_number == 1
        assert modified_result.executed[0].url == BOGUS_URL
        assert modified_result.executed[0].name == "example"
        assert modified_result.executed[1].target_id == "syn789"
        assert modified_result.executed[1].target_version_number == 1

        # Clean up
        result.delete(parent=file)

    async def test_store_with_no_references(self, project: Synapse_Project) -> None:
        # GIVEN a file in a project that has an activity with no references
        activity = Activity(
            name="some_name",
            description="some_description",
        )
        path = utils.make_bogus_uuid_file()
        file = File(
            parent_id=project["id"],
            path=path,
            name=f"bogus_file_{str(uuid.uuid4())}",
            activity=activity,
        )
        self.schedule_for_cleanup(file.path)

        # WHEN I store the file with the activity
        file.store()
        self.schedule_for_cleanup(file.id)

        # THEN I expect the activity to have been stored
        assert file.activity.name == activity.name
        assert file.activity.description == activity.description
        assert file.activity.used == []
        assert file.activity.executed == []
        assert file.activity.id is not None
        assert file.activity.etag is not None
        assert file.activity.created_on is not None
        assert file.activity.modified_on is not None
        assert file.activity.created_by is not None
        assert file.activity.modified_by is not None

        # Clean up
        file.activity.delete(parent=file)

    async def test_from_parent(self, project: Synapse_Project) -> None:
        # GIVEN a file in a project that has an activity
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
        path = utils.make_bogus_uuid_file()
        file = File(
            parent_id=project["id"],
            path=path,
            name=f"bogus_file_{str(uuid.uuid4())}",
            activity=activity,
        )
        self.schedule_for_cleanup(file.path)
        file.store()
        self.schedule_for_cleanup(file.id)

        # WHEN I get the activity from the file
        result = Activity.from_parent(parent=file)

        # THEN I expect the activity to be returned
        assert result == activity
        assert result.name == "some_name"
        assert result.description == "some_description"
        assert result.id is not None
        assert result.etag is not None
        assert result.created_on is not None
        assert result.modified_on is not None
        assert result.created_by is not None
        assert result.modified_by is not None
        assert result.used[0].url == BOGUS_URL
        assert result.used[0].name == "example"
        assert result.used[1].target_id == "syn456"
        assert result.used[1].target_version_number == 1
        assert result.executed[0].url == BOGUS_URL
        assert result.executed[0].name == "example"
        assert result.executed[1].target_id == "syn789"
        assert result.executed[1].target_version_number == 1

        # Clean up
        result.delete(parent=file)

    async def test_delete(self, project: Synapse_Project) -> None:
        # GIVEN a file in a project that has an activity
        activity = Activity(
            name="some_name",
            description="some_description",
        )
        path = utils.make_bogus_uuid_file()
        file = File(
            parent_id=project["id"],
            path=path,
            name=f"bogus_file_{str(uuid.uuid4())}",
            activity=activity,
        )
        self.schedule_for_cleanup(file.path)

        # AND I store the file with the activity
        file.store()
        self.schedule_for_cleanup(file.id)

        # AND the activity exists
        assert file.activity.id is not None

        # WHEN I delete the activity
        file.activity.delete(parent=file)

        # THEN I expect to receieve None
        activity = Activity.from_parent(parent=file)
        assert activity is None
