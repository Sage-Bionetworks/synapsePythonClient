"""Unit tests for Activity."""

from unittest.mock import patch
import pytest
from synapseclient.models import Activity, UsedURL, UsedEntity, File
from synapseclient.activity import Activity as Synapse_Activity
from synapseclient.core.constants.concrete_types import USED_ENTITY, USED_URL

ACTIVITY_NAME = "some_name"
DESCRIPTION = "some_description"
BOGUS_URL = "https://www.synapse.org/"
CREATED_ON = "2022-01-01T00:00:00Z"
MODIFIED_ON = "2022-01-02T00:00:00Z"
CREATED_BY = "user1"
MODIFIED_BY = "user2"
ETAG = "some_etag"
SYN_123 = "syn123"
SYN_456 = "syn456"
SYN_789 = "syn789"
EXAMPLE_NAME = "example"


class TestActivity:
    """Unit tests for Activity."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def get_example_synapse_activity_input(self) -> Synapse_Activity:
        return Synapse_Activity(
            name=ACTIVITY_NAME,
            description=DESCRIPTION,
            used=[
                {
                    "wasExecuted": False,
                    "concreteType": USED_URL,
                    "url": BOGUS_URL,
                    "name": EXAMPLE_NAME,
                },
                {
                    "wasExecuted": False,
                    "concreteType": USED_ENTITY,
                    "reference": {
                        "targetId": SYN_456,
                        "targetVersionNumber": 1,
                    },
                },
            ],
            executed=[
                {
                    "wasExecuted": True,
                    "concreteType": USED_URL,
                    "url": BOGUS_URL,
                    "name": EXAMPLE_NAME,
                },
                {
                    "wasExecuted": True,
                    "concreteType": USED_ENTITY,
                    "reference": {
                        "targetId": SYN_789,
                        "targetVersionNumber": 1,
                    },
                },
            ],
        )

    def get_example_synapse_activity_output(self) -> Synapse_Activity:
        synapse_activity = Synapse_Activity(
            name=ACTIVITY_NAME,
            description=DESCRIPTION,
            used=[
                {
                    "wasExecuted": False,
                    "concreteType": USED_URL,
                    "url": BOGUS_URL,
                    "name": EXAMPLE_NAME,
                },
                {
                    "wasExecuted": False,
                    "concreteType": USED_ENTITY,
                    "reference": {
                        "targetId": SYN_456,
                        "targetVersionNumber": 1,
                    },
                },
            ],
            executed=[
                {
                    "wasExecuted": True,
                    "concreteType": USED_URL,
                    "url": BOGUS_URL,
                    "name": EXAMPLE_NAME,
                },
                {
                    "wasExecuted": True,
                    "concreteType": USED_ENTITY,
                    "reference": {
                        "targetId": SYN_789,
                        "targetVersionNumber": 1,
                    },
                },
            ],
        )
        synapse_activity["id"] = SYN_123
        synapse_activity["etag"] = ETAG
        synapse_activity["createdOn"] = CREATED_ON
        synapse_activity["modifiedOn"] = MODIFIED_ON
        synapse_activity["createdBy"] = CREATED_BY
        synapse_activity["modifiedBy"] = MODIFIED_BY
        return synapse_activity

    def test_fill_from_dict(self) -> None:
        # GIVEN a blank activity
        activity = Activity()

        # WHEN we fill it from a dictionary with all fields
        activity.fill_from_dict(
            synapse_activity=self.get_example_synapse_activity_output()
        )

        # THEN the activity should have all fields filled
        assert activity.id == SYN_123
        assert activity.etag == ETAG
        assert activity.name == ACTIVITY_NAME
        assert activity.description == DESCRIPTION
        assert activity.created_on == CREATED_ON
        assert activity.modified_on == MODIFIED_ON
        assert activity.created_by == CREATED_BY
        assert activity.modified_by == MODIFIED_BY
        assert len(activity.used) == 2
        assert isinstance(activity.used[0], UsedURL)
        assert activity.used[0].url == BOGUS_URL
        assert activity.used[0].name == EXAMPLE_NAME
        assert isinstance(activity.used[1], UsedEntity)
        assert activity.used[1].target_id == SYN_456
        assert activity.used[1].target_version_number == 1
        assert len(activity.executed) == 2
        assert isinstance(activity.executed[0], UsedURL)
        assert activity.executed[0].url == BOGUS_URL
        assert activity.executed[0].name == EXAMPLE_NAME
        assert isinstance(activity.executed[1], UsedEntity)
        assert activity.executed[1].target_id == SYN_789
        assert activity.executed[1].target_version_number == 1

    @pytest.mark.asyncio
    async def test_store_with_id(self) -> None:
        # GIVEN an activity with an id
        activity = Activity(
            id=SYN_123,
            name=ACTIVITY_NAME,
            description=DESCRIPTION,
            used=[
                UsedURL(name=EXAMPLE_NAME, url=BOGUS_URL),
                UsedEntity(target_id=SYN_456, target_version_number=1),
            ],
            executed=[
                UsedURL(name=EXAMPLE_NAME, url=BOGUS_URL),
                UsedEntity(target_id=SYN_789, target_version_number=1),
            ],
        )

        # WHEN we store it
        with patch.object(
            self.syn,
            "updateActivity",
            return_value=(self.get_example_synapse_activity_output()),
        ) as path_update_activity:
            result_of_store = await activity.store_async()

            # THEN we should call the method with this data
            sample_input = self.get_example_synapse_activity_input()
            sample_input["id"] = SYN_123
            sample_input["etag"] = None
            path_update_activity.assert_called_once_with(
                activity=sample_input,
            )

            # AND we should get back the stored activity
            assert result_of_store.id == SYN_123
            assert result_of_store.etag == ETAG
            assert result_of_store.name == ACTIVITY_NAME
            assert result_of_store.description == DESCRIPTION
            assert result_of_store.created_on == CREATED_ON
            assert result_of_store.modified_on == MODIFIED_ON
            assert result_of_store.created_by == CREATED_BY
            assert result_of_store.modified_by == MODIFIED_BY
            assert len(result_of_store.used) == 2
            assert isinstance(result_of_store.used[0], UsedURL)
            assert result_of_store.used[0].url == BOGUS_URL
            assert result_of_store.used[0].name == EXAMPLE_NAME
            assert isinstance(result_of_store.used[1], UsedEntity)
            assert result_of_store.used[1].target_id == SYN_456
            assert result_of_store.used[1].target_version_number == 1
            assert len(result_of_store.executed) == 2
            assert isinstance(result_of_store.executed[0], UsedURL)
            assert result_of_store.executed[0].url == BOGUS_URL
            assert result_of_store.executed[0].name == EXAMPLE_NAME
            assert isinstance(result_of_store.executed[1], UsedEntity)
            assert result_of_store.executed[1].target_id == SYN_789
            assert result_of_store.executed[1].target_version_number == 1

    @pytest.mark.asyncio
    async def test_store_with_parent(self) -> None:
        # GIVEN an activity with a parent
        activity = Activity(
            name=ACTIVITY_NAME,
            description=DESCRIPTION,
            used=[
                UsedURL(name=EXAMPLE_NAME, url=BOGUS_URL),
                UsedEntity(target_id=SYN_456, target_version_number=1),
            ],
            executed=[
                UsedURL(name=EXAMPLE_NAME, url=BOGUS_URL),
                UsedEntity(target_id=SYN_789, target_version_number=1),
            ],
        )

        # WHEN we store it
        with patch.object(
            self.syn,
            "setProvenance",
            return_value=(self.get_example_synapse_activity_output()),
        ) as path_set_provenance:
            result_of_store = await activity.store_async(parent=File("syn999"))

            # THEN we should call the method with this data
            sample_input = self.get_example_synapse_activity_input()
            path_set_provenance.assert_called_once_with(
                entity="syn999",
                activity=sample_input,
            )

            # AND we should get back the stored activity
            assert result_of_store.id == SYN_123
            assert result_of_store.etag == ETAG
            assert result_of_store.name == ACTIVITY_NAME
            assert result_of_store.description == DESCRIPTION
            assert result_of_store.created_on == CREATED_ON
            assert result_of_store.modified_on == MODIFIED_ON
            assert result_of_store.created_by == CREATED_BY
            assert result_of_store.modified_by == MODIFIED_BY
            assert len(result_of_store.used) == 2
            assert isinstance(result_of_store.used[0], UsedURL)
            assert result_of_store.used[0].url == BOGUS_URL
            assert result_of_store.used[0].name == EXAMPLE_NAME
            assert isinstance(result_of_store.used[1], UsedEntity)
            assert result_of_store.used[1].target_id == SYN_456
            assert result_of_store.used[1].target_version_number == 1
            assert len(result_of_store.executed) == 2
            assert isinstance(result_of_store.executed[0], UsedURL)
            assert result_of_store.executed[0].url == BOGUS_URL
            assert result_of_store.executed[0].name == EXAMPLE_NAME
            assert isinstance(result_of_store.executed[1], UsedEntity)
            assert result_of_store.executed[1].target_id == SYN_789
            assert result_of_store.executed[1].target_version_number == 1

    @pytest.mark.asyncio
    async def test_from_parent(self) -> None:
        # GIVEN a parent with an activity
        parent = File("syn999", version_number=1)

        # WHEN I get the activity
        with patch.object(
            self.syn,
            "getProvenance",
            return_value=(self.get_example_synapse_activity_output()),
        ) as path_get_provenance:
            result_of_get = await Activity.from_parent_async(parent=parent)

            # THEN we should call the method with this data
            path_get_provenance.assert_called_once_with(
                entity="syn999",
                version=1,
            )

            # AND we should get back the stored activity
            assert result_of_get.id == SYN_123
            assert result_of_get.etag == ETAG
            assert result_of_get.name == ACTIVITY_NAME
            assert result_of_get.description == DESCRIPTION
            assert result_of_get.created_on == CREATED_ON
            assert result_of_get.modified_on == MODIFIED_ON
            assert result_of_get.created_by == CREATED_BY
            assert result_of_get.modified_by == MODIFIED_BY
            assert len(result_of_get.used) == 2
            assert isinstance(result_of_get.used[0], UsedURL)
            assert result_of_get.used[0].url == BOGUS_URL
            assert result_of_get.used[0].name == EXAMPLE_NAME
            assert isinstance(result_of_get.used[1], UsedEntity)
            assert result_of_get.used[1].target_id == SYN_456
            assert result_of_get.used[1].target_version_number == 1
            assert len(result_of_get.executed) == 2
            assert isinstance(result_of_get.executed[0], UsedURL)
            assert result_of_get.executed[0].url == BOGUS_URL
            assert result_of_get.executed[0].name == EXAMPLE_NAME
            assert isinstance(result_of_get.executed[1], UsedEntity)
            assert result_of_get.executed[1].target_id == SYN_789
            assert result_of_get.executed[1].target_version_number == 1

    @pytest.mark.asyncio
    async def test_delete(self) -> None:
        # GIVEN a parent with an activity
        parent = File(id="syn999")

        # WHEN I delete the activity
        with patch.object(
            self.syn,
            attribute="deleteProvenance",
            return_value=None,
        ) as path_delete_provenance:
            await Activity.delete_async(parent=parent)

            # THEN we should call the method with this data
            path_delete_provenance.assert_called_once_with(
                entity="syn999",
            )
