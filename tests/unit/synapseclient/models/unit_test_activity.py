from unittest.mock import ANY, patch
import pytest
from synapseclient.models import Activity, UsedURL, UsedEntity, File
from synapseclient.activity import Activity as Synapse_Activity


class TestActivity:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def get_example_synapse_activity_input(self) -> Synapse_Activity:
        return Synapse_Activity(
            name="some_name",
            description="some_description",
            used=[
                {
                    "wasExecuted": False,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
                    "url": "http://www.example.com",
                    "name": "example",
                },
                {
                    "wasExecuted": False,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedEntity",
                    "reference": {
                        "targetId": "syn456",
                        "targetVersionNumber": 1,
                    },
                },
            ],
            executed=[
                {
                    "wasExecuted": True,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
                    "url": "http://www.example.com",
                    "name": "example",
                },
                {
                    "wasExecuted": True,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedEntity",
                    "reference": {
                        "targetId": "syn789",
                        "targetVersionNumber": 1,
                    },
                },
            ],
        )

    def get_example_synapse_activity_output(self) -> Synapse_Activity:
        synapse_activity = Synapse_Activity(
            name="some_name",
            description="some_description",
            used=[
                {
                    "wasExecuted": False,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
                    "url": "http://www.example.com",
                    "name": "example",
                },
                {
                    "wasExecuted": False,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedEntity",
                    "reference": {
                        "targetId": "syn456",
                        "targetVersionNumber": 1,
                    },
                },
            ],
            executed=[
                {
                    "wasExecuted": True,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
                    "url": "http://www.example.com",
                    "name": "example",
                },
                {
                    "wasExecuted": True,
                    "concreteType": "org.sagebionetworks.repo.model.provenance.UsedEntity",
                    "reference": {
                        "targetId": "syn789",
                        "targetVersionNumber": 1,
                    },
                },
            ],
        )
        synapse_activity["id"] = "syn123"
        synapse_activity["etag"] = "some_etag"
        synapse_activity["createdOn"] = "2022-01-01T00:00:00Z"
        synapse_activity["modifiedOn"] = "2022-01-02T00:00:00Z"
        synapse_activity["createdBy"] = "user1"
        synapse_activity["modifiedBy"] = "user2"
        return synapse_activity

    def test_fill_from_dict(self) -> None:
        # GIVEN a blank activity
        activity = Activity()

        # WHEN we fill it from a dictionary with all fields
        activity.fill_from_dict(
            synapse_activity=self.get_example_synapse_activity_output()
        )

        # THEN the activity should have all fields filled
        assert activity.id == "syn123"
        assert activity.etag == "some_etag"
        assert activity.name == "some_name"
        assert activity.description == "some_description"
        assert activity.created_on == "2022-01-01T00:00:00Z"
        assert activity.modified_on == "2022-01-02T00:00:00Z"
        assert activity.created_by == "user1"
        assert activity.modified_by == "user2"
        assert len(activity.used) == 2
        assert isinstance(activity.used[0], UsedURL)
        assert activity.used[0].url == "http://www.example.com"
        assert activity.used[0].name == "example"
        assert isinstance(activity.used[1], UsedEntity)
        assert activity.used[1].target_id == "syn456"
        assert activity.used[1].target_version_number == 1
        assert len(activity.executed) == 2
        assert isinstance(activity.executed[0], UsedURL)
        assert activity.executed[0].url == "http://www.example.com"
        assert activity.executed[0].name == "example"
        assert isinstance(activity.executed[1], UsedEntity)
        assert activity.executed[1].target_id == "syn789"
        assert activity.executed[1].target_version_number == 1

    @pytest.mark.asyncio
    async def test_store_with_id(self) -> None:
        # GIVEN an activity with an id
        activity = Activity(
            id="syn123",
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="http://www.example.com"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="http://www.example.com"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )

        # WHEN we store it
        with patch.object(
            self.syn,
            "updateActivity",
            return_value=(self.get_example_synapse_activity_output()),
        ) as path_update_activity:
            result_of_store = await activity.store()

            # THEN we should call the method with this data
            sample_input = self.get_example_synapse_activity_input()
            sample_input["id"] = "syn123"
            sample_input["etag"] = None
            path_update_activity.assert_called_once_with(
                activity=sample_input,
                opentelemetry_context=ANY,
            )

            # AND we should get back the stored activity
            assert result_of_store.id == "syn123"
            assert result_of_store.etag == "some_etag"
            assert result_of_store.name == "some_name"
            assert result_of_store.description == "some_description"
            assert result_of_store.created_on == "2022-01-01T00:00:00Z"
            assert result_of_store.modified_on == "2022-01-02T00:00:00Z"
            assert result_of_store.created_by == "user1"
            assert result_of_store.modified_by == "user2"
            assert len(result_of_store.used) == 2
            assert isinstance(result_of_store.used[0], UsedURL)
            assert result_of_store.used[0].url == "http://www.example.com"
            assert result_of_store.used[0].name == "example"
            assert isinstance(result_of_store.used[1], UsedEntity)
            assert result_of_store.used[1].target_id == "syn456"
            assert result_of_store.used[1].target_version_number == 1
            assert len(result_of_store.executed) == 2
            assert isinstance(result_of_store.executed[0], UsedURL)
            assert result_of_store.executed[0].url == "http://www.example.com"
            assert result_of_store.executed[0].name == "example"
            assert isinstance(result_of_store.executed[1], UsedEntity)
            assert result_of_store.executed[1].target_id == "syn789"
            assert result_of_store.executed[1].target_version_number == 1

    @pytest.mark.asyncio
    async def test_store_with_parent(self) -> None:
        # GIVEN an activity with a parent
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="http://www.example.com"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="http://www.example.com"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )

        # WHEN we store it
        with patch.object(
            self.syn,
            "setProvenance",
            return_value=(self.get_example_synapse_activity_output()),
        ) as path_set_provenance:
            result_of_store = await activity.store(parent=File("syn999"))

            # THEN we should call the method with this data
            sample_input = self.get_example_synapse_activity_input()
            path_set_provenance.assert_called_once_with(
                entity="syn999",
                activity=sample_input,
                opentelemetry_context=ANY,
            )

            # AND we should get back the stored activity
            assert result_of_store.id == "syn123"
            assert result_of_store.etag == "some_etag"
            assert result_of_store.name == "some_name"
            assert result_of_store.description == "some_description"
            assert result_of_store.created_on == "2022-01-01T00:00:00Z"
            assert result_of_store.modified_on == "2022-01-02T00:00:00Z"
            assert result_of_store.created_by == "user1"
            assert result_of_store.modified_by == "user2"
            assert len(result_of_store.used) == 2
            assert isinstance(result_of_store.used[0], UsedURL)
            assert result_of_store.used[0].url == "http://www.example.com"
            assert result_of_store.used[0].name == "example"
            assert isinstance(result_of_store.used[1], UsedEntity)
            assert result_of_store.used[1].target_id == "syn456"
            assert result_of_store.used[1].target_version_number == 1
            assert len(result_of_store.executed) == 2
            assert isinstance(result_of_store.executed[0], UsedURL)
            assert result_of_store.executed[0].url == "http://www.example.com"
            assert result_of_store.executed[0].name == "example"
            assert isinstance(result_of_store.executed[1], UsedEntity)
            assert result_of_store.executed[1].target_id == "syn789"
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
            result_of_get = await Activity.from_parent(parent=parent)

            # THEN we should call the method with this data
            path_get_provenance.assert_called_once_with(
                entity="syn999",
                version=1,
                opentelemetry_context=ANY,
            )

            # AND we should get back the stored activity
            assert result_of_get.id == "syn123"
            assert result_of_get.etag == "some_etag"
            assert result_of_get.name == "some_name"
            assert result_of_get.description == "some_description"
            assert result_of_get.created_on == "2022-01-01T00:00:00Z"
            assert result_of_get.modified_on == "2022-01-02T00:00:00Z"
            assert result_of_get.created_by == "user1"
            assert result_of_get.modified_by == "user2"
            assert len(result_of_get.used) == 2
            assert isinstance(result_of_get.used[0], UsedURL)
            assert result_of_get.used[0].url == "http://www.example.com"
            assert result_of_get.used[0].name == "example"
            assert isinstance(result_of_get.used[1], UsedEntity)
            assert result_of_get.used[1].target_id == "syn456"
            assert result_of_get.used[1].target_version_number == 1
            assert len(result_of_get.executed) == 2
            assert isinstance(result_of_get.executed[0], UsedURL)
            assert result_of_get.executed[0].url == "http://www.example.com"
            assert result_of_get.executed[0].name == "example"
            assert isinstance(result_of_get.executed[1], UsedEntity)
            assert result_of_get.executed[1].target_id == "syn789"
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
            await Activity.delete(parent=parent)

            # THEN we should call the method with this data
            path_delete_provenance.assert_called_once_with(
                entity="syn999",
                opentelemetry_context=ANY,
            )
