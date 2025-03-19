from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.models import Annotations, Dataset, EntityRef, File, Folder


class TestEntityRef:
    async def test_to_synapse_request(self):
        # GIVEN an EntityRef
        entity_ref = EntityRef(id="syn1234", version=1)
        # WHEN I convert it to a Synapse request
        result = entity_ref.to_synapse_request()
        # THEN I expect the result to be the expected Synapse request
        assert result == {
            "entityId": "syn1234",
            "versionNumber": 1,
        }


class TestDataset:
    synapse_response = {
        "id": "syn1234",
        "name": "test_dataset",
        "description": "test_description",
        "parentId": "syn1234",
        "etag": "etag_value",
        "createdOn": "createdOn_value",
        "createdBy": "createdBy_value",
        "modifiedOn": "modifiedOn_value",
        "modifiedBy": "modifiedBy_value",
        "versionNumber": 1,
        "versionLabel": "versionLabel_value",
        "versionComment": "versionComment_value",
        "isLatestVersion": True,
        "isSearchEnabled": True,
        "size": 100,
        "checksum": "checksum_value",
        "count": 100,
        "items": [
            {"entityId": "syn1234", "versionNumber": 1},
            {"entityId": "syn1235", "versionNumber": 1},
        ],
        "annotations": {"key": "value"},
    }

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I fill it from a Synapse response
        dataset.fill_from_dict(self.synapse_response, set_annotations=True)
        # THEN I expect the Dataset to be filled with the expected values
        assert dataset.id == self.synapse_response["id"]
        assert dataset.name == self.synapse_response["name"]
        assert dataset.description == self.synapse_response["description"]
        assert dataset.parent_id == self.synapse_response["parentId"]
        assert dataset.etag == self.synapse_response["etag"]
        assert dataset.created_on == self.synapse_response["createdOn"]
        assert dataset.created_by == self.synapse_response["createdBy"]
        assert dataset.modified_on == self.synapse_response["modifiedOn"]
        assert dataset.modified_by == self.synapse_response["modifiedBy"]
        assert dataset.version_number == self.synapse_response["versionNumber"]
        assert dataset.version_label == self.synapse_response["versionLabel"]
        assert dataset.version_comment == self.synapse_response["versionComment"]
        assert dataset.is_latest_version == self.synapse_response["isLatestVersion"]
        assert dataset.is_search_enabled == self.synapse_response["isSearchEnabled"]
        assert dataset.size == self.synapse_response["size"]
        assert dataset.checksum == self.synapse_response["checksum"]
        assert dataset.count == self.synapse_response["count"]
        assert dataset.items == [
            EntityRef(id=item["entityId"], version=item["versionNumber"])
            for item in self.synapse_response["items"]
        ]
        assert dataset.annotations == Annotations.from_dict(
            self.synapse_response["annotations"]
        )

    def test_to_synapse_request(self):
        # GIVEN a Dataset
        dataset = Dataset(
            id="syn1234",
            name="test_dataset",
            description="test_description",
            parent_id="syn1234",
            etag="etag_value",
            created_on="createdOn_value",
            created_by="createdBy_value",
            modified_on="modifiedOn_value",
            modified_by="modifiedBy_value",
            version_number=1,
            version_label="versionLabel_value",
            version_comment="versionComment_value",
            is_latest_version=True,
            is_search_enabled=True,
            size=100,
            checksum="checksum_value",
            count=100,
            items=[
                EntityRef(id="syn1234", version=1),
                EntityRef(id="syn1235", version=1),
            ],
        )
        request = dataset.to_synapse_request()
        for key, value in self.synapse_response.items():
            if key != "annotations":
                assert request["entity"][key] == value

    def test_append_entity_ref(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I append an EntityRef to it
        dataset._append_entity_ref(EntityRef(id="syn1234", version=1))
        # THEN I expect the Dataset to have the EntityRef in its items
        assert dataset.items == [EntityRef(id="syn1234", version=1)]

    async def test_add_item_entity_ref(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add an EntityRef to it
        await dataset.add_item_async(EntityRef(id="syn1234", version=1))
        # THEN I expect the Dataset to have the EntityRef in its items
        assert dataset.items == [EntityRef(id="syn1234", version=1)]

    async def test_add_item_file(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add a File to it
        await dataset.add_item_async(File(id="syn1234", version_number=1))
        # THEN I expect the Dataset to have the File in its items
        assert dataset.items == [EntityRef(id="syn1234", version=1)]

    async def test_add_item_folder(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add a Folder to it
        with patch(
            "synapseclient.models.Folder._retrieve_children",
            return_value=[
                {
                    "id": "syn1235",
                    "versionNumber": 1,
                    "type": concrete_types.FILE_ENTITY,
                }
            ],
        ):
            await dataset.add_item_async(Folder(id="syn1234"))
        # THEN I expect the Dataset to have the Folder in its items
        assert dataset.items == [
            EntityRef(id="syn1235", version=1),
        ]

    async def test_add_item_invalid_type(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add an invalid type to it
        with pytest.raises(
            ValueError,
            match="item must be one of EntityRef, File, or Folder. 1 is a <class 'int'>",
        ):
            await dataset.add_item_async(1)

    async def test_remove_entity_ref(self):
        # GIVEN a Dataset with an EntityRef
        dataset = Dataset(items=[EntityRef(id="syn1234", version=1)])
        # WHEN I remove the EntityRef from it
        dataset._remove_entity_ref(EntityRef(id="syn1234", version=1))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []

    async def test_remove_item_entity_ref(self):
        # GIVEN a Dataset with an EntityRef
        dataset = Dataset(items=[EntityRef(id="syn1234", version=1)])
        # WHEN I remove the EntityRef from it
        await dataset.remove_item_async(EntityRef(id="syn1234", version=1))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []

    async def test_remove_item_file(self):
        # GIVEN a Dataset with a File
        dataset = Dataset(items=[EntityRef(id="syn1234", version=1)])
        # WHEN I remove the File from it
        await dataset.remove_item_async(File(id="syn1234", version_number=1))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []

    async def test_remove_item_folder(self):
        # GIVEN a Dataset with a Folder
        dataset = Dataset(items=[EntityRef(id="syn1235", version=1)])
        with patch(
            "synapseclient.models.Folder._retrieve_children",
            return_value=[
                {
                    "id": "syn1235",
                    "versionNumber": 1,
                    "type": concrete_types.FILE_ENTITY,
                }
            ],
        ):
            # WHEN I remove the Folder from it
            await dataset.remove_item_async(Folder(id="syn1234"))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []
