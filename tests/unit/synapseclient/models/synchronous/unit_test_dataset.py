from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.models import Annotations, Dataset, EntityRef, File, Folder


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

    def test_add_item_entity_ref(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add an EntityRef to it
        dataset.add_item(EntityRef(id="syn1234", version=1))
        # THEN I expect the Dataset to have the EntityRef in its items
        assert dataset.items == [EntityRef(id="syn1234", version=1)]

    def test_add_item_file(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add a File to it
        dataset.add_item(File(id="syn1234", version_number=1))
        # THEN I expect the Dataset to have the File in its items
        assert dataset.items == [EntityRef(id="syn1234", version=1)]

    def test_add_item_folder(self):
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
            dataset.add_item(Folder(id="syn1234"))
        # THEN I expect the Dataset to have the Folder in its items
        assert dataset.items == [
            EntityRef(id="syn1235", version=1),
        ]

    def test_add_item_invalid_type(self):
        # GIVEN an empty Dataset
        dataset = Dataset()
        # WHEN I add an invalid type to it
        with pytest.raises(
            ValueError,
            match="item must be one of EntityRef, File, or Folder. 1 is a <class 'int'>",
        ):
            dataset.add_item(1)

    def test_remove_item_entity_ref(self):
        # GIVEN a Dataset with an EntityRef
        dataset = Dataset(items=[EntityRef(id="syn1234", version=1)])
        # WHEN I remove the EntityRef from it
        dataset.remove_item(EntityRef(id="syn1234", version=1))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []

    def test_remove_item_file(self):
        # GIVEN a Dataset with a File
        dataset = Dataset(items=[EntityRef(id="syn1234", version=1)])
        # WHEN I remove the File from it
        dataset.remove_item(File(id="syn1234", version_number=1))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []

    def test_remove_item_folder(self):
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
            dataset.remove_item(Folder(id="syn1234"))
        # THEN I expect the Dataset to have no items
        assert dataset.items == []
