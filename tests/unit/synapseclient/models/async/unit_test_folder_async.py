"""Tests for the Folder class."""
import uuid
from typing import Dict
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Folder as Synapse_Folder
from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.core.constants.concrete_types import FILE_ENTITY
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models import FailureStrategy, File, Folder

SYN_123 = "syn123"
SYN_456 = "syn456"
FOLDER_NAME = "example_folder"
PARENT_ID = "parent_id_value"
DESCRIPTION = "This is an example folder."
ETAG = "etag_value"
CREATED_ON = "createdOn_value"
MODIFIED_ON = "modifiedOn_value"
CREATED_BY = "createdBy_value"
MODIFIED_BY = "modifiedBy_value"


class TestFolder:
    """Tests for the Folder class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_synapse_folder_output(self) -> Synapse_Folder:
        return Synapse_Folder(
            id=SYN_123,
            name=FOLDER_NAME,
            parentId=PARENT_ID,
            description=DESCRIPTION,
            etag=ETAG,
            createdOn=CREATED_ON,
            modifiedOn=MODIFIED_ON,
            createdBy=CREATED_BY,
            modifiedBy=MODIFIED_BY,
        )

    def get_example_rest_api_folder_output(self) -> Dict[str, str]:
        return {
            "entity": {
                "concreteType": concrete_types.FOLDER_ENTITY,
                "id": SYN_123,
                "name": FOLDER_NAME,
                "parentId": PARENT_ID,
                "description": DESCRIPTION,
                "etag": ETAG,
                "createdOn": CREATED_ON,
                "modifiedOn": MODIFIED_ON,
                "createdBy": CREATED_BY,
                "modifiedBy": MODIFIED_BY,
            },
        }

    def test_fill_from_dict(self) -> None:
        # GIVEN an example Synapse Folder `get_example_synapse_folder_output`
        # WHEN I call `fill_from_dict` with the example Synapse Folder
        folder_output = Folder().fill_from_dict(
            self.get_example_synapse_folder_output()
        )

        # THEN the Folder object should be filled with the example Synapse Folder
        assert folder_output.id == SYN_123
        assert folder_output.name == FOLDER_NAME
        assert folder_output.parent_id == PARENT_ID
        assert folder_output.description == DESCRIPTION
        assert folder_output.etag == ETAG
        assert folder_output.created_on == CREATED_ON
        assert folder_output.modified_on == MODIFIED_ON
        assert folder_output.created_by == CREATED_BY
        assert folder_output.modified_by == MODIFIED_BY

    async def test_store_with_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # AND a random description
        description = str(uuid.uuid4())
        folder.description = description

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                    description=description,
                ),
                set_annotations=False,
                isRestricted=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND the folder should be stored with the mock return data
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_store_with_no_changes(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
        ) as mocked_store, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should not call store because there are no changes
            mocked_store.assert_not_called()

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND the folder should only contain the ID
            assert result.id == SYN_123

    async def test_store_after_get(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # AND I call `get` on the Folder object
        with patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            await folder.get_async(synapse_client=self.syn)

            mocked_get.assert_called_once_with(
                entity_id=folder.id, synapse_client=self.syn
            )
            assert folder.id == SYN_123

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
        ) as mocked_store, patch.object(
            self.syn,
            "get",
            return_value=Synapse_Folder(
                id=folder.id,
            ),
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should not call store because there are no changes
            mocked_store.assert_not_called()

            # AND we should not call get as we already have
            mocked_get.assert_not_called()

            # AND the folder should only contain the ID
            assert result.id == SYN_123

    async def test_store_after_get_with_changes(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # AND I call `get` on the Folder object
        with patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            await folder.get_async(synapse_client=self.syn)

            mocked_get.assert_called_once_with(
                entity_id=folder.id, synapse_client=self.syn
            )
            assert folder.id == SYN_123

        # AND I update a field on the folder
        description = str(uuid.uuid4())
        folder.description = description

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_store, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should  call store because there are changes
            mocked_store.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                    description=description,
                ),
                set_annotations=False,
                isRestricted=False,
                createOrUpdate=False,
            )

            # AND we should not call get as we already have
            mocked_get.assert_not_called()

            # AND the folder should contained the mocked store return data
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_store_with_annotations(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
            annotations={
                "my_single_key_string": ["a"],
                "my_key_string": ["b", "a", "c"],
                "my_key_bool": [False, False, False],
                "my_key_double": [1.2, 3.4, 5.6],
                "my_key_long": [1, 2, 3],
            },
        )

        # AND a random description
        description = str(uuid.uuid4())
        folder.description = description

        # WHEN I call `store` with the Folder object
        with patch(
            "synapseclient.models.folder.store_entity_components",
            return_value=(None),
        ) as mocked_store_entity_components, patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                    description=description,
                ),
                set_annotations=False,
                isRestricted=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND we should store the annotations component
            mocked_store_entity_components.assert_called_once_with(
                root_resource=folder,
                failure_strategy=FailureStrategy.LOG_EXCEPTION,
                synapse_client=self.syn,
            )

            # AND the folder should be stored with the mock return data
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_store_with_name_and_parent_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name=FOLDER_NAME,
            parent_id=PARENT_ID,
        )

        # AND a random description
        description = str(uuid.uuid4())
        folder.description = description

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call, patch.object(
            self.syn,
            "findEntityId",
            return_value=SYN_123,
        ) as mocked_get, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                    name=folder.name,
                    parent=folder.parent_id,
                    description=description,
                ),
                set_annotations=False,
                isRestricted=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND findEntityId should be called
            mocked_get.assert_called_once()

            # AND the folder should be stored
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_store_with_name_and_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name=FOLDER_NAME,
        )

        # AND a random description
        description = str(uuid.uuid4())
        folder.description = description

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call, patch.object(
            self.syn,
            "findEntityId",
            return_value=SYN_123,
        ) as mocked_get, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=(
                {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": folder.id,
                    }
                }
            ),
        ) as mocked_get:
            result = await folder.store_async(
                parent=Folder(id=PARENT_ID), synapse_client=self.syn
            )

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                    name=folder.name,
                    parent=folder.parent_id,
                    description=description,
                ),
                set_annotations=False,
                isRestricted=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND findEntityId should be called
            mocked_get.assert_called_once()

            # AND the folder should be stored
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_store_no_id_name_or_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder()

        # WHEN I call `store` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.store_async(synapse_client=self.syn)

        # THEN we should get an error
        assert (
            str(e.value) == "The folder must have an id or a "
            "(name and (`parent_id` or parent with an id)) set."
        )

    async def test_store_no_id_or_name(self) -> None:
        # GIVEN a Folder object
        folder = Folder(parent_id=PARENT_ID)

        # WHEN I call `store` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.store_async(synapse_client=self.syn)

        # THEN we should get an error
        assert (
            str(e.value) == "The folder must have an id or a "
            "(name and (`parent_id` or parent with an id)) set."
        )

    async def test_store_no_id_or_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder(name=FOLDER_NAME)

        # WHEN I call `store` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.store_async(synapse_client=self.syn)

        # THEN we should get an error
        assert (
            str(e.value) == "The folder must have an id or a "
            "(name and (`parent_id` or parent with an id)) set."
        )

    async def test_get_by_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # WHEN I call `get` with the Folder object
        with patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=self.get_example_rest_api_folder_output(),
        ) as mocked_client_call:
            result = await folder.get_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity_id=folder.id, synapse_client=self.syn
            )

            # AND the folder should be stored
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_get_by_name_and_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name=FOLDER_NAME,
            parent_id=PARENT_ID,
        )

        # WHEN I call `get` with the Folder object
        with patch.object(
            self.syn,
            "findEntityId",
            return_value=(SYN_123),
        ) as mocked_client_search, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=self.get_example_rest_api_folder_output(),
        ) as mocked_client_call:
            result = await folder.get_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity_id=folder.id, synapse_client=self.syn
            )

            # AND we should search for the entity
            mocked_client_search.assert_called_once_with(
                name=folder.name,
                parent=folder.parent_id,
            )

            # AND the folder should be stored
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    async def test_get_by_name_and_parent_not_found(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name=FOLDER_NAME,
            parent_id=PARENT_ID,
        )

        # WHEN I call `get` with the Folder object
        with patch.object(
            self.syn,
            "findEntityId",
            return_value=(None),
        ) as mocked_client_search:
            with pytest.raises(SynapseNotFoundError) as e:
                await folder.get_async(synapse_client=self.syn)
            assert (
                str(e.value)
                == "Folder [Id: None, Name: example_folder, Parent: parent_id_value] not found in Synapse."
            )

            mocked_client_search.assert_called_once_with(
                name=folder.name,
                parent=folder.parent_id,
            )

    async def test_delete_with_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # WHEN I call `delete` with the Folder object
        with patch.object(
            self.syn,
            "delete",
            return_value=(None),
        ) as mocked_client_call:
            await folder.delete_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=folder.id,
            )

    async def test_delete_missing_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder()

        # WHEN I call `delete` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.delete_async(synapse_client=self.syn)

        # THEN we should get an error
        assert str(e.value) == "The folder must have an id set."

    async def test_copy(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # AND a returned Folder object
        returned_folder = Folder(id=SYN_456)

        # AND a copy mapping exists
        copy_mapping = {
            SYN_123: SYN_456,
        }

        # WHEN I call `copy` with the Folder object
        with patch(
            "synapseclient.models.folder.copy",
            return_value=(copy_mapping),
        ) as mocked_copy, patch(
            "synapseclient.models.folder.Folder.get_async",
            return_value=(returned_folder),
        ) as mocked_get, patch(
            "synapseclient.models.folder.Folder.sync_from_synapse_async",
            return_value=(returned_folder),
        ) as mocked_sync:
            result = await folder.copy_async(
                parent_id="destination_id", synapse_client=self.syn
            )

            # THEN we should call the method with this data
            mocked_copy.assert_called_once_with(
                syn=self.syn,
                entity=folder.id,
                destinationId="destination_id",
                excludeTypes=[],
                skipCopyAnnotations=False,
                updateExisting=False,
                setProvenance="traceback",
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND we should call the sync method
            mocked_sync.assert_called_once_with(
                download_file=False,
                synapse_client=self.syn,
            )

            # AND the file should be stored
            assert result.id == SYN_456

    async def test_copy_missing_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder()

        # WHEN I call `copy` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.copy_async(parent_id="destination_id", synapse_client=self.syn)

        # THEN we should get an error
        assert str(e.value) == "The folder must have an ID and parent_id to copy."

    async def test_copy_missing_destination(self) -> None:
        # GIVEN a Folder object
        folder = Folder(id=SYN_123)

        # WHEN I call `copy` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.copy_async(parent_id=None, synapse_client=self.syn)

        # THEN we should get an error
        assert str(e.value) == "The folder must have an ID and parent_id to copy."

    async def test_sync_from_synapse(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id=SYN_123,
        )

        # AND Children that exist on the folder in Synapse
        children = [
            {
                "id": SYN_456,
                "type": FILE_ENTITY,
                "name": "example_file_1",
            }
        ]

        # WHEN I call `sync_from_synapse` with the Folder object
        async def mock_get_children(*args, **kwargs):
            for child in children:
                yield child

        with patch(
            "synapseclient.models.mixins.storable_container.get_children",
            side_effect=mock_get_children,
        ) as mocked_children_call, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=self.get_example_rest_api_folder_output(),
        ) as mocked_folder_get, patch(
            "synapseclient.models.file.File.get_async",
            return_value=(File(id=SYN_456, name="example_file_1")),
        ):
            result = await folder.sync_from_synapse_async(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_children_call.assert_called_once()

            # AND we should call the get method
            mocked_folder_get.assert_called_once()

            # AND the file/folder should be retrieved
            assert result.id == SYN_123
            assert result.name == FOLDER_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.files[0].id == SYN_456
            assert result.files[0].name == "example_file_1"
