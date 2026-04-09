"""Tests for the Folder class."""
import csv
import datetime
import io
import os
import tempfile
import uuid
from typing import Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Folder as Synapse_Folder
from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.core.constants.concrete_types import FILE_ENTITY, FOLDER_ENTITY
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models import Activity, FailureStrategy, File, Folder
from synapseclient.models.services.manifest import (
    _extract_entity_metadata_for_manifest_csv,
    _write_manifest_data_csv,
    generate_manifest_csv,
)

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
        with patch(
            "synapseclient.models.services.storable_entity.put_entity",
            new_callable=AsyncMock,
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
            mocked_client_call.assert_called_once()
            call_args = mocked_client_call.call_args
            assert call_args.kwargs["entity_id"] == folder.id
            assert call_args.kwargs["new_version"] is False
            assert call_args.kwargs["synapse_client"] == self.syn
            # The request should be a dict with the folder properties
            request_dict = call_args.kwargs["request"]
            assert (
                request_dict["concreteType"] == "org.sagebionetworks.repo.model.Folder"
            )
            assert request_dict["id"] == folder.id
            assert request_dict["description"] == description

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
        with patch(
            "synapseclient.models.services.storable_entity.put_entity",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_store, patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
        ) as mocked_get:
            result = await folder.store_async(synapse_client=self.syn)

            # THEN we should  call store because there are changes
            mocked_store.assert_called_once()
            call_args = mocked_store.call_args
            assert call_args.kwargs["entity_id"] == folder.id
            assert call_args.kwargs["new_version"] is False
            assert call_args.kwargs["synapse_client"] == self.syn
            # The request should be a dict with the folder properties
            request_dict = call_args.kwargs["request"]
            assert (
                request_dict["concreteType"] == "org.sagebionetworks.repo.model.Folder"
            )
            assert request_dict["id"] == folder.id
            assert request_dict["description"] == description

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
        ) as mocked_store_entity_components, patch(
            "synapseclient.models.services.storable_entity.put_entity",
            new_callable=AsyncMock,
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
            mocked_client_call.assert_called_once()
            call_args = mocked_client_call.call_args
            assert call_args.kwargs["entity_id"] == folder.id
            assert call_args.kwargs["new_version"] is False
            assert call_args.kwargs["synapse_client"] == self.syn
            # The request should be a dict with the folder properties
            request_dict = call_args.kwargs["request"]
            assert (
                request_dict["concreteType"] == "org.sagebionetworks.repo.model.Folder"
            )
            assert request_dict["id"] == folder.id
            assert request_dict["description"] == description

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
        with patch(
            "synapseclient.models.services.storable_entity.put_entity",
            new_callable=AsyncMock,
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
            mocked_client_call.assert_called_once()
            call_args = mocked_client_call.call_args
            assert call_args.kwargs["entity_id"] == SYN_123  # From findEntityId mock
            assert call_args.kwargs["new_version"] is False
            assert call_args.kwargs["synapse_client"] == self.syn
            # The request should be a dict with the folder properties
            request_dict = call_args.kwargs["request"]
            assert (
                request_dict["concreteType"] == "org.sagebionetworks.repo.model.Folder"
            )
            assert request_dict["name"] == folder.name
            assert request_dict["parentId"] == folder.parent_id
            assert request_dict["description"] == description

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
        with patch(
            "synapseclient.models.services.storable_entity.put_entity",
            new_callable=AsyncMock,
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
            mocked_client_call.assert_called_once()
            call_args = mocked_client_call.call_args
            assert call_args.kwargs["entity_id"] == SYN_123  # From findEntityId mock
            assert call_args.kwargs["new_version"] is False
            assert call_args.kwargs["synapse_client"] == self.syn
            # The request should be a dict with the folder properties
            request_dict = call_args.kwargs["request"]
            assert (
                request_dict["concreteType"] == "org.sagebionetworks.repo.model.Folder"
            )
            assert request_dict["name"] == folder.name
            assert request_dict["parentId"] == PARENT_ID
            assert request_dict["description"] == description

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

    async def test_sync_from_synapse_manifest_all_generates_per_directory(
        self,
    ) -> None:
        SUB_FOLDER_ID = "syn789"
        SUB_FOLDER_NAME = "sub_folder"
        FILE_2_ID = "syn012"
        FILE_2_NAME = "example_file_2"

        # GIVEN a root folder with one file and one subfolder containing one file
        folder = Folder(id=SYN_123)

        root_children = [
            {"id": SYN_456, "type": FILE_ENTITY, "name": "example_file_1"},
            {"id": SUB_FOLDER_ID, "type": FOLDER_ENTITY, "name": SUB_FOLDER_NAME},
        ]
        sub_children = [
            {"id": FILE_2_ID, "type": FILE_ENTITY, "name": FILE_2_NAME},
        ]
        get_children_call_count = 0

        async def mock_get_children(*args, **kwargs):
            nonlocal get_children_call_count
            children = root_children if get_children_call_count == 0 else sub_children
            get_children_call_count += 1
            for child in children:
                yield child

        downloaded_file_1 = File(
            id=SYN_456,
            name="example_file_1",
            parent_id=SYN_123,
        )
        downloaded_file_2 = File(
            id=FILE_2_ID,
            name=FILE_2_NAME,
            parent_id=SUB_FOLDER_ID,
        )
        file_map = {SYN_456: downloaded_file_1, FILE_2_ID: downloaded_file_2}

        async def mock_file_get(self_file, **kwargs):
            return file_map[self_file.id]

        async def mock_get_entity_bundle(entity_id, *args, **kwargs):
            if entity_id == SUB_FOLDER_ID:
                return {
                    "entity": {
                        "concreteType": concrete_types.FOLDER_ENTITY,
                        "id": SUB_FOLDER_ID,
                        "name": SUB_FOLDER_NAME,
                        "parentId": SYN_123,
                        "etag": ETAG,
                        "createdOn": CREATED_ON,
                        "modifiedOn": MODIFIED_ON,
                        "createdBy": CREATED_BY,
                        "modifiedBy": MODIFIED_BY,
                    }
                }
            return self.get_example_rest_api_folder_output()

        # WHEN I call sync_from_synapse with manifest="all" and a path
        with patch(
            "synapseclient.models.mixins.storable_container.get_children",
            side_effect=mock_get_children,
        ), patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            side_effect=mock_get_entity_bundle,
        ), patch(
            "synapseclient.models.file.File.get_async",
            side_effect=mock_file_get,
        ), patch(
            "synapseclient.models.mixins.storable_container.os.path.exists",
            return_value=True,
        ), patch(
            "synapseclient.models.mixins.storable_container.generate_manifest_csv",
        ) as mock_generate:
            await folder.sync_from_synapse_async(
                path="/tmp/mydir", manifest="all", synapse_client=self.syn
            )

        # THEN generate_manifest_csv is called once per directory (root + subfolder)
        assert mock_generate.call_count == 2
        calls_by_path = {
            c.kwargs["path"]: c.kwargs["all_files"]
            for c in mock_generate.call_args_list
        }
        assert any(f.id == SYN_456 for f in calls_by_path["/tmp/mydir"])
        assert any(
            f.id == FILE_2_ID for f in calls_by_path[f"/tmp/mydir/{SUB_FOLDER_NAME}"]
        )

    async def test_sync_from_synapse_manifest_root_generates_only_at_root(
        self,
    ) -> None:
        # GIVEN a Folder object with a path
        folder = Folder(id=SYN_123)
        children = [{"id": SYN_456, "type": FILE_ENTITY, "name": "example_file_1"}]

        async def mock_get_children(*args, **kwargs):
            for child in children:
                yield child

        downloaded_file = File(
            id=SYN_456,
            name="example_file_1",
            path="/tmp/mydir/example_file_1.txt",
            parent_id=SYN_123,
        )

        # WHEN I call sync_from_synapse with manifest="root" and a path
        with patch(
            "synapseclient.models.mixins.storable_container.get_children",
            side_effect=mock_get_children,
        ), patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=self.get_example_rest_api_folder_output(),
        ), patch(
            "synapseclient.models.file.File.get_async",
            return_value=downloaded_file,
        ), patch(
            "synapseclient.models.mixins.storable_container.generate_manifest_csv",
        ) as mock_generate:
            await folder.sync_from_synapse_async(
                path="/tmp/mydir", manifest="root", synapse_client=self.syn
            )

        # THEN generate_manifest_csv should be called exactly once with the root path
        mock_generate.assert_called_once()
        assert mock_generate.call_args.kwargs["path"] == "/tmp/mydir"

    async def test_sync_from_synapse_manifest_suppress_skips_generation(
        self,
    ) -> None:
        # GIVEN a Folder object with a path
        folder = Folder(id=SYN_123)
        children = [{"id": SYN_456, "type": FILE_ENTITY, "name": "example_file_1"}]

        async def mock_get_children(*args, **kwargs):
            for child in children:
                yield child

        # WHEN I call sync_from_synapse with manifest="suppress"
        with patch(
            "synapseclient.models.mixins.storable_container.get_children",
            side_effect=mock_get_children,
        ), patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=self.get_example_rest_api_folder_output(),
        ), patch(
            "synapseclient.models.file.File.get_async",
            return_value=(File(id=SYN_456, name="example_file_1")),
        ), patch(
            "synapseclient.models.mixins.storable_container.generate_manifest_csv",
        ) as mock_generate:
            await folder.sync_from_synapse_async(
                path="/tmp/mydir", manifest="suppress", synapse_client=self.syn
            )

        # THEN generate_manifest_csv should never be called
        mock_generate.assert_not_called()

    async def test_sync_from_synapse_no_manifest_without_path(self) -> None:
        # GIVEN a Folder with no path specified
        folder = Folder(id=SYN_123)
        children = [{"id": SYN_456, "type": FILE_ENTITY, "name": "example_file_1"}]

        async def mock_get_children(*args, **kwargs):
            for child in children:
                yield child

        # WHEN I call sync_from_synapse with no path (default manifest="all")
        with patch(
            "synapseclient.models.mixins.storable_container.get_children",
            side_effect=mock_get_children,
        ), patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value=self.get_example_rest_api_folder_output(),
        ), patch(
            "synapseclient.models.file.File.get_async",
            return_value=(File(id=SYN_456, name="example_file_1")),
        ), patch(
            "synapseclient.models.mixins.storable_container.generate_manifest_csv",
        ) as mock_generate:
            await folder.sync_from_synapse_async(synapse_client=self.syn)

        # THEN generate_manifest_csv should not be called (no path to write to)
        mock_generate.assert_not_called()


class TestGenerateManifestCsv:
    """Tests for the generate_manifest_csv and related helper functions."""

    def _make_file(
        self,
        syn_id: str = "syn123",
        name: str = "file.txt",
        path: str = "/data/file.txt",
        parent_id: str = "syn456",
        content_type: str = "text/plain",
        synapse_store: bool = True,
        annotations: dict = None,
        activity: Activity = None,
    ) -> File:
        f = File(
            id=syn_id,
            name=name,
            path=path,
            parent_id=parent_id,
            content_type=content_type,
            synapse_store=synapse_store,
        )
        if annotations:
            f.annotations = annotations
        if activity:
            f.activity = activity
        return f

    def test_extract_entity_metadata_uses_parentId_and_ID_columns(self) -> None:
        # GIVEN a File entity
        f = self._make_file()

        # WHEN metadata is extracted
        keys, data = _extract_entity_metadata_for_manifest_csv([f])

        assert data[0]["parentId"] == "syn456"
        assert data[0]["ID"] == "syn123"
        assert data[0]["path"] == "/data/file.txt"
        assert data[0]["name"] == "file.txt"

    def test_extract_entity_metadata_includes_annotations(self) -> None:
        # GIVEN a File entity with annotations
        f = self._make_file(annotations={"tissue": ["brain"], "count": [42]})

        # WHEN metadata is extracted
        keys, data = _extract_entity_metadata_for_manifest_csv([f])

        # THEN annotation keys are included and single-item lists are serialized as scalars
        assert "tissue" in keys
        assert "count" in keys
        assert data[0]["tissue"] == "brain"
        assert data[0]["count"] == "42"

    def test_write_manifest_data_csv_produces_comma_separated_output(self) -> None:
        # GIVEN a File with a name containing a comma and mixed-type annotations
        f = self._make_file(
            name="a, b, c",
            path="/data/file.txt",
            annotations={
                "single_str": "hello",
                "multi_str": ["a", "b", "c"],
                "str_with_comma": ["hello,world", "plain"],
                "booleans": [True, False],
                "integers": [1],
                "single_dt": [
                    datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
                ],
                "multi_dt": [
                    datetime.datetime(
                        2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                    ),
                    datetime.datetime(
                        2021, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc
                    ),
                ],
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called
            generate_manifest_csv(all_files=[f], path=tmpdir)
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            content = open(manifest_path, encoding="utf8").read()
            with open(manifest_path, newline="", encoding="utf8") as fp:
                row = next(csv.DictReader(fp))

        # THEN the name with a comma is quoted in the CSV output
        assert '"a, b, c"' in content
        # AND single-item list is serialized as a plain scalar
        assert row["single_str"] == "hello"
        # AND multi-value list is serialized as a bracketed string
        assert row["multi_str"] == "[a,b,c]"
        # AND a value with a comma is quoted inside the brackets
        assert row["str_with_comma"] == '["hello,world",plain]'
        # AND booleans are serialized unquoted
        assert row["booleans"] == "[True,False]"
        # AND integers are serialized unquoted
        assert row["integers"] == "1"
        # AND a single datetime is serialized as ISO 8601 UTC
        assert row["single_dt"] == "2020-01-01T00:00:00Z"
        # AND multiple datetimes are serialized as a bracketed ISO 8601 list
        assert row["multi_dt"] == "[2020-01-01T00:00:00Z,2021-06-15T12:30:00Z]"

    def test_generate_manifest_csv_creates_file(self) -> None:
        # GIVEN a list of File entities and a temp directory
        f = self._make_file(
            syn_id="syn123",
            name="data.csv",
            path="/tmp/data.csv",
            parent_id="syn456",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called
            generate_manifest_csv(all_files=[f], path=tmpdir)

            # THEN manifest.csv is created in the directory
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            assert os.path.exists(manifest_path)

            # AND it has the expected columns with new naming convention
            with open(manifest_path, newline="", encoding="utf8") as fp:
                reader = csv.DictReader(fp)
                row = next(reader)
                assert row["parentId"] == "syn456"
                assert row["ID"] == "syn123"

    def test_generate_manifest_csv_skips_when_no_files(self) -> None:
        # GIVEN an empty file list
        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called with no files
            generate_manifest_csv(all_files=[], path=tmpdir)

            # THEN no manifest.csv is created
            assert not os.path.exists(os.path.join(tmpdir, "manifest.csv"))

    def test_generate_manifest_csv_quotes_values_with_commas(self) -> None:
        # GIVEN a File whose name contains a comma
        f = self._make_file(name="file, extra.txt", path="/tmp/file, extra.txt")

        with tempfile.TemporaryDirectory() as tmpdir:
            generate_manifest_csv(all_files=[f], path=tmpdir)
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            content = open(manifest_path, encoding="utf8").read()
        # THEN the comma-containing value is quoted in the CSV
        assert '"file, extra.txt"' in content
