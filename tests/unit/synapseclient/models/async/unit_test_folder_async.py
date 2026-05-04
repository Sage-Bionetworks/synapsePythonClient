"""Tests for the Folder class."""

import uuid
from typing import Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Folder as Synapse_Folder
from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.core.constants.concrete_types import FILE_ENTITY
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models import FailureStrategy, File, Folder
from synapseclient.models.project_setting import ProjectSetting
from synapseclient.models.services.migration_types import MigrationResult

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
        with (
            patch(
                "synapseclient.models.services.storable_entity.put_entity",
                new_callable=AsyncMock,
                return_value=(self.get_example_synapse_folder_output()),
            ) as mocked_client_call,
            patch(
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
            ) as mocked_get,
        ):
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
        with (
            patch.object(
                self.syn,
                "store",
            ) as mocked_store,
            patch(
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
            ) as mocked_get,
        ):
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
        with (
            patch.object(
                self.syn,
                "store",
            ) as mocked_store,
            patch.object(
                self.syn,
                "get",
                return_value=Synapse_Folder(
                    id=folder.id,
                ),
            ) as mocked_get,
        ):
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
        with (
            patch(
                "synapseclient.models.services.storable_entity.put_entity",
                new_callable=AsyncMock,
                return_value=(self.get_example_synapse_folder_output()),
            ) as mocked_store,
            patch(
                "synapseclient.api.entity_factory.get_entity_id_bundle2",
                new_callable=AsyncMock,
            ) as mocked_get,
        ):
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
        with (
            patch(
                "synapseclient.models.folder.store_entity_components",
                return_value=(None),
            ) as mocked_store_entity_components,
            patch(
                "synapseclient.models.services.storable_entity.put_entity",
                new_callable=AsyncMock,
                return_value=(self.get_example_synapse_folder_output()),
            ) as mocked_client_call,
            patch(
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
            ) as mocked_get,
        ):
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
        with (
            patch(
                "synapseclient.models.services.storable_entity.put_entity",
                new_callable=AsyncMock,
                return_value=(self.get_example_synapse_folder_output()),
            ) as mocked_client_call,
            patch.object(
                self.syn,
                "findEntityId",
                return_value=SYN_123,
            ) as mocked_get,
            patch(
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
            ) as mocked_get,
        ):
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
        with (
            patch(
                "synapseclient.models.services.storable_entity.put_entity",
                new_callable=AsyncMock,
                return_value=(self.get_example_synapse_folder_output()),
            ) as mocked_client_call,
            patch.object(
                self.syn,
                "findEntityId",
                return_value=SYN_123,
            ) as mocked_get,
            patch(
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
            ) as mocked_get,
        ):
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
        with (
            patch.object(
                self.syn,
                "findEntityId",
                return_value=(SYN_123),
            ) as mocked_client_search,
            patch(
                "synapseclient.api.entity_factory.get_entity_id_bundle2",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_folder_output(),
            ) as mocked_client_call,
        ):
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
        with (
            patch(
                "synapseclient.models.folder.copy",
                return_value=(copy_mapping),
            ) as mocked_copy,
            patch(
                "synapseclient.models.folder.Folder.get_async",
                return_value=(returned_folder),
            ) as mocked_get,
            patch(
                "synapseclient.models.folder.Folder.sync_from_synapse_async",
                return_value=(returned_folder),
            ) as mocked_sync,
        ):
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

        with (
            patch(
                "synapseclient.models.mixins.storable_container.get_children",
                side_effect=mock_get_children,
            ) as mocked_children_call,
            patch(
                "synapseclient.api.entity_factory.get_entity_id_bundle2",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_folder_output(),
            ) as mocked_folder_get,
            patch(
                "synapseclient.models.file.File.get_async",
                return_value=(File(id=SYN_456, name="example_file_1")),
            ),
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


class TestStorageLocationMixin:
    """Tests for ProjectSettingsMixin methods on Folder."""

    STORAGE_LOCATION_ID = 12345
    SETTING_ID = "setting_abc"

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture()
    def example_setting(self):
        return ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[self.STORAGE_LOCATION_ID],
        )

    # -------------------------------------------------------------------------
    # set_storage_location_async
    # -------------------------------------------------------------------------

    async def test_set_storage_location_creates_new_custom_storage_location(
        self, example_setting
    ) -> None:
        """Test that when there is no existing project setting and we set a storage location, a new project setting is created."""
        folder = Folder(id=SYN_123)

        with (
            patch.object(
                ProjectSetting, "get_async", new_callable=AsyncMock, return_value=None
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                autospec=True,
                return_value=example_setting,
            ) as mocked_store,
        ):
            result = await folder.set_storage_location_async(
                storage_location_id=self.STORAGE_LOCATION_ID,
                synapse_client=self.syn,
            )

        # THEN store was called and the new setting has the correct locations and project
        stored_setting = mocked_store.call_args.args[0]
        assert stored_setting.project_id == SYN_123
        assert stored_setting.locations == [self.STORAGE_LOCATION_ID]
        assert result.id == self.SETTING_ID

    async def test_set_storage_location_updates_existing_setting(
        self, example_setting
    ) -> None:
        """Test that when there is an existing project setting and we set a storage location, the existing project setting is updated."""
        folder = Folder(id=SYN_123)

        updated_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[99999],
        )

        with (
            patch.object(
                ProjectSetting,
                "get_async",
                new_callable=AsyncMock,
                return_value=example_setting,
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                autospec=True,
                return_value=updated_setting,
            ) as mocked_store,
        ):
            result = await folder.set_storage_location_async(
                storage_location_id=99999,
                synapse_client=self.syn,
            )

        # THEN store was called with the updated locations
        stored_setting = mocked_store.call_args.args[0]
        assert stored_setting.locations == [99999]
        assert result.locations == [99999]

    async def test_set_storage_location_replaces_all_existing_locations(self) -> None:
        """Test that set_storage_location_async is destructive — the provided
        location(s) fully replace any previously configured locations."""
        folder = Folder(id=SYN_123)

        existing_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[111, 222],
        )
        updated_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[333],
        )

        with (
            patch.object(
                ProjectSetting,
                "get_async",
                new_callable=AsyncMock,
                return_value=existing_setting,
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                new_callable=AsyncMock,
                return_value=updated_setting,
            ),
        ):
            result = await folder.set_storage_location_async(
                storage_location_id=333,
                synapse_client=self.syn,
            )

        # THEN only the new location is present — the previous [111, 222] are gone
        assert result.locations == [333]

    async def test_set_storage_location_use_default_storage_location_instead(
        self, example_setting
    ) -> None:
        """Test that when storage_location_id is not provided, the default Synapse S3 storage location is used."""
        from synapseclient.models.mixins.storage_location_mixin import (
            DEFAULT_STORAGE_LOCATION_ID,
        )

        folder = Folder(id=SYN_123)

        default_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[DEFAULT_STORAGE_LOCATION_ID],
        )

        with (
            patch.object(
                ProjectSetting,
                "get_async",
                new_callable=AsyncMock,
                return_value=example_setting,
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                autospec=True,
                return_value=default_setting,
            ) as mocked_store,
        ):
            result = await folder.set_storage_location_async(
                synapse_client=self.syn,
            )

        stored_setting = mocked_store.call_args.args[0]
        assert stored_setting.locations == [DEFAULT_STORAGE_LOCATION_ID]
        assert result.locations == [DEFAULT_STORAGE_LOCATION_ID]

    async def test_set_storage_location_uses_default_storage_location_instead_when_storage_location_id_is_none(
        self, example_setting
    ) -> None:
        """Test that when storage_location_id is not provided, the default Synapse S3 storage location is used."""
        from synapseclient.models.mixins.storage_location_mixin import (
            DEFAULT_STORAGE_LOCATION_ID,
        )

        folder = Folder(id=SYN_123)

        default_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[DEFAULT_STORAGE_LOCATION_ID],
        )

        with (
            patch.object(
                ProjectSetting,
                "get_async",
                new_callable=AsyncMock,
                return_value=example_setting,
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                autospec=True,
                return_value=default_setting,
            ) as mocked_store,
        ):
            result = await folder.set_storage_location_async(
                storage_location_id=None,
                synapse_client=self.syn,
            )

        stored_setting = mocked_store.call_args.args[0]
        assert stored_setting.locations == [DEFAULT_STORAGE_LOCATION_ID]
        assert result.locations == [DEFAULT_STORAGE_LOCATION_ID]

    async def test_set_storage_location_accepts_list_of_ids(
        self, example_setting
    ) -> None:
        """Test that when storage_location_id is a list of integers, all are stored as-is."""
        folder = Folder(id=SYN_123)

        with (
            patch.object(
                ProjectSetting, "get_async", new_callable=AsyncMock, return_value=None
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                autospec=True,
                return_value=example_setting,
            ) as mocked_store,
        ):
            await folder.set_storage_location_async(
                storage_location_id=[111, 222, 333],
                synapse_client=self.syn,
            )

        stored_setting = mocked_store.call_args.args[0]
        assert stored_setting.locations == [111, 222, 333]

    async def test_set_storage_location_converts_single_id_to_list(
        self, example_setting
    ) -> None:
        """Test that when storage_location_id is a single integer, it is wrapped in a list."""
        folder = Folder(id=SYN_123)

        with (
            patch.object(
                ProjectSetting, "get_async", new_callable=AsyncMock, return_value=None
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                autospec=True,
                return_value=example_setting,
            ) as mocked_store,
        ):
            await folder.set_storage_location_async(
                storage_location_id=111,
                synapse_client=self.syn,
            )

        stored_setting = mocked_store.call_args.args[0]
        assert stored_setting.locations == [111]

    async def test_partial_update_locations_via_get_and_store(self) -> None:
        """Test the partial update pattern: retrieve the existing setting, append a
        location, and store — without losing previously configured locations."""
        folder = Folder(id=SYN_123)

        existing_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[111, 222],
        )
        updated_setting = ProjectSetting(
            id=self.SETTING_ID,
            project_id=SYN_123,
            settings_type="upload",
            locations=[111, 222, 333],
        )

        with (
            patch.object(
                ProjectSetting,
                "get_async",
                new_callable=AsyncMock,
                return_value=existing_setting,
            ),
            patch.object(
                ProjectSetting,
                "store_async",
                new_callable=AsyncMock,
                return_value=updated_setting,
            ) as mocked_store,
        ):
            setting = await folder.get_project_setting_async(
                setting_type="upload",
                synapse_client=self.syn,
            )
            setting.locations.append(333)
            result = await setting.store_async(synapse_client=self.syn)

        # THEN all three locations are present — the existing ones were preserved
        assert result.locations == [111, 222, 333]
        mocked_store.assert_awaited_once_with(synapse_client=self.syn)

    async def test_set_storage_location_raises_when_no_id(self) -> None:
        """Test that when a folder without an id, an error is raised."""
        folder = Folder()

        with pytest.raises(ValueError, match="The entity must have an id set."):
            await folder.set_storage_location_async(
                storage_location_id=self.STORAGE_LOCATION_ID,
                synapse_client=self.syn,
            )

    # -------------------------------------------------------------------------
    # get_project_setting_async
    # -------------------------------------------------------------------------

    async def test_get_project_setting_returns_setting(self, example_setting) -> None:
        """Test that when a project setting exists, it is returned."""
        folder = Folder(id=SYN_123)

        with patch.object(
            ProjectSetting,
            "get_async",
            new_callable=AsyncMock,
            return_value=example_setting,
        ):
            result = await folder.get_project_setting_async(
                setting_type="upload",
                synapse_client=self.syn,
            )

        assert result.id == self.SETTING_ID
        assert result.locations == [self.STORAGE_LOCATION_ID]

    async def test_get_project_setting_raises_when_no_id(self) -> None:
        """Test that when a folder without an id, an error is raised."""
        folder = Folder()

        with pytest.raises(ValueError, match="The entity must have an id set."):
            await folder.get_project_setting_async(synapse_client=self.syn)

    # -------------------------------------------------------------------------
    # delete_project_setting_async
    # -------------------------------------------------------------------------

    async def test_delete_project_setting_calls_service(self) -> None:
        """Test that when a project setting exists, it is deleted."""
        folder = Folder(id=SYN_123)

        with patch.object(
            ProjectSetting, "delete_async", new_callable=AsyncMock, return_value=None
        ) as mocked_delete:
            await folder.delete_project_setting_async(
                setting_id=self.SETTING_ID,
                synapse_client=self.syn,
            )

        mocked_delete.assert_awaited_once_with(synapse_client=self.syn)

    async def test_delete_project_setting_raises_when_no_id(self) -> None:
        """Test that when a folder without an id, an error is raised."""
        folder = Folder(id=SYN_123)

        with pytest.raises(
            ValueError, match="The id is required to delete a project setting."
        ):
            await folder.delete_project_setting_async(
                setting_id=None,
                synapse_client=self.syn,
            )

    # -------------------------------------------------------------------------
    # get_sts_storage_token_async
    # -------------------------------------------------------------------------

    async def test_get_sts_storage_token_returns_credentials(self) -> None:
        """Test that when a folder with an id, the STS credentials are returned."""
        folder = Folder(id=SYN_123)

        expected_credentials = {
            "aws_access_key_id": "AKIA...",
            "aws_secret_access_key": "secret",
            "aws_session_token": "token",
        }

        with patch(
            "synapseclient.models.mixins.storage_location_mixin.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=expected_credentials,
        ) as mocked_to_thread:
            result = await folder.get_sts_storage_token_async(
                permission="read_only",
                synapse_client=self.syn,
            )

            mocked_to_thread.assert_called_once()
            call_args = mocked_to_thread.call_args
            assert call_args.args[0].__name__ == "get_sts_credentials"
            assert call_args.args[2] == SYN_123
            assert call_args.args[3] == "read_only"
            assert call_args.kwargs["output_format"] == "json"
            assert call_args.kwargs["min_remaining_life"] is None

            assert result == expected_credentials

    async def test_get_sts_storage_token_passes_output_format_and_min_remaining_life(
        self,
    ) -> None:
        """Test that when a folder with an id, the STS credentials are returned with the output format and min remaining life."""
        folder = Folder(id=SYN_123)

        with patch(
            "synapseclient.models.mixins.storage_location_mixin.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value={},
        ) as mocked_to_thread:
            await folder.get_sts_storage_token_async(
                permission="read_write",
                output_format="boto",
                min_remaining_life=300,
                synapse_client=self.syn,
            )

            call_args = mocked_to_thread.call_args
            assert call_args.args[0].__name__ == "get_sts_credentials"
            assert call_args.args[2] == SYN_123
            assert call_args.args[3] == "read_write"
            assert call_args.kwargs["output_format"] == "boto"
            assert call_args.kwargs["min_remaining_life"] == 300

    async def test_get_sts_storage_token_raises_when_no_id(self) -> None:
        """Test that when a folder without an id, an error is raised."""
        folder = Folder()

        with pytest.raises(ValueError, match="The entity must have an id set."):
            await folder.get_sts_storage_token_async(
                permission="read_only",
                synapse_client=self.syn,
            )

    # -------------------------------------------------------------------------
    # index_files_for_migration_async
    # -------------------------------------------------------------------------

    async def test_index_files_for_migration_calls_service(self) -> None:
        """Test that when a folder with an id, the files are indexed."""
        folder = Folder(id=SYN_123)

        mock_result = MagicMock(spec=MigrationResult)

        with patch(
            "synapseclient.models.mixins.storage_location_mixin._index_files_for_migration_async",
            new_callable=AsyncMock,
            return_value=mock_result,
        ) as mocked_index:
            result = await folder.index_files_for_migration_async(
                dest_storage_location_id=self.STORAGE_LOCATION_ID,
                synapse_client=self.syn,
            )

            mocked_index.assert_called_once_with(
                folder,
                dest_storage_location_id=str(self.STORAGE_LOCATION_ID),
                db_path=None,
                source_storage_location_ids=None,
                file_version_strategy="new",
                include_table_files=False,
                continue_on_error=False,
                synapse_client=self.syn,
            )
            assert result == mock_result

    async def test_index_files_for_migration_converts_source_ids_to_strings(
        self,
    ) -> None:
        """Test that when source_storage_location_ids are integers, they are converted to strings."""
        folder = Folder(id=SYN_123)

        with patch(
            "synapseclient.models.mixins.storage_location_mixin._index_files_for_migration_async",
            new_callable=AsyncMock,
            return_value=MagicMock(spec=MigrationResult),
        ) as mocked_index:
            await folder.index_files_for_migration_async(
                dest_storage_location_id=self.STORAGE_LOCATION_ID,
                source_storage_location_ids=[111, 222],
                synapse_client=self.syn,
            )

            call_kwargs = mocked_index.call_args.kwargs
            assert call_kwargs["source_storage_location_ids"] == ["111", "222"]

    async def test_index_files_for_migration_raises_when_no_id(self) -> None:
        """Test that when a folder without an id, an error is raised."""
        folder = Folder()

        with pytest.raises(ValueError, match="The entity must have an id set."):
            await folder.index_files_for_migration_async(
                dest_storage_location_id=self.STORAGE_LOCATION_ID,
                synapse_client=self.syn,
            )

    # -------------------------------------------------------------------------
    # migrate_indexed_files_async
    # -------------------------------------------------------------------------

    async def test_migrate_indexed_files_calls_service(self) -> None:
        """Test that when a folder with an id, the files are migrated."""
        folder = Folder(id=SYN_123)

        db_path = "/tmp/migration.db"
        mock_result = MagicMock(spec=MigrationResult)

        with patch(
            "synapseclient.models.mixins.storage_location_mixin._migrate_indexed_files_async",
            new_callable=AsyncMock,
            return_value=mock_result,
        ) as mocked_migrate:
            result = await folder.migrate_indexed_files_async(
                db_path=db_path,
                synapse_client=self.syn,
            )

            mocked_migrate.assert_called_once_with(
                db_path=db_path,
                create_table_snapshots=True,
                continue_on_error=False,
                force=False,
                synapse_client=self.syn,
            )
            assert result == mock_result

    async def test_migrate_indexed_files_passes_all_options(self) -> None:
        """Test that when a folder with an id, the files are migrated with all options."""
        folder = Folder(id=SYN_123)

        mock_result = MagicMock(spec=MigrationResult)
        with patch(
            "synapseclient.models.mixins.storage_location_mixin._migrate_indexed_files_async",
            new_callable=AsyncMock,
            return_value=mock_result,
        ) as mocked_migrate:
            result = await folder.migrate_indexed_files_async(
                db_path="/tmp/migration.db",
                create_table_snapshots=False,
                continue_on_error=True,
                force=True,
                synapse_client=self.syn,
            )

            mocked_migrate.assert_called_once_with(
                db_path="/tmp/migration.db",
                create_table_snapshots=False,
                continue_on_error=True,
                force=True,
                synapse_client=self.syn,
            )
            assert result == mock_result

    async def test_migrate_indexed_files_raises_when_no_id(self) -> None:
        """Test that when a folder without an id, an error is raised."""
        folder = Folder()

        with pytest.raises(ValueError, match="The entity must have an id set."):
            await folder.migrate_indexed_files_async(
                db_path="/tmp/migration.db",
                synapse_client=self.syn,
            )
