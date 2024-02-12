from unittest.mock import patch
import pytest
from synapseclient.models import File, Folder, FailureStrategy
from synapseclient import Folder as Synapse_Folder
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.core.constants.concrete_types import FILE_ENTITY


class TestFolder:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def get_example_synapse_folder_output(self) -> Synapse_Folder:
        return Synapse_Folder(
            id="syn123",
            name="example_folder",
            parentId="parent_id_value",
            description="This is an example folder.",
            etag="etag_value",
            createdOn="createdOn_value",
            modifiedOn="modifiedOn_value",
            createdBy="createdBy_value",
            modifiedBy="modifiedBy_value",
        )

    def test_fill_from_dict(self) -> None:
        # GIVEN an example Synapse Folder `get_example_synapse_folder_output`
        # WHEN I call `fill_from_dict` with the example Synapse Folder
        folder_output = Folder().fill_from_dict(
            self.get_example_synapse_folder_output()
        )

        # THEN the Folder object should be filled with the example Synapse Folder
        assert folder_output.id == "syn123"
        assert folder_output.name == "example_folder"
        assert folder_output.parent_id == "parent_id_value"
        assert folder_output.description == "This is an example folder."
        assert folder_output.etag == "etag_value"
        assert folder_output.created_on == "createdOn_value"
        assert folder_output.modified_on == "modifiedOn_value"
        assert folder_output.created_by == "createdBy_value"
        assert folder_output.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_store_with_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id="syn123",
        )

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call:
            result = await folder.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                ),
                set_annotations=False,
            )

            # AND the folder should be stored
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_store_with_annotations(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id="syn123",
            annotations={
                "my_single_key_string": ["a"],
                "my_key_string": ["b", "a", "c"],
                "my_key_bool": [False, False, False],
                "my_key_double": [1.2, 3.4, 5.6],
                "my_key_long": [1, 2, 3],
            },
        )

        # WHEN I call `store` with the Folder object
        with patch(
            "synapseclient.models.folder.store_entity_components",
            return_value=(None),
        ) as mocked_change_meta_data, patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call:
            result = await folder.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    id=folder.id,
                ),
                set_annotations=False,
            )

            # AND we should store the annotations component
            mocked_change_meta_data.assert_called_once_with(
                root_resource=folder,
                failure_strategy=FailureStrategy.LOG_EXCEPTION,
                synapse_client=None,
            )

            # AND the folder should be stored
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_store_with_name_and_parent_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name="example_folder",
            parent_id="parent_id_value",
        )

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call:
            result = await folder.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    name=folder.name,
                    parent=folder.parent_id,
                ),
                set_annotations=False,
            )

            # AND the folder should be stored
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_store_with_name_and_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name="example_folder",
        )

        # WHEN I call `store` with the Folder object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call:
            result = await folder.store(parent=Folder(id="parent_id_value"))

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Folder(
                    name=folder.name,
                    parent=folder.parent_id,
                ),
                set_annotations=False,
            )

            # AND the folder should be stored
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_store_no_id_name_or_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder()

        # WHEN I call `store` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.store()

        # THEN we should get an error
        assert (
            str(e.value) == "The folder must have an id or a "
            "(name and (`parent_id` or parent with an id)) set."
        )

    @pytest.mark.asyncio
    async def test_store_no_id_or_name(self) -> None:
        # GIVEN a Folder object
        folder = Folder(parent_id="parent_id_value")

        # WHEN I call `store` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.store()

        # THEN we should get an error
        assert (
            str(e.value) == "The folder must have an id or a "
            "(name and (`parent_id` or parent with an id)) set."
        )

    @pytest.mark.asyncio
    async def test_store_no_id_or_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder(name="example_folder")

        # WHEN I call `store` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.store()

        # THEN we should get an error
        assert (
            str(e.value) == "The folder must have an id or a "
            "(name and (`parent_id` or parent with an id)) set."
        )

    @pytest.mark.asyncio
    async def test_get_by_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id="syn123",
        )

        # WHEN I call `get` with the Folder object
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call:
            result = await folder.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=folder.id,
            )

            # AND the folder should be stored
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_get_by_name_and_parent(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name="example_folder",
            parent_id="parent_id_value",
        )

        # WHEN I call `get` with the Folder object
        with patch.object(
            self.syn,
            "findEntityId",
            return_value=("syn123"),
        ) as mocked_client_search, patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_client_call:
            result = await folder.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=folder.id,
            )

            # AND we should search for the entity
            mocked_client_search.assert_called_once_with(
                name=folder.name,
                parent=folder.parent_id,
            )

            # AND the folder should be stored
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"

    @pytest.mark.asyncio
    async def test_get_by_name_and_parent_not_found(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            name="example_folder",
            parent_id="parent_id_value",
        )

        # WHEN I call `get` with the Folder object
        with patch.object(
            self.syn,
            "findEntityId",
            return_value=(None),
        ) as mocked_client_search:
            with pytest.raises(SynapseNotFoundError) as e:
                await folder.get()
            assert (
                str(e.value)
                == "Folder [Id: None, Name: example_folder, Parent: parent_id_value] not found in Synapse."
            )

            mocked_client_search.assert_called_once_with(
                name=folder.name,
                parent=folder.parent_id,
            )

    @pytest.mark.asyncio
    async def test_delete_with_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id="syn123",
        )

        # WHEN I call `delete` with the Folder object
        with patch.object(
            self.syn,
            "delete",
            return_value=(None),
        ) as mocked_client_call:
            await folder.delete()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=folder.id,
            )

    @pytest.mark.asyncio
    async def test_delete_missing_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder()

        # WHEN I call `delete` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.delete()

        # THEN we should get an error
        assert str(e.value) == "The folder must have an id set."

    @pytest.mark.asyncio
    async def test_copy(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id="syn123",
        )

        # AND a returned Folder object
        returned_folder = Folder(id="syn456")

        # AND a copy mapping exists
        copy_mapping = {
            "syn123": "syn456",
        }

        # WHEN I call `copy` with the Folder object
        with patch(
            "synapseclient.models.folder.copy",
            return_value=(copy_mapping),
        ) as mocked_copy, patch(
            "synapseclient.models.folder.Folder.get",
            return_value=(returned_folder),
        ) as mocked_get, patch(
            "synapseclient.models.folder.Folder.sync_from_synapse",
            return_value=(returned_folder),
        ) as mocked_sync:
            result = await folder.copy(parent_id="destination_id")

            # THEN we should call the method with this data
            mocked_copy.assert_called_once_with(
                syn=self.syn,
                entity=folder.id,
                destinationId="destination_id",
                excludeTypes=[],
                skipCopyAnnotations=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND we should call the sync method
            mocked_sync.assert_called_once_with(
                download_file=False,
                synapse_client=None,
            )

            # AND the file should be stored
            assert result.id == "syn456"

    @pytest.mark.asyncio
    async def test_copy_missing_id(self) -> None:
        # GIVEN a Folder object
        folder = Folder()

        # WHEN I call `copy` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.copy(parent_id="destination_id")

        # THEN we should get an error
        assert str(e.value) == "The folder must have an ID and parent_id to copy."

    @pytest.mark.asyncio
    async def test_copy_missing_destination(self) -> None:
        # GIVEN a Folder object
        folder = Folder(id="syn123")

        # WHEN I call `copy` with the Folder object
        with pytest.raises(ValueError) as e:
            await folder.copy(parent_id=None)

        # THEN we should get an error
        assert str(e.value) == "The folder must have an ID and parent_id to copy."

    @pytest.mark.asyncio
    async def test_sync_from_synapse(self) -> None:
        # GIVEN a Folder object
        folder = Folder(
            id="syn123",
        )

        # AND Children that exist on the folder in Synapse
        children = [
            {
                "id": "syn456",
                "type": FILE_ENTITY,
                "name": "example_file_1",
            }
        ]

        # WHEN I call `sync_from_synapse` with the Folder object
        with patch.object(
            self.syn,
            "getChildren",
            return_value=(children),
        ) as mocked_children_call, patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_folder_output()),
        ) as mocked_folder_get, patch(
            "synapseclient.models.file.File.get",
            return_value=(File(id="syn456", name="example_file_1")),
        ):
            result = await folder.sync_from_synapse()

            # THEN we should call the method with this data
            mocked_children_call.assert_called_once()

            # AND we should call the get method
            mocked_folder_get.assert_called_once()

            # AND the file/folder should be retrieved
            assert result.id == "syn123"
            assert result.name == "example_folder"
            assert result.parent_id == "parent_id_value"
            assert result.description == "This is an example folder."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.files[0].id == "syn456"
            assert result.files[0].name == "example_file_1"
