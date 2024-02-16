from unittest.mock import patch
import pytest
from synapseclient.models import Activity, UsedURL, File, Project
from synapseclient import File as Synapse_File


class TestFile:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def get_example_synapse_file_output(self) -> Synapse_File:
        return Synapse_File(
            id="syn123",
            name="example_file",
            path="~/example_file.txt",
            description="This is an example file.",
            etag="etag_value",
            createdOn="createdOn_value",
            modifiedOn="modifiedOn_value",
            createdBy="createdBy_value",
            modifiedBy="modifiedBy_value",
            parentId="parent_id_value",
            versionNumber=1,
            versionLabel="v1",
            versionComment="This is version 1.",
            dataFileHandleId="dataFileHandleId_value",
            _file_handle={
                "id": "file_handle_id_value",
                "etag": "file_handle_etag_value",
                "createdBy": "file_handle_createdBy_value",
                "createdOn": "file_handle_createdOn_value",
                "modifiedOn": "file_handle_modifiedOn_value",
                "concreteType": "file_handle_concreteType_value",
                "contentType": "file_handle_contentType_value",
                "contentMd5": "file_handle_contentMd5_value",
                "fileName": "file_handle_fileName_value",
                "storageLocationId": "file_handle_storageLocationId_value",
                "contentSize": 123,
                "status": "file_handle_status_value",
                "bucketName": "file_handle_bucketName_value",
                "key": "file_handle_key_value",
                "previewId": "file_handle_previewId_value",
                "isPreview": True,
                "externalURL": "file_handle_externalURL_value",
            },
        )

    @pytest.mark.asyncio
    async def test_fill_from_dict(self) -> None:
        # GIVEN an example Synapse File `get_example_synapse_file_output`
        # WHEN I call `fill_from_dict` with the example Synapse File
        file_output = File().fill_from_dict(self.get_example_synapse_file_output())

        # THEN the File object should be filled with the example Synapse File
        assert file_output.id == "syn123"
        assert file_output.name == "example_file"
        assert file_output.path == "~/example_file.txt"
        assert file_output.description == "This is an example file."
        assert file_output.etag == "etag_value"
        assert file_output.created_on == "createdOn_value"
        assert file_output.modified_on == "modifiedOn_value"
        assert file_output.created_by == "createdBy_value"
        assert file_output.modified_by == "modifiedBy_value"
        assert file_output.parent_id == "parent_id_value"
        assert file_output.version_number == 1
        assert file_output.version_label == "v1"
        assert file_output.version_comment == "This is version 1."
        assert file_output.data_file_handle_id == "dataFileHandleId_value"
        assert file_output.file_handle.id == "file_handle_id_value"
        assert file_output.file_handle.etag == "file_handle_etag_value"
        assert file_output.file_handle.created_by == "file_handle_createdBy_value"
        assert file_output.file_handle.created_on == "file_handle_createdOn_value"
        assert file_output.file_handle.modified_on == "file_handle_modifiedOn_value"
        assert file_output.file_handle.concrete_type == "file_handle_concreteType_value"
        assert file_output.file_handle.content_type == "file_handle_contentType_value"
        assert file_output.file_handle.content_md5 == "file_handle_contentMd5_value"
        assert file_output.file_handle.file_name == "file_handle_fileName_value"
        assert (
            file_output.file_handle.storage_location_id
            == "file_handle_storageLocationId_value"
        )
        assert file_output.file_handle.content_size == 123
        assert file_output.file_handle.status == "file_handle_status_value"
        assert file_output.file_handle.bucket_name == "file_handle_bucketName_value"
        assert file_output.file_handle.key == "file_handle_key_value"
        assert file_output.file_handle.preview_id == "file_handle_previewId_value"
        assert file_output.file_handle.is_preview
        assert file_output.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_store_with_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(id="syn123", path="~/example_file.txt")

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(id="syn123", path="~/example_file.txt"),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_store_with_id_and_file_handle(self) -> None:
        # GIVEN an example file
        file = File(id="syn123", data_file_handle_id="dataFileHandleId_value")

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(
                    id="syn123", dataFileHandleId="dataFileHandleId_value"
                ),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_store_with_parent_and_path(self) -> None:
        # GIVEN an example file
        file = File(path="~/example_file.txt")

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.store_async(parent=Project(id="syn999"))

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(path="~/example_file.txt", parentId="syn999"),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_store_with_parent_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(path="~/example_file.txt", parent_id="syn999")

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(path="~/example_file.txt", parentId="syn999"),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_store_with_components(self) -> None:
        # GIVEN an example file
        file = File(
            path="~/example_file.txt",
            parent_id="syn999",
            annotations={"key": "value"},
            activity=Activity(
                name="My Activity",
                executed=[
                    UsedURL(name="Used URL", url="https://www.synapse.org/"),
                ],
            ),
        )

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call, patch(
            "synapseclient.models.file.store_entity_components",
            return_value=True,
        ) as mocked_store_entity_components, patch.object(
            file,
            "get_async",
        ) as mocked_get:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(path="~/example_file.txt", parentId="syn999"),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
            )

            # AND we should store the components
            mocked_store_entity_components.assert_called_once()

            # AND we should get the file
            mocked_get.assert_called_once()

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_store_id_with_no_path_or_file_handle(self) -> None:
        # GIVEN an example file
        file = File(id="syn123")

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async()

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    @pytest.mark.asyncio
    async def test_store_path_with_no_id(self) -> None:
        # GIVEN an example file
        file = File(path="~/example_file.txt")

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async()

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    @pytest.mark.asyncio
    async def test_store_id_with_parent_id(self) -> None:
        # GIVEN an example file
        file = File(id="syn123", parent_id="syn999")

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async()

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    @pytest.mark.asyncio
    async def test_store_id_with_parent(self) -> None:
        # GIVEN an example file
        file = File(id="syn123")

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async(parent=Project(id="syn999"))

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    @pytest.mark.asyncio
    async def test_change_file_metadata(self) -> None:
        # GIVEN an example file
        file = File(
            id="syn123",
        )

        # WHEN I change the metadata on the example file
        with patch(
            "synapseclient.models.file.changeFileMetaData",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_change_meta_data:
            result = await file.change_metadata_async(
                name="modified_file.txt",
                download_as="modified_file.txt",
                content_type="text/plain",
            )

            # THEN we should call the method with this data
            mocked_change_meta_data.assert_called_once_with(
                syn=self.syn,
                entity="syn123",
                name="modified_file.txt",
                downloadAs="modified_file.txt",
                contentType="text/plain",
                forceVersion=True,
            )

            # THEN the file should be updated with the mock return
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_change_file_metadata_missing_id(self) -> None:
        # GIVEN an example file
        file = File()

        # WHEN I change the metadata on the example file
        with pytest.raises(ValueError) as e:
            await file.change_metadata_async()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID to change metadata."

    @pytest.mark.asyncio
    async def test_get_with_id(self) -> None:
        # GIVEN an example file
        file = File(
            id="syn123",
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.get_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity="syn123",
                version=None,
                ifcollision=file.if_collision,
                limitSearch=file.synapse_container_limit,
                downloadFile=file.download_file,
                downloadLocation=file.download_location,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_get_with_path(self) -> None:
        # GIVEN an example file
        file = File(
            path="~/example_file.txt",
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.get_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity="~/example_file.txt",
                version=None,
                ifcollision=file.if_collision,
                limitSearch=file.synapse_container_limit,
                downloadFile=file.download_file,
                downloadLocation=file.download_location,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_from_path(self) -> None:
        # GIVEN an example path
        path = "~/example_file.txt"

        # AND a default File
        default_file = File()

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await File.from_path_async(path=path)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity="~/example_file.txt",
                version=None,
                ifcollision=default_file.if_collision,
                limitSearch=default_file.synapse_container_limit,
                downloadFile=default_file.download_file,
                downloadLocation=default_file.download_location,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_from_id(self) -> None:
        # GIVEN an example id
        synapse_id = "syn123"

        # AND a default File
        default_file = File()

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await File.from_id_async(synapse_id=synapse_id)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity="syn123",
                version=None,
                ifcollision=default_file.if_collision,
                limitSearch=default_file.synapse_container_limit,
                downloadFile=default_file.download_file,
                downloadLocation=default_file.download_location,
            )

            # THEN the file should be stored
            assert result.id == "syn123"
            assert result.name == "example_file"
            assert result.path == "~/example_file.txt"
            assert result.description == "This is an example file."
            assert result.etag == "etag_value"
            assert result.created_on == "createdOn_value"
            assert result.modified_on == "modifiedOn_value"
            assert result.created_by == "createdBy_value"
            assert result.modified_by == "modifiedBy_value"
            assert result.parent_id == "parent_id_value"
            assert result.version_number == 1
            assert result.version_label == "v1"
            assert result.version_comment == "This is version 1."
            assert result.data_file_handle_id == "dataFileHandleId_value"
            assert result.file_handle.id == "file_handle_id_value"
            assert result.file_handle.etag == "file_handle_etag_value"
            assert result.file_handle.created_by == "file_handle_createdBy_value"
            assert result.file_handle.created_on == "file_handle_createdOn_value"
            assert result.file_handle.modified_on == "file_handle_modifiedOn_value"
            assert result.file_handle.concrete_type == "file_handle_concreteType_value"
            assert result.file_handle.content_type == "file_handle_contentType_value"
            assert result.file_handle.content_md5 == "file_handle_contentMd5_value"
            assert result.file_handle.file_name == "file_handle_fileName_value"
            assert (
                result.file_handle.storage_location_id
                == "file_handle_storageLocationId_value"
            )
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == "file_handle_status_value"
            assert result.file_handle.bucket_name == "file_handle_bucketName_value"
            assert result.file_handle.key == "file_handle_key_value"
            assert result.file_handle.preview_id == "file_handle_previewId_value"
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == "file_handle_externalURL_value"

    @pytest.mark.asyncio
    async def test_get_missing_id_and_path(self) -> None:
        # GIVEN an example file
        file = File()

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.get_async()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID or path to get."

    @pytest.mark.asyncio
    async def test_delete(self) -> None:
        # GIVEN an example file
        file = File(
            id="syn123",
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "delete",
            return_value=(None),
        ) as mocked_client_call:
            await file.delete_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj="syn123",
            )

    @pytest.mark.asyncio
    async def test_delete_missing_id(self) -> None:
        # GIVEN an example file
        file = File()

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.delete_async()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID to delete."
