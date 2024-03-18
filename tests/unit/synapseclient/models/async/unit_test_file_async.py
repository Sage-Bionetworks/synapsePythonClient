"""Unit tests for the File model"""
import os
from typing import Dict, Union
from unittest.mock import AsyncMock, patch
import pytest
from synapseclient.models import Activity, UsedURL, File, Project
from synapseclient import File as Synapse_File
from synapseclient.core import utils

SYN_123 = "syn123"
FILE_NAME = "example_file.txt"
PATH = "/asdf/example_file.txt"
DESCRIPTION = "This is an example file."
ETAG = "etag_value"
CREATED_ON = "createdOn_value"
MODIFIED_ON = "modifiedOn_value"
CREATED_BY = "createdBy_value"
MODIFIED_BY = "modifiedBy_value"
PARENT_ID = "parent_id_value"
VERSION_LABEL = "v1"
VERSION_COMMENT = "This is version 1."
DATA_FILE_HANDLE_ID = 888
FILE_HANDLE_ID = 888
FILE_HANDLE_ETAG = "file_handle_etag_value"
FILE_HANDLE_CREATED_BY = "file_handle_createdBy_value"
FILE_HANDLE_CREATED_ON = "file_handle_createdOn_value"
FILE_HANDLE_MODIFIED_ON = "file_handle_modifiedOn_value"
FILE_HANDLE_CONCRETE_TYPE = "file_handle_concreteType_value"
FILE_HANDLE_CONTENT_TYPE = "file_handle_contentType_value"
FILE_HANDLE_CONTENT_MD5 = "file_handle_contentMd5_value"
FILE_HANDLE_CONTENT_SIZE = 123
FILE_HANDLE_FILE_NAME = "file_handle_fileName_value"
FILE_HANDLE_STORAGE_LOCATION_ID = "file_handle_storageLocationId_value"
FILE_HANDLE_STATUS = "file_handle_status_value"
FILE_HANDLE_BUCKET_NAME = "file_handle_bucketName_value"
FILE_HANDLE_KEY = "file_handle_key_value"
FILE_HANDLE_PREVIEW_ID = "file_handle_previewId_value"
FILE_HANDLE_EXTERNAL_URL = "file_handle_externalURL_value"

MODIFIED_DESCRIPTION = "This is a modified description."
ACTUAL_PARENT_ID = "syn999"

CANNOT_STORE_FILE_ERROR = "The file must have an (ID with a (path or `data_file_handle_id`)), or a (path with a (`parent_id` or parent with an id)), or a (data_file_handle_id with a (`parent_id` or parent with an id)) to store."


class TestFile:
    """Tests for the File model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def get_example_synapse_file(self) -> Synapse_File:
        return Synapse_File(
            id=SYN_123,
            name=FILE_NAME,
            path=PATH,
            description=DESCRIPTION,
            etag=ETAG,
            createdOn=CREATED_ON,
            modifiedOn=MODIFIED_ON,
            createdBy=CREATED_BY,
            contentSize=FILE_HANDLE_CONTENT_SIZE,
            contentType=FILE_HANDLE_CONTENT_TYPE,
            modifiedBy=MODIFIED_BY,
            parentId=PARENT_ID,
            versionNumber=1,
            versionLabel=VERSION_LABEL,
            versionComment=VERSION_COMMENT,
            dataFileHandleId=DATA_FILE_HANDLE_ID,
        )

    def get_example_synapse_file_output(self) -> Synapse_File:
        return Synapse_File(
            id=SYN_123,
            name=FILE_NAME,
            path=PATH,
            description=DESCRIPTION,
            etag=ETAG,
            createdOn=CREATED_ON,
            modifiedOn=MODIFIED_ON,
            createdBy=CREATED_BY,
            modifiedBy=MODIFIED_BY,
            parentId=PARENT_ID,
            versionNumber=1,
            versionLabel=VERSION_LABEL,
            versionComment=VERSION_COMMENT,
            dataFileHandleId=DATA_FILE_HANDLE_ID,
            _file_handle=self.get_example_synapse_file_handle(),
        )

    def get_example_synapse_file_handle(self) -> Dict[str, Union[str, int, bool]]:
        return {
            "id": FILE_HANDLE_ID,
            "etag": FILE_HANDLE_ETAG,
            "createdBy": FILE_HANDLE_CREATED_BY,
            "createdOn": FILE_HANDLE_CREATED_ON,
            "modifiedOn": FILE_HANDLE_MODIFIED_ON,
            "concreteType": FILE_HANDLE_CONCRETE_TYPE,
            "contentType": FILE_HANDLE_CONTENT_TYPE,
            "contentMd5": FILE_HANDLE_CONTENT_MD5,
            "fileName": FILE_HANDLE_FILE_NAME,
            "storageLocationId": FILE_HANDLE_STORAGE_LOCATION_ID,
            "contentSize": FILE_HANDLE_CONTENT_SIZE,
            "status": FILE_HANDLE_STATUS,
            "bucketName": FILE_HANDLE_BUCKET_NAME,
            "key": FILE_HANDLE_KEY,
            "previewId": FILE_HANDLE_PREVIEW_ID,
            "isPreview": True,
            "externalURL": FILE_HANDLE_EXTERNAL_URL,
        }

    @pytest.mark.asyncio
    async def test_fill_from_dict(self) -> None:
        # GIVEN an example Synapse File `get_example_synapse_file_output`
        # WHEN I call `fill_from_dict` with the example Synapse File
        file_output = File().fill_from_dict(self.get_example_synapse_file_output())

        # THEN the File object should be filled with the example Synapse File
        assert file_output.id == SYN_123
        assert file_output.name == FILE_NAME
        assert file_output.description == DESCRIPTION
        assert file_output.etag == ETAG
        assert file_output.created_on == CREATED_ON
        assert file_output.modified_on == MODIFIED_ON
        assert file_output.created_by == CREATED_BY
        assert file_output.modified_by == MODIFIED_BY
        assert file_output.parent_id == PARENT_ID
        assert file_output.version_number == 1
        assert file_output.version_label == VERSION_LABEL
        assert file_output.version_comment == VERSION_COMMENT
        assert file_output.data_file_handle_id == DATA_FILE_HANDLE_ID
        assert file_output.file_handle.id == FILE_HANDLE_ID
        assert file_output.file_handle.etag == FILE_HANDLE_ETAG
        assert file_output.file_handle.created_by == FILE_HANDLE_CREATED_BY
        assert file_output.file_handle.created_on == FILE_HANDLE_CREATED_ON
        assert file_output.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
        assert file_output.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
        assert file_output.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
        assert file_output.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
        assert file_output.file_handle.file_name == FILE_HANDLE_FILE_NAME
        assert (
            file_output.file_handle.storage_location_id
            == FILE_HANDLE_STORAGE_LOCATION_ID
        )
        assert file_output.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
        assert file_output.file_handle.status == FILE_HANDLE_STATUS
        assert file_output.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
        assert file_output.file_handle.key == FILE_HANDLE_KEY
        assert file_output.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
        assert file_output.file_handle.is_preview
        assert file_output.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_store_with_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, path=PATH, description=MODIFIED_DESCRIPTION)

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_get_call, patch(
            "synapseclient.models.file.upload_file_handle",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_handle()),
        ) as mocked_file_handle_upload, patch(
            "synapseclient.models.file.store_entity",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_store_entity:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_get_call.assert_called_once_with(
                entity=SYN_123,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=None,
                downloadFile=False,
                downloadLocation=None,
                md5=None,
            )

            # AND We should upload the file handle
            mocked_file_handle_upload.assert_called_once_with(
                syn=self.syn,
                parent_entity_id=PARENT_ID,
                path=PATH,
                synapse_store=True,
                md5=FILE_HANDLE_CONTENT_MD5,
                file_size=FILE_HANDLE_CONTENT_SIZE,
                mimetype=FILE_HANDLE_CONTENT_TYPE,
            )

            # AND We should store the entity
            mocked_store_entity.assert_called_once()

            # THEN the file should be stored
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == PATH
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_store_with_id_and_file_handle(self) -> None:
        # GIVEN an example file
        file = File(
            id=SYN_123,
            data_file_handle_id=DATA_FILE_HANDLE_ID,
            description=MODIFIED_DESCRIPTION,
        )

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_get_call, patch(
            "synapseclient.models.file.upload_file_handle",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_handle()),
        ) as mocked_file_handle_upload, patch(
            "synapseclient.models.file.store_entity",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_store_entity:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_get_call.assert_called_once_with(
                entity=SYN_123,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=None,
                downloadFile=False,
                downloadLocation=None,
                md5=None,
            )

            # AND We should not upload the file handle
            mocked_file_handle_upload.assert_not_called()

            # AND We should store the entity
            mocked_store_entity.assert_called_once()

            # THEN the file should be stored
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_store_with_parent_and_path(self) -> None:
        # GIVEN an example file
        file = File(path=PATH, description=MODIFIED_DESCRIPTION)

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_get_call, patch(
            "synapseclient.models.file.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.file.upload_file_handle",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_handle()),
        ) as mocked_file_handle_upload, patch(
            "synapseclient.models.file.store_entity",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_store_entity:
            result = await file.store_async(parent=Project(id=ACTUAL_PARENT_ID))

            # THEN we should call the method with this data
            mocked_get_call.assert_called_once_with(
                entity=PATH,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=None,
                downloadFile=False,
                downloadLocation=None,
                md5=None,
            )

            # AND We should upload the file handle
            mocked_file_handle_upload.assert_called_once_with(
                syn=self.syn,
                parent_entity_id=ACTUAL_PARENT_ID,
                path=PATH,
                synapse_store=True,
                md5=FILE_HANDLE_CONTENT_MD5,
                file_size=FILE_HANDLE_CONTENT_SIZE,
                mimetype=FILE_HANDLE_CONTENT_TYPE,
            )

            # AND We should store the entity
            mocked_store_entity.assert_called_once()

            # THEN the file should be stored
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == PATH
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_store_with_parent_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(
            path=PATH, parent_id=ACTUAL_PARENT_ID, description=MODIFIED_DESCRIPTION
        )

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_get_call, patch(
            "synapseclient.models.file.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.file.upload_file_handle",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_handle()),
        ) as mocked_file_handle_upload, patch(
            "synapseclient.models.file.store_entity",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_store_entity:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_get_call.assert_called_once_with(
                entity=PATH,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=None,
                downloadFile=False,
                downloadLocation=None,
                md5=None,
            )

            # AND We should upload the file handle
            mocked_file_handle_upload.assert_called_once_with(
                syn=self.syn,
                parent_entity_id=ACTUAL_PARENT_ID,
                path=PATH,
                synapse_store=True,
                md5=FILE_HANDLE_CONTENT_MD5,
                file_size=FILE_HANDLE_CONTENT_SIZE,
                mimetype=FILE_HANDLE_CONTENT_TYPE,
            )

            # AND We should store the entity
            mocked_store_entity.assert_called_once()

            # THEN the file should be stored
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == PATH
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_store_with_components(self) -> None:
        # GIVEN an example file
        file = File(
            path=PATH,
            parent_id=ACTUAL_PARENT_ID,
            annotations={"key": "value"},
            activity=Activity(
                name="My Activity",
                executed=[
                    UsedURL(name="Used URL", url="https://www.synapse.org/"),
                ],
            ),
            description=MODIFIED_DESCRIPTION,
        )

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_get_call, patch(
            "synapseclient.models.file.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.file.upload_file_handle",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_handle()),
        ) as mocked_file_handle_upload, patch(
            "synapseclient.models.file.store_entity",
            new_callable=AsyncMock,
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_store_entity, patch(
            "synapseclient.models.file.store_entity_components",
            return_value=True,
        ) as mocked_store_entity_components, patch.object(
            file,
            "get_async",
        ) as mocked_get:
            result = await file.store_async()

            # THEN we should call the method with this data
            mocked_get_call.assert_called_once_with(
                entity=PATH,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=None,
                downloadFile=False,
                downloadLocation=None,
                md5=None,
            )

            # AND We should upload the file handle
            mocked_file_handle_upload.assert_called_once_with(
                syn=self.syn,
                parent_entity_id=ACTUAL_PARENT_ID,
                path=PATH,
                synapse_store=True,
                md5=FILE_HANDLE_CONTENT_MD5,
                file_size=FILE_HANDLE_CONTENT_SIZE,
                mimetype=FILE_HANDLE_CONTENT_TYPE,
            )

            # AND We should store the entity
            mocked_store_entity.assert_called_once()

            # AND we should store the components
            mocked_store_entity_components.assert_called_once()

            # AND we should get the file
            mocked_get.assert_called_once()

            # THEN the file should be stored
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == PATH
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_store_id_with_no_path_or_file_handle(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, description=MODIFIED_DESCRIPTION)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async()

        # THEN we should get an error
        assert str(e.value) == CANNOT_STORE_FILE_ERROR

    @pytest.mark.asyncio
    async def test_store_path_with_no_id(self) -> None:
        # GIVEN an example file
        file = File(path=PATH, description=MODIFIED_DESCRIPTION)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async()

        # THEN we should get an error
        assert str(e.value) == CANNOT_STORE_FILE_ERROR

    @pytest.mark.asyncio
    async def test_store_id_with_parent_id(self) -> None:
        # GIVEN an example file
        file = File(
            id=SYN_123, parent_id=ACTUAL_PARENT_ID, description=MODIFIED_DESCRIPTION
        )

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async()

        # THEN we should get an error
        assert str(e.value) == CANNOT_STORE_FILE_ERROR

    @pytest.mark.asyncio
    async def test_store_id_with_parent(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, description=MODIFIED_DESCRIPTION)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.store_async(parent=Project(id=ACTUAL_PARENT_ID))

        # THEN we should get an error
        assert str(e.value) == CANNOT_STORE_FILE_ERROR

    @pytest.mark.asyncio
    async def test_change_file_metadata(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, description=MODIFIED_DESCRIPTION)

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
                entity=SYN_123,
                name="modified_file.txt",
                downloadAs="modified_file.txt",
                contentType="text/plain",
                forceVersion=True,
            )

            # THEN the file should be updated with the mock return
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_change_file_metadata_missing_id(self) -> None:
        # GIVEN an example file
        file = File(description=MODIFIED_DESCRIPTION)

        # WHEN I change the metadata on the example file
        with pytest.raises(ValueError) as e:
            await file.change_metadata_async()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID to change metadata."

    @pytest.mark.asyncio
    async def test_get_with_id(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, description=MODIFIED_DESCRIPTION)

        # AND An actual file
        bogus_file = utils.make_bogus_uuid_file()

        # AND a local file cache
        self.syn.cache.add(
            file_handle_id=DATA_FILE_HANDLE_ID,
            path=bogus_file,
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.get_async()
            os.remove(bogus_file)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=SYN_123,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=file.synapse_container_limit,
                downloadFile=file.download_file,
                downloadLocation=file.download_location,
                md5=None,
            )

            # THEN the file should be retrieved
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == bogus_file
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_get_with_path(self) -> None:
        # GIVEN an example file
        file = File(path=PATH, description=MODIFIED_DESCRIPTION)

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await file.get_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=PATH,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=file.synapse_container_limit,
                downloadFile=file.download_file,
                downloadLocation=file.download_location,
                md5=None,
            )

            # THEN the file should be stored
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == PATH
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_from_path(self) -> None:
        # GIVEN an example path
        path = PATH

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
                entity=PATH,
                version=None,
                ifcollision=default_file.if_collision,
                limitSearch=default_file.synapse_container_limit,
                downloadFile=default_file.download_file,
                downloadLocation=default_file.download_location,
                md5=None,
            )

            # THEN the file should be retrieved
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == PATH
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_from_id(self) -> None:
        # GIVEN an example id
        synapse_id = SYN_123

        # AND a default File
        default_file = File()

        # AND An actual file
        bogus_file = utils.make_bogus_uuid_file()

        # AND a local file cache
        self.syn.cache.add(
            file_handle_id=DATA_FILE_HANDLE_ID,
            path=bogus_file,
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = await File.from_id_async(synapse_id=synapse_id)
            os.remove(bogus_file)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=SYN_123,
                version=None,
                ifcollision=default_file.if_collision,
                limitSearch=default_file.synapse_container_limit,
                downloadFile=default_file.download_file,
                downloadLocation=default_file.download_location,
                md5=None,
            )

            # THEN the file should be retrieved
            assert result.id == SYN_123
            assert result.name == FILE_NAME
            assert result.path == bogus_file
            assert result.description == DESCRIPTION
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.parent_id == PARENT_ID
            assert result.version_number == 1
            assert result.version_label == VERSION_LABEL
            assert result.version_comment == VERSION_COMMENT
            assert result.data_file_handle_id == DATA_FILE_HANDLE_ID
            assert result.file_handle.id == FILE_HANDLE_ID
            assert result.file_handle.etag == FILE_HANDLE_ETAG
            assert result.file_handle.created_by == FILE_HANDLE_CREATED_BY
            assert result.file_handle.created_on == FILE_HANDLE_CREATED_ON
            assert result.file_handle.modified_on == FILE_HANDLE_MODIFIED_ON
            assert result.file_handle.concrete_type == FILE_HANDLE_CONCRETE_TYPE
            assert result.file_handle.content_type == FILE_HANDLE_CONTENT_TYPE
            assert result.file_handle.content_md5 == FILE_HANDLE_CONTENT_MD5
            assert result.file_handle.file_name == FILE_HANDLE_FILE_NAME
            assert (
                result.file_handle.storage_location_id
                == FILE_HANDLE_STORAGE_LOCATION_ID
            )
            assert result.file_handle.content_size == FILE_HANDLE_CONTENT_SIZE
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    @pytest.mark.asyncio
    async def test_get_missing_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(description=MODIFIED_DESCRIPTION)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            await file.get_async()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID or path to get."

    @pytest.mark.asyncio
    async def test_delete(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, description=MODIFIED_DESCRIPTION)

        # WHEN I delete the example file
        with patch.object(
            self.syn,
            "delete",
            return_value=(None),
        ) as mocked_client_call:
            await file.delete_async()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=SYN_123,
                version=None,
            )

    @pytest.mark.asyncio
    async def test_delete_missing_id(self) -> None:
        # GIVEN an example file
        file = File(description=MODIFIED_DESCRIPTION)

        # WHEN I delete the file
        with pytest.raises(ValueError) as e:
            await file.delete_async()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID to delete."

    @pytest.mark.asyncio
    async def test_delete_version_missing_version(self) -> None:
        # GIVEN an example file
        file = File(id="syn123", description=MODIFIED_DESCRIPTION)

        # WHEN I delete the file
        with pytest.raises(ValueError) as e:
            await file.delete_async(version_only=True)

        # THEN we should get an error
        assert (
            str(e.value) == "The file must have a version number to delete a version."
        )
