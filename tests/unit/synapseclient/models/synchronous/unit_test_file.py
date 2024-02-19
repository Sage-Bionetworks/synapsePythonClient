from unittest.mock import patch
import pytest
from synapseclient.models import Activity, UsedURL, File, Project
from synapseclient import File as Synapse_File

SYN_123 = "syn123"
FILE_NAME = "example_file"
PATH = "~/example_file.txt"
DESCRIPTION = "This is an example file."
ETAG = "etag_value"
CREATED_ON = "createdOn_value"
MODIFIED_ON = "modifiedOn_value"
CREATED_BY = "createdBy_value"
MODIFIED_BY = "modifiedBy_value"
PARENT_ID = "parent_id_value"
VERSION_LABEL = "v1"
VERSION_COMMENT = "This is version 1."
DATA_FILE_HANDLE_ID = "dataFileHandleId_value"
FILE_HANDLE_ID = "file_handle_id_value"
FILE_HANDLE_ETAG = "file_handle_etag_value"
FILE_HANDLE_CREATED_BY = "file_handle_createdBy_value"
FILE_HANDLE_CREATED_ON = "file_handle_createdOn_value"
FILE_HANDLE_MODIFIED_ON = "file_handle_modifiedOn_value"
FILE_HANDLE_CONCRETE_TYPE = "file_handle_concreteType_value"
FILE_HANDLE_CONTENT_TYPE = "file_handle_contentType_value"
FILE_HANDLE_CONTENT_MD5 = "file_handle_contentMd5_value"
FILE_HANDLE_FILE_NAME = "file_handle_fileName_value"
FILE_HANDLE_STORAGE_LOCATION_ID = "file_handle_storageLocationId_value"
FILE_HANDLE_STATUS = "file_handle_status_value"
FILE_HANDLE_BUCKET_NAME = "file_handle_bucketName_value"
FILE_HANDLE_KEY = "file_handle_key_value"
FILE_HANDLE_PREVIEW_ID = "file_handle_previewId_value"
FILE_HANDLE_EXTERNAL_URL = "file_handle_externalURL_value"


class TestFile:
    """Tests for the File model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

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
            _file_handle={
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
                "contentSize": 123,
                "status": FILE_HANDLE_STATUS,
                "bucketName": FILE_HANDLE_BUCKET_NAME,
                "key": FILE_HANDLE_KEY,
                "previewId": FILE_HANDLE_PREVIEW_ID,
                "isPreview": True,
                "externalURL": FILE_HANDLE_EXTERNAL_URL,
            },
        )

    def test_fill_from_dict(self) -> None:
        # GIVEN an example Synapse File `get_example_synapse_file_output`
        # WHEN I call `fill_from_dict` with the example Synapse File
        file_output = File().fill_from_dict(self.get_example_synapse_file_output())

        # THEN the File object should be filled with the example Synapse File
        assert file_output.id == SYN_123
        assert file_output.name == FILE_NAME
        assert file_output.path == PATH
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
        assert file_output.file_handle.content_size == 123
        assert file_output.file_handle.status == FILE_HANDLE_STATUS
        assert file_output.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
        assert file_output.file_handle.key == FILE_HANDLE_KEY
        assert file_output.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
        assert file_output.file_handle.is_preview
        assert file_output.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_store_with_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, path=PATH)

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = file.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(id=SYN_123, path=PATH),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_store_with_id_and_file_handle(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, data_file_handle_id=DATA_FILE_HANDLE_ID)

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = file.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(id=SYN_123, dataFileHandleId=DATA_FILE_HANDLE_ID),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_store_with_parent_and_path(self) -> None:
        # GIVEN an example file
        file = File(path=PATH)

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = file.store(parent=Project(id="syn999"))

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(path=PATH, parentId="syn999"),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_store_with_parent_id_and_path(self) -> None:
        # GIVEN an example file
        file = File(path=PATH, parent_id="syn999")

        # WHEN I store the example file
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = file.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(path=PATH, parentId="syn999"),
                createOrUpdate=file.create_or_update,
                forceVersion=file.force_version,
                isRestricted=file.is_restricted,
                set_annotations=False,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_store_with_components(self) -> None:
        # GIVEN an example file
        file = File(
            path=PATH,
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
            result = file.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_File(path=PATH, parentId="syn999"),
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_store_id_with_no_path_or_file_handle(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            file.store()

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    def test_store_path_with_no_id(self) -> None:
        # GIVEN an example file
        file = File(path=PATH)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            file.store()

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    def test_store_id_with_parent_id(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123, parent_id="syn999")

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            file.store()

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    def test_store_id_with_parent(self) -> None:
        # GIVEN an example file
        file = File(id=SYN_123)

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            file.store(parent=Project(id="syn999"))

        # THEN we should get an error
        assert (
            str(e.value)
            == "The file must have an (ID with a (path or `data_file_handle_id`)), or "
            "a (path with a (`parent_id` or parent with an id)) to store."
        )

    def test_change_file_metadata(self) -> None:
        # GIVEN an example file
        file = File(
            id=SYN_123,
        )

        # WHEN I change the metadata on the example file
        with patch(
            "synapseclient.models.file.changeFileMetaData",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_change_meta_data:
            result = file.change_metadata(
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_change_file_metadata_missing_id(self) -> None:
        # GIVEN an example file
        file = File()

        # WHEN I change the metadata on the example file
        with pytest.raises(ValueError) as e:
            file.change_metadata()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID to change metadata."

    def test_get_with_id(self) -> None:
        # GIVEN an example file
        file = File(
            id=SYN_123,
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = file.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=SYN_123,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=file.synapse_container_limit,
                downloadFile=file.download_file,
                downloadLocation=file.download_location,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_get_with_path(self) -> None:
        # GIVEN an example file
        file = File(
            path=PATH,
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = file.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=PATH,
                version=None,
                ifcollision=file.if_collision,
                limitSearch=file.synapse_container_limit,
                downloadFile=file.download_file,
                downloadLocation=file.download_location,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_from_path(self) -> None:
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
            result = File.from_path(path=path)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=PATH,
                version=None,
                ifcollision=default_file.if_collision,
                limitSearch=default_file.synapse_container_limit,
                downloadFile=default_file.download_file,
                downloadLocation=default_file.download_location,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_from_id(self) -> None:
        # GIVEN an example id
        synapse_id = SYN_123

        # AND a default File
        default_file = File()

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_file_output()),
        ) as mocked_client_call:
            result = File.from_id(synapse_id=synapse_id)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=SYN_123,
                version=None,
                ifcollision=default_file.if_collision,
                limitSearch=default_file.synapse_container_limit,
                downloadFile=default_file.download_file,
                downloadLocation=default_file.download_location,
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
            assert result.file_handle.content_size == 123
            assert result.file_handle.status == FILE_HANDLE_STATUS
            assert result.file_handle.bucket_name == FILE_HANDLE_BUCKET_NAME
            assert result.file_handle.key == FILE_HANDLE_KEY
            assert result.file_handle.preview_id == FILE_HANDLE_PREVIEW_ID
            assert result.file_handle.is_preview
            assert result.file_handle.external_url == FILE_HANDLE_EXTERNAL_URL

    def test_get_missing_id_and_path(self) -> None:
        # GIVEN an example file
        file = File()

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            file.get()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID or path to get."

    def test_delete(self) -> None:
        # GIVEN an example file
        file = File(
            id=SYN_123,
        )

        # WHEN I get the example file
        with patch.object(
            self.syn,
            "delete",
            return_value=(None),
        ) as mocked_client_call:
            file.delete()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=SYN_123,
            )

    def test_delete_missing_id(self) -> None:
        # GIVEN an example file
        file = File()

        # WHEN I get the file
        with pytest.raises(ValueError) as e:
            file.delete()

        # THEN we should get an error
        assert str(e.value) == "The file must have an ID to delete."
