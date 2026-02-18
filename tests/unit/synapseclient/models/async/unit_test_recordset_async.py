"""Unit tests for the RecordSet model."""

import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.models import Activity, RecordSet
from synapseclient.models.recordset import ValidationSummary

SYN_123 = "syn123"
SYN_456 = "syn456"
RECORD_SET_NAME = "test_record_set.csv"
PATH = "/tmp/test_record_set.csv"
DOWNLOAD_DIR = "/tmp/download_dir"
DESCRIPTION = "A test record set"
ETAG = "etag-abc-123"
CREATED_ON = "2024-01-01T00:00:00.000Z"
MODIFIED_ON = "2024-01-02T00:00:00.000Z"
CREATED_BY = "111111"
MODIFIED_BY = "222222"
PARENT_ID = "syn999"
VERSION_LABEL = "v1"
VERSION_COMMENT = "Initial version"
DATA_FILE_HANDLE_ID = "888"
VALIDATION_FILE_HANDLE_ID = "999"
VERSION_NUMBER = 1
CONTENT_MD5 = "abc123md5"


def _get_record_set_entity_response(**overrides):
    """Return a mock RecordSet entity response from the REST API."""
    response = {
        "id": SYN_123,
        "name": RECORD_SET_NAME,
        "description": DESCRIPTION,
        "etag": ETAG,
        "createdOn": CREATED_ON,
        "modifiedOn": MODIFIED_ON,
        "createdBy": CREATED_BY,
        "modifiedBy": MODIFIED_BY,
        "parentId": PARENT_ID,
        "versionNumber": VERSION_NUMBER,
        "versionLabel": VERSION_LABEL,
        "versionComment": VERSION_COMMENT,
        "isLatestVersion": True,
        "dataFileHandleId": DATA_FILE_HANDLE_ID,
        "concreteType": concrete_types.RECORD_SET_ENTITY,
        "validationFileHandleId": VALIDATION_FILE_HANDLE_ID,
    }
    response.update(overrides)
    return response


class TestRecordSet:
    """Unit tests for the RecordSet model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self) -> None:
        # GIVEN a RecordSet entity response
        entity = _get_record_set_entity_response()

        # WHEN I fill a RecordSet from the entity response
        record_set = RecordSet()
        record_set.fill_from_dict(entity)

        # THEN all fields should be populated correctly
        assert record_set.id == SYN_123
        assert record_set.name == RECORD_SET_NAME
        assert record_set.description == DESCRIPTION
        assert record_set.etag == ETAG
        assert record_set.created_on == CREATED_ON
        assert record_set.modified_on == MODIFIED_ON
        assert record_set.created_by == CREATED_BY
        assert record_set.modified_by == MODIFIED_BY
        assert record_set.parent_id == PARENT_ID
        assert record_set.version_number == VERSION_NUMBER
        assert record_set.version_label == VERSION_LABEL
        assert record_set.version_comment == VERSION_COMMENT
        assert record_set.is_latest_version is True
        assert record_set.data_file_handle_id == DATA_FILE_HANDLE_ID
        assert record_set.validation_file_handle_id == VALIDATION_FILE_HANDLE_ID

    def test_fill_from_dict_with_validation_summary(self) -> None:
        # GIVEN a RecordSet entity response with validation summary
        entity = _get_record_set_entity_response(
            validationSummary={
                "containerId": SYN_123,
                "totalNumberOfChildren": 10,
                "numberOfValidChildren": 7,
                "numberOfInvalidChildren": 2,
                "numberOfUnknownChildren": 1,
                "generatedOn": "2024-05-01T00:00:00.000Z",
            }
        )

        # WHEN I fill a RecordSet from the entity response
        record_set = RecordSet()
        record_set.fill_from_dict(entity)

        # THEN the validation_summary should be populated
        assert record_set.validation_summary is not None
        assert record_set.validation_summary.container_id == SYN_123
        assert record_set.validation_summary.total_number_of_children == 10
        assert record_set.validation_summary.number_of_valid_children == 7
        assert record_set.validation_summary.number_of_invalid_children == 2
        assert record_set.validation_summary.number_of_unknown_children == 1

    def test_fill_from_dict_with_upsert_keys(self) -> None:
        # GIVEN a RecordSet entity response with upsert keys
        entity = _get_record_set_entity_response(upsertKey=["col_a", "col_b"])

        # WHEN I fill a RecordSet from the entity response
        record_set = RecordSet()
        record_set.fill_from_dict(entity)

        # THEN the upsert_keys should be populated
        assert record_set.upsert_keys == ["col_a", "col_b"]

    def test_to_synapse_request(self) -> None:
        # GIVEN a RecordSet with fields set
        record_set = RecordSet(
            id=SYN_123,
            name=RECORD_SET_NAME,
            description=DESCRIPTION,
            parent_id=PARENT_ID,
            etag=ETAG,
            data_file_handle_id=DATA_FILE_HANDLE_ID,
            version_label=VERSION_LABEL,
        )

        # WHEN I convert it to a Synapse request
        request = record_set.to_synapse_request()

        # THEN the request should contain the correct values
        assert request["concreteType"] == concrete_types.RECORD_SET_ENTITY
        assert request["id"] == SYN_123
        assert request["name"] == RECORD_SET_NAME
        assert request["description"] == DESCRIPTION
        assert request["parentId"] == PARENT_ID
        assert request["etag"] == ETAG
        assert request["dataFileHandleId"] == DATA_FILE_HANDLE_ID
        assert request["versionLabel"] == VERSION_LABEL

    def test_to_synapse_request_with_validation_summary(self) -> None:
        # GIVEN a RecordSet with a validation_summary
        record_set = RecordSet(
            id=SYN_123,
            name=RECORD_SET_NAME,
            parent_id=PARENT_ID,
            validation_summary=ValidationSummary(
                container_id=SYN_123,
                total_number_of_children=5,
                number_of_valid_children=4,
                number_of_invalid_children=1,
            ),
        )

        # WHEN I convert it to a request
        request = record_set.to_synapse_request()

        # THEN the validation summary should be included
        assert "validationSummary" in request
        assert request["validationSummary"]["containerId"] == SYN_123
        assert request["validationSummary"]["totalNumberOfChildren"] == 5

    def test_cannot_store_no_id_no_path_no_parent(self) -> None:
        # GIVEN a RecordSet with no id, path, or parent
        record_set = RecordSet(name=RECORD_SET_NAME)

        # WHEN I check _cannot_store
        # THEN it should be True
        assert record_set._cannot_store() is True

    def test_cannot_store_with_path_and_parent(self) -> None:
        # GIVEN a RecordSet with a path and parent_id
        record_set = RecordSet(path=PATH, parent_id=PARENT_ID)

        # WHEN I check _cannot_store
        # THEN it should be False
        assert record_set._cannot_store() is False

    def test_cannot_store_with_id_and_path(self) -> None:
        # GIVEN a RecordSet with an id and path
        record_set = RecordSet(id=SYN_123, path=PATH)

        # WHEN I check _cannot_store
        # THEN it should be False
        assert record_set._cannot_store() is False

    def test_cannot_store_with_id_and_data_file_handle_id(self) -> None:
        # GIVEN a RecordSet with an id and data_file_handle_id
        record_set = RecordSet(id=SYN_123, data_file_handle_id=DATA_FILE_HANDLE_ID)

        # WHEN I check _cannot_store
        # THEN it should be False
        assert record_set._cannot_store() is False

    def test_cannot_store_with_parent_and_data_file_handle_id(self) -> None:
        # GIVEN a RecordSet with a parent_id and data_file_handle_id
        record_set = RecordSet(
            parent_id=PARENT_ID, data_file_handle_id=DATA_FILE_HANDLE_ID
        )

        # WHEN I check _cannot_store
        # THEN it should be False
        assert record_set._cannot_store() is False

    def test_has_changed_true_initially(self) -> None:
        # GIVEN a new RecordSet
        record_set = RecordSet(id=SYN_123, name=RECORD_SET_NAME)

        # WHEN I check has_changed before any persistent instance
        # THEN it should be True
        assert record_set.has_changed is True

    def test_has_changed_false_after_set(self) -> None:
        # GIVEN a RecordSet with a persistent instance set
        record_set = RecordSet(id=SYN_123, name=RECORD_SET_NAME)
        record_set._set_last_persistent_instance()

        # WHEN I check has_changed without modification
        # THEN it should be False
        assert record_set.has_changed is False

    def test_has_changed_true_after_modification(self) -> None:
        # GIVEN a RecordSet with a persistent instance set
        record_set = RecordSet(id=SYN_123, name=RECORD_SET_NAME)
        record_set._set_last_persistent_instance()

        # WHEN I modify the record set
        record_set.description = "Changed description"

        # THEN has_changed should be True
        assert record_set.has_changed is True

    def test_determine_fields_to_ignore_default(self) -> None:
        # GIVEN a RecordSet with default settings
        record_set = RecordSet()

        # WHEN I determine fields to ignore
        result = record_set._determine_fields_to_ignore_in_merge()

        # THEN annotations should not be ignored (merge_existing_annotations=True)
        assert "annotations" not in result
        # AND activity should be ignored (associate_activity_to_new_version=False)
        assert "activity" in result

    def test_determine_fields_to_ignore_no_merge_annotations(self) -> None:
        # GIVEN a RecordSet with merge_existing_annotations=False
        record_set = RecordSet(merge_existing_annotations=False)

        # WHEN I determine fields to ignore
        result = record_set._determine_fields_to_ignore_in_merge()

        # THEN annotations should be in the ignore list
        assert "annotations" in result

    def test_determine_fields_to_ignore_with_activity_association(self) -> None:
        # GIVEN a RecordSet with associate_activity_to_new_version=True
        record_set = RecordSet(associate_activity_to_new_version=True)

        # WHEN I determine fields to ignore
        result = record_set._determine_fields_to_ignore_in_merge()

        # THEN activity should NOT be in the ignore list
        assert "activity" not in result

    async def test_store_async_with_path_and_parent(self) -> None:
        # GIVEN a new RecordSet with a path and parent_id
        record_set = RecordSet(
            path=PATH,
            parent_id=PARENT_ID,
            name=RECORD_SET_NAME,
            description=DESCRIPTION,
        )

        entity_response = _get_record_set_entity_response()

        # Mock the parallel file transfer semaphore
        mock_semaphore = MagicMock()

        @asynccontextmanager
        async def mock_semaphore_ctx(*args, **kwargs):
            yield mock_semaphore

        self.syn._get_parallel_file_transfer_semaphore = mock_semaphore_ctx

        # WHEN I call store_async
        with patch(
            "synapseclient.models.recordset.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.file._upload_file",
            new_callable=AsyncMock,
        ) as mock_upload, patch(
            "synapseclient.models.recordset.store_entity",
            new_callable=AsyncMock,
            return_value=entity_response,
        ) as mock_store_entity, patch(
            "synapseclient.models.recordset.store_entity_components",
            new_callable=AsyncMock,
            return_value=False,
        ), patch(
            "os.path.expanduser",
            return_value=PATH,
        ):
            result = await record_set.store_async(synapse_client=self.syn)

            # THEN the upload function should be called
            mock_upload.assert_called_once()

            # AND the store_entity function should be called
            mock_store_entity.assert_called_once()

            # AND the result should be populated from the response
            assert result.id == SYN_123
            assert result.name == RECORD_SET_NAME

    async def test_store_async_with_data_file_handle_id(self) -> None:
        # GIVEN a RecordSet with a data_file_handle_id and parent_id
        record_set = RecordSet(
            data_file_handle_id=DATA_FILE_HANDLE_ID,
            parent_id=PARENT_ID,
            name=RECORD_SET_NAME,
        )

        entity_response = _get_record_set_entity_response()

        # Mock cache.get to return None (no cached path)
        self.syn.cache = MagicMock()
        self.syn.cache.get.return_value = None

        # WHEN I call store_async
        with patch(
            "synapseclient.models.recordset.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.recordset.store_entity",
            new_callable=AsyncMock,
            return_value=entity_response,
        ) as mock_store_entity, patch(
            "synapseclient.models.recordset.store_entity_components",
            new_callable=AsyncMock,
            return_value=False,
        ):
            result = await record_set.store_async(synapse_client=self.syn)

            # THEN store_entity should be called (no file upload needed)
            mock_store_entity.assert_called_once()

            # AND the result should be populated
            assert result.id == SYN_123

    async def test_store_async_with_parent_object(self) -> None:
        # GIVEN a RecordSet with a path but parent passed as argument
        record_set = RecordSet(
            path=PATH,
            name=RECORD_SET_NAME,
        )

        entity_response = _get_record_set_entity_response()

        # Create a mock parent
        from synapseclient.models import Folder

        parent = Folder(id=PARENT_ID)

        # Mock semaphore
        mock_semaphore = MagicMock()

        @asynccontextmanager
        async def mock_semaphore_ctx(*args, **kwargs):
            yield mock_semaphore

        self.syn._get_parallel_file_transfer_semaphore = mock_semaphore_ctx

        # WHEN I call store_async with a parent object
        with patch(
            "synapseclient.models.recordset.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.file._upload_file",
            new_callable=AsyncMock,
        ), patch(
            "synapseclient.models.recordset.store_entity",
            new_callable=AsyncMock,
            return_value=entity_response,
        ), patch(
            "synapseclient.models.recordset.store_entity_components",
            new_callable=AsyncMock,
            return_value=False,
        ), patch(
            "os.path.expanduser",
            return_value=PATH,
        ):
            result = await record_set.store_async(
                parent=parent, synapse_client=self.syn
            )

            # THEN the parent_id should be set from the parent object
            assert result.parent_id == PARENT_ID

    async def test_store_async_cannot_store_raises(self) -> None:
        # GIVEN a RecordSet that cannot be stored (missing required info)
        record_set = RecordSet(name=RECORD_SET_NAME)

        # WHEN I call store_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have"):
            await record_set.store_async(synapse_client=self.syn)

    async def test_store_async_update_existing(self) -> None:
        # GIVEN a RecordSet that has been retrieved from Synapse
        record_set = RecordSet(
            id=SYN_123,
            path=PATH,
            parent_id=PARENT_ID,
            name=RECORD_SET_NAME,
            description="Updated description",
            create_or_update=True,
        )

        existing_entity_response = _get_record_set_entity_response()
        updated_entity_response = _get_record_set_entity_response(
            description="Updated description"
        )

        # Mock the semaphore
        mock_semaphore = MagicMock()

        @asynccontextmanager
        async def mock_semaphore_ctx(*args, **kwargs):
            yield mock_semaphore

        self.syn._get_parallel_file_transfer_semaphore = mock_semaphore_ctx

        # WHEN I call store_async and an existing entity is found
        with patch(
            "synapseclient.models.recordset.get_id",
            new_callable=AsyncMock,
            return_value=SYN_123,
        ), patch(
            "synapseclient.api.entity_factory.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value={"entity": existing_entity_response, "fileHandles": []},
        ), patch(
            "synapseclient.models.file._upload_file",
            new_callable=AsyncMock,
        ), patch(
            "synapseclient.models.recordset.store_entity",
            new_callable=AsyncMock,
            return_value=updated_entity_response,
        ), patch(
            "synapseclient.models.recordset.store_entity_components",
            new_callable=AsyncMock,
            return_value=False,
        ), patch(
            "os.path.expanduser",
            return_value=PATH,
        ):
            result = await record_set.store_async(synapse_client=self.syn)

            # THEN the result should have the merged/updated fields
            assert result.id == SYN_123

    async def test_store_async_re_read_required(self) -> None:
        # GIVEN a RecordSet that triggers a re-read after store
        record_set = RecordSet(
            data_file_handle_id=DATA_FILE_HANDLE_ID,
            parent_id=PARENT_ID,
            name=RECORD_SET_NAME,
        )

        entity_response = _get_record_set_entity_response()

        self.syn.cache = MagicMock()
        self.syn.cache.get.return_value = None

        # WHEN I call store_async and store_entity_components returns True
        with patch(
            "synapseclient.models.recordset.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "synapseclient.models.recordset.store_entity",
            new_callable=AsyncMock,
            return_value=entity_response,
        ), patch(
            "synapseclient.models.recordset.store_entity_components",
            new_callable=AsyncMock,
            return_value=True,
        ), patch.object(
            RecordSet,
            "get_async",
            new_callable=AsyncMock,
            return_value=record_set,
        ) as mock_get:
            result = await record_set.store_async(synapse_client=self.syn)

            # THEN get_async should be called again to re-read the entity
            mock_get.assert_called_once()

    async def test_get_async_with_id(self) -> None:
        # GIVEN a RecordSet with an id
        record_set = RecordSet(id=SYN_123)

        entity_response = _get_record_set_entity_response()

        # WHEN I call get_async
        with patch(
            "synapseclient.models.recordset.get_from_entity_factory",
            new_callable=AsyncMock,
        ) as mock_factory:
            # Mock the entity factory to fill the record set
            async def side_effect(**kwargs):
                entity_to_update = kwargs["entity_to_update"]
                entity_to_update.fill_from_dict(entity_response)

            mock_factory.side_effect = side_effect

            self.syn.cache = MagicMock()
            self.syn.cache.get.return_value = None

            result = await record_set.get_async(synapse_client=self.syn)

            # THEN the entity factory should be called
            mock_factory.assert_called_once()

            # AND the result should be populated
            assert result.id == SYN_123
            assert result.name == RECORD_SET_NAME

    async def test_get_async_with_path(self) -> None:
        # GIVEN a RecordSet with a path (looking up by local file)
        record_set = RecordSet(path=PATH)

        entity_response = _get_record_set_entity_response()

        # WHEN I call get_async
        with patch(
            "synapseclient.models.recordset.get_from_entity_factory",
            new_callable=AsyncMock,
        ) as mock_factory, patch(
            "os.path.isfile",
            return_value=False,
        ):

            async def side_effect(**kwargs):
                entity_to_update = kwargs["entity_to_update"]
                entity_to_update.fill_from_dict(entity_response)

            mock_factory.side_effect = side_effect

            self.syn.cache = MagicMock()
            self.syn.cache.get.return_value = None

            result = await record_set.get_async(synapse_client=self.syn)

            # THEN the entity factory should be called with the path
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args[1]
            assert call_kwargs["synapse_id_or_path"] == PATH

    async def test_get_async_without_id_or_path_raises(self) -> None:
        # GIVEN a RecordSet with no id or path
        record_set = RecordSet()

        # WHEN I call get_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have an ID or path"):
            await record_set.get_async(synapse_client=self.syn)

    async def test_get_async_include_activity(self) -> None:
        # GIVEN a RecordSet with an id
        record_set = RecordSet(id=SYN_123)

        entity_response = _get_record_set_entity_response()

        activity_response = Activity(
            id="act123",
            name="Test Activity",
        )

        # WHEN I call get_async with include_activity=True
        with patch(
            "synapseclient.models.recordset.get_from_entity_factory",
            new_callable=AsyncMock,
        ) as mock_factory, patch(
            "synapseclient.models.Activity.from_parent_async",
            new_callable=AsyncMock,
            return_value=activity_response,
        ) as mock_from_parent:

            async def side_effect(**kwargs):
                entity_to_update = kwargs["entity_to_update"]
                entity_to_update.fill_from_dict(entity_response)

            mock_factory.side_effect = side_effect

            self.syn.cache = MagicMock()
            self.syn.cache.get.return_value = None

            result = await record_set.get_async(
                include_activity=True, synapse_client=self.syn
            )

            # THEN the activity should be retrieved
            mock_from_parent.assert_called_once()

            # AND the result should have the activity
            assert result.activity is not None
            assert result.activity.name == "Test Activity"

    async def test_get_async_uses_cache_path(self) -> None:
        # GIVEN a RecordSet with an id and data_file_handle_id
        record_set = RecordSet(id=SYN_123)

        entity_response = _get_record_set_entity_response()
        cached_path = "/cached/path/to/file.csv"

        # WHEN I call get_async and cache has the file
        with patch(
            "synapseclient.models.recordset.get_from_entity_factory",
            new_callable=AsyncMock,
        ) as mock_factory:

            async def side_effect(**kwargs):
                entity_to_update = kwargs["entity_to_update"]
                entity_to_update.fill_from_dict(entity_response)
                # Simulate the factory setting data_file_handle_id but no path
                entity_to_update.path = None

            mock_factory.side_effect = side_effect

            self.syn.cache = MagicMock()
            self.syn.cache.get.return_value = cached_path

            result = await record_set.get_async(synapse_client=self.syn)

            # THEN the cache should be checked
            self.syn.cache.get.assert_called_once_with(
                file_handle_id=DATA_FILE_HANDLE_ID
            )

            # AND the path should come from cache
            assert result.path == cached_path

    async def test_delete_async_with_id(self) -> None:
        # GIVEN a RecordSet with an id
        record_set = RecordSet(id=SYN_123)

        # WHEN I call delete_async
        with patch.object(
            self.syn,
            "delete",
            return_value=None,
        ) as mock_delete:
            await record_set.delete_async(synapse_client=self.syn)

            # THEN the Synapse delete should be called with the id
            mock_delete.assert_called_once_with(obj=SYN_123, version=None)

    async def test_delete_async_without_id_raises(self) -> None:
        # GIVEN a RecordSet without an id
        record_set = RecordSet()

        # WHEN I call delete_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have an ID to delete"):
            await record_set.delete_async(synapse_client=self.syn)

    async def test_delete_async_version_only(self) -> None:
        # GIVEN a RecordSet with an id and version_number
        record_set = RecordSet(id=SYN_123, version_number=2)

        # WHEN I call delete_async with version_only=True
        with patch.object(
            self.syn,
            "delete",
            return_value=None,
        ) as mock_delete:
            await record_set.delete_async(version_only=True, synapse_client=self.syn)

            # THEN the Synapse delete should be called with the version
            mock_delete.assert_called_once_with(obj=SYN_123, version=2)

    async def test_delete_async_version_only_no_version_raises(self) -> None:
        # GIVEN a RecordSet with an id but no version_number
        record_set = RecordSet(id=SYN_123)

        # WHEN I call delete_async with version_only=True
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="must have a version number to delete a version"
        ):
            await record_set.delete_async(version_only=True, synapse_client=self.syn)

    async def test_get_detailed_validation_results_async_with_handle_id(
        self,
    ) -> None:
        # GIVEN a RecordSet with a validation_file_handle_id
        record_set = RecordSet(
            id=SYN_123, validation_file_handle_id=VALIDATION_FILE_HANDLE_ID
        )

        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=10)
        cached_path = "/cached/validation_results.csv"

        self.syn.cache = MagicMock()
        self.syn.cache.get.return_value = cached_path
        self.syn.cache.get_cache_dir.return_value = "/syn_cache_dir"

        # WHEN I call get_detailed_validation_results_async
        with patch(
            "synapseclient.models.recordset.test_import_pandas",
        ), patch(
            "synapseclient.models.recordset.download_by_file_handle",
            new_callable=AsyncMock,
            return_value="/cached/validation_results.csv",
        ) as mock_download, patch(
            "pandas.read_csv",
            return_value=mock_df,
        ) as mock_read_csv:
            result = await record_set.get_detailed_validation_results_async(
                synapse_client=self.syn
            )

            # THEN the download function should be called
            mock_download.assert_called_once()

            # AND pandas should read the CSV
            mock_read_csv.assert_called_once_with("/cached/validation_results.csv")

            # AND the result should be the DataFrame
            assert result == mock_df

    async def test_get_detailed_validation_results_async_no_handle_id(self) -> None:
        # GIVEN a RecordSet without a validation_file_handle_id
        record_set = RecordSet(id=SYN_123)

        # WHEN I call get_detailed_validation_results_async
        with patch(
            "synapseclient.models.recordset.test_import_pandas",
        ), patch(
            "pandas.read_csv",
        ):
            result = await record_set.get_detailed_validation_results_async(
                synapse_client=self.syn
            )

            # THEN the result should be None
            assert result is None

    async def test_get_detailed_validation_results_async_download_location(
        self,
    ) -> None:
        # GIVEN a RecordSet with a validation_file_handle_id
        record_set = RecordSet(
            id=SYN_123, validation_file_handle_id=VALIDATION_FILE_HANDLE_ID
        )

        mock_df = MagicMock()
        download_location = "/custom/download/dir"

        self.syn.cache = MagicMock()
        self.syn.cache.get.return_value = None
        self.syn.cache.get_cache_dir.return_value = "/syn_cache_dir"

        # WHEN I call get_detailed_validation_results_async with a download_location
        with patch(
            "synapseclient.models.recordset.test_import_pandas",
        ), patch(
            "synapseclient.models.recordset.ensure_download_location_is_directory",
            return_value=download_location,
        ), patch(
            "synapseclient.models.recordset.download_by_file_handle",
            new_callable=AsyncMock,
            return_value=f"{download_location}/SYNAPSE_RECORDSET_VALIDATION_{VALIDATION_FILE_HANDLE_ID}.csv",
        ) as mock_download, patch(
            "pandas.read_csv",
            return_value=mock_df,
        ):
            result = await record_set.get_detailed_validation_results_async(
                download_location=download_location, synapse_client=self.syn
            )

            # THEN the download should use the custom location
            call_kwargs = mock_download.call_args[1]
            assert download_location in call_kwargs["destination"]

            # AND the result should be the DataFrame
            assert result == mock_df


class TestValidationSummary:
    """Tests for the ValidationSummary dataclass."""

    def test_creation(self) -> None:
        # GIVEN validation summary data
        # WHEN I create a ValidationSummary
        summary = ValidationSummary(
            container_id=SYN_123,
            total_number_of_children=20,
            number_of_valid_children=15,
            number_of_invalid_children=3,
            number_of_unknown_children=2,
            generated_on="2024-05-01T00:00:00.000Z",
        )

        # THEN all fields should be set
        assert summary.container_id == SYN_123
        assert summary.total_number_of_children == 20
        assert summary.number_of_valid_children == 15
        assert summary.number_of_invalid_children == 3
        assert summary.number_of_unknown_children == 2
        assert summary.generated_on == "2024-05-01T00:00:00.000Z"

    def test_defaults_to_none(self) -> None:
        # GIVEN no arguments
        # WHEN I create a ValidationSummary
        summary = ValidationSummary()

        # THEN all fields should be None
        assert summary.container_id is None
        assert summary.total_number_of_children is None
        assert summary.number_of_valid_children is None
        assert summary.number_of_invalid_children is None
        assert summary.number_of_unknown_children is None
        assert summary.generated_on is None
