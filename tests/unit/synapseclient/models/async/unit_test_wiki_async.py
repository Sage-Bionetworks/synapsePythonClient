"""Tests for the synapseclient.models.wiki classes."""
import copy
import os
import uuid
from typing import Any, AsyncGenerator, Dict, List
from unittest.mock import AsyncMock, Mock, call, mock_open, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models.wiki import (
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)


class TestWikiOrderHint:
    """Tests for the WikiOrderHint class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    # Construct a WikiOrderHint object
    order_hint = WikiOrderHint(
        owner_id="syn123",
        owner_object_type="org.sagebionetworks.repo.model.Project",
        id_list=["wiki1", "wiki2", "wiki3"],
        etag="etag123",
    )

    api_response = {
        "ownerId": "syn123",
        "ownerObjectType": "org.sagebionetworks.repo.model.Project",
        "idList": ["wiki1", "wiki2", "wiki3"],
        "etag": "etag123",
    }

    async def test_fill_from_dict(self) -> None:
        # WHEN I call `fill_from_dict` with the API response
        result = self.order_hint.fill_from_dict(self.api_response)

        # THEN the WikiOrderHint object should be filled with the example data
        assert result == self.order_hint

    async def test_to_synapse_request(self):
        # WHEN I call `to_synapse_request` on an initialized order hint
        results = self.order_hint.to_synapse_request()

        # THEN the request should contain the correct data
        assert results == self.api_response

    async def test_to_synapse_request_with_none_values(self) -> None:
        # GIVEN a WikiOrderHint object with None values
        order_hint = WikiOrderHint(
            owner_id="syn123",
            owner_object_type=None,
            id_list=[],
            etag=None,
        )

        # WHEN I call `to_synapse_request`
        results = order_hint.to_synapse_request()

        # THEN the request should not contain None values
        assert results == {"ownerId": "syn123", "idList": []}

    async def test_store_async_success(self) -> None:
        # GIVEN a mock response
        with patch(
            "synapseclient.models.wiki.put_wiki_order_hint",
            new_callable=AsyncMock,
            return_value=self.api_response,
        ) as mocked_put:
            results = await self.order_hint.store_async(synapse_client=self.syn)

            # THEN the API should be called with correct parameters
            mocked_put.assert_called_once_with(
                owner_id=self.order_hint.owner_id,
                request=self.order_hint.to_synapse_request(),
                synapse_client=self.syn,
            )

            # AND the result should be updated with the response
            assert results == self.order_hint

    async def test_store_async_missing_owner_id(self) -> None:
        # GIVEN a WikiOrderHint object without owner_id
        order_hint = WikiOrderHint(
            owner_object_type="org.sagebionetworks.repo.model.Project",
            id_list=["wiki1", "wiki2"],
        )

        # WHEN I call `store_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to store wiki order hint."
        ):
            await order_hint.store_async(synapse_client=self.syn)

    async def test_get_async_success(self) -> None:
        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_order_hint",
            new_callable=AsyncMock,
            return_value=self.api_response,
        ) as mocked_get:
            results = await self.order_hint.get_async(synapse_client=self.syn)

            # THEN the API should be called with correct parameters
            mocked_get.assert_called_once_with(
                owner_id="syn123",
                synapse_client=self.syn,
            )

            # AND the result should be filled with the response
            assert results == self.order_hint

    async def test_get_async_missing_owner_id(self) -> None:
        # WHEN I call `get_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to get wiki order hint."
        ):
            await self.order_hint.get_async(synapse_client=self.syn)


class TestWikiHistorySnapshot:
    """Tests for the WikiHistorySnapshot class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    # Construct a WikiHistorySnapshot object
    history_snapshot = WikiHistorySnapshot(
        version="1",
        modified_on="2023-01-01T00:00:00.000Z",
        modified_by="12345",
    )

    # Construct an API response
    api_response = {
        "version": "1",
        "modifiedOn": "2023-01-01T00:00:00.000Z",
        "modifiedBy": "12345",
    }

    async def test_fill_from_dict(self) -> None:
        # WHEN I call `fill_from_dict` with the API response
        results = self.history_snapshot.fill_from_dict(self.api_response)

        # THEN the WikiHistorySnapshot object should be filled with the example data
        assert results == self.history_snapshot

    async def test_get_async_success(self) -> None:
        # GIVEN mock responses
        mock_responses = [
            {
                "version": 1,
                "modifiedOn": "2023-01-01T00:00:00.000Z",
                "modifiedBy": "12345",
            },
            {
                "version": 2,
                "modifiedOn": "2023-01-02T00:00:00.000Z",
                "modifiedBy": "12345",
            },
            {
                "version": 3,
                "modifiedOn": "2023-01-03T00:00:00.000Z",
                "modifiedBy": "12345",
            },
        ]

        # Create an async generator mock
        async def mock_async_generator(
            items: List[Dict[str, Any]]
        ) -> AsyncGenerator[Dict[str, Any], None]:
            for item in items:
                yield item

        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_history",
            return_value=mock_async_generator(mock_responses),
        ) as mocked_get:
            results = await WikiHistorySnapshot.get_async(
                owner_id="syn123",
                id="wiki1",
                offset=0,
                limit=20,
                synapse_client=self.syn,
            )
            # THEN the API should be called with correct parameters
            mocked_get.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                offset=0,
                limit=20,
                synapse_client=self.syn,
            )

            history_snapshot_list = [
                WikiHistorySnapshot(
                    version=1,
                    modified_on="2023-01-01T00:00:00.000Z",
                    modified_by="12345",
                ),
                WikiHistorySnapshot(
                    version=2,
                    modified_on="2023-01-02T00:00:00.000Z",
                    modified_by="12345",
                ),
                WikiHistorySnapshot(
                    version=3,
                    modified_on="2023-01-03T00:00:00.000Z",
                    modified_by="12345",
                ),
            ]
            # AND the results should contain the expected data
            assert results == history_snapshot_list

    async def test_get_async_missing_owner_id(self) -> None:
        # WHEN I call `get_async` without owner_id
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to get wiki history."
        ):
            await WikiHistorySnapshot.get_async(
                id="wiki1",
                synapse_client=self.syn,
            )

    async def test_get_async_missing_id(self) -> None:
        # WHEN I call `get_async` without id
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="Must provide id to get wiki history."):
            await WikiHistorySnapshot.get_async(
                owner_id="syn123",
                synapse_client=self.syn,
            )


class TestWikiHeader:
    """Tests for the WikiHeader class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    wiki_header = WikiHeader(
        id="wiki1",
        title="Test Wiki",
        parent_id="1234",
    )

    api_response = {
        "id": "wiki1",
        "title": "Test Wiki",
        "parentId": "1234",
    }

    async def test_fill_from_dict(self) -> None:
        # WHEN I call `fill_from_dict` with the example data
        results = self.wiki_header.fill_from_dict(self.api_response)

        # THEN the WikiHeader object should be filled with the example data
        assert results == self.wiki_header

    async def test_get_async_success(self) -> None:
        # GIVEN mock responses
        mock_responses = [
            {
                "id": "wiki1",
                "title": "Test Wiki",
                "parentId": "1234",
            },
            {
                "id": "wiki2",
                "title": "Test Wiki 2",
                "parentId": "1234",
            },
        ]

        # Create an async generator mock
        async def mock_async_generator(
            values: List[Dict[str, Any]]
        ) -> AsyncGenerator[Dict[str, Any], None]:
            for item in values:
                yield item

        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_header_tree",
            return_value=mock_async_generator(mock_responses),
        ) as mocked_get:
            results = await WikiHeader.get_async(
                owner_id="syn123",
                synapse_client=self.syn,
                offset=0,
                limit=20,
            )

            # THEN the API should be called with correct parameters
            mocked_get.assert_called_once_with(
                owner_id="syn123",
                offset=0,
                limit=20,
                synapse_client=self.syn,
            )

            # AND the results should contain the expected data
            wiki_header_list = [
                WikiHeader(id="wiki1", title="Test Wiki", parent_id="1234"),
                WikiHeader(id="wiki2", title="Test Wiki 2", parent_id="1234"),
            ]
            assert results == wiki_header_list

    async def test_get_async_missing_owner_id(self) -> None:
        # WHEN I call `get_async` without owner_id
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to get wiki header tree."
        ):
            await WikiHeader.get_async(synapse_client=self.syn)


class TestWikiPage:
    """Tests for the WikiPage class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    wiki_page = WikiPage(
        id="wiki1",
        etag="etag123",
        title="Test Wiki Page",
        parent_id="parent_wiki",
        markdown="# Test markdown text",
        attachments=["test_1.txt", "test_2.txt"],
        owner_id="syn123",
        created_on="2023-01-01T00:00:00.000Z",
        created_by="12345",
        modified_on="2023-01-02T00:00:00.000Z",
        modified_by="12345",
        wiki_version="0",
        markdown_file_handle_id=None,
        attachment_file_handle_ids=[],
    )

    api_response = {
        "id": "wiki1",
        "etag": "etag123",
        "title": "Test Wiki Page",
        "parentId": "parent_wiki",
        "markdown": "# Test markdown text",
        "attachments": ["test_1.txt", "test_2.txt"],
        "ownerId": "syn123",
        "createdOn": "2023-01-01T00:00:00.000Z",
        "createdBy": "12345",
        "modifiedOn": "2023-01-02T00:00:00.000Z",
        "modifiedBy": "12345",
        "wikiVersion": "0",
        "markdownFileHandleId": None,
        "attachmentFileHandleIds": [],
    }

    async def test_fill_from_dict(self) -> None:
        # WHEN I call `fill_from_dict` with the example data
        results = self.wiki_page.fill_from_dict(self.api_response)

        # THEN the WikiPage object should be filled with the example data
        assert results == self.wiki_page

    async def test_to_synapse_request(self) -> None:
        # WHEN I call `to_synapse_request`
        results = self.wiki_page.to_synapse_request()

        # THEN the request should contain the correct data
        assert results == self.api_response

    def test_to_synapse_request_with_none_values(self) -> None:
        # GIVEN a WikiPage object with None values
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki",
            markdown="# Test Content",
            owner_id="syn123",
            parent_id=None,
        )

        # WHEN I call `to_synapse_request`
        results = wiki_page.to_synapse_request()

        # THEN the request should not contain None values
        assert results == {
            "id": "wiki1",
            "title": "Test Wiki",
            "markdown": "# Test Content",
            "ownerId": "syn123",
        }

    def test_to_gzip_file_with_string_content(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"

        # WHEN I call `_to_gzip_file` with a markdown string
        with patch("os.path.isfile", return_value=False), patch(
            "builtins.open", mock_open(read_data=b"test content")
        ), patch("gzip.open", mock_open()), patch("os.path.exists", return_value=True):
            file_path, cache_dir = self.wiki_page._to_gzip_file(
                self.wiki_page.markdown, self.syn
            )

        # THEN the content should be written to a gzipped file
        assert file_path == "/tmp/cache/wiki_content/wiki_markdown_Test Wiki Page.md.gz"
        assert cache_dir == "/tmp/cache/wiki_content"

    def test_to_gzip_file_with_gzipped_file(self) -> None:
        with patch("os.path.isfile", return_value=True):
            self.syn.cache.cache_root_dir = "/tmp/cache"
            markdown_file_path = "wiki_markdown_Test Wiki Page.md.gz"
            # WHEN I call `_to_gzip_file` with a gzipped file
            file_path, cache_dir = self.wiki_page._to_gzip_file(
                markdown_file_path, self.syn
            )

            # THEN the filepath should be the same as the input
            assert file_path == markdown_file_path
            assert cache_dir == "/tmp/cache/wiki_content"

    def test_to_gzip_file_with_non_gzipped_file(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"

        # WHEN I call `_to_gzip_file` with a file path
        with patch("os.path.isfile", return_value=True), patch(
            "builtins.open", mock_open(read_data=b"test content")
        ), patch("gzip.open", mock_open()), patch("os.path.exists", return_value=True):
            file_path, cache_dir = self.wiki_page._to_gzip_file(
                "/path/to/test.txt", self.syn
            )

            # THEN the file should be processed
            assert file_path == "/tmp/cache/wiki_content/test.txt.gz"
            assert cache_dir == "/tmp/cache/wiki_content"

    def test_to_gzip_file_with_invalid_content(self) -> None:
        # WHEN I call `_to_gzip_file` with invalid content type
        # THEN it should raise SyntaxError
        with pytest.raises(SyntaxError, match="Expected a string, got int"):
            self.wiki_page._to_gzip_file(123, self.syn)

    def test_get_file_size_success(self) -> None:
        # GIVEN a filehandle dictionary
        filehandle_dict = {
            "list": [
                {"fileName": "test1.txt", "contentSize": "100"},
                {"fileName": "test2.txt", "contentSize": "200"},
            ]
        }

        # WHEN I call `_get_file_size`
        results = WikiPage._get_file_size(filehandle_dict, "test1.txt")

        # THEN the result should be the content size
        assert results == "100"

    def test_get_file_size_file_not_found(self) -> None:
        # GIVEN a filehandle dictionary
        filehandle_dict = {
            "list": [
                {"fileName": "test1.txt", "contentSize": "100"},
                {"fileName": "test2.txt", "contentSize": "200"},
            ]
        }

        # WHEN I call `_get_file_size` with a non-existent file
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="File nonexistent.txt not found in filehandle_dict"
        ):
            WikiPage._get_file_size(filehandle_dict, "nonexistent.txt")

    async def test_get_markdown_file_handle_success_with_markdown(self) -> WikiPage:
        with patch(
            "synapseclient.models.wiki.WikiPage._to_gzip_file",
            return_value=("test.txt.gz"),
        ) as mock_to_gzip_file, patch(
            "synapseclient.models.wiki.upload_file_handle",
            return_value={"id": "handle1"},
        ) as mock_upload, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger, patch(
            "os.path.exists", return_value=True
        ), patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_markdown_file_handle`
            results = await self.wiki_page._get_markdown_file_handle(
                synapse_client=self.syn
            )

            # THEN the markdown file handle should be uploaded
            mock_to_gzip_file.assert_called_once_with(
                wiki_content=self.wiki_page.markdown, synapse_client=self.syn
            )
            mock_upload.assert_called_once_with(
                syn=self.syn,
                parent_entity_id=self.wiki_page.owner_id,
                path="test.txt.gz",
            )
            assert mock_logger.call_count == 2
            assert mock_logger.has_calls(
                [
                    call("Uploaded file handle handle1 for wiki page markdown."),
                    call("Deleted temp directory test.txt.gz"),
                ]
            )

            # AND the temp gzipped file should be deleted
            assert mock_remove.call_count == 1

            # AND the result should be filled with the response
            self.wiki_page.markdown_file_handle_id = "handle1"
            assert results == self.wiki_page

    async def test_get_markdown_file_handle_no_markdown(self) -> WikiPage:
        # GIVEN a WikiPage with no markdown
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki Page",
            attachments=["test_1.txt", "test_2.txt"],
        )

        # WHEN I call `_get_markdown_file_handle`
        results = await wiki_page._get_markdown_file_handle(synapse_client=self.syn)

        # THEN the result should be the same WikiPage
        assert self.syn.logger.info.call_count == 0
        assert results == wiki_page

    async def test_get_attachment_file_handles_success_multiple_attachments(
        self,
    ) -> WikiPage:
        # GIVEN mock responses for file handles
        mock_to_gzip_file_responses = [
            ("/tmp/cache1/test_1.txt.gz"),
            ("/tmp/cache1/test_2.txt.gz"),
        ]
        mock_upload_responses = [
            {"id": "handle1"},
            {"id": "handle2"},
        ]

        with patch(
            "synapseclient.models.wiki.WikiPage._to_gzip_file",
            side_effect=mock_to_gzip_file_responses,
        ) as mock_to_gzip_file, patch(
            "synapseclient.models.wiki.upload_file_handle",
            side_effect=mock_upload_responses,
        ) as mock_upload, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger, patch(
            "os.path.exists", return_value=True
        ), patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_attachment_file_handles`
            results = await self.wiki_page._get_attachment_file_handles(
                synapse_client=self.syn
            )

            # THEN _to_gzip_file should be called for each attachment
            assert mock_to_gzip_file.call_count == len(self.wiki_page.attachments)
            mock_to_gzip_file.assert_any_call(
                wiki_content="test_1.txt", synapse_client=self.syn
            )
            mock_to_gzip_file.assert_any_call(
                wiki_content="test_2.txt", synapse_client=self.syn
            )

            # AND upload_file_handle should be called for each attachment
            assert mock_upload.call_count == len(self.wiki_page.attachments)
            mock_upload.assert_any_call(
                syn=self.syn,
                parent_entity_id=self.wiki_page.owner_id,
                path="/tmp/cache1/test_1.txt.gz",
            )
            mock_upload.assert_any_call(
                syn=self.syn,
                parent_entity_id=self.wiki_page.owner_id,
                path="/tmp/cache1/test_2.txt.gz",
            )
            assert mock_logger.call_count == 4
            assert mock_logger.has_calls(
                [
                    call("Uploaded file handle handle1 for wiki page attachment."),
                    call("Uploaded file handle handle2 for wiki page attachment."),
                    call("Deleted temp directory /tmp/cache1/test_1.txt.gz"),
                    call("Deleted temp directory /tmp/cache1/test_2.txt.gz"),
                ]
            )

            # AND the temp directories should be cleaned up
            mock_remove.assert_has_calls(
                [
                    call("/tmp/cache1/test_1.txt.gz"),
                    call("/tmp/cache1/test_2.txt.gz"),
                ]
            )

            # AND the attachment file handle IDs should be set correctly
            expected_attachment_handles = ["handle1", "handle2"]
            assert results.attachment_file_handle_ids == expected_attachment_handles

            # AND the result should be the updated WikiPage
            self.wiki_page.attachment_file_handle_ids = expected_attachment_handles
            assert results == self.wiki_page

    async def test_get_attachment_file_handles_empty_attachments(self) -> WikiPage:
        # GIVEN a WikiPage with no attachments
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki Page",
            markdown="# Test markdown text",
            attachments=[],  # Empty attachments
            owner_id="syn123",
        )

        # WHEN I call `_get_attachment_file_handles`
        results = await wiki_page._get_attachment_file_handles(synapse_client=self.syn)

        # THEN the result should be the same WikiPage
        assert results == wiki_page

    async def test_get_attachment_file_handles_single_attachment(self) -> WikiPage:
        # GIVEN a WikiPage with a single attachment
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki Page",
            markdown="# Test markdown text",
            attachments=["test_1.txt"],
            owner_id="syn123",
        )

        with patch(
            "synapseclient.models.wiki.WikiPage._to_gzip_file",
            return_value=("/tmp/cache/test_1.txt.gz"),
        ) as mock_to_gzip_file, patch(
            "synapseclient.models.wiki.upload_file_handle",
            return_value={"id": "handle1"},
        ) as mock_upload, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger, patch(
            "os.path.exists", return_value=True
        ), patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_attachment_file_handles`
            results = await wiki_page._get_attachment_file_handles(
                synapse_client=self.syn
            )

            # THEN _to_gzip_file should be called once
            mock_to_gzip_file.assert_called_once_with(
                wiki_content="test_1.txt", synapse_client=self.syn
            )

            # AND upload_file_handle should be called once
            mock_upload.assert_called_once_with(
                syn=self.syn,
                parent_entity_id=wiki_page.owner_id,
                path="/tmp/cache/test_1.txt.gz",
            )
            assert mock_logger.call_count == 2
            assert mock_logger.has_calls(
                [
                    call("Uploaded file handle handle1 for wiki page attachment."),
                    call("Deleted temp directory /tmp/cache/test_1.txt.gz"),
                ]
            )
            # AND the temp directory should be cleaned up
            mock_remove.assert_called_once_with("/tmp/cache/test_1.txt.gz")

            # AND the attachment file handle ID should be set correctly
            assert results.attachment_file_handle_ids == ["handle1"]

            # AND the result should be the updated WikiPage
            wiki_page.attachment_file_handle_ids = ["handle1"]
            assert results == wiki_page

    async def test_get_attachment_file_handles_cache_dir_not_exists(self) -> WikiPage:
        # GIVEN a WikiPage with attachments
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki Page",
            markdown="# Test markdown text",
            attachments=["test_1.txt"],
            owner_id="syn123",
        )

        with patch(
            "synapseclient.models.wiki.WikiPage._to_gzip_file",
            return_value=("/tmp/cache/test_1.txt.gz"),
        ) as mock_to_gzip_file, patch(
            "synapseclient.models.wiki.upload_file_handle",
            return_value={"id": "handle1"},
        ) as mock_upload, patch(
            "os.path.exists", return_value=False
        ), patch.object(
            self.syn.logger, "debug"
        ) as mock_logger, patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_attachment_file_handles`
            results = await wiki_page._get_attachment_file_handles(
                synapse_client=self.syn
            )

            # THEN the function should complete successfully
            assert results.attachment_file_handle_ids == ["handle1"]
            assert mock_logger.call_count == 1
            assert (
                mock_logger.call_args[0][0]
                == "Uploaded file handle handle1 for wiki page attachment."
            )
            # AND cleanup should not be attempted since directory doesn't exist
            mock_remove.assert_not_called()

    async def test_get_attachment_file_handles_upload_failure(self) -> WikiPage:
        # GIVEN a WikiPage with attachments
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki Page",
            markdown="# Test markdown text",
            attachments=["test_1.txt"],
            owner_id="syn123",
        )

        with patch(
            "synapseclient.models.wiki.WikiPage._to_gzip_file",
            return_value=("/tmp/cache/test_1.txt.gz"),
        ) as mock_to_gzip_file, patch(
            "synapseclient.models.wiki.upload_file_handle",
            side_effect=Exception("Upload failed"),
        ) as mock_upload, patch(
            "os.path.exists", return_value=True
        ), patch.object(
            self.syn.logger, "debug"
        ) as mock_logger, patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_attachment_file_handles`
            # THEN it should raise the exception
            with pytest.raises(Exception, match="Upload failed"):
                await wiki_page._get_attachment_file_handles(synapse_client=self.syn)

            # AND cleanup should still be attempted
            mock_remove.assert_called_once_with("/tmp/cache/test_1.txt.gz")
            assert mock_logger.call_count == 1
            assert (
                mock_logger.call_args[0][0]
                == "Deleted temp directory /tmp/cache/test_1.txt.gz"
            )

    async def test_determine_wiki_action_error_no_owner_id(self) -> None:
        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            side_effect=SynapseHTTPError(response=Mock(status_code=404)),
        ) as mock_get_header:
            # GIVEN a WikiPage with no parent_id
            wiki_page = WikiPage(
                id="wiki1",
                title="Test Wiki Page",
            )

            # WHEN I cal `determine_wiki_action`
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="Must provide owner_id to modify a wiki page."
            ):
                await wiki_page._determine_wiki_action()
                mock_get_header.assert_not_called()

    async def test_determine_wiki_action_create_root(self) -> None:
        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            side_effect=SynapseHTTPError(response=Mock(status_code=404)),
        ) as mock_get_header:
            # GIVEN a WikiPage with no parent_id
            wiki_page = WikiPage(
                owner_id="syn123",
                title="Test Wiki Page",
            )
            # WHEN I call `determine_wiki_action`
            # THEN it should return "create_root_wiki_page"
            assert await wiki_page._determine_wiki_action() == "create_root_wiki_page"
            mock_get_header.assert_called_once_with(owner_id="syn123")

    async def test_determine_wiki_action_create_sub(self) -> None:
        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            side_effect=SynapseHTTPError(response=Mock(status_code=404)),
        ) as mock_get_header:
            # GIVEN a WikiPage with a parent_id
            wiki_page = WikiPage(
                owner_id="syn123",
                title="Test Wiki Page",
                parent_id="parent_wiki",
            )
            # WHEN I call `determine_wiki_action`
            # THEN it should return "create_sub_wiki_page"
            assert await wiki_page._determine_wiki_action() == "create_sub_wiki_page"
            mock_get_header.assert_not_called()

    async def test_determine_wiki_action_update_existing_root(self) -> None:
        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            return_value=WikiHeader(id="wiki1", title="Test Wiki Page"),
        ) as mock_get_header:
            # GIVEN a WikiPage with an id
            wiki_page = WikiPage(
                id="wiki1",
                owner_id="syn123",
                title="Test Wiki Page",
            )

            # WHEN I call `determine_wiki_action`
            # THEN it should return "update_existing_wiki_page"
            assert (
                await wiki_page._determine_wiki_action() == "update_existing_wiki_page"
            )
            mock_get_header.assert_called_once_with(owner_id="syn123")

    async def test_determine_wiki_action_update_existing_without_passing_id(
        self,
    ) -> None:
        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            return_value=WikiHeader(id="wiki1", title="Test Wiki Page"),
        ) as mock_get_header:
            # GIVEN a WikiPage with an id and parent_id
            wiki_page = WikiPage(
                owner_id="syn123",
                title="Test Wiki Page",
            )
            # WHEN I call `determine_wiki_action`
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="Must provide id to update existing wiki page."
            ):
                await wiki_page._determine_wiki_action()
                mock_get_header.assert_called_once_with(owner_id="syn123")

    async def test_store_async_new_root_wiki_success(self) -> None:
        # Update the wiki_page with file handle ids
        self.wiki_page.parent_id = None

        # AND mock the post_wiki_page response
        post_api_response = copy.deepcopy(self.api_response)
        post_api_response["parentId"] = None
        post_api_response["markdownFileHandleId"] = "markdown_file_handle_id"
        post_api_response["attachmentFileHandleIds"] = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # Create mock WikiPage objects with the expected file handle IDs for markdown
        mock_wiki_with_markdown = copy.deepcopy(self.wiki_page)
        mock_wiki_with_markdown.markdown_file_handle_id = "markdown_file_handle_id"

        # Create mock WikiPage objects with the expected file handle IDs for attachments
        mock_wiki_with_attachments = copy.deepcopy(mock_wiki_with_markdown)
        mock_wiki_with_attachments.attachment_file_handle_ids = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # AND mock responses
        with patch(
            "synapseclient.models.wiki.WikiPage._determine_wiki_action",
            return_value="create_root_wiki_page",
        ), patch(
            "synapseclient.models.wiki.WikiPage._get_markdown_file_handle",
            return_value=mock_wiki_with_markdown,
        ), patch(
            "synapseclient.models.wiki.WikiPage._get_attachment_file_handles",
            return_value=mock_wiki_with_attachments,
        ), patch(
            "synapseclient.models.wiki.post_wiki_page", return_value=post_api_response
        ) as mock_post_wiki, patch.object(
            self.syn.logger, "info"
        ) as mock_logger:
            # WHEN I call `store_async`

            results = await self.wiki_page.store_async(synapse_client=self.syn)

            # THEN log messages should be printed
            assert mock_logger.call_count == 2
            assert mock_logger.has_calls(
                [
                    call(
                        "No wiki page exists within the owner. Create a new wiki page."
                    ),
                    call(
                        f"Created wiki page: {post_api_response['title']} with ID: {post_api_response['id']}."
                    ),
                ]
            )
            # Update the wiki_page with file handle ids for validation
            self.wiki_page.markdown_file_handle_id = "markdown_file_handle_id"
            self.wiki_page.attachment_file_handle_ids = [
                "attachment_file_handle_id_1",
                "attachment_file_handle_id_2",
            ]

            # AND the wiki should be created
            mock_post_wiki.assert_called_once_with(
                owner_id="syn123",
                request=self.wiki_page.to_synapse_request(),
            )

            # AND the result should be filled with the response
            expected_wiki = self.wiki_page.fill_from_dict(post_api_response)
            assert results == expected_wiki

    async def test_store_async_update_existing_wiki_success(self) -> None:
        # Update the wiki_page with file handle ids
        self.wiki_page.parent_id = None
        self.wiki_page.etag = None
        self.wiki_page.created_on = None
        self.wiki_page.created_by = None
        self.wiki_page.modified_on = None
        self.wiki_page.modified_by = None

        # AND mock the get_wiki_page response
        mock_get_wiki_response = copy.deepcopy(self.api_response)
        mock_get_wiki_response["parentId"] = None
        mock_get_wiki_response["markdown"] = None
        mock_get_wiki_response["attachments"] = []
        mock_get_wiki_response["markdownFileHandleId"] = None
        mock_get_wiki_response["attachmentFileHandleIds"] = []

        # Create mock WikiPage objects with the expected file handle IDs for markdown
        mock_wiki_with_markdown = copy.deepcopy(self.wiki_page)
        mock_wiki_with_markdown.markdown_file_handle_id = "markdown_file_handle_id"

        # Create mock WikiPage objects with the expected file handle IDs for attachments
        mock_wiki_with_attachments = copy.deepcopy(mock_wiki_with_markdown)
        mock_wiki_with_attachments.attachment_file_handle_ids = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # AND mock the put_wiki_page response
        # Create mock WikiPage objects with the expected file handle IDs for markdown
        mock_put_wiki_response = copy.deepcopy(self.api_response)
        mock_put_wiki_response["parentId"] = None
        mock_put_wiki_response["markdownFileHandleId"] = "markdown_file_handle_id"
        mock_put_wiki_response["attachmentFileHandleIds"] = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # AND mock responses
        with patch(
            "synapseclient.models.wiki.WikiPage._determine_wiki_action",
            return_value="update_existing_wiki_page",
        ), patch(
            "synapseclient.models.wiki.WikiPage._get_markdown_file_handle",
            return_value=mock_wiki_with_markdown,
        ), patch(
            "synapseclient.models.wiki.WikiPage._get_attachment_file_handles",
            return_value=mock_wiki_with_attachments,
        ), patch(
            "synapseclient.models.wiki.get_wiki_page",
            return_value=mock_get_wiki_response,
        ) as mock_get_wiki, patch(
            "synapseclient.models.wiki.put_wiki_page",
            return_value=mock_put_wiki_response,
        ) as mock_put_wiki, patch.object(
            self.syn.logger, "info"
        ) as mock_logger:
            # WHEN I call `store_async`
            results = await self.wiki_page.store_async(synapse_client=self.syn)
            # THEN the existing wiki should be retrieved
            mock_get_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
            )

            # AND the wiki should be updated after merging dataclass objects
            self.wiki_page.etag = "etag123"
            self.wiki_page.created_on = "2023-01-01T00:00:00.000Z"
            self.wiki_page.created_by = "12345"
            self.wiki_page.modified_on = "2023-01-02T00:00:00.000Z"
            self.wiki_page.modified_by = "12345"
            self.wiki_page.markdown_file_handle_id = "markdown_file_handle_id"
            self.wiki_page.attachment_file_handle_ids = [
                "attachment_file_handle_id_1",
                "attachment_file_handle_id_2",
            ]
            mock_put_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                request=self.wiki_page.to_synapse_request(),
            )

            # AND log messages should be printed
            assert mock_logger.call_count == 2
            assert mock_logger.has_calls(
                [
                    call(
                        "A wiki page already exists within the owner. Update the existing wiki page."
                    ),
                    call(
                        f"Updated wiki page: {self.api_response['title']} with ID: {self.api_response['id']}."
                    ),
                ]
            )
            # AND the result should be filled with the response
            expected_wiki = self.wiki_page.fill_from_dict(mock_put_wiki_response)
            assert results == expected_wiki

    async def test_store_async_create_sub_wiki_success(self) -> None:
        # AND mock the post_wiki_page response
        post_api_response = copy.deepcopy(self.api_response)
        post_api_response["markdownFileHandleId"] = "markdown_file_handle_id"
        post_api_response["attachmentFileHandleIds"] = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # Create mock WikiPage objects with the expected file handle IDs for markdown
        mock_wiki_with_markdown = copy.deepcopy(self.wiki_page)
        mock_wiki_with_markdown.markdown_file_handle_id = "markdown_file_handle_id"

        # Create mock WikiPage objects with the expected file handle IDs for attachments
        mock_wiki_with_attachments = copy.deepcopy(mock_wiki_with_markdown)
        mock_wiki_with_attachments.attachment_file_handle_ids = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # AND mock responses
        with patch(
            "synapseclient.models.wiki.WikiPage._determine_wiki_action",
            return_value="create_sub_wiki_page",
        ), patch(
            "synapseclient.models.wiki.WikiPage._get_markdown_file_handle",
            return_value=mock_wiki_with_markdown,
        ), patch(
            "synapseclient.models.wiki.WikiPage._get_attachment_file_handles",
            return_value=mock_wiki_with_attachments,
        ), patch(
            "synapseclient.models.wiki.post_wiki_page", return_value=post_api_response
        ) as mock_post_wiki, patch.object(
            self.syn.logger, "info"
        ) as mock_logger:
            # WHEN I call `store_async`
            results = await self.wiki_page.store_async(synapse_client=self.syn)

            # THEN log messages should be printed
            assert mock_logger.call_count == 2
            assert mock_logger.has_calls(
                [
                    call("Creating sub-wiki page under parent ID: parent_wiki"),
                    call(
                        f"Created sub-wiki page: {post_api_response['title']} with ID: {post_api_response['id']} under parent: parent_wiki"
                    ),
                ]
            )

            # Update the wiki_page with file handle ids for validation
            self.wiki_page.markdown_file_handle_id = "markdown_file_handle_id"
            self.wiki_page.attachment_file_handle_ids = [
                "attachment_file_handle_id_1",
                "attachment_file_handle_id_2",
            ]

            # AND the wiki should be created
            mock_post_wiki.assert_called_once_with(
                owner_id="syn123",
                request=self.wiki_page.to_synapse_request(),
                synapse_client=self.syn,
            )

            # AND the result should be filled with the response
            expected_wiki = self.wiki_page.fill_from_dict(post_api_response)
            assert results == expected_wiki

    @pytest.mark.parametrize(
        "wiki_page, expected_error",
        [
            (
                WikiPage(owner_id=None, title="Test Wiki", wiki_version="0"),
                "Must provide owner_id to restore a wiki page.",
            ),
            (
                WikiPage(owner_id="syn123", id=None, wiki_version="0"),
                "Must provide id to restore a wiki page.",
            ),
            (
                WikiPage(owner_id="syn123", id="wiki1", wiki_version=None),
                "Must provide wiki_version to restore a wiki page.",
            ),
        ],
    )
    async def test_restore_async_missing_required_parameters(
        self, wiki_page, expected_error
    ) -> None:
        # WHEN I call `restore_async`
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match=expected_error):
            await wiki_page.restore_async(synapse_client=self.syn)

    async def test_restore_async_success(self) -> None:
        with patch(
            "synapseclient.models.wiki.put_wiki_version", return_value=self.api_response
        ) as mock_put_wiki_version:
            # WHEN I call `restore_async`
            results = await self.wiki_page.restore_async(synapse_client=self.syn)
            # THEN the API should be called with correct parameters
            mock_put_wiki_version.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                request=self.wiki_page.to_synapse_request(),
                synapse_client=self.syn,
            )
            # AND the result should be filled with the response
            expected_wiki = self.wiki_page.fill_from_dict(self.api_response)
            assert results == expected_wiki

    async def test_get_async_by_id_success(self) -> None:
        # GIVEN a WikiPage object with id
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
        )

        # AND a mock response
        with patch("synapseclient.api.wiki_service.get_wiki_page") as mock_get_wiki:
            mock_get_wiki.return_value = self.api_response

            # WHEN I call `get_async`
            result = await wiki.get_async(synapse_client=self.syn)

            # THEN the API should be called with correct parameters
            mock_get_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version=None,
                synapse_client=self.syn,
            )

            # AND the result should be filled with the response
            assert result.id == "wiki1"
            assert result.title == "Test Wiki Page"

    async def test_get_async_by_title_success(self) -> None:
        # GIVEN a WikiPage object with title but no id
        wiki = WikiPage(
            title="Test Wiki",
            owner_id="syn123",
        )

        # AND mock responses
        mock_responses = [{"id": "wiki1", "title": "Test Wiki", "parentId": None}]

        # Create an async generator mock
        async def mock_async_generator():
            for item in mock_responses:
                yield item

        with patch(
            "synapseclient.api.wiki_service.get_wiki_header_tree"
        ) as mock_get_header_tree, patch(
            "synapseclient.api.wiki_service.get_wiki_page"
        ) as mock_get_wiki:
            mock_get_header_tree.return_value = mock_async_generator()
            mock_get_wiki.return_value = self.api_response

            # WHEN I call `get_async`
            result = await wiki.get_async(synapse_client=self.syn)

            # THEN the header tree should be retrieved
            mock_get_header_tree.assert_called_once_with(
                owner_id="syn123",
                synapse_client=self.syn,
            )

            # AND the wiki should be retrieved by id
            mock_get_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version=None,
                synapse_client=self.syn,
            )

            # AND the result should be filled with the response
            assert result.id == "wiki1"
            assert result.title == "Test Wiki Page"

    async def test_get_async_by_title_not_found(self) -> None:
        # GIVEN a WikiPage object with title but no id
        wiki = WikiPage(
            title="Non-existent Wiki",
            owner_id="syn123",
        )

        # AND mock responses that don't contain the title
        mock_responses = [{"id": "wiki1", "title": "Different Wiki", "parentId": None}]

        # Create an async generator mock
        async def mock_async_generator():
            for item in mock_responses:
                yield item

        with patch(
            "synapseclient.api.wiki_service.get_wiki_header_tree"
        ) as mock_get_header_tree:
            mock_get_header_tree.return_value = mock_async_generator()

            # WHEN I call `get_async`
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="No wiki page found with title: Non-existent Wiki"
            ):
                await wiki.get_async(synapse_client=self.syn)

    async def test_get_async_missing_owner_id(self) -> None:
        # GIVEN a WikiPage object without owner_id
        wiki = WikiPage(
            id="wiki1",
        )

        # WHEN I call `get_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to get a wiki page."
        ):
            await wiki.get_async(synapse_client=self.syn)

    async def test_delete_async_success(self) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
        )

        # WHEN I call `delete_async`
        with patch("synapseclient.api.wiki_service.delete_wiki_page") as mock_delete:
            await wiki.delete_async(synapse_client=self.syn)

            # THEN the API should be called with correct parameters
            mock_delete.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                synapse_client=self.syn,
            )

    async def test_delete_async_missing_owner_id(self) -> None:
        # GIVEN a WikiPage object without owner_id
        wiki = WikiPage(
            id="wiki1",
        )

        # WHEN I call `delete_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to delete a wiki page."
        ):
            await wiki.delete_async(synapse_client=self.syn)

    async def test_delete_async_missing_id(self) -> None:
        # GIVEN a WikiPage object without id
        wiki = WikiPage(
            owner_id="syn123",
        )

        # WHEN I call `delete_async`
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="Must provide id to delete a wiki page."):
            await wiki.delete_async(synapse_client=self.syn)

    @patch("synapseclient.api.wiki_service.get_attachment_handles")
    async def test_get_attachment_handles_async_success(self, mock_get_handles) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
        )

        # AND a mock response
        mock_handles = [{"id": "handle1", "fileName": "test.txt"}]
        mock_get_handles.return_value = mock_handles

        # WHEN I call `get_attachment_handles_async`
        result = await wiki.get_attachment_handles_async(synapse_client=self.syn)

        # THEN the API should be called with correct parameters
        mock_get_handles.assert_called_once_with(
            owner_id="syn123",
            wiki_id="wiki1",
            wiki_version=None,
            synapse_client=self.syn,
        )

        # AND the result should be the handles
        assert result == mock_handles

    async def test_get_attachment_handles_async_missing_owner_id(self) -> None:
        # GIVEN a WikiPage object without owner_id
        wiki = WikiPage(
            id="wiki1",
        )

        # WHEN I call `get_attachment_handles_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to get attachment handles."
        ):
            await wiki.get_attachment_handles_async(synapse_client=self.syn)

    async def test_get_attachment_handles_async_missing_id(self) -> None:
        # GIVEN a WikiPage object without id
        wiki = WikiPage(
            owner_id="syn123",
        )

        # WHEN I call `get_attachment_handles_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide id to get attachment handles."
        ):
            await wiki.get_attachment_handles_async(synapse_client=self.syn)

    @patch("synapseclient.api.wiki_service.get_attachment_url")
    async def test_get_attachment_async_url_only(self, mock_get_url) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
        )

        # AND a mock response
        mock_url = "https://example.com/attachment.txt"
        mock_get_url.return_value = mock_url

        # WHEN I call `get_attachment_async` with download_file=False
        result = await wiki.get_attachment_async(
            file_name="test.txt",
            download_file=False,
            synapse_client=self.syn,
        )

        # THEN the API should be called with correct parameters
        mock_get_url.assert_called_once_with(
            owner_id="syn123",
            wiki_id="wiki1",
            file_name="test.txt",
            wiki_version=None,
            redirect=False,
            synapse_client=self.syn,
        )

        # AND the result should be the URL
        assert result == mock_url

    async def test_get_attachment_async_missing_owner_id(self) -> None:
        # GIVEN a WikiPage object without owner_id
        wiki = WikiPage(
            id="wiki1",
        )

        # WHEN I call `get_attachment_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to get attachment URL."
        ):
            await wiki.get_attachment_async(
                file_name="test.txt",
                synapse_client=self.syn,
            )

    async def test_get_attachment_async_missing_id(self) -> None:
        # GIVEN a WikiPage object without id
        wiki = WikiPage(
            owner_id="syn123",
        )

        # WHEN I call `get_attachment_async`
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="Must provide id to get attachment URL."):
            await wiki.get_attachment_async(
                file_name="test.txt",
                synapse_client=self.syn,
            )

    async def test_get_attachment_async_missing_file_name(self) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
        )

        # WHEN I call `get_attachment_async` without file_name
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide file_name to get attachment URL."
        ):
            await wiki.get_attachment_async(
                file_name="",
                synapse_client=self.syn,
            )

    async def test_restore_async_missing_owner_id(self) -> None:
        # GIVEN a WikiPage object without owner_id
        wiki = WikiPage(
            id="wiki1",
            wiki_version="1",
        )

        # WHEN I call `restore_async`
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="Must provide owner_id to restore a wiki page."
        ):
            await wiki.restore_async(synapse_client=self.syn)

    async def test_restore_async_missing_id(self) -> None:
        # GIVEN a WikiPage object without id
        wiki = WikiPage(
            owner_id="syn123",
            wiki_version="1",
        )

        # WHEN I call `restore_async`
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="Must provide id to restore a wiki page."):
            await wiki.restore_async(synapse_client=self.syn)
