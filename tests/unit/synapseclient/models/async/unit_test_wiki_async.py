"""Tests for the synapseclient.models.wiki classes."""
import copy
import os
from typing import Any, AsyncGenerator, Dict, List
from unittest.mock import AsyncMock, Mock, call, mock_open, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.download.download_async import PresignedUrlInfo
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
        with patch(
            "synapseclient.models.wiki.put_wiki_order_hint",
            new_callable=AsyncMock,
            return_value=self.api_response,
        ) as mocked_put, pytest.raises(
            ValueError, match="Must provide owner_id to store wiki order hint."
        ):
            await order_hint.store_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_put.assert_not_called()

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
        # GIVEN a WikiOrderHint object without owner_id
        self.order_hint.owner_id = None
        # WHEN I call `get_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.get_wiki_order_hint"
        ) as mocked_get, pytest.raises(
            ValueError, match="Must provide owner_id to get wiki order hint."
        ):
            await self.order_hint.get_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_get.assert_not_called()


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
        async def mock_responses() -> AsyncGenerator[Dict[str, Any], None]:
            yield {
                "version": 1,
                "modifiedOn": "2023-01-01T00:00:00.000Z",
                "modifiedBy": "12345",
            }
            yield {
                "version": 2,
                "modifiedOn": "2023-01-02T00:00:00.000Z",
                "modifiedBy": "12345",
            }
            yield {
                "version": 3,
                "modifiedOn": "2023-01-03T00:00:00.000Z",
                "modifiedBy": "12345",
            }

        # Create an async generator mock
        async def mock_async_generator() -> AsyncGenerator[WikiHistorySnapshot, None]:
            async for item in mock_responses():
                yield item

        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_history",
            return_value=mock_async_generator(),
        ) as mocked_get:
            results = []
            async for item in WikiHistorySnapshot().get_async(
                owner_id="syn123",
                id="wiki1",
                offset=0,
                limit=20,
                synapse_client=self.syn,
            ):
                results.append(item)
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
        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_history"
        ) as mocked_get, pytest.raises(
            ValueError, match="Must provide owner_id to get wiki history."
        ):
            async for _ in WikiHistorySnapshot.get_async(
                owner_id=None,
                id="wiki1",
                synapse_client=self.syn,
            ):
                pass
            # THEN the API should not be called
            mocked_get.assert_not_called()

    async def test_get_async_missing_id(self) -> None:
        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_history"
        ) as mocked_get, pytest.raises(
            ValueError, match="Must provide id to get wiki history."
        ):
            async for _ in WikiHistorySnapshot.get_async(
                owner_id="syn123",
                id=None,
                synapse_client=self.syn,
            ):
                pass
            # THEN the API should not be called
            mocked_get.assert_not_called()


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
        async def mock_responses() -> AsyncGenerator[Dict[str, Any], None]:
            yield {
                "id": "wiki1",
                "title": "Test Wiki",
                "parentId": "1234",
            }
            yield {
                "id": "wiki2",
                "title": "Test Wiki 2",
                "parentId": "1234",
            }

        # Create an async generator mock
        async def mock_async_generator() -> AsyncGenerator[WikiHeader, None]:
            async for item in mock_responses():
                yield item

        # WHEN I call `get_async`
        with patch(
            "synapseclient.models.wiki.get_wiki_header_tree",
            return_value=mock_async_generator(),
        ) as mocked_get:
            results = []
            async for item in WikiHeader.get_async(
                owner_id="syn123",
                synapse_client=self.syn,
                offset=0,
                limit=20,
            ):
                results.append(item)
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
        # WHEN I call `get_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.get_wiki_header_tree"
        ) as mocked_get, pytest.raises(
            ValueError, match="Must provide owner_id to get wiki header tree."
        ):
            async for _ in WikiHeader.get_async(owner_id=None, synapse_client=self.syn):
                pass
            # THEN the API should not be called
            mocked_get.assert_not_called()


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
        "parentWikiId": "parent_wiki",
        "createdOn": "2023-01-01T00:00:00.000Z",
        "createdBy": "12345",
        "modifiedOn": "2023-01-02T00:00:00.000Z",
        "modifiedBy": "12345",
        "markdownFileHandleId": None,
        "attachmentFileHandleIds": [],
    }

    def get_fresh_wiki_page(self) -> WikiPage:
        """Helper method to get a fresh copy of the wiki_page for tests that need to modify it."""
        return copy.deepcopy(self.wiki_page)

    async def test_fill_from_dict(self) -> None:
        # WHEN I call `fill_from_dict` with the example data
        results = self.wiki_page.fill_from_dict(self.api_response)

        # THEN the WikiPage object should be filled with the example data
        assert results == self.wiki_page

    def test_to_synapse_request_delete_none_keys(self) -> None:
        # WHEN I call `to_synapse_request`
        results = self.wiki_page.to_synapse_request()
        # delete none keys for expected response
        expected_results = copy.deepcopy(self.api_response)
        expected_results.pop("markdownFileHandleId", None)
        expected_results.pop("ownerId", None)
        expected_results["attachments"] = self.wiki_page.attachments
        expected_results["markdown"] = self.wiki_page.markdown
        expected_results["wikiVersion"] = self.wiki_page.wiki_version
        # THEN the request should contain the correct data
        assert results == expected_results

    def test_to_gzip_file_with_string_content(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"

        # WHEN I call `_to_gzip_file` with a markdown string
        with patch("builtins.open") as mock_open_file, patch(
            "gzip.open"
        ) as mock_gzip_open, patch("os.path.exists", return_value=True):
            file_path = self.wiki_page._to_gzip_file(self.wiki_page.markdown, self.syn)

        # THEN the content should be written to a gzipped file
        mock_open_file.assert_not_called()
        mock_gzip_open.assert_called_once_with(
            os.path.join(
                self.syn.cache.cache_root_dir,
                "wiki_content",
                "wiki_markdown_Test Wiki Page.md.gz",
            ),
            "wt",
            encoding="utf-8",
        )
        mock_gzip_open.return_value.__enter__.return_value.write.assert_called_once_with(
            self.wiki_page.markdown
        )
        # normalize the file path
        assert file_path == os.path.join(
            self.syn.cache.cache_root_dir,
            "wiki_content",
            "wiki_markdown_Test Wiki Page.md.gz",
        )

    def test_to_gzip_file_with_gzipped_file(self) -> None:
        with patch("os.path.isfile"), patch("gzip.open") as mock_gzip_open, patch(
            "builtins.open"
        ) as mock_open_file:
            self.syn.cache.cache_root_dir = "/tmp/cache"
            markdown_file_path = "wiki_markdown_Test Wiki Page.md.gz"

            # WHEN I call `_to_gzip_file` with a gzipped file
            file_path = self.wiki_page._to_gzip_file(markdown_file_path, self.syn)
            mock_open_file.assert_not_called()
            mock_gzip_open.assert_not_called()
            assert file_path == markdown_file_path

    def test_to_gzip_file_with_non_gzipped_file(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"

        # WHEN I call `_to_gzip_file` with a file path
        with patch("os.path.isfile", return_value=True), patch(
            "builtins.open", new=mock_open(read_data=b"test content")
        ) as mock_open_file, patch("gzip.open") as mock_gzip_open, patch(
            "os.path.exists", return_value=True
        ):
            file_path = self.wiki_page._to_gzip_file("/path/to/test.txt", self.syn)

            # THEN the file should be processed
            mock_open_file.assert_called_once_with("/path/to/test.txt", "rb")
            mock_gzip_open.assert_called_once_with(
                "/tmp/cache/wiki_content/test.txt.gz", "wb"
            )
            gzip_handle = mock_gzip_open.return_value.__enter__.return_value
            open_handle = mock_open_file.return_value.__enter__.return_value
            gzip_handle.writelines.assert_called_once_with(open_handle)
            assert file_path == "/tmp/cache/wiki_content/test.txt.gz"

    def test_to_gzip_file_with_invalid_content(self) -> None:
        # WHEN I call `_to_gzip_file` with invalid content type
        # THEN it should raise SyntaxError
        with pytest.raises(SyntaxError, match="Expected a string, got int"):
            self.wiki_page._to_gzip_file(123, self.syn)

    def test_unzip_gzipped_file_with_markdown(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"
        gzipped_file_path = os.path.join(self.syn.cache.cache_root_dir, "test.md.gz")
        expected_unzipped_file_path = os.path.join(
            self.syn.cache.cache_root_dir, "test.md"
        )
        markdown_content = "# Test Markdown\n\nThis is a test."
        markdown_content_bytes = markdown_content.encode("utf-8")

        # WHEN I call `_unzip_gzipped_file` with a binary file
        with patch("gzip.open") as mock_gzip_open, patch(
            "builtins.open"
        ) as mock_open_file, patch("pprint.pp") as mock_pprint:
            mock_gzip_open.return_value.__enter__.return_value.read.return_value = (
                markdown_content_bytes
            )
            unzipped_file_path = self.wiki_page.unzip_gzipped_file(gzipped_file_path)

        # THEN the file should be unzipped correctly
        mock_gzip_open.assert_called_once_with(gzipped_file_path, "rb")
        mock_pprint.assert_called_once_with(markdown_content)
        mock_open_file.assert_called_once_with(
            expected_unzipped_file_path, "wt", encoding="utf-8"
        )
        mock_open_file.return_value.__enter__.return_value.write.assert_called_once_with(
            markdown_content
        )
        assert unzipped_file_path == expected_unzipped_file_path

    def test_unzip_gzipped_file_with_binary_file(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"
        gzipped_file_path = os.path.join(self.syn.cache.cache_root_dir, "test.bin.gz")
        expected_unzipped_file_path = os.path.join(
            self.syn.cache.cache_root_dir, "test.bin"
        )
        binary_content = b"\x00\x01\x02\x03\xff\xfe\xfd"

        # WHEN I call `_unzip_gzipped_file` with a binary file
        with patch("gzip.open") as mock_gzip_open, patch(
            "builtins.open"
        ) as mock_open_file, patch("pprint.pp") as mock_pprint:
            mock_gzip_open.return_value.__enter__.return_value.read.return_value = (
                binary_content
            )
            unzipped_file_path = self.wiki_page.unzip_gzipped_file(gzipped_file_path)

        # THEN the file should be unzipped correctly
        mock_gzip_open.assert_called_once_with(gzipped_file_path, "rb")
        mock_pprint.assert_not_called()
        mock_open_file.assert_called_once_with(unzipped_file_path, "wb")
        mock_open_file.return_value.__enter__.return_value.write.assert_called_once_with(
            binary_content
        )
        assert unzipped_file_path == expected_unzipped_file_path

    def test_unzip_gzipped_file_with_text_file(self) -> None:
        self.syn.cache.cache_root_dir = "/tmp/cache"
        gzipped_file_path = os.path.join(self.syn.cache.cache_root_dir, "test.txt.gz")
        expected_unzipped_file_path = os.path.join(
            self.syn.cache.cache_root_dir, "test.txt"
        )
        text_content = "This is plain text content."
        text_content_bytes = text_content.encode("utf-8")

        # WHEN I call `_unzip_gzipped_file` with a text file
        with patch("gzip.open") as mock_gzip_open, patch(
            "builtins.open"
        ) as mock_open_file, patch(
            "synapseclient.models.wiki.pprint.pp"
        ) as mock_pprint:
            mock_gzip_open.return_value.__enter__.return_value.read.return_value = (
                text_content_bytes
            )
            unzipped_file_path = self.wiki_page.unzip_gzipped_file(gzipped_file_path)

        # THEN the file should be unzipped correctly
        mock_gzip_open.assert_called_once_with(gzipped_file_path, "rb")
        mock_pprint.assert_not_called()
        mock_open_file.assert_called_once_with(
            unzipped_file_path, "wt", encoding="utf-8"
        )
        mock_open_file.return_value.__enter__.return_value.write.assert_called_once_with(
            text_content
        )
        assert unzipped_file_path == expected_unzipped_file_path

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

    @pytest.mark.parametrize(
        "file_name,expected",
        [
            ("test.txt", "test%2Etxt"),
            ("test.txt.gz", "test%2Etxt%2Egz"),
            ("test_1.txt", "test%5F1%2Etxt"),
        ],
        ids=[
            "file_name_with_one_dot",
            "file_name_with_multiple_dots",
            "file_name_with_dot_underscore",
        ],
    )
    def test_reformat_attachment_file_name(self, file_name: str, expected: str) -> None:
        # WHEN I call `_reformat_attachment_file_name` with a file name
        result = WikiPage.reformat_attachment_file_name(file_name)
        # THEN the result should be the reformatted file name
        assert result == expected

    @pytest.mark.parametrize(
        "file_name,expected",
        [
            ("test.png", False),
            ("test.jpg", False),
            ("test.jpeg", False),
            ("test.txt.gz", False),
            ("test.txt", True),
        ],
    )
    def test_should_gzip_file(self, file_name: str, expected: bool) -> None:
        # WHEN I call `_should_gzip_file` with a file name
        result = WikiPage._should_gzip_file(file_name)
        # THEN the result should match the expected value
        assert result == expected

    def test_should_gzip_file_with_invalid_content(self) -> None:
        # WHEN I call `_should_gzip_file` with invalid content (non-string)
        # THEN it should raise an AttributeError
        with pytest.raises(AttributeError):
            WikiPage._should_gzip_file(123)

    async def test_get_markdown_file_handle_success_with_markdown(self) -> WikiPage:
        with patch(
            "synapseclient.models.wiki.WikiPage._to_gzip_file",
            return_value=("test.txt.gz"),
        ) as mock_to_gzip_file, patch(
            "synapseclient.models.wiki.upload_file_handle",
            return_value={"id": "handle1"},
        ) as mock_upload, patch.object(
            self.syn.logger, "info"
        ) as mock_logger_info, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug, patch(
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
            mock_logger_info.assert_called_once_with(
                "Uploaded file handle handle1 for wiki page markdown."
            )
            mock_logger_debug.assert_called_once_with(
                "Deleted temp directory test.txt.gz"
            )
            # AND the temp gzipped file should be deleted
            assert mock_remove.call_count == 1

            # AND the result should be filled with the response
            expected_results = self.get_fresh_wiki_page()
            expected_results.markdown_file_handle_id = "handle1"
            assert results == expected_results

    async def test_get_markdown_file_handle_no_markdown(self) -> WikiPage:
        # GIVEN a WikiPage with no markdown
        wiki_page = WikiPage(
            id="wiki1",
            title="Test Wiki Page",
            attachments=["test_1.txt", "test_2.txt"],
        )
        with patch.object(self.syn.logger, "info") as mock_logger:
            # WHEN I call `_get_markdown_file_handle`
            results = await wiki_page._get_markdown_file_handle(synapse_client=self.syn)

            # THEN the result should be the same WikiPage
            assert mock_logger.call_count == 0
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
            self.syn.logger, "info"
        ) as mock_logger_info, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug, patch(
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
            mock_logger_info.assert_has_calls(
                [
                    call("Uploaded file handle handle1 for wiki page attachment."),
                    call("Uploaded file handle handle2 for wiki page attachment."),
                ]
            )
            mock_logger_debug.assert_has_calls(
                [
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
            expected_results = self.get_fresh_wiki_page()
            expected_results.attachment_file_handle_ids = expected_attachment_handles
            assert results == expected_results

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
            self.syn.logger, "info"
        ) as mock_logger_info, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug, patch(
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
            mock_logger_info.assert_called_once_with(
                "Uploaded file handle handle1 for wiki page attachment."
            )
            mock_logger_debug.assert_called_once_with(
                "Deleted temp directory /tmp/cache/test_1.txt.gz"
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
        ), patch(
            "synapseclient.models.wiki.upload_file_handle",
            return_value={"id": "handle1"},
        ), patch(
            "os.path.exists", return_value=False
        ), patch.object(
            self.syn.logger, "info"
        ) as mock_logger_info, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug, patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_attachment_file_handles`
            results = await wiki_page._get_attachment_file_handles(
                synapse_client=self.syn
            )

            # THEN the function should complete successfully
            assert results.attachment_file_handle_ids == ["handle1"]
            mock_logger_info.assert_called_once_with(
                "Uploaded file handle handle1 for wiki page attachment."
            )
            mock_logger_debug.assert_not_called()
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
        ), patch(
            "synapseclient.models.wiki.upload_file_handle",
            side_effect=Exception("Upload failed"),
        ), patch(
            "os.path.exists", return_value=True
        ), patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug, patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `_get_attachment_file_handles`
            # THEN it should raise the exception
            with pytest.raises(Exception, match="Upload failed"):
                await wiki_page._get_attachment_file_handles(synapse_client=self.syn)

            # AND cleanup should still be attempted
            mock_remove.assert_called_once_with("/tmp/cache/test_1.txt.gz")
            mock_logger_debug.assert_called_once_with(
                "Deleted temp directory /tmp/cache/test_1.txt.gz"
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
                await wiki_page._determine_wiki_action(synapse_client=self.syn)
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
            assert (
                await wiki_page._determine_wiki_action(synapse_client=self.syn)
                == "create_root_wiki_page"
            )
            mock_get_header.assert_called_once_with(
                owner_id="syn123", synapse_client=self.syn
            )

    async def test_determine_wiki_action_create_sub(self) -> None:
        async def mock_get_async(*args, **kwargs) -> AsyncGenerator[WikiHeader, None]:
            yield WikiHeader(id="wiki1", title="Test Wiki Page")

        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            side_effect=mock_get_async,
        ) as mock_get_header:
            # GIVEN a WikiPage with a parent_id
            wiki_page = WikiPage(
                owner_id="syn123",
                title="Test Wiki Page",
                parent_id="parent_wiki",
            )
            # WHEN I call `determine_wiki_action`
            # THEN it should return "create_sub_wiki_page"
            assert (
                await wiki_page._determine_wiki_action(synapse_client=self.syn)
                == "create_sub_wiki_page"
            )
            mock_get_header.assert_not_called()

    async def test_determine_wiki_action_update_existing_root(self) -> None:
        async def mock_get_async(*args, **kwargs) -> AsyncGenerator[WikiHeader, None]:
            yield WikiHeader(id="wiki1", title="Test Wiki Page")

        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            side_effect=mock_get_async,
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
                await wiki_page._determine_wiki_action(synapse_client=self.syn)
                == "update_existing_wiki_page"
            )
            mock_get_header.assert_called_once_with(
                owner_id="syn123", synapse_client=self.syn
            )

    async def test_determine_wiki_action_update_existing_without_passing_id(
        self,
    ) -> None:
        async def mock_get_async(*args, **kwargs) -> AsyncGenerator[WikiHeader, None]:
            yield WikiHeader(id="wiki1", title="Test Wiki Page")

        with patch(
            "synapseclient.models.wiki.WikiHeader.get_async",
            side_effect=mock_get_async,
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
                await wiki_page._determine_wiki_action(synapse_client=self.syn)
                mock_get_header.assert_called_once_with(
                    owner_id="syn123", synapse_client=self.syn
                )

    async def test_store_async_new_root_wiki_success(self) -> None:
        # GIVEN a WikiPage
        new_wiki_page = self.get_fresh_wiki_page()
        new_wiki_page.parent_id = None

        # AND mock the post_wiki_page response
        post_api_response = copy.deepcopy(self.api_response)
        post_api_response["parentId"] = None
        post_api_response["markdownFileHandleId"] = "markdown_file_handle_id"
        post_api_response["attachmentFileHandleIds"] = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # Create mock WikiPage objects with the expected file handle IDs for markdown
        mock_wiki_with_markdown = copy.deepcopy(new_wiki_page)
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

            results = await new_wiki_page.store_async(synapse_client=self.syn)

            # THEN log messages should be printed
            assert mock_logger.call_count == 2
            mock_logger.assert_has_calls(
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
            new_wiki_page.markdown_file_handle_id = "markdown_file_handle_id"
            new_wiki_page.attachment_file_handle_ids = [
                "attachment_file_handle_id_1",
                "attachment_file_handle_id_2",
            ]

            # AND the wiki should be created
            mock_post_wiki.assert_called_once_with(
                owner_id="syn123",
                request=new_wiki_page.to_synapse_request(),
                synapse_client=self.syn,
            )
            # AND the result should be filled with the response
            expected_results = new_wiki_page.fill_from_dict(post_api_response)
            assert results == expected_results

    async def test_store_async_update_existing_wiki_success(self) -> None:
        # GIVEN a WikiPage
        new_wiki_page = self.get_fresh_wiki_page()
        new_wiki_page.title = "Updated Wiki Page"
        new_wiki_page.parent_id = None
        new_wiki_page.etag = None

        # AND mock the get_wiki_page response
        mock_get_wiki_response = copy.deepcopy(self.api_response)
        mock_get_wiki_response["parentWikiId"] = None
        mock_get_wiki_response["markdown"] = None
        mock_get_wiki_response["attachments"] = []
        mock_get_wiki_response["markdownFileHandleId"] = None
        mock_get_wiki_response["attachmentFileHandleIds"] = []

        # Create mock WikiPage objects
        mock_wiki_with_markdown = self.get_fresh_wiki_page()
        mock_wiki_with_markdown.title = "Updated Wiki Page"
        mock_wiki_with_markdown.parent_id = None
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
        mock_put_wiki_response["title"] = "Updated Wiki Page"
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
            results = await new_wiki_page.store_async(synapse_client=self.syn)
            # THEN the existing wiki should be retrieved
            mock_get_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the wiki should be updated after merging dataclass objects
            new_wiki_page.etag = "etag123"
            new_wiki_page.created_on = "2023-01-01T00:00:00.000Z"
            new_wiki_page.created_by = "12345"
            new_wiki_page.modified_on = "2023-01-02T00:00:00.000Z"
            new_wiki_page.modified_by = "12345"
            new_wiki_page.markdown_file_handle_id = "markdown_file_handle_id"
            new_wiki_page.attachment_file_handle_ids = [
                "attachment_file_handle_id_1",
                "attachment_file_handle_id_2",
            ]
            mock_put_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                request=new_wiki_page.to_synapse_request(),
                synapse_client=self.syn,
            )

            # AND log messages should be printed
            assert mock_logger.call_count == 2
            mock_logger.assert_has_calls(
                [
                    call(
                        "A wiki page already exists within the owner. Update the existing wiki page."
                    ),
                    call(
                        f"Updated wiki page: {mock_put_wiki_response['title']} with ID: {self.api_response['id']}."
                    ),
                ]
            )
            # AND the result should be filled with the response
            expected_results = new_wiki_page.fill_from_dict(mock_put_wiki_response)
            assert results == expected_results

    async def test_store_async_create_sub_wiki_success(self) -> None:
        # AND mock the post_wiki_page response
        post_api_response = copy.deepcopy(self.api_response)
        post_api_response["markdownFileHandleId"] = "markdown_file_handle_id"
        post_api_response["attachmentFileHandleIds"] = [
            "attachment_file_handle_id_1",
            "attachment_file_handle_id_2",
        ]

        # Create mock WikiPage objects with the expected file handle IDs for markdown
        mock_wiki_with_markdown = self.get_fresh_wiki_page()
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
            mock_logger.assert_has_calls(
                [
                    call("Creating sub-wiki page under parent ID: parent_wiki"),
                    call(
                        f"Created sub-wiki page: {post_api_response['title']} with ID: {post_api_response['id']} under parent: parent_wiki"
                    ),
                ]
            )

            # Update the wiki_page with file handle ids for validation
            new_wiki_page = self.get_fresh_wiki_page()
            new_wiki_page.markdown_file_handle_id = "markdown_file_handle_id"
            new_wiki_page.attachment_file_handle_ids = [
                "attachment_file_handle_id_1",
                "attachment_file_handle_id_2",
            ]

            # AND the wiki should be created
            mock_post_wiki.assert_called_once_with(
                owner_id="syn123",
                request=new_wiki_page.to_synapse_request(),
                synapse_client=self.syn,
            )

            # AND the result should be filled with the response
            expected_results = new_wiki_page.fill_from_dict(post_api_response)
            assert results == expected_results

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
        with patch(
            "synapseclient.models.wiki.put_wiki_version"
        ) as mocked_put, pytest.raises(ValueError, match=expected_error):
            await wiki_page.restore_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_put.assert_not_called()

    async def test_restore_async_success(self) -> None:
        # GIVEN a WikiPage
        new_wiki_page = self.get_fresh_wiki_page()
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
                request=new_wiki_page.to_synapse_request(),
                synapse_client=self.syn,
            )
            # AND the result should be filled with the response
            expected_results = new_wiki_page.fill_from_dict(self.api_response)
            assert results == expected_results

    async def test_get_async_by_id_success(self) -> None:
        # GIVEN a WikiPage object with id
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
        )

        # AND a mock response
        with patch("synapseclient.models.wiki.get_wiki_page") as mock_get_wiki:
            mock_get_wiki.return_value = self.api_response

            # WHEN I call `get_async`
            results = await wiki.get_async(synapse_client=self.syn)

            # THEN the API should be called with correct parameters
            mock_get_wiki.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version=None,
                synapse_client=self.syn,
            )

            # AND the result should be filled with the response
            wiki_page = self.get_fresh_wiki_page()
            expected_wiki = wiki_page.fill_from_dict(self.api_response)
            expected_wiki.attachments = wiki.attachments
            expected_wiki.markdown = wiki.markdown
            expected_wiki.wiki_version = wiki.wiki_version
            assert results == expected_wiki

    async def test_get_async_by_title_success(self) -> None:
        # GIVEN a WikiPage object with title but no id
        wiki = WikiPage(
            title="Test Wiki",
            owner_id="syn123",
        )

        # AND mock responses
        mock_responses = [
            {"id": "wiki1", "title": "Test Wiki", "parentId": None},
            {"id": "wiki2", "title": "Test Wiki 2", "parentId": None},
        ]

        # Create an async generator mock
        async def mock_async_generator(values):
            for item in values:
                yield item

        with patch(
            "synapseclient.models.wiki.get_wiki_header_tree",
            return_value=mock_async_generator(mock_responses),
        ) as mock_get_header_tree, patch(
            "synapseclient.models.wiki.get_wiki_page", return_value=self.api_response
        ) as mock_get_wiki:
            # WHEN I call `get_async`
            results = await wiki.get_async(synapse_client=self.syn)

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
            wiki_page = self.get_fresh_wiki_page()
            expected_wiki = wiki_page.fill_from_dict(self.api_response)
            expected_wiki.attachments = wiki.attachments
            expected_wiki.markdown = wiki.markdown
            expected_wiki.wiki_version = wiki.wiki_version
            assert results == expected_wiki

    async def test_get_async_by_title_not_found(self) -> None:
        # GIVEN a WikiPage object with title but no id
        wiki = WikiPage(
            title="Non-existent Wiki",
            owner_id="syn123",
        )

        # AND mock responses that don't contain the title
        mock_responses = [{"id": "wiki1", "title": "Different Wiki", "parentId": None}]

        # Create an async generator mock
        async def mock_async_generator(values):
            for item in values:
                yield item

        with patch(
            "synapseclient.models.wiki.get_wiki_header_tree",
            return_value=mock_async_generator(mock_responses),
        ) as mock_get_header_tree:
            # WHEN I call `get_async`
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="No wiki page found with title: Non-existent Wiki"
            ):
                await wiki.get_async(synapse_client=self.syn)
                mock_get_header_tree.assert_called_once_with(
                    owner_id="syn123",
                    synapse_client=self.syn,
                )

    @pytest.mark.parametrize(
        "wiki_page, expected_error",
        [
            (
                WikiPage(id="wiki1"),
                "Must provide owner_id to delete a wiki page.",
            ),
            (
                WikiPage(owner_id="syn123"),
                "Must provide id to delete a wiki page.",
            ),
        ],
    )
    async def test_delete_async_missing_required_parameters(
        self, wiki_page, expected_error
    ) -> None:
        # WHEN I call `delete_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.delete_wiki_page"
        ) as mocked_delete, pytest.raises(ValueError, match=expected_error):
            await wiki_page.delete_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_delete.assert_not_called()

    async def test_delete_async_success(self) -> None:
        # WHEN I call `delete_async`
        # THEN it should call the API with the correct parameters
        with patch("synapseclient.models.wiki.delete_wiki_page") as mocked_delete:
            await self.wiki_page.delete_async(synapse_client=self.syn)
            mocked_delete.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                synapse_client=self.syn,
            )

    @pytest.mark.parametrize(
        "wiki_page, expected_error",
        [
            (
                WikiPage(id="wiki1"),
                "Must provide owner_id to get attachment handles.",
            ),
            (
                WikiPage(owner_id="syn123"),
                "Must provide id to get attachment handles.",
            ),
        ],
    )
    async def test_get_attachment_handles_async_missing_required_parameters(
        self, wiki_page, expected_error
    ) -> None:
        # WHEN I call `get_attachment_handles_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.get_attachment_handles"
        ) as mocked_get, pytest.raises(ValueError, match=expected_error):
            await wiki_page.get_attachment_handles_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_get.assert_not_called()

    async def test_get_attachment_handles_async_success(self) -> None:
        # mock responses
        mock_handles = [{"id": "handle1", "fileName": "test.txt"}]
        with patch(
            "synapseclient.models.wiki.get_attachment_handles",
            return_value=mock_handles,
        ) as mock_get_handles:
            # WHEN I call `get_attachment_handles_async`
            results = await self.wiki_page.get_attachment_handles_async(
                synapse_client=self.syn
            )

            # THEN the API should be called with correct parameters
            mock_get_handles.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )
            # AND the result should be the handles
            assert results == mock_handles

    @pytest.mark.parametrize(
        "wiki_page, file_name, expected_error",
        [
            (
                WikiPage(id="wiki1"),
                "test.txt",
                "Must provide owner_id to get attachment URL.",
            ),
            (
                WikiPage(owner_id="syn123"),
                "test.txt",
                "Must provide id to get attachment URL.",
            ),
            (
                WikiPage(owner_id="syn123", id="wiki1"),
                None,
                "Must provide file_name to get attachment URL.",
            ),
        ],
    )
    async def test_get_attachment_async_missing_required_parameters(
        self, file_name, wiki_page, expected_error
    ) -> None:
        # WHEN I call `get_attachment_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.get_attachment_url"
        ) as mocked_get, pytest.raises(ValueError, match=expected_error):
            await wiki_page.get_attachment_async(
                file_name=file_name,
                synapse_client=self.syn,
            )
            # THEN the API should not be called
            mocked_get.assert_not_called()

    @pytest.mark.parametrize("file_size", [8 * 1024 * 1024 - 1, 8 * 1024 * 1024 + 1])
    async def test_get_attachment_async_download_file_success(self, file_size) -> None:
        # AND mock responses
        mock_attachment_url = "https://example.com/attachment.txt"
        mock_filehandle_dict = {
            "list": [
                {
                    "fileName": "test.txt.gz",
                    "contentSize": str(file_size),
                }
            ]
        }

        with patch(
            "synapseclient.models.wiki.get_attachment_url",
            return_value=mock_attachment_url,
        ) as mock_get_url, patch(
            "synapseclient.models.wiki.get_attachment_handles",
            return_value=mock_filehandle_dict,
        ) as mock_get_handles, patch(
            "synapseclient.models.wiki.download_from_url",
            return_value="/tmp/download/test.txt.gz",
        ) as mock_download_from_url, patch(
            "synapseclient.models.wiki.download_from_url_multi_threaded",
            return_value="/tmp/download/test.txt.gz",
        ) as mock_download_from_url_multi_threaded, patch(
            "synapseclient.models.wiki._pre_signed_url_expiration_time",
            return_value="2030-01-01T00:00:00.000Z",
        ) as mock_expiration_time, patch.object(
            self.syn.logger, "info"
        ) as mock_logger_info, patch(
            "os.remove"
        ) as mock_remove, patch(
            "synapseclient.models.wiki.WikiPage.unzip_gzipped_file"
        ) as mock_unzip_gzipped_file, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug:
            # WHEN I call `get_attachment_async` with download_file=True
            result = await self.wiki_page.get_attachment_async(
                file_name="test.txt",
                download_file=True,
                download_location="/tmp/download",
                synapse_client=self.syn,
            )

            # THEN the attachment URL should be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                file_name="test.txt.gz",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the attachment handles should be retrieved
            mock_get_handles.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the expiration time should be calculated
            mock_expiration_time.assert_called_once_with(mock_attachment_url)

            # AND the appropriate download method should be called based on file size
            if file_size < 8 * 1024 * 1024:
                # Single-threaded download for files smaller than 8 MiB
                mock_download_from_url.assert_called_once_with(
                    url=mock_attachment_url,
                    destination="/tmp/download",
                    url_is_presigned=True,
                    synapse_client=self.syn,
                )
                mock_download_from_url_multi_threaded.assert_not_called()

            else:
                # construct a mock presigned url info
                mock_presigned_url_info = PresignedUrlInfo(
                    file_name="test.txt.gz",
                    url=mock_attachment_url,
                    expiration_utc="2030-01-01T00:00:00.000Z",
                )
                # Multi-threaded download for files larger than or equal to 8 MiB
                mock_download_from_url_multi_threaded.assert_called_once_with(
                    presigned_url=mock_presigned_url_info,
                    destination="/tmp/download",
                    synapse_client=self.syn,
                )
                mock_download_from_url.assert_not_called()

            # AND debug log should be called once (only the general one)
            mock_logger_info.assert_called_once_with(
                f"Downloaded file test.txt to {result}."
            )
            # AND the file should be unzipped
            mock_unzip_gzipped_file.assert_called_once_with("/tmp/download/test.txt.gz")
            # AND the gzipped file should be removed
            mock_remove.assert_called_once_with("/tmp/download/test.txt.gz")
            # AND debug log should be called
            mock_logger_debug.assert_called_once_with(
                "Removed the gzipped file /tmp/download/test.txt.gz."
            )

    async def test_get_attachment_async_no_file_download(self) -> None:
        with patch(
            "synapseclient.models.wiki.get_attachment_url",
            return_value="https://example.com/attachment.txt",
        ) as mock_get_url:
            # WHEN I call `get_attachment_async` with download_file=True but no download_location
            # THEN it should return the attachment URL
            results = await self.wiki_page.get_attachment_async(
                file_name="test.txt",
                download_file=False,
                synapse_client=self.syn,
            )
            # AND the result should be the attachment URL
            assert results == "https://example.com/attachment.txt"

    async def test_get_attachment_async_download_file_missing_location(self) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
            wiki_version="0",
        )

        # AND a mock attachment URL
        mock_attachment_url = "https://example.com/attachment.txt"

        with patch(
            "synapseclient.models.wiki.get_attachment_url",
            return_value=mock_attachment_url,
        ) as mock_get_url, patch(
            "synapseclient.models.wiki.get_attachment_handles"
        ) as mock_get_handles:
            # WHEN I call `get_attachment_async` with download_file=True but no download_location
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="Must provide download_location to download a file."
            ):
                await wiki.get_attachment_async(
                    file_name="test.txt",
                    download_file=True,
                    download_location=None,
                    synapse_client=self.syn,
                )

            # AND the attachment URL should still be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                file_name="test.txt.gz",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the attachment handles should not be retrieved
            mock_get_handles.assert_not_called()

    @pytest.mark.parametrize(
        "wiki_page, file_name, expected_error",
        [
            (
                WikiPage(id="wiki1"),
                "test.txt",
                "Must provide owner_id to get attachment preview URL.",
            ),
            (
                WikiPage(owner_id="syn123"),
                "test.txt",
                "Must provide id to get attachment preview URL.",
            ),
            (
                WikiPage(owner_id="syn123", id="wiki1"),
                None,
                "Must provide file_name to get attachment preview URL.",
            ),
        ],
    )
    async def test_get_attachment_preview_async_missing_required_parameters(
        self, file_name, wiki_page, expected_error
    ) -> None:
        # WHEN I call `get_attachment_preview_url_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.get_attachment_preview_url"
        ) as mocked_get, pytest.raises(ValueError, match=expected_error):
            await wiki_page.get_attachment_preview_async(
                file_name=file_name,
                synapse_client=self.syn,
            )
            # THEN the API should not be called
            mocked_get.assert_not_called()

    @pytest.mark.parametrize("file_size", [8 * 1024 * 1024 - 1, 8 * 1024 * 1024 + 1])
    async def test_get_attachment_preview_async_download_file_success(
        self, file_size
    ) -> None:
        # Mock responses
        mock_attachment_url = "https://example.com/attachment.txt"
        mock_filehandle_dict = {
            "list": [
                {
                    "fileName": "test.txt.gz",
                    "contentSize": str(file_size),
                }
            ]
        }

        with patch(
            "synapseclient.models.wiki.get_attachment_preview_url",
            return_value=mock_attachment_url,
        ) as mock_get_url, patch(
            "synapseclient.models.wiki.get_attachment_handles",
            return_value=mock_filehandle_dict,
        ) as mock_get_handles, patch(
            "synapseclient.models.wiki.download_from_url",
            return_value="/tmp/download/test.txt.gz",
        ) as mock_download_from_url, patch(
            "synapseclient.models.wiki.download_from_url_multi_threaded",
            return_value="/tmp/download/test.txt.gz",
        ) as mock_download_from_url_multi_threaded, patch(
            "synapseclient.models.wiki._pre_signed_url_expiration_time",
            return_value="2030-01-01T00:00:00.000Z",
        ) as mock_expiration_time, patch.object(
            self.syn.logger, "info"
        ) as mock_logger_info:
            # WHEN I call `get_attachment_async` with download_file=True
            result = await self.wiki_page.get_attachment_preview_async(
                file_name="test.txt",
                download_file=True,
                download_location="/tmp/download",
                synapse_client=self.syn,
            )

            # THEN the attachment URL should be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                file_name="test.txt.gz",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the attachment handles should be retrieved
            mock_get_handles.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the expiration time should be calculated
            mock_expiration_time.assert_called_once_with(mock_attachment_url)

            # AND the appropriate download method should be called based on file size
            if file_size < 8 * 1024 * 1024:
                # Single-threaded download for files smaller than 8 MiB
                mock_download_from_url.assert_called_once_with(
                    url=mock_attachment_url,
                    destination="/tmp/download",
                    url_is_presigned=True,
                    synapse_client=self.syn,
                )
                mock_download_from_url_multi_threaded.assert_not_called()

            else:
                # construct a mock presigned url info
                mock_presigned_url_info = PresignedUrlInfo(
                    file_name="test.txt.gz",
                    url=mock_attachment_url,
                    expiration_utc="2030-01-01T00:00:00.000Z",
                )
                # Multi-threaded download for files larger than or equal to 8 MiB
                mock_download_from_url_multi_threaded.assert_called_once_with(
                    presigned_url=mock_presigned_url_info,
                    destination="/tmp/download",
                    synapse_client=self.syn,
                )
                mock_download_from_url.assert_not_called()

                # AND debug log should be called once (only the general one)
            mock_logger_info.assert_called_once_with(
                f"Downloaded the preview file test.txt to {result}."
            )

    async def test_get_attachment_preview_async_no_file_download(self) -> None:
        with patch(
            "synapseclient.models.wiki.get_attachment_preview_url",
            return_value="https://example.com/attachment.txt",
        ) as mock_get_url:
            # WHEN I call `get_attachment_preview_async` with download_file=False
            # THEN it should return the attachment URL
            results = await self.wiki_page.get_attachment_preview_async(
                file_name="test.txt",
                download_file=False,
                synapse_client=self.syn,
            )
            # AND the result should be the attachment URL
            assert results == "https://example.com/attachment.txt"

    async def test_get_attachment_preview_async_download_file_missing_location(
        self,
    ) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
            wiki_version="0",
        )

        # AND a mock attachment URL
        mock_attachment_url = "https://example.com/attachment.txt"

        with patch(
            "synapseclient.models.wiki.get_attachment_preview_url",
            return_value=mock_attachment_url,
        ) as mock_get_url, patch(
            "synapseclient.models.wiki.get_attachment_handles"
        ) as mock_get_handles:
            # WHEN I call `get_attachment_async` with download_file=True but no download_location
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="Must provide download_location to download a file."
            ):
                await wiki.get_attachment_preview_async(
                    file_name="test.txt",
                    download_file=True,
                    download_location=None,
                    synapse_client=self.syn,
                )

            # AND the attachment URL should still be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                file_name="test.txt.gz",
                wiki_version="0",
                synapse_client=self.syn,
            )
            # AND the attachment handles should not be retrieved
            mock_get_handles.assert_not_called()

    @pytest.mark.parametrize(
        "wiki_page, expected_error",
        [
            (
                WikiPage(id="wiki1"),
                "Must provide owner_id to get markdown URL.",
            ),
            (
                WikiPage(owner_id="syn123"),
                "Must provide id to get markdown URL.",
            ),
        ],
    )
    async def test_get_markdown_file_async_missing_required_parameters(
        self, wiki_page, expected_error
    ) -> None:
        # WHEN I call `get_markdown_async`
        # THEN it should raise ValueError
        with patch(
            "synapseclient.models.wiki.get_markdown_url"
        ) as mocked_get, pytest.raises(ValueError, match=expected_error):
            await wiki_page.get_markdown_file_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_get.assert_not_called()

    async def test_get_markdown_file_async_download_file_success(self) -> None:
        # Mock responses
        mock_markdown_url = "https://example.com/markdown.md.gz"

        with patch(
            "synapseclient.models.wiki.get_markdown_url",
            return_value=mock_markdown_url,
        ) as mock_get_url, patch(
            "synapseclient.models.wiki.download_from_url",
            return_value="/tmp/download/markdown.md.gz",
        ) as mock_download_from_url, patch(
            "synapseclient.models.wiki.WikiPage.unzip_gzipped_file"
        ) as mock_unzip_gzipped_file, patch.object(
            self.syn.logger, "info"
        ) as mock_logger_info, patch.object(
            self.syn.logger, "debug"
        ) as mock_logger_debug, patch(
            "os.remove"
        ) as mock_remove:
            # WHEN I call `get_markdown_async` with download_file=True
            result = await self.wiki_page.get_markdown_file_async(
                download_file=True,
                download_location="/tmp/download",
                synapse_client=self.syn,
            )

            # THEN the markdown URL should be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the file should be downloaded using single-threaded download
            mock_download_from_url.assert_called_once_with(
                url=mock_markdown_url,
                destination="/tmp/download",
                url_is_presigned=True,
                synapse_client=self.syn,
            )
            # AND the file should be unzipped
            mock_unzip_gzipped_file.assert_called_once_with(
                "/tmp/download/markdown.md.gz"
            )
            mock_logger_info.assert_called_once_with(
                f"Downloaded and unzipped the markdown file for wiki page wiki1 to {result}."
            )
            # AND the gzipped file should be removed
            mock_remove.assert_called_once_with("/tmp/download/markdown.md.gz")
            # AND debug log should be called
            mock_logger_debug.assert_called_once_with(
                f"Removed the gzipped file /tmp/download/markdown.md.gz."
            )

    async def test_get_markdown_file_async_no_file_download(self) -> None:
        with patch(
            "synapseclient.models.wiki.get_markdown_url",
            return_value="https://example.com/markdown.md",
        ) as mock_get_url:
            # WHEN I call `get_markdown_async` with download_file=False
            results = await self.wiki_page.get_markdown_file_async(
                download_file=False,
                synapse_client=self.syn,
            )

            # THEN the markdown URL should be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )

            # AND the result should be the markdown URL
            assert results == "https://example.com/markdown.md"

    async def test_get_markdown_file_async_download_file_missing_location(self) -> None:
        # GIVEN a WikiPage object
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
            wiki_version="0",
        )

        # AND a mock markdown URL
        mock_markdown_url = "https://example.com/markdown.md"

        with patch(
            "synapseclient.models.wiki.get_markdown_url",
            return_value=mock_markdown_url,
        ) as mock_get_url, patch(
            "synapseclient.models.wiki.get_attachment_handles"
        ) as mock_get_handles:
            # WHEN I call `get_markdown_async` with download_file=True but no download_location
            # THEN it should raise ValueError
            with pytest.raises(
                ValueError, match="Must provide download_location to download a file."
            ):
                await wiki.get_markdown_file_async(
                    download_file=True,
                    download_location=None,
                    synapse_client=self.syn,
                )

            # AND the markdown URL should still be retrieved
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="0",
                synapse_client=self.syn,
            )
            # AND the attachment handles should not be retrieved
            mock_get_handles.assert_not_called()

    async def test_get_markdown_file_async_with_different_wiki_version(self) -> None:
        # GIVEN a WikiPage object with a specific wiki version
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
            wiki_version="2",
        )

        with patch(
            "synapseclient.models.wiki.get_markdown_url",
            return_value="https://example.com/markdown_v2.md",
        ) as mock_get_url:
            # WHEN I call `get_markdown_async`
            results = await wiki.get_markdown_file_async(
                download_file=False,
                synapse_client=self.syn,
            )

            # THEN the markdown URL should be retrieved with the correct wiki version
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version="2",
                synapse_client=self.syn,
            )

            # AND the result should be the markdown URL
            assert results == "https://example.com/markdown_v2.md"

    async def test_get_markdown_file_async_with_none_wiki_version(self) -> None:
        # GIVEN a WikiPage object with None wiki version
        wiki = WikiPage(
            id="wiki1",
            owner_id="syn123",
            wiki_version=None,
        )

        with patch(
            "synapseclient.models.wiki.get_markdown_url",
            return_value="https://example.com/markdown_latest.md",
        ) as mock_get_url:
            # WHEN I call `get_markdown_async`
            results = await wiki.get_markdown_file_async(
                download_file=False,
                synapse_client=self.syn,
            )

            # THEN the markdown URL should be retrieved with None wiki version
            mock_get_url.assert_called_once_with(
                owner_id="syn123",
                wiki_id="wiki1",
                wiki_version=None,
                synapse_client=self.syn,
            )

            # AND the result should be the markdown URL
            assert results == "https://example.com/markdown_latest.md"
