"""Tests for the synapseclient.models.wiki classes."""

import contextlib
import copy
import os
import tempfile
from typing import Any, AsyncGenerator, Dict
from unittest.mock import ANY, AsyncMock, Mock, call, mock_open, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.download.download_async import PresignedUrlInfo
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models.wiki import (
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
    _collect_wiki_sub_tree_headers,
    _copy_wiki_pages,
    _ensure_destination_has_no_root_wiki,
    _get_existing_destination_wiki_page,
    _update_internal_links,
    _update_synapse_id_references,
    _validate_and_format_copy_inputs,
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
        with (
            patch(
                "synapseclient.models.wiki.put_wiki_order_hint",
                new_callable=AsyncMock,
                return_value=self.api_response,
            ) as mocked_put,
            pytest.raises(
                ValueError, match="Must provide owner_id to store wiki order hint."
            ),
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
        with (
            patch("synapseclient.models.wiki.get_wiki_order_hint") as mocked_get,
            pytest.raises(
                ValueError, match="Must provide owner_id to get wiki order hint."
            ),
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
        with (
            patch("synapseclient.models.wiki.get_wiki_history") as mocked_get,
            pytest.raises(
                ValueError, match="Must provide owner_id to get wiki history."
            ),
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
        with (
            patch("synapseclient.models.wiki.get_wiki_history") as mocked_get,
            pytest.raises(ValueError, match="Must provide id to get wiki history."),
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
        with (
            patch("synapseclient.models.wiki.get_wiki_header_tree") as mocked_get,
            pytest.raises(
                ValueError, match="Must provide owner_id to get wiki header tree."
            ),
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
        self.syn.cache.cache_root_dir = tempfile.gettempdir()

        # WHEN I call `_to_gzip_file` with a markdown string
        with (
            patch("builtins.open") as mock_open_file,
            patch("gzip.open") as mock_gzip_open,
            patch("os.path.exists", return_value=True),
        ):
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
        with (
            patch("os.path.isfile"),
            patch("gzip.open") as mock_gzip_open,
            patch("builtins.open") as mock_open_file,
        ):
            self.syn.cache.cache_root_dir = tempfile.gettempdir()
            markdown_file_path = "wiki_markdown_Test Wiki Page.md.gz"

            # WHEN I call `_to_gzip_file` with a gzipped file
            file_path = self.wiki_page._to_gzip_file(markdown_file_path, self.syn)
            mock_open_file.assert_not_called()
            mock_gzip_open.assert_not_called()
            assert file_path == markdown_file_path

    def test_to_gzip_file_with_non_gzipped_file(self) -> None:
        self.syn.cache.cache_root_dir = tempfile.gettempdir()

        # WHEN I call `_to_gzip_file` with a file path
        with (
            patch("os.path.isfile", return_value=True),
            patch(
                "builtins.open", new=mock_open(read_data=b"test content")
            ) as mock_open_file,
            patch("gzip.open") as mock_gzip_open,
            patch("os.path.exists", return_value=True),
        ):
            test_file_path = os.path.join("file_path", "test.txt")
            file_path = self.wiki_page._to_gzip_file(test_file_path, self.syn)

            # THEN the file should be processed
            mock_open_file.assert_called_once_with(test_file_path, "rb")
            mock_gzip_open.assert_called_once_with(
                os.path.join(
                    self.syn.cache.cache_root_dir, "wiki_content", "test.txt.gz"
                ),
                "wb",
            )
            gzip_handle = mock_gzip_open.return_value.__enter__.return_value
            open_handle = mock_open_file.return_value.__enter__.return_value
            gzip_handle.writelines.assert_called_once_with(open_handle)
            assert file_path == os.path.join(
                self.syn.cache.cache_root_dir, "wiki_content", "test.txt.gz"
            )

    def test_to_gzip_file_with_invalid_content(self) -> None:
        # WHEN I call `_to_gzip_file` with invalid content type
        # THEN it should raise SyntaxError
        with pytest.raises(SyntaxError, match="Expected a string, got int"):
            self.wiki_page._to_gzip_file(123, self.syn)

    def test_unzip_gzipped_file_with_markdown(self) -> None:
        self.syn.cache.cache_root_dir = tempfile.gettempdir()

        gzipped_file_path = os.path.join(self.syn.cache.cache_root_dir, "test.md.gz")
        expected_unzipped_file_path = os.path.join(
            self.syn.cache.cache_root_dir, "test.md"
        )
        markdown_content = "# Test Markdown\n\nThis is a test."
        markdown_content_bytes = markdown_content.encode("utf-8")

        # WHEN I call `_unzip_gzipped_file` with a binary file
        with (
            patch("gzip.open") as mock_gzip_open,
            patch("builtins.open") as mock_open_file,
            patch("pprint.pp") as mock_pprint,
        ):
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
        self.syn.cache.cache_root_dir = tempfile.gettempdir()

        gzipped_file_path = os.path.join(self.syn.cache.cache_root_dir, "test.bin.gz")
        expected_unzipped_file_path = os.path.join(
            self.syn.cache.cache_root_dir, "test.bin"
        )
        binary_content = b"\x00\x01\x02\x03\xff\xfe\xfd"

        # WHEN I call `_unzip_gzipped_file` with a binary file
        with (
            patch("gzip.open") as mock_gzip_open,
            patch("builtins.open") as mock_open_file,
            patch("pprint.pp") as mock_pprint,
        ):
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
        self.syn.cache.cache_root_dir = tempfile.gettempdir()

        gzipped_file_path = os.path.join(self.syn.cache.cache_root_dir, "test.txt.gz")
        expected_unzipped_file_path = os.path.join(
            self.syn.cache.cache_root_dir, "test.txt"
        )
        text_content = "This is plain text content."
        text_content_bytes = text_content.encode("utf-8")

        # WHEN I call `_unzip_gzipped_file` with a text file
        with (
            patch("gzip.open") as mock_gzip_open,
            patch("builtins.open") as mock_open_file,
            patch("synapseclient.models.wiki.pprint.pp") as mock_pprint,
        ):
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
        with (
            patch(
                "synapseclient.models.wiki.WikiPage._to_gzip_file",
                return_value=("test.txt.gz"),
            ) as mock_to_gzip_file,
            patch(
                "synapseclient.models.wiki.upload_file_handle",
                return_value={"id": "handle1"},
            ) as mock_upload,
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
            patch("os.path.exists", return_value=True),
            patch("os.remove") as mock_remove,
        ):
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
                "[syn123:wiki1]: Uploaded file handle handle1 for wiki page markdown."
            )
            mock_logger_debug.assert_called_once_with(
                "[syn123:wiki1]: Deleted temp directory test.txt.gz"
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

        with (
            patch(
                "synapseclient.models.wiki.WikiPage._to_gzip_file",
                side_effect=mock_to_gzip_file_responses,
            ) as mock_to_gzip_file,
            patch(
                "synapseclient.models.wiki.upload_file_handle",
                side_effect=mock_upload_responses,
            ) as mock_upload,
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
            patch("os.path.exists", return_value=True),
            patch("os.remove") as mock_remove,
        ):
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
                    call(
                        "[syn123:wiki1]: Uploaded file handle handle1 for wiki page attachment."
                    ),
                    call(
                        "[syn123:wiki1]: Uploaded file handle handle2 for wiki page attachment."
                    ),
                ]
            )
            mock_logger_debug.assert_has_calls(
                [
                    call(
                        "[syn123:wiki1]: Deleted temp directory /tmp/cache1/test_1.txt.gz"
                    ),
                    call(
                        "[syn123:wiki1]: Deleted temp directory /tmp/cache1/test_2.txt.gz"
                    ),
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

        with (
            patch(
                "synapseclient.models.wiki.WikiPage._to_gzip_file",
                return_value=("/tmp/cache/test_1.txt.gz"),
            ) as mock_to_gzip_file,
            patch(
                "synapseclient.models.wiki.upload_file_handle",
                return_value={"id": "handle1"},
            ) as mock_upload,
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
            patch("os.path.exists", return_value=True),
            patch("os.remove") as mock_remove,
        ):
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
                "[syn123:wiki1]: Uploaded file handle handle1 for wiki page attachment."
            )
            mock_logger_debug.assert_called_once_with(
                "[syn123:wiki1]: Deleted temp directory /tmp/cache/test_1.txt.gz"
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

        with (
            patch(
                "synapseclient.models.wiki.WikiPage._to_gzip_file",
                return_value=("/tmp/cache/test_1.txt.gz"),
            ),
            patch(
                "synapseclient.models.wiki.upload_file_handle",
                return_value={"id": "handle1"},
            ),
            patch("os.path.exists", return_value=False),
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
            patch("os.remove") as mock_remove,
        ):
            # WHEN I call `_get_attachment_file_handles`
            results = await wiki_page._get_attachment_file_handles(
                synapse_client=self.syn
            )

            # THEN the function should complete successfully
            assert results.attachment_file_handle_ids == ["handle1"]
            mock_logger_info.assert_called_once_with(
                "[syn123:wiki1]: Uploaded file handle handle1 for wiki page attachment."
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

        with (
            patch(
                "synapseclient.models.wiki.WikiPage._to_gzip_file",
                return_value=("/tmp/cache/test_1.txt.gz"),
            ),
            patch(
                "synapseclient.models.wiki.upload_file_handle",
                side_effect=Exception("Upload failed"),
            ),
            patch("os.path.exists", return_value=True),
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
            patch("os.remove") as mock_remove,
        ):
            # WHEN I call `_get_attachment_file_handles`
            # THEN it should raise the exception
            with pytest.raises(Exception, match="Upload failed"):
                await wiki_page._get_attachment_file_handles(synapse_client=self.syn)

            # AND cleanup should still be attempted
            mock_remove.assert_called_once_with("/tmp/cache/test_1.txt.gz")
            mock_logger_debug.assert_called_once_with(
                "[syn123:wiki1]: Deleted temp directory /tmp/cache/test_1.txt.gz"
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
        with (
            patch(
                "synapseclient.models.wiki.WikiPage._determine_wiki_action",
                return_value="create_root_wiki_page",
            ),
            patch(
                "synapseclient.models.wiki.WikiPage._get_markdown_file_handle",
                return_value=mock_wiki_with_markdown,
            ),
            patch(
                "synapseclient.models.wiki.WikiPage._get_attachment_file_handles",
                return_value=mock_wiki_with_attachments,
            ),
            patch(
                "synapseclient.models.wiki.post_wiki_page",
                return_value=post_api_response,
            ) as mock_post_wiki,
            patch.object(self.syn.logger, "info") as mock_logger,
        ):
            # WHEN I call `store_async`

            results = await new_wiki_page.store_async(synapse_client=self.syn)

            # THEN log messages should be printed
            assert mock_logger.call_count == 2
            mock_logger.assert_has_calls(
                [
                    call(
                        "[syn123]: No wiki page exists within the owner. Create a new wiki page."
                    ),
                    call(
                        f"[syn123]: Created wiki page: {post_api_response['title']} with ID: {post_api_response['id']}."
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
        with (
            patch(
                "synapseclient.models.wiki.WikiPage._determine_wiki_action",
                return_value="update_existing_wiki_page",
            ),
            patch(
                "synapseclient.models.wiki.WikiPage._get_markdown_file_handle",
                return_value=mock_wiki_with_markdown,
            ),
            patch(
                "synapseclient.models.wiki.WikiPage._get_attachment_file_handles",
                return_value=mock_wiki_with_attachments,
            ),
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                return_value=mock_get_wiki_response,
            ) as mock_get_wiki,
            patch(
                "synapseclient.models.wiki.put_wiki_page",
                return_value=mock_put_wiki_response,
            ) as mock_put_wiki,
            patch.object(self.syn.logger, "info") as mock_logger,
        ):
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
                        "[syn123:wiki1]: A wiki page already exists within the owner. Update the existing wiki page."
                    ),
                    call(
                        f"[syn123]: Updated wiki page: {mock_put_wiki_response['title']} with ID: {self.api_response['id']}."
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
        with (
            patch(
                "synapseclient.models.wiki.WikiPage._determine_wiki_action",
                return_value="create_sub_wiki_page",
            ),
            patch(
                "synapseclient.models.wiki.WikiPage._get_markdown_file_handle",
                return_value=mock_wiki_with_markdown,
            ),
            patch(
                "synapseclient.models.wiki.WikiPage._get_attachment_file_handles",
                return_value=mock_wiki_with_attachments,
            ),
            patch(
                "synapseclient.models.wiki.post_wiki_page",
                return_value=post_api_response,
            ) as mock_post_wiki,
            patch.object(self.syn.logger, "info") as mock_logger,
        ):
            # WHEN I call `store_async`
            results = await self.wiki_page.store_async(synapse_client=self.syn)

            # THEN log messages should be printed
            assert mock_logger.call_count == 2
            mock_logger.assert_has_calls(
                [
                    call(
                        "[syn123]: Creating sub-wiki page under parent ID: parent_wiki"
                    ),
                    call(
                        f"[syn123]: Created sub-wiki page: {post_api_response['title']} with ID: {post_api_response['id']} under parent: parent_wiki"
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
        with (
            patch("synapseclient.models.wiki.put_wiki_version") as mocked_put,
            pytest.raises(ValueError, match=expected_error),
        ):
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

        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                return_value=mock_async_generator(mock_responses),
            ) as mock_get_header_tree,
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                return_value=self.api_response,
            ) as mock_get_wiki,
        ):
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
        with (
            patch("synapseclient.models.wiki.delete_wiki_page") as mocked_delete,
            pytest.raises(ValueError, match=expected_error),
        ):
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
        with (
            patch("synapseclient.models.wiki.get_attachment_handles") as mocked_get,
            pytest.raises(ValueError, match=expected_error),
        ):
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
        with (
            patch("synapseclient.models.wiki.get_attachment_url") as mocked_get,
            pytest.raises(ValueError, match=expected_error),
        ):
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

        with (
            patch(
                "synapseclient.models.wiki.get_attachment_url",
                return_value=mock_attachment_url,
            ) as mock_get_url,
            patch(
                "synapseclient.models.wiki.get_attachment_handles",
                return_value=mock_filehandle_dict,
            ) as mock_get_handles,
            patch(
                "synapseclient.models.wiki.download_from_url",
                return_value="/tmp/download/test.txt.gz",
            ) as mock_download_from_url,
            patch(
                "synapseclient.models.wiki.download_from_url_multi_threaded",
                return_value="/tmp/download/test.txt.gz",
            ) as mock_download_from_url_multi_threaded,
            patch(
                "synapseclient.models.wiki._pre_signed_url_expiration_time",
                return_value="2030-01-01T00:00:00.000Z",
            ) as mock_expiration_time,
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch("os.remove") as mock_remove,
            patch(
                "synapseclient.models.wiki.WikiPage.unzip_gzipped_file"
            ) as mock_unzip_gzipped_file,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
        ):
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
                f"[syn123:wiki1]: Downloaded file test.txt to {result}."
            )
            # AND the file should be unzipped
            mock_unzip_gzipped_file.assert_called_once_with("/tmp/download/test.txt.gz")
            # AND the gzipped file should be removed
            mock_remove.assert_called_once_with("/tmp/download/test.txt.gz")
            # AND debug log should be called
            mock_logger_debug.assert_called_once_with(
                "[syn123:wiki1]: Removed the gzipped file /tmp/download/test.txt.gz."
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

        with (
            patch(
                "synapseclient.models.wiki.get_attachment_url",
                return_value=mock_attachment_url,
            ) as mock_get_url,
            patch(
                "synapseclient.models.wiki.get_attachment_handles"
            ) as mock_get_handles,
        ):
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
        with (
            patch("synapseclient.models.wiki.get_attachment_preview_url") as mocked_get,
            pytest.raises(ValueError, match=expected_error),
        ):
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

        with (
            patch(
                "synapseclient.models.wiki.get_attachment_preview_url",
                return_value=mock_attachment_url,
            ) as mock_get_url,
            patch(
                "synapseclient.models.wiki.get_attachment_handles",
                return_value=mock_filehandle_dict,
            ) as mock_get_handles,
            patch(
                "synapseclient.models.wiki.download_from_url",
                return_value="/tmp/download/test.txt.gz",
            ) as mock_download_from_url,
            patch(
                "synapseclient.models.wiki.download_from_url_multi_threaded",
                return_value="/tmp/download/test.txt.gz",
            ) as mock_download_from_url_multi_threaded,
            patch(
                "synapseclient.models.wiki._pre_signed_url_expiration_time",
                return_value="2030-01-01T00:00:00.000Z",
            ) as mock_expiration_time,
            patch.object(self.syn.logger, "info") as mock_logger_info,
        ):
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
                f"[syn123:wiki1]: Downloaded the preview file test.txt to {result}."
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

        with (
            patch(
                "synapseclient.models.wiki.get_attachment_preview_url",
                return_value=mock_attachment_url,
            ) as mock_get_url,
            patch(
                "synapseclient.models.wiki.get_attachment_handles"
            ) as mock_get_handles,
        ):
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
        with (
            patch("synapseclient.models.wiki.get_markdown_url") as mocked_get,
            pytest.raises(ValueError, match=expected_error),
        ):
            await wiki_page.get_markdown_file_async(synapse_client=self.syn)
            # THEN the API should not be called
            mocked_get.assert_not_called()

    async def test_get_markdown_file_async_download_file_success(self) -> None:
        # Mock responses
        mock_markdown_url = "https://example.com/markdown.md.gz"

        with (
            patch(
                "synapseclient.models.wiki.get_markdown_url",
                return_value=mock_markdown_url,
            ) as mock_get_url,
            patch(
                "synapseclient.models.wiki.download_from_url",
                return_value="/tmp/download/markdown.md.gz",
            ) as mock_download_from_url,
            patch(
                "synapseclient.models.wiki.WikiPage.unzip_gzipped_file"
            ) as mock_unzip_gzipped_file,
            patch.object(self.syn.logger, "info") as mock_logger_info,
            patch.object(self.syn.logger, "debug") as mock_logger_debug,
            patch("os.remove") as mock_remove,
        ):
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
                f"[syn123:wiki1]: Downloaded and unzipped the markdown file to {result}."
            )
            # AND the gzipped file should be removed
            mock_remove.assert_called_once_with("/tmp/download/markdown.md.gz")
            # AND debug log should be called
            mock_logger_debug.assert_called_once_with(
                f"[syn123:wiki1]: Removed the gzipped file /tmp/download/markdown.md.gz."
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

        with (
            patch(
                "synapseclient.models.wiki.get_markdown_url",
                return_value=mock_markdown_url,
            ) as mock_get_url,
            patch(
                "synapseclient.models.wiki.get_attachment_handles"
            ) as mock_get_handles,
        ):
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


class TestWikiPageCopy:
    """Tests for the WikiPage.copy_async method."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @staticmethod
    def _header_generator(
        headers: list,
    ) -> AsyncGenerator[Dict[str, str], None]:
        """Return an async generator yielding the given wiki headers."""

        async def generator() -> AsyncGenerator[Dict[str, str], None]:
            for header in headers:
                yield header

        return generator()

    async def test_copy_async_missing_owner_id(self) -> None:
        # WHEN I call `copy_async` on a WikiPage without owner_id
        # THEN it should raise ValueError before calling the API
        with patch("synapseclient.models.wiki.get_wiki_header_tree") as mocked_get:
            with pytest.raises(
                ValueError, match="Must provide owner_id to copy a wiki."
            ):
                await WikiPage().copy_async(
                    destination_owner_id="syn456", synapse_client=self.syn
                )
            mocked_get.assert_not_called()

    @pytest.mark.parametrize(
        "destination_owner_id", [None, "", "project123", "syn", "syn123abc"]
    )
    async def test_copy_async_invalid_destination_owner_id(
        self, destination_owner_id
    ) -> None:
        # WHEN I call `copy_async` with a missing or malformed destination_owner_id
        # THEN it should raise ValueError before calling the API
        with patch("synapseclient.models.wiki.get_wiki_header_tree") as mocked_get:
            with pytest.raises(
                ValueError, match="destination_owner_id must be a Synapse ID"
            ):
                await WikiPage(owner_id="syn123").copy_async(
                    destination_owner_id=destination_owner_id,
                    synapse_client=self.syn,
                )
            mocked_get.assert_not_called()

    async def test_copy_async_invalid_entity_map(self) -> None:
        # WHEN I call `copy_async` with an entity_map containing a value that
        # is not a Synapse ID
        # THEN it should raise ValueError before calling the API
        with patch("synapseclient.models.wiki.get_wiki_header_tree") as mocked_get:
            with pytest.raises(
                ValueError, match="entity_map values must be Synapse IDs"
            ):
                await WikiPage(owner_id="syn123").copy_async(
                    destination_owner_id="syn456",
                    entity_map={"syn111": "not_an_id"},
                    synapse_client=self.syn,
                )
            mocked_get.assert_not_called()

    async def test_copy_async_source_without_wiki_returns_empty_list(self) -> None:
        # GIVEN a source entity whose wiki header tree request fails with a 404
        with patch(
            "synapseclient.models.wiki.get_wiki_header_tree",
            side_effect=SynapseHTTPError(response=Mock(status_code=404)),
        ):
            # WHEN I call `copy_async`
            results = await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456", synapse_client=self.syn
            )

        # THEN an empty list should be returned instead of raising
        assert results == []

    async def test_copy_async_header_tree_error_propagates(self) -> None:
        # GIVEN a source entity whose wiki header tree request fails with a
        # non-404 error
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=SynapseHTTPError(response=Mock(status_code=500)),
            ),
            # WHEN I call `copy_async`
            # THEN the error should be re-raised
            pytest.raises(SynapseHTTPError),
        ):
            await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456", synapse_client=self.syn
            )

    async def test_copy_async_unknown_sub_page_id_raises_value_error(self) -> None:
        # GIVEN a source wiki whose header tree does not contain the requested
        # page and a destination without an existing wiki
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=lambda **kwargs: self._header_generator(
                    [{"id": "8688", "title": "Root"}]
                ),
            ),
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                side_effect=SynapseHTTPError(response=Mock(status_code=404)),
            ),
            # WHEN I call `copy_async` with an id that is not in the tree
            # THEN it should raise ValueError
            pytest.raises(
                ValueError,
                match="The wiki page 9999 does not exist in the owner entity syn123.",
            ),
        ):
            await WikiPage(owner_id="syn123", id="9999").copy_async(
                destination_owner_id="syn456", synapse_client=self.syn
            )

    async def test_copy_async_sub_page_id_on_source_without_wiki_raises_value_error(
        self,
    ) -> None:
        # GIVEN a source entity without a wiki, so the header tree request
        # fails with a 404
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=SynapseHTTPError(response=Mock(status_code=404)),
            ),
            # WHEN I call `copy_async` with an id set
            # THEN it should raise ValueError instead of returning an empty list
            pytest.raises(
                ValueError,
                match="The wiki page 9999 does not exist in the owner entity syn123.",
            ),
        ):
            await WikiPage(owner_id="syn123", id="9999").copy_async(
                destination_owner_id="syn456", synapse_client=self.syn
            )

    @pytest.mark.parametrize(
        "entity_sub_page_id,destination_sub_page_id",
        [("8688", "4"), (8688, 4)],
    )
    async def test_copy_async_sub_page_id_coercion(
        self, entity_sub_page_id, destination_sub_page_id
    ) -> None:
        # GIVEN a copy request with sub page IDs given as numeric strings or integers
        headers = [{"id": "8688", "title": "Root"}]
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=lambda **kwargs: self._header_generator(headers),
            ),
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                return_value={"id": "4", "title": "Existing"},
            ) as mocked_get_page,
            patch(
                "synapseclient.models.wiki._collect_wiki_sub_tree_headers",
                return_value=None,
            ) as mocked_collect,
        ):
            # WHEN I call `copy_async`
            with pytest.raises(
                ValueError,
                match="The wiki page 8688 does not exist in the owner entity syn123.",
            ):
                await WikiPage(owner_id="syn123", id=entity_sub_page_id).copy_async(
                    destination_owner_id="syn456",
                    destination_sub_page_id=destination_sub_page_id,
                    synapse_client=self.syn,
                )

            # THEN both IDs should be coerced to integer strings
            mocked_get_page.assert_called_once_with(
                owner_id="syn456", wiki_id="4", synapse_client=self.syn
            )
            mocked_collect.assert_called_once_with(
                wiki_headers=headers, sub_page_id="8688"
            )

    @pytest.mark.parametrize(
        "entity_sub_page_id,destination_sub_page_id",
        [("some_string", None), (None, "some_string")],
    )
    async def test_copy_async_non_numeric_ids_raise_value_error(
        self, entity_sub_page_id, destination_sub_page_id
    ) -> None:
        # WHEN I call `copy_async` with a non-numeric sub page ID
        # THEN it should raise ValueError before calling the API
        with patch("synapseclient.models.wiki.get_wiki_header_tree") as mocked_get:
            with pytest.raises(ValueError):
                await WikiPage(owner_id="syn123", id=entity_sub_page_id).copy_async(
                    destination_owner_id="syn456",
                    destination_sub_page_id=destination_sub_page_id,
                    synapse_client=self.syn,
                )
            mocked_get.assert_not_called()

    async def test_copy_async_destination_page_error_propagates(self) -> None:
        # GIVEN a destination page check that fails with a non-404 error
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=lambda **kwargs: self._header_generator(
                    [{"id": "8688", "title": "Root"}]
                ),
            ),
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                side_effect=SynapseHTTPError(response=Mock(status_code=403)),
            ),
            # WHEN I call `copy_async`
            # THEN the error should be re-raised
            pytest.raises(SynapseHTTPError),
        ):
            await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456",
                destination_sub_page_id="4",
                synapse_client=self.syn,
            )

    async def test_copy_async_nonexistent_destination_sub_page_raises_value_error(
        self,
    ) -> None:
        # GIVEN a source wiki with a single root page and a destination sub
        # page ID that does not exist in the destination
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=lambda **kwargs: self._header_generator(
                    [{"id": "8688", "title": "Root"}]
                ),
            ),
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                side_effect=SynapseHTTPError(response=Mock(status_code=404)),
            ) as mocked_get_page,
            patch(
                "synapseclient.models.wiki.WikiPage._copy_attachment_file_handles",
                new_callable=AsyncMock,
            ) as mocked_copy_attachments,
            patch(
                "synapseclient.models.wiki.post_wiki_page",
                new_callable=AsyncMock,
            ) as mocked_post,
        ):
            # WHEN I call `copy_async`
            # THEN a ValueError should be raised
            with pytest.raises(ValueError, match="does not exist"):
                await WikiPage(owner_id="syn123").copy_async(
                    destination_owner_id="syn456",
                    destination_sub_page_id="4",
                    synapse_client=self.syn,
                )

            # AND the destination page should have been checked
            mocked_get_page.assert_called_once_with(
                owner_id="syn456", wiki_id="4", synapse_client=self.syn
            )

            # AND no attachments should have been copied and no pages created
            mocked_copy_attachments.assert_not_called()
            mocked_post.assert_not_called()

    @staticmethod
    @contextlib.contextmanager
    def _patched_flag_copy():
        """Patch what copy_async does around the link and entity ID rewrites.

        The source wiki is a single root page 1 that is copied to new1. The
        rewrite helpers are patched to return the new_wikis dict they were
        handed so the tests can check which of them ran and what they were
        given, and the re-store loop is patched at the markdown upload and
        put boundaries.

        Yields:
            A dict with the mocks under the keys "links", "entity_ids", and
            "put".
        """
        source_headers = [{"id": "1", "title": "Root"}]
        destination_headers = [{"id": "new1", "title": "Root"}]
        new_wikis = {"new1": WikiPage(owner_id="syn456", id="new1", markdown="md-1")}
        wiki_id_map = {"1": "new1"}

        def fake_header_tree(**kwargs):
            headers = (
                destination_headers
                if kwargs["owner_id"] == "syn456"
                else source_headers
            )

            async def generator():
                for header in headers:
                    yield header

            return generator()

        with (
            patch(
                "synapseclient.models.wiki.get_wiki_header_tree",
                side_effect=fake_header_tree,
            ),
            patch(
                "synapseclient.models.wiki._ensure_destination_has_no_root_wiki",
                new_callable=AsyncMock,
            ),
            patch(
                "synapseclient.models.wiki._copy_wiki_pages",
                new_callable=AsyncMock,
                return_value=(new_wikis, wiki_id_map),
            ),
            patch(
                "synapseclient.models.wiki._update_internal_links",
                side_effect=lambda **kwargs: kwargs["new_wikis"],
            ) as mock_links,
            patch(
                "synapseclient.models.wiki._update_synapse_id_references",
                side_effect=lambda **kwargs: kwargs["new_wikis"],
            ) as mock_entity_ids,
            patch.object(
                WikiPage,
                "_get_markdown_file_handle",
                autospec=True,
                side_effect=lambda self, *args, **kwargs: self,
            ),
            patch(
                "synapseclient.models.wiki.put_wiki_page",
                new_callable=AsyncMock,
                return_value={"id": "new1", "title": "Root"},
            ) as mock_put,
        ):
            yield {
                "links": mock_links,
                "entity_ids": mock_entity_ids,
                "put": mock_put,
            }

    async def test_copy_async_updates_links_by_default(self) -> None:
        # GIVEN a source wiki with a single page
        with self._patched_flag_copy() as mocks:
            # WHEN I call `copy_async` without passing update_links or entity_map
            new_headers = await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456", synapse_client=self.syn
            )

        # THEN the internal links should be rewritten
        mocks["links"].assert_called_once_with(
            new_wikis=ANY,
            wiki_id_map={"1": "new1"},
            source_owner_id="syn123",
            destination_owner_id="syn456",
        )

        # AND the Synapse ID references should be left unchanged
        mocks["entity_ids"].assert_not_called()

        # AND the rewritten page should be stored back once
        mocks["put"].assert_called_once()
        assert mocks["put"].call_args.kwargs["owner_id"] == "syn456"
        assert mocks["put"].call_args.kwargs["wiki_id"] == "new1"

        # AND the destination header tree should be returned
        assert [header.id for header in new_headers] == ["new1"]

    async def test_copy_async_without_update_links_or_entity_map_skips_rewrites(
        self,
    ) -> None:
        # GIVEN a source wiki with a single page
        with self._patched_flag_copy() as mocks:
            # WHEN I call `copy_async` with update_links disabled and no entity_map
            new_headers = await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456",
                update_links=False,
                synapse_client=self.syn,
            )

        # THEN neither rewrite should be applied
        mocks["links"].assert_not_called()
        mocks["entity_ids"].assert_not_called()

        # AND the copied pages should not be stored a second time
        mocks["put"].assert_not_called()

        # AND the destination header tree should still be returned
        assert [header.id for header in new_headers] == ["new1"]

    async def test_copy_async_entity_map_without_update_links(self) -> None:
        # GIVEN a source wiki with a single page
        with self._patched_flag_copy() as mocks:
            # WHEN I call `copy_async` with an entity_map but update_links disabled
            await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456",
                update_links=False,
                entity_map={"syn111": "syn222"},
                synapse_client=self.syn,
            )

        # THEN only the Synapse ID references should be rewritten
        mocks["links"].assert_not_called()
        mocks["entity_ids"].assert_called_once_with(
            new_wikis=ANY,
            wiki_id_map={"1": "new1"},
            entity_map={"syn111": "syn222"},
        )

        # AND the rewritten page should be stored back once
        mocks["put"].assert_called_once()

    async def test_copy_async_update_links_and_entity_map(self) -> None:
        # GIVEN a source wiki with a single page
        with self._patched_flag_copy() as mocks:
            # WHEN I call `copy_async` with both link updating and an entity_map
            await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456",
                entity_map={"syn111": "syn222"},
                synapse_client=self.syn,
            )

        # THEN both rewrites should be applied
        mocks["links"].assert_called_once()
        mocks["entity_ids"].assert_called_once()

        # AND the entity ID rewrite should operate on the pages returned by
        # the link rewrite rather than on a separate copy
        assert (
            mocks["entity_ids"].call_args.kwargs["new_wikis"]
            is mocks["links"].call_args.kwargs["new_wikis"]
        )

        # AND the page should be stored back only once for both rewrites
        mocks["put"].assert_called_once()


class TestValidateAndFormatCopyInputs:
    """Tests for the _validate_and_format_copy_inputs helper function."""

    @pytest.mark.parametrize(
        "entity_sub_page_id,destination_sub_page_id,expected",
        [
            (None, None, (None, None)),
            ("8688", None, ("8688", None)),
            (None, "4", (None, "4")),
            (8688, 4, ("8688", "4")),
        ],
    )
    def test_valid_inputs_return_coerced_sub_page_ids(
        self, entity_sub_page_id, destination_sub_page_id, expected
    ) -> None:
        # WHEN I validate copy inputs with valid owner IDs and sub page IDs
        result = _validate_and_format_copy_inputs(
            owner_id="syn123",
            destination_owner_id="syn456",
            entity_sub_page_id=entity_sub_page_id,
            destination_sub_page_id=destination_sub_page_id,
        )

        # THEN the sub page IDs should be coerced to integer strings or None
        assert result == expected

    def test_missing_owner_id_raises_value_error(self) -> None:
        # WHEN I validate copy inputs without an owner ID
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="Must provide owner_id to copy a wiki."):
            _validate_and_format_copy_inputs(
                owner_id=None,
                destination_owner_id="syn456",
                entity_sub_page_id=None,
                destination_sub_page_id=None,
            )

    @pytest.mark.parametrize(
        "destination_owner_id", [None, "", "project123", "123", "syn123abc"]
    )
    def test_invalid_destination_owner_id_raises_value_error(
        self, destination_owner_id
    ) -> None:
        # WHEN I validate copy inputs with a missing or malformed destination
        # owner ID
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="destination_owner_id must be a Synapse ID"
        ):
            _validate_and_format_copy_inputs(
                owner_id="syn123",
                destination_owner_id=destination_owner_id,
                entity_sub_page_id=None,
                destination_sub_page_id=None,
            )

    def test_valid_entity_map_passes(self) -> None:
        # WHEN I validate copy inputs with an entity_map of Synapse IDs
        # THEN no error should be raised
        result = _validate_and_format_copy_inputs(
            owner_id="syn123",
            destination_owner_id="syn456",
            entity_sub_page_id=None,
            destination_sub_page_id=None,
            entity_map={"syn111": "syn222", "syn333": "syn444"},
        )
        assert result == (None, None)

    @pytest.mark.parametrize(
        "entity_map,offender",
        [
            ({"not_an_id": "syn222"}, "keys"),
            ({"111": "syn222"}, "keys"),
            ({None: "syn222"}, "keys"),
            ({"syn111": "not_an_id"}, "values"),
            ({"syn111": "222"}, "values"),
            ({"syn111": None}, "values"),
            ({"syn111": "syn222", "syn333": "oops"}, "values"),
        ],
    )
    def test_invalid_entity_map_raises_value_error(self, entity_map, offender) -> None:
        # WHEN I validate copy inputs with an entity_map containing a key or
        # value that is not a Synapse ID
        # THEN it should raise ValueError naming keys or values
        with pytest.raises(
            ValueError, match=f"entity_map {offender} must be Synapse IDs"
        ):
            _validate_and_format_copy_inputs(
                owner_id="syn123",
                destination_owner_id="syn456",
                entity_sub_page_id=None,
                destination_sub_page_id=None,
                entity_map=entity_map,
            )

    @pytest.mark.parametrize(
        "entity_sub_page_id,destination_sub_page_id,argument_name",
        [
            ("some_string", None, "The id of the WikiPage"),
            (None, "some_string", "destination_sub_page_id"),
            (8688.0, None, "The id of the WikiPage"),
            (None, 4.9, "destination_sub_page_id"),
            (True, None, "The id of the WikiPage"),
            (None, True, "destination_sub_page_id"),
            (-8688, None, "The id of the WikiPage"),
            (None, "-4", "destination_sub_page_id"),
        ],
    )
    def test_non_numeric_sub_page_id_raises_value_error(
        self, entity_sub_page_id, destination_sub_page_id, argument_name
    ) -> None:
        # WHEN I validate copy inputs with a non-numeric, float,
        # boolean, or negative sub page ID
        # THEN it should raise ValueError naming the offending argument
        with pytest.raises(
            ValueError, match=f"{argument_name} must be a numeric wiki page ID"
        ):
            _validate_and_format_copy_inputs(
                owner_id="syn123",
                destination_owner_id="syn456",
                entity_sub_page_id=entity_sub_page_id,
                destination_sub_page_id=destination_sub_page_id,
            )


class TestGetExistingDestinationWikiPage:
    """Tests for the _get_existing_destination_wiki_page helper function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_existing_destination_sub_page_is_returned(self) -> None:
        # GIVEN a destination sub page that exists in the destination entity
        destination_wiki_data = {"id": "4", "title": "Existing", "etag": "etag1"}
        with patch(
            "synapseclient.models.wiki.get_wiki_page",
            new_callable=AsyncMock,
            return_value=destination_wiki_data,
        ) as mocked_get_page:
            # WHEN I fetch the destination wiki page
            result = await _get_existing_destination_wiki_page(
                destination_owner_id="syn456",
                destination_sub_page_id="4",
                synapse_client=self.syn,
            )

            # THEN the page should be fetched from the destination entity
            mocked_get_page.assert_called_once_with(
                owner_id="syn456", wiki_id="4", synapse_client=self.syn
            )

        # AND returned as a WikiPage owned by the destination entity
        assert result.id == "4"
        assert result.title == "Existing"
        assert result.owner_id == "syn456"

    async def test_destination_page_error_propagates(self) -> None:
        # GIVEN a destination page lookup that fails with a non-404 error
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                side_effect=SynapseHTTPError(response=Mock(status_code=403)),
            ),
            # WHEN I fetch the destination wiki page
            # THEN the error should be re-raised
            pytest.raises(SynapseHTTPError),
        ):
            await _get_existing_destination_wiki_page(
                destination_owner_id="syn456",
                destination_sub_page_id="4",
                synapse_client=self.syn,
            )

    async def test_nonexistent_destination_sub_page_raises_value_error(self) -> None:
        # GIVEN a destination_sub_page_id that does not exist in the
        # destination entity
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                side_effect=SynapseHTTPError(response=Mock(status_code=404)),
            ),
            # WHEN I fetch the destination wiki page
            # THEN a ValueError should be raised before any wiki content is
            # copied, instead of silently returning None and letting the copy
            # proceed toward a confusing server-side failure
            pytest.raises(ValueError, match="does not exist"),
        ):
            await _get_existing_destination_wiki_page(
                destination_owner_id="syn456",
                destination_sub_page_id="4",
                synapse_client=self.syn,
            )


class TestEnsureDestinationHasNoRootWiki:
    """Tests for the _ensure_destination_has_no_root_wiki helper function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_existing_root_wiki_raises_value_error(self) -> None:
        # GIVEN a destination entity that already has a root wiki page
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                return_value={"id": "1", "title": "Existing root"},
            ),
            # WHEN I check the destination for an existing root wiki
            # THEN a ValueError should be raised, since the server would
            # reject creating a second root wiki page after attachments have
            # already been copied
            pytest.raises(ValueError, match="already has a root wiki"),
        ):
            await _ensure_destination_has_no_root_wiki(
                destination_owner_id="syn456",
                synapse_client=self.syn,
            )

    async def test_no_existing_wiki_passes(self) -> None:
        # GIVEN a destination entity without an existing wiki
        with patch(
            "synapseclient.models.wiki.get_wiki_page",
            new_callable=AsyncMock,
            side_effect=SynapseHTTPError(response=Mock(status_code=404)),
        ):
            # WHEN I check the destination for an existing root wiki
            # THEN no error should be raised so the copy creates a new root page
            await _ensure_destination_has_no_root_wiki(
                destination_owner_id="syn456",
                synapse_client=self.syn,
            )

    async def test_root_wiki_check_error_propagates(self) -> None:
        # GIVEN a root wiki lookup that fails with a non-404 error
        with (
            patch(
                "synapseclient.models.wiki.get_wiki_page",
                new_callable=AsyncMock,
                side_effect=SynapseHTTPError(response=Mock(status_code=403)),
            ),
            # WHEN I check the destination for an existing root wiki
            # THEN the error should be re-raised
            pytest.raises(SynapseHTTPError),
        ):
            await _ensure_destination_has_no_root_wiki(
                destination_owner_id="syn456",
                synapse_client=self.syn,
            )


class TestCollectWikiSubTreeHeaders:
    """Tests for the _collect_wiki_sub_tree_headers helper function.

    The sample wiki header tree used by these tests:

        root (1)
        |-- methods (2)
        |   |-- sequencing (4)
        |   |   `-- deep (6)
        |   `-- analysis (5)
        `-- results (3)
    """

    def get_wiki_headers(self) -> list:
        return [
            {"id": "1", "title": "root"},
            {"id": "2", "title": "methods", "parentId": "1"},
            {"id": "4", "title": "sequencing", "parentId": "2"},
            {"id": "6", "title": "deep", "parentId": "4"},
            {"id": "5", "title": "analysis", "parentId": "2"},
            {"id": "3", "title": "results", "parentId": "1"},
        ]

    @pytest.mark.parametrize(
        "sub_page_id,expected",
        [
            (
                # Mid-level page: the page itself (re-rooted) plus all descendants
                "2",
                [
                    {"id": "2", "title": "methods"},
                    {"id": "4", "title": "sequencing", "parentId": "2"},
                    {"id": "6", "title": "deep", "parentId": "4"},
                    {"id": "5", "title": "analysis", "parentId": "2"},
                ],
            ),
            (
                # The root of the whole wiki: everything is returned
                "1",
                [
                    {"id": "1", "title": "root"},
                    {"id": "2", "title": "methods", "parentId": "1"},
                    {"id": "4", "title": "sequencing", "parentId": "2"},
                    {"id": "6", "title": "deep", "parentId": "4"},
                    {"id": "5", "title": "analysis", "parentId": "2"},
                    {"id": "3", "title": "results", "parentId": "1"},
                ],
            ),
            (
                # A leaf page: only the page itself, re-rooted
                "6",
                [{"id": "6", "title": "deep"}],
            ),
            (
                # An integer ID is coerced to a string before matching
                2,
                [
                    {"id": "2", "title": "methods"},
                    {"id": "4", "title": "sequencing", "parentId": "2"},
                    {"id": "6", "title": "deep", "parentId": "4"},
                    {"id": "5", "title": "analysis", "parentId": "2"},
                ],
            ),
        ],
    )
    def test_collects_sub_tree(self, sub_page_id, expected) -> None:
        # GIVEN a flat list of wiki headers
        wiki_headers = self.get_wiki_headers()

        # WHEN I collect the sub-tree rooted at sub_page_id
        result = _collect_wiki_sub_tree_headers(
            wiki_headers=wiki_headers,
            sub_page_id=sub_page_id,
        )

        # THEN only the requested page and its descendants are returned,
        # with the requested page re-rooted (no parentId)
        assert result == expected

    def test_parents_appear_before_children(self) -> None:
        # GIVEN a flat list of wiki headers
        wiki_headers = self.get_wiki_headers()

        # WHEN I collect a sub-tree with nested descendants
        result = _collect_wiki_sub_tree_headers(
            wiki_headers=wiki_headers,
            sub_page_id="2",
        )

        # THEN every page with a parent appears after that parent in the list
        positions = {header["id"]: index for index, header in enumerate(result)}
        for header in result:
            parent_id = header.get("parentId")
            if parent_id is not None:
                assert positions[parent_id] < positions[header["id"]]

    def test_unknown_sub_page_id_returns_none(self) -> None:
        # GIVEN a flat list of wiki headers
        wiki_headers = self.get_wiki_headers()

        # WHEN I collect a sub-tree for an ID that is not in the tree
        result = _collect_wiki_sub_tree_headers(
            wiki_headers=wiki_headers,
            sub_page_id="999",
        )

        # THEN nothing is returned
        assert result is None

    def test_input_headers_are_not_mutated(self) -> None:
        # GIVEN a flat list of wiki headers
        wiki_headers = self.get_wiki_headers()
        original = copy.deepcopy(wiki_headers)

        # WHEN I collect a sub-tree whose root has a parentId
        _collect_wiki_sub_tree_headers(
            wiki_headers=wiki_headers,
            sub_page_id="2",
        )

        # THEN the input headers are unchanged, including the parentId
        # of the requested page
        assert wiki_headers == original


class TestUpdateInternalLinks:
    """Tests for the _update_internal_links helper function.

    The scenario used by these tests: a wiki was copied from syn123 to syn456,
    where source page 8688 became 9901, page 8689 became 9902, and page 8
    became 1000.
    """

    wiki_id_map = {"8688": "9901", "8689": "9902", "8": "1000"}

    def get_new_wikis(self, markdown) -> dict[str, WikiPage]:
        return {
            "9901": WikiPage(owner_id="syn456", id="9901", markdown=markdown),
            "9902": WikiPage(owner_id="syn456", id="9902", markdown=""),
            "1000": WikiPage(owner_id="syn456", id="1000", markdown=""),
        }

    @pytest.mark.parametrize(
        "markdown,expected",
        [
            (
                # A link to a copied page is retargeted to the copy
                "See syn123/wiki/8688 for details.",
                "See syn456/wiki/9901 for details.",
            ),
            (
                # Multiple links in one page are all retargeted
                "syn123/wiki/8688 and syn123/wiki/8689",
                "syn456/wiki/9901 and syn456/wiki/9902",
            ),
            (
                # The rule for page 8 must not clobber the longer page ID 8688,
                # and a link to page 8 itself is still retargeted
                "syn123/wiki/8688 then syn123/wiki/8 end",
                "syn456/wiki/9901 then syn456/wiki/1000 end",
            ),
            (
                # A link to a page that was not copied keeps its wiki ID but
                # still gets the destination owner ID
                "syn123/wiki/7777",
                "syn456/wiki/7777",
            ),
            (
                # A bare reference to the source entity is replaced even when
                # it is not a wiki link
                "Data originally from syn123.",
                "Data originally from syn456.",
            ),
            (
                # Markdown without any source references is left unchanged
                "No links here.",
                "No links here.",
            ),
            (
                # A longer entity ID that merely starts with the source owner
                # ID must not be corrupted
                "Related data in syn1234.",
                "Related data in syn1234.",
            ),
        ],
    )
    def test_rewrites_markdown(self, markdown: str, expected: str) -> None:
        # GIVEN copied wiki pages where one page contains the markdown
        new_wikis = self.get_new_wikis(markdown=markdown)

        # WHEN I update the internal links
        result = _update_internal_links(
            new_wikis=new_wikis,
            wiki_id_map=self.wiki_id_map,
            source_owner_id="syn123",
            destination_owner_id="syn456",
        )

        # THEN the markdown points at the destination wiki
        assert result["9901"].markdown == expected

    def test_updates_every_copied_page(self) -> None:
        # GIVEN copied wiki pages that link to each other and to the source entity
        new_wikis = {
            "9901": WikiPage(
                owner_id="syn456", id="9901", markdown="Next: syn123/wiki/8689"
            ),
            "9902": WikiPage(
                owner_id="syn456", id="9902", markdown="Back: syn123/wiki/8688"
            ),
            "1000": WikiPage(owner_id="syn456", id="1000", markdown="Home: syn123"),
        }

        # WHEN I update the internal links
        result = _update_internal_links(
            new_wikis=new_wikis,
            wiki_id_map=self.wiki_id_map,
            source_owner_id="syn123",
            destination_owner_id="syn456",
        )

        # THEN every page's markdown is updated in place
        assert result is new_wikis
        assert new_wikis["9901"].markdown == "Next: syn456/wiki/9902"
        assert new_wikis["9902"].markdown == "Back: syn456/wiki/9901"
        assert new_wikis["1000"].markdown == "Home: syn456"

    def test_none_markdown_becomes_empty_string(self) -> None:
        # GIVEN a copied wiki page without markdown
        new_wikis = self.get_new_wikis(markdown=None)

        # WHEN I update the internal links
        result = _update_internal_links(
            new_wikis=new_wikis,
            wiki_id_map=self.wiki_id_map,
            source_owner_id="syn123",
            destination_owner_id="syn456",
        )

        # THEN the markdown is normalized to an empty string without error
        assert result["9901"].markdown == ""


class TestUpdateSynapseIdReferences:
    """Tests for the _update_synapse_id_references helper function.

    The scenario used by these tests: a wiki was copied from one project to
    another, where source wiki page 8688 became 9901 and page 8689 became
    9902. Alongside the wiki, entity syn111 was copied as syn999 and entity
    syn1112 was copied as syn888.
    """

    wiki_id_map = {"8688": "9901", "8689": "9902"}
    entity_map = {"syn111": "syn999", "syn1112": "syn888"}

    def get_new_wikis(self, markdown) -> dict:
        return {
            "9901": WikiPage(owner_id="syn456", id="9901", markdown=markdown),
            "9902": WikiPage(owner_id="syn456", id="9902", markdown=""),
        }

    @pytest.mark.parametrize(
        "markdown,expected",
        [
            (
                # A reference to a copied entity is rewritten to the copy
                "Data in syn111.",
                "Data in syn999.",
            ),
            (
                # The rule for syn111 must not clobber the longer ID syn1112,
                # and both references are rewritten
                "See syn111 and syn1112.",
                "See syn999 and syn888.",
            ),
            (
                # An entity that is not in the entity map is left unchanged
                "Not copied: syn777.",
                "Not copied: syn777.",
            ),
            (
                # Markdown without any entity references is left unchanged
                "No ids here.",
                "No ids here.",
            ),
        ],
    )
    def test_rewrites_markdown(self, markdown: str, expected: str) -> None:
        # GIVEN copied wiki pages where one page contains the markdown
        new_wikis = self.get_new_wikis(markdown=markdown)

        # WHEN I update the Synapse ID references
        result = _update_synapse_id_references(
            new_wikis=new_wikis,
            wiki_id_map=self.wiki_id_map,
            entity_map=self.entity_map,
        )

        # THEN the markdown points at the copied entities
        assert result["9901"].markdown == expected

    def test_updates_every_copied_page(self) -> None:
        # GIVEN copied wiki pages that both reference copied entities
        new_wikis = {
            "9901": WikiPage(owner_id="syn456", id="9901", markdown="Raw data: syn111"),
            "9902": WikiPage(
                owner_id="syn456", id="9902", markdown="Results table: syn1112"
            ),
        }

        # WHEN I update the Synapse ID references
        result = _update_synapse_id_references(
            new_wikis=new_wikis,
            wiki_id_map=self.wiki_id_map,
            entity_map=self.entity_map,
        )

        # THEN every page's markdown is updated in place
        assert result is new_wikis
        assert new_wikis["9901"].markdown == "Raw data: syn999"
        assert new_wikis["9902"].markdown == "Results table: syn888"

    def test_none_markdown_becomes_empty_string(self) -> None:
        # GIVEN a copied wiki page without markdown
        new_wikis = self.get_new_wikis(markdown=None)

        # WHEN I update the Synapse ID references
        result = _update_synapse_id_references(
            new_wikis=new_wikis,
            wiki_id_map=self.wiki_id_map,
            entity_map=self.entity_map,
        )

        # THEN the markdown is normalized to an empty string without error
        assert result["9901"].markdown == ""


class TestCopyWikiPages:
    """Tests for the _copy_wiki_pages helper function.

    These tests stub out the network boundary (get_async, markdown download,
    attachment copy, markdown file handle upload, and the wiki create/update
    calls) so the helper's own bookkeeping can be checked: the old-to-new ID
    map it builds, which pages are created versus written into an existing
    destination page, and the parent links it sends for child pages.

    The stubs are deterministic functions of a page's ID: get_async returns a
    source page whose title is title-<id>, its markdown is md-<id>, and its
    copied attachment file handle is fh-<id>. New IDs assigned by the create
    call are looked up from new_id_by_title.
    """

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @staticmethod
    @contextlib.contextmanager
    def _patched_copy(new_id_by_title: dict):
        """Patch the network boundary of _copy_wiki_pages.

        Arguments:
            new_id_by_title: Maps the title sent to post_wiki_page to the new
                wiki ID the server should return for it.

        Yields:
            A dict with the post_wiki_page and put_wiki_page mocks under the
            keys "post" and "put".
        """

        def fake_get(self, *args, **kwargs):
            self.title = f"title-{self.id}"
            self.markdown = f"md-{self.id}"
            return self

        def fake_markdown(self, *args, **kwargs):
            return f"md-{self.id}"

        def fake_attachments(self, *args, **kwargs):
            return [f"fh-{self.id}"]

        def fake_get_fh(self, *args, **kwargs):
            return self

        def fake_post(**kwargs):
            request = kwargs["request"]
            title = request["title"]
            return {
                "id": new_id_by_title[title],
                "title": title,
                "parentWikiId": request.get("parentWikiId"),
            }

        def fake_put(**kwargs):
            request = kwargs["request"]
            return {
                "id": kwargs["wiki_id"],
                "title": request["title"],
                "parentWikiId": request.get("parentWikiId"),
            }

        with (
            patch.object(WikiPage, "get_async", autospec=True, side_effect=fake_get),
            patch.object(
                WikiPage,
                "_get_markdown_text",
                autospec=True,
                side_effect=fake_markdown,
            ),
            patch.object(
                WikiPage,
                "_copy_attachment_file_handles",
                autospec=True,
                side_effect=fake_attachments,
            ),
            patch.object(
                WikiPage,
                "_get_markdown_file_handle",
                autospec=True,
                side_effect=fake_get_fh,
            ),
            patch(
                "synapseclient.models.wiki.post_wiki_page",
                new_callable=AsyncMock,
                side_effect=fake_post,
            ) as mock_post,
            patch(
                "synapseclient.models.wiki.put_wiki_page",
                new_callable=AsyncMock,
                side_effect=fake_put,
            ) as mock_put,
        ):
            yield {"post": mock_post, "put": mock_put}

    async def test_copies_whole_tree_preserving_hierarchy(self) -> None:
        # GIVEN a three level wiki tree with root -> methods -> analysis
        headers = [
            {"id": "1"},
            {"id": "2", "parentId": "1"},
            {"id": "3", "parentId": "2"},
        ]
        new_id_by_title = {
            "title-1": "new1",
            "title-2": "new2",
            "title-3": "new3",
        }

        # WHEN copying the whole tree with no existing destination page
        with self._patched_copy(new_id_by_title) as mocks:
            new_wikis, wiki_id_map = await _copy_wiki_pages(
                old_wiki_headers=headers,
                source_owner_id="syn123",
                destination_owner_id="syn456",
                destination_wiki_page=None,
                destination_sub_page_id=None,
                synapse_client=self.syn,
            )

        # THEN every source page is mapped to its new ID and returned keyed
        # by that new ID
        assert wiki_id_map == {"1": "new1", "2": "new2", "3": "new3"}
        assert set(new_wikis) == {"new1", "new2", "new3"}

        # AND all three pages are created with post, none with put
        assert mocks["put"].call_count == 0
        requests = {
            mock_call.kwargs["request"]["title"]: mock_call.kwargs["request"]
            for mock_call in mocks["post"].call_args_list
        }
        # AND the root is created as a root page with no parent
        assert "parentWikiId" not in requests["title-1"]
        # AND each child is linked to the new ID of its parent
        assert requests["title-2"]["parentWikiId"] == "new1"
        assert requests["title-3"]["parentWikiId"] == "new2"
        # AND each page carries its own copied attachment file handle
        assert requests["title-1"]["attachmentFileHandleIds"] == ["fh-1"]
        assert requests["title-2"]["attachmentFileHandleIds"] == ["fh-2"]
        assert requests["title-3"]["attachmentFileHandleIds"] == ["fh-3"]

    async def test_root_created_under_destination_sub_page_id(self) -> None:
        # GIVEN a single root page and a destination sub page to nest it under
        headers = [{"id": "1"}]

        # WHEN copying with destination_sub_page_id but no existing page object
        with self._patched_copy({"title-1": "new1"}) as mocks:
            _, wiki_id_map = await _copy_wiki_pages(
                old_wiki_headers=headers,
                source_owner_id="syn123",
                destination_owner_id="syn456",
                destination_wiki_page=None,
                destination_sub_page_id="900",
                synapse_client=self.syn,
            )

        # THEN the copied root is created beneath the destination sub page
        assert wiki_id_map == {"1": "new1"}
        assert mocks["put"].call_count == 0
        assert mocks["post"].call_args.kwargs["request"]["parentWikiId"] == "900"

    async def test_root_written_into_existing_destination_page(self) -> None:
        # GIVEN a root -> child tree and an existing destination page
        headers = [{"id": "1"}, {"id": "2", "parentId": "1"}]
        destination_page = WikiPage(owner_id="syn456", id="500")

        # WHEN copying into that existing destination page
        with self._patched_copy({"title-2": "new2"}) as mocks:
            new_wikis, wiki_id_map = await _copy_wiki_pages(
                old_wiki_headers=headers,
                source_owner_id="syn123",
                destination_owner_id="syn456",
                destination_wiki_page=destination_page,
                destination_sub_page_id="500",
                synapse_client=self.syn,
            )

        # THEN the root maps to the existing page ID and the child to a new ID
        assert wiki_id_map == {"1": "500", "2": "new2"}
        assert set(new_wikis) == {"500", "new2"}

        # AND the root is written with put into the existing page
        mocks["put"].assert_called_once()
        put_request = mocks["put"].call_args.kwargs
        assert put_request["wiki_id"] == "500"
        assert put_request["request"]["title"] == "title-1"
        assert put_request["request"]["markdown"] == "md-1"

        # AND the child is created with post and linked to the existing page
        mocks["post"].assert_called_once()
        assert mocks["post"].call_args.kwargs["request"]["parentWikiId"] == "500"

    async def test_returns_empty_when_no_headers(self) -> None:
        # GIVEN no wiki headers to copy
        # WHEN copying
        with self._patched_copy({}) as mocks:
            new_wikis, wiki_id_map = await _copy_wiki_pages(
                old_wiki_headers=[],
                source_owner_id="syn123",
                destination_owner_id="syn456",
                destination_wiki_page=None,
                destination_sub_page_id=None,
                synapse_client=self.syn,
            )

        # THEN nothing is created and empty results are returned
        assert new_wikis == {}
        assert wiki_id_map == {}
        assert mocks["post"].call_count == 0
        assert mocks["put"].call_count == 0
