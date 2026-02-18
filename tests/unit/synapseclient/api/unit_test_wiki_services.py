"""Unit tests for wiki_services utility functions."""

import json
from unittest.mock import AsyncMock, patch

import pytest

import synapseclient.api.wiki_services as wiki_services

OWNER_ID = "syn123456"
WIKI_ID = "987654"
WIKI_VERSION = 3
FILE_NAME = "diagram.png"
WIKI_REQUEST = {
    "title": "Test Wiki",
    "markdown": "# Hello World",
    "attachmentFileHandleIds": [],
}
WIKI_ORDER_HINT = {
    "ownerId": OWNER_ID,
    "idList": [WIKI_ID, "111111"],
    "etag": "etag-abc",
}


class TestPostWikiPage:
    """Tests for post_wiki_page function."""

    @patch("synapseclient.Synapse")
    async def test_post_wiki_page(self, mock_synapse):
        """Test creating a new wiki page."""
        # GIVEN a mock client that returns the created wiki page
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": WIKI_ID, **WIKI_REQUEST}
        mock_client.rest_post_async.return_value = expected_response

        # WHEN I call post_wiki_page
        result = await wiki_services.post_wiki_page(
            owner_id=OWNER_ID, request=WIKI_REQUEST, synapse_client=None
        )

        # THEN I expect a POST to /entity/{ownerId}/wiki2
        assert result == expected_response
        mock_client.rest_post_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2",
            body=json.dumps(WIKI_REQUEST),
        )


class TestGetWikiPage:
    """Tests for get_wiki_page function."""

    @patch("synapseclient.Synapse")
    async def test_get_wiki_page_with_wiki_id(self, mock_synapse):
        """Test getting a specific wiki page by wiki_id."""
        # GIVEN a mock client that returns a wiki page
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": WIKI_ID, "title": "Test Wiki"}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_wiki_page with a wiki_id
        result = await wiki_services.get_wiki_page(
            owner_id=OWNER_ID, wiki_id=WIKI_ID, synapse_client=None
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2/{wikiId}
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}",
            params={},
        )

    @patch("synapseclient.Synapse")
    async def test_get_wiki_page_root(self, mock_synapse):
        """Test getting the root wiki page (no wiki_id)."""
        # GIVEN a mock client that returns the root wiki page
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": WIKI_ID, "title": "Root Page"}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_wiki_page without a wiki_id
        result = await wiki_services.get_wiki_page(
            owner_id=OWNER_ID, synapse_client=None
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2",
            params={},
        )

    @patch("synapseclient.Synapse")
    async def test_get_wiki_page_with_version(self, mock_synapse):
        """Test getting a wiki page at a specific version."""
        # GIVEN a mock client that returns a versioned wiki page
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": WIKI_ID, "title": "Test Wiki"}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_wiki_page with wiki_id and wiki_version
        result = await wiki_services.get_wiki_page(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            wiki_version=WIKI_VERSION,
            synapse_client=None,
        )

        # THEN I expect a GET with wikiVersion as a query parameter
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}",
            params={"wikiVersion": WIKI_VERSION},
        )


class TestPutWikiPage:
    """Tests for put_wiki_page function."""

    @patch("synapseclient.Synapse")
    async def test_put_wiki_page(self, mock_synapse):
        """Test updating a wiki page."""
        # GIVEN a mock client that returns the updated wiki page
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": WIKI_ID, **WIKI_REQUEST}
        mock_client.rest_put_async.return_value = expected_response

        # WHEN I call put_wiki_page
        result = await wiki_services.put_wiki_page(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            request=WIKI_REQUEST,
            synapse_client=None,
        )

        # THEN I expect a PUT to /entity/{ownerId}/wiki2/{wikiId}
        assert result == expected_response
        mock_client.rest_put_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}",
            body=json.dumps(WIKI_REQUEST),
        )


class TestPutWikiVersion:
    """Tests for put_wiki_version function."""

    @patch("synapseclient.Synapse")
    async def test_put_wiki_version(self, mock_synapse):
        """Test restoring a specific version of a wiki page."""
        # GIVEN a mock client that returns the restored wiki page
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        wiki_version_str = str(WIKI_VERSION)
        expected_response = {"id": WIKI_ID, **WIKI_REQUEST}
        mock_client.rest_put_async.return_value = expected_response

        # WHEN I call put_wiki_version
        result = await wiki_services.put_wiki_version(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            wiki_version=wiki_version_str,
            request=WIKI_REQUEST,
            synapse_client=None,
        )

        # THEN I expect a PUT to /entity/{ownerId}/wiki2/{wikiId}/{wikiVersion}
        assert result == expected_response
        mock_client.rest_put_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/{wiki_version_str}",
            body=json.dumps(WIKI_REQUEST),
        )


class TestDeleteWikiPage:
    """Tests for delete_wiki_page function."""

    @patch("synapseclient.Synapse")
    async def test_delete_wiki_page(self, mock_synapse):
        """Test deleting a wiki page."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call delete_wiki_page
        await wiki_services.delete_wiki_page(
            owner_id=OWNER_ID, wiki_id=WIKI_ID, synapse_client=None
        )

        # THEN I expect a DELETE to /entity/{ownerId}/wiki2/{wikiId}
        mock_client.rest_delete_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}",
        )


class TestGetWikiHeaderTree:
    """Tests for get_wiki_header_tree function."""

    @patch("synapseclient.api.wiki_services.rest_get_paginated_async")
    @patch("synapseclient.Synapse")
    async def test_get_wiki_header_tree(self, mock_synapse, mock_paginated):
        """Test getting the wiki header tree."""
        # GIVEN a mock client and paginated results
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        async def mock_gen(*args, **kwargs):
            for item in [
                {"id": WIKI_ID, "title": "Root", "parentId": None},
                {"id": "111", "title": "Child", "parentId": WIKI_ID},
            ]:
                yield item

        mock_paginated.return_value = mock_gen()

        # WHEN I call get_wiki_header_tree
        results = []
        async for item in wiki_services.get_wiki_header_tree(
            owner_id=OWNER_ID, synapse_client=None
        ):
            results.append(item)

        # THEN I expect all header items from the generator
        assert len(results) == 2
        assert results[0]["title"] == "Root"
        assert results[1]["title"] == "Child"
        mock_paginated.assert_called_once_with(
            uri=f"/entity/{OWNER_ID}/wikiheadertree2",
            limit=20,
            offset=0,
            synapse_client=mock_client,
        )


class TestGetWikiHistory:
    """Tests for get_wiki_history function."""

    @patch("synapseclient.api.wiki_services.rest_get_paginated_async")
    @patch("synapseclient.Synapse")
    async def test_get_wiki_history(self, mock_synapse, mock_paginated):
        """Test getting wiki page history."""
        # GIVEN a mock client and paginated results
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        async def mock_gen(*args, **kwargs):
            for item in [
                {"version": "1", "modifiedOn": "2024-01-01"},
                {"version": "2", "modifiedOn": "2024-01-15"},
            ]:
                yield item

        mock_paginated.return_value = mock_gen()

        # WHEN I call get_wiki_history
        results = []
        async for item in wiki_services.get_wiki_history(
            owner_id=OWNER_ID, wiki_id=WIKI_ID, synapse_client=None
        ):
            results.append(item)

        # THEN I expect all history snapshots
        assert len(results) == 2
        assert results[0]["version"] == "1"
        mock_paginated.assert_called_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/wikihistory",
            limit=20,
            offset=0,
            synapse_client=mock_client,
        )


class TestGetAttachmentHandles:
    """Tests for get_attachment_handles function."""

    @patch("synapseclient.Synapse")
    async def test_get_attachment_handles(self, mock_synapse):
        """Test getting attachment handles without version."""
        # GIVEN a mock client that returns file handles
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "list": [
                {"id": "fh-111", "fileName": FILE_NAME},
            ]
        }
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_attachment_handles
        result = await wiki_services.get_attachment_handles(
            owner_id=OWNER_ID, wiki_id=WIKI_ID, synapse_client=None
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2/{wikiId}/attachmenthandles
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/attachmenthandles",
            params={},
        )

    @patch("synapseclient.Synapse")
    async def test_get_attachment_handles_with_version(self, mock_synapse):
        """Test getting attachment handles at a specific version."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"list": []}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_attachment_handles with wiki_version
        result = await wiki_services.get_attachment_handles(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            wiki_version=WIKI_VERSION,
            synapse_client=None,
        )

        # THEN I expect wikiVersion in the params
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/attachmenthandles",
            params={"wikiVersion": WIKI_VERSION},
        )


class TestGetAttachmentUrl:
    """Tests for get_attachment_url function."""

    @patch("synapseclient.Synapse")
    async def test_get_attachment_url(self, mock_synapse):
        """Test getting attachment download URL."""
        # GIVEN a mock client that returns a URL
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_url = "https://example.com/download/diagram.png"
        mock_client.rest_get_async.return_value = expected_url

        # WHEN I call get_attachment_url
        result = await wiki_services.get_attachment_url(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            file_name=FILE_NAME,
            synapse_client=None,
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2/{wikiId}/attachment
        assert result == expected_url
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/attachment",
            params={"redirect": False, "fileName": FILE_NAME},
        )

    @patch("synapseclient.Synapse")
    async def test_get_attachment_url_with_version(self, mock_synapse):
        """Test getting attachment URL at a specific wiki version."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_url = "https://example.com/download/diagram.png?v=3"
        mock_client.rest_get_async.return_value = expected_url

        # WHEN I call get_attachment_url with wiki_version
        result = await wiki_services.get_attachment_url(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            file_name=FILE_NAME,
            wiki_version=WIKI_VERSION,
            synapse_client=None,
        )

        # THEN I expect wikiVersion in the params
        assert result == expected_url
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/attachment",
            params={
                "redirect": False,
                "fileName": FILE_NAME,
                "wikiVersion": WIKI_VERSION,
            },
        )


class TestGetAttachmentPreviewUrl:
    """Tests for get_attachment_preview_url function."""

    @patch("synapseclient.Synapse")
    async def test_get_attachment_preview_url(self, mock_synapse):
        """Test getting attachment preview URL."""
        # GIVEN a mock client that returns a preview URL
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_url = "https://example.com/preview/diagram.png"
        mock_client.rest_get_async.return_value = expected_url

        # WHEN I call get_attachment_preview_url
        result = await wiki_services.get_attachment_preview_url(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            file_name=FILE_NAME,
            synapse_client=None,
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2/{wikiId}/attachmentpreview
        assert result == expected_url
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/attachmentpreview",
            params={"redirect": False, "fileName": FILE_NAME},
        )

    @patch("synapseclient.Synapse")
    async def test_get_attachment_preview_url_with_version(self, mock_synapse):
        """Test getting attachment preview URL at a specific version."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_url = "https://example.com/preview/diagram.png?v=3"
        mock_client.rest_get_async.return_value = expected_url

        # WHEN I call get_attachment_preview_url with wiki_version
        result = await wiki_services.get_attachment_preview_url(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            file_name=FILE_NAME,
            wiki_version=WIKI_VERSION,
            synapse_client=None,
        )

        # THEN I expect wikiVersion in the params
        assert result == expected_url
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/attachmentpreview",
            params={
                "redirect": False,
                "fileName": FILE_NAME,
                "wikiVersion": WIKI_VERSION,
            },
        )


class TestGetMarkdownUrl:
    """Tests for get_markdown_url function."""

    @patch("synapseclient.Synapse")
    async def test_get_markdown_url(self, mock_synapse):
        """Test getting markdown download URL."""
        # GIVEN a mock client that returns a markdown URL
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_url = "https://example.com/markdown/wiki.md"
        mock_client.rest_get_async.return_value = expected_url

        # WHEN I call get_markdown_url
        result = await wiki_services.get_markdown_url(
            owner_id=OWNER_ID, wiki_id=WIKI_ID, synapse_client=None
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2/{wikiId}/markdown
        assert result == expected_url
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/markdown",
            params={"redirect": False},
        )

    @patch("synapseclient.Synapse")
    async def test_get_markdown_url_with_version(self, mock_synapse):
        """Test getting markdown URL at a specific version."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_url = "https://example.com/markdown/wiki.md?v=3"
        mock_client.rest_get_async.return_value = expected_url

        # WHEN I call get_markdown_url with wiki_version
        result = await wiki_services.get_markdown_url(
            owner_id=OWNER_ID,
            wiki_id=WIKI_ID,
            wiki_version=WIKI_VERSION,
            synapse_client=None,
        )

        # THEN I expect wikiVersion in the params
        assert result == expected_url
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2/{WIKI_ID}/markdown",
            params={"redirect": False, "wikiVersion": WIKI_VERSION},
        )


class TestGetWikiOrderHint:
    """Tests for get_wiki_order_hint function."""

    @patch("synapseclient.Synapse")
    async def test_get_wiki_order_hint(self, mock_synapse):
        """Test getting wiki order hint."""
        # GIVEN a mock client that returns an order hint
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_get_async.return_value = WIKI_ORDER_HINT

        # WHEN I call get_wiki_order_hint
        result = await wiki_services.get_wiki_order_hint(
            owner_id=OWNER_ID, synapse_client=None
        )

        # THEN I expect a GET to /entity/{ownerId}/wiki2orderhint
        assert result == WIKI_ORDER_HINT
        mock_client.rest_get_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2orderhint",
        )


class TestPutWikiOrderHint:
    """Tests for put_wiki_order_hint function."""

    @patch("synapseclient.Synapse")
    async def test_put_wiki_order_hint(self, mock_synapse):
        """Test updating wiki order hint."""
        # GIVEN a mock client that returns the updated order hint
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        mock_client.rest_put_async.return_value = WIKI_ORDER_HINT

        # WHEN I call put_wiki_order_hint
        result = await wiki_services.put_wiki_order_hint(
            owner_id=OWNER_ID, request=WIKI_ORDER_HINT, synapse_client=None
        )

        # THEN I expect a PUT to /entity/{ownerId}/wiki2orderhint
        assert result == WIKI_ORDER_HINT
        mock_client.rest_put_async.assert_awaited_once_with(
            uri=f"/entity/{OWNER_ID}/wiki2orderhint",
            body=json.dumps(WIKI_ORDER_HINT),
        )
