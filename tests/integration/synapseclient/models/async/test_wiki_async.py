"""Integration tests for the synapseclient.models.wiki module."""

import asyncio
import gzip
import os
import re
import tempfile
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Folder,
    Project,
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)
from tests.integration.helpers import wait_for_condition


class TestWikiPageBasicOperations:
    """Tests for basic WikiPage CRUD operations."""

    @pytest.fixture(scope="class")
    async def wiki_page_fixture(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> WikiPage:
        """Create a root wiki page fixture shared across tests in this class.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki via a Folder created inside the session-shared project
        rather than creating its own Project.
        """
        folder = await Folder(
            name=f"Test Wiki Basic Operations Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        wiki_title = f"Root Wiki Page {str(uuid.uuid4())}"
        wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."
        wiki_page = WikiPage(
            owner_id=folder.id,
            title=wiki_title,
            markdown=wiki_markdown,
        )
        root_wiki = await wiki_page.store_async(synapse_client=syn)
        return root_wiki

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_wiki_page_by_id(
        self,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting a wiki page by ID."""
        # GIVEN an existing wiki page (from fixture)
        root_wiki = wiki_page_fixture

        # WHEN retrieving the wiki page by ID
        retrieved_wiki = await WikiPage(
            owner_id=root_wiki.owner_id, id=root_wiki.id
        ).get_async(synapse_client=self.syn)
        schedule_for_cleanup(retrieved_wiki.id)

        # THEN the retrieved wiki should match the created one
        assert retrieved_wiki.id == root_wiki.id
        assert retrieved_wiki.title == root_wiki.title
        assert retrieved_wiki.owner_id == root_wiki.owner_id

    async def test_get_wiki_page_by_title(
        self,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting a wiki page by title."""
        # GIVEN an existing wiki page (from fixture)
        root_wiki = wiki_page_fixture

        # WHEN retrieving the wiki page by title
        retrieved_wiki = await WikiPage(
            owner_id=root_wiki.owner_id, title=root_wiki.title
        ).get_async(synapse_client=self.syn)
        schedule_for_cleanup(retrieved_wiki.id)
        # THEN the retrieved wiki should match the created one
        assert retrieved_wiki.id == root_wiki.id
        assert retrieved_wiki.title == root_wiki.title
        assert retrieved_wiki.owner_id == root_wiki.owner_id

    async def test_delete_wiki_page(
        self,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN an existing wiki page (from fixture)
        root_wiki = wiki_page_fixture

        # WHEN creating a wiki page to delete
        wiki_page_to_delete = await WikiPage(
            owner_id=root_wiki.owner_id,
            parent_id=root_wiki.id,
            title=f"Wiki Page to be deleted {str(uuid.uuid4())}",
            markdown="# Wiki Page to be deleted\n\nThis is a wiki page to be deleted.",
        ).store_async(synapse_client=self.syn)
        schedule_for_cleanup(wiki_page_to_delete.id)
        # WHEN deleting the wiki page
        await wiki_page_to_delete.delete_async(synapse_client=self.syn)

        # THEN the wiki page should be deleted
        with pytest.raises(SynapseHTTPError, match="404"):
            await WikiPage(
                owner_id=root_wiki.owner_id, id=wiki_page_to_delete.id
            ).get_async(synapse_client=self.syn)

    async def test_create_sub_wiki_page(
        self,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test creating a sub-wiki page under a root wiki page."""
        # GIVEN a root wiki page
        root_wiki = wiki_page_fixture

        # WHEN creating a sub-wiki page
        title = f"Sub Wiki Basic Operations {str(uuid.uuid4())}"
        sub_wiki = await WikiPage(
            owner_id=root_wiki.owner_id,
            parent_id=root_wiki.id,
            title=title,
            markdown="# Sub Wiki Basic Operations\n\nThis is a sub wiki basic operations page.",
        ).store_async(synapse_client=self.syn)
        schedule_for_cleanup(sub_wiki.id)
        # THEN the sub-wiki page should be created
        assert sub_wiki.id is not None
        assert sub_wiki.title == title
        assert sub_wiki.parent_id == root_wiki.id
        assert sub_wiki.owner_id == root_wiki.owner_id


class TestWikiPageAttachments:
    """Tests for WikiPage attachment operations."""

    @pytest.fixture(scope="class")
    async def wiki_page_fixture(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> WikiPage:
        """Create a root wiki page fixture shared across tests in this class.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki via a Folder created inside the session-shared project
        rather than creating its own Project.
        """
        folder = await Folder(
            name=f"Test Wiki Attachments Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        wiki_title = f"Root Wiki Page {str(uuid.uuid4())}"
        wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."
        wiki_page = WikiPage(
            owner_id=folder.id,
            title=wiki_title,
            markdown=wiki_markdown,
        )
        root_wiki = await wiki_page.store_async(synapse_client=syn)
        return root_wiki

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def wiki_page_with_attachment(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> tuple[WikiPage, str]:
        """Create a wiki page with an attachment."""
        # Create a temporary file for attachment
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        # GIVEN a root wiki page
        root_wiki = wiki_page_fixture
        # Create wiki page with attachment
        wiki_page = WikiPage(
            owner_id=root_wiki.owner_id,
            title=f"Sub Wiki with Attachment {str(uuid.uuid4())}",
            markdown="# Sub Wiki with Attachment\n\nThis is a sub wiki with an attachment page.",
            parent_id=root_wiki.id,
            attachments=[filename],
        )
        wiki_page = await wiki_page.store_async(synapse_client=syn)
        schedule_for_cleanup(wiki_page.id)
        attachment_name = os.path.basename(filename)
        return wiki_page, attachment_name

    async def test_get_attachment_handles(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # WHEN getting attachment handles
        attachment_handles = await wiki_page.get_attachment_handles_async(
            synapse_client=self.syn
        )

        # THEN attachment handles should be returned
        assert len(attachment_handles["list"]) > 0
        schedule_for_cleanup(attachment_handles)

    async def test_get_attachment_url(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # WHEN getting attachment URL
        attachment_url = await wiki_page.get_attachment_async(
            file_name=attachment_name, download_file=False, synapse_client=self.syn
        )

        # THEN a URL should be returned
        assert len(attachment_url) > 0
        schedule_for_cleanup(attachment_url)

    async def test_download_attachment(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # AND a download location
        download_dir = tempfile.mkdtemp()
        self.schedule_for_cleanup(download_dir)

        # WHEN downloading the attachment
        downloaded_path = await wiki_page.get_attachment_async(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)

    async def test_get_attachment_preview_url(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # WHEN polling until attachment preview is available
        preview_url = await wait_for_condition(
            condition_fn=lambda: wiki_page.get_attachment_preview_async(
                file_name=attachment_name,
                download_file=False,
                synapse_client=self.syn,
            ),
            timeout_seconds=60,
            poll_interval_seconds=5,
            description="attachment preview to be generated",
        )

        # THEN a URL should be returned
        assert len(preview_url) > 0
        schedule_for_cleanup(preview_url)

    async def test_download_attachment_preview(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # AND a download location
        download_dir = tempfile.mkdtemp()
        self.schedule_for_cleanup(download_dir)

        # Poll until attachment preview is available for download
        await wait_for_condition(
            condition_fn=lambda: wiki_page.get_attachment_preview_async(
                file_name=attachment_name,
                download_file=False,
                synapse_client=self.syn,
            ),
            timeout_seconds=60,
            poll_interval_seconds=5,
            description="attachment preview to be available for download",
        )

        # WHEN downloading the attachment preview
        downloaded_path = await wiki_page.get_attachment_preview_async(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) == "preview.txt"

    @pytest.mark.skipif(
        os.getenv("GITHUB_ACTIONS") == "true",
        reason="This test runs only locally, not in CI/CD environments.",
    )
    async def test_download_attachment_large_file(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> None:
        """Test downloading a large attachment file (> 8 MiB) - local only."""
        # GIVEN a wiki page with an attachment
        root_wiki = wiki_page_fixture
        # Create a temporary file for attachment with > 8 MiB
        filename = utils.make_bogus_uuid_file()
        with open(filename, "wb") as f:
            f.write(b"\0" * (9 * 1024 * 1024))

        # AND a download location
        download_dir = tempfile.mkdtemp()
        schedule_for_cleanup(download_dir)

        # Create wiki page with attachment
        wiki_page = WikiPage(
            owner_id=root_wiki.owner_id,
            title=f"Sub Wiki with large Attachment {str(uuid.uuid4())}",
            markdown="# Sub Wiki with large Attachment\n\nThis is a sub wiki with a large attachment page.",
            parent_id=root_wiki.id,
            attachments=[filename],
        )
        wiki_page = await wiki_page.store_async(synapse_client=self.syn)
        schedule_for_cleanup(wiki_page.id)
        # WHEN downloading the attachment
        downloaded_path = await wiki_page.get_attachment_async(
            file_name=os.path.basename(filename),
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) == os.path.basename(filename)

    @pytest.fixture(scope="function")
    async def wiki_page_with_gz_attachment(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> tuple[WikiPage, str]:
        """Create a wiki page with a gzipped attachment."""
        # Create a gzipped file
        filename = utils.make_bogus_uuid_file()
        # Rename to add .gz extension
        gz_filename = filename + ".gz"
        os.rename(filename, gz_filename)
        with gzip.open(gz_filename, "wt") as f:
            f.write("hello world\n")
        schedule_for_cleanup(gz_filename)

        # GIVEN a root wiki page
        root_wiki = wiki_page_fixture
        # Create wiki page with gz attachment
        wiki_page = WikiPage(
            owner_id=root_wiki.owner_id,
            title=f"Sub Wiki with GZ Attachment {str(uuid.uuid4())}",
            markdown="# Sub Wiki with GZ Attachment\n\nThis is a sub wiki with a gz attachment page.",
            parent_id=root_wiki.id,
            attachments=[gz_filename],
        )
        sub_wiki = await wiki_page.store_async(synapse_client=syn)
        schedule_for_cleanup(sub_wiki.id)
        attachment_name = os.path.basename(gz_filename)
        return sub_wiki, attachment_name

    async def test_get_attachment_handles_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting attachment handles for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment
        # WHEN getting attachment handles
        attachment_handles = await wiki_page.get_attachment_handles_async(
            synapse_client=self.syn
        )

        # THEN attachment handles should be returned
        assert len(attachment_handles["list"]) > 0
        # Verify the attachment name contains .gz
        assert any(
            handle.get("fileName", "").endswith(".gz")
            for handle in attachment_handles["list"]
        )
        schedule_for_cleanup(attachment_handles)

    async def test_get_attachment_url_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting attachment URL for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment

        # WHEN getting attachment URL
        attachment_url = await wiki_page.get_attachment_async(
            file_name=attachment_name, download_file=False, synapse_client=self.syn
        )
        # THEN a URL should be returned
        assert len(attachment_url) > 0
        schedule_for_cleanup(attachment_url)

    async def test_download_attachment_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test downloading a gz attachment file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment

        # AND a download location
        download_dir = tempfile.mkdtemp()
        schedule_for_cleanup(download_dir)

        # WHEN downloading the gz attachment
        downloaded_path = await wiki_page.get_attachment_async(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)

        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) + ".gz" == attachment_name

    async def test_get_attachment_preview_url_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting attachment preview URL for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment

        # WHEN polling until attachment preview is available
        preview_url = await wait_for_condition(
            condition_fn=lambda: wiki_page.get_attachment_preview_async(
                file_name=attachment_name,
                download_file=False,
                synapse_client=self.syn,
            ),
            timeout_seconds=60,
            poll_interval_seconds=5,
            description="attachment preview to be generated",
        )

        # THEN a URL should be returned
        assert len(preview_url) > 0
        schedule_for_cleanup(preview_url)

    async def test_download_attachment_preview_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test downloading attachment preview for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment

        # Poll until attachment preview is available for download
        await wait_for_condition(
            condition_fn=lambda: wiki_page.get_attachment_preview_async(
                file_name=attachment_name,
                download_file=False,
                synapse_client=self.syn,
            ),
            timeout_seconds=60,
            poll_interval_seconds=5,
            description="attachment preview to be available for download",
        )

        # AND a download location
        download_dir = tempfile.mkdtemp()
        self.schedule_for_cleanup(download_dir)

        # WHEN downloading the attachment preview
        downloaded_path = await wiki_page.get_attachment_preview_async(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)

        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) == "preview.txt"


class TestWikiPageMarkdown:
    """Tests for WikiPage markdown operations."""

    @pytest.fixture(scope="class")
    async def wiki_page_fixture(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> WikiPage:
        """Create a root wiki page fixture shared across tests in this class.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki via a Folder created inside the session-shared project
        rather than creating its own Project.
        """
        folder = await Folder(
            name=f"Test Wiki Markdown Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        wiki_title = f"Root Wiki Page {str(uuid.uuid4())}"
        wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."
        wiki_page = WikiPage(
            owner_id=folder.id,
            title=wiki_title,
            markdown=wiki_markdown,
        )
        root_wiki = await wiki_page.store_async(synapse_client=syn)
        return root_wiki

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def wiki_page_with_markdown(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> WikiPage:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_fixture
        wiki_page = WikiPage(
            owner_id=root_wiki.owner_id,
            title=f"Sub Wiki Markdown {str(uuid.uuid4())}",
            markdown="# Sub Wiki Markdown\n\nThis is a sub wiki markdown page.",
            parent_id=root_wiki.id,
        )
        sub_wiki = await wiki_page.store_async(synapse_client=syn)
        schedule_for_cleanup(sub_wiki.id)
        return sub_wiki

    async def test_get_markdown_url(
        self,
        wiki_page_with_markdown: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_with_markdown

        # WHEN getting markdown URL
        markdown_url = await WikiPage(
            owner_id=root_wiki.owner_id, id=root_wiki.id
        ).get_markdown_file_async(download_file=False, synapse_client=self.syn)
        schedule_for_cleanup(markdown_url)
        # THEN a URL should be returned
        assert len(markdown_url) > 0

    async def test_download_markdown_file(
        self,
        wiki_page_with_markdown: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_with_markdown

        # AND a download location
        download_dir = tempfile.mkdtemp()

        # WHEN downloading the markdown file
        downloaded_path = await WikiPage(
            owner_id=root_wiki.owner_id, id=root_wiki.id
        ).get_markdown_file_async(
            download_file=True, download_location=download_dir, synapse_client=self.syn
        )
        # THEN the file should be downloaded and unzipped
        assert os.path.exists(downloaded_path)
        # Verify content
        with open(downloaded_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "Sub Wiki Markdown" in content
        schedule_for_cleanup(download_dir)

    @pytest.fixture(scope="function")
    async def wiki_page_with_markdown_gz(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> WikiPage:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_fixture
        filename = utils.make_bogus_uuid_file()
        # Rename to add .md.gz extension
        md_gz_filename = filename.replace(".txt", ".md.gz")
        os.rename(filename, md_gz_filename)
        with gzip.open(md_gz_filename, "wt") as f:
            f.write("# Test Wiki\n\nThis is test content.")
        schedule_for_cleanup(md_gz_filename)

        # Create wiki page with markdown gz
        wiki_page = WikiPage(
            owner_id=root_wiki.owner_id,
            title=f"Test Wiki with GZ Markdown {str(uuid.uuid4())}",
            markdown="# Test Wiki with GZ Markdown\n\nThis is test content.",
            parent_id=root_wiki.id,
        )
        sub_wiki = await wiki_page.store_async(synapse_client=syn)
        schedule_for_cleanup(sub_wiki.id)
        return sub_wiki

    async def test_get_markdown_url_gz_file(
        self,
        wiki_page_with_markdown_gz: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown gz
        root_wiki = wiki_page_with_markdown_gz

        # WHEN getting markdown URL
        markdown_url = await WikiPage(
            owner_id=root_wiki.owner_id, id=root_wiki.id
        ).get_markdown_file_async(download_file=False, synapse_client=self.syn)
        schedule_for_cleanup(markdown_url)
        # THEN a URL should be returned
        assert len(markdown_url) > 0

    async def test_download_markdown_file_gz_file(
        self,
        wiki_page_with_markdown_gz: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown gz
        root_wiki = wiki_page_with_markdown_gz

        # AND a download location
        download_dir = tempfile.mkdtemp()

        # WHEN downloading the markdown file
        downloaded_path = await WikiPage(
            owner_id=root_wiki.owner_id, id=root_wiki.id
        ).get_markdown_file_async(
            download_file=True, download_location=download_dir, synapse_client=self.syn
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloaded and unzipped
        assert os.path.exists(downloaded_path)
        # Verify content
        with open(downloaded_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "Test Wiki" in content
        schedule_for_cleanup(download_dir)


class TestWikiPageVersioning:
    """Tests for WikiPage version operations."""

    @pytest.fixture(scope="class")
    async def wiki_page_fixture(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> WikiPage:
        """Create a root wiki page fixture shared across tests in this class.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki via a Folder created inside the session-shared project
        rather than creating its own Project.
        """
        folder = await Folder(
            name=f"Test Wiki Versioning Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        wiki_title = f"Root Wiki Page {str(uuid.uuid4())}"
        wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."
        wiki_page = WikiPage(
            owner_id=folder.id,
            title=wiki_title,
            markdown=wiki_markdown,
        )
        root_wiki = await wiki_page.store_async(synapse_client=syn)
        return root_wiki

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def wiki_page_with_multiple_versions(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> WikiPage:
        # GIVEN a wiki page with multiple versions
        root_wiki = wiki_page_fixture
        wiki_page = WikiPage(
            owner_id=root_wiki.owner_id,
            title=f"Sub Wiki Versioning {str(uuid.uuid4())}",
            markdown="# Sub Wiki Versioning\n\nThis is a sub wiki versioning page.",
            parent_id=root_wiki.id,
        )
        updated_wiki = await wiki_page.store_async(synapse_client=syn)
        # Update the wiki page
        updated_wiki = await WikiPage(
            owner_id=root_wiki.owner_id, id=updated_wiki.id, title="Version 1"
        ).store_async(synapse_client=syn)
        # Update the wiki page
        updated_wiki = await WikiPage(
            owner_id=root_wiki.owner_id, id=updated_wiki.id, title="Version 2"
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(updated_wiki.id)
        return updated_wiki

    async def test_wiki_page_history(
        self,
        wiki_page_with_multiple_versions,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with multiple versions
        sub_wiki = wiki_page_with_multiple_versions
        # WHEN getting wiki history
        history = []
        async for item in WikiHistorySnapshot.get_async(
            owner_id=sub_wiki.owner_id, id=sub_wiki.id, synapse_client=self.syn
        ):
            history.append(item)
        # THEN history should be returned
        assert len(history) == 3
        schedule_for_cleanup(history)

    async def test_restore_wiki_page_version(
        self,
        wiki_page_with_multiple_versions,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with multiple versions
        root_wiki = wiki_page_with_multiple_versions
        # Get initial version
        initial_version = "0"
        # WHEN restoring to the initial version
        restored_wiki = await WikiPage(
            owner_id=root_wiki.owner_id,
            id=root_wiki.id,
            wiki_version=initial_version,
        ).restore_async(synapse_client=self.syn)

        # THEN the wiki should be restored
        assert "Sub Wiki Versioning" in restored_wiki.title
        schedule_for_cleanup(restored_wiki)


class TestWikiHeader:
    """Tests for WikiHeader operations."""

    @pytest.fixture(scope="class")
    async def wiki_page_fixture(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> WikiPage:
        """Create a root wiki page fixture shared across tests in this class.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki via a Folder created inside the session-shared project
        rather than creating its own Project.
        """
        folder = await Folder(
            name=f"Test Wiki Header Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        wiki_title = f"Root Wiki Page {str(uuid.uuid4())}"
        wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."
        wiki_page = WikiPage(
            owner_id=folder.id,
            title=wiki_title,
            markdown=wiki_markdown,
        )
        root_wiki = await wiki_page.store_async(synapse_client=syn)
        return root_wiki

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_wiki_header_tree(
        self, wiki_page_fixture: WikiPage, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        await asyncio.sleep(5)
        # WHEN getting the wiki header tree
        headers = []
        async for header in WikiHeader.get_async(
            owner_id=wiki_page_fixture.owner_id, synapse_client=self.syn
        ):
            headers.append(header)

        # THEN headers should be returned
        assert len(headers) >= 1
        schedule_for_cleanup(headers)


class TestWikiPageCopy:
    """Tests for WikiPage copy operations."""

    OLD_ENTITY_ID = "syn000000123"
    NEW_ENTITY_ID = "syn000000999"

    @pytest.fixture(scope="class")
    async def source_wiki_tree(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> dict:
        """Create a source entity with a three-level wiki tree.

        The tree is root -> sub -> sub_sub. The sub and sub_sub pages each have
        a file attachment. The root page markdown contains an internal link to
        the sub page and a reference to a fake entity ID used to test
        entity_map rewriting.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki tree via a Folder created inside the session-shared
        project rather than creating its own Project.
        """
        owner_folder = await Folder(
            name=f"Test Wiki Copy Source Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(owner_folder.id)
        project = owner_folder

        root_wiki = await WikiPage(
            owner_id=project.id,
            title=f"Copy Root {str(uuid.uuid4())}",
            markdown="# Root\n\nPlaceholder.",
        ).store_async(synapse_client=syn)

        attachment_file = utils.make_bogus_uuid_file()
        schedule_for_cleanup(attachment_file)
        sub_wiki = await WikiPage(
            owner_id=project.id,
            parent_id=root_wiki.id,
            title=f"Copy Sub {str(uuid.uuid4())}",
            markdown="# Sub\n\nThis is the sub wiki page.",
            attachments=[attachment_file],
        ).store_async(synapse_client=syn)

        sub_sub_attachment_file = utils.make_bogus_uuid_file()
        schedule_for_cleanup(sub_sub_attachment_file)
        sub_sub_wiki = await WikiPage(
            owner_id=project.id,
            parent_id=sub_wiki.id,
            title=f"Copy Sub Sub {str(uuid.uuid4())}",
            markdown="# Sub Sub\n\nThis is the sub sub wiki page.",
            attachments=[sub_sub_attachment_file],
        ).store_async(synapse_client=syn)

        # Update the root markdown now that the sub wiki ID is known so it
        # contains an internal wiki link and an entity reference
        root_wiki.markdown = (
            "# Root\n\n"
            f"See the sub page: {project.id}/wiki/{sub_wiki.id}\n\n"
            f"Data is stored at {self.OLD_ENTITY_ID}."
        )
        root_wiki = await root_wiki.store_async(synapse_client=syn)

        # Allow the wiki header tree to become consistent
        await asyncio.sleep(5)

        # The source tree is never modified by the tests, so its markdown and
        # attachment names are read once here and shared instead of being
        # downloaded again by every test that compares against them
        pages = [root_wiki, sub_wiki, sub_sub_wiki]
        source_markdown = {
            page.id: await self._read_markdown(
                owner_id=project.id,
                wiki_id=page.id,
                syn=syn,
                schedule_for_cleanup=schedule_for_cleanup,
            )
            for page in pages
        }
        source_attachment_names = {
            page.id: await self._attachment_file_names(
                owner_id=project.id, wiki_id=page.id, syn=syn
            )
            for page in pages
        }

        return {
            "project": project,
            "root": root_wiki,
            "sub": sub_wiki,
            "sub_sub": sub_sub_wiki,
            "attachment_name": os.path.basename(attachment_file),
            "markdown": source_markdown,
            "attachment_names": source_attachment_names,
        }

    @pytest.fixture(scope="function")
    async def destination_project(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> Folder:
        """Create a fresh destination Folder for each test.

        Each test writes a new root wiki page into this fixture, so it must
        stay function-scoped for isolation. A Folder inside the session-shared
        project is a valid wiki owner and much cheaper to create than a Project.
        """
        folder = await Folder(
            name=f"Test Wiki Copy Destination Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        return folder

    @staticmethod
    async def _read_markdown(
        owner_id: str,
        wiki_id: str,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> str:
        """Download the markdown file of a wiki page and return its content."""
        download_dir = tempfile.mkdtemp()
        schedule_for_cleanup(download_dir)
        downloaded_path = await WikiPage(
            owner_id=owner_id, id=wiki_id
        ).get_markdown_file_async(
            download_file=True,
            download_location=download_dir,
            synapse_client=syn,
        )
        with open(downloaded_path, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    async def _attachment_file_names(
        owner_id: str, wiki_id: str, syn: Synapse
    ) -> list[str]:
        """Return the sorted non-preview attachment file names of a wiki page."""
        attachment_handles = await WikiPage(
            owner_id=owner_id, id=wiki_id
        ).get_attachment_handles_async(synapse_client=syn)
        return sorted(
            handle.get("fileName")
            for handle in attachment_handles["list"]
            if not handle.get("isPreview")
        )

    async def test_copy_entire_wiki_tree(
        self,
        source_wiki_tree: dict,
        destination_project: Folder,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test copying an entire wiki tree to another entity in a single copy,
        verifying the hierarchy, that internal links and entity IDs are
        rewritten while the rest of every page's markdown stays byte-for-byte
        identical, and that attachments are copied at every level of the tree.
        """
        # GIVEN a source project with a wiki tree and an empty destination project
        source_project = source_wiki_tree["project"]
        source_pages = [
            source_wiki_tree["root"],
            source_wiki_tree["sub"],
            source_wiki_tree["sub_sub"],
        ]

        # WHEN copying the entire wiki with link updating and an entity_map
        new_headers = await WikiPage(owner_id=source_project.id).copy_async(
            destination_owner_id=destination_project.id,
            entity_map={self.OLD_ENTITY_ID: self.NEW_ENTITY_ID},
            synapse_client=syn,
        )

        # THEN all three pages should be copied with their titles preserved
        assert len(new_headers) == 3
        assert all(isinstance(header, WikiHeader) for header in new_headers)
        headers_by_title = {header.title: header for header in new_headers}
        new_root = headers_by_title[source_wiki_tree["root"].title]
        new_sub = headers_by_title[source_wiki_tree["sub"].title]
        new_sub_sub = headers_by_title[source_wiki_tree["sub_sub"].title]

        # AND the page hierarchy should be preserved
        assert new_root.parent_id is None
        assert new_sub.parent_id == new_root.id
        assert new_sub_sub.parent_id == new_sub.id

        # AND every copied page's markdown should equal the source markdown
        # with only the expected link and entity ID substitutions applied
        wiki_id_map = {
            page.id: headers_by_title[page.title].id for page in source_pages
        }
        copied_markdown_by_source_id = {}
        for page in source_pages:
            expected_markdown = source_wiki_tree["markdown"][page.id]
            for old_wiki_id, new_wiki_id in wiki_id_map.items():
                expected_markdown = expected_markdown.replace(
                    f"{source_project.id}/wiki/{old_wiki_id}",
                    f"{destination_project.id}/wiki/{new_wiki_id}",
                )
            expected_markdown = re.sub(
                self.OLD_ENTITY_ID + r"\b", self.NEW_ENTITY_ID, expected_markdown
            )

            copied_markdown = await self._read_markdown(
                owner_id=destination_project.id,
                wiki_id=wiki_id_map[page.id],
                syn=syn,
                schedule_for_cleanup=schedule_for_cleanup,
            )
            assert copied_markdown == expected_markdown
            copied_markdown_by_source_id[page.id] = copied_markdown

        # AND the internal wiki link should point at the copied sub page rather
        # than the source
        copied_root_markdown = copied_markdown_by_source_id[source_wiki_tree["root"].id]
        assert f"{destination_project.id}/wiki/{new_sub.id}" in copied_root_markdown
        assert source_project.id not in copied_root_markdown

        # AND each copied page should have the same non-preview attachment file
        # names as its source page
        pages_with_attachments = 0
        for page in source_pages:
            source_names = source_wiki_tree["attachment_names"][page.id]
            copied_names = await self._attachment_file_names(
                owner_id=destination_project.id,
                wiki_id=wiki_id_map[page.id],
                syn=syn,
            )
            assert copied_names == source_names
            if source_names:
                pages_with_attachments += 1

        # AND the comparison is not vacuous - the source tree has attachments
        # at two different levels. Text attachments are gzipped on upload, so
        # the stored file name has a .gz suffix.
        assert pages_with_attachments == 2
        assert source_wiki_tree["attachment_names"][source_wiki_tree["sub"].id] == [
            f"{source_wiki_tree['attachment_name']}.gz"
        ]

    async def test_copy_wiki_sub_tree(
        self,
        source_wiki_tree: dict,
        destination_project: Folder,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test copying only a wiki sub-tree, verifying the sub page becomes
        the root of the destination wiki."""
        # GIVEN a source project with a wiki tree and an empty destination project
        source_project = source_wiki_tree["project"]
        sub_wiki = source_wiki_tree["sub"]

        # WHEN copying only the sub-tree rooted at the sub wiki page
        new_headers = await WikiPage(
            owner_id=source_project.id, id=sub_wiki.id
        ).copy_async(
            destination_owner_id=destination_project.id,
            synapse_client=syn,
        )

        # THEN only the sub page and its child should be copied
        assert len(new_headers) == 2
        headers_by_title = {header.title: header for header in new_headers}
        new_sub = headers_by_title[sub_wiki.title]
        new_sub_sub = headers_by_title[source_wiki_tree["sub_sub"].title]

        # AND the copied sub page should become the root of the destination wiki
        assert new_sub.parent_id is None
        assert new_sub_sub.parent_id == new_sub.id

    async def test_copy_wiki_into_existing_destination_page(
        self,
        source_wiki_tree: dict,
        destination_project: Folder,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test copying a wiki sub-tree into an existing destination wiki page
        via destination_sub_page_id, overwriting that page with the copied root."""
        # GIVEN a destination project with an existing root wiki page
        source_project = source_wiki_tree["project"]
        sub_wiki = source_wiki_tree["sub"]
        destination_root = await WikiPage(
            owner_id=destination_project.id,
            title=f"Destination Root {str(uuid.uuid4())}",
            markdown="# Destination Root\n\nThis page will be overwritten.",
        ).store_async(synapse_client=syn)

        # WHEN copying the sub-tree into the existing destination page
        new_headers = await WikiPage(
            owner_id=source_project.id, id=sub_wiki.id
        ).copy_async(
            destination_owner_id=destination_project.id,
            destination_sub_page_id=destination_root.id,
            synapse_client=syn,
        )

        # THEN the root of the copied tree should be written into the
        # existing destination page
        assert len(new_headers) == 2
        updated_destination_root = await WikiPage(
            owner_id=destination_project.id, id=destination_root.id
        ).get_async(synapse_client=syn)
        assert updated_destination_root.title == sub_wiki.title

        # AND the child page should be created under the destination page
        headers_by_title = {header.title: header for header in new_headers}
        new_sub_sub = headers_by_title[source_wiki_tree["sub_sub"].title]
        assert new_sub_sub.parent_id == destination_root.id

        # AND the copied pages' markdown should match the source pages,
        # replacing the original destination page content. Neither source
        # page contains links or entity IDs, so the markdown should be
        # copied verbatim.
        destination_root_markdown = await self._read_markdown(
            owner_id=destination_project.id,
            wiki_id=destination_root.id,
            syn=syn,
            schedule_for_cleanup=schedule_for_cleanup,
        )
        assert destination_root_markdown == source_wiki_tree["markdown"][sub_wiki.id]

        new_sub_sub_markdown = await self._read_markdown(
            owner_id=destination_project.id,
            wiki_id=new_sub_sub.id,
            syn=syn,
            schedule_for_cleanup=schedule_for_cleanup,
        )
        assert (
            new_sub_sub_markdown
            == source_wiki_tree["markdown"][source_wiki_tree["sub_sub"].id]
        )

    async def test_copy_wiki_from_entity_without_wiki(
        self,
        source_wiki_tree: dict,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """Test that copying from an entity that has no wiki returns an
        empty list instead of raising an error."""
        # GIVEN a source Folder without any wiki pages
        empty_source_project = await Folder(
            name=f"Test Wiki Copy Empty Source Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(empty_source_project.id)

        # WHEN copying its wiki to another entity. No destination project is
        # created because the copy returns before the destination is contacted.
        # The class source project is reused as the destination ID, and because
        # it already has a root wiki the copy would fail rather than silently
        # write anything if that short-circuit ever stopped happening.
        new_headers = await WikiPage(owner_id=empty_source_project.id).copy_async(
            destination_owner_id=source_wiki_tree["project"].id,
            synapse_client=syn,
        )

        # THEN an empty list should be returned
        assert new_headers == []


class TestWikiOrderHint:
    """Tests for WikiOrderHint operations."""

    @pytest.fixture(scope="class")
    async def wiki_page_fixture(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> WikiPage:
        """Create a root wiki page fixture shared across tests in this class.

        Synapse allows only one root wiki page per owner entity, so this class
        owns its wiki via a Folder created inside the session-shared project
        rather than creating its own Project.
        """
        folder = await Folder(
            name=f"Test Wiki Order Hint Folder_" + str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)
        wiki_title = f"Root Wiki Page {str(uuid.uuid4())}"
        wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."
        wiki_page = WikiPage(
            owner_id=folder.id,
            title=wiki_title,
            markdown=wiki_markdown,
        )
        root_wiki = await wiki_page.store_async(synapse_client=syn)
        return root_wiki

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_wiki_order_hint(
        self,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        await asyncio.sleep(5)
        # WHEN getting the wiki order hint
        order_hint = await WikiOrderHint(owner_id=wiki_page_fixture.owner_id).get_async(
            synapse_client=self.syn
        )
        # THEN order hint should be returned
        assert (
            len(order_hint.id_list) == 0
        )  # this is expected because the order hint is not set by default
        schedule_for_cleanup(order_hint)

    async def test_store_wiki_order_hint(
        self, wiki_page_fixture: WikiPage, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        await asyncio.sleep(5)
        # Get headers
        headers = []
        async for header in WikiHeader.get_async(
            owner_id=wiki_page_fixture.owner_id, synapse_client=self.syn
        ):
            headers.append(header)
        # Get the ids of the headers
        header_ids = [header.id for header in headers]
        # Get initial order hint
        order_hint = await WikiOrderHint(owner_id=wiki_page_fixture.owner_id).get_async(
            synapse_client=self.syn
        )
        schedule_for_cleanup(order_hint)
        # WHEN setting a custom order
        order_hint.id_list = header_ids
        updated_order_hint = await order_hint.store_async(synapse_client=self.syn)
        schedule_for_cleanup(updated_order_hint)
        await asyncio.sleep(5)
        # THEN the order hint should be updated
        # Retrieve the updated order hint
        retrieved_order_hint = await WikiOrderHint(
            owner_id=wiki_page_fixture.owner_id
        ).get_async(synapse_client=self.syn)
        schedule_for_cleanup(retrieved_order_hint)
        assert retrieved_order_hint.id_list == header_ids
        assert len(retrieved_order_hint.id_list) >= 1

    # clean up the wiki pages for other tests in the same session
    async def test_cleanup_wiki_pages(self, wiki_page_fixture: WikiPage):
        root_wiki = wiki_page_fixture
        await root_wiki.delete_async(synapse_client=self.syn)
        assert True
