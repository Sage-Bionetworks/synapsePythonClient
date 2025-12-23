"""Integration tests for the synapseclient.models.wiki module."""

import gzip
import os
import tempfile
import time
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Project,
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)


@pytest.fixture(scope="session")
def wiki_page_fixture(
    project_model: Project, syn: Synapse, schedule_for_cleanup: Callable[..., None]
) -> WikiPage:
    """Create a root wiki page fixture that can be shared across tests."""
    wiki_title = f"Root Wiki Page"
    wiki_markdown = "# Root Wiki Page\n\nThis is a root wiki page."

    wiki_page = WikiPage(
        owner_id=project_model.id,
        title=wiki_title,
        markdown=wiki_markdown,
    )
    root_wiki = wiki_page.store(synapse_client=syn)
    schedule_for_cleanup(root_wiki.id)
    return root_wiki


class TestWikiPageBasicOperations:
    """Tests for basic WikiPage CRUD operations."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_get_wiki_page_by_id(
        self,
        project_model: Project,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting a wiki page by ID."""
        # GIVEN an existing wiki page (from fixture)
        root_wiki = wiki_page_fixture

        # WHEN retrieving the wiki page by ID
        retrieved_wiki = WikiPage(owner_id=project_model.id, id=root_wiki.id).get(
            synapse_client=self.syn
        )
        schedule_for_cleanup(retrieved_wiki.id)

        # THEN the retrieved wiki should match the created one
        assert retrieved_wiki.id == root_wiki.id
        assert retrieved_wiki.title == root_wiki.title
        assert retrieved_wiki.owner_id == project_model.id

    def test_get_wiki_page_by_title(
        self,
        project_model: Project,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting a wiki page by title."""
        # GIVEN an existing wiki page (from fixture)
        root_wiki = wiki_page_fixture

        # WHEN retrieving the wiki page by title
        retrieved_wiki = WikiPage(owner_id=project_model.id, title=root_wiki.title).get(
            synapse_client=self.syn
        )
        schedule_for_cleanup(retrieved_wiki.id)
        # THEN the retrieved wiki should match the created one
        assert retrieved_wiki.id == root_wiki.id
        assert retrieved_wiki.title == root_wiki.title
        assert retrieved_wiki.owner_id == project_model.id

    def test_delete_wiki_page(
        self,
        project_model: Project,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN an existing wiki page (from fixture)
        root_wiki = wiki_page_fixture

        # WHEN creating a wiki page to delete
        wiki_page_to_delete = WikiPage(
            owner_id=project_model.id,
            parent_id=root_wiki.id,
            title="Wiki Page to be deleted",
            markdown=f"# Wiki Page to be deleted\n\nThis is a wiki page to be deleted.",
        ).store(synapse_client=self.syn)
        schedule_for_cleanup(wiki_page_to_delete.id)
        # WHEN deleting the wiki page
        wiki_page_to_delete.delete(synapse_client=self.syn)

        # THEN the wiki page should be deleted
        with pytest.raises(SynapseHTTPError, match="404"):
            WikiPage(owner_id=project_model.id, id=wiki_page_to_delete.id).get(
                synapse_client=self.syn
            )

    def test_create_sub_wiki_page(
        self,
        project_model: Project,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test creating a sub-wiki page under a root wiki page."""
        # GIVEN a root wiki page
        root_wiki = wiki_page_fixture

        # WHEN creating a sub-wiki page
        sub_wiki = WikiPage(
            owner_id=project_model.id,
            parent_id=root_wiki.id,
            title="Sub Wiki Basic Operations",
            markdown="# Sub Wiki Basic Operations\n\nThis is a sub wiki basic operations page.",
        ).store(synapse_client=self.syn)
        schedule_for_cleanup(sub_wiki.id)
        # THEN the sub-wiki page should be created
        assert sub_wiki.id is not None
        assert sub_wiki.title == "Sub Wiki Basic Operations"
        assert sub_wiki.parent_id == root_wiki.id
        assert sub_wiki.owner_id == project_model.id


class TestWikiPageAttachments:
    """Tests for WikiPage attachment operations."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    def wiki_page_with_attachment(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> tuple[WikiPage, str]:
        """Create a wiki page with an attachment."""
        # Create a temporary file for attachment
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("hello world\n")
        schedule_for_cleanup(f.name)
        # GIVEN a root wiki page
        root_wiki = wiki_page_fixture
        # Create wiki page with attachment
        wiki_page = WikiPage(
            owner_id=project_model.id,
            title=f"Sub Wiki with Attachment",
            markdown="# Sub Wiki with Attachment\n\nThis is a sub wiki with an attachment page.",
            parent_id=root_wiki.id,
            attachments=[f.name],
        )
        wiki_page = wiki_page.store(synapse_client=syn)
        schedule_for_cleanup(wiki_page.id)
        attachment_name = os.path.basename(f.name)
        schedule_for_cleanup(attachment_name)
        return wiki_page, attachment_name

    def test_get_attachment_handles(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # WHEN getting attachment handles
        attachment_handles = wiki_page.get_attachment_handles(synapse_client=self.syn)

        # THEN attachment handles should be returned
        assert len(attachment_handles["list"]) > 0
        schedule_for_cleanup(attachment_handles)

    def test_get_attachment_url(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # WHEN getting attachment URL
        attachment_url = wiki_page.get_attachment(
            file_name=attachment_name, download_file=False, synapse_client=self.syn
        )

        # THEN a URL should be returned
        assert len(attachment_url) > 0
        schedule_for_cleanup(attachment_url)

    def test_download_attachment(
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
        downloaded_path = wiki_page.get_attachment(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)

    def test_get_attachment_preview_url(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment
        # Sleep for 0.5 minutes to ensure the attachment preview is created
        time.sleep(0.5 * 60)
        # WHEN getting attachment preview URL
        preview_url = wiki_page.get_attachment_preview(
            file_name=attachment_name, download_file=False, synapse_client=self.syn
        )

        # THEN a URL should be returned
        assert len(preview_url) > 0
        schedule_for_cleanup(preview_url)

    def test_download_attachment_preview(
        self,
        wiki_page_with_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with an attachment
        wiki_page, attachment_name = wiki_page_with_attachment

        # AND a download location
        download_dir = tempfile.mkdtemp()
        self.schedule_for_cleanup(download_dir)

        # WHEN downloading the attachment preview
        downloaded_path = wiki_page.get_attachment_preview(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloadeds
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) == "preview.txt"

    @pytest.mark.skipif(
        os.getenv("GITHUB_ACTIONS") == "true",
        reason="This test runs only locally, not in CI/CD environments.",
    )
    def test_download_attachment_large_file(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> None:
        """Test downloading a large attachment file (> 8 MiB) - local only."""
        # GIVEN a wiki page with an attachment
        root_wiki = wiki_page_fixture
        # Create a temporary file for attachment with > 8 MiB
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"\0" * (9 * 1024 * 1024))

        # AND a download location
        download_dir = tempfile.mkdtemp()
        schedule_for_cleanup(download_dir)

        # Create wiki page with attachment
        wiki_page = WikiPage(
            owner_id=project_model.id,
            title=f"Sub Wiki with large Attachment",
            markdown="# Sub Wiki with large Attachment\n\nThis is a sub wiki with a large attachment page.",
            parent_id=root_wiki.id,
            attachments=[f.name],
        )
        wiki_page = wiki_page.store(synapse_client=self.syn)
        schedule_for_cleanup(wiki_page.id)
        # WHEN downloading the attachment
        downloaded_path = wiki_page.get_attachment(
            file_name=os.path.basename(f.name),
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)
        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) == os.path.basename(f.name)

    @pytest.fixture(scope="class")
    def wiki_page_with_gz_attachment(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> tuple[WikiPage, str]:
        """Create a wiki page with a gzipped attachment."""
        # Create a gzipped file
        tmp = tempfile.NamedTemporaryFile(suffix=".txt.gz", delete=False)
        tmp.close()  # Close the file so it can be written to by gzip
        with gzip.open(tmp.name, "wt") as f:
            f.write("hello world\n")
        schedule_for_cleanup(tmp.name)

        # GIVEN a root wiki page
        root_wiki = wiki_page_fixture
        # Create wiki page with gz attachment
        wiki_page = WikiPage(
            owner_id=project_model.id,
            title=f"Sub Wiki with GZ Attachment",
            markdown="# Sub Wiki with GZ Attachment\n\nThis is a sub wiki with a gz attachment page.",
            parent_id=root_wiki.id,
            attachments=[tmp.name],
        )
        sub_wiki = wiki_page.store(synapse_client=syn)
        schedule_for_cleanup(sub_wiki.id)
        attachment_name = os.path.basename(tmp.name)
        schedule_for_cleanup(attachment_name)
        return sub_wiki, attachment_name

    def test_get_attachment_handles_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting attachment handles for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment
        # WHEN getting attachment handles
        attachment_handles = wiki_page.get_attachment_handles(synapse_client=self.syn)

        # THEN attachment handles should be returned
        assert len(attachment_handles["list"]) > 0
        # Verify the attachment name contains .gz
        assert any(
            handle.get("fileName", "").endswith(".gz")
            for handle in attachment_handles["list"]
        )
        schedule_for_cleanup(attachment_handles)

    def test_get_attachment_url_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting attachment URL for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment

        # WHEN getting attachment URL
        attachment_url = wiki_page.get_attachment(
            file_name=attachment_name, download_file=False, synapse_client=self.syn
        )
        # THEN a URL should be returned
        assert len(attachment_url) > 0
        schedule_for_cleanup(attachment_url)

    def test_download_attachment_gz_file(
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
        downloaded_path = wiki_page.get_attachment(
            file_name=attachment_name,
            download_file=True,
            download_location=download_dir,
            synapse_client=self.syn,
        )
        schedule_for_cleanup(downloaded_path)

        # THEN the file should be downloaded
        assert os.path.exists(downloaded_path)
        assert os.path.basename(downloaded_path) + ".gz" == attachment_name

    def test_get_attachment_preview_url_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test getting attachment preview URL for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment
        # Sleep for 0.5 minutes to ensure the attachment preview is created
        time.sleep(0.5 * 60)
        # WHEN getting attachment preview URL
        preview_url = wiki_page.get_attachment_preview(
            file_name=attachment_name, download_file=False, synapse_client=self.syn
        )

        # THEN a URL should be returned
        assert len(preview_url) > 0
        schedule_for_cleanup(preview_url)

    def test_download_attachment_preview_gz_file(
        self,
        wiki_page_with_gz_attachment: tuple[WikiPage, str],
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test downloading attachment preview for a gz file."""
        # GIVEN a wiki page with a gz attachment
        wiki_page, attachment_name = wiki_page_with_gz_attachment

        # AND a download location
        download_dir = tempfile.mkdtemp()
        self.schedule_for_cleanup(download_dir)

        # WHEN downloading the attachment preview
        downloaded_path = wiki_page.get_attachment_preview(
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

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    def wiki_page_with_markdown(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> WikiPage:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_fixture
        wiki_page = WikiPage(
            owner_id=project_model.id,
            title=f"Sub Wiki Markdown",
            markdown="# Sub Wiki Markdown\n\nThis is a sub wiki markdown page.",
            parent_id=root_wiki.id,
        )
        sub_wiki = wiki_page.store(synapse_client=syn)
        schedule_for_cleanup(sub_wiki.id)
        return sub_wiki

    def test_get_markdown_url(
        self,
        project_model: Project,
        wiki_page_with_markdown: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_with_markdown

        # WHEN getting markdown URL
        markdown_url = WikiPage(
            owner_id=project_model.id, id=root_wiki.id
        ).get_markdown_file(download_file=False, synapse_client=self.syn)
        schedule_for_cleanup(markdown_url)
        # THEN a URL should be returned
        assert len(markdown_url) > 0

    def test_download_markdown_file(
        self,
        project_model: Project,
        wiki_page_with_markdown: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_with_markdown

        # AND a download location
        download_dir = tempfile.mkdtemp()

        # WHEN downloading the markdown file
        downloaded_path = WikiPage(
            owner_id=project_model.id, id=root_wiki.id
        ).get_markdown_file(
            download_file=True, download_location=download_dir, synapse_client=self.syn
        )
        # THEN the file should be downloaded and unzipped
        assert os.path.exists(downloaded_path)
        # Verify content
        with open(downloaded_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "Sub Wiki Markdown" in content
        schedule_for_cleanup(download_dir)

    @pytest.fixture(scope="class")
    def wiki_page_with_markdown_gz(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> WikiPage:
        # GIVEN a wiki page with markdown
        root_wiki = wiki_page_fixture
        tmp = tempfile.NamedTemporaryFile(suffix=".md.gz", delete=False)
        tmp.close()  # Close the file so it can be written to by gzip
        with gzip.open(tmp.name, "wt") as f:
            f.write("# Test Wiki\n\nThis is test content.")
        schedule_for_cleanup(tmp.name)

        # Create wiki page with markdown gz
        wiki_page = WikiPage(
            owner_id=project_model.id,
            title=f"Test Wiki with GZ Markdown",
            markdown="# Test Wiki with GZ Markdown\n\nThis is test content.",
            parent_id=root_wiki.id,
        )
        sub_wiki = wiki_page.store(synapse_client=syn)
        schedule_for_cleanup(sub_wiki.id)
        return sub_wiki

    def test_get_markdown_url_gz_file(
        self,
        project_model: Project,
        wiki_page_with_markdown_gz: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown gz
        root_wiki = wiki_page_with_markdown_gz

        # WHEN getting markdown URL
        markdown_url = WikiPage(
            owner_id=project_model.id, id=root_wiki.id
        ).get_markdown_file(download_file=False, synapse_client=self.syn)
        schedule_for_cleanup(markdown_url)
        # THEN a URL should be returned
        assert len(markdown_url) > 0

    def test_download_markdown_file_gz_file(
        self,
        project_model: Project,
        wiki_page_with_markdown_gz: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with markdown gz
        root_wiki = wiki_page_with_markdown_gz

        # AND a download location
        download_dir = tempfile.mkdtemp()

        # WHEN downloading the markdown file
        downloaded_path = WikiPage(
            owner_id=project_model.id, id=root_wiki.id
        ).get_markdown_file(
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

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    def wiki_page_with_multiple_versions(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        wiki_page_fixture: WikiPage,
    ) -> WikiPage:
        # GIVEN a wiki page with multiple versions
        root_wiki = wiki_page_fixture
        wiki_page = WikiPage(
            owner_id=project_model.id,
            title="Sub Wiki Versioning",
            markdown="# Sub Wiki Versioning\n\nThis is a sub wiki versioning page.",
            parent_id=root_wiki.id,
        )
        updated_wiki = wiki_page.store(synapse_client=syn)
        # Update the wiki page
        updated_wiki = WikiPage(
            owner_id=project_model.id, id=updated_wiki.id, title="Version 1"
        ).store(synapse_client=syn)
        # Update the wiki page
        updated_wiki = WikiPage(
            owner_id=project_model.id, id=updated_wiki.id, title="Version 2"
        ).store(synapse_client=syn)
        schedule_for_cleanup(updated_wiki.id)
        return updated_wiki

    def test_wiki_page_history(
        self,
        project_model: Project,
        wiki_page_with_multiple_versions,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with multiple versions
        sub_wiki = wiki_page_with_multiple_versions
        # WHEN getting wiki history
        history = WikiHistorySnapshot.get(
            owner_id=project_model.id, id=sub_wiki.id, synapse_client=self.syn
        )
        # THEN history should be returned
        assert len(history) == 3
        schedule_for_cleanup(history)

    def test_restore_wiki_page_version(
        self,
        project_model: Project,
        wiki_page_with_multiple_versions,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a wiki page with multiple versions
        root_wiki = wiki_page_with_multiple_versions
        # Get initial version
        initial_version = "0"
        # WHEN restoring to the initial version
        restored_wiki = WikiPage(
            owner_id=project_model.id,
            id=root_wiki.id,
            wiki_version=initial_version,
        ).restore(synapse_client=self.syn)

        # THEN the wiki should be restored
        assert restored_wiki.title == "Sub Wiki Versioning"
        schedule_for_cleanup(restored_wiki)


class TestWikiHeader:
    """Tests for WikiHeader operations."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_get_wiki_header_tree(
        self,
        project_model: Project,
        wiki_page_fixture: WikiPage,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # WHEN getting the wiki header tree
        headers = WikiHeader.get(owner_id=project_model.id, synapse_client=self.syn)

        # THEN headers should be returned
        assert len(headers) == 8
        schedule_for_cleanup(headers)


class TestWikiOrderHint:
    """Tests for WikiOrderHint operations."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_get_wiki_order_hint(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # WHEN getting the wiki order hint
        order_hint = WikiOrderHint(owner_id=project_model.id).get(
            synapse_client=self.syn
        )
        # THEN order hint should be returned
        assert (
            len(order_hint.id_list) == 0
        )  # this is expected because the order hint is not set by default
        schedule_for_cleanup(order_hint)

    def test_store_wiki_order_hint(
        self, project_model: Project, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        # Get headers
        headers = WikiHeader.get(owner_id=project_model.id, synapse_client=self.syn)
        # Get the ids of the headers
        header_ids = [header.id for header in headers]
        # Get initial order hint
        order_hint = WikiOrderHint(owner_id=project_model.id).get(
            synapse_client=self.syn
        )
        schedule_for_cleanup(order_hint)
        # WHEN setting a custom order
        order_hint.id_list = header_ids
        updated_order_hint = order_hint.store(synapse_client=self.syn)
        schedule_for_cleanup(updated_order_hint)
        # THEN the order hint should be updated
        # Retrieve the updated order hint
        retrieved_order_hint = WikiOrderHint(owner_id=project_model.id).get(
            synapse_client=self.syn
        )
        schedule_for_cleanup(retrieved_order_hint)
        assert retrieved_order_hint.id_list == header_ids
        assert len(retrieved_order_hint.id_list) == 8
