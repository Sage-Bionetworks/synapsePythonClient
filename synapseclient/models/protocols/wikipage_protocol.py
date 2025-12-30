"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import (
        WikiHeader,
        WikiHistorySnapshot,
        WikiOrderHint,
        WikiPage,
    )

from synapseclient.core.async_utils import wrap_async_generator_to_sync_generator


class WikiOrderHintSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiOrderHint class that have synchronous counterparts
    generated at runtime.
    """

    def store(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "WikiOrderHint":
        """
        Store the order hint of a wiki page tree.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The updated WikiOrderHint object for the entity.

        Example: Set the WikiOrderHint for a project
            This example shows how to set a WikiOrderHint for existing wiki pages in a project.
            The WikiOrderHint is not set by default, so you need to set it explicitly.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import (
                Project,
                WikiOrderHint,
            )
            syn = Synapse()
            syn.login()

            project = Project(name="My uniquely named project about Alzheimer's Disease").get()
            # get the WikiOrderHint for the project
            wiki_order_hint = WikiOrderHint(owner_id=project.id).get()

            wiki_order_hint.id_list = [
                root_wiki_page.id,
                wiki_page_1.id,
                wiki_page_3.id,
                wiki_page_2.id,
                wiki_page_4.id,
            ]
            wiki_order_hint.store()
            print(wiki_order_hint)

        Example: Update the WikiOrderHint for a project
            This example shows how to update a WikiOrderHint for existing wiki pages in a project.
            ```python
            wiki_order_hint.id_list = [
                root_wiki_page.id,
                wiki_page_1.id,
                wiki_page_2.id,
                wiki_page_3.id,
                wiki_page_4.id,
            ]
            wiki_order_hint.store()
            print(wiki_order_hint)
            ```
        """
        return self

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "WikiOrderHint":
        """
        Get the order hint of a wiki page tree.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A WikiOrderHint object for the entity.

        Example: Get the WikiOrderHint for a project
            This example shows how to get a WikiOrderHint for existing wiki pages in a project.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import (
                Project,
                WikiOrderHint,
            )
            syn = Synapse()
            syn.login()

            project = Project(name="My uniquely named project about Alzheimer's Disease").get()
            # get the WikiOrderHint for the project
            wiki_order_hint = WikiOrderHint(owner_id=project.id).get()

            print(wiki_order_hint)
            ```
        """
        return self


class WikiHistorySnapshotSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiHistorySnapshot class that have synchronous counterparts
    generated at runtime."""

    @classmethod
    def get(
        cls,
        owner_id: str,
        wiki_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> Generator["WikiHistorySnapshot", None, None]:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.
        Arguments:
            owner_id: The ID of the owner entity.
            wiki_id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Yields:
            Individual WikiHistorySnapshot objects from each page of the response.

        Example: Get the history of a wiki page
            ```python
            for history in WikiHistorySnapshot.get(owner_id=project.id, id=wiki_page.id):
                print(f"History: {history}")
            ```
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=WikiHistorySnapshot.get_async,
            owner_id=owner_id,
            id=wiki_id,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )


class WikiHeaderSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiHeader class that have synchronous counterparts
    generated at runtime."""

    @classmethod
    def get(
        cls,
        owner_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> Generator["WikiHeader", None, None]:
        """
        Get the header tree (hierarchy) of wiki pages for an entity.
        Arguments:
            owner_id: The ID of the owner entity.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Yields:
            Individual WikiHeader objects for the entity.

        Example: Get the header tree (hierarchy) of wiki pages for an entity
            ```python
            for header in WikiHeader.get(owner_id=project.id):
                print(f"Header: {header}")
            ```
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=WikiHeader.get_async,
            owner_id=owner_id,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )


class WikiPageSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiPage class that have synchronous counterparts
    generated at runtime."""

    def store(self, *, synapse_client: Optional["Synapse"] = None) -> "WikiPage":
        """
        Store the wiki page. If there is no wiki page, a new wiki page will be created.
        If the wiki page already exists, it will be updated.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The created WikiPage object.

        Example: Store a wiki page
            This example shows how to store a wiki page.
            from synapseclient import Synapse
            from synapseclient.models import (
                Project,
                WikiPage,
            )
            syn = Synapse()
            syn.login()

            project = Project(name="My uniquely named project about Alzheimer's Disease").get()
            wiki_page = WikiPage(owner_id=project.id, title="My wiki page").store()
            print(wiki_page)
        """
        return self

    def restore(self, *, synapse_client: Optional["Synapse"] = None) -> "WikiPage":
        """
        Restore a specific version of the wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The restored WikiPage object.

        Example: Restore a specific version of a wiki page
            This example shows how to restore a specific version of a wiki page.
            wiki_page_restored = WikiPage(owner_id=project.id, id=root_wiki_page.id, wiki_version="0").restore()
            print(wiki_page_restored)
        """
        return self

    def get(self, *, synapse_client: Optional["Synapse"] = None) -> "WikiPage":
        """
        Get a wiki page from Synapse.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The WikiPage object.

        Example: Get a wiki page from Synapse
            This example shows how to get a wiki page from Synapse.
            wiki_page = WikiPage(owner_id=project.id, id=wiki_page.id).get()
            print(wiki_page)
        """
        return self

    def delete(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Delete this wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            None

        Example: Delete a wiki page
            This example shows how to delete a wiki page.
            wiki_page = WikiPage(owner_id=project.id, id=wiki_page.id).delete()
            print(f"Wiki page {wiki_page.title} deleted successfully.")
        """
        return None

    def get_attachment_handles(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> List[Dict[str, Any]]:
        """
        Get the file handles of all attachments on this wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The list of FileHandles for all file attachments of this WikiPage.

        Example: Get the file handles of all attachments on a wiki page
            This example shows how to get the file handles of all attachments on a wiki page.
            attachment_handles = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_handles()
            print(f"Attachment handles: {attachment_handles['list']}")
        """
        return list({})

    def get_attachment(
        self,
        file_name: str,
        *,
        download_file: bool = True,
        download_location: Optional[str] = None,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the wiki page attachment to a local file or return the URL.
        Arguments:
            file_name: The name of the file to get.
            download_file: Whether to download the file. Default is True.
            download_location: The location to download the file to. Required if download_file is True.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the attachment file will be downloaded to the download_location. Otherwise, the URL will be returned.

        Example: Get the attachment URL for a wiki page
            This example shows how to get the attachment file or URL for a wiki page.
            attachment_file_or_url = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment(file_name="attachment.txt", download_file=False)
            print(f"Attachment URL: {attachment_file_or_url}")

        Example: Download the attachment file for a wiki page
            This example shows how to download the attachment file for a wiki page.
            attachment_file_path = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment(file_name="attachment.txt", download_file=True, download_location="~/temp")
            print(f"Attachment file path: {attachment_file_path}")
        """
        return ""

    def get_attachment_preview(
        self,
        file_name: str,
        *,
        wiki_version: Optional[int] = None,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the wiki page attachment preview to a local file or return the URL.
        Arguments:
            file_name: The name of the file to get.
            wiki_version: Optional version of the wiki page. If not provided, uses self.wiki_version.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the attachment preview file will be downloaded to the download_location. Otherwise, the URL will be returned.

        Example: Get the attachment preview URL for a wiki page
            This example shows how to get the attachment preview URL for a wiki page.
            Instead of using the file_name from the attachmenthandle response when isPreview=True, you should use the original file name in the get_attachment_preview request.
            The downloaded file will still be named according to the file_name provided in the response when isPreview=True.
            attachment_preview_url = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_preview(file_name="attachment.txt.gz", download_file=False)
            print(f"Attachment preview URL: {attachment_preview_url}")

        Example: Download the attachment preview file for a wiki page
            This example shows how to download the attachment preview file for a wiki page.
            attachment_preview_file_path = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_preview(file_name="attachment.txt.gz", download_file=True, download_location="~/temp")
            print(f"Attachment preview file path: {attachment_preview_file_path}")
        """
        return ""

    def get_markdown_file(
        self,
        *,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the markdown file to a local file or return the URL.
        Arguments:
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the markdown file will be downloaded to the download_location. Otherwise, the URL will be returned.

        Example: Get the markdown URL for a wiki page
            This example shows how to get the markdown URL for a wiki page.
            markdown_url = WikiPage(owner_id=project.id, id=wiki_page.id).get_markdown_file(download_file=False)
            print(f"Markdown URL: {markdown_url}")

        Example: Download the markdown file for a wiki page
            This example shows how to download the markdown file for a wiki page.
            markdown_file_path = WikiPage(owner_id=project.id, id=wiki_page.id).get_markdown_file(download_file=True, download_location="~/temp")
            print(f"Markdown file path: {markdown_file_path}")
        """
        return ""
