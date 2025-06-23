"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, List, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import (
        WikiHeader,
        WikiHistorySnapshot,
        WikiOrderHint,
        WikiPage,
    )


class WikiOrderHintSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiOrderHint class that have synchronous counterparts
    generated at runtime."""

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> WikiOrderHint:
        """
        Get the order hint of a wiki page tree.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A WikiOrderHint object for the entity.
        """
        return self

    def update(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> WikiOrderHint:
        """
        Update the order hint of a wiki page tree.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The updated WikiOrderHint object for the entity.
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
    ) -> List[WikiHistorySnapshot]:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.
        Arguments:
            owner_id: The ID of the owner entity.
            wiki_id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHistorySnapshot objects for the wiki page.
        """
        return list({})


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
    ) -> List[WikiHeader]:
        """
        Get the header tree (hierarchy) of wiki pages for an entity.
        Arguments:
            owner_id: The ID of the owner entity.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHeader objects for the entity.
        """
        return list({})


class WikiPageSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiPage class that have synchronous counterparts
    generated at runtime."""

    def create(
        self, *, synapse_client: Optional["Synapse"] = None, force_version: bool = False
    ) -> WikiPage:
        """
        Create a new wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
            force_version: If True, the wiki page will be created with a new version number.
        Returns:
            The created WikiPage object.
        """
        return self

    def get(self, *, synapse_client: Optional["Synapse"] = None) -> WikiPage:
        """
        Get a wiki page from Synapse asynchronously.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The WikiPage object.
        """
        return self

    def update(
        self, *, force_version: bool = False, synapse_client: Optional["Synapse"] = None
    ) -> WikiPage:
        """
        Update a wiki page asynchronously. If force_version is True, restore a specific version of the content.
        Arguments:
            force_version: If True, update a specific version of the wiki page (restore).
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The updated WikiPage object.
        """
        return self

    def delete(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Delete this wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            None
        """
        return None

    def get_attachment_handles(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> list:
        """
        Get the file handles of all attachments on this wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The list of FileHandles for all file attachments of this WikiPage.
        """
        return list([])

    def get_attachment_url(
        self,
        file_name: str,
        *,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the URL of a wiki page attachment.
        Arguments:
            file_name: The name of the file to get.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The URL that can be used to download a file for a given WikiPage file attachment.
        """
        return ""

    def get_attachment_preview_url(
        self,
        file_name: str,
        *,
        wiki_version: Optional[int] = None,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the preview URL of a wiki page attachment asynchronously.
        Arguments:
            file_name: The name of the file to get.
            wiki_version: Optional version of the wiki page. If not provided, uses self.wiki_version.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The URL that can be used to download a preview file for a given WikiPage file attachment.
        """
        return ""

    def get_markdown_url_async(
        self,
        *,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the markdown URL of this wiki page asynchronously.
        Arguments:
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The URL that can be used to download the markdown file for this WikiPage.
        """
        return ""
