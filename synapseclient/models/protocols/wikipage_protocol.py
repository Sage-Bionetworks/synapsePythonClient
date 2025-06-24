"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient.models import (
        WikiHeader,
        WikiHistorySnapshot,
        WikiOrderHint,
        WikiPage,
    )


@async_to_sync
class WikiOrderHintSynchronousProtocol(Protocol):
    """Protocol for the methods of the WikiOrderHint class that have synchronous counterparts
    generated at runtime."""

    def store(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "WikiOrderHint":
        """
        Update the order hint of a wiki page tree.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The updated WikiOrderHint object for the entity.
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
        """
        return self


@async_to_sync
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
    ) -> List["WikiHistorySnapshot"]:
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


@async_to_sync
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
    ) -> List["WikiHeader"]:
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


@async_to_sync
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
        """
        return self

    def restore(self, *, synapse_client: Optional["Synapse"] = None) -> "WikiPage":
        """
        Restore a specific version of the wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The restored WikiPage object.
        """
        return self

    def get(self, *, synapse_client: Optional["Synapse"] = None) -> "WikiPage":
        """
        Get a wiki page from Synapse.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The WikiPage object.
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
    ) -> List[Dict[str, Any]]:
        """
        Get the file handles of all attachments on this wiki page.
        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The list of FileHandles for all file attachments of this WikiPage.
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
        """
        return ""

    def get_markdown(
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
        """
        return ""
