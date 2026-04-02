"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, List, Optional, Protocol

from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.download_list import DownloadListItem


@async_to_sync
class DownloadListSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def download_files(
        self,
        download_location: Optional[str] = None,
        *,
        parallel: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """Download all files in the Synapse download list (cart) to a local directory.

        Files are downloaded individually. The cart is not packaged into a zip because
        download lists can exceed 100 GB. Only successfully downloaded files are removed
        from the cart after the full pass completes, so interrupted runs are safely
        resumable.

        Arguments:
            download_location: Directory to download files to. Defaults to the
                current working directory.
            parallel: If ``True``, all files are downloaded concurrently using
                ``asyncio.gather``. If ``False`` (default), files are downloaded
                sequentially. Parallel mode is faster for large carts but places
                more load on Synapse servers.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Path to the result manifest CSV, which contains all original manifest
            columns plus ``path`` (local file path) and ``error`` (error message or
            empty string) columns.
        """
        return ""

    def get_manifest(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """Generate and download the download list manifest CSV without downloading files.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Local path to the downloaded manifest CSV.
        """
        return ""

    def add_files(
        self,
        files: List["DownloadListItem"],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> int:
        """Add specific file versions to the Synapse download list.

        Arguments:
            files: List of DownloadListItem objects identifying file versions to add.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The number of files added.
        """
        return 0

    def remove_files(
        self,
        files: List["DownloadListItem"],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> int:
        """Remove specific file versions from the Synapse download list.

        Arguments:
            files: List of DownloadListItem objects identifying file versions to remove.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The number of files removed.
        """
        return 0

    def clear(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """Clear all files from the Synapse download list (cart).

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
        """
        return
