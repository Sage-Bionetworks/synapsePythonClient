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
        max_concurrent: int = 10,
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
            parallel: If ``True``, files are downloaded concurrently up to
                ``max_concurrent`` at a time using ``asyncio.gather``. If ``False``
                (default), files are downloaded sequentially.
            max_concurrent: Maximum number of files to download concurrently when
                ``parallel=True``. Defaults to 10. Has no effect when
                ``parallel=False``.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Path to the result manifest CSV, which contains all original manifest
            columns plus ``path`` (local file path) and ``error`` (error message or
            empty string) columns.

        Example: Using this function:
            Download all files in the cart to a directory:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DownloadList

            syn = Synapse()
            syn.login()

            manifest_path = DownloadList().download_files(download_location="./downloads")
            ```

            Use ``parallel=True`` with ``max_concurrent`` for faster downloads on large carts:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DownloadList

            syn = Synapse()
            syn.login()

            manifest_path = DownloadList().download_files(
                download_location="./downloads",
                parallel=True,
                max_concurrent=5,
            )
            ```
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

        Example: Using this function:
            Inspect the cart contents before downloading:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DownloadList

            syn = Synapse()
            syn.login()

            manifest_path = DownloadList().get_manifest()
            ```
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

        Example: Using this function:
            Add specific file versions to the cart:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DownloadList, DownloadListItem

            syn = Synapse()
            syn.login()

            count = DownloadList().add_files([
                DownloadListItem(file_entity_id="syn123", version_number=1),
                DownloadListItem(file_entity_id="syn456", version_number=2),
            ])
            ```
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

        Example: Using this function:
            Remove specific file versions from the cart:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DownloadList, DownloadListItem

            syn = Synapse()
            syn.login()

            count = DownloadList().remove_files([
                DownloadListItem(file_entity_id="syn123", version_number=1),
            ])
            ```
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

        Example: Using this function:
            Remove all files from the cart:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DownloadList

            syn = Synapse()
            syn.login()

            DownloadList().clear()
            ```
        """
        return
