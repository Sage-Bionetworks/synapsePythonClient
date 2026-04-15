"""Model representing a user's Synapse Download List (cart)."""

import asyncio
import csv
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse

from synapseclient.api.download_list_services import (
    add_to_download_list_async,
    clear_download_list_async,
    remove_from_download_list_async,
)
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants.concrete_types import DOWNLOAD_LIST_MANIFEST_REQUEST
from synapseclient.core.exceptions import SynapseError
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.protocols.download_list_protocol import (
    DownloadListSynchronousProtocol,
)
from synapseclient.models.table_components import CsvTableDescriptor

_PATH_COLUMN = "path"
_ERROR_COLUMN = "error"


@dataclass
class DownloadListItem:
    """A single item for a user's download list.

    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/download/DownloadListItem.html>

    Attributes:
        file_entity_id: Synapse ID of the file entity (e.g. "syn123").
        version_number: Version of the file to target.
    """

    file_entity_id: str
    """Synapse ID of the file entity (e.g. "syn123")."""

    version_number: Optional[int] = None
    """Version of the file to target."""


@dataclass
class DownloadListManifestRequest(AsynchronousCommunicator):
    """Drives the full lifecycle of a Synapse async manifest job in one object.

    Calling send_job_and_wait_async() executes four phases automatically:

    **Phase 1 — Submit** (to_synapse_request)
        Builds the POST body and submits it to
        POST /download/list/manifest/async/start. Synapse starts a
        background job and returns a token.

    **Phase 2 — Poll** (AsynchronousCommunicator base class)
        Polls GET /download/list/manifest/async/get/{token} until the job
        state is COMPLETE (or the timeout is reached). No code needed here
        — the base class handles this using the endpoint registered in
        ASYNC_JOB_URIS for this class's concrete_type.

    **Phase 3 — Parse response** (fill_from_dict)
        Extracts resultFileHandleId from the completed job response and
        stores it in self.result_file_handle_id.

    **Phase 4 — Download** (_post_exchange_async)
        Retrieves file handle metadata via get_file_handle() and a
        pre-signed S3 URL via get_file_handle_presigned_url(), then
        streams the CSV to disk via download_from_url() (run in a thread
        pool via asyncio.to_thread since it is a blocking sync method).
        Stores the local path in self.manifest_path.

    After send_job_and_wait_async() returns, manifest_path holds the
    local path to the downloaded CSV and is ready to use.

    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/download/DownloadListManifestRequest.html>
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/download/DownloadListManifestResponse.html>
    """

    concrete_type: str = field(
        init=False,
        default=DOWNLOAD_LIST_MANIFEST_REQUEST,
    )
    """The concreteType string sent in the request body. Set automatically;
    registered in ASYNC_JOB_URIS to resolve the REST endpoint."""

    result_file_handle_id: Optional[str] = field(init=False, default=None)
    """File handle ID of the generated manifest CSV. Populated by
    fill_from_dict() from the resultFileHandleId field of the job
    response. None until the job completes."""

    manifest_path: Optional[str] = field(init=False, default=None)
    """Absolute local path of the downloaded manifest CSV. Populated by
    _post_exchange_async() after the file is downloaded. None until
    send_job_and_wait_async() returns."""

    csv_table_descriptor: CsvTableDescriptor = field(
        default_factory=CsvTableDescriptor,
    )
    """Describes the format of the generated CSV manifest."""

    def to_synapse_request(self) -> dict[str, Any]:
        """Build the request body for the manifest async job.

        Constructs the POST body for
        POST /download/list/manifest/async/start including the concrete type
        and CSV descriptor.

        Returns:
            A dictionary containing the request body expected by the Synapse
            REST API.
        """
        return {
            "concreteType": self.concrete_type,
            "csvTableDescriptor": self.csv_table_descriptor.to_synapse_request(),
        }

    def fill_from_dict(
        self, synapse_response: dict[str, Any]
    ) -> "DownloadListManifestRequest":
        """Converts the data coming from the Synapse async job response into
        this data class.

        Extracts the resultFileHandleId from the completed job response and
        stores it in result_file_handle_id.

        Arguments:
            synapse_response: The response dict from the completed Synapse
                async manifest job.

        Returns:
            The DownloadListManifestRequest object instance.
        """
        self.result_file_handle_id = synapse_response.get("resultFileHandleId")
        return self

    async def _post_exchange_async(
        self, synapse_client: Optional["Synapse"] = None, **kwargs
    ) -> None:
        """Download the manifest CSV from Synapse after the async job completes.

        Retrieves the file handle metadata and a pre-signed S3 URL using
        creator-based endpoints (no entity association required), then
        streams the CSV to disk using download_from_url (run in a thread
        pool to avoid blocking the event loop). On success, sets
        self.manifest_path to the local path of the downloaded file.

        Arguments:
            synapse_client: The Synapse client to use for the request. Uses
                the cached singleton if omitted.
            **kwargs: Additional arguments. Supports destination (str) to
                control the download directory; defaults to the current
                working directory.
        """
        from synapseclient import Synapse
        from synapseclient.api.file_services import (
            get_file_handle,
            get_file_handle_presigned_url,
        )
        from synapseclient.core.download.download_functions import download_from_url

        destination = kwargs.get("destination", ".")
        client = Synapse.get_client(synapse_client=synapse_client)
        file_handle = await get_file_handle(
            file_handle_id=self.result_file_handle_id,
            synapse_client=client,
        )
        presigned_url = await get_file_handle_presigned_url(
            file_handle_id=self.result_file_handle_id,
            synapse_client=client,
        )
        self.manifest_path = await asyncio.to_thread(
            download_from_url,
            url=presigned_url,
            destination=destination,
            file_handle_id=file_handle["id"],
            expected_md5=file_handle.get("contentMd5"),
            url_is_presigned=True,
            synapse_client=client,
        )


@dataclass
@async_to_sync
class DownloadList(DownloadListSynchronousProtocol):
    """Represents a user's Synapse Download List (cart).

    The download list is a user-scoped cart of files queued for bulk download.
    Files can be added via the Synapse web UI or API and downloaded in batch.

    Note: Files are not packaged into a zip because download lists can exceed
    100 GB. Instead, files are downloaded individually and removed from the list
    after successful download, so interrupted runs are safely resumable.

    Example: Download all files in the cart
        &nbsp;
        Download all files in the user's download list to a local directory.
        ```python
        from synapseclient.models import DownloadList
        from synapseclient import Synapse

        syn = Synapse()
        syn.login()

        manifest_path = DownloadList.download_files(download_location="./data")
        ```
    """

    # No persistent fields — the cart is user-scoped server-side state.

    @staticmethod
    async def download_files_async(
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

        Files that cannot be accessed or fail to download are left in the cart and
        recorded with an error value in the result manifest.

        Arguments:
            download_location: Directory to download files to. Defaults to the
                current working directory.
            parallel: If True, files are downloaded concurrently up to
                max_concurrent at a time using asyncio.gather. If False
                (default), files are downloaded sequentially.
            max_concurrent: Maximum number of files to download concurrently when
                parallel=True. Defaults to 10. Has no effect when
                parallel=False.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Path to the result manifest CSV, which contains all original manifest
            columns plus path (local file path) and error (error message or
            empty string) columns.

        Raises:
            SynapseHTTPError: If the manifest async job fails or the cart is empty
                ("No files available for download").
            SynapseError: If the manifest job completes but produces no local file,
                or if the downloaded CSV has no headers or contains reserved column
                names ("path" or "error").
        """
        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)

        if download_location is not None:
            download_location = os.path.expandvars(
                os.path.expanduser(download_location)
            )

        # 1. Fetch the server-generated manifest and read it into memory
        manifest_path = await DownloadList.get_manifest_async(
            synapse_client=client,
        )
        try:
            columns, rows = await asyncio.to_thread(
                DownloadList._read_manifest_rows, manifest_path
            )
        finally:
            os.remove(manifest_path)

        # 2. Validate manifest columns and append result columns
        columns = DownloadList._validate_and_extend_columns(columns)

        # 3. Download each file in the manifest
        downloaded_files = await DownloadList._download_all_rows(
            rows=rows,
            download_location=download_location,
            parallel=parallel,
            max_concurrent=max_concurrent,
            synapse_client=client,
        )

        # 4. Write the result manifest with path/error columns
        new_manifest_path = await DownloadList._save_result_manifest(
            rows=rows,
            columns=columns,
            download_location=download_location,
        )

        # 5. Remove successfully downloaded files from the cart
        if downloaded_files:
            await remove_from_download_list_async(
                files=downloaded_files,
                synapse_client=client,
            )
        else:
            client.logger.warning(
                "A manifest was created, but no files were downloaded"
            )

        return new_manifest_path

    @staticmethod
    def _read_manifest_rows(
        path: str,
    ) -> tuple[Optional[list[str]], list[dict[str, Any]]]:
        """
        Read the server-generated manifest CSV into memory.

        Arguments:
            path: Local path to the server-generated manifest CSV.
        Returns:
            (columns, rows) where columns is the list of field names and
            rows is a list of row dicts (possibly empty). Returns
            (None, []) if the CSV file has no column headers.
        """
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
            rows = list(reader)
        if not columns:
            return None, []
        return list(columns), rows

    @staticmethod
    def _validate_and_extend_columns(
        columns: Optional[list[str]],
    ) -> list[str]:
        """Validate server manifest columns and append the result columns.

        Ensures the server-generated manifest has headers and does not already
        contain the reserved "path" or "error" column names that are appended
        to the output manifest.

        Arguments:
            columns: Column names from the server manifest, or None if the
                CSV had no headers.

        Raises:
            SynapseError: If columns is None (empty manifest) or contains
                reserved column names.

        Returns:
            The original columns with "path" and "error" appended.
        """
        if columns is None:
            raise SynapseError(
                "Manifest job succeeded but the downloaded CSV has no headers. "
                "This is unexpected — the Synapse server may have returned an empty file."
            )

        if _PATH_COLUMN in columns or _ERROR_COLUMN in columns:
            raise SynapseError(
                "The downloaded manifest CSV contains reserved column names 'path' or 'error'. "
                "This is unexpected and may indicate a malformed manifest from the server, "
                "or Synapse has added these columns."
            )

        return list(columns) + [_PATH_COLUMN, _ERROR_COLUMN]

    @staticmethod
    def _write_result_manifest(
        path: str,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> None:
        """
        Write the annotated result rows to the output manifest CSV.
        Intended to be called via asyncio.to_thread to avoid blocking the
        event loop on synchronous file I/O.

        Arguments:
            path: Destination path for the output manifest CSV.
            columns: Field names for the CSV header, including "path" and
                "error".
            rows: List of row dicts, each mutated by _download_row to
                include "path" and "error" values.
        """
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    async def _download_all_rows(
        rows: list[dict[str, Any]],
        download_location: Optional[str],
        parallel: bool = False,
        max_concurrent: int = 10,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> list[DownloadListItem]:
        """Download all rows from the manifest, either sequentially or concurrently.

        Arguments:
            rows: List of row dicts from the manifest. Each row is mutated in
                place by _download_row to include "path" and
                "error" values.
            download_location: Directory to download files to.
            parallel: If True, rows are downloaded concurrently (bounded by
                max_concurrent) via asyncio.gather. If False, rows are
                downloaded one at a time.
            max_concurrent: Maximum number of concurrent downloads when
                parallel=True. Defaults to 10. Must be at least 1. Has no
                effect when parallel=False.
            synapse_client: Optional Synapse client.

        Raises:
            ValueError: If max_concurrent is less than 1.

        Returns:
            List of DownloadListItem for each successfully downloaded file.
        """
        if max_concurrent < 1:
            raise ValueError(
                f"max_concurrent must be at least 1, got {max_concurrent}."
            )
        if parallel:
            # asyncio.gather schedules all coroutines immediately, so without a
            # semaphore a large cart would fire hundreds of concurrent HTTP requests
            # at once — risking rate-limiting from Synapse and exhausting local
            # file-descriptor / memory limits. The semaphore lets all coroutines
            # be created (preserving gather's result ordering) while ensuring that
            # at most max_concurrent are actually running at any given time.
            sem = asyncio.Semaphore(max_concurrent)

            async def bounded_download(
                row: dict[str, Any],
            ) -> Optional[DownloadListItem]:
                async with sem:
                    return await DownloadList._download_row(
                        row,
                        download_location=download_location,
                        synapse_client=synapse_client,
                    )

            items = await asyncio.gather(*[bounded_download(row) for row in rows])
            return [item for item in items if item is not None]
        else:
            downloaded: list[DownloadListItem] = []
            for row in rows:
                item = await DownloadList._download_row(
                    row,
                    download_location=download_location,
                    synapse_client=synapse_client,
                )
                if item is not None:
                    downloaded.append(item)
            return downloaded

    @staticmethod
    async def _download_row(
        row: dict[str, Any],
        download_location: Optional[str] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> Optional[DownloadListItem]:
        """Download the file described by a manifest row and record the result in place.

        On success, sets row["path"] to the local file path and row["error"]
        to "". On failure, sets row["path"] to "" and row["error"] to the
        error message. Failures are logged but never raised, so one bad file
        does not abort the entire batch.

        Arguments:
            row: A manifest row dict. Must contain "ID" and "versionNumber"
                keys. Modified in place to add "path" and "error" entries.
            download_location: Directory to download the file to. Defaults to
                the Synapse cache location if None.
            synapse_client: Optional Synapse client. Uses cached singleton if omitted.

        Returns:
            A DownloadListItem on success, or None on failure.
        """
        from synapseclient import Synapse
        from synapseclient.models.file import File

        client = Synapse.get_client(synapse_client=synapse_client)
        entity_id = row["ID"]
        version_str = row.get("versionNumber")
        version_number = int(version_str) if version_str else None

        if version_number is None:
            msg = f"Manifest row for {entity_id} is missing versionNumber"
            row[_PATH_COLUMN] = ""
            row[_ERROR_COLUMN] = msg
            client.logger.warning(msg)
            return None

        try:
            file = await File(
                id=entity_id,
                version_number=version_number,
                path=download_location,
            ).get_async(synapse_client=client)
            row[_PATH_COLUMN] = file.path or ""
            row[_ERROR_COLUMN] = ""
            return DownloadListItem(
                file_entity_id=entity_id,
                version_number=version_number,
            )
        except Exception as e:
            row[_PATH_COLUMN] = ""
            row[_ERROR_COLUMN] = str(e)
            client.logger.exception(f"Unable to download {entity_id} v{version_number}")
            return None

    @staticmethod
    async def _save_result_manifest(
        rows: list[dict[str, Any]],
        columns: list[str],
        download_location: Optional[str],
    ) -> str:
        """Write the annotated rows to a new result manifest CSV and return its path.

        Arguments:
            rows: List of row dicts, each mutated by _download_row to
                include "path" and "error" values.
            columns: Field names for the CSV header, including "path" and
                "error".
            download_location: Directory to write the manifest to. Defaults to
                the current working directory if None.

        Returns:
            Absolute path to the written manifest CSV.
        """
        directory = download_location or "."
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, f"manifest_{time.time_ns()}.csv")
        # Run the synchronous CSV write in a thread pool so it does not block
        # the event loop. Blocking the event loop here would stall all other
        # pending coroutines (network requests, timeouts, etc.) for the
        # duration of the file write.
        await asyncio.to_thread(
            DownloadList._write_result_manifest,
            path=path,
            columns=columns,
            rows=rows,
        )
        return path

    @staticmethod
    async def get_manifest_async(
        *,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        destination: str = ".",
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """
        Generate and download the manifest CSV for the current cart contents.
        Submits an async job to Synapse to generate the manifest, then downloads
        the resulting CSV. The manifest contains the same columns as the zip
        manifest downloaded from the Synapse web UI.

        Arguments:
            csv_table_descriptor: Optional CsvTableDescriptor controlling the
                format of the generated CSV (separator, quote character, escape
                character, line ending, and whether the first line is a header).
                When omitted the Synapse defaults are used.
            destination: Directory to download the manifest CSV to. Defaults to
                the current working directory.
            synapse_client: Optional Synapse client. Uses cached singleton if omitted.
        Raises:
            SynapseError: If the async job completes without producing a manifest
        Returns:
            Path to the downloaded manifest CSV.
        """
        manifest_request = DownloadListManifestRequest(
            csv_table_descriptor=csv_table_descriptor or CsvTableDescriptor(),
        )
        await manifest_request.send_job_and_wait_async(
            post_exchange_args={"destination": destination},
            synapse_client=synapse_client,
        )
        if manifest_request.manifest_path is None:
            raise SynapseError(
                "Manifest job completed but no local file was produced. "
                "The download from Synapse may have failed silently."
            )
        return manifest_request.manifest_path

    @staticmethod
    async def add_files_async(
        files: list[DownloadListItem],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> int:
        """
        Add files to the Synapse download list.
        If a file is added with no version specified, the latest version will be downloaded.

        Arguments:
            files: List of DownloadListItem objects identifying the file
                versions to add.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The number of files added.
        """
        return await add_to_download_list_async(
            files=files,
            synapse_client=synapse_client,
        )

    @staticmethod
    async def remove_files_async(
        files: list[DownloadListItem],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> int:
        """
        Remove files from the Synapse download list.
        If a file was added with a version specified, then that version must be specified to remove it.
        If a file was added with no version specified, then no version must be specified to remove it.

        Arguments:
            files: List of DownloadListItem objects identifying the file versions to remove.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The number of files removed.
        """
        return await remove_from_download_list_async(
            files=files,
            synapse_client=synapse_client,
        )

    @staticmethod
    async def clear_async(
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """Clear all files from the Synapse download list (cart).

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last created
                instance from the Synapse class constructor.
        """
        await clear_download_list_async(synapse_client=synapse_client)
