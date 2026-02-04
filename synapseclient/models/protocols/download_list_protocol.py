"""Protocol for the specific methods of download list classes that have synchronous counterparts
generated at runtime."""

from typing import Any, Dict, Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse


class DownloadListManifestRequestSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def send_job_and_wait(
        self,
        post_exchange_args: Optional[Dict[str, Any]] = None,
        timeout: int = 120,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Self:
        """Send the job to the Asynchronous Job service and wait for it to complete.

        This method sends the manifest generation request to Synapse and waits
        for the job to complete. After completion, the `result_file_handle_id`
        attribute will be populated.

        Arguments:
            post_exchange_args: Additional arguments to pass to the request.
            timeout: The number of seconds to wait for the job to complete or progress
                before raising a SynapseTimeoutError. Defaults to 120.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            This instance with `result_file_handle_id` populated.

        Raises:
            SynapseTimeoutError: If the job does not complete within the timeout.
            SynapseError: If the job fails.

        Example: Generate a manifest
            Generate a manifest from the download list:

                from synapseclient.models import DownloadListManifestRequest
                import synapseclient

                synapseclient.login()

                request = DownloadListManifestRequest()
                request.send_job_and_wait()
                print(f"Manifest file handle: {request.result_file_handle_id}")
        """
        return self

    def download_manifest(
        self,
        download_path: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """
        Download the generated manifest file to a local path.

        This method should be called after `send_job_and_wait()` has completed
        successfully and `result_file_handle_id` is populated.

        Arguments:
            download_path: The local directory path where the manifest will be saved.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The full path to the downloaded manifest file.

        Raises:
            ValueError: If the manifest has not been generated yet (no result_file_handle_id).

        Example: Download the manifest after generation
            Generate and download a manifest:

                from synapseclient.models import DownloadListManifestRequest
                import synapseclient

                synapseclient.login()

                request = DownloadListManifestRequest()
                request.send_job_and_wait()

                manifest_path = request.download_manifest(download_path="/path/to/download")
                print(f"Manifest downloaded to: {manifest_path}")
        """
        return ""
