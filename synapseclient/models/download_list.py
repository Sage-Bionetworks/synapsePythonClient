"""Models for interacting with Synapse's Download List functionality.

This module provides classes for generating manifest files from a user's download list
using the Synapse Asynchronous Job service.

See: https://rest-docs.synapse.org/rest/POST/download/list/manifest/async/start.html
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants.concrete_types import DOWNLOAD_LIST_MANIFEST_REQUEST
from synapseclient.core.download import download_by_file_handle
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.protocols.download_list_protocol import (
    DownloadListManifestRequestSynchronousProtocol,
)
from synapseclient.models.table_components import CsvTableDescriptor


@dataclass
@async_to_sync
class DownloadListManifestRequest(
    DownloadListManifestRequestSynchronousProtocol, AsynchronousCommunicator
):
    """
    A request to generate a manifest file (CSV) of the current user's download list.

    This class uses the Synapse Asynchronous Job service to generate a manifest file
    containing metadata about files in the user's download list. The manifest can be
    used to download files or for record-keeping purposes.

    See: https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/download/DownloadListManifestRequest.html

    Attributes:
        csv_table_descriptor: Optional CSV formatting options for the manifest.
        result_file_handle_id: The file handle ID of the generated manifest (populated after completion).

    Example: Generate a manifest from download list
        Generate a CSV manifest from your download list:

            from synapseclient.models import DownloadListManifestRequest
            import synapseclient

            synapseclient.login()

            # Create and send the request
            request = DownloadListManifestRequest()
            request.send_job_and_wait()

            print(f"Manifest file handle: {request.result_file_handle_id}")

    Example: Generate manifest with custom CSV formatting
        Use custom separator and quote characters:

            from synapseclient.models import DownloadListManifestRequest, CsvTableDescriptor
            import synapseclient

            synapseclient.login()

            request = DownloadListManifestRequest(
                csv_table_descriptor=CsvTableDescriptor(
                    separator="\t",  # Tab-separated
                    is_first_line_header=True
                )
            )
            request.send_job_and_wait()
    """

    concrete_type: str = field(
        default=DOWNLOAD_LIST_MANIFEST_REQUEST, repr=False, compare=False
    )
    """The concrete type of this request."""

    csv_table_descriptor: Optional[CsvTableDescriptor] = None
    """Optional CSV formatting options for the manifest file."""

    result_file_handle_id: Optional[str] = None
    """The file handle ID of the generated manifest file. Populated after the job completes."""

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Convert this request to the format expected by the Synapse REST API.

        Returns:
            A dictionary containing the request body for the Synapse API.
        """
        request = {
            "concreteType": self.concrete_type,
        }
        if self.csv_table_descriptor:
            request[
                "csvTableDescriptor"
            ] = self.csv_table_descriptor.to_synapse_request()
        delete_none_keys(request)
        return request

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> Self:
        """
        Populate this object from a Synapse REST API response.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            This object with fields populated from the response.
        """
        self.result_file_handle_id = synapse_response.get("resultFileHandleId", None)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: "DownloadListManifestRequest_send_job_and_wait"
    )
    async def send_job_and_wait_async(
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
        return await super().send_job_and_wait_async(
            post_exchange_args=post_exchange_args,
            timeout=timeout,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: "DownloadListManifestRequest_download_manifest"
    )
    async def download_manifest_async(
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
        if not self.result_file_handle_id:
            raise ValueError(
                "Manifest has not been generated yet. "
                "Call send_job_and_wait() before downloading."
            )

        # Download the file handle using the download module
        # For download list manifests, the synapse_id parameter is set to the file handle ID
        # because these manifests are not associated with a specific entity. The download
        # service handles this case by using the file handle directly.
        downloaded_path = await download_by_file_handle(
            file_handle_id=self.result_file_handle_id,
            synapse_id=self.result_file_handle_id,
            entity_type="FileEntity",
            destination=download_path,
            synapse_client=synapse_client,
        )

        return downloaded_path
