"""Protocol for the specific methods of ManifestGeneratable mixin that have
synchronous counterparts generated at runtime."""

from typing import Dict, List, Optional, Protocol, Tuple

from synapseclient import Synapse


class ManifestGeneratableSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def generate_manifest(
        self,
        path: str,
        manifest_scope: str = "all",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional[str]:
        """Generate a manifest TSV file for all files in this container.

        This method should be called after `sync_from_synapse()` to generate
        a manifest of all downloaded files with their metadata.

        Arguments:
            path: The directory where the manifest file(s) will be written.
            manifest_scope: Controls manifest file generation:

                - "all": Create a manifest in each directory level
                - "root": Create a single manifest at the root path only
                - "suppress": Do not create any manifest files
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The path to the root manifest file if created, or None if suppressed.

        Raises:
            ValueError: If the container has not been synced from Synapse.
            ValueError: If manifest_scope is not one of 'all', 'root', 'suppress'.

        Example: Generate manifest after sync
            Generate a manifest file after syncing from Synapse:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                project = Project(id="syn123").sync_from_synapse(
                    path="/path/to/download"
                )
                manifest_path = project.generate_manifest(
                    path="/path/to/download",
                    manifest_scope="root"
                )
                print(f"Manifest created at: {manifest_path}")
        """
        return None

    @classmethod
    def from_manifest(
        cls,
        manifest_path: str,
        parent_id: str,
        dry_run: bool = False,
        merge_existing_annotations: bool = True,
        associate_activity_to_new_version: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List:
        """Upload files to Synapse from a manifest TSV file.

        This method reads a manifest TSV file and uploads all files defined in it
        to Synapse. The manifest file must contain at minimum the 'path' and 'parent'
        columns.

        Arguments:
            manifest_path: Path to the manifest TSV file.
            parent_id: The Synapse ID of the parent container (Project or Folder)
                where files will be uploaded if not specified in the manifest.
            dry_run: If True, validate the manifest but do not upload.
            merge_existing_annotations: If True, merge annotations with existing
                annotations on the file. If False, replace existing annotations.
            associate_activity_to_new_version: If True, copy the activity
                (provenance) from the previous version to the new version.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            List of File objects that were uploaded.

        Example: Upload files from a manifest
            Upload files from a manifest TSV file:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                files = Project.from_manifest(
                    manifest_path="/path/to/manifest.tsv",
                    parent_id="syn123"
                )
                for file in files:
                    print(f"Uploaded: {file.name} ({file.id})")
        """
        return []

    @staticmethod
    def validate_manifest(
        manifest_path: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Tuple[bool, List[str]]:
        """Validate a manifest TSV file without uploading.

        This method validates a manifest file to ensure it is properly formatted
        and all paths exist.

        Arguments:
            manifest_path: Path to the manifest TSV file.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Tuple of (is_valid, list_of_error_messages). If the manifest is valid,
            is_valid will be True and the list will be empty.

        Example: Validate a manifest file
            Validate a manifest file before uploading:

                from synapseclient.models import Project

                is_valid, errors = Project.validate_manifest(
                    manifest_path="/path/to/manifest.tsv"
                )
                if is_valid:
                    print("Manifest is valid")
                else:
                    for error in errors:
                        print(f"Error: {error}")
        """
        return (True, [])

    def get_manifest_data(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Tuple[List[str], List[Dict[str, str]]]:
        """Get manifest data for all files in this container.

        This method extracts metadata from all files that have been synced
        to this container. The data can be used to generate a manifest file
        or for other purposes.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Tuple of (keys, data) where keys is a list of column headers
            and data is a list of dictionaries, one per file, containing
            the file metadata.

        Raises:
            ValueError: If the container has not been synced from Synapse.

        Example: Get manifest data
            Get manifest data for all files in a project:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                project = Project(id="syn123").sync_from_synapse(
                    path="/path/to/download"
                )
                keys, data = project.get_manifest_data()
                for row in data:
                    print(f"File: {row['name']} at {row['path']}")
        """
        return ([], [])

    @staticmethod
    def generate_download_list_manifest(
        download_path: str,
        csv_separator: str = ",",
        include_header: bool = True,
        timeout: int = 120,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """Generate a manifest file from the current user's download list.

        This method creates a CSV manifest containing metadata about all files in
        the user's download list. The manifest is generated server-side by Synapse
        and then downloaded to the specified path.

        This is interoperable with the Synapse download list feature and provides
        a way to export the download list as a manifest file that can be used for
        bulk operations.

        Arguments:
            download_path: The local directory path where the manifest will be saved.
            csv_separator: The delimiter character for the CSV file.
                Defaults to "," for comma-separated values. Use "\t" for tab-separated.
            include_header: Whether to include column headers in the first row.
                Defaults to True.
            timeout: The number of seconds to wait for the job to complete.
                Defaults to 120 seconds.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The full path to the downloaded manifest file.

        Example: Generate manifest from download list
            Generate a manifest from your Synapse download list:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                # Generate manifest from download list
                manifest_path = Project.generate_download_list_manifest(
                    download_path="/path/to/download"
                )
                print(f"Manifest downloaded to: {manifest_path}")
        """
        return ""
