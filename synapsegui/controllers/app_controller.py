"""
Main application controller - orchestrates components
"""
import asyncio
import threading
from typing import Any, Dict

from ..models.config import ConfigManager
from ..models.synapse_client import SynapseClientManager
from ..utils.progress import ProgressManager


class ApplicationController:
    """
    Main application controller that coordinates between UI components and business logic.

    Handles user interactions, manages the Synapse client, and orchestrates
    data flow between the UI components and backend services.
    """

    def __init__(self) -> None:
        """Initialize the application controller with necessary managers and components."""
        self.synapse_client = SynapseClientManager()
        self.config_manager = ConfigManager()
        self.progress_manager = ProgressManager()

        self.login_component = None
        self.download_component = None
        self.upload_component = None
        self.output_component = None
        self.bulk_download_component = None
        self.bulk_upload_component = None
        self.root = None

    def set_ui_components(
        self,
        login_component,
        download_component,
        upload_component,
        output_component,
        root,
        bulk_download_component=None,
        bulk_upload_component=None,
    ) -> None:
        """
        Set references to UI components for interaction.

        Args:
            login_component: Login UI component
            download_component: Download UI component
            upload_component: Upload UI component
            output_component: Output/logging UI component
            root: Main tkinter window for thread-safe GUI updates (required)
            bulk_download_component: Bulk download UI component (optional)
            bulk_upload_component: Bulk upload UI component (optional)
        """
        if root is None:
            raise ValueError("Root window is required for thread-safe GUI operations")

        self.login_component = login_component
        self.download_component = download_component
        self.upload_component = upload_component
        self.output_component = output_component
        self.bulk_download_component = bulk_download_component
        self.bulk_upload_component = bulk_upload_component
        self.root = root

    def handle_login(self, mode: str, credentials: Dict[str, Any]) -> None:
        """
        Handle login request from UI components.

        Args:
            mode: Login mode ("manual" or "config")
            credentials: Dictionary containing login credentials
        """

        def login_worker() -> None:
            """Worker function to handle login in background thread."""
            if mode == "manual":
                result = asyncio.run(
                    self.synapse_client.login_manual(
                        credentials.get("username", ""), credentials.get("token", "")
                    )
                )
            else:  # config
                result = asyncio.run(
                    self.synapse_client.login_with_profile(
                        credentials.get("profile", "")
                    )
                )

            # Update UI based on result - marshal to main thread
            def update_ui_on_success():
                """Update UI components on successful login."""
                self.login_component.update_login_state(
                    True, result.get("username", "")
                )
                self._enable_operations(True)
                self.output_component.log_message(
                    f"Login successful! Logged in as: {result.get('username', '')}"
                )
                self.login_component.set_login_button_state(True)

            def update_ui_on_failure():
                """Update UI components on failed login."""
                self.login_component.update_login_state(False, error=result["error"])
                self.output_component.log_message(
                    f"Login failed: {result['error']}", error=True
                )
                self.login_component.set_login_button_state(True)

            if result["success"]:
                self.root.after(0.001, update_ui_on_success)
            else:
                self.root.after(0.001, update_ui_on_failure)

        self.login_component.set_login_button_state(False)
        threading.Thread(target=login_worker, daemon=True).start()

    def handle_logout(self) -> None:
        """Handle logout request and update UI components accordingly."""
        self.synapse_client.logout()
        self.login_component.update_login_state(False)
        self._enable_operations(False)
        self.output_component.log_message("Logged out successfully")
        self.login_component.set_login_button_state(True)

    def handle_download(
        self, synapse_id: str, version: str, download_path: str
    ) -> None:
        """
        Handle file download request from UI.

        Args:
            synapse_id: Synapse entity ID to download
            version: Entity version (optional, empty string if not specified)
            download_path: Local path where file should be downloaded
        """

        def download_worker() -> None:
            """Worker function to handle download in background thread."""
            version_num = None
            if version:
                try:
                    version_num = int(version)
                except ValueError:
                    self.root.after(
                        0.001,
                        lambda: self.output_component.log_message(
                            "Version must be a number", error=True
                        ),
                    )
                    return

            def progress_callback(progress: int, message: str) -> None:
                """Callback for download progress updates."""

                def update_progress():
                    self.download_component.update_progress(progress, message)

                self.root.after(0.001, update_progress)

            def detail_callback(detail_message: str) -> None:
                """Callback for detailed download messages."""

                def log_message():
                    self.output_component.log_message(detail_message)

                self.root.after(0.001, log_message)

            result = asyncio.run(
                self.synapse_client.download_file(
                    synapse_id,
                    version_num,
                    download_path,
                    progress_callback,
                    detail_callback,
                )
            )

            def handle_success():
                """Handle successful download result."""
                self.output_component.log_message(f"Downloaded: {result['path']}")
                self.download_component.show_success(f"Downloaded: {result['path']}")

            def handle_error():
                """Handle failed download result."""
                self.output_component.log_message(
                    f"Download failed: {result['error']}", error=True
                )
                self.download_component.show_error(result["error"])

            if result["success"]:
                self.root.after(0.001, handle_success)
            else:
                self.root.after(0.001, handle_error)

        self.download_component.start_operation()
        threading.Thread(target=download_worker, daemon=True).start()

    def handle_upload(
        self, file_path: str, parent_id: str, entity_id: str, name: str
    ) -> None:
        """
        Handle file upload request from UI.

        Args:
            file_path: Local path to file to upload
            parent_id: Parent entity ID for new files (empty for updates)
            entity_id: Entity ID for file updates (empty for new files)
            name: Entity name (optional)
        """

        def upload_worker() -> None:
            """Worker function to handle upload in background thread."""

            def progress_callback(progress: int, message: str) -> None:
                """Callback for upload progress updates."""

                def update_progress():
                    self.upload_component.update_progress(progress, message)

                self.root.after(0.001, update_progress)

            def detail_callback(detail_message: str) -> None:
                """Callback for detailed upload messages."""

                def log_message():
                    self.output_component.log_message(detail_message)

                self.root.after(0.001, log_message)

            result = asyncio.run(
                self.synapse_client.upload_file(
                    file_path,
                    parent_id or None,
                    entity_id or None,
                    name or None,
                    progress_callback,
                    detail_callback,
                )
            )

            def handle_success():
                """Handle successful upload result."""
                message = (
                    f"Created/Updated entity: {result['entity_id']} - {result['name']}"
                )
                self.output_component.log_message(message)
                self.upload_component.show_success(message)

            def handle_error():
                """Handle failed upload result."""
                self.output_component.log_message(
                    f"Upload failed: {result['error']}", error=True
                )
                self.upload_component.show_error(result["error"])

            if result["success"]:
                self.root.after(0.001, handle_success)
            else:
                self.root.after(0.001, handle_error)

        self.upload_component.start_operation()
        threading.Thread(target=upload_worker, daemon=True).start()

    def handle_enumerate(self, container_id: str, recursive: bool) -> None:
        """
        Handle container enumeration request from UI.

        Args:
            container_id: Synapse container ID to enumerate
            recursive: Whether to enumerate contents recursively
        """

        def enumerate_worker() -> None:
            """Worker function to handle enumeration in background thread."""
            result = asyncio.run(
                self.synapse_client.enumerate_container(container_id, recursive)
            )

            if self.bulk_download_component:

                def handle_success():
                    self.bulk_download_component.handle_enumeration_result(
                        result["items"]
                    )

                def handle_error():
                    self.bulk_download_component.handle_enumeration_result(
                        [], result["error"]
                    )

                if result["success"]:
                    self.root.after(0.001, handle_success)
                else:
                    self.root.after(0.001, handle_error)

        threading.Thread(target=enumerate_worker, daemon=True).start()

    def _enable_operations(self, enabled: bool) -> None:
        """
        Enable or disable operation buttons based on login state.

        Args:
            enabled: True to enable operations, False to disable
        """
        if self.download_component:
            self.download_component.set_enabled(enabled)
        if self.upload_component:
            self.upload_component.set_enabled(enabled)
        if self.bulk_download_component:
            pass
        if self.bulk_upload_component:
            pass

    def handle_bulk_download(
        self, items: list, download_path: str, recursive: bool
    ) -> None:
        """
        Handle bulk download request from UI.

        Args:
            items: List of BulkItem objects to download
            download_path: Local directory path where files will be downloaded
            recursive: Whether to download folder contents recursively.
                      If True, all subfolders and files within folders will be downloaded.
                      If False, only the immediate contents of folders will be downloaded.
        """

        def bulk_download_worker() -> None:
            """Worker function to handle bulk download in background thread."""
            # Start the operation - marshal to main thread
            if self.bulk_download_component:
                self.root.after(0, self.bulk_download_component.start_bulk_operation)

            result = asyncio.run(
                self.synapse_client.bulk_download(
                    items,
                    download_path,
                    recursive,
                    self._on_bulk_download_progress_update,
                    self._on_detail_message,
                )
            )

            if result["success"]:

                def handle_success():
                    self.output_component.log_message(
                        f"Bulk download complete: {result['summary']}"
                    )
                    if self.bulk_download_component:
                        self.bulk_download_component.complete_bulk_operation(
                            True, result["summary"]
                        )

                self.root.after(0.001, handle_success)
            else:

                def handle_error():
                    self.output_component.log_message(
                        f"Bulk download failed: {result['error']}", error=True
                    )
                    if self.bulk_download_component:
                        self.bulk_download_component.complete_bulk_operation(
                            False, result["error"]
                        )

                self.root.after(0.001, handle_error)

        self.output_component.log_message(
            f"Starting bulk download of {len(items)} items..."
        )
        threading.Thread(target=bulk_download_worker, daemon=True).start()

    def handle_bulk_upload(
        self, items: list, parent_id: str, preserve_structure: bool
    ) -> None:
        """
        Handle bulk upload request from UI.

        Args:
            items: List of BulkItem objects to upload
            parent_id: Synapse parent folder ID for uploads
            preserve_structure: Whether to preserve directory structure
        """

        def bulk_upload_worker() -> None:
            """Worker function to handle bulk upload in background thread."""
            # Start the operation - marshal to main thread
            if self.bulk_upload_component:
                self.root.after(0, self.bulk_upload_component.start_bulk_operation)

            result = asyncio.run(
                self.synapse_client.bulk_upload(
                    items,
                    parent_id,
                    preserve_structure,
                    self._on_bulk_upload_progress_update,
                    self._on_detail_message,
                )
            )

            if result["success"]:

                def handle_success():
                    self.output_component.log_message(
                        f"Bulk upload complete: {result['summary']}"
                    )
                    if self.bulk_upload_component:
                        self.bulk_upload_component.complete_bulk_operation(
                            True, result["summary"]
                        )

                self.root.after(0.001, handle_success)
            else:

                def handle_error():
                    self.output_component.log_message(
                        f"Bulk upload failed: {result['error']}", error=True
                    )
                    if self.bulk_upload_component:
                        self.bulk_upload_component.complete_bulk_operation(
                            False, result["error"]
                        )

                self.root.after(0.001, handle_error)

        self.output_component.log_message(
            f"Starting bulk upload of {len(items)} items..."
        )
        threading.Thread(target=bulk_upload_worker, daemon=True).start()

    def _on_bulk_download_progress_update(self, progress: int, message: str) -> None:
        """
        Handle progress updates from bulk download operations.

        Args:
            progress: Progress percentage (0-100)
            message: Progress message to display
        """

        def update_ui():
            if self.bulk_download_component:
                self.bulk_download_component.update_progress(progress, message)
            if self.output_component:
                self.output_component.log_message(
                    f"Download Progress: {progress}% - {message}"
                )

        self.root.after(0.001, update_ui)

    def _on_bulk_upload_progress_update(self, progress: int, message: str) -> None:
        """
        Handle progress updates from bulk upload operations.

        Args:
            progress: Progress percentage (0-100)
            message: Progress message to display
        """

        def update_ui():
            if self.bulk_upload_component:
                self.bulk_upload_component.update_progress(progress, message)
            if self.output_component:
                self.output_component.log_message(
                    f"Upload Progress: {progress}% - {message}"
                )

        self.root.after(0.001, update_ui)

    def _on_progress_update(self, progress: int, message: str) -> None:
        """
        Handle progress updates from single file operations.

        Args:
            progress: Progress percentage (0-100)
            message: Progress message to display
        """
        if self.output_component:
            self.output_component.log_message(f"Progress: {progress}% - {message}")

    def _on_detail_message(self, message: str) -> None:
        """
        Handle detailed progress messages from bulk operations.

        Args:
            message: Detailed message to log
        """

        def update_ui():
            if self.output_component:
                self.output_component.log_message(message)

        self.root.after(0.001, update_ui)
